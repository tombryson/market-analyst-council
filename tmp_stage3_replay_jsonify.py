import argparse
import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.council import stage3_synthesize_final
from backend.main import build_enhanced_context
from backend.openrouter import query_model
from backend.template_loader import get_template_loader


def _parse_json_loose(text: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    raw = (text or "").strip()
    if not raw:
        return None, "empty response"
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None, None
    except json.JSONDecodeError:
        pass

    block = re.search(r"```json\s*(\{.*?\})\s*```", raw, re.DOTALL)
    if block:
        try:
            parsed = json.loads(block.group(1))
            return parsed if isinstance(parsed, dict) else None, None
        except json.JSONDecodeError as exc:
            return None, f"json codeblock parse error: {exc}"

    obj = re.search(r"\{.*\}", raw, re.DOTALL)
    if obj:
        try:
            parsed = json.loads(obj.group(0))
            return parsed if isinstance(parsed, dict) else None, None
        except json.JSONDecodeError as exc:
            return None, f"json object parse error: {exc}"

    return None, "no parseable json found"


def _build_label_map(stage1_results: List[Dict[str, Any]]) -> Dict[str, str]:
    return {
        f"Response {chr(65 + idx)}": row.get("model", f"unknown_{idx}")
        for idx, row in enumerate(stage1_results)
    }


async def _jsonify_stage3(
    *,
    model: str,
    timeout_s: float,
    max_tokens: int,
    template_id: str,
    chairman_output: Dict[str, Any],
) -> Dict[str, Any]:
    loader = get_template_loader()
    template = loader.get_template(template_id) or {}
    schema = ((template.get("output_schema") or {}).get("structure")) or {}
    schema_json = json.dumps(schema, indent=2)
    raw_response = chairman_output.get("response") or ""
    structured_candidate = chairman_output.get("structured_data")

    prompt = f"""You are a strict JSON normalizer for investment analysis outputs.
Return ONLY valid JSON that conforms to this target schema shape:

{schema_json}

Input chairman output may be prose, malformed JSON, or partial JSON.

Rules:
1. Preserve existing facts/values from the input when present.
2. Do not invent numeric values or citations.
3. If a field is missing, use null, empty string, or [] as appropriate.
4. Keep neutral tone; do not add new claims.
5. Output only a single valid JSON object, no markdown.

Chairman raw output:
{raw_response}

Chairman parsed candidate JSON:
{json.dumps(structured_candidate, ensure_ascii=False, default=str)}
"""

    response = await query_model(
        model,
        [{"role": "user", "content": prompt}],
        timeout=timeout_s,
        max_tokens=max_tokens if max_tokens > 0 else None,
    )
    if response is None:
        return {
            "model": model,
            "response": "",
            "structured_data": None,
            "parse_error": "jsonifier model failed to respond",
        }

    response_text = response.get("content") or ""
    parsed, parse_error = _parse_json_loose(response_text)
    return {
        "model": model,
        "response": response_text,
        "structured_data": parsed,
        "parse_error": parse_error,
        "usage": response.get("usage"),
        "finish_reason": response.get("finish_reason"),
        "provider": response.get("provider"),
    }


async def _replay_one(
    *,
    artifact_path: Path,
    chairman_model: str,
    jsonifier_model: str,
    jsonifier_timeout_s: float,
    jsonifier_max_tokens: int,
) -> Dict[str, Any]:
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))

    stage1_results = (
        payload.get("stage1_results_for_stage3")
        or payload.get("stage1_results")
        or []
    )
    stage2_results = payload.get("stage2_results") or []
    if not stage1_results or not stage2_results:
        return {
            "source_artifact": str(artifact_path),
            "error": "artifact missing stage1/stage2 results for stage3 replay",
        }

    label_to_model = _build_label_map(stage1_results)

    effective_query = payload.get("effective_query") or ""
    search_results = (payload.get("metadata") or {}).get("aggregated_search_results") or {}
    market_facts = (payload.get("input_audit") or {}).get("market_facts") or {}
    enhanced_context = build_enhanced_context(
        effective_query,
        search_results,
        [],
        market_facts=market_facts,
    )

    selection = payload.get("selection") or {}
    template_id = selection.get("template_id") or "gold_miner"
    company_name = selection.get("company_name")
    exchange = selection.get("exchange")

    ticker = None
    primary_sd = ((payload.get("stage3_result_primary") or {}).get("structured_data")) or {}
    if isinstance(primary_sd, dict):
        ticker = primary_sd.get("ticker")
    if not ticker:
        ticker = ((payload.get("stage3_result") or {}).get("structured_data") or {}).get("ticker")

    stage3_result = await stage3_synthesize_final(
        enhanced_context,
        stage1_results,
        stage2_results,
        label_to_model=label_to_model,
        use_structured_analysis=True,
        template_id=template_id,
        ticker=ticker,
        company_name=company_name,
        exchange=exchange,
        chairman_model=chairman_model,
        market_facts=market_facts,
        evidence_pack=(search_results.get("evidence_pack") if isinstance(search_results, dict) else None),
    )

    jsonified = await _jsonify_stage3(
        model=jsonifier_model,
        timeout_s=jsonifier_timeout_s,
        max_tokens=jsonifier_max_tokens,
        template_id=template_id,
        chairman_output=stage3_result,
    )

    return {
        "source_artifact": str(artifact_path),
        "selection": selection,
        "chairman_model": chairman_model,
        "jsonifier_model": jsonifier_model,
        "label_to_model": label_to_model,
        "stage3_result": stage3_result,
        "stage3_jsonified": jsonified,
    }


def _default_artifacts(outputs_dir: Path, count: int) -> List[Path]:
    candidates = sorted(
        [
            p
            for p in outputs_dir.glob("quality*.json")
            if p.is_file()
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[:count]


async def main() -> None:
    import backend.config as config

    parser = argparse.ArgumentParser(description="Replay Stage 3 on prior artifacts with optional JSON normalization pass.")
    parser.add_argument(
        "--artifact",
        action="append",
        default=[],
        help="Artifact path (repeatable). If omitted, uses latest quality*.json files.",
    )
    parser.add_argument(
        "--latest-count",
        type=int,
        default=3,
        help="When no --artifact provided, replay this many latest quality artifacts.",
    )
    parser.add_argument(
        "--chairman-model",
        default="google/gemini-3-pro-preview",
        help="Chairman model for Stage 3 replay.",
    )
    parser.add_argument(
        "--jsonifier-model",
        default="openai/gpt-4o-mini",
        help="Model used to normalize chairman output into strict JSON.",
    )
    parser.add_argument(
        "--chairman-timeout",
        type=float,
        default=420.0,
        help="Chairman call timeout seconds.",
    )
    parser.add_argument(
        "--jsonifier-timeout",
        type=float,
        default=180.0,
        help="JSONifier call timeout seconds.",
    )
    parser.add_argument(
        "--chairman-max-tokens",
        type=int,
        default=16000,
        help="Chairman completion token cap.",
    )
    parser.add_argument(
        "--jsonifier-max-tokens",
        type=int,
        default=8000,
        help="JSONifier completion token cap.",
    )
    args = parser.parse_args()
    config.CHAIRMAN_TIMEOUT_SECONDS = float(args.chairman_timeout)
    config.CHAIRMAN_MAX_OUTPUT_TOKENS = int(args.chairman_max_tokens)

    outputs_dir = Path(__file__).resolve().parent / "outputs"
    artifacts: List[Path]
    if args.artifact:
        artifacts = [Path(p).resolve() for p in args.artifact]
    else:
        artifacts = _default_artifacts(outputs_dir, max(1, args.latest_count))

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = outputs_dir / f"stage3_replay_batch_{timestamp}.json"
    batch_results: List[Dict[str, Any]] = []

    for artifact in artifacts:
        print(f"[stage3-replay] artifact={artifact}")
        if not artifact.exists():
            batch_results.append(
                {"source_artifact": str(artifact), "error": "artifact not found"}
            )
            continue
        run_result = await _replay_one(
            artifact_path=artifact,
            chairman_model=args.chairman_model,
            jsonifier_model=args.jsonifier_model,
            jsonifier_timeout_s=args.jsonifier_timeout,
            jsonifier_max_tokens=args.jsonifier_max_tokens,
        )
        batch_results.append(run_result)

    output = {
        "created_at": datetime.utcnow().isoformat(),
        "artifacts_count": len(artifacts),
        "chairman_model": args.chairman_model,
        "chairman_timeout_seconds": config.CHAIRMAN_TIMEOUT_SECONDS,
        "chairman_max_output_tokens": config.CHAIRMAN_MAX_OUTPUT_TOKENS,
        "jsonifier_model": args.jsonifier_model,
        "results": batch_results,
    }
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    asyncio.run(main())
