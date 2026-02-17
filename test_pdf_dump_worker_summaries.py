#!/usr/bin/env python3
"""
Worker summarizer for PDF dump announcements.

Reads markdown files produced by test_perplexity_pdf_dump.py, applies a rubric
with a lightweight model, and emits structured JSON summaries.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

from backend.openrouter import query_model


DEFAULT_WORKER_MODEL = "openai/gpt-4o-mini"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize pdf_dump announcements with rubric")
    parser.add_argument(
        "--dump-dir",
        required=True,
        help="Directory containing pdf dump markdown files",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_WORKER_MODEL,
        help=f"Worker model (default: {DEFAULT_WORKER_MODEL})",
    )
    parser.add_argument(
        "--max-key-points",
        type=int,
        default=30,
        help="Maximum key points per kept document",
    )
    parser.add_argument(
        "--max-doc-chars",
        type=int,
        default=120000,
        help="Maximum decoded-text chars sent to model per doc",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=180.0,
        help="OpenRouter request timeout per document",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help="Parallel worker calls",
    )
    parser.add_argument(
        "--max-docs",
        type=int,
        default=0,
        help="Optional cap for number of docs to process (0=all)",
    )
    parser.add_argument(
        "--output-markdown",
        default="",
        help="Optional output markdown path (default: <dump-dir>/announcement_summaries.md)",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional output json path (default: <dump-dir>/announcement_summaries.json)",
    )
    return parser.parse_args()


def _extract_fenced_json(text: str) -> Optional[Dict[str, Any]]:
    payload = str(text or "").strip()
    if not payload:
        return None
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    fence = re.search(r"```json\s*(\{.*?\})\s*```", payload, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        try:
            obj = json.loads(fence.group(1))
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

    first = payload.find("{")
    last = payload.rfind("}")
    if first >= 0 and last > first:
        candidate = payload[first : last + 1]
        try:
            obj = json.loads(candidate)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
    return None


def _read_dump_markdown(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines()

    title = ""
    source_url = ""
    pdf_url = ""
    domain = ""
    published_at = ""
    page_count = 0
    decoded_chars = 0

    for line in lines[:80]:
        stripped = line.strip()
        if stripped.startswith("# PDF Dump:"):
            title = stripped.replace("# PDF Dump:", "", 1).strip()
        elif stripped.startswith("- source_url:"):
            source_url = stripped.replace("- source_url:", "", 1).strip()
        elif stripped.startswith("- pdf_url:"):
            pdf_url = stripped.replace("- pdf_url:", "", 1).strip()
        elif stripped.startswith("- domain:"):
            domain = stripped.replace("- domain:", "", 1).strip()
        elif stripped.startswith("- published_at:"):
            published_at = stripped.replace("- published_at:", "", 1).strip()
        elif stripped.startswith("- page_count:"):
            try:
                page_count = int(stripped.replace("- page_count:", "", 1).strip())
            except Exception:
                page_count = 0
        elif stripped.startswith("- decoded_chars:"):
            try:
                decoded_chars = int(stripped.replace("- decoded_chars:", "", 1).strip())
            except Exception:
                decoded_chars = 0

    marker = "## Full Decoded Text"
    full_text = raw
    if marker in raw:
        after = raw.split(marker, 1)[1]
        full_text = after.strip()

    return {
        "file": str(path),
        "file_name": path.name,
        "title": title or path.stem,
        "source_url": source_url,
        "pdf_url": pdf_url,
        "domain": domain,
        "published_at": published_at,
        "page_count": page_count,
        "decoded_chars": decoded_chars,
        "full_text": full_text.strip(),
    }


def _build_worker_prompt(
    doc: Dict[str, Any],
    max_key_points: int,
    max_doc_chars: int,
) -> str:
    text = str(doc.get("full_text", ""))
    truncated = False
    if len(text) > max_doc_chars:
        text = text[:max_doc_chars]
        truncated = True

    rubric = f"""
You are a strict financial-announcement triage worker.

Task:
1) Determine if the document is price-sensitive or important for investment analysis.
2) If NOT important, stop early and return minimal JSON classification only.
3) If important, return a compact structured summary with at most {max_key_points} key points.

Materiality/importance checks:
- Funding and capital structure: debt facilities, equity raises, dilution, covenant changes, liquidity changes
- Project economics: NPV, IRR, AISC, capex, opex, production profile, resource/reserve updates
- Development/timeline: first gold, commissioning, key milestones, delays/accelerations
- Regulatory/permitting/legal outcomes that alter project risk or timeline
- Management/strategy changes with likely execution impact
- Quantitative sensitivities (gold price, FX, cost assumptions)
- Guidance changes, major contracts, offtake, M&A, asset sales/acquisitions

Low-signal examples (usually NOT important by themselves):
- boilerplate legal notices, generic cleansing/quotation notices, procedural admin with no valuation impact.

Return STRICT JSON only with this schema:
{{
  "doc_id": "<file name>",
  "price_sensitive": {{
    "is_price_sensitive": true/false,
    "confidence": 0.0-1.0,
    "reason": "<short reason>"
  }},
  "importance": {{
    "is_important": true/false,
    "importance_score": 0-100,
    "tier": "critical|high|medium|low|ignore",
    "keep_for_injection": true/false,
    "reason": "<short reason>"
  }},
  "summary": {{
    "one_line": "<single line summary>",
    "key_points": ["... up to {max_key_points} ..."],
    "numeric_facts": [
      {{"metric":"", "value":"", "unit":"", "context":"", "source_snippet":""}}
    ],
    "timeline_milestones": [
      {{"milestone":"", "target_window":"", "direction":"new|reconfirmed|delayed|accelerated|unclear", "source_snippet":""}}
    ],
    "capital_structure": ["..."],
    "catalysts_next_12m": ["..."],
    "risks_headwinds": ["..."],
    "market_impact_assessment": "<2-4 lines>"
  }},
  "extraction_quality": {{
    "text_truncated_for_model": true/false,
    "signal_quality": "high|medium|low",
    "notes": ["..."]
  }}
}}

Rules:
- If importance.is_important=false, set keep_for_injection=false and keep summary fields empty/minimal.
- Never exceed {max_key_points} key points.
- Prefer concrete numeric/timeline facts and avoid fluff.
- Do not output markdown, code fences, or prose outside JSON.
""".strip()

    metadata = {
        "doc_id": doc.get("file_name", ""),
        "title": doc.get("title", ""),
        "source_url": doc.get("source_url", ""),
        "pdf_url": doc.get("pdf_url", ""),
        "domain": doc.get("domain", ""),
        "published_at": doc.get("published_at", ""),
        "decoded_chars_in_file": int(doc.get("decoded_chars", 0) or 0),
        "text_chars_sent_to_model": len(text),
        "text_truncated_for_model": truncated,
    }

    return (
        f"{rubric}\n\n"
        f"DOCUMENT METADATA:\n{json.dumps(metadata, ensure_ascii=True)}\n\n"
        f"DOCUMENT TEXT:\n{text}"
    )


def _normalize_summary_object(
    obj: Dict[str, Any],
    doc: Dict[str, Any],
    max_key_points: int,
    text_truncated: bool,
) -> Dict[str, Any]:
    out = dict(obj or {})
    out["doc_id"] = str(out.get("doc_id") or doc.get("file_name", ""))

    ps = out.get("price_sensitive", {})
    if not isinstance(ps, dict):
        ps = {}
    ps_out = {
        "is_price_sensitive": bool(ps.get("is_price_sensitive", False)),
        "confidence": float(ps.get("confidence", 0.0) or 0.0),
        "reason": str(ps.get("reason", "")).strip(),
    }
    ps_out["confidence"] = max(0.0, min(1.0, ps_out["confidence"]))
    out["price_sensitive"] = ps_out

    imp = out.get("importance", {})
    if not isinstance(imp, dict):
        imp = {}
    tier = str(imp.get("tier", "ignore")).strip().lower()
    if tier not in {"critical", "high", "medium", "low", "ignore"}:
        tier = "ignore"
    importance_score = int(imp.get("importance_score", 0) or 0)
    importance_score = max(0, min(100, importance_score))
    is_important = bool(imp.get("is_important", False))
    keep = bool(imp.get("keep_for_injection", is_important))
    out["importance"] = {
        "is_important": is_important,
        "importance_score": importance_score,
        "tier": tier,
        "keep_for_injection": keep,
        "reason": str(imp.get("reason", "")).strip(),
    }

    summary = out.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    key_points = summary.get("key_points", [])
    if not isinstance(key_points, list):
        key_points = []
    key_points = [str(item).strip() for item in key_points if str(item).strip()][:max_key_points]

    def _normalize_list(key: str) -> List[str]:
        val = summary.get(key, [])
        if not isinstance(val, list):
            return []
        return [str(item).strip() for item in val if str(item).strip()]

    numeric_facts = summary.get("numeric_facts", [])
    if not isinstance(numeric_facts, list):
        numeric_facts = []
    numeric_out: List[Dict[str, str]] = []
    for row in numeric_facts[:20]:
        if not isinstance(row, dict):
            continue
        numeric_out.append(
            {
                "metric": str(row.get("metric", "")).strip(),
                "value": str(row.get("value", "")).strip(),
                "unit": str(row.get("unit", "")).strip(),
                "context": str(row.get("context", "")).strip(),
                "source_snippet": str(row.get("source_snippet", "")).strip(),
            }
        )

    timeline_rows = summary.get("timeline_milestones", [])
    if not isinstance(timeline_rows, list):
        timeline_rows = []
    timeline_out: List[Dict[str, str]] = []
    valid_directions = {"new", "reconfirmed", "delayed", "accelerated", "unclear"}
    for row in timeline_rows[:20]:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction", "unclear")).strip().lower()
        if direction not in valid_directions:
            direction = "unclear"
        timeline_out.append(
            {
                "milestone": str(row.get("milestone", "")).strip(),
                "target_window": str(row.get("target_window", "")).strip(),
                "direction": direction,
                "source_snippet": str(row.get("source_snippet", "")).strip(),
            }
        )

    if keep:
        out["summary"] = {
            "one_line": str(summary.get("one_line", "")).strip(),
            "key_points": key_points,
            "numeric_facts": numeric_out,
            "timeline_milestones": timeline_out,
            "capital_structure": _normalize_list("capital_structure"),
            "catalysts_next_12m": _normalize_list("catalysts_next_12m"),
            "risks_headwinds": _normalize_list("risks_headwinds"),
            "market_impact_assessment": str(summary.get("market_impact_assessment", "")).strip(),
        }
    else:
        out["summary"] = {
            "one_line": "",
            "key_points": [],
            "numeric_facts": [],
            "timeline_milestones": [],
            "capital_structure": [],
            "catalysts_next_12m": [],
            "risks_headwinds": [],
            "market_impact_assessment": "",
        }

    quality = out.get("extraction_quality", {})
    if not isinstance(quality, dict):
        quality = {}
    signal_quality = str(quality.get("signal_quality", "low")).strip().lower()
    if signal_quality not in {"high", "medium", "low"}:
        signal_quality = "low"
    notes = quality.get("notes", [])
    if not isinstance(notes, list):
        notes = []
    out["extraction_quality"] = {
        "text_truncated_for_model": bool(quality.get("text_truncated_for_model", text_truncated)),
        "signal_quality": signal_quality,
        "notes": [str(item).strip() for item in notes if str(item).strip()],
    }

    out["source_meta"] = {
        "file_name": str(doc.get("file_name", "")),
        "file": str(doc.get("file", "")),
        "title": str(doc.get("title", "")),
        "source_url": str(doc.get("source_url", "")),
        "pdf_url": str(doc.get("pdf_url", "")),
        "domain": str(doc.get("domain", "")),
        "published_at": str(doc.get("published_at", "")),
        "decoded_chars_in_file": int(doc.get("decoded_chars", 0) or 0),
    }

    return out


async def _summarize_one(
    *,
    doc: Dict[str, Any],
    model: str,
    max_key_points: int,
    max_doc_chars: int,
    timeout_seconds: float,
) -> Dict[str, Any]:
    text = str(doc.get("full_text", "") or "")
    prompt = _build_worker_prompt(doc, max_key_points=max_key_points, max_doc_chars=max_doc_chars)
    text_truncated = len(text) > max_doc_chars

    errors: List[str] = []
    last_obj: Optional[Dict[str, Any]] = None
    attempts = 3
    for attempt in range(1, attempts + 1):
        response = await query_model(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            timeout=float(timeout_seconds),
            max_tokens=2200,
            reasoning_effort="low",
        )
        if not response or not response.get("content"):
            errors.append(f"attempt_{attempt}:empty_response")
            await asyncio.sleep(min(2.0 * attempt, 6.0))
            continue

        obj = _extract_fenced_json(str(response.get("content", "")))
        if isinstance(obj, dict):
            last_obj = obj
            break

        errors.append(f"attempt_{attempt}:json_parse_failed")
        repair_prompt = (
            "Return STRICT JSON only for the same schema. "
            "No markdown, no prose, no code fences.\n\n"
            f"Previous output:\n{response.get('content', '')}"
        )
        prompt = repair_prompt
        await asyncio.sleep(min(1.0 * attempt, 4.0))

    if last_obj is None:
        fallback = {
            "doc_id": doc.get("file_name", ""),
            "price_sensitive": {
                "is_price_sensitive": False,
                "confidence": 0.0,
                "reason": "worker_failed",
            },
            "importance": {
                "is_important": False,
                "importance_score": 0,
                "tier": "ignore",
                "keep_for_injection": False,
                "reason": "worker_failed",
            },
            "summary": {
                "one_line": "",
                "key_points": [],
                "numeric_facts": [],
                "timeline_milestones": [],
                "capital_structure": [],
                "catalysts_next_12m": [],
                "risks_headwinds": [],
                "market_impact_assessment": "",
            },
            "extraction_quality": {
                "text_truncated_for_model": text_truncated,
                "signal_quality": "low",
                "notes": errors or ["worker_failed"],
            },
        }
        return _normalize_summary_object(
            fallback,
            doc=doc,
            max_key_points=max_key_points,
            text_truncated=text_truncated,
        )

    normalized = _normalize_summary_object(
        last_obj,
        doc=doc,
        max_key_points=max_key_points,
        text_truncated=text_truncated,
    )
    if errors:
        notes = normalized["extraction_quality"].get("notes", [])
        normalized["extraction_quality"]["notes"] = [*notes, *errors]
    return normalized


def _render_markdown_report(
    *,
    model: str,
    dump_dir: Path,
    processed: List[Dict[str, Any]],
    kept: List[Dict[str, Any]],
    dropped: List[Dict[str, Any]],
) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    lines: List[str] = [
        "# Announcement Summaries",
        "",
        f"- generated_at_utc: {generated_at}",
        f"- worker_model: {model}",
        f"- dump_dir: {dump_dir}",
        f"- total_processed: {len(processed)}",
        f"- kept_for_injection: {len(kept)}",
        f"- dropped_as_unimportant: {len(dropped)}",
        "",
        "## Kept Documents (JSON Elements)",
        "",
    ]

    for item in kept:
        lines.append(f"### {item.get('source_meta', {}).get('file_name', item.get('doc_id', 'document'))}")
        lines.append("```json")
        lines.append(json.dumps(item, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")

    lines.extend(["## Dropped Documents (Classification Only)", ""])
    for item in dropped:
        compact = {
            "doc_id": item.get("doc_id", ""),
            "price_sensitive": item.get("price_sensitive", {}),
            "importance": item.get("importance", {}),
            "source_meta": item.get("source_meta", {}),
            "extraction_quality": item.get("extraction_quality", {}),
        }
        lines.append(f"### {compact.get('source_meta', {}).get('file_name', compact.get('doc_id', 'document'))}")
        lines.append("```json")
        lines.append(json.dumps(compact, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


async def _run(args: argparse.Namespace) -> int:
    load_dotenv()

    dump_dir = Path(args.dump_dir).resolve()
    if not dump_dir.exists() or not dump_dir.is_dir():
        raise FileNotFoundError(f"Dump directory not found: {dump_dir}")

    md_files = sorted(
        [
            path
            for path in dump_dir.glob("*.md")
            if path.is_file() and path.name != "index.md"
        ]
    )
    if not md_files:
        print(f"No markdown dump files found in: {dump_dir}")
        return 1

    if int(args.max_docs) > 0:
        md_files = md_files[: int(args.max_docs)]

    docs = [_read_dump_markdown(path) for path in md_files]

    sem = asyncio.Semaphore(max(1, int(args.concurrency)))
    results: List[Dict[str, Any]] = []

    async def _worker(doc: Dict[str, Any]) -> Dict[str, Any]:
        async with sem:
            return await _summarize_one(
                doc=doc,
                model=str(args.model),
                max_key_points=max(1, int(args.max_key_points)),
                max_doc_chars=max(5000, int(args.max_doc_chars)),
                timeout_seconds=float(args.timeout_seconds),
            )

    tasks = [_worker(doc) for doc in docs]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
        meta = result.get("source_meta", {}) or {}
        imp = result.get("importance", {}) or {}
        print(
            f"processed: {meta.get('file_name', result.get('doc_id', 'doc'))} "
            f"keep={imp.get('keep_for_injection', False)} "
            f"score={imp.get('importance_score', 0)} "
            f"tier={imp.get('tier', 'ignore')}"
        )

    # Preserve input order in output.
    order_map = {doc.get("file_name", ""): idx for idx, doc in enumerate(docs)}
    results.sort(key=lambda item: order_map.get((item.get("source_meta", {}) or {}).get("file_name", ""), 10**9))

    kept = [item for item in results if bool((item.get("importance", {}) or {}).get("keep_for_injection", False))]
    dropped = [item for item in results if item not in kept]

    out_md = Path(args.output_markdown).resolve() if args.output_markdown else (dump_dir / "announcement_summaries.md")
    out_json = Path(args.output_json).resolve() if args.output_json else (dump_dir / "announcement_summaries.json")

    report = _render_markdown_report(
        model=str(args.model),
        dump_dir=dump_dir,
        processed=results,
        kept=kept,
        dropped=dropped,
    )
    out_md.write_text(report, encoding="utf-8")

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "worker_model": str(args.model),
        "dump_dir": str(dump_dir),
        "total_processed": len(results),
        "kept_for_injection": len(kept),
        "dropped_as_unimportant": len(dropped),
        "results": results,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Output markdown: {out_md}")
    print(f"Output json: {out_json}")
    print(f"Processed={len(results)} kept={len(kept)} dropped={len(dropped)}")
    return 0


def main() -> None:
    args = _parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()

