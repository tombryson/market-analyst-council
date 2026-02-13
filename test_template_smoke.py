"""End-to-end template smoke test (Stage 1 -> Stage 2 -> Stage 3).

This script validates:
- template routing
- company-type routing
- exchange routing
- company-name substitution
- multi-model Stage 1 research execution
"""

import argparse
import asyncio
import json
import os
import shutil
import sys
from datetime import datetime
from time import perf_counter
from typing import Any, Dict


def _progress(message: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}][test_template_smoke] {message}", flush=True)


def _ensure_pymupdf_runtime() -> None:
    """Re-exec with uv-managed environment if current Python cannot import PyMuPDF."""
    if os.environ.get("LLM_COUNCIL_NO_REEXEC") == "1":
        return

    try:
        import pymupdf  # noqa: F401
        return
    except Exception as e:
        uv = shutil.which("uv")
        if uv:
            print(
                "PyMuPDF unavailable in current interpreter; re-running with uv environment. "
                f"Reason: {type(e).__name__}: {e}"
            )
            os.environ["LLM_COUNCIL_NO_REEXEC"] = "1"
            os.execvpe(uv, [uv, "run", "python", *sys.argv], os.environ)
        print(
            "PyMuPDF unavailable and uv was not found. "
            "Run with: uv run python test_template_smoke.py ..."
        )
        sys.exit(1)


def _print_header(args: argparse.Namespace, selection: Dict[str, Any]) -> None:
    print("=" * 100)
    print("TEMPLATE SMOKE TEST")
    print("=" * 100)
    print(f"Query: {args.query}")
    print(f"Ticker: {args.ticker or '(none)'}")
    print(f"Requested Template: {args.template_id or '(auto)'}")
    print(f"Requested Company Type: {args.company_type or '(auto)'}")
    print(f"Requested Exchange: {args.exchange or '(auto)'}")
    print(
        "Resolved Selection: "
        f"template={selection.get('template_id')} "
        f"company={selection.get('company_name')} "
        f"company_type={selection.get('company_type')} "
        f"exchange={selection.get('exchange')} "
        f"source={selection.get('selection_source')} "
        f"exchange_source={selection.get('exchange_selection_source')}"
    )
    if args.council_models:
        print(f"Stage 1 models override: {args.council_models}")
    if args.stage2_models:
        print(f"Stage 2 models override: {args.stage2_models}")
    if args.chairman_model:
        print(f"Chairman model override: {args.chairman_model}")
    print(f"Depth: {args.depth}")
    print(f"Max sources: {args.max_sources}")
    print(f"Decode max per model: {args.decode_max_per_model}")
    print("-" * 100)


def _print_stage1(metadata: Dict[str, Any]) -> None:
    print("\nSTAGE 1")
    print("-" * 100)
    attempted = metadata.get("models_attempted", [])
    succeeded = metadata.get("models_succeeded", [])
    print(f"Models attempted: {len(attempted)}")
    print(f"Models succeeded: {len(succeeded)}")
    for run in metadata.get("per_model_research_runs", []):
        model = run.get("model", "unknown")
        result = run.get("result", {})
        provider_meta = result.get("provider_metadata", {}) or {}
        decode_meta = provider_meta.get("source_decoding", {}) or {}
        status = "OK" if not result.get("error") else "FAILED"
        print(f"[{status}] {model}")
        print(f"  result_count: {result.get('result_count', 0)}")
        if provider_meta.get("stage1_attempts") is not None:
            print(f"  stage1_attempts: {provider_meta.get('stage1_attempts')}")
        if decode_meta:
            print(
                "  decoded_sources: "
                f"{decode_meta.get('decoded', 0)}/{decode_meta.get('attempted', 0)}"
            )
        if provider_meta.get("template_compliance_required") is not None:
            print(
                "  template_compliant: "
                f"{provider_meta.get('template_compliant')} "
                f"(reason={provider_meta.get('template_compliance_reason', 'n/a')})"
            )
        if provider_meta.get("stage1_template_retry_triggered") is not None:
            print(
                "  template_retry_triggered: "
                f"{provider_meta.get('stage1_template_retry_triggered')}"
            )
        if result.get("error"):
            print(f"  error: {result.get('error')}")


def _print_stage2(stage2_results: list[Dict[str, Any]], aggregate_rankings: list[Dict[str, Any]]) -> None:
    print("\nSTAGE 2")
    print("-" * 100)
    print(f"Rankings received: {len(stage2_results)}")
    if aggregate_rankings:
        print("Aggregate ranking:")
        for i, item in enumerate(aggregate_rankings, 1):
            print(f"  {i}. {item['model']} (avg rank: {item['average_rank']:.2f})")


def _print_stage3(stage3_result: Dict[str, Any]) -> None:
    print("\nSTAGE 3")
    print("-" * 100)
    print(f"Chairman model: {stage3_result.get('model', 'unknown')}")
    if stage3_result.get("parse_error"):
        print(f"Parse error: {stage3_result['parse_error']}")

    structured = stage3_result.get("structured_data") or {}
    if not structured:
        print("No structured_data returned.")
        text = (stage3_result.get("response") or "").strip()
        if len(text) > 700:
            text = text[:697] + "..."
        print(text or "(empty)")
        return

    print(f"Analysis type: {structured.get('analysis_type', 'unknown')}")
    print(
        f"Company: {structured.get('company_name') or structured.get('company') or 'unknown'} | "
        f"Ticker: {structured.get('ticker', 'unknown')}"
    )
    if structured.get("quality_score"):
        q = structured["quality_score"]
        print(f"Quality score total: {q.get('total', 'n/a')}")
    if structured.get("value_score"):
        v = structured["value_score"]
        print(f"Value score total: {v.get('total', 'n/a')}")
    rec = (structured.get("investment_recommendation") or {}).get("rating")
    if rec:
        print(f"Recommendation: {rec}")
    council_meta = structured.get("council_metadata") or {}
    if council_meta.get("resolved_company_name"):
        print(f"Resolved company name: {council_meta.get('resolved_company_name')}")


async def _run(args: argparse.Namespace) -> None:
    # Runtime overrides for this test run.
    os.environ["PERPLEXITY_MAX_STEPS"] = str(args.max_steps)
    os.environ["MAX_SOURCES"] = str(args.max_sources)
    os.environ["PERPLEXITY_MAX_RESULTS_PER_QUERY"] = str(args.max_sources)
    os.environ["SOURCE_DECODING_MAX_PER_MODEL"] = str(args.decode_max_per_model)
    os.environ["PERPLEXITY_REASONING_EFFORT"] = args.reasoning_effort
    if args.council_models:
        os.environ["PERPLEXITY_COUNCIL_MODELS"] = args.council_models
    if args.stage2_models:
        os.environ["COUNCIL_MODELS"] = args.stage2_models
    if args.chairman_model:
        os.environ["CHAIRMAN_MODEL"] = args.chairman_model

    # Import after env overrides so backend config picks up runtime settings.
    from backend.council import (
        stage1_collect_perplexity_research_responses,
        stage2_collect_rankings,
        stage3_synthesize_final,
        calculate_aggregate_rankings,
    )
    from backend.config import CHAIRMAN_MODEL
    from backend.main import build_enhanced_context, build_template_context_for_prompt
    from backend.template_loader import get_template_loader, resolve_template_selection

    selection = resolve_template_selection(
        user_query=args.query,
        ticker=args.ticker,
        explicit_template_id=args.template_id,
        company_type=args.company_type,
        exchange=args.exchange,
    )

    selected_template_id = selection["template_id"]
    selected_company_name = selection.get("company_name")
    selected_company_type = selection.get("company_type")
    selected_exchange = selection.get("exchange")

    loader = get_template_loader()
    template_data = loader.get_template(selected_template_id) or {}
    use_structured_analysis = loader.is_structured_template(selected_template_id)
    stage1_research_brief = loader.get_stage1_research_brief(
        selected_template_id,
        selected_company_type,
        selected_exchange,
        selected_company_name,
    )
    template_context = build_template_context_for_prompt(
        selected_template_id,
        template_data,
        selected_company_name,
        selected_company_type,
        selected_exchange,
        selection.get("exchange_assumptions", ""),
    )

    total_start = perf_counter()
    _print_header(args, selection)

    _progress("Stage 1 start")
    stage1_start = perf_counter()
    stage1_results, metadata = await stage1_collect_perplexity_research_responses(
        user_query=args.query,
        ticker=args.ticker,
        attachment_context="",
        depth=args.depth,
        research_brief=stage1_research_brief,
    )
    _progress(f"Stage 1 done in {perf_counter() - stage1_start:.1f}s")
    _print_stage1(metadata)

    if not stage1_results:
        print("\nNo Stage 1 responses generated. Stopping.")
        return

    search_results = metadata.get("aggregated_search_results", {})
    enhanced_context = build_enhanced_context(
        args.query,
        search_results,
        [],
        template_context=template_context,
    )
    ranking_models = [item.get("model") for item in stage1_results if item.get("model")]
    chairman_model = (
        CHAIRMAN_MODEL
        if CHAIRMAN_MODEL in ranking_models
        else (ranking_models[0] if ranking_models else CHAIRMAN_MODEL)
    )

    _progress("Stage 2 start")
    stage2_start = perf_counter()
    stage2_results, label_to_model = await stage2_collect_rankings(
        enhanced_context,
        stage1_results,
        ranking_models=ranking_models,
    )
    aggregate_rankings = calculate_aggregate_rankings(stage2_results, label_to_model)
    _progress(f"Stage 2 done in {perf_counter() - stage2_start:.1f}s")
    _print_stage2(stage2_results, aggregate_rankings)

    _progress("Stage 3 start")
    stage3_start = perf_counter()
    stage3_result = await stage3_synthesize_final(
        enhanced_context,
        stage1_results,
        stage2_results,
        label_to_model=label_to_model,
        use_structured_analysis=use_structured_analysis,
        template_id=selected_template_id,
        ticker=args.ticker,
        company_name=selected_company_name,
        exchange=selected_exchange,
        chairman_model=chairman_model,
    )
    _progress(f"Stage 3 done in {perf_counter() - stage3_start:.1f}s")
    _print_stage3(stage3_result)

    if args.dump_json:
        payload = {
            "debug": {
                "query": args.query,
                "ticker": args.ticker,
                "template_id": selected_template_id,
                "company_type": selected_company_type,
                "exchange": selected_exchange,
                "stage1_research_brief_chars": len(stage1_research_brief or ""),
                "template_context_chars": len(template_context or ""),
                "stage1_research_brief": stage1_research_brief,
                "template_context": template_context,
            },
            "selection": selection,
            "stage1_results": stage1_results,
            "stage2_results": stage2_results,
            "stage3_result": stage3_result,
            "metadata": metadata,
        }
        with open(args.dump_json, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"\nSaved JSON output to: {args.dump_json}")

    _progress(f"Run complete in {perf_counter() - total_start:.1f}s")
    print("\nTemplate smoke test complete.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run end-to-end template smoke test.",
    )
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="Prompt to evaluate.",
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Optional ticker (e.g. ASX:WWI, NYSE:NEM).",
    )
    parser.add_argument(
        "--template-id",
        type=str,
        default=None,
        help="Optional template ID override.",
    )
    parser.add_argument(
        "--company-type",
        type=str,
        default=None,
        help="Optional company type override.",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        help="Optional exchange override (asx, nyse, nasdaq, tsx, tsxv, lse, aim).",
    )
    parser.add_argument(
        "--depth",
        type=str,
        default="deep",
        choices=["basic", "deep"],
        help="Research depth.",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=4,
        help="PERPLEXITY_MAX_STEPS override for this run.",
    )
    parser.add_argument(
        "--max-sources",
        type=int,
        default=10,
        help="MAX_SOURCES and PERPLEXITY_MAX_RESULTS_PER_QUERY override for this run.",
    )
    parser.add_argument(
        "--decode-max-per-model",
        type=int,
        default=10,
        help="SOURCE_DECODING_MAX_PER_MODEL override for this run.",
    )
    parser.add_argument(
        "--reasoning-effort",
        type=str,
        default="low",
        choices=["low", "medium", "high"],
        help="PERPLEXITY_REASONING_EFFORT override for this run.",
    )
    parser.add_argument(
        "--council-models",
        type=str,
        default=None,
        help="PERPLEXITY_COUNCIL_MODELS override (comma-separated).",
    )
    parser.add_argument(
        "--stage2-models",
        type=str,
        default=None,
        help="COUNCIL_MODELS override for Stage 2 peer ranking (comma-separated).",
    )
    parser.add_argument(
        "--chairman-model",
        type=str,
        default=None,
        help="CHAIRMAN_MODEL override for Stage 3.",
    )
    parser.add_argument(
        "--dump-json",
        type=str,
        default=None,
        help="Optional path to write full JSON output.",
    )
    return parser


if __name__ == "__main__":
    _ensure_pymupdf_runtime()
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))
