"""Smoke test for emulated Perplexity deep-research council Stage 1.

Usage:
  uv run python test_emulated_council.py --query "Analyze ASX:WWI"
  uv run python test_emulated_council.py --query "Analyze BHP valuation" --ticker BHP
"""

import argparse
import asyncio
import json
import os
import shutil
import sys
from typing import Dict, Any, List


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
            "Run with: uv run python test_emulated_council.py ..."
        )
        sys.exit(1)


def _print_header(
    args: argparse.Namespace,
    runtime_cfg: Dict[str, Any],
    selection: Dict[str, Any],
):
    print("=" * 90)
    print("EMULATED PERPLEXITY COUNCIL SMOKE TEST")
    print("=" * 90)
    print(f"Query mode: {args.query_mode}")
    print(f"Query: {args.query or '(template rubric mode)'}")
    print(f"Ticker: {args.ticker or '(none)'}")
    print(f"Requested Template Override: {args.template_id or '(auto)'}")
    print(f"Requested Company Type: {args.company_type or '(auto)'}")
    print(f"Requested Exchange: {args.exchange or '(auto)'}")
    print(
        "Resolved Selection: "
        f"template={selection.get('template_id')} "
        f"company={selection.get('company_name')} "
        f"company_type={selection.get('company_type')} "
        f"exchange={selection.get('exchange')} "
        f"source={selection.get('selection_source')}"
    )
    print(f"Configured Council Execution Mode: {runtime_cfg['council_execution_mode']}")
    print(f"Configured Research Provider: {runtime_cfg['research_provider']}")
    print(f"Research Service Enabled: {runtime_cfg['enable_research_service']}")
    print(f"Research Depth: {runtime_cfg['research_depth']}")
    print(f"Max Sources: {runtime_cfg['max_sources']}")
    print(f"Perplexity Stage1 Execution Mode: {runtime_cfg['stage1_execution_mode']}")
    print(f"Perplexity Stage1 Stagger Seconds: {runtime_cfg['stage1_stagger_seconds']}")
    print("Perplexity Council Models:")
    for model in runtime_cfg["council_models"]:
        print(f"  - {model}")
    print("-" * 90)


def _status_for_run(run: Dict[str, Any]) -> str:
    if run.get("error"):
        return "FAILED"
    if run.get("result_count", 0) > 0:
        return "OK"
    return "EMPTY"


def _print_run_summary(per_model_runs: List[Dict[str, Any]]):
    print("\nPER-MODEL RUN DETAILS")
    print("-" * 90)
    for item in per_model_runs:
        model = item.get("model", "unknown")
        run = item.get("result") or {}
        status = _status_for_run(run)
        provider_meta = run.get("provider_metadata") or {}

        print(f"[{status}] {model}")
        print(f"  result_count: {run.get('result_count', 0)}")
        print(f"  provider: {run.get('provider', 'unknown')}")
        print(f"  provider_model: {provider_meta.get('model', 'n/a')}")
        print(f"  provider_preset: {provider_meta.get('preset', 'n/a')}")
        decode_meta = provider_meta.get("source_decoding") or {}
        stage1_attempts = provider_meta.get("stage1_attempts")
        if stage1_attempts is not None:
            print(f"  stage1_attempts: {stage1_attempts}")
        if decode_meta:
            print(
                "  decoded_sources: "
                f"{decode_meta.get('decoded', 0)}/{decode_meta.get('attempted', 0)}"
            )
        summary = (run.get("research_summary") or "").strip()
        if summary:
            compact = " ".join(summary.split())
            if len(compact) > 280:
                compact = compact[:277] + "..."
            print(f"  summary_preview: {compact}")
        if run.get("error"):
            print(f"  error: {run['error']}")
        prompt = run.get("research_prompt", "")
        if prompt:
            print(f"  research_prompt_chars: {len(prompt)}")
            print("  --- research_prompt_start ---")
            print(prompt)
            print("  --- research_prompt_end ---")

        updates = run.get("latest_updates", [])
        if updates:
            print("  latest_updates:")
            for update in updates[:3]:
                print(
                    "    - "
                    f"{update.get('date', 'Unknown')} | {update.get('update', 'Update')} "
                    f"| {update.get('source_url', '')}"
                )

        results = run.get("results", [])
        if results:
            top = results[0]
            print(f"  top_source: {top.get('title', 'Untitled')}")
            print(f"  top_url: {top.get('url', '')}")
        print("")


def _print_aggregate_summary(stage1_results: List[Dict[str, Any]], metadata: Dict[str, Any]):
    attempted = metadata.get("models_attempted", [])
    succeeded = metadata.get("models_succeeded", [])
    failed = [m for m in attempted if m not in succeeded]

    agg = metadata.get("aggregated_search_results", {})
    evidence_pack = agg.get("evidence_pack", {})
    source_count = len((evidence_pack.get("sources") or []))

    print("AGGREGATE SUMMARY")
    print("-" * 90)
    print(f"Models attempted: {len(attempted)}")
    print(f"Models succeeded: {len(succeeded)}")
    print(f"Models failed: {len(failed)}")
    if failed:
        print(f"Failed models: {', '.join(failed)}")

    print(f"Stage1 responses generated: {len(stage1_results)}")
    print(f"Aggregated source count: {agg.get('result_count', 0)}")
    print(f"Evidence pack source count: {source_count}")

    key_facts = evidence_pack.get("key_facts", [])[:3]
    if key_facts:
        print("Sample key facts:")
        for fact in key_facts:
            print(f"  - {fact}")

    missing = evidence_pack.get("missing_data", [])
    if missing:
        print("Missing-data signals:")
        for item in missing:
            print(f"  - {item}")


def _print_input_audit(
    selection: Dict[str, Any],
    effective_query: str,
    stage1_query_sent: str,
    stage1_research_brief: str,
    market_facts: Dict[str, Any] | None,
    market_facts_query_prefix: str,
):
    print("\nINPUT AUDIT")
    print("-" * 90)
    print("Selection:")
    print(json.dumps(selection, indent=2))
    print("\nEffective Query (template/user query before market-facts prefix):")
    print(effective_query)
    print("\nStage 1 Query Sent (with minimal market-facts prefix):")
    print(stage1_query_sent)
    print("\nStage 1 Research Brief (sent as research_brief to Stage 1):")
    print(stage1_research_brief)
    print("\nMarket Facts Query Prefix (minimal block):")
    print(market_facts_query_prefix or "(none)")
    print("\nMarket Facts Object:")
    print(json.dumps(market_facts or {}, indent=2))


async def _run(args: argparse.Namespace):
    from backend.council import stage1_collect_perplexity_research_responses
    from backend.config import (
        PERPLEXITY_COUNCIL_MODELS,
        RESEARCH_DEPTH,
        MAX_SOURCES,
        COUNCIL_EXECUTION_MODE,
        RESEARCH_PROVIDER,
        ENABLE_RESEARCH_SERVICE,
        PERPLEXITY_STAGE1_EXECUTION_MODE,
        PERPLEXITY_STAGE1_STAGGER_SECONDS,
        ENABLE_MARKET_FACTS_PREPASS,
    )
    from backend.market_facts import (
        gather_market_facts_prepass,
        format_market_facts_query_prefix,
        prepend_market_facts_to_query,
    )
    from backend.template_loader import get_template_loader, resolve_template_selection

    runtime_cfg = {
        "council_models": PERPLEXITY_COUNCIL_MODELS,
        "research_depth": RESEARCH_DEPTH,
        "max_sources": MAX_SOURCES,
        "council_execution_mode": COUNCIL_EXECUTION_MODE,
        "research_provider": RESEARCH_PROVIDER,
        "enable_research_service": ENABLE_RESEARCH_SERVICE,
        "stage1_execution_mode": PERPLEXITY_STAGE1_EXECUTION_MODE,
        "stage1_stagger_seconds": PERPLEXITY_STAGE1_STAGGER_SECONDS,
    }

    selection = resolve_template_selection(
        user_query=args.query or "",
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
    if args.query_mode == "template_only":
        effective_query = loader.render_template_rubric(
            selected_template_id,
            company_name=selected_company_name,
            exchange=selected_exchange,
        )
        if not effective_query:
            raise ValueError(
                f"Template '{selected_template_id}' has no rubric to use as query."
            )
    else:
        effective_query = (args.query or "").strip()
        if not effective_query:
            raise ValueError("--query is required when --query-mode=user.")

    stage1_research_brief = loader.get_stage1_research_brief(
        selected_template_id,
        selected_company_type,
        selected_exchange,
        selected_company_name,
        include_rubric=(args.query_mode != "template_only"),
    )

    market_facts = None
    if ENABLE_MARKET_FACTS_PREPASS and args.ticker:
        market_facts = await gather_market_facts_prepass(
            ticker=args.ticker,
            company_name=selected_company_name,
            exchange=selected_exchange,
        )

    stage1_effective_research_brief = stage1_research_brief
    market_facts_query_prefix = format_market_facts_query_prefix(market_facts)
    stage1_effective_query = prepend_market_facts_to_query(effective_query, market_facts)

    _print_header(args, runtime_cfg, selection)
    if market_facts:
        print(f"Market facts prepass: {market_facts.get('status')}")
    _print_input_audit(
        selection=selection,
        effective_query=effective_query,
        stage1_query_sent=stage1_effective_query,
        stage1_research_brief=stage1_effective_research_brief,
        market_facts=market_facts,
        market_facts_query_prefix=market_facts_query_prefix,
    )

    stage1_results, metadata = await stage1_collect_perplexity_research_responses(
        user_query=stage1_effective_query,
        ticker=args.ticker,
        attachment_context="",
        research_brief=stage1_effective_research_brief,
    )

    per_model_runs = metadata.get("per_model_research_runs", [])
    _print_run_summary(per_model_runs)
    _print_aggregate_summary(stage1_results, metadata)

    if args.dump_json:
        payload = {
            "effective_query": effective_query,
            "stage1_query_sent": stage1_effective_query,
            "input_audit": {
                "selection": selection,
                "stage1_research_brief": stage1_effective_research_brief,
                "market_facts_query_prefix": market_facts_query_prefix,
                "market_facts": market_facts or {},
            },
            "stage1_results": stage1_results,
            "metadata": metadata,
            "selection": selection,
        }
        with open(args.dump_json, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"\nSaved JSON output to: {args.dump_json}")

    print("\nSmoke test complete.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run emulated Perplexity council Stage 1 smoke test.",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Research query to run through emulated council.",
    )
    parser.add_argument(
        "--query-mode",
        type=str,
        choices=["user", "template_only"],
        default="template_only",
        help="Use a user query, or use the rendered template rubric as the full task query.",
    )
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Optional ticker (e.g. WWI, BHP).",
    )
    parser.add_argument(
        "--template-id",
        type=str,
        default=None,
        help="Optional explicit template ID override.",
    )
    parser.add_argument(
        "--company-type",
        type=str,
        default=None,
        help="Optional company type (e.g., gold_miner, pharma_biotech, software_saas).",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        help="Optional exchange (e.g., asx, nyse, nasdaq, tsx, tsxv, lse, aim).",
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
