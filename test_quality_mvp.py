"""End-to-end MVP test for out-of-100 financial quality scoring.

Runs:
1) Perplexity-emulated Stage 1 deep research (with decoding if enabled)
2) Stage 2 peer ranking
3) Stage 3 structured synthesis using financial_quality_mvp template
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
    print(f"[{ts}][test_quality_mvp] {message}", flush=True)


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
            "Run with: uv run python test_quality_mvp.py ..."
        )
        sys.exit(1)

def _print_header(args: argparse.Namespace, selection: Dict[str, Any]) -> None:
    print("=" * 90)
    print("FINANCIAL QUALITY MVP TEST")
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
    if args.max_steps is not None:
        print(f"Max steps override: {args.max_steps}")
    if args.council_models:
        print(f"Council models override: {args.council_models}")
    if args.reasoning_effort:
        print(f"Reasoning effort override: {args.reasoning_effort}")
    if args.max_sources is not None:
        print(f"Max sources override: {args.max_sources}")
    if args.decode_max_per_model is not None:
        print(f"Decode max per model override: {args.decode_max_per_model}")
    print("-" * 90)


def _print_stage1(metadata: Dict[str, Any]) -> None:
    print("\nSTAGE 1")
    print("-" * 90)
    attempted = metadata.get("models_attempted", [])
    succeeded = metadata.get("models_succeeded", [])
    print(f"Models attempted: {len(attempted)}")
    print(f"Models succeeded: {len(succeeded)}")
    if metadata.get("stage1_second_pass_enabled") is not None:
        print(
            "Second-pass settings: "
            f"enabled={metadata.get('stage1_second_pass_enabled')} "
            f"max_sources={metadata.get('stage1_second_pass_max_sources')} "
            f"max_chars_per_source={metadata.get('stage1_second_pass_max_chars_per_source')}"
        )
    if metadata.get("stage1_openai_guardrails_enabled") is not None:
        print(
            "OpenAI pass-1 guardrails: "
            f"enabled={metadata.get('stage1_openai_guardrails_enabled')} "
            f"max_sources={metadata.get('stage1_openai_base_max_sources')} "
            f"max_steps={metadata.get('stage1_openai_base_max_steps')} "
            f"reasoning={metadata.get('stage1_openai_base_reasoning_effort')}"
        )
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
        if provider_meta.get("stage1_second_pass_enabled") is not None:
            print(
                "  second_pass: "
                f"enabled={provider_meta.get('stage1_second_pass_enabled')} "
                f"success={provider_meta.get('stage1_second_pass_success')} "
                f"attempts={provider_meta.get('stage1_second_pass_attempts')}"
            )
            if provider_meta.get("stage1_second_pass_prompt_chars") is not None:
                print(
                    "  second_pass_prompt_chars: "
                    f"{provider_meta.get('stage1_second_pass_prompt_chars')}"
                )
            if provider_meta.get("stage1_second_pass_response_chars") is not None:
                print(
                    "  second_pass_response_chars: "
                    f"{provider_meta.get('stage1_second_pass_response_chars')}"
                )
            if provider_meta.get("stage1_second_pass_fact_pack_chars") is not None:
                print(
                    "  second_pass_fact_pack: "
                    f"chars={provider_meta.get('stage1_second_pass_fact_pack_chars')} "
                    f"facts={provider_meta.get('stage1_second_pass_fact_pack_total_facts')} "
                    f"sections={provider_meta.get('stage1_second_pass_fact_pack_sections_with_facts')}"
                )
            if provider_meta.get("stage1_final_template_compliant") is not None:
                print(
                    "  second_pass_template_compliant: "
                    f"{provider_meta.get('stage1_final_template_compliant')} "
                    f"reason={provider_meta.get('stage1_final_template_reason')}"
                )
            if provider_meta.get("stage1_second_pass_error"):
                print(f"  second_pass_error: {provider_meta.get('stage1_second_pass_error')}")
        prompt = result.get("research_prompt", "")
        if prompt:
            print(f"  research_prompt_chars: {len(prompt)}")
            print("  --- research_prompt_start ---")
            print(prompt)
            print("  --- research_prompt_end ---")
        second_pass_prompt = result.get("stage1_second_pass_prompt", "")
        if second_pass_prompt:
            print(f"  second_pass_prompt_chars(full): {len(second_pass_prompt)}")
            print("  --- second_pass_prompt_start ---")
            print(second_pass_prompt)
            print("  --- second_pass_prompt_end ---")
        if result.get("error"):
            print(f"  error: {result.get('error')}")


def _print_stage2(stage2_results: list[Dict[str, Any]], aggregate_rankings: list[Dict[str, Any]]) -> None:
    print("\nSTAGE 2")
    print("-" * 90)
    print(f"Rankings received: {len(stage2_results)}")
    if aggregate_rankings:
        print("Aggregate ranking:")
        for i, item in enumerate(aggregate_rankings, 1):
            print(f"  {i}. {item['model']} (avg rank: {item['average_rank']:.2f})")


def _print_stage3(stage3_result: Dict[str, Any]) -> None:
    print("\nSTAGE 3")
    print("-" * 90)
    print(f"Chairman model: {stage3_result.get('model', 'unknown')}")
    if stage3_result.get("parse_error"):
        print(f"Parse error: {stage3_result['parse_error']}")

    structured = stage3_result.get("structured_data") or {}
    if not structured:
        print("No structured_data returned. Raw response preview:")
        text = (stage3_result.get("response") or "").strip()
        if len(text) > 600:
            text = text[:597] + "..."
        print(text or "(empty)")
        return

    company_name = (
        structured.get("company_name")
        or structured.get("company")
        or "Unknown"
    )
    print(f"Company: {company_name}")
    print(f"Ticker: {structured.get('ticker', 'Unknown')}")

    required_fields = [
        "analysis_type",
        "ticker",
        "company_name",
        "quality_score",
        "value_score",
        "price_targets",
        "development_timeline",
        "investment_recommendation",
    ]
    present_required = [
        field
        for field in required_fields
        if structured.get(field) not in (None, "", [], {})
    ]
    missing_required = [field for field in required_fields if field not in present_required]
    print(
        "Template Coverage: "
        f"{len(present_required)}/{len(required_fields)} required fields populated"
    )
    if missing_required:
        print(f"Missing required fields: {', '.join(missing_required)}")

    recommendation = structured.get("investment_recommendation", {}) or {}
    if recommendation:
        print(
            "Investment Recommendation: "
            f"rating={recommendation.get('rating', 'n/a')} "
            f"conviction={recommendation.get('conviction', 'n/a')} "
            f"summary={recommendation.get('summary', '')[:180]}"
        )

    quality = structured.get("quality_score", {}) or {}
    value = structured.get("value_score", {}) or {}
    if quality.get("total") is not None or value.get("total") is not None:
        print(
            "Scoring: "
            f"quality_total={quality.get('total', 'n/a')} "
            f"value_total={value.get('total', 'n/a')}"
        )

    # Keep compatibility with financial_quality_mvp fields.
    if structured.get("confidence_pct") is not None:
        print(f"Confidence (%): {structured.get('confidence_pct')}")
    if structured.get("recommendation") is not None:
        print(f"Recommendation: {structured.get('recommendation')}")

    price_targets = structured.get("price_targets", {}) or {}
    if price_targets:
        print(
            "Price Targets: "
            f"12m={price_targets.get('target_12m', 'n/a')} "
            f"24m={price_targets.get('target_24m', 'n/a')} "
            f"base={((price_targets.get('scenarios') or {}).get('base', 'n/a'))} "
            f"bull={((price_targets.get('scenarios') or {}).get('bull', 'n/a'))} "
            f"bear={((price_targets.get('scenarios') or {}).get('bear', 'n/a'))}"
        )

    timeline = structured.get("development_timeline", []) or []
    if timeline:
        print("Development Timeline (top 3):")
        for item in timeline[:3]:
            print(
                "  - "
                f"{item.get('milestone', 'milestone')} | "
                f"{item.get('target_period', 'period')} | "
                f"status={item.get('status', 'n/a')} | "
                f"confidence={item.get('confidence_pct', 'n/a')}"
            )

    ht = structured.get("headwinds_tailwinds", {}) or {}
    q_hw = (ht.get("quantitative") or [])[:3]
    ql_hw = (ht.get("qualitative") or [])[:3]
    if q_hw:
        print("Quantitative Headwinds/Tailwinds:")
        for item in q_hw:
            print(f"  - {item}")
    if ql_hw:
        print("Qualitative Headwinds/Tailwinds:")
        for item in ql_hw:
            print(f"  - {item}")

    verdict = structured.get("investment_verdict", {}) or {}
    if verdict:
        print("Investment Verdict:")
        reasons = verdict.get("top_reasons") or []
        failures = verdict.get("failure_conditions") or []
        if reasons:
            print("  Top reasons:")
            for item in reasons[:3]:
                print(f"    - {item}")
        if failures:
            print("  Failure conditions:")
            for item in failures[:3]:
                print(f"    - {item}")

    extended = structured.get("extended_analysis", {}) or {}
    if extended:
        catalysts = (extended.get("next_major_catalysts") or [])[:3]
        sensitivities = (extended.get("sensitivity_analysis") or [])[:3]
        market_context = (extended.get("market_context") or [])[:3]
        if catalysts:
            print("Next Catalysts:")
            for item in catalysts:
                print(f"  - {item}")
        if sensitivities:
            print("Sensitivity Analysis (top 3):")
            for item in sensitivities:
                print(f"  - {item}")
        if market_context:
            print("Market Context:")
            for item in market_context:
                print(f"  - {item}")

    rationale = structured.get("rationale", "")
    if rationale:
        print("Rationale:")
        print(f"  {rationale}")

    evidence = structured.get("evidence_used", []) or []
    if evidence:
        print("Top evidence:")
        for item in evidence[:5]:
            print(f"  - {item.get('source', '')}")

    missing = structured.get("missing_information", []) or []
    if missing:
        print("Missing information:")
        for item in missing[:5]:
            print(f"  - {item}")


def _print_input_audit(
    selection: Dict[str, Any],
    effective_query: str,
    stage1_query_sent: str,
    stage1_research_brief: str,
    market_facts: Dict[str, Any] | None,
    market_facts_query_prefix: str,
) -> None:
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


def _print_provider_request_audit(
    *,
    endpoint: str,
    models: list[str],
    prompt: str,
    payloads: Dict[str, Dict[str, Any]],
) -> None:
    print("\nPROVIDER REQUEST AUDIT")
    print("-" * 90)
    print(f"Endpoint: {endpoint}")
    print(f"Models: {models}")
    print(f"Prompt chars: {len(prompt)}")
    print("\nFull Prompt Sent To Provider:")
    print(prompt)
    for model in models:
        print(f"\nPayload for model={model}:")
        print(json.dumps(payloads.get(model, {}), indent=2))


async def _run(args: argparse.Namespace) -> None:
    if args.max_steps is not None:
        os.environ["PERPLEXITY_MAX_STEPS"] = str(args.max_steps)
    if args.council_models:
        os.environ["PERPLEXITY_COUNCIL_MODELS"] = args.council_models
    if args.reasoning_effort:
        os.environ["PERPLEXITY_REASONING_EFFORT"] = args.reasoning_effort
    if args.max_sources is not None:
        os.environ["MAX_SOURCES"] = str(args.max_sources)
        os.environ["PERPLEXITY_MAX_RESULTS_PER_QUERY"] = str(args.max_sources)
    if args.decode_max_per_model is not None:
        os.environ["SOURCE_DECODING_MAX_PER_MODEL"] = str(args.decode_max_per_model)

    # Import after env overrides so backend config picks up runtime test settings.
    from backend.council import (
        stage1_collect_perplexity_research_responses,
        stage2_collect_rankings,
        stage3_synthesize_final,
        calculate_aggregate_rankings,
    )
    from backend.config import CHAIRMAN_MODEL
    from backend.config import ENABLE_MARKET_FACTS_PREPASS
    from backend.config import PERPLEXITY_API_URL, PERPLEXITY_COUNCIL_MODELS, MAX_SOURCES
    from backend.main import build_enhanced_context
    from backend.market_facts import (
        gather_market_facts_prepass,
        format_market_facts_query_prefix,
        prepend_market_facts_to_query,
    )
    from backend.research.providers.perplexity import PerplexityResearchProvider
    from backend.template_loader import get_template_loader, resolve_template_selection

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
    use_structured_analysis = loader.is_structured_template(selected_template_id)

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
            raise ValueError(
                "--query is required when --query-mode=user."
            )

    stage1_research_brief = loader.get_stage1_research_brief(
        selected_template_id,
        selected_company_type,
        selected_exchange,
        selected_company_name,
        include_rubric=(args.query_mode != "template_only"),
    )

    total_start = perf_counter()
    _print_header(args, selection)

    market_facts = None
    if ENABLE_MARKET_FACTS_PREPASS and args.ticker:
        _progress("Market facts prepass start")
        market_facts = await gather_market_facts_prepass(
            ticker=args.ticker,
            company_name=selected_company_name,
            exchange=selected_exchange,
        )
        if market_facts:
            print(f"Market facts status: {market_facts.get('status')}")
            market_facts_text = format_market_facts_query_prefix(market_facts)
            if market_facts_text:
                print("Market facts prepass prepared; minimal normalized_facts block will be prepended to Stage 1 query.")

    stage1_effective_research_brief = stage1_research_brief
    market_facts_query_prefix = format_market_facts_query_prefix(market_facts)
    stage1_effective_query = prepend_market_facts_to_query(effective_query, market_facts)

    _print_input_audit(
        selection=selection,
        effective_query=effective_query,
        stage1_query_sent=stage1_effective_query,
        stage1_research_brief=stage1_effective_research_brief,
        market_facts=market_facts,
        market_facts_query_prefix=market_facts_query_prefix,
    )

    if args.dry_run_input:
        provider = PerplexityResearchProvider()
        depth = "deep"
        max_sources = args.max_sources if args.max_sources is not None else MAX_SOURCES
        models = [
            item.strip()
            for item in (
                args.council_models.split(",")
                if args.council_models
                else PERPLEXITY_COUNCIL_MODELS
            )
            if item.strip()
        ]
        prompt = provider._build_prompt(
            user_query=stage1_effective_query,
            ticker=args.ticker,
            depth=depth,
            max_sources=max_sources,
            research_brief=stage1_effective_research_brief,
        )
        payloads: Dict[str, Dict[str, Any]] = {}
        for model in models:
            payloads[model] = provider._build_payload(
                prompt=prompt,
                depth=depth,
                max_sources=max_sources,
                model_override=model,
            )
        _print_provider_request_audit(
            endpoint=PERPLEXITY_API_URL,
            models=models,
            prompt=prompt,
            payloads=payloads,
        )
        return

    _progress("Stage 1 start")

    stage1_start = perf_counter()
    stage1_results, metadata = await stage1_collect_perplexity_research_responses(
        user_query=stage1_effective_query,
        ticker=args.ticker,
        attachment_context="",
        depth="deep",
        research_brief=stage1_effective_research_brief,
    )
    _progress(f"Stage 1 done in {perf_counter() - stage1_start:.1f}s")
    _print_stage1(metadata)

    if not stage1_results:
        print("\nNo Stage 1 responses generated. Stopping.")
        return

    search_results = metadata.get("aggregated_search_results", {})
    enhanced_context = build_enhanced_context(
        effective_query,
        search_results,
        [],
        market_facts=market_facts,
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
        market_facts=market_facts,
    )
    _progress(f"Stage 3 done in {perf_counter() - stage3_start:.1f}s")
    _print_stage3(stage3_result)

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
            "stage2_results": stage2_results,
            "stage3_result": stage3_result,
            "metadata": metadata,
            "selection": selection,
        }
        with open(args.dump_json, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"\nSaved JSON output to: {args.dump_json}")

    _progress(f"Run complete in {perf_counter() - total_start:.1f}s")
    print("\nMVP quality test complete.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run end-to-end MVP financial quality score test.",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Prompt to evaluate.",
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
        help="Optional explicit template ID override for structured synthesis.",
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
    parser.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="Optional override for PERPLEXITY_MAX_STEPS for this run.",
    )
    parser.add_argument(
        "--council-models",
        type=str,
        default=None,
        help="Optional override for PERPLEXITY_COUNCIL_MODELS (comma-separated).",
    )
    parser.add_argument(
        "--reasoning-effort",
        type=str,
        default=None,
        choices=["none", "low", "medium", "high"],
        help="Optional override for PERPLEXITY_REASONING_EFFORT for this run.",
    )
    parser.add_argument(
        "--max-sources",
        type=int,
        default=None,
        help="Optional override for MAX_SOURCES and PERPLEXITY_MAX_RESULTS_PER_QUERY.",
    )
    parser.add_argument(
        "--decode-max-per-model",
        type=int,
        default=None,
        help="Optional override for SOURCE_DECODING_MAX_PER_MODEL.",
    )
    parser.add_argument(
        "--dry-run-input",
        action="store_true",
        help="Print exact Stage 1 prompt/payload and exit without API calls.",
    )
    return parser


if __name__ == "__main__":
    _ensure_pymupdf_runtime()
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))
