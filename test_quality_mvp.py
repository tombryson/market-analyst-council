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
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple


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


def _build_injection_bundle_attachment_context(
    *,
    bundle_path: str,
    max_docs: int,
    max_chars: int,
) -> Tuple[str, Dict[str, Any]]:
    path = Path(str(bundle_path or "")).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Injection bundle not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    docs = list(payload.get("docs", []) or [])
    total_docs = len(docs)
    if int(max_docs) > 0:
        docs = docs[: int(max_docs)]

    lines: List[str] = [
        "EXTERNAL PREPASS INJECTION BUNDLE (STRICT, USER-PROVIDED)",
        "Use these points as high-priority evidence alongside live retrieval.",
        "",
    ]
    for idx, row in enumerate(docs, 1):
        if not isinstance(row, dict):
            continue
        lines.append(
            f"[D{idx}] {row.get('title', '')} | {row.get('published_at', '')} "
            f"| importance={row.get('importance_score', '')}"
        )
        one_line = str(row.get("one_line", "")).strip()
        if one_line:
            lines.append(f"- Summary: {one_line}")

        for item in list(row.get("key_points", []) or [])[:10]:
            text = str(item).strip()
            if text:
                lines.append(f"- Key: {text}")
        for item in list(row.get("timeline_milestones", []) or [])[:4]:
            if isinstance(item, dict):
                milestone = str(item.get("milestone", "")).strip()
                target = str(item.get("target_window", "")).strip()
                direction = str(item.get("direction", "")).strip()
                if milestone or target:
                    lines.append(
                        f"- Timeline: {milestone} | target={target} | direction={direction}"
                    )
            else:
                text = str(item).strip()
                if text:
                    lines.append(f"- Timeline: {text}")
        for item in list(row.get("catalysts_next_12m", []) or [])[:4]:
            text = str(item).strip()
            if text:
                lines.append(f"- Catalyst: {text}")
        for item in list(row.get("risks_headwinds", []) or [])[:4]:
            text = str(item).strip()
            if text:
                lines.append(f"- Risk: {text}")
        lines.append("")

    raw = "\n".join(lines).strip()
    cap = int(max_chars)
    truncated = False
    if cap > 0:
        trimmed = raw[:cap]
        if len(raw) > cap:
            truncated = True
            trimmed = trimmed.rstrip() + "\n\n[TRUNCATED DUE TO MAX CHARS]"
    else:
        trimmed = raw

    meta = {
        "bundle_path": str(path),
        "total_docs_in_bundle": int(total_docs),
        "docs_included": int(len(docs)),
        "raw_chars": int(len(raw)),
        "included_chars": int(len(trimmed)),
        "max_chars": int(cap),
        "truncated": bool(truncated),
    }
    return trimmed, meta

def _print_header(args: argparse.Namespace, selection: Dict[str, Any]) -> None:
    print("=" * 90)
    print("FINANCIAL QUALITY MVP TEST")
    print("=" * 90)
    print("Query mode: template (fixed)")
    print(f"User query hint: {args.query or '(none)'}")
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
    company_type_detection = selection.get("company_type_detection", {})
    if isinstance(company_type_detection, dict) and company_type_detection:
        print(
            "Company-type detection: "
            f"provider={company_type_detection.get('provider')} "
            f"status={company_type_detection.get('status')} "
            f"selected={company_type_detection.get('selected_company_type')} "
            f"candidate={company_type_detection.get('candidate_company_type')} "
            f"confidence={company_type_detection.get('confidence')}"
        )
    print("Runtime config source: .env (no CLI overrides)")
    print("-" * 90)


def _print_stage1(metadata: Dict[str, Any]) -> None:
    print("\nSTAGE 1")
    print("-" * 90)
    attempted = metadata.get("models_attempted", [])
    succeeded = metadata.get("models_succeeded", [])
    print(f"Models attempted: {len(attempted)}")
    print(f"Models succeeded: {len(succeeded)}")
    if metadata.get("stage1_execution_mode") is not None:
        print(
            "Stage1 routing: "
            f"execution_mode={metadata.get('stage1_execution_mode')} "
            f"mixed_mode={metadata.get('stage1_mixed_mode_enabled')} "
            f"perplexity_pool={metadata.get('stage1_mixed_mode_perplexity_pool')} "
            f"openrouter_pool={metadata.get('stage1_mixed_mode_openrouter_pool')}"
        )
    if metadata.get("stage1_second_pass_enabled") is not None:
        print(
            "Second-pass settings: "
            f"enabled={metadata.get('stage1_second_pass_enabled')} "
            f"max_sources={metadata.get('stage1_second_pass_max_sources')} "
            f"max_chars_per_source={metadata.get('stage1_second_pass_max_chars_per_source')} "
            f"appendix_max_sources={metadata.get('stage1_second_pass_appendix_max_sources')} "
            f"max_output_tokens={metadata.get('stage1_second_pass_max_output_tokens')} "
            f"reasoning_effort={metadata.get('stage1_second_pass_reasoning_effort')}"
        )
        if metadata.get("stage1_timeline_guard_enabled") is not None:
            print(
                "Timeline guard settings: "
                f"enabled={metadata.get('stage1_timeline_guard_enabled')} "
                f"hard_fail={metadata.get('stage1_timeline_guard_hard_fail')} "
                f"digest_max_items={metadata.get('stage1_timeline_digest_max_items')}"
            )
        if metadata.get("stage1_fact_digest_v2_enabled") is not None:
            print(
                "Fact digest v2 settings: "
                f"enabled={metadata.get('stage1_fact_digest_v2_enabled')} "
                "max_facts_per_section="
                f"{metadata.get('stage1_fact_digest_v2_max_facts_per_section')} "
                "max_summary_bullets="
                f"{metadata.get('stage1_fact_digest_v2_max_summary_bullets')} "
                "max_narrative_words="
                f"{metadata.get('stage1_fact_digest_v2_max_narrative_words')}"
            )
        if metadata.get("stage1_second_pass_citation_gate_enabled") is not None:
            print(
                "Second-pass citation gate: "
                f"enabled={metadata.get('stage1_second_pass_citation_gate_enabled')} "
                f"min_count={metadata.get('stage1_second_pass_citation_min_count')} "
                "max_uncited_numeric_lines="
                f"{metadata.get('stage1_second_pass_citation_max_uncited_numeric_lines')}"
            )
        if metadata.get("stage1_second_pass_compliance_min_score") is not None:
            print(
                "Second-pass compliance thresholds: "
                f"min_score={metadata.get('stage1_second_pass_compliance_min_score')} "
                "min_rubric_coverage="
                f"{metadata.get('stage1_second_pass_compliance_min_rubric_coverage_pct')} "
                "min_numeric_citation="
                f"{metadata.get('stage1_second_pass_compliance_min_numeric_citation_pct')} "
                "catastrophic_score="
                f"{metadata.get('stage1_second_pass_compliance_catastrophic_score')}"
            )
    if metadata.get("stage1_asx_deterministic_announcements_enabled") is not None:
        print(
            "Deterministic ASX ingest: "
            f"enabled={metadata.get('stage1_asx_deterministic_announcements_enabled')} "
            f"target={metadata.get('stage1_asx_deterministic_target_announcements')} "
            f"lookback_years={metadata.get('stage1_asx_deterministic_lookback_years')} "
            "price_sensitive_only="
            f"{metadata.get('stage1_asx_deterministic_price_sensitive_only')} "
            "fill_non_sensitive="
            f"{metadata.get('stage1_asx_deterministic_include_non_sensitive_fill')} "
            f"max_decode={metadata.get('stage1_asx_deterministic_max_decode')}"
        )
    if metadata.get("stage1_shared_retrieval_requested") is not None:
        print(
            "Shared retrieval: "
            f"requested={metadata.get('stage1_shared_retrieval_requested')} "
            f"used={metadata.get('stage1_shared_retrieval_used')} "
            f"model={metadata.get('stage1_shared_retrieval_model')}"
        )
        if metadata.get("stage1_shared_retrieval_error"):
            print(f"Shared retrieval fallback reason: {metadata.get('stage1_shared_retrieval_error')}")
    if metadata.get("stage1_openai_guardrails_enabled") is not None:
        print(
            "OpenAI pass-1 guardrails: "
            f"enabled={metadata.get('stage1_openai_guardrails_enabled')} "
            f"max_sources={metadata.get('stage1_openai_base_max_sources')} "
            f"max_steps={metadata.get('stage1_openai_base_max_steps')} "
            f"reasoning={metadata.get('stage1_openai_base_reasoning_effort')}"
        )
    if metadata.get("stage1_verification_compliance_markers") is not None:
        print(
            "Verification profile: "
            f"template={metadata.get('stage1_verification_template_id') or 'none'} "
            "digest_sections="
            f"{metadata.get('stage1_verification_digest_sections')} "
            "compliance_markers="
            f"{metadata.get('stage1_verification_compliance_markers')} "
            "critical_sections="
            f"{metadata.get('stage1_verification_critical_sections')}"
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
            if provider_meta.get("stage1_second_pass_citation_gate_passed") is not None:
                print(
                    "  second_pass_citation_gate: "
                    f"passed={provider_meta.get('stage1_second_pass_citation_gate_passed')} "
                    f"reason={provider_meta.get('stage1_second_pass_citation_gate_reason')} "
                    f"citations={provider_meta.get('stage1_second_pass_citation_count')} "
                    "uncited_numeric_lines="
                    f"{provider_meta.get('stage1_second_pass_citation_uncited_numeric_lines')}"
                )
                if provider_meta.get("stage1_second_pass_compliance_score") is not None:
                    print(
                        "  second_pass_compliance: "
                        f"score={provider_meta.get('stage1_second_pass_compliance_score'):.3f} "
                        f"rating={provider_meta.get('stage1_second_pass_compliance_rating')} "
                        "rubric_coverage_pct="
                        f"{provider_meta.get('stage1_second_pass_rubric_coverage_pct')} "
                        "numeric_citation_pct="
                        f"{provider_meta.get('stage1_second_pass_citation_numeric_citation_pct')} "
                        "retry_recommended="
                        f"{provider_meta.get('stage1_second_pass_compliance_retry_recommended')}"
                    )
            if provider_meta.get("stage1_second_pass_timeline_guard_passed") is not None:
                print(
                    "  second_pass_timeline_guard: "
                    f"passed={provider_meta.get('stage1_second_pass_timeline_guard_passed')} "
                    f"reason={provider_meta.get('stage1_second_pass_timeline_guard_reason')} "
                    "shifted_quarters="
                    f"{provider_meta.get('stage1_second_pass_timeline_guard_shifted_quarters')} "
                    "evidence_windows="
                    f"{provider_meta.get('stage1_second_pass_timeline_guard_evidence_windows')} "
                    "response_windows="
                    f"{provider_meta.get('stage1_second_pass_timeline_guard_response_windows')}"
                )
            if provider_meta.get("stage1_second_pass_source_rows_count") is not None:
                print(
                    "  second_pass_injection: "
                    f"source_rows={provider_meta.get('stage1_second_pass_source_rows_count')} "
                    "timeline_evidence_rows="
                    f"{provider_meta.get('stage1_second_pass_timeline_evidence_count')} "
                    "timeline_digest_chars="
                    f"{provider_meta.get('stage1_second_pass_timeline_digest_chars')}"
                )
            if provider_meta.get("stage1_second_pass_fact_digest_v2_total_facts") is not None:
                print(
                    "  second_pass_fact_digest_v2: "
                    f"enabled={provider_meta.get('stage1_second_pass_fact_digest_v2_enabled')} "
                    f"facts={provider_meta.get('stage1_second_pass_fact_digest_v2_total_facts')} "
                    "sections="
                    f"{provider_meta.get('stage1_second_pass_fact_digest_v2_sections_with_facts')} "
                    "summary_bullets="
                    f"{provider_meta.get('stage1_second_pass_fact_digest_v2_summary_bullets')} "
                    "conflicts="
                    f"{provider_meta.get('stage1_second_pass_fact_digest_v2_conflicts')} "
                    "chars="
                    f"{provider_meta.get('stage1_second_pass_fact_digest_v2_chars')}"
                )
            if provider_meta.get("stage1_second_pass_verification_template_id") is not None:
                print(
                    "  second_pass_verification: "
                    f"template={provider_meta.get('stage1_second_pass_verification_template_id') or 'none'} "
                    "digest_sections="
                    f"{provider_meta.get('stage1_second_pass_verification_digest_sections')} "
                    "compliance_markers="
                    f"{provider_meta.get('stage1_second_pass_verification_compliance_markers')} "
                    "critical_sections="
                    f"{provider_meta.get('stage1_second_pass_verification_critical_sections')}"
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
            if provider_meta.get("stage1_second_pass_asx_deterministic_enabled") is not None:
                print(
                    "  second_pass_asx_deterministic: "
                    f"enabled={provider_meta.get('stage1_second_pass_asx_deterministic_enabled')} "
                    f"used={provider_meta.get('stage1_second_pass_asx_deterministic_used')} "
                    f"symbol={provider_meta.get('stage1_second_pass_asx_deterministic_symbol')} "
                    f"reason={provider_meta.get('stage1_second_pass_asx_deterministic_reason')} "
                    f"cache_hit={provider_meta.get('stage1_second_pass_asx_deterministic_cache_hit')} "
                    "selected_rows="
                    f"{provider_meta.get('stage1_second_pass_asx_deterministic_selected_rows')} "
                    "decoded_rows="
                    f"{provider_meta.get('stage1_second_pass_asx_deterministic_decoded_rows')}"
                )
            if provider_meta.get("stage1_final_template_compliant") is not None:
                print(
                    "  second_pass_template_compliant: "
                    f"{provider_meta.get('stage1_final_template_compliant')} "
                    f"reason={provider_meta.get('stage1_final_template_reason')}"
                )
            if provider_meta.get("stage1_second_pass_error"):
                print(f"  second_pass_error: {provider_meta.get('stage1_second_pass_error')}")
            if provider_meta.get("stage1_second_pass_warning"):
                print(f"  second_pass_warning: {provider_meta.get('stage1_second_pass_warning')}")
        if provider_meta.get("stage1_shared_retrieval_used") is not None:
            print(
                "  shared_retrieval: "
                f"used={provider_meta.get('stage1_shared_retrieval_used')} "
                f"source_model={provider_meta.get('stage1_shared_retrieval_model')}"
            )
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
        second_pass_rows = result.get("stage1_second_pass_source_rows", []) or []
        if second_pass_rows:
            print(f"  second_pass_source_rows_count(full): {len(second_pass_rows)}")
            print("  --- second_pass_source_rows_top3 ---")
            print(json.dumps(second_pass_rows[:3], indent=2))
            print("  --- second_pass_source_rows_end ---")
        timeline_rows = result.get("stage1_second_pass_timeline_evidence", []) or []
        if timeline_rows:
            print(f"  second_pass_timeline_rows_count(full): {len(timeline_rows)}")
            print("  --- second_pass_timeline_rows_top5 ---")
            print(json.dumps(timeline_rows[:5], indent=2))
            print("  --- second_pass_timeline_rows_end ---")
        fact_digest_v2 = result.get("stage1_second_pass_fact_digest_v2", {}) or {}
        if fact_digest_v2:
            print("  --- second_pass_fact_digest_v2_summary ---")
            print(
                json.dumps(
                    {
                        "counts": fact_digest_v2.get("counts", {}),
                        "summary_bullets": (fact_digest_v2.get("summary_bullets", []) or [])[:6],
                        "conflicts": (fact_digest_v2.get("conflicts", []) or [])[:3],
                    },
                    indent=2,
                )
            )
            print("  --- second_pass_fact_digest_v2_end ---")
        if result.get("error"):
            print(f"  error: {result.get('error')}")


def _classify_response_type(response: str) -> str:
    text = (response or "").strip()
    if not text:
        return "empty"
    if text.startswith("Perplexity Deep Research Run for model"):
        return "fallback_research_log"
    json_candidate = text
    fence_match = re.match(r"^```(?:json)?\s*\n([\s\S]+?)\n```$", text, flags=re.IGNORECASE)
    if fence_match:
        json_candidate = fence_match.group(1).strip()
    if json_candidate.startswith("{"):
        try:
            json.loads(json_candidate)
            return "json_analysis"
        except Exception:
            return "json_malformed"
    if text.startswith("#") or text.startswith("**"):
        return "markdown_analysis"
    return "plain_text"


def _extract_named_score(response: str, label: str) -> Optional[float]:
    text = response or ""
    candidates: List[float] = []

    # Code-fenced JSON fallback.
    fence_match = re.match(r"^```(?:json)?\s*\n([\s\S]+?)\n```$", text.strip(), flags=re.IGNORECASE)
    if fence_match:
        fenced = fence_match.group(1).strip()
        if fenced:
            text = fenced

    # Structured JSON key forms.
    for m in re.finditer(
        rf'(?i)"{label}[_\s-]*score"\s*:\s*([0-9]{{1,3}}(?:\.[0-9]+)?)',
        text,
    ):
        try:
            value = float(m.group(1))
            if 0 <= value <= 100:
                candidates.append(value)
        except (TypeError, ValueError):
            continue

    # Nested JSON form like "quality_score": {"score": 88, ...}
    for m in re.finditer(
        rf'(?is)"{label}[_\s-]*score"\s*:\s*\{{[^{{}}]{{0,400}}?"score"\s*:\s*([0-9]{{1,3}}(?:\.[0-9]+)?)',
        text,
    ):
        try:
            value = float(m.group(1))
            if 0 <= value <= 100:
                candidates.append(value)
        except (TypeError, ValueError):
            continue

    # Common markdown/table forms like "Quality Score ... 66.5 / 100".
    for m in re.finditer(
        rf"(?i)\b{label}\s*score\b[^\n]{{0,240}}?([0-9]{{1,3}}(?:\.[0-9]+)?)\s*/\s*100\b",
        text,
    ):
        try:
            value = float(m.group(1))
            if 0 <= value <= 100:
                candidates.append(value)
        except (TypeError, ValueError):
            continue

    # Direct label assignment forms like "Quality Score: 63.75".
    for m in re.finditer(
        rf"(?i)\b{label}\s*score\b\s*[:=]\s*\*{{0,2}}\s*([0-9]{{1,3}}(?:\.[0-9]+)?)\b",
        text,
    ):
        try:
            value = float(m.group(1))
            if 0 <= value <= 100:
                candidates.append(value)
        except (TypeError, ValueError):
            continue

    # Formula-line fallback: pick last RHS value in lines mentioning "{label} score".
    for line in text.splitlines():
        if not re.search(rf"(?i)\b{label}\s*score\b", line):
            continue
        rhs = re.findall(r"=\s*\*{0,2}\s*([0-9]{1,3}(?:\.[0-9]+)?)", line)
        if rhs:
            try:
                value = float(rhs[-1])
                if 0 <= value <= 100:
                    candidates.append(value)
            except (TypeError, ValueError):
                continue

    if candidates:
        # Prefer the latest mention in the response (often the final roll-up total).
        return candidates[-1]
    return None


def _build_stage1_model_audit(
    stage1_results: List[Dict[str, Any]],
    metadata: Dict[str, Any],
) -> List[Dict[str, Any]]:
    by_model = {item.get("model"): item for item in stage1_results}
    audits: List[Dict[str, Any]] = []
    for run in metadata.get("per_model_research_runs", []):
        model = run.get("model", "unknown")
        result = run.get("result", {}) or {}
        provider_meta = result.get("provider_metadata", {}) or {}
        response = (by_model.get(model, {}) or {}).get("response", "") or ""
        response_type = _classify_response_type(response)

        quality_score = _extract_named_score(response, "quality")
        value_score = _extract_named_score(response, "value")
        has_price_targets = bool(
            re.search(r"(?i)\b12[-\s]*month\b|\b24[-\s]*month\b|\bprice\s*target", response)
        )
        has_timeline = bool(re.search(r"(?i)\bdevelopment\s+timeline\b|\bmilestone", response))
        has_certainty = bool(re.search(r"(?i)\bcertainty\b|\bconfidence", response))

        second_pass_success = provider_meta.get("stage1_second_pass_success")
        compliance_rating = (provider_meta.get("stage1_second_pass_compliance_rating") or "").lower()
        compliance_score = provider_meta.get("stage1_second_pass_compliance_score")
        citation_gate_passed = provider_meta.get("stage1_second_pass_citation_gate_passed")
        second_pass_error = provider_meta.get("stage1_second_pass_error")
        second_pass_warning = provider_meta.get("stage1_second_pass_warning")

        reasons: List[str] = []
        if response_type in {"empty", "fallback_research_log", "json_malformed"}:
            reasons.append(f"response_type={response_type}")
        if second_pass_success is False:
            reasons.append("second_pass_failed")
        if compliance_rating == "red":
            reasons.append("compliance_rating=red")
        elif compliance_rating == "amber":
            reasons.append("compliance_rating=amber")
        if citation_gate_passed is False:
            reasons.append("citation_gate_failed")
        if quality_score is None:
            reasons.append("missing_quality_score")
        if value_score is None:
            reasons.append("missing_value_score")
        if not has_price_targets:
            reasons.append("missing_price_targets")
        if not has_timeline:
            reasons.append("missing_timeline")
        if not has_certainty:
            reasons.append("missing_certainty")
        if second_pass_error:
            reasons.append(f"second_pass_error={second_pass_error}")
        if second_pass_warning:
            reasons.append(f"second_pass_warning={second_pass_warning}")

        if (
            second_pass_success is False
            or response_type in {"empty", "fallback_research_log", "json_malformed"}
            or compliance_rating == "red"
        ):
            recommendation = "remove"
        elif (
            compliance_rating == "amber"
            or citation_gate_passed is False
            or quality_score is None
            or value_score is None
            or not has_price_targets
            or not has_timeline
            or not has_certainty
        ):
            recommendation = "watch"
        else:
            recommendation = "keep"

        audits.append(
            {
                "model": model,
                "analysis_provider": provider_meta.get("stage1_analysis_provider"),
                "response_type": response_type,
                "response_chars": len(response),
                "quality_score_detected": quality_score,
                "value_score_detected": value_score,
                "has_price_targets": has_price_targets,
                "has_timeline": has_timeline,
                "has_certainty": has_certainty,
                "second_pass_success": second_pass_success,
                "second_pass_attempts": provider_meta.get("stage1_second_pass_attempts"),
                "compliance_rating": compliance_rating or None,
                "compliance_score": compliance_score,
                "rubric_coverage_pct": provider_meta.get("stage1_second_pass_rubric_coverage_pct"),
                "numeric_citation_pct": provider_meta.get("stage1_second_pass_citation_numeric_citation_pct"),
                "citation_count": provider_meta.get("stage1_second_pass_citation_count"),
                "citation_gate_passed": citation_gate_passed,
                "second_pass_error": second_pass_error,
                "second_pass_warning": second_pass_warning,
                "recommendation": recommendation,
                "reasons": reasons,
            }
        )
    return audits


def _print_stage1_model_audit(stage1_model_audit: List[Dict[str, Any]]) -> None:
    print("\nSTAGE 1 MODEL AUDIT")
    print("-" * 90)
    if not stage1_model_audit:
        print("No model audit rows available.")
        return
    keep = sum(1 for row in stage1_model_audit if row.get("recommendation") == "keep")
    watch = sum(1 for row in stage1_model_audit if row.get("recommendation") == "watch")
    remove = sum(1 for row in stage1_model_audit if row.get("recommendation") == "remove")
    print(f"Recommendations summary: keep={keep} watch={watch} remove={remove}")
    for row in stage1_model_audit:
        print(
            f"[{(row.get('recommendation') or 'watch').upper()}] {row.get('model')} "
            f"(provider={row.get('analysis_provider')}, type={row.get('response_type')})"
        )
        print(
            "  conformance: "
            f"second_pass_success={row.get('second_pass_success')} "
            f"rating={row.get('compliance_rating')} "
            f"score={row.get('compliance_score')} "
            f"citation_gate={row.get('citation_gate_passed')}"
        )
        print(
            "  output_signals: "
            f"quality={row.get('quality_score_detected')} "
            f"value={row.get('value_score_detected')} "
            f"price_targets={row.get('has_price_targets')} "
            f"timeline={row.get('has_timeline')} "
            f"certainty={row.get('has_certainty')}"
        )
        reasons = row.get("reasons") or []
        if reasons:
            print(f"  reasons: {' | '.join(reasons[:6])}")


def _print_stage2(stage2_results: list[Dict[str, Any]], aggregate_rankings: list[Dict[str, Any]]) -> None:
    print("\nSTAGE 2")
    print("-" * 90)
    print(f"Rankings received: {len(stage2_results)}")
    if aggregate_rankings:
        print("Aggregate ranking:")
        for i, item in enumerate(aggregate_rankings, 1):
            print(f"  {i}. {item['model']} (avg rank: {item['average_rank']:.2f})")


def _print_stage2_revision(
    stage2_revision_summary: Dict[str, Any],
    stage2_revision_results: List[Dict[str, Any]],
) -> None:
    print("\nSTAGE 2.5 (REVISION PASS)")
    print("-" * 90)
    if not stage2_revision_summary or not stage2_revision_summary.get("enabled"):
        print("Revision pass disabled.")
        return
    print(
        "Revision summary: "
        f"attempted={len(stage2_revision_summary.get('models_attempted', []) or [])} "
        f"accepted={stage2_revision_summary.get('accepted_count', 0)} "
        f"changed={stage2_revision_summary.get('changed_count', 0)} "
        f"parse_failed={stage2_revision_summary.get('parse_failed_count', 0)}"
    )
    apply_summary = stage2_revision_summary.get("apply") or {}
    if apply_summary:
        print(
            "Applied deltas: "
            f"{apply_summary.get('revisions_applied', 0)}/"
            f"{apply_summary.get('models_total', 0)}"
        )
    for row in stage2_revision_results or []:
        print(
            f"  - {row.get('model')}: "
            f"accepted={row.get('accepted')} "
            f"changed={row.get('changed')} "
            f"parse_error={row.get('parse_error') or 'none'} "
            f"response_chars={row.get('response_chars', 0)}"
        )


def _print_stage3(stage3_result: Dict[str, Any], *, title: str = "STAGE 3") -> None:
    print(f"\n{title}")
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


def _build_stage3_comparison(
    primary: Dict[str, Any],
    secondary: Dict[str, Any] | None,
) -> Dict[str, Any]:
    if not secondary:
        return {}

    p = primary.get("structured_data") or {}
    s = secondary.get("structured_data") or {}
    if not isinstance(p, dict) or not isinstance(s, dict):
        return {
            "available": False,
            "reason": "missing_structured_data",
            "primary_model": primary.get("model"),
            "secondary_model": secondary.get("model"),
        }

    def _score_total(block: Dict[str, Any], key: str) -> Optional[float]:
        obj = block.get(key) or {}
        if isinstance(obj, dict):
            val = obj.get("total")
            try:
                return float(val) if val is not None else None
            except (TypeError, ValueError):
                return None
        return None

    p_quality = _score_total(p, "quality_score")
    s_quality = _score_total(s, "quality_score")
    p_value = _score_total(p, "value_score")
    s_value = _score_total(s, "value_score")

    p_targets = p.get("price_targets") or {}
    s_targets = s.get("price_targets") or {}

    p_rec = (p.get("investment_recommendation") or {}).get("rating")
    s_rec = (s.get("investment_recommendation") or {}).get("rating")

    return {
        "available": True,
        "primary_model": primary.get("model"),
        "secondary_model": secondary.get("model"),
        "quality_total": {"primary": p_quality, "secondary": s_quality},
        "value_total": {"primary": p_value, "secondary": s_value},
        "quality_delta_secondary_minus_primary": (
            (s_quality - p_quality)
            if (s_quality is not None and p_quality is not None)
            else None
        ),
        "value_delta_secondary_minus_primary": (
            (s_value - p_value)
            if (s_value is not None and p_value is not None)
            else None
        ),
        "recommendation": {"primary": p_rec, "secondary": s_rec},
        "target_12m": {
            "primary": p_targets.get("target_12m"),
            "secondary": s_targets.get("target_12m"),
        },
        "target_24m": {
            "primary": p_targets.get("target_24m"),
            "secondary": s_targets.get("target_24m"),
        },
    }


def _print_stage3_comparison(stage3_comparison: Dict[str, Any]) -> None:
    print("\nSTAGE 3 COMPARISON")
    print("-" * 90)
    if not stage3_comparison:
        print("No secondary chairman run.")
        return
    if not stage3_comparison.get("available"):
        print(
            "Comparison unavailable: "
            f"{stage3_comparison.get('reason', 'unknown')}"
        )
        print(
            "Models: "
            f"{stage3_comparison.get('primary_model')} vs "
            f"{stage3_comparison.get('secondary_model')}"
        )
        return

    print(
        "Models: "
        f"{stage3_comparison.get('primary_model')} vs "
        f"{stage3_comparison.get('secondary_model')}"
    )
    print(
        "Quality totals: "
        f"primary={stage3_comparison.get('quality_total', {}).get('primary')} "
        f"secondary={stage3_comparison.get('quality_total', {}).get('secondary')} "
        f"delta={stage3_comparison.get('quality_delta_secondary_minus_primary')}"
    )
    print(
        "Value totals: "
        f"primary={stage3_comparison.get('value_total', {}).get('primary')} "
        f"secondary={stage3_comparison.get('value_total', {}).get('secondary')} "
        f"delta={stage3_comparison.get('value_delta_secondary_minus_primary')}"
    )
    print(
        "Recommendations: "
        f"primary={stage3_comparison.get('recommendation', {}).get('primary')} "
        f"secondary={stage3_comparison.get('recommendation', {}).get('secondary')}"
    )
    print(
        "12m targets: "
        f"primary={stage3_comparison.get('target_12m', {}).get('primary')} "
        f"secondary={stage3_comparison.get('target_12m', {}).get('secondary')}"
    )
    print(
        "24m targets: "
        f"primary={stage3_comparison.get('target_24m', {}).get('primary')} "
        f"secondary={stage3_comparison.get('target_24m', {}).get('secondary')}"
    )


def _print_input_audit(
    selection: Dict[str, Any],
    effective_query: str,
    stage1_query_sent: str,
    stage1_research_brief: str,
    market_facts: Dict[str, Any] | None,
    market_facts_query_prefix: str,
    injection_bundle_meta: Dict[str, Any] | None,
    injection_bundle_context: str,
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
    print("\nInjection Bundle Meta:")
    print(json.dumps(injection_bundle_meta or {}, indent=2))
    print("\nInjection Bundle Context Sent As Attachment Context:")
    print(injection_bundle_context or "(none)")


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
    # Import runtime config from .env (authoritative).
    from backend.council import (
        stage1_collect_perplexity_research_responses,
        stage2_collect_rankings,
        stage2_collect_revision_deltas,
        apply_stage2_revision_deltas,
        stage3_synthesize_final,
        calculate_aggregate_rankings,
    )
    from backend.config import CHAIRMAN_MODEL
    from backend.config import ENABLE_MARKET_FACTS_PREPASS
    from backend.config import STAGE2_REVISION_PASS_ENABLED
    from backend.config import PERPLEXITY_API_URL, PERPLEXITY_COUNCIL_MODELS, MAX_SOURCES
    from backend.main import build_enhanced_context
    from backend.market_facts import (
        gather_market_facts_prepass,
        format_market_facts_query_prefix,
        prepend_market_facts_to_query,
    )
    from backend.company_type_detector import detect_company_type_via_api
    from backend.research.providers.perplexity import PerplexityResearchProvider
    from backend.template_loader import get_template_loader, resolve_template_selection

    detected_company_type_payload: Dict[str, Any] = {}
    detected_company_type = None
    if not args.company_type:
        detected_company_type_payload = await detect_company_type_via_api(
            user_query=args.query or "",
            ticker=args.ticker,
            company_name=None,
            exchange=args.exchange,
        )
        candidate = str(
            detected_company_type_payload.get("selected_company_type") or ""
        ).strip()
        if candidate:
            detected_company_type = candidate

    selection = resolve_template_selection(
        user_query=args.query or "",
        ticker=args.ticker,
        explicit_template_id=args.template_id,
        company_type=(args.company_type or detected_company_type),
        exchange=args.exchange,
    )
    if detected_company_type_payload:
        selection["company_type_detection"] = detected_company_type_payload
        if detected_company_type and not args.company_type:
            selection["selection_source"] = "api_company_type_detected"
    selected_template_id = selection["template_id"]
    selected_company_name = selection.get("company_name")
    selected_company_type = selection.get("company_type")
    selected_exchange = selection.get("exchange")
    loader = get_template_loader()
    use_structured_analysis = loader.is_structured_template(selected_template_id)

    effective_query = loader.render_stage1_query_prompt(
        selected_template_id,
        company_name=selected_company_name,
        exchange=selected_exchange,
    )
    if not effective_query:
        raise ValueError(
            f"Template '{selected_template_id}' has no Stage 1 prompt to use as query."
        )

    stage1_research_brief = loader.get_stage1_research_brief(
        selected_template_id,
        selected_company_type,
        selected_exchange,
        selected_company_name,
        include_rubric=(
            True
            if args.brief_include_rubric == "always"
            else (
                False
                if args.brief_include_rubric == "never"
                else False
            )
        ),
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
    injection_bundle_context = ""
    injection_bundle_meta: Dict[str, Any] = {}
    if args.injection_bundle_json:
        _progress("External injection bundle load start")
        injection_bundle_context, injection_bundle_meta = _build_injection_bundle_attachment_context(
            bundle_path=str(args.injection_bundle_json),
            max_docs=int(args.injection_max_docs),
            max_chars=int(args.injection_max_chars),
        )
        _progress(
            "External injection bundle loaded: "
            f"docs={injection_bundle_meta.get('docs_included', 0)} "
            f"raw_chars={injection_bundle_meta.get('raw_chars', 0)} "
            f"chars={injection_bundle_meta.get('included_chars', 0)} "
            f"max_chars={injection_bundle_meta.get('max_chars', 0)} "
            f"truncated={injection_bundle_meta.get('truncated', False)}"
        )

    _print_input_audit(
        selection=selection,
        effective_query=effective_query,
        stage1_query_sent=stage1_effective_query,
        stage1_research_brief=stage1_effective_research_brief,
        market_facts=market_facts,
        market_facts_query_prefix=market_facts_query_prefix,
        injection_bundle_meta=injection_bundle_meta,
        injection_bundle_context=injection_bundle_context,
    )

    if args.dry_run_input:
        provider = PerplexityResearchProvider()
        depth = "deep"
        max_sources = MAX_SOURCES
        models = [item.strip() for item in PERPLEXITY_COUNCIL_MODELS if item.strip()]
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
        attachment_context=injection_bundle_context,
        depth="deep",
        research_brief=stage1_effective_research_brief,
        template_id=selected_template_id,
        diagnostic_mode=bool(args.diagnostic_mode),
    )
    _progress(f"Stage 1 done in {perf_counter() - stage1_start:.1f}s")
    _print_stage1(metadata)
    stage1_model_audit = _build_stage1_model_audit(stage1_results, metadata)
    _print_stage1_model_audit(stage1_model_audit)

    if not stage1_results:
        print("\nNo Stage 1 responses generated. Stopping.")
        return

    if args.stage1_only:
        if args.dump_json:
            payload = {
                "effective_query": effective_query,
                "stage1_query_sent": stage1_effective_query,
                "input_audit": {
                    "selection": selection,
                    "stage1_research_brief": stage1_effective_research_brief,
                    "market_facts_query_prefix": market_facts_query_prefix,
                    "market_facts": market_facts or {},
                    "injection_bundle_meta": injection_bundle_meta,
                    "injection_bundle_context": injection_bundle_context,
                },
                "stage1_results": stage1_results,
                "stage1_model_audit": stage1_model_audit,
                "stage2_results": [],
                "stage3_result": {},
                "metadata": metadata,
                "selection": selection,
            }
            with open(args.dump_json, "w") as f:
                json.dump(payload, f, indent=2)
            print(f"\nSaved JSON output to: {args.dump_json}")
        _progress(f"Run complete in {perf_counter() - total_start:.1f}s")
        print("\nStage 1-only test complete.")
        return

    search_results = metadata.get("aggregated_search_results", {})
    enhanced_context = build_enhanced_context(
        effective_query,
        search_results,
        [],
        market_facts=market_facts,
    )
    ranking_models = [item.get("model") for item in stage1_results if item.get("model")]
    primary_chairman_model = (
        CHAIRMAN_MODEL
        if CHAIRMAN_MODEL in ranking_models
        else (ranking_models[0] if ranking_models else CHAIRMAN_MODEL)
    )
    secondary_chairman_model = str(args.secondary_chairman_model or "").strip() or None

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

    revision_mode = (args.stage2_revision_pass or "auto").strip().lower()
    revision_enabled = (
        bool(STAGE2_REVISION_PASS_ENABLED)
        if revision_mode == "auto"
        else (revision_mode == "on")
    )
    stage2_revision_results: List[Dict[str, Any]] = []
    stage2_revision_summary: Dict[str, Any] = {"enabled": False}
    stage1_results_for_stage3 = stage1_results
    if revision_enabled:
        _progress("Stage 2.5 revision pass start")
        stage2_revision_start = perf_counter()
        stage2_revision_results, stage2_revision_summary = await stage2_collect_revision_deltas(
            enhanced_context,
            stage1_results,
            stage2_results,
            label_to_model,
            revision_models=ranking_models,
        )
        stage1_results_for_stage3, apply_summary = apply_stage2_revision_deltas(
            stage1_results,
            stage2_revision_results,
        )
        stage2_revision_summary["apply"] = apply_summary
        _progress(
            "Stage 2.5 revision pass done in "
            f"{perf_counter() - stage2_revision_start:.1f}s"
        )
    _print_stage2_revision(stage2_revision_summary, stage2_revision_results)

    _progress("Stage 3 start")
    stage3_primary_start = perf_counter()
    stage3_result_primary = await stage3_synthesize_final(
        enhanced_context,
        stage1_results_for_stage3,
        stage2_results,
        label_to_model=label_to_model,
        use_structured_analysis=use_structured_analysis,
        template_id=selected_template_id,
        ticker=args.ticker,
        company_name=selected_company_name,
        exchange=selected_exchange,
        chairman_model=primary_chairman_model,
        market_facts=market_facts,
        evidence_pack=search_results.get("evidence_pack", {}),
    )
    stage3_primary_elapsed = perf_counter() - stage3_primary_start
    _progress(f"Stage 3 primary done in {stage3_primary_elapsed:.1f}s")
    _print_stage3(stage3_result_primary, title="STAGE 3 (PRIMARY CHAIRMAN)")

    stage3_result_secondary: Dict[str, Any] | None = None
    stage3_secondary_elapsed: Optional[float] = None
    if secondary_chairman_model and secondary_chairman_model != primary_chairman_model:
        _progress(f"Stage 3 secondary start ({secondary_chairman_model})")
        stage3_secondary_start = perf_counter()
        stage3_result_secondary = await stage3_synthesize_final(
            enhanced_context,
            stage1_results_for_stage3,
            stage2_results,
            label_to_model=label_to_model,
            use_structured_analysis=use_structured_analysis,
            template_id=selected_template_id,
            ticker=args.ticker,
            company_name=selected_company_name,
            exchange=selected_exchange,
            chairman_model=secondary_chairman_model,
            market_facts=market_facts,
            evidence_pack=search_results.get("evidence_pack", {}),
        )
        stage3_secondary_elapsed = perf_counter() - stage3_secondary_start
        _progress(f"Stage 3 secondary done in {stage3_secondary_elapsed:.1f}s")
        _print_stage3(stage3_result_secondary, title="STAGE 3 (SECONDARY CHAIRMAN)")

    stage3_comparison = _build_stage3_comparison(
        stage3_result_primary,
        stage3_result_secondary,
    )
    _print_stage3_comparison(stage3_comparison)

    stage3_result = stage3_result_primary

    if args.dump_json:
        payload = {
            "effective_query": effective_query,
            "stage1_query_sent": stage1_effective_query,
            "input_audit": {
                "selection": selection,
                "stage1_research_brief": stage1_effective_research_brief,
                "market_facts_query_prefix": market_facts_query_prefix,
                "market_facts": market_facts or {},
                "injection_bundle_meta": injection_bundle_meta,
                "injection_bundle_context": injection_bundle_context,
            },
            "stage1_results": stage1_results,
            "stage1_results_for_stage3": stage1_results_for_stage3,
            "stage1_model_audit": stage1_model_audit,
            "stage2_results": stage2_results,
            "stage2_revision_results": stage2_revision_results,
            "stage2_revision_summary": stage2_revision_summary,
            "stage3_primary_model": primary_chairman_model,
            "stage3_secondary_model": secondary_chairman_model,
            "stage3_timing_seconds": {
                "primary": round(stage3_primary_elapsed, 3),
                "secondary": round(stage3_secondary_elapsed, 3)
                if stage3_secondary_elapsed is not None
                else None,
            },
            "stage3_result_primary": stage3_result_primary,
            "stage3_result_secondary": stage3_result_secondary,
            "stage3_comparison": stage3_comparison,
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
        help=(
            "Optional company/query hint for template/company detection. "
            "The rendered template Stage 1 prompt is used as the Stage 1 task prompt."
        ),
    )
    parser.add_argument(
        "--brief-include-rubric",
        type=str,
        choices=["auto", "always", "never"],
        default="auto",
        help=(
            "Control whether template rubric is included in Stage 1 research_brief. "
            "auto preserves existing behavior."
        ),
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
        "--secondary-chairman-model",
        type=str,
        default=None,
        help=(
            "Optional secondary chairman model for side-by-side Stage 3 output "
            "(e.g., openai/gpt-5.3)."
        ),
    )
    parser.add_argument(
        "--injection-bundle-json",
        type=str,
        default=None,
        help="Optional path to a prebuilt injection_bundle.json to pass into Stage 1 attachment context.",
    )
    parser.add_argument(
        "--injection-max-docs",
        type=int,
        default=0,
        help="Optional cap for docs from injection bundle (0=all in file order).",
    )
    parser.add_argument(
        "--injection-max-chars",
        type=int,
        default=0,
        help="Max chars from injection bundle context sent to Stage 1 attachment context (0=no truncation).",
    )
    parser.add_argument(
        "--dump-json",
        type=str,
        default=None,
        help="Optional path to write full JSON output.",
    )
    parser.add_argument(
        "--dry-run-input",
        action="store_true",
        help="Print exact Stage 1 prompt/payload and exit without API calls.",
    )
    parser.add_argument(
        "--stage1-only",
        action="store_true",
        help="Run only Stage 1 and exit (skips Stage 2/3).",
    )
    parser.add_argument(
        "--stage2-revision-pass",
        type=str,
        choices=["auto", "on", "off"],
        default="on",
        help=(
            "Run Stage 2.5 self-revision pass. "
            "on=enabled by default, off=disable, auto=use .env STAGE2_REVISION_PASS_ENABLED."
        ),
    )
    parser.add_argument(
        "--diagnostic-mode",
        action="store_true",
        help="Allow Stage 1 execution while SYSTEM_ENABLED=false (audit-only).",
    )
    return parser


if __name__ == "__main__":
    _ensure_pymupdf_runtime()
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))
