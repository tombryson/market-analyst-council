"""Stage-1 collection orchestration: the main Perplexity + OpenRouter pipeline.

Builds prompts, runs the multi-wave retrieval loop, applies the second-pass
citation-gate and truncation-repair, and returns per-model Stage-1 responses.
"""

import asyncio
import copy
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from html import unescape
from time import perf_counter
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

from ..config import (
    ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED,
    CHAIRMAN_MODEL,
    COUNCIL_MODELS,
    DETERMINISTIC_FINANCE_LANE_ENABLED,
    MAX_SOURCES,
    OPENROUTER_API_KEY,
    PERPLEXITY_API_KEY,
    PERPLEXITY_API_URL,
    PERPLEXITY_COUNCIL_MODELS,
    PERPLEXITY_PRESET_ADVANCED,
    PERPLEXITY_PRESET_DEEP,
    PERPLEXITY_PRESET_STRATEGY,
    PERPLEXITY_STAGE1_ATTACHMENT_CONTEXT_MAX_CHARS,
    PERPLEXITY_STAGE1_EXECUTION_MODE,
    PERPLEXITY_STAGE1_FACT_DIGEST_V2_ENABLED,
    PERPLEXITY_STAGE1_MAX_ATTEMPTS,
    PERPLEXITY_STAGE1_MAX_RETRIES,
    PERPLEXITY_STAGE1_MIXED_MODE_ENABLED,
    PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED,
    PERPLEXITY_STAGE1_MULTI_WAVE_ENABLED,
    PERPLEXITY_STAGE1_MULTI_WAVE_GAP_QUERY_LIMIT,
    PERPLEXITY_STAGE1_MULTI_WAVE_MAX_WAVES,
    PERPLEXITY_STAGE1_MULTI_WAVE_MIN_NEW_PRIMARY_SOURCES,
    PERPLEXITY_STAGE1_OPENROUTER_MODELS,
    PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS,
    PERPLEXITY_STAGE1_SECOND_PASS_APPENDIX_MAX_SOURCES,
    PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MAX_UNCITED_NUMERIC_LINES,
    PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MIN_COUNT,
    PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_CATASTROPHIC_SCORE,
    PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT,
    PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT,
    PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE,
    PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS,
    PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_PER_SOURCE,
    PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_WORDS_PER_SOURCE,
    PERPLEXITY_STAGE1_SECOND_PASS_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_OUTPUT_TOKENS,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES,
    PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_COMPRESSION_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_TARGET_CHARS,
    PERPLEXITY_STAGE1_SECOND_PASS_REASONING_EFFORT,
    PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS,
    PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS,
    PERPLEXITY_STAGE1_STAGGER_SECONDS,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_ENABLED,
    PERPLEXITY_STAGE1_TIMELINE_DIGEST_MAX_ITEMS,
    PERPLEXITY_STAGE1_TIMELINE_GUARD_ENABLED,
    PERPLEXITY_STAGE1_TIMELINE_GUARD_HARD_FAIL,
    PROGRESS_LOGGING,
    RESEARCH_DEPTH,
    STAGE1_CASHFLOW_CLASSIFIER_ENABLED,
    STAGE1_CASHFLOW_CLASSIFIER_MAX_OUTPUT_TOKENS,
    STAGE1_CASHFLOW_CLASSIFIER_MIN_CONFIDENCE_PCT,
    STAGE1_CASHFLOW_CLASSIFIER_MODEL,
    STAGE1_CASHFLOW_CLASSIFIER_REASONING_EFFORT,
    STAGE1_CASHFLOW_CLASSIFIER_TIMEOUT_SECONDS,
    STAGE1_TRUNCATION_CHECKER_ENABLED,
    STAGE1_TRUNCATION_CHECKER_MAX_OUTPUT_TOKENS,
    STAGE1_TRUNCATION_CHECKER_MIN_CONFIDENCE_PCT,
    STAGE1_TRUNCATION_CHECKER_MODEL,
    STAGE1_TRUNCATION_CHECKER_REASONING_EFFORT,
    STAGE1_TRUNCATION_CHECKER_TIMEOUT_SECONDS,
)
from ..openrouter import query_model, query_models_parallel
from ..reasoning import build_reasoning_payload, normalize_reasoning_effort
from ..source_fact_context import build_source_fact_context
from .asx_ingestion import (
    _augment_run_with_deterministic_asx_sources,
    _extract_normalized_facts_from_query_text,
)
from .fact_digest import _build_stage1_fact_digest_v2, _normalize_fact_key
from .perplexity_client import (
    _dedupe_model_ids,
    _evaluate_stage1_sonar_telemetry,
    _is_openrouter_compatible_model,
    _is_sonar_model,
    _normalize_perplexity_model_id,
    _probe_perplexity_model_support,
    _query_model_via_perplexity,
    _select_shared_retrieval_model,
    _supports_perplexity_reasoning_payload,
)
from .source_analysis import (
    _excerpt_material_signal_score,
    _extract_source_sentences,
    _extract_timeline_windows,
    _is_heading_like_sentence,
    _is_low_signal_legal_boilerplate,
    _is_low_signal_notice_source_item,
    _source_authority_rank,
    _window_to_quarter_index,
)
from .stage1_attempt import (
    _build_stage1_attempt_profile,
    _build_strict_research_brief,
    _ensure_system_enabled,
    _evaluate_stage1_template_compliance,
    _expected_domains_for_exchange,
    _extract_status_code,
    _extract_synthesis_block,
    _has_expected_source_domain,
    _infer_exchange_from_ticker,
    _is_gpt_5_4_model,
    _is_retryable_stage1_error,
    _progress_log,
    _resolve_stage1_preset_for_attempt,
    _stage1_requires_template_compliance,
)
from .stage1_multi_wave import (
    _build_stage1_gap_query_block,
    _build_stage1_research_planner,
    _build_stage1_verification_profile,
    _count_new_primary_sources,
    _default_stage1_verification_profile,
    _evaluate_stage1_section_coverage,
    _merge_stage1_wave_runs,
    _normalize_terms_list,
)
from .stage1_xai_lane import _collect_stage1_supplementary_macro_news
from .stage2 import _coerce_bool, _coerce_float, _parse_json_object_from_text
from ..config import ASX_DETERMINISTIC_FETCH_TIMEOUT_SECONDS, ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL, ASX_DETERMINISTIC_LOOKBACK_YEARS, ASX_DETERMINISTIC_MAX_DECODE, ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY, ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS, PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_FACTS_PER_SECTION, PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_NARRATIVE_WORDS, PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_SUMMARY_BULLETS, PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN, PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS, PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED, PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES, PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS, PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT, PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED, PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED, PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_RECENCY_DAYS, PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_SOURCES, PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_RETRIEVAL_MAX_SOURCES, PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED, STAGE1_CASHFLOW_DETECTION_MAX_SOURCES
from .perplexity_client import _FACT_DIGEST_V2_KEYWORDS, _FACT_DIGEST_V2_NARRATIVE_ORDER, _FACT_PACK_KEYWORDS, _FACT_PACK_SECTIONS, _STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS, _STAGE1_DEFAULT_TIMELINE_TERMS, _STAGE1_SECOND_PASS_MIN_RESPONSE_CHARS

logger = logging.getLogger(__name__)

def _prepare_stage1_source_rows(
    run: Dict[str, Any],
    max_sources: int,
    max_chars_per_source: int,
) -> List[Dict[str, Any]]:
    """Normalize top retrieved sources into reusable rows with stable source IDs."""
    safe_max_sources = max(1, int(max_sources))
    safe_max_chars = max(300, int(max_chars_per_source))
    rows: List[Dict[str, Any]] = []

    all_sources = list(run.get("results") or [])
    preferred_sources = [
        source
        for source in all_sources
        if not _is_low_signal_notice_source_item(source)
    ]
    ordered_sources = preferred_sources
    current_year = datetime.utcnow().year

    for source in ordered_sources:
        if len(rows) >= safe_max_sources:
            break

        source_id = f"S{len(rows) + 1}"
        title = str(source.get("title", "Untitled")).strip() or "Untitled"
        url = str(source.get("url", "")).strip()
        published = str(source.get("published_at", "")).strip()
        source_year = _infer_source_year(published, title, url)
        decode_status = str(source.get("decode_status", "")).strip()
        decoded = bool(decode_status == "decoded" or source.get("decoded_excerpt"))

        excerpt = str(
            source.get("decoded_excerpt")
            or source.get("content")
            or source.get("source_snippet")
            or ""
        ).strip()
        if not excerpt:
            continue

        # Hard gate: avoid link-only/title-only source rows. Stage 1 models need
        # quote-bearing evidence text, not URL metadata.
        extracted_sentences = _extract_source_sentences(excerpt)
        if not extracted_sentences:
            normalized_excerpt = re.sub(r"\s+", " ", excerpt).strip()
            low_excerpt = normalized_excerpt.lower()
            strong_tokens = (
                "npv",
                "irr",
                "aisc",
                "capex",
                "resource",
                "reserve",
                "production",
                "funding",
                "facility",
                "cash",
                "debt",
                "market cap",
                "shares",
                "enterprise value",
                "timeline",
                "milestone",
                "commissioning",
                "ramp-up",
            )
            has_min_signal = bool(
                len(normalized_excerpt) >= 180
                and re.search(r"\d", low_excerpt)
                and any(token in low_excerpt for token in strong_tokens)
            )
            if not has_min_signal:
                continue
        material_signal_score = _excerpt_material_signal_score(excerpt)
        source_is_low_signal = _is_low_signal_notice_source_item(source)
        if source_is_low_signal and material_signal_score < 2:
            continue
        if material_signal_score < 0 and len(rows) >= max(2, safe_max_sources - 2):
            continue
        if material_signal_score < 2 and len(rows) >= max(3, safe_max_sources - 3):
            continue
        if (
            source_year is not None
            and source_year <= (current_year - 3)
            and len(rows) >= max(3, safe_max_sources - 3)
        ):
            continue
        if len(excerpt) > safe_max_chars:
            excerpt = excerpt[: safe_max_chars - 3].rstrip() + "..."

        rows.append(
            {
                "source_id": source_id,
                "title": title,
                "url": url,
                "published_at": published,
                "decode_status": decode_status,
                "decoded": decoded,
                "excerpt": excerpt,
                "material_signal_score": material_signal_score,
            }
        )

    return rows


def _infer_reporting_period_key(*, title: str, excerpt: str, published_at: str) -> Optional[str]:
    """Infer a reporting-period key (e.g., 2025Q4, 2025FY, 2025H2) from source metadata."""
    text = f"{title}\n{excerpt}".lower()
    year_match = re.search(r"\b(20\d{2})\b", text)
    year: Optional[int] = int(year_match.group(1)) if year_match else None
    if year is None:
        published_match = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(published_at or "").strip())
        if published_match:
            year = int(published_match.group(1))
            month = int(published_match.group(2))
        else:
            month = None
    else:
        month = None

    quarter_match = re.search(r"\bq([1-4])\b", text)
    if quarter_match and year is not None:
        return f"{year}Q{quarter_match.group(1)}"

    month_map = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    if month is None:
        month_token = re.search(
            r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
            r"jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|oct(?:ober)?|nov(?:ember)?|"
            r"dec(?:ember)?)\b",
            text,
        )
        if month_token:
            month = month_map.get(month_token.group(1), None)

    if any(token in text for token in ("half-year", "half year", "interim", "h1", "h2")):
        if year is not None:
            if "h1" in text:
                return f"{year}H1"
            if "h2" in text:
                return f"{year}H2"
            if month is not None:
                return f"{year}H1" if month <= 6 else f"{year}H2"
            return f"{year}H?"

    if any(token in text for token in ("annual report", "full year", "fy", "10-k")):
        if year is not None:
            return f"{year}FY"

    if any(token in text for token in ("quarterly", "quarter", "appendix 5b", "10-q", "cashflow report")):
        if year is not None:
            if month is not None:
                quarter = ((month - 1) // 3) + 1
                return f"{year}Q{quarter}"
            return f"{year}Q?"

    if year is not None:
        return str(year)
    return None


def _detect_cashflow_schema_activation(
    *,
    source_rows: List[Dict[str, Any]],
    mode: str,
    min_reporting_periods: int,
    require_operating_cashflow: bool,
) -> Dict[str, Any]:
    """
    Determine whether cashflow schema should be enforced for Stage 1 output.

    Modes:
    - disabled: never enforce
    - required: always enforce
    - auto: enforce only when source evidence indicates operating-period cashflow reporting
    """
    normalized_mode = str(mode or "disabled").strip().lower()
    if normalized_mode not in {"disabled", "auto", "required"}:
        normalized_mode = "disabled"

    if normalized_mode == "disabled":
        return {
            "active": False,
            "reason": "mode_disabled",
            "mode": normalized_mode,
            "periods_detected": 0,
            "reporting_period_keys_detected": [],
            "rows_with_cashflow_terms": 0,
            "rows_with_operating_cashflow_terms": 0,
            "rows_with_forward_guidance_terms": 0,
            "rows_with_reporting_terms": 0,
        }
    if normalized_mode == "required":
        return {
            "active": True,
            "reason": "mode_required",
            "mode": normalized_mode,
            "periods_detected": 0,
            "reporting_period_keys_detected": [],
            "rows_with_cashflow_terms": 0,
            "rows_with_operating_cashflow_terms": 0,
            "rows_with_forward_guidance_terms": 0,
            "rows_with_reporting_terms": 0,
        }

    periods = set()
    reporting_period_keys = set()
    rows_with_cashflow_terms = 0
    rows_with_operating_cashflow_terms = 0
    rows_with_forward_guidance_terms = 0
    rows_with_reporting_terms = 0

    cashflow_terms = (
        "cashflow",
        "cash flow",
        "operating cash",
        "free cash flow",
        "fcf",
        "ocf",
        "cash receipts",
        "appendix 5b",
        "10-q",
        "10-k",
    )
    operating_cashflow_terms = (
        "operating cash flow",
        "net operating cash flow",
        "cash from operations",
        "ocf",
    )
    forward_terms = (
        "guidance",
        "forecast",
        "target",
        "outlook",
        "fy20",
        "2026",
        "2027",
        "2028",
        "2029",
        "12m",
        "24m",
    )
    reporting_terms = (
        "quarterly",
        "quarterly activities",
        "cashflow report",
        "cash flow report",
        "appendix 5b",
        "annual report",
        "half-year",
        "half year",
        "interim report",
        "10-q",
        "10-k",
        "form 10-q",
        "form 10-k",
        "results",
    )

    for row in (source_rows or []):
        if not isinstance(row, dict):
            continue
        published = str(row.get("published_at", "")).strip()
        m = re.match(r"(\d{4})[-/]", published)
        if m:
            periods.add(m.group(1))
        title = str(row.get("title", "")).strip().lower()
        excerpt = str(row.get("excerpt", "")).strip().lower()
        blob = f"{title}\n{excerpt}"
        has_reporting_term = any(term in blob for term in reporting_terms)
        if has_reporting_term:
            rows_with_reporting_terms += 1
        if any(term in blob for term in cashflow_terms):
            rows_with_cashflow_terms += 1
        if any(term in blob for term in operating_cashflow_terms):
            rows_with_operating_cashflow_terms += 1
        if any(term in blob for term in forward_terms):
            rows_with_forward_guidance_terms += 1
        if has_reporting_term or any(term in blob for term in cashflow_terms):
            period_key = _infer_reporting_period_key(
                title=str(row.get("title", "")),
                excerpt=str(row.get("excerpt", "")),
                published_at=published,
            )
            if period_key:
                reporting_period_keys.add(period_key)

    min_periods = max(1, int(min_reporting_periods))
    periods_detected = max(len(reporting_period_keys), len(periods))
    period_gate = periods_detected >= min_periods
    cashflow_gate = rows_with_cashflow_terms >= 2
    operating_gate = (
        rows_with_operating_cashflow_terms >= 1 if require_operating_cashflow else True
    )
    guidance_gate = rows_with_forward_guidance_terms >= 1

    active = bool(period_gate and cashflow_gate and operating_gate and guidance_gate)
    reason_parts = []
    if period_gate:
        reason_parts.append("periods_ok")
    else:
        reason_parts.append("periods_insufficient")
    if cashflow_gate:
        reason_parts.append("cashflow_terms_ok")
    else:
        reason_parts.append("cashflow_terms_insufficient")
    if operating_gate:
        reason_parts.append("operating_cashflow_ok")
    else:
        reason_parts.append("operating_cashflow_missing")
    if guidance_gate:
        reason_parts.append("forward_terms_ok")
    else:
        reason_parts.append("forward_terms_missing")

    return {
        "active": active,
        "reason": ",".join(reason_parts),
        "mode": normalized_mode,
        "periods_detected": periods_detected,
        "reporting_period_keys_detected": sorted(reporting_period_keys),
        "rows_with_cashflow_terms": rows_with_cashflow_terms,
        "rows_with_operating_cashflow_terms": rows_with_operating_cashflow_terms,
        "rows_with_forward_guidance_terms": rows_with_forward_guidance_terms,
        "rows_with_reporting_terms": rows_with_reporting_terms,
    }


def _build_cashflow_schema_contract_text() -> str:
    """Additional mandatory section contract for cashflow-capable operating businesses."""
    return (
        "9) Cashflow Analysis (Historical / Current / Forward)\n"
        "- Historical (minimum 3 reported periods): include Revenue, Operating Cash Flow, Capex, and Free Cash Flow where disclosed.\n"
        "- Current period/run-rate: state latest reported cash, debt, and run-rate operating cash generation.\n"
        "- Forward (12m and 24m): provide base/bull/bear cashflow bridge with explicit assumptions and key sensitivities.\n"
        "- Each period assumption must be source-backed with [S#] or marked ESTIMATE with one-line rationale."
    )


async def _classify_cashflow_schema_with_agent(
    *,
    source_rows: List[Dict[str, Any]],
    template_id: str,
    mode: str,
    min_reporting_periods: int,
    require_operating_cashflow: bool,
) -> Dict[str, Any]:
    """Run a low-cost classifier agent to decide if cashflow schema should be active."""
    if not STAGE1_CASHFLOW_CLASSIFIER_ENABLED:
        return {"used": False, "reason": "classifier_disabled"}
    if str(mode or "").strip().lower() != "auto":
        return {"used": False, "reason": "mode_not_auto"}
    if not OPENROUTER_API_KEY:
        return {"used": False, "reason": "missing_openrouter_key"}
    model = str(STAGE1_CASHFLOW_CLASSIFIER_MODEL or "").strip()
    if not model:
        return {"used": False, "reason": "missing_classifier_model"}

    rows = []
    for row in (source_rows or [])[:20]:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "source_id": str(row.get("source_id", "")),
                "published_at": str(row.get("published_at", "")),
                "title": _truncate_text_for_prompt(str(row.get("title", "")), 180),
                "excerpt": _truncate_text_for_prompt(str(row.get("excerpt", "")), 280),
            }
        )
    payload = {
        "template_id": str(template_id or ""),
        "mode": "auto",
        "min_reporting_periods": int(max(1, min_reporting_periods)),
        "require_operating_cashflow": bool(require_operating_cashflow),
        "sources": rows,
    }
    prompt = (
        "Classify whether this company should include a dedicated cashflow-analysis section "
        "(historical/current/forward) in Stage-1 investment analysis.\n"
        "Decision rules:\n"
        "1) ACTIVE=true only if evidence supports a cashflow-capable operating business.\n"
        "2) Require reported-period evidence (quarterly/half-year/annual or 10-Q/10-K style reporting) "
        "and operating-cashflow signal.\n"
        "3) If evidence is weak/ambiguous, set ACTIVE=false.\n\n"
        "Return JSON only with this exact shape:\n"
        "{"
        "\"active\": <bool>, "
        "\"confidence_pct\": <0-100 number>, "
        "\"reason\": \"<short reason>\", "
        "\"periods_detected_estimate\": <int>, "
        "\"evidence\": [\"<max 3 short bullets>\"]"
        "}\n\n"
        "Input JSON:\n"
        f"{json.dumps(payload, ensure_ascii=True, separators=(',', ':'))}"
    )
    response = await query_model(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        timeout=float(max(10.0, STAGE1_CASHFLOW_CLASSIFIER_TIMEOUT_SECONDS)),
        max_tokens=int(max(120, STAGE1_CASHFLOW_CLASSIFIER_MAX_OUTPUT_TOKENS)),
        reasoning_effort=str(STAGE1_CASHFLOW_CLASSIFIER_REASONING_EFFORT or "low"),
    )
    if not response:
        return {
            "used": False,
            "reason": "classifier_no_response",
            "model": model,
        }
    raw = str(response.get("content", "") or "")
    parsed, parse_error = _parse_json_object_from_text(raw)
    if not parsed:
        return {
            "used": False,
            "reason": f"classifier_parse_failed:{parse_error or 'unknown'}",
            "model": model,
            "raw_preview": _truncate_text_for_prompt(raw, 240),
        }
    active = _coerce_bool(parsed.get("active"))
    if active is None:
        return {
            "used": False,
            "reason": "classifier_missing_active",
            "model": model,
            "raw_preview": _truncate_text_for_prompt(raw, 240),
        }
    confidence_raw = parsed.get("confidence_pct", 0)
    try:
        confidence = float(confidence_raw)
    except Exception:
        confidence = 0.0
    periods_estimate_raw = parsed.get("periods_detected_estimate", 0)
    try:
        periods_estimate = int(periods_estimate_raw)
    except Exception:
        periods_estimate = 0
    evidence = []
    if isinstance(parsed.get("evidence"), list):
        for item in parsed.get("evidence", [])[:3]:
            snippet = _truncate_text_for_prompt(str(item or ""), 120)
            if snippet:
                evidence.append(snippet)
    return {
        "used": True,
        "model": model,
        "active": bool(active),
        "confidence_pct": max(0.0, min(100.0, confidence)),
        "reason": _truncate_text_for_prompt(str(parsed.get("reason", "")), 180),
        "periods_detected_estimate": max(0, periods_estimate),
        "evidence": evidence,
    }


def _infer_source_year(published: str, title: str, url: str) -> Optional[int]:
    """Infer best year for staleness filtering from metadata/title/url."""
    candidates = [str(published or ""), str(title or ""), str(url or "")]
    for text in candidates:
        for match in re.findall(r"(20\d{2})", text):
            try:
                year = int(match)
            except Exception:
                continue
            if 2000 <= year <= datetime.utcnow().year:
                return year
    return None


def _classify_fact_pack_section(sentence: str) -> str:
    """Assign sentence to best-fit rubric section via keyword scoring."""
    text = sentence.lower()
    best_section = "other_material_facts"
    best_score = 0

    for section, keywords in _FACT_PACK_KEYWORDS.items():
        score = sum(1 for token in keywords if token in text)
        if score > best_score:
            best_score = score
            best_section = section

    return best_section


def _build_stage1_rubric_fact_pack(
    source_rows: List[Dict[str, Any]],
    max_facts_per_section: int = 4,
) -> Dict[str, Any]:
    """Build rubric-aligned fact pack from decoded source rows."""
    sections: Dict[str, List[Dict[str, str]]] = {key: [] for key in _FACT_PACK_SECTIONS}
    seen_facts = set()
    safe_limit = max(2, int(max_facts_per_section))

    for row in source_rows:
        source_id = str(row.get("source_id", "S?"))
        excerpt = str(row.get("excerpt", ""))

        for sentence in _extract_source_sentences(excerpt):
            sentence_lower = sentence.lower()
            if sentence_lower in seen_facts:
                continue

            # Keep only materially useful sentences for financial rubric execution.
            signal_tokens = (
                "npv",
                "irr",
                "aisc",
                "capex",
                "resource",
                "reserve",
                "grade",
                "production",
                "gold",
                "funding",
                "facility",
                "debt",
                "cash",
                "market cap",
                "shares",
                "first gold",
                "milestone",
                "risk",
                "tailwind",
                "headwind",
                "valuation",
                "ev/oz",
            )
            if not re.search(r"\d", sentence_lower) and not any(
                token in sentence_lower for token in signal_tokens
            ):
                continue

            section = _classify_fact_pack_section(sentence)
            bucket = sections.get(section, [])
            if len(bucket) >= safe_limit:
                continue

            bucket.append(
                {
                    "source_id": source_id,
                    "fact": sentence,
                }
            )
            seen_facts.add(sentence_lower)

    # Starvation fallback: if keyword extraction is sparse, keep a few high-signal
    # generic facts so second-pass analysis still has minimum context.
    if sum(len(items) for items in sections.values()) < 4:
        fallback_bucket = sections["other_material_facts"]
        fallback_limit = max(safe_limit * 2, 6)
        for row in source_rows:
            source_id = str(row.get("source_id", "S?"))
            excerpt = str(row.get("excerpt", ""))
            for sentence in _extract_source_sentences(excerpt)[:2]:
                sentence_lower = sentence.lower()
                if sentence_lower in seen_facts:
                    continue
                if len(fallback_bucket) >= fallback_limit:
                    break
                fallback_bucket.append(
                    {
                        "source_id": source_id,
                        "fact": sentence,
                    }
                )
                seen_facts.add(sentence_lower)
            if len(fallback_bucket) >= fallback_limit:
                break

    compact_sections = {
        name: items
        for name, items in sections.items()
        if items
    }
    total_facts = sum(len(items) for items in compact_sections.values())
    sections_with_facts = list(compact_sections.keys())
    critical = [
        "market_data",
        "project_economics_npv_inputs",
        "resource_and_reserve",
        "development_timeline_and_milestones",
    ]
    critical_gaps = [
        f"Missing evidence for section: {name}"
        for name in critical
        if not sections.get(name)
    ]

    source_index = [
        {
            "source_id": row.get("source_id", ""),
            "title": row.get("title", ""),
            "url": row.get("url", ""),
            "published_at": row.get("published_at", ""),
            "decoded": bool(row.get("decoded")),
        }
        for row in source_rows
    ]

    return {
        "schema": "rubric_fact_pack_v1",
        "source_index": source_index,
        "sections": compact_sections,
        "critical_gaps": critical_gaps,
        "counts": {
            "source_count": len(source_rows),
            "decoded_source_count": sum(1 for row in source_rows if row.get("decoded")),
            "total_facts": total_facts,
            "sections_with_facts": len(sections_with_facts),
        },
    }


def _map_to_compact_fact_category(section_name: str) -> str:
    """Map dense fact-pack/fact-digest section ids into compact categories."""
    key = str(section_name or "").strip().lower()
    if any(token in key for token in ("timeline", "milestone", "deadline")):
        return "timeline_milestones"
    if any(token in key for token in ("market", "share", "valuation")):
        return "market_share_structure"
    if any(token in key for token in ("economics", "npv", "cost", "resource", "reserve")):
        return "project_economics_resource"
    if any(token in key for token in ("funding", "financing", "balance", "debt", "cash")):
        return "funding_and_balance_sheet"
    if any(token in key for token in ("risk", "constraint", "headwind")):
        return "risks_and_constraints"
    if any(token in key for token in ("tailwind", "catalyst", "upside")):
        return "catalysts_and_tailwinds"
    if any(token in key for token in ("management", "governance")):
        return "management_and_governance"
    return "other_material_facts"


def _build_stage1_compact_fact_bundle(
    *,
    source_rows: List[Dict[str, Any]],
    fact_digest: Dict[str, Any],
    fact_pack: Dict[str, Any],
    timeline_rows: List[Dict[str, Any]],
    max_facts_per_category: int = 3,
) -> Dict[str, Any]:
    """
    Build compact denoised fact bundle injected before Stage 1 model analysis.

    Keeps only high-signal claim rows with source ids and dates so prompt size
    stays bounded while preserving evidence traceability.
    """
    safe_limit = max(1, int(max_facts_per_category))
    categories: Dict[str, List[Dict[str, str]]] = {
        "timeline_milestones": [],
        "project_economics_resource": [],
        "funding_and_balance_sheet": [],
        "market_share_structure": [],
        "risks_and_constraints": [],
        "catalysts_and_tailwinds": [],
        "management_and_governance": [],
        "other_material_facts": [],
    }
    seen = set()

    def _timeline_priority(text: str) -> int:
        low = str(text or "").lower()
        if any(token in low for token in ("first gold", "gold pour")):
            return 4
        if any(token in low for token in ("commercial production", "ramp-up", "ramp up")):
            return 3
        if re.search(r"\bq[1-4]\b", low) or re.search(r"\b20\d{2}\b", low):
            return 2
        if "timeline" in low or "milestone" in low:
            return 1
        return 0

    def _add_row(category: str, source_id: str, fact: str, published_at: str = "") -> None:
        bucket = categories.setdefault(category, [])
        clean_fact = re.sub(r"\s+", " ", str(fact or "")).strip()
        if not clean_fact:
            return
        if _is_low_signal_legal_boilerplate(clean_fact):
            return
        if _is_heading_like_sentence(clean_fact):
            return
        if len(clean_fact) > 420:
            clean_fact = clean_fact[:417].rstrip() + "..."
        key = f"{category}|{source_id}|{clean_fact.lower()}"
        if key in seen:
            return
        row = {
            "source_id": str(source_id or "").strip() or "S?",
            "fact": clean_fact,
        }
        date_value = str(published_at or "").strip()
        if date_value:
            row["published_at"] = date_value

        if len(bucket) >= safe_limit:
            if category != "timeline_milestones":
                return
            new_priority = _timeline_priority(clean_fact)
            if new_priority <= 0:
                return
            worst_idx = -1
            worst_priority = 10
            for idx, existing in enumerate(bucket):
                existing_priority = _timeline_priority(str(existing.get("fact", "")))
                if existing_priority < worst_priority:
                    worst_priority = existing_priority
                    worst_idx = idx
            if worst_idx < 0 or new_priority <= worst_priority:
                return
            # Replace low-priority timeline line with higher-priority milestone.
            bucket[worst_idx] = row
            seen.add(key)
            return

        seen.add(key)
        bucket.append(row)

    pack_sections = (fact_pack.get("sections", {}) or {}) if isinstance(fact_pack, dict) else {}
    if isinstance(pack_sections, dict):
        for section_name, rows in pack_sections.items():
            category = _map_to_compact_fact_category(section_name)
            for row in (rows or []):
                if not isinstance(row, dict):
                    continue
                _add_row(
                    category,
                    str(row.get("source_id", "S?")),
                    str(row.get("fact", "")),
                    str(row.get("published_at", "")),
                )

    digest_sections = (fact_digest.get("sections", {}) or {}) if isinstance(fact_digest, dict) else {}
    if isinstance(digest_sections, dict):
        for section_name, rows in digest_sections.items():
            category = _map_to_compact_fact_category(section_name)
            for row in (rows or []):
                if not isinstance(row, dict):
                    continue
                _add_row(
                    category,
                    str(row.get("source_id", "S?")),
                    str(row.get("fact", "")),
                    str(row.get("published_at", "")),
                )

    for row in timeline_rows[: max(3, safe_limit + 1)]:
        if not isinstance(row, dict):
            continue
        _add_row(
            "timeline_milestones",
            str(row.get("source_id", "S?")),
            str(row.get("fact", "")),
            str(row.get("published_at", "")),
        )

    # Starvation fallback: if upstream extraction is sparse, pull compact
    # summary bullets and one high-signal sentence per source so Stage 1
    # never receives an empty/near-empty denoised bundle.
    current_total = sum(len(rows) for rows in categories.values())
    if current_total < 5:
        for bullet in (fact_digest.get("summary_bullets", []) or [])[:8]:
            text = re.sub(r"\s+", " ", str(bullet or "")).strip()
            if not text:
                continue
            source_id = "S?"
            source_match = re.match(r"^\[(S\d+)\]\s*(.*)$", text)
            if source_match:
                source_id = source_match.group(1)
                text = source_match.group(2).strip()
            category = _map_to_compact_fact_category(text)
            _add_row(category, source_id, text, "")

    current_total = sum(len(rows) for rows in categories.values())
    if current_total < 5:
        for row in source_rows:
            source_id = str(row.get("source_id", "S?"))
            published = str(row.get("published_at", ""))
            excerpt = str(row.get("excerpt", ""))
            for sentence in _extract_source_sentences(excerpt)[:1]:
                category = _map_to_compact_fact_category(sentence)
                _add_row(category, source_id, sentence, published)
            if sum(len(rows) for rows in categories.values()) >= 6:
                break

    compact_categories = {
        key: rows
        for key, rows in categories.items()
        if rows
    }
    total_facts = sum(len(rows) for rows in compact_categories.values())

    source_index = []
    for row in source_rows:
        source_index.append(
            {
                "source_id": str(row.get("source_id", "")),
                "title": str(row.get("title", "")),
                "url": str(row.get("url", "")),
                "published_at": str(row.get("published_at", "")),
            }
        )

    critical_gaps = []
    if isinstance(fact_pack, dict):
        critical_gaps = list(fact_pack.get("critical_gaps", []) or [])

    return {
        "schema": "compact_fact_bundle_v1",
        "source_index": source_index,
        "categories": compact_categories,
        "critical_gaps": critical_gaps,
        "counts": {
            "source_count": len(source_rows),
            "decoded_source_count": sum(1 for row in source_rows if row.get("decoded")),
            "categories_with_facts": len(compact_categories),
            "total_facts": total_facts,
        },
    }


def _truncate_text_for_prompt(text: str, max_chars: int) -> str:
    """Trim text for prompt payloads while preserving sentence readability."""
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    safe_max = max(60, int(max_chars))
    if len(value) <= safe_max:
        return value
    clipped = value[:safe_max].rstrip()
    # Try to avoid chopping at mid-token when possible.
    boundary = max(clipped.rfind("."), clipped.rfind(";"), clipped.rfind(","), clipped.rfind(" "))
    if boundary >= max(40, safe_max // 2):
        clipped = clipped[:boundary].rstrip()
    return clipped + "..."


def _count_words(text: str) -> int:
    """Approximate word count for prompt budget controls."""
    return len(re.findall(r"\b[\w\-]+\b", str(text or "")))


def _compact_prompt_fact_row(item: Dict[str, Any], max_fact_chars: int) -> Dict[str, Any]:
    """Keep only high-signal fields from a fact row for prompt payloads."""
    if not isinstance(item, dict):
        return {}
    out: Dict[str, Any] = {}
    source_id = str(item.get("source_id", "")).strip()
    if source_id:
        out["source_id"] = source_id
    published = str(item.get("published_at", "")).strip()
    if published:
        out["published_at"] = published
    fact = _truncate_text_for_prompt(str(item.get("fact", "")), max_fact_chars)
    if fact:
        out["fact"] = fact
    windows = item.get("windows")
    if isinstance(windows, list) and windows:
        normalized = []
        for raw in windows:
            token = str(raw or "").strip()
            if token and token not in normalized:
                normalized.append(token)
        if normalized:
            out["windows"] = normalized[:2]
    return out


_MANDATORY_FACT_FAMILY_KEYWORDS = {
    "market_structure": (
        "market cap",
        "enterprise value",
        "shares outstanding",
        "share price",
        "cash",
        "debt",
    ),
    "project_economics": (
        "npv",
        "irr",
        "capex",
        "opex",
        "aisc",
        "c1",
        "payback",
        "mine life",
        "pfs",
        "dfs",
        "scoping study",
    ),
    "resource_reserve": (
        "resource",
        "reserve",
        "jorc",
        "mineral resource",
        "ore reserve",
        "measured",
        "indicated",
        "inferred",
        "probable",
        "proved",
        "treo",
        "mreo",
        "ppm",
        "mt @",
    ),
    "metallurgy_recovery": (
        "recovery",
        "recoveries",
        "metallurgical",
        "metallurgy",
        "leach",
        "flotation",
        "mrec",
        "testwork",
        "ansto",
        "flowsheet",
    ),
    "funding_balance_sheet": (
        "funding",
        "financing",
        "placement",
        "cash",
        "debt",
        "facility",
        "loan",
        "liquidity",
        "runway",
        "eca",
    ),
    "permitting_regulatory": (
        "permit",
        "permitting",
        "licence",
        "license",
        "approval",
        "regulatory",
        "environmental",
        "ministry",
        "feam",
        "concession",
    ),
    "timeline_milestones": (
        "timeline",
        "target",
        "targeted",
        "on track",
        "milestone",
        "q1",
        "q2",
        "q3",
        "q4",
        "h1",
        "h2",
        "fid",
        "commissioning",
        "first production",
    ),
    "operations_production": (
        "production",
        "throughput",
        "plant",
        "commissioning",
        "operations",
        "run-rate",
        "guidance",
        "volume",
    ),
    "offtake_commercial": (
        "offtake",
        "customer",
        "contract",
        "agreement",
        "mou",
        "sales",
        "qualification",
    ),
}

_MANDATORY_FACT_TEMPLATE_FAMILIES = {
    "rare_earths_critical_minerals": {
        "project_economics",
        "resource_reserve",
        "metallurgy_recovery",
        "funding_balance_sheet",
        "permitting_regulatory",
        "timeline_milestones",
        "operations_production",
        "offtake_commercial",
    },
    "energy_oil_gas": {
        "project_economics",
        "resource_reserve",
        "funding_balance_sheet",
        "permitting_regulatory",
        "timeline_milestones",
        "operations_production",
        "offtake_commercial",
    },
}


def _mandatory_fact_families_for_template(template_id: str) -> List[str]:
    """Return source-backed fact families that must not be lost for a template."""
    template = str(template_id or "").strip().lower()
    families = set(_MANDATORY_FACT_TEMPLATE_FAMILIES.get(template, set()))
    if not families:
        if (
            template.startswith("resources_")
            or "miner" in template
            or "mineral" in template
            or template in {"base_metals", "coal", "iron_ore"}
        ):
            families.update(
                {
                    "project_economics",
                    "resource_reserve",
                    "metallurgy_recovery",
                    "funding_balance_sheet",
                    "permitting_regulatory",
                    "timeline_milestones",
                    "operations_production",
                    "offtake_commercial",
                }
            )
        elif template:
            families.update(
                {
                    "market_structure",
                    "project_economics",
                    "funding_balance_sheet",
                    "timeline_milestones",
                    "operations_production",
                    "offtake_commercial",
                }
            )
    if not families:
        families.update(
            {
                "market_structure",
                "project_economics",
                "funding_balance_sheet",
                "timeline_milestones",
            }
        )
    ordered = [
        "market_structure",
        "project_economics",
        "resource_reserve",
        "metallurgy_recovery",
        "funding_balance_sheet",
        "permitting_regulatory",
        "timeline_milestones",
        "operations_production",
        "offtake_commercial",
    ]
    return [family for family in ordered if family in families]


def _classify_mandatory_fact_family(sentence: str) -> str:
    """Classify a source sentence into the strongest mandatory fact family."""
    text = str(sentence or "").lower()
    best_family = ""
    best_score = 0
    for family, keywords in _MANDATORY_FACT_FAMILY_KEYWORDS.items():
        score = sum(1 for token in keywords if token in text)
        if family == "resource_reserve" and re.search(
            r"\b\d[\d,]*(?:\.\d+)?\s*(?:mt|moz|ppm|g/t)\b", text
        ):
            score += 2
        if family == "metallurgy_recovery" and re.search(r"\b\d[\d,]*(?:\.\d+)?\s*%", text):
            score += 2
        if family == "project_economics" and re.search(r"\b(npv|irr|capex|opex|aisc)\b", text):
            score += 2
        if score > best_score:
            best_score = score
            best_family = family
    return best_family


def _score_mandatory_fact_candidate(sentence: str, family: str, source: Dict[str, Any]) -> int:
    """Rank source-backed facts by source quality and template relevance."""
    text = str(sentence or "")
    low = text.lower()
    score = int(_source_authority_rank(str(source.get("url", "")))) * 4
    score += int(source.get("material_signal_score", 0) or 0)
    if source.get("asx_price_sensitive"):
        score += 5
    if re.search(r"\d", text):
        score += 4
    if str(source.get("published_at", "")).startswith("2026-"):
        score += 3
    elif str(source.get("published_at", "")).startswith("2025-"):
        score += 2
    if family == "resource_reserve":
        if re.search(r"\b\d[\d,]*(?:\.\d+)?\s*mt\b", low):
            score += 10
        if "@" in text or "ppm" in low or "treo" in low or "mreo" in low:
            score += 8
        if "jorc" in low or "ore reserve" in low or "mineral resource" in low:
            score += 6
    elif family == "metallurgy_recovery":
        if "recovery" in low or "recoveries" in low:
            score += 8
        if "%" in text:
            score += 6
        if "metallurg" in low or "ansto" in low or "flowsheet" in low:
            score += 4
    elif family == "project_economics":
        if "npv" in low:
            score += 8
        if "irr" in low:
            score += 6
        if "capex" in low or "opex" in low or "aisc" in low:
            score += 4
    return score


def _build_stage1_mandatory_fact_ledger(
    source_rows: List[Dict[str, Any]],
    *,
    template_id: str = "",
    max_facts_per_family: int = 3,
    max_total_facts: int = 24,
) -> Dict[str, Any]:
    """Build a non-compressible, source-backed fact ledger for Stage 1 prompts."""
    required_families = _mandatory_fact_families_for_template(template_id)
    required_set = set(required_families)
    candidates: List[Tuple[int, str, Dict[str, Any]]] = []
    seen = set()

    for source in source_rows:
        if not isinstance(source, dict):
            continue
        source_id = str(source.get("source_id", "")).strip() or "S?"
        for sentence in _extract_source_sentences(str(source.get("excerpt", ""))):
            family = _classify_mandatory_fact_family(sentence)
            if not family or family not in required_set:
                continue
            clean_fact = _truncate_text_for_prompt(sentence, 420)
            key = _normalize_fact_key(f"{family}|{source_id}|{clean_fact}")
            if not key or key in seen:
                continue
            seen.add(key)
            row = {
                "family": family,
                "fact": clean_fact,
                "source_id": source_id,
                "source_title": _truncate_text_for_prompt(str(source.get("title", "")), 140),
                "published_at": str(source.get("published_at", "")),
                "source_url": str(source.get("url", "")),
                "mandatory": True,
            }
            score = _score_mandatory_fact_candidate(clean_fact, family, source)
            candidates.append((score, family, row))

    candidates.sort(
        key=lambda item: (
            item[0],
            -required_families.index(item[1]) if item[1] in required_families else -999,
        ),
        reverse=True,
    )

    selected: List[Dict[str, Any]] = []
    per_family: Dict[str, int] = {}
    for score, family, row in candidates:
        if len(selected) >= max(1, int(max_total_facts)):
            break
        if per_family.get(family, 0) >= max(1, int(max_facts_per_family)):
            continue
        per_family[family] = per_family.get(family, 0) + 1
        selected_row = {
            "fact_id": f"MF{len(selected) + 1}",
            "score": int(score),
            **row,
        }
        selected.append(selected_row)

    return {
        "schema": "stage1_mandatory_source_fact_ledger_v1",
        "template_id": str(template_id or ""),
        "required_families": required_families,
        "facts": selected,
        "counts": {
            "source_count": len(source_rows),
            "fact_count": len(selected),
            "families_present": len({str(row.get("family", "")) for row in selected}),
        },
        "instruction": (
            "These facts are non-compressible primary/prepass evidence. "
            "Do not state a ledger fact is absent, unknown, unavailable, or unverified."
        ),
    }


def _normalize_prompt_coverage_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


_PROMPT_UNICODE_ESCAPE_RE = re.compile(
    r"\\u([0-9a-fA-F]{4})|\\U([0-9a-fA-F]{8})"
)


def _decode_prompt_unicode_escapes(text: str) -> str:
    """Decode JSON-style unicode escapes for prompt coverage checks only."""

    def _replace(match: re.Match[str]) -> str:
        codepoint = match.group(1) or match.group(2)
        try:
            return chr(int(codepoint, 16))
        except (TypeError, ValueError):
            return match.group(0)

    return _PROMPT_UNICODE_ESCAPE_RE.sub(_replace, str(text or ""))


def _validate_stage1_prompt_mandatory_fact_coverage(
    prompt: str,
    ledger: Dict[str, Any],
) -> Dict[str, Any]:
    """Verify every mandatory ledger fact survived into the final prompt string."""
    facts = [
        row
        for row in (ledger.get("facts", []) or [])
        if isinstance(row, dict) and row.get("mandatory", True)
    ]
    if not facts:
        return {"passed": True, "mandatory_fact_count": 0, "missing_fact_ids": []}

    raw_prompt_text = str(prompt or "")
    decoded_prompt_text = _decode_prompt_unicode_escapes(raw_prompt_text)
    prompt_text = (
        raw_prompt_text
        if decoded_prompt_text == raw_prompt_text
        else f"{raw_prompt_text}\n{decoded_prompt_text}"
    )
    normalized_prompt = _normalize_prompt_coverage_text(prompt_text)
    prompt_numbers = {
        re.sub(r"[,\s]", "", item).lower()
        for item in re.findall(r"\d[\d,\s]*(?:\.\d+)?", prompt_text)
    }
    stopwords = {
        "about",
        "above",
        "after",
        "against",
        "company",
        "project",
        "source",
        "these",
        "those",
        "their",
        "there",
        "where",
        "which",
        "with",
    }
    missing: List[Dict[str, Any]] = []
    for row in facts:
        fact = _decode_prompt_unicode_escapes(str(row.get("fact", ""))).strip()
        if not fact:
            continue
        covered = fact in prompt_text
        if not covered:
            normalized_fact = _normalize_prompt_coverage_text(fact)
            covered = bool(normalized_fact and normalized_fact in normalized_prompt)
        if not covered:
            fact_numbers = [
                re.sub(r"[,\s]", "", item).lower()
                for item in re.findall(r"\d[\d,\s]*(?:\.\d+)?", fact)
            ]
            words = [
                token
                for token in re.findall(r"[a-z][a-z0-9]{4,}", fact.lower())
                if token not in stopwords
            ][:8]
            number_hits = sum(1 for token in fact_numbers[:5] if token in prompt_numbers)
            word_hits = sum(1 for token in words if token in normalized_prompt)
            required_number_hits = min(len(fact_numbers[:5]), 2)
            required_word_hits = min(max(2, len(words) // 2), len(words))
            covered = (
                number_hits >= required_number_hits
                and word_hits >= required_word_hits
                and (fact_numbers or words)
            )
        if not covered:
            missing.append(
                {
                    "fact_id": str(row.get("fact_id", "")),
                    "family": str(row.get("family", "")),
                    "source_id": str(row.get("source_id", "")),
                    "fact": fact[:240],
                }
            )

    return {
        "passed": not missing,
        "mandatory_fact_count": len(facts),
        "covered_fact_count": len(facts) - len(missing),
        "missing_fact_ids": [str(item.get("fact_id", "")) for item in missing],
        "missing_facts": missing,
    }


def _build_stage1_prompt_fact_digest(
    fact_digest: Dict[str, Any],
    *,
    max_rows_per_section: int,
    max_fact_chars: int,
    max_summary_bullets: int,
) -> Dict[str, Any]:
    """Compact fact digest payload for Stage 1 prompt injection."""
    if not isinstance(fact_digest, dict):
        return {}
    sections: Dict[str, List[Dict[str, Any]]] = {}
    for section_name, rows in (fact_digest.get("sections", {}) or {}).items():
        if not isinstance(rows, list):
            continue
        compact_rows: List[Dict[str, Any]] = []
        for row in rows[: max(1, int(max_rows_per_section))]:
            compact_row = _compact_prompt_fact_row(row, max_fact_chars)
            if compact_row:
                compact_rows.append(compact_row)
        if compact_rows:
            sections[str(section_name)] = compact_rows
    summary_bullets = [
        _truncate_text_for_prompt(str(item or ""), max_fact_chars)
        for item in (fact_digest.get("summary_bullets", []) or [])[: max(1, int(max_summary_bullets))]
        if str(item or "").strip()
    ]
    return {
        "schema": str(fact_digest.get("schema", "fact_digest_v2")),
        "counts": fact_digest.get("counts", {}) or {},
        "sections": sections,
        "summary_bullets": summary_bullets,
        "conflicts": list((fact_digest.get("conflicts", []) or [])[:4]),
    }


def _build_stage1_prompt_fact_pack(
    fact_pack: Dict[str, Any],
    *,
    max_rows_per_section: int,
    max_fact_chars: int,
) -> Dict[str, Any]:
    """Compact rubric fact-pack payload for Stage 1 prompt injection."""
    if not isinstance(fact_pack, dict):
        return {}
    sections: Dict[str, List[Dict[str, Any]]] = {}
    for section_name, rows in (fact_pack.get("sections", {}) or {}).items():
        if not isinstance(rows, list):
            continue
        compact_rows: List[Dict[str, Any]] = []
        for row in rows[: max(1, int(max_rows_per_section))]:
            compact_row = _compact_prompt_fact_row(row, max_fact_chars)
            if compact_row:
                compact_rows.append(compact_row)
        if compact_rows:
            sections[str(section_name)] = compact_rows
    return {
        "schema": str(fact_pack.get("schema", "rubric_fact_pack_v1")),
        "counts": fact_pack.get("counts", {}) or {},
        "critical_gaps": list((fact_pack.get("critical_gaps", []) or [])[:8]),
        "sections": sections,
    }


def _build_stage1_prompt_compact_fact_bundle(
    compact_fact_bundle: Dict[str, Any],
    *,
    max_rows_per_category: int,
    max_fact_chars: int,
) -> Dict[str, Any]:
    """Compact denoised bundle payload for Stage 1 prompt injection."""
    if not isinstance(compact_fact_bundle, dict):
        return {}
    source_index = []
    for row in (compact_fact_bundle.get("source_index", []) or []):
        if not isinstance(row, dict):
            continue
        source_index.append(
            {
                "source_id": str(row.get("source_id", "")),
                "title": _truncate_text_for_prompt(str(row.get("title", "")), 120),
                "published_at": str(row.get("published_at", "")),
                "url": str(row.get("url", "")),
            }
        )
    categories: Dict[str, List[Dict[str, Any]]] = {}
    for category_name, rows in (compact_fact_bundle.get("categories", {}) or {}).items():
        if not isinstance(rows, list):
            continue
        compact_rows: List[Dict[str, Any]] = []
        for row in rows[: max(1, int(max_rows_per_category))]:
            compact_row = _compact_prompt_fact_row(row, max_fact_chars)
            if compact_row:
                compact_rows.append(compact_row)
        if compact_rows:
            categories[str(category_name)] = compact_rows
    return {
        "schema": str(compact_fact_bundle.get("schema", "compact_fact_bundle_v1")),
        "source_index": source_index,
        "categories": categories,
        "critical_gaps": list((compact_fact_bundle.get("critical_gaps", []) or [])[:8]),
        "counts": compact_fact_bundle.get("counts", {}) or {},
    }


def _build_stage1_doc_key_points_bundle(
    source_rows: List[Dict[str, Any]],
    *,
    max_points_per_source: int,
    max_words_per_source: int,
    max_fact_chars: int,
) -> Dict[str, Any]:
    """Derive concise source key points for prompt-budget-safe evidence injection."""
    safe_points = max(2, int(max_points_per_source))
    safe_words = max(80, int(max_words_per_source))
    safe_chars = max(80, int(max_fact_chars))
    section_tags = {
        "market_data": "market",
        "project_economics_npv_inputs": "economics",
        "resource_and_reserve": "resource",
        "funding_and_balance_sheet": "funding",
        "development_timeline_and_milestones": "timeline",
        "headwinds_and_risks": "risk",
        "other_material_facts": "other",
    }
    sources: List[Dict[str, Any]] = []
    total_points = 0
    total_words = 0

    for row in source_rows:
        source_id = str(row.get("source_id", "")).strip() or "S?"
        excerpt = str(row.get("excerpt", "")).strip()
        sentences = _extract_source_sentences(excerpt)
        candidates: List[Tuple[int, str, str]] = []
        for sentence in sentences:
            section = _classify_fact_pack_section(sentence)
            score = _excerpt_material_signal_score(sentence)
            if _extract_timeline_windows(sentence):
                score += 3
            if re.search(r"\d", sentence):
                score += 1
            if section in {"project_economics_npv_inputs", "funding_and_balance_sheet"}:
                score += 1
            candidates.append((score, section, sentence))

        candidates.sort(key=lambda item: (item[0], len(item[2])), reverse=True)
        selected: List[Tuple[str, str]] = []
        used_sentences = set()
        used_sections = set()
        used_words = 0

        # Pass 1: section diversity.
        for score, section, sentence in candidates:
            if len(selected) >= safe_points:
                break
            if sentence in used_sentences or section in used_sections:
                continue
            sentence_words = _count_words(sentence)
            if used_words + sentence_words > safe_words and selected:
                continue
            selected.append((section, sentence))
            used_sentences.add(sentence)
            used_sections.add(section)
            used_words += sentence_words

        # Pass 2: fill remaining slots by score.
        for score, section, sentence in candidates:
            if len(selected) >= safe_points:
                break
            if sentence in used_sentences:
                continue
            sentence_words = _count_words(sentence)
            if used_words + sentence_words > safe_words and selected:
                continue
            selected.append((section, sentence))
            used_sentences.add(sentence)
            used_words += sentence_words

        if not selected and excerpt:
            fallback = _truncate_text_for_prompt(excerpt, safe_chars)
            if fallback:
                selected.append(("other_material_facts", fallback))
                used_words += _count_words(fallback)

        key_points: List[Dict[str, str]] = []
        for section, sentence in selected:
            key_points.append(
                {
                    "tag": section_tags.get(section, "other"),
                    "fact": _truncate_text_for_prompt(sentence, safe_chars),
                }
            )

        total_points += len(key_points)
        total_words += used_words
        sources.append(
            {
                "source_id": source_id,
                "title": _truncate_text_for_prompt(str(row.get("title", "")), 120),
                "published_at": str(row.get("published_at", "")),
                "url": str(row.get("url", "")),
                "key_points": key_points,
            }
        )

    return {
        "schema": "source_key_points_v1",
        "sources": sources,
        "counts": {
            "source_count": len(source_rows),
            "sources_with_points": sum(1 for item in sources if item.get("key_points")),
            "total_points": total_points,
            "total_words": total_words,
            "max_points_per_source": safe_points,
            "max_words_per_source": safe_words,
        },
    }


def _apply_doc_key_points_to_source_rows(
    source_rows: List[Dict[str, Any]],
    key_points_bundle: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Replace long excerpts with deterministic key-point bullets for appendix rendering."""
    points_by_source: Dict[str, List[Dict[str, str]]] = {}
    for item in (key_points_bundle.get("sources", []) or []):
        if not isinstance(item, dict):
            continue
        source_id = str(item.get("source_id", "")).strip()
        points = item.get("key_points", []) or []
        if source_id and isinstance(points, list):
            points_by_source[source_id] = points

    rewritten: List[Dict[str, Any]] = []
    for row in source_rows:
        source_id = str(row.get("source_id", "")).strip()
        points = points_by_source.get(source_id, [])
        if not points:
            rewritten.append(dict(row))
            continue
        lines = []
        for point in points:
            if not isinstance(point, dict):
                continue
            tag = str(point.get("tag", "")).strip()
            fact = str(point.get("fact", "")).strip()
            if not fact:
                continue
            prefix = f"[{tag}] " if tag else ""
            lines.append(f"- {prefix}{fact}")
        compact_excerpt = "\n".join(lines).strip()
        updated = dict(row)
        if compact_excerpt:
            updated["excerpt"] = compact_excerpt
            updated["excerpt_doc_key_points"] = True
        rewritten.append(updated)
    return rewritten


def _build_stage1_decoded_evidence_block(
    source_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build compact source excerpt appendix from prepared source rows."""
    rows: List[str] = []
    total_excerpt_chars = 0
    decoded_count = 0

    for source in source_rows:
        label = str(source.get("source_id", "S?"))
        title = str(source.get("title", "Untitled"))
        url = str(source.get("url", ""))
        published = str(source.get("published_at", ""))
        excerpt = str(source.get("excerpt", ""))
        decoded = bool(source.get("decoded"))

        total_excerpt_chars += len(excerpt)
        if decoded:
            decoded_count += 1

        rows.append(f"[{label}] {title}")
        if url:
            rows.append(f"URL: {url}")
        if published:
            rows.append(f"Published: {published}")
        if excerpt:
            rows.append(f"Excerpt: {excerpt}")
        rows.append("")

    block = "\n".join(rows).strip()
    return {
        "block": block,
        "source_count": len(source_rows),
        "decoded_count": decoded_count,
        "total_excerpt_chars": total_excerpt_chars,
    }


def _parse_claim_number(raw_value: str) -> Optional[float]:
    """Parse claim numeric token with comma separators."""
    text = str(raw_value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _claim_value_conflicts(a: Any, b: Any) -> bool:
    """Determine whether two claim values are materially different."""
    a_num = _parse_claim_number(str(a))
    b_num = _parse_claim_number(str(b))
    if a_num is not None and b_num is not None:
        tolerance = max(0.5, abs(a_num) * 0.08)
        return abs(a_num - b_num) > tolerance
    return str(a).strip().lower() != str(b).strip().lower()


def _claim_recency_score(published_at: str) -> float:
    """Simple recency score for reconciliation ranking."""
    value = str(published_at or "").strip()
    if len(value) < 10:
        return 0.0
    try:
        year = int(value[:4])
        current_year = datetime.utcnow().year
        if year >= current_year:
            return 2.0
        if year == current_year - 1:
            return 1.2
        if year == current_year - 2:
            return 0.5
    except Exception:
        return 0.0
    return -0.2


def _claim_row_score(row: Dict[str, Any]) -> float:
    """Rank claim rows by confidence + authority + recency."""
    confidence = float(row.get("confidence", 0.0))
    authority = int(row.get("authority_rank", 1))
    recency = _claim_recency_score(str(row.get("published_at", "")))
    return (confidence * 10.0) + (authority * 1.8) + recency


def _extract_claims_from_text_block(
    *,
    text: str,
    source_id: str,
    url: str,
    published_at: str,
    model: str,
    authority_rank: int,
) -> List[Dict[str, Any]]:
    """Extract claim candidates from a text block using deterministic regex rules."""
    raw = str(text or "").strip()
    if not raw:
        return []
    compact = re.sub(r"\s+", " ", raw)
    claims: List[Dict[str, Any]] = []

    patterns: List[Tuple[str, str, str]] = [
        (
            "post_tax_npv_usd_m",
            r"post[-\s]*tax[^\.]{0,60}npv[^0-9]{0,25}(?:us\$|usd)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*m",
            "usd_m",
        ),
        (
            "post_tax_npv_aud_m",
            r"post[-\s]*tax[^\.]{0,60}npv[^0-9]{0,25}(?:a\$|aud)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*m",
            "aud_m",
        ),
        ("irr_pct", r"\birr[^0-9]{0,20}([0-9]{1,3}(?:\.\d+)?)\s*%", "pct"),
        (
            "aisc_usd_per_oz",
            r"\baisc[^0-9]{0,25}(?:us\$|usd)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(?:/|per)?\s*oz",
            "usd_per_oz",
        ),
        (
            "capex_usd_m",
            r"\bcapex[^0-9]{0,25}(?:us\$|usd)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*m",
            "usd_m",
        ),
        (
            "resource_moz",
            r"\b(?:jorc[^\.]{0,30})?(?:resource|mre)[^0-9]{0,25}([0-9][0-9,]*(?:\.\d+)?)\s*moz",
            "moz",
        ),
        (
            "production_koz_pa",
            r"\b([0-9][0-9,]*(?:\.\d+)?)\s*koz\s*(?:pa|p\.a\.|per annum|/yr|year)",
            "koz_pa",
        ),
        (
            "mine_life_years",
            r"\b(?:lom|mine life|life of mine)[^0-9]{0,25}([0-9]{1,2}(?:\.\d+)?)\s*(?:years|year|yrs|yr)\b",
            "years",
        ),
        (
            "market_cap_aud_m",
            r"\bmarket cap(?:italisation)?[^0-9]{0,20}(?:a\$|aud)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*m",
            "aud_m",
        ),
        (
            "shares_outstanding_b",
            r"\bshares(?:\s+outstanding)?[^0-9]{0,20}([0-9](?:\.\d+)?)\s*b(?:illion)?\b",
            "billions",
        ),
    ]

    for field, pattern, unit in patterns:
        for match in re.finditer(pattern, compact, flags=re.IGNORECASE):
            raw_value = match.group(1)
            value_num = _parse_claim_number(raw_value)
            if value_num is None:
                continue
            confidence = 0.58 + (0.04 * max(1, authority_rank))
            claims.append(
                {
                    "field": field,
                    "value": value_num,
                    "raw_value": raw_value,
                    "unit": unit,
                    "source_id": source_id,
                    "url": url,
                    "published_at": published_at,
                    "model": model,
                    "authority_rank": authority_rank,
                    "confidence": min(0.98, confidence),
                    "evidence": compact[max(0, match.start() - 80): match.end() + 80].strip(),
                }
            )

    stage_map = [
        ("peak production", 1.0),
        ("ramp-up", 0.9),
        ("ramp up", 0.9),
        ("first gold pour", 0.8),
        ("first gold", 0.8),
        ("development", 0.6),
        ("definitive feasibility study", 0.4),
        ("dfs", 0.4),
        ("pre-feasibility study", 0.25),
        ("pre feasibility study", 0.25),
        ("pfs", 0.25),
        ("scoping", 0.15),
    ]
    low_compact = compact.lower()
    stage_claim_added = False
    # If text explicitly frames first-gold as a future target, treat as development.
    if re.search(
        r"(target(?:ed|ing)?|expected|planned|on track|scheduled|schedule)[^\.]{0,120}(first gold|gold pour)",
        low_compact,
        flags=re.IGNORECASE,
    ):
        claims.append(
            {
                "field": "project_stage",
                "value": "development",
                "raw_value": "development",
                "unit": "categorical",
                "source_id": source_id,
                "url": url,
                "published_at": published_at,
                "model": model,
                "authority_rank": authority_rank,
                "confidence": min(0.94, 0.62 + (0.04 * max(1, authority_rank))),
                "evidence": compact[:320],
            }
        )
        claims.append(
            {
                "field": "stage_multiplier",
                "value": 0.6,
                "raw_value": "0.6",
                "unit": "multiplier",
                "source_id": source_id,
                "url": url,
                "published_at": published_at,
                "model": model,
                "authority_rank": authority_rank,
                "confidence": min(0.92, 0.60 + (0.04 * max(1, authority_rank))),
                "evidence": compact[:320],
            }
        )
        stage_claim_added = True

    if not stage_claim_added:
        for stage_label, multiplier in stage_map:
            if stage_label not in low_compact:
                continue
            claims.append(
                {
                    "field": "project_stage",
                    "value": stage_label,
                    "raw_value": stage_label,
                    "unit": "categorical",
                    "source_id": source_id,
                    "url": url,
                    "published_at": published_at,
                    "model": model,
                    "authority_rank": authority_rank,
                    "confidence": min(0.92, 0.56 + (0.04 * max(1, authority_rank))),
                    "evidence": compact[:320],
                }
            )
            claims.append(
                {
                    "field": "stage_multiplier",
                    "value": multiplier,
                    "raw_value": str(multiplier),
                    "unit": "multiplier",
                    "source_id": source_id,
                    "url": url,
                    "published_at": published_at,
                    "model": model,
                    "authority_rank": authority_rank,
                    "confidence": min(0.90, 0.55 + (0.04 * max(1, authority_rank))),
                    "evidence": compact[:320],
                }
            )
            break

    if re.search(r"\bfully funded\b|\bfunded to first gold\b|\bsecured funding\b", compact, flags=re.IGNORECASE):
        claims.append(
            {
                "field": "funding_status",
                "value": "funded",
                "raw_value": "funded",
                "unit": "categorical",
                "source_id": source_id,
                "url": url,
                "published_at": published_at,
                "model": model,
                "authority_rank": authority_rank,
                "confidence": min(0.96, 0.65 + (0.04 * max(1, authority_rank))),
                "evidence": compact[:260],
            }
        )

    return claims


def _build_claim_ledger_from_model_runs(
    model_runs: List[Dict[str, Any]],
    verification_profile: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build and reconcile a cross-model claim ledger before Stage 2.

    The ledger is deterministic and source-referenced, so Stage 2/3 can consume
    corrected field values rather than only prose summaries.
    """
    claims: List[Dict[str, Any]] = []

    for model_run in model_runs:
        model = str(model_run.get("model", "")).strip()
        run = model_run.get("result") or {}
        if not model or not isinstance(run, dict) or run.get("error"):
            continue

        # Include top source excerpts.
        for idx, source in enumerate((run.get("results") or [])[:12], start=1):
            if _is_low_signal_notice_source_item(source):
                continue
            source_id = f"{model}:S{idx}"
            url = str(source.get("url", "")).strip()
            published_at = str(source.get("published_at", "")).strip()
            authority_rank = _source_authority_rank(url)
            content = str(
                source.get("decoded_excerpt")
                or source.get("content")
                or source.get("source_snippet")
                or ""
            ).strip()
            claims.extend(
                _extract_claims_from_text_block(
                    text=content,
                    source_id=source_id,
                    url=url,
                    published_at=published_at,
                    model=model,
                    authority_rank=authority_rank,
                )
            )

        # Intentionally do not extract deterministic claims from generated
        # summaries/update prose. Those fields are too prone to model-induced
        # drift versus primary-source excerpts already included above.

    by_field: Dict[str, List[Dict[str, Any]]] = {}
    for claim in claims:
        field = str(claim.get("field", "")).strip()
        if not field:
            continue
        by_field.setdefault(field, []).append(claim)

    resolved_claims: Dict[str, Dict[str, Any]] = {}
    conflicts: List[Dict[str, Any]] = []
    for field, rows in by_field.items():
        ranked = sorted(rows, key=_claim_row_score, reverse=True)
        selected = ranked[0]
        resolved_claims[field] = {
            "field": field,
            "value": selected.get("value"),
            "unit": selected.get("unit", ""),
            "source_id": selected.get("source_id", ""),
            "url": selected.get("url", ""),
            "published_at": selected.get("published_at", ""),
            "model": selected.get("model", ""),
            "confidence": selected.get("confidence", 0.0),
            "authority_rank": selected.get("authority_rank", 1),
            "resolution_rule": "highest confidence + authority + recency",
        }

        conflict_candidates = [
            row
            for row in ranked[1:6]
            if _claim_value_conflicts(row.get("value"), selected.get("value"))
        ]
        if conflict_candidates:
            conflicts.append(
                {
                    "field": field,
                    "selected_value": selected.get("value"),
                    "selected_source_id": selected.get("source_id", ""),
                    "selected_url": selected.get("url", ""),
                    "selected_published_at": selected.get("published_at", ""),
                    "candidates": [
                        {
                            "value": row.get("value"),
                            "source_id": row.get("source_id", ""),
                            "url": row.get("url", ""),
                            "published_at": row.get("published_at", ""),
                            "confidence": row.get("confidence", 0.0),
                        }
                        for row in conflict_candidates
                    ],
                    "resolution_rule": "highest confidence + authority + recency",
                }
            )

    # Coverage proxy from verification markers.
    section_markers = verification_profile.get("compliance_section_markers", []) or []
    critical_sections = set(verification_profile.get("compliance_critical_sections", set()) or set())
    resolved_text = " ".join(
        [
            f"{field} {resolved.get('value')} {resolved.get('unit')}"
            for field, resolved in resolved_claims.items()
        ]
    ).lower()
    section_coverage: Dict[str, bool] = {}
    for section_id, markers in section_markers:
        sid = str(section_id or "").strip().lower()
        if not sid:
            continue
        section_coverage[sid] = any(str(marker).lower() in resolved_text for marker in (markers or []))

    coverage = {
        "sections_total": len(section_coverage),
        "sections_covered": sum(1 for covered in section_coverage.values() if covered),
        "critical_sections_total": len(critical_sections),
        "critical_sections_covered": sum(
            1 for sid in critical_sections if section_coverage.get(sid, False)
        ),
        "missing_sections": [sid for sid, covered in section_coverage.items() if not covered],
        "missing_critical_sections": [
            sid for sid in critical_sections if not section_coverage.get(sid, False)
        ],
    }

    return {
        "schema": "claim_ledger_v1",
        "generated_at": datetime.utcnow().isoformat(),
        "claims": claims[:400],
        "resolved_claims": resolved_claims,
        "conflicts": conflicts,
        "coverage": coverage,
        "counts": {
            "raw_claims": len(claims),
            "resolved_fields": len(resolved_claims),
            "conflicts": len(conflicts),
        },
    }


def _build_deterministic_finance_lane_from_claim_ledger(
    claim_ledger: Dict[str, Any],
    baseline_market_facts: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build deterministic finance/scoring lane from reconciled claim fields.

    This lane is intentionally compact and strictly source-referenced.
    """
    resolved = claim_ledger.get("resolved_claims", {}) if isinstance(claim_ledger, dict) else {}
    if not isinstance(resolved, dict):
        resolved = {}

    def _resolved(field: str) -> Dict[str, Any]:
        item = resolved.get(field, {})
        if isinstance(item, dict):
            return item
        return {}

    def _as_float(field: str) -> Optional[float]:
        value = _resolved(field).get("value")
        return _parse_claim_number(str(value))

    baseline = baseline_market_facts if isinstance(baseline_market_facts, dict) else {}

    def _baseline_float(field: str) -> Optional[float]:
        value = baseline.get(field)
        if value is None:
            return None
        return _parse_claim_number(str(value))

    def _baseline_market_cap_aud_m() -> Optional[float]:
        # Prefer pre-normalized market_cap_m. Fallback to raw market_cap if needed.
        direct_m = _baseline_float("market_cap_m")
        if direct_m is not None and direct_m > 0:
            return direct_m
        raw_cap = _baseline_float("market_cap")
        if raw_cap is None or raw_cap <= 0:
            return None
        # Raw market cap in absolute units -> convert to millions.
        return raw_cap / 1_000_000.0 if raw_cap > 10_000 else raw_cap

    stage_multiplier = _as_float("stage_multiplier")
    project_stage = str(_resolved("project_stage").get("value", "")).strip()
    post_tax_npv_aud_m = _as_float("post_tax_npv_aud_m")
    post_tax_npv_usd_m = _as_float("post_tax_npv_usd_m")
    market_cap_aud_m = _as_float("market_cap_aud_m")
    aisc_usd_per_oz = _as_float("aisc_usd_per_oz")
    baseline_market_cap_m = _baseline_market_cap_aud_m()
    baseline_market_cap_used = False

    # Deterministic lane should honor injected market-facts baseline first.
    if baseline_market_cap_m is not None and baseline_market_cap_m > 0:
        market_cap_aud_m = baseline_market_cap_m
        baseline_market_cap_used = True

    risked_npv_aud_m: Optional[float] = None
    risked_npv_usd_m: Optional[float] = None
    if stage_multiplier is not None:
        if post_tax_npv_aud_m is not None:
            risked_npv_aud_m = post_tax_npv_aud_m * stage_multiplier
        if post_tax_npv_usd_m is not None:
            risked_npv_usd_m = post_tax_npv_usd_m * stage_multiplier

    npv_market_cap_ratio: Optional[float] = None
    ratio_basis = ""
    if market_cap_aud_m and market_cap_aud_m > 0 and risked_npv_aud_m is not None:
        npv_market_cap_ratio = risked_npv_aud_m / market_cap_aud_m
        ratio_basis = "risked_npv_aud_m/market_cap_aud_m"

    npv_ratio_score: Optional[float] = None
    if npv_market_cap_ratio is not None:
        if npv_market_cap_ratio > 3.0:
            npv_ratio_score = 100.0
        elif npv_market_cap_ratio >= 2.0:
            npv_ratio_score = 80.0
        elif npv_market_cap_ratio >= 1.0:
            npv_ratio_score = 60.0
        else:
            npv_ratio_score = 40.0

    cost_competitiveness_score: Optional[float] = None
    if aisc_usd_per_oz is not None:
        # USD proxy thresholds used when only USD AISC is verified.
        if aisc_usd_per_oz < 1500:
            cost_competitiveness_score = 100.0
        elif aisc_usd_per_oz < 2000:
            cost_competitiveness_score = 80.0
        elif aisc_usd_per_oz < 2500:
            cost_competitiveness_score = 60.0
        else:
            cost_competitiveness_score = 40.0

    stage_score_component: Optional[float] = None
    if stage_multiplier is not None:
        stage_score_component = max(0.0, min(100.0, stage_multiplier * 100.0))

    funding_status = str(_resolved("funding_status").get("value", "")).strip().lower()
    funding_score_component: Optional[float] = None
    if funding_status:
        funding_score_component = 95.0 if "funded" in funding_status else 60.0

    verified_fields = {}
    for field_name in (
        "project_stage",
        "stage_multiplier",
        "post_tax_npv_aud_m",
        "post_tax_npv_usd_m",
        "irr_pct",
        "aisc_usd_per_oz",
        "capex_usd_m",
        "resource_moz",
        "production_koz_pa",
        "mine_life_years",
        "market_cap_aud_m",
        "shares_outstanding_b",
        "funding_status",
    ):
        row = _resolved(field_name)
        if not row:
            continue
        verified_fields[field_name] = {
            "value": row.get("value"),
            "unit": row.get("unit", ""),
            "source_id": row.get("source_id", ""),
            "url": row.get("url", ""),
            "published_at": row.get("published_at", ""),
            "confidence": row.get("confidence", 0.0),
            "model": row.get("model", ""),
        }

    if baseline_market_cap_used:
        verified_fields["market_cap_aud_m"] = {
            "value": market_cap_aud_m,
            "unit": "aud_m",
            "source_id": "normalized_facts_prepass",
            "url": "",
            "published_at": "",
            "confidence": 0.99,
            "model": "system",
        }

    missing_critical_fields = []
    for field in ("stage_multiplier", "post_tax_npv_aud_m", "market_cap_aud_m"):
        if field not in verified_fields:
            missing_critical_fields.append(field)

    status = "ready" if not missing_critical_fields else "partial"

    return {
        "schema": "deterministic_finance_lane_v1",
        "generated_at": datetime.utcnow().isoformat(),
        "status": status,
        "project_stage": project_stage,
        "verified_fields": verified_fields,
        "derived_metrics": {
            "risked_npv_aud_m": risked_npv_aud_m,
            "risked_npv_usd_m": risked_npv_usd_m,
            "npv_market_cap_ratio": npv_market_cap_ratio,
            "npv_market_cap_ratio_basis": ratio_basis,
        },
        "score_components": {
            "value_npv_vs_market_cap_score": npv_ratio_score,
            "value_cost_competitiveness_score_proxy": cost_competitiveness_score,
            "quality_stage_score_component": stage_score_component,
            "quality_funding_score_component": funding_score_component,
        },
        "market_facts_baseline": {
            "used": baseline_market_cap_used,
            "market_cap_aud_m": market_cap_aud_m if baseline_market_cap_used else None,
            "currency": str(baseline.get("currency", "")),
        },
        "calculation_trace": [
            "risked_npv = post_tax_npv * stage_multiplier",
            "npv_market_cap_ratio = risked_npv_aud_m / market_cap_aud_m",
            "npv_vs_market_cap_score thresholds: >3x=100, 2-3x=80, 1-2x=60, <1x=40",
        ],
        "missing_critical_fields": missing_critical_fields,
    }


def _extract_stage1_timeline_evidence(
    source_rows: List[Dict[str, Any]],
    max_items: int,
    *,
    timeline_terms: Optional[List[str]] = None,
    timeline_focus_terms: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Build a prioritized timeline digest from decoded evidence rows."""
    safe_limit = max(2, int(max_items))
    rows: List[Dict[str, Any]] = []
    seen = set()
    effective_timeline_terms = _normalize_terms_list(timeline_terms or _STAGE1_DEFAULT_TIMELINE_TERMS)
    effective_focus_terms = _normalize_terms_list(
        timeline_focus_terms or _STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS
    )

    for source in source_rows:
        if _is_low_signal_notice_source_item(source):
            continue
        source_id = str(source.get("source_id", "")).strip() or "S?"
        title = str(source.get("title", "")).strip()
        url = str(source.get("url", "")).strip()
        published = str(source.get("published_at", "")).strip()
        authority = _source_authority_rank(url)

        for sentence in _extract_source_sentences(str(source.get("excerpt", ""))):
            low = sentence.lower()
            if effective_timeline_terms and not any(token in low for token in effective_timeline_terms):
                continue
            if not _extract_timeline_windows(sentence) and (
                effective_focus_terms and not any(token in low for token in effective_focus_terms)
            ):
                continue

            key = re.sub(r"\s+", " ", low).strip()
            if key in seen:
                continue
            seen.add(key)

            score = 0
            if effective_focus_terms and any(token in low for token in effective_focus_terms):
                score += 4
            if "first ore" in low or "launch" in low or "approval" in low:
                score += 3
            if "stockpile" in low or "processing" in low:
                score += 2
            if "on track" in low or "targeting" in low:
                score += 1
            score += authority * 2
            if published.startswith("2026-"):
                score += 2
            elif published.startswith("2025-"):
                score += 1

            rows.append(
                {
                    "source_id": source_id,
                    "title": title,
                    "url": url,
                    "published_at": published,
                    "authority_rank": authority,
                    "score": score,
                    "fact": sentence,
                    "windows": _extract_timeline_windows(sentence),
                }
            )

    def _published_to_quarter_idx(value: str) -> Optional[int]:
        raw = str(value or "").strip()
        if len(raw) < 7:
            return None
        match = re.match(r"^(\d{4})-(\d{2})", raw)
        if not match:
            return None
        try:
            year = int(match.group(1))
            month = int(match.group(2))
            quarter = ((month - 1) // 3) + 1
            return (year * 4) + (quarter - 1)
        except Exception:
            return None

    # Drop stale timeline rows when fresher windows are available.
    row_latest_idx: List[Tuple[Dict[str, Any], Optional[int]]] = []
    all_row_indices: List[int] = []
    for row in rows:
        indices: List[int] = []
        pub_idx = _published_to_quarter_idx(str(row.get("published_at", "")))
        if pub_idx is not None:
            indices.append(pub_idx)
        for token in (row.get("windows") or []):
            idx = _window_to_quarter_index(str(token))
            if idx is not None:
                indices.append(idx)
        row_idx = max(indices) if indices else None
        row_latest_idx.append((row, row_idx))
        if row_idx is not None:
            all_row_indices.append(row_idx)

    if all_row_indices:
        newest_idx = max(all_row_indices)
        filtered_rows: List[Dict[str, Any]] = []
        for row, row_idx in row_latest_idx:
            if row_idx is None:
                # Keep undated rows only when not enough dated evidence exists.
                if len(all_row_indices) >= safe_limit:
                    continue
                filtered_rows.append(row)
                continue
            # If fresh evidence exists, drop rows older than ~2 years (8 quarters).
            if newest_idx - row_idx > 8:
                continue
            filtered_rows.append(row)
        if filtered_rows:
            rows = filtered_rows

    rows.sort(
        key=lambda item: (
            int(item.get("score", 0)),
            str(item.get("published_at", "")),
            int(item.get("authority_rank", 0)),
        ),
        reverse=True,
    )
    return rows[:safe_limit]


def _build_stage1_timeline_digest_block(timeline_rows: List[Dict[str, Any]]) -> str:
    """Format timeline evidence digest for second-pass prompt injection."""
    lines: List[str] = []
    for row in timeline_rows:
        source_id = str(row.get("source_id", "S?"))
        published = str(row.get("published_at", "")).strip() or "Unknown date"
        fact = str(row.get("fact", "")).strip()
        windows = row.get("windows") or []
        window_text = f" windows={', '.join(windows)}" if windows else ""
        lines.append(f"- [{source_id}] {published}:{window_text} {fact}")
    return "\n".join(lines).strip()


def _evaluate_stage1_timeline_guard(
    response_text: str,
    timeline_rows: List[Dict[str, Any]],
    *,
    focus_terms: Optional[List[str]] = None,
    conflict_field: str = "timeline_window",
    max_shift_quarters: int = 3,
) -> Dict[str, Any]:
    """
    Compare timeline windows between evidence and model output.

    This check is observational-only and never blocks model acceptance.
    """
    if not PERPLEXITY_STAGE1_TIMELINE_GUARD_ENABLED:
        return {
            "enabled": False,
            "passed": True,
            "reason": "timeline_guard_disabled",
            "evidence_windows": [],
            "response_windows": [],
        }

    effective_focus_terms = _normalize_terms_list(focus_terms or _STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS)

    evidence_facts = [
        str(row.get("fact", ""))
        for row in timeline_rows
        if (
            not effective_focus_terms
            or any(token in str(row.get("fact", "")).lower() for token in effective_focus_terms)
        )
    ]
    if not evidence_facts:
        evidence_facts = [str(row.get("fact", "")) for row in timeline_rows]
    evidence_text = "\n".join(evidence_facts).strip()
    evidence_windows = _extract_timeline_windows(evidence_text)

    response_lines = [
        line
        for line in (response_text or "").splitlines()
        if (
            not effective_focus_terms
            or any(token in line.lower() for token in effective_focus_terms)
        )
    ]
    response_focus = "\n".join(response_lines).strip() or (response_text or "")
    response_windows = _extract_timeline_windows(response_focus)

    if not evidence_windows or not response_windows:
        return {
            "enabled": True,
            "passed": True,
            "reason": "timeline_windows_not_comparable",
            "evidence_windows": evidence_windows,
            "response_windows": response_windows,
        }

    evidence_idx = [idx for idx in (_window_to_quarter_index(token) for token in evidence_windows) if idx is not None]
    response_idx = [idx for idx in (_window_to_quarter_index(token) for token in response_windows) if idx is not None]
    if not evidence_idx or not response_idx:
        return {
            "enabled": True,
            "passed": True,
            "reason": "timeline_index_parse_failed",
            "evidence_windows": evidence_windows,
            "response_windows": response_windows,
        }

    evidence_latest = max(evidence_idx)
    response_earliest = min(response_idx)
    shifted_quarters = response_earliest - evidence_latest
    # Observational shift (positive means response timeline is later than evidence).
    threshold = max(1, int(max_shift_quarters))
    reason = "timeline_observation_ok"
    if shifted_quarters >= threshold:
        reason = f"timeline_observation_later_by_{shifted_quarters}_quarters_non_blocking"
    elif shifted_quarters <= -threshold:
        reason = f"timeline_observation_earlier_by_{abs(shifted_quarters)}_quarters_non_blocking"
    return {
        "enabled": True,
        "passed": True,
        "reason": reason,
        "evidence_windows": evidence_windows,
        "response_windows": response_windows,
        "shifted_quarters": shifted_quarters,
    }


def _build_stage1_second_pass_prompt(
    *,
    user_query: str,
    research_brief: str,
    run: Dict[str, Any],
    mandatory_fact_ledger_json: str,
    compact_fact_bundle_json: str,
    fact_digest_json: str,
    fact_pack_json: str,
    evidence_appendix: str,
    timeline_digest: str,
    source_key_points_json: str = "",
    supplementary_macro_news_json: str = "",
    cashflow_schema_contract: str = "",
) -> str:
    """
    Build second-pass prompt with injected evidence bundles.

    The model must base analysis on provided evidence artifacts and cite [S#]
    source ids for numeric claims.
    """
    task = (user_query or "").strip()
    brief = _truncate_text_for_prompt((research_brief or "").strip(), 1800)
    run_model = str(run.get("model", "")).strip()
    run_ticker = str(run.get("ticker", "")).strip()
    run_depth = str(run.get("depth", "")).strip()

    requirements = (
        "MANDATORY OUTPUT SECTIONS (use these exact section labels):\n"
        "1) Quality Score\n"
        "2) Value Score\n"
        "3) Price Targets (12-month and 24-month)\n"
        "4) Development Timeline\n"
        "5) Certainty % (24 months)\n"
        "6) Headwinds/Tailwinds\n"
        "7) Thesis Map (bull/base/bear)\n"
        "8) Investment Verdict\n\n"
        "EVIDENCE AND CITATION RULES:\n"
        "- Base analysis only on injected evidence below.\n"
        "- Treat MANDATORY_SOURCE_FACT_LEDGER_JSON as non-compressible primary/prepass evidence.\n"
        "- Do not state any mandatory ledger fact is absent, unknown, unavailable, or unverified.\n"
        "- Every key numeric claim must include at least one [S#] citation.\n"
        "- Mark inferred values with ESTIMATE and one-line rationale.\n"
        "- If evidence conflicts, prefer the newest dated primary source and state conflict."
    )
    if cashflow_schema_contract.strip():
        requirements = f"{requirements}\n\n{cashflow_schema_contract.strip()}"

    evidence_blocks: List[str] = []
    if mandatory_fact_ledger_json.strip():
        evidence_blocks.append(
            "MANDATORY_SOURCE_FACT_LEDGER_JSON:\n"
            f"```json\n{mandatory_fact_ledger_json.strip()}\n```"
        )
    if source_key_points_json.strip():
        evidence_blocks.append(
            "SOURCE_KEY_POINTS_JSON:\n"
            f"```json\n{source_key_points_json.strip()}\n```"
        )
    if supplementary_macro_news_json.strip():
        evidence_blocks.append(
            "SUPPLEMENTARY_NEWS_SEGMENT_JSON:\n"
            f"```json\n{supplementary_macro_news_json.strip()}\n```"
        )
    if compact_fact_bundle_json.strip():
        evidence_blocks.append(
            "COMPACT_FACT_BUNDLE_JSON:\n"
            f"```json\n{compact_fact_bundle_json.strip()}\n```"
        )
    if fact_digest_json.strip():
        evidence_blocks.append(
            "FACT_DIGEST_V2_JSON:\n"
            f"```json\n{fact_digest_json.strip()}\n```"
        )
    if fact_pack_json.strip():
        evidence_blocks.append(
            "RUBRIC_FACT_PACK_JSON:\n"
            f"```json\n{fact_pack_json.strip()}\n```"
        )
    if timeline_digest.strip():
        evidence_blocks.append(
            "TIMELINE_EVIDENCE_DIGEST:\n"
            f"{timeline_digest.strip()}"
        )
    if evidence_appendix.strip():
        evidence_blocks.append(
            "EVIDENCE_APPENDIX:\n"
            f"{evidence_appendix.strip()}"
        )

    prompt_parts: List[str] = [
        "You are Stage 1 council analyst. Produce a complete investment analysis from injected evidence.",
        (
            "RUN CONTEXT:\n"
            f"- Model: {run_model or 'unknown'}\n"
            f"- Ticker: {run_ticker or 'unknown'}\n"
            f"- Depth: {run_depth or 'unknown'}"
        ),
        f"USER TASK:\n{task}",
    ]
    if brief:
        prompt_parts.append(f"RESEARCH BRIEF (CONDENSED):\n{brief}")
    prompt_parts.append(requirements)
    if evidence_blocks:
        prompt_parts.append("INJECTED EVIDENCE BUNDLE:\n" + "\n\n".join(evidence_blocks))
    prompt_parts.append(
        "Return analysis now. Do not output a source log only; output full rubric-aligned analysis."
    )
    return "\n\n".join(part for part in prompt_parts if part.strip()).strip()


def _extract_source_citations(text: str) -> List[str]:
    """Return all citation markers like [S1], [S2] in appearance order."""
    if not text:
        return []
    return re.findall(r"\[(S\d+)\]", text)


def _count_uncited_numeric_lines(text: str) -> Dict[str, int]:
    """
    Count lines containing numeric claims that do not include source citations.

    Excludes URL-only lines and trivial short lines.
    """
    numeric_lines = 0
    uncited_numeric_lines = 0
    claim_tokens = (
        "market cap",
        "shares",
        "enterprise value",
        "current price",
        "npv",
        "irr",
        "aisc",
        "capex",
        "opex",
        "resource",
        "reserve",
        "grade",
        "mine life",
        "production",
        "cash",
        "debt",
        "funding",
        "price target",
        "valuation",
        "ev/oz",
        "quality score",
        "value score",
        "certainty",
        "timeline",
        "milestone",
        "headwind",
        "tailwind",
    )
    # Evaluate by paragraph block first so structured JSON/markdown sections are not
    # over-penalized when citation appears adjacent to (not on) numeric lines.
    blocks = re.split(r"\n\s*\n", text or "")
    for raw_block in blocks:
        block = (raw_block or "").strip()
        if not block:
            continue
        block_lower = block.lower()
        block_has_source = bool(re.search(r"\[S\d+\]", block))
        block_has_estimate = "estimate" in block_lower
        block_lines = [line.strip() for line in block.splitlines() if line.strip()]
        for line in block_lines:
            if len(line) < 12:
                continue
            line_lower = line.lower()
            if line_lower.startswith(("http://", "https://", "url:")):
                continue
            if not re.search(r"\d", line_lower):
                continue
            # Ignore template boilerplate and formula scaffolding copied into output.
            if (
                line_lower.startswith(("step ", "quality score formula", "value score formula"))
                or "weighted framework" in line_lower
                or "core formulas" in line_lower
                or "npv template" in line_lower
            ):
                continue
            looks_claim = any(token in line_lower for token in claim_tokens) or bool(
                re.search(r"(a\$|us\$|aud|usd|%|moz|koz|g/t|oz\b)", line_lower)
            )
            if not looks_claim:
                continue
            numeric_lines += 1
            line_has_source = bool(re.search(r"\[S\d+\]", line))
            # Allow ESTIMATE-tagged blocks when they include explicit estimate rationale.
            if not (line_has_source or block_has_source or block_has_estimate):
                uncited_numeric_lines += 1
    return {
        "numeric_lines": numeric_lines,
        "uncited_numeric_lines": uncited_numeric_lines,
    }


def _stage1_response_looks_truncated(text: str) -> bool:
    """Heuristic detector for cut-off second-pass outputs."""
    body = (text or "").strip()
    if len(body) < 200:
        return False
    if body.count("```") % 2 == 1:
        return True
    if body[0] == "{":
        # Most JSON outputs should close cleanly.
        if not body.endswith("}"):
            return True
        try:
            json.loads(body)
        except Exception:
            # Treat malformed JSON-like payload as truncated/corrupted for retry.
            return True
    if body[-1] in {":", ",", "/", "(", "[", "{", '"'}:
        return True
    return False


async def _assess_stage1_truncation(
    *,
    model: str,
    response_text: str,
    output_tokens_used: int,
    finish_reason: str,
) -> Dict[str, Any]:
    """Adjudicate premature truncation with strong evidence only."""
    body = (response_text or "").strip()
    if not body:
        return {
            "used": False,
            "truncated": True,
            "confidence_pct": 100.0,
            "reason": "empty_response",
        }

    if _stage1_response_looks_truncated(body):
        return {
            "used": False,
            "truncated": True,
            "confidence_pct": 99.0,
            "reason": "deterministic_high_confidence",
        }

    if not STAGE1_TRUNCATION_CHECKER_ENABLED:
        return {
            "used": False,
            "truncated": False,
            "confidence_pct": 0.0,
            "reason": "checker_disabled_fail_open",
        }
    if not OPENROUTER_API_KEY:
        return {
            "used": False,
            "truncated": False,
            "confidence_pct": 0.0,
            "reason": "missing_openrouter_key_fail_open",
        }
    checker_model = str(STAGE1_TRUNCATION_CHECKER_MODEL or "").strip()
    if not checker_model:
        return {
            "used": False,
            "truncated": False,
            "confidence_pct": 0.0,
            "reason": "missing_checker_model_fail_open",
        }

    payload = {
        "model": str(model or ""),
        "output_tokens_used": int(max(0, output_tokens_used)),
        "finish_reason": str(finish_reason or "").strip(),
        "response_chars": len(body),
        "response_head_preview": _truncate_text_for_prompt(body[:1600], 1600),
        "response_tail_preview": _truncate_text_for_prompt(body[-3200:], 3200),
    }
    prompt = (
        "Decide whether this Stage-1 response was prematurely truncated.\n"
        "Rules:\n"
        "1) Set truncated=true only if confidence is very high the answer cut off early.\n"
        "2) Long responses are not truncated just because they are long.\n"
        "3) Focus mainly on the tail/end of the response.\n"
        "4) If the ending looks complete enough, set truncated=false.\n\n"
        "Return JSON only with this exact shape:\n"
        "{"
        "\"truncated\": <bool>, "
        "\"confidence_pct\": <0-100 number>, "
        "\"reason\": \"<short reason>\", "
        "\"tail_looks_complete\": <bool>"
        "}\n\n"
        "Input JSON:\n"
        f"{json.dumps(payload, ensure_ascii=True, separators=(',', ':'))}"
    )
    response = await query_model(
        model=checker_model,
        messages=[{"role": "user", "content": prompt}],
        timeout=float(max(10.0, STAGE1_TRUNCATION_CHECKER_TIMEOUT_SECONDS)),
        max_tokens=int(max(120, STAGE1_TRUNCATION_CHECKER_MAX_OUTPUT_TOKENS)),
        reasoning_effort=str(STAGE1_TRUNCATION_CHECKER_REASONING_EFFORT or "low"),
    )
    if not response:
        return {
            "used": False,
            "truncated": False,
            "confidence_pct": 0.0,
            "reason": "checker_no_response_fail_open",
            "model": checker_model,
        }
    raw = str(response.get("content", "") or "")
    parsed, parse_error = _parse_json_object_from_text(raw)
    if not parsed:
        return {
            "used": False,
            "truncated": False,
            "confidence_pct": 0.0,
            "reason": f"checker_parse_failed:{parse_error or 'unknown'}",
            "model": checker_model,
            "raw_preview": _truncate_text_for_prompt(raw, 220),
        }
    truncated = _coerce_bool(parsed.get("truncated"))
    if truncated is None:
        return {
            "used": False,
            "truncated": False,
            "confidence_pct": 0.0,
            "reason": "checker_missing_truncated_fail_open",
            "model": checker_model,
            "raw_preview": _truncate_text_for_prompt(raw, 220),
        }
    confidence = _coerce_float(parsed.get("confidence_pct"))
    if confidence is None:
        confidence = 0.0
    min_confidence = max(
        0.0,
        min(100.0, float(STAGE1_TRUNCATION_CHECKER_MIN_CONFIDENCE_PCT)),
    )
    high_conf_truncated = bool(truncated and confidence >= min_confidence)
    return {
        "used": True,
        "model": checker_model,
        "truncated": high_conf_truncated,
        "raw_truncated": bool(truncated),
        "confidence_pct": float(confidence),
        "reason": str(parsed.get("reason", "") or "").strip() or "checker_ok",
        "tail_looks_complete": _coerce_bool(parsed.get("tail_looks_complete")),
        "min_confidence_pct": min_confidence,
    }


from .stage1_attempt import _STAGE1_RUBRIC_SECTION_MARKERS, _STAGE1_RUBRIC_CRITICAL_SECTIONS



def _evaluate_stage1_rubric_coverage(
    response_text: str,
    user_query: str,
    research_brief: str,
    *,
    section_markers: Optional[List[Tuple[str, List[str]]]] = None,
    critical_sections: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """Estimate rubric conformance coverage from required output sections."""
    requires_template = _stage1_requires_template_compliance(user_query, research_brief)
    if not requires_template:
        return {
            "required": False,
            "sections_total": 0,
            "sections_covered": 0,
            "coverage_pct": 1.0,
            "missing_sections": [],
            "critical_missing_sections": [],
        }

    text = (response_text or "").lower()
    markers_spec = section_markers or _STAGE1_RUBRIC_SECTION_MARKERS
    critical_spec = critical_sections or _STAGE1_RUBRIC_CRITICAL_SECTIONS
    section_hits: Dict[str, bool] = {}
    for section_id, markers in markers_spec:
        section_hits[section_id] = any(marker in text for marker in markers)

    sections_total = len(markers_spec)
    sections_covered = sum(1 for hit in section_hits.values() if hit)
    coverage_pct = (sections_covered / sections_total) if sections_total else 1.0
    missing_sections = [section for section, hit in section_hits.items() if not hit]
    critical_missing_sections = [
        section
        for section in missing_sections
        if section in critical_spec
    ]

    return {
        "required": True,
        "sections_total": sections_total,
        "sections_covered": sections_covered,
        "coverage_pct": coverage_pct,
        "missing_sections": missing_sections,
        "critical_missing_sections": critical_missing_sections,
    }


def _evaluate_stage1_citation_gate(
    response_text: str,
    valid_source_ids: List[str],
    *,
    user_query: str,
    research_brief: str,
    section_markers: Optional[List[Tuple[str, List[str]]]] = None,
    critical_sections: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """
    Lightweight conformance gate using percentages (not hard line-count cutoffs).

    Score = 0.6 * rubric_coverage_pct + 0.4 * numeric_citation_pct
    Retry is recommended only for catastrophic failures.
    """
    valid_ids = {item.strip() for item in valid_source_ids if item}
    citations = _extract_source_citations(response_text)
    unique_citations = sorted(set(citations))
    invalid_citations = sorted([item for item in unique_citations if item not in valid_ids])
    numeric_stats = _count_uncited_numeric_lines(response_text)
    citation_count = len(citations)
    numeric_lines = int(numeric_stats["numeric_lines"])
    uncited_numeric_lines = int(numeric_stats["uncited_numeric_lines"])
    cited_numeric_lines = max(0, numeric_lines - uncited_numeric_lines)
    numeric_citation_pct = (
        (cited_numeric_lines / numeric_lines)
        if numeric_lines > 0
        else 1.0
    )

    rubric = _evaluate_stage1_rubric_coverage(
        response_text=response_text,
        user_query=user_query,
        research_brief=research_brief,
        section_markers=section_markers,
        critical_sections=critical_sections,
    )
    rubric_required = bool(rubric.get("required", False))
    rubric_coverage_pct = float(rubric.get("coverage_pct", 1.0))
    sections_total = int(rubric.get("sections_total", 0))
    sections_covered = int(rubric.get("sections_covered", 0))
    missing_sections = list(rubric.get("missing_sections", []) or [])
    critical_missing_sections = list(rubric.get("critical_missing_sections", []) or [])

    min_score = max(0.0, min(1.0, float(PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE)))
    min_rubric = max(
        0.0,
        min(1.0, float(PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT)),
    )
    min_numeric = max(
        0.0,
        min(1.0, float(PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT)),
    )
    catastrophic_score = max(
        0.0,
        min(1.0, float(PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_CATASTROPHIC_SCORE)),
    )
    min_count = max(0, int(PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MIN_COUNT))
    max_uncited = max(0, int(PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MAX_UNCITED_NUMERIC_LINES))

    compliance_score = (0.6 * rubric_coverage_pct) + (0.4 * numeric_citation_pct)
    catastrophic_failure = bool(
        compliance_score < catastrophic_score
        or len(critical_missing_sections) >= 3
        or (rubric_required and rubric_coverage_pct < max(0.30, min_rubric * 0.50))
        or (
            len(critical_missing_sections) >= 2
            and numeric_citation_pct < max(0.30, min_numeric * 0.60)
        )
    )

    if not PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED:
        return {
            "enabled": False,
            "passed": True,
            "reason": "citation_gate_disabled",
            "citation_count": citation_count,
            "unique_citation_count": len(unique_citations),
            "invalid_citations": invalid_citations,
            "numeric_lines": numeric_lines,
            "uncited_numeric_lines": uncited_numeric_lines,
            "cited_numeric_lines": cited_numeric_lines,
            "numeric_citation_pct": numeric_citation_pct,
            "rubric_required": rubric_required,
            "rubric_sections_total": sections_total,
            "rubric_sections_covered": sections_covered,
            "rubric_coverage_pct": rubric_coverage_pct,
            "rubric_missing_sections": missing_sections,
            "rubric_critical_missing_sections": critical_missing_sections,
            "compliance_score": compliance_score,
            "compliance_rating": "green",
            "retry_recommended": False,
            "catastrophic_failure": False,
            "compliance_fail_reasons": [],
            "compliance_warning_reasons": [],
            "compliance_hard_fail_reasons": [],
            "compliance_soft_fail_reasons": [],
        }

    if not valid_ids:
        return {
            "enabled": True,
            "passed": True,
            "reason": "no_source_ids_available",
            "citation_count": citation_count,
            "unique_citation_count": len(unique_citations),
            "invalid_citations": invalid_citations,
            "numeric_lines": numeric_lines,
            "uncited_numeric_lines": uncited_numeric_lines,
            "cited_numeric_lines": cited_numeric_lines,
            "numeric_citation_pct": numeric_citation_pct,
            "rubric_required": rubric_required,
            "rubric_sections_total": sections_total,
            "rubric_sections_covered": sections_covered,
            "rubric_coverage_pct": rubric_coverage_pct,
            "rubric_missing_sections": missing_sections,
            "rubric_critical_missing_sections": critical_missing_sections,
            "compliance_score": compliance_score,
            "compliance_rating": "green",
            "retry_recommended": False,
            "catastrophic_failure": False,
            "compliance_fail_reasons": [],
            "compliance_warning_reasons": [],
            "compliance_hard_fail_reasons": [],
            "compliance_soft_fail_reasons": [],
        }

    fail_reasons: List[str] = []
    warning_reasons: List[str] = []

    if rubric_coverage_pct < min_rubric:
        fail_reasons.append(f"rubric_coverage_pct<{min_rubric:.2f}")
    if numeric_citation_pct < min_numeric:
        fail_reasons.append(f"numeric_citation_pct<{min_numeric:.2f}")
    if compliance_score < min_score:
        fail_reasons.append(f"compliance_score<{min_score:.2f}")
    if citation_count < min_count:
        fail_reasons.append(f"citation_count<{min_count}")
    if invalid_citations:
        fail_reasons.append(f"invalid_source_refs={len(invalid_citations)}")
    if critical_missing_sections:
        fail_reasons.append(f"critical_sections_missing={len(critical_missing_sections)}")
    if uncited_numeric_lines > max_uncited:
        warning_reasons.append(f"uncited_numeric_lines>{max_uncited}")

    passed = len(fail_reasons) == 0
    compliance_rating = "green" if passed else ("red" if catastrophic_failure else "amber")
    retry_recommended = bool((not passed) and catastrophic_failure)
    reason = "ok"
    if not passed:
        reason = "|".join(fail_reasons + warning_reasons)
    elif warning_reasons:
        reason = "ok_warn:" + "|".join(warning_reasons)
    hard_fail_reasons = list(fail_reasons) if catastrophic_failure else []
    soft_fail_reasons = list(fail_reasons) if (fail_reasons and not catastrophic_failure) else []

    return {
        "enabled": True,
        "passed": passed,
        "reason": reason,
        "citation_count": citation_count,
        "unique_citation_count": len(unique_citations),
        "invalid_citations": invalid_citations,
        "numeric_lines": numeric_lines,
        "uncited_numeric_lines": uncited_numeric_lines,
        "cited_numeric_lines": cited_numeric_lines,
        "numeric_citation_pct": numeric_citation_pct,
        "rubric_required": rubric_required,
        "rubric_sections_total": sections_total,
        "rubric_sections_covered": sections_covered,
        "rubric_coverage_pct": rubric_coverage_pct,
        "rubric_missing_sections": missing_sections,
        "rubric_critical_missing_sections": critical_missing_sections,
        "compliance_score": compliance_score,
        "compliance_rating": compliance_rating,
        "retry_recommended": retry_recommended,
        "catastrophic_failure": catastrophic_failure,
        "compliance_fail_reasons": list(fail_reasons),
        "compliance_warning_reasons": list(warning_reasons),
        "compliance_hard_fail_reasons": hard_fail_reasons,
        "compliance_soft_fail_reasons": soft_fail_reasons,
    }


def _build_stage1_citation_repair_prompt(base_prompt: str, gate: Dict[str, Any]) -> str:
    """Append concise retry guidance when citation gate fails."""
    reason = str(gate.get("reason", "citation_gate_failed")).strip()
    min_score = max(0.0, min(1.0, float(PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE)))
    min_rubric = max(
        0.0,
        min(1.0, float(PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT)),
    )
    min_numeric = max(
        0.0,
        min(1.0, float(PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT)),
    )
    missing_sections = ", ".join(list(gate.get("rubric_missing_sections", []) or [])[:6]) or "none"
    return (
        f"{base_prompt}\n\n"
        "CONFORMANCE REPAIR RETRY (mandatory):\n"
        f"- Prior attempt failed conformance checks: {reason}.\n"
        f"- Raise rubric coverage to >= {int(min_rubric * 100)}% (missing: {missing_sections}).\n"
        f"- Raise numeric citation coverage to >= {int(min_numeric * 100)}% for numeric claims.\n"
        f"- Raise combined compliance score to >= {int(min_score * 100)}%.\n"
        "- Every key numeric claim must carry [S#] or ESTIMATE with one-line justification.\n"
        "- Avoid dumping rubric boilerplate; provide analysis outputs directly.\n"
    ).strip()


def _build_stage1_truncation_repair_prompt(base_prompt: str) -> str:
    """Append compactness guidance when prior response appears cut off."""
    return (
        f"{base_prompt}\n\n"
        "TRUNCATION REPAIR RETRY (mandatory):\n"
        "- Prior attempt appears truncated or capped.\n"
        "- Keep output <= 1,800 words and avoid repeating rubric/formula text.\n"
        "- Prioritize final outputs: scores, price targets, timeline, certainty, catalysts, risks.\n"
        "- Keep numeric claims source-backed with [S#] or ESTIMATE with one-line justification.\n"
        "- Ensure the response ends cleanly and completely (no partial JSON/partial sentence).\n"
    ).strip()


async def _run_stage1_second_pass_analysis(
    *,
    model: str,
    user_query: str,
    research_brief: str,
    run: Dict[str, Any],
    verification_profile: Optional[Dict[str, Any]] = None,
    supplementary_macro_news_override: Optional[Dict[str, Any]] = None,
    prepass_source_rows: Optional[List[Dict[str, Any]]] = None,
    analysis_provider: str = "openrouter",
) -> Dict[str, Any]:
    """
    Run a second-pass model analysis on decoded evidence.

    This pass reasons over locally decoded source excerpts and can route through
    either OpenRouter or Perplexity depending on stage-1 mixed-mode configuration.
    """
    available_sources = (
        len(prepass_source_rows or [])
        if prepass_source_rows
        else len(run.get("results") or [])
    )
    configured_sources = max(1, int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES))
    source_budget = max(configured_sources, min(max(1, int(MAX_SOURCES)), available_sources))
    profile = verification_profile or _default_stage1_verification_profile()
    profile_fact_keywords = profile.get("fact_digest_keywords") or _FACT_DIGEST_V2_KEYWORDS
    profile_narrative_order = profile.get("fact_digest_narrative_order") or _FACT_DIGEST_V2_NARRATIVE_ORDER
    profile_timeline_terms = profile.get("timeline_terms") or _STAGE1_DEFAULT_TIMELINE_TERMS
    profile_timeline_focus_terms = (
        profile.get("timeline_focus_terms") or _STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS
    )
    profile_conflict_field = str(
        profile.get("timeline_conflict_field", "timeline_window")
    )
    profile_conflict_resolution_rule = str(
        profile.get(
            "timeline_conflict_resolution_rule",
            "prefer newest dated primary-source timeline evidence",
        )
    )
    profile_conflict_max_shift_quarters = max(
        1,
        int(profile.get("timeline_conflict_max_shift_quarters", 3)),
    )
    profile_section_markers = list(
        profile.get("compliance_section_markers") or _STAGE1_RUBRIC_SECTION_MARKERS
    )
    profile_critical_sections_raw = (
        profile.get("compliance_critical_sections") or _STAGE1_RUBRIC_CRITICAL_SECTIONS
    )
    profile_critical_sections = set(profile_critical_sections_raw)
    asx_deterministic_ingestion_summary: Dict[str, Any] = {}
    using_prepass_source_rows = bool(prepass_source_rows)
    prepass_source_rows_cleaned: List[Dict[str, Any]] = []
    if using_prepass_source_rows:
        current_year = datetime.utcnow().year
        for row in (prepass_source_rows or []):
            if not isinstance(row, dict):
                continue
            excerpt = str(row.get("excerpt", "")).strip()
            if not excerpt:
                continue
            source_year = _infer_source_year(
                str(row.get("published_at", "")).strip(),
                str(row.get("title", "")).strip(),
                str(row.get("url", "")).strip(),
            )
            if (
                source_year is not None
                and source_year <= (current_year - 3)
                and len(prepass_source_rows_cleaned) >= max(3, source_budget - 3)
            ):
                continue
            prepass_source_rows_cleaned.append(
                {
                    "source_id": str(row.get("source_id", "")).strip(),
                    "title": str(row.get("title", "")).strip() or "Untitled",
                    "url": str(row.get("url", "")).strip(),
                    "published_at": str(row.get("published_at", "")).strip(),
                    "decode_status": str(row.get("decode_status", "")).strip()
                    or "prepass_bundle",
                    "decoded": bool(row.get("decoded", True)),
                    "excerpt": excerpt[
                        : max(300, int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE))
                    ],
                    "material_signal_score": int(row.get("material_signal_score", 0) or 0),
                }
            )
            if len(prepass_source_rows_cleaned) >= source_budget:
                break
        asx_deterministic_ingestion_summary = {
            "enabled": bool(ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED),
            "used": False,
            "symbol": "",
            "reason": "prepass_source_rows_applied",
            "cache_hit": False,
            "fetched_rows": 0,
            "selected_rows": 0,
            "decoded_rows": 0,
            "target_rows": 0,
            "price_sensitive_only": bool(ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY),
            "include_non_sensitive_fill": bool(
                ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL
            ),
            "years_queried": [],
            "errors": [],
        }
    else:
        run, asx_deterministic_ingestion_summary = await _augment_run_with_deterministic_asx_sources(
            user_query=user_query,
            research_brief=research_brief,
            run=run,
        )

    source_rows = (
        prepass_source_rows_cleaned
        if using_prepass_source_rows
        else _prepare_stage1_source_rows(
            run=run,
            max_sources=source_budget,
            max_chars_per_source=PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE,
        )
    )
    if using_prepass_source_rows:
        cashflow_detection_limit = max(
            len(source_rows),
            int(
                profile.get(
                    "cashflow_schema_detection_max_sources",
                    STAGE1_CASHFLOW_DETECTION_MAX_SOURCES,
                )
            ),
        )
        cashflow_detection_rows = prepass_source_rows_cleaned[: max(1, cashflow_detection_limit)]
    else:
        cashflow_detection_rows = _prepare_stage1_source_rows(
            run=run,
            max_sources=max(
                len(source_rows),
                int(
                    profile.get(
                        "cashflow_schema_detection_max_sources",
                        STAGE1_CASHFLOW_DETECTION_MAX_SOURCES,
                    )
                ),
            ),
            max_chars_per_source=min(
                max(600, PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE),
                1600,
            ),
        )
    cashflow_schema_status = _detect_cashflow_schema_activation(
        source_rows=cashflow_detection_rows,
        mode=str(profile.get("cashflow_schema_mode", "disabled")),
        min_reporting_periods=int(profile.get("cashflow_schema_min_reporting_periods", 3)),
        require_operating_cashflow=bool(
            profile.get("cashflow_schema_require_operating_cashflow", True)
        ),
    )
    cashflow_schema_status["decision_source"] = "rules"
    cashflow_schema_status["detection_source_rows_count"] = int(len(cashflow_detection_rows))
    classifier_result = await _classify_cashflow_schema_with_agent(
        source_rows=cashflow_detection_rows,
        template_id=str(profile.get("template_id", "")),
        mode=str(profile.get("cashflow_schema_mode", "auto")),
        min_reporting_periods=int(profile.get("cashflow_schema_min_reporting_periods", 3)),
        require_operating_cashflow=bool(
            profile.get("cashflow_schema_require_operating_cashflow", True)
        ),
    )
    cashflow_schema_status["agent_classifier"] = classifier_result
    classifier_used = bool(isinstance(classifier_result, dict) and classifier_result.get("used", False))
    classifier_active = (
        bool(classifier_result.get("active"))
        if classifier_used and isinstance(classifier_result, dict)
        else None
    )
    classifier_confidence = 0.0
    if classifier_used and isinstance(classifier_result, dict):
        try:
            classifier_confidence = float(classifier_result.get("confidence_pct", 0.0))
        except Exception:
            classifier_confidence = 0.0
    confidence_gate = max(0.0, min(100.0, float(STAGE1_CASHFLOW_CLASSIFIER_MIN_CONFIDENCE_PCT)))
    if (
        classifier_used
        and classifier_active is not None
        and classifier_confidence >= confidence_gate
    ):
        cashflow_schema_status["rules_active"] = bool(cashflow_schema_status.get("active", False))
        cashflow_schema_status["active"] = bool(classifier_active)
        cashflow_schema_status["decision_source"] = "agent_high_confidence"
        cashflow_schema_status["reason"] = (
            f"{str(cashflow_schema_status.get('reason', '')).strip()}|"
            f"agent:{str(classifier_result.get('reason', '')).strip()}|"
            f"confidence:{classifier_confidence:.1f}"
        ).strip("|")
    elif (
        classifier_used
        and classifier_active is not None
        and classifier_active == bool(cashflow_schema_status.get("active", False))
    ):
        cashflow_schema_status["decision_source"] = "rules_confirmed_by_agent"
    cashflow_schema_contract = (
        _build_cashflow_schema_contract_text()
        if bool(cashflow_schema_status.get("active", False))
        else ""
    )
    if bool(cashflow_schema_status.get("active", False)):
        cashflow_section_id = "cashflow_analysis"
        existing_section_ids = {
            str(item[0]).strip().lower()
            for item in profile_section_markers
            if isinstance(item, (list, tuple)) and len(item) == 2
        }
        if cashflow_section_id not in existing_section_ids:
            profile_section_markers.append(
                (
                    cashflow_section_id,
                    [
                        "cashflow analysis",
                        "cash flow analysis",
                        "historical / current / forward",
                        "operating cash flow",
                        "free cash flow",
                    ],
                )
            )
        profile_critical_sections.add(cashflow_section_id)
    if isinstance(supplementary_macro_news_override, dict):
        # Reuse one shared supplementary macro brief across all Stage-1 model
        # second-pass calls in the same run to keep evidence injection consistent.
        supplementary_macro_news = copy.deepcopy(supplementary_macro_news_override)
    else:
        supplementary_macro_news = await _collect_stage1_supplementary_macro_news(
            model=model,
            user_query=user_query,
            run=run,
            template_id=str(profile.get("template_id", "")),
            existing_source_rows=source_rows,
        )
    supplementary_macro_news_sources = list(supplementary_macro_news.get("sources", []) or [])
    supplementary_macro_news_summary = str(
        supplementary_macro_news.get("summary_paragraph", "")
    ).strip()
    supplementary_macro_news_prompt_payload = {
        "segment": "supplementary_macro_news",
        "news_text": _truncate_text_for_prompt(
            supplementary_macro_news_summary,
            2400,
        ),
    }
    supplementary_macro_news_json = json.dumps(
        supplementary_macro_news_prompt_payload,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    timeline_rows = _extract_stage1_timeline_evidence(
        source_rows,
        max_items=PERPLEXITY_STAGE1_TIMELINE_DIGEST_MAX_ITEMS,
        timeline_terms=profile_timeline_terms,
        timeline_focus_terms=profile_timeline_focus_terms,
    )
    fact_digest: Dict[str, Any] = {}
    if PERPLEXITY_STAGE1_FACT_DIGEST_V2_ENABLED:
        fact_digest = _build_stage1_fact_digest_v2(
            source_rows,
            timeline_rows,
            section_keywords=profile_fact_keywords,
            narrative_order=profile_narrative_order,
            conflict_terms=profile_timeline_focus_terms,
            conflict_field=profile_conflict_field,
            conflict_resolution_rule=profile_conflict_resolution_rule,
        )

    fact_pack = _build_stage1_rubric_fact_pack(source_rows)
    compact_fact_bundle = _build_stage1_compact_fact_bundle(
        source_rows=source_rows,
        fact_digest=fact_digest,
        fact_pack=fact_pack,
        timeline_rows=timeline_rows,
        max_facts_per_category=5,
    )
    mandatory_fact_ledger = _build_stage1_mandatory_fact_ledger(
        source_rows,
        template_id=str(profile.get("template_id", "")),
    )
    prompt_fact_chars = max(
        100,
        int(PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS),
    )
    fact_digest_prompt = _build_stage1_prompt_fact_digest(
        fact_digest,
        max_rows_per_section=2,
        max_fact_chars=prompt_fact_chars,
        max_summary_bullets=8,
    )
    fact_pack_prompt = _build_stage1_prompt_fact_pack(
        fact_pack,
        max_rows_per_section=2,
        max_fact_chars=prompt_fact_chars,
    )
    compact_fact_bundle_prompt = _build_stage1_prompt_compact_fact_bundle(
        compact_fact_bundle,
        max_rows_per_category=3,
        max_fact_chars=prompt_fact_chars,
    )
    fact_digest_json = json.dumps(
        fact_digest_prompt or fact_digest,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    fact_pack_json = json.dumps(
        fact_pack_prompt or fact_pack,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    compact_fact_bundle_json = json.dumps(
        compact_fact_bundle_prompt or compact_fact_bundle,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    mandatory_fact_ledger_json = json.dumps(
        mandatory_fact_ledger,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    timeline_digest_block = _build_stage1_timeline_digest_block(timeline_rows)
    appendix_source_count = max(
        1,
        min(
            int(PERPLEXITY_STAGE1_SECOND_PASS_APPENDIX_MAX_SOURCES),
            max(1, len(source_rows)),
        ),
    )
    if using_prepass_source_rows:
        appendix_rows = [
            {
                **row,
                "excerpt": str(row.get("excerpt", ""))[
                    : min(450, int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE))
                ],
            }
            for row in source_rows[:appendix_source_count]
        ]
    else:
        appendix_rows = _prepare_stage1_source_rows(
            run=run,
            max_sources=appendix_source_count,
            max_chars_per_source=min(450, PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE),
        )
    evidence = _build_stage1_decoded_evidence_block(appendix_rows)
    source_key_points_bundle = _build_stage1_doc_key_points_bundle(
        source_rows,
        max_points_per_source=PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_PER_SOURCE,
        max_words_per_source=PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_WORDS_PER_SOURCE,
        max_fact_chars=prompt_fact_chars,
    )
    source_key_points_prompt = {
        "schema": "source_key_points_v1",
        "sources": [],
        "counts": source_key_points_bundle.get("counts", {}) or {},
    }
    for item in (source_key_points_bundle.get("sources", []) or [])[:10]:
        if not isinstance(item, dict):
            continue
        key_points = []
        for point in (item.get("key_points", []) or [])[:4]:
            if not isinstance(point, dict):
                continue
            fact = _truncate_text_for_prompt(str(point.get("fact", "")), prompt_fact_chars)
            if not fact:
                continue
            key_points.append(
                {
                    "tag": str(point.get("tag", "")),
                    "fact": fact,
                }
            )
        source_key_points_prompt["sources"].append(
            {
                "source_id": str(item.get("source_id", "")),
                "title": _truncate_text_for_prompt(str(item.get("title", "")), 100),
                "published_at": str(item.get("published_at", "")),
                "key_points": key_points,
            }
        )
    source_key_points_json = json.dumps(
        source_key_points_prompt,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    prompt_target_chars = max(10000, int(PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_TARGET_CHARS))
    prompt_compression_enabled = bool(PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_COMPRESSION_ENABLED)
    prompt_compression_applied = False
    prompt_compression_appendix_omitted = False

    prompt = _build_stage1_second_pass_prompt(
        user_query=user_query,
        research_brief=research_brief,
        run=run,
        mandatory_fact_ledger_json=mandatory_fact_ledger_json,
        source_key_points_json=source_key_points_json,
        supplementary_macro_news_json=supplementary_macro_news_json,
        compact_fact_bundle_json=compact_fact_bundle_json,
        fact_digest_json=fact_digest_json,
        fact_pack_json=fact_pack_json,
        evidence_appendix=evidence.get("block", ""),
        timeline_digest=timeline_digest_block,
        cashflow_schema_contract=cashflow_schema_contract,
    )
    prompt_chars_before_compression = len(prompt)

    if prompt_compression_enabled and prompt_chars_before_compression > prompt_target_chars:
        fact_digest_slim = {
            "schema": str(fact_digest_prompt.get("schema", "fact_digest_v2")),
            "counts": fact_digest_prompt.get("counts", {}) or {},
            "conflicts": list((fact_digest_prompt.get("conflicts", []) or [])[:4]),
        }
        fact_pack_slim = {
            "schema": str(fact_pack_prompt.get("schema", "rubric_fact_pack_v1")),
            "counts": fact_pack_prompt.get("counts", {}) or {},
            "critical_gaps": list((fact_pack_prompt.get("critical_gaps", []) or [])[:8]),
        }
        compact_categories_slim = {}
        for category_name, rows in (compact_fact_bundle_prompt.get("categories", {}) or {}).items():
            if not isinstance(rows, list) or not rows:
                continue
            first = rows[0] if isinstance(rows[0], dict) else {}
            compact_row = _compact_prompt_fact_row(first, prompt_fact_chars)
            if compact_row:
                compact_categories_slim[str(category_name)] = [compact_row]
        compact_fact_bundle_slim = {
            "schema": str(compact_fact_bundle_prompt.get("schema", "compact_fact_bundle_v1")),
            "source_index": list((compact_fact_bundle_prompt.get("source_index", []) or [])[:10]),
            "categories": compact_categories_slim,
            "critical_gaps": list((compact_fact_bundle_prompt.get("critical_gaps", []) or [])[:8]),
            "counts": compact_fact_bundle_prompt.get("counts", {}) or {},
        }
        prompt = _build_stage1_second_pass_prompt(
            user_query=user_query,
            research_brief=research_brief,
            run=run,
            mandatory_fact_ledger_json=mandatory_fact_ledger_json,
            source_key_points_json=source_key_points_json,
            supplementary_macro_news_json=supplementary_macro_news_json,
            compact_fact_bundle_json=json.dumps(
                compact_fact_bundle_slim,
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            fact_digest_json=json.dumps(
                fact_digest_slim,
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            fact_pack_json=json.dumps(
                fact_pack_slim,
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            evidence_appendix="(omitted due to prompt budget; represented in Source Key Points)",
            timeline_digest=timeline_digest_block,
            cashflow_schema_contract=cashflow_schema_contract,
        )
        if len(prompt) > prompt_target_chars:
            source_key_points_tiny = {
                "schema": "source_key_points_v1",
                "counts": source_key_points_prompt.get("counts", {}) or {},
                "sources": [],
            }
            for item in (source_key_points_prompt.get("sources", []) or [])[:8]:
                if not isinstance(item, dict):
                    continue
                tiny_points = []
                for point in (item.get("key_points", []) or [])[:1]:
                    if not isinstance(point, dict):
                        continue
                    tiny_fact = _truncate_text_for_prompt(
                        str(point.get("fact", "")),
                        max(120, prompt_fact_chars // 2),
                    )
                    if tiny_fact:
                        tiny_points.append(
                            {
                                "tag": str(point.get("tag", "")),
                                "fact": tiny_fact,
                            }
                        )
                source_key_points_tiny["sources"].append(
                    {
                        "source_id": str(item.get("source_id", "")),
                        "published_at": str(item.get("published_at", "")),
                        "key_points": tiny_points,
                    }
                )
            prompt = _build_stage1_second_pass_prompt(
                user_query=user_query,
                research_brief=research_brief,
                run=run,
                mandatory_fact_ledger_json=mandatory_fact_ledger_json,
                source_key_points_json=json.dumps(
                    source_key_points_tiny,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                supplementary_macro_news_json=supplementary_macro_news_json,
                compact_fact_bundle_json=json.dumps(
                    compact_fact_bundle_slim,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                fact_digest_json=json.dumps(
                    fact_digest_slim,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                fact_pack_json=json.dumps(
                    fact_pack_slim,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                evidence_appendix="(omitted due to prompt budget; represented in Source Key Points)",
                timeline_digest=timeline_digest_block,
                cashflow_schema_contract=cashflow_schema_contract,
            )
        prompt_compression_applied = True
        prompt_compression_appendix_omitted = True

    prompt_chars_after_compression = len(prompt)
    mandatory_fact_prompt_coverage = _validate_stage1_prompt_mandatory_fact_coverage(
        prompt,
        mandatory_fact_ledger,
    )
    source_rows_preview = [
        {
            "source_id": str(row.get("source_id", "")),
            "published_at": str(row.get("published_at", "")),
            "title": str(row.get("title", ""))[:180],
            "is_low_signal_notice": bool(_is_low_signal_notice_source_item(row)),
            "asx_deterministic": bool(row.get("asx_deterministic", False)),
            "asx_price_sensitive": bool(row.get("asx_price_sensitive", False)),
        }
        for row in source_rows[:10]
    ]
    compact_categories = (compact_fact_bundle.get("categories", {}) or {})
    compact_category_preview = {
        key: [
            {
                "source_id": str(item.get("source_id", "")),
                "fact": str(item.get("fact", ""))[:220],
            }
            for item in (rows or [])[:2]
            if isinstance(item, dict)
        ]
        for key, rows in compact_categories.items()
    }
    supplementary_summary = str(supplementary_macro_news.get("summary_paragraph", "")).strip()
    supplementary_preview = (
        [
            {
                "type": "xai_sector_macro_brief",
                "sector_label": str(supplementary_macro_news.get("sector_label", "")),
                "summary_provider": str(supplementary_macro_news.get("summary_provider", "")),
                "summary_model": str(supplementary_macro_news.get("summary_model", "")),
                "summary_preview": _truncate_text_for_prompt(supplementary_summary, 320),
            }
        ]
        if supplementary_summary
        else []
    )
    injection_audit = {
        "template_id": str(profile.get("template_id", "")),
        "prepass_source_rows_used": bool(using_prepass_source_rows),
        "prepass_source_rows_count": int(len(prepass_source_rows_cleaned)),
        "cashflow_schema": cashflow_schema_status,
        "source_rows_preview": source_rows_preview,
        "supplementary_macro_news_preview": supplementary_preview,
        "supplementary_macro_news_used": bool(supplementary_macro_news.get("used", False)),
        "supplementary_macro_news_reason": str(supplementary_macro_news.get("reason", "")),
        "supplementary_macro_news_profile": str(supplementary_macro_news.get("commodity_profile", "")),
        "compact_fact_bundle_preview": compact_category_preview,
        "fact_digest_counts": (fact_digest.get("counts", {}) or {}),
        "fact_pack_counts": (fact_pack.get("counts", {}) or {}),
        "timeline_evidence_count": len(timeline_rows),
        "asx_deterministic_ingestion": asx_deterministic_ingestion_summary,
        "source_key_points_counts": (source_key_points_bundle.get("counts", {}) or {}),
        "mandatory_fact_ledger_counts": (mandatory_fact_ledger.get("counts", {}) or {}),
        "mandatory_fact_prompt_coverage": mandatory_fact_prompt_coverage,
        "prompt_compression_enabled": prompt_compression_enabled,
        "prompt_compression_applied": prompt_compression_applied,
        "prompt_compression_appendix_omitted": prompt_compression_appendix_omitted,
        "prompt_target_chars": prompt_target_chars,
        "prompt_chars_before_compression": prompt_chars_before_compression,
        "prompt_chars_after_compression": prompt_chars_after_compression,
        "prompt_chars_saved": max(
            0,
            int(prompt_chars_before_compression - prompt_chars_after_compression),
        ),
        "prompt_chars": len(prompt),
    }
    _progress_log(
        f"Stage1 injection audit model={model} "
        f"sources={len(source_rows_preview)} "
        f"supplementary_sources={len(supplementary_preview)} "
        f"compact_categories={len(compact_category_preview)} "
        f"prompt_chars={len(prompt)} "
        f"compressed={prompt_compression_applied}"
    )
    if not bool(mandatory_fact_prompt_coverage.get("passed", True)):
        missing_ids = list(mandatory_fact_prompt_coverage.get("missing_fact_ids", []) or [])
        _progress_log(
            f"Stage1 mandatory fact coverage failed model={model} "
            f"missing={missing_ids[:8]}"
        )
        return {
            "success": False,
            "response": "",
            "attempts": 0,
            "error": "mandatory_fact_prompt_coverage_failed",
            "warning": "mandatory_fact_prompt_coverage_failed",
            "prompt": prompt,
            "prompt_chars": len(prompt),
            "prompt_chars_before_compression": int(prompt_chars_before_compression),
            "prompt_chars_after_compression": int(prompt_chars_after_compression),
            "prompt_chars_saved": max(
                0,
                int(prompt_chars_before_compression - prompt_chars_after_compression),
            ),
            "prompt_target_chars": int(prompt_target_chars),
            "prompt_compression_enabled": bool(prompt_compression_enabled),
            "prompt_compression_applied": bool(prompt_compression_applied),
            "prompt_compression_appendix_omitted": bool(prompt_compression_appendix_omitted),
            "response_chars": 0,
            "last_model_finish_reason": "",
            "last_model_response_id": "",
            "last_model_usage": {},
            "last_model_provider": "",
            "last_model_reasoning_effort": "",
            "truncation_assessment": {
                "used": False,
                "truncated": False,
                "confidence_pct": 0.0,
                "reason": "not_called_mandatory_fact_prompt_coverage_failed",
            },
            "source_rows": source_rows,
            "supplementary_macro_news": supplementary_macro_news,
            "supplementary_macro_news_sources": supplementary_macro_news_sources,
            "supplementary_macro_news_count": int(
                supplementary_macro_news.get(
                    "count",
                    1 if str(supplementary_macro_news.get("summary_paragraph", "")).strip() else 0,
                )
            ),
            "supplementary_macro_news_profile": str(
                supplementary_macro_news.get("commodity_profile", "")
            ),
            "supplementary_macro_news_reason": str(
                supplementary_macro_news.get("reason", "")
            ),
            "supplementary_macro_news_retrieval_attempted": bool(
                supplementary_macro_news.get("retrieval_attempted", False)
            ),
            "supplementary_macro_news_retrieval_result_count": int(
                supplementary_macro_news.get("retrieval_result_count", 0)
            ),
            "supplementary_macro_news_retrieval_error": str(
                supplementary_macro_news.get("retrieval_error", "")
            ),
            "evidence_source_count": int(fact_pack.get("counts", {}).get("source_count", 0)),
            "decoded_source_count": int(
                fact_pack.get("counts", {}).get("decoded_source_count", 0)
            ),
            "evidence_total_excerpt_chars": int(
                sum(len(str(row.get("excerpt", ""))) for row in source_rows)
            ),
            "source_key_points_counts": (source_key_points_bundle.get("counts", {}) or {}),
            "mandatory_fact_ledger": mandatory_fact_ledger,
            "mandatory_fact_ledger_chars": len(mandatory_fact_ledger_json),
            "mandatory_fact_prompt_coverage": mandatory_fact_prompt_coverage,
            "fact_digest_v2": fact_digest,
            "fact_digest_v2_chars": len(fact_digest_json),
            "fact_digest_v2_total_facts": int(
                (fact_digest.get("counts", {}) or {}).get("total_facts", 0)
            ),
            "fact_digest_v2_sections_with_facts": int(
                (fact_digest.get("counts", {}) or {}).get("sections_with_facts", 0)
            ),
            "fact_digest_v2_summary_bullets": int(
                (fact_digest.get("counts", {}) or {}).get("summary_bullets", 0)
            ),
            "fact_digest_v2_conflicts": int(
                (fact_digest.get("counts", {}) or {}).get("conflicts", 0)
            ),
            "compact_fact_bundle": compact_fact_bundle,
            "compact_fact_bundle_chars": len(compact_fact_bundle_json),
            "compact_fact_bundle_total_facts": int(
                (compact_fact_bundle.get("counts", {}) or {}).get("total_facts", 0)
            ),
            "compact_fact_bundle_categories_with_facts": int(
                (compact_fact_bundle.get("counts", {}) or {}).get(
                    "categories_with_facts",
                    0,
                )
            ),
            "fact_pack": fact_pack,
            "fact_pack_chars": len(fact_pack_json),
            "timeline_evidence": timeline_rows,
            "timeline_digest_chars": len(timeline_digest_block),
            "timeline_guard_enabled": bool(PERPLEXITY_STAGE1_TIMELINE_GUARD_ENABLED),
            "timeline_guard_passed": True,
            "timeline_guard_reason": "not_called_mandatory_fact_prompt_coverage_failed",
            "timeline_guard_evidence_windows": [],
            "timeline_guard_response_windows": [],
            "timeline_guard_shifted_quarters": 0,
            "verification_profile_template_id": str(profile.get("template_id", "")),
            "verification_profile_digest_sections": int(
                len((profile_fact_keywords or {}).keys())
            ),
            "verification_profile_compliance_markers": int(
                len(profile_section_markers or [])
            ),
            "verification_profile_critical_sections": int(
                len(profile_critical_sections or set())
            ),
            "cashflow_schema": cashflow_schema_status,
            "injection_audit": injection_audit,
            "asx_deterministic_ingestion": asx_deterministic_ingestion_summary,
            "fact_pack_total_facts": int(fact_pack.get("counts", {}).get("total_facts", 0)),
            "fact_pack_sections_with_facts": int(
                fact_pack.get("counts", {}).get("sections_with_facts", 0)
            ),
            "citation_gate_enabled": bool(PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED),
            "citation_gate_passed": False,
            "citation_gate_reason": "mandatory_fact_prompt_coverage_failed",
            "citation_count": 0,
            "citation_unique_count": 0,
            "citation_invalid_source_refs": [],
            "citation_numeric_lines": 0,
            "citation_uncited_numeric_lines": 0,
            "citation_cited_numeric_lines": 0,
            "citation_numeric_citation_pct": 0.0,
            "rubric_required": False,
            "rubric_sections_total": 0,
            "rubric_sections_covered": 0,
            "rubric_coverage_pct": 0.0,
            "rubric_missing_sections": [],
            "rubric_critical_missing_sections": [],
            "compliance_score": 0.0,
            "compliance_rating": "red",
            "compliance_retry_recommended": False,
            "compliance_catastrophic_failure": True,
            "compliance_fail_reasons": ["mandatory_fact_prompt_coverage_failed"],
            "compliance_warning_reasons": [],
            "compliance_hard_fail_reasons": ["mandatory_fact_prompt_coverage_failed"],
            "compliance_soft_fail_reasons": [],
        }

    max_attempts = max(1, int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS))
    backoff = max(0.0, float(PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS))
    timeout = max(30.0, float(PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS))
    configured_reasoning_effort = normalize_reasoning_effort(
        PERPLEXITY_STAGE1_SECOND_PASS_REASONING_EFFORT or ""
    )
    if _is_gpt_5_4_model(model):
        configured_reasoning_effort = "low"
    attempts_used = 0
    last_error = ""
    last_warning = ""
    last_response_finish_reason = ""
    last_response_id = ""
    last_response_usage: Dict[str, Any] = {}
    last_response_provider = ""
    last_response_had_content = False
    last_output_tokens_used = 0
    last_reasoning_effort_applied = configured_reasoning_effort
    last_truncation_assessment: Dict[str, Any] = {
        "used": False,
        "truncated": False,
        "confidence_pct": 0.0,
        "reason": "not_evaluated",
    }
    last_timeline_guard: Dict[str, Any] = {
        "enabled": bool(PERPLEXITY_STAGE1_TIMELINE_GUARD_ENABLED),
        "passed": True,
        "reason": "not_evaluated",
        "evidence_windows": [],
        "response_windows": [],
        "shifted_quarters": 0,
    }
    last_gate: Dict[str, Any] = {
        "enabled": bool(PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED),
        "passed": True,
        "reason": "not_evaluated",
        "citation_count": 0,
        "unique_citation_count": 0,
        "invalid_citations": [],
        "numeric_lines": 0,
        "uncited_numeric_lines": 0,
        "cited_numeric_lines": 0,
        "numeric_citation_pct": 0.0,
        "rubric_required": False,
        "rubric_sections_total": 0,
        "rubric_sections_covered": 0,
        "rubric_coverage_pct": 0.0,
        "rubric_missing_sections": [],
        "rubric_critical_missing_sections": [],
        "compliance_score": 0.0,
        "compliance_rating": "unknown",
        "retry_recommended": False,
        "catastrophic_failure": False,
        "compliance_fail_reasons": [],
        "compliance_warning_reasons": [],
        "compliance_hard_fail_reasons": [],
        "compliance_soft_fail_reasons": [],
    }
    prompt_used = prompt
    valid_source_ids = [
        str(row.get("source_id", "")).strip()
        for row in (source_rows + supplementary_macro_news_sources)
        if isinstance(row, dict)
    ]

    for attempt in range(1, max_attempts + 1):
        attempts_used = attempt
        if attempt > 1 and backoff > 0:
            sleep_seconds = backoff * (2 ** (attempt - 2))
            _progress_log(
                f"Stage1 second-pass backoff for {model}: sleeping {sleep_seconds:.1f}s "
                f"(attempt {attempt}/{max_attempts})"
            )
            import asyncio
            await asyncio.sleep(sleep_seconds)

        prompt_for_attempt = prompt
        if (
            attempt > 1
            and PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED
            and not bool(last_gate.get("passed", True))
        ):
            prompt_for_attempt = _build_stage1_citation_repair_prompt(prompt, last_gate)
        prompt_used = prompt_for_attempt
        attempt_reasoning_effort = normalize_reasoning_effort(configured_reasoning_effort)
        if attempt > 1:
            # Apply strict staged degradation by retry index:
            # xhigh -> high -> medium -> low (never jump directly to low).
            step_down = {
                "xhigh": "high",
                "high": "medium",
                "medium": "low",
                "low": "low",
                "minimal": "minimal",
            }
            for _ in range(attempt - 1):
                attempt_reasoning_effort = step_down.get(
                    attempt_reasoning_effort,
                    attempt_reasoning_effort,
                )
        effective_reasoning_for_attempt = attempt_reasoning_effort
        if (
            str(analysis_provider).strip().lower() == "perplexity"
            and not _supports_perplexity_reasoning_payload(model)
        ):
            effective_reasoning_for_attempt = "low"
        last_reasoning_effort_applied = effective_reasoning_for_attempt

        _progress_log(
            f"Stage1 second-pass start model={model} attempt={attempt}/{max_attempts} "
            f"sources={len(source_rows)} "
            f"decoded_sources={fact_pack.get('counts', {}).get('decoded_source_count', 0)} "
            f"digest_facts={(fact_digest.get('counts', {}) or {}).get('total_facts', 0)} "
            f"prompt_chars={len(prompt_for_attempt)} "
            f"reasoning_effort={effective_reasoning_for_attempt or 'default'} "
            f"analysis_provider={analysis_provider}"
        )
        if str(analysis_provider).strip().lower() == "perplexity":
            response = await _query_model_via_perplexity(
                model=model,
                prompt=prompt_for_attempt,
                timeout=timeout,
                max_tokens=int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_OUTPUT_TOKENS),
                reasoning_effort=attempt_reasoning_effort,
            )
        else:
            response = await query_model(
                model,
                [{"role": "user", "content": prompt_for_attempt}],
                timeout=timeout,
                max_tokens=int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_OUTPUT_TOKENS),
                reasoning_effort=attempt_reasoning_effort,
                include_error_details=True,
            )
        if response and response.get("error"):
            last_response_provider = str(response.get("provider", "") or "")
            last_error = str(response.get("error", "") or "second_pass_failed")
            error_type = str(response.get("error_type", "") or "unknown")
            error_status = response.get("error_status")
            retryable = bool(response.get("error_retryable", False))
            last_truncation_assessment = {
                "used": False,
                "truncated": False,
                "confidence_pct": 100.0 if not retryable else 0.0,
                "reason": error_type,
            }
            _progress_log(
                f"Stage1 second-pass model error model={model} "
                f"attempt={attempt}/{max_attempts} "
                f"type={error_type} status={error_status} retryable={retryable} "
                f"error={last_error}"
            )
            if retryable and attempt < max_attempts:
                continue
            break
        if response:
            last_response_finish_reason = str(response.get("finish_reason", "") or "")
            last_response_id = str(response.get("id", "") or "")
            usage_obj = response.get("usage")
            last_response_usage = usage_obj if isinstance(usage_obj, dict) else {}
            last_output_tokens_used = int(last_response_usage.get("output_tokens", 0) or 0)
            last_response_provider = str(response.get("provider", "") or "")
            response_reasoning_effort = str(
                response.get("reasoning_effort_effective", "") or ""
            ).strip().lower()
            if response_reasoning_effort in {"xhigh", "high", "medium", "low", "minimal"}:
                last_reasoning_effort_applied = response_reasoning_effort
            elif str(analysis_provider).strip().lower() == "perplexity":
                if response.get("reasoning_payload_sent") is False:
                    last_reasoning_effort_applied = "low"
        content = ""
        if response and response.get("content"):
            content = str(response.get("content", "")).strip()
            last_response_had_content = bool(content)
        elif response:
            last_response_had_content = False
            _progress_log(
                f"Stage1 second-pass non-text response model={model} "
                f"attempt={attempt}/{max_attempts} "
                f"finish_reason={response.get('finish_reason')} "
                f"usage={response.get('usage')} "
                f"response_id={response.get('id')}"
            )
        else:
            last_response_had_content = False

        if content:
            output_tokens_used = int(
                ((response or {}).get("usage", {}) or {}).get("output_tokens", 0) or 0
            )
            truncation_assessment = await _assess_stage1_truncation(
                model=model,
                response_text=content,
                output_tokens_used=output_tokens_used,
                finish_reason=last_response_finish_reason,
            )
            last_truncation_assessment = truncation_assessment
            if bool(truncation_assessment.get("truncated")) and attempt < max_attempts:
                reason = str(truncation_assessment.get("reason", "truncated_response")) or "truncated_response"
                _progress_log(
                    f"Stage1 second-pass retry trigger model={model} "
                    f"attempt={attempt}/{max_attempts} reason={reason} "
                    f"output_tokens={output_tokens_used} response_chars={len(content)} "
                    f"confidence_pct={float(truncation_assessment.get('confidence_pct', 0.0)):.1f}"
                )
                prompt = _build_stage1_truncation_repair_prompt(prompt)
                continue
            if len(content) < int(_STAGE1_SECOND_PASS_MIN_RESPONSE_CHARS):
                _progress_log(
                    f"Stage1 second-pass retry trigger model={model} "
                    f"attempt={attempt}/{max_attempts} reason=response_too_short "
                    f"response_chars={len(content)} min_required={_STAGE1_SECOND_PASS_MIN_RESPONSE_CHARS}"
                )
                last_error = "response_too_short"
                if attempt < max_attempts:
                    continue
                # Final attempt returned unusable short output.
                last_warning = "response_too_short_unusable"
                break

            gate = _evaluate_stage1_citation_gate(
                response_text=content,
                valid_source_ids=valid_source_ids,
                user_query=user_query,
                research_brief=research_brief,
                section_markers=profile_section_markers,
                critical_sections=profile_critical_sections,
            )
            timeline_guard = _evaluate_stage1_timeline_guard(
                content,
                timeline_rows,
                focus_terms=profile_timeline_focus_terms,
                conflict_field=profile_conflict_field,
                max_shift_quarters=profile_conflict_max_shift_quarters,
            )
            last_timeline_guard = timeline_guard
            last_gate = gate

            gate_failed = bool(gate.get("enabled")) and not bool(gate.get("passed"))
            retry_recommended = bool(gate.get("retry_recommended", False))
            if gate_failed:
                last_warning = f"conformance_gate_failed:{gate.get('reason', 'unknown')}"
                _progress_log(
                    f"Stage1 second-pass conformance gate failed model={model} "
                    f"attempt={attempt}/{max_attempts} reason={gate.get('reason')} "
                    f"score={float(gate.get('compliance_score', 0.0)):.2f} "
                    f"rating={gate.get('compliance_rating', 'unknown')} "
                    f"retry_recommended={retry_recommended}"
                )
                if retry_recommended and attempt < max_attempts:
                    continue
            else:
                last_warning = ""

            _progress_log(
                f"Stage1 second-pass success model={model} "
                f"attempt={attempt}/{max_attempts} response_chars={len(content)}"
            )
            return {
                "success": True,
                "response": content,
                "attempts": attempts_used,
                "error": "",
                "warning": last_warning,
                "prompt": prompt_used,
                "prompt_chars": len(prompt_used),
                "prompt_chars_before_compression": int(prompt_chars_before_compression),
                "prompt_chars_after_compression": int(prompt_chars_after_compression),
                "prompt_chars_saved": max(
                    0,
                    int(prompt_chars_before_compression - prompt_chars_after_compression),
                ),
                "prompt_target_chars": int(prompt_target_chars),
                "prompt_compression_enabled": bool(prompt_compression_enabled),
                "prompt_compression_applied": bool(prompt_compression_applied),
                "prompt_compression_appendix_omitted": bool(
                    prompt_compression_appendix_omitted
                ),
                "response_chars": len(content),
                "last_model_finish_reason": last_response_finish_reason,
                "last_model_response_id": last_response_id,
                "last_model_usage": last_response_usage,
                "last_model_provider": last_response_provider,
                "last_model_reasoning_effort": last_reasoning_effort_applied,
                "truncation_assessment": last_truncation_assessment,
                "source_rows": source_rows,
                "supplementary_macro_news": supplementary_macro_news,
                "supplementary_macro_news_sources": supplementary_macro_news_sources,
                "supplementary_macro_news_count": int(
                    supplementary_macro_news.get(
                        "count",
                        1 if str(supplementary_macro_news.get("summary_paragraph", "")).strip() else 0,
                    )
                ),
                "supplementary_macro_news_profile": str(
                    supplementary_macro_news.get("commodity_profile", "")
                ),
                "supplementary_macro_news_reason": str(
                    supplementary_macro_news.get("reason", "")
                ),
                "supplementary_macro_news_retrieval_attempted": bool(
                    supplementary_macro_news.get("retrieval_attempted", False)
                ),
                "supplementary_macro_news_retrieval_result_count": int(
                    supplementary_macro_news.get("retrieval_result_count", 0)
                ),
                "supplementary_macro_news_retrieval_error": str(
                    supplementary_macro_news.get("retrieval_error", "")
                ),
                "evidence_source_count": int(fact_pack.get("counts", {}).get("source_count", 0)),
                "decoded_source_count": int(
                    fact_pack.get("counts", {}).get("decoded_source_count", 0)
                ),
                "evidence_total_excerpt_chars": int(
                    sum(len(str(row.get("excerpt", ""))) for row in source_rows)
                ),
                "source_key_points_counts": (source_key_points_bundle.get("counts", {}) or {}),
                "mandatory_fact_ledger": mandatory_fact_ledger,
                "mandatory_fact_ledger_chars": len(mandatory_fact_ledger_json),
                "mandatory_fact_prompt_coverage": mandatory_fact_prompt_coverage,
                "fact_digest_v2": fact_digest,
                "fact_digest_v2_chars": len(fact_digest_json),
                "fact_digest_v2_total_facts": int(
                    (fact_digest.get("counts", {}) or {}).get("total_facts", 0)
                ),
                "fact_digest_v2_sections_with_facts": int(
                    (fact_digest.get("counts", {}) or {}).get("sections_with_facts", 0)
                ),
                "fact_digest_v2_summary_bullets": int(
                    (fact_digest.get("counts", {}) or {}).get("summary_bullets", 0)
                ),
                "fact_digest_v2_conflicts": int(
                    (fact_digest.get("counts", {}) or {}).get("conflicts", 0)
                ),
                "compact_fact_bundle": compact_fact_bundle,
                "compact_fact_bundle_chars": len(compact_fact_bundle_json),
                "compact_fact_bundle_total_facts": int(
                    (compact_fact_bundle.get("counts", {}) or {}).get("total_facts", 0)
                ),
                "compact_fact_bundle_categories_with_facts": int(
                    (compact_fact_bundle.get("counts", {}) or {}).get(
                        "categories_with_facts",
                        0,
                    )
                ),
                "fact_pack": fact_pack,
                "fact_pack_chars": len(fact_pack_json),
                "timeline_evidence": timeline_rows,
                "timeline_digest_chars": len(timeline_digest_block),
                "timeline_guard_enabled": bool(timeline_guard.get("enabled", False)),
                "timeline_guard_passed": bool(timeline_guard.get("passed", True)),
                "timeline_guard_reason": str(timeline_guard.get("reason", "")),
                "timeline_guard_evidence_windows": list(
                    timeline_guard.get("evidence_windows", []) or []
                ),
                "timeline_guard_response_windows": list(
                    timeline_guard.get("response_windows", []) or []
                ),
                "timeline_guard_shifted_quarters": int(
                    timeline_guard.get("shifted_quarters", 0) or 0
                ),
                "verification_profile_template_id": str(profile.get("template_id", "")),
                "verification_profile_digest_sections": int(
                    len((profile_fact_keywords or {}).keys())
                ),
                "verification_profile_compliance_markers": int(
                    len(profile_section_markers or [])
                ),
                "verification_profile_critical_sections": int(
                    len(profile_critical_sections or set())
                ),
                "cashflow_schema": cashflow_schema_status,
                "injection_audit": injection_audit,
                "asx_deterministic_ingestion": asx_deterministic_ingestion_summary,
                "fact_pack_total_facts": int(fact_pack.get("counts", {}).get("total_facts", 0)),
                "fact_pack_sections_with_facts": int(
                    fact_pack.get("counts", {}).get("sections_with_facts", 0)
                ),
                "citation_gate_enabled": bool(gate.get("enabled", False)),
                "citation_gate_passed": bool(gate.get("passed", True)),
                "citation_gate_reason": str(gate.get("reason", "")),
                "citation_count": int(gate.get("citation_count", 0)),
                "citation_unique_count": int(gate.get("unique_citation_count", 0)),
                "citation_invalid_source_refs": list(gate.get("invalid_citations", []) or []),
                "citation_numeric_lines": int(gate.get("numeric_lines", 0)),
                "citation_uncited_numeric_lines": int(gate.get("uncited_numeric_lines", 0)),
                "citation_cited_numeric_lines": int(gate.get("cited_numeric_lines", 0)),
                "citation_numeric_citation_pct": float(gate.get("numeric_citation_pct", 0.0)),
                "rubric_required": bool(gate.get("rubric_required", False)),
                "rubric_sections_total": int(gate.get("rubric_sections_total", 0)),
                "rubric_sections_covered": int(gate.get("rubric_sections_covered", 0)),
                "rubric_coverage_pct": float(gate.get("rubric_coverage_pct", 0.0)),
                "rubric_missing_sections": list(gate.get("rubric_missing_sections", []) or []),
                "rubric_critical_missing_sections": list(
                    gate.get("rubric_critical_missing_sections", []) or []
                ),
                "compliance_score": float(gate.get("compliance_score", 0.0)),
                "compliance_rating": str(gate.get("compliance_rating", "")),
                "compliance_retry_recommended": bool(gate.get("retry_recommended", False)),
                "compliance_catastrophic_failure": bool(gate.get("catastrophic_failure", False)),
                "compliance_fail_reasons": list(gate.get("compliance_fail_reasons", []) or []),
                "compliance_warning_reasons": list(
                    gate.get("compliance_warning_reasons", []) or []
                ),
                "compliance_hard_fail_reasons": list(
                    gate.get("compliance_hard_fail_reasons", []) or []
                ),
                "compliance_soft_fail_reasons": list(
                    gate.get("compliance_soft_fail_reasons", []) or []
                ),
            }

        last_error = "empty_response"
        last_warning = ""
        last_truncation_assessment = {
            "used": False,
            "truncated": True,
            "confidence_pct": 100.0,
            "reason": "empty_response",
        }
        last_gate = {
            "enabled": bool(PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED),
            "passed": False,
            "reason": "empty_response",
            "citation_count": 0,
            "unique_citation_count": 0,
            "invalid_citations": [],
            "numeric_lines": 0,
            "uncited_numeric_lines": 0,
            "cited_numeric_lines": 0,
            "numeric_citation_pct": 0.0,
            "rubric_required": False,
            "rubric_sections_total": 0,
            "rubric_sections_covered": 0,
            "rubric_coverage_pct": 0.0,
            "rubric_missing_sections": [],
            "rubric_critical_missing_sections": [],
            "compliance_score": 0.0,
            "compliance_rating": "red",
            "retry_recommended": True,
            "catastrophic_failure": True,
            "compliance_fail_reasons": ["empty_response"],
            "compliance_warning_reasons": [],
            "compliance_hard_fail_reasons": ["empty_response"],
            "compliance_soft_fail_reasons": [],
        }
        _progress_log(
            f"Stage1 second-pass empty response model={model} "
            f"attempt={attempt}/{max_attempts}"
        )

    return {
        "success": False,
        "response": "",
        "attempts": attempts_used,
        "error": last_error or "second_pass_failed",
        "warning": last_warning,
        "prompt": prompt_used,
        "prompt_chars": len(prompt_used),
        "prompt_chars_before_compression": int(prompt_chars_before_compression),
        "prompt_chars_after_compression": int(prompt_chars_after_compression),
        "prompt_chars_saved": max(
            0,
            int(prompt_chars_before_compression - prompt_chars_after_compression),
        ),
        "prompt_target_chars": int(prompt_target_chars),
        "prompt_compression_enabled": bool(prompt_compression_enabled),
        "prompt_compression_applied": bool(prompt_compression_applied),
        "prompt_compression_appendix_omitted": bool(prompt_compression_appendix_omitted),
        "response_chars": 0,
        "last_model_finish_reason": last_response_finish_reason,
        "last_model_response_id": last_response_id,
        "last_model_usage": last_response_usage,
        "last_model_provider": last_response_provider,
        "last_model_reasoning_effort": last_reasoning_effort_applied,
        "truncation_assessment": last_truncation_assessment,
        "source_rows": source_rows,
        "supplementary_macro_news": supplementary_macro_news,
        "supplementary_macro_news_sources": supplementary_macro_news_sources,
        "supplementary_macro_news_count": int(
            supplementary_macro_news.get(
                "count",
                1 if str(supplementary_macro_news.get("summary_paragraph", "")).strip() else 0,
            )
        ),
        "supplementary_macro_news_profile": str(
            supplementary_macro_news.get("commodity_profile", "")
        ),
        "supplementary_macro_news_reason": str(supplementary_macro_news.get("reason", "")),
        "supplementary_macro_news_retrieval_attempted": bool(
            supplementary_macro_news.get("retrieval_attempted", False)
        ),
        "supplementary_macro_news_retrieval_result_count": int(
            supplementary_macro_news.get("retrieval_result_count", 0)
        ),
        "supplementary_macro_news_retrieval_error": str(
            supplementary_macro_news.get("retrieval_error", "")
        ),
        "evidence_source_count": int(fact_pack.get("counts", {}).get("source_count", 0)),
        "decoded_source_count": int(
            fact_pack.get("counts", {}).get("decoded_source_count", 0)
        ),
        "evidence_total_excerpt_chars": int(
            sum(len(str(row.get("excerpt", ""))) for row in source_rows)
        ),
        "source_key_points_counts": (source_key_points_bundle.get("counts", {}) or {}),
        "mandatory_fact_ledger": mandatory_fact_ledger,
        "mandatory_fact_ledger_chars": len(mandatory_fact_ledger_json),
        "mandatory_fact_prompt_coverage": mandatory_fact_prompt_coverage,
        "fact_digest_v2": fact_digest,
        "fact_digest_v2_chars": len(fact_digest_json),
        "fact_digest_v2_total_facts": int(
            (fact_digest.get("counts", {}) or {}).get("total_facts", 0)
        ),
        "fact_digest_v2_sections_with_facts": int(
            (fact_digest.get("counts", {}) or {}).get("sections_with_facts", 0)
        ),
        "fact_digest_v2_summary_bullets": int(
            (fact_digest.get("counts", {}) or {}).get("summary_bullets", 0)
        ),
        "fact_digest_v2_conflicts": int(
            (fact_digest.get("counts", {}) or {}).get("conflicts", 0)
        ),
        "compact_fact_bundle": compact_fact_bundle,
        "compact_fact_bundle_chars": len(compact_fact_bundle_json),
        "compact_fact_bundle_total_facts": int(
            (compact_fact_bundle.get("counts", {}) or {}).get("total_facts", 0)
        ),
        "compact_fact_bundle_categories_with_facts": int(
            (compact_fact_bundle.get("counts", {}) or {}).get(
                "categories_with_facts",
                0,
            )
        ),
        "fact_pack": fact_pack,
        "fact_pack_chars": len(fact_pack_json),
        "timeline_evidence": timeline_rows,
        "timeline_digest_chars": len(timeline_digest_block),
        "timeline_guard_enabled": bool(last_timeline_guard.get("enabled", False)),
        "timeline_guard_passed": bool(last_timeline_guard.get("passed", False)),
        "timeline_guard_reason": str(last_timeline_guard.get("reason", "")),
        "timeline_guard_evidence_windows": list(
            last_timeline_guard.get("evidence_windows", []) or []
        ),
        "timeline_guard_response_windows": list(
            last_timeline_guard.get("response_windows", []) or []
        ),
        "timeline_guard_shifted_quarters": int(
            last_timeline_guard.get("shifted_quarters", 0) or 0
        ),
        "verification_profile_template_id": str(profile.get("template_id", "")),
        "verification_profile_digest_sections": int(
            len((profile_fact_keywords or {}).keys())
        ),
        "verification_profile_compliance_markers": int(
            len(profile_section_markers or [])
        ),
        "verification_profile_critical_sections": int(
            len(profile_critical_sections or set())
        ),
        "cashflow_schema": cashflow_schema_status,
        "injection_audit": injection_audit,
        "asx_deterministic_ingestion": asx_deterministic_ingestion_summary,
        "fact_pack_total_facts": int(fact_pack.get("counts", {}).get("total_facts", 0)),
        "fact_pack_sections_with_facts": int(
            fact_pack.get("counts", {}).get("sections_with_facts", 0)
        ),
        "citation_gate_enabled": bool(last_gate.get("enabled", False)),
        "citation_gate_passed": bool(last_gate.get("passed", False)),
        "citation_gate_reason": str(last_gate.get("reason", "")),
        "citation_count": int(last_gate.get("citation_count", 0)),
        "citation_unique_count": int(last_gate.get("unique_citation_count", 0)),
        "citation_invalid_source_refs": list(last_gate.get("invalid_citations", []) or []),
        "citation_numeric_lines": int(last_gate.get("numeric_lines", 0)),
        "citation_uncited_numeric_lines": int(last_gate.get("uncited_numeric_lines", 0)),
        "citation_cited_numeric_lines": int(last_gate.get("cited_numeric_lines", 0)),
        "citation_numeric_citation_pct": float(last_gate.get("numeric_citation_pct", 0.0)),
        "rubric_required": bool(last_gate.get("rubric_required", False)),
        "rubric_sections_total": int(last_gate.get("rubric_sections_total", 0)),
        "rubric_sections_covered": int(last_gate.get("rubric_sections_covered", 0)),
        "rubric_coverage_pct": float(last_gate.get("rubric_coverage_pct", 0.0)),
        "rubric_missing_sections": list(last_gate.get("rubric_missing_sections", []) or []),
        "rubric_critical_missing_sections": list(
            last_gate.get("rubric_critical_missing_sections", []) or []
        ),
        "compliance_score": float(last_gate.get("compliance_score", 0.0)),
        "compliance_rating": str(last_gate.get("compliance_rating", "")),
        "compliance_retry_recommended": bool(last_gate.get("retry_recommended", False)),
        "compliance_catastrophic_failure": bool(last_gate.get("catastrophic_failure", False)),
        "compliance_fail_reasons": list(last_gate.get("compliance_fail_reasons", []) or []),
        "compliance_warning_reasons": list(
            last_gate.get("compliance_warning_reasons", []) or []
        ),
        "compliance_hard_fail_reasons": list(
            last_gate.get("compliance_hard_fail_reasons", []) or []
        ),
        "compliance_soft_fail_reasons": list(
            last_gate.get("compliance_soft_fail_reasons", []) or []
        ),
    }


async def _apply_stage1_second_pass(
    *,
    model: str,
    user_query: str,
    research_brief: str,
    run: Dict[str, Any],
    verification_profile: Optional[Dict[str, Any]] = None,
    supplementary_macro_news_override: Optional[Dict[str, Any]] = None,
    prepass_source_rows: Optional[List[Dict[str, Any]]] = None,
    analysis_provider: str = "openrouter",
) -> Dict[str, Any]:
    """Attach second-pass analysis/metadata to an existing Stage 1 retrieval run."""
    provider_meta = run.setdefault("provider_metadata", {})
    if not isinstance(provider_meta, dict):
        provider_meta = {}
        run["provider_metadata"] = provider_meta

    if not PERPLEXITY_STAGE1_SECOND_PASS_ENABLED:
        provider_meta["stage1_second_pass_enabled"] = False
        return run

    second_pass_result = await _run_stage1_second_pass_analysis(
        model=model,
        user_query=user_query,
        research_brief=research_brief,
        run=run,
        verification_profile=verification_profile,
        supplementary_macro_news_override=supplementary_macro_news_override,
        prepass_source_rows=prepass_source_rows,
        analysis_provider=analysis_provider,
    )
    run["stage1_second_pass"] = second_pass_result
    provider_meta["stage1_second_pass_enabled"] = True
    provider_meta["stage1_second_pass_success"] = bool(second_pass_result.get("success"))
    provider_meta["stage1_second_pass_attempts"] = int(second_pass_result.get("attempts", 0))
    provider_meta["stage1_second_pass_error"] = str(second_pass_result.get("error", ""))
    provider_meta["stage1_second_pass_warning"] = str(second_pass_result.get("warning", ""))
    provider_meta["stage1_second_pass_prompt_chars"] = int(
        second_pass_result.get("prompt_chars", 0)
    )
    provider_meta["stage1_second_pass_prompt_chars_before_compression"] = int(
        second_pass_result.get("prompt_chars_before_compression", 0)
    )
    provider_meta["stage1_second_pass_prompt_chars_after_compression"] = int(
        second_pass_result.get("prompt_chars_after_compression", 0)
    )
    provider_meta["stage1_second_pass_prompt_chars_saved"] = int(
        second_pass_result.get("prompt_chars_saved", 0)
    )
    provider_meta["stage1_second_pass_prompt_target_chars"] = int(
        second_pass_result.get("prompt_target_chars", 0)
    )
    provider_meta["stage1_second_pass_prompt_compression_enabled"] = bool(
        second_pass_result.get("prompt_compression_enabled", False)
    )
    provider_meta["stage1_second_pass_prompt_compression_applied"] = bool(
        second_pass_result.get("prompt_compression_applied", False)
    )
    provider_meta["stage1_second_pass_prompt_compression_appendix_omitted"] = bool(
        second_pass_result.get("prompt_compression_appendix_omitted", False)
    )
    provider_meta["stage1_second_pass_response_chars"] = int(
        second_pass_result.get("response_chars", 0)
    )
    provider_meta["stage1_second_pass_last_finish_reason"] = str(
        second_pass_result.get("last_model_finish_reason", "")
    )
    provider_meta["stage1_second_pass_last_response_id"] = str(
        second_pass_result.get("last_model_response_id", "")
    )
    provider_meta["stage1_second_pass_last_provider"] = str(
        second_pass_result.get("last_model_provider", "")
    )
    provider_meta["stage1_second_pass_analysis_provider"] = str(analysis_provider or "")
    provider_meta["stage1_second_pass_last_reasoning_effort"] = str(
        second_pass_result.get("last_model_reasoning_effort", "")
    )
    provider_meta["stage1_second_pass_last_usage"] = (
        second_pass_result.get("last_model_usage", {})
        if isinstance(second_pass_result.get("last_model_usage", {}), dict)
        else {}
    )
    provider_meta["stage1_second_pass_evidence_source_count"] = int(
        second_pass_result.get("evidence_source_count", 0)
    )
    provider_meta["stage1_second_pass_decoded_source_count"] = int(
        second_pass_result.get("decoded_source_count", 0)
    )
    provider_meta["stage1_second_pass_evidence_total_excerpt_chars"] = int(
        second_pass_result.get("evidence_total_excerpt_chars", 0)
    )
    source_key_points_counts = second_pass_result.get("source_key_points_counts", {}) or {}
    if isinstance(source_key_points_counts, dict):
        provider_meta["stage1_second_pass_source_key_points_sources_with_points"] = int(
            source_key_points_counts.get("sources_with_points", 0)
        )
        provider_meta["stage1_second_pass_source_key_points_total_points"] = int(
            source_key_points_counts.get("total_points", 0)
        )
        provider_meta["stage1_second_pass_source_key_points_total_words"] = int(
            source_key_points_counts.get("total_words", 0)
        )
    mandatory_fact_ledger = second_pass_result.get("mandatory_fact_ledger", {}) or {}
    mandatory_fact_counts = (
        mandatory_fact_ledger.get("counts", {})
        if isinstance(mandatory_fact_ledger, dict)
        else {}
    )
    provider_meta["stage1_second_pass_mandatory_fact_count"] = int(
        (mandatory_fact_counts or {}).get("fact_count", 0)
    )
    provider_meta["stage1_second_pass_mandatory_fact_families_present"] = int(
        (mandatory_fact_counts or {}).get("families_present", 0)
    )
    provider_meta["stage1_second_pass_mandatory_fact_ledger_chars"] = int(
        second_pass_result.get("mandatory_fact_ledger_chars", 0)
    )
    mandatory_fact_coverage = (
        second_pass_result.get("mandatory_fact_prompt_coverage", {}) or {}
    )
    if isinstance(mandatory_fact_coverage, dict):
        provider_meta["stage1_second_pass_mandatory_fact_coverage_passed"] = bool(
            mandatory_fact_coverage.get("passed", True)
        )
        provider_meta["stage1_second_pass_mandatory_fact_covered_count"] = int(
            mandatory_fact_coverage.get("covered_fact_count", 0)
            or (
                mandatory_fact_coverage.get("mandatory_fact_count", 0)
                if mandatory_fact_coverage.get("passed", True)
                else 0
            )
        )
        provider_meta["stage1_second_pass_mandatory_fact_missing_ids"] = list(
            mandatory_fact_coverage.get("missing_fact_ids", []) or []
        )
    provider_meta["stage1_second_pass_fact_pack_chars"] = int(
        second_pass_result.get("fact_pack_chars", 0)
    )
    provider_meta["stage1_second_pass_fact_pack_total_facts"] = int(
        second_pass_result.get("fact_pack_total_facts", 0)
    )
    provider_meta["stage1_second_pass_fact_pack_sections_with_facts"] = int(
        second_pass_result.get("fact_pack_sections_with_facts", 0)
    )
    provider_meta["stage1_second_pass_fact_digest_v2_enabled"] = bool(
        PERPLEXITY_STAGE1_FACT_DIGEST_V2_ENABLED
    )
    provider_meta["stage1_second_pass_fact_digest_v2_chars"] = int(
        second_pass_result.get("fact_digest_v2_chars", 0)
    )
    provider_meta["stage1_second_pass_fact_digest_v2_total_facts"] = int(
        second_pass_result.get("fact_digest_v2_total_facts", 0)
    )
    provider_meta["stage1_second_pass_fact_digest_v2_sections_with_facts"] = int(
        second_pass_result.get("fact_digest_v2_sections_with_facts", 0)
    )
    provider_meta["stage1_second_pass_fact_digest_v2_summary_bullets"] = int(
        second_pass_result.get("fact_digest_v2_summary_bullets", 0)
    )
    provider_meta["stage1_second_pass_fact_digest_v2_conflicts"] = int(
        second_pass_result.get("fact_digest_v2_conflicts", 0)
    )
    provider_meta["stage1_second_pass_compact_fact_bundle_chars"] = int(
        second_pass_result.get("compact_fact_bundle_chars", 0)
    )
    provider_meta["stage1_second_pass_compact_fact_bundle_total_facts"] = int(
        second_pass_result.get("compact_fact_bundle_total_facts", 0)
    )
    provider_meta["stage1_second_pass_compact_fact_bundle_categories_with_facts"] = int(
        second_pass_result.get("compact_fact_bundle_categories_with_facts", 0)
    )
    cashflow_schema_meta = second_pass_result.get("cashflow_schema", {}) or {}
    if isinstance(cashflow_schema_meta, dict):
        provider_meta["stage1_second_pass_cashflow_schema_active"] = bool(
            cashflow_schema_meta.get("active", False)
        )
        provider_meta["stage1_second_pass_cashflow_schema_mode"] = str(
            cashflow_schema_meta.get("mode", "")
        )
        provider_meta["stage1_second_pass_cashflow_schema_reason"] = str(
            cashflow_schema_meta.get("reason", "")
        )
        provider_meta["stage1_second_pass_cashflow_schema_decision_source"] = str(
            cashflow_schema_meta.get("decision_source", "")
        )
        provider_meta["stage1_second_pass_cashflow_schema_detection_source_rows_count"] = int(
            cashflow_schema_meta.get("detection_source_rows_count", 0)
        )
        provider_meta["stage1_second_pass_cashflow_schema_periods_detected"] = int(
            cashflow_schema_meta.get("periods_detected", 0)
        )
        provider_meta["stage1_second_pass_cashflow_schema_rows_with_cashflow_terms"] = int(
            cashflow_schema_meta.get("rows_with_cashflow_terms", 0)
        )
        provider_meta["stage1_second_pass_cashflow_schema_rows_with_reporting_terms"] = int(
            cashflow_schema_meta.get("rows_with_reporting_terms", 0)
        )
        provider_meta[
            "stage1_second_pass_cashflow_schema_rows_with_operating_cashflow_terms"
        ] = int(cashflow_schema_meta.get("rows_with_operating_cashflow_terms", 0))
        provider_meta["stage1_second_pass_cashflow_schema_rows_with_forward_terms"] = int(
            cashflow_schema_meta.get("rows_with_forward_guidance_terms", 0)
        )
        classifier_meta = cashflow_schema_meta.get("agent_classifier", {}) or {}
        if isinstance(classifier_meta, dict):
            provider_meta["stage1_second_pass_cashflow_schema_agent_used"] = bool(
                classifier_meta.get("used", False)
            )
            provider_meta["stage1_second_pass_cashflow_schema_agent_model"] = str(
                classifier_meta.get("model", "")
            )
            provider_meta["stage1_second_pass_cashflow_schema_agent_reason"] = str(
                classifier_meta.get("reason", "")
            )
            provider_meta["stage1_second_pass_cashflow_schema_agent_confidence_pct"] = float(
                classifier_meta.get("confidence_pct", 0.0) or 0.0
            )
    asx_ingestion = second_pass_result.get("asx_deterministic_ingestion", {}) or {}
    if isinstance(asx_ingestion, dict):
        provider_meta["stage1_second_pass_asx_deterministic_enabled"] = bool(
            asx_ingestion.get("enabled", False)
        )
        provider_meta["stage1_second_pass_asx_deterministic_used"] = bool(
            asx_ingestion.get("used", False)
        )
        provider_meta["stage1_second_pass_asx_deterministic_symbol"] = str(
            asx_ingestion.get("symbol", "")
        )
        provider_meta["stage1_second_pass_asx_deterministic_reason"] = str(
            asx_ingestion.get("reason", "")
        )
        provider_meta["stage1_second_pass_asx_deterministic_cache_hit"] = bool(
            asx_ingestion.get("cache_hit", False)
        )
        provider_meta["stage1_second_pass_asx_deterministic_selected_rows"] = int(
            asx_ingestion.get("selected_rows", 0)
        )
        provider_meta["stage1_second_pass_asx_deterministic_decoded_rows"] = int(
            asx_ingestion.get("decoded_rows", 0)
        )
    provider_meta["stage1_second_pass_source_rows_count"] = int(
        len(second_pass_result.get("source_rows", []) or [])
    )
    provider_meta["stage1_second_pass_prepass_source_rows_used"] = bool(
        (second_pass_result.get("injection_audit", {}) or {}).get(
            "prepass_source_rows_used",
            False,
        )
    )
    # Legacy alias kept for backward-compatible consumers.
    provider_meta["stage1_second_pass_source_rows_override_used"] = bool(
        provider_meta.get("stage1_second_pass_prepass_source_rows_used", False)
    )
    provider_meta["stage1_second_pass_supplementary_macro_news_count"] = int(
        second_pass_result.get("supplementary_macro_news_count", 0)
    )
    provider_meta["stage1_second_pass_supplementary_macro_news_profile"] = str(
        second_pass_result.get("supplementary_macro_news_profile", "")
    )
    provider_meta["stage1_second_pass_supplementary_macro_news_reason"] = str(
        second_pass_result.get("supplementary_macro_news_reason", "")
    )
    provider_meta["stage1_second_pass_supplementary_macro_news_retrieval_attempted"] = bool(
        second_pass_result.get("supplementary_macro_news_retrieval_attempted", False)
    )
    provider_meta["stage1_second_pass_supplementary_macro_news_retrieval_result_count"] = int(
        second_pass_result.get("supplementary_macro_news_retrieval_result_count", 0)
    )
    provider_meta["stage1_second_pass_supplementary_macro_news_retrieval_error"] = str(
        second_pass_result.get("supplementary_macro_news_retrieval_error", "")
    )
    provider_meta["stage1_second_pass_timeline_evidence_count"] = int(
        len(second_pass_result.get("timeline_evidence", []) or [])
    )
    provider_meta["stage1_second_pass_timeline_digest_chars"] = int(
        second_pass_result.get("timeline_digest_chars", 0)
    )
    provider_meta["stage1_second_pass_timeline_guard_enabled"] = bool(
        second_pass_result.get("timeline_guard_enabled", False)
    )
    provider_meta["stage1_second_pass_timeline_guard_passed"] = bool(
        second_pass_result.get("timeline_guard_passed", True)
    )
    provider_meta["stage1_second_pass_timeline_guard_reason"] = str(
        second_pass_result.get("timeline_guard_reason", "")
    )
    provider_meta["stage1_second_pass_timeline_guard_evidence_windows"] = list(
        second_pass_result.get("timeline_guard_evidence_windows", []) or []
    )
    provider_meta["stage1_second_pass_timeline_guard_response_windows"] = list(
        second_pass_result.get("timeline_guard_response_windows", []) or []
    )
    provider_meta["stage1_second_pass_timeline_guard_shifted_quarters"] = int(
        second_pass_result.get("timeline_guard_shifted_quarters", 0)
    )
    provider_meta["stage1_second_pass_verification_template_id"] = str(
        second_pass_result.get("verification_profile_template_id", "")
    )
    provider_meta["stage1_second_pass_verification_digest_sections"] = int(
        second_pass_result.get("verification_profile_digest_sections", 0)
    )
    provider_meta["stage1_second_pass_verification_compliance_markers"] = int(
        second_pass_result.get("verification_profile_compliance_markers", 0)
    )
    provider_meta["stage1_second_pass_verification_critical_sections"] = int(
        second_pass_result.get("verification_profile_critical_sections", 0)
    )
    provider_meta["stage1_second_pass_citation_gate_enabled"] = bool(
        second_pass_result.get("citation_gate_enabled", False)
    )
    provider_meta["stage1_second_pass_citation_gate_passed"] = bool(
        second_pass_result.get("citation_gate_passed", False)
    )
    provider_meta["stage1_second_pass_citation_gate_reason"] = str(
        second_pass_result.get("citation_gate_reason", "")
    )
    provider_meta["stage1_second_pass_citation_count"] = int(
        second_pass_result.get("citation_count", 0)
    )
    provider_meta["stage1_second_pass_citation_unique_count"] = int(
        second_pass_result.get("citation_unique_count", 0)
    )
    provider_meta["stage1_second_pass_citation_invalid_source_refs"] = list(
        second_pass_result.get("citation_invalid_source_refs", []) or []
    )
    provider_meta["stage1_second_pass_citation_numeric_lines"] = int(
        second_pass_result.get("citation_numeric_lines", 0)
    )
    provider_meta["stage1_second_pass_citation_uncited_numeric_lines"] = int(
        second_pass_result.get("citation_uncited_numeric_lines", 0)
    )
    provider_meta["stage1_second_pass_citation_cited_numeric_lines"] = int(
        second_pass_result.get("citation_cited_numeric_lines", 0)
    )
    provider_meta["stage1_second_pass_citation_numeric_citation_pct"] = float(
        second_pass_result.get("citation_numeric_citation_pct", 0.0)
    )
    provider_meta["stage1_second_pass_rubric_required"] = bool(
        second_pass_result.get("rubric_required", False)
    )
    provider_meta["stage1_second_pass_rubric_sections_total"] = int(
        second_pass_result.get("rubric_sections_total", 0)
    )
    provider_meta["stage1_second_pass_rubric_sections_covered"] = int(
        second_pass_result.get("rubric_sections_covered", 0)
    )
    provider_meta["stage1_second_pass_rubric_coverage_pct"] = float(
        second_pass_result.get("rubric_coverage_pct", 0.0)
    )
    provider_meta["stage1_second_pass_rubric_missing_sections"] = list(
        second_pass_result.get("rubric_missing_sections", []) or []
    )
    provider_meta["stage1_second_pass_rubric_critical_missing_sections"] = list(
        second_pass_result.get("rubric_critical_missing_sections", []) or []
    )
    provider_meta["stage1_second_pass_compliance_score"] = float(
        second_pass_result.get("compliance_score", 0.0)
    )
    provider_meta["stage1_second_pass_compliance_rating"] = str(
        second_pass_result.get("compliance_rating", "")
    )
    provider_meta["stage1_second_pass_compliance_retry_recommended"] = bool(
        second_pass_result.get("compliance_retry_recommended", False)
    )
    provider_meta["stage1_second_pass_compliance_catastrophic_failure"] = bool(
        second_pass_result.get("compliance_catastrophic_failure", False)
    )
    provider_meta["stage1_second_pass_compliance_fail_reasons"] = list(
        second_pass_result.get("compliance_fail_reasons", []) or []
    )
    provider_meta["stage1_second_pass_compliance_warning_reasons"] = list(
        second_pass_result.get("compliance_warning_reasons", []) or []
    )
    provider_meta["stage1_second_pass_compliance_hard_fail_reasons"] = list(
        second_pass_result.get("compliance_hard_fail_reasons", []) or []
    )
    provider_meta["stage1_second_pass_compliance_soft_fail_reasons"] = list(
        second_pass_result.get("compliance_soft_fail_reasons", []) or []
    )

    if second_pass_result.get("prompt"):
        run["stage1_second_pass_prompt"] = second_pass_result["prompt"]
    if second_pass_result.get("fact_digest_v2"):
        run["stage1_second_pass_fact_digest_v2"] = second_pass_result["fact_digest_v2"]
    if second_pass_result.get("fact_pack"):
        run["stage1_second_pass_fact_pack"] = second_pass_result["fact_pack"]
    if second_pass_result.get("mandatory_fact_ledger"):
        run["stage1_second_pass_mandatory_fact_ledger"] = (
            second_pass_result.get("mandatory_fact_ledger") or {}
        )
    if second_pass_result.get("mandatory_fact_prompt_coverage"):
        run["stage1_second_pass_mandatory_fact_prompt_coverage"] = (
            second_pass_result.get("mandatory_fact_prompt_coverage") or {}
        )
    if isinstance(cashflow_schema_meta, dict) and cashflow_schema_meta:
        run["stage1_second_pass_cashflow_schema"] = cashflow_schema_meta
    if "compact_fact_bundle" in second_pass_result:
        run["stage1_second_pass_compact_fact_bundle"] = (
            second_pass_result.get("compact_fact_bundle") or {}
        )
    if second_pass_result.get("injection_audit"):
        run["stage1_second_pass_injection_audit"] = (
            second_pass_result.get("injection_audit") or {}
        )
        provider_meta["stage1_second_pass_injection_sources"] = int(
            len(
                (
                    (second_pass_result.get("injection_audit") or {}).get("source_rows_preview", [])
                    or []
                )
            )
        )
        provider_meta["stage1_second_pass_injection_categories"] = int(
            len(
                (
                    (second_pass_result.get("injection_audit") or {}).get(
                        "compact_fact_bundle_preview",
                        {},
                    )
                    or {}
                ).keys()
            )
        )
        provider_meta["stage1_second_pass_injection_supplementary_sources"] = int(
            len(
                (
                    (second_pass_result.get("injection_audit") or {}).get(
                        "supplementary_macro_news_preview",
                        [],
                    )
                    or []
                )
            )
        )
    if isinstance(asx_ingestion, dict):
        run["stage1_second_pass_asx_deterministic_ingestion"] = asx_ingestion
    if second_pass_result.get("source_rows"):
        run["stage1_second_pass_source_rows"] = second_pass_result.get("source_rows", [])
    if second_pass_result.get("supplementary_macro_news_sources"):
        run["stage1_second_pass_supplementary_macro_news_sources"] = (
            second_pass_result.get("supplementary_macro_news_sources", [])
        )
    if second_pass_result.get("supplementary_macro_news"):
        run["stage1_second_pass_supplementary_macro_news"] = (
            second_pass_result.get("supplementary_macro_news", {})
        )
    if second_pass_result.get("timeline_evidence"):
        run["stage1_second_pass_timeline_evidence"] = second_pass_result.get(
            "timeline_evidence",
            [],
        )
    if second_pass_result.get("success") and second_pass_result.get("response"):
        run["stage1_analysis_response"] = str(second_pass_result["response"]).strip()
        final_compliance = _evaluate_stage1_template_compliance(
            summary_text=run["stage1_analysis_response"],
            user_query=user_query,
            research_brief=research_brief,
            section_markers=(
                (verification_profile or {}).get("compliance_section_markers")
                if verification_profile
                else None
            ),
        )
        provider_meta["stage1_final_template_compliant"] = bool(
            final_compliance["compliant"]
        )
        provider_meta["stage1_final_template_reason"] = str(
            final_compliance["reason"]
        )
        provider_meta["stage1_final_template_marker_hits"] = int(
            final_compliance.get("marker_hits", 0)
        )
        provider_meta["stage1_final_template_primary_marker_hits"] = int(
            final_compliance.get("primary_marker_hits", 0)
        )
        provider_meta["stage1_final_template_secondary_marker_hits"] = int(
            final_compliance.get("secondary_marker_hits", 0)
        )

    return run


async def stage1_collect_responses(
    enhanced_context: str,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    """
    Stage 1: Collect individual responses from all council models.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content

    Returns:
        List of dicts with 'model' and 'response' keys
    """
    messages = [{"role": "user", "content": enhanced_context}]
    total_models = len(COUNCIL_MODELS)

    if progress_callback is not None:
        try:
            progress_callback(
                {
                    "type": "stage1_progress",
                    "data": {
                        "stage": "stage1",
                        "phase": "local_start",
                        "model": "",
                        "status": "running",
                        "completed": 0,
                        "total": total_models,
                        "progress_pct": 0,
                        "stage_message": f"Stage 1 started with {total_models} local council model(s)",
                    },
                }
            )
        except Exception:
            pass
    _progress_log(
        f"Stage1 progress: phase=local_start model= completed=0/{max(1, total_models)} pct=0 status=running"
    )

    def _on_model_complete(
        model: str,
        response: Optional[Dict[str, Any]],
        completed: int,
        total: int,
    ) -> None:
        if progress_callback is None:
            return
        status = "success" if response is not None else "failed"
        progress_pct = int(round((completed / max(total, 1)) * 100)) if total else 100
        progress_message = (
            f"Stage 1 model complete: {model} ({completed}/{total})"
            if status == "success"
            else f"Stage 1 model failed: {model} ({completed}/{total})"
        )
        try:
            progress_callback(
                {
                    "type": "stage1_progress",
                    "data": {
                        "stage": "stage1",
                        "phase": "local_model_complete",
                        "model": model,
                        "status": status,
                        "completed": completed,
                        "total": total,
                        "progress_pct": progress_pct,
                        "stage_message": progress_message,
                    },
                }
            )
        except Exception:
            pass
        _progress_log(
            f"Stage1 progress: phase=local_model_complete model={model} "
            f"completed={completed}/{total} pct={progress_pct} status={status}"
        )

    # Query all models in parallel
    responses = await query_models_parallel(
        COUNCIL_MODELS,
        messages,
        on_model_complete=_on_model_complete,
    )

    # Format results
    stage1_results = []
    for model in COUNCIL_MODELS:
        response = responses.get(model)
        if response is not None:  # Only include successful responses
            stage1_results.append({
                "model": model,
                "response": response.get('content', '')
            })

    return stage1_results


async def stage1_collect_perplexity_research_responses(
    user_query: str,
    ticker: Optional[str] = None,
    attachment_context: str = "",
    prepass_source_rows: Optional[List[Dict[str, Any]]] = None,
    source_rows_override: Optional[List[Dict[str, Any]]] = None,
    depth: str = "deep",
    research_brief: str = "",
    template_id: Optional[str] = None,
    diagnostic_mode: bool = False,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Stage 1 (emulated): run one Perplexity deep-research call per configured model.

    Args:
        user_query: User question
        ticker: Optional ticker symbol
        attachment_context: Optional attached-document context
        prepass_source_rows: Optional prepass source rows for authoritative
            second-pass evidence injection.
        source_rows_override: Deprecated alias for prepass_source_rows.
        depth: basic|deep research depth
        research_brief: Optional template/company-type framing to steer retrieval
        template_id: Optional selected template id for verification profile
        diagnostic_mode: Allow execution during global shutdown for audit scripts

    Returns:
        Tuple of:
        - stage1_results: List[{"model": ..., "response": ...}]
        - metadata: {"per_model_research_runs": [...], "aggregated_search_results": {...}}
    """
    _ensure_system_enabled(diagnostic_mode=diagnostic_mode)
    import asyncio
    from ..research.providers.perplexity import PerplexityResearchProvider

    if prepass_source_rows is None and source_rows_override is not None:
        prepass_source_rows = list(source_rows_override)
    authoritative_prepass_mode = bool(prepass_source_rows)

    total_start = perf_counter()
    perplexity_models_requested = _dedupe_model_ids(
        [
            _normalize_perplexity_model_id(model)
            for model in (PERPLEXITY_COUNCIL_MODELS or COUNCIL_MODELS)
        ]
    )
    perplexity_models = list(perplexity_models_requested)
    preflight_results: List[Dict[str, Any]] = []
    preflight_removed_models: List[str] = []
    preflight_skipped_reason = ""
    if PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED:
        if not PERPLEXITY_API_KEY:
            preflight_skipped_reason = "missing_api_key"
        elif not perplexity_models_requested:
            preflight_skipped_reason = "no_perplexity_models_requested"
        else:
            _progress_log(
                "Stage1 Perplexity model preflight start: "
                f"requested_models={perplexity_models_requested}, "
                f"timeout={float(PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS):.1f}s, "
                f"fail_open={bool(PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN)}"
            )
            probe_tasks = [
                _probe_perplexity_model_support(
                    model=model,
                    timeout_seconds=float(PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS),
                )
                for model in perplexity_models_requested
            ]
            preflight_results = await asyncio.gather(*probe_tasks)
            supported_models: List[str] = []
            transient_models: List[str] = []
            unsupported_models: List[str] = []
            for row in preflight_results:
                resolved_model = str(
                    row.get("resolved_model")
                    or row.get("requested_model")
                    or ""
                ).strip()
                if not resolved_model:
                    continue
                if bool(row.get("supported", False)):
                    supported_models.append(resolved_model)
                    continue
                error_type = str(row.get("error_type", "")).strip().lower()
                if error_type == "unsupported":
                    unsupported_models.append(resolved_model)
                else:
                    transient_models.append(resolved_model)

            supported_models = _dedupe_model_ids(supported_models)
            transient_models = _dedupe_model_ids(transient_models)
            unsupported_models = _dedupe_model_ids(unsupported_models)
            if PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN:
                effective_models = _dedupe_model_ids(supported_models + transient_models)
            else:
                effective_models = list(supported_models)

            if effective_models:
                perplexity_models = effective_models
                preflight_removed_models = [
                    model
                    for model in perplexity_models_requested
                    if model not in effective_models
                ]
            else:
                # No supported results; keep original list in fail-open mode.
                if PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN:
                    perplexity_models = list(perplexity_models_requested)
                    preflight_skipped_reason = "all_models_probe_failed_fail_open"
                else:
                    perplexity_models = []
                    preflight_skipped_reason = "all_models_unsupported"
            _progress_log(
                "Stage1 Perplexity model preflight done: "
                f"effective_models={perplexity_models}, "
                f"removed={preflight_removed_models}"
            )
    if (
        PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED
        and preflight_skipped_reason
        and not preflight_results
    ):
        _progress_log(f"Stage1 Perplexity model preflight skipped: {preflight_skipped_reason}")
    mixed_mode_enabled = bool(PERPLEXITY_STAGE1_MIXED_MODE_ENABLED)
    openrouter_pool_models: List[str] = []
    if mixed_mode_enabled:
        openrouter_pool_models = _dedupe_model_ids(
            PERPLEXITY_STAGE1_OPENROUTER_MODELS or COUNCIL_MODELS
        )
        openrouter_pool_models = [
            model
            for model in openrouter_pool_models
            if _is_openrouter_compatible_model(model)
        ]
    models = _dedupe_model_ids(perplexity_models + openrouter_pool_models) if mixed_mode_enabled else list(perplexity_models)
    if not models:
        models = _dedupe_model_ids(COUNCIL_MODELS)
    perplexity_model_set = set(perplexity_models)
    openrouter_model_set = set(openrouter_pool_models)
    shared_retrieval_requested = bool(
        (not authoritative_prepass_mode)
        and PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED
        and len(models) > 1
    )
    if mixed_mode_enabled and len(models) > 1 and not shared_retrieval_requested:
        # Mixed provider fanout is only coherent when retrieval/decode is shared,
        # except when authoritative prepass rows are supplied.
        if authoritative_prepass_mode:
            _progress_log(
                "Stage1 authoritative prepass mode: skipping shared retrieval "
                "and provider retrieval fanout."
            )
        else:
            shared_retrieval_requested = True
            _progress_log(
                "Stage1 mixed mode forcing shared retrieval: "
                "shared_retrieval_enabled=false -> true"
            )

    stage1_total_units = len(models) + (1 if shared_retrieval_requested else 0)
    stage1_completed_units = 0

    def _emit_stage1_progress(
        *,
        model: str,
        status: str,
        phase: str,
        result_count: Optional[int] = None,
    ) -> None:
        nonlocal stage1_completed_units
        stage1_completed_units += 1
        total = max(1, stage1_total_units)
        pct = int(round((stage1_completed_units / total) * 100))
        payload: Dict[str, Any] = {
            "type": "stage1_progress",
            "data": {
                "stage": "stage1",
                "phase": phase,
                "model": model,
                "status": status,
                "completed": stage1_completed_units,
                "total": total,
                "progress_pct": pct,
                "stage_message": (
                    f"Stage 1 progress: phase={phase} model={model} "
                    f"completed={stage1_completed_units}/{total} pct={pct} status={status}"
                ),
            },
        }
        if result_count is not None:
            payload["data"]["result_count"] = int(result_count)
        if progress_callback is not None:
            try:
                progress_callback(payload)
            except Exception:
                pass
        _progress_log(
            f"Stage1 progress: phase={phase} model={model} "
            f"completed={stage1_completed_units}/{total} pct={pct} status={status}"
        )

    if progress_callback is not None:
        try:
            progress_callback(
                {
                    "type": "stage1_progress",
                    "data": {
                        "stage": "stage1",
                        "phase": "start",
                        "model": "",
                        "status": "running",
                        "completed": 0,
                        "total": max(1, stage1_total_units),
                        "progress_pct": 0,
                        "stage_message": f"Stage 1 started with {stage1_total_units} unit(s)",
                    },
                }
            )
        except Exception:
            pass
    _progress_log(
        f"Stage1 progress: phase=start model= completed=0/{max(1, stage1_total_units)} pct=0 status=running"
    )
    provider = PerplexityResearchProvider()
    _progress_log(
        "Stage1 perplexity emulation start: "
        f"models={models}, depth={depth}, max_sources={MAX_SOURCES}, "
        f"mixed_mode_enabled={mixed_mode_enabled}, "
        f"perplexity_pool={perplexity_models}, "
        f"openrouter_pool={openrouter_pool_models}, "
        f"execution_mode={PERPLEXITY_STAGE1_EXECUTION_MODE}, "
        f"second_pass_enabled={PERPLEXITY_STAGE1_SECOND_PASS_ENABLED}, "
        f"shared_retrieval_config_enabled={PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED}, "
        f"shared_retrieval_requested={shared_retrieval_requested}, "
        f"authoritative_prepass_mode={authoritative_prepass_mode}"
    )
    verification_profile = _build_stage1_verification_profile(template_id)
    if PROGRESS_LOGGING:
        marker_count = len(verification_profile.get("compliance_section_markers", []) or [])
        critical_count = len(verification_profile.get("compliance_critical_sections", set()) or [])
        _progress_log(
            "Stage1 verification profile: "
            f"template_id={template_id or 'none'}, "
            f"digest_sections={len((verification_profile.get('fact_digest_keywords') or {}).keys())}, "
            f"timeline_focus_terms={len(verification_profile.get('timeline_focus_terms', []) or [])}, "
            f"compliance_markers={marker_count}, critical_sections={critical_count}"
        )

    # Optional attachment-context cap (0 = no truncation).
    bounded_attachment_context = attachment_context or ""
    attachment_cap = int(PERPLEXITY_STAGE1_ATTACHMENT_CONTEXT_MAX_CHARS)
    if attachment_cap > 0 and len(bounded_attachment_context) > attachment_cap:
        _progress_log(
            "Stage1 attachment context truncated: "
            f"original_chars={len(bounded_attachment_context)} "
            f"cap={attachment_cap}"
        )
        bounded_attachment_context = bounded_attachment_context[:attachment_cap]
    research_query = user_query
    if bounded_attachment_context:
        research_query = (
            f"{user_query}\n\n"
            "Additional attached-document context provided by the user:\n"
            f"{bounded_attachment_context}"
        )

    # Keep full brief by default for strict template fidelity.
    bounded_research_brief = (research_brief or "").strip()

    raw_runs: List[Dict[str, Any]] = []
    execution_mode = (PERPLEXITY_STAGE1_EXECUTION_MODE or "parallel").strip().lower()
    # Prefer explicit total attempts. Fall back to legacy retries+1 semantics.
    max_attempts_cfg = int(PERPLEXITY_STAGE1_MAX_ATTEMPTS)
    if max_attempts_cfg > 0:
        max_attempts = max_attempts_cfg
    else:
        max_attempts = max(1, int(PERPLEXITY_STAGE1_MAX_RETRIES) + 1)
    base_backoff = max(0.0, float(PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS))
    # One supplementary macro brief per Stage-1 run; reused across all model
    # second-pass prompts to avoid model-by-model drift in injected context.
    shared_supplementary_macro_news: Optional[Dict[str, Any]] = None
    multi_wave_enabled = bool(PERPLEXITY_STAGE1_MULTI_WAVE_ENABLED and depth == "deep")
    max_waves = max(1, int(PERPLEXITY_STAGE1_MULTI_WAVE_MAX_WAVES))
    gap_query_limit = max(1, int(PERPLEXITY_STAGE1_MULTI_WAVE_GAP_QUERY_LIMIT))
    min_new_primary_sources = max(0, int(PERPLEXITY_STAGE1_MULTI_WAVE_MIN_NEW_PRIMARY_SOURCES))

    def _analysis_provider_for_model(model: str) -> str:
        """
        Select second-pass API lane per model in mixed mode.

        - perplexity pool models -> Perplexity API
        - openrouter pool models -> OpenRouter API
        """
        model_key = str(model or "").strip()
        # Route Gemini second-pass analysis via OpenRouter when available.
        # This avoids recurring short/truncated non-search outputs observed on
        # Perplexity non-search second-pass calls for large prompts.
        if (
            OPENROUTER_API_KEY
            and model_key.lower().startswith("google/")
            and _is_openrouter_compatible_model(model_key)
        ):
            return "openrouter"
        if not mixed_mode_enabled:
            # In non-mixed Perplexity execution, keep retrieval on Perplexity but
            # route second-pass analysis through Perplexity by default.
            return "perplexity"
        if model_key in perplexity_model_set:
            return "perplexity"
        if model_key in openrouter_model_set:
            return "openrouter"
        # Default unknowns to Perplexity lane in mixed mode for safer attribution.
        return "perplexity"

    def _build_authoritative_prepass_seed_run(model: str) -> Dict[str, Any]:
        now_iso = datetime.utcnow().isoformat()
        return {
            "id": f"stage1_prepass_seed_{uuid.uuid4().hex}",
            "query": user_query,
            "ticker": ticker,
            "depth": depth,
            "model": model,
            "generated_at": now_iso,
            "result_count": 0,
            "results": [],
            "latest_updates": [],
            "research_summary": "",
            "provider_metadata": {
                "model": model,
                "preset": "prepass_authoritative",
                "tools": [],
                "source_decoding": {
                    "attempted": int(len(prepass_source_rows or [])),
                    "decoded": int(len(prepass_source_rows or [])),
                    "failed": 0,
                },
                "stage1_prepass_authoritative_mode": True,
                "stage1_prepass_source_rows_supplied": int(len(prepass_source_rows or [])),
                "stage1_retrieval_skipped": True,
                "stage1_retrieval_skipped_reason": "authoritative_prepass_mode",
                "stage1_shared_retrieval_enabled": False,
                "stage1_shared_retrieval_used": False,
            },
        }

    async def _run_retrieval_with_planner(
        *,
        model: str,
        attempt_profile: Dict[str, Any],
        active_research_brief: str,
    ) -> Dict[str, Any]:
        """
        Execute retrieval with planner + wave gap-filling when enabled.

        Wave 1 performs broad retrieval; wave 2/3 target unresolved rubric sections.
        """
        preset_value = str(attempt_profile.get("preset", "")).strip() or None
        if not multi_wave_enabled or max_waves <= 1:
            return await provider.gather(
                research_query,
                ticker=ticker,
                depth=depth,
                max_sources=int(attempt_profile["max_sources"]),
                model_override=model,
                research_brief=active_research_brief,
                max_steps_override=int(attempt_profile["max_steps"]),
                max_output_tokens_override=int(attempt_profile["max_output_tokens"]),
                reasoning_effort_override=str(attempt_profile["reasoning_effort"]),
                preset_override=preset_value,
            ) or {}

        planner = _build_stage1_research_planner(
            user_query=user_query,
            research_brief=active_research_brief,
            ticker=ticker,
            verification_profile=verification_profile,
            max_waves=max_waves,
            gap_query_limit=gap_query_limit,
        )
        _progress_log(
            "Stage1 planner created: "
            f"model={model}, objectives={len(planner.get('objectives', []))}, "
            f"max_waves={planner.get('max_waves')}"
        )

        seen_primary_urls: set[str] = set()
        wave_runs: List[Dict[str, Any]] = []
        wave_reports: List[Dict[str, Any]] = []
        missing_sections: List[str] = []
        missing_critical_sections: List[str] = []

        for wave_idx in range(1, max_waves + 1):
            wave_query = research_query
            wave_type = "broad_primary" if wave_idx == 1 else "gap_fill"

            if wave_idx > 1:
                if not missing_sections:
                    _progress_log(
                        f"Stage1 planner stop model={model} wave={wave_idx} reason=no_missing_sections"
                    )
                    break
                gap_block = _build_stage1_gap_query_block(
                    missing_sections=missing_sections,
                    verification_profile=verification_profile,
                    ticker=ticker,
                    gap_query_limit=gap_query_limit,
                )
                if gap_block:
                    wave_query = (
                        f"{research_query}\n\n"
                        f"Gap-Fill Retrieval Wave {wave_idx}:\n"
                        f"{gap_block}"
                    )

            _progress_log(
                "Stage1 planner wave start: "
                f"model={model}, wave={wave_idx}/{max_waves}, type={wave_type}, "
                f"missing_sections={len(missing_sections)}, missing_critical={len(missing_critical_sections)}"
            )
            wave_run = await provider.gather(
                wave_query,
                ticker=ticker,
                depth=depth,
                max_sources=int(attempt_profile["max_sources"]),
                model_override=model,
                research_brief=active_research_brief,
                max_steps_override=int(attempt_profile["max_steps"]),
                max_output_tokens_override=int(attempt_profile["max_output_tokens"]),
                reasoning_effort_override=str(attempt_profile["reasoning_effort"]),
                preset_override=preset_value,
            ) or {}

            if wave_run.get("error"):
                wave_reports.append(
                    {
                        "wave": wave_idx,
                        "type": wave_type,
                        "status": "error",
                        "error": str(wave_run.get("error", "")),
                    }
                )
                # First wave failure is hard-fail for attempt. Later waves are optional.
                if wave_idx == 1:
                    return wave_run
                _progress_log(
                    f"Stage1 planner wave error model={model} wave={wave_idx}: "
                    f"{str(wave_run.get('error', ''))[:220]}"
                )
                break

            coverage = _evaluate_stage1_section_coverage(
                wave_run,
                verification_profile=verification_profile,
            )
            missing_sections = list(coverage.get("missing_sections", []))
            missing_critical_sections = list(coverage.get("missing_critical_sections", []))
            new_primary = _count_new_primary_sources(wave_run, seen_primary_urls)

            wave_reports.append(
                {
                    "wave": wave_idx,
                    "type": wave_type,
                    "status": "ok",
                    "result_count": int(wave_run.get("result_count", 0)),
                    "new_primary_sources": int(new_primary),
                    "missing_sections": missing_sections[:8],
                    "missing_critical_sections": missing_critical_sections[:8],
                    "critical_sections_covered": int(coverage.get("critical_sections_covered", 0)),
                    "critical_sections_total": int(coverage.get("critical_sections_total", 0)),
                }
            )
            wave_runs.append(wave_run)

            _progress_log(
                "Stage1 planner wave done: "
                f"model={model}, wave={wave_idx}, results={wave_run.get('result_count', 0)}, "
                f"new_primary={new_primary}, missing={len(missing_sections)}, "
                f"missing_critical={len(missing_critical_sections)}"
            )

            if not missing_sections:
                break
            if wave_idx >= max_waves:
                break
            if (
                wave_idx >= 2
                and not missing_critical_sections
                and new_primary < min_new_primary_sources
            ):
                _progress_log(
                    "Stage1 planner stop: "
                    f"model={model} reason=insufficient_new_primary_sources({new_primary})"
                )
                break

        merged = _merge_stage1_wave_runs(
            wave_runs=wave_runs,
            original_query=research_query,
            max_sources=int(attempt_profile["max_sources"]),
            planner=planner,
            wave_reports=wave_reports,
        )
        return merged

    async def _gather_model_with_retries(model: str, run_second_pass: bool = True) -> Dict[str, Any]:
        nonlocal shared_supplementary_macro_news
        if authoritative_prepass_mode:
            run = _build_authoritative_prepass_seed_run(model)
            provider_meta = run.setdefault("provider_metadata", {})
            if not isinstance(provider_meta, dict):
                provider_meta = {}
                run["provider_metadata"] = provider_meta
            analysis_provider = _analysis_provider_for_model(model)
            provider_meta["stage1_analysis_provider"] = analysis_provider
            provider_meta["stage1_attempts"] = 1
            provider_meta["stage1_retried"] = False
            provider_meta["stage1_template_retry_triggered"] = False
            provider_meta["stage1_template_retry_fallback_used"] = False
            provider_meta["stage1_attempt_history"] = [
                {
                    "attempt": 1,
                    "status": "prepass_authoritative_analysis_only",
                    "profile": {
                        "name": "prepass_authoritative",
                        "max_sources": int(len(prepass_source_rows or [])),
                        "reasoning_effort": "",
                    },
                }
            ]
            if run_second_pass:
                if analysis_provider == "perplexity":
                    run = await _apply_stage1_second_pass(
                        model=model,
                        user_query=user_query,
                        research_brief=bounded_research_brief,
                        run=run,
                        verification_profile=verification_profile,
                        supplementary_macro_news_override=shared_supplementary_macro_news,
                        prepass_source_rows=prepass_source_rows,
                        analysis_provider="perplexity",
                    )
                elif _is_openrouter_compatible_model(model):
                    run = await _apply_stage1_second_pass(
                        model=model,
                        user_query=user_query,
                        research_brief=bounded_research_brief,
                        run=run,
                        verification_profile=verification_profile,
                        supplementary_macro_news_override=shared_supplementary_macro_news,
                        prepass_source_rows=prepass_source_rows,
                        analysis_provider="openrouter",
                    )
                else:
                    provider_meta["stage1_second_pass_enabled"] = False
                    provider_meta["stage1_second_pass_skipped_reason"] = (
                        "model_not_openrouter_compatible"
                    )
                if shared_supplementary_macro_news is None:
                    maybe_shared = run.get("stage1_second_pass_supplementary_macro_news", {})
                    if isinstance(maybe_shared, dict) and maybe_shared:
                        shared_supplementary_macro_news = copy.deepcopy(maybe_shared)
            else:
                provider_meta["stage1_second_pass_enabled"] = False
                provider_meta["stage1_second_pass_skipped_reason"] = "second_pass_disabled"
            return run

        run: Dict[str, Any] = {}
        last_successful_run: Optional[Dict[str, Any]] = None
        active_research_brief = bounded_research_brief
        template_retry_triggered = False
        template_retry_fallback_used = False
        final_retry_error = ""
        attempt_history: List[Dict[str, Any]] = []
        for attempt in range(1, max_attempts + 1):
            attempt_profile = _build_stage1_attempt_profile(
                model=model,
                attempt=attempt,
                depth=depth,
                base_preset=str(provider.preset),
                base_max_sources=MAX_SOURCES,
                base_max_steps=int(provider.max_steps),
                base_max_output_tokens=int(provider.max_output_tokens),
                base_reasoning_effort=str(provider.reasoning_effort),
            )
            if attempt > 1:
                wait_seconds = base_backoff * (2 ** (attempt - 2))
                if wait_seconds > 0:
                    _progress_log(
                        f"Stage1 retry backoff for {model}: sleeping {wait_seconds:.1f}s "
                        f"(attempt {attempt}/{max_attempts})"
                    )
                    await asyncio.sleep(wait_seconds)
                _progress_log(f"Stage1 retry attempt {attempt}/{max_attempts} for {model}")
            _progress_log(
                f"Stage1 attempt profile {attempt}/{max_attempts} for {model}: "
                f"profile={attempt_profile['name']}, preset={attempt_profile['preset']}, "
                f"max_sources={attempt_profile['max_sources']}, "
                f"max_steps={attempt_profile['max_steps']}, "
                f"max_output_tokens={attempt_profile['max_output_tokens']}, "
                f"reasoning_effort={attempt_profile['reasoning_effort'] or 'low'}"
            )

            run = await _run_retrieval_with_planner(
                model=model,
                attempt_profile=attempt_profile,
                active_research_brief=active_research_brief,
            ) or {}

            if not run.get("error"):
                last_successful_run = copy.deepcopy(run)
                provider_meta = run.setdefault("provider_metadata", {})
                if not isinstance(provider_meta, dict):
                    provider_meta = {}
                    run["provider_metadata"] = provider_meta

                compliance = _evaluate_stage1_template_compliance(
                    summary_text=str(run.get("research_summary", "")),
                    user_query=user_query,
                    research_brief=bounded_research_brief,
                    section_markers=verification_profile.get("compliance_section_markers"),
                )
                provider_meta["template_compliance_required"] = bool(compliance["required"])
                provider_meta["template_compliant"] = bool(compliance["compliant"])
                provider_meta["template_compliance_reason"] = str(compliance["reason"])
                provider_meta["template_synthesis_chars"] = int(compliance["synthesis_chars"])
                provider_meta["template_marker_hits"] = int(compliance["marker_hits"])
                if compliance.get("primary_marker_hits") is not None:
                    provider_meta["template_primary_marker_hits"] = int(
                        compliance["primary_marker_hits"]
                    )
                if compliance.get("secondary_marker_hits") is not None:
                    provider_meta["template_secondary_marker_hits"] = int(
                        compliance["secondary_marker_hits"]
                    )
                attempt_history.append(
                    {
                        "attempt": attempt,
                        "status": "success",
                        "profile": attempt_profile,
                        "template_compliant": bool(compliance["compliant"]),
                        "template_reason": str(compliance["reason"]),
                        "template_synthesis_chars": int(compliance["synthesis_chars"]),
                        "template_marker_hits": int(compliance["marker_hits"]),
                        "template_primary_marker_hits": int(
                            compliance.get("primary_marker_hits", 0)
                        ),
                        "template_secondary_marker_hits": int(
                            compliance.get("secondary_marker_hits", 0)
                        ),
                    }
                )

                sonar_telemetry = _evaluate_stage1_sonar_telemetry(
                    model=model,
                    provider_meta=provider_meta,
                )
                provider_meta["sonar_multistep_required"] = bool(
                    sonar_telemetry.get("required", False)
                )
                provider_meta["sonar_multistep_passed"] = bool(
                    sonar_telemetry.get("passed", True)
                )
                provider_meta["sonar_multistep_reason"] = str(
                    sonar_telemetry.get("reason", "")
                )
                provider_meta["is_sonar_model"] = bool(
                    sonar_telemetry.get("is_sonar_model", False)
                )

                if (
                    bool(sonar_telemetry.get("required", False))
                    and not bool(sonar_telemetry.get("passed", True))
                ):
                    sonar_reason = str(sonar_telemetry.get("reason", "unknown"))
                    attempt_history.append(
                        {
                            "attempt": attempt,
                            "status": "sonar_telemetry_failed",
                            "profile": attempt_profile,
                            "reason": sonar_reason,
                            "retryable": attempt < max_attempts,
                        }
                    )
                    if attempt < max_attempts:
                        _progress_log(
                            f"Stage1 sonar telemetry retry for {model}: "
                            f"{sonar_reason} (attempt {attempt}/{max_attempts})"
                        )
                        continue
                    _progress_log(
                        f"Stage1 sonar telemetry warning for {model}: "
                        f"{sonar_reason} (no retry attempts left)"
                    )

                if compliance["required"] and not compliance["compliant"] and attempt < max_attempts:
                    template_retry_allowed = (
                        PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED
                        and not PERPLEXITY_STAGE1_SECOND_PASS_ENABLED
                    )
                    if template_retry_allowed:
                        template_retry_triggered = True
                        active_research_brief = _build_strict_research_brief(bounded_research_brief)
                        _progress_log(
                            f"Stage1 template compliance retry for {model}: "
                            f"{compliance['reason']} (attempt {attempt}/{max_attempts})"
                        )
                        continue
                    if PERPLEXITY_STAGE1_SECOND_PASS_ENABLED:
                        _progress_log(
                            f"Stage1 template compliance warning for {model} before second pass: "
                            f"{compliance['reason']}"
                        )
                    else:
                        _progress_log(
                            f"Stage1 template compliance warning for {model} without retry: "
                            f"{compliance['reason']} (set PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED=true to retry)"
                        )
                elif compliance["required"] and not compliance["compliant"] and PERPLEXITY_STAGE1_SECOND_PASS_ENABLED:
                    _progress_log(
                        f"Stage1 template compliance warning for {model} before second pass: "
                        f"{compliance['reason']}"
                    )
                break

            error_text = str(run.get("error", ""))
            final_retry_error = error_text
            retryable = bool(_is_retryable_stage1_error(error_text))
            attempt_history.append(
                {
                    "attempt": attempt,
                    "profile": attempt_profile,
                    "status": "error",
                    "error": error_text,
                    "retryable": retryable,
                }
            )
            if attempt >= max_attempts or not retryable:
                break
            _progress_log(
                f"Stage1 transient failure for {model}: {error_text[:220]} "
                f"(will retry)"
            )

        # Preserve the last successful output if template retry attempts subsequently fail.
        # This avoids throwing away usable analysis due to transient API errors on strict retries.
        if (run is None or run.get("error")) and last_successful_run is not None and template_retry_triggered:
            run = last_successful_run
            template_retry_fallback_used = True
            _progress_log(
                f"Stage1 template retry fallback kept previous successful result for {model} "
                f"after final error: {final_retry_error[:220]}"
            )

        if run and not run.get("error"):
            analysis_provider = _analysis_provider_for_model(model)
            provider_meta = run.setdefault("provider_metadata", {})
            if not isinstance(provider_meta, dict):
                provider_meta = {}
                run["provider_metadata"] = provider_meta
            provider_meta["stage1_analysis_provider"] = analysis_provider
            if run_second_pass:
                if analysis_provider == "perplexity":
                    run = await _apply_stage1_second_pass(
                        model=model,
                        user_query=user_query,
                        research_brief=bounded_research_brief,
                        run=run,
                        verification_profile=verification_profile,
                        supplementary_macro_news_override=shared_supplementary_macro_news,
                        prepass_source_rows=prepass_source_rows,
                        analysis_provider="perplexity",
                    )
                elif _is_openrouter_compatible_model(model):
                    run = await _apply_stage1_second_pass(
                        model=model,
                        user_query=user_query,
                        research_brief=bounded_research_brief,
                        run=run,
                        verification_profile=verification_profile,
                        supplementary_macro_news_override=shared_supplementary_macro_news,
                        prepass_source_rows=prepass_source_rows,
                        analysis_provider="openrouter",
                    )
                else:
                    provider_meta["stage1_second_pass_enabled"] = False
                    provider_meta["stage1_second_pass_skipped_reason"] = (
                        "model_not_openrouter_compatible"
                    )
                if shared_supplementary_macro_news is None:
                    maybe_shared = run.get("stage1_second_pass_supplementary_macro_news", {})
                    if isinstance(maybe_shared, dict) and maybe_shared:
                        shared_supplementary_macro_news = copy.deepcopy(maybe_shared)
            else:
                provider_meta["stage1_second_pass_enabled"] = False
                provider_meta["stage1_second_pass_skipped_reason"] = "shared_retrieval_mode"

        provider_meta = run.setdefault("provider_metadata", {})
        if not isinstance(provider_meta, dict):
            provider_meta = {}
            run["provider_metadata"] = provider_meta
        provider_meta["stage1_attempts"] = attempt
        provider_meta["stage1_retried"] = attempt > 1
        provider_meta["stage1_template_retry_triggered"] = template_retry_triggered
        provider_meta["stage1_template_retry_fallback_used"] = template_retry_fallback_used
        provider_meta["stage1_attempt_history"] = attempt_history
        provider_meta.setdefault(
            "stage1_shared_retrieval_enabled",
            bool(PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED),
        )
        provider_meta.setdefault("stage1_shared_retrieval_used", False)
        if final_retry_error:
            provider_meta["stage1_final_retry_error"] = final_retry_error
        return run

    def _log_stage1_model_result(model: str, run: Dict[str, Any], elapsed: float) -> None:
        if run and not run.get("error"):
            decode_meta = (run.get("provider_metadata", {}) or {}).get("source_decoding", {}) or {}
            provider_meta = (run.get("provider_metadata", {}) or {})
            stage1_attempts = int(provider_meta.get("stage1_attempts", 1))
            template_compliant = provider_meta.get("stage1_final_template_compliant")
            if template_compliant is None:
                template_compliant = provider_meta.get("template_compliant")
            second_pass_success = provider_meta.get("stage1_second_pass_success")
            citation_gate_passed = provider_meta.get("stage1_second_pass_citation_gate_passed")
            timeline_guard_passed = provider_meta.get("stage1_second_pass_timeline_guard_passed")
            timeline_guard_reason = provider_meta.get("stage1_second_pass_timeline_guard_reason")
            source_rows_count = provider_meta.get("stage1_second_pass_source_rows_count")
            timeline_evidence_count = provider_meta.get("stage1_second_pass_timeline_evidence_count")
            digest_facts = provider_meta.get("stage1_second_pass_fact_digest_v2_total_facts")
            digest_conflicts = provider_meta.get("stage1_second_pass_fact_digest_v2_conflicts")
            verification_template = provider_meta.get("stage1_second_pass_verification_template_id")
            injection_sources = provider_meta.get("stage1_second_pass_injection_sources")
            injection_categories = provider_meta.get("stage1_second_pass_injection_categories")
            shared_used = provider_meta.get("stage1_shared_retrieval_used")
            sonar_passed = provider_meta.get("sonar_multistep_passed")
            sonar_reason = provider_meta.get("sonar_multistep_reason")
            template_flag = (
                f", template_compliant={template_compliant}"
                if template_compliant is not None
                else ""
            )
            second_pass_flag = (
                f", second_pass_success={second_pass_success}"
                if second_pass_success is not None
                else ""
            )
            citation_flag = (
                f", citation_gate_passed={citation_gate_passed}"
                if citation_gate_passed is not None
                else ""
            )
            timeline_flag = (
                f", timeline_guard_passed={timeline_guard_passed}"
                if timeline_guard_passed is not None
                else ""
            )
            timeline_reason_flag = (
                f", timeline_guard_reason={timeline_guard_reason}"
                if timeline_guard_reason
                else ""
            )
            evidence_flag = (
                f", second_pass_sources={source_rows_count}, timeline_evidence={timeline_evidence_count}"
                if source_rows_count is not None or timeline_evidence_count is not None
                else ""
            )
            digest_flag = (
                f", digest_facts={digest_facts}, digest_conflicts={digest_conflicts}"
                if digest_facts is not None or digest_conflicts is not None
                else ""
            )
            verification_flag = (
                f", verification_template={verification_template}"
                if verification_template
                else ""
            )
            injection_flag = (
                f", injection_sources={injection_sources}, injection_categories={injection_categories}"
                if injection_sources is not None or injection_categories is not None
                else ""
            )
            shared_flag = (
                f", shared_retrieval_used={shared_used}"
                if shared_used is not None
                else ""
            )
            sonar_flag = (
                f", sonar_multistep_passed={sonar_passed}"
                if sonar_passed is not None
                else ""
            )
            sonar_reason_flag = (
                f", sonar_multistep_reason={sonar_reason}"
                if sonar_reason
                else ""
            )
            _progress_log(
                f"Stage1 model done: {model} "
                f"(elapsed={elapsed:.1f}s, result_count={run.get('result_count', 0)}, "
                f"decoded={decode_meta.get('decoded', 0)}/{decode_meta.get('attempted', 0)}, "
                f"attempts={stage1_attempts}{template_flag}{second_pass_flag}{citation_flag}"
                f"{timeline_flag}{timeline_reason_flag}{evidence_flag}{digest_flag}"
                f"{verification_flag}{injection_flag}{shared_flag}{sonar_flag}{sonar_reason_flag})"
            )
        else:
            stage1_attempts = int((run.get("provider_metadata", {}) or {}).get("stage1_attempts", 1))
            _progress_log(
                f"Stage1 model failed: {model} "
                f"(elapsed={elapsed:.1f}s, attempts={stage1_attempts}, "
                f"error={run.get('error') if run else 'unknown'})"
            )

    shared_retrieval_used = False
    shared_retrieval_model = ""
    shared_retrieval_error = ""
    stagger_seconds = max(0.0, float(PERPLEXITY_STAGE1_STAGGER_SECONDS))

    if shared_retrieval_requested:
        retrieval_candidates = perplexity_models if perplexity_models else models
        shared_retrieval_model = _select_shared_retrieval_model(retrieval_candidates)
        _progress_log(
            "Stage1 shared retrieval mode: "
            f"retrieval_model={shared_retrieval_model}, fanout_models={models}, "
            f"execution_mode={execution_mode}"
        )
        shared_start = perf_counter()
        shared_seed_run = await _gather_model_with_retries(
            shared_retrieval_model,
            run_second_pass=False,
        )
        shared_elapsed = perf_counter() - shared_start

        if shared_seed_run and not shared_seed_run.get("error"):
            shared_retrieval_used = True
            _progress_log(
                "Stage1 shared retrieval complete: "
                f"model={shared_retrieval_model}, elapsed={shared_elapsed:.1f}s, "
                f"result_count={shared_seed_run.get('result_count', 0)}"
            )
        else:
            shared_retrieval_error = (
                str(shared_seed_run.get("error", "unknown"))
                if isinstance(shared_seed_run, dict)
                else "unknown"
            )
            _progress_log(
                "Stage1 shared retrieval failed; falling back to per-model retrieval: "
                f"model={shared_retrieval_model}, elapsed={shared_elapsed:.1f}s, "
                f"error={shared_retrieval_error}"
            )
        _emit_stage1_progress(
            model=shared_retrieval_model,
            status="success" if shared_retrieval_used else "failed",
            phase="shared_seed",
            result_count=int(shared_seed_run.get("result_count", 0)) if isinstance(shared_seed_run, dict) else None,
        )

        if shared_retrieval_used:
            async def _run_shared_one(model: str) -> Dict[str, Any]:
                nonlocal shared_supplementary_macro_news
                model_start = perf_counter()
                _progress_log(f"Stage1 model start (shared fanout): {model}")
                run = copy.deepcopy(shared_seed_run)
                provider_meta = run.setdefault("provider_metadata", {})
                if not isinstance(provider_meta, dict):
                    provider_meta = {}
                    run["provider_metadata"] = provider_meta
                provider_meta["stage1_shared_retrieval_enabled"] = True
                provider_meta["stage1_shared_retrieval_used"] = True
                provider_meta["stage1_shared_retrieval_model"] = shared_retrieval_model
                provider_meta["stage1_analysis_model"] = model
                # In shared mode, retrieval is performed once by the seed model.
                # Override per-run analysis model attribution to avoid mislabeling.
                provider_meta["model"] = model
                provider_meta["stage1_shared_retrieval_result_count"] = int(
                    shared_seed_run.get("result_count", 0)
                )
                provider_meta["stage1_shared_retrieval_reused_for_model"] = model
                analysis_provider = _analysis_provider_for_model(model)
                provider_meta["stage1_analysis_provider"] = analysis_provider

                if PERPLEXITY_STAGE1_SECOND_PASS_ENABLED:
                    if analysis_provider == "perplexity":
                        run = await _apply_stage1_second_pass(
                            model=model,
                            user_query=user_query,
                            research_brief=bounded_research_brief,
                            run=run,
                            verification_profile=verification_profile,
                            supplementary_macro_news_override=shared_supplementary_macro_news,
                            prepass_source_rows=prepass_source_rows,
                            analysis_provider="perplexity",
                        )
                    elif _is_openrouter_compatible_model(model):
                        run = await _apply_stage1_second_pass(
                            model=model,
                            user_query=user_query,
                            research_brief=bounded_research_brief,
                            run=run,
                            verification_profile=verification_profile,
                            supplementary_macro_news_override=shared_supplementary_macro_news,
                            prepass_source_rows=prepass_source_rows,
                            analysis_provider="openrouter",
                        )
                    else:
                        provider_meta["stage1_second_pass_enabled"] = False
                        provider_meta["stage1_second_pass_skipped_reason"] = (
                            "model_not_openrouter_compatible"
                        )
                    if shared_supplementary_macro_news is None:
                        maybe_shared = run.get("stage1_second_pass_supplementary_macro_news", {})
                        if isinstance(maybe_shared, dict) and maybe_shared:
                            shared_supplementary_macro_news = copy.deepcopy(maybe_shared)
                else:
                    provider_meta["stage1_second_pass_enabled"] = False
                    provider_meta["stage1_second_pass_skipped_reason"] = "second_pass_disabled"

                elapsed = perf_counter() - model_start
                _log_stage1_model_result(model, run, elapsed)
                _emit_stage1_progress(
                    model=model,
                    status="success" if not run.get("error") else "failed",
                    phase="shared_fanout",
                    result_count=int(run.get("result_count", 0)) if isinstance(run, dict) else None,
                )
                return run

            if execution_mode == "staggered":
                for index, model in enumerate(models):
                    if index > 0 and stagger_seconds > 0:
                        _progress_log(
                            f"Stage1 waiting {stagger_seconds:.1f}s before next model: {model}"
                        )
                        await asyncio.sleep(stagger_seconds)
                    raw_runs.append(await _run_shared_one(model))
            else:
                shared_tasks = [_run_shared_one(model) for model in models]
                raw_runs = await asyncio.gather(*shared_tasks)

    if (
        mixed_mode_enabled
        and not authoritative_prepass_mode
        and not shared_retrieval_used
        and openrouter_pool_models
    ):
        _progress_log(
            "Stage1 mixed-mode fallback: shared retrieval unavailable; "
            f"running Perplexity pool only ({perplexity_models})"
        )
        models = list(perplexity_models) if perplexity_models else list(models)

    if not shared_retrieval_used:
        if execution_mode == "staggered":
            for index, model in enumerate(models):
                if index > 0 and stagger_seconds > 0:
                    _progress_log(
                        f"Stage1 waiting {stagger_seconds:.1f}s before next model: {model}"
                    )
                    await asyncio.sleep(stagger_seconds)
                model_start = perf_counter()
                _progress_log(f"Stage1 model start: {model}")
                run = await _gather_model_with_retries(model, run_second_pass=True)
                elapsed = perf_counter() - model_start
                _log_stage1_model_result(model, run, elapsed)
                _emit_stage1_progress(
                    model=model,
                    status="success" if not run.get("error") else "failed",
                    phase="model_complete",
                    result_count=int(run.get("result_count", 0)) if isinstance(run, dict) else None,
                )
                raw_runs.append(run)
        else:
            async def _run_one(model: str) -> Dict[str, Any]:
                model_start = perf_counter()
                _progress_log(f"Stage1 model start: {model}")
                run = await _gather_model_with_retries(model, run_second_pass=True)
                elapsed = perf_counter() - model_start
                _log_stage1_model_result(model, run, elapsed)
                _emit_stage1_progress(
                    model=model,
                    status="success" if not run.get("error") else "failed",
                    phase="model_complete",
                    result_count=int(run.get("result_count", 0)) if isinstance(run, dict) else None,
                )
                return run

            tasks = [_run_one(model) for model in models]
            raw_runs = await asyncio.gather(*tasks)

    stage1_results: List[Dict[str, Any]] = []
    per_model_research_runs: List[Dict[str, Any]] = []

    for model, run in zip(models, raw_runs):
        model_run = {"model": model, "result": run}
        per_model_research_runs.append(model_run)

        if run is None or run.get("error"):
            continue

        stage1_results.append(
            {
                "model": model,
                "response": _format_perplexity_research_as_stage1_response(model, run),
            }
        )

    claim_ledger = _build_claim_ledger_from_model_runs(
        per_model_research_runs,
        verification_profile=verification_profile,
    )
    baseline_market_facts = _extract_normalized_facts_from_query_text(user_query)
    deterministic_finance_lane = (
        _build_deterministic_finance_lane_from_claim_ledger(
            claim_ledger,
            baseline_market_facts=baseline_market_facts,
        )
        if DETERMINISTIC_FINANCE_LANE_ENABLED
        else {}
    )

    aggregated_search_results = _aggregate_perplexity_research_runs(
        user_query=user_query,
        ticker=ticker,
        model_runs=per_model_research_runs,
        depth=depth,
        claim_ledger=claim_ledger,
        deterministic_finance_lane=deterministic_finance_lane,
    )

    sonar_models_total = 0
    sonar_models_passed = 0
    sonar_failed_models: List[str] = []
    for model_run in per_model_research_runs:
        model = str(model_run.get("model", ""))
        result = model_run.get("result") or {}
        if not isinstance(result, dict):
            continue
        provider_meta = result.get("provider_metadata", {}) or {}
        if not isinstance(provider_meta, dict):
            provider_meta = {}
        is_sonar = bool(provider_meta.get("is_sonar_model", False)) or _is_sonar_model(model)
        if not is_sonar:
            continue
        sonar_models_total += 1
        passed = bool(provider_meta.get("sonar_multistep_passed", False))
        if passed:
            sonar_models_passed += 1
        else:
            sonar_failed_models.append(model)

    metadata = {
        "per_model_research_runs": per_model_research_runs,
        "aggregated_search_results": aggregated_search_results,
        "claim_ledger": claim_ledger,
        "deterministic_finance_lane": deterministic_finance_lane,
        "models_attempted": models,
        "models_succeeded": [item["model"] for item in stage1_results],
        "stage1_verification_template_id": str(template_id or ""),
        "stage1_verification_digest_sections": int(
            len((verification_profile.get("fact_digest_keywords") or {}).keys())
        ),
        "stage1_verification_timeline_focus_terms": list(
            verification_profile.get("timeline_focus_terms", []) or []
        ),
        "stage1_verification_compliance_markers": int(
            len(verification_profile.get("compliance_section_markers", []) or [])
        ),
        "stage1_verification_required_sections": list(
            [
                str(item[0]).strip().lower()
                for item in (verification_profile.get("compliance_section_markers", []) or [])
                if isinstance(item, (tuple, list)) and item
            ]
        ),
        "stage1_verification_critical_sections": list(
            sorted(verification_profile.get("compliance_critical_sections", set()) or [])
        ),
        "stage1_perplexity_model_preflight_enabled": bool(
            PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED
        ),
        "stage1_perplexity_model_preflight_timeout_seconds": float(
            PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS
        ),
        "stage1_perplexity_model_preflight_fail_open": bool(
            PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN
        ),
        "stage1_perplexity_model_preflight_skipped_reason": str(preflight_skipped_reason),
        "stage1_perplexity_model_preflight_requested_models": list(perplexity_models_requested),
        "stage1_perplexity_model_preflight_effective_models": list(perplexity_models),
        "stage1_perplexity_model_preflight_removed_models": list(preflight_removed_models),
        "stage1_perplexity_model_preflight_results": list(preflight_results),
        "stage1_mixed_mode_enabled": bool(mixed_mode_enabled),
        "stage1_mixed_mode_perplexity_pool": list(perplexity_models),
        "stage1_mixed_mode_openrouter_pool": list(openrouter_pool_models),
        "stage1_execution_mode": execution_mode,
        "stage1_stagger_seconds": float(PERPLEXITY_STAGE1_STAGGER_SECONDS),
        "stage1_max_attempts": int(max_attempts),
        "stage1_retry_backoff_seconds": float(base_backoff),
        "stage1_second_pass_enabled": bool(PERPLEXITY_STAGE1_SECOND_PASS_ENABLED),
        "stage1_second_pass_timeout_seconds": float(PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS),
        "stage1_second_pass_max_attempts": int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS),
        "stage1_second_pass_retry_backoff_seconds": float(
            PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS
        ),
        "stage1_second_pass_max_sources": int(PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES),
        "stage1_second_pass_max_chars_per_source": int(
            PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE
        ),
        "stage1_prepass_source_rows_supplied": bool(prepass_source_rows),
        "stage1_prepass_source_rows_count": int(len(prepass_source_rows or [])),
        "stage1_prepass_authoritative_mode": bool(authoritative_prepass_mode),
        # Legacy aliases retained for backward-compatible consumers.
        "stage1_source_rows_override_supplied": bool(prepass_source_rows),
        "stage1_source_rows_override_count": int(len(prepass_source_rows or [])),
        "stage1_second_pass_appendix_max_sources": int(
            PERPLEXITY_STAGE1_SECOND_PASS_APPENDIX_MAX_SOURCES
        ),
        "stage1_second_pass_max_output_tokens": int(
            PERPLEXITY_STAGE1_SECOND_PASS_MAX_OUTPUT_TOKENS
        ),
        "stage1_second_pass_reasoning_effort": normalize_reasoning_effort(
            str(PERPLEXITY_STAGE1_SECOND_PASS_REASONING_EFFORT or "")
        ),
        "stage1_second_pass_prompt_compression_enabled": bool(
            PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_COMPRESSION_ENABLED
        ),
        "stage1_second_pass_prompt_target_chars": int(
            PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_TARGET_CHARS
        ),
        "stage1_second_pass_doc_keypoints_max_per_source": int(
            PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_PER_SOURCE
        ),
        "stage1_second_pass_doc_keypoints_max_words_per_source": int(
            PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_WORDS_PER_SOURCE
        ),
        "stage1_second_pass_doc_keypoints_max_fact_chars": int(
            PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS
        ),
        "stage1_cashflow_detection_max_sources": int(STAGE1_CASHFLOW_DETECTION_MAX_SOURCES),
        "stage1_cashflow_classifier_enabled": bool(STAGE1_CASHFLOW_CLASSIFIER_ENABLED),
        "stage1_cashflow_classifier_model": str(STAGE1_CASHFLOW_CLASSIFIER_MODEL or ""),
        "stage1_cashflow_classifier_timeout_seconds": float(
            STAGE1_CASHFLOW_CLASSIFIER_TIMEOUT_SECONDS
        ),
        "stage1_cashflow_classifier_max_output_tokens": int(
            STAGE1_CASHFLOW_CLASSIFIER_MAX_OUTPUT_TOKENS
        ),
        "stage1_cashflow_classifier_reasoning_effort": normalize_reasoning_effort(
            str(STAGE1_CASHFLOW_CLASSIFIER_REASONING_EFFORT or "")
        ),
        "stage1_cashflow_classifier_min_confidence_pct": float(
            STAGE1_CASHFLOW_CLASSIFIER_MIN_CONFIDENCE_PCT
        ),
        "stage1_supplementary_news_enabled": bool(PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_ENABLED),
        "stage1_supplementary_news_max_sources": int(
            PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_SOURCES
        ),
        "stage1_supplementary_news_retrieval_max_sources": int(
            PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_RETRIEVAL_MAX_SOURCES
        ),
        "stage1_supplementary_news_max_recency_days": int(
            PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_RECENCY_DAYS
        ),
        "stage1_asx_deterministic_announcements_enabled": bool(
            ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED
        ),
        "stage1_asx_deterministic_target_announcements": int(
            ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS
        ),
        "stage1_asx_deterministic_lookback_years": int(
            ASX_DETERMINISTIC_LOOKBACK_YEARS
        ),
        "stage1_asx_deterministic_price_sensitive_only": bool(
            ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY
        ),
        "stage1_asx_deterministic_include_non_sensitive_fill": bool(
            ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL
        ),
        "stage1_asx_deterministic_max_decode": int(ASX_DETERMINISTIC_MAX_DECODE),
        "stage1_asx_deterministic_fetch_timeout_seconds": float(
            ASX_DETERMINISTIC_FETCH_TIMEOUT_SECONDS
        ),
        "stage1_timeline_guard_enabled": bool(PERPLEXITY_STAGE1_TIMELINE_GUARD_ENABLED),
        "stage1_timeline_guard_hard_fail": bool(PERPLEXITY_STAGE1_TIMELINE_GUARD_HARD_FAIL),
        "stage1_timeline_digest_max_items": int(PERPLEXITY_STAGE1_TIMELINE_DIGEST_MAX_ITEMS),
        "stage1_fact_digest_v2_enabled": bool(PERPLEXITY_STAGE1_FACT_DIGEST_V2_ENABLED),
        "stage1_fact_digest_v2_max_facts_per_section": int(
            PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_FACTS_PER_SECTION
        ),
        "stage1_fact_digest_v2_max_summary_bullets": int(
            PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_SUMMARY_BULLETS
        ),
        "stage1_fact_digest_v2_max_narrative_words": int(
            PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_NARRATIVE_WORDS
        ),
        "stage1_second_pass_citation_gate_enabled": bool(
            PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED
        ),
        "stage1_second_pass_citation_min_count": int(
            PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MIN_COUNT
        ),
        "stage1_second_pass_citation_max_uncited_numeric_lines": int(
            PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MAX_UNCITED_NUMERIC_LINES
        ),
        "stage1_second_pass_compliance_min_score": float(
            PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE
        ),
        "stage1_second_pass_compliance_min_rubric_coverage_pct": float(
            PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT
        ),
        "stage1_second_pass_compliance_min_numeric_citation_pct": float(
            PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT
        ),
        "stage1_second_pass_compliance_catastrophic_score": float(
            PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_CATASTROPHIC_SCORE
        ),
        "stage1_shared_retrieval_config_enabled": bool(PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED),
        "stage1_shared_retrieval_enabled": bool(PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED),
        "stage1_shared_retrieval_requested": bool(shared_retrieval_requested),
        "stage1_shared_retrieval_used": bool(shared_retrieval_used),
        "stage1_shared_retrieval_model": str(shared_retrieval_model),
        "stage1_shared_retrieval_error": str(shared_retrieval_error),
        "stage1_openai_guardrails_enabled": bool(
            PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED
        ),
        "stage1_openai_base_max_sources": int(PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES),
        "stage1_openai_base_max_steps": int(PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS),
        "stage1_openai_base_reasoning_effort": normalize_reasoning_effort(
            str(PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT or "")
        ),
        "stage1_preset_strategy": str(PERPLEXITY_PRESET_STRATEGY),
        "stage1_preset_deep": str(PERPLEXITY_PRESET_DEEP),
        "stage1_preset_advanced": str(PERPLEXITY_PRESET_ADVANCED),
        "stage1_multi_wave_enabled": bool(multi_wave_enabled),
        "stage1_multi_wave_max_waves": int(max_waves),
        "stage1_multi_wave_gap_query_limit": int(gap_query_limit),
        "stage1_multi_wave_min_new_primary_sources": int(min_new_primary_sources),
        "stage1_sonar_multistep_required": bool(PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED),
        "deterministic_finance_lane_enabled": bool(DETERMINISTIC_FINANCE_LANE_ENABLED),
        "stage1_sonar_models_total": int(sonar_models_total),
        "stage1_sonar_models_passed": int(sonar_models_passed),
        "stage1_sonar_models_failed": sonar_failed_models,
        "claim_ledger_raw_claims": int((claim_ledger.get("counts", {}) or {}).get("raw_claims", 0)),
        "claim_ledger_resolved_fields": int(
            (claim_ledger.get("counts", {}) or {}).get("resolved_fields", 0)
        ),
        "claim_ledger_conflicts": int((claim_ledger.get("counts", {}) or {}).get("conflicts", 0)),
        "deterministic_finance_lane_status": str(
            (deterministic_finance_lane or {}).get("status", "")
        ),
        "market_facts_baseline_fields_detected": int(
            len([key for key, value in (baseline_market_facts or {}).items() if value is not None])
        ),
    }

    total_elapsed = perf_counter() - total_start
    _progress_log(
        "Stage1 perplexity emulation complete: "
        f"succeeded={len(metadata['models_succeeded'])}/{len(models)}, "
        f"aggregated_sources={aggregated_search_results.get('result_count', 0)}, "
        f"elapsed={total_elapsed:.1f}s"
    )

    return stage1_results, metadata


def _format_perplexity_research_as_stage1_response(model: str, run: Dict[str, Any]) -> str:
    """Turn a Perplexity research run into Stage 1 response text."""
    second_pass_response = str(run.get("stage1_analysis_response", "")).strip()
    if second_pass_response:
        return second_pass_response

    lines = [
        f"Perplexity Deep Research Run for model: {model}",
        "",
    ]

    provider_meta = run.get("provider_metadata", {})
    if provider_meta:
        lines.append(
            f"Profile: model={provider_meta.get('model', model)} "
            f"preset={provider_meta.get('preset', 'n/a')} "
            f"tools={', '.join(provider_meta.get('tools', [])) or 'n/a'}"
        )
        decode_meta = provider_meta.get("source_decoding", {}) or {}
        attempted = int(decode_meta.get("attempted", 0))
        decoded = int(decode_meta.get("decoded", 0))
        if attempted > 0:
            lines.append(f"Decoding: {decoded}/{attempted} sources decoded locally")
        lines.append("")

    summary = (run.get("research_summary") or "").strip()
    if summary:
        lines.append("Findings:")
        lines.append(summary)
        lines.append("")
    else:
        updates = run.get("latest_updates", [])[:6]
        if updates:
            lines.append("Latest Updates (with links):")
            lines.append("| Date | Update | Why it matters | Source |")
            lines.append("|---|---|---|---|")
            for update in updates:
                date_value = str(update.get("date", "Unknown")).replace("|", "\\|")
                title = str(update.get("update", "Update")).replace("|", "\\|")
                why = str(update.get("why_it_matters", "")).replace("|", "\\|")
                source = update.get("source_url", "")
                source_cell = f"[link]({source})" if source else "N/A"
                lines.append(f"| {date_value} | {title} | {why} | {source_cell} |")
            lines.append("")

    sources = run.get("results", [])[:8]
    if sources:
        lines.append("Key Sources:")
        for idx, source in enumerate(sources, start=1):
            title = source.get("title", "Untitled")
            url = source.get("url", "")
            snippet = source.get("content", "")
            snippet = snippet[:220] + ("..." if len(snippet) > 220 else "")
            lines.append(f"{idx}. {title}")
            if url:
                lines.append(f"   URL: {url}")
            if snippet:
                lines.append(f"   Note: {snippet}")

    return "\n".join(lines).strip()


def _aggregate_perplexity_research_runs(
    user_query: str,
    ticker: Optional[str],
    model_runs: List[Dict[str, Any]],
    depth: str,
    claim_ledger: Optional[Dict[str, Any]] = None,
    deterministic_finance_lane: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Aggregate per-model Perplexity runs into one search/evidence payload."""
    merged_by_url: Dict[str, Dict[str, Any]] = {}
    key_facts: List[str] = []
    failed_models: List[str] = []

    for model_run in model_runs:
        model = model_run["model"]
        result = model_run.get("result") or {}

        if result.get("error"):
            failed_models.append(model)
            continue

        latest_updates = result.get("latest_updates", []) or []
        for update in latest_updates[:2]:
            date_value = str(update.get("date", "Unknown")).strip()
            update_title = str(update.get("update", "")).strip()
            if update_title:
                key_facts.append(f"{model}: {date_value} - {update_title[:200]}")

        if not latest_updates:
            summary = (result.get("research_summary") or "").strip()
            if summary:
                first_line = summary.splitlines()[0].strip()
                if first_line:
                    key_facts.append(f"{model}: {first_line[:240]}")

        for src in result.get("results", []):
            url = src.get("url", "").strip()
            if not url:
                continue

            entry = merged_by_url.get(url)
            if entry is None:
                entry = {
                    "title": src.get("title", "Untitled"),
                    "url": url,
                    "content": src.get("content", ""),
                    "score": float(src.get("score", 0.0)),
                    "published_at": src.get("published_at", ""),
                    "models": [model],
                }
                merged_by_url[url] = entry
            else:
                if model not in entry["models"]:
                    entry["models"].append(model)
                entry["score"] = max(entry["score"], float(src.get("score", 0.0)))
                if len(src.get("content", "")) > len(entry.get("content", "")):
                    entry["content"] = src.get("content", "")
                if not entry.get("published_at") and src.get("published_at"):
                    entry["published_at"] = src.get("published_at")

    merged_results = list(merged_by_url.values())
    merged_results.sort(key=lambda item: (-len(item["models"]), -item["score"]))

    formatted_results = []
    for item in merged_results[:MAX_SOURCES]:
        content = item.get("content", "").strip()
        if not content:
            content = "Referenced by council models."

        result_item = {
            "title": item.get("title", "Untitled"),
            "url": item.get("url", ""),
            "content": content,
            "score": item.get("score", 0.0),
            "referenced_by_models": list(item.get("models", []) or []),
        }
        if item.get("published_at"):
            result_item["published_at"] = item["published_at"]
        formatted_results.append(result_item)

    missing_data = []
    if not formatted_results:
        missing_data.append("No sources were retrieved from emulated Perplexity council runs.")
    if failed_models:
        missing_data.append(f"Models with failed research runs: {', '.join(failed_models)}")
    if ticker:
        exchange = _infer_exchange_from_ticker(ticker)
        expected_domains = _expected_domains_for_exchange(exchange)
        if expected_domains and not _has_expected_source_domain(formatted_results, expected_domains):
            missing_data.append(
                f"No expected primary-source domain found in aggregated {exchange.upper()} model research."
            )

    evidence_pack_sources = []
    for result in formatted_results:
        evidence_pack_sources.append(
            {
                "url": result.get("url", ""),
                "title": result.get("title", "Untitled"),
                "snippet": result.get("content", ""),
                "source_type": "web",
                "published_at": result.get("published_at", ""),
                "score": float(result.get("score", 0.0)),
                "provider": "perplexity",
            }
        )

    evidence_pack = {
        "question": user_query,
        "ticker": ticker or "",
        "provider": "perplexity_council_emulated",
        "depth": depth,
        "generated_at": datetime.utcnow().isoformat(),
        "sources": evidence_pack_sources,
        "key_facts": key_facts[:12],
        "missing_data": missing_data,
        "claim_ledger": claim_ledger or {},
        "deterministic_finance_lane": deterministic_finance_lane or {},
    }

    return {
        "query": user_query,
        "results": formatted_results,
        "result_count": len(formatted_results),
        "performed_at": datetime.utcnow().isoformat(),
        "search_type": "perplexity_emulated_council",
        "provider": "perplexity",
        "evidence_pack": evidence_pack,
        "metadata": {
            "claim_ledger_counts": (
                (claim_ledger or {}).get("counts", {})
                if isinstance(claim_ledger, dict)
                else {}
            ),
            "deterministic_finance_lane_status": str(
                (deterministic_finance_lane or {}).get("status", "")
            ),
        },
    }
