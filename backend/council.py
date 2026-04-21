"""3-stage LLM Council orchestration."""

import asyncio
import copy
import json
import re
import uuid
import httpx
from html import unescape
from typing import List, Dict, Any, Tuple, Optional, Callable
from datetime import datetime
from time import perf_counter
from urllib.parse import urljoin, urlparse, parse_qs
from .openrouter import query_models_parallel, query_model
from .reasoning import build_reasoning_payload, normalize_reasoning_effort
from .config import (
    OPENROUTER_API_KEY,
    COUNCIL_MODELS,
    CHAIRMAN_MODEL,
    PERPLEXITY_COUNCIL_MODELS,
    PERPLEXITY_STAGE1_MIXED_MODE_ENABLED,
    PERPLEXITY_STAGE1_OPENROUTER_MODELS,
    PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED,
    PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS,
    PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN,
    RESEARCH_DEPTH,
    MAX_SOURCES,
    ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED,
    ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS,
    ASX_DETERMINISTIC_LOOKBACK_YEARS,
    ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY,
    ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL,
    ASX_DETERMINISTIC_MAX_DECODE,
    ASX_DETERMINISTIC_FETCH_TIMEOUT_SECONDS,
    PERPLEXITY_API_KEY,
    PERPLEXITY_API_URL,
    PERPLEXITY_STAGE1_EXECUTION_MODE,
    PERPLEXITY_STAGE1_STAGGER_SECONDS,
    PERPLEXITY_STAGE1_ATTACHMENT_CONTEXT_MAX_CHARS,
    PERPLEXITY_STAGE1_MULTI_WAVE_ENABLED,
    PERPLEXITY_STAGE1_MULTI_WAVE_MAX_WAVES,
    PERPLEXITY_STAGE1_MULTI_WAVE_GAP_QUERY_LIMIT,
    PERPLEXITY_STAGE1_MULTI_WAVE_MIN_NEW_PRIMARY_SOURCES,
    PERPLEXITY_STAGE1_MAX_ATTEMPTS,
    PERPLEXITY_STAGE1_MAX_RETRIES,
    PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS,
    PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS,
    PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE,
    PERPLEXITY_STAGE1_SECOND_PASS_MAX_OUTPUT_TOKENS,
    PERPLEXITY_STAGE1_SECOND_PASS_REASONING_EFFORT,
    PERPLEXITY_STAGE1_SECOND_PASS_APPENDIX_MAX_SOURCES,
    PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_COMPRESSION_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_TARGET_CHARS,
    PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_PER_SOURCE,
    PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_WORDS_PER_SOURCE,
    PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS,
    STAGE1_CASHFLOW_DETECTION_MAX_SOURCES,
    STAGE1_CASHFLOW_CLASSIFIER_ENABLED,
    STAGE1_CASHFLOW_CLASSIFIER_MODEL,
    STAGE1_CASHFLOW_CLASSIFIER_TIMEOUT_SECONDS,
    STAGE1_CASHFLOW_CLASSIFIER_MAX_OUTPUT_TOKENS,
    STAGE1_CASHFLOW_CLASSIFIER_REASONING_EFFORT,
    STAGE1_CASHFLOW_CLASSIFIER_MIN_CONFIDENCE_PCT,
    STAGE1_TRUNCATION_CHECKER_ENABLED,
    STAGE1_TRUNCATION_CHECKER_MODEL,
    STAGE1_TRUNCATION_CHECKER_TIMEOUT_SECONDS,
    STAGE1_TRUNCATION_CHECKER_MAX_OUTPUT_TOKENS,
    STAGE1_TRUNCATION_CHECKER_REASONING_EFFORT,
    STAGE1_TRUNCATION_CHECKER_MIN_CONFIDENCE_PCT,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_ENABLED,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_SOURCES,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_RETRIEVAL_MAX_SOURCES,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_RECENCY_DAYS,
    XAI_API_KEY,
    XAI_API_URL,
    STAGE1_SUPPLEMENTARY_XAI_MODEL,
    STAGE1_SUPPLEMENTARY_XAI_TIMEOUT_SECONDS,
    STAGE1_SUPPLEMENTARY_XAI_MAX_TOKENS,
    STAGE1_SUPPLEMENTARY_XAI_TEMPERATURE,
    STAGE1_SUPPLEMENTARY_XAI_MAX_TOOL_ITERATIONS,
    PERPLEXITY_STAGE1_TIMELINE_GUARD_ENABLED,
    PERPLEXITY_STAGE1_TIMELINE_GUARD_HARD_FAIL,
    PERPLEXITY_STAGE1_TIMELINE_DIGEST_MAX_ITEMS,
    PERPLEXITY_STAGE1_FACT_DIGEST_V2_ENABLED,
    PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_FACTS_PER_SECTION,
    PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_SUMMARY_BULLETS,
    PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_NARRATIVE_WORDS,
    PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED,
    PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MIN_COUNT,
    PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MAX_UNCITED_NUMERIC_LINES,
    PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE,
    PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT,
    PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT,
    PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_CATASTROPHIC_SCORE,
    PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED,
    PERPLEXITY_STAGE1_SHARED_RETRIEVAL_MODEL,
    PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED,
    PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES,
    PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS,
    PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT,
    PERPLEXITY_STAGE1_OPENAI_BASE_DOWNGRADE_HIGH_REASONING,
    STAGE2_REVISION_PASS_ENABLED,
    STAGE2_REVISION_PASS_TIMEOUT_SECONDS,
    STAGE2_REVISION_PASS_MAX_OUTPUT_TOKENS,
    STAGE2_RECONCILIATION_ENABLED,
    STAGE2_RECONCILIATION_MODEL,
    STAGE2_RECONCILIATION_TIMEOUT_SECONDS,
    STAGE2_RECONCILIATION_MAX_OUTPUT_TOKENS,
    STAGE2_RECONCILIATION_MAX_SOURCE_CHARS,
    STAGE2_RECONCILIATION_MAX_RESPONSE_CHARS,
    STAGE2_RECONCILIATION_TOP_N,
    PERPLEXITY_PRESET_STRATEGY,
    PERPLEXITY_PRESET_DEEP,
    PERPLEXITY_PRESET_ADVANCED,
    PERPLEXITY_STREAM_ENABLED,
    PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED,
    DETERMINISTIC_FINANCE_LANE_ENABLED,
    PROGRESS_LOGGING,
    SYSTEM_ENABLED,
    SYSTEM_SHUTDOWN_REASON,
    SYSTEM_ALLOW_DIAGNOSTICS_WHEN_DISABLED,
)


def _progress_log(message: str) -> None:
    """Timestamped progress logs for long-running research orchestration."""
    if not PROGRESS_LOGGING:
        return
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}][council] {message}", flush=True)


def _ensure_system_enabled(*, diagnostic_mode: bool = False) -> None:
    """Block execution when global shutdown is active."""
    if SYSTEM_ENABLED:
        return
    if diagnostic_mode and SYSTEM_ALLOW_DIAGNOSTICS_WHEN_DISABLED:
        return
    reason = SYSTEM_SHUTDOWN_REASON or "maintenance mode active"
    raise RuntimeError(f"System disabled: {reason}")


def _extract_status_code(error_text: str) -> Optional[int]:
    match = re.search(r"Perplexity API error:\s*(\d+)", error_text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _is_retryable_stage1_error(error_text: str) -> bool:
    text = (error_text or "").lower()
    if not text:
        return False
    if "timed out" in text or "timeout" in text:
        return True
    if "perplexity research failed" in text:
        return True
    status_code = _extract_status_code(error_text)
    if status_code in {408, 409, 425, 429, 500, 502, 503, 504}:
        return True
    return False


def _is_gpt_5_4_model(model: str) -> bool:
    """Return True when the model is GPT-5.4 and should default to low reasoning."""
    key = str(model or "").strip().lower()
    return key in {"openai/gpt-5.4", "gpt-5.4"} or key.endswith("/gpt-5.4")


def _build_stage1_attempt_profile(
    model: str,
    attempt: int,
    depth: str,
    base_preset: str,
    base_max_sources: int,
    base_max_steps: int,
    base_max_output_tokens: int,
    base_reasoning_effort: str,
) -> Dict[str, Any]:
    """
    Build per-attempt request profile.

    For OpenAI-routed Stage 1 calls, retries progressively reduce workload while
    keeping the same analysis prompt/rubric.
    """
    requested_output_tokens = int(base_max_output_tokens)
    profile: Dict[str, Any] = {
        "name": "default",
        "preset": _resolve_stage1_preset_for_attempt(
            attempt=attempt,
            depth=depth,
            base_preset=base_preset,
        ),
        "max_sources": max(1, int(base_max_sources)),
        "max_steps": max(1, int(base_max_steps)),
        # 0 means "do not send max_output_tokens; let provider-side limits apply".
        "max_output_tokens": (
            max(512, requested_output_tokens)
            if requested_output_tokens > 0
            else 0
        ),
        "reasoning_effort": normalize_reasoning_effort(base_reasoning_effort),
    }

    model_key = (model or "").strip().lower()
    if not model_key.startswith("openai/"):
        return profile

    gpt_54_low_default = _is_gpt_5_4_model(model)
    if gpt_54_low_default:
        profile["reasoning_effort"] = "low"

    if PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED:
        max_sources_cap = max(1, int(PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES))
        max_steps_cap = max(1, int(PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS))
        profile["max_sources"] = min(int(profile["max_sources"]), max_sources_cap)
        profile["max_steps"] = min(int(profile["max_steps"]), max_steps_cap)

    if attempt == 1:
        if PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED:
            profile["name"] = "openai_base_guardrail"
            forced_effort = str(PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT or "").strip().lower()
            if forced_effort in {"xhigh", "high", "medium", "low", "minimal"} and not gpt_54_low_default:
                profile["reasoning_effort"] = forced_effort
            elif (
                PERPLEXITY_STAGE1_OPENAI_BASE_DOWNGRADE_HIGH_REASONING
                and profile["reasoning_effort"] == "high"
                and not gpt_54_low_default
            ):
                profile["reasoning_effort"] = "medium"
        if gpt_54_low_default:
            profile["reasoning_effort"] = "low"
        return profile

    base_effort = normalize_reasoning_effort(base_reasoning_effort)
    if gpt_54_low_default:
        base_effort = "low"

    if attempt == 2:
        profile["name"] = "openai_retry_2"
        profile["max_sources"] = max(4, int(profile["max_sources"]) - 1)
        profile["max_steps"] = max(2, int(profile["max_steps"]) - 1)
        # Step down one level first: high -> medium -> low.
        if base_effort == "xhigh":
            profile["reasoning_effort"] = "high"
        elif base_effort == "high":
            profile["reasoning_effort"] = "medium"
        elif base_effort == "medium":
            profile["reasoning_effort"] = "low"
        else:
            profile["reasoning_effort"] = "low"
        return profile

    profile["name"] = "openai_retry_3plus"
    profile["max_sources"] = max(3, int(profile["max_sources"]) - 2)
    profile["max_steps"] = max(1, int(profile["max_steps"]) - 2)
    profile["reasoning_effort"] = "low"
    return profile


def _resolve_stage1_preset_for_attempt(
    *,
    attempt: int,
    depth: str,
    base_preset: str,
) -> str:
    """Resolve Stage 1 retrieval preset with optional dual-preset strategy."""
    strategy = str(PERPLEXITY_PRESET_STRATEGY or "single").strip().lower()
    deep_preset = str(PERPLEXITY_PRESET_DEEP or "deep-research").strip() or "deep-research"
    advanced_preset = (
        str(PERPLEXITY_PRESET_ADVANCED or "advanced-deep-research").strip()
        or "advanced-deep-research"
    )
    effective_base = str(base_preset or "").strip() or deep_preset
    normalized_depth = str(depth or "").strip().lower()

    if normalized_depth != "deep":
        if effective_base == "deep-research":
            return "search"
        return effective_base

    if strategy in {"adaptive", "dual_retry"}:
        return deep_preset if int(attempt) <= 1 else advanced_preset
    if strategy in {"advanced", "advanced_only"}:
        return advanced_preset
    if strategy in {"deep", "deep_only"}:
        return deep_preset

    # Legacy/default behavior: fixed single preset.
    return effective_base


def _extract_synthesis_block(summary_text: str) -> str:
    """Extract the synthesis section from normalized Stage 1 summary text."""
    text = summary_text or ""
    marker = "### Synthesis"
    latest_marker = "### Latest Updates"
    if marker not in text:
        return text.strip()
    tail = text.split(marker, 1)[1]
    if latest_marker in tail:
        tail = tail.split(latest_marker, 1)[0]
    return tail.strip()


def _stage1_requires_template_compliance(user_query: str, research_brief: str) -> bool:
    """Heuristic: enforce compliance when prompt clearly asks for scored/template analysis."""
    joined = f"{user_query or ''}\n{research_brief or ''}".lower()
    triggers = (
        "quality score",
        "value score",
        "out of 100",
        "npv",
        "price target",
        "certainty",
        "headwinds",
        "tailwinds",
        "rubric",
    )
    return any(token in joined for token in triggers)


def _evaluate_stage1_template_compliance(
    summary_text: str,
    user_query: str,
    research_brief: str,
    *,
    section_markers: Optional[List[Tuple[str, List[str]]]] = None,
) -> Dict[str, Any]:
    """
    Evaluate whether Stage 1 output is analysis-grade vs a shallow source log.
    """
    requires = _stage1_requires_template_compliance(user_query, research_brief)
    synthesis = _extract_synthesis_block(summary_text)
    synthesis_lower = synthesis.lower()

    markers_spec = section_markers or _STAGE1_RUBRIC_SECTION_MARKERS
    section_hits = {
        section_id: any(marker in synthesis_lower for marker in markers)
        for section_id, markers in markers_spec
    }
    hit_count = sum(1 for hit in section_hits.values() if hit)
    primary_hit_count = sum(
        1
        for section_id in _STAGE1_RUBRIC_CRITICAL_SECTIONS
        if section_hits.get(section_id)
    )
    secondary_hit_count = max(0, hit_count - primary_hit_count)
    minimum_chars = 220
    is_substantive = len(synthesis) >= minimum_chars
    compliant = (not requires) or (
        is_substantive
        and (
            (primary_hit_count >= 1 and hit_count >= 2)
            or hit_count >= 3
        )
    )

    reason = "ok"
    if requires and not compliant:
        reason = (
            "non_compliant_template_summary("
            f"chars={len(synthesis)}, "
            f"primary_hits={primary_hit_count}, "
            f"secondary_hits={secondary_hit_count})"
        )

    return {
        "required": requires,
        "compliant": compliant,
        "reason": reason,
        "synthesis_chars": len(synthesis),
        "marker_hits": hit_count,
        "primary_marker_hits": primary_hit_count,
        "secondary_marker_hits": secondary_hit_count,
    }


def _build_strict_research_brief(base_brief: str) -> str:
    """Append strict contract to force full rubric analysis on retry."""
    strict_contract = (
        "STRICT OUTPUT CONTRACT (must follow exactly):\n"
        "- Provide a full investment analysis, not a research log.\n"
        "- Include explicit Quality Score (0-100) and Value Score (0-100).\n"
        "- Include NPV/risked-NPV discussion with assumptions.\n"
        "- Include explicit 12-month and 24-month price targets.\n"
        "- Include certainty percentage for 24-month milestones.\n"
        "- Include key quantitative and qualitative headwinds/tailwinds.\n"
        "- Tie numeric claims to cited source URLs (or mark as ESTIMATE).\n"
        "- If data is missing, state assumptions clearly and proceed.\n"
    )
    combined = (base_brief or "").strip()
    if strict_contract not in combined:
        combined = f"{combined}\n\n{strict_contract}".strip()
    return combined


def _infer_exchange_from_ticker(ticker: str) -> str:
    """Infer exchange from common ticker formats (PREFIX:SYM or suffix)."""
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        return ""
    if ":" in normalized:
        return normalized.split(":", 1)[0]

    suffix_map = {
        ".AX": "ASX",
        ".N": "NYSE",
        ".O": "NASDAQ",
        ".Q": "NASDAQ",
        ".TO": "TSX",
        ".V": "TSXV",
        ".L": "LSE",
    }
    for suffix, exchange in suffix_map.items():
        if normalized.endswith(suffix):
            return exchange
    return ""


def _expected_domains_for_exchange(exchange: str) -> List[str]:
    """Exchange-primary domains for missing-data diagnostics."""
    fallback_map = {
        "ASX": ["asx.com.au", "marketindex.com.au", "wcsecure.weblink.com.au"],
        "NYSE": ["sec.gov"],
        "NASDAQ": ["sec.gov"],
        "TSX": ["globenewswire.com"],
        "TSXV": ["globenewswire.com"],
        "LSE": ["londonstockexchange.com", "investegate.co.uk"],
        "AIM": ["londonstockexchange.com", "investegate.co.uk"],
    }

    key = (exchange or "").strip()
    key_upper = key.upper()
    try:
        from .template_loader import get_template_loader

        loader = get_template_loader()
        normalized = loader.normalize_exchange(key) or loader.normalize_exchange(key_upper)
        if not normalized and key_upper:
            alias_map = {
                "ASX": "asx",
                "NYSE": "nyse",
                "NASDAQ": "nasdaq",
                "TSX": "tsx",
                "TSXV": "tsxv",
                "LSE": "lse",
                "AIM": "aim",
            }
            normalized = alias_map.get(key_upper)
        if normalized:
            params = loader.get_exchange_retrieval_params(normalized)
            suffixes = [
                str(item).strip().lower()
                for item in (params.get("allowed_domain_suffixes", []) or [])
                if str(item).strip()
            ]
            deduped: List[str] = []
            for item in suffixes:
                if item not in deduped:
                    deduped.append(item)
            if deduped:
                return deduped
    except Exception:
        pass

    return fallback_map.get(key_upper, [])


def _has_expected_source_domain(results: List[Dict[str, Any]], expected_domains: List[str]) -> bool:
    expected = [domain.lower() for domain in expected_domains if domain]
    if not expected:
        return True
    for result in results:
        url = str(result.get("url", "")).strip()
        if not url:
            continue
        try:
            host = urlparse(url).netloc.lower()
        except Exception:
            continue
        if any(domain in host for domain in expected):
            return True
    return False


_SUPPLEMENTARY_MACRO_PROFILE_CONFIG: Dict[str, Dict[str, Any]] = {
    "oil_gas": {
        "sector_label": "oil and gas sector",
        "query_focus": (
            "Brent WTI Henry Hub trend, OPEC+ policy decisions, supply disruption risk "
            "(Middle East/Strait of Hormuz), inventories and demand balance"
        ),
        "terms": [
            "brent",
            "wti",
            "henry hub",
            "opec",
            "opec+",
            "hormuz",
            "strait of hormuz",
            "inventory",
            "inventories",
            "oil demand",
            "gas demand",
            "supply disruption",
        ],
    },
    "uranium": {
        "sector_label": "uranium sector",
        "query_focus": (
            "U3O8 spot/term pricing, utility contracting cycle, reactor restart/build pipeline, "
            "policy and fuel-cycle constraints"
        ),
        "terms": [
            "u3o8",
            "uranium spot",
            "uranium term",
            "utility contracting",
            "reactor",
            "nuclear build",
            "enrichment",
            "conversion",
            "kazatomprom",
            "cameco",
        ],
    },
    "gold": {
        "sector_label": "gold mining sector",
        "query_focus": (
            "gold market drivers: real yields, USD trend, central-bank demand, "
            "safe-haven/geopolitical flows"
        ),
        "terms": [
            "gold price",
            "real yields",
            "usd index",
            "central bank gold",
            "bullion demand",
            "safe haven",
            "geopolitical risk",
        ],
    },
    "silver": {
        "sector_label": "silver mining sector",
        "query_focus": (
            "silver market drivers: industrial/PV demand, mine supply, inventory trends, "
            "gold-silver ratio regime"
        ),
        "terms": [
            "silver price",
            "gold silver ratio",
            "pv demand",
            "solar demand",
            "industrial demand",
            "silver inventory",
            "mine supply",
        ],
    },
    "copper": {
        "sector_label": "copper mining sector",
        "query_focus": (
            "copper market drivers: demand cycle, inventories/TC-RC, supply disruptions, "
            "grid/electrification demand"
        ),
        "terms": [
            "copper price",
            "lme copper",
            "comex copper",
            "inventory",
            "treatment charges",
            "tc/rc",
            "supply disruption",
            "china demand",
            "electrification demand",
        ],
    },
    "lithium": {
        "sector_label": "lithium sector",
        "query_focus": (
            "lithium market drivers: spodumene/LCE pricing, conversion margins, EV demand, "
            "inventory cycle and supply curtailments"
        ),
        "terms": [
            "lithium price",
            "spodumene",
            "lce",
            "carbonate",
            "hydroxide",
            "ev demand",
            "battery demand",
            "conversion margin",
            "inventory cycle",
        ],
    },
}

def _resolve_template_commodity_profile(template_id: str) -> str:
    """Resolve commodity profile from template behavior."""
    key = str(template_id or "").strip()
    if not key:
        return ""
    try:
        from .template_loader import get_template_loader

        loader = get_template_loader()
        behavior = loader.get_template_behavior(key) or {}
        profile = str(behavior.get("commodity_profile", "")).strip().lower()
        return profile if profile in _SUPPLEMENTARY_MACRO_PROFILE_CONFIG else ""
    except Exception:
        return ""


def _build_supplementary_macro_summary_prompt(*, sector_label: str) -> str:
    """Build xAI prompt for supplementary macro context (single dense paragraph)."""
    sector = str(sector_label or "").strip() or "sector"
    return (
        f"Provide one single-paragraph macro news brief for the [{sector}]. "
        "Minimum 200 words (target 220-320 words). "
        "Cover: the last week, the last month, the last year, and the 12-24 month forward outlook. "
        "Include concrete levels where relevant (e.g., commodity prices, inventories, policy moves, supply disruptions) "
        "and make reference to the broader macro environment including oil prices, inflation, interest rates, "
        "and the four quadrant global macro framework. "
        "Keep the paragraph decision-useful for scenario assumptions. "
        "Output plain text only. Do NOT include URLs, citation markers, footnotes, source lists, markdown, or bullet points."
    ).strip()


def _sanitize_supplementary_macro_summary_text(text: str) -> str:
    """Normalize xAI macro brief to plain-text paragraph with no citation/link artifacts."""
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned = raw
    # Drop markdown links -> keep anchor text only.
    cleaned = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", cleaned, flags=re.IGNORECASE)
    # Drop explicit citation markers like [[1]] / [1].
    cleaned = re.sub(r"\[\[\d+\]\]|\[\d+\]", "", cleaned)
    # Drop raw URLs.
    cleaned = re.sub(r"https?://\S+", "", cleaned, flags=re.IGNORECASE)
    # Single paragraph.
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


async def _fetch_xai_supplementary_macro_summary(
    *,
    sector_label: str,
    user_query: str,
) -> Dict[str, Any]:
    """Generate a single supplementary sector-macro paragraph via xAI."""
    if not XAI_API_KEY:
        return {
            "attempted": False,
            "success": False,
            "error": "xai_api_key_missing",
            "summary": "",
            "prompt": "",
            "http_status": 0,
            "request_count": 0,
            "tool_calls_count": 0,
            "finish_reason": "",
        }

    prompt = _build_supplementary_macro_summary_prompt(sector_label=sector_label)
    # Per requested behavior, send the exact sector prompt only.
    input_text = prompt
    tools: List[Dict[str, Any]] = [
        {"type": "web_search"},
        {"type": "x_search"},
    ]

    request_count = 0
    tool_calls_count = 0
    http_status = 0
    finish_reason = ""
    final_content = ""
    timeout_seconds = max(20.0, float(STAGE1_SUPPLEMENTARY_XAI_TIMEOUT_SECONDS))
    max_iterations = max(1, int(STAGE1_SUPPLEMENTARY_XAI_MAX_TOOL_ITERATIONS))
    max_tokens = max(128, int(STAGE1_SUPPLEMENTARY_XAI_MAX_TOKENS))
    temperature = max(0.0, min(1.5, float(STAGE1_SUPPLEMENTARY_XAI_TEMPERATURE)))
    endpoint = str(XAI_API_URL or "https://api.x.ai/v1/responses").strip()

    def _extract_responses_output_text(data: Dict[str, Any]) -> str:
        direct = data.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        output = data.get("output")
        texts: List[str] = []
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict):
                    continue
                item_text = item.get("text")
                if isinstance(item_text, str) and item_text.strip():
                    texts.append(item_text.strip())
                content = item.get("content")
                if isinstance(content, list):
                    for part in content:
                        if not isinstance(part, dict):
                            continue
                        txt = part.get("text")
                        if isinstance(txt, str) and txt.strip():
                            texts.append(txt.strip())
        if not texts:
            return ""
        return texts[-1]

    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            for _ in range(max_iterations):
                payload = {
                    "model": str(STAGE1_SUPPLEMENTARY_XAI_MODEL or "grok-4-1-fast-reasoning").strip(),
                    "input": input_text,
                    "tools": tools,
                    "max_output_tokens": max_tokens,
                    "temperature": temperature,
                }
                request_count += 1
                response = await client.post(
                    endpoint,
                    headers={
                        "Authorization": f"Bearer {XAI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                http_status = int(response.status_code)
                if response.status_code >= 400:
                    err_text = (response.text or "").strip()
                    return {
                        "attempted": True,
                        "success": False,
                        "error": f"xai_http_{response.status_code}:{err_text[:800]}",
                        "summary": "",
                        "prompt": prompt,
                        "http_status": http_status,
                        "request_count": request_count,
                        "tool_calls_count": tool_calls_count,
                        "finish_reason": finish_reason,
                    }

                data = response.json() if response.content else {}
                finish_reason = str(
                    (data.get("status") if isinstance(data, dict) else "") or ""
                ).strip().lower()
                output = (data.get("output") if isinstance(data, dict) else None) or []
                if isinstance(output, list):
                    tool_calls_count += len(
                        [
                            item
                            for item in output
                            if isinstance(item, dict)
                            and str(item.get("type", "")).strip().lower().endswith("_call")
                        ]
                    )

                content = _extract_responses_output_text(data if isinstance(data, dict) else {})
                if content:
                    final_content = content
                    break
    except Exception as exc:
        return {
            "attempted": True,
            "success": False,
            "error": str(exc).strip() or "xai_request_failed",
            "summary": "",
            "prompt": prompt,
            "http_status": http_status,
            "request_count": request_count,
            "tool_calls_count": tool_calls_count,
            "finish_reason": finish_reason,
        }

    normalized = _sanitize_supplementary_macro_summary_text(str(final_content or ""))
    return {
        "attempted": True,
        "success": bool(normalized),
        "error": "" if normalized else "xai_empty_response",
        "summary": normalized,
        "prompt": prompt,
        "http_status": http_status,
        "request_count": request_count,
        "tool_calls_count": tool_calls_count,
        "finish_reason": finish_reason,
    }


async def _collect_stage1_supplementary_macro_news(
    *,
    model: str,
    user_query: str,
    run: Dict[str, Any],
    template_id: str,
    existing_source_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build one xAI-generated supplementary macro paragraph without mutating core rows.

    This lane is additive only. It injects summary text (not extra source rows).
    """
    if not PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_ENABLED:
        return {
            "enabled": False,
            "used": False,
            "commodity_profile": "",
            "sector_label": "",
            "summary_paragraph": "",
            "sources": [],
            "reason": "supplementary_news_disabled",
        }

    max_sources = max(0, int(PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_SOURCES))
    if max_sources <= 0:
        return {
            "enabled": True,
            "used": False,
            "commodity_profile": "",
            "sector_label": "",
            "summary_paragraph": "",
            "sources": [],
            "reason": "max_sources_zero",
        }

    commodity_profile = _resolve_template_commodity_profile(template_id)
    lane_cfg = _SUPPLEMENTARY_MACRO_PROFILE_CONFIG.get(commodity_profile, {})
    sector_label = ""
    query_focus = ""
    try:
        from .template_loader import get_template_loader

        loader = get_template_loader()
        behavior = loader.get_template_behavior(str(template_id or "").strip()) or {}
        sector_label = str(behavior.get("supplementary_sector_label", "")).strip()
        query_focus = str(behavior.get("supplementary_query_focus", "")).strip()
    except Exception:
        sector_label = ""
        query_focus = ""
    if not sector_label:
        sector_label = str(lane_cfg.get("sector_label", "")).strip()
    if not query_focus:
        query_focus = str(lane_cfg.get("query_focus", "")).strip()
    if not sector_label:
        return {
            "enabled": True,
            "used": False,
            "commodity_profile": commodity_profile,
            "sector_label": sector_label,
            "summary_paragraph": "",
            "sources": [],
            "reason": "no_sector_label",
        }

    summary_result = await _fetch_xai_supplementary_macro_summary(
        sector_label=sector_label,
        user_query=user_query,
    )
    summary_paragraph = str(summary_result.get("summary", "")).strip()
    retrieval_error = str(summary_result.get("error", "")).strip()
    retrieval_attempted = bool(summary_result.get("attempted", False))
    retrieval_result_count = int(1 if summary_paragraph else 0)
    reason = "ok" if summary_paragraph else "xai_summary_empty"
    if retrieval_error and not summary_paragraph:
        reason = f"xai_error:{retrieval_error}"

    return {
        "enabled": True,
        "used": bool(summary_paragraph),
        "commodity_profile": commodity_profile,
        "sector_label": sector_label,
        "query_focus": query_focus,
        "summary_paragraph": summary_paragraph,
        "summary_prompt": str(summary_result.get("prompt", "")),
        "summary_model": str(STAGE1_SUPPLEMENTARY_XAI_MODEL or "grok-4-1-fast-reasoning"),
        "summary_provider": "xai",
        "summary_http_status": int(summary_result.get("http_status", 0) or 0),
        "summary_request_count": int(summary_result.get("request_count", 0) or 0),
        "summary_tool_calls_count": int(summary_result.get("tool_calls_count", 0) or 0),
        "summary_finish_reason": str(summary_result.get("finish_reason", "")),
        "sources": [],
        "count": int(retrieval_result_count),
        "reason": reason,
        "retrieval_attempted": retrieval_attempted,
        "retrieval_query": str(summary_result.get("prompt", "")),
        "retrieval_result_count": int(retrieval_result_count),
        "retrieval_error": retrieval_error,
        "max_sources": int(max_sources),
        "max_recency_days": int(max(7, int(PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_RECENCY_DAYS))),
    }


def _dedupe_model_ids(models: List[str]) -> List[str]:
    """Preserve-order dedupe for model-id lists."""
    deduped: List[str] = []
    for item in models or []:
        cleaned = str(item or "").strip()
        if cleaned and cleaned not in deduped:
            deduped.append(cleaned)
    return deduped


_PERPLEXITY_MODEL_ID_ALIASES: Dict[str, str] = {
    # Perplexity model IDs use hyphenated semantic versions.
    "anthropic/claude-sonnet-4.5": "anthropic/claude-sonnet-4-5",
    "anthropic/claude-opus-4.5": "anthropic/claude-opus-4-5",
}


def _normalize_perplexity_model_id(model: str) -> str:
    """Normalize known Perplexity model-id aliases to canonical IDs."""
    raw = str(model or "").strip()
    if not raw:
        return ""
    normalized = _PERPLEXITY_MODEL_ID_ALIASES.get(raw, raw)
    if normalized != raw:
        _progress_log(f"Perplexity model alias normalized: {raw} -> {normalized}")
    return normalized


def _is_perplexity_model_unsupported_error(status_code: int, body: str) -> bool:
    text = str(body or "").lower()
    if status_code != 400:
        return False
    return (
        "not supported" in text
        or "unsupported" in text
        or "model" in text and "validation failed" in text
    )


async def _probe_perplexity_model_support(
    *,
    model: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    """Probe whether Perplexity accepts a model ID with a minimal request."""
    raw_model = str(model or "").strip()
    resolved_model = _normalize_perplexity_model_id(raw_model)
    if not PERPLEXITY_API_KEY:
        return {
            "requested_model": raw_model,
            "resolved_model": resolved_model,
            "supported": False,
            "status_code": 0,
            "reason": "missing_api_key",
            "error_type": "config",
        }

    payload: Dict[str, Any] = {
        "model": resolved_model,
        "input": "Reply with exactly OK.",
        "max_output_tokens": 24,
    }
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=max(5.0, float(timeout_seconds))) as client:
            response = await client.post(
                PERPLEXITY_API_URL,
                headers=headers,
                json=payload,
            )
        status_code = int(response.status_code)
        body = (response.text or "")[:400]
        if status_code == 200:
            return {
                "requested_model": raw_model,
                "resolved_model": resolved_model,
                "supported": True,
                "status_code": status_code,
                "reason": "ok",
                "error_type": "",
            }
        if _is_perplexity_model_unsupported_error(status_code, body):
            return {
                "requested_model": raw_model,
                "resolved_model": resolved_model,
                "supported": False,
                "status_code": status_code,
                "reason": "unsupported_model",
                "error_type": "unsupported",
                "body_preview": body,
            }
        return {
            "requested_model": raw_model,
            "resolved_model": resolved_model,
            "supported": False,
            "status_code": status_code,
            "reason": "probe_request_failed",
            "error_type": "transient",
            "body_preview": body,
        }
    except Exception as exc:
        return {
            "requested_model": raw_model,
            "resolved_model": resolved_model,
            "supported": False,
            "status_code": 0,
            "reason": f"{type(exc).__name__}: {exc}",
            "error_type": "transient",
        }


def _extract_perplexity_finish_reason(data: Dict[str, Any]) -> str:
    """Best-effort finish/status extraction from Responses API payload."""
    output = data.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            finish = item.get("finish_reason")
            if finish:
                return str(finish)
            status = item.get("status")
            if status:
                return str(status)
    for key in ("finish_reason", "status"):
        value = data.get(key)
        if value:
            return str(value)
    return ""


def _supports_perplexity_reasoning_payload(model: str) -> bool:
    """Reasoning payload is enabled for all routed models."""
    _ = model
    return True


async def _query_model_via_perplexity(
    *,
    model: str,
    prompt: str,
    timeout: float,
    max_tokens: Optional[int],
    reasoning_effort: str,
) -> Optional[Dict[str, Any]]:
    """
    Query one model via Perplexity Responses API for Stage 1 second-pass analysis.

    This call intentionally disables web-search tools and uses only injected prompt context.
    """
    if not PERPLEXITY_API_KEY:
        _progress_log(f"Perplexity second-pass skipped model={model}: missing_api_key")
        return None

    resolved_model = _normalize_perplexity_model_id(model)
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "model": resolved_model or model,
        "input": prompt,
    }
    stream_requested = bool(PERPLEXITY_STREAM_ENABLED)
    if stream_requested:
        payload["stream"] = True
    if isinstance(max_tokens, int) and max_tokens > 0:
        payload["max_output_tokens"] = int(max_tokens)
    effort = normalize_reasoning_effort(reasoning_effort)
    reasoning_payload_sent = False
    reasoning_effort_effective = effort
    if _supports_perplexity_reasoning_payload(resolved_model or model):
        payload["reasoning"] = build_reasoning_payload(
            resolved_model or model,
            effort,
            provider="perplexity",
        )
        reasoning_payload_sent = True

    def _is_invalid_request_400(exc: httpx.HTTPStatusError) -> bool:
        if exc.response is None or exc.response.status_code != 400:
            return False
        body = (exc.response.text or "").strip().lower()
        return "invalid request" in body

    async def _post_once(
        client: httpx.AsyncClient,
        req_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        response = await client.post(
            PERPLEXITY_API_URL,
            headers=headers,
            json=req_payload,
        )
        response.raise_for_status()
        return response.json()

    async def _post_stream(
        client: httpx.AsyncClient,
        req_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        text_deltas: List[str] = []
        final_response: Dict[str, Any] = {}
        async with client.stream(
            "POST",
            PERPLEXITY_API_URL,
            headers=headers,
            json=req_payload,
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line:
                    continue
                chunk = line.strip()
                if not chunk or chunk.startswith(":"):
                    continue
                if chunk.startswith("event:"):
                    continue
                if chunk.startswith("data:"):
                    chunk = chunk[5:].strip()
                if not chunk or chunk == "[DONE]":
                    continue
                try:
                    event = json.loads(chunk)
                except Exception:
                    continue
                event_type = str(event.get("type", "")).strip().lower()
                if event_type in {"response.output_text.delta", "output_text.delta"}:
                    delta = event.get("delta")
                    if isinstance(delta, str) and delta:
                        text_deltas.append(delta)
                if event_type in {"response.completed", "completed"}:
                    response_obj = event.get("response")
                    if isinstance(response_obj, dict):
                        final_response = response_obj
                elif event_type in {"response", "output"} and isinstance(event, dict):
                    final_response = event

        merged_text = "".join(text_deltas).strip()
        if final_response:
            if merged_text:
                final_response["output_text"] = merged_text
            return final_response
        if merged_text:
            return {
                "output_text": merged_text,
                "output": [{"type": "output_text", "text": merged_text}],
            }
        raise RuntimeError("perplexity_stream_empty_payload")

    async def _perform_request(
        client: httpx.AsyncClient,
        req_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        use_stream = bool(req_payload.get("stream"))
        if use_stream:
            try:
                return await _post_stream(client, req_payload)
            except RuntimeError as exc:
                if "perplexity_stream_empty_payload" not in str(exc):
                    raise
                retry_payload = dict(req_payload)
                retry_payload.pop("stream", None)
                _progress_log(
                    f"Perplexity second-pass stream fallback model={model}: using non-stream"
                )
                return await _post_once(client, retry_payload)
        return await _post_once(client, req_payload)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                data = await _perform_request(client, payload)
            except httpx.HTTPStatusError as exc:
                # Some routed models reject specific reasoning payload shapes.
                # Retry once with a conservative low-effort payload.
                if _is_invalid_request_400(exc) and "reasoning" in payload:
                    retry_payload = dict(payload)
                    retry_payload["reasoning"] = build_reasoning_payload(
                        resolved_model or model,
                        "low",
                        provider="perplexity",
                    )
                    reasoning_payload_sent = True
                    reasoning_effort_effective = "low"
                    _progress_log(
                        f"Perplexity second-pass retry with low reasoning model={model} "
                        f"after 400 invalid_request"
                    )
                    data = await _perform_request(client, retry_payload)
                else:
                    raise
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        body = (exc.response.text or "")[:500] if exc.response is not None else ""
        _progress_log(
            f"Perplexity second-pass HTTP error model={model} status={status} body={body}"
        )
        return None
    except Exception as exc:
        _progress_log(
            f"Perplexity second-pass error model={model}: {type(exc).__name__}: {exc}"
        )
        return None

    content = ""
    try:
        from .research.providers.perplexity import PerplexityResearchProvider
        parser = PerplexityResearchProvider()
        content = parser._extract_content(data).strip()
    except Exception:
        content = ""

    if not content:
        output_text = data.get("output_text")
        if isinstance(output_text, str):
            content = output_text.strip()
        elif isinstance(output_text, list):
            content = "\n".join([str(item) for item in output_text if isinstance(item, str)]).strip()

    return {
        "content": content,
        "finish_reason": _extract_perplexity_finish_reason(data),
        "usage": data.get("usage"),
        "id": data.get("id"),
        "provider": data.get("provider") or "perplexity",
        "reasoning_payload_sent": bool(reasoning_payload_sent),
        "reasoning_effort_effective": str(reasoning_effort_effective or ""),
    }


def _select_shared_retrieval_model(models: List[str]) -> str:
    """
    Pick the model used for shared retrieval/decode.

    Priority:
    1) explicit env override when present in configured model list
    2) first non-openai model (typically more stable for retrieval latency)
    3) first configured model
    """
    if not models:
        return ""

    preferred = str(PERPLEXITY_STAGE1_SHARED_RETRIEVAL_MODEL or "").strip()
    if preferred:
        for model in models:
            if model == preferred:
                return model
        preferred_lower = preferred.lower()
        for model in models:
            if model.lower() == preferred_lower:
                return model

    for model in models:
        if not str(model).strip().lower().startswith("openai/"):
            return model

    return models[0]


def _is_openrouter_compatible_model(model: str) -> bool:
    """
    Return True when model id is expected to route through OpenRouter.

    Perplexity-native families like Sonar should not be sent to OpenRouter
    for Stage 1 second-pass or Stage 2 judging.
    """
    key = str(model or "").strip().lower()
    if not key:
        return False
    if "sonar" in key:
        return False
    if key.startswith("pplx/") or key.startswith("perplexity/"):
        return False
    return True


def _is_sonar_model(model: str) -> bool:
    """Return True when model id appears to be a Perplexity Sonar family model."""
    key = str(model or "").strip().lower()
    if not key:
        return False
    return "sonar" in key or key.startswith("pplx/") or key.startswith("perplexity/")


def _evaluate_stage1_sonar_telemetry(
    *,
    model: str,
    provider_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Validate Sonar multistep telemetry per run.

    We treat this as a run-quality gate for Sonar family models when enabled.
    """
    if not _is_sonar_model(model):
        return {
            "required": False,
            "passed": True,
            "reason": "not_sonar_model",
            "is_sonar_model": False,
        }

    stream_requested = bool(provider_meta.get("stream_requested", False))
    stream_used = bool(provider_meta.get("stream_used", False))
    stream_event_count = int(provider_meta.get("stream_event_count", 0) or 0)
    stream_completed = bool(provider_meta.get("stream_completed_event_seen", False))
    search_mode = str(provider_meta.get("search_mode", "") or "").strip().lower()
    search_type = str(provider_meta.get("search_type", "") or "").strip().lower()

    required = bool(PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED)
    if not required:
        return {
            "required": False,
            "passed": True,
            "reason": "sonar_multistep_not_required",
            "is_sonar_model": True,
            "stream_requested": stream_requested,
            "stream_used": stream_used,
            "stream_event_count": stream_event_count,
            "stream_completed_event_seen": stream_completed,
            "search_mode": search_mode,
            "search_type": search_type,
        }

    stream_ok = bool(stream_used and stream_event_count >= 3 and stream_completed)
    pro_mode_ok = (search_mode != "pro") or (search_type == "pro")
    passed = bool(stream_ok and pro_mode_ok)

    reasons: List[str] = []
    if not stream_requested:
        reasons.append("stream_not_requested")
    if not stream_used:
        reasons.append("stream_not_used")
    if stream_event_count < 3:
        reasons.append("insufficient_stream_events")
    if not stream_completed:
        reasons.append("stream_completion_event_missing")
    if search_mode == "pro" and search_type != "pro":
        reasons.append("search_type_not_pro_for_pro_mode")

    return {
        "required": True,
        "passed": passed,
        "reason": ",".join(reasons) if reasons else "ok",
        "is_sonar_model": True,
        "stream_requested": stream_requested,
        "stream_used": stream_used,
        "stream_event_count": stream_event_count,
        "stream_completed_event_seen": stream_completed,
        "search_mode": search_mode,
        "search_type": search_type,
    }


_FACT_PACK_SECTIONS = [
    "market_data",
    "project_economics_npv_inputs",
    "resource_and_reserve",
    "funding_and_balance_sheet",
    "development_timeline_and_milestones",
    "headwinds_and_risks",
    "tailwinds_and_catalysts",
    "management_and_governance",
    "valuation_and_peer_signals",
    "other_material_facts",
]

_FACT_PACK_KEYWORDS = {
    "market_data": [
        "market cap",
        "shares outstanding",
        "share price",
        "enterprise value",
        "ev ",
        "cash",
        "debt",
    ],
    "project_economics_npv_inputs": [
        "npv",
        "irr",
        "aisc",
        "capex",
        "opex",
        "mine life",
        "recovery",
        "royalty",
        "tax",
        "production",
        "oz",
    ],
    "resource_and_reserve": [
        "resource",
        "reserve",
        "jorc",
        "grade",
        "g/t",
        "moz",
        "ore",
    ],
    "funding_and_balance_sheet": [
        "facility",
        "loan",
        "placement",
        "capital raising",
        "financing",
        "liquidity",
        "runway",
        "cash",
    ],
    "development_timeline_and_milestones": [
        "first gold",
        "commission",
        "ramp-up",
        "production",
        "milestone",
        "q1",
        "q2",
        "q3",
        "q4",
        "202",
        "dfs",
        "pfs",
        "feasibility",
    ],
    "headwinds_and_risks": [
        "risk",
        "headwind",
        "delay",
        "dilution",
        "permit",
        "regulatory",
        "inflation",
        "power",
        "labor",
        "execution",
        "geopolitical",
    ],
    "tailwinds_and_catalysts": [
        "tailwind",
        "catalyst",
        "upside",
        "expansion",
        "drilling",
        "resource growth",
        "gold price",
        "strategic",
        "offtake",
    ],
    "management_and_governance": [
        "management",
        "board",
        "director",
        "ceo",
        "governance",
        "track record",
        "insider",
    ],
    "valuation_and_peer_signals": [
        "valuation",
        "ev/oz",
        "peer",
        "undervalued",
        "multiple",
        "discount",
        "premium",
    ],
}

_FACT_DIGEST_V2_SECTIONS = [
    "timelines_deadlines",
    "financing_deals",
    "project_economics",
    "market_share_structure",
    "management_governance",
    "operational_objectives",
    "risks_constraints",
    "catalysts_tailwinds",
    "other_material_facts",
]

_FACT_DIGEST_V2_KEYWORDS = {
    "timelines_deadlines": [
        "first gold",
        "gold pour",
        "milestone",
        "q1",
        "q2",
        "q3",
        "q4",
        "march",
        "april",
        "deadline",
        "target",
        "on track",
    ],
    "financing_deals": [
        "facility",
        "loan",
        "debt",
        "capital raise",
        "placement",
        "financing",
        "offtake",
        "agreement",
        "deal",
        "funded",
        "cash",
    ],
    "project_economics": [
        "npv",
        "irr",
        "aisc",
        "capex",
        "opex",
        "free cash flow",
        "payback",
        "mine life",
        "production",
        "gold price",
        "resource",
        "reserve",
        "grade",
    ],
    "market_share_structure": [
        "market cap",
        "enterprise value",
        "shares",
        "price",
        "valuation",
        "ev/oz",
        "multiple",
    ],
    "management_governance": [
        "management",
        "board",
        "director",
        "ceo",
        "cfo",
        "executive",
        "insider ownership",
        "governance",
        "track record",
        "appointment",
        "resignation",
    ],
    "operational_objectives": [
        "objective",
        "guidance",
        "commissioning",
        "ramp-up",
        "development",
        "stockpiling",
        "processing",
        "production",
    ],
    "risks_constraints": [
        "risk",
        "headwind",
        "delay",
        "dilution",
        "permit",
        "regulatory",
        "inflation",
        "power",
        "labor",
        "geopolitical",
        "uncertain",
    ],
    "catalysts_tailwinds": [
        "catalyst",
        "tailwind",
        "upside",
        "drilling",
        "resource growth",
        "expansion",
        "strategic",
        "improved",
    ],
}

_FACT_DIGEST_V2_NARRATIVE_ORDER = [
    "timelines_deadlines",
    "financing_deals",
    "project_economics",
    "market_share_structure",
    "management_governance",
    "operational_objectives",
    "risks_constraints",
    "catalysts_tailwinds",
]

_STAGE1_DEFAULT_TIMELINE_TERMS = [
    "first gold",
    "gold pour",
    "first ore",
    "stockpile",
    "processing",
    "on track",
    "targeting",
    "milestone",
    "timeline",
    "guidance",
    "launch",
    "approval",
    "commissioning",
    "ramp-up",
    "production",
    "q1",
    "q2",
    "q3",
    "q4",
    "march",
    "april",
    "may",
    "june",
]

_STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS = [
    "milestone",
    "timeline",
    "commissioning",
    "production",
    "ramp-up",
    "target",
]
_STAGE1_SECOND_PASS_MIN_RESPONSE_CHARS = 300


def _normalize_terms_list(raw_terms: Any) -> List[str]:
    out: List[str] = []
    for item in (raw_terms or []):
        value = str(item or "").strip().lower()
        if value and value not in out:
            out.append(value)
    return out


def _markers_for_field_name(field_name: str) -> List[str]:
    key = str(field_name or "").strip().lower()
    if not key:
        return []
    variants = [
        key,
        key.replace("_", " "),
        key.replace("_", "-"),
    ]
    if key.endswith("_pct"):
        base = key[: -len("_pct")]
        variants.extend(
            [
                base,
                f"{base} %",
                f"{base.replace('_', ' ')} %",
            ]
        )
    if key.endswith("_score"):
        base = key[: -len("_score")]
        variants.extend(
            [
                f"{base} score",
                f"{base.replace('_', ' ')} score",
            ]
        )
    deduped: List[str] = []
    for item in variants:
        cleaned = re.sub(r"\s+", " ", item).strip()
        if cleaned and cleaned not in deduped:
            deduped.append(cleaned)
    return deduped


def _default_stage1_verification_profile() -> Dict[str, Any]:
    return {
        "template_id": "",
        "fact_digest_keywords": copy.deepcopy(_FACT_DIGEST_V2_KEYWORDS),
        "fact_digest_narrative_order": list(_FACT_DIGEST_V2_NARRATIVE_ORDER),
        "timeline_terms": list(_STAGE1_DEFAULT_TIMELINE_TERMS),
        "timeline_focus_terms": list(_STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS),
        "timeline_conflict_field": "timeline_window",
        "timeline_conflict_resolution_rule": "prefer newest dated primary-source timeline evidence",
        "timeline_conflict_max_shift_quarters": 3,
        "compliance_section_markers": list(_STAGE1_RUBRIC_SECTION_MARKERS),
        "compliance_critical_sections": set(_STAGE1_RUBRIC_CRITICAL_SECTIONS),
        "cashflow_schema_mode": "auto",
        "cashflow_schema_min_reporting_periods": 3,
        "cashflow_schema_require_operating_cashflow": True,
        "cashflow_schema_detection_max_sources": int(STAGE1_CASHFLOW_DETECTION_MAX_SOURCES),
    }


def _build_stage1_verification_profile(template_id: Optional[str]) -> Dict[str, Any]:
    profile = _default_stage1_verification_profile()
    if not template_id:
        return profile

    from .template_loader import get_template_loader

    loader = get_template_loader()
    template_data = loader.get_template(template_id) or {}
    verification = loader.get_verification_schema(template_id)
    profile["template_id"] = str(template_id)
    template_behavior = loader.get_template_behavior(template_id) or {}

    fact_digest_cfg = verification.get("fact_digest", {}) if isinstance(verification, dict) else {}
    sections_cfg = fact_digest_cfg.get("sections", {})
    normalized_sections: Dict[str, List[str]] = {}
    if isinstance(sections_cfg, dict):
        for section_name, section_payload in sections_cfg.items():
            sid = str(section_name or "").strip().lower()
            if not sid:
                continue
            keywords: List[str] = []
            if isinstance(section_payload, dict):
                keywords = _normalize_terms_list(section_payload.get("keywords", []))
            elif isinstance(section_payload, list):
                keywords = _normalize_terms_list(section_payload)
            if keywords:
                normalized_sections[sid] = keywords
    if normalized_sections:
        profile["fact_digest_keywords"] = normalized_sections

    narrative_order = [
        str(item or "").strip().lower()
        for item in (fact_digest_cfg.get("narrative_order", []) or [])
        if str(item or "").strip()
    ]
    if narrative_order:
        profile["fact_digest_narrative_order"] = narrative_order

    timeline_terms = _normalize_terms_list(fact_digest_cfg.get("timeline_terms", []))
    if timeline_terms:
        profile["timeline_terms"] = timeline_terms
    timeline_focus_terms = _normalize_terms_list(fact_digest_cfg.get("timeline_focus_terms", []))
    if timeline_focus_terms:
        profile["timeline_focus_terms"] = timeline_focus_terms

    conflict_cfg = fact_digest_cfg.get("conflict", {})
    if isinstance(conflict_cfg, dict):
        field_name = str(conflict_cfg.get("field", "")).strip()
        if field_name:
            profile["timeline_conflict_field"] = field_name
        max_shift = conflict_cfg.get("max_shift_quarters")
        if isinstance(max_shift, (int, float)):
            profile["timeline_conflict_max_shift_quarters"] = max(1, int(max_shift))
        resolution = str(conflict_cfg.get("resolution_rule", "")).strip()
        if resolution:
            profile["timeline_conflict_resolution_rule"] = resolution
        conflict_terms = _normalize_terms_list(conflict_cfg.get("terms", []))
        if conflict_terms:
            profile["timeline_focus_terms"] = conflict_terms

    compliance_cfg = verification.get("compliance", {}) if isinstance(verification, dict) else {}
    markers_cfg = compliance_cfg.get("section_markers", {})
    normalized_markers: List[Tuple[str, List[str]]] = []
    normalized_critical: set[str] = set()

    if isinstance(markers_cfg, dict):
        for section_id, marker_payload in markers_cfg.items():
            sid = str(section_id or "").strip().lower()
            if not sid:
                continue
            markers: List[str] = []
            critical = False
            if isinstance(marker_payload, dict):
                markers = _normalize_terms_list(marker_payload.get("markers", []))
                critical = bool(marker_payload.get("critical", False))
            elif isinstance(marker_payload, list):
                markers = _normalize_terms_list(marker_payload)
            if not markers:
                markers = _markers_for_field_name(sid)
            if not markers:
                continue
            normalized_markers.append((sid, markers))
            if critical:
                normalized_critical.add(sid)

    required_sections = _normalize_terms_list(compliance_cfg.get("required_sections", []))
    normalized_critical.update(required_sections)
    normalized_critical.update(
        _normalize_terms_list(compliance_cfg.get("critical_sections", []))
    )

    if not normalized_markers:
        required_fields = (
            ((template_data.get("output_schema") or {}).get("required_fields") or [])
            if isinstance(template_data, dict)
            else []
        )
        for field in required_fields:
            sid = str(field or "").strip().lower()
            if not sid:
                continue
            markers = _markers_for_field_name(sid)
            if not markers:
                continue
            normalized_markers.append((sid, markers))
        # Keep the scoring/timeline-related fields as critical by default.
        for sid in ("quality_score", "value_score", "price_targets", "development_timeline"):
            if any(item[0] == sid for item in normalized_markers):
                normalized_critical.add(sid)

    if normalized_markers:
        profile["compliance_section_markers"] = normalized_markers
    if normalized_critical:
        profile["compliance_critical_sections"] = normalized_critical

    cashflow_cfg = (
        template_behavior.get("cashflow_schema", {})
        if isinstance(template_behavior, dict)
        else {}
    )
    if isinstance(cashflow_cfg, dict):
        mode = str(cashflow_cfg.get("mode", "")).strip().lower()
        if mode in {"disabled", "auto", "required"}:
            profile["cashflow_schema_mode"] = mode
        min_periods = cashflow_cfg.get("min_reporting_periods")
        if isinstance(min_periods, (int, float)):
            profile["cashflow_schema_min_reporting_periods"] = max(1, int(min_periods))
        require_ocf = cashflow_cfg.get("require_operating_cashflow")
        if isinstance(require_ocf, bool):
            profile["cashflow_schema_require_operating_cashflow"] = bool(require_ocf)
        detection_max_sources = cashflow_cfg.get("detection_max_sources")
        if isinstance(detection_max_sources, (int, float)):
            profile["cashflow_schema_detection_max_sources"] = max(
                6,
                int(detection_max_sources),
            )

    return profile


def _keywords_for_gap_section(section_id: str, verification_profile: Dict[str, Any]) -> List[str]:
    """Resolve keyword hints for a compliance section id."""
    sid = str(section_id or "").strip().lower()
    digest_keywords = verification_profile.get("fact_digest_keywords", {}) or {}
    if isinstance(digest_keywords, dict):
        direct = _normalize_terms_list(digest_keywords.get(sid, []))
        if direct:
            return direct

    fallback: Dict[str, List[str]] = {
        "quality_score": ["quality score", "jurisdiction", "management", "funding", "esg"],
        "value_score": ["value score", "npv", "market cap", "ev/resource", "aisc"],
        "price_targets": ["12-month target", "24-month target", "upside scenario"],
        "development_timeline": ["timeline", "milestone", "first gold", "production target"],
        "certainty": ["certainty", "probability", "risk to milestones"],
        "headwinds_tailwinds": ["headwind", "tailwind", "sensitivity", "threshold"],
        "npv_assessment": ["npv", "irr", "capex", "aisc", "mine life"],
        "management_competition_assessment": [
            "management",
            "board",
            "executive",
            "ceo",
            "cfo",
            "insider ownership",
            "governance",
            "leadership changes",
            "track record",
            "competition",
            "peer positioning",
        ],
    }
    return fallback.get(sid, _markers_for_field_name(sid))


def _build_stage1_research_planner(
    *,
    user_query: str,
    research_brief: str,
    ticker: Optional[str],
    verification_profile: Dict[str, Any],
    max_waves: int,
    gap_query_limit: int,
) -> Dict[str, Any]:
    """Create deterministic planner payload for multi-wave retrieval."""
    section_markers = verification_profile.get("compliance_section_markers", []) or []
    critical_sections = set(verification_profile.get("compliance_critical_sections", set()) or set())
    objectives: List[Dict[str, Any]] = []
    ordered_sections: List[str] = []

    for section_id, markers in section_markers:
        sid = str(section_id or "").strip().lower()
        if not sid:
            continue
        if sid in ordered_sections:
            continue
        ordered_sections.append(sid)
        objectives.append(
            {
                "section": sid,
                "critical": sid in critical_sections,
                "markers": list(markers or [])[:6],
                "keywords": _keywords_for_gap_section(sid, verification_profile)[:8],
            }
        )

    if not objectives:
        for sid, keywords in (verification_profile.get("fact_digest_keywords", {}) or {}).items():
            sid_norm = str(sid or "").strip().lower()
            if not sid_norm:
                continue
            objectives.append(
                {
                    "section": sid_norm,
                    "critical": False,
                    "markers": _markers_for_field_name(sid_norm)[:4],
                    "keywords": _normalize_terms_list(keywords)[:8],
                }
            )
            ordered_sections.append(sid_norm)

    wave_plan: List[Dict[str, Any]] = [
        {
            "wave": 1,
            "type": "broad_primary",
            "focus_sections": ordered_sections[: max(2, min(5, len(ordered_sections)))],
            "query_intent": "broad primary-source coverage",
        }
    ]

    safe_gap_limit = max(1, int(gap_query_limit))
    safe_max_waves = max(1, int(max_waves))
    for wave in range(2, safe_max_waves + 1):
        wave_plan.append(
            {
                "wave": wave,
                "type": "gap_fill",
                "focus_sections": [],
                "query_intent": f"target unresolved sections (top {safe_gap_limit})",
            }
        )

    return {
        "planner_version": "stage1_multi_wave_v1",
        "ticker": str(ticker or "").strip(),
        "query_preview": (user_query or "").strip()[:260],
        "brief_preview": (research_brief or "").strip()[:260],
        "max_waves": safe_max_waves,
        "gap_query_limit": safe_gap_limit,
        "objectives": objectives,
        "wave_plan": wave_plan,
    }


def _evaluate_stage1_section_coverage(
    run: Dict[str, Any],
    verification_profile: Dict[str, Any],
) -> Dict[str, Any]:
    """Estimate which rubric sections are currently evidenced by retrieved output."""
    summary_text = str(run.get("research_summary", ""))
    filtered_summary_lines: List[str] = []
    for line in summary_text.splitlines():
        clean_line = re.sub(r"\s+", " ", str(line or "")).strip()
        if not clean_line:
            continue
        if _is_low_signal_legal_boilerplate(clean_line):
            continue
        filtered_summary_lines.append(clean_line)
    text_parts: List[str] = ["\n".join(filtered_summary_lines)]
    for update in (run.get("latest_updates", []) or [])[:8]:
        text_parts.append(str(update.get("update", "")))
        text_parts.append(str(update.get("why_it_matters", "")))
    raw_results = list((run.get("results", []) or [])[:12])
    non_low_results = [row for row in raw_results if not _is_low_signal_notice_source_item(row)]
    coverage_results = non_low_results if non_low_results else raw_results[:4]

    for result in coverage_results[:10]:
        text_parts.append(str(result.get("title", "")))
        text_parts.append(str(result.get("content", ""))[:900])
    corpus = "\n".join(text_parts).lower()

    section_markers = verification_profile.get("compliance_section_markers", []) or []
    critical_sections = set(verification_profile.get("compliance_critical_sections", set()) or set())
    coverage: Dict[str, bool] = {}
    marker_hits: Dict[str, int] = {}
    for section_id, markers in section_markers:
        sid = str(section_id or "").strip().lower()
        if not sid:
            continue
        hit_count = sum(1 for marker in (markers or []) if str(marker).lower() in corpus)
        coverage[sid] = hit_count > 0
        marker_hits[sid] = int(hit_count)

    missing_sections = [sid for sid, covered in coverage.items() if not covered]
    missing_critical = [sid for sid in missing_sections if sid in critical_sections]
    return {
        "coverage": coverage,
        "marker_hits": marker_hits,
        "missing_sections": missing_sections,
        "missing_critical_sections": missing_critical,
        "covered_sections": [sid for sid, covered in coverage.items() if covered],
        "critical_sections_total": len(critical_sections),
        "critical_sections_covered": sum(
            1 for sid in critical_sections if coverage.get(sid, False)
        ),
    }


def _build_stage1_gap_query_block(
    *,
    missing_sections: List[str],
    verification_profile: Dict[str, Any],
    ticker: Optional[str],
    gap_query_limit: int,
) -> str:
    """Create targeted gap-fill query hints for follow-up retrieval waves."""
    if not missing_sections:
        return ""
    lines = ["GAP-FILL OBJECTIVES FOR THIS WAVE:"]
    safe_limit = max(1, int(gap_query_limit))
    ticker_prefix = str(ticker or "").strip()
    for section_id in missing_sections[:safe_limit]:
        keywords = _keywords_for_gap_section(section_id, verification_profile)
        focus = ", ".join(keywords[:5]) if keywords else section_id.replace("_", " ")
        if ticker_prefix:
            query_hint = f"{ticker_prefix} {focus}"
        else:
            query_hint = focus
        lines.append(f"- {section_id}: {query_hint}")
    lines.append("Use primary filings/official investor materials first; then fill with secondary sources if needed.")
    return "\n".join(lines)


def _count_new_primary_sources(run: Dict[str, Any], seen_primary_urls: set[str]) -> int:
    """Count newly discovered primary/high-authority sources in a wave run."""
    new_primary = 0
    for result in (run.get("results", []) or []):
        url = str(result.get("url", "")).strip()
        if not url:
            continue
        authority = _source_authority_rank(url)
        if authority < 2:
            continue
        if url in seen_primary_urls:
            continue
        seen_primary_urls.add(url)
        new_primary += 1
    return new_primary


def _merge_stage1_wave_runs(
    *,
    wave_runs: List[Dict[str, Any]],
    original_query: str,
    max_sources: int,
    planner: Dict[str, Any],
    wave_reports: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge multi-wave retrieval outputs into a single Stage 1 run payload."""
    if not wave_runs:
        return {
            "query": original_query,
            "results": [],
            "result_count": 0,
            "provider": "perplexity",
            "error": "No successful wave runs",
        }

    merged_by_url: Dict[str, Dict[str, Any]] = {}
    for run in wave_runs:
        for item in (run.get("results", []) or []):
            url = str(item.get("url", "")).strip()
            if not url:
                continue
            existing = merged_by_url.get(url)
            if existing is None:
                merged_by_url[url] = dict(item)
                continue
            if float(item.get("score", 0.0)) > float(existing.get("score", 0.0)):
                existing["score"] = float(item.get("score", 0.0))
            if len(str(item.get("content", ""))) > len(str(existing.get("content", ""))):
                existing["content"] = item.get("content", "")
            if not str(existing.get("published_at", "")).strip() and str(
                item.get("published_at", "")
            ).strip():
                existing["published_at"] = item.get("published_at")
            existing_title = str(existing.get("title", "")).strip()
            if existing_title.lower().startswith("asx announcement pdf") and str(
                item.get("title", "")
            ).strip():
                existing["title"] = item.get("title", "")

    merged_results = list(merged_by_url.values())
    merged_results.sort(
        key=lambda row: (
            float(row.get("score", 0.0)),
            str(row.get("published_at", "")),
            _source_authority_rank(str(row.get("url", ""))),
        ),
        reverse=True,
    )

    low_signal_total = sum(1 for row in merged_results if _is_low_signal_notice_source_item(row))
    limit = max(1, int(max_sources))

    filtered_results: List[Dict[str, Any]] = []
    low_signal_used = 0
    for row in merged_results:
        if len(filtered_results) >= limit:
            break
        is_low_signal = _is_low_signal_notice_source_item(row)
        if is_low_signal:
            continue
        filtered_results.append(row)

    merged_results = filtered_results[:limit]

    updates_by_key: Dict[str, Dict[str, Any]] = {}
    for run in wave_runs:
        for update in (run.get("latest_updates", []) or []):
            key = (
                str(update.get("date", "")).strip(),
                str(update.get("update", "")).strip(),
                str(update.get("source_url", "")).strip(),
            )
            if key in updates_by_key:
                continue
            updates_by_key[key] = dict(update)
    merged_updates = list(updates_by_key.values())[:8]

    summary_parts: List[str] = []
    for idx, run in enumerate(wave_runs, start=1):
        summary = str(run.get("research_summary", "")).strip()
        if not summary:
            continue
        first_line = summary.splitlines()[0].strip()
        if first_line:
            summary_parts.append(f"Wave {idx}: {first_line}")
    merged_summary = str(wave_runs[-1].get("research_summary", "")).strip()
    if summary_parts:
        merged_summary = (
            f"{merged_summary}\n\n### Retrieval Waves\n" + "\n".join(f"- {part}" for part in summary_parts)
        ).strip()

    decode_attempted = 0
    decode_decoded = 0
    decode_failed = 0
    decode_sources: List[Dict[str, Any]] = []
    for run in wave_runs:
        decode_meta = (run.get("provider_metadata", {}) or {}).get("source_decoding", {}) or {}
        decode_attempted += int(decode_meta.get("attempted", 0))
        decode_decoded += int(decode_meta.get("decoded", 0))
        decode_failed += int(decode_meta.get("failed", 0))
        for row in (decode_meta.get("sources", []) or [])[:12]:
            decode_sources.append(dict(row))

    merged = copy.deepcopy(wave_runs[-1])
    merged["query"] = original_query
    merged["results"] = merged_results
    merged["result_count"] = len(merged_results)
    merged["latest_updates"] = merged_updates
    merged["research_summary"] = merged_summary

    provider_meta = merged.setdefault("provider_metadata", {})
    if not isinstance(provider_meta, dict):
        provider_meta = {}
        merged["provider_metadata"] = provider_meta
    provider_meta["source_decoding"] = {
        "enabled": True,
        "attempted": decode_attempted,
        "decoded": decode_decoded,
        "failed": decode_failed,
        "sources": decode_sources[:60],
    }
    provider_meta["stage1_multi_wave"] = {
        "enabled": True,
        "planner": planner,
        "waves_requested": int(planner.get("max_waves", len(wave_runs))),
        "waves_completed": len(wave_runs),
        "low_signal_sources_total": int(low_signal_total),
        "low_signal_sources_kept": int(low_signal_used),
        "low_signal_sources_dropped": int(max(0, low_signal_total - low_signal_used)),
        "wave_reports": wave_reports,
    }
    return merged


def _normalize_fact_key(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _truncate_to_word_limit(text: str, max_words: int) -> str:
    words = (text or "").split()
    safe_limit = max(40, int(max_words))
    if len(words) <= safe_limit:
        return (text or "").strip()
    return " ".join(words[:safe_limit]).strip() + " ..."


def _classify_fact_digest_v2_section(
    sentence: str,
    section_keywords: Dict[str, List[str]],
) -> str:
    text = (sentence or "").lower()
    best_section = "other_material_facts"
    best_score = 0
    for section, keywords in (section_keywords or {}).items():
        score = sum(1 for token in keywords if token in text)
        if score > best_score:
            best_score = score
            best_section = section
    return best_section


def _extract_fact_digest_number_tokens(sentence: str, max_tokens: int = 4) -> List[str]:
    tokens: List[str] = []
    for match in re.findall(
        r"(?:A\$|AU\$|US\$|\$)?\s*\d[\d,]*(?:\.\d+)?\s*(?:%|moz|koz|oz|g/t|Mt|M|B|bn|million|billion)?",
        sentence or "",
        flags=re.IGNORECASE,
    ):
        token = re.sub(r"\s+", " ", match).strip()
        if token and token not in tokens:
            tokens.append(token)
        if len(tokens) >= max(1, int(max_tokens)):
            break
    return tokens


def _score_fact_digest_sentence(sentence: str, published_at: str, authority_rank: int) -> int:
    low = (sentence or "").lower()
    score = max(0, int(authority_rank)) * 3
    if re.search(r"\d", low):
        score += 2
    if any(token in low for token in ("first gold", "gold pour", "launch", "approval", "first production")):
        score += 6
    if any(token in low for token in ("funded", "facility", "placement", "capital raise", "loan")):
        score += 4
    if any(token in low for token in ("npv", "irr", "aisc", "capex", "free cash flow", "payback")):
        score += 4
    if published_at.startswith("2026-"):
        score += 3
    elif published_at.startswith("2025-"):
        score += 2
    elif published_at.startswith("2024-"):
        score += 1
    return score


def _build_stage1_fact_digest_v2(
    source_rows: List[Dict[str, Any]],
    timeline_rows: List[Dict[str, Any]],
    *,
    section_keywords: Optional[Dict[str, List[str]]] = None,
    narrative_order: Optional[List[str]] = None,
    conflict_terms: Optional[List[str]] = None,
    conflict_field: str = "timeline_window",
    conflict_resolution_rule: str = "prefer newest dated primary-source timeline evidence",
) -> Dict[str, Any]:
    """
    Build de-noised, rubric-adjacent fact digest to accompany source injection.

    This is a deterministic extraction pass: compact, source-referenced, and conflict-aware.
    """
    safe_max_facts_per_section = max(2, int(PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_FACTS_PER_SECTION))
    safe_max_summary_bullets = max(4, int(PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_SUMMARY_BULLETS))
    safe_max_narrative_words = max(120, int(PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_NARRATIVE_WORDS))
    effective_keywords = section_keywords or _FACT_DIGEST_V2_KEYWORDS
    section_names = list(effective_keywords.keys()) or list(_FACT_DIGEST_V2_SECTIONS)
    if "other_material_facts" not in section_names:
        section_names.append("other_material_facts")
    effective_narrative_order = [
        str(item or "").strip().lower()
        for item in (narrative_order or _FACT_DIGEST_V2_NARRATIVE_ORDER)
        if str(item or "").strip()
    ]
    effective_conflict_terms = _normalize_terms_list(conflict_terms or _STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS)

    sections: Dict[str, List[Dict[str, Any]]] = {
        key: []
        for key in section_names
    }
    seen = set()
    scored_facts: List[Tuple[int, str, Dict[str, Any]]] = []
    material_tokens = (
        "first gold",
        "gold pour",
        "funded",
        "facility",
        "loan",
        "placement",
        "capital",
        "npv",
        "irr",
        "aisc",
        "capex",
        "resource",
        "reserve",
        "grade",
        "production",
        "market cap",
        "shares",
        "enterprise value",
        "cash",
        "debt",
        "timeline",
        "milestone",
        "target",
        "on track",
        "risk",
        "delay",
        "catalyst",
        "tailwind",
        "headwind",
    )

    for source in source_rows:
        source_id = str(source.get("source_id", "S?")).strip() or "S?"
        published = str(source.get("published_at", "")).strip()
        authority_rank = _source_authority_rank(str(source.get("url", "")))
        excerpt = str(source.get("excerpt", ""))
        for sentence in _extract_source_sentences(excerpt):
            low = sentence.lower()
            if not re.search(r"\d", low) and not any(token in low for token in material_tokens):
                continue
            normalized = _normalize_fact_key(sentence)
            if not normalized or normalized in seen:
                continue

            section = _classify_fact_digest_v2_section(sentence, effective_keywords)
            bucket = sections.get(section, [])
            if len(bucket) >= safe_max_facts_per_section:
                continue

            item = {
                "source_id": source_id,
                "published_at": published,
                "fact": sentence,
                "windows": _extract_timeline_windows(sentence),
                "number_tokens": _extract_fact_digest_number_tokens(sentence),
            }
            bucket.append(item)
            seen.add(normalized)
            scored_facts.append(
                (
                    _score_fact_digest_sentence(sentence, published, authority_rank),
                    section,
                    item,
                )
            )

    # Ensure critical timeline facts from dedicated timeline extractor are included.
    for row in timeline_rows:
        sentence = str(row.get("fact", "")).strip()
        if not sentence:
            continue
        normalized = _normalize_fact_key(sentence)
        if normalized in seen:
            continue
        bucket = sections.setdefault("timelines_deadlines", [])
        if len(bucket) >= safe_max_facts_per_section:
            break
        item = {
            "source_id": str(row.get("source_id", "S?")).strip() or "S?",
            "published_at": str(row.get("published_at", "")).strip(),
            "fact": sentence,
            "windows": list(row.get("windows", []) or []),
            "number_tokens": _extract_fact_digest_number_tokens(sentence),
        }
        bucket.append(item)
        seen.add(normalized)
        scored_facts.append(
            (
                _score_fact_digest_sentence(
                    sentence,
                    str(row.get("published_at", "")).strip(),
                    int(row.get("authority_rank", 0)),
                ),
                "timelines_deadlines",
                item,
            )
        )

    compact_sections = {name: rows for name, rows in sections.items() if rows}
    total_facts = sum(len(rows) for rows in compact_sections.values())
    sections_with_facts = list(compact_sections.keys())

    # Minimal conflict table: timeline disagreements across extracted milestone facts.
    timeline_candidates: List[Dict[str, Any]] = []
    for row in compact_sections.get("timelines_deadlines", []):
        low = str(row.get("fact", "")).lower()
        if effective_conflict_terms and not any(token in low for token in effective_conflict_terms):
            continue
        windows = list(row.get("windows", []) or [])
        if not windows:
            windows = _extract_timeline_windows(str(row.get("fact", "")))
        for window in windows:
            timeline_candidates.append(
                {
                    "window": str(window),
                    "source_id": str(row.get("source_id", "S?")),
                    "published_at": str(row.get("published_at", "")),
                }
            )

    conflicts: List[Dict[str, Any]] = []
    unique_windows = []
    for item in timeline_candidates:
        window = item.get("window", "")
        if window and window not in unique_windows:
            unique_windows.append(window)
    if len(unique_windows) > 1:
        ranked = sorted(
            timeline_candidates,
            key=lambda item: (
                str(item.get("published_at", "")),
                int(_window_to_quarter_index(str(item.get("window", ""))) or -1),
            ),
            reverse=True,
        )
        canonical = ranked[0] if ranked else {}
        conflicts.append(
            {
                "field": conflict_field,
                "values": timeline_candidates[:8],
                "canonical": canonical,
                "resolution_rule": conflict_resolution_rule,
            }
        )

    # High-signal bullets used as a de-noised digest for downstream reasoning.
    scored_facts.sort(key=lambda item: item[0], reverse=True)
    summary_bullets: List[str] = []
    for _, _, item in scored_facts:
        source_id = str(item.get("source_id", "S?"))
        published = str(item.get("published_at", "")).strip()
        fact = str(item.get("fact", "")).strip()
        if not fact:
            continue
        line = f"[{source_id}] {published}: {fact}" if published else f"[{source_id}] {fact}"
        if line in summary_bullets:
            continue
        summary_bullets.append(line)
        if len(summary_bullets) >= safe_max_summary_bullets:
            break

    narrative_parts: List[str] = []
    for section in effective_narrative_order:
        rows = compact_sections.get(section, [])
        if not rows:
            continue
        top_facts = "; ".join(str(row.get("fact", "")).strip() for row in rows[:2] if row.get("fact"))
        if not top_facts:
            continue
        narrative_parts.append(f"{section.replace('_', ' ').title()}: {top_facts}")
    narrative_summary = _truncate_to_word_limit(
        " ".join(narrative_parts).strip(),
        safe_max_narrative_words,
    )

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
        "schema": "fact_digest_v2",
        "source_index": source_index,
        "sections": compact_sections,
        "summary_bullets": summary_bullets,
        "narrative_summary": narrative_summary,
        "conflicts": conflicts,
        "counts": {
            "source_count": len(source_rows),
            "decoded_source_count": sum(1 for row in source_rows if row.get("decoded")),
            "total_facts": total_facts,
            "sections_with_facts": len(sections_with_facts),
            "summary_bullets": len(summary_bullets),
            "conflicts": len(conflicts),
        },
    }


_ASX_ANNOUNCEMENT_SEARCH_URL = "https://www.asx.com.au/asx/v2/statistics/announcements.do"
_ASX_DETERMINISTIC_CACHE: Dict[str, Dict[str, Any]] = {}


def _extract_asx_symbol_from_context(user_query: str, run: Dict[str, Any]) -> str:
    """Infer ASX code from user query/run context."""
    def _normalize_symbol(raw_value: str) -> str:
        text = str(raw_value or "").strip().upper()
        if not text:
            return ""
        if ":" in text:
            text = text.split(":")[-1].strip()
        if "." in text:
            text = text.split(".")[0].strip()
        if not re.fullmatch(r"[A-Z0-9]{2,6}", text):
            return ""
        if sum(1 for ch in text if ch.isalpha()) < 2:
            return ""
        return text

    # Direct run hints, if available.
    for key in ("ticker", "symbol", "asx_code", "asx_symbol"):
        symbol = _normalize_symbol(str(run.get(key, "")))
        if symbol:
            return symbol

    texts = [
        str(user_query or ""),
        str(run.get("query", "") or ""),
        str(run.get("research_prompt", "") or ""),
        str(run.get("research_summary", "") or ""),
    ]
    for text in texts:
        match = re.search(r"\bASX\s*:\s*([A-Z0-9]{2,6})\b", text, flags=re.IGNORECASE)
        if match:
            symbol = _normalize_symbol(match.group(1))
            if symbol:
                return symbol
        match = re.search(r"\bASX\s+([A-Z0-9]{2,6})\b", text, flags=re.IGNORECASE)
        if match:
            symbol = _normalize_symbol(match.group(1))
            if symbol:
                return symbol
        # Stage-1 research briefs often carry ticker as "Ticker focus: WWI".
        match = re.search(
            r"\b(?:ticker(?:\s+focus)?|symbol)\s*[:=]\s*(?:ASX\s*[:\-]\s*)?([A-Z0-9]{2,6})\b",
            text,
            flags=re.IGNORECASE,
        )
        if match:
            symbol = _normalize_symbol(match.group(1))
            if symbol:
                return symbol
        suffix_match = re.search(r"\b([A-Z][A-Z0-9]{1,5})\.AX\b", text, flags=re.IGNORECASE)
        if suffix_match:
            return suffix_match.group(1).upper()

    for source in (run.get("results") or []):
        if not isinstance(source, dict):
            continue
        url = str(source.get("url", "")).strip()
        if not url:
            continue
        try:
            parsed = urlparse(url)
            qs = parse_qs(parsed.query or "")
        except Exception:
            continue
        for key in ("asxCode", "asxcode"):
            values = qs.get(key) or []
            for value in values:
                code = _normalize_symbol(str(value or ""))
                if code:
                    return code
        # Common secondary URL shape: /shares/asx-wwi/...
        path = (parsed.path or "").lower()
        path_match = re.search(r"/shares/asx-([a-z0-9]{2,6})\b", path)
        if path_match:
            code = _normalize_symbol(path_match.group(1))
            if code:
                return code
    return ""


def _extract_normalized_facts_from_query_text(query_text: str) -> Dict[str, Any]:
    """
    Parse injected normalized_facts JSON block from a prefixed user query string.

    Expected format:
      { "normalized_facts": { ... } }
      <template query text...>
    """
    raw = str(query_text or "")
    if not raw.strip():
        return {}
    match = re.search(r"\{\s*\"normalized_facts\"\s*:", raw)
    if not match:
        return {}
    try:
        parsed, _ = json.JSONDecoder().raw_decode(raw[match.start():].lstrip())
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    facts = parsed.get("normalized_facts", {})
    if not isinstance(facts, dict):
        return {}
    return dict(facts)


def _clean_html_fragment(text: str) -> str:
    """Remove HTML tags/entities from a title fragment."""
    value = re.sub(r"(?is)<[^>]+>", " ", str(text or ""))
    value = unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _parse_asx_datetime(date_ddmmyyyy: str, time_text: str) -> Optional[datetime]:
    """Parse ASX row date/time into datetime."""
    date_value = str(date_ddmmyyyy or "").strip()
    if not date_value:
        return None
    time_value = re.sub(r"\s+", " ", str(time_text or "").strip()).lower()
    if not time_value:
        time_value = "12:00 pm"
    for fmt in ("%d/%m/%Y %I:%M %p", "%d/%m/%Y %I:%M%p", "%d/%m/%Y"):
        try:
            if fmt == "%d/%m/%Y":
                return datetime.strptime(date_value, fmt)
            return datetime.strptime(f"{date_value} {time_value.upper()}", fmt)
        except Exception:
            continue
    return None


def _parse_asx_ids_id(url: str) -> str:
    """Extract ASX idsId token from display URL."""
    raw = str(url or "")
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        ids_values = parse_qs(parsed.query or "").get("idsId", [])
        if ids_values:
            return str(ids_values[0] or "").strip()
    except Exception:
        pass
    match = re.search(r"idsId=(\d+)", raw, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return ""


def _is_low_signal_asx_title(title: str) -> bool:
    """Title-level filter for routine legal/admin ASX notices."""
    low = str(title or "").lower()
    if not low:
        return True
    low_tokens = (
        "cleansing notice",
        "appendix 2a",
        "appendix 3b",
        "appendix 3c",
        "appendix 3y",
        "notification regarding unquoted securities",
        "quotation of securities",
        "notice for quotation of securities",
        "notice of quotation of securities",
        "proposed issue of securities",
        "proposed issue of quoted securities",
        "proposed issue of unquoted securities",
        "trading halt",
        "pause in trading",
        "voluntary suspension",
        "suspension from quotation",
        "request for trading halt",
        "request for voluntary suspension",
        "change of director",
        "director interest",
        "becoming a substantial holder",
        "ceasing to be substantial holder",
        "notice of annual general meeting",
        "s708a",
        "section 708a",
        "application for quotation",
    )
    return any(token in low for token in low_tokens)


def _asx_title_signal_rank(title: str, price_sensitive: bool) -> int:
    """Heuristic rank for valuation-relevant ASX announcements."""
    low = str(title or "").lower()
    if not low:
        return -10
    if _is_low_signal_asx_title(low):
        return -5
    score = 0
    if price_sensitive:
        score += 3
    critical_tokens = (
        "investor presentation",
        "corporate presentation",
        "quarterly",
        "activities report",
        "annual report",
        "financial report",
        "resource",
        "reserve",
        "jorc",
        "dfs",
        "definitive feasibility",
        "pfs",
        "feasibility",
        "funding",
        "facility",
        "placement",
        "production",
        "first gold",
        "gold pour",
        "npv",
        "irr",
    )
    score += min(8, sum(1 for token in critical_tokens if token in low))
    return score


def _parse_asx_announcement_rows(html_text: str) -> List[Dict[str, Any]]:
    """Parse ASX announcement search page into row records."""
    rows: List[Dict[str, Any]] = []
    if not html_text:
        return rows

    row_chunks = re.findall(r"(?is)<tr>(.*?)</tr>", html_text)
    for chunk in row_chunks:
        if "displayannouncement.do" not in chunk.lower():
            continue
        date_match = re.search(
            r"(?is)(\d{2}/\d{2}/\d{4})\s*<br>\s*(?:<span[^>]*>([^<]+)</span>)?",
            chunk,
        )
        if not date_match:
            continue
        date_text = str(date_match.group(1) or "").strip()
        time_text = str(date_match.group(2) or "").strip()

        link_match = re.search(
            r'(?is)<a[^>]+href="([^"]*displayAnnouncement\.do[^"]+)"[^>]*>',
            chunk,
        )
        if not link_match:
            continue
        display_url = urljoin("https://www.asx.com.au", unescape(link_match.group(1)))

        title_match = re.search(
            r'(?is)<a[^>]+href="[^"]*displayAnnouncement\.do[^"]+"[^>]*>\s*(.*?)<br',
            chunk,
        )
        title = _clean_html_fragment(title_match.group(1) if title_match else "")
        if not title:
            title = "ASX Announcement"

        price_sensitive = (
            "icon-price-sensitive" in chunk.lower()
            or "title=\"price sensitive\"" in chunk.lower()
            or "title='price sensitive'" in chunk.lower()
        )
        published_dt = _parse_asx_datetime(date_text, time_text)
        published_iso = published_dt.strftime("%Y-%m-%d") if published_dt else ""
        rows.append(
            {
                "display_url": display_url,
                "ids_id": _parse_asx_ids_id(display_url),
                "title": title,
                "price_sensitive": bool(price_sensitive),
                "published_dt": published_dt,
                "published_at": published_iso,
                "signal_rank": _asx_title_signal_rank(title, bool(price_sensitive)),
            }
        )
    return rows


async def _resolve_asx_display_to_pdf_url(
    client: httpx.AsyncClient,
    display_url: str,
) -> Tuple[str, str]:
    """Resolve ASX displayAnnouncement URL to direct announcements PDF URL."""
    last_err = "resolve_unknown"
    for attempt in range(1, 4):
        try:
            response = await client.get(display_url)
        except Exception as exc:
            last_err = f"resolve_fetch_failed:{type(exc).__name__}:{str(exc)[:180]}"
            if attempt < 3:
                await asyncio.sleep(0.25 * attempt)
                continue
            return "", last_err

        if response.status_code >= 400:
            last_err = f"resolve_http_{response.status_code}"
            if response.status_code in {403, 425, 429, 500, 502, 503, 504} and attempt < 3:
                await asyncio.sleep(0.35 * attempt)
                continue
            return "", last_err

        html_text = str(response.text or "")
        hidden = re.search(r'(?is)name="pdfURL"\s+value="([^"]+)"', html_text)
        if hidden:
            return unescape(hidden.group(1)).strip(), ""

        direct = re.search(
            r"(https://announcements\.asx\.com\.au/asxpdf/[^\s\"']+\.pdf)",
            html_text,
            flags=re.IGNORECASE,
        )
        if direct:
            return unescape(direct.group(1)).strip(), ""

        last_err = "resolve_pdf_url_not_found"
        if attempt < 3:
            await asyncio.sleep(0.2 * attempt)
            continue
    return "", last_err


def _asx_doc_key(url: str) -> str:
    """Build coarse de-duplication key for ASX announcement URLs."""
    raw = str(url or "").strip()
    if not raw:
        return ""
    ids = _parse_asx_ids_id(raw)
    if ids:
        return f"ids:{ids}"
    parsed = urlparse(raw)
    host = parsed.netloc.lower()
    path = parsed.path or ""
    if "announcements.asx.com.au" in host:
        filename = path.rsplit("/", 1)[-1]
        if filename:
            return f"asxpdf:{filename.lower()}"
    return raw.lower()


def _asx_cache_key(symbol: str, user_query: str, research_brief: str) -> str:
    """Stable cache key for per-run deterministic ASX ingest."""
    query_seed = re.sub(r"\s+", " ", f"{user_query} {research_brief}").strip().lower()
    query_seed = query_seed[:240]
    return (
        f"{symbol.upper()}|{int(ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS)}|"
        f"{int(ASX_DETERMINISTIC_LOOKBACK_YEARS)}|"
        f"{int(ASX_DETERMINISTIC_MAX_DECODE)}|"
        f"{bool(ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY)}|"
        f"{bool(ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL)}|{query_seed}"
    )


async def _collect_deterministic_asx_sources(
    *,
    user_query: str,
    research_brief: str,
    run: Dict[str, Any],
) -> Dict[str, Any]:
    """Fetch and decode latest material ASX announcements for injection."""
    report: Dict[str, Any] = {
        "enabled": bool(ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED),
        "used": False,
        "symbol": "",
        "reason": "",
        "cache_hit": False,
        "fetched_rows": 0,
        "selected_rows": 0,
        "decoded_rows": 0,
        "target_rows": int(max(1, int(ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS))),
        "price_sensitive_only": bool(ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY),
        "include_non_sensitive_fill": bool(ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL),
        "years_queried": [],
        "sources": [],
        "errors": [],
    }
    if not ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED:
        report["reason"] = "disabled"
        return report

    symbol = _extract_asx_symbol_from_context(user_query, run)
    report["symbol"] = symbol
    if not symbol:
        report["reason"] = "symbol_not_detected"
        return report

    cache_key = _asx_cache_key(symbol, user_query, research_brief)
    cached = _ASX_DETERMINISTIC_CACHE.get(cache_key)
    if cached:
        clone = copy.deepcopy(cached)
        clone["cache_hit"] = True
        return clone

    lookback_years = max(1, int(ASX_DETERMINISTIC_LOOKBACK_YEARS))
    target_rows = max(1, int(ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS))
    decode_limit = max(0, int(ASX_DETERMINISTIC_MAX_DECODE))
    timeout = max(8.0, float(ASX_DETERMINISTIC_FETCH_TIMEOUT_SECONDS))
    years = [datetime.utcnow().year - idx for idx in range(lookback_years)]
    report["years_queried"] = list(years)

    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
    all_rows: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": user_agent},
    ) as client:
        for year in years:
            params = {
                "by": "asxCode",
                "asxCode": symbol,
                "timeframe": "Y",
                "year": str(year),
            }
            try:
                response = await client.get(_ASX_ANNOUNCEMENT_SEARCH_URL, params=params)
            except Exception as exc:
                report["errors"].append(f"year_{year}:fetch_failed:{type(exc).__name__}")
                continue
            if response.status_code >= 400:
                report["errors"].append(f"year_{year}:http_{response.status_code}")
                continue
            parsed = _parse_asx_announcement_rows(str(response.text or ""))
            all_rows.extend(parsed)

    deduped_rows: List[Dict[str, Any]] = []
    seen_row_keys = set()
    for row in all_rows:
        key = str(row.get("ids_id", "")).strip() or str(row.get("display_url", "")).strip()
        if not key or key in seen_row_keys:
            continue
        seen_row_keys.add(key)
        deduped_rows.append(row)

    deduped_rows.sort(
        key=lambda item: (
            item.get("published_dt") or datetime.min,
            int(item.get("signal_rank", -10)),
        ),
        reverse=True,
    )
    report["fetched_rows"] = len(deduped_rows)

    selected: List[Dict[str, Any]] = []
    for row in deduped_rows:
        if int(row.get("signal_rank", -10)) < 0:
            continue
        if ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY and not bool(row.get("price_sensitive")):
            continue
        selected.append(row)
        if len(selected) >= target_rows:
            break

    if (
        ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY
        and ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL
        and len(selected) < target_rows
    ):
        for row in deduped_rows:
            if row in selected:
                continue
            if int(row.get("signal_rank", -10)) < 3:
                continue
            selected.append(row)
            if len(selected) >= target_rows:
                break

    if not selected:
        report["reason"] = "no_material_rows"
        _ASX_DETERMINISTIC_CACHE[cache_key] = copy.deepcopy(report)
        return report

    sem = asyncio.Semaphore(2)
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers={"User-Agent": user_agent},
    ) as resolve_client:
        async def _resolve_row(row: Dict[str, Any]) -> Dict[str, Any]:
            async with sem:
                display_url = str(row.get("display_url", "")).strip()
                pdf_url, err = await _resolve_asx_display_to_pdf_url(resolve_client, display_url)
                out = dict(row)
                out["pdf_url"] = pdf_url or display_url
                if err:
                    out["resolve_error"] = err
                return out

        resolved_rows = await asyncio.gather(
            *[_resolve_row(row) for row in selected],
            return_exceptions=False,
        )

    from .research.providers.perplexity import PerplexityResearchProvider

    decoder = PerplexityResearchProvider()
    decode_targets = resolved_rows[: min(len(resolved_rows), decode_limit)]
    query_context = f"ASX:{symbol}\n{user_query}\n{research_brief}".strip()

    async def _decode_row(row: Dict[str, Any]) -> Dict[str, Any]:
        url = str(row.get("pdf_url", "")).strip()
        title = str(row.get("title", "")).strip()
        if not url:
            return {"status": "failed", "error": "missing_url", "decoded_chars": 0}
        return await decoder._decode_one_source(url=url, title=title, query_context=query_context)

    decoded_outputs = await asyncio.gather(
        *[_decode_row(row) for row in decode_targets],
        return_exceptions=True,
    )
    decode_by_doc_key: Dict[str, Dict[str, Any]] = {}
    for row, output in zip(decode_targets, decoded_outputs):
        doc_key = _asx_doc_key(str(row.get("pdf_url", "")))
        if isinstance(output, Exception):
            decode_by_doc_key[doc_key] = {
                "status": "failed",
                "error": f"{type(output).__name__}",
                "decoded_chars": 0,
            }
            continue
        decode_by_doc_key[doc_key] = output if isinstance(output, dict) else {}

    sources: List[Dict[str, Any]] = []
    decoded_rows = 0
    for row in resolved_rows:
        pdf_url = str(row.get("pdf_url", "")).strip()
        doc_key = _asx_doc_key(pdf_url)
        decoded = decode_by_doc_key.get(doc_key, {})
        excerpt = str(decoded.get("excerpt", "")).strip()
        status = str(decoded.get("status", "")).strip() or "pending"
        if status == "decoded" and excerpt:
            decoded_rows += 1
        title = str(row.get("title", "")).strip() or "ASX Announcement"
        published_at = str(row.get("published_at", "")).strip()
        signal_rank = int(row.get("signal_rank", 0))
        source_snippet = (
            f"ASX announcement title: {title}. "
            f"{'Price sensitive announcement.' if row.get('price_sensitive') else 'Announcement.'}"
        )
        source_item: Dict[str, Any] = {
            "title": title,
            "url": pdf_url or str(row.get("display_url", "")).strip(),
            "published_at": published_at,
            "content": excerpt or source_snippet,
            "source_snippet": source_snippet,
            "decode_status": status,
            "decoded_excerpt": excerpt,
            "decoded_chars": int(decoded.get("decoded_chars", 0) or 0),
            "asx_deterministic": True,
            "asx_price_sensitive": bool(row.get("price_sensitive")),
            "asx_ids_id": str(row.get("ids_id", "")),
            "material_signal_score": max(signal_rank, _excerpt_material_signal_score(excerpt or source_snippet)),
            "score": 1.0,
        }
        if decoded.get("error"):
            source_item["decode_error"] = str(decoded.get("error"))
        if row.get("resolve_error"):
            source_item["resolve_error"] = str(row.get("resolve_error"))
        sources.append(source_item)

    report["used"] = bool(sources)
    report["reason"] = "ok" if sources else "decode_or_selection_empty"
    report["selected_rows"] = len(sources)
    report["decoded_rows"] = decoded_rows
    report["sources"] = sources
    _ASX_DETERMINISTIC_CACHE[cache_key] = copy.deepcopy(report)
    return report


def _merge_deterministic_sources_into_results(
    existing_results: List[Dict[str, Any]],
    deterministic_sources: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Place deterministic sources first and dedupe overlap with existing rows."""
    if not deterministic_sources:
        return list(existing_results or [])

    existing = [dict(item) for item in (existing_results or []) if isinstance(item, dict)]
    existing_by_key: Dict[str, Dict[str, Any]] = {}
    for row in existing:
        key = _asx_doc_key(str(row.get("url", "")))
        if key and key not in existing_by_key:
            existing_by_key[key] = row

    merged: List[Dict[str, Any]] = []
    seen_keys = set()
    for source in deterministic_sources:
        if not isinstance(source, dict):
            continue
        row = dict(source)
        key = _asx_doc_key(str(row.get("url", "")))
        if key and key in existing_by_key:
            existing_row = existing_by_key[key]
            existing_excerpt = str(
                existing_row.get("decoded_excerpt")
                or existing_row.get("content")
                or existing_row.get("source_snippet")
                or ""
            ).strip()
            new_excerpt = str(
                row.get("decoded_excerpt")
                or row.get("content")
                or row.get("source_snippet")
                or ""
            ).strip()
            if len(existing_excerpt) > len(new_excerpt):
                row["content"] = existing_excerpt
                row["decoded_excerpt"] = str(existing_row.get("decoded_excerpt", "")).strip()
                row["decode_status"] = str(existing_row.get("decode_status", row.get("decode_status", "")))
        merged.append(row)
        if key:
            seen_keys.add(key)

    for row in existing:
        key = _asx_doc_key(str(row.get("url", "")))
        if key and key in seen_keys:
            continue
        merged.append(row)
    return merged


async def _augment_run_with_deterministic_asx_sources(
    *,
    user_query: str,
    research_brief: str,
    run: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Inject deterministic ASX sources into run results for Stage 1 evidence prep."""
    ingestion = await _collect_deterministic_asx_sources(
        user_query=user_query,
        research_brief=research_brief,
        run=run,
    )
    ingestion_summary = {
        "enabled": bool(ingestion.get("enabled", False)),
        "used": bool(ingestion.get("used", False)),
        "symbol": str(ingestion.get("symbol", "")),
        "reason": str(ingestion.get("reason", "")),
        "cache_hit": bool(ingestion.get("cache_hit", False)),
        "fetched_rows": int(ingestion.get("fetched_rows", 0)),
        "selected_rows": int(ingestion.get("selected_rows", 0)),
        "decoded_rows": int(ingestion.get("decoded_rows", 0)),
        "target_rows": int(ingestion.get("target_rows", 0)),
        "price_sensitive_only": bool(ingestion.get("price_sensitive_only", False)),
        "include_non_sensitive_fill": bool(
            ingestion.get("include_non_sensitive_fill", False)
        ),
        "years_queried": list(ingestion.get("years_queried", []) or []),
        "errors": list(ingestion.get("errors", []) or [])[:8],
    }
    provider_meta = run.setdefault("provider_metadata", {})
    if not isinstance(provider_meta, dict):
        provider_meta = {}
        run["provider_metadata"] = provider_meta
    provider_meta["asx_deterministic_ingestion"] = ingestion_summary

    sources = ingestion.get("sources", []) or []
    if not sources:
        return run, ingestion_summary

    merged_results = _merge_deterministic_sources_into_results(
        existing_results=list(run.get("results") or []),
        deterministic_sources=sources,
    )
    run["results"] = merged_results
    run["result_count"] = len(merged_results)
    run["asx_deterministic_sources"] = sources
    _progress_log(
        "Stage1 deterministic ASX injection: "
        f"symbol={ingestion_summary.get('symbol')}, "
        f"selected={ingestion_summary.get('selected_rows')}, "
        f"decoded={ingestion_summary.get('decoded_rows')}, "
        f"cache_hit={ingestion_summary.get('cache_hit')}"
    )
    return run, ingestion_summary


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


def _excerpt_material_signal_score(excerpt: str) -> int:
    """Rough materiality score for a decoded excerpt."""
    text = re.sub(r"\s+", " ", str(excerpt or "")).strip()
    if not text:
        return -5
    low = text.lower()
    score = 0
    if re.search(r"\d", low):
        score += 1
    signal_tokens = (
        "npv",
        "irr",
        "aisc",
        "capex",
        "resource",
        "reserve",
        "production",
        "first gold",
        "gold pour",
        "funding",
        "facility",
        "loan",
        "cash",
        "debt",
        "market cap",
        "shares",
        "enterprise value",
        "ev/oz",
        "milestone",
        "timeline",
    )
    score += min(8, sum(1 for token in signal_tokens if token in low))
    if _is_low_signal_legal_boilerplate(text):
        score -= 6
    if _is_heading_like_sentence(text):
        score -= 3
    if len(text) < 120:
        score -= 1
    return score


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


def _extract_source_sentences(excerpt: str) -> List[str]:
    """Split decoded excerpt into cleaned sentence candidates."""
    if not excerpt:
        return []
    raw_parts = re.split(r"(?<=[\.\!\?])\s+|\n+", excerpt)
    out: List[str] = []
    for part in raw_parts:
        sentence = re.sub(r"\s+", " ", part).strip(" \t-")
        if len(sentence) < 40:
            continue
        if re.match(r"^[a-z]{3,}[,;:]\s", sentence):
            low_sentence = sentence.lower()
            if not any(
                token in low_sentence
                for token in (
                    "npv",
                    "irr",
                    "aisc",
                    "capex",
                    "resource",
                    "reserve",
                    "production",
                    "first gold",
                    "gold pour",
                    "funding",
                    "facility",
                    "cash",
                    "debt",
                    "market cap",
                    "shares",
                    "enterprise value",
                )
            ):
                continue
        if _is_low_signal_legal_boilerplate(sentence):
            continue
        if _is_heading_like_sentence(sentence):
            continue
        if len(sentence) > 420:
            sentence = sentence[:417].rstrip() + "..."
        out.append(sentence)
    return out


def _is_heading_like_sentence(sentence: str) -> bool:
    """Drop short heading/table-style lines that are not evidence-bearing facts."""
    text = re.sub(r"\s+", " ", str(sentence or "")).strip()
    if not text:
        return True
    low = text.lower()

    if low in {
        "contents",
        "table of contents",
        "for personal use only",
        "announcements",
        "presentations",
        "project highlights",
    }:
        return True

    # Keep compact heading-like strings only if they include strong signal.
    strong_tokens = (
        "npv",
        "irr",
        "aisc",
        "capex",
        "resource",
        "reserve",
        "production",
        "first gold",
        "gold pour",
        "funding",
        "facility",
        "cash",
        "debt",
        "market cap",
        "shares",
        "enterprise value",
    )
    if any(token in low for token in strong_tokens):
        return False

    words = [token for token in re.split(r"\s+", text) if token]
    if len(words) <= 12 and len(text) <= 95:
        if not re.search(r"[\.!?;:]", text):
            alpha = [c for c in text if c.isalpha()]
            if alpha:
                upper_ratio = sum(1 for c in alpha if c.isupper()) / float(len(alpha))
                if upper_ratio >= 0.72:
                    return True
            # Short title-style strings with no punctuation are often headings.
            if not re.search(r"\d", text):
                return True
    return False


def _is_low_signal_legal_boilerplate(sentence: str) -> bool:
    """Filter legal/admin boilerplate that adds minimal valuation signal."""
    low = str(sentence or "").lower()
    if not low:
        return True

    legal_patterns = (
        "708a cleansing notice",
        "cleansing notice",
        "application for quotation of securities",
        "notice for quotation of securities",
        "notice of quotation of securities",
        "proposed issue of securities",
        "proposed issue of quoted securities",
        "proposed issue of unquoted securities",
        "appendix 2a",
        "appendix 3b",
        "appendix 3c",
        "part 6d.2",
        "chapter 2m",
        "sections 674 and 674a",
        "corporations act 2001",
        "without disclosure to investors",
        "this notice is given under paragraph 5(e)",
        "for personal use only",
        "announcement summary entity name",
        "trading halt",
        "pause in trading",
        "voluntary suspension",
        "suspension from quotation",
        "request for trading halt",
        "request for voluntary suspension",
    )
    if any(token in low for token in legal_patterns):
        # Keep if the same sentence also carries unusually strong valuation signal.
        override_tokens = (
            "npv",
            "irr",
            "aisc",
            "capex",
            "resource",
            "reserve",
            "production",
            "first gold",
            "gold pour",
            "market cap",
            "shares outstanding",
            "enterprise value",
            "cash",
            "debt",
            "funding",
            "loan facility",
        )
        if any(token in low for token in override_tokens):
            return False
        return True
    return False


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


def _is_low_signal_notice_source_item(source: Dict[str, Any]) -> bool:
    """Detect legal/admin notice docs with low valuation signal."""
    title = str(source.get("title", "")).lower()
    content = str(
        source.get("decoded_excerpt")
        or source.get("content")
        or source.get("source_snippet")
        or ""
    ).lower()
    url = str(source.get("url", "")).lower()
    text = f"{title} {content} {url}"

    hard_block_patterns = (
        "trading halt",
        "pause in trading",
        "voluntary suspension",
        "suspension from quotation",
        "request for trading halt",
        "request for voluntary suspension",
        "application for quotation of securities",
        "notice for quotation of securities",
        "notice of quotation of securities",
        "proposed issue of securities",
        "proposed issue of quoted securities",
        "proposed issue of unquoted securities",
        "quotation of securities",
        "appendix 2a",
        "appendix 3b",
        "appendix 3c",
        "cleansing notice",
        "708a cleansing notice",
    )
    if any(token in text for token in hard_block_patterns):
        return True

    # Historical index/listing pages are usually retrieval scaffolding, not
    # evidence-bearing documents, unless they carry strong valuation terms.
    index_patterns = (
        "quarterly reports - 2017 to 2022",
        "presentations and interviews",
        "announcements and media releases",
        "investor centre",
    )
    if any(token in title for token in index_patterns):
        override_tokens = (
            "npv",
            "irr",
            "aisc",
            "capex",
            "resource",
            "reserve",
            "production",
            "first gold",
            "gold pour",
            "funding",
            "facility",
            "cash",
            "debt",
            "market cap",
            "shares",
            "enterprise value",
        )
        if not any(token in text for token in override_tokens):
            return True
    if (
        re.search(r"\b20\d{2}\s*(?:to|\-)\s*20\d{2}\b", title)
        and any(token in title for token in ("quarterly reports", "annual reports", "presentations"))
    ):
        if not any(token in text for token in ("npv", "irr", "aisc", "resource", "production", "first gold", "gold pour")):
            return True

    low_patterns = (
        "part 6d.2",
        "chapter 2m",
        "sections 674 and 674a",
        "corporations act 2001",
    )
    if not any(token in text for token in low_patterns):
        return False

    # Keep if there is clear valuation/timeline signal in the same source.
    override_tokens = (
        "npv",
        "irr",
        "aisc",
        "capex",
        "resource",
        "reserve",
        "production",
        "first gold",
        "gold pour",
        "funding",
        "loan facility",
        "cash",
        "debt",
        "market cap",
        "shares outstanding",
        "enterprise value",
    )
    return not any(token in text for token in override_tokens)


def _source_authority_rank(url: str) -> int:
    """Rough source-authority rank for timeline evidence ordering."""
    domain = ""
    try:
        domain = urlparse(url or "").netloc.lower()
    except Exception:
        domain = ""
    if domain.endswith("asx.com.au") or domain.endswith("sec.gov"):
        return 4
    if domain.endswith("wcsecure.weblink.com.au"):
        return 3
    if "investor" in domain or "announcements" in domain:
        return 2
    return 1


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


def _extract_timeline_windows(text: str) -> List[str]:
    """Extract quarter/month window tokens (e.g., Q1 2026, March 2026)."""
    raw = text or ""
    windows: List[str] = []

    quarter_matches = re.findall(r"\bq([1-4])\s*[\|/\-]?\s*(20\d{2})\b", raw, flags=re.IGNORECASE)
    for q, year in quarter_matches:
        windows.append(f"Q{int(q)} {int(year)}")

    month_matches = re.findall(
        r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b",
        raw,
        flags=re.IGNORECASE,
    )
    for month, year in month_matches:
        windows.append(f"{month.title()} {int(year)}")

    deduped: List[str] = []
    seen = set()
    for token in windows:
        norm = token.lower()
        if norm in seen:
            continue
        seen.add(norm)
        deduped.append(token)
    return deduped


def _window_to_quarter_index(token: str) -> Optional[int]:
    """Map timeline token into sortable quarter index."""
    value = (token or "").strip()
    q_match = re.match(r"^Q([1-4])\s+(20\d{2})$", value, flags=re.IGNORECASE)
    if q_match:
        q = int(q_match.group(1))
        year = int(q_match.group(2))
        return (year * 4) + (q - 1)

    m_match = re.match(
        r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})$",
        value,
        flags=re.IGNORECASE,
    )
    if not m_match:
        return None
    month_name = m_match.group(1).lower()
    year = int(m_match.group(2))
    month_to_q = {
        "january": 1,
        "february": 1,
        "march": 1,
        "april": 2,
        "may": 2,
        "june": 2,
        "july": 3,
        "august": 3,
        "september": 3,
        "october": 4,
        "november": 4,
        "december": 4,
    }
    q = month_to_q.get(month_name)
    if q is None:
        return None
    return (year * 4) + (q - 1)


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
        "- Every key numeric claim must include at least one [S#] citation.\n"
        "- Mark inferred values with ESTIMATE and one-line rationale.\n"
        "- If evidence conflicts, prefer the newest dated primary source and state conflict."
    )
    if cashflow_schema_contract.strip():
        requirements = f"{requirements}\n\n{cashflow_schema_contract.strip()}"

    evidence_blocks: List[str] = []
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


_STAGE1_RUBRIC_SECTION_MARKERS = [
    ("quality_score", ["quality score", "quality_score"]),
    ("value_score", ["value score", "value_score"]),
    (
        "price_targets",
        ["12-month", "24-month", "12/24", "price target", "price_targets"],
    ),
    ("development_timeline", ["development timeline", "timeline", "milestone"]),
    ("certainty", ["certainty", "certainty %", "certainty_pct"]),
    ("headwinds_tailwinds", ["headwind", "tailwind", "headwinds", "tailwinds"]),
    (
        "management_competition_assessment",
        [
            "management & competition",
            "management_competition_assessment",
            "management quality",
            "governance",
            "insider ownership",
            "board",
            "executive",
        ],
    ),
    ("npv_assessment", ["npv", "risked npv", "dcf"]),
]

_STAGE1_RUBRIC_CRITICAL_SECTIONS = {
    "quality_score",
    "value_score",
    "price_targets",
    "development_timeline",
}


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
            )
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
    from .research.providers.perplexity import PerplexityResearchProvider

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


async def stage2_collect_rankings(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    ranking_models: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """
    Stage 2: Each model ranks the anonymized responses.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content
        stage1_results: Results from Stage 1
        ranking_models: Optional explicit judge model list (defaults to COUNCIL_MODELS)

    Returns:
        Tuple of (rankings list, label_to_model mapping)
    """
    # Create anonymized labels for responses (Response A, Response B, etc.)
    labels = [chr(65 + i) for i in range(len(stage1_results))]  # A, B, C, ...

    # Create mapping from label to model name
    label_to_model = {
        f"Response {label}": result['model']
        for label, result in zip(labels, stage1_results)
    }

    # Build the ranking prompt
    responses_text = "\n\n".join([
        f"Response {label}:\n{result['response']}"
        for label, result in zip(labels, stage1_results)
    ])

    ranking_prompt = f"""You are evaluating different responses to the following question:

{enhanced_context}

Here are the responses from different models (anonymized):

{responses_text}

Your task:
1. First, evaluate each response individually. For each response, explain what it does well and what it does poorly.
2. Then, at the very end of your response, provide a final ranking.

IMPORTANT: Your final ranking MUST be formatted EXACTLY as follows:
- Start with the line "FINAL RANKING:" (all caps, with colon)
- Then list the responses from best to worst as a numbered list
- Each line should be: number, period, space, then ONLY the response label (e.g., "1. Response A")
- Do not add any other text or explanations in the ranking section

Example of the correct format for your ENTIRE response:

Response A provides good detail on X but misses Y...
Response B is accurate but lacks depth on Z...
Response C offers the most comprehensive answer...

FINAL RANKING:
1. Response C
2. Response A
3. Response B

Now provide your evaluation and ranking:"""

    messages = [{"role": "user", "content": ranking_prompt}]

    # Get rankings from selected judge models in parallel
    judge_models = [m for m in (ranking_models or COUNCIL_MODELS) if m]
    responses = await query_models_parallel(judge_models, messages)

    # Format results
    stage2_results = []
    for model, response in responses.items():
        if response is not None:
            full_text = response.get('content', '')
            parsed = parse_ranking_from_text(full_text)
            ranking_entries = _ranking_entries_from_labels(parsed, label_to_model)
            stage2_results.append({
                "model": model,
                "ranking": full_text,
                "parsed_ranking": parsed,
                "parsed_ranking_models": [
                    str(item.get("model") or item.get("label") or "")
                    for item in ranking_entries
                ],
                "ranking_entries": ranking_entries,
                "top_choice_label": ranking_entries[0].get("label") if ranking_entries else None,
                "top_choice_model": ranking_entries[0].get("model") if ranking_entries else None,
            })

    return stage2_results, label_to_model


def _parse_json_object_from_text(text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Extract a JSON object from model text output using permissive fallbacks."""
    payload = (text or "").strip()
    if not payload:
        return None, "empty_response"

    try:
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed, None
    except json.JSONDecodeError:
        pass

    fenced = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", payload, re.IGNORECASE)
    if fenced:
        try:
            parsed = json.loads(fenced.group(1))
            if isinstance(parsed, dict):
                return parsed, None
        except json.JSONDecodeError:
            pass

    # Last fallback: scan for first decodable JSON object in the text.
    decoder = json.JSONDecoder()
    for idx, char in enumerate(payload):
        if char != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(payload[idx:])
            if isinstance(candidate, dict):
                return candidate, None
        except json.JSONDecodeError:
            continue

    return None, "no_json_object_found"


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return None


def _coerce_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        token = value.strip().replace("%", "")
        try:
            return float(token)
        except ValueError:
            return None
    return None


def _normalize_stage2_revision_delta(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Normalize revision payload returned by a model.

    This parser is intentionally permissive:
    - accepts missing/implicit changed flag
    - accepts notes-only payloads
    - only rejects truly empty/unusable payloads
    """
    changed = _coerce_bool(raw.get("changed"))
    reason_text = str(raw.get("reason", "") or "").strip()
    revision_notes = str(
        raw.get("revision_notes")
        or raw.get("notes")
        or raw.get("summary")
        or reason_text
        or ""
    ).strip()

    normalized: Dict[str, Any] = {
        "changed": False,
        "reason": reason_text,
        "revision_notes": revision_notes,
        "changes": raw.get("changes") if isinstance(raw.get("changes"), list) else [],
        "updated_scores": raw.get("updated_scores") if isinstance(raw.get("updated_scores"), dict) else {},
        "updated_price_targets": (
            raw.get("updated_price_targets")
            if isinstance(raw.get("updated_price_targets"), dict)
            else {}
        ),
        "updated_observations": (
            [str(item).strip() for item in (raw.get("updated_observations") or []) if str(item).strip()]
            if isinstance(raw.get("updated_observations"), list)
            else []
        ),
        "evidence_refs": raw.get("evidence_refs") if isinstance(raw.get("evidence_refs"), list) else [],
        "confidence": None,
    }

    if not normalized["updated_scores"] and isinstance(raw.get("scores"), dict):
        normalized["updated_scores"] = dict(raw.get("scores") or {})
    if not normalized["updated_price_targets"] and isinstance(raw.get("price_targets"), dict):
        normalized["updated_price_targets"] = dict(raw.get("price_targets") or {})

    confidence = _coerce_float(raw.get("confidence"))
    if confidence is not None:
        if confidence > 1.0 and confidence <= 100.0:
            confidence = confidence / 100.0
        if 0.0 <= confidence <= 1.0:
            normalized["confidence"] = round(confidence, 4)

    updated_scores = normalized["updated_scores"]
    if updated_scores:
        for key in ("quality", "value"):
            if key not in updated_scores:
                continue
            score = _coerce_float(updated_scores.get(key))
            if score is None or score < 0 or score > 100:
                updated_scores.pop(key, None)
            else:
                updated_scores[key] = round(float(score), 2)

    inferred_changed = bool(
        normalized["changes"]
        or normalized["updated_scores"]
        or normalized["updated_price_targets"]
        or normalized["updated_observations"]
        or normalized["revision_notes"]
    )
    if changed is None:
        normalized["changed"] = inferred_changed
    else:
        normalized["changed"] = changed

    if not inferred_changed and not reason_text:
        return None, "empty_revision_payload"
    return normalized, None


def _extract_changed_flag_from_text(text: str) -> Optional[bool]:
    payload = str(text or "")
    m = re.search(r"(?im)^\s*CHANGED\s*:\s*(YES|NO|TRUE|FALSE|1|0)\s*$", payload)
    if m:
        token = m.group(1).strip().lower()
        return token in {"yes", "true", "1"}
    m2 = re.search(r'(?i)"changed"\s*:\s*(true|false)', payload)
    if m2:
        return m2.group(1).lower() == "true"
    return None


def _extract_revision_notes_from_text(text: str) -> str:
    payload = str(text or "").strip()
    if not payload:
        return ""
    m = re.search(r"(?is)REVISION_NOTES\s*:\s*(.+)$", payload)
    if m:
        return m.group(1).strip()
    # Strip code fences when present.
    fence = re.search(r"(?is)```(?:json)?\s*(.+?)\s*```", payload)
    if fence:
        return fence.group(1).strip()
    return payload


def _ranking_labels_from_result(ranking: Dict[str, Any]) -> List[str]:
    parsed = ranking.get("parsed_ranking")
    if isinstance(parsed, list):
        labels = []
        for item in parsed:
            if isinstance(item, str):
                label = item.strip()
            elif isinstance(item, dict):
                label = str(item.get("label") or "").strip()
            else:
                label = ""
            if label:
                labels.append(label)
        if labels:
            return labels

    entries = ranking.get("ranking_entries")
    if isinstance(entries, list):
        labels = [
            str(item.get("label") or "").strip()
            for item in entries
            if isinstance(item, dict) and str(item.get("label") or "").strip()
        ]
        if labels:
            return labels

    ranking_text = str(ranking.get("ranking") or "")
    return parse_ranking_from_text(ranking_text)


def _ranking_entries_from_labels(
    labels: List[str],
    label_to_model: Dict[str, str],
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for position, label in enumerate(labels or [], start=1):
        clean_label = str(label or "").strip()
        if not clean_label:
            continue
        entries.append(
            {
                "rank": position,
                "label": clean_label,
                "model": label_to_model.get(clean_label),
            }
        )
    return entries


def _build_stage2_revision_prompt(
    *,
    enhanced_context: str,
    own_label: str,
    responses_text: str,
    aggregate_rankings: List[Dict[str, Any]],
) -> str:
    ranking_lines: List[str] = []
    for i, item in enumerate(aggregate_rankings or [], start=1):
        ranking_lines.append(
            f"{i}. {item.get('model')} (avg_rank={item.get('average_rank')})"
        )
    ranking_block = "\n".join(ranking_lines) if ranking_lines else "(none)"

    return f"""You are re-evaluating your own Stage 1 analysis after seeing peer model outputs.

You authored: {own_label}

Original question/context:
{enhanced_context}

Peer outputs (anonymized):
{responses_text}

Stage 2 aggregate ranking by model:
{ranking_block}

Task:
1) Decide whether peer responses reveal material points you missed.
2) If yes, propose only incremental updates to your prior conclusions.
3) If no, keep unchanged.
4) Keep your output short.

Output format (strict plain text):
CHANGED: YES or NO
REVISION_NOTES:
- short bullet
- short bullet
OPTIONAL_UPDATES:
- quality: <value or unchanged>
- value: <value or unchanged>
- target_12m: <value/range or unchanged>
- target_24m: <value/range or unchanged>

Rules:
- Max 10 bullets total.
- If no material change: CHANGED: NO and one short note.
- Prefer concrete evidence references to peer responses.
- Do not restate your full report.
"""


async def stage2_collect_revision_deltas(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str],
    revision_models: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Stage 2.5 (WIP): ask each model to self-revise after peer review.

    If delta JSON is malformed/invalid, the caller should keep the previous Stage 1
    output unchanged.
    """
    labels = [chr(65 + i) for i in range(len(stage1_results))]
    model_to_label = {
        result.get("model", ""): f"Response {label}"
        for label, result in zip(labels, stage1_results)
    }
    def _clip(text: str, limit: int = 1800) -> str:
        payload = str(text or "").strip()
        if len(payload) <= limit:
            return payload
        return payload[:limit].rstrip() + "\n...[TRUNCATED FOR REVISION PASS]"

    def _build_responses_text(*, clip_limit: int, max_responses: Optional[int] = None) -> str:
        pairs = list(zip(labels, stage1_results))
        if isinstance(max_responses, int) and max_responses > 0:
            pairs = pairs[:max_responses]
        return "\n\n".join(
            f"Response {label}:\n{_clip(result.get('response', ''), limit=clip_limit)}"
            for label, result in pairs
        )

    responses_text = _build_responses_text(clip_limit=1800)
    compact_responses_text = _build_responses_text(clip_limit=900, max_responses=5)
    aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)
    targets = [m for m in (revision_models or [r.get("model") for r in stage1_results]) if m]
    timeout = float(STAGE2_REVISION_PASS_TIMEOUT_SECONDS)
    max_tokens = int(STAGE2_REVISION_PASS_MAX_OUTPUT_TOKENS)

    async def _run_one(model: str) -> Dict[str, Any]:
        own_label = model_to_label.get(model, "")
        prompt = _build_stage2_revision_prompt(
            enhanced_context=enhanced_context,
            own_label=own_label,
            responses_text=responses_text,
            aggregate_rankings=aggregate,
        )
        attempts = 0
        raw_text = ""
        used_compact_retry = False
        for prompt_text in (
            prompt,
            _build_stage2_revision_prompt(
                enhanced_context=enhanced_context,
                own_label=own_label,
                responses_text=compact_responses_text,
                aggregate_rankings=aggregate,
            ),
        ):
            attempts += 1
            response = await query_model(
                model,
                [{"role": "user", "content": prompt_text}],
                timeout=timeout,
                max_tokens=max_tokens,
            )
            raw_text = (response or {}).get("content", "") if response else ""
            if str(raw_text or "").strip():
                break
        used_compact_retry = attempts > 1
        parsed, parse_error = _parse_json_object_from_text(raw_text)
        normalized = None
        normalize_error = None
        if parsed is not None:
            normalized, normalize_error = _normalize_stage2_revision_delta(parsed)
        # Fallback: accept non-empty plain-text revision notes even when JSON fails.
        if normalized is None and str(raw_text or "").strip():
            fallback_changed = _extract_changed_flag_from_text(raw_text)
            fallback_notes = _extract_revision_notes_from_text(raw_text)
            normalized = {
                "changed": bool(fallback_changed) if fallback_changed is not None else bool(fallback_notes),
                "reason": "",
                "revision_notes": fallback_notes,
                "changes": [],
                "updated_scores": {},
                "updated_price_targets": {},
                "updated_observations": [],
                "evidence_refs": [],
                "confidence": None,
            }
            normalize_error = parse_error or "non_json_fallback_used"
            parse_error = None
        accepted = normalized is not None
        return {
            "model": model,
            "own_label": own_label,
            "prompt_chars": len(prompt),
            "response_chars": len(raw_text or ""),
            "accepted": accepted,
            "changed": bool((normalized or {}).get("changed")) if accepted else False,
            "delta_json": normalized,
            "parse_error": None if accepted else (parse_error or normalize_error),
            "decode_warning": normalize_error if accepted else None,
            "raw_response": raw_text,
            "attempts": attempts,
            "compact_retry_used": used_compact_retry,
        }

    _progress_log(
        "Stage2.5 revision pass start: "
        f"models={targets}, timeout={timeout:.1f}s, max_output_tokens={max_tokens}"
    )
    tasks = [_run_one(model) for model in targets]
    results = await asyncio.gather(*tasks)
    accepted = sum(1 for row in results if row.get("accepted"))
    changed = sum(1 for row in results if row.get("accepted") and row.get("changed"))
    unchanged_count = int(
        sum(
            1
            for row in results
            if row.get("accepted") and (not row.get("changed"))
        )
    )
    empty_response_count = int(
        sum(
            1
            for row in results
            if (not row.get("accepted")) and (row.get("parse_error") == "empty_response")
        )
    )
    parse_failed_count = int(
        sum(
            1
            for row in results
            if (not row.get("accepted")) and (row.get("parse_error") not in {None, "empty_response"})
        )
    )
    summary = {
        "enabled": True,
        "models_attempted": list(targets),
        "models_succeeded": [row.get("model") for row in results if row.get("raw_response")],
        "accepted_count": int(accepted),
        "changed_count": int(changed),
        "no_amendment_count": unchanged_count,
        "empty_response_count": empty_response_count,
        "parse_failed_count": parse_failed_count,
    }
    _progress_log(
        "Stage2.5 revision pass done: "
        f"accepted={accepted}/{len(targets)}, changed={changed}"
    )
    return results, summary


def apply_stage2_revision_deltas(
    stage1_results: List[Dict[str, Any]],
    revision_results: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Apply accepted Stage 2.5 deltas to Stage 1 responses.

    Safe behavior: if revision payload is not accepted, keep original Stage 1 response.
    """
    accepted_by_model: Dict[str, Dict[str, Any]] = {}
    for row in revision_results or []:
        if not row.get("accepted"):
            continue
        model = str(row.get("model") or "").strip()
        if not model:
            continue
        accepted_by_model[model] = row

    updated: List[Dict[str, Any]] = []
    changed_models: List[str] = []
    notes_applied_models: List[str] = []
    for item in stage1_results or []:
        row = dict(item or {})
        model = str(row.get("model") or "").strip()
        accepted = accepted_by_model.get(model)
        if not accepted:
            updated.append(row)
            continue
        delta = accepted.get("delta_json") or {}
        raw_response = str(accepted.get("raw_response") or "").strip()
        if not raw_response:
            updated.append(row)
            continue
        block = (
            "\n\n[STAGE2_REVISION_NOTES]\n"
            f"{raw_response}\n"
        )
        row["response"] = f"{str(row.get('response') or '').rstrip()}{block}"
        notes_applied_models.append(model)
        if bool((delta or {}).get("changed")):
            changed_models.append(model)
        updated.append(row)

    summary = {
        "models_total": len(stage1_results or []),
        "revisions_received": len(revision_results or []),
        "revisions_applied": len(notes_applied_models),
        "revision_notes_applied_models": notes_applied_models,
        "models_changed": changed_models,
        "models_unchanged_due_to_empty_response": [
            str(row.get("model") or "")
            for row in (revision_results or [])
            if (not row.get("accepted")) and (row.get("parse_error") == "empty_response")
        ],
        "models_unchanged_due_to_parse_or_validation": [
            str(row.get("model") or "")
            for row in (revision_results or [])
            if (not row.get("accepted")) and (row.get("parse_error") != "empty_response")
        ],
    }
    return updated, summary


def _clip_for_reconciliation(text: Any, limit: int, marker: str) -> str:
    payload = str(text or "").strip()
    if limit <= 0 or len(payload) <= limit:
        return payload
    head = max(1000, int(limit * 0.62))
    tail = max(1000, limit - head)
    if head + tail >= len(payload):
        return payload
    return (
        payload[:head].rstrip()
        + f"\n\n[{marker}: {len(payload) - head - tail} chars omitted]\n\n"
        + payload[-tail:].lstrip()
    )


def _normalize_reconciliation_issue(item: Any) -> Optional[Dict[str, Any]]:
    if isinstance(item, str):
        issue = item.strip()
        if not issue:
            return None
        return {
            "topic": "",
            "issue": issue,
            "source_resolved_position": "",
            "prefer_models": [],
            "downweight_models": [],
            "affected_claims": [],
            "stage3_instruction": issue,
            "confidence": None,
        }
    if not isinstance(item, dict):
        return None

    def _string_list(value: Any, max_items: int = 8) -> List[str]:
        if isinstance(value, str):
            return [value.strip()] if value.strip() else []
        if not isinstance(value, list):
            return []
        return [str(part).strip() for part in value[:max_items] if str(part).strip()]

    confidence = _coerce_float(item.get("confidence"))
    if confidence is not None:
        if confidence > 1.0 and confidence <= 100.0:
            confidence = confidence / 100.0
        if confidence < 0.0 or confidence > 1.0:
            confidence = None

    topic = str(item.get("topic") or "").strip()
    issue = str(item.get("issue") or item.get("finding") or "").strip()
    instruction = str(item.get("stage3_instruction") or item.get("instruction") or "").strip()
    if not issue and not instruction:
        return None
    return {
        "topic": topic,
        "issue": issue or instruction,
        "source_resolved_position": str(item.get("source_resolved_position") or "").strip(),
        "prefer_models": _string_list(item.get("prefer_models")),
        "downweight_models": _string_list(item.get("downweight_models")),
        "affected_claims": _string_list(item.get("affected_claims"), max_items=12),
        "stage3_instruction": instruction or issue,
        "confidence": round(confidence, 4) if confidence is not None else None,
    }


def _normalize_stage2_reconciliation_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    allowed_status = {
        "issues_found",
        "no_material_issues",
        "insufficient_source_context",
    }
    status = str(raw.get("status") or "").strip().lower()
    if status not in allowed_status:
        has_issues = any(raw.get(key) for key in ("blocking", "material", "unresolved", "topic_overrides"))
        status = "issues_found" if has_issues else "no_material_issues"

    def _issue_list(key: str, max_items: int = 10) -> List[Dict[str, Any]]:
        values = raw.get(key) if isinstance(raw.get(key), list) else []
        normalized: List[Dict[str, Any]] = []
        for value in values[:max_items]:
            row = _normalize_reconciliation_issue(value)
            if row:
                normalized.append(row)
        return normalized

    def _string_list(key: str, max_items: int = 12) -> List[str]:
        values = raw.get(key)
        if isinstance(values, str):
            return [values.strip()] if values.strip() else []
        if not isinstance(values, list):
            return []
        return [str(value).strip() for value in values[:max_items] if str(value).strip()]

    topic_overrides: List[Dict[str, Any]] = []
    values = raw.get("topic_overrides") if isinstance(raw.get("topic_overrides"), list) else []
    for value in values[:10]:
        if not isinstance(value, dict):
            continue
        issue = _normalize_reconciliation_issue(value)
        if not issue:
            continue
        topic_overrides.append(issue)

    normalized = {
        "status": status,
        "blocking": _issue_list("blocking", max_items=8),
        "material": _issue_list("material", max_items=12),
        "minor": _issue_list("minor", max_items=8),
        "unresolved": _issue_list("unresolved", max_items=10),
        "topic_overrides": topic_overrides,
        "stage3_constraints": _string_list("stage3_constraints", max_items=14),
        "summary": str(raw.get("summary") or "").strip(),
    }
    if (
        status == "no_material_issues"
        and (
            normalized["blocking"]
            or normalized["material"]
            or normalized["unresolved"]
            or normalized["topic_overrides"]
        )
    ):
        normalized["status"] = "issues_found"
    return normalized


def _build_stage2_reconciliation_prompt(
    *,
    source_context: str,
    responses_text: str,
    rankings_text: str,
) -> str:
    return f"""You are a lightweight discrepancy reviewer for an investment-analysis council.

You are NOT writing the investment memo. You are NOT re-running research. You are checking whether Stage 3 should trust, distrust, or qualify specific council claims.

INPUT A - PRIMARY/PREPASS CONTEXT
This may contain filings, attachment excerpts, deterministic market facts, injection bundles, and source summaries:
{source_context}

INPUT B - STAGE 1 COUNCIL RESPONSES
{responses_text}

INPUT C - STAGE 2 PEER RANKINGS
{rankings_text}

TASK
Run one compact pass across all inputs and identify:
1. Claims in Stage 1 that conflict with the primary/prepass context.
2. Stale assumptions, especially production, financing, hedging, reserves/resources, commodity exposure, or project-stage baselines that look superseded by dated source material.
3. Cases where a model says "unknown", "not disclosed", or "data gap" but the primary/prepass context appears to contain the answer.
4. Material disagreements between Stage 1 models that Stage 3 must explicitly adjudicate.
5. Topic-specific overrides where a lower-ranked response appears better aligned with source evidence than a higher-ranked response.

Rules:
- Do not introduce external facts not present in the inputs.
- Do not perform valuation or write a replacement memo.
- Prefer primary/prepass context over peer ranking when they conflict.
- Preserve uncertainty. If the source context is too thin, say so.
- Be strict: only list issues that could materially change the final synthesis or prevent a misleading memo.

Return JSON only with this schema:
{{
  "status": "issues_found | no_material_issues | insufficient_source_context",
  "blocking": [
    {{
      "topic": "short topic",
      "issue": "what is wrong or contradictory",
      "source_resolved_position": "what the primary/prepass context supports, if clear",
      "prefer_models": ["model names whose claim is more evidence-aligned"],
      "downweight_models": ["model names whose claim is contradicted or stale"],
      "affected_claims": ["short quoted/paraphrased claims"],
      "stage3_instruction": "specific instruction for the chairman",
      "confidence": 0.0
    }}
  ],
  "material": [],
  "minor": [],
  "unresolved": [
    {{
      "topic": "short topic",
      "issue": "what remains unresolved",
      "source_resolved_position": "",
      "prefer_models": [],
      "downweight_models": [],
      "affected_claims": [],
      "stage3_instruction": "how Stage 3 should qualify it",
      "confidence": 0.0
    }}
  ],
  "topic_overrides": [
    {{
      "topic": "short topic",
      "issue": "why ranking should be overridden for this topic",
      "source_resolved_position": "evidence-aligned position",
      "prefer_models": ["lower-ranked but better-supported models"],
      "downweight_models": ["higher-ranked but contradicted models"],
      "affected_claims": [],
      "stage3_instruction": "topic-specific synthesis rule",
      "confidence": 0.0
    }}
  ],
  "stage3_constraints": [
    "hard synthesis constraint the chairman must follow"
  ],
  "summary": "one-paragraph summary"
}}
"""


async def stage2_collect_reconciliation(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str],
    reconciliation_model: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """Single cheap discrepancy pass before Stage 3 synthesis."""
    should_run = bool(STAGE2_RECONCILIATION_ENABLED) if enabled is None else bool(enabled)
    if not should_run:
        return {"enabled": False, "accepted": False, "status": "disabled"}
    if not stage1_results:
        return {"enabled": True, "accepted": False, "status": "no_stage1_results"}

    aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)
    rank_by_model = {
        str(row.get("model") or ""): row
        for row in aggregate
        if row.get("model")
    }
    labels = [chr(65 + i) for i in range(len(stage1_results or []))]
    model_to_label = {
        str(result.get("model") or ""): f"Response {label}"
        for label, result in zip(labels, stage1_results or [])
    }
    result_by_model = {
        str(result.get("model") or ""): result
        for result in stage1_results or []
        if result.get("model")
    }

    ordered_models = [row["model"] for row in aggregate if row.get("model") in result_by_model]
    for result in stage1_results or []:
        model = str(result.get("model") or "").strip()
        if model and model not in ordered_models:
            ordered_models.append(model)
    top_n = int(STAGE2_RECONCILIATION_TOP_N)
    if top_n > 0:
        ordered_models = ordered_models[:top_n]

    max_response_chars = int(STAGE2_RECONCILIATION_MAX_RESPONSE_CHARS)
    response_blocks: List[str] = []
    for model in ordered_models:
        result = result_by_model.get(model) or {}
        rank = rank_by_model.get(model) or {}
        rank_text = (
            f"average_rank={rank.get('average_rank')} "
            f"rankings_count={rank.get('rankings_count')}"
            if rank
            else "not_ranked"
        )
        response_blocks.append(
            f"{model_to_label.get(model, '')} | model={model} | {rank_text}\n"
            f"{_clip_for_reconciliation(result.get('response', ''), max_response_chars, 'TRUNCATED RESPONSE')}"
        )

    ranking_lines = ["Aggregate peer ranking:"]
    if aggregate:
        for i, item in enumerate(aggregate, start=1):
            ranking_lines.append(
                f"{i}. {item.get('model')} "
                f"(avg_rank={item.get('average_rank')}, votes={item.get('rankings_count')})"
            )
    else:
        ranking_lines.append("(no parseable aggregate ranking)")

    source_context = _clip_for_reconciliation(
        enhanced_context,
        int(STAGE2_RECONCILIATION_MAX_SOURCE_CHARS),
        "TRUNCATED SOURCE/PREPASS CONTEXT",
    )
    prompt = _build_stage2_reconciliation_prompt(
        source_context=source_context,
        responses_text="\n\n---\n\n".join(response_blocks),
        rankings_text="\n".join(ranking_lines),
    )

    selected_model = (reconciliation_model or STAGE2_RECONCILIATION_MODEL or CHAIRMAN_MODEL).strip()
    timeout = float(STAGE2_RECONCILIATION_TIMEOUT_SECONDS)
    max_tokens = int(STAGE2_RECONCILIATION_MAX_OUTPUT_TOKENS)
    _progress_log(
        "Stage2.5 reconciliation start: "
        f"model={selected_model}, responses={len(ordered_models)}, "
        f"prompt_chars={len(prompt)}, timeout={timeout:.1f}s"
    )
    response = await query_model(
        selected_model,
        [{"role": "user", "content": prompt}],
        timeout=timeout,
        max_tokens=max_tokens if max_tokens > 0 else None,
    )
    raw_text = (response or {}).get("content", "") if response else ""
    parsed, parse_error = _parse_json_object_from_text(raw_text)
    if not parsed:
        _progress_log(
            "Stage2.5 reconciliation failed: "
            f"model={selected_model}, parse_error={parse_error}"
        )
        return {
            "enabled": True,
            "accepted": False,
            "status": "parse_failed" if raw_text else "model_failed",
            "model": selected_model,
            "selected_models": ordered_models,
            "prompt_chars": len(prompt),
            "response_chars": len(raw_text or ""),
            "parse_error": parse_error or "empty_response",
            "raw_response": raw_text,
        }

    normalized = _normalize_stage2_reconciliation_payload(parsed)
    issue_count = sum(
        len(normalized.get(key) or [])
        for key in ("blocking", "material", "minor", "unresolved", "topic_overrides")
    )
    out = {
        "enabled": True,
        "accepted": True,
        "model": selected_model,
        "selected_models": ordered_models,
        "prompt_chars": len(prompt),
        "response_chars": len(raw_text or ""),
        "issue_count": int(issue_count),
        **normalized,
    }
    _progress_log(
        "Stage2.5 reconciliation done: "
        f"status={out.get('status')}, issues={issue_count}"
    )
    return out


async def stage3_synthesize_final(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str] = None,
    use_structured_analysis: bool = False,
    template_id: str = None,
    ticker: str = None,
    company_name: str = None,
    exchange: str = None,
    chairman_model: Optional[str] = None,
    market_facts: Optional[Dict[str, Any]] = None,
    evidence_pack: Optional[Dict[str, Any]] = None,
    stage2_reconciliation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Stage 3: Chairman synthesizes final response.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content
        stage1_results: Individual model responses from Stage 1
        stage2_results: Rankings from Stage 2
        label_to_model: Mapping from labels to models (for weighted synthesis)
        use_structured_analysis: If True, use investment analysis rubric
        template_id: Template ID to use for synthesis
        ticker: Stock ticker for structured analysis
        company_name: Optional explicit company name
        exchange: Optional exchange id/name
        chairman_model: Optional chairman model override for this run
        evidence_pack: Optional normalized evidence pack (claim ledger + deterministic lane)

    Returns:
        Dict with 'model' and 'response' keys (and 'structured_data' if applicable)
    """
    # Check if we should use structured investment analysis
    selected_chairman_model = chairman_model or CHAIRMAN_MODEL

    if use_structured_analysis and template_id and label_to_model:
        from .investment_synthesis import synthesize_structured_analysis
        return await synthesize_structured_analysis(
            enhanced_context,
            stage1_results,
            stage2_results,
            label_to_model,
            template_id,
            ticker,
            company_name=company_name,
            exchange=exchange,
            chairman_model=selected_chairman_model,
            market_facts=market_facts,
            evidence_pack=evidence_pack,
            stage2_reconciliation=stage2_reconciliation,
        )

    # Otherwise use standard synthesis
    # Build comprehensive context for chairman
    stage1_text = "\n\n".join([
        f"Model: {result['model']}\nResponse: {result['response']}"
        for result in stage1_results
    ])

    stage2_text = "\n\n".join([
        f"Model: {result['model']}\nRanking: {result['ranking']}"
        for result in stage2_results
    ])
    reconciliation_text = ""
    if isinstance(stage2_reconciliation, dict) and stage2_reconciliation.get("accepted"):
        reconciliation_text = (
            "\n\nSTAGE 2.5 - Discrepancy Review:\n"
            f"{json.dumps(stage2_reconciliation, indent=2)[:8000]}\n\n"
            "Instruction: peer rankings are useful, but source-evidence contradictions "
            "and topic-specific overrides in this review must take precedence."
        )

    chairman_prompt = f"""You are the Chairman of an LLM Council. Multiple AI models have provided responses to a user's question, and then ranked each other's responses.

Original Question (with context):
{enhanced_context}

STAGE 1 - Individual Responses:
{stage1_text}

STAGE 2 - Peer Rankings:
{stage2_text}
{reconciliation_text}

Your task as Chairman is to synthesize all of this information into a single, comprehensive, accurate answer to the user's original question. Consider:
- The individual responses and their insights
- The peer rankings and what they reveal about response quality
- Any Stage 2.5 discrepancy review constraints, which can override peer ranking on specific evidence conflicts
- Any patterns of agreement or disagreement

Provide a clear, well-reasoned final answer that represents the council's collective wisdom:"""

    messages = [{"role": "user", "content": chairman_prompt}]

    # Query the chairman model
    response = await query_model(selected_chairman_model, messages)

    if response is None:
        # Fallback if chairman fails
        return {
            "model": selected_chairman_model,
            "response": "Error: Unable to generate final synthesis.",
            "stage2_reconciliation": stage2_reconciliation,
        }

    return {
        "model": selected_chairman_model,
        "response": response.get('content', ''),
        "stage2_reconciliation": stage2_reconciliation,
    }


def parse_ranking_from_text(ranking_text: str) -> List[str]:
    """
    Parse the FINAL RANKING section from the model's response.

    Args:
        ranking_text: The full text response from the model

    Returns:
        List of response labels in ranked order
    """
    import re

    # Look for "FINAL RANKING:" section
    if "FINAL RANKING:" in ranking_text:
        # Extract everything after "FINAL RANKING:"
        parts = ranking_text.split("FINAL RANKING:")
        if len(parts) >= 2:
            ranking_section = parts[1]
            # Try to extract numbered list format (e.g., "1. Response A")
            # This pattern looks for: number, period, optional space, "Response X"
            numbered_matches = re.findall(r'\d+\.\s*Response [A-Z]', ranking_section)
            if numbered_matches:
                # Extract just the "Response X" part
                return [re.search(r'Response [A-Z]', m).group() for m in numbered_matches]

            # Fallback: Extract all "Response X" patterns in order
            matches = re.findall(r'Response [A-Z]', ranking_section)
            return matches

    # Fallback: try to find any "Response X" patterns in order
    matches = re.findall(r'Response [A-Z]', ranking_text)
    return matches


def calculate_aggregate_rankings(
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Calculate aggregate rankings across all models.

    Args:
        stage2_results: Rankings from each model
        label_to_model: Mapping from anonymous labels to model names

    Returns:
        List of dicts with model name and average rank, sorted best to worst
    """
    from collections import defaultdict

    # Track positions for each model
    model_positions = defaultdict(list)
    first_place_votes = defaultdict(int)
    borda_scores = defaultdict(int)

    for ranking in stage2_results:
        parsed_ranking = _ranking_labels_from_result(ranking)
        total = len(parsed_ranking)

        for position, label in enumerate(parsed_ranking, start=1):
            if label in label_to_model:
                model_name = label_to_model[label]
                model_positions[model_name].append(position)
                borda_scores[model_name] += (total - position + 1)
                if position == 1:
                    first_place_votes[model_name] += 1

    # Calculate average position for each model
    aggregate = []
    for model, positions in model_positions.items():
        if positions:
            avg_rank = sum(positions) / len(positions)
            aggregate.append({
                "model": model,
                "average_rank": round(avg_rank, 2),
                "rankings_count": len(positions),
                "first_place_votes": int(first_place_votes.get(model, 0)),
                "borda_score": int(borda_scores.get(model, 0)),
            })

    # Sort by average rank (lower is better)
    aggregate.sort(key=lambda x: (x['average_rank'], -x.get('first_place_votes', 0), -x.get('borda_score', 0)))

    return aggregate


async def generate_conversation_title(user_query: str) -> str:
    """
    Generate a short title for a conversation based on the first user message.

    Args:
        user_query: The first user message

    Returns:
        A short title (3-5 words)
    """
    title_prompt = f"""Generate a very short title (3-5 words maximum) that summarizes the following question.
The title should be concise and descriptive. Do not use quotes or punctuation in the title.

Question: {user_query}

Title:"""

    messages = [{"role": "user", "content": title_prompt}]

    # Use gemini-2.5-flash for title generation (fast and cheap)
    response = await query_model("google/gemini-2.5-flash", messages, timeout=30.0)

    if response is None:
        # Fallback to a generic title
        return "New Conversation"

    title = response.get('content', 'New Conversation').strip()

    # Clean up the title - remove quotes, limit length
    title = title.strip('"\'')

    # Truncate if too long
    if len(title) > 50:
        title = title[:47] + "..."

    return title


async def run_full_council(
    enhanced_context: str,
    use_structured_analysis: bool = False,
    template_id: str = None,
    ticker: str = None,
    company_name: str = None,
    exchange: str = None,
) -> Tuple[List, List, Dict, Dict]:
    """
    Run the complete 3-stage council process.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content
        use_structured_analysis: If True, use analysis template with structured output
        template_id: Template ID to use for synthesis
        ticker: Stock ticker for structured analysis
        company_name: Optional explicit company name
        exchange: Optional exchange id/name

    Returns:
        Tuple of (stage1_results, stage2_results, stage3_result, metadata)
    """
    _ensure_system_enabled(diagnostic_mode=False)
    # Stage 1: Collect individual responses
    stage1_results = await stage1_collect_responses(enhanced_context)

    # If no models responded successfully, return error
    if not stage1_results:
        return [], [], {
            "model": "error",
            "response": "All models failed to respond. Please try again."
        }, {}

    # Stage 2: Collect rankings
    stage2_results, label_to_model = await stage2_collect_rankings(enhanced_context, stage1_results)

    # Calculate aggregate rankings
    aggregate_rankings = calculate_aggregate_rankings(stage2_results, label_to_model)

    stage1_results_for_stage3 = stage1_results
    stage2_revision_results: List[Dict[str, Any]] = []
    stage2_revision_summary: Dict[str, Any] = {"enabled": False}
    if STAGE2_REVISION_PASS_ENABLED:
        stage2_revision_results, stage2_revision_summary = await stage2_collect_revision_deltas(
            enhanced_context,
            stage1_results,
            stage2_results,
            label_to_model,
            revision_models=[item.get("model") for item in stage1_results if item.get("model")],
        )
        stage1_results_for_stage3, apply_summary = apply_stage2_revision_deltas(
            stage1_results,
            stage2_revision_results,
        )
        stage2_revision_summary["apply"] = apply_summary

    stage2_reconciliation = await stage2_collect_reconciliation(
        enhanced_context,
        stage1_results_for_stage3,
        stage2_results,
        label_to_model,
    )

    # Stage 3: Synthesize final answer (with optional structured analysis)
    stage3_result = await stage3_synthesize_final(
        enhanced_context,
        stage1_results_for_stage3,
        stage2_results,
        label_to_model=label_to_model,
        use_structured_analysis=use_structured_analysis,
        template_id=template_id,
        ticker=ticker,
        company_name=company_name,
        exchange=exchange,
        evidence_pack=None,
        stage2_reconciliation=stage2_reconciliation,
    )

    # Prepare metadata
    metadata = {
        "label_to_model": label_to_model,
        "aggregate_rankings": aggregate_rankings,
        "stage2_revision_pass_enabled": bool(STAGE2_REVISION_PASS_ENABLED),
        "stage2_revision_summary": stage2_revision_summary,
        "stage2_revision_results": stage2_revision_results,
        "stage2_reconciliation": stage2_reconciliation,
    }

    return stage1_results_for_stage3, stage2_results, stage3_result, metadata
