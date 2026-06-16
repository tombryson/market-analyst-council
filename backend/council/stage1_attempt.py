"""Stage-1 attempt profiles, retry logic, compliance checks, and shared helpers.

`_progress_log` and `_ensure_system_enabled` live here because they are the
lowest-level utilities referenced by every other council sub-module.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from ..config import (
    MAX_SOURCES,
    PERPLEXITY_PRESET_ADVANCED,
    PERPLEXITY_PRESET_DEEP,
    PERPLEXITY_PRESET_STRATEGY,
    PERPLEXITY_STAGE1_OPENAI_BASE_DOWNGRADE_HIGH_REASONING,
    PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED,
    PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES,
    PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS,
    PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_RETRIEVAL_MAX_SOURCES,
    PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED,
    PROGRESS_LOGGING,
    SYSTEM_ALLOW_DIAGNOSTICS_WHEN_DISABLED,
    SYSTEM_ENABLED,
    SYSTEM_SHUTDOWN_REASON,
)

logger = logging.getLogger(__name__)

def _progress_log(message: str) -> None:
    """Debug-level progress logs for long-running research orchestration.

    Gated by PROGRESS_LOGGING so they don't appear in normal production output.
    The logging framework handles timestamps and levels — no need to embed them.
    """
    if not PROGRESS_LOGGING:
        return
    logger.debug("[council] %s", message)


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
    """Return True when the OpenAI GPT-5.x research model should default to low reasoning."""
    key = str(model or "").strip().lower()
    return (
        key in {"openai/gpt-5.4", "gpt-5.4", "openai/gpt-5.5", "gpt-5.5"}
        or key.endswith("/gpt-5.4")
        or key.endswith("/gpt-5.5")
    )


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

