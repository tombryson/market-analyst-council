#!/usr/bin/env python3
"""Async portfolio positioning memo pipeline.

This is a separate portfolio-level workflow from the company/ticker council run.
It gathers cheap web-grounded research, compresses the evidence, and then asks a
stronger final model for an asset-class positioning memo.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from backend.openrouter import query_model
from backend.research.providers.perplexity import PerplexityResearchProvider
from backend.research.providers.tavily import TavilyResearchProvider

DEFAULT_SUMMARY_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_SUMMARY_MODEL",
    "x-ai/grok-4.1-fast",
).strip()
DEFAULT_DEEP_SUMMARY_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_DEEP_SUMMARY_MODEL",
    "anthropic/claude-sonnet-4",
).strip()
DEFAULT_CHAIRMAN_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_CHAIRMAN_MODEL",
    "google/gemini-3.1-pro-preview",
).strip()
DEFAULT_DEEP_CHAIRMAN_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_DEEP_CHAIRMAN_MODEL",
    "anthropic/claude-sonnet-4",
).strip()
DEFAULT_COMMENTARY_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_COMMENTARY_MODEL",
    DEFAULT_CHAIRMAN_MODEL,
).strip()
DEFAULT_DEEP_COMMENTARY_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_DEEP_COMMENTARY_MODEL",
    DEFAULT_DEEP_CHAIRMAN_MODEL,
).strip()
XAI_API_KEY = os.getenv("XAI_API_KEY")
XAI_API_URL = os.getenv("XAI_API_URL", "https://api.x.ai/v1/responses").strip()
DEFAULT_MACRO_NEWS_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_MACRO_NEWS_MODEL",
    os.getenv("STAGE1_SUPPLEMENTARY_XAI_MODEL", "x-ai/grok-4.1-fast"),
).strip()
DEFAULT_MACRO_NEWS_TIMEOUT_SECONDS = float(
    os.getenv("PORTFOLIO_POSITIONING_MACRO_NEWS_TIMEOUT_SECONDS", os.getenv("STAGE1_SUPPLEMENTARY_XAI_TIMEOUT_SECONDS", "90"))
    or 90
)
DEFAULT_MACRO_NEWS_MAX_TOKENS = int(
    os.getenv("PORTFOLIO_POSITIONING_MACRO_NEWS_MAX_TOKENS", os.getenv("STAGE1_SUPPLEMENTARY_XAI_MAX_TOKENS", "900"))
    or 900
)
DEFAULT_MACRO_NEWS_TEMPERATURE = float(
    os.getenv("PORTFOLIO_POSITIONING_MACRO_NEWS_TEMPERATURE", os.getenv("STAGE1_SUPPLEMENTARY_XAI_TEMPERATURE", "0.15"))
    or 0.15
)
DEFAULT_MACRO_NEWS_MAX_TOOL_ITERATIONS = int(
    os.getenv(
        "PORTFOLIO_POSITIONING_MACRO_NEWS_MAX_TOOL_ITERATIONS",
        os.getenv("STAGE1_SUPPLEMENTARY_XAI_MAX_TOOL_ITERATIONS", "2"),
    )
    or 2
)
DEFAULT_MAX_SOURCES_FAST = int(os.getenv("PORTFOLIO_POSITIONING_MAX_SOURCES_FAST", "8") or 8)
DEFAULT_MAX_SOURCES_DEEP = int(os.getenv("PORTFOLIO_POSITIONING_MAX_SOURCES_DEEP", "12") or 12)

QUADRANT_DEFINITIONS: Dict[str, str] = {
    "Q1": "Goldilocks / disinflationary bull / normal risk-on",
    "Q2": "overheating / inflation-up while growth still holds",
    "Q3": "stagflation / growth-down + inflation-up / oil shock risk",
    "Q4": "deflation / growth-down + inflation-down",
}


def _parse_model_list(value: str) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


DEFAULT_FAST_ALLOCATOR_COUNCIL_MODELS = _parse_model_list(
    os.getenv(
        "PORTFOLIO_POSITIONING_FAST_ALLOCATOR_COUNCIL_MODELS",
        ",".join(
            [
                "x-ai/grok-4.1-fast",
                "google/gemini-3.1-pro-preview",
            ]
        ),
    )
)
DEFAULT_DEEP_ALLOCATOR_COUNCIL_MODELS = _parse_model_list(
    os.getenv(
        "PORTFOLIO_POSITIONING_DEEP_ALLOCATOR_COUNCIL_MODELS",
        ",".join(
            [
                "anthropic/claude-sonnet-4",
                "google/gemini-3.1-pro-preview",
                "x-ai/grok-4.1-fast",
            ]
        ),
    )
)


def _summary_model_for_mode(mode: str) -> str:
    return DEFAULT_DEEP_SUMMARY_MODEL if str(mode or "").strip().lower() == "deep" else DEFAULT_SUMMARY_MODEL


def _chairman_model_for_mode(mode: str) -> str:
    return DEFAULT_DEEP_CHAIRMAN_MODEL if str(mode or "").strip().lower() == "deep" else DEFAULT_CHAIRMAN_MODEL


def _commentary_model_for_mode(mode: str) -> str:
    return DEFAULT_DEEP_COMMENTARY_MODEL if str(mode or "").strip().lower() == "deep" else DEFAULT_COMMENTARY_MODEL


def _allocator_council_models_for_mode(mode: str) -> List[str]:
    models = DEFAULT_DEEP_ALLOCATOR_COUNCIL_MODELS if str(mode or "").strip().lower() == "deep" else DEFAULT_FAST_ALLOCATOR_COUNCIL_MODELS
    deduped: List[str] = []
    seen: set[str] = set()
    for item in models:
        key = str(item or "").strip()
        if key and key not in seen:
            seen.add(key)
            deduped.append(key)
    return deduped or [_chairman_model_for_mode(mode)]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _log(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}][portfolio_positioning] {message}", flush=True)


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    fence = re.search(r"```json\s*(\{.*?\})\s*```", raw, flags=re.IGNORECASE | re.DOTALL)
    if fence:
        try:
            parsed = json.loads(fence.group(1))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    brace = re.search(r"(\{.*\})", raw, flags=re.DOTALL)
    if brace:
        try:
            parsed = json.loads(brace.group(1))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)



def _clamp_pct(value: Any) -> float:
    return max(0.0, min(100.0, round(_safe_float(value), 2)))



def _read_context(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("portfolio context must be a JSON object")
    return payload



def _compact_snapshot(context: Dict[str, Any]) -> Dict[str, Any]:
    portfolio = context.get("portfolio") if isinstance(context.get("portfolio"), dict) else {}
    overlay = context.get("overlay") if isinstance(context.get("overlay"), dict) else {}
    raw_asset_classes = context.get("asset_classes") if isinstance(context.get("asset_classes"), list) else []
    raw_available_asset_classes = context.get("available_asset_classes") if isinstance(context.get("available_asset_classes"), list) else []
    raw_positions = context.get("positions") if isinstance(context.get("positions"), list) else []

    asset_classes: List[Dict[str, Any]] = []
    for item in raw_asset_classes:
        if not isinstance(item, dict):
            continue
        asset_classes.append(
            {
                "asset_class": str(item.get("asset_class") or "").strip(),
                "display_name": str(item.get("display_name") or item.get("asset_class") or "").strip(),
                "portfolio_pct": _clamp_pct(item.get("portfolio_pct") or item.get("actual_pct") or 0),
                "invested_pct": _clamp_pct(item.get("invested_pct") or 0),
                "tactical_cash_pct": _clamp_pct(item.get("tactical_cash_pct") or 0),
                "overlay_eligible": bool(item.get("overlay_eligible")),
                "q1_governed": bool(item.get("overlay_eligible")),
                "notes": str(item.get("notes") or "").strip(),
            }
        )
    asset_classes.sort(key=lambda row: row["portfolio_pct"], reverse=True)

    positions: List[Dict[str, Any]] = []
    for item in raw_positions[:80]:
        if not isinstance(item, dict):
            continue
        positions.append(
            {
                "ticker": str(item.get("ticker") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "asset_class": str(item.get("asset_class") or "UNASSIGNED").strip(),
                "value": round(_safe_float(item.get("value"), 0.0), 2),
                "cash": round(_safe_float(item.get("cash"), 0.0), 2),
                "portfolio_pct": _clamp_pct(item.get("portfolio_pct") or 0),
                "q1_governed": bool(item.get("q1_governed")),
            }
        )

    q1_governed_now = sum(row["portfolio_pct"] for row in asset_classes if row.get("q1_governed"))
    q1_exempt_now = sum(row["portfolio_pct"] for row in asset_classes if not row.get("q1_governed"))

    available_asset_classes: List[Dict[str, Any]] = []
    for item in raw_available_asset_classes:
        if isinstance(item, dict):
            asset_class = str(item.get("asset_class") or "").strip()
            display_name = str(item.get("display_name") or asset_class).strip()
        else:
            asset_class = str(item or "").strip()
            display_name = asset_class
        if not asset_class:
            continue
        available_asset_classes.append(
            {
                "asset_class": asset_class,
                "display_name": display_name,
            }
        )
    if not available_asset_classes:
        available_asset_classes = [
            {
                "asset_class": str(row.get("asset_class") or "").strip(),
                "display_name": str(row.get("display_name") or row.get("asset_class") or "").strip(),
            }
            for row in asset_classes
            if str(row.get("asset_class") or "").strip()
        ]

    return {
        "as_of": str(context.get("as_of") or _utc_now_iso()),
        "portfolio": {
            "total_value": round(_safe_float(portfolio.get("total_value"), 0.0), 2),
            "cash_pct": _clamp_pct(portfolio.get("cash_pct") or portfolio.get("cash_on_hand_pct") or 0),
            "cash_value": round(_safe_float(portfolio.get("cash_value") or portfolio.get("cash_on_hand") or 0.0), 2),
            "holdings_count": int(portfolio.get("holdings_count") or len(positions)),
        },
        "overlay": {
            "q1_exposure_pct": _clamp_pct(overlay.get("q1_exposure_pct") or overlay.get("effective_q1_pct") or 100),
            "last_applied_q1_exposure_pct": _clamp_pct(overlay.get("last_applied_q1_exposure_pct") or 100),
            "status": str(overlay.get("status") or "").strip(),
            "required_de_risk_pct": _clamp_pct(overlay.get("required_de_risk_pct") or 0),
            "required_de_risk_value": round(_safe_float(overlay.get("required_de_risk_value"), 0.0), 2),
            "available_headroom_pct": _clamp_pct(overlay.get("available_headroom_pct") or 0),
            "available_headroom_value": round(_safe_float(overlay.get("available_headroom_value"), 0.0), 2),
            "regime_cash_pct": _clamp_pct(overlay.get("regime_cash_pct") or overlay.get("portfolio_cash_bucket_pct") or 0),
            "regime_cash_value": round(_safe_float(overlay.get("regime_cash_value") or overlay.get("portfolio_cash_bucket_value"), 0.0), 2),
        },
        "q1_governed_now_pct": round(q1_governed_now, 2),
        "q1_exempt_now_pct": round(q1_exempt_now, 2),
        "available_asset_classes": available_asset_classes,
        "asset_classes": asset_classes,
        "positions": positions,
    }



def _normalize_asset_key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").strip().upper())


def _build_asset_class_vocabulary(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    rows: List[Dict[str, Any]] = []
    source_rows = snapshot.get("available_asset_classes") or snapshot.get("asset_classes") or []
    for item in source_rows:
        if not isinstance(item, dict):
            continue
        asset_class = str(item.get("asset_class") or "").strip()
        display_name = str(item.get("display_name") or asset_class).strip()
        key = _normalize_asset_key(asset_class or display_name)
        if not key or key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "asset_class": asset_class,
                "display_name": display_name,
                "q1_governed": bool(item.get("q1_governed")),
            }
        )
    return rows


GENERIC_ASSET_CLASS_CANDIDATES: Dict[str, List[str]] = {
    "DEVELOPEDEQUITIES": ["EQUITY"],
    "GLOBALEQUITIES": ["EQUITY"],
    "USEQUITIES": ["EQUITY"],
    "USLARGECAPEQUITIES": ["EQUITY"],
    "EMERGINGMARKEQUITIES": ["EQUITY"],
    "INTERNATIONALEQUITIES": ["EQUITY"],
    "BROADEQUITIES": ["EQUITY"],
    "EQUITIES": ["EQUITY"],
    "GOVERNMENTBONDS": ["BONDS"],
    "INVESTMENTGRADECREDIT": ["BONDS"],
    "SHORTDURATIONFIXEDINCOME": ["BONDS"],
    "INFLATIONLINKEDBONDS": ["BONDS"],
    "TREASURIES": ["BONDS"],
    "CASHANDSHORTTERMTREASURIES": ["BONDS", "MISC"],
    "CASHANDEQUIVALENTS": ["BONDS", "MISC"],
    "FIXEDINCOME": ["BONDS"],
    "PHARMACEUTICALS": ["PHARMA"],
    "BIOTECH": ["PHARMA", "HEALTHCARE"],
    "HEALTHCARE": ["HEALTHCARE", "PHARMA"],
    "CONSUMERSTAPLES": ["STAPLES"],
    "FOOD": ["STAPLES"],
    "BANKS": ["FINANCIALS"],
    "DEFENSE": ["DEFENCE"],
    "MILITARY": ["DEFENCE"],
    "OILPRODUCERS": ["ENERGY"],
    "COMMODITIES": ["MATERIALS", "ETF"],
    "MINERS": ["MATERIALS"],
    "MINING": ["MATERIALS"],
    "BATTERYMETALS": ["LITHIUM", "MATERIALS"],
    "RAREEARTHS": ["REE", "MATERIALS"],
    "CRITICALMINERALS": ["REE", "MATERIALS"],
    "IRONORE": ["IRON", "MATERIALS"],
    "PHYSICALGOLD": ["GOLD"],
    "PHYSICALSILVER": ["SILVER"],
    "TRENDGLOBALMACRO": ["ETF", "MISC"],
    "GLOBALMACRO": ["ETF", "MISC"],
    "ALTERNATIVES": ["ETF", "MISC"],
    "CTA": ["ETF", "MISC"],
}


def _build_allowed_asset_class_index(asset_class_vocabulary: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for row in asset_class_vocabulary:
        if not isinstance(row, dict):
            continue
        for field in ("asset_class", "display_name"):
            key = _normalize_asset_key(row.get(field))
            if key and key not in index:
                index[key] = row
    return index


def _resolve_allowed_asset_class(
    raw_value: Any,
    *,
    allowed_index: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    key = _normalize_asset_key(raw_value)
    if not key:
        return None
    direct = allowed_index.get(key)
    if direct:
        return direct
    candidates = GENERIC_ASSET_CLASS_CANDIDATES.get(key, [])
    for candidate in candidates:
        resolved = allowed_index.get(_normalize_asset_key(candidate))
        if resolved:
            return resolved
    for alias, alias_candidates in GENERIC_ASSET_CLASS_CANDIDATES.items():
        if alias in key or key in alias:
            for candidate in alias_candidates:
                resolved = allowed_index.get(_normalize_asset_key(candidate))
                if resolved:
                    return resolved
    for allowed_key, row in allowed_index.items():
        if allowed_key and (allowed_key in key or key in allowed_key):
            return row
    return None


def _normalize_macro_positioning_taxonomy(
    macro_positioning: Dict[str, Any],
    *,
    asset_class_vocabulary: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not asset_class_vocabulary:
        return macro_positioning

    allowed_index = _build_allowed_asset_class_index(asset_class_vocabulary)
    normalized = dict(macro_positioning)
    dropped_labels: List[str] = []

    def _merge_target_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            resolved = _resolve_allowed_asset_class(row.get("asset_class") or row.get("display_name"), allowed_index=allowed_index)
            if not resolved:
                label = str(row.get("display_name") or row.get("asset_class") or "").strip()
                if label:
                    dropped_labels.append(label)
                continue
            asset_class = str(resolved.get("asset_class") or "").strip()
            if not asset_class:
                continue
            current = merged.get(asset_class)
            if current is None:
                merged[asset_class] = {
                    **row,
                    "asset_class": asset_class,
                    "display_name": str(resolved.get("display_name") or asset_class).strip(),
                    "min_pct": _clamp_pct(row.get("min_pct")),
                    "target_pct": _clamp_pct(row.get("target_pct")),
                    "max_pct": _clamp_pct(row.get("max_pct")),
                    "rationale": str(row.get("rationale") or "").strip(),
                }
                continue
            current["min_pct"] = _clamp_pct(_safe_float(current.get("min_pct")) + _safe_float(row.get("min_pct")))
            current["target_pct"] = _clamp_pct(_safe_float(current.get("target_pct")) + _safe_float(row.get("target_pct")))
            current["max_pct"] = _clamp_pct(_safe_float(current.get("max_pct")) + _safe_float(row.get("max_pct")))
            existing_rationale = str(current.get("rationale") or "").strip()
            next_rationale = str(row.get("rationale") or "").strip()
            if next_rationale and next_rationale not in existing_rationale:
                current["rationale"] = f"{existing_rationale} {next_rationale}".strip()
        return list(merged.values())

    normalized["asset_class_targets"] = _merge_target_rows(
        normalized.get("asset_class_targets") if isinstance(normalized.get("asset_class_targets"), list) else []
    )

    new_classes = _merge_target_rows(
        normalized.get("suggested_new_asset_classes") if isinstance(normalized.get("suggested_new_asset_classes"), list) else []
    )
    for row in new_classes:
        row.pop("min_pct", None)
        row.pop("max_pct", None)
    normalized["suggested_new_asset_classes"] = new_classes

    if dropped_labels:
        risk_flags = normalized.get("risk_flags") if isinstance(normalized.get("risk_flags"), list) else []
        risk_flags.append(
            "Unsupported generic asset-class labels were removed or remapped: "
            + ", ".join(dict.fromkeys(dropped_labels))
        )
        normalized["risk_flags"] = risk_flags
    return normalized


def _sharpen_quadrant_assessment(evidence_brief: Dict[str, Any]) -> Dict[str, Any]:
    quadrant = dict(evidence_brief.get("quadrant_assessment") or {})
    if not quadrant:
        return quadrant

    macro_scorecard = evidence_brief.get("macro_scorecard") if isinstance(evidence_brief.get("macro_scorecard"), dict) else {}
    market_view = evidence_brief.get("market_view") if isinstance(evidence_brief.get("market_view"), dict) else {}
    commodity_prices = evidence_brief.get("commodity_prices") if isinstance(evidence_brief.get("commodity_prices"), list) else []

    growth_text = " ".join(
        [
            str(macro_scorecard.get("growth_nowcast") or "").strip(),
            str(market_view.get("growth_view") or "").strip(),
            str(market_view.get("soft_landing_verdict") or "").strip(),
        ]
    ).upper()
    inflation_text = " ".join(
        [
            str(macro_scorecard.get("inflation") or "").strip(),
            str(market_view.get("inflation_view") or "").strip(),
            str(market_view.get("rates_view") or "").strip(),
        ]
    ).upper()

    oil_text = " ".join(
        " ".join(
            [
                str(row.get("commodity") or "").strip(),
                str(row.get("price_context") or "").strip(),
                str(row.get("portfolio_implication") or "").strip(),
            ]
        )
        for row in commodity_prices
        if isinstance(row, dict) and "OIL" in str(row.get("commodity") or "").upper()
    ).upper()

    slowdown_markers = ("<1", "0.", "SLOWDOWN", "WEAKENING", "MATERIAL SLOWDOWN", "NOT_SUPPORTED", "REVISED DOWN")
    inflation_markers = ("STICKY", "ABOVE TARGET", "HIGHER-FOR-LONGER", "RESTRICTIVE", "ELEVATED")
    disinflation_markers = ("DISINFLATION", "COOLING", "FALLING", "SOFTENING", "BELOW TARGET")
    oil_hot_markers = ("$100", "$90", "$80", "SUPPLY CONSTRAINT", "SUPPLY TIGHT", "GEOPOLIT", "ELEVATED")

    slowdown = any(marker in growth_text for marker in slowdown_markers)
    inflationary = any(marker in inflation_text for marker in inflation_markers)
    disinflationary = any(marker in inflation_text for marker in disinflation_markers)
    oil_hot = any(marker in oil_text for marker in oil_hot_markers)

    if slowdown and (oil_hot or inflationary):
        quadrant["best_fit"] = "Q3"
        quadrant["secondary_fit"] = "Q4"
    elif slowdown:
        quadrant["best_fit"] = "Q4"
        quadrant["secondary_fit"] = "Q3"
    elif inflationary or oil_hot:
        quadrant["best_fit"] = "Q2"
        quadrant["secondary_fit"] = "Q3"
    elif disinflationary and not slowdown:
        quadrant["best_fit"] = "Q1"
        quadrant["secondary_fit"] = "Q2"
    else:
        quadrant["best_fit"] = "Q1"
        quadrant["secondary_fit"] = "Q2"

    quadrant["q1_view"] = QUADRANT_DEFINITIONS["Q1"]
    quadrant["q2_view"] = QUADRANT_DEFINITIONS["Q2"]
    quadrant["q3_view"] = QUADRANT_DEFINITIONS["Q3"]
    quadrant["q4_view"] = QUADRANT_DEFINITIONS["Q4"]
    return quadrant


def _build_research_query(user_query: str) -> str:
    base = str(user_query or "").strip()
    if not base:
        base = (
            "Analyse the current macro environment and build an ideal top-down portfolio by asset class. "
            "Cover inflation, rates, oil, USD, liquidity, credit, breadth, commodities, geopolitics, "
            "and 12-24 month forward risks."
        )
    return (
        f"{base} "
        "Start from macro conditions only. Do not anchor to any existing portfolio structure. "
        "Decide what the ideal asset-class mix should be first, then compare against the current portfolio later. "
        "Do not recommend individual stocks."
    ).strip()


def _build_macro_environment_news_prompt(*, user_query: str) -> str:
    focus = str(user_query or "").strip()
    prompt = (
        "Provide one single-paragraph macro environment brief for a multi-asset investment portfolio. "
        "Minimum 260 words (target 280-380 words). "
        "Cover: the last week, the last month, the last year, and the 12-24 month forward outlook. "
        "Include concrete levels where relevant for policy rates, front-end rates, 10-year bond yields, USD, credit stress, "
        "equity breadth, and major commodity prices. "
        "Commodity coverage should include at minimum oil, natural gas, gold, silver, copper, iron ore, uranium if available, coal, "
        "and at least one agricultural benchmark where relevant. "
        "State the latest dated US growth nowcast or current-year growth estimate you can verify, and do not describe a soft landing unless the latest evidence clearly supports it. "
        "If recent growth expectations have been revised below 1%, say that explicitly. "
        "Assess the environment against this exact quadrant framework and state which quadrant fits best and why: "
        f"Q1 = {QUADRANT_DEFINITIONS['Q1']}; "
        f"Q2 = {QUADRANT_DEFINITIONS['Q2']}; "
        f"Q3 = {QUADRANT_DEFINITIONS['Q3']}; "
        f"Q4 = {QUADRANT_DEFINITIONS['Q4']}. "
        "Do not rename or rotate these quadrant definitions. "
        "Note the most prominent current investment themes being pushed by major brokers, strategists, or bank research desks where visible. "
        "Explain which asset classes deserve more capital and which deserve less in the current environment. "
        "This brief must be independent of any existing portfolio. "
        "Output plain text only. Do NOT include URLs, citation markers, footnotes, source lists, markdown, or bullet points."
    )
    if focus:
        prompt += f" User focus: {focus}."
    return prompt.strip()


def _sanitize_plain_text(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned = raw
    cleaned = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\[\[\d+\]\]|\[\d+\]", "", cleaned)
    cleaned = re.sub(r"https?://\S+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


async def _fetch_xai_macro_environment_summary(*, user_query: str) -> Dict[str, Any]:
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
            "model": DEFAULT_MACRO_NEWS_MODEL,
        }

    prompt = _build_macro_environment_news_prompt(user_query=user_query)
    request_count = 0
    tool_calls_count = 0
    http_status = 0
    finish_reason = ""
    final_content = ""

    def _extract_output_text(data: Dict[str, Any]) -> str:
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
        return texts[-1] if texts else ""

    try:
        async with httpx.AsyncClient(timeout=max(20.0, DEFAULT_MACRO_NEWS_TIMEOUT_SECONDS)) as client:
            for _ in range(max(1, DEFAULT_MACRO_NEWS_MAX_TOOL_ITERATIONS)):
                payload = {
                    "model": DEFAULT_MACRO_NEWS_MODEL,
                    "input": prompt,
                    "tools": [{"type": "web_search"}, {"type": "x_search"}],
                    "max_output_tokens": max(128, DEFAULT_MACRO_NEWS_MAX_TOKENS),
                    "temperature": max(0.0, min(1.5, DEFAULT_MACRO_NEWS_TEMPERATURE)),
                }
                request_count += 1
                response = await client.post(
                    XAI_API_URL,
                    headers={
                        "Authorization": f"Bearer {XAI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                http_status = int(response.status_code)
                if response.status_code >= 400:
                    body = response.text[:1200]
                    return {
                        "attempted": True,
                        "success": False,
                        "error": f"http_{response.status_code}:{body}",
                        "summary": "",
                        "prompt": prompt,
                        "http_status": http_status,
                        "request_count": request_count,
                        "tool_calls_count": tool_calls_count,
                        "finish_reason": finish_reason,
                        "model": DEFAULT_MACRO_NEWS_MODEL,
                    }
                data = response.json()
                finish_reason = str(data.get("finish_reason") or "")
                output = data.get("output")
                if isinstance(output, list):
                    tool_calls_count += sum(
                        1
                        for item in output
                        if isinstance(item, dict) and str(item.get("type") or "").strip() in {"web_search_call", "x_search_call"}
                    )
                final_content = _extract_output_text(data)
                if final_content.strip():
                    break
        summary = _sanitize_plain_text(final_content)
        return {
            "attempted": True,
            "success": bool(summary),
            "error": "" if summary else "empty_summary",
            "summary": summary,
            "prompt": prompt,
            "http_status": http_status,
            "request_count": request_count,
            "tool_calls_count": tool_calls_count,
            "finish_reason": finish_reason,
            "model": DEFAULT_MACRO_NEWS_MODEL,
        }
    except Exception as exc:
        return {
            "attempted": True,
            "success": False,
            "error": str(exc),
            "summary": "",
            "prompt": prompt,
            "http_status": http_status,
            "request_count": request_count,
            "tool_calls_count": tool_calls_count,
            "finish_reason": finish_reason,
            "model": DEFAULT_MACRO_NEWS_MODEL,
        }


async def _run_research_lanes(query: str, mode: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    tavily = TavilyResearchProvider()
    perplexity = PerplexityResearchProvider()
    max_sources = DEFAULT_MAX_SOURCES_DEEP if mode == "deep" else DEFAULT_MAX_SOURCES_FAST
    perplexity_depth = "deep" if mode == "deep" else "basic"
    tavily_depth = "advanced" if mode == "deep" else "basic"

    tavily_task = tavily.gather(
        user_query=query,
        depth=tavily_depth,
        max_sources=max_sources,
    )
    perplexity_task = perplexity.gather(
        user_query=query,
        depth=perplexity_depth,
        max_sources=max_sources,
    )

    tavily_result, perplexity_result = await asyncio.gather(tavily_task, perplexity_task, return_exceptions=True)

    def _normalize_result(result: Any, provider: str) -> Dict[str, Any]:
        if isinstance(result, Exception):
            return {"provider": provider, "error": str(result), "results": [], "research_summary": ""}
        if not isinstance(result, dict):
            return {"provider": provider, "error": "invalid_result", "results": [], "research_summary": ""}
        result.setdefault("provider", provider)
        result.setdefault("results", [])
        result.setdefault("research_summary", "")
        return result

    return _normalize_result(tavily_result, "tavily"), _normalize_result(perplexity_result, "perplexity")



def _dedupe_sources(*lanes: Dict[str, Any]) -> List[Dict[str, Any]]:
    seen: Dict[str, Dict[str, Any]] = {}
    for lane in lanes:
        provider = str(lane.get("provider") or "").strip()
        for row in lane.get("results") or []:
            if not isinstance(row, dict):
                continue
            url = str(row.get("url") or "").strip()
            title = str(row.get("title") or "Untitled").strip()
            key = url or title.lower()
            if not key:
                continue
            current = seen.get(key)
            current_score = _safe_float((current or {}).get("score"), -1e9)
            next_score = _safe_float(row.get("score"), 0.0)
            if current is None or next_score > current_score:
                seen[key] = {
                    "title": title,
                    "url": url,
                    "snippet": str(row.get("content") or "").strip(),
                    "provider": provider,
                    "published_at": str(row.get("published_at") or "").strip(),
                    "score": round(next_score, 4),
                }
    deduped = list(seen.values())
    deduped.sort(key=lambda item: _safe_float(item.get("score"), 0.0), reverse=True)
    return deduped[:16]


async def _build_evidence_brief(
    *,
    query: str,
    mode: str,
    macro_news: Dict[str, Any],
    tavily_result: Dict[str, Any],
    perplexity_result: Dict[str, Any],
) -> Dict[str, Any]:
    sources = _dedupe_sources(tavily_result, perplexity_result)
    prompt = {
        "task": "Compress macro and cross-asset research into a clean evidence brief for building an ideal portfolio from macro conditions first.",
        "rules": [
            "Return JSON only.",
            "Do not recommend individual stocks.",
            "Focus on macro, cross-asset, regime, rates, oil, inflation, breadth, credit, and sector leadership.",
            "Do not anchor to any existing portfolio.",
            "Keep it concise and high-signal.",
            "Use dated facts and concrete levels where possible rather than timeless macro language.",
            f"Use this exact quadrant framework: Q1 = {QUADRANT_DEFINITIONS['Q1']}; Q2 = {QUADRANT_DEFINITIONS['Q2']}; Q3 = {QUADRANT_DEFINITIONS['Q3']}; Q4 = {QUADRANT_DEFINITIONS['Q4']}.",
            "Do not rotate, rename, or reinterpret the quadrant labels.",
            "Commodity coverage must include oil, natural gas, gold, silver, copper, iron ore, uranium if available, coal, and at least one agricultural benchmark.",
            "If a commodity does not have a clean spot market, say that explicitly instead of omitting it.",
            "Include the dominant themes currently being pushed by major brokers, banks, or strategists where the research lanes support it.",
            "Use the latest dated growth evidence you have; do not call it a soft landing or cite resilient ~2.5% US growth unless the current evidence explicitly supports that.",
            "Prefer the latest nowcast / current-year estimate over stale consensus framing.",
            "If oil is very high and supply looks structurally tight, state the allocation implication directly instead of defaulting to diversification language.",
            "Do not hide behind MIXED unless the evidence is genuinely split; prefer a clear primary regime call and a clear secondary risk.",
        ],
        "required_schema": {
            "executive_summary": "string",
            "macro_scorecard": {
                "growth_nowcast": "string",
                "policy_rates": "string",
                "bond_yields": "string",
                "usd_liquidity": "string",
                "inflation": "string",
                "credit_stress": "string",
                "equity_breadth": "string"
            },
            "market_view": {
                "risk_tone": "RISK_ON | RISK_OFF | MIXED",
                "growth_view": "string",
                "soft_landing_verdict": "SUPPORTED | NOT_SUPPORTED | MIXED",
                "inflation_view": "string",
                "rates_view": "string",
                "oil_view": "string",
                "equity_breadth_view": "string",
                "key_messages": ["string"]
            },
            "commodity_prices": [
                {
                    "commodity": "string",
                    "price_context": "string",
                    "trend": "UP | DOWN | SIDEWAYS | MIXED",
                    "portfolio_implication": "string"
                }
            ],
            "quadrant_assessment": {
                "best_fit": "Q1 | Q2 | Q3 | Q4 | MIXED",
                "secondary_fit": "Q1 | Q2 | Q3 | Q4 | NONE",
                "q1_view": "string",
                "q2_view": "string",
                "q3_view": "string",
                "q4_view": "string",
                "primary_risk": "string",
                "secondary_risk": "string",
                "why_now": "string"
            },
            "broker_themes": [
                {
                    "theme": "string",
                    "firms": ["string"],
                    "stance": "BULLISH | BEARISH | MIXED",
                    "why_it_matters": "string"
                }
            ],
            "asset_class_implications": [
                {
                    "asset_class": "string",
                    "stance": "OVERWEIGHT | UNDERWEIGHT | HOLD | WATCH",
                    "reason": "string"
                }
            ],
            "watchpoints": ["string"],
            "source_shortlist": [
                {
                    "title": "string",
                    "url": "string",
                    "why_it_matters": "string"
                }
            ]
        },
        "user_query": query,
        "mode": mode,
        "macro_news_lane": {
            "summary": str(macro_news.get("summary") or "").strip(),
            "error": str(macro_news.get("error") or "").strip(),
            "model": str(macro_news.get("model") or "").strip(),
            "tool_calls_count": int(macro_news.get("tool_calls_count") or 0),
        },
        "research_lanes": {
            "tavily": {
                "summary": str(tavily_result.get("research_summary") or "").strip(),
                "top_results": (tavily_result.get("results") or [])[:8],
                "error": str(tavily_result.get("error") or "").strip(),
            },
            "perplexity": {
                "summary": str(perplexity_result.get("research_summary") or "").strip(),
                "top_results": (perplexity_result.get("results") or [])[:8],
                "error": str(perplexity_result.get("error") or "").strip(),
            },
        },
        "deduped_sources": sources[:10],
    }

    summary_model = _summary_model_for_mode(mode)
    response = await query_model(
        summary_model,
        [{"role": "user", "content": json.dumps(prompt, ensure_ascii=False, indent=2)}],
        timeout=180.0,
        max_tokens=4000,
        reasoning_effort="low",
    )
    parsed = _extract_json_object((response or {}).get("content") or "") if isinstance(response, dict) else None
    if isinstance(parsed, dict):
        parsed["summary_model"] = summary_model
        parsed["source_count"] = len(sources)
        return parsed

    return {
        "summary_model": summary_model,
        "source_count": len(sources),
        "executive_summary": str(
            macro_news.get("summary")
            or perplexity_result.get("research_summary")
            or tavily_result.get("research_summary")
            or ""
        ).strip()[:1200],
        "macro_scorecard": {
            "growth_nowcast": "See research shortlist.",
            "policy_rates": "See research shortlist.",
            "bond_yields": "See research shortlist.",
            "usd_liquidity": "See research shortlist.",
            "inflation": "See research shortlist.",
            "credit_stress": "See research shortlist.",
            "equity_breadth": "See research shortlist.",
        },
        "market_view": {
            "risk_tone": "MIXED",
            "growth_view": "See source shortlist and watchpoints.",
            "soft_landing_verdict": "MIXED",
            "inflation_view": "See source shortlist and watchpoints.",
            "rates_view": "See source shortlist and watchpoints.",
            "oil_view": "See source shortlist and watchpoints.",
            "equity_breadth_view": "See source shortlist and watchpoints.",
            "key_messages": [
                "Fallback evidence brief generated because the summary model did not return valid JSON.",
            ],
        },
        "commodity_prices": [],
        "quadrant_assessment": {
            "best_fit": "MIXED",
            "secondary_fit": "NONE",
            "q1_view": "Insufficient structured summary output.",
            "q2_view": "Insufficient structured summary output.",
            "q3_view": "Insufficient structured summary output.",
            "q4_view": "Insufficient structured summary output.",
            "why_now": "Fallback evidence brief used.",
        },
        "broker_themes": [],
        "asset_class_implications": [],
        "watchpoints": [],
        "source_shortlist": [
            {
                "title": str(item.get("title") or "Untitled"),
                "url": str(item.get("url") or ""),
                "why_it_matters": str(item.get("snippet") or "")[:180],
            }
            for item in sources[:8]
        ],
    }


def _fallback_macro_positioning(query: str, mode: str, evidence_brief: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "analysis_kind": "portfolio_positioning",
        "analysis_date": _utc_now_iso(),
        "mode": mode,
        "query": query,
        "executive_summary": str(evidence_brief.get("executive_summary") or "Macro positioning fallback memo.").strip(),
        "strategic_view": {
            "primary_theme": "Hold capital patiently until a valid macro strategist pass is available.",
            "secondary_theme": "Use current macro evidence as context, not as a textbook allocation exercise.",
            "cash_target_pct": 15.0,
            "cash_role": "Optional reserve while macro evidence is being re-checked.",
            "notes": [
                "Fallback output used because the strategist model did not return valid JSON.",
            ],
        },
        "asset_class_targets": [],
        "suggested_new_asset_classes": [],
        "implementation_notes": [
            "No ideal asset-class map was returned by the strategist model.",
        ],
        "monitoring_triggers": [],
        "risk_flags": [
            "Model fallback used — review manually before acting.",
        ],
        "confidence_note": "Low confidence fallback.",
    }


async def _run_chairman(
    *,
    query: str,
    mode: str,
    evidence_brief: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
    model_override: Optional[str] = None,
    lane_label: str = "chairman",
) -> Dict[str, Any]:
    chairman_model = str(model_override or _chairman_model_for_mode(mode)).strip()
    prompt = {
        "task": "Act as a top-down macro allocator. Build an ideal asset-class portfolio from the macro environment first, before seeing any existing holdings.",
        "rules": [
            "Return JSON only.",
            "Work at the asset-class level, not the stock level.",
            "This is not a Q1 overlay memo and not a balanced-fund diversification exercise.",
            "Do not anchor to any existing portfolio or assume current weights are sensible.",
            "Build the ideal portfolio from macro conditions first.",
            f"Use this exact quadrant framework: Q1 = {QUADRANT_DEFINITIONS['Q1']}; Q2 = {QUADRANT_DEFINITIONS['Q2']}; Q3 = {QUADRANT_DEFINITIONS['Q3']}; Q4 = {QUADRANT_DEFINITIONS['Q4']}.",
            "Do not rotate, rename, or reinterpret the quadrant labels.",
            "Targets should be pragmatic ranges, not false precision.",
            "Think in allocation ranges, not single fixed sizes.",
            "Keep target_pct between min_pct and max_pct.",
            "Concentrated allocations are acceptable when supported by macro evidence.",
            "Cash is one asset-class decision among the others.",
            "Use only the asset classes listed in available_asset_class_vocabulary.",
            "Do not invent generic allocator buckets like Developed Equities, Global Macro, Government Bonds, or Investment Grade Credit if a listed local asset class can express the same view.",
            "If you want broad equity exposure, map it into the provided asset-class system rather than creating a new label.",
            "Prefer concrete supported sleeves over umbrella labels. Avoid abstract parent buckets like EQUITY when more specific supported asset classes can express the view.",
            "The final comparison must be intelligible inside the user's asset-class system.",
            "Do not default to balanced-fund or institutional allocator logic.",
            "If oil is very high and the supply picture is tight or worsening, Energy can be a major core overweight rather than a token diversifier.",
            "If oil is around or above 100 dollars with a tight supply backdrop, large Energy ranges such as 25-45% can be reasonable if the evidence supports them.",
            "Do not underweight Energy purely for diversification optics when the macro evidence supports a concentrated commodity producer stance.",
            "Do not call the environment a soft landing unless the latest dated growth evidence clearly supports it.",
            "If the latest growth nowcast or current-year estimate is below 1%, treat that as a material slowdown and reflect it in the allocation logic.",
            "Use the quadrant assessment and the commodity tape directly; do not smooth them away into generic middle-of-the-road positioning.",
            "Every target row must represent a deliberate sleeve in the ideal portfolio. Do not emit placeholder 0-0-0 ranges just to mention a class.",
        ],
        "required_schema": {
            "analysis_kind": "portfolio_positioning",
            "analysis_date": "ISO-8601 string",
            "mode": "fast | deep",
            "query": "string",
            "executive_summary": "string",
            "strategic_view": {
                "primary_theme": "string",
                "secondary_theme": "string",
                "cash_target_pct": "number",
                "cash_role": "string",
                "notes": ["string"]
            },
            "asset_class_targets": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "min_pct": "number",
                    "target_pct": "number",
                    "max_pct": "number",
                    "thesis_role": "core | tactical | optional | hedge",
                    "rationale": "string",
                    "implementation_priority": "high | medium | low"
                }
            ],
            "suggested_new_asset_classes": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "target_pct": "number",
                    "rationale": "string"
                }
            ],
            "implementation_notes": ["string"],
            "monitoring_triggers": [
                {
                    "trigger": "string",
                    "what_changes": "string",
                    "direction": "risk_on | risk_off | watch"
                }
            ],
            "risk_flags": ["string"],
            "confidence_note": "string"
        },
        "evidence_brief": evidence_brief,
        "available_asset_class_vocabulary": asset_class_vocabulary,
    }

    response = await query_model(
        chairman_model,
        [{"role": "user", "content": json.dumps(prompt, ensure_ascii=False, indent=2)}],
        timeout=240.0,
        max_tokens=8000,
        reasoning_effort="medium",
    )
    parsed = _extract_json_object((response or {}).get("content") or "") if isinstance(response, dict) else None
    if not isinstance(parsed, dict):
        parsed = _fallback_macro_positioning(query, mode, evidence_brief)
    parsed["analysis_kind"] = "portfolio_positioning"
    parsed["analysis_date"] = _utc_now_iso()
    parsed["mode"] = mode
    parsed["query"] = query
    parsed["chairman_model"] = chairman_model
    parsed["council_lane"] = lane_label
    return _normalize_macro_positioning_taxonomy(parsed, asset_class_vocabulary=asset_class_vocabulary)


def _summarize_allocator_output(positioning: Dict[str, Any]) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for row in (positioning.get("asset_class_targets") or [])[:16]:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "asset_class": str(row.get("asset_class") or "").strip(),
                "display_name": str(row.get("display_name") or row.get("asset_class") or "").strip(),
                "min_pct": _clamp_pct(row.get("min_pct")),
                "target_pct": _clamp_pct(row.get("target_pct")),
                "max_pct": _clamp_pct(row.get("max_pct")),
                "thesis_role": str(row.get("thesis_role") or "").strip(),
                "rationale": str(row.get("rationale") or "").strip(),
                "implementation_priority": str(row.get("implementation_priority") or "").strip(),
            }
        )
    return {
        "model": str(positioning.get("chairman_model") or "").strip(),
        "lane": str(positioning.get("council_lane") or "allocator").strip(),
        "executive_summary": str(positioning.get("executive_summary") or "").strip(),
        "strategic_view": positioning.get("strategic_view") if isinstance(positioning.get("strategic_view"), dict) else {},
        "asset_class_targets": rows,
        "suggested_new_asset_classes": [
            row
            for row in (positioning.get("suggested_new_asset_classes") or [])[:12]
            if isinstance(row, dict)
        ],
        "implementation_notes": [
            str(item).strip()
            for item in (positioning.get("implementation_notes") or [])[:8]
            if str(item).strip()
        ],
        "monitoring_triggers": [
            row
            for row in (positioning.get("monitoring_triggers") or [])[:8]
            if isinstance(row, dict)
        ],
        "risk_flags": [
            str(item).strip()
            for item in (positioning.get("risk_flags") or [])[:8]
            if str(item).strip()
        ],
        "confidence_note": str(positioning.get("confidence_note") or "").strip(),
    }


async def _run_allocator_judge(
    *,
    query: str,
    mode: str,
    evidence_brief: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
    allocator_outputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    valid_outputs = [row for row in allocator_outputs if isinstance(row, dict)]
    if not valid_outputs:
        return _fallback_macro_positioning(query, mode, evidence_brief)
    if len(valid_outputs) == 1:
        winner = dict(valid_outputs[0])
        winner["judge_model"] = str(winner.get("chairman_model") or "").strip()
        winner["allocator_council"] = {
            "mode": "single",
            "models": [str(winner.get("chairman_model") or "").strip()],
            "consensus_summary": "Only one allocator lane returned usable output.",
            "disagreement_notes": [],
        }
        return winner

    judge_model = _chairman_model_for_mode(mode)
    prompt = {
        "task": "Act as the portfolio-positioning judge. Compare multiple independent allocator outputs built from the same macro evidence and produce one final best-judgement asset-class portfolio.",
        "rules": [
            "Return JSON only.",
            "Work at the asset-class level, not the stock level.",
            f"Use this exact quadrant framework: Q1 = {QUADRANT_DEFINITIONS['Q1']}; Q2 = {QUADRANT_DEFINITIONS['Q2']}; Q3 = {QUADRANT_DEFINITIONS['Q3']}; Q4 = {QUADRANT_DEFINITIONS['Q4']}.",
            "Do not rotate, rename, or reinterpret the quadrant labels.",
            "Use only the provided asset-class vocabulary.",
            "Do not mechanically average the allocators.",
            "Select the most defensible ranges based on evidence quality, internal coherence, and agreement across allocators.",
            "If allocators disagree, explain the disagreement briefly and then choose a side.",
            "Prefer the output that best matches the evidence brief, commodity tape, rates, and quadrant logic.",
            "Do not invent placeholder target rows just to mention a sleeve.",
            "Concentrated allocations are acceptable when supported by the evidence.",
        ],
        "required_schema": {
            "analysis_kind": "portfolio_positioning",
            "analysis_date": "ISO-8601 string",
            "mode": "fast | deep",
            "query": "string",
            "executive_summary": "string",
            "strategic_view": {
                "primary_theme": "string",
                "secondary_theme": "string",
                "cash_target_pct": "number",
                "cash_role": "string",
                "notes": ["string"]
            },
            "asset_class_targets": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "min_pct": "number",
                    "target_pct": "number",
                    "max_pct": "number",
                    "thesis_role": "core | tactical | optional | hedge",
                    "rationale": "string",
                    "implementation_priority": "high | medium | low"
                }
            ],
            "suggested_new_asset_classes": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "target_pct": "number",
                    "rationale": "string"
                }
            ],
            "implementation_notes": ["string"],
            "monitoring_triggers": [
                {
                    "trigger": "string",
                    "what_changes": "string",
                    "direction": "risk_on | risk_off | watch"
                }
            ],
            "risk_flags": ["string"],
            "confidence_note": "string",
            "allocator_council": {
                "mode": "judged",
                "models": ["string"],
                "consensus_summary": "string",
                "disagreement_notes": ["string"]
            }
        },
        "user_query": query,
        "mode": mode,
        "evidence_brief": evidence_brief,
        "available_asset_class_vocabulary": asset_class_vocabulary,
        "allocator_outputs": [_summarize_allocator_output(item) for item in valid_outputs],
    }

    response = await query_model(
        judge_model,
        [{"role": "user", "content": json.dumps(prompt, ensure_ascii=False, indent=2)}],
        timeout=240.0,
        max_tokens=9000,
        reasoning_effort="medium",
    )
    parsed = _extract_json_object((response or {}).get("content") or "") if isinstance(response, dict) else None
    if not isinstance(parsed, dict):
        fallback = dict(valid_outputs[0])
        fallback["judge_model"] = judge_model
        fallback["allocator_council"] = {
            "mode": "fallback_first_valid",
            "models": [str(item.get("chairman_model") or "").strip() for item in valid_outputs if isinstance(item, dict)],
            "consensus_summary": "Judge stage did not return valid JSON, so the first valid allocator lane was used.",
            "disagreement_notes": [],
        }
        return fallback

    parsed["analysis_kind"] = "portfolio_positioning"
    parsed["analysis_date"] = _utc_now_iso()
    parsed["mode"] = mode
    parsed["query"] = query
    parsed["judge_model"] = judge_model
    council = parsed.get("allocator_council") if isinstance(parsed.get("allocator_council"), dict) else {}
    council["mode"] = "judged"
    council["models"] = [str(item.get("chairman_model") or "").strip() for item in valid_outputs if isinstance(item, dict)]
    parsed["allocator_council"] = council
    normalized = _normalize_macro_positioning_taxonomy(parsed, asset_class_vocabulary=asset_class_vocabulary)
    normalized["judge_model"] = judge_model
    return normalized


async def _run_allocator_council(
    *,
    query: str,
    mode: str,
    evidence_brief: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    models = _allocator_council_models_for_mode(mode)
    tasks = [
        _run_chairman(
            query=query,
            mode=mode,
            evidence_brief=evidence_brief,
            asset_class_vocabulary=asset_class_vocabulary,
            model_override=model,
            lane_label=f"allocator_{idx + 1}",
        )
        for idx, model in enumerate(models)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    allocator_outputs: List[Dict[str, Any]] = []
    for idx, result in enumerate(results):
        model = models[idx]
        if isinstance(result, Exception):
            allocator_outputs.append(
                {
                    "analysis_kind": "portfolio_positioning",
                    "analysis_date": _utc_now_iso(),
                    "mode": mode,
                    "query": query,
                    "chairman_model": model,
                    "council_lane": f"allocator_{idx + 1}",
                    "error": str(result),
                    "asset_class_targets": [],
                }
            )
            continue
        row = dict(result)
        row["chairman_model"] = str(row.get("chairman_model") or model).strip()
        row["council_lane"] = str(row.get("council_lane") or f"allocator_{idx + 1}").strip()
        allocator_outputs.append(row)

    valid_outputs = [
        row
        for row in allocator_outputs
        if isinstance(row, dict) and not row.get("error") and isinstance(row.get("asset_class_targets"), list) and row.get("asset_class_targets")
    ]
    judged = await _run_allocator_judge(
        query=query,
        mode=mode,
        evidence_brief=evidence_brief,
        asset_class_vocabulary=asset_class_vocabulary,
        allocator_outputs=valid_outputs,
    )
    return judged, allocator_outputs


def _default_action_from_range(*, current_pct: float, min_pct: float, max_pct: float) -> str:
    if current_pct > max_pct + 0.25:
        return "TRIM"
    if current_pct < max(min_pct - 0.25, 0):
        return "ADD"
    return "HOLD"


def _default_conviction_from_row(row: Dict[str, Any]) -> str:
    current_pct = _clamp_pct(row.get("current_pct"))
    min_pct = _clamp_pct(row.get("min_pct"))
    max_pct = _clamp_pct(row.get("max_pct"))
    target_pct = _clamp_pct(row.get("target_pct"))
    action = str(row.get("action") or _default_action_from_range(current_pct=current_pct, min_pct=min_pct, max_pct=max_pct)).strip().upper()
    gap = abs(current_pct - target_pct)
    if action in {"ADD", "TRIM"}:
        if gap >= 10:
            return "STRONG"
        if gap >= 4:
            return "MEDIUM"
        return "WEAK"
    return "WEAK" if gap >= 3 else "MEDIUM"


def _fallback_allocator_commentary(structured: Dict[str, Any]) -> Dict[str, Any]:
    comments: List[Dict[str, Any]] = []
    for row in (structured.get("asset_class_targets") or [])[:16]:
        if not isinstance(row, dict):
            continue
        current_pct = _clamp_pct(row.get("current_pct"))
        min_pct = _clamp_pct(row.get("min_pct"))
        max_pct = _clamp_pct(row.get("max_pct"))
        action = str(row.get("action") or _default_action_from_range(current_pct=current_pct, min_pct=min_pct, max_pct=max_pct)).strip().upper()
        comments.append(
            {
                "asset_class": str(row.get("asset_class") or "").strip(),
                "display_name": str(row.get("display_name") or row.get("asset_class") or "").strip(),
                "direction": action,
                "conviction": _default_conviction_from_row(row),
                "commentary": f"Current weight is {current_pct:.1f}% against an ideal range of {min_pct:.1f}-{max_pct:.1f}%.",
            }
        )
    return {
        "comparison_summary": "Fallback allocator commentary used. Review the range gaps manually.",
        "overall_conviction": "MEDIUM",
        "reasonable_distribution_read": "The ideal portfolio is expressed as asset-class ranges rather than fixed point targets.",
        "portfolio_level_comments": [
            "Macro-first ranges were produced, but the final allocator commentary model did not return valid JSON.",
        ],
        "asset_class_comments": comments,
    }


async def _run_allocator_commentary(
    *,
    query: str,
    mode: str,
    evidence_brief: Dict[str, Any],
    structured: Dict[str, Any],
) -> Dict[str, Any]:
    commentary_model = _commentary_model_for_mode(mode)
    asset_rows = []
    for row in structured.get("asset_class_targets") or []:
        if not isinstance(row, dict):
            continue
        asset_rows.append(
            {
                "asset_class": str(row.get("asset_class") or "").strip(),
                "display_name": str(row.get("display_name") or row.get("asset_class") or "").strip(),
                "current_pct": _clamp_pct(row.get("current_pct")),
                "min_pct": _clamp_pct(row.get("min_pct")),
                "target_pct": _clamp_pct(row.get("target_pct")),
                "max_pct": _clamp_pct(row.get("max_pct")),
                "action": str(row.get("action") or "").strip().upper() or _default_action_from_range(
                    current_pct=_clamp_pct(row.get("current_pct")),
                    min_pct=_clamp_pct(row.get("min_pct")),
                    max_pct=_clamp_pct(row.get("max_pct")),
                ),
                "rationale": str(row.get("rationale") or "").strip(),
                "thesis_role": str(row.get("thesis_role") or "").strip(),
            }
        )

    prompt = {
        "task": "Review a macro-built ideal portfolio against the user's current asset-class percentages. You are only seeing asset classes and percentages, not stocks.",
        "rules": [
            "Return JSON only.",
            "Do not recommend individual securities.",
            "Comment on percentage sizing and direction of travel at the asset-class level.",
            "Ranges matter more than point targets.",
            "Use conviction labels exactly as STRONG, MEDIUM, or WEAK.",
            "If a current overweight is defensible because of the macro backdrop, say so explicitly instead of mechanically calling for a trim.",
            "If a current underweight is sensible because the evidence is mixed, say so explicitly instead of forcing an add.",
            "Keep the comments practical and allocator-focused.",
            "Do not police concentration just because it looks unconventional.",
            "If oil is very high and supply is tight, say plainly when a large Energy allocation is defensible.",
            "Do not use generic balanced-fund language.",
            "Do not describe the backdrop as a soft landing if the latest growth evidence says otherwise.",
            "If a current sleeve is not part of the ideal map, comment on it directly rather than pretending it does not exist.",
        ],
        "required_schema": {
            "comparison_summary": "string",
            "overall_conviction": "STRONG | MEDIUM | WEAK",
            "reasonable_distribution_read": "string",
            "portfolio_level_comments": ["string"],
            "asset_class_comments": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "direction": "ADD | TRIM | HOLD | REVIEW | WATCH",
                    "conviction": "STRONG | MEDIUM | WEAK",
                    "commentary": "string"
                }
            ],
        },
        "user_query": query,
        "mode": mode,
        "macro_context": {
            "executive_summary": str(evidence_brief.get("executive_summary") or "").strip(),
            "macro_scorecard": evidence_brief.get("macro_scorecard") if isinstance(evidence_brief.get("macro_scorecard"), dict) else {},
            "quadrant_assessment": evidence_brief.get("quadrant_assessment") if isinstance(evidence_brief.get("quadrant_assessment"), dict) else {},
            "broker_themes": (evidence_brief.get("broker_themes") or [])[:8],
            "commodity_prices": (evidence_brief.get("commodity_prices") or [])[:12],
        },
        "asset_class_ranges_and_current_weights": asset_rows[:20],
        "current_asset_classes_not_in_ideal_map": [
            {
                "asset_class": str(row.get("asset_class") or "").strip(),
                "display_name": str(row.get("display_name") or row.get("asset_class") or "").strip(),
                "current_pct": _clamp_pct(row.get("current_pct")),
                "rationale": str(row.get("rationale") or "").strip(),
            }
            for row in (structured.get("unmapped_current_asset_classes") or [])
            if isinstance(row, dict)
        ][:20],
    }

    response = await query_model(
        commentary_model,
        [{"role": "user", "content": json.dumps(prompt, ensure_ascii=False, indent=2)}],
        timeout=180.0,
        max_tokens=5000,
        reasoning_effort="medium",
    )
    parsed = _extract_json_object((response or {}).get("content") or "") if isinstance(response, dict) else None
    if not isinstance(parsed, dict):
        parsed = _fallback_allocator_commentary(structured)
    parsed["commentary_model"] = commentary_model
    return parsed


def _apply_allocator_commentary(
    *,
    structured: Dict[str, Any],
    commentary: Dict[str, Any],
) -> Dict[str, Any]:
    comments_by_key: Dict[str, Dict[str, Any]] = {}
    for row in commentary.get("asset_class_comments") or []:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        display_name = str(row.get("display_name") or "").strip()
        key = _normalize_asset_key(asset_class or display_name)
        if key:
            comments_by_key[key] = row

    enriched_targets: List[Dict[str, Any]] = []
    for row in structured.get("asset_class_targets") or []:
        if not isinstance(row, dict):
            continue
        key = _normalize_asset_key(row.get("asset_class") or row.get("display_name"))
        comment = comments_by_key.get(key, {})
        action = str(comment.get("direction") or row.get("action") or "").strip().upper()
        if not action:
            action = _default_action_from_range(
                current_pct=_clamp_pct(row.get("current_pct")),
                min_pct=_clamp_pct(row.get("min_pct")),
                max_pct=_clamp_pct(row.get("max_pct")),
            )
        conviction = str(comment.get("conviction") or "").strip().upper() or _default_conviction_from_row({**row, "action": action})
        note = str(comment.get("commentary") or "").strip()
        enriched = dict(row)
        enriched["action"] = action
        enriched["conviction"] = conviction
        if note:
            enriched["allocator_commentary"] = note
        enriched_targets.append(enriched)

    enriched_unmapped: List[Dict[str, Any]] = []
    for row in structured.get("unmapped_current_asset_classes") or []:
        if not isinstance(row, dict):
            continue
        key = _normalize_asset_key(row.get("asset_class") or row.get("display_name"))
        comment = comments_by_key.get(key, {})
        action = str(comment.get("direction") or row.get("action") or "REVIEW").strip().upper() or "REVIEW"
        conviction = str(comment.get("conviction") or "").strip().upper() or "MEDIUM"
        note = str(comment.get("commentary") or row.get("rationale") or "").strip()
        enriched = dict(row)
        enriched["action"] = action
        enriched["conviction"] = conviction
        if note:
            enriched["allocator_commentary"] = note
        enriched_unmapped.append(enriched)

    updated = dict(structured)
    updated["asset_class_targets"] = enriched_targets
    updated["unmapped_current_asset_classes"] = enriched_unmapped
    updated["allocator_commentary"] = {
        "comparison_summary": str(commentary.get("comparison_summary") or "").strip(),
        "overall_conviction": str(commentary.get("overall_conviction") or "").strip().upper(),
        "reasonable_distribution_read": str(commentary.get("reasonable_distribution_read") or "").strip(),
        "portfolio_level_comments": [
            str(item).strip()
            for item in (commentary.get("portfolio_level_comments") or [])
            if str(item).strip()
        ],
        "commentary_model": str(commentary.get("commentary_model") or "").strip(),
    }
    return updated


def _build_portfolio_diagnosis(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    asset_classes = [row for row in (snapshot.get("asset_classes") or []) if isinstance(row, dict)]
    dominant = [
        str(row.get("display_name") or row.get("asset_class") or "").strip()
        for row in asset_classes[:4]
        if str(row.get("display_name") or row.get("asset_class") or "").strip()
    ]
    cash_pct = _clamp_pct(((snapshot.get("portfolio") or {}).get("cash_pct")))
    top_weights = [_clamp_pct(row.get("portfolio_pct")) for row in asset_classes[:3]]
    risks: List[str] = []
    if top_weights and top_weights[0] >= 30:
        risks.append("Top sleeve concentration is high relative to the rest of the portfolio.")
    if sum(top_weights) >= 70:
        risks.append("Top three sleeves dominate the portfolio shape.")
    if cash_pct <= 3:
        risks.append("Cash reserve is thin relative to portfolio flexibility.")
    current_structure = " / ".join(
        f"{str(row.get('display_name') or row.get('asset_class') or '').strip()} {(_clamp_pct(row.get('portfolio_pct'))):.1f}%"
        for row in asset_classes[:5]
        if str(row.get("display_name") or row.get("asset_class") or "").strip()
    )
    return {
        "current_structure": current_structure or "Current portfolio snapshot available.",
        "current_cash_pct": cash_pct,
        "dominant_asset_classes": dominant,
        "concentration_risks": risks,
    }


def _merge_positioning_with_snapshot(
    *,
    snapshot: Dict[str, Any],
    macro_positioning: Dict[str, Any],
    evidence_brief: Dict[str, Any],
    query: str,
    mode: str,
) -> Dict[str, Any]:
    diagnosis = _build_portfolio_diagnosis(snapshot)
    sharpened_quadrant = _sharpen_quadrant_assessment(evidence_brief)
    current_by_class: Dict[str, Dict[str, Any]] = {}
    for row in snapshot.get("asset_classes") or []:
        if not isinstance(row, dict):
            continue
        key = str(row.get("asset_class") or "").strip()
        if key:
            current_by_class[key] = row

    merged_targets: List[Dict[str, Any]] = []
    unmapped_current_asset_classes: List[Dict[str, Any]] = []
    overweights: List[str] = []
    underweights: List[str] = []
    aligned: List[str] = []

    for row in macro_positioning.get("asset_class_targets") or []:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        if not asset_class:
            continue
        current_row = current_by_class.get(asset_class, {})
        current_pct = _clamp_pct(current_row.get("portfolio_pct"))
        min_pct = _clamp_pct(row.get("min_pct"))
        target_pct = _clamp_pct(row.get("target_pct"))
        max_pct = _clamp_pct(row.get("max_pct"))
        if max_pct < min_pct:
            min_pct, max_pct = max_pct, min_pct
        if target_pct < min_pct:
            target_pct = min_pct
        if target_pct > max_pct:
            target_pct = max_pct
        if current_pct > max_pct + 0.25:
            action = "TRIM"
            overweights.append(str(row.get("display_name") or asset_class))
        elif current_pct < max(min_pct - 0.25, 0):
            action = "ADD"
            underweights.append(str(row.get("display_name") or asset_class))
        else:
            action = "HOLD"
            aligned.append(str(row.get("display_name") or asset_class))

        merged_targets.append(
            {
                "asset_class": asset_class,
                "display_name": str(row.get("display_name") or current_row.get("display_name") or asset_class).strip(),
                "current_pct": current_pct,
                "min_pct": min_pct,
                "target_pct": target_pct,
                "max_pct": max_pct,
                "thesis_role": str(row.get("thesis_role") or "core").strip(),
                "action": action,
                "rationale": str(row.get("rationale") or "").strip(),
                "implementation_priority": str(row.get("implementation_priority") or "medium").strip(),
                "conviction": "",
            }
        )

    known = {str(row.get("asset_class") or "").strip() for row in merged_targets if isinstance(row, dict)}
    for asset_class, current_row in current_by_class.items():
        if asset_class in known:
            continue
        current_pct = _clamp_pct(current_row.get("portfolio_pct"))
        if current_pct <= 0:
            continue
        name = str(current_row.get("display_name") or asset_class).strip()
        unmapped_current_asset_classes.append(
            {
                "asset_class": asset_class,
                "display_name": name,
                "current_pct": current_pct,
                "action": "REVIEW",
                "rationale": "Current sleeve is not a named priority in the independent macro-built ideal map and needs a separate reassessment.",
                "implementation_priority": "medium",
                "conviction": "",
            }
        )

    merged_targets.sort(key=lambda item: (_safe_float(item.get("target_pct"), 0.0), _safe_float(item.get("current_pct"), 0.0)), reverse=True)
    unmapped_current_asset_classes.sort(key=lambda item: _safe_float(item.get("current_pct"), 0.0), reverse=True)

    implementation_notes = [
        str(item).strip()
        for item in (macro_positioning.get("implementation_notes") or [])
        if str(item).strip()
    ]
    if overweights:
        implementation_notes.append("Review the main overweights against the macro-built ideal portfolio rather than against textbook diversification rules.")
    if underweights:
        implementation_notes.append("Underweights represent sleeves the macro view wants funded, not mandatory trades.")

    return {
        "analysis_kind": "portfolio_positioning",
        "analysis_date": str(macro_positioning.get("analysis_date") or _utc_now_iso()),
        "mode": mode,
        "query": query,
        "executive_summary": str(macro_positioning.get("executive_summary") or "").strip(),
        "portfolio_diagnosis": diagnosis,
        "strategic_view": macro_positioning.get("strategic_view") if isinstance(macro_positioning.get("strategic_view"), dict) else {},
        "macro_scorecard": evidence_brief.get("macro_scorecard") if isinstance(evidence_brief.get("macro_scorecard"), dict) else {},
        "market_view": evidence_brief.get("market_view") if isinstance(evidence_brief.get("market_view"), dict) else {},
        "commodity_prices": evidence_brief.get("commodity_prices") if isinstance(evidence_brief.get("commodity_prices"), list) else [],
        "quadrant_assessment": sharpened_quadrant,
        "broker_themes": evidence_brief.get("broker_themes") if isinstance(evidence_brief.get("broker_themes"), list) else [],
        "asset_class_targets": merged_targets,
        "unmapped_current_asset_classes": unmapped_current_asset_classes,
        "suggested_new_asset_classes": macro_positioning.get("suggested_new_asset_classes") if isinstance(macro_positioning.get("suggested_new_asset_classes"), list) else [],
        "current_vs_ideal": {
            "main_overweights": overweights[:8],
            "main_underweights": underweights[:8],
            "aligned": aligned[:8],
        },
        "implementation_notes": implementation_notes,
        "monitoring_triggers": macro_positioning.get("monitoring_triggers") if isinstance(macro_positioning.get("monitoring_triggers"), list) else [],
        "risk_flags": macro_positioning.get("risk_flags") if isinstance(macro_positioning.get("risk_flags"), list) else [],
        "confidence_note": str(macro_positioning.get("confidence_note") or "").strip(),
        "chairman_model": str(macro_positioning.get("chairman_model") or DEFAULT_CHAIRMAN_MODEL).strip(),
        "judge_model": str(macro_positioning.get("judge_model") or "").strip(),
        "allocator_council": macro_positioning.get("allocator_council") if isinstance(macro_positioning.get("allocator_council"), dict) else {},
        "allocator_commentary": {},
    }



def _render_markdown(
    *,
    snapshot: Dict[str, Any],
    structured: Dict[str, Any],
    evidence_brief: Dict[str, Any],
    citations: List[Dict[str, Any]],
) -> str:
    lines: List[str] = []
    lines.append("# Portfolio Positioning Memo")
    lines.append("")
    lines.append(f"Generated: {structured.get('analysis_date') or _utc_now_iso()}")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(str(structured.get("executive_summary") or "No summary returned.").strip())
    lines.append("")

    diagnosis = structured.get("portfolio_diagnosis") if isinstance(structured.get("portfolio_diagnosis"), dict) else {}
    strategic_view = structured.get("strategic_view") if isinstance(structured.get("strategic_view"), dict) else {}
    current_vs_ideal = structured.get("current_vs_ideal") if isinstance(structured.get("current_vs_ideal"), dict) else {}
    allocator_commentary = structured.get("allocator_commentary") if isinstance(structured.get("allocator_commentary"), dict) else {}
    allocator_council = structured.get("allocator_council") if isinstance(structured.get("allocator_council"), dict) else {}
    quadrant_assessment = structured.get("quadrant_assessment") if isinstance(structured.get("quadrant_assessment"), dict) else {}
    macro_scorecard = structured.get("macro_scorecard") if isinstance(structured.get("macro_scorecard"), dict) else {}
    commodity_prices = structured.get("commodity_prices") if isinstance(structured.get("commodity_prices"), list) else []
    broker_themes = structured.get("broker_themes") if isinstance(structured.get("broker_themes"), list) else []
    unmapped_current_asset_classes = structured.get("unmapped_current_asset_classes") if isinstance(structured.get("unmapped_current_asset_classes"), list) else []
    lines.append("## Current Shape")
    lines.append("")
    lines.append(f"- Cash now: {diagnosis.get('current_cash_pct', ((snapshot.get('portfolio') or {}).get('cash_pct', 0)))}%")
    if diagnosis.get("current_structure"):
        lines.append(f"- Read: {str(diagnosis.get('current_structure')).strip()}")
    dominant = diagnosis.get("dominant_asset_classes")
    if isinstance(dominant, list) and dominant:
        lines.append(f"- Dominant sleeves: {', '.join(str(v).strip() for v in dominant if str(v).strip())}")
    concentration_risks = diagnosis.get("concentration_risks")
    if isinstance(concentration_risks, list):
        for item in concentration_risks[:5]:
            text = str(item).strip()
            if text:
                lines.append(f"- Concentration risk: {text}")
    for key, label in (("main_overweights", "Main overweights"), ("main_underweights", "Main underweights"), ("aligned", "Already close to ideal")):
        values = current_vs_ideal.get(key)
        if isinstance(values, list) and values:
            lines.append(f"- {label}: {', '.join(str(v).strip() for v in values if str(v).strip())}")
    lines.append("")

    lines.append("## Strategic View")
    lines.append("")
    if strategic_view.get("primary_theme"):
        lines.append(f"- Primary theme: {str(strategic_view.get('primary_theme')).strip()}")
    if strategic_view.get("secondary_theme"):
        lines.append(f"- Secondary theme: {str(strategic_view.get('secondary_theme')).strip()}")
    lines.append(f"- Cash target: {strategic_view.get('cash_target_pct', ((snapshot.get('portfolio') or {}).get('cash_pct', 0)))}%")
    if strategic_view.get("cash_role"):
        lines.append(f"- Cash role: {str(strategic_view.get('cash_role')).strip()}")
    notes = strategic_view.get("notes")
    if isinstance(notes, list):
        for note in notes[:5]:
            note_text = str(note).strip()
            if note_text:
                lines.append(f"- {note_text}")
    lines.append("")

    if allocator_council:
        lines.append("## Allocator Council")
        lines.append("")
        models = allocator_council.get("models") if isinstance(allocator_council.get("models"), list) else []
        consensus_summary = str(allocator_council.get("consensus_summary") or "").strip()
        disagreements = allocator_council.get("disagreement_notes") if isinstance(allocator_council.get("disagreement_notes"), list) else []
        if models:
            lines.append(f"- Models: {', '.join(str(item).strip() for item in models if str(item).strip())}")
        if consensus_summary:
            lines.append(f"- Consensus: {consensus_summary}")
        for item in disagreements[:5]:
            text = str(item).strip()
            if text:
                lines.append(f"- Disagreement: {text}")
        lines.append("")

    if macro_scorecard:
        lines.append("## Macro Scorecard")
        lines.append("")
        for key, label in (
            ("growth_nowcast", "Growth nowcast"),
            ("policy_rates", "Policy rates"),
            ("bond_yields", "Bond yields"),
            ("usd_liquidity", "USD / liquidity"),
            ("inflation", "Inflation"),
            ("credit_stress", "Credit stress"),
            ("equity_breadth", "Equity breadth"),
        ):
            value = str(macro_scorecard.get(key) or "").strip()
            if value:
                lines.append(f"- {label}: {value}")
        lines.append("")

    if quadrant_assessment:
        lines.append("## Quadrant Assessment")
        lines.append("")
        best_fit = str(quadrant_assessment.get("best_fit") or "").strip()
        secondary_fit = str(quadrant_assessment.get("secondary_fit") or "").strip()
        primary_risk = str(quadrant_assessment.get("primary_risk") or "").strip()
        secondary_risk = str(quadrant_assessment.get("secondary_risk") or "").strip()
        why_now = str(quadrant_assessment.get("why_now") or "").strip()
        if best_fit:
            lines.append(f"- Best fit: {best_fit}")
        if secondary_fit and secondary_fit != "NONE":
            lines.append(f"- Secondary fit: {secondary_fit}")
        if primary_risk:
            lines.append(f"- Primary regime risk: {primary_risk}")
        if secondary_risk:
            lines.append(f"- Secondary regime risk: {secondary_risk}")
        if why_now:
            lines.append(f"- Why now: {why_now}")
        for key, label in (("q1_view", "Q1"), ("q2_view", "Q2"), ("q3_view", "Q3"), ("q4_view", "Q4")):
            value = str(quadrant_assessment.get(key) or "").strip()
            if value:
                lines.append(f"- {label}: {value}")
        lines.append("")

    if commodity_prices:
        lines.append("## Commodity Context")
        lines.append("")
        for row in commodity_prices[:12]:
            if not isinstance(row, dict):
                continue
            commodity = str(row.get("commodity") or "").strip()
            price_context = str(row.get("price_context") or "").strip()
            trend = str(row.get("trend") or "").strip()
            implication = str(row.get("portfolio_implication") or "").strip()
            text = commodity
            if trend:
                text += f" ({trend})"
            if price_context:
                text += f": {price_context}"
            if implication:
                text += f" — {implication}"
            if text:
                lines.append(f"- {text}")
        lines.append("")

    if broker_themes:
        lines.append("## Broker Themes")
        lines.append("")
        for row in broker_themes[:8]:
            if not isinstance(row, dict):
                continue
            theme = str(row.get("theme") or "").strip()
            firms = row.get("firms") if isinstance(row.get("firms"), list) else []
            stance = str(row.get("stance") or "").strip()
            why_it_matters = str(row.get("why_it_matters") or "").strip()
            text = theme
            if stance:
                text += f" ({stance})"
            if firms:
                text += f" — {', '.join(str(item).strip() for item in firms if str(item).strip())}"
            if why_it_matters:
                text += f": {why_it_matters}"
            if text:
                lines.append(f"- {text}")
        lines.append("")

    lines.append("## Asset Class Targets")
    lines.append("")
    targets = structured.get("asset_class_targets") if isinstance(structured.get("asset_class_targets"), list) else []
    if targets:
        lines.append("| Asset Class | Current | Range | Target | Direction | Conviction |")
        lines.append("| --- | ---: | ---: | ---: | --- | --- |")
        for row in targets:
            if not isinstance(row, dict):
                continue
            name = str(row.get("display_name") or row.get("asset_class") or "").strip()
            current_pct = _clamp_pct(row.get("current_pct"))
            min_pct = _clamp_pct(row.get("min_pct"))
            target_pct = _clamp_pct(row.get("target_pct"))
            max_pct = _clamp_pct(row.get("max_pct"))
            action = str(row.get("action") or "HOLD").strip().upper()
            conviction = str(row.get("conviction") or "").strip().upper() or _default_conviction_from_row(row)
            lines.append(f"| {name} | {current_pct:.1f}% | {min_pct:.1f}-{max_pct:.1f}% | {target_pct:.1f}% | {action} | {conviction} |")
        lines.append("")
        for row in targets[:16]:
            if not isinstance(row, dict):
                continue
            name = str(row.get("display_name") or row.get("asset_class") or "").strip()
            rationale = str(row.get("allocator_commentary") or row.get("rationale") or "").strip()
            thesis_role = str(row.get("thesis_role") or "").strip()
            action = str(row.get("action") or "").strip().upper()
            conviction = str(row.get("conviction") or "").strip().upper()
            prefix = f" ({thesis_role})" if thesis_role else ""
            if name and rationale:
                lead = f"{action} / {conviction}" if action and conviction else action or conviction
                if lead:
                    lines.append(f"- **{name}{prefix} — {lead}:** {rationale}")
                else:
                    lines.append(f"- **{name}{prefix}:** {rationale}")
        lines.append("")

    if unmapped_current_asset_classes:
        lines.append("## Current Sleeves To Reassess")
        lines.append("")
        for row in unmapped_current_asset_classes[:16]:
            if not isinstance(row, dict):
                continue
            name = str(row.get("display_name") or row.get("asset_class") or "").strip()
            current_pct = _clamp_pct(row.get("current_pct"))
            action = str(row.get("action") or "REVIEW").strip().upper()
            conviction = str(row.get("conviction") or "").strip().upper()
            rationale = str(row.get("allocator_commentary") or row.get("rationale") or "").strip()
            lead = f"{action} / {conviction}" if action and conviction else action or conviction
            if name and rationale:
                lines.append(f"- **{name} ({current_pct:.1f}%) — {lead}:** {rationale}")
            elif name:
                lines.append(f"- **{name} ({current_pct:.1f}%) — {lead}**")
        lines.append("")

    if allocator_commentary:
        lines.append("## Allocator Commentary")
        lines.append("")
        summary = str(allocator_commentary.get("comparison_summary") or "").strip()
        if summary:
            lines.append(summary)
            lines.append("")
        overall_conviction = str(allocator_commentary.get("overall_conviction") or "").strip()
        if overall_conviction:
            lines.append(f"- Overall conviction: {overall_conviction}")
        reasonable_read = str(allocator_commentary.get("reasonable_distribution_read") or "").strip()
        if reasonable_read:
            lines.append(f"- Reasonable distribution read: {reasonable_read}")
        comments = allocator_commentary.get("portfolio_level_comments")
        if isinstance(comments, list):
            for item in comments[:8]:
                text = str(item).strip()
                if text:
                    lines.append(f"- {text}")
        lines.append("")

    new_classes = structured.get("suggested_new_asset_classes") if isinstance(structured.get("suggested_new_asset_classes"), list) else []
    if new_classes:
        lines.append("## New Asset Classes To Consider")
        lines.append("")
        for row in new_classes[:8]:
            if not isinstance(row, dict):
                continue
            name = str(row.get("display_name") or row.get("asset_class") or "").strip()
            target_pct = _clamp_pct(row.get("target_pct"))
            rationale = str(row.get("rationale") or "").strip()
            lines.append(f"- **{name}**: {target_pct:.1f}% target. {rationale}")
        lines.append("")

    implementation_notes = structured.get("implementation_notes") if isinstance(structured.get("implementation_notes"), list) else []
    if implementation_notes:
        lines.append("## Implementation Notes")
        lines.append("")
        for item in implementation_notes[:8]:
            text = str(item).strip()
            if text:
                lines.append(f"- {text}")
        lines.append("")

    triggers = structured.get("monitoring_triggers") if isinstance(structured.get("monitoring_triggers"), list) else []
    if triggers:
        lines.append("## Monitoring Triggers")
        lines.append("")
        for item in triggers[:10]:
            if not isinstance(item, dict):
                continue
            trigger = str(item.get("trigger") or "").strip()
            change = str(item.get("what_changes") or "").strip()
            direction = str(item.get("direction") or "").strip()
            text = trigger
            if direction:
                text += f" ({direction})"
            if change:
                text += f": {change}"
            if text:
                lines.append(f"- {text}")
        lines.append("")

    risk_flags = structured.get("risk_flags") if isinstance(structured.get("risk_flags"), list) else []
    if risk_flags:
        lines.append("## Risk Flags")
        lines.append("")
        for item in risk_flags[:8]:
            text = str(item).strip()
            if text:
                lines.append(f"- {text}")
        lines.append("")

    lines.append("## Research Notes")
    lines.append("")
    lines.append(str(evidence_brief.get("executive_summary") or "").strip() or "No evidence brief summary returned.")
    market_view = evidence_brief.get("market_view") if isinstance(evidence_brief.get("market_view"), dict) else {}
    key_messages = market_view.get("key_messages") if isinstance(market_view.get("key_messages"), list) else []
    for item in key_messages[:6]:
        text = str(item).strip()
        if text:
            lines.append(f"- {text}")
    lines.append("")

    if citations:
        lines.append("## Sources")
        lines.append("")
        for item in citations[:12]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "Untitled").strip()
            url = str(item.get("url") or "").strip()
            provider = str(item.get("provider") or "").strip()
            if url:
                lines.append(f"- {title} — {provider} — {url}")
            else:
                lines.append(f"- {title} — {provider}")
        lines.append("")

    confidence_note = str(structured.get("confidence_note") or "").strip()
    if confidence_note:
        lines.append("## Confidence")
        lines.append("")
        lines.append(confidence_note)
        lines.append("")

    return "\n".join(lines).strip() + "\n"

    if citations:
        lines.append("## Sources")
        lines.append("")
        for item in citations[:12]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "Untitled").strip()
            url = str(item.get("url") or "").strip()
            provider = str(item.get("provider") or "").strip()
            if url:
                lines.append(f"- {title} — {provider} — {url}")
            else:
                lines.append(f"- {title} — {provider}")
        lines.append("")

    confidence_note = str(structured.get("confidence_note") or "").strip()
    if confidence_note:
        lines.append("## Confidence")
        lines.append("")
        lines.append(confidence_note)
        lines.append("")

    return "\n".join(lines).strip() + "\n"


async def _run_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    context = _read_context(Path(args.portfolio_context_file))
    snapshot = _compact_snapshot(context)
    asset_class_vocabulary = _build_asset_class_vocabulary(snapshot)
    query = _build_research_query(args.query or "")
    mode = str(args.mode or "fast").strip().lower()
    if mode not in {"fast", "deep"}:
        mode = "fast"

    _log(f"query ready mode={mode} holdings={((snapshot.get('portfolio') or {}).get('holdings_count') or 0)}")
    print("stage 1 start", flush=True)
    macro_news = await _fetch_xai_macro_environment_summary(user_query=query)
    tavily_result, perplexity_result = await _run_research_lanes(query, mode)
    print("stage 1 done", flush=True)

    print("stage 2 start", flush=True)
    evidence_brief = await _build_evidence_brief(
        query=query,
        mode=mode,
        macro_news=macro_news,
        tavily_result=tavily_result,
        perplexity_result=perplexity_result,
    )
    print("stage 2 done", flush=True)

    print("stage 3 start", flush=True)
    macro_positioning, allocator_council_runs = await _run_allocator_council(
        query=query,
        mode=mode,
        evidence_brief=evidence_brief,
        asset_class_vocabulary=asset_class_vocabulary,
    )
    structured = _merge_positioning_with_snapshot(
        snapshot=snapshot,
        macro_positioning=macro_positioning,
        evidence_brief=evidence_brief,
        query=query,
        mode=mode,
    )
    print("stage 3 primary done", flush=True)

    print("stage 4 start", flush=True)
    allocator_commentary = await _run_allocator_commentary(
        query=query,
        mode=mode,
        evidence_brief=evidence_brief,
        structured=structured,
    )
    structured = _apply_allocator_commentary(
        structured=structured,
        commentary=allocator_commentary,
    )
    print("stage 4 done", flush=True)

    citations = _dedupe_sources(tavily_result, perplexity_result)
    markdown = _render_markdown(
        snapshot=snapshot,
        structured=structured,
        evidence_brief=evidence_brief,
        citations=citations,
    )

    artifact = {
        "id": Path(args.dump_json).name,
        "file": Path(args.dump_json).name,
        "label": str(args.run_label or "portfolio_positioning").strip() or "portfolio_positioning",
        "updated_at": _utc_now_iso(),
        "analysis_kind": "portfolio_positioning",
        "mode": mode,
        "query": query,
        "portfolio_snapshot": snapshot,
        "research_runs": {
            "xai_macro_news": macro_news,
            "tavily": tavily_result,
            "perplexity": perplexity_result,
        },
        "evidence_brief": evidence_brief,
        "allocator_council_runs": allocator_council_runs,
        "macro_positioning": macro_positioning,
        "allocator_commentary": allocator_commentary,
        "structured_data": structured,
        "analyst_memo_markdown": markdown,
        "chairman_memo_markdown": markdown,
    }
    print("run complete", flush=True)
    return artifact


async def _async_main(args: argparse.Namespace) -> int:
    artifact = await _run_pipeline(args)
    output_path = Path(args.dump_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"wrote artifact {output_path}")
    return 0



def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a portfolio positioning memo pipeline")
    parser.add_argument("--query", default="", help="Optional portfolio positioning question")
    parser.add_argument("--portfolio-context-file", required=True, help="Path to normalized portfolio context JSON")
    parser.add_argument("--mode", default="fast", choices=["fast", "deep"], help="Research depth / cost mode")
    parser.add_argument("--run-label", default="portfolio_positioning", help="Optional run label")
    parser.add_argument("--dump-json", required=True, help="Artifact output path")
    return parser


if __name__ == "__main__":
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_async_main(args)))
