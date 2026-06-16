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
    "google/gemini-3.1-pro-preview",
).strip()
DEFAULT_DEEP_SUMMARY_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_DEEP_SUMMARY_MODEL",
    "openai/gpt-5.5",
).strip()
DEFAULT_CHAIRMAN_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_CHAIRMAN_MODEL",
    "google/gemini-3.1-pro-preview",
).strip()
DEFAULT_DEEP_CHAIRMAN_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_DEEP_CHAIRMAN_MODEL",
    "openai/gpt-5.5",
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
    os.getenv("STAGE1_SUPPLEMENTARY_XAI_MODEL", "grok-4-1-fast-reasoning"),
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
DEFAULT_FAST_EVIDENCE_RUNS = int(os.getenv("PORTFOLIO_POSITIONING_FAST_EVIDENCE_RUNS", "1") or 1)
DEFAULT_DEEP_EVIDENCE_RUNS = int(os.getenv("PORTFOLIO_POSITIONING_DEEP_EVIDENCE_RUNS", "3") or 3)

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
                "x-ai/grok-4.3",
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
                "openai/gpt-5.5",
                "google/gemini-3.1-pro-preview",
                "x-ai/grok-4.3",
            ]
        ),
    )
)
DEFAULT_FAST_ENSEMBLE_RUNS = int(os.getenv("PORTFOLIO_POSITIONING_FAST_ENSEMBLE_RUNS", "1") or 1)
DEFAULT_DEEP_ENSEMBLE_RUNS = int(os.getenv("PORTFOLIO_POSITIONING_DEEP_ENSEMBLE_RUNS", "3") or 3)
DEFAULT_SYNTHESIS_MODEL = os.getenv(
    "PORTFOLIO_POSITIONING_SYNTHESIS_MODEL",
    DEFAULT_DEEP_CHAIRMAN_MODEL,
).strip()


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


def _extract_pct_from_fields(row: Dict[str, Any], fields: List[str], default: float = 0.0) -> float:
    """Accept common frontend naming variants and decimal-vs-percent representations."""
    for field in fields:
        if field not in row:
            continue
        value = row.get(field)
        if value is None or value == "":
            continue
        pct = _safe_float(value, default)
        field_key = str(field or "").lower()
        if 0 < pct <= 1 and not field_key.endswith("_pct") and "percent" not in field_key:
            pct *= 100.0
        return _clamp_pct(pct)
    return _clamp_pct(default)


def _extract_numeric_from_fields(row: Dict[str, Any], fields: List[str], default: float = 0.0) -> float:
    for field in fields:
        if field not in row:
            continue
        value = row.get(field)
        if value is None or value == "":
            continue
        return round(_safe_float(value, default), 2)
    return round(float(default), 2)


def _snapshot_weight_diagnostics(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    asset_classes = [row for row in (snapshot.get("asset_classes") or []) if isinstance(row, dict)]
    nonzero = [row for row in asset_classes if _safe_float(row.get("portfolio_pct")) > 0]
    total_pct = round(sum(_safe_float(row.get("portfolio_pct")) for row in asset_classes), 2)
    return {
        "asset_class_count": len(asset_classes),
        "nonzero_asset_class_count": len(nonzero),
        "asset_class_total_pct": total_pct,
        "all_asset_class_weights_zero": bool(asset_classes and not nonzero),
        "current_cash_pct": _clamp_pct(((snapshot.get("portfolio") or {}).get("cash_pct"))),
        "dominant_asset_classes": [
            str(row.get("display_name") or row.get("asset_class") or "").strip()
            for row in asset_classes[:6]
            if str(row.get("display_name") or row.get("asset_class") or "").strip()
        ],
    }



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
        value = _extract_numeric_from_fields(
            item,
            [
                "value",
                "market_value",
                "current_value",
                "amount",
                "total_class_capital_value",
                "trigger_total_class_value",
                "actual_invested_value",
                "allowed_invested_value",
                "trigger_invested_value",
                "strategic_weight_value",
            ],
            0.0,
        )
        asset_classes.append(
            {
                "asset_class": str(item.get("asset_class") or "").strip(),
                "display_name": str(item.get("display_name") or item.get("asset_class") or "").strip(),
                "portfolio_pct": _extract_pct_from_fields(
                    item,
                    [
                        "portfolio_pct",
                        "actual_pct",
                        "current_pct",
                        "weight_pct",
                        "allocation_pct",
                        "percentage",
                        "percent",
                        "portfolio_weight",
                        "current_weight",
                        "target_weight",
                        "total_class_capital_pct",
                        "trigger_total_class_pct",
                        "actual_invested_pct",
                        "allowed_invested_pct",
                        "trigger_invested_pct",
                        "strategic_weight_pct",
                    ],
                ),
                "invested_pct": _extract_pct_from_fields(
                    item,
                    [
                        "invested_pct",
                        "invested_weight",
                        "invested_weight_pct",
                        "actual_invested_pct",
                        "allowed_invested_pct",
                        "trigger_invested_pct",
                    ],
                    0,
                ),
                "tactical_cash_pct": _extract_pct_from_fields(item, ["tactical_cash_pct", "cash_pct", "cash_weight_pct"], 0),
                "overlay_eligible": bool(item.get("overlay_eligible")),
                "q1_governed": bool(item.get("overlay_eligible")),
                "value": value,
                "notes": str(item.get("notes") or "").strip(),
            }
        )

    positive_asset_weights = [_safe_float(row.get("portfolio_pct")) for row in asset_classes if _safe_float(row.get("portfolio_pct")) > 0]
    positive_asset_pct_total = sum(positive_asset_weights)
    if 0 < positive_asset_pct_total <= 1.5 and positive_asset_weights and max(positive_asset_weights) <= 1.0:
        for row in asset_classes:
            row["portfolio_pct"] = _clamp_pct(_safe_float(row.get("portfolio_pct")) * 100.0)

    if asset_classes and not any(_safe_float(row.get("portfolio_pct")) > 0 for row in asset_classes):
        total_asset_value = sum(_safe_float(row.get("value")) for row in asset_classes)
        if total_asset_value > 0:
            for row in asset_classes:
                row["portfolio_pct"] = _clamp_pct((_safe_float(row.get("value")) / total_asset_value) * 100.0)

    asset_classes.sort(key=lambda row: row["portfolio_pct"], reverse=True)

    positions: List[Dict[str, Any]] = []
    for item in raw_positions[:80]:
        if not isinstance(item, dict):
            continue
        value = _extract_numeric_from_fields(item, ["value", "market_value", "current_value", "amount"], 0.0)
        cash = _extract_numeric_from_fields(item, ["cash", "cash_value", "tactical_cash"], 0.0)
        positions.append(
            {
                "ticker": str(item.get("ticker") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "asset_class": str(item.get("asset_class") or "UNASSIGNED").strip(),
                "value": value,
                "cash": cash,
                "portfolio_pct": _extract_pct_from_fields(
                    item,
                    [
                        "portfolio_pct",
                        "actual_pct",
                        "current_pct",
                        "weight_pct",
                        "allocation_pct",
                        "percentage",
                        "percent",
                        "portfolio_weight",
                        "current_weight",
                    ],
                ),
                "q1_governed": bool(item.get("q1_governed")),
            }
        )

    positive_position_weights = [_safe_float(row.get("portfolio_pct")) for row in positions if _safe_float(row.get("portfolio_pct")) > 0]
    positive_position_pct_total = sum(positive_position_weights)
    if 0 < positive_position_pct_total <= 1.5 and positive_position_weights and max(positive_position_weights) <= 1.0:
        for row in positions:
            row["portfolio_pct"] = _clamp_pct(_safe_float(row.get("portfolio_pct")) * 100.0)

    if asset_classes and not any(_safe_float(row.get("portfolio_pct")) > 0 for row in asset_classes) and positions:
        aggregate: Dict[str, Dict[str, Any]] = {}
        total_position_value = sum(_safe_float(row.get("value")) for row in positions)
        if total_position_value > 0:
            for row in positions:
                asset_class = str(row.get("asset_class") or "UNASSIGNED").strip() or "UNASSIGNED"
                display_name = asset_class
                current = aggregate.setdefault(
                    asset_class,
                    {
                        "asset_class": asset_class,
                        "display_name": display_name,
                        "portfolio_pct": 0.0,
                        "invested_pct": 0.0,
                        "tactical_cash_pct": 0.0,
                        "overlay_eligible": False,
                        "q1_governed": bool(row.get("q1_governed")),
                        "value": 0.0,
                        "notes": "Derived from position market values because supplied asset-class weights were zero.",
                    },
                )
                current["value"] = round(_safe_float(current.get("value")) + _safe_float(row.get("value")), 2)
                current["q1_governed"] = bool(current.get("q1_governed")) or bool(row.get("q1_governed"))
            asset_classes = list(aggregate.values())
            for row in asset_classes:
                row["portfolio_pct"] = _clamp_pct((_safe_float(row.get("value")) / total_position_value) * 100.0)
            asset_classes.sort(key=lambda row: row["portfolio_pct"], reverse=True)

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

    snapshot = {
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
    snapshot["weight_diagnostics"] = _snapshot_weight_diagnostics(snapshot)
    return snapshot



def _normalize_asset_key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").strip().upper())


def _humanize_asset_label(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    if "_" in text or "-" in text:
        text = re.sub(r"[_-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return fallback
    if text.islower() or text.isupper():
        text = text.title()
    return text


def _memo_asset_label(row: Dict[str, Any], fallback: str = "") -> str:
    raw_name = str(row.get("display_name") or row.get("asset_class") or "").strip()
    asset_class = str(row.get("asset_class") or "").strip()
    if raw_name and asset_class and _normalize_asset_key(raw_name) == _normalize_asset_key(asset_class):
        raw_name = _humanize_asset_label(raw_name)
    else:
        raw_name = _humanize_asset_label(raw_name, fallback)
    return (raw_name or fallback).upper()


def _build_asset_class_vocabulary(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    rows: List[Dict[str, Any]] = []
    source_rows = snapshot.get("available_asset_classes") or snapshot.get("asset_classes") or []
    source_keys = {
        _normalize_asset_key((item or {}).get("asset_class") if isinstance(item, dict) else item)
        for item in source_rows
    }
    specific_equity_keys = source_keys - {"", "EQUITY", "MISC", "UNASSIGNED", "ETF", "BONDS"}
    for item in source_rows:
        if not isinstance(item, dict):
            continue
        asset_class = str(item.get("asset_class") or "").strip()
        display_name = str(item.get("display_name") or asset_class).strip()
        key = _normalize_asset_key(asset_class or display_name)
        if not key or key in seen:
            continue
        if key == "EQUITY" and specific_equity_keys:
            continue
        seen.add(key)
        rows.append(
            {
                "asset_class": asset_class,
                "display_name": display_name,
                "q1_governed": bool(item.get("q1_governed")),
            }
        )
    if "CASH" not in seen:
        rows.append(
            {
                "asset_class": "CASH",
                "display_name": "CASH",
                "q1_governed": False,
            }
        )
    return rows


GENERIC_ASSET_CLASS_CANDIDATES: Dict[str, List[str]] = {
    # Broad equity / portfolio vocabulary.
    "DEVELOPEDEQUITIES": ["EQUITY"],
    "GLOBALEQUITIES": ["EQUITY"],
    "USEQUITIES": ["EQUITY"],
    "USLARGECAPEQUITIES": ["EQUITY"],
    "EMERGINGMARKEQUITIES": ["EQUITY"],
    "INTERNATIONALEQUITIES": ["EQUITY"],
    "BROADEQUITIES": ["EQUITY"],
    "EQUITIES": ["EQUITY"],
    "STOCKS": ["EQUITY"],
    "SHARES": ["EQUITY"],
    "CONSUMERDISCRETIONARY": ["EQUITY", "MISC"],
    "RETAIL": ["EQUITY", "STAPLES"],
    "ECOMMERCE": ["EQUITY", "TECHNOLOGY"],
    "REALESTATE": ["EQUITY", "MISC"],
    "REIT": ["EQUITY", "MISC"],
    "REITS": ["EQUITY", "MISC"],

    # Rates / defensive liquidity.
    "GOVERNMENTBONDS": ["BONDS"],
    "INVESTMENTGRADECREDIT": ["BONDS"],
    "SHORTDURATIONFIXEDINCOME": ["BONDS"],
    "SHORTDURATIONBONDS": ["BONDS"],
    "CORPORATEBONDS": ["BONDS"],
    "INFLATIONLINKEDBONDS": ["BONDS"],
    "TREASURIES": ["BONDS"],
    "TREASURYBILLS": ["BONDS"],
    "CASHANDSHORTTERMTREASURIES": ["BONDS", "MISC"],
    "CASHANDEQUIVALENTS": ["BONDS", "MISC"],
    "CASH": ["BONDS", "MISC"],
    "MONEYMARKET": ["BONDS", "MISC"],
    "FIXEDINCOME": ["BONDS"],
    "CREDIT": ["BONDS"],
    "LIQUIDITY": ["CASH", "BONDS"],
    "DRYPOWDER": ["CASH"],
    "CASHRESERVE": ["CASH"],
    "CASHBUFFER": ["CASH"],

    # Healthcare.
    "PHARMACEUTICALS": ["PHARMA"],
    "PHARMA": ["PHARMA"],
    "BIOTECH": ["PHARMA", "HEALTHCARE"],
    "BIOTECHNOLOGY": ["PHARMA", "HEALTHCARE"],
    "PHARMABIOTECH": ["PHARMA", "HEALTHCARE"],
    "HEALTHCARE": ["HEALTHCARE", "PHARMA"],
    "HEALTHCARESERVICES": ["HEALTHCARE"],
    "MEDTECH": ["HEALTHCARE"],
    "MEDICALTECHNOLOGY": ["HEALTHCARE"],

    # Staples / consumer defensives.
    "CONSUMERSTAPLES": ["STAPLES"],
    "STAPLES": ["STAPLES"],
    "FOOD": ["STAPLES"],
    "FOODANDBEVERAGE": ["STAPLES"],
    "BEVERAGES": ["STAPLES"],
    "AGRICULTURE": ["STAPLES", "MATERIALS"],
    "AGRIBUSINESS": ["STAPLES", "MATERIALS"],
    "AGRICULTUREAGRIBUSINESS": ["STAPLES", "MATERIALS"],
    "GRAIN": ["STAPLES", "MATERIALS"],
    "RURALSERVICES": ["STAPLES", "MATERIALS"],
    "FERTILIZER": ["MATERIALS", "STAPLES"],
    "FERTILISER": ["MATERIALS", "STAPLES"],
    "CROPINPUTS": ["MATERIALS", "STAPLES"],
    "FORESTRY": ["STAPLES", "MATERIALS"],
    "PAPER": ["STAPLES", "MATERIALS"],
    "PACKAGING": ["STAPLES", "MATERIALS"],
    "FORESTRYPAPERPACKAGING": ["STAPLES", "MATERIALS"],
    "PULP": ["STAPLES", "MATERIALS"],
    "TIMBER": ["STAPLES", "MATERIALS"],

    # Financials.
    "BANKS": ["FINANCIALS"],
    "BANKING": ["FINANCIALS"],
    "LENDERS": ["FINANCIALS"],
    "FINANCIALS": ["FINANCIALS"],
    "DIVERSIFIEDFINANCIALS": ["FINANCIALS"],
    "ASSETMANAGERS": ["FINANCIALS"],
    "ASSETMANAGEMENT": ["FINANCIALS"],
    "ASSETMANAGERSDIVERSIFIEDFINANCIALS": ["FINANCIALS"],
    "FUNDMANAGERS": ["FINANCIALS"],
    "WEALTHMANAGEMENT": ["FINANCIALS"],
    "BROKERS": ["FINANCIALS"],
    "BROKERAGE": ["FINANCIALS"],
    "EXCHANGES": ["FINANCIALS"],
    "MARKETINFRASTRUCTURE": ["FINANCIALS"],
    "INSURERS": ["INSURANCE", "FINANCIALS"],
    "INSURANCEBROKERS": ["INSURANCE", "FINANCIALS"],

    # Defence / aerospace / industrials.
    "DEFENSE": ["DEFENCE"],
    "MILITARY": ["DEFENCE"],
    "DEFENCECONTRACTORS": ["DEFENCE"],
    "AEROSPACEDEFENCE": ["DEFENCE", "INDUSTRIALS"],
    "AEROSPACEANDDEFENCE": ["DEFENCE", "INDUSTRIALS"],
    "CIVILAEROSPACE": ["INDUSTRIALS"],
    "COMMERCIALAEROSPACE": ["INDUSTRIALS"],
    "AEROSPACE": ["INDUSTRIALS", "DEFENCE"],
    "AIRCRAFT": ["INDUSTRIALS"],
    "AIRLINES": ["INDUSTRIALS"],
    "AIRPORTS": ["INDUSTRIALS"],
    "TRANSPORT": ["INDUSTRIALS"],
    "TRANSPORTATION": ["INDUSTRIALS"],
    "TRANSPORTLOGISTICS": ["INDUSTRIALS"],
    "LOGISTICS": ["INDUSTRIALS"],
    "FREIGHT": ["INDUSTRIALS"],
    "RAIL": ["INDUSTRIALS"],
    "SHIPPING": ["INDUSTRIALS", "ENERGY"],
    "PORTS": ["INDUSTRIALS"],
    "CONSTRUCTION": ["INDUSTRIALS"],
    "ENGINEERING": ["INDUSTRIALS"],
    "CONSTRUCTIONENGINEERING": ["INDUSTRIALS"],
    "CONTRACTORS": ["INDUSTRIALS"],
    "EPC": ["INDUSTRIALS"],
    "CIVILENGINEERING": ["INDUSTRIALS"],
    "UTILITIES": ["INDUSTRIALS", "ENERGY"],
    "POWERUTILITIES": ["ENERGY", "INDUSTRIALS"],
    "WATERUTILITIES": ["INDUSTRIALS"],
    "GASUTILITIES": ["ENERGY", "INDUSTRIALS"],
    "REGULATEDUTILITIES": ["INDUSTRIALS"],
    "INFRASTRUCTURE": ["INDUSTRIALS"],
    "REGULATEDINFRASTRUCTURE": ["INDUSTRIALS"],

    # Energy.
    "OILPRODUCERS": ["ENERGY"],
    "OILGAS": ["ENERGY"],
    "OILANDGAS": ["ENERGY"],
    "ENERGYOILGAS": ["ENERGY"],
    "LNG": ["ENERGY"],
    "GASPRODUCERS": ["ENERGY"],
    "COAL": ["ENERGY"],
    "COALMINER": ["ENERGY"],
    "THERMALCOAL": ["ENERGY"],
    "METCOAL": ["ENERGY"],
    "COKINGCOAL": ["ENERGY"],
    "URANIUMMINER": ["URANIUM", "ENERGY"],
    "NUCLEAR": ["URANIUM", "ENERGY"],

    # Materials and mining.
    "COMMODITIES": ["MATERIALS", "ETF"],
    "MINERS": ["MATERIALS"],
    "MINING": ["MATERIALS"],
    "MATERIALS": ["MATERIALS"],
    "DIVERSIFIEDMINER": ["MATERIALS"],
    "DIVERSIFIEDMINERS": ["MATERIALS"],
    "CHEMICALS": ["MATERIALS"],
    "CHEMICALSMATERIALS": ["MATERIALS"],
    "MATERIALSCHEMICALS": ["MATERIALS"],
    "SPECIALTYCHEMICALS": ["MATERIALS"],
    "INDUSTRIALGASES": ["MATERIALS"],
    "STEEL": ["BASEMETALS", "MATERIALS"],
    "STEELMAKERS": ["BASEMETALS", "MATERIALS"],
    "STEELBASEMETALSPROCESSING": ["BASEMETALS", "MATERIALS"],
    "METALPROCESSING": ["BASEMETALS", "MATERIALS"],
    "BASEMETALSPROCESSING": ["BASEMETALS", "MATERIALS"],
    "BASEMETALS": ["BASEMETALS", "MATERIALS"],
    "BASEMETAL": ["BASEMETALS", "MATERIALS"],
    "SCRAP": ["BASEMETALS", "MATERIALS"],
    "METALRECYCLING": ["BASEMETALS", "MATERIALS"],
    "SMELTERS": ["BASEMETALS", "MATERIALS"],
    "COPPERMINER": ["COPPER", "BASEMETALS", "MATERIALS"],
    "COPPERMINERS": ["COPPER", "BASEMETALS", "MATERIALS"],
    "BATTERYMETALS": ["LITHIUM", "MATERIALS"],
    "LITHIUMMINER": ["LITHIUM", "MATERIALS"],
    "LITHIUMMINERS": ["LITHIUM", "MATERIALS"],
    "RAREEARTHS": ["REE", "MATERIALS"],
    "RAREEARTH": ["REE", "MATERIALS"],
    "RAREEARTHMINER": ["REE", "MATERIALS"],
    "RAREEARTHMINERS": ["REE", "MATERIALS"],
    "RAREEARTHSCRITICALMINERALS": ["REE", "MATERIALS"],
    "CRITICALMINERALS": ["REE", "MATERIALS"],
    "IRONORE": ["IRON", "MATERIALS"],
    "IRONOREMINER": ["IRON", "MATERIALS"],
    "IRONOREMINERS": ["IRON", "MATERIALS"],
    "IRONMINER": ["IRON", "MATERIALS"],
    "IRONMINERS": ["IRON", "MATERIALS"],
    "BAUXITE": ["ALUMINIUM", "MATERIALS"],
    "BAUXITEMINER": ["ALUMINIUM", "MATERIALS"],
    "ALUMINA": ["ALUMINIUM", "MATERIALS"],
    "ALUMINUM": ["ALUMINIUM", "MATERIALS"],
    "ALUMINIUM": ["ALUMINIUM", "MATERIALS"],
    "GOLDMINER": ["GOLD", "MATERIALS"],
    "GOLDMINERS": ["GOLD", "MATERIALS"],
    "PHYSICALGOLD": ["GOLD"],
    "SILVERMINER": ["SILVER", "MATERIALS"],
    "SILVERMINERS": ["SILVER", "MATERIALS"],
    "PHYSICALSILVER": ["SILVER"],

    # Technology / communications / media.
    "TECH": ["TECHNOLOGY"],
    "TECHNOLOGYPLATFORMS": ["TECHNOLOGY"],
    "TECHPLATFORMS": ["TECHNOLOGY"],
    "SOFTWARE": ["TECHNOLOGY"],
    "SOFTWARESAAS": ["TECHNOLOGY"],
    "SAAS": ["TECHNOLOGY"],
    "INTERNET": ["TECHNOLOGY"],
    "FINTECH": ["TECHNOLOGY", "FINANCIALS"],
    "ADTECH": ["TECHNOLOGY", "MISC"],
    "DATACENTERS": ["TECHNOLOGY", "INDUSTRIALS"],
    "DATACENTRES": ["TECHNOLOGY", "INDUSTRIALS"],
    "DATAINFRASTRUCTURE": ["TECHNOLOGY", "INDUSTRIALS"],
    "DIGITALINFRASTRUCTURE": ["TECHNOLOGY", "INDUSTRIALS"],
    "TELECOMMUNICATIONS": ["TECHNOLOGY", "INDUSTRIALS"],
    "TELECOM": ["TECHNOLOGY", "INDUSTRIALS"],
    "TELCO": ["TECHNOLOGY", "INDUSTRIALS"],
    "BROADBAND": ["TECHNOLOGY", "INDUSTRIALS"],
    "FIBRE": ["TECHNOLOGY", "INDUSTRIALS"],
    "MOBILECARRIERS": ["TECHNOLOGY", "INDUSTRIALS"],
    "SEMIS": ["SEMICONDUCTORS"],
    "CHIPS": ["SEMICONDUCTORS"],
    "SEMICONDUCTOR": ["SEMICONDUCTORS"],
    "SEMICONDUCTORS": ["SEMICONDUCTORS"],
    "MEDIA": ["MISC", "TECHNOLOGY"],
    "MEDIAPUBLISHING": ["MISC", "TECHNOLOGY"],
    "PUBLISHING": ["MISC", "TECHNOLOGY"],
    "BROADCAST": ["MISC", "TECHNOLOGY"],
    "ADVERTISING": ["MISC", "TECHNOLOGY"],
    "STREAMING": ["TECHNOLOGY", "MISC"],
    "CLASSIFIEDS": ["TECHNOLOGY", "MISC"],
    "NEWS": ["MISC"],
    "CRYPTO": ["MISC", "TECHNOLOGY", "ETF"],
    "DIGITALASSETS": ["MISC", "TECHNOLOGY", "ETF"],
    "CRYPTODIGITALASSETS": ["MISC", "TECHNOLOGY", "ETF"],
    "BITCOIN": ["MISC", "ETF"],
    "BLOCKCHAIN": ["MISC", "TECHNOLOGY"],
    "CRYPTOEXCHANGES": ["MISC", "TECHNOLOGY"],
    "CRYPTOMINERS": ["MISC", "TECHNOLOGY"],

    # Other explicitly supported sleeves or company templates without a direct sleeve.
    "GAMBLING": ["GAMBLING"],
    "WAGERING": ["GAMBLING"],
    "CASINO": ["GAMBLING"],
    "LOTTERY": ["GAMBLING"],
    "GAMING": ["GAMING"],
    "VIDEOGAMES": ["GAMING"],
    "INTERACTIVEGAMING": ["GAMING"],
    "GAMINGINTERACTIVE": ["GAMING"],
    "EDUCATION": ["MISC"],
    "EDTECH": ["MISC", "TECHNOLOGY"],
    "TRAINING": ["MISC"],
    "STUDENTSERVICES": ["MISC"],
    "TRENDGLOBALMACRO": ["ETF", "MISC"],
    "GLOBALMACRO": ["ETF", "MISC"],
    "ALTERNATIVES": ["ETF", "MISC"],
    "CTA": ["ETF", "MISC"],
    "ETFS": ["ETF"],
    "ETF": ["ETF"],
}

PORTFOLIO_ASSET_CLASS_COLLAPSE_RULES: List[Tuple[str, str]] = [
    ("Utilities / regulated infrastructure", "INDUSTRIALS; use ENERGY if the thesis is specifically power, gas, LNG, oil, coal, or uranium exposure."),
    ("Telecommunications / digital infrastructure", "TECHNOLOGY."),
    ("Transport, logistics, construction, engineering, civilian aerospace", "INDUSTRIALS."),
    ("Agriculture / agribusiness", "STAPLES unless the thesis is fertilizer, crop chemicals, or hard materials, then MATERIALS."),
    ("Chemicals and materials processing", "MATERIALS."),
    ("Steel and base-metals processing", "BASEMETALS."),
    ("Iron ore", "IRON."),
    ("Rare earths / critical minerals", "REE."),
    ("Coal", "ENERGY."),
    ("Bauxite, alumina, aluminium", "ALUMINIUM."),
    ("Asset managers, brokers, exchanges, diversified financials", "FINANCIALS."),
    ("Pharma and biotech", "PHARMA."),
    ("Healthcare services and medtech", "HEALTHCARE."),
    ("Media, publishing, education, crypto, and uncategorised specialist sleeves", "MISC unless a clearer supported sleeve exists."),
]


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


def _asset_class_vocab_ids(asset_class_vocabulary: List[Dict[str, Any]]) -> List[str]:
    ids: List[str] = []
    seen: set[str] = set()
    for row in asset_class_vocabulary:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        if asset_class and asset_class not in seen:
            ids.append(asset_class)
            seen.add(asset_class)
    return ids


EXPOSURE_BASKET_SPECS: List[Tuple[str, str, List[str]]] = [
    (
        "precious_metals",
        "Physical and miner expressions of monetary/inflation hedges; miners add operating, equity-beta, funding, and jurisdiction risk.",
        ["physical_gold", "gold_miners", "physical_silver", "silver_miners"],
    ),
    (
        "energy_inflation",
        "Oil/gas supply, energy-security, and energy-inflation expressions; producers and direct commodities are related but not interchangeable.",
        ["energy_producers", "energy_commodities", "uranium_miners", "coal_miner", "coal_miners"],
    ),
    (
        "electrification_critical_materials",
        "Electrification, grid, AI-power, and strategic-supply-chain beta; copper, lithium, rare earths, uranium, and base metals should be reconciled as a basket.",
        ["copper_miners", "lithium_miners", "rare_earths_critical_minerals", "base_metals_miners", "diversified_miners", "uranium_miners"],
    ),
    (
        "defensive_equity",
        "Defensive or less-cyclical equity ballast; especially relevant when the memo calls Q3/stagflation or Q4/deflation risks.",
        ["consumer_staples", "healthcare_services", "pharma_biotech", "medtech", "utilities", "insurance"],
    ),
    (
        "ai_power_infrastructure",
        "AI capex, power scarcity, grids, datacentres, semiconductors, and platforms; includes enabling commodities where relevant.",
        ["semiconductors", "technology_platforms", "software_saas", "datacentres", "utilities", "copper_miners", "uranium_miners"],
    ),
]


def _display_by_asset_class(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    display: Dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        if asset_class:
            display[asset_class] = str(row.get("display_name") or asset_class).strip()
    return display


def _cash_asset_class_id(asset_class_vocabulary: List[Dict[str, Any]]) -> str:
    for row in asset_class_vocabulary:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        if _normalize_asset_key(asset_class) == "CASH":
            return asset_class
    return "cash"


def _current_asset_class_rows(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in snapshot.get("asset_classes") or []:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        if not asset_class:
            continue
        current_pct = _clamp_pct(row.get("portfolio_pct"))
        if current_pct <= 0:
            continue
        rows.append(
            {
                "asset_class": asset_class,
                "display_name": str(row.get("display_name") or asset_class).strip(),
                "current_pct": current_pct,
                "value": round(_safe_float(row.get("value")), 2),
            }
        )

    cash_pct = _clamp_pct(((snapshot.get("portfolio") or {}).get("cash_pct")))
    if cash_pct > 0 and not any(_normalize_asset_key(row.get("asset_class")) == "CASH" for row in rows):
        rows.append(
            {
                "asset_class": "cash",
                "display_name": "Cash",
                "current_pct": cash_pct,
                "value": round(_safe_float(((snapshot.get("portfolio") or {}).get("cash_value"))), 2),
            }
        )
    rows.sort(key=lambda row: _safe_float(row.get("current_pct")), reverse=True)
    return rows


def _candidate_new_asset_class_rows(
    *,
    snapshot: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    current_keys = {_normalize_asset_key(row.get("asset_class")) for row in _current_asset_class_rows(snapshot)}
    candidates: List[Dict[str, Any]] = []
    for row in asset_class_vocabulary:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        if not asset_class or _normalize_asset_key(asset_class) in current_keys:
            continue
        candidates.append(
            {
                "asset_class": asset_class,
                "display_name": str(row.get("display_name") or asset_class).strip(),
            }
        )
    return candidates


def _build_exposure_basket_context(
    *,
    snapshot: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    display = _display_by_asset_class(asset_class_vocabulary)
    current_by_key = {
        _normalize_asset_key(row.get("asset_class")): row
        for row in _current_asset_class_rows(snapshot)
        if isinstance(row, dict)
    }
    available_by_key = {
        _normalize_asset_key(row.get("asset_class")): row
        for row in asset_class_vocabulary
        if isinstance(row, dict)
    }
    baskets: List[Dict[str, Any]] = []
    for basket_id, description, asset_classes in EXPOSURE_BASKET_SPECS:
        members: List[Dict[str, Any]] = []
        for asset_class in asset_classes:
            key = _normalize_asset_key(asset_class)
            vocab_row = available_by_key.get(key)
            current_row = current_by_key.get(key, {})
            if not vocab_row and not current_row:
                continue
            canonical = str((vocab_row or current_row).get("asset_class") or asset_class).strip()
            members.append(
                {
                    "asset_class": canonical,
                    "display_name": str(display.get(canonical) or (vocab_row or current_row).get("display_name") or canonical).strip(),
                    "current_pct": _clamp_pct(current_row.get("current_pct")),
                    "currently_held": bool(current_row),
                }
            )
        if members:
            baskets.append(
                {
                    "basket": basket_id,
                    "description": description,
                    "members": members,
                    "current_pct": round(sum(_clamp_pct(row.get("current_pct")) for row in members), 2),
                }
            )
    return baskets


def _build_portfolio_context_packet(
    *,
    snapshot: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
) -> Dict[str, Any]:
    current_rows = _current_asset_class_rows(snapshot)
    candidates = _candidate_new_asset_class_rows(snapshot=snapshot, asset_class_vocabulary=asset_class_vocabulary)
    return {
        "portfolio": snapshot.get("portfolio") if isinstance(snapshot.get("portfolio"), dict) else {},
        "weight_diagnostics": snapshot.get("weight_diagnostics") if isinstance(snapshot.get("weight_diagnostics"), dict) else {},
        "current_asset_classes": current_rows,
        "candidate_new_asset_classes": candidates,
        "exposure_baskets": _build_exposure_basket_context(snapshot=snapshot, asset_class_vocabulary=asset_class_vocabulary),
        "rules": [
            "Current asset classes are actual portfolio exposures; candidate_new_asset_classes are allowed proposals but not currently held.",
            "Every current_asset_classes row must receive an explicit keep/add/trim/exit/review decision somewhere in the final portfolio memo.",
            "Before replacing a current sleeve with a new sleeve, explain whether the new sleeve is a cleaner exposure, a lower-risk implementation, or a genuinely different macro exposure.",
        ],
    }


def _build_asset_class_mapping_guidance(asset_class_vocabulary: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Prompt-facing guardrail: portfolio memos must use portfolio sleeves, not company templates."""
    canonical_ids = _asset_class_vocab_ids(asset_class_vocabulary)
    available = set(canonical_ids)
    supported_examples: List[str] = []
    for source_label, target_rule in PORTFOLIO_ASSET_CLASS_COLLAPSE_RULES:
        if any(_normalize_asset_key(asset_id) in _normalize_asset_key(target_rule) for asset_id in available):
            supported_examples.append(f"{source_label} -> {target_rule}")
    return {
        "canonical_asset_class_ids": canonical_ids,
        "hard_rule": "asset_class must exactly equal one canonical_asset_class_id; display_name must match the corresponding vocabulary row.",
        "collapse_rules": supported_examples,
        "unsupported_label_policy": "If a desired sleeve is not listed, collapse it to the closest canonical parent. Do not output new asset-class labels.",
    }


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

    fallback_text = " ".join(
        [
            str(((evidence_brief.get("market_view") or {}).get("key_messages") or "")),
            str(evidence_brief.get("executive_summary") or ""),
            str(quadrant.get("why_now") or ""),
        ]
    ).upper()
    if "FALLBACK EVIDENCE BRIEF" in fallback_text or "SUMMARY MODEL DID NOT RETURN VALID JSON" in fallback_text:
        quadrant["best_fit"] = str(quadrant.get("best_fit") or "MIXED").strip() or "MIXED"
        quadrant["secondary_fit"] = str(quadrant.get("secondary_fit") or "NONE").strip() or "NONE"
        quadrant["why_now"] = str(quadrant.get("why_now") or "Fallback evidence brief used.").strip()
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
        "Use the macro evidence to form an independent regime view, then allow portfolio-aware allocator lanes to test that view against current exposures. "
        "Do not recommend individual stocks."
    ).strip()


def _evidence_runs_for_mode(mode: str) -> int:
    default_runs = DEFAULT_DEEP_EVIDENCE_RUNS if str(mode or "").strip().lower() == "deep" else DEFAULT_FAST_EVIDENCE_RUNS
    return max(1, min(4, int(default_runs or 1)))


def _build_evidence_lane_specs(query: str, mode: str) -> List[Dict[str, Any]]:
    lane_count = _evidence_runs_for_mode(mode)
    specs: List[Dict[str, Any]] = [
        {
            "label": "macro_regime",
            "focus": "Open-ended macro regime, inflation, rates, FX, liquidity, bonds, oil, gold, credit, growth and recession risk.",
            "query_suffix": (
                "Focus on macro regime evidence first: rates, inflation, oil, USD, bonds, credit stress, liquidity, "
                "growth nowcasts, recession risk, commodity prices, and the dominant 12-24 month portfolio regime."
            ),
            "include_coverage_checklist": False,
        },
        {
            "label": "sector_leadership",
            "focus": "Open-ended sector and asset-class leadership from brokers, banks, strategists, earnings breadth and major investable themes.",
            "query_suffix": (
                "Focus on sector and asset-class leadership: broker/strategist allocation calls, earnings breadth, "
                "AI infrastructure, semiconductors, technology, defence, healthcare, staples, industrials, financials, "
                "materials, and which sleeves deserve capital now."
            ),
            "include_coverage_checklist": False,
        },
        {
            "label": "coverage_audit",
            "focus": "Explicit coverage audit of available portfolio asset classes, including non-obvious beneficiaries and classes with no current evidence.",
            "query_suffix": (
                "Run a coverage audit over the available portfolio asset classes. For each class, look for current evidence that supports "
                "overweight, underweight, hold, watch, or not-relevant treatment. Do not force an allocation where evidence is absent."
            ),
            "include_coverage_checklist": True,
        },
        {
            "label": "cross_asset_risk",
            "focus": "Open-ended cross-asset stress test, geopolitical risk, supply shocks, commodities, defensive sleeves, omissions and contrary evidence.",
            "query_suffix": (
                "Focus on cross-asset risk and omissions: geopolitical shocks, supply chains, energy security, commodity scarcity, "
                "bond/credit/liquidity stress, defensive sleeves, and asset classes that may be wrongly ignored by a narrow macro read."
            ),
            "include_coverage_checklist": False,
        },
    ]
    selected = specs[:lane_count]
    return [
        {
            "label": str(spec["label"]),
            "focus": str(spec["focus"]),
            "query": f"{query} {spec['query_suffix']}".strip(),
            "include_coverage_checklist": bool(spec.get("include_coverage_checklist")),
        }
        for spec in selected
    ]


def _asset_class_coverage_seed(asset_class_vocabulary: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for item in asset_class_vocabulary:
        if not isinstance(item, dict):
            continue
        asset_class = str(item.get("asset_class") or "").strip()
        display_name = str(item.get("display_name") or asset_class).strip()
        if asset_class:
            rows.append({"asset_class": asset_class, "display_name": display_name})
    return rows


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
        "This brief is macro evidence, not a final allocation; portfolio-aware allocator lanes may later compare it against current holdings. "
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


async def _run_single_evidence_lane(
    *,
    query: str,
    mode: str,
    asset_class_vocabulary: List[Dict[str, Any]],
    lane_label: str,
    lane_focus: str,
    include_coverage_checklist: bool,
) -> Dict[str, Any]:
    print(f"stage 1 evidence lane start {lane_label}", flush=True)
    macro_news_task = _fetch_xai_macro_environment_summary(user_query=query)
    research_task = _run_research_lanes(query, mode)
    macro_news, research_pair = await asyncio.gather(macro_news_task, research_task)
    tavily_result, perplexity_result = research_pair
    print(f"stage 1 evidence lane done {lane_label}", flush=True)

    print(f"stage 2 evidence brief start {lane_label}", flush=True)
    evidence_brief = await _build_evidence_brief(
        query=query,
        mode=mode,
        macro_news=macro_news,
        tavily_result=tavily_result,
        perplexity_result=perplexity_result,
        asset_class_vocabulary=asset_class_vocabulary,
        lane_label=lane_label,
        lane_focus=lane_focus,
        include_coverage_checklist=include_coverage_checklist,
    )
    print(f"stage 2 evidence brief done {lane_label}", flush=True)
    return {
        "label": lane_label,
        "focus": lane_focus,
        "query": query,
        "coverage_checklist": include_coverage_checklist,
        "macro_news": macro_news,
        "tavily": tavily_result,
        "perplexity": perplexity_result,
        "evidence_brief": evidence_brief,
        "citations": _dedupe_sources(tavily_result, perplexity_result),
    }


async def _run_evidence_ensemble(
    *,
    query: str,
    mode: str,
    asset_class_vocabulary: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    specs = _build_evidence_lane_specs(query, mode)
    tasks = [
        _run_single_evidence_lane(
            query=str(spec.get("query") or query),
            mode=mode,
            asset_class_vocabulary=asset_class_vocabulary,
            lane_label=str(spec.get("label") or f"evidence_{idx + 1}"),
            lane_focus=str(spec.get("focus") or "Portfolio evidence lane."),
            include_coverage_checklist=bool(spec.get("include_coverage_checklist")),
        )
        for idx, spec in enumerate(specs)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    evidence_runs: List[Dict[str, Any]] = []
    evidence_briefs: List[Dict[str, Any]] = []
    citations_by_key: Dict[str, Dict[str, Any]] = {}

    for idx, result in enumerate(results):
        spec = specs[idx]
        if isinstance(result, Exception):
            evidence_runs.append(
                {
                    "label": str(spec.get("label") or f"evidence_{idx + 1}"),
                    "focus": str(spec.get("focus") or ""),
                    "query": str(spec.get("query") or query),
                    "coverage_checklist": bool(spec.get("include_coverage_checklist")),
                    "error": str(result),
                    "macro_news": {},
                    "tavily": {"provider": "tavily", "error": str(result), "results": [], "research_summary": ""},
                    "perplexity": {"provider": "perplexity", "error": str(result), "results": [], "research_summary": ""},
                    "evidence_brief": {},
                    "citations": [],
                }
            )
            continue
        lane = dict(result)
        evidence_runs.append(lane)
        if isinstance(lane.get("evidence_brief"), dict) and lane.get("evidence_brief"):
            evidence_briefs.append(lane["evidence_brief"])
        for citation in lane.get("citations") or []:
            if not isinstance(citation, dict):
                continue
            key = str(citation.get("url") or citation.get("title") or "").strip()
            if key and key not in citations_by_key:
                citations_by_key[key] = citation

    evidence_brief = await _run_evidence_reconciliation(
        query=query,
        mode=mode,
        evidence_briefs=evidence_briefs,
        asset_class_vocabulary=asset_class_vocabulary,
    )
    citations = list(citations_by_key.values())[:24]
    return evidence_brief, evidence_runs, citations


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
    asset_class_vocabulary: List[Dict[str, Any]],
    lane_label: str = "core",
    lane_focus: str = "General macro and cross-asset evidence.",
    include_coverage_checklist: bool = False,
) -> Dict[str, Any]:
    sources = _dedupe_sources(tavily_result, perplexity_result)
    asset_class_mapping_guidance = _build_asset_class_mapping_guidance(asset_class_vocabulary)
    coverage_seed = _asset_class_coverage_seed(asset_class_vocabulary)
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
            "Use available_asset_class_vocabulary as a coverage checklist, not as an instruction to allocate to every class.",
            "For every available asset class, fill asset_class_coverage with whether the research supports OVERWEIGHT, UNDERWEIGHT, HOLD, WATCH, or NOT_RELEVANT.",
            "An asset class can be NOT_RELEVANT or WATCH if no current evidence supports a meaningful allocation change; do not invent evidence.",
            "If raw research mentions a theme such as AI infrastructure, semiconductors, uranium, defence, energy, gold, or bonds, preserve it in asset_class_coverage even if the allocation implication is only WATCH.",
            "Every asset_class in asset_class_coverage and asset_class_implications must exactly match available_asset_class_vocabulary.",
            "If a source uses an unsupported label, collapse it using asset_class_mapping_guidance.",
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
            "asset_class_coverage": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "stance": "OVERWEIGHT | UNDERWEIGHT | HOLD | WATCH | NOT_RELEVANT",
                    "evidence_strength": "HIGH | MEDIUM | LOW | NONE",
                    "evidence_summary": "string",
                    "source_titles": ["string"],
                    "allocation_relevance": "string"
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
        "evidence_lane": {
            "label": lane_label,
            "focus": lane_focus,
        },
        "available_asset_class_vocabulary": coverage_seed,
        "asset_class_mapping_guidance": asset_class_mapping_guidance,
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
    if not include_coverage_checklist:
        coverage_markers = (
            "Use available_asset_class_vocabulary",
            "For every available asset class",
            "An asset class can be NOT_RELEVANT",
            "If raw research mentions a theme",
            "Every asset_class in asset_class_coverage",
            "If a source uses an unsupported label",
        )
        prompt["rules"] = [
            rule
            for rule in prompt["rules"]
            if not any(str(rule).startswith(marker) for marker in coverage_markers)
        ]
        if isinstance(prompt.get("required_schema"), dict):
            prompt["required_schema"].pop("asset_class_coverage", None)
        prompt.pop("available_asset_class_vocabulary", None)
        prompt.pop("asset_class_mapping_guidance", None)

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
        parsed["evidence_lane"] = {
            "label": lane_label,
            "focus": lane_focus,
            "coverage_checklist": include_coverage_checklist,
        }
        if include_coverage_checklist:
            parsed = _normalize_evidence_asset_coverage(
                parsed,
                asset_class_vocabulary=asset_class_vocabulary,
            )
        else:
            parsed.pop("asset_class_coverage", None)
        parsed["prompt_audit"] = {
            "stage": "evidence_brief",
            "model": summary_model,
            "lane_label": lane_label,
            "coverage_checklist": include_coverage_checklist,
            "prompt": prompt,
        }
        return parsed

    fallback = {
        "summary_model": summary_model,
        "source_count": len(sources),
        "evidence_lane": {
            "label": lane_label,
            "focus": lane_focus,
            "coverage_checklist": include_coverage_checklist,
        },
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
    if include_coverage_checklist:
        fallback["asset_class_coverage"] = [
            {
                "asset_class": row["asset_class"],
                "display_name": row["display_name"],
                "stance": "WATCH",
                "evidence_strength": "NONE",
                "evidence_summary": "Structured evidence summary failed; review raw research lanes.",
                "source_titles": [],
                "allocation_relevance": "Not assessed due to fallback evidence brief.",
            }
            for row in coverage_seed
        ]
        fallback = _normalize_evidence_asset_coverage(
            fallback,
            asset_class_vocabulary=asset_class_vocabulary,
        )
    fallback["prompt_audit"] = {
        "stage": "evidence_brief",
        "model": summary_model,
        "lane_label": lane_label,
        "coverage_checklist": include_coverage_checklist,
        "prompt": prompt,
    }
    return fallback


def _normalize_evidence_asset_coverage(
    evidence_brief: Dict[str, Any],
    *,
    asset_class_vocabulary: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not asset_class_vocabulary:
        return evidence_brief

    allowed_index = _build_allowed_asset_class_index(asset_class_vocabulary)
    by_asset: Dict[str, Dict[str, Any]] = {}
    for row in evidence_brief.get("asset_class_coverage") or []:
        if not isinstance(row, dict):
            continue
        resolved = _resolve_allowed_asset_class(row.get("asset_class") or row.get("display_name"), allowed_index=allowed_index)
        if not resolved:
            continue
        asset_class = str(resolved.get("asset_class") or "").strip()
        if not asset_class:
            continue
        existing = by_asset.get(asset_class, {})
        by_asset[asset_class] = {
            "asset_class": asset_class,
            "display_name": str(resolved.get("display_name") or asset_class).strip(),
            "stance": str(row.get("stance") or existing.get("stance") or "WATCH").strip().upper(),
            "evidence_strength": str(row.get("evidence_strength") or existing.get("evidence_strength") or "LOW").strip().upper(),
            "evidence_summary": str(row.get("evidence_summary") or existing.get("evidence_summary") or "").strip(),
            "source_titles": [
                str(item).strip()
                for item in (row.get("source_titles") if isinstance(row.get("source_titles"), list) else existing.get("source_titles") or [])
                if str(item).strip()
            ][:6],
            "allocation_relevance": str(row.get("allocation_relevance") or existing.get("allocation_relevance") or "").strip(),
        }

    for row in evidence_brief.get("asset_class_implications") or []:
        if not isinstance(row, dict):
            continue
        resolved = _resolve_allowed_asset_class(row.get("asset_class") or row.get("display_name"), allowed_index=allowed_index)
        if not resolved:
            continue
        asset_class = str(resolved.get("asset_class") or "").strip()
        if not asset_class:
            continue
        reason = str(row.get("reason") or row.get("rationale") or "").strip()
        stance = str(row.get("stance") or "WATCH").strip().upper()
        existing = by_asset.get(asset_class, {})
        existing_strength = str(existing.get("evidence_strength") or "").strip().upper()
        if existing_strength in {"", "NONE"} or str(existing.get("stance") or "").strip().upper() == "NOT_RELEVANT":
            by_asset[asset_class] = {
                "asset_class": asset_class,
                "display_name": str(resolved.get("display_name") or asset_class).strip(),
                "stance": stance if stance in {"OVERWEIGHT", "UNDERWEIGHT", "HOLD", "WATCH"} else "WATCH",
                "evidence_strength": "MEDIUM" if reason else "LOW",
                "evidence_summary": reason or "Asset-class implication was present in the evidence packet.",
                "source_titles": [],
                "allocation_relevance": reason or "Evidence packet included this sleeve as allocation-relevant.",
            }

    commodity_text_to_assets: List[Tuple[Tuple[str, ...], List[str]]] = [
        (("GOLD",), ["physical_gold", "gold_miners"]),
        (("SILVER",), ["physical_silver", "silver_miners"]),
        (("COPPER",), ["copper_miners", "base_metals_miners"]),
        (("OIL", "BRENT", "WTI", "GAS", "ENERGY"), ["energy_producers", "energy_commodities"]),
        (("URANIUM", "NUCLEAR"), ["uranium_miners"]),
        (("AGRIC", "WHEAT", "CORN", "SOY", "FOOD"), ["agriculture_agribusiness", "consumer_staples"]),
        (("IRON",), ["iron_ore_miners", "diversified_miners"]),
        (("LITHIUM",), ["lithium_miners"]),
        (("RARE EARTH", "CRITICAL MINERAL"), ["rare_earths_critical_minerals"]),
    ]
    for row in evidence_brief.get("commodity_prices") or []:
        if not isinstance(row, dict):
            continue
        text = " ".join(
            [
                str(row.get("commodity") or ""),
                str(row.get("price_context") or ""),
                str(row.get("portfolio_implication") or ""),
            ]
        ).upper()
        if not text.strip():
            continue
        implication = str(row.get("portfolio_implication") or row.get("price_context") or "").strip()
        for markers, asset_ids in commodity_text_to_assets:
            if not any(marker in text for marker in markers):
                continue
            for asset_id in asset_ids:
                resolved = _resolve_allowed_asset_class(asset_id, allowed_index=allowed_index)
                if not resolved:
                    continue
                asset_class = str(resolved.get("asset_class") or "").strip()
                if not asset_class:
                    continue
                existing = by_asset.get(asset_class, {})
                existing_strength = str(existing.get("evidence_strength") or "").strip().upper()
                if existing_strength in {"HIGH", "MEDIUM"} and str(existing.get("stance") or "").strip().upper() != "NOT_RELEVANT":
                    continue
                by_asset[asset_class] = {
                    "asset_class": asset_class,
                    "display_name": str(resolved.get("display_name") or asset_class).strip(),
                    "stance": "WATCH",
                    "evidence_strength": "LOW",
                    "evidence_summary": implication or f"Commodity evidence referenced {row.get('commodity')}.",
                    "source_titles": [],
                    "allocation_relevance": implication or "Commodity tape is relevant to this sleeve but not sufficient alone for a forced allocation.",
                }

    for row in asset_class_vocabulary:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        if not asset_class or asset_class in by_asset:
            continue
        by_asset[asset_class] = {
            "asset_class": asset_class,
            "display_name": str(row.get("display_name") or asset_class).strip(),
            "stance": "NOT_RELEVANT",
            "evidence_strength": "NONE",
            "evidence_summary": "No material evidence identified in this evidence packet.",
            "source_titles": [],
            "allocation_relevance": "No allocation directive from available evidence.",
        }

    updated = dict(evidence_brief)
    updated["asset_class_coverage"] = list(by_asset.values())
    coverage_rows = [row for row in updated["asset_class_coverage"] if isinstance(row, dict)]
    non_none_rows = [
        row
        for row in coverage_rows
        if str(row.get("evidence_strength") or "").strip().upper() != "NONE"
        or str(row.get("stance") or "").strip().upper() != "NOT_RELEVANT"
    ]
    updated["asset_class_coverage_quality"] = {
        "status": "ok" if non_none_rows else "failed_all_none",
        "row_count": len(coverage_rows),
        "non_none_row_count": len(non_none_rows),
    }
    return updated


def _summarize_evidence_brief(evidence_brief: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "evidence_lane": evidence_brief.get("evidence_lane") if isinstance(evidence_brief.get("evidence_lane"), dict) else {},
        "summary_model": str(evidence_brief.get("summary_model") or "").strip(),
        "source_count": int(evidence_brief.get("source_count") or 0),
        "executive_summary": str(evidence_brief.get("executive_summary") or "").strip(),
        "macro_scorecard": evidence_brief.get("macro_scorecard") if isinstance(evidence_brief.get("macro_scorecard"), dict) else {},
        "market_view": evidence_brief.get("market_view") if isinstance(evidence_brief.get("market_view"), dict) else {},
        "commodity_prices": (evidence_brief.get("commodity_prices") or [])[:14],
        "quadrant_assessment": evidence_brief.get("quadrant_assessment") if isinstance(evidence_brief.get("quadrant_assessment"), dict) else {},
        "broker_themes": (evidence_brief.get("broker_themes") or [])[:10],
        "asset_class_implications": (evidence_brief.get("asset_class_implications") or [])[:18],
        "asset_class_coverage": (evidence_brief.get("asset_class_coverage") or [])[:40],
        "watchpoints": (evidence_brief.get("watchpoints") or [])[:12],
        "source_shortlist": (evidence_brief.get("source_shortlist") or [])[:10],
    }


def _merge_evidence_fallback(
    *,
    evidence_briefs: List[Dict[str, Any]],
    asset_class_vocabulary: List[Dict[str, Any]],
) -> Dict[str, Any]:
    first = dict(evidence_briefs[0]) if evidence_briefs else {}
    source_shortlist: List[Dict[str, Any]] = []
    seen_sources: set[str] = set()
    watchpoints: List[str] = []
    for brief in evidence_briefs:
        for item in brief.get("source_shortlist") or []:
            if not isinstance(item, dict):
                continue
            key = str(item.get("url") or item.get("title") or "").strip()
            if not key or key in seen_sources:
                continue
            seen_sources.add(key)
            source_shortlist.append(item)
        for item in brief.get("watchpoints") or []:
            text = str(item).strip()
            if text and text not in watchpoints:
                watchpoints.append(text)
    first["source_shortlist"] = source_shortlist[:16]
    first["watchpoints"] = watchpoints[:16]
    first["evidence_ensemble"] = {
        "enabled": len(evidence_briefs) > 1,
        "mode": "fallback_first_valid",
        "lane_count": len(evidence_briefs),
        "agreement_summary": "Evidence reconciliation failed or was unnecessary; first valid evidence brief was used with merged sources.",
        "major_disagreements": [],
        "missing_or_weak_coverage": [],
        "material_minority_themes": [],
    }
    return _normalize_evidence_asset_coverage(first, asset_class_vocabulary=asset_class_vocabulary)


async def _run_evidence_reconciliation(
    *,
    query: str,
    mode: str,
    evidence_briefs: List[Dict[str, Any]],
    asset_class_vocabulary: List[Dict[str, Any]],
) -> Dict[str, Any]:
    valid_briefs = [brief for brief in evidence_briefs if isinstance(brief, dict)]
    if not valid_briefs:
        return _normalize_evidence_asset_coverage({}, asset_class_vocabulary=asset_class_vocabulary)
    if len(valid_briefs) == 1:
        single = dict(valid_briefs[0])
        single["evidence_ensemble"] = {
            "enabled": False,
            "mode": "single_evidence_lane",
            "lane_count": 1,
            "agreement_summary": "Single evidence lane used.",
            "major_disagreements": [],
            "missing_or_weak_coverage": [],
            "material_minority_themes": [],
        }
        single.setdefault("prompt_audit", {})
        return _normalize_evidence_asset_coverage(single, asset_class_vocabulary=asset_class_vocabulary)

    summary_model = _summary_model_for_mode(mode)
    asset_class_mapping_guidance = _build_asset_class_mapping_guidance(asset_class_vocabulary)
    coverage_seed = _asset_class_coverage_seed(asset_class_vocabulary)
    prompt = {
        "task": "Reconcile multiple independent portfolio evidence briefs into one robust evidence packet for a macro asset-class allocation memo.",
        "rules": [
            "Return JSON only.",
            "Do not recommend individual stocks.",
            "Do not mechanically average evidence lanes.",
            "Preserve consensus themes, material minority themes, and disagreements.",
            "If a theme appears in only one lane but could materially affect allocation, preserve it as a material minority theme rather than deleting it.",
            "Use available_asset_class_vocabulary as a coverage checklist, not as an instruction to allocate to every class.",
            "For every available asset class, fill asset_class_coverage with OVERWEIGHT, UNDERWEIGHT, HOLD, WATCH, or NOT_RELEVANT.",
            "Asset classes with no supporting evidence should be NOT_RELEVANT or WATCH, not forced into the portfolio.",
            "If any lane mentions AI infrastructure, semiconductors, uranium, defence, energy, gold, bonds, credit stress, or another investable sleeve, explicitly decide whether it is allocation-relevant.",
            "Every asset_class in asset_class_coverage and asset_class_implications must exactly match available_asset_class_vocabulary.",
            "If a source uses an unsupported label, collapse it using asset_class_mapping_guidance.",
            f"Use this exact quadrant framework: Q1 = {QUADRANT_DEFINITIONS['Q1']}; Q2 = {QUADRANT_DEFINITIONS['Q2']}; Q3 = {QUADRANT_DEFINITIONS['Q3']}; Q4 = {QUADRANT_DEFINITIONS['Q4']}.",
            "Do not rotate, rename, or reinterpret the quadrant labels.",
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
            "asset_class_coverage": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "stance": "OVERWEIGHT | UNDERWEIGHT | HOLD | WATCH | NOT_RELEVANT",
                    "evidence_strength": "HIGH | MEDIUM | LOW | NONE",
                    "evidence_summary": "string",
                    "source_titles": ["string"],
                    "allocation_relevance": "string"
                }
            ],
            "watchpoints": ["string"],
            "source_shortlist": [
                {
                    "title": "string",
                    "url": "string",
                    "why_it_matters": "string"
                }
            ],
            "evidence_ensemble": {
                "enabled": True,
                "mode": "reconciled",
                "lane_count": "integer",
                "agreement_summary": "string",
                "major_disagreements": ["string"],
                "missing_or_weak_coverage": ["string"],
                "material_minority_themes": ["string"]
            }
        },
        "user_query": query,
        "mode": mode,
        "available_asset_class_vocabulary": coverage_seed,
        "asset_class_mapping_guidance": asset_class_mapping_guidance,
        "evidence_briefs": [_summarize_evidence_brief(brief) for brief in valid_briefs],
    }
    response = await query_model(
        summary_model,
        [{"role": "user", "content": json.dumps(prompt, ensure_ascii=False, indent=2)}],
        timeout=240.0,
        max_tokens=7000,
        reasoning_effort="medium",
    )
    parsed = _extract_json_object((response or {}).get("content") or "") if isinstance(response, dict) else None
    if not isinstance(parsed, dict):
        return _merge_evidence_fallback(evidence_briefs=valid_briefs, asset_class_vocabulary=asset_class_vocabulary)

    parsed["summary_model"] = summary_model
    parsed["source_count"] = sum(int(brief.get("source_count") or 0) for brief in valid_briefs)
    ensemble = parsed.get("evidence_ensemble") if isinstance(parsed.get("evidence_ensemble"), dict) else {}
    ensemble["enabled"] = True
    ensemble["mode"] = str(ensemble.get("mode") or "reconciled").strip()
    ensemble["lane_count"] = len(valid_briefs)
    ensemble["summary_model"] = summary_model
    parsed["evidence_ensemble"] = ensemble
    parsed["prompt_audit"] = {
        "stage": "evidence_reconciliation",
        "model": summary_model,
        "prompt": prompt,
    }
    return _normalize_evidence_asset_coverage(parsed, asset_class_vocabulary=asset_class_vocabulary)


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
    snapshot: Optional[Dict[str, Any]] = None,
    model_override: Optional[str] = None,
    lane_label: str = "chairman",
    lane_strategy: str = "blind_macro_prior",
) -> Dict[str, Any]:
    chairman_model = str(model_override or _chairman_model_for_mode(mode)).strip()
    asset_class_mapping_guidance = _build_asset_class_mapping_guidance(asset_class_vocabulary)
    portfolio_context = _build_portfolio_context_packet(
        snapshot=snapshot or {},
        asset_class_vocabulary=asset_class_vocabulary,
    )
    cash_asset_id = _cash_asset_class_id(asset_class_vocabulary)
    lane_strategy = str(lane_strategy or "blind_macro_prior").strip().lower()
    task_by_strategy = {
        "blind_macro_prior": "Act as a top-down macro allocator. Build a macro-prior asset-class portfolio before considering the user's current holdings.",
        "portfolio_aware_allocator": "Act as a portfolio-aware macro allocator. Judge the user's current asset-class exposures against the macro evidence before proposing target ranges.",
        "exposure_equivalence_reviewer": "Act as an exposure-equivalence reviewer. Reconcile related sleeves before recommending adds/trims, especially physical metals versus miners and critical-materials baskets.",
        "defensive_regime_reviewer": "Act as a defensive-regime reviewer. Stress-test whether the allocation is coherent with Q2/Q3/Q4 regime risks and the user's current defensive sleeves.",
    }
    strategy_rules: List[str] = []
    if lane_strategy == "blind_macro_prior":
        strategy_rules = [
            "This lane is the only blind macro-prior lane: do not anchor to existing weights when setting initial target ranges.",
            "Still use the provided current portfolio context only to label whether a recommended sleeve is already held or a new proposal.",
        ]
    else:
        strategy_rules = [
            "Start with current_asset_classes and explicitly judge every current nonzero sleeve before proposing new sleeves.",
            "Do not treat candidate_new_asset_classes as current exposure; they are new-sleeve proposals only.",
            "If a current sleeve already expresses the same regime exposure as a new sleeve, compare implementation quality before recommending a replacement.",
            "Do not recommend trimming a current sleeve without naming the substitute exposure or the reason cash/fixed income is superior.",
        ]
    if lane_strategy == "exposure_equivalence_reviewer":
        strategy_rules.extend(
            [
                "Use exposure_baskets as hard reconciliation checks. Do not add copper while trimming lithium or rare earths without explaining why the basket mix should change.",
                "Do not add physical gold or physical silver while ignoring gold_miners or silver_miners; discuss beta, operating risk, valuation, and whether miner exposure partly satisfies the hedge.",
                "Treat energy producers and direct energy commodities as related oil/inflation exposures, not unrelated sleeves.",
            ]
        )
    if lane_strategy == "defensive_regime_reviewer":
        strategy_rules.extend(
            [
                "If the regime call includes stagflation/Q3 or deflation/Q4 risk, explicitly assess consumer_staples, healthcare_services, pharma_biotech, utilities, insurance, cash, fixed_income, and precious metals.",
                "Do not cut defensive sleeves merely because they are not high-beta macro winners; decide whether they provide ballast.",
            ]
        )
    prompt = {
        "task": task_by_strategy.get(lane_strategy, task_by_strategy["blind_macro_prior"]),
        "rules": [
            "Return JSON only.",
            "Work at the asset-class level, not the stock level.",
            "This is not a Q1 overlay memo and not a balanced-fund diversification exercise.",
            *strategy_rules,
            f"Use this exact quadrant framework: Q1 = {QUADRANT_DEFINITIONS['Q1']}; Q2 = {QUADRANT_DEFINITIONS['Q2']}; Q3 = {QUADRANT_DEFINITIONS['Q3']}; Q4 = {QUADRANT_DEFINITIONS['Q4']}.",
            "Do not rotate, rename, or reinterpret the quadrant labels.",
            "Targets should be pragmatic ranges, not false precision.",
            "Think in allocation ranges, not single fixed sizes.",
            "Keep target_pct between min_pct and max_pct.",
            "Concentrated allocations are acceptable when supported by macro evidence.",
            "Cash is one asset-class decision among the others.",
            "Use only the asset classes listed in available_asset_class_vocabulary.",
            "Every asset_class value must exactly match one asset_class from available_asset_class_vocabulary.",
            "If a desired sleeve is not listed, collapse it into the closest supported parent using asset_class_mapping_guidance.",
            "Do not output unsupported company-analysis template labels such as Utilities, Telecommunications, Transport, Construction, Agriculture, Chemicals, Steel, Forestry, Civil Aerospace, Education, Media, Asset Managers, Coal, Crypto, Iron Ore, or Rare Earths.",
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
            f"If strategic_view.cash_target_pct is above zero, include a {cash_asset_id} target row when {cash_asset_id} is available in the asset-class vocabulary.",
            "Do not use EQUITY as a broad residual bucket when more specific supported sleeves are available.",
            "Keep strategic_view notes focused on investable implementation implications, not quadrant label exposition. Do not write notes like 'Q1 is rejected' unless it directly changes allocation.",
            "Use evidence_brief.asset_class_coverage as the coverage audit. It is not a forced allocation list.",
            "If a supported asset class has HIGH or MEDIUM evidence_strength and stance OVERWEIGHT, UNDERWEIGHT, or WATCH, either include it in targets or explain the omission in risk_flags/confidence_note.",
            "If a supported asset class has NOT_RELEVANT or NONE evidence, do not allocate to it merely because it appears in the vocabulary.",
            "Do not skip semiconductors, AI infrastructure, defence, uranium, energy, gold, bonds, or healthcare if the coverage audit says the evidence is allocation-relevant.",
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
            "current_sleeve_actions": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "current_pct": "number",
                    "action": "ADD | HOLD | TRIM | EXIT | REVIEW | WATCH",
                    "related_or_substitute_exposure": "string",
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
        "asset_class_mapping_guidance": asset_class_mapping_guidance,
        "portfolio_context": portfolio_context,
        "lane_strategy": lane_strategy,
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
    parsed["lane_strategy"] = lane_strategy
    parsed["prompt_audit"] = {
        "stage": "allocator_lane",
        "model": chairman_model,
        "lane_label": lane_label,
        "lane_strategy": lane_strategy,
        "prompt": prompt,
    }
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
        "lane_strategy": str(positioning.get("lane_strategy") or "").strip(),
        "executive_summary": str(positioning.get("executive_summary") or "").strip(),
        "strategic_view": positioning.get("strategic_view") if isinstance(positioning.get("strategic_view"), dict) else {},
        "asset_class_targets": rows,
        "current_sleeve_actions": [
            row
            for row in (positioning.get("current_sleeve_actions") or [])[:20]
            if isinstance(row, dict)
        ],
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
    snapshot: Optional[Dict[str, Any]] = None,
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
    asset_class_mapping_guidance = _build_asset_class_mapping_guidance(asset_class_vocabulary)
    portfolio_context = _build_portfolio_context_packet(
        snapshot=snapshot or {},
        asset_class_vocabulary=asset_class_vocabulary,
    )
    cash_asset_id = _cash_asset_class_id(asset_class_vocabulary)
    prompt = {
        "task": "Act as the portfolio-positioning judge. Compare multiple independent allocator outputs built from the same macro evidence and produce one final best-judgement asset-class portfolio.",
        "rules": [
            "Return JSON only.",
            "Work at the asset-class level, not the stock level.",
            f"Use this exact quadrant framework: Q1 = {QUADRANT_DEFINITIONS['Q1']}; Q2 = {QUADRANT_DEFINITIONS['Q2']}; Q3 = {QUADRANT_DEFINITIONS['Q3']}; Q4 = {QUADRANT_DEFINITIONS['Q4']}.",
            "Do not rotate, rename, or reinterpret the quadrant labels.",
            "Use only the provided asset-class vocabulary.",
            "Every asset_class value must exactly match one asset_class from available_asset_class_vocabulary.",
            "If an allocator used an unsupported label, collapse it into the closest supported parent using asset_class_mapping_guidance.",
            "Do not preserve unsupported company-analysis template labels in the final output.",
            "Do not mechanically average the allocators.",
            "Select the most defensible ranges based on evidence quality, internal coherence, and agreement across allocators.",
            "Treat blind_macro_prior as useful but not final. Portfolio-aware and exposure-equivalence lanes should overrule it when they better account for the user's actual exposure.",
            "Every current_asset_classes row with nonzero current_pct must be judged in current_sleeve_actions or included in asset_class_targets.",
            "Do not treat candidate_new_asset_classes as current exposure; label them as new-sleeve proposals if selected.",
            "Use exposure_baskets to prevent contradictory decisions inside a shared regime exposure.",
            "If recommending physical_gold or physical_silver, explicitly decide how existing gold_miners or silver_miners should be handled.",
            "If recommending copper_miners while trimming or ignoring lithium_miners or rare_earths_critical_minerals, explain the basket-level reason.",
            "If Q3/stagflation or Q4/deflation is a live risk, explicitly address staples/healthcare/insurance/cash/fixed income/precious metals as defensive or hedge sleeves.",
            "If allocators disagree, explain the disagreement briefly and then choose a side.",
            "Prefer the output that best matches the evidence brief, commodity tape, rates, and quadrant logic.",
            "Do not invent placeholder target rows just to mention a sleeve.",
            "Concentrated allocations are acceptable when supported by the evidence.",
            f"If strategic_view.cash_target_pct is above zero, include a {cash_asset_id} target row when {cash_asset_id} is available in the asset-class vocabulary.",
            "Do not use EQUITY as a broad residual bucket when more specific supported sleeves are available.",
            "Use evidence_brief.asset_class_coverage to detect material omissions across allocator outputs.",
            "If all allocators omit an asset class with HIGH or MEDIUM evidence_strength and allocation relevance, preserve that as a disagreement/risk note or include the sleeve.",
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
            "current_sleeve_actions": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "current_pct": "number",
                    "action": "ADD | HOLD | TRIM | EXIT | REVIEW | WATCH",
                    "related_or_substitute_exposure": "string",
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
        "asset_class_mapping_guidance": asset_class_mapping_guidance,
        "portfolio_context": portfolio_context,
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
        fallback["judge_prompt_audit"] = {
            "stage": "allocator_judge",
            "model": judge_model,
            "prompt": prompt,
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
    normalized["judge_prompt_audit"] = {
        "stage": "allocator_judge",
        "model": judge_model,
        "prompt": prompt,
    }
    return normalized


async def _run_allocator_council(
    *,
    query: str,
    mode: str,
    evidence_brief: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
    snapshot: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    models = _allocator_council_models_for_mode(mode)
    strategy_cycle = [
        "blind_macro_prior",
        "portfolio_aware_allocator",
        "exposure_equivalence_reviewer",
        "defensive_regime_reviewer",
    ]
    tasks = [
        _run_chairman(
            query=query,
            mode=mode,
            evidence_brief=evidence_brief,
            asset_class_vocabulary=asset_class_vocabulary,
            snapshot=snapshot,
            model_override=model,
            lane_label=f"allocator_{idx + 1}",
            lane_strategy=strategy_cycle[min(idx, len(strategy_cycle) - 1)],
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
        snapshot=snapshot,
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
        "current_sleeve_decisions": [
            row
            for row in (structured.get("current_sleeve_decisions") or [])
            if isinstance(row, dict)
        ][:24],
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
    parsed["prompt_audit"] = {
        "stage": "allocator_commentary",
        "model": commentary_model,
        "prompt": prompt,
    }
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

    enriched_current_decisions: List[Dict[str, Any]] = []
    for row in structured.get("current_sleeve_decisions") or []:
        if not isinstance(row, dict):
            continue
        key = _normalize_asset_key(row.get("asset_class") or row.get("display_name"))
        comment = comments_by_key.get(key, {})
        enriched = dict(row)
        if comment:
            enriched["action"] = str(comment.get("direction") or row.get("action") or "REVIEW").strip().upper()
            enriched["conviction"] = str(comment.get("conviction") or row.get("conviction") or "").strip().upper()
            note = str(comment.get("commentary") or "").strip()
            if note:
                enriched["allocator_commentary"] = note
        enriched_current_decisions.append(enriched)

    updated = dict(structured)
    updated["asset_class_targets"] = enriched_targets
    updated["unmapped_current_asset_classes"] = enriched_unmapped
    updated["current_sleeve_decisions"] = enriched_current_decisions
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
    weight_diagnostics = snapshot.get("weight_diagnostics") if isinstance(snapshot.get("weight_diagnostics"), dict) else {}
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
    if weight_diagnostics.get("all_asset_class_weights_zero"):
        risks.append("Supplied asset-class weights were all zero; portfolio allocation context may be incomplete.")
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
        "weight_diagnostics": weight_diagnostics,
    }


def _build_asset_class_coverage_audit(
    *,
    evidence_brief: Dict[str, Any],
    target_asset_classes: List[str],
) -> Dict[str, Any]:
    target_keys = {_normalize_asset_key(item) for item in target_asset_classes if str(item).strip()}
    material_omissions: List[Dict[str, Any]] = []
    weakly_supported_targets: List[Dict[str, Any]] = []
    coverage_rows = evidence_brief.get("asset_class_coverage") if isinstance(evidence_brief.get("asset_class_coverage"), list) else []
    for row in coverage_rows:
        if not isinstance(row, dict):
            continue
        asset_class = str(row.get("asset_class") or "").strip()
        key = _normalize_asset_key(asset_class)
        stance = str(row.get("stance") or "").strip().upper()
        strength = str(row.get("evidence_strength") or "").strip().upper()
        summary = str(row.get("evidence_summary") or row.get("allocation_relevance") or "").strip()
        is_material = strength in {"HIGH", "MEDIUM"} and stance in {"OVERWEIGHT", "UNDERWEIGHT", "WATCH"}
        if is_material and key not in target_keys:
            material_omissions.append(
                {
                    "asset_class": asset_class,
                    "display_name": str(row.get("display_name") or asset_class).strip(),
                    "stance": stance,
                    "evidence_strength": strength,
                    "reason": summary,
                }
            )
        if key in target_keys and strength in {"NONE", "LOW"} and stance in {"NOT_RELEVANT", "WATCH"}:
            weakly_supported_targets.append(
                {
                    "asset_class": asset_class,
                    "display_name": str(row.get("display_name") or asset_class).strip(),
                    "stance": stance,
                    "evidence_strength": strength,
                    "reason": summary,
                }
            )
    return {
        "material_omissions": material_omissions[:12],
        "weakly_supported_targets": weakly_supported_targets[:12],
        "coverage_row_count": len([row for row in coverage_rows if isinstance(row, dict)]),
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
    current_by_key: Dict[str, Dict[str, Any]] = {}
    for row in snapshot.get("asset_classes") or []:
        if not isinstance(row, dict):
            continue
        key = str(row.get("asset_class") or "").strip()
        if key:
            current_by_class[key] = row
            current_by_key[_normalize_asset_key(key)] = row
    cash_asset_class = "cash"
    cash_display_name = "Cash"
    cash_row = {
        "asset_class": cash_asset_class,
        "display_name": cash_display_name,
        "portfolio_pct": _clamp_pct(((snapshot.get("portfolio") or {}).get("cash_pct"))),
    }
    current_by_class.setdefault(
        cash_asset_class,
        {
            **cash_row,
        },
    )
    current_by_key.setdefault("CASH", cash_row)

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
        current_row = current_by_class.get(asset_class) or current_by_key.get(_normalize_asset_key(asset_class), {})
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

    cash_target = _clamp_pct(((macro_positioning.get("strategic_view") or {}).get("cash_target_pct")) if isinstance(macro_positioning.get("strategic_view"), dict) else 0)
    has_cash_target = any(_normalize_asset_key(row.get("asset_class")) == "CASH" for row in merged_targets)
    if cash_target > 0 and not has_cash_target:
        current_row = current_by_key.get("CASH", {})
        current_pct = _clamp_pct(current_row.get("portfolio_pct"))
        min_pct = max(0.0, round(cash_target - 3.0, 2))
        max_pct = min(100.0, round(cash_target + 3.0, 2))
        action = _default_action_from_range(current_pct=current_pct, min_pct=min_pct, max_pct=max_pct)
        merged_targets.append(
            {
                "asset_class": cash_asset_class,
                "display_name": cash_display_name,
                "current_pct": current_pct,
                "min_pct": min_pct,
                "target_pct": cash_target,
                "max_pct": max_pct,
                "thesis_role": "hedge",
                "action": action,
                "rationale": str(((macro_positioning.get("strategic_view") or {}).get("cash_role")) or "Liquidity reserve and dry powder.").strip(),
                "implementation_priority": "high" if cash_target >= 10 else "medium",
                "conviction": "",
            }
        )

    known = {_normalize_asset_key(row.get("asset_class")) for row in merged_targets if isinstance(row, dict)}
    for asset_class, current_row in current_by_class.items():
        if _normalize_asset_key(asset_class) in known:
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

    model_actions_by_key: Dict[str, Dict[str, Any]] = {}
    for row in macro_positioning.get("current_sleeve_actions") or []:
        if not isinstance(row, dict):
            continue
        key = _normalize_asset_key(row.get("asset_class") or row.get("display_name"))
        if key:
            model_actions_by_key[key] = row

    current_sleeve_decisions: List[Dict[str, Any]] = []
    for row in merged_targets:
        if not isinstance(row, dict):
            continue
        current_pct = _clamp_pct(row.get("current_pct"))
        if current_pct <= 0:
            continue
        current_sleeve_decisions.append(
            {
                "asset_class": str(row.get("asset_class") or "").strip(),
                "display_name": str(row.get("display_name") or row.get("asset_class") or "").strip(),
                "current_pct": current_pct,
                "min_pct": _clamp_pct(row.get("min_pct")),
                "target_pct": _clamp_pct(row.get("target_pct")),
                "max_pct": _clamp_pct(row.get("max_pct")),
                "action": str(row.get("action") or "HOLD").strip().upper(),
                "related_or_substitute_exposure": "",
                "rationale": str(row.get("rationale") or "").strip(),
                "source": "target_row",
            }
        )
    for row in unmapped_current_asset_classes:
        if not isinstance(row, dict):
            continue
        key = _normalize_asset_key(row.get("asset_class") or row.get("display_name"))
        model_action = model_actions_by_key.get(key, {})
        action = str(model_action.get("action") or row.get("action") or "REVIEW").strip().upper()
        rationale = str(model_action.get("rationale") or row.get("rationale") or "").strip()
        current_sleeve_decisions.append(
            {
                "asset_class": str(row.get("asset_class") or "").strip(),
                "display_name": str(row.get("display_name") or row.get("asset_class") or "").strip(),
                "current_pct": _clamp_pct(row.get("current_pct")),
                "min_pct": 0.0,
                "target_pct": _clamp_pct(row.get("current_pct")),
                "max_pct": _clamp_pct(row.get("current_pct")),
                "action": action,
                "related_or_substitute_exposure": str(model_action.get("related_or_substitute_exposure") or "").strip(),
                "rationale": rationale,
                "source": "current_only",
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

    coverage_audit = _build_asset_class_coverage_audit(
        evidence_brief=evidence_brief,
        target_asset_classes=[
            str(row.get("asset_class") or "").strip()
            for row in merged_targets
            if isinstance(row, dict)
        ],
    )
    risk_flags = [
        str(item).strip()
        for item in (macro_positioning.get("risk_flags") or [])
        if str(item).strip()
    ]
    coverage_quality = evidence_brief.get("asset_class_coverage_quality") if isinstance(evidence_brief.get("asset_class_coverage_quality"), dict) else {}
    if str(coverage_quality.get("status") or "").strip() == "failed_all_none":
        risk_flags.append(
            "Evidence coverage failed: every available asset class was marked NOT_RELEVANT/NONE, so final positioning must rely on macro narrative and portfolio-aware reconciliation rather than the coverage table."
        )
    for row in coverage_audit.get("material_omissions") or []:
        if not isinstance(row, dict):
            continue
        label = str(row.get("display_name") or row.get("asset_class") or "").strip()
        stance = str(row.get("stance") or "").strip()
        strength = str(row.get("evidence_strength") or "").strip()
        if label:
            risk_flags.append(f"Coverage audit: {label} had {strength} {stance} evidence but was not included in final targets.")
    for row in coverage_audit.get("weakly_supported_targets") or []:
        if not isinstance(row, dict):
            continue
        label = str(row.get("display_name") or row.get("asset_class") or "").strip()
        strength = str(row.get("evidence_strength") or "").strip()
        if label:
            risk_flags.append(f"Coverage audit: {label} appears in final targets despite only {strength} direct evidence.")

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
        "current_sleeve_decisions": current_sleeve_decisions,
        "suggested_new_asset_classes": macro_positioning.get("suggested_new_asset_classes") if isinstance(macro_positioning.get("suggested_new_asset_classes"), list) else [],
        "current_vs_ideal": {
            "main_overweights": overweights[:8],
            "main_underweights": underweights[:8],
            "aligned": aligned[:8],
        },
        "asset_class_coverage_audit": coverage_audit,
        "implementation_notes": implementation_notes,
        "monitoring_triggers": macro_positioning.get("monitoring_triggers") if isinstance(macro_positioning.get("monitoring_triggers"), list) else [],
        "risk_flags": risk_flags,
        "confidence_note": str(macro_positioning.get("confidence_note") or "").strip(),
        "chairman_model": str(macro_positioning.get("chairman_model") or macro_positioning.get("judge_model") or DEFAULT_CHAIRMAN_MODEL).strip(),
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

    def _is_placeholder_value(value: Any) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return True
        return text in {
            "see research shortlist.",
            "see research shortlist",
            "see source shortlist and watchpoints.",
            "see source shortlist and watchpoints",
            "insufficient structured summary output.",
            "insufficient structured summary output",
            "fallback evidence brief used.",
            "fallback evidence brief used",
        }

    def _is_quadrant_label_note(value: Any) -> bool:
        text = str(value or "").strip().upper()
        return bool(re.match(r"^Q[1-4]\\b", text))

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
    ensemble_consensus = structured.get("portfolio_memo_ensemble_consensus") if isinstance(structured.get("portfolio_memo_ensemble_consensus"), dict) else {}
    quadrant_assessment = structured.get("quadrant_assessment") if isinstance(structured.get("quadrant_assessment"), dict) else {}
    macro_scorecard = structured.get("macro_scorecard") if isinstance(structured.get("macro_scorecard"), dict) else {}
    commodity_prices = structured.get("commodity_prices") if isinstance(structured.get("commodity_prices"), list) else []
    broker_themes = structured.get("broker_themes") if isinstance(structured.get("broker_themes"), list) else []
    unmapped_current_asset_classes = structured.get("unmapped_current_asset_classes") if isinstance(structured.get("unmapped_current_asset_classes"), list) else []
    current_sleeve_decisions = structured.get("current_sleeve_decisions") if isinstance(structured.get("current_sleeve_decisions"), list) else []
    coverage_audit = structured.get("asset_class_coverage_audit") if isinstance(structured.get("asset_class_coverage_audit"), dict) else {}
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
            if note_text and not _is_quadrant_label_note(note_text):
                lines.append(f"- {note_text}")
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

    if ensemble_consensus:
        lines.append("## Memo Ensemble Consensus")
        lines.append("")
        agreement_summary = str(ensemble_consensus.get("agreement_summary") or "").strip()
        synthesis_model = str(ensemble_consensus.get("synthesis_model") or "").strip()
        if synthesis_model:
            lines.append(f"- Synthesis model: {synthesis_model}")
        if agreement_summary:
            lines.append(f"- Agreement: {agreement_summary}")
        for key, label in (
            ("major_disagreements", "Disagreement"),
            ("chosen_positions", "Accepted"),
            ("rejected_positions", "Rejected"),
        ):
            values = ensemble_consensus.get(key) if isinstance(ensemble_consensus.get(key), list) else []
            for item in values[:6]:
                text = str(item).strip()
                if text:
                    lines.append(f"- {label}: {text}")
        lines.append("")

    if macro_scorecard:
        score_lines: List[str] = []
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
            if value and not _is_placeholder_value(value):
                score_lines.append(f"- {label}: {value}")
        if score_lines:
            lines.append("## Macro Scorecard")
            lines.append("")
            lines.extend(score_lines)
            lines.append("")

    if quadrant_assessment:
        best_fit = str(quadrant_assessment.get("best_fit") or "").strip()
        secondary_fit = str(quadrant_assessment.get("secondary_fit") or "").strip()
        primary_risk = str(quadrant_assessment.get("primary_risk") or "").strip()
        secondary_risk = str(quadrant_assessment.get("secondary_risk") or "").strip()
        why_now = str(quadrant_assessment.get("why_now") or "").strip()
        quadrant_lines: List[str] = []
        if best_fit and best_fit != "MIXED" and not _is_placeholder_value(why_now):
            quadrant_lines.append(f"- Best fit: {best_fit}")
        if secondary_fit and secondary_fit not in {"NONE", "MIXED"} and not _is_placeholder_value(why_now):
            quadrant_lines.append(f"- Secondary fit: {secondary_fit}")
        if primary_risk:
            quadrant_lines.append(f"- Primary regime risk: {primary_risk}")
        if secondary_risk:
            quadrant_lines.append(f"- Secondary regime risk: {secondary_risk}")
        if why_now and not _is_placeholder_value(why_now):
            quadrant_lines.append(f"- Why now: {why_now}")
        if quadrant_lines:
            lines.append("## Quadrant Assessment")
            lines.append("")
            lines.extend(quadrant_lines)
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

    if coverage_audit:
        material_omissions = coverage_audit.get("material_omissions") if isinstance(coverage_audit.get("material_omissions"), list) else []
        weak_targets = coverage_audit.get("weakly_supported_targets") if isinstance(coverage_audit.get("weakly_supported_targets"), list) else []
        if material_omissions or weak_targets:
            lines.append("## Asset Class Coverage Audit")
            lines.append("")
            for row in material_omissions[:8]:
                if not isinstance(row, dict):
                    continue
                name = _memo_asset_label(row)
                stance = str(row.get("stance") or "").strip()
                strength = str(row.get("evidence_strength") or "").strip()
                reason = str(row.get("reason") or "").strip()
                if name:
                    lines.append(f"- Omitted despite evidence: {name} ({strength} {stance}). {reason}")
            for row in weak_targets[:8]:
                if not isinstance(row, dict):
                    continue
                name = _memo_asset_label(row)
                strength = str(row.get("evidence_strength") or "").strip()
                reason = str(row.get("reason") or "").strip()
                if name:
                    lines.append(f"- Weak direct evidence in targets: {name} ({strength}). {reason}")
            lines.append("")

    if current_sleeve_decisions:
        lines.append("## Current Sleeve Decisions")
        lines.append("")
        lines.append("| Current Sleeve | Current | Direction | Related / Substitute | Rationale |")
        lines.append("| --- | ---: | --- | --- | --- |")
        for row in current_sleeve_decisions[:24]:
            if not isinstance(row, dict):
                continue
            name = _memo_asset_label(row)
            if not name:
                continue
            current_pct = _clamp_pct(row.get("current_pct"))
            action = str(row.get("action") or "REVIEW").strip().upper()
            related = str(row.get("related_or_substitute_exposure") or "").strip()
            rationale = str(row.get("allocator_commentary") or row.get("rationale") or "").strip()
            lines.append(f"| {name} | {current_pct:.1f}% | {action} | {related or 'n/a'} | {rationale or 'Needs explicit reassessment.'} |")
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
            name = _memo_asset_label(row)
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
            name = _memo_asset_label(row)
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
            name = _memo_asset_label(row)
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
            name = _memo_asset_label(row)
            target_pct = _clamp_pct(row.get("target_pct"))
            rationale = str(row.get("rationale") or "").strip()
            lines.append(f"- **{name}**: {target_pct:.1f}% target. {rationale}")
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

    return "\n".join(lines).strip() + "\n"


def _ensemble_runs_for_mode(mode: str) -> int:
    default_runs = DEFAULT_DEEP_ENSEMBLE_RUNS if str(mode or "").strip().lower() == "deep" else DEFAULT_FAST_ENSEMBLE_RUNS
    return max(1, min(5, int(default_runs or 1)))


async def _run_single_portfolio_memo_lane(
    *,
    query: str,
    mode: str,
    snapshot: Dict[str, Any],
    evidence_brief: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
    citations: List[Dict[str, Any]],
    lane_label: str,
) -> Dict[str, Any]:
    macro_positioning, allocator_council_runs = await _run_allocator_council(
        query=query,
        mode=mode,
        evidence_brief=evidence_brief,
        asset_class_vocabulary=asset_class_vocabulary,
        snapshot=snapshot,
    )
    structured = _merge_positioning_with_snapshot(
        snapshot=snapshot,
        macro_positioning=macro_positioning,
        evidence_brief=evidence_brief,
        query=query,
        mode=mode,
    )
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
    markdown = _render_markdown(
        snapshot=snapshot,
        structured=structured,
        evidence_brief=evidence_brief,
        citations=citations,
    )
    return {
        "lane": lane_label,
        "macro_positioning": macro_positioning,
        "allocator_council_runs": allocator_council_runs,
        "allocator_commentary": allocator_commentary,
        "structured_data": structured,
        "markdown": markdown,
    }


def _summarize_portfolio_memo_lane(lane: Dict[str, Any]) -> Dict[str, Any]:
    structured = lane.get("structured_data") if isinstance(lane.get("structured_data"), dict) else {}
    targets = []
    for row in (structured.get("asset_class_targets") or [])[:18]:
        if not isinstance(row, dict):
            continue
        targets.append(
            {
                "asset_class": str(row.get("asset_class") or "").strip(),
                "display_name": str(row.get("display_name") or row.get("asset_class") or "").strip(),
                "current_pct": _clamp_pct(row.get("current_pct")),
                "min_pct": _clamp_pct(row.get("min_pct")),
                "target_pct": _clamp_pct(row.get("target_pct")),
                "max_pct": _clamp_pct(row.get("max_pct")),
                "action": str(row.get("action") or "").strip().upper(),
                "conviction": str(row.get("conviction") or "").strip().upper(),
                "thesis_role": str(row.get("thesis_role") or "").strip(),
                "rationale": str(row.get("rationale") or row.get("allocator_commentary") or "").strip(),
            }
        )
    council = structured.get("allocator_council") if isinstance(structured.get("allocator_council"), dict) else {}
    return {
        "lane": str(lane.get("lane") or "").strip(),
        "executive_summary": str(structured.get("executive_summary") or "").strip(),
        "strategic_view": structured.get("strategic_view") if isinstance(structured.get("strategic_view"), dict) else {},
        "asset_class_targets": targets,
        "current_sleeve_actions": [
            row
            for row in (structured.get("current_sleeve_decisions") or structured.get("current_sleeve_actions") or [])[:24]
            if isinstance(row, dict)
        ],
        "unmapped_current_asset_classes": [
            row
            for row in (structured.get("unmapped_current_asset_classes") or [])[:16]
            if isinstance(row, dict)
        ],
        "implementation_notes": [
            str(item).strip()
            for item in (structured.get("implementation_notes") or [])[:8]
            if str(item).strip()
        ],
        "risk_flags": [
            str(item).strip()
            for item in (structured.get("risk_flags") or [])[:8]
            if str(item).strip()
        ],
        "confidence_note": str(structured.get("confidence_note") or "").strip(),
        "allocator_council": {
            "models": council.get("models") if isinstance(council.get("models"), list) else [],
            "consensus_summary": str(council.get("consensus_summary") or "").strip(),
            "disagreement_notes": council.get("disagreement_notes") if isinstance(council.get("disagreement_notes"), list) else [],
        },
    }


def _allocator_models_from_memo_lanes(lanes: List[Dict[str, Any]]) -> List[str]:
    models: List[str] = []
    seen: set[str] = set()
    for lane in lanes:
        if not isinstance(lane, dict):
            continue
        structured = lane.get("structured_data") if isinstance(lane.get("structured_data"), dict) else {}
        council = structured.get("allocator_council") if isinstance(structured.get("allocator_council"), dict) else {}
        candidates = council.get("models") if isinstance(council.get("models"), list) else []
        if not candidates:
            candidates = [
                str(run.get("chairman_model") or run.get("model") or "").strip()
                for run in (lane.get("allocator_council_runs") or [])
                if isinstance(run, dict)
            ]
        for model in candidates:
            key = str(model or "").strip()
            if key and key not in seen:
                seen.add(key)
                models.append(key)
    return models


async def _run_portfolio_memo_synthesis(
    *,
    query: str,
    mode: str,
    snapshot: Dict[str, Any],
    evidence_brief: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
    lanes: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    valid_lanes = [lane for lane in lanes if isinstance(lane, dict) and isinstance(lane.get("structured_data"), dict)]
    if not valid_lanes:
        return _fallback_macro_positioning(query, mode, evidence_brief), {
            "mode": "fallback_no_lanes",
            "confidence": "LOW",
            "agreement_summary": "No valid memo lanes returned.",
            "major_disagreements": [],
            "chosen_positions": [],
            "rejected_positions": [],
        }
    if len(valid_lanes) == 1:
        macro = valid_lanes[0].get("macro_positioning") if isinstance(valid_lanes[0].get("macro_positioning"), dict) else {}
        return dict(macro), {
            "mode": "single_lane",
            "confidence": "MEDIUM",
            "agreement_summary": "Only one portfolio memo lane was run.",
            "major_disagreements": [],
            "chosen_positions": [],
            "rejected_positions": [],
        }

    synthesis_model = DEFAULT_SYNTHESIS_MODEL or _chairman_model_for_mode(mode)
    asset_class_mapping_guidance = _build_asset_class_mapping_guidance(asset_class_vocabulary)
    portfolio_context = _build_portfolio_context_packet(
        snapshot=snapshot,
        asset_class_vocabulary=asset_class_vocabulary,
    )
    cash_asset_id = _cash_asset_class_id(asset_class_vocabulary)
    prompt = {
        "task": "Synthesize three independent portfolio-positioning memos into one final macro-positioning decision.",
        "rules": [
            "Return JSON only.",
            "Use the shared evidence brief as the source of truth.",
            "Do not mechanically average target ranges.",
            "Prefer allocations that are supported by evidence and appear across multiple lanes, but keep a superior minority view if it better matches the evidence.",
            "Treat blind macro-prior lanes as only one input; the final answer must be portfolio-aware.",
            "Every current_asset_classes row with nonzero current_pct must be judged in current_sleeve_actions or included in asset_class_targets.",
            "Do not treat candidate_new_asset_classes as current exposure; label them as new-sleeve proposals if selected.",
            "Use exposure_baskets to prevent contradictory decisions inside the same regime exposure.",
            "If recommending physical_gold or physical_silver, explicitly decide how existing gold_miners or silver_miners should be handled.",
            "If recommending copper_miners while reducing or ignoring lithium_miners or rare_earths_critical_minerals, explain the critical-materials basket logic.",
            "If Q3/stagflation or Q4/deflation is a live risk, explicitly account for staples, healthcare, insurance, cash, fixed income, and precious metals.",
            "Preserve meaningful disagreement in portfolio_memo_ensemble_consensus.major_disagreements.",
            "Use only the provided asset-class vocabulary.",
            "Every asset_class value must exactly match one asset_class from available_asset_class_vocabulary.",
            "If memo lanes used unsupported labels, collapse them into the closest supported parent using asset_class_mapping_guidance.",
            "Do not preserve unsupported company-analysis template labels in the final output.",
            "Do not recommend individual securities.",
            "Do not emit placeholder 0-0-0 target rows.",
            "The output should be a final macro_positioning object, not a review essay.",
            f"If strategic_view.cash_target_pct is above zero, include a {cash_asset_id} target row when {cash_asset_id} is available in the asset-class vocabulary.",
            "Do not use EQUITY as a broad residual bucket when more specific supported sleeves are available.",
            "Keep strategic_view notes focused on investable implementation implications, not quadrant label exposition. Do not write notes like 'Q1 is rejected' unless it directly changes allocation.",
            "Use evidence_brief.asset_class_coverage to detect material omissions across memo lanes.",
            "A material minority theme from the evidence ensemble can beat majority allocator agreement if it is better supported by sources.",
            "If a high-relevance asset class is excluded from final targets, explain why in portfolio_memo_ensemble_consensus.rejected_positions or risk_flags.",
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
            "current_sleeve_actions": [
                {
                    "asset_class": "string",
                    "display_name": "string",
                    "current_pct": "number",
                    "action": "ADD | HOLD | TRIM | EXIT | REVIEW | WATCH",
                    "related_or_substitute_exposure": "string",
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
            "portfolio_memo_ensemble_consensus": {
                "mode": "synthesized",
                "confidence": "HIGH | MEDIUM | LOW",
                "agreement_summary": "string",
                "major_disagreements": ["string"],
                "chosen_positions": ["string"],
                "rejected_positions": ["string"]
            }
        },
        "user_query": query,
        "mode": mode,
        "evidence_brief": evidence_brief,
        "available_asset_class_vocabulary": asset_class_vocabulary,
        "asset_class_mapping_guidance": asset_class_mapping_guidance,
        "portfolio_context": portfolio_context,
        "memo_lanes": [_summarize_portfolio_memo_lane(lane) for lane in valid_lanes],
    }
    response = await query_model(
        synthesis_model,
        [{"role": "user", "content": json.dumps(prompt, ensure_ascii=False, indent=2)}],
        timeout=240.0,
        max_tokens=9000,
        reasoning_effort="medium",
    )
    parsed = _extract_json_object((response or {}).get("content") or "") if isinstance(response, dict) else None
    if not isinstance(parsed, dict):
        fallback = dict(valid_lanes[0].get("macro_positioning") or {})
        return fallback, {
            "mode": "fallback_first_valid",
            "confidence": "LOW",
            "agreement_summary": "Synthesis stage did not return valid JSON, so the first valid memo lane was used.",
            "major_disagreements": [],
            "chosen_positions": [],
            "rejected_positions": [],
            "synthesis_model": synthesis_model,
            "prompt_audit": {
                "stage": "portfolio_memo_synthesis",
                "model": synthesis_model,
                "prompt": prompt,
            },
        }

    consensus = parsed.pop("portfolio_memo_ensemble_consensus", {})
    if not isinstance(consensus, dict):
        consensus = {}
    consensus["mode"] = str(consensus.get("mode") or "synthesized").strip()
    consensus["synthesis_model"] = synthesis_model
    consensus["lane_count"] = len(valid_lanes)
    consensus["prompt_audit"] = {
        "stage": "portfolio_memo_synthesis",
        "model": synthesis_model,
        "prompt": prompt,
    }
    parsed["analysis_kind"] = "portfolio_positioning"
    parsed["analysis_date"] = _utc_now_iso()
    parsed["mode"] = mode
    parsed["query"] = query
    parsed["judge_model"] = synthesis_model
    parsed["allocator_council"] = {
        "mode": "memo_ensemble_synthesis",
        "models": _allocator_models_from_memo_lanes(valid_lanes),
        "consensus_summary": str(consensus.get("agreement_summary") or "").strip(),
        "disagreement_notes": consensus.get("major_disagreements") if isinstance(consensus.get("major_disagreements"), list) else [],
    }
    return _normalize_macro_positioning_taxonomy(parsed, asset_class_vocabulary=asset_class_vocabulary), consensus


async def _run_portfolio_memo_ensemble(
    *,
    query: str,
    mode: str,
    snapshot: Dict[str, Any],
    evidence_brief: Dict[str, Any],
    asset_class_vocabulary: List[Dict[str, Any]],
    citations: List[Dict[str, Any]],
    ensemble_runs: int,
) -> Dict[str, Any]:
    tasks = [
        _run_single_portfolio_memo_lane(
            query=query,
            mode=mode,
            snapshot=snapshot,
            evidence_brief=evidence_brief,
            asset_class_vocabulary=asset_class_vocabulary,
            citations=citations,
            lane_label=f"memo_{idx + 1}",
        )
        for idx in range(max(1, ensemble_runs))
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    lanes: List[Dict[str, Any]] = []
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            lanes.append(
                {
                    "lane": f"memo_{idx + 1}",
                    "error": str(result),
                    "structured_data": {},
                    "macro_positioning": {},
                    "allocator_council_runs": [],
                    "allocator_commentary": {},
                    "markdown": "",
                }
            )
        else:
            lanes.append(result)

    macro_positioning, consensus = await _run_portfolio_memo_synthesis(
        query=query,
        mode=mode,
        snapshot=snapshot,
        evidence_brief=evidence_brief,
        asset_class_vocabulary=asset_class_vocabulary,
        lanes=lanes,
    )
    structured = _merge_positioning_with_snapshot(
        snapshot=snapshot,
        macro_positioning=macro_positioning,
        evidence_brief=evidence_brief,
        query=query,
        mode=mode,
    )
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
    structured["portfolio_memo_ensemble_consensus"] = consensus
    markdown = _render_markdown(
        snapshot=snapshot,
        structured=structured,
        evidence_brief=evidence_brief,
        citations=citations,
    )
    return {
        "macro_positioning": macro_positioning,
        "structured_data": structured,
        "allocator_commentary": allocator_commentary,
        "markdown": markdown,
        "portfolio_memo_ensemble": {
            "enabled": True,
            "lane_count": len(lanes),
            "shared_evidence_brief": True,
            "synthesis_model": str(consensus.get("synthesis_model") or DEFAULT_SYNTHESIS_MODEL).strip(),
            "consensus": consensus,
            "lanes": [
                {
                    "lane": str(lane.get("lane") or "").strip(),
                    "error": str(lane.get("error") or "").strip(),
                    "structured_data": lane.get("structured_data") if isinstance(lane.get("structured_data"), dict) else {},
                    "macro_positioning": lane.get("macro_positioning") if isinstance(lane.get("macro_positioning"), dict) else {},
                    "allocator_council_runs": lane.get("allocator_council_runs") if isinstance(lane.get("allocator_council_runs"), list) else [],
                    "allocator_commentary": lane.get("allocator_commentary") if isinstance(lane.get("allocator_commentary"), dict) else {},
                    "markdown": str(lane.get("markdown") or ""),
                }
                for lane in lanes
            ],
        },
    }


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
    evidence_brief, evidence_runs, citations = await _run_evidence_ensemble(
        query=query,
        mode=mode,
        asset_class_vocabulary=asset_class_vocabulary,
    )
    print("stage 1 done", flush=True)

    print("stage 2 start", flush=True)
    print(
        f"stage 2 evidence ensemble done lanes={len(evidence_runs)}",
        flush=True,
    )
    print("stage 2 done", flush=True)

    first_evidence_run = evidence_runs[0] if evidence_runs else {}
    macro_news = first_evidence_run.get("macro_news") if isinstance(first_evidence_run.get("macro_news"), dict) else {}
    tavily_result = first_evidence_run.get("tavily") if isinstance(first_evidence_run.get("tavily"), dict) else {}
    perplexity_result = first_evidence_run.get("perplexity") if isinstance(first_evidence_run.get("perplexity"), dict) else {}
    ensemble_runs = _ensemble_runs_for_mode(mode)
    print("stage 3 start", flush=True)
    if ensemble_runs > 1:
        print(f"stage 3 ensemble start lanes={ensemble_runs}", flush=True)
        ensemble_result = await _run_portfolio_memo_ensemble(
            query=query,
            mode=mode,
            snapshot=snapshot,
            evidence_brief=evidence_brief,
            asset_class_vocabulary=asset_class_vocabulary,
            citations=citations,
            ensemble_runs=ensemble_runs,
        )
        macro_positioning = ensemble_result["macro_positioning"]
        structured = ensemble_result["structured_data"]
        allocator_commentary = ensemble_result["allocator_commentary"]
        markdown = ensemble_result["markdown"]
        portfolio_memo_ensemble = ensemble_result["portfolio_memo_ensemble"]
        allocator_council_runs = [
            {
                "lane": str(lane.get("lane") or "").strip(),
                "error": str(lane.get("error") or "").strip(),
                "allocator_council_runs": lane.get("allocator_council_runs") if isinstance(lane.get("allocator_council_runs"), list) else [],
            }
            for lane in portfolio_memo_ensemble.get("lanes", [])
            if isinstance(lane, dict)
        ]
        print("stage 3 ensemble done", flush=True)
    else:
        single_lane = await _run_single_portfolio_memo_lane(
            query=query,
            mode=mode,
            snapshot=snapshot,
            evidence_brief=evidence_brief,
            asset_class_vocabulary=asset_class_vocabulary,
            citations=citations,
            lane_label="memo_1",
        )
        macro_positioning = single_lane["macro_positioning"]
        structured = single_lane["structured_data"]
        allocator_commentary = single_lane["allocator_commentary"]
        markdown = single_lane["markdown"]
        allocator_council_runs = single_lane["allocator_council_runs"]
        portfolio_memo_ensemble = {
            "enabled": False,
            "lane_count": 1,
            "shared_evidence_brief": True,
            "synthesis_model": "",
            "consensus": {
                "mode": "single_lane",
                "confidence": "MEDIUM",
                "agreement_summary": "Single memo lane used.",
                "major_disagreements": [],
                "chosen_positions": [],
                "rejected_positions": [],
            },
            "lanes": [
                {
                    "lane": "memo_1",
                    "error": "",
                    "structured_data": structured,
                    "macro_positioning": macro_positioning,
                    "allocator_council_runs": allocator_council_runs,
                    "allocator_commentary": allocator_commentary,
                    "markdown": markdown,
                }
            ],
        }
        print("stage 3 single memo done", flush=True)

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
            "evidence_ensemble": {
                "enabled": len(evidence_runs) > 1,
                "lane_count": len(evidence_runs),
                "lanes": evidence_runs,
            },
        },
        "portfolio_context_diagnostics": snapshot.get("weight_diagnostics") if isinstance(snapshot.get("weight_diagnostics"), dict) else {},
        "evidence_runs": evidence_runs,
        "evidence_brief": evidence_brief,
        "source_citations": citations,
        "allocator_council_runs": allocator_council_runs,
        "portfolio_memo_ensemble": portfolio_memo_ensemble,
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
