"""Read-only bridge for Alpha Edge security price history."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx


DEFAULT_ALPHA_EDGE_API_BASE = "https://alpha-edge-backend.fly.dev/api"


def alpha_edge_api_base() -> str:
    """Return the Alpha Edge API base URL without a trailing slash."""

    raw = os.getenv("ALPHA_EDGE_API_BASE", DEFAULT_ALPHA_EDGE_API_BASE)
    return str(raw or "").strip().rstrip("/")


def ticker_candidates(raw_ticker: Any) -> List[str]:
    """Return likely Alpha Edge ticker spellings, preserving order."""

    raw = str(raw_ticker or "").strip().upper()
    if not raw:
        return []
    candidates: List[str] = []

    def add(value: str) -> None:
        cleaned = str(value or "").strip().upper()
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)

    add(raw)
    if ":" in raw:
        _, symbol = raw.split(":", 1)
        add(symbol)
    else:
        add(f"ASX:{raw}")
    return candidates


def _parse_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text[:10]


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"n/a", "na", "none", "null"}:
        return None
    try:
        return float(text.replace(",", ""))
    except Exception:
        return None


def normalize_security_history_rows(rows: Any) -> List[Dict[str, Any]]:
    """Normalize Alpha Edge /performance/security rows for chart overlays."""

    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        date = _parse_date(row.get("observed_at") or row.get("date"))
        price = _to_float(row.get("price") or row.get("close_price") or row.get("adjusted_close_price"))
        if not date or price is None or price <= 0:
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        exchange_prefix = str(row.get("exchange_prefix") or "").strip().upper()
        key = (date, ticker, exchange_prefix)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "date": date,
                "price": price,
                "ticker": ticker,
                "exchange_prefix": exchange_prefix,
                "name": str(row.get("name") or "").strip(),
                "currency": str(row.get("currency") or "").strip().upper(),
                "source": str(row.get("source") or "ALPHA_EDGE").strip().upper(),
                "statement_id": row.get("statement_id"),
                "quantity": _to_float(row.get("quantity")),
                "market_value_aud": _to_float(row.get("market_value_aud")),
                "portfolio_weight_pct": _to_float(row.get("portfolio_weight_pct")),
            }
        )
    out.sort(key=lambda item: str(item.get("date") or ""))
    return out


async def fetch_alpha_edge_security_history(
    ticker: Any,
    *,
    base_url: str = "",
    timeout_seconds: float = 10.0,
) -> Dict[str, Any]:
    """Fetch security price history from Alpha Edge with ticker fallbacks."""

    base = str(base_url or alpha_edge_api_base()).strip().rstrip("/")
    candidates = ticker_candidates(ticker)
    started = datetime.now(timezone.utc)
    if not base or not candidates:
        return {
            "available": False,
            "source": "ALPHA_EDGE",
            "source_url": base,
            "requested_ticker": str(ticker or "").strip(),
            "resolved_ticker": "",
            "attempted_tickers": candidates,
            "points": [],
            "latest": None,
            "error": "missing_base_or_ticker",
            "fetched_at_utc": started.isoformat(),
        }

    last_error = ""
    async with httpx.AsyncClient(timeout=max(2.0, float(timeout_seconds))) as client:
        for candidate in candidates:
            url = f"{base}/performance/security/{candidate}"
            try:
                response = await client.get(url)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:
                last_error = str(exc)
                continue
            points = normalize_security_history_rows(payload)
            if points:
                latest = points[-1]
                return {
                    "available": True,
                    "source": "ALPHA_EDGE",
                    "source_url": base,
                    "requested_ticker": str(ticker or "").strip(),
                    "resolved_ticker": candidate,
                    "attempted_tickers": candidates,
                    "points": points,
                    "latest": latest,
                    "point_count": len(points),
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                    "error": "",
                }

    return {
        "available": False,
        "source": "ALPHA_EDGE",
        "source_url": base,
        "requested_ticker": str(ticker or "").strip(),
        "resolved_ticker": "",
        "attempted_tickers": candidates,
        "points": [],
        "latest": None,
        "point_count": 0,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "error": last_error or "no_price_history",
    }
