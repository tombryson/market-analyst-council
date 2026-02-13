"""Deterministic market-facts prepass for ticker-level baseline fields."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import re
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlparse

import httpx

from .config import MARKET_FACTS_TIMEOUT_SECONDS, TAVILY_API_KEY


EXCHANGE_TO_YAHOO_SUFFIX = {
    "ASX": ".AX",
    "NYSE": "",
    "NASDAQ": "",
    "TSX": ".TO",
    "TSXV": ".V",
    "LSE": ".L",
    "AIM": ".L",
}


def _parse_ticker(ticker: str) -> Dict[str, str]:
    raw = str(ticker or "").strip().upper()
    exchange = ""
    symbol = ""

    if ":" in raw:
        prefix, rest = raw.split(":", 1)
        exchange = prefix.strip().upper()
        symbol = rest.strip().upper()
    else:
        symbol = raw

    suffix = EXCHANGE_TO_YAHOO_SUFFIX.get(exchange, "")
    yahoo_symbol = f"{symbol}{suffix}" if symbol else ""
    normalized_ticker = f"{exchange}:{symbol}" if exchange and symbol else symbol

    return {
        "raw": raw,
        "exchange": exchange,
        "symbol": symbol,
        "normalized_ticker": normalized_ticker,
        "yahoo_symbol": yahoo_symbol,
    }


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _scale_suffix(value: float, suffix: str) -> float:
    suffix_clean = (suffix or "").strip().upper()
    scales = {
        "": 1.0,
        "K": 1_000.0,
        "THOUSAND": 1_000.0,
        "M": 1_000_000.0,
        "MM": 1_000_000.0,
        "MN": 1_000_000.0,
        "MILLION": 1_000_000.0,
        "B": 1_000_000_000.0,
        "BN": 1_000_000_000.0,
        "BILLION": 1_000_000_000.0,
        "T": 1_000_000_000_000.0,
        "TRILLION": 1_000_000_000_000.0,
    }
    return value * scales.get(suffix_clean, 1.0)


def _parse_numeric_with_suffix(raw_value: str, raw_suffix: str = "") -> Optional[float]:
    clean = (raw_value or "").strip().replace(",", "")
    if not clean:
        return None
    try:
        value = float(clean)
    except (TypeError, ValueError):
        return None
    return _scale_suffix(value, raw_suffix)


def _pick_numeric(mapping: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        if key not in mapping:
            continue
        parsed = _to_float(mapping.get(key))
        if parsed is not None:
            return parsed
    return None


def _pick_string(mapping: Dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        value = mapping.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _normalize_facts(
    *,
    current_price: Optional[float],
    market_cap: Optional[float],
    shares_outstanding: Optional[float],
    enterprise_value: Optional[float],
    currency: Optional[str],
) -> Dict[str, Any]:
    return {
        "current_price": current_price,
        "market_cap": market_cap,
        "market_cap_m": (market_cap / 1_000_000.0) if market_cap is not None else None,
        "shares_outstanding": shares_outstanding,
        "shares_outstanding_m": (
            shares_outstanding / 1_000_000.0
            if shares_outstanding is not None
            else None
        ),
        "enterprise_value": enterprise_value,
        "enterprise_value_m": (
            enterprise_value / 1_000_000.0
            if enterprise_value is not None
            else None
        ),
        "currency": (currency or "").strip().upper() or None,
    }


def _available_field_count(normalized_facts: Dict[str, Any]) -> int:
    keys = (
        "current_price",
        "market_cap",
        "shares_outstanding",
        "enterprise_value",
        "currency",
    )
    return sum(1 for key in keys if normalized_facts.get(key) is not None)


def _sanitize_normalized_facts(normalized_facts: Dict[str, Any]) -> Dict[str, Any]:
    """Drop implausible parsed values from fallback snippets."""
    out = dict(normalized_facts or {})

    price = _to_float(out.get("current_price"))
    if price is not None and not (0.00001 <= price <= 100_000):
        out["current_price"] = None

    market_cap = _to_float(out.get("market_cap"))
    # Market cap should be at least in low millions for listed equities.
    if market_cap is not None and market_cap < 1_000_000:
        out["market_cap"] = None
        out["market_cap_m"] = None

    shares = _to_float(out.get("shares_outstanding"))
    if shares is not None and shares < 1_000_000:
        out["shares_outstanding"] = None
        out["shares_outstanding_m"] = None

    enterprise_value = _to_float(out.get("enterprise_value"))
    if enterprise_value is not None and enterprise_value < 1_000_000:
        out["enterprise_value"] = None
        out["enterprise_value_m"] = None

    if out.get("market_cap") is not None and out.get("market_cap_m") is None:
        out["market_cap_m"] = out["market_cap"] / 1_000_000.0
    if out.get("shares_outstanding") is not None and out.get("shares_outstanding_m") is None:
        out["shares_outstanding_m"] = out["shares_outstanding"] / 1_000_000.0
    if out.get("enterprise_value") is not None and out.get("enterprise_value_m") is None:
        out["enterprise_value_m"] = out["enterprise_value"] / 1_000_000.0

    return out


def _select_better(
    bucket: Dict[str, Tuple[float, float, str]],
    key: str,
    value: Optional[float],
    score: float,
    source_url: str,
) -> None:
    if value is None:
        return
    current = bucket.get(key)
    if current is None or score > current[0]:
        bucket[key] = (score, value, source_url)


def _source_domain_score(url: str) -> float:
    lower = (url or "").lower()
    if "marketindex.com.au" in lower:
        return 3.0
    if "asx.com.au/markets/company" in lower:
        return 2.5
    if "announcements.asx.com.au" in lower:
        return 2.0
    if "asx.com.au" in lower:
        return 1.6
    return 1.0


def _url_matches_ticker(url: str, parsed: Dict[str, str]) -> bool:
    """
    Require strong ticker match for fallback snippets to avoid near-symbol pollution
    (for example, WWI vs WWIN pages).
    """
    symbol = str(parsed.get("symbol") or "").strip().upper()
    if not symbol:
        return True

    try:
        parsed_url = urlparse(url or "")
    except Exception:
        return False

    host = (parsed_url.netloc or "").lower()
    path = (parsed_url.path or "").lower()
    symbol_lower = symbol.lower()
    symbol_upper = symbol.upper()

    if "marketindex.com.au" in host:
        # Require exact company slug path segment.
        return re.search(rf"/asx/{re.escape(symbol_lower)}(?:/|$)", path) is not None

    if "asx.com.au" in host:
        if "/markets/company/" in path:
            return re.search(
                rf"/markets/company/{re.escape(symbol_lower)}(?:/|$)",
                path,
            ) is not None
        # announcements/data-api URLs may not use a fixed path; require symbol token.
        return (
            re.search(
                rf"(?<![A-Z0-9]){re.escape(symbol_upper)}(?![A-Z0-9])",
                f"{host}{path}".upper(),
            )
            is not None
        )

    # Generic strict token match fallback.
    return (
        re.search(
            rf"(?<![A-Z0-9]){re.escape(symbol_upper)}(?![A-Z0-9])",
            (url or "").upper(),
        )
        is not None
    )


def _extract_metrics_from_text(text: str) -> Dict[str, Optional[float]]:
    payload = {
        "current_price": None,
        "market_cap": None,
        "shares_outstanding": None,
        "enterprise_value": None,
    }
    haystack = (text or "")
    if not haystack:
        return payload

    market_cap_patterns = [
        r"(?:market\s*cap(?:itali[sz]ation)?|market\s*capitalisation)\D{0,40}(?:A\$|\$)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*([KMBT]?)\b",
        r"(?:market\s*cap(?:itali[sz]ation)?|market\s*capitalisation)\D{0,40}(?:A\$|\$)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(million|billion|thousand|trillion|mn|bn)\b",
    ]
    shares_patterns = [
        r"(?:shares\s*(?:on\s*issue|in\s*issue|outstanding|issued))\D{0,35}([0-9][0-9,]*(?:\.[0-9]+)?)\s*([KMBT]?)\b",
        r"(?:shares\s*(?:on\s*issue|in\s*issue|outstanding|issued))\D{0,35}([0-9][0-9,]*(?:\.[0-9]+)?)\s*(million|billion|thousand|trillion|mn|bn)\b",
    ]
    enterprise_patterns = [
        r"(?:enterprise\s*value)\D{0,40}(?:A\$|\$)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*([KMBT]?)\b",
        r"(?:enterprise\s*value)\D{0,40}(?:A\$|\$)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(million|billion|thousand|trillion|mn|bn)\b",
    ]
    price_patterns = [
        r"(?:last\s*price|share\s*price|price\s*/\s*today'?s\s*change|close(?:d)?\s*at)\D{0,20}(?:A\$|\$)?\s*([0-9]+(?:\.[0-9]+)?)\s*(c|¢)?",
        r"(?:@|at)\s*([0-9]+(?:\.[0-9]+)?)\s*(c|¢)\s*per\s*share",
        r"(?:A\$|\$)\s*([0-9]+(?:\.[0-9]+)?)\s*(?:per\s*share)?",
    ]

    for pattern in market_cap_patterns:
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if match:
            payload["market_cap"] = _parse_numeric_with_suffix(match.group(1), match.group(2))
            break

    for pattern in shares_patterns:
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if match:
            payload["shares_outstanding"] = _parse_numeric_with_suffix(match.group(1), match.group(2))
            break

    for pattern in enterprise_patterns:
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if match:
            payload["enterprise_value"] = _parse_numeric_with_suffix(match.group(1), match.group(2))
            break

    for pattern in price_patterns:
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if match:
            cents = match.lastindex and match.lastindex >= 2 and (match.group(2) or "").strip()
            parsed = _parse_numeric_with_suffix(match.group(1), "")
            if parsed is not None:
                payload["current_price"] = (parsed / 100.0) if cents else parsed
                break

    return payload


async def _gather_yfinance_facts(
    parsed: Dict[str, str],
    timeout: float,
) -> Dict[str, Any]:
    yahoo_symbol = parsed.get("yahoo_symbol") or parsed.get("symbol")
    if not yahoo_symbol:
        return {
            "normalized_facts": {},
            "source_urls": [],
            "notes": ["No symbol for yfinance prepass."],
            "error": "",
        }

    quote_page_url = f"https://finance.yahoo.com/quote/{yahoo_symbol}"

    async def _run_thread_call(
        label: str,
        fn: Any,
        call_timeout: float,
        notes: List[str],
    ) -> Any:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(fn),
                timeout=max(1.0, float(call_timeout)),
            )
        except Exception as exc:
            notes.append(f"yfinance {label} failed: {type(exc).__name__}: {exc}")
            return None

    async def _fetch_once(call_timeout: float) -> Dict[str, Any]:
        import yfinance as yf

        notes: List[str] = []
        ticker = yf.Ticker(yahoo_symbol)

        fast_info = await _run_thread_call(
            "fast_info",
            lambda: dict(ticker.fast_info or {}),
            min(call_timeout, 8.0),
            notes,
        )
        if not isinstance(fast_info, dict):
            fast_info = {}

        history_close: Optional[float] = None
        history = await _run_thread_call(
            "history",
            lambda: ticker.history(period="5d", interval="1d", auto_adjust=False),
            min(call_timeout, 8.0),
            notes,
        )
        try:
            if history is not None:
                closes = history.get("Close")
                if closes is not None:
                    closes = closes.dropna()
                    if len(closes) > 0:
                        history_close = _to_float(closes.iloc[-1])
        except Exception as exc:
            notes.append(f"yfinance history parse failed: {type(exc).__name__}: {exc}")

        # Avoid the heavy .info call unless core fields are still missing.
        has_fast_market_cap = _pick_numeric(fast_info, "marketCap") is not None
        has_fast_shares = _pick_numeric(fast_info, "shares", "sharesOutstanding") is not None
        has_fast_price = _pick_numeric(fast_info, "lastPrice", "regularMarketPrice", "currentPrice") is not None
        need_info = not (has_fast_market_cap and has_fast_shares and (has_fast_price or history_close is not None))

        info: Dict[str, Any] = {}
        if need_info:
            info_raw = await _run_thread_call(
                "info",
                lambda: dict(ticker.info or {}),
                min(call_timeout, 12.0),
                notes,
            )
            if isinstance(info_raw, dict):
                info = info_raw

        return {
            "fast_info": fast_info,
            "info": info,
            "history_close": history_close,
            "notes": notes,
        }

    def _is_transient_note(note: str) -> bool:
        text = (note or "").lower()
        transient_tokens = (
            "timeout",
            "timed out",
            "readtimeout",
            "connecttimeout",
            "connectionerror",
            "temporar",
            "429",
            "503",
            "504",
        )
        return any(token in text for token in transient_tokens)

    fetch_error = ""
    recovered_error = ""
    payload: Dict[str, Any] = {}
    last_notes: List[str] = []
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            payload = await _fetch_once(timeout)
        except Exception as exc:
            payload = {}
            last_notes = [f"yfinance prepass failed: {type(exc).__name__}: {exc}"]
        else:
            last_notes = payload.get("notes") or []

        fast_info_attempt = payload.get("fast_info") or {}
        info_attempt = payload.get("info") or {}
        history_close_attempt = _to_float(payload.get("history_close"))
        has_any_payload_data = bool(
            fast_info_attempt
            or info_attempt
            or history_close_attempt is not None
        )
        if has_any_payload_data:
            if recovered_error:
                last_notes = list(last_notes) + [f"yfinance transient retry recovered after: {recovered_error}"]
            fetch_error = ""
            break

        fetch_error = "; ".join(last_notes[:2]).strip() or "No yfinance data returned"
        if attempt < max_attempts and any(_is_transient_note(note) for note in last_notes):
            recovered_error = fetch_error
            await asyncio.sleep(0.8 * attempt)
            continue
        break

    if fetch_error and not payload:
        return {
            "normalized_facts": {},
            "source_urls": [quote_page_url],
            "notes": [f"yfinance prepass failed: {fetch_error}"],
            "error": fetch_error,
        }

    fast_info = payload.get("fast_info") or {}
    info = payload.get("info") or {}
    history_close = _to_float(payload.get("history_close"))
    last_notes = payload.get("notes") or []

    current_price = (
        _pick_numeric(fast_info, "lastPrice", "regularMarketPrice", "currentPrice")
        or _pick_numeric(info, "currentPrice", "regularMarketPrice", "previousClose")
        or history_close
    )
    market_cap = _pick_numeric(fast_info, "marketCap") or _pick_numeric(info, "marketCap")
    shares_outstanding = (
        _pick_numeric(fast_info, "shares", "sharesOutstanding")
        or _pick_numeric(info, "sharesOutstanding", "impliedSharesOutstanding")
    )
    enterprise_value = _pick_numeric(fast_info, "enterpriseValue") or _pick_numeric(info, "enterpriseValue")
    currency = _pick_string(fast_info, "currency") or _pick_string(info, "currency")

    normalized_facts = _normalize_facts(
        current_price=current_price,
        market_cap=market_cap,
        shares_outstanding=shares_outstanding,
        enterprise_value=enterprise_value,
        currency=currency or ("AUD" if parsed.get("exchange") == "ASX" else ""),
    )
    normalized_facts = _sanitize_normalized_facts(normalized_facts)

    notes = list(last_notes)
    if fetch_error:
        notes.append(f"yfinance transient retry recovered after: {fetch_error}")

    return {
        "normalized_facts": normalized_facts,
        "source_urls": [quote_page_url],
        "notes": notes,
        "error": "",
    }


async def _tavily_search(query: str, timeout: float, max_results: int = 4) -> Dict[str, Any]:
    if not TAVILY_API_KEY:
        return {"error": "TAVILY_API_KEY not configured", "results": []}

    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "basic",
        "max_results": max_results,
        "include_answer": True,
        "include_raw_content": False,
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                "https://api.tavily.com/search",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            return response.json() or {}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "results": []}


async def _gather_asx_marketindex_fallback(
    parsed: Dict[str, str],
    timeout: float,
) -> Dict[str, Any]:
    symbol = parsed.get("symbol", "")
    exchange = parsed.get("exchange", "")
    if not symbol:
        return {"normalized_facts": {}, "source_urls": [], "notes": ["No symbol for fallback queries."]}

    queries = [
        f"site:marketindex.com.au ASX:{symbol} market cap shares on issue last price",
        f"site:asx.com.au/markets/company/{symbol} key statistics market cap share price",
        f"site:announcements.asx.com.au ASX:{symbol} investor presentation market capitalisation shares",
    ]
    if exchange and exchange != "ASX":
        queries = [
            f"site:asx.com.au {symbol} market cap shares",
            f"site:marketindex.com.au {symbol} market cap shares",
        ]

    best_values: Dict[str, Tuple[float, float, str]] = {}
    source_urls: List[str] = []
    notes: List[str] = []

    for query in queries:
        payload = await _tavily_search(query=query, timeout=timeout, max_results=4)
        if payload.get("error"):
            notes.append(f"Tavily fallback query failed: {query} ({payload['error']})")
            continue

        for result in payload.get("results", []) or []:
            url = str(result.get("url") or "").strip()
            title = str(result.get("title") or "")
            content = str(result.get("content") or "")
            if not url:
                continue
            if not _url_matches_ticker(url, parsed):
                notes.append(f"Skipped fallback URL due ticker mismatch: {url}")
                continue
            if url not in source_urls:
                source_urls.append(url)

            text = f"{title}\n{content}"
            extracted = _extract_metrics_from_text(text)
            base_score = _source_domain_score(url)

            for metric_key in ("current_price", "market_cap", "shares_outstanding", "enterprise_value"):
                value = extracted.get(metric_key)
                if value is None:
                    continue
                confidence = base_score + 0.2
                if metric_key in title.lower():
                    confidence += 0.1
                _select_better(best_values, metric_key, value, confidence, url)

    normalized_facts = _normalize_facts(
        current_price=best_values.get("current_price", (0.0, None, ""))[1],
        market_cap=best_values.get("market_cap", (0.0, None, ""))[1],
        shares_outstanding=best_values.get("shares_outstanding", (0.0, None, ""))[1],
        enterprise_value=best_values.get("enterprise_value", (0.0, None, ""))[1],
        currency="AUD" if exchange == "ASX" else "",
    )
    normalized_facts = _sanitize_normalized_facts(normalized_facts)

    return {
        "normalized_facts": normalized_facts,
        "source_urls": source_urls,
        "notes": notes,
    }


async def gather_market_facts_prepass(
    ticker: Optional[str],
    company_name: Optional[str] = None,
    exchange: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch deterministic market baseline fields using a provider chain.

    Provider order:
      1) yfinance (Yahoo-backed, library mediated)
      2) Tavily + exchange-specific fallback snippets
    """
    resolved_ticker = str(ticker or "").strip()
    if not resolved_ticker:
        return {
            "status": "skipped",
            "reason": "No ticker provided for market facts prepass.",
            "normalized_facts": {},
            "source_urls": [],
            "as_of_utc": datetime.now(timezone.utc).isoformat(),
        }

    parsed = _parse_ticker(resolved_ticker)
    if exchange and not parsed["exchange"]:
        parsed["exchange"] = str(exchange).strip().upper()
        suffix = EXCHANGE_TO_YAHOO_SUFFIX.get(parsed["exchange"], "")
        parsed["yahoo_symbol"] = f"{parsed['symbol']}{suffix}" if parsed["symbol"] else parsed["symbol"]
        if parsed["exchange"] and parsed["symbol"]:
            parsed["normalized_ticker"] = f"{parsed['exchange']}:{parsed['symbol']}"

    yahoo_symbol = parsed["yahoo_symbol"] or parsed["symbol"]
    if not yahoo_symbol:
        return {
            "status": "skipped",
            "reason": "Ticker parsing failed for market facts prepass.",
            "normalized_facts": {},
            "source_urls": [],
            "as_of_utc": datetime.now(timezone.utc).isoformat(),
        }

    quote_page_url = f"https://finance.yahoo.com/quote/{yahoo_symbol}"
    as_of = datetime.now(timezone.utc).isoformat()

    timeout = max(5.0, float(MARKET_FACTS_TIMEOUT_SECONDS))
    yfinance_error = ""
    yfinance_notes: List[str] = []
    normalized_facts = _normalize_facts(
        current_price=None,
        market_cap=None,
        shares_outstanding=None,
        enterprise_value=None,
        currency="AUD" if parsed.get("exchange") == "ASX" else "",
    )
    source_urls: List[str] = [quote_page_url]

    yfinance_result = await _gather_yfinance_facts(parsed, timeout=timeout)
    yfinance_facts = yfinance_result.get("normalized_facts", {}) or {}
    yfinance_sources = yfinance_result.get("source_urls", []) or []
    yfinance_notes = yfinance_result.get("notes", []) or []
    yfinance_error = str(yfinance_result.get("error") or "").strip()
    for key, value in yfinance_facts.items():
        if normalized_facts.get(key) is None and value is not None:
            normalized_facts[key] = value
    normalized_facts = _sanitize_normalized_facts(normalized_facts)
    for url in yfinance_sources:
        if url and url not in source_urls:
            source_urls.append(url)

    available_fields = _available_field_count(normalized_facts)

    # Fallback path for ASX/MarketIndex oriented coverage when Yahoo is unavailable or sparse.
    fallback_used = False
    fallback_notes: List[str] = []
    if available_fields < 3:
        fallback = await _gather_asx_marketindex_fallback(parsed, timeout=timeout)
        fallback_facts = fallback.get("normalized_facts", {}) or {}
        fallback_sources = fallback.get("source_urls", []) or []
        fallback_notes = fallback.get("notes", []) or []
        fallback_has_core = (
            fallback_facts.get("market_cap") is not None
            and fallback_facts.get("shares_outstanding") is not None
        )
        if fallback_has_core:
            for key, value in fallback_facts.items():
                if normalized_facts.get(key) is None and value is not None:
                    normalized_facts[key] = value
                    fallback_used = True
        elif any(
            fallback_facts.get(key) is not None
            for key in ("current_price", "market_cap", "shares_outstanding", "enterprise_value")
        ):
            fallback_notes.append(
                "Fallback numeric facts rejected: missing core fields "
                "(market_cap and shares_outstanding)."
            )

        normalized_facts = _sanitize_normalized_facts(normalized_facts)

        for url in fallback_sources:
            if url and url not in source_urls:
                source_urls.append(url)

        available_fields = _available_field_count(normalized_facts)

    if available_fields >= 3:
        status = "ok" if not fallback_used else "fallback_ok"
    elif available_fields > 0:
        status = "partial" if not fallback_used else "fallback_partial"
    else:
        status = "error" if (yfinance_error or fallback_notes) else "empty"

    reason_parts: List[str] = []

    def _append_unique(items: List[str]) -> None:
        for item in items:
            clean = str(item or "").strip()
            if not clean:
                continue
            if clean not in reason_parts:
                reason_parts.append(clean)

    if yfinance_error:
        _append_unique([f"yfinance prepass failed: {yfinance_error}"])
    if yfinance_notes:
        _append_unique(yfinance_notes[:2])
    if fallback_used:
        _append_unique(["Applied ASX/MarketIndex Tavily fallback."])
    if fallback_notes:
        _append_unique(fallback_notes[:2])
    reason = " | ".join(reason_parts).strip()

    return {
        "status": status,
        "reason": reason,
        "ticker": parsed["normalized_ticker"] or resolved_ticker,
        "exchange": parsed["exchange"],
        "symbol": parsed["symbol"],
        "yahoo_symbol": yahoo_symbol,
        "company_name": company_name or "",
        "as_of_utc": as_of,
        "source_urls": source_urls,
        "normalized_facts": normalized_facts,
        "providers_attempted": ["yfinance", "tavily_fallback"],
    }


def format_market_facts_for_prompt(market_facts: Optional[Dict[str, Any]]) -> str:
    """Format market facts for prompt/context injection."""
    if not market_facts:
        return ""

    normalized = market_facts.get("normalized_facts", {}) or {}
    if not normalized:
        return ""

    lines = [
        "Authoritative Market Facts Prepass (deterministic baseline)",
        f"- as_of_utc: {market_facts.get('as_of_utc', 'unknown')}",
        f"- ticker: {market_facts.get('ticker', '')}",
        f"- yahoo_symbol: {market_facts.get('yahoo_symbol', '')}",
    ]

    def _line(name: str, key: str) -> None:
        value = normalized.get(key)
        if value is not None:
            lines.append(f"- {name}: {value}")

    _line("current_price", "current_price")
    _line("market_cap", "market_cap")
    _line("market_cap_m", "market_cap_m")
    _line("shares_outstanding", "shares_outstanding")
    _line("shares_outstanding_m", "shares_outstanding_m")
    _line("enterprise_value", "enterprise_value")
    _line("enterprise_value_m", "enterprise_value_m")
    _line("currency", "currency")

    source_urls = market_facts.get("source_urls", []) or []
    if source_urls:
        for idx, url in enumerate(source_urls[:3], start=1):
            lines.append(f"- source_url_{idx}: {url}")

    lines.append(
        "- rule: Use these baseline market facts unless a newer dated primary source is explicitly cited."
    )
    return "\n".join(lines).strip()


def minimal_market_facts_payload(
    market_facts: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Return the minimal market-facts payload expected by Stage 1 prompts.

    Shape:
    {
      "normalized_facts": {
        current_price,
        market_cap,
        market_cap_m,
        shares_outstanding,
        shares_outstanding_m,
        enterprise_value,
        enterprise_value_m,
        currency
      }
    }
    """
    normalized = (market_facts or {}).get("normalized_facts", {}) or {}
    return {
        "normalized_facts": {
            "current_price": normalized.get("current_price"),
            "market_cap": normalized.get("market_cap"),
            "market_cap_m": normalized.get("market_cap_m"),
            "shares_outstanding": normalized.get("shares_outstanding"),
            "shares_outstanding_m": normalized.get("shares_outstanding_m"),
            "enterprise_value": normalized.get("enterprise_value"),
            "enterprise_value_m": normalized.get("enterprise_value_m"),
            "currency": normalized.get("currency"),
        }
    }


def format_market_facts_query_prefix(
    market_facts: Optional[Dict[str, Any]],
) -> str:
    """Render minimal market facts block for prepending directly to the query."""
    payload = minimal_market_facts_payload(market_facts)
    normalized = payload.get("normalized_facts", {}) or {}
    has_core_market_facts = (
        normalized.get("market_cap") is not None
        and normalized.get("shares_outstanding") is not None
    )
    if not has_core_market_facts:
        return ""
    return json.dumps(payload, indent=2)


def prepend_market_facts_to_query(
    query: str,
    market_facts: Optional[Dict[str, Any]],
) -> str:
    """
    Prefix minimal market facts block before the main query/template text.
    """
    core_query = (query or "").strip()
    prefix = format_market_facts_query_prefix(market_facts)
    if not prefix:
        return core_query
    if not core_query:
        return prefix
    return f"{prefix}\n\n{core_query}"
