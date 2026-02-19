#!/usr/bin/env python3
"""
Perplexity PDF dump utility.

Flow:
1) Run Perplexity retrieval.
2) Keep ASX / MarketIndex / Intelligent Investor sources within last 12 months.
3) Expand source pages to PDFs and detect Intelligent Investor price-sensitive markers.
4) Select balanced set: up to 10 price-sensitive + up to 10 non-price-sensitive.
5) Decode full PDF text (no excerpt truncation).
6) Dump one markdown file per PDF + manifest/index.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlsplit, urlunsplit

import httpx
from dotenv import load_dotenv

from backend.pdf_processor import extract_text_from_pdf
from backend.research.providers.perplexity import PerplexityResearchProvider
from backend.template_loader import get_template_loader
from backend.openrouter import query_model


DEFAULT_ALLOWED_DOMAIN_SUFFIXES = (
    "asx.com.au",
    "marketindex.com.au",
    "intelligentinvestor.com.au",
)

INJECTION_MIN_IMPORTANCE_SCORE = 80
US_EXCHANGE_IDS = {"nyse", "nasdaq"}
US_HIGH_SIGNAL_SEC_FORMS = {
    "8-K",
    "10-Q",
    "10-K",
    "20-F",
    "6-K",
    "DEF 14A",
}
NEWSWIRE_DOMAINS = (
    "businesswire.com",
    "globenewswire.com",
    "prnewswire.com",
)
DEFAULT_US_PS_MODEL = "openai/gpt-4o-mini"

US_PS_FORM_WEIGHTS = {
    "8-K": 0.62,
    "10-Q": 0.54,
    "10-K": 0.54,
    "20-F": 0.50,
    "6-K": 0.46,
    "DEF 14A": 0.24,
}

US_PS_POSITIVE_KEYWORDS = (
    "material definitive agreement",
    "earnings",
    "guidance",
    "acquisition",
    "merger",
    "financing",
    "credit facility",
    "debt facility",
    "capital raise",
    "public offering",
    "registered direct offering",
    "private placement",
    "atm program",
    "at-the-market",
    "covenant",
    "default",
    "bankruptcy",
    "production",
    "resource update",
    "reserve update",
    "feasibility study",
    "npv",
    "irr",
    "aisc",
    "first gold",
    "gold pour",
    "project update",
)

US_PS_NEGATIVE_KEYWORDS = (
    "form 3",
    "form 4",
    "form 5",
    "schedule 13g",
    "13g/a",
    "beneficial ownership",
    "section 16",
)


def _normalize_ticker_symbol(ticker: str) -> str:
    text = str(ticker or "").strip().upper()
    if not text:
        return ""
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    text = re.sub(r"[^A-Z0-9.\-]", "", text)
    return text


def _sec_request_headers() -> Dict[str, str]:
    identity = str(os.getenv("SEC_API_IDENTITY", "")).strip()
    if not identity:
        identity = "llm-council/1.0 (research@llm-council.local)"
    return {
        "User-Agent": identity,
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
    }


async def _http_get_with_sec_fallback(
    client: httpx.AsyncClient,
    url: str,
    *,
    timeout_seconds: float = 45.0,
) -> httpx.Response:
    host = urlparse(str(url or "")).netloc.lower()
    if "sec.gov" in host:
        response = await client.get(
            url,
            headers=_sec_request_headers(),
            timeout=timeout_seconds,
        )
    else:
        response = await client.get(url, timeout=timeout_seconds)
    if response.status_code != 403 or "sec.gov" not in host:
        return response

    retry_response = await client.get(
        url,
        headers=_sec_request_headers(),
        timeout=timeout_seconds,
    )
    return retry_response


def _parse_iso_date(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        try:
            return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _parse_human_datetime(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    # Example: "3 Feb 2026 9:45AM"
    text = re.sub(r"\s+", " ", text)
    for fmt in ("%d %b %Y %I:%M%p", "%d %b %Y"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _is_allowed_domain(url: str, allowed_domain_suffixes: List[str]) -> bool:
    host = urlparse(str(url or "")).netloc.lower()
    if not host:
        return False
    for suffix in (allowed_domain_suffixes or []):
        if host == suffix or host.endswith(f".{suffix}"):
            return True
    return False


def _looks_like_pdf_url(url: str) -> bool:
    lower = str(url or "").lower()
    return lower.endswith(".pdf") or ".pdf?" in lower or "/asxpdf/" in lower


def _looks_like_sec_filing_url(url: str) -> bool:
    low = str(url or "").lower()
    if "sec.gov/ixviewer/ix.html" in low and "doc=/archives/edgar/data/" in low:
        return True
    if "sec.gov/archives/edgar/data/" in low and (
        low.endswith(".htm") or low.endswith(".html") or ".htm?" in low or ".html?" in low
    ):
        return True
    return False


def _html_to_text(html: str) -> str:
    text = str(html or "")
    if not text:
        return ""
    # Remove script/style/noscript blocks before stripping tags.
    text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
    text = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", text)
    text = re.sub(r"(?is)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p\\s*>", "\n\n", text)
    text = re.sub(r"(?is)<li[^>]*>", "\n- ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \\t\\r\\f\\v]+", " ", text)
    text = re.sub(r"\\n{3,}", "\n\n", text)
    return text.strip()


def _infer_exchange_from_ticker(ticker: str) -> str:
    text = str(ticker or "").strip().upper()
    if not text:
        return ""
    if ":" in text:
        return text.split(":", 1)[0].strip().lower()
    suffix_map = {
        ".AX": "asx",
        ".N": "nyse",
        ".O": "nasdaq",
        ".Q": "nasdaq",
        ".TO": "tsx",
        ".V": "tsxv",
        ".L": "lse",
    }
    for suffix, exchange in suffix_map.items():
        if text.endswith(suffix):
            return exchange
    return ""


def _resolve_exchange_profile(
    *,
    query: str,
    ticker: str,
    explicit_exchange: str,
) -> Dict[str, Any]:
    loader = get_template_loader()
    exchange = loader.normalize_exchange(explicit_exchange)
    if not exchange:
        inferred = _infer_exchange_from_ticker(ticker)
        exchange = loader.normalize_exchange(inferred)
    if not exchange:
        exchange = loader.detect_exchange(user_query=str(query or ""), ticker=str(ticker or ""))
    exchange = loader.normalize_exchange(exchange) or "unknown"
    params = loader.get_exchange_retrieval_params(exchange)
    allowed_domains = [
        str(item).strip().lower()
        for item in list(params.get("allowed_domain_suffixes", []) or [])
        if str(item).strip()
    ]
    if not allowed_domains:
        allowed_domains = list(DEFAULT_ALLOWED_DOMAIN_SUFFIXES)
    params["allowed_domain_suffixes"] = allowed_domains
    params["exchange"] = exchange
    return params


def _build_candidate_row(
    *,
    source_url: str,
    title: str,
    published_dt: Optional[datetime],
    score: float,
    discovery_tier: int,
    discovery_method: str,
    source_snippet: str = "",
) -> Dict[str, Any]:
    url = str(source_url or "").strip()
    dt = published_dt if isinstance(published_dt, datetime) else None
    return {
        "source_url": url,
        "title": str(title or "").strip() or "Untitled",
        "published_at": dt.strftime("%Y-%m-%d") if dt else "",
        "published_dt": dt,
        "score": float(score or 0.0),
        "domain": urlparse(url).netloc.lower(),
        "discovery_tier": int(discovery_tier),
        "discovery_method": str(discovery_method or "").strip(),
        "source_snippet": str(source_snippet or "").strip(),
    }


async def _discover_us_tier1_sec_data_api(
    client: httpx.AsyncClient,
    *,
    ticker: str,
    cutoff: datetime,
    max_rows: int = 80,
) -> tuple[List[Dict[str, Any]], str]:
    symbol = _normalize_ticker_symbol(ticker)
    if not symbol:
        return [], "missing_symbol"

    try:
        ticker_map_resp = await _http_get_with_sec_fallback(
            client,
            "https://www.sec.gov/files/company_tickers.json",
        )
    except Exception as exc:
        return [], f"ticker_map_request_failed:{type(exc).__name__}"
    if ticker_map_resp.status_code >= 400:
        return [], f"ticker_map_http_{ticker_map_resp.status_code}"

    try:
        ticker_map = ticker_map_resp.json()
    except Exception:
        return [], "ticker_map_json_parse_failed"

    cik_int: Optional[int] = None
    if isinstance(ticker_map, dict):
        for value in ticker_map.values():
            row = value if isinstance(value, dict) else {}
            row_ticker = str(row.get("ticker", "")).strip().upper()
            if row_ticker == symbol:
                try:
                    cik_int = int(row.get("cik_str"))
                    break
                except Exception:
                    continue
    if cik_int is None:
        return [], "ticker_not_found_in_sec_map"

    cik = f"{cik_int:010d}"
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        submissions_resp = await _http_get_with_sec_fallback(client, submissions_url)
    except Exception as exc:
        return [], f"submissions_request_failed:{type(exc).__name__}"
    if submissions_resp.status_code >= 400:
        return [], f"submissions_http_{submissions_resp.status_code}"

    try:
        submissions = submissions_resp.json()
    except Exception:
        return [], "submissions_json_parse_failed"

    recent = ((submissions or {}).get("filings", {}) or {}).get("recent", {}) or {}
    forms = list(recent.get("form", []) or [])
    accession_numbers = list(recent.get("accessionNumber", []) or [])
    filing_dates = list(recent.get("filingDate", []) or [])
    primary_docs = list(recent.get("primaryDocument", []) or [])

    rows: List[Dict[str, Any]] = []
    seen = set()
    for idx, form_value in enumerate(forms):
        form = str(form_value or "").strip().upper()
        if form not in US_HIGH_SIGNAL_SEC_FORMS:
            continue
        filing_date_text = str(filing_dates[idx] if idx < len(filing_dates) else "").strip()
        filing_dt = _parse_iso_date(filing_date_text)
        if filing_dt and filing_dt < cutoff:
            continue
        accession = str(accession_numbers[idx] if idx < len(accession_numbers) else "").strip()
        if not accession:
            continue
        accession_no_dash = accession.replace("-", "")
        base_path = f"https://www.sec.gov/Archives/edgar/data/{int(cik_int)}/{accession_no_dash}"
        primary_doc = str(primary_docs[idx] if idx < len(primary_docs) else "").strip()
        urls = []
        if primary_doc:
            urls.append(f"{base_path}/{primary_doc}")
        urls.append(f"{base_path}/{accession}-index.html")
        for url in urls:
            if not url or url in seen:
                continue
            seen.add(url)
            rows.append(
                _build_candidate_row(
                    source_url=url,
                    title=f"SEC {form} filing ({filing_date_text or 'undated'})",
                    published_dt=filing_dt,
                    score=11.0 - min(5.0, idx * 0.02),
                    discovery_tier=1,
                    discovery_method="tier1_sec_data_api",
                )
            )
            if len(rows) >= max_rows:
                return rows, ""

    if not rows:
        return [], "no_recent_high_signal_forms"
    return rows, ""


async def _discover_us_tier2_sec_report(
    client: httpx.AsyncClient,
    *,
    ticker: str,
    cutoff: datetime,
    max_rows: int = 60,
) -> tuple[List[Dict[str, Any]], str]:
    symbol = _normalize_ticker_symbol(ticker)
    if not symbol:
        return [], "missing_symbol"

    url = f"https://sec.report/Ticker/{symbol}"
    try:
        resp = await client.get(url, timeout=45.0)
    except Exception as exc:
        return [], f"sec_report_request_failed:{type(exc).__name__}"
    if resp.status_code >= 400:
        return [], f"sec_report_http_{resp.status_code}"

    html = str(resp.text or "")
    if not html.strip():
        return [], "empty_sec_report_page"

    rows: List[Dict[str, Any]] = []
    seen = set()
    href_matches = list(re.finditer(r'(?is)href=["\']([^"\']+)["\']', html))
    for idx, match in enumerate(href_matches):
        href_raw = str(match.group(1) or "").strip()
        href = urljoin(url, href_raw)
        if not href:
            continue
        href_low = href.lower()
        if (
            "/document/" not in href_low
            and "archives/edgar/data" not in href_low
            and "-index.html" not in href_low
            and "-index.htm" not in href_low
        ):
            continue
        if href in seen:
            continue
        seen.add(href)
        pos = int(match.start(1))
        context_window = html[max(0, pos - 220) : min(len(html), pos + 220)]
        date_match = re.search(r"(?<!\d)(20\d{2}-\d{2}-\d{2})(?!\d)", context_window)
        updated_dt = _parse_iso_date(date_match.group(1)) if date_match else None
        if isinstance(updated_dt, datetime) and updated_dt < cutoff:
            continue
        title = _clean_html_fragment(context_window) or f"SEC Report filing link {idx + 1}"
        rows.append(
            _build_candidate_row(
                source_url=href,
                title=title,
                published_dt=updated_dt,
                score=8.0 - min(3.5, idx * 0.03),
                discovery_tier=2,
                discovery_method="tier2_sec_report",
            )
        )
        if len(rows) >= max_rows:
            break

    if not rows:
        return [], "no_sec_report_entries"
    return rows, ""


def _discover_us_tier3_newswire_candidates(
    *,
    sources: List[Dict[str, Any]],
    cutoff: datetime,
) -> tuple[List[Dict[str, Any]], str]:
    rows: List[Dict[str, Any]] = []
    for source in list(sources or []):
        url = str(source.get("url", "")).strip()
        if not url:
            continue
        host = urlparse(url).netloc.lower()
        if not any(host == domain or host.endswith(f".{domain}") for domain in NEWSWIRE_DOMAINS):
            continue
        published_dt = _parse_iso_date(str(source.get("published_at", "")).strip())
        if published_dt and published_dt < cutoff:
            continue
        rows.append(
            _build_candidate_row(
                source_url=url,
                title=str(source.get("title", "")).strip() or "Newswire release",
                published_dt=published_dt,
                score=float(source.get("score", 0.0) or 0.0) + 2.0,
                discovery_tier=3,
                discovery_method="tier3_newswire_fallback",
            )
        )

    if not rows:
        return [], "no_newswire_sources"
    return rows, ""


async def _augment_us_candidates_with_fallback_chain(
    client: httpx.AsyncClient,
    *,
    exchange_id: str,
    ticker: str,
    sources: List[Dict[str, Any]],
    candidate_rows: List[Dict[str, Any]],
    allowed_domain_suffixes: List[str],
    cutoff: datetime,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    normalized_exchange = str(exchange_id or "").strip().lower()
    rows = list(candidate_rows or [])
    chain_log: Dict[str, Any] = {
        "enabled": normalized_exchange in US_EXCHANGE_IDS,
        "exchange": normalized_exchange,
        "tier_used": "tier4_existing_perplexity",
        "attempts": [],
    }
    if normalized_exchange not in US_EXCHANGE_IDS:
        return rows, chain_log

    existing_urls = {str(row.get("source_url", "")).strip() for row in rows if str(row.get("source_url", "")).strip()}
    for row in rows:
        row.setdefault("discovery_tier", 4)
        row.setdefault("discovery_method", "tier4_existing_perplexity")

    def _merge_rows(new_rows: List[Dict[str, Any]]) -> int:
        added = 0
        for item in new_rows:
            url = str(item.get("source_url", "")).strip()
            if not url:
                continue
            if not _is_allowed_domain(url, allowed_domain_suffixes):
                continue
            if url in existing_urls:
                continue
            existing_urls.add(url)
            rows.append(item)
            added += 1
        return added

    tier1_rows, tier1_error = await _discover_us_tier1_sec_data_api(
        client,
        ticker=ticker,
        cutoff=cutoff,
    )
    tier1_added = _merge_rows(tier1_rows)
    chain_log["attempts"].append(
        {
            "tier": 1,
            "name": "sec_data_api",
            "status": "success" if tier1_added > 0 else "failed",
            "added": tier1_added,
            "error": tier1_error,
        }
    )
    if tier1_added > 0:
        chain_log["tier_used"] = "tier1_sec_data_api"
        return rows, chain_log

    tier2_rows, tier2_error = await _discover_us_tier2_sec_report(
        client,
        ticker=ticker,
        cutoff=cutoff,
    )
    tier2_added = _merge_rows(tier2_rows)
    chain_log["attempts"].append(
        {
            "tier": 2,
            "name": "sec_report_mirror",
            "status": "success" if tier2_added > 0 else "failed",
            "added": tier2_added,
            "error": tier2_error,
        }
    )
    if tier2_added > 0:
        chain_log["tier_used"] = "tier2_sec_report"
        return rows, chain_log

    tier3_rows, tier3_error = _discover_us_tier3_newswire_candidates(
        sources=sources,
        cutoff=cutoff,
    )
    tier3_added = _merge_rows(tier3_rows)
    chain_log["attempts"].append(
        {
            "tier": 3,
            "name": "newswire_fallback",
            "status": "success" if tier3_added > 0 else "failed",
            "added": tier3_added,
            "error": tier3_error,
        }
    )
    if tier3_added > 0:
        chain_log["tier_used"] = "tier3_newswire_fallback"
        return rows, chain_log

    chain_log["attempts"].append(
        {
            "tier": 4,
            "name": "existing_perplexity_allowed_domains",
            "status": "success" if len(rows) > 0 else "failed",
            "added": len(rows),
            "error": "" if len(rows) > 0 else "no_allowed_domain_candidates",
        }
    )
    chain_log["tier_used"] = "tier4_existing_perplexity"
    return rows, chain_log


def _has_material_filing_token(text: str, tokens: List[str]) -> bool:
    low = str(text or "").lower()
    for token in list(tokens or []):
        needle = str(token or "").strip().lower()
        if needle and needle in low:
            return True
    return False


def _slugify(value: str, max_len: int = 80) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    if not text:
        return "document"
    return text[:max_len].strip("_") or "document"


def _with_query_params(url: str, updates: Dict[str, str]) -> str:
    parts = urlsplit(str(url or "").strip())
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    for key, value in updates.items():
        query[str(key)] = str(value)
    new_query = urlencode(query, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _parse_date_from_pdf_url(url: str) -> Optional[datetime]:
    text = str(url or "").strip()
    if not text:
        return None
    asx_match = re.search(r"/asxpdf/(\d{8})/", text, flags=re.IGNORECASE)
    if asx_match:
        stamp = asx_match.group(1)
        try:
            return datetime.strptime(stamp, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _clean_html_fragment(value: str) -> str:
    text = re.sub(r"(?is)<[^>]+>", " ", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _infer_us_form_from_text(text: str) -> str:
    low = str(text or "").lower()
    patterns = [
        (r"\b(?:form\s*)?8[\-\s]?k\b", "8-K"),
        (r"\b(?:form\s*)?10[\-\s]?q\b", "10-Q"),
        (r"\b(?:form\s*)?10[\-\s]?k\b", "10-K"),
        (r"\b(?:form\s*)?20[\-\s]?f\b", "20-F"),
        (r"\b(?:form\s*)?6[\-\s]?k\b", "6-K"),
        (r"\b(?:form\s*)?def[\-\s]*14a\b", "DEF 14A"),
    ]
    for pattern, label in patterns:
        if re.search(pattern, low):
            return label
    return ""


def _extract_json_object_from_text(text: str) -> Optional[Dict[str, Any]]:
    payload = str(text or "").strip()
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    fenced = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", payload, flags=re.IGNORECASE)
    if fenced:
        try:
            parsed = json.loads(fenced.group(1))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    first = payload.find("{")
    last = payload.rfind("}")
    if first >= 0 and last > first:
        candidate = payload[first : last + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


def _build_price_sensitivity_assessment(
    *,
    exchange_id: str,
    title: str,
    source_title: str,
    source_url: str,
    pdf_url: str,
    ii_price_sensitive_marker: bool,
    token_marker: bool,
) -> Dict[str, Any]:
    exchange = str(exchange_id or "").strip().lower()
    text = " ".join(
        [
            str(title or ""),
            str(source_title or ""),
            str(source_url or ""),
            str(pdf_url or ""),
        ]
    )
    low = text.lower()

    reason_codes: List[str] = []
    score = 0.0

    explicit_hit = bool(ii_price_sensitive_marker or "price-sensitive" in low or "price sensitive" in low)
    if explicit_hit:
        score += 0.90
        reason_codes.append("explicit_price_sensitive_marker")

    if token_marker:
        score += 0.30
        reason_codes.append("material_filing_token_match")

    inferred_form = ""
    form_weight = 0.0
    if exchange in US_EXCHANGE_IDS:
        inferred_form = _infer_us_form_from_text(low)
        form_weight = float(US_PS_FORM_WEIGHTS.get(inferred_form, 0.0))
        if form_weight > 0:
            score += form_weight
            reason_codes.append(f"sec_form:{inferred_form}")

    pos_hits = [kw for kw in US_PS_POSITIVE_KEYWORDS if kw in low] if exchange in US_EXCHANGE_IDS else []
    neg_hits = [kw for kw in US_PS_NEGATIVE_KEYWORDS if kw in low] if exchange in US_EXCHANGE_IDS else []
    if pos_hits:
        score += min(0.35, 0.08 * len(pos_hits))
        reason_codes.append("material_keyword_hit")
    if neg_hits:
        score -= min(0.40, 0.12 * len(neg_hits))
        reason_codes.append("low_signal_keyword_hit")

    score = max(0.0, min(1.50, score))
    is_ps = bool(score >= 0.72)
    margin = abs(score - 0.72)
    confidence = max(0.35, min(0.95, 0.45 + margin))
    if explicit_hit:
        confidence = max(confidence, 0.85)
    if inferred_form in {"8-K", "10-Q", "10-K", "20-F", "6-K"}:
        confidence = max(confidence, 0.76)

    label = "uncertain"
    if confidence >= 0.86:
        label = "high"
    elif confidence >= 0.72:
        label = "medium"
    elif confidence >= 0.58:
        label = "low"

    if not reason_codes:
        reason_codes.append("no_strong_signal")

    return {
        "is_price_sensitive": is_ps,
        "confidence": round(float(confidence), 4),
        "score": round(float(score), 4),
        "label": label,
        "reason_codes": reason_codes[:12],
        "layers": {
            "explicit": {
                "hit": bool(explicit_hit),
                "signals": [
                    signal
                    for signal in [
                        "ii_marker" if ii_price_sensitive_marker else "",
                        "text_price_sensitive_marker"
                        if ("price-sensitive" in low or "price sensitive" in low)
                        else "",
                    ]
                    if signal
                ],
            },
            "form": {
                "form": inferred_form,
                "weight": round(float(form_weight), 4),
            },
            "keyword": {
                "positive_hits": pos_hits[:12],
                "negative_hits": neg_hits[:12],
            },
            "model": {
                "applied": False,
                "model": "",
                "is_price_sensitive": None,
                "confidence": 0.0,
                "reason_codes": [],
                "brief_reason": "",
            },
        },
    }


async def _apply_us_model_price_sensitivity_layer(
    *,
    rows: List[Dict[str, Any]],
    exchange_id: str,
    enabled: bool,
    model: str,
    timeout_seconds: float,
    max_candidates: int,
    min_heuristic_confidence: float,
    max_output_tokens: int,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "enabled": bool(enabled and str(exchange_id or "").strip().lower() in US_EXCHANGE_IDS),
        "model": str(model or ""),
        "attempted": 0,
        "applied": 0,
        "updated": 0,
        "errors": [],
    }
    if not report["enabled"]:
        return report

    candidates: List[Dict[str, Any]] = []
    for row in list(rows or []):
        ps = dict(row.get("price_sensitivity", {}) or {})
        conf = float(ps.get("confidence", 0.0) or 0.0)
        if conf >= float(min_heuristic_confidence):
            continue
        candidates.append(row)
        if len(candidates) >= max(1, int(max_candidates)):
            break

    for row in candidates:
        report["attempted"] += 1
        ps = dict(row.get("price_sensitivity", {}) or {})
        prompt_payload = {
            "exchange": str(exchange_id or "").upper(),
            "title": str(row.get("title", "")),
            "source_title": str(row.get("source_title", "")),
            "source_url": str(row.get("source_url", "")),
            "source_snippet": str(row.get("source_snippet", ""))[:1200],
            "pdf_url": str(row.get("pdf_url", "")),
            "published_at": str(row.get("published_at", "")),
            "heuristic_assessment": ps,
        }
        prompt = (
            "You are classifying whether an announcement/filing is likely price-sensitive for listed equities.\n"
            "Use only the metadata provided.\n"
            "Return STRICT JSON only:\n"
            "{\n"
            '  "is_price_sensitive": true/false,\n'
            '  "confidence": 0.0-1.0,\n'
            '  "reason_codes": ["..."],\n'
            '  "brief_reason": "..."\n'
            "}\n\n"
            f"INPUT:\n{json.dumps(prompt_payload, ensure_ascii=True)}"
        )
        response = await query_model(
            str(model),
            [{"role": "user", "content": prompt}],
            timeout=float(max(10.0, timeout_seconds)),
            max_tokens=max(120, int(max_output_tokens)),
        )
        if not response:
            report["errors"].append("empty_response")
            continue
        parsed = _extract_json_object_from_text(str(response.get("content", "")))
        if not isinstance(parsed, dict):
            report["errors"].append("json_parse_failed")
            continue
        model_bool = bool(parsed.get("is_price_sensitive", False))
        try:
            model_conf = float(parsed.get("confidence", 0.0) or 0.0)
        except Exception:
            model_conf = 0.0
        model_conf = max(0.0, min(1.0, model_conf))
        reason_codes = [str(item).strip() for item in (parsed.get("reason_codes") or []) if str(item).strip()]
        brief_reason = str(parsed.get("brief_reason", "")).strip()

        layers = dict(ps.get("layers", {}) or {})
        layers["model"] = {
            "applied": True,
            "model": str(model),
            "is_price_sensitive": model_bool,
            "confidence": round(model_conf, 4),
            "reason_codes": reason_codes[:8],
            "brief_reason": brief_reason,
        }
        ps["layers"] = layers
        report["applied"] += 1

        if model_conf >= 0.60:
            old_bool = bool(ps.get("is_price_sensitive", False))
            old_conf = float(ps.get("confidence", 0.0) or 0.0)
            ps["is_price_sensitive"] = model_bool
            ps["confidence"] = round(max(old_conf * 0.55 + model_conf * 0.45, model_conf), 4)
            codes = list(ps.get("reason_codes", []) or [])
            if "model_layer_override" not in codes:
                codes.append("model_layer_override")
            ps["reason_codes"] = codes[:16]
            if old_bool != model_bool:
                report["updated"] += 1
        row["price_sensitivity"] = ps

    return report


def _extract_ii_popover_blocks(html: str) -> Dict[str, Dict[str, Any]]:
    blocks: Dict[str, Dict[str, Any]] = {}
    for match in re.finditer(r'id="summary_popover_content_(\d+)"', html):
        pop_id = match.group(1)
        start = match.start()
        next_match = re.search(r'id="summary_popover_content_\d+"', html[match.end() :])
        if next_match:
            end = match.end() + next_match.start()
        else:
            end = min(len(html), start + 12000)
        block = html[start:end]
        is_price_sensitive = "Price-sensitive ASX Announcement" in block
        key_points_raw = re.findall(r"(?is)<li[^>]*class=\"mt-2\"[^>]*>(.*?)</li>", block)
        key_points = [_clean_html_fragment(item) for item in key_points_raw if _clean_html_fragment(item)]
        blocks[pop_id] = {
            "is_price_sensitive": bool(is_price_sensitive),
            "key_points": key_points,
        }
    return blocks


def _extract_ii_date_from_row(row_html: str) -> str:
    cells = re.findall(r'(?is)<td class="text-left">(.*?)</td>', row_html)
    for cell in cells:
        text = _clean_html_fragment(cell)
        if re.search(r"\b\d{1,2}\s+[A-Za-z]{3}\s+20\d{2}\b", text):
            return text
    return ""


def _extract_ii_title_for_pdf(row_html: str, pdf_url: str) -> str:
    escaped = re.escape(pdf_url)
    matches = re.findall(rf'(?is)<a[^>]+href="{escaped}"[^>]*>(.*?)</a>', row_html)
    best = ""
    for item in matches:
        text = _clean_html_fragment(item)
        if len(text) > len(best):
            best = text
    return best


def _extract_ii_pdf_entries(source_url: str, html: str) -> List[Dict[str, Any]]:
    popovers = _extract_ii_popover_blocks(html)
    rows = re.findall(r"(?is)<tr>.*?</tr>", html)
    entries_by_pdf: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        pdf_urls = re.findall(
            r"https://www\.aspecthuntley\.com\.au/asxdata/\d{8}/pdf/\d+\.pdf",
            row,
            flags=re.IGNORECASE,
        )
        if not pdf_urls:
            continue
        pdf_url = str(pdf_urls[0]).strip()
        if not pdf_url:
            continue
        summary_match = re.search(r'id="summary_popover_(\d+)"', row)
        summary_id = summary_match.group(1) if summary_match else ""
        pop = popovers.get(summary_id, {})
        row_date_raw = _extract_ii_date_from_row(row)
        row_date_dt = _parse_human_datetime(row_date_raw)
        title = _extract_ii_title_for_pdf(row, pdf_url)
        entries_by_pdf[pdf_url] = {
            "pdf_url": pdf_url,
            "title": title,
            "published_at_raw": row_date_raw,
            "published_dt_row": row_date_dt,
            "ii_has_summary_popover": bool(summary_id),
            "ii_summary_popover_id": summary_id,
            "ii_price_sensitive_marker": bool(pop.get("is_price_sensitive", False)),
            "ii_key_points": list(pop.get("key_points", []) or []),
            "discovery_source": source_url,
        }

    return list(entries_by_pdf.values())


def _is_intelligentinvestor_announcements_url(url: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    return (
        "intelligentinvestor.com.au" in host
        and "/shares/" in path
        and "/announcements" in path
    )


async def _extract_pdf_entries_from_page(
    client: httpx.AsyncClient,
    source_url: str,
    allowed_domain_suffixes: List[str],
) -> List[Dict[str, Any]]:
    source_url = str(source_url or "").strip()
    if not source_url:
        return []
    if _looks_like_pdf_url(source_url) or _looks_like_sec_filing_url(source_url):
        return [
            {
                "pdf_url": source_url,
                "discovery_source": source_url,
            }
        ]

    if _is_intelligentinvestor_announcements_url(source_url):
        probe_urls = [
            _with_query_params(source_url, {"page": "1", "size": "50"}),
            _with_query_params(source_url, {"page": "1", "size": "25"}),
        ]
        fallback_rows: List[Dict[str, Any]] = []
        for idx, probe_url in enumerate(probe_urls):
            try:
                response = await _http_get_with_sec_fallback(client, probe_url)
            except Exception:
                continue
            if response.status_code >= 400:
                continue
            html = str(response.text or "")
            if not html.strip():
                continue
            rows = _extract_ii_pdf_entries(probe_url, html)
            if not rows:
                continue
            flagged = sum(1 for row in rows if bool(row.get("ii_price_sensitive_marker", False)))
            if idx == 0 and flagged > 0:
                return rows
            if idx == 0:
                fallback_rows = rows
                continue
            return rows
        return fallback_rows

    try:
        response = await _http_get_with_sec_fallback(client, source_url)
    except Exception:
        return []
    if response.status_code >= 400:
        return []

    html = str(response.text or "")
    if not html.strip():
        return []

    found: List[str] = []
    found_norm = set()

    def _add(candidate: str) -> None:
        resolved = urljoin(source_url, str(candidate or "").strip())
        if not resolved:
            return
        norm = resolved.strip()
        if not norm or norm in found_norm:
            return
        found_norm.add(norm)
        found.append(norm)

    # ASX display pages often embed a hidden pdfURL.
    hidden = re.search(r'(?is)name="pdfURL"\s+value="([^"]+)"', html)
    if hidden:
        _add(hidden.group(1).strip())

    # Any direct ASX pdf link in page.
    asxpdf_matches = re.findall(
        r"(https://announcements\.asx\.com\.au/asxpdf/[^\s\"']+\.pdf)",
        html,
        flags=re.IGNORECASE,
    )
    for item in asxpdf_matches:
        _add(item.strip())

    # Generic href PDF extraction.
    hrefs = re.findall(r'(?is)href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']', html)
    for href in hrefs:
        _add(href.strip())

    # SEC filing HTML links (ixviewer/doc + direct EDGAR filing pages).
    sec_ix_links = re.findall(
        r'(?is)href=["\']([^"\']*sec\.gov/ixviewer/ix\.html\?doc=/Archives/EDGAR/data/[^"\']+)["\']',
        html,
    )
    for href in sec_ix_links:
        _add(href.strip())
    sec_archive_links = re.findall(
        r'(?is)href=["\']([^"\']*sec\.gov/Archives/EDGAR/data/[^"\']+\.(?:htm|html)(?:\?[^"\']*)?)["\']',
        html,
    )
    for href in sec_archive_links:
        _add(href.strip())

    scored: List[tuple[float, str]] = []
    for resolved in found:
        if not resolved:
            continue
        score = 0.0
        low = resolved.lower()
        if "announcements.asx.com.au/asxpdf/" in low:
            score += 4.0
        if "/sec.gov/archives/edgar/data/" in low or "sec.gov/ixviewer/ix.html" in low:
            score += 3.4
        if _is_allowed_domain(resolved, allowed_domain_suffixes):
            score += 1.0
        if "/pdf/" in low or low.endswith(".pdf"):
            score += 0.8
        if low.endswith(".htm") or low.endswith(".html") or ".htm?" in low or ".html?" in low:
            score += 0.5
        scored.append((score, resolved))

    if not scored:
        if _is_allowed_domain(source_url, allowed_domain_suffixes):
            # Fallback: decode the source page itself when no direct document
            # links are discoverable (common on some NYSE/SEC mirror pages).
            return [
                {
                    "pdf_url": source_url,
                    "discovery_source": source_url,
                    "fallback_page_decode": True,
                }
            ]
        return []
    scored.sort(key=lambda row: row[0], reverse=True)
    return [
        {
            "pdf_url": row[1],
            "discovery_source": source_url,
        }
        for row in scored
    ]


def _looks_like_dilution_signal_title(title: str) -> bool:
    low = str(title or "").lower()
    tokens = (
        "placement",
        "private placement",
        "registered direct",
        "atm program",
        "at-the-market",
        "shelf registration",
        "public offering",
        "capital raising",
        "issue of shares",
        "issue of options",
        "warrant",
        "convertible",
        "lender shares",
        "tranche",
        "quotation of securities",
        "appendix 2a",
        "appendix 3b",
        "appendix 3c",
    )
    return any(token in low for token in tokens)


def _is_high_signal_non_ps_title(title: str, extra_tokens: Optional[List[str]] = None) -> bool:
    low = str(title or "").lower()
    high_tokens = [
        "appendix 4c",
        "appendix 5b",
        "quarterly",
        "cashflow report",
        "annual report",
        "interim report",
        "investor presentation",
        "presentation",
        "feasibility study",
        "dfs",
        "pfs",
        "funding",
        "loan facility",
        "project update",
        "operations update",
        "production",
        "resource",
        "reserve",
    ]
    for token in list(extra_tokens or []):
        cleaned = str(token or "").strip().lower()
        if cleaned:
            high_tokens.append(cleaned)
    return any(token in low for token in high_tokens)


def _is_low_signal_admin_title(title: str, extra_tokens: Optional[List[str]] = None) -> bool:
    low = str(title or "").lower()
    low_tokens = [
        "cleansing notice",
        "change of director",
        "becoming a substantial holder",
        "application for quotation of securities",
        "notification regarding unquoted securities",
        "results of general meeting",
    ]
    for token in list(extra_tokens or []):
        cleaned = str(token or "").strip().lower()
        if cleaned:
            low_tokens.append(cleaned)
    return any(token in low for token in low_tokens)


def _overall_rank_tuple(row: Dict[str, Any]) -> tuple:
    dt = row.get("published_dt")
    if not isinstance(dt, datetime):
        dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
    score = float(row.get("score", 0.0) or 0.0)
    priority_boost = float(row.get("non_ps_priority_boost", 0.0) or 0.0)
    ps_conf = float(row.get("price_sensitive_confidence", 0.0) or 0.0)
    ps_bonus = (1.2 + min(1.2, ps_conf)) if bool(row.get("price_sensitive_marker", False)) else 0.0
    return (dt, score + priority_boost + ps_bonus)


def _is_strict_low_priority_non_ps(row: Dict[str, Any]) -> bool:
    """True for non-PS rows that should be fallback-only (e.g., cleansing/admin notices)."""
    if bool(row.get("price_sensitive_marker", False)):
        return False
    if bool(row.get("non_ps_high_signal", False)):
        return False
    if bool(row.get("dilution_signal_title", False)):
        return False
    if list(row.get("ii_key_points", []) or []):
        return False
    return bool(row.get("low_signal_admin_title", False))


def _select_balanced_entries(
    *,
    rows: List[Dict[str, Any]],
    target_ps: int,
    target_non_ps: int,
) -> List[Dict[str, Any]]:
    ranked = list(rows)
    ranked.sort(key=_overall_rank_tuple, reverse=True)

    ps_candidates = [row for row in ranked if bool(row.get("price_sensitive_marker", False))]
    non_ps_candidates = [row for row in ranked if not bool(row.get("price_sensitive_marker", False))]
    non_ps_preferred = [row for row in non_ps_candidates if not _is_strict_low_priority_non_ps(row)]
    non_ps_fallback_low = [row for row in non_ps_candidates if _is_strict_low_priority_non_ps(row)]

    # Rank non-price-sensitive by recency + explicit non-PS signal priority.
    non_ps_preferred.sort(
        key=lambda row: (
            row.get("published_dt") or datetime(1970, 1, 1, tzinfo=timezone.utc),
            float(row.get("score", 0.0) or 0.0) + float(row.get("non_ps_priority_boost", 0.0) or 0.0),
        ),
        reverse=True,
    )
    non_ps_fallback_low.sort(
        key=lambda row: (
            row.get("published_dt") or datetime(1970, 1, 1, tzinfo=timezone.utc),
            float(row.get("score", 0.0) or 0.0),
        ),
        reverse=True,
    )
    ps_candidates.sort(
        key=lambda row: (
            row.get("published_dt") or datetime(1970, 1, 1, tzinfo=timezone.utc),
            float(row.get("score", 0.0) or 0.0),
        ),
        reverse=True,
    )

    selected: List[Dict[str, Any]] = []
    used = set()

    def _take(pool: List[Dict[str, Any]], need: int, bucket: str) -> int:
        taken = 0
        for item in pool:
            if taken >= need:
                break
            key = str(item.get("pdf_url", ""))
            if not key or key in used:
                continue
            used.add(key)
            selected.append({**item, "selection_bucket": bucket})
            taken += 1
        return taken

    target_non_ps_int = max(0, int(target_non_ps))
    taken_ps = _take(ps_candidates, max(0, int(target_ps)), "price_sensitive")
    taken_non_ps_pref = _take(non_ps_preferred, target_non_ps_int, "non_price_sensitive")
    if taken_non_ps_pref < target_non_ps_int:
        remaining = target_non_ps_int - taken_non_ps_pref
        _take(non_ps_fallback_low, remaining, "non_price_sensitive_low_priority_backfill")
    taken_non_ps = sum(
        1
        for row in selected
        if str(row.get("selection_bucket", "")).startswith("non_price_sensitive")
    )

    # Backfill if one side is short.
    short_ps = max(0, int(target_ps) - taken_ps)
    short_non_ps = max(0, int(target_non_ps) - taken_non_ps)
    if short_ps > 0:
        taken_from_pref = _take(non_ps_preferred, short_ps, "backfill_non_ps")
        remaining = short_ps - taken_from_pref
        if remaining > 0:
            _take(non_ps_fallback_low, remaining, "backfill_non_ps_low_priority")
    if short_non_ps > 0:
        _take(ps_candidates, short_non_ps, "backfill_ps")

    # Final sort for processing determinism.
    selected.sort(key=_overall_rank_tuple, reverse=True)
    return selected


async def _decode_source_to_text(
    client: httpx.AsyncClient,
    source_url: str,
) -> Dict[str, Any]:
    try:
        response = await _http_get_with_sec_fallback(client, source_url)
    except Exception as exc:
        return {"ok": False, "error": f"download_failed: {exc}"}

    if response.status_code >= 400:
        return {"ok": False, "error": f"http_{response.status_code}"}

    content_type = str(response.headers.get("content-type", "")).lower()
    raw_url = str(source_url or "").strip().lower()
    looks_pdf = (
        "application/pdf" in content_type
        or _looks_like_pdf_url(raw_url)
    )
    if not looks_pdf:
        html = ""
        try:
            html = response.text
        except Exception:
            html = response.content.decode("utf-8", errors="replace")
        text = _html_to_text(html)
        if not text:
            return {"ok": False, "error": "empty_decoded_text"}
        return {
            "ok": True,
            "text": text,
            "page_count": 0,
            "title": "",
            "content_type": str(response.headers.get("content-type", "")),
            "bytes": len(response.content),
            "decode_mode": "html",
        }

    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        extracted = await extract_text_from_pdf(tmp_path)
        error = str(extracted.get("error", "")).strip()
        text = str(extracted.get("text", "")).strip()
        metadata = extracted.get("metadata", {}) or {}
        if error:
            return {"ok": False, "error": error}
        if not text:
            return {"ok": False, "error": "empty_decoded_text"}
        return {
            "ok": True,
            "text": text,
            "page_count": int(metadata.get("page_count", 0) or 0),
            "title": str(metadata.get("title", "")).strip(),
            "content_type": str(response.headers.get("content-type", "")),
            "bytes": len(response.content),
            "decode_mode": "pdf",
        }
    finally:
        if tmp_path:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass


def _write_dump_markdown(path: Path, row: Dict[str, Any], decoded: Dict[str, Any]) -> None:
    ii_key_points = list(row.get("ii_key_points", []) or [])
    ps = dict(row.get("price_sensitivity", {}) or {})
    ps_layers = dict(ps.get("layers", {}) or {})
    lines = [
        f"# PDF Dump: {row.get('title', 'Untitled')}",
        "",
        f"- exchange: {row.get('exchange', '')}",
        f"- source_url: {row.get('source_url', '')}",
        f"- pdf_url: {row.get('pdf_url', '')}",
        f"- domain: {row.get('domain', '')}",
        f"- published_at: {row.get('published_at', '')}",
        f"- score: {row.get('score', 0.0)}",
        f"- selection_bucket: {row.get('selection_bucket', '')}",
        f"- discovery_tier: {row.get('discovery_tier', '')}",
        f"- discovery_method: {row.get('discovery_method', '')}",
        f"- price_sensitive_marker: {bool(row.get('price_sensitive_marker', False))}",
        f"- price_sensitive_confidence: {float(row.get('price_sensitive_confidence', 0.0) or 0.0):.3f}",
        f"- price_sensitive_reason_codes: {', '.join(ps.get('reason_codes', []) or [])}",
        f"- ii_price_sensitive_marker: {bool(row.get('ii_price_sensitive_marker', False))}",
        f"- ii_key_points_count: {len(ii_key_points)}",
        f"- downloaded_at_utc: {datetime.now(timezone.utc).isoformat()}",
        f"- page_count: {decoded.get('page_count', 0)}",
        f"- decoded_chars: {len(str(decoded.get('text', '')))}",
        f"- content_type: {decoded.get('content_type', '')}",
        "",
        "---",
        "",
    ]
    if ps_layers:
        lines.extend(
            [
                "## Price Sensitivity Layers",
                "",
                "```json",
                json.dumps(ps_layers, ensure_ascii=False, indent=2),
                "```",
                "",
            ]
        )
    if ii_key_points:
        lines.extend(
            [
                "## Intelligent Investor Key Points",
                "",
                *[f"- {point}" for point in ii_key_points if str(point).strip()],
                "",
            ]
        )
    lines.extend(
        [
        "## Full Decoded Text",
        "",
        str(decoded.get("text", "")),
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Perplexity recent-PDF full-text dump")
    parser.add_argument("--query", required=True, help="User query for Perplexity retrieval")
    parser.add_argument("--ticker", default="", help="Ticker like ASX:WWI")
    parser.add_argument(
        "--exchange",
        default="",
        help="Optional exchange override (asx, nyse, nasdaq, tsx, tsxv, lse, aim)",
    )
    parser.add_argument("--depth", default="deep", choices=["basic", "deep"], help="Research depth")
    parser.add_argument(
        "--max-sources",
        type=int,
        default=0,
        help="Perplexity retrieval source window (0=exchange profile default)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Legacy total cap for dumped PDFs when bucket targets are zero",
    )
    parser.add_argument(
        "--target-price-sensitive",
        type=int,
        default=-1,
        help="Target count for price-sensitive announcements (-1=exchange profile default)",
    )
    parser.add_argument(
        "--target-non-price-sensitive",
        type=int,
        default=-1,
        help="Target count for non-price-sensitive high-signal announcements (-1=exchange profile default)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Recency window in days (0=exchange profile default)",
    )
    parser.add_argument(
        "--us-ps-model-layer",
        action="store_true",
        help="Enable model-assisted NYSE/NASDAQ price-sensitivity adjudication layer",
    )
    parser.add_argument(
        "--us-ps-model",
        default=DEFAULT_US_PS_MODEL,
        help=f"Model for US price-sensitivity layer (default: {DEFAULT_US_PS_MODEL})",
    )
    parser.add_argument(
        "--us-ps-model-timeout-seconds",
        type=float,
        default=30.0,
        help="Timeout per model adjudication call",
    )
    parser.add_argument(
        "--us-ps-model-max-candidates",
        type=int,
        default=20,
        help="Max uncertain US candidates to adjudicate with model layer",
    )
    parser.add_argument(
        "--us-ps-model-min-heuristic-confidence",
        type=float,
        default=0.78,
        help="Only rows below this heuristic confidence are sent to model layer",
    )
    parser.add_argument(
        "--us-ps-model-max-output-tokens",
        type=int,
        default=220,
        help="Completion token cap for model layer adjudication",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Optional output dir. Default: outputs/pdf_dump/<timestamp>_<ticker>",
    )
    parser.add_argument(
        "--skip-worker-summaries",
        action="store_true",
        help="Do not run worker summarization after PDF dump",
    )
    parser.add_argument(
        "--worker-model",
        default="openai/gpt-4o-mini",
        help="Model used by worker summarization stage",
    )
    parser.add_argument(
        "--worker-max-key-points",
        type=int,
        default=30,
        help="Maximum key points per kept document in worker summaries",
    )
    parser.add_argument(
        "--worker-concurrency",
        type=int,
        default=2,
        help="Parallel worker calls for summarization stage",
    )
    parser.add_argument(
        "--worker-timeout-seconds",
        type=float,
        default=180.0,
        help="Worker request timeout per document",
    )
    parser.add_argument(
        "--worker-enable-vision",
        action="store_true",
        help="Enable hybrid vision extraction in worker stage (default: enabled)",
    )
    parser.add_argument(
        "--worker-disable-vision",
        dest="worker_enable_vision",
        action="store_false",
        help="Disable hybrid vision extraction in worker stage",
    )
    parser.add_argument(
        "--worker-vision-model",
        default="openai/gpt-4o-mini",
        help="Vision model used by worker summarization stage",
    )
    parser.add_argument(
        "--worker-vision-max-pages",
        type=int,
        default=50,
        help="Max visual pages per PDF in worker stage (0=all pages)",
    )
    parser.add_argument(
        "--worker-vision-page-batch-size",
        type=int,
        default=4,
        help="Pages sent per vision model call in worker stage",
    )
    parser.add_argument(
        "--worker-vision-max-page-facts",
        type=int,
        default=12,
        help="Max visual key facts extracted per page in worker stage",
    )
    parser.add_argument(
        "--worker-vision-timeout-seconds",
        type=float,
        default=180.0,
        help="Vision extraction call timeout in worker stage",
    )
    parser.add_argument(
        "--worker-vision-max-tokens",
        type=int,
        default=1200,
        help="Vision extraction completion token cap per call in worker stage",
    )
    parser.set_defaults(worker_enable_vision=True)
    return parser


def _build_injection_bundle_from_worker_summary(
    *,
    out_dir: Path,
    worker_summary_json: Path,
) -> Dict[str, Any]:
    payload = json.loads(worker_summary_json.read_text(encoding="utf-8"))
    rows = list(payload.get("results", []) or [])
    prefiltered = [
        row
        for row in rows
        if bool(((row.get("importance", {}) or {}).get("keep_for_injection", False)))
    ]
    kept = [
        row
        for row in prefiltered
        if int((row.get("importance", {}) or {}).get("importance_score", 0) or 0) >= int(INJECTION_MIN_IMPORTANCE_SCORE)
    ]

    def _row_sort_key(row: Dict[str, Any]) -> tuple:
        source = row.get("source_meta", {}) or {}
        published = str(source.get("published_at", ""))
        ps = 1 if bool((row.get("price_sensitive", {}) or {}).get("is_price_sensitive", False)) else 0
        score = int((row.get("importance", {}) or {}).get("importance_score", 0) or 0)
        return (ps, score, published)

    kept.sort(key=_row_sort_key, reverse=True)

    compact_docs: List[Dict[str, Any]] = []
    for row in kept:
        source = row.get("source_meta", {}) or {}
        summary = row.get("summary", {}) or {}
        compact_docs.append(
            {
                "doc_id": str(row.get("doc_id", "")),
                "title": str(source.get("title", "")),
                "pdf_url": str(source.get("pdf_url", "")),
                "published_at": str(source.get("published_at", "")),
                "price_sensitive": bool((row.get("price_sensitive", {}) or {}).get("is_price_sensitive", False)),
                "price_sensitive_confidence": float((row.get("price_sensitive", {}) or {}).get("confidence", 0.0) or 0.0),
                "price_sensitive_reason": str((row.get("price_sensitive", {}) or {}).get("reason", "")),
                "importance_score": int((row.get("importance", {}) or {}).get("importance_score", 0) or 0),
                "importance_tier": str((row.get("importance", {}) or {}).get("tier", "")),
                "one_line": str(summary.get("one_line", "")),
                "key_points": list(summary.get("key_points", []) or [])[:30],
                "timeline_milestones": list(summary.get("timeline_milestones", []) or [])[:20],
                "capital_structure": list(summary.get("capital_structure", []) or [])[:10],
                "catalysts_next_12m": list(summary.get("catalysts_next_12m", []) or [])[:10],
                "risks_headwinds": list(summary.get("risks_headwinds", []) or [])[:10],
                "market_impact_assessment": str(summary.get("market_impact_assessment", "")),
            }
        )

    dropped_low_importance = max(0, len(prefiltered) - len(kept))
    bundle = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "worker_model": str(payload.get("worker_model", "")),
        "dump_dir": str(out_dir),
        "injection_policy": {
            "min_importance_score": int(INJECTION_MIN_IMPORTANCE_SCORE),
            "include_numeric_facts": False,
        },
        "total_processed": int(payload.get("total_processed", len(rows))),
        "kept_for_injection": len(compact_docs),
        "dropped_as_unimportant": int(payload.get("dropped_as_unimportant", max(0, len(rows) - len(compact_docs)))),
        "dropped_below_importance_threshold": int(dropped_low_importance),
        "docs": compact_docs,
    }

    json_path = out_dir / "injection_bundle.json"
    md_path = out_dir / "injection_bundle.md"
    json_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        "# Injection Bundle",
        "",
        f"- generated_at_utc: {bundle['generated_at_utc']}",
        f"- worker_model: {bundle['worker_model']}",
        f"- min_importance_score: {bundle['injection_policy']['min_importance_score']}",
        f"- include_numeric_facts: {bundle['injection_policy']['include_numeric_facts']}",
        f"- total_processed: {bundle['total_processed']}",
        f"- kept_for_injection: {bundle['kept_for_injection']}",
        f"- dropped_as_unimportant: {bundle['dropped_as_unimportant']}",
        f"- dropped_below_importance_threshold: {bundle['dropped_below_importance_threshold']}",
        "",
        "## Documents",
        "",
    ]
    for idx, row in enumerate(compact_docs, start=1):
        md_lines.append(
            f"{idx}. `{row.get('doc_id','')}` | ps={row.get('price_sensitive', False)} | "
            f"importance={row.get('importance_score', 0)} | {row.get('published_at', '')} | {row.get('title', '')}"
        )
    md_lines.extend(["", "## JSON", "", "```json", json.dumps(bundle, ensure_ascii=False, indent=2), "```", ""])
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    return {
        "bundle_json": str(json_path),
        "bundle_markdown": str(md_path),
        "kept_for_injection": len(compact_docs),
    }


async def _run(args: argparse.Namespace) -> int:
    load_dotenv()
    exchange_profile = _resolve_exchange_profile(
        query=str(args.query or ""),
        ticker=str(args.ticker or ""),
        explicit_exchange=str(args.exchange or ""),
    )
    exchange_id = str(exchange_profile.get("exchange", "unknown")).strip() or "unknown"
    allowed_domain_suffixes = list(exchange_profile.get("allowed_domain_suffixes", []) or [])
    lookback_days = (
        int(args.lookback_days)
        if int(args.lookback_days) > 0
        else int(exchange_profile.get("lookback_days_default", 365) or 365)
    )
    max_sources_default = int(exchange_profile.get("max_sources_default", 30) or 30)
    requested_max_sources = int(args.max_sources)
    if requested_max_sources <= 0:
        requested_max_sources = max_sources_default
    material_filing_tokens = list(exchange_profile.get("material_filing_tokens", []) or [])
    low_signal_notice_tokens = list(exchange_profile.get("low_signal_notice_tokens", []) or [])
    price_sensitive_strategy = str(exchange_profile.get("price_sensitive_strategy", "none") or "none")

    target_ps = (
        int(args.target_price_sensitive)
        if int(args.target_price_sensitive) >= 0
        else int(exchange_profile.get("target_price_sensitive_default", 0) or 0)
    )
    target_non_ps = (
        int(args.target_non_price_sensitive)
        if int(args.target_non_price_sensitive) >= 0
        else int(exchange_profile.get("target_non_price_sensitive_default", 20) or 20)
    )
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max(1, lookback_days))

    provider = PerplexityResearchProvider()
    result = await provider.gather(
        user_query=str(args.query),
        ticker=str(args.ticker or ""),
        depth=str(args.depth),
        max_sources=max(1, int(requested_max_sources)),
        research_brief="",
    )

    sources = list(result.get("results", []) or [])
    if not sources:
        print("No retrieval sources returned by Perplexity.")
        return 1

    candidate_rows: List[Dict[str, Any]] = []
    for source in sources:
        source_url = str(source.get("url", "")).strip()
        if not source_url or not _is_allowed_domain(source_url, allowed_domain_suffixes):
            continue
        published_at = str(source.get("published_at", "")).strip()
        parsed = _parse_iso_date(published_at)
        candidate_rows.append(
            _build_candidate_row(
                source_url=source_url,
                title=str(source.get("title", "")).strip() or "Untitled",
                published_dt=parsed,
                score=float(source.get("score", 0.0) or 0.0),
                discovery_tier=4,
                discovery_method="tier4_existing_perplexity",
                source_snippet=str(source.get("content", "")).strip(),
            )
        )

    ticker_slug = _slugify(args.ticker or "noticker", max_len=24)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir) if args.output_dir else Path("outputs") / "pdf_dump" / f"{ts}_{ticker_slug}"
    out_dir.mkdir(parents=True, exist_ok=True)

    target_ps = max(0, int(target_ps))
    target_non_ps = max(0, int(target_non_ps))
    if target_ps == 0 and target_non_ps == 0:
        target_files = max(1, int(args.top))
    else:
        target_files = max(1, target_ps + target_non_ps)

    timeout = httpx.Timeout(45.0, connect=20.0, read=45.0, write=20.0)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )
    }
    us_fallback_chain: Dict[str, Any] = {
        "enabled": False,
        "exchange": exchange_id,
        "tier_used": "tier4_existing_perplexity",
        "attempts": [],
    }
    us_ps_model_layer_report: Dict[str, Any] = {
        "enabled": False,
        "model": str(args.us_ps_model),
        "attempted": 0,
        "applied": 0,
        "updated": 0,
        "errors": [],
    }
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
        candidate_rows, us_fallback_chain = await _augment_us_candidates_with_fallback_chain(
            client,
            exchange_id=exchange_id,
            ticker=str(args.ticker or ""),
            sources=sources,
            candidate_rows=candidate_rows,
            allowed_domain_suffixes=allowed_domain_suffixes,
            cutoff=cutoff,
        )

        if not candidate_rows:
            print("No allowed-domain sources in lookback window.")
            return 1

        candidate_rows.sort(
            key=lambda row: (
                row["published_dt"] or datetime(1970, 1, 1, tzinfo=timezone.utc),
                row["score"],
            ),
            reverse=True,
        )

        pdf_pool: List[Dict[str, Any]] = []
        seen_pool_urls = set()
        for row in candidate_rows:
            discovered_entries = await _extract_pdf_entries_from_page(
                client,
                row["source_url"],
                allowed_domain_suffixes,
            )
            for discovered in discovered_entries:
                resolved_pdf = str(discovered.get("pdf_url", "")).strip()
                if not resolved_pdf:
                    continue
                if resolved_pdf in seen_pool_urls:
                    continue
                seen_pool_urls.add(resolved_pdf)
                discovered_row_dt = discovered.get("published_dt_row")
                if not isinstance(discovered_row_dt, datetime):
                    discovered_row_dt = None
                published_dt = (
                    _parse_date_from_pdf_url(resolved_pdf)
                    or discovered_row_dt
                    or row.get("published_dt")
                )
                if not published_dt:
                    continue
                if published_dt < cutoff:
                    continue
                published_at = published_dt.strftime("%Y-%m-%d")
                title = str(discovered.get("title", "")).strip() or str(row.get("title", "")).strip() or "Untitled"
                ii_price_sensitive_marker = bool(discovered.get("ii_price_sensitive_marker", False))
                ii_key_points = list(discovered.get("ii_key_points", []) or [])
                token_marker = _has_material_filing_token(
                    f"{title} {row.get('title', '')} {resolved_pdf} {row.get('source_url', '')}",
                    material_filing_tokens,
                )
                ps_assessment = _build_price_sensitivity_assessment(
                    exchange_id=exchange_id,
                    title=title,
                    source_title=str(row.get("title", "")),
                    source_url=str(row.get("source_url", "")),
                    pdf_url=resolved_pdf,
                    ii_price_sensitive_marker=ii_price_sensitive_marker,
                    token_marker=bool(token_marker),
                )
                price_sensitive_marker = bool(ps_assessment.get("is_price_sensitive", False))
                ps_confidence = float(ps_assessment.get("confidence", 0.0) or 0.0)
                title_low = title.lower()
                is_appendix_cashflow = ("appendix 4c" in title_low) or ("appendix 5b" in title_low)
                dilution_signal = _looks_like_dilution_signal_title(title)
                high_signal_non_ps = _is_high_signal_non_ps_title(title, material_filing_tokens)
                low_signal_admin = _is_low_signal_admin_title(title, low_signal_notice_tokens)

                non_ps_priority_boost = 0.0
                if is_appendix_cashflow:
                    non_ps_priority_boost += 7.0
                if high_signal_non_ps:
                    non_ps_priority_boost += 4.0
                if dilution_signal:
                    non_ps_priority_boost += 3.0
                if ii_key_points:
                    non_ps_priority_boost += 1.5
                if low_signal_admin and not dilution_signal and not is_appendix_cashflow:
                    non_ps_priority_boost -= 5.0
                if not price_sensitive_marker and ps_confidence >= 0.78:
                    non_ps_priority_boost += 1.2

                pdf_pool.append(
                    {
                        **row,
                        "exchange": exchange_id,
                        "source_title": str(row.get("title", "")),
                        "source_snippet": str(row.get("source_snippet", "")),
                        "title": title,
                        "pdf_url": resolved_pdf,
                        "published_dt": published_dt,
                        "published_at": published_at,
                        "price_sensitive_marker": bool(price_sensitive_marker),
                        "price_sensitive_confidence": round(ps_confidence, 4),
                        "price_sensitivity": ps_assessment,
                        "ii_price_sensitive_marker": ii_price_sensitive_marker,
                        "ii_key_points": ii_key_points,
                        "ii_has_summary_popover": bool(discovered.get("ii_has_summary_popover", False)),
                        "ii_summary_popover_id": str(discovered.get("ii_summary_popover_id", "")).strip(),
                        "published_at_raw": str(discovered.get("published_at_raw", "")).strip(),
                        "non_ps_priority_boost": float(non_ps_priority_boost),
                        "non_ps_high_signal": bool(high_signal_non_ps or is_appendix_cashflow),
                        "dilution_signal_title": bool(dilution_signal),
                        "low_signal_admin_title": bool(low_signal_admin),
                        "discovery_source": str(discovered.get("discovery_source", row.get("source_url", ""))),
                    }
                )

        if not pdf_pool:
            print("No resolvable PDF URLs found from filtered candidates.")
            return 1

        us_ps_model_layer_report = await _apply_us_model_price_sensitivity_layer(
            rows=pdf_pool,
            exchange_id=exchange_id,
            enabled=bool(args.us_ps_model_layer),
            model=str(args.us_ps_model),
            timeout_seconds=float(args.us_ps_model_timeout_seconds),
            max_candidates=max(1, int(args.us_ps_model_max_candidates)),
            min_heuristic_confidence=float(args.us_ps_model_min_heuristic_confidence),
            max_output_tokens=max(120, int(args.us_ps_model_max_output_tokens)),
        )
        for row in pdf_pool:
            ps = dict(row.get("price_sensitivity", {}) or {})
            row["price_sensitive_marker"] = bool(ps.get("is_price_sensitive", False))
            row["price_sensitive_confidence"] = round(float(ps.get("confidence", 0.0) or 0.0), 4)

        pdf_pool.sort(key=_overall_rank_tuple, reverse=True)

        if target_ps == 0 and target_non_ps == 0:
            selected_primary = []
            seen_primary = set()
            for item in pdf_pool:
                key = str(item.get("pdf_url", ""))
                if not key or key in seen_primary:
                    continue
                seen_primary.add(key)
                selected_primary.append({**item, "selection_bucket": "legacy_ranked"})
                if len(selected_primary) >= target_files:
                    break
        else:
            selected_primary = _select_balanced_entries(
                rows=pdf_pool,
                target_ps=target_ps,
                target_non_ps=target_non_ps,
            )

        if not selected_primary:
            print("No resolvable PDF URLs found from filtered candidates.")
            return 1

        selected_primary_urls = {str(item.get("pdf_url", "")) for item in selected_primary}
        fallback_pool = [
            item
            for item in pdf_pool
            if str(item.get("pdf_url", "")) not in selected_primary_urls
        ]
        fallback_pool.sort(key=_overall_rank_tuple, reverse=True)
        fallback_budget = max(target_files * 3, target_files)
        decode_queue = list(selected_primary) + [
            {**item, "selection_bucket": "overflow_fallback"}
            for item in fallback_pool[:fallback_budget]
        ]

        written: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []

        for idx, row in enumerate(decode_queue, start=1):
            if len(written) >= target_files:
                break
            decoded = await _decode_source_to_text(client, row["pdf_url"])
            if not decoded.get("ok"):
                failed.append(
                    {
                        "index": idx,
                        "title": row["title"],
                        "source_url": row["source_url"],
                        "pdf_url": row["pdf_url"],
                        "selection_bucket": row.get("selection_bucket", ""),
                        "price_sensitive_marker": bool(row.get("price_sensitive_marker", False)),
                        "price_sensitive_confidence": float(row.get("price_sensitive_confidence", 0.0) or 0.0),
                        "ii_price_sensitive_marker": bool(row.get("ii_price_sensitive_marker", False)),
                        "price_sensitivity": dict(row.get("price_sensitivity", {}) or {}),
                        "error": decoded.get("error", "decode_failed"),
                    }
                )
                continue

            date_part = row["published_dt"].strftime("%Y-%m-%d")
            name = _slugify(row["title"], max_len=72)
            file_index = len(written) + 1
            file_name = f"{file_index:02d}_{date_part}_{name}.md"
            file_path = out_dir / file_name
            _write_dump_markdown(file_path, row, decoded)

            written.append(
                {
                    "index": file_index,
                    "file": str(file_path),
                    "title": row["title"],
                    "source_url": row["source_url"],
                    "pdf_url": row["pdf_url"],
                    "published_at": row["published_at"],
                    "score": row["score"],
                    "selection_bucket": row.get("selection_bucket", ""),
                    "discovery_tier": row.get("discovery_tier", ""),
                    "discovery_method": row.get("discovery_method", ""),
                    "price_sensitive_marker": bool(row.get("price_sensitive_marker", False)),
                    "price_sensitive_confidence": float(row.get("price_sensitive_confidence", 0.0) or 0.0),
                    "price_sensitive_reason_codes": list((row.get("price_sensitivity", {}) or {}).get("reason_codes", []) or []),
                    "ii_price_sensitive_marker": bool(row.get("ii_price_sensitive_marker", False)),
                    "ii_key_points_count": len(list(row.get("ii_key_points", []) or [])),
                    "decode_mode": str(decoded.get("decode_mode", "")),
                    "decoded_chars": len(str(decoded.get("text", ""))),
                    "page_count": int(decoded.get("page_count", 0) or 0),
                }
            )

    written_ps = sum(1 for row in written if bool(row.get("price_sensitive_marker", False)))
    written_non_ps = len(written) - written_ps
    selected_primary_ps = sum(1 for row in selected_primary if bool(row.get("price_sensitive_marker", False)))
    selected_primary_non_ps = len(selected_primary) - selected_primary_ps

    selection_notes = [
        f"Exchange profile applied: {exchange_id}.",
        f"Price-sensitive strategy: {price_sensitive_strategy}.",
        "Layered price sensitivity: explicit marker + filing form + material keywords"
        + (" + model adjudication." if bool(us_ps_model_layer_report.get("enabled", False)) else "."),
        "If a bucket is short, backfill is taken from the opposite bucket.",
    ]
    if price_sensitive_strategy == "ii_popover_markers":
        selection_notes.insert(
            2,
            "Price-sensitive marker sourced from IntelligentInvestor announcement popovers.",
        )
    else:
        selection_notes.insert(
            2,
            "Price-sensitive marker sourced from exchange material-filing token heuristics.",
        )
    if bool(us_fallback_chain.get("enabled", False)):
        selection_notes.append(
            f"US fallback chain tier used: {us_fallback_chain.get('tier_used', 'tier4_existing_perplexity')}."
        )

    manifest = {
        "query": args.query,
        "ticker": args.ticker,
        "exchange": exchange_id,
        "depth": args.depth,
        "lookback_days": int(lookback_days),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "exchange_retrieval_profile": exchange_profile,
        "us_fallback_chain": us_fallback_chain,
        "price_sensitivity_layer_report": us_ps_model_layer_report,
        "allowed_domains": list(allowed_domain_suffixes),
        "retrieved_sources": len(sources),
        "selection_policy": {
            "target_price_sensitive": int(target_ps),
            "target_non_price_sensitive": int(target_non_ps),
            "fallback_mode_when_zero_targets": int(args.top),
            "notes": selection_notes,
        },
        "target_files": target_files,
        "candidate_sources_considered": len(candidate_rows),
        "candidate_pdfs_in_window": len(pdf_pool),
        "selected_primary_candidates": len(selected_primary),
        "selected_primary_price_sensitive": int(selected_primary_ps),
        "selected_primary_non_price_sensitive": int(selected_primary_non_ps),
        "decode_queue_candidates": len(decode_queue),
        "written_price_sensitive": int(written_ps),
        "written_non_price_sensitive": int(written_non_ps),
        "written_files": written,
        "failed_files": failed,
        "worker_settings": {
            "model": str(args.worker_model),
            "max_key_points": int(args.worker_max_key_points),
            "concurrency": int(args.worker_concurrency),
            "timeout_seconds": float(args.worker_timeout_seconds),
            "hybrid_vision_enabled": bool(args.worker_enable_vision),
            "hybrid_vision_model": str(args.worker_vision_model),
            "hybrid_vision_max_pages": int(args.worker_vision_max_pages),
            "hybrid_vision_page_batch_size": int(args.worker_vision_page_batch_size),
            "hybrid_vision_max_page_facts": int(args.worker_vision_max_page_facts),
            "hybrid_vision_timeout_seconds": float(args.worker_vision_timeout_seconds),
            "hybrid_vision_max_tokens": int(args.worker_vision_max_tokens),
        },
    }

    worker_summary_markdown = ""
    worker_summary_json = ""
    injection_bundle_json = ""
    injection_bundle_markdown = ""
    if not bool(args.skip_worker_summaries) and written:
        worker_cmd = [
            "uv",
            "run",
            "python",
            "test_pdf_dump_worker_summaries.py",
            "--dump-dir",
            str(out_dir),
            "--model",
            str(args.worker_model),
            "--max-key-points",
            str(max(1, int(args.worker_max_key_points))),
            "--concurrency",
            str(max(1, int(args.worker_concurrency))),
            "--timeout-seconds",
            str(max(30.0, float(args.worker_timeout_seconds))),
            "--vision-model",
            str(args.worker_vision_model),
            "--vision-max-pages",
            str(int(args.worker_vision_max_pages)),
            "--vision-page-batch-size",
            str(max(1, int(args.worker_vision_page_batch_size))),
            "--vision-max-page-facts",
            str(max(1, int(args.worker_vision_max_page_facts))),
            "--vision-timeout-seconds",
            str(max(30.0, float(args.worker_vision_timeout_seconds))),
            "--vision-max-tokens",
            str(max(400, int(args.worker_vision_max_tokens))),
        ]
        if bool(args.worker_enable_vision):
            worker_cmd.append("--worker-enable-vision")
        else:
            worker_cmd.append("--worker-disable-vision")
        print(f"Worker summaries: running {' '.join(worker_cmd)}")
        worker_proc = subprocess.run(worker_cmd, capture_output=True, text=True)
        if worker_proc.stdout.strip():
            print(worker_proc.stdout.strip())
        if worker_proc.returncode != 0:
            print("Worker summaries failed:")
            if worker_proc.stderr.strip():
                print(worker_proc.stderr.strip())
        else:
            worker_summary_markdown = str(out_dir / "announcement_summaries.md")
            worker_summary_json = str(out_dir / "announcement_summaries.json")
            try:
                bundle_info = _build_injection_bundle_from_worker_summary(
                    out_dir=out_dir,
                    worker_summary_json=Path(worker_summary_json),
                )
                injection_bundle_json = str(bundle_info.get("bundle_json", ""))
                injection_bundle_markdown = str(bundle_info.get("bundle_markdown", ""))
                print(
                    "Injection bundle: "
                    f"kept={bundle_info.get('kept_for_injection', 0)} "
                    f"json={injection_bundle_json}"
                )
            except Exception as exc:
                print(f"Injection bundle generation failed: {type(exc).__name__}: {exc}")

    manifest["worker_summary_markdown"] = worker_summary_markdown
    manifest["worker_summary_json"] = worker_summary_json
    manifest["injection_bundle_json"] = injection_bundle_json
    manifest["injection_bundle_markdown"] = injection_bundle_markdown

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    index_lines = [
        "# PDF Dump Index",
        "",
        f"- query: {args.query}",
        f"- ticker: {args.ticker}",
        f"- exchange: {exchange_id}",
        f"- depth: {args.depth}",
        f"- lookback_days: {lookback_days}",
        f"- retrieved_sources: {len(sources)}",
        f"- us_fallback_enabled: {bool(us_fallback_chain.get('enabled', False))}",
        f"- us_fallback_tier_used: {us_fallback_chain.get('tier_used', '')}",
        f"- us_ps_model_layer_enabled: {bool(us_ps_model_layer_report.get('enabled', False))}",
        f"- us_ps_model_layer_attempted: {int(us_ps_model_layer_report.get('attempted', 0) or 0)}",
        f"- us_ps_model_layer_updated: {int(us_ps_model_layer_report.get('updated', 0) or 0)}",
        f"- target_price_sensitive: {target_ps}",
        f"- target_non_price_sensitive: {target_non_ps}",
        f"- candidate_sources_considered: {len(candidate_rows)}",
        f"- candidate_pdfs_in_window: {len(pdf_pool)}",
        f"- selected_primary_candidates: {len(selected_primary)}",
        f"- selected_primary_price_sensitive: {selected_primary_ps}",
        f"- selected_primary_non_price_sensitive: {selected_primary_non_ps}",
        f"- decode_queue_candidates: {len(decode_queue)}",
        f"- written_files: {len(written)}",
        f"- written_price_sensitive: {written_ps}",
        f"- written_non_price_sensitive: {written_non_ps}",
        f"- failed_files: {len(failed)}",
        "",
        "## Written Files",
        "",
    ]
    for row in written:
        index_lines.append(
            f"- {row['index']:02d}. `{Path(row['file']).name}` | {row['published_at']} | "
            f"bucket={row.get('selection_bucket','')} | ps={row.get('price_sensitive_marker', False)} | "
            f"ps_conf={float(row.get('price_sensitive_confidence', 0.0) or 0.0):.2f} | "
            f"tier={row.get('discovery_tier','')} | mode={row.get('decode_mode','')} "
            f"| chars={row['decoded_chars']} | pages={row['page_count']} | {row['pdf_url']}"
        )
    if failed:
        index_lines.extend(["", "## Failures", ""])
        for row in failed:
            index_lines.append(
                f"- {row['index']:02d}. {row['title']} | bucket={row.get('selection_bucket','')} "
                f"| ps={row.get('price_sensitive_marker', False)} "
                f"| ps_conf={float(row.get('price_sensitive_confidence', 0.0) or 0.0):.2f} "
                f"| {row['error']} | {row['pdf_url']}"
            )
    (out_dir / "index.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")

    print(f"Output directory: {out_dir}")
    print(f"Exchange profile: {exchange_id}")
    print(f"Price-sensitive strategy: {price_sensitive_strategy}")
    print(f"Allowed domains: {', '.join(allowed_domain_suffixes)}")
    if bool(us_fallback_chain.get("enabled", False)):
        print(f"US fallback chain tier used: {us_fallback_chain.get('tier_used', '')}")
    if bool(us_ps_model_layer_report.get("enabled", False)):
        print(
            "US PS model layer: "
            f"attempted={int(us_ps_model_layer_report.get('attempted', 0) or 0)} "
            f"applied={int(us_ps_model_layer_report.get('applied', 0) or 0)} "
            f"updated={int(us_ps_model_layer_report.get('updated', 0) or 0)}"
        )
    print(f"Retrieved sources: {len(sources)}")
    print(f"Candidate sources considered: {len(candidate_rows)}")
    print(f"Candidate PDFs in window: {len(pdf_pool)}")
    print(f"Selected primary candidates: {len(selected_primary)}")
    print(f"Selected primary PS/non-PS: {selected_primary_ps}/{selected_primary_non_ps}")
    print(f"Decode queue candidates: {len(decode_queue)}")
    print(f"Written markdown dumps: {len(written)}")
    print(f"Written PS/non-PS: {written_ps}/{written_non_ps}")
    print(f"Failed decodes: {len(failed)}")
    if worker_summary_json:
        print(f"Worker summary JSON: {worker_summary_json}")
    if injection_bundle_json:
        print(f"Injection bundle JSON: {injection_bundle_json}")
    print(f"Manifest: {out_dir / 'manifest.json'}")
    print(f"Index: {out_dir / 'index.md'}")
    return 0 if written else 1


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
