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
import select
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse, urlsplit, urlunsplit

import httpx
from dotenv import load_dotenv

from backend.pdf_processor import extract_text_from_pdf
from backend.document_pipeline.io import write_json
from backend.document_pipeline.parse_service import parse_documents
from backend.research.providers.perplexity import PerplexityResearchProvider
from backend.template_loader import get_template_loader
from backend.openrouter import query_model
from backend.search import classify_asx_announcement, scrape_marketindex_announcements

try:
    from bs4 import BeautifulSoup  # type: ignore

    BS4_AVAILABLE = True
except Exception:
    BeautifulSoup = None  # type: ignore
    BS4_AVAILABLE = False


DEFAULT_ALLOWED_DOMAIN_SUFFIXES = (
    "asx.com.au",
    "marketindex.com.au",
    "intelligentinvestor.com.au",
)
_ASX_ANNOUNCEMENT_SEARCH_URL = "https://www.asx.com.au/asx/v2/statistics/announcements.do"

INJECTION_MIN_IMPORTANCE_SCORE = 80
US_EXCHANGE_IDS = {"nyse", "nasdaq"}
CANADIAN_EXCHANGE_IDS = {"tsx", "tsxv", "cse"}
ASX_EXCHANGE_IDS = {"asx"}
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
DEFAULT_OFFICIAL_SITE_FILTER_MODEL = os.getenv("OFFICIAL_SITE_FILTER_MODEL", "openai/gpt-5-mini")

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

CANADIAN_PS_POSITIVE_KEYWORDS = (
    "news release",
    "ni 43-101",
    "technical report",
    "resource estimate",
    "reserve estimate",
    "preliminary economic assessment",
    "pea",
    "pfs",
    "dfs",
    "drill",
    "assay",
    "intersects",
    "md&a",
    "financial statements",
    "funding",
    "private placement",
    "flow-through",
    "royalty",
    "offtake",
    "project update",
    "operations update",
    "investor presentation",
)

CANADIAN_PS_NEGATIVE_KEYWORDS = (
    "stock option grant",
    "grant of options",
    "warrant exercise",
    "insider report",
    "early warning report",
    "notice of annual meeting",
    "agm notice",
)


async def _await_with_phase_progress(
    coro: Any,
    *,
    label: str,
    interval_seconds: float = 15.0,
) -> Any:
    start = time.perf_counter()
    task = asyncio.create_task(coro)
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=max(1.0, float(interval_seconds)))
            if task in done:
                return task.result()
            elapsed = time.perf_counter() - start
            print(f"[{label}] waiting elapsed={elapsed:.1f}s", flush=True)
    finally:
        if not task.done():
            task.cancel()


def _print_source_preview(
    label: str,
    rows: List[Dict[str, Any]],
    *,
    url_key: str = "url",
    limit: int = 5,
) -> None:
    items = list(rows or [])
    print(f"[{label}] count={len(items)}", flush=True)
    for idx, row in enumerate(items[: max(0, int(limit))], start=1):
        url = str(row.get(url_key, "")).strip()
        host = urlparse(url).netloc.lower() if url else ""
        title = str(row.get("title", "")).strip() or "Untitled"
        published = (
            str(row.get("published_at", "")).strip()
            or (
                row.get("published_dt").strftime("%Y-%m-%d")
                if isinstance(row.get("published_dt"), datetime)
                else ""
            )
            or "n/a"
        )
        print(
            f"[{label}] {idx}. {published} | {host or 'no-host'} | {title[:140]}",
            flush=True,
        )

CANADIAN_PS_HIGH_IMPACT_KEYWORDS = (
    "private placement",
    "bought deal",
    "flow-through",
    "resource estimate",
    "reserve estimate",
    "ni 43-101",
    "drill",
    "assay",
    "intersects",
    "permit",
    "approval",
    "debt facility",
    "financing",
    "project update",
    "operations update",
    "offtake",
    "acquisition",
    "merger",
)

CANADIAN_BACKFILL_TRUSTED_DOMAINS = (
    "globenewswire.com",
)

CANADIAN_TRUST_DOMAIN_SCORE_OVERRIDES: Dict[str, int] = {
    "globenewswire.com": 100,
}

EXCHANGE_TO_YAHOO_SUFFIX = {
    "ASX": ".AX",
    "TSX": ".TO",
    "TSXV": ".V",
    "CSE": ".CN",
    "NYSE": "",
    "NASDAQ": "",
    "AMEX": "",
    "LSE": ".L",
    "AIM": ".L",
    "JSE": ".JO",
}


NON_ISSUER_OFFICIAL_SITE_BLOCKLIST = (
    "finance.yahoo.com",
    "query1.finance.yahoo.com",
    "marketwatch.com",
    "stockanalysis.com",
    "sec.gov",
    "tmx.com",
    "money.tmx.com",
    "globenewswire.com",
    "businesswire.com",
    "newsfilecorp.com",
    "thenewswire.com",
    "investing.com",
    "morningstar.com",
    "simplywall.st",
    "tradingview.com",
    "wikipedia.org",
    "linkedin.com",
    "x.com",
    "twitter.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
)


def _normalize_ticker_symbol(ticker: str) -> str:
    text = str(ticker or "").strip().upper()
    if not text:
        return ""
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    text = re.sub(r"[^A-Z0-9.\-]", "", text)
    return text


def _ticker_to_yahoo_symbol(ticker: str, exchange_id: str = "") -> str:
    symbol = _normalize_ticker_symbol(ticker)
    if not symbol:
        return ""
    if "." in symbol:
        return symbol
    exchange = str(exchange_id or "").strip().upper()
    suffix = EXCHANGE_TO_YAHOO_SUFFIX.get(exchange, "")
    if suffix and not symbol.endswith(suffix):
        return f"{symbol}{suffix}"
    return symbol


def _extract_site_domain_from_query(query: str) -> str:
    text = str(query or "")
    match = re.search(r"\bsite:([A-Za-z0-9.-]+\.[A-Za-z]{2,})\b", text)
    if not match:
        return ""
    host = str(match.group(1) or "").strip().lower()
    if not host:
        return ""
    host = host.split("/", 1)[0].strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_non_issuer_official_site_host(host: str) -> bool:
    host_low = str(host or "").strip().lower()
    if host_low.startswith("www."):
        host_low = host_low[4:]
    if not host_low:
        return True
    return any(_host_matches_suffix(host_low, bad) for bad in NON_ISSUER_OFFICIAL_SITE_BLOCKLIST)


def _discover_official_site_via_duckduckgo(
    *,
    ticker: str,
    exchange_id: str,
    query: str,
) -> Dict[str, str]:
    ticker_symbol = _normalize_ticker_symbol(ticker)
    if not ticker_symbol:
        return {}
    exchange_txt = str(exchange_id or "").upper().strip()
    search_q = f"{exchange_txt}:{ticker_symbol} official company website investor relations"
    if str(query or "").strip():
        search_q = f"{search_q} {str(query).strip()}"
    search_url = "https://duckduckgo.com/html/?q=" + quote(search_q)
    try:
        resp = httpx.get(
            search_url,
            timeout=20.0,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                ),
            },
        )
        if resp.status_code >= 400 or not str(resp.text or "").strip():
            return {}
        html = str(resp.text or "")
        hrefs = re.findall(r'(?is)href=["\']([^"\']+)["\']', html)
        candidates: List[str] = []
        for href in hrefs:
            value = str(href or "").strip()
            if not value:
                continue
            if "duckduckgo.com/l/?" in value and "uddg=" in value:
                try:
                    query_parts = dict(parse_qsl(urlparse(value).query, keep_blank_values=True))
                    value = str(query_parts.get("uddg", "")).strip() or value
                except Exception:
                    pass
            if not value.lower().startswith(("http://", "https://")):
                continue
            parsed = urlparse(value)
            host = parsed.netloc.lower().strip()
            if host.startswith("www."):
                host = host[4:]
            if not host or _is_non_issuer_official_site_host(host):
                continue
            candidates.append(host)
        if not candidates:
            return {}
        preferred = ""
        ticker_low = ticker_symbol.lower()
        for host in candidates:
            if ticker_low and ticker_low in host:
                preferred = host
                break
        if not preferred:
            preferred = candidates[0]
        return {
            "website_url": f"https://{preferred}",
            "website_domain": preferred,
            "yahoo_symbol": _ticker_to_yahoo_symbol(ticker, exchange_id=exchange_id),
            "source": "duckduckgo.search.official_site_fallback",
        }
    except Exception:
        return {}


def _resolve_official_issuer_site(
    *,
    ticker: str,
    exchange_id: str,
    query: str = "",
) -> Dict[str, str]:
    yahoo_symbol = _ticker_to_yahoo_symbol(ticker, exchange_id=exchange_id)
    if not yahoo_symbol:
        return {}

    website_url = ""
    resolved_source = ""
    try:
        import yfinance as yf  # type: ignore

        info = yf.Ticker(yahoo_symbol).get_info() or {}
        website_url = str(info.get("website", "")).strip()
        if website_url:
            resolved_source = "yfinance.info.website"
    except Exception:
        website_url = ""

    # Fallback: direct Yahoo quoteSummary API (avoids local yfinance/numpy import issues).
    if not website_url:
        try:
            quote_url = (
                "https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
                f"{quote(yahoo_symbol)}?modules=assetProfile"
            )
            resp = httpx.get(
                quote_url,
                timeout=15.0,
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json",
                },
            )
            if resp.status_code < 400:
                payload = resp.json() if resp.text else {}
                result_rows = (
                    (((payload or {}).get("quoteSummary") or {}).get("result") or [])
                    if isinstance(payload, dict)
                    else []
                )
                if isinstance(result_rows, list) and result_rows:
                    profile = (result_rows[0] or {}).get("assetProfile") or {}
                    website_url = str(profile.get("website", "")).strip()
                    if website_url:
                        resolved_source = "yahoo.quoteSummary.assetProfile.website"
        except Exception:
            website_url = website_url or ""

    if not website_url:
        return _discover_official_site_via_duckduckgo(
            ticker=ticker,
            exchange_id=exchange_id,
            query=query,
        )

    parsed = urlparse(website_url if "://" in website_url else f"https://{website_url}")
    host = parsed.netloc.lower().strip()
    if host.startswith("www."):
        host = host[4:]

    if not host:
        return {}
    if _is_non_issuer_official_site_host(host):
        return _discover_official_site_via_duckduckgo(
            ticker=ticker,
            exchange_id=exchange_id,
            query=query,
        )

    canonical_url = f"https://{host}"
    return {
        "website_url": canonical_url,
        "website_domain": host,
        "yahoo_symbol": yahoo_symbol,
        "source": resolved_source or "unknown",
    }


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
    raw = str(html or "")
    if not raw:
        return ""

    text = ""
    if BS4_AVAILABLE and BeautifulSoup is not None:
        try:
            soup = BeautifulSoup(raw, "html.parser")
            for tag in soup(["script", "style", "noscript", "svg", "canvas", "nav", "header", "footer", "aside", "form"]):
                tag.decompose()

            # Prefer article-like containers to avoid site navigation boilerplate.
            selector_order = (
                "article",
                "main article",
                "div.article-body",
                "div.news-release",
                "div.c-article-body",
                "section.article",
                "main",
            )
            best_node = None
            best_len = 0
            for selector in selector_order:
                for node in soup.select(selector):
                    node_text = node.get_text(" ", strip=True)
                    node_len = len(node_text)
                    if node_len > best_len:
                        best_len = node_len
                        best_node = node
            if best_node is not None and best_len > 500:
                soup = BeautifulSoup(str(best_node), "html.parser")
                for tag in soup(["script", "style", "noscript", "svg", "canvas", "nav", "header", "footer", "aside", "form"]):
                    tag.decompose()

            for br in soup.find_all("br"):
                br.replace_with("\n")
            for li in soup.find_all("li"):
                li.insert_before("\n- ")
                li.append("\n")
            for block in soup.find_all(["p", "div", "section", "article", "tr", "table", "h1", "h2", "h3", "h4", "h5", "h6"]):
                block.append("\n")
            text = soup.get_text(separator=" ", strip=False)
        except Exception:
            text = ""

    if not text:
        # Fallback path if parser is unavailable or parsing fails.
        text = raw
        text = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", text)
        text = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", text)
        text = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", text)
        text = re.sub(r"(?is)<br\s*/?>", "\n", text)
        text = re.sub(r"(?is)</p\s*>", "\n\n", text)
        text = re.sub(r"(?is)<li[^>]*>", "\n- ", text)
        text = re.sub(r"(?is)<[^>]+>", " ", text)

    text = unescape(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
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


def _int_config(value: Any, default: int) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


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


def _is_globenewswire_release_url(url: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    return _host_matches_suffix(host, "globenewswire.com") and "/news-release/" in path


def _is_globenewswire_org_search_url(url: str) -> bool:
    parsed = urlparse(str(url or "").strip())
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    return _host_matches_suffix(host, "globenewswire.com") and "/search/organization/" in path


def _is_low_signal_source_page(source_url: str, title: str = "") -> bool:
    parsed = urlparse(str(source_url or "").strip())
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    title_low = str(title or "").strip().lower()

    if _host_matches_suffix(host, "globenewswire.com"):
        # Organization/category/search pages are not filings.
        if "/en/search/" in path or "/fr/search/" in path:
            return True
        if "/newsroom/" in path:
            return True
        if "/news-release/" not in path:
            return True
        if "press release distribution and management" in title_low:
            return True

    return False


def _extract_row_datetime_for_recency(row: Dict[str, Any]) -> Optional[datetime]:
    published_raw = str(row.get("published_at", "")).strip()
    parsed = _parse_iso_date(published_raw)
    if parsed:
        return parsed
    src = str(row.get("source_url", "")).strip()
    return _parse_date_from_pdf_url(src)


def _host_matches_suffix(host: str, suffix: str) -> bool:
    host_low = str(host or "").strip().lower()
    suffix_low = str(suffix or "").strip().lower()
    if not host_low or not suffix_low:
        return False
    return host_low == suffix_low or host_low.endswith(f".{suffix_low}")


def _source_priority_score_for_url(
    source_url: str,
    *,
    source_quality_priority: Optional[Dict[str, Any]] = None,
) -> int:
    host = urlparse(str(source_url or "").strip()).netloc.lower()
    if not host:
        return 0

    best = 0
    for suffix, value in dict(source_quality_priority or {}).items():
        if _host_matches_suffix(host, str(suffix)):
            try:
                best = max(best, int(value))
            except Exception:
                continue
    for suffix, score in CANADIAN_TRUST_DOMAIN_SCORE_OVERRIDES.items():
        if _host_matches_suffix(host, suffix):
            best = max(best, int(score))
    return max(0, min(100, int(best)))


def _is_trusted_canadian_backfill_domain(
    *,
    source_url: str,
    allowed_domain_suffixes: List[str],
    source_quality_priority: Optional[Dict[str, Any]] = None,
) -> bool:
    host = urlparse(str(source_url or "").strip()).netloc.lower()
    if not host:
        return False

    # Backfill is only for non-allowlisted domains.
    for suffix in list(allowed_domain_suffixes or []):
        if _host_matches_suffix(host, str(suffix)):
            return False

    for suffix in CANADIAN_BACKFILL_TRUSTED_DOMAINS:
        if _host_matches_suffix(host, suffix):
            return True

    # Optional override via exchange profile quality map.
    return _source_priority_score_for_url(
        source_url,
        source_quality_priority=source_quality_priority,
    ) >= 86


def _looks_like_issuer_domain(
    *,
    source_url: str,
    title: str,
    ticker_symbol: str,
) -> bool:
    host = urlparse(str(source_url or "").strip()).netloc.lower()
    if not host:
        return False

    blocked_hosts = (
        "wikipedia.org",
        "reddit.com",
        "youtube.com",
        "yahoo.com",
        "marketwatch.com",
        "stockanalysis.com",
        "stockwatch.com",
        "streetwisereports.com",
        "taiwannews.com.tw",
        "taiwannews.com",
        "newsfilecorp.com",
        "globenewswire.com",
        "businesswire.com",
        "thenewswire.com",
        "fintel.io",
        "fintel.com",
        "investing.com",
        "marketbeat.com",
        "tipranks.com",
        "benzinga.com",
    )
    if any(_host_matches_suffix(host, suffix) for suffix in blocked_hosts):
        return False

    root = host
    if root.startswith("www."):
        root = root[4:]

    token_candidates: List[str] = []
    ticker = str(ticker_symbol or "").strip().lower()
    if ticker and len(ticker) >= 3:
        token_candidates.append(ticker)

    common_noise_tokens = {
        "gold",
        "silver",
        "uranium",
        "lithium",
        "mining",
        "resources",
        "resource",
        "energy",
        "corp",
        "corporation",
        "limited",
        "ltd",
        "inc",
        "company",
        "stockwatch",
        "streetwise",
        "report",
        "reports",
        "wire",
        "announces",
        "news",
        "release",
        "update",
        "project",
    }
    title_words = re.findall(r"[a-zA-Z0-9]{3,}", str(title or "").lower())[:8]
    for word in title_words:
        cleaned = str(word).strip().lower()
        if len(cleaned) < 4:
            continue
        if cleaned in common_noise_tokens:
            continue
        token_candidates.append(cleaned)
        if len(token_candidates) >= 4:
            break

    for token in dict.fromkeys(token_candidates):
        if token and token in root:
            return True
    return False


def _infer_issuer_domain_from_sources(
    *,
    sources: List[Dict[str, Any]],
    ticker_symbol: str,
) -> str:
    host_counts: Dict[str, int] = {}
    for row in list(sources or []):
        source_url = str(row.get("url", "")).strip()
        if not source_url:
            continue
        title = str(row.get("title", "")).strip()
        if not _looks_like_issuer_domain(
            source_url=source_url,
            title=title,
            ticker_symbol=ticker_symbol,
        ):
            continue
        host = urlparse(source_url).netloc.lower().strip()
        if not host:
            continue
        if host.startswith("www."):
            host = host[4:]
        if not host:
            continue
        host_counts[host] = host_counts.get(host, 0) + 1
    if not host_counts:
        return ""
    ranked = sorted(
        host_counts.items(),
        key=lambda item: (int(item[1]), -len(str(item[0]))),
        reverse=True,
    )
    top_host, top_hits = ranked[0]
    if int(top_hits) < 1:
        return ""
    return str(top_host).strip().lower()


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


def _is_canadian_backfill_candidate(
    *,
    exchange_id: str,
    title: str,
    source_url: str,
    source_snippet: str,
    material_filing_tokens: List[str],
    allowed_domain_suffixes: List[str],
    ticker_symbol: str,
    source_quality_priority: Optional[Dict[str, Any]] = None,
) -> bool:
    if str(exchange_id or "").strip().lower() not in CANADIAN_EXCHANGE_IDS:
        return False
    trusted_domain = _is_trusted_canadian_backfill_domain(
        source_url=source_url,
        allowed_domain_suffixes=allowed_domain_suffixes,
        source_quality_priority=source_quality_priority,
    )
    issuer_domain = _looks_like_issuer_domain(
        source_url=source_url,
        title=title,
        ticker_symbol=ticker_symbol,
    )
    if not trusted_domain and not issuer_domain:
        return False
    text = " ".join([str(title or ""), str(source_url or ""), str(source_snippet or "")]).lower()
    if not text.strip():
        return False
    # Exclude obvious low-value social/video/forum sources.
    blocked_markers = (
        "youtube.com",
        "youtu.be",
        "reddit.com",
        "stocktwits.com",
        "twitter.com",
        "x.com/",
        "seekingalpha.com",
        "fintel.io",
        "fintel.com",
        "marketbeat.com",
        "benzinga.com",
        "investing.com",
        "tipranks.com",
    )
    if any(marker in text for marker in blocked_markers):
        return False
    if issuer_domain and not any(
        token in text
        for token in (
            "investor",
            "announcement",
            "announcements",
            "news",
            "press",
            "release",
            "filing",
            "report",
            "results",
            "presentation",
        )
    ):
        return False
    if _has_material_filing_token(text, material_filing_tokens):
        return True
    if any(keyword in text for keyword in CANADIAN_PS_POSITIVE_KEYWORDS):
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
    low = text.lower()
    asx_match = re.search(r"/asxpdf/(\d{8})/", text, flags=re.IGNORECASE)
    if asx_match:
        stamp = asx_match.group(1)
        try:
            return datetime.strptime(stamp, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    # Generic path date (e.g., GlobeNewswire /YYYY/MM/DD/...)
    generic_match = re.search(r"/(20\d{2})/(0[1-9]|1[0-2])/([0-3]\d)/", text)
    if generic_match:
        y, m, d = generic_match.groups()
        try:
            return datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    # ISO-style date anywhere in URL/path/filename.
    iso_match = re.search(r"\b(20\d{2})[-_](0[1-9]|1[0-2])[-_](0[1-9]|[12]\d|3[01])\b", low)
    if iso_match:
        y, m, d = iso_match.groups()
        try:
            return datetime(int(y), int(m), int(d), tzinfo=timezone.utc)
        except ValueError:
            return None

    # Month name date in filename, e.g. June-30-2025.
    month_name_match = re.search(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*[-_ ]+([0-3]?\d)[-_ ,]+(20\d{2})\b",
        low,
    )
    if month_name_match:
        month_map = {
            "jan": 1,
            "feb": 2,
            "mar": 3,
            "apr": 4,
            "may": 5,
            "jun": 6,
            "jul": 7,
            "aug": 8,
            "sep": 9,
            "sept": 9,
            "oct": 10,
            "nov": 11,
            "dec": 12,
        }
        mon_txt, day_txt, year_txt = month_name_match.groups()
        month = month_map.get(mon_txt, 1)
        try:
            return datetime(int(year_txt), int(month), int(day_txt), tzinfo=timezone.utc)
        except ValueError:
            return None

    # Quarter-year tokens, e.g. q3_2025 or 2025_q3.
    qy_match = re.search(r"\bq([1-4])[-_ ]*(20\d{2})\b", low)
    if not qy_match:
        qy_match = re.search(r"\b(20\d{2})[-_ ]*q([1-4])\b", low)
        if qy_match:
            year_txt, q_txt = qy_match.groups()
            quarter = int(q_txt)
            month = {1: 3, 2: 6, 3: 9, 4: 12}[quarter]
            return datetime(int(year_txt), month, 1, tzinfo=timezone.utc)
    else:
        q_txt, year_txt = qy_match.groups()
        quarter = int(q_txt)
        month = {1: 3, 2: 6, 3: 9, 4: 12}[quarter]
        return datetime(int(year_txt), month, 1, tzinfo=timezone.utc)

    # Fallback: nearest plausible year in filename/path.
    year_matches = re.findall(r"\b(19\d{2}|20\d{2})\b", low)
    plausible_years = [int(y) for y in year_matches if 1990 <= int(y) <= datetime.now(timezone.utc).year + 1]
    if plausible_years:
        return datetime(max(plausible_years), 1, 1, tzinfo=timezone.utc)
    return None


async def _augment_with_globenewswire_org_feed(
    client: httpx.AsyncClient,
    *,
    sources: List[Dict[str, Any]],
    max_additional: int = 20,
) -> List[Dict[str, Any]]:
    """Expand source set with latest company releases from GlobeNewswire org feed."""
    base_sources = list(sources or [])
    if not base_sources:
        return base_sources

    seed_url = ""
    org_url = ""
    for row in base_sources:
        u = str(row.get("url", "")).strip()
        if _is_globenewswire_org_search_url(u):
            org_url = u
            break
    for row in base_sources:
        u = str(row.get("url", "")).strip()
        if _is_globenewswire_release_url(u):
            seed_url = u
            break
    if not org_url and not seed_url:
        return base_sources

    if not org_url:
        try:
            seed_resp = await _http_get_with_sec_fallback(client, seed_url)
        except Exception:
            return base_sources
        if seed_resp.status_code >= 400:
            return base_sources
        seed_html = str(seed_resp.text or "")
        if not seed_html.strip():
            return base_sources

        org_match = re.search(
            r'href="(/(?:(?:en|fr)/)?search/organization/[^"]+?)"',
            seed_html,
            flags=re.IGNORECASE,
        )
        if not org_match:
            return base_sources
        org_path = unescape(str(org_match.group(1) or "").strip())
        org_path = org_path.replace("§", "%C2%A7")
        if not org_path:
            return base_sources
        org_url = urljoin(seed_url, org_path)

    org_url = _with_query_params(org_url, {"page": "1"})

    try:
        org_resp = await _http_get_with_sec_fallback(client, org_url)
    except Exception:
        return base_sources
    if org_resp.status_code >= 400:
        return base_sources
    org_html = str(org_resp.text or "")
    if not org_html.strip():
        return base_sources

    release_matches = re.findall(
        r'href="(/(?:(?:en|fr)/)?news-release/20\d{2}/\d{2}/\d{2}/[^"]+)"[^>]*>(.*?)</a>',
        org_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not release_matches:
        return base_sources

    existing_urls = {str(row.get("url", "")).strip() for row in base_sources}
    additional_rows: List[Dict[str, Any]] = []
    seen_release_keys: set[str] = set()
    for rel_path, rel_title in release_matches:
        rel_url = urljoin(org_url, str(rel_path).strip())
        rel_key_match = re.search(r"/news-release/\d{4}/\d{2}/\d{2}/(\d+)/", rel_url)
        rel_key = rel_key_match.group(1) if rel_key_match else rel_url
        if rel_key in seen_release_keys:
            continue
        seen_release_keys.add(rel_key)

        if rel_url in existing_urls:
            continue
        title = _clean_html_fragment(unescape(str(rel_title or ""))).strip() or "Untitled"
        dt = _parse_date_from_pdf_url(rel_url)
        additional_rows.append(
            {
                "url": rel_url,
                "title": title,
                "published_at": dt.strftime("%Y-%m-%d") if dt else "",
                "content": "",
                "score": 0.0,
            }
        )
        existing_urls.add(rel_url)
        if len(additional_rows) >= max(1, int(max_additional)):
            break

    if not additional_rows:
        return base_sources
    return base_sources + additional_rows


def _globenewswire_release_sort_key(row: Dict[str, Any]) -> tuple[datetime, str]:
    dt = _parse_iso_date(str(row.get("published_at", "")).strip())
    if not dt:
        dt = _parse_date_from_pdf_url(str(row.get("url", "")).strip())
    if not dt:
        dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (dt, str(row.get("url", "")).strip())


async def _build_strict_canadian_release_sources(
    client: httpx.AsyncClient,
    *,
    seed_sources: List[Dict[str, Any]],
    max_releases: int = 30,
) -> List[Dict[str, Any]]:
    expanded = await _augment_with_globenewswire_org_feed(
        client,
        sources=seed_sources,
        max_additional=max(10, int(max_releases)),
    )
    release_rows: List[Dict[str, Any]] = []
    seen_release_ids: set[str] = set()
    for row in expanded:
        url = str(row.get("url", "")).strip()
        title = str(row.get("title", "")).strip()
        if not _is_globenewswire_release_url(url):
            continue
        if _is_low_signal_source_page(url, title):
            continue
        release_id_match = re.search(r"/news-release/\d{4}/\d{2}/\d{2}/(\d+)/", url)
        release_id = release_id_match.group(1) if release_id_match else url
        if release_id in seen_release_ids:
            continue
        seen_release_ids.add(release_id)
        release_rows.append(row)

    release_rows.sort(key=_globenewswire_release_sort_key, reverse=True)
    return release_rows[: max(1, int(max_releases))]


def _extract_company_term_candidates(
    *,
    ticker: str,
    user_query: str,
    seed_sources: List[Dict[str, Any]],
    official_domain: str,
) -> List[str]:
    terms: List[str] = []
    stopwords = {
        "latest",
        "filings",
        "filing",
        "news",
        "announcement",
        "announcements",
        "release",
        "releases",
        "tsxv",
        "tsx",
        "cse",
        "and",
        "or",
        "the",
        "for",
        "with",
        "from",
        "into",
        "corp",
        "inc",
        "ltd",
        "limited",
        "company",
        "mining",
        "resources",
        "powerpoint",
        "presentation",
        "investor",
    }
    symbol = _normalize_ticker_symbol(ticker)
    if symbol:
        terms.append(symbol)
    if official_domain:
        label = str(official_domain).split(".", 1)[0].strip()
        if label and len(label) >= 3 and label.lower() not in stopwords:
            terms.append(label)

    for row in list(seed_sources or [])[:10]:
        title = str(row.get("title", "")).strip()
        if not title:
            continue
        lead = re.match(r"^([A-Za-z][A-Za-z0-9&.\-]{2,})", title)
        if lead:
            lead_token = str(lead.group(1)).strip()
            if lead_token and lead_token.lower() not in stopwords:
                terms.append(lead_token)

    query_words = re.findall(r"[A-Za-z][A-Za-z0-9&.\-]{2,}", str(user_query or ""))
    for token in query_words:
        low = token.lower()
        if low in stopwords:
            continue
        if len(low) < 4 and low != symbol.lower():
            continue
        terms.append(token)

    deduped: List[str] = []
    seen = set()
    for item in terms:
        key = str(item or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(str(item).strip())
    if symbol and symbol.lower() not in {d.lower() for d in deduped}:
        deduped.insert(0, symbol)
    return deduped[:4]


async def _discover_globenewswire_search_release_sources(
    client: httpx.AsyncClient,
    *,
    terms: List[str],
    max_releases: int = 30,
    max_pages_per_term: int = 6,
    company_guard_terms: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen_release_keys: set[str] = set()
    limit = max(1, int(max_releases))
    max_pages = max(1, int(max_pages_per_term))
    guard_norm = [
        re.sub(r"[^a-z0-9]+", "", str(item or "").lower())
        for item in list(company_guard_terms or [])
        if str(item or "").strip()
    ]
    guard_norm = [item for item in guard_norm if len(item) >= 4]

    for term in list(terms or []):
        if len(rows) >= limit:
            break
        encoded_term = quote(str(term).strip())
        endpoint_templates = [
            (
                f"https://www.globenewswire.com/en/search/keyword/{encoded_term}",
                {"pageSize": "100"},
            ),
            (
                f"https://www.globenewswire.com/Search?query={encoded_term}",
                {},
            ),
        ]
        for endpoint_base, static_params in endpoint_templates:
            if len(rows) >= limit:
                break
            for page in range(1, max_pages + 1):
                if len(rows) >= limit:
                    break
                params = dict(static_params)
                params["page"] = str(page)
                search_url = _with_query_params(endpoint_base, params)
                try:
                    resp = await _http_get_with_sec_fallback(client, search_url)
                except Exception:
                    continue
                if resp.status_code >= 400:
                    continue
                html = str(resp.text or "")
                if not html.strip():
                    continue
                matches = re.findall(
                    r'(?is)<a[^>]+href=["\'](/(?:(?:en|fr)/)?news-release/20\d{2}/\d{2}/\d{2}/[^"\']+)["\'][^>]*>(.*?)</a>',
                    html,
                )
                if not matches and page > 1:
                    break
                for path, title_html in matches:
                    rel_url = urljoin(search_url, str(path or "").strip())
                    if not rel_url:
                        continue
                    if not _is_globenewswire_release_url(rel_url):
                        continue
                    release_id_match = re.search(r"/news-release/\d{4}/\d{2}/\d{2}/(\d+)/", rel_url)
                    release_key = release_id_match.group(1) if release_id_match else rel_url
                    if release_key in seen_release_keys:
                        continue
                    title = _clean_html_fragment(unescape(str(title_html or ""))).strip() or "Untitled"
                    low = f"{title} {rel_url}".lower()
                    term_norm = re.sub(r"[^a-z0-9]+", "", str(term or "").lower())
                    low_norm = re.sub(r"[^a-z0-9]+", "", low)
                    if term_norm and term_norm not in low_norm:
                        continue
                    if guard_norm and not any(g in low_norm for g in guard_norm):
                        continue
                    seen_release_keys.add(release_key)
                    dt = _parse_date_from_pdf_url(rel_url)
                    rows.append(
                        {
                            "url": rel_url,
                            "title": title,
                            "published_at": dt.strftime("%Y-%m-%d") if dt else "",
                            "content": "",
                            "score": 0.0,
                        }
                    )
                    if len(rows) >= limit:
                        break

    rows.sort(
        key=lambda row: (
            _parse_iso_date(str(row.get("published_at", "")).strip())
            or _parse_date_from_pdf_url(str(row.get("url", "")).strip())
            or datetime(1970, 1, 1, tzinfo=timezone.utc),
            str(row.get("url", "")),
        ),
        reverse=True,
    )
    return rows[:limit]


def _extract_company_guard_terms(
    *,
    ticker: str,
    user_query: str,
    official_domain: str,
) -> List[str]:
    terms: List[str] = []
    if official_domain:
        label = str(official_domain).split(".", 1)[0].strip().lower()
        if len(label) >= 4:
            terms.append(label)
    query_tokens = re.findall(r"[A-Za-z][A-Za-z0-9]{3,}", str(user_query or ""))
    stop = {
        "latest",
        "filings",
        "filing",
        "news",
        "announcement",
        "announcements",
        "release",
        "releases",
        "tsxv",
        "tsx",
        "cse",
        "company",
        "corp",
        "inc",
        "limited",
        "ltd",
    }
    for token in query_tokens:
        low = token.lower().strip()
        if low in stop:
            continue
        if len(low) >= 5:
            terms.append(low)
    symbol = _normalize_ticker_symbol(str(ticker or "")).lower()
    if symbol and len(symbol) >= 5:
        terms.append(symbol)

    out: List[str] = []
    seen = set()
    for item in terms:
        key = str(item or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out[:4]


def _clean_html_fragment(value: str) -> str:
    text = re.sub(r"(?is)<[^>]+>", " ", str(value or ""))
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_asx_datetime(date_text: str, time_text: str = "") -> Optional[datetime]:
    raw_date = str(date_text or "").strip()
    raw_time = str(time_text or "").strip().upper()
    if not raw_date:
        return None
    base: Optional[datetime] = None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            base = datetime.strptime(raw_date, fmt).replace(tzinfo=timezone.utc)
            break
        except Exception:
            continue
    if base is None:
        return None
    if not raw_time:
        return base
    for fmt in ("%I:%M %p", "%H:%M", "%I %p"):
        try:
            parsed_time = datetime.strptime(raw_time, fmt)
            return base.replace(hour=parsed_time.hour, minute=parsed_time.minute)
        except Exception:
            continue
    return base


def _parse_asx_announcement_rows(html_text: str) -> List[Dict[str, Any]]:
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
        published_at = published_dt.strftime("%Y-%m-%d") if published_dt else ""
        category, priority = classify_asx_announcement(title, display_url)
        rows.append(
            {
                "display_url": display_url,
                "title": title,
                "price_sensitive": bool(price_sensitive),
                "published_dt": published_dt,
                "published_at": published_at,
                "category": category,
                "priority": priority,
            }
        )
    return rows


async def _resolve_asx_display_to_pdf_url(
    client: httpx.AsyncClient,
    display_url: str,
) -> str:
    url = str(display_url or "").strip()
    if not url:
        return ""
    if _looks_like_pdf_url(url):
        return url
    for attempt in range(1, 4):
        try:
            response = await client.get(url)
        except Exception:
            if attempt < 3:
                await asyncio.sleep(0.25 * attempt)
                continue
            return ""
        if response.status_code >= 400:
            if response.status_code in {403, 425, 429, 500, 502, 503, 504} and attempt < 3:
                await asyncio.sleep(0.35 * attempt)
                continue
            return ""
        html_text = str(response.text or "")
        hidden = re.search(r'(?is)name="pdfURL"\s+value="([^"]+)"', html_text)
        if hidden:
            return unescape(hidden.group(1)).strip()
        direct = re.search(
            r"(https://announcements\.asx\.com\.au/asxpdf/[^\s\"']+\.pdf)",
            html_text,
            flags=re.IGNORECASE,
        )
        if direct:
            return unescape(direct.group(1)).strip()
        if attempt < 3:
            await asyncio.sleep(0.2 * attempt)
    return ""


async def _discover_direct_asx_primary_sources(
    *,
    symbol: str,
    lookback_days: int,
    max_rows: int,
) -> List[Dict[str, Any]]:
    normalized_symbol = _normalize_ticker_symbol(symbol)
    if not normalized_symbol:
        return []
    lookback_years = max(1, int((max(1, lookback_days) + 364) / 365))
    years = [datetime.now(timezone.utc).year - idx for idx in range(lookback_years)]
    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
    timeout = httpx.Timeout(35.0, connect=15.0, read=35.0, write=15.0)
    rows: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers={"User-Agent": user_agent}) as client:
        for year in years:
            params = {"by": "asxCode", "asxCode": normalized_symbol, "timeframe": "Y", "year": str(year)}
            try:
                response = await client.get(_ASX_ANNOUNCEMENT_SEARCH_URL, params=params)
            except Exception:
                continue
            if response.status_code >= 400:
                continue
            rows.extend(_parse_asx_announcement_rows(str(response.text or "")))

    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("display_url", "")).strip().lower()
        if not key or key in deduped:
            continue
        deduped[key] = row
    ranked = list(deduped.values())
    ranked.sort(
        key=lambda row: (
            row.get("published_dt") or datetime(1970, 1, 1, tzinfo=timezone.utc),
            1 if bool(row.get("price_sensitive", False)) else 0,
            -int(row.get("priority", 99) or 99),
        ),
        reverse=True,
    )
    ranked = [row for row in ranked if str(row.get("category", "")).strip().lower() != "ignore"]
    ranked = ranked[: max(1, int(max_rows))]
    if not ranked:
        return []

    resolved_rows: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers={"User-Agent": user_agent}) as client:
        for row in ranked:
            display_url = str(row.get("display_url", "")).strip()
            pdf_url = await _resolve_asx_display_to_pdf_url(client, display_url)
            final_url = pdf_url or display_url
            priority = int(row.get("priority", 3) or 3)
            resolved_rows.append(
                {
                    "url": final_url,
                    "title": str(row.get("title", "")).strip() or "ASX announcement",
                    "published_at": str(row.get("published_at", "")).strip(),
                    "content": (
                        "Deterministic ASX direct announcement search lane. "
                        + ("price-sensitive asx announcement. " if bool(row.get("price_sensitive", False)) else "")
                        + f"category={str(row.get('category', '')).strip().lower() or 'routine'} "
                        + f"priority={priority}."
                    ),
                    "score": {1: 0.98, 2: 0.84, 3: 0.62}.get(priority, 0.5),
                    "deterministic_source_kind": "asx_direct_announcement_pdf",
                    "price_sensitive_seed": bool(row.get("price_sensitive", False)),
                    "marketindex_priority": priority,
                }
            )
    return resolved_rows


def _official_row_datetime(row: Dict[str, Any]) -> Optional[datetime]:
    published_raw = str(row.get("published_at", "")).strip()
    parsed = _parse_iso_date(published_raw)
    if parsed:
        return parsed
    return _parse_date_from_pdf_url(str(row.get("url", "")).strip())


def _infer_official_doc_family(*, title: str, url: str) -> str:
    low = f"{str(title or '')} {str(url or '')}".lower()
    if any(token in low for token in ("md&a", "management discussion", "-mda", "_mda", " mda ")):
        return "mda"
    if any(token in low for token in ("financial statement", "consolidated financial", "-fs", "_fs")):
        return "financial_statements"
    if "aif" in low or "annual information form" in low:
        return "aif"
    if any(token in low for token in ("ni 43-101", "technical report", "pfs", "dfs", "pea", "feasibility")):
        return "technical_report"
    if any(token in low for token in ("annual report", "year-end", "ye-", "ye_")):
        return "annual_report"
    if any(token in low for token in ("interim", "quarterly", "q1-", "q2-", "q3-", "q4-")):
        return "interim_or_quarterly"
    if any(token in low for token in ("presentation", "deck", "corporate-presentation")):
        return "presentation"
    if any(token in low for token in ("news release", "/news/", "announcement")):
        return "news_release"
    if any(token in low for token in ("meeting", "voting", "board", "director", "equity grant", "compensation")):
        return "governance"
    return "other"


def _official_row_heuristic_score(row: Dict[str, Any], *, now: datetime) -> float:
    dt = _official_row_datetime(row)
    age_days = 99999.0
    if isinstance(dt, datetime):
        age_days = max(0.0, float((now - dt).days))

    family = _infer_official_doc_family(
        title=str(row.get("title", "")),
        url=str(row.get("url", "")),
    )
    family_weight = {
        "financial_statements": 9.0,
        "mda": 8.8,
        "aif": 8.0,
        "technical_report": 7.6,
        "annual_report": 7.0,
        "interim_or_quarterly": 6.6,
        "presentation": 6.0,
        "news_release": 5.5,
        "governance": 4.8,
        "other": 4.0,
    }.get(family, 4.0)

    recency = max(0.0, 2200.0 - age_days) / 2200.0
    ext_bonus = 0.0
    low_url = str(row.get("url", "")).lower()
    if low_url.endswith(".pdf"):
        ext_bonus += 0.45
    if "/wp-content/uploads/" in low_url:
        ext_bonus += 0.20
    return float((family_weight * 10.0) + (recency * 25.0) + ext_bonus)


def _heuristic_select_recent_official_rows(
    rows: List[Dict[str, Any]],
    *,
    max_keep: int,
) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    keep = max(1, int(max_keep))
    enriched: List[Dict[str, Any]] = []
    for row in list(rows or []):
        dt = _official_row_datetime(row)
        age_days = 99999
        if isinstance(dt, datetime):
            age_days = max(0, int((now - dt).days))
        family = _infer_official_doc_family(
            title=str(row.get("title", "")),
            url=str(row.get("url", "")),
        )
        enriched.append(
            {
                "row": row,
                "dt": dt,
                "age_days": age_days,
                "family": family,
                "score": _official_row_heuristic_score(row, now=now),
            }
        )

    enriched.sort(
        key=lambda item: (
            0 if item.get("age_days", 99999) <= 1460 else 1,  # prefer last 4 years
            -float(item.get("score", 0.0)),
            item.get("dt") or datetime(1970, 1, 1, tzinfo=timezone.utc),
        ),
        reverse=False,
    )

    selected: List[Dict[str, Any]] = []
    seen_urls = set()
    for item in enriched:
        url = str((item.get("row") or {}).get("url", "")).strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        selected.append(item["row"])
        if len(selected) >= keep:
            break
    return selected


async def _apply_official_site_mini_filter(
    *,
    rows: List[Dict[str, Any]],
    ticker: str,
    exchange_id: str,
    official_domain: str,
    enabled: bool,
    model: str,
    timeout_seconds: float,
    max_output_tokens: int,
    max_candidates: int,
    max_keep: int,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "enabled": bool(enabled),
        "model": str(model or ""),
        "input_rows": int(len(rows or [])),
        "attempted": 0,
        "applied": 0,
        "selected_rows": 0,
        "selected_indexes": [],
        "topped_up_with_heuristic": 0,
        "error": "",
        "mode": "heuristic_fallback",
    }
    keep_n = max(1, int(max_keep))
    base_rows = list(rows or [])
    if not base_rows:
        report["selected_rows"] = 0
        report["mode"] = "empty"
        return {"rows": [], "report": report}

    heuristic_selected = _heuristic_select_recent_official_rows(base_rows, max_keep=keep_n)
    if not bool(enabled):
        report["selected_rows"] = int(len(heuristic_selected))
        report["mode"] = "disabled"
        return {"rows": heuristic_selected, "report": report}

    now = datetime.now(timezone.utc)
    candidate_items: List[Dict[str, Any]] = []
    for idx, row in enumerate(base_rows):
        dt = _official_row_datetime(row)
        age_days = None
        if isinstance(dt, datetime):
            age_days = max(0, int((now - dt).days))
        family = _infer_official_doc_family(
            title=str(row.get("title", "")),
            url=str(row.get("url", "")),
        )
        candidate_items.append(
            {
                "index": int(idx),
                "title": str(row.get("title", "")).strip(),
                "url": str(row.get("url", "")).strip(),
                "published_at": str(row.get("published_at", "")).strip(),
                "family": family,
                "age_days": age_days,
                "heuristic_score": round(_official_row_heuristic_score(row, now=now), 3),
            }
        )

    candidate_items.sort(
        key=lambda item: (
            -float(item.get("heuristic_score", 0.0)),
            item.get("age_days") if isinstance(item.get("age_days"), int) else 999999,
            str(item.get("url", "")),
        )
    )
    shortlist = candidate_items[: max(10, int(max_candidates))]

    prompt_payload = {
        "task": "Select recent, investment-relevant primary documents from issuer website crawl results.",
        "ticker": str(ticker or ""),
        "exchange": str(exchange_id or "").upper(),
        "official_domain": str(official_domain or ""),
        "selection_rules": [
            "Prefer latest 24-36 months; older only if critical baseline filings.",
            "Prioritize financial statements, MD&A, annual/interim reports, AIF, NI 43-101/technical studies, quarterlys, investor presentations.",
            "Avoid repetitive stale archives and weak utility pages.",
            "Return at most max_keep indexes.",
        ],
        "max_keep": int(keep_n),
        "candidates": shortlist,
    }
    prompt = (
        "You are a document selector for investment prepass.\n"
        "Return STRICT JSON only:\n"
        "{\n"
        '  "selected_indexes": [int, ...],\n'
        '  "drop_indexes": [int, ...],\n'
        '  "notes": "short rationale"\n'
        "}\n\n"
        f"INPUT:\n{json.dumps(prompt_payload, ensure_ascii=True)}"
    )

    report["attempted"] = 1
    response = await query_model(
        str(model),
        [{"role": "user", "content": prompt}],
        timeout=float(max(10.0, timeout_seconds)),
        max_tokens=max(200, int(max_output_tokens)),
    )
    if not response:
        report["error"] = "model_no_response"
        report["selected_rows"] = int(len(heuristic_selected))
        return {"rows": heuristic_selected, "report": report}

    parsed = _extract_json_object_from_text(str(response.get("content", "")))
    if not parsed:
        report["error"] = "model_parse_failed"
        report["selected_rows"] = int(len(heuristic_selected))
        return {"rows": heuristic_selected, "report": report}

    selected_indexes_raw = parsed.get("selected_indexes", [])
    selected_indexes: List[int] = []
    for item in list(selected_indexes_raw or []):
        try:
            idx = int(item)
        except Exception:
            continue
        if 0 <= idx < len(base_rows):
            selected_indexes.append(idx)
    selected_indexes = list(dict.fromkeys(selected_indexes))
    if not selected_indexes:
        report["error"] = "model_empty_selection"
        report["selected_rows"] = int(len(heuristic_selected))
        return {"rows": heuristic_selected, "report": report}

    selected_rows: List[Dict[str, Any]] = []
    seen = set()
    for idx, row in enumerate(base_rows):
        if idx not in selected_indexes:
            continue
        url = str(row.get("url", "")).strip()
        if not url or url in seen:
            continue
        seen.add(url)
        selected_rows.append(row)
        if len(selected_rows) >= keep_n:
            break

    if not selected_rows:
        report["error"] = "model_selection_empty_after_validation"
        report["selected_rows"] = int(len(heuristic_selected))
        return {"rows": heuristic_selected, "report": report}

    if len(selected_rows) < keep_n:
        for row in heuristic_selected:
            url = str(row.get("url", "")).strip()
            if not url or url in seen:
                continue
            seen.add(url)
            selected_rows.append(row)
            report["topped_up_with_heuristic"] = int(report["topped_up_with_heuristic"]) + 1
            if len(selected_rows) >= keep_n:
                break

    report["applied"] = 1
    report["mode"] = "model"
    report["selected_rows"] = int(len(selected_rows))
    report["selected_indexes"] = selected_indexes[: keep_n]
    return {"rows": selected_rows, "report": report}


def _issuer_row_datetime(row: Dict[str, Any]) -> Optional[datetime]:
    published_raw = str(row.get("published_at", "")).strip()
    parsed = _parse_iso_date(published_raw)
    if parsed:
        return parsed
    return _parse_date_from_pdf_url(str(row.get("url", "")).strip())


def _issuer_row_priority_score(
    row: Dict[str, Any],
    *,
    now: datetime,
    official_domain: str,
) -> float:
    url = str(row.get("url", "")).strip()
    title = str(row.get("title", "")).strip()
    snippet = str(row.get("content", "")).strip()
    host = urlparse(url).netloc.lower().strip()
    dt = _issuer_row_datetime(row)
    age_days = 99999.0
    if isinstance(dt, datetime):
        age_days = max(0.0, float((now - dt).days))

    score = float(row.get("score", 0.0) or 0.0)
    if official_domain and _host_matches_suffix(host, official_domain):
        score += 6.0
    if _is_globenewswire_release_url(url):
        score += 2.0
    if host.endswith(".ca"):
        score += 0.7
    if "/wp-content/uploads/" in url.lower() or url.lower().endswith(".pdf"):
        score += 0.8
    text = " ".join([title, snippet]).lower()
    if "news release" in text:
        score += 1.0
    if "financial" in text or "md&a" in text or "management discussion" in text:
        score += 1.5
    if "technical report" in text or "ni 43-101" in text:
        score += 1.6
    # mild recency preference only, agent does final issuer relevance decision.
    score += max(0.0, 3650.0 - age_days) / 3650.0
    return float(score)


async def _apply_issuer_source_mini_filter(
    *,
    rows: List[Dict[str, Any]],
    ticker: str,
    exchange_id: str,
    official_domain: str,
    user_query: str,
    enabled: bool,
    model: str,
    timeout_seconds: float,
    max_output_tokens: int,
    max_candidates: int,
    min_keep: int,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "enabled": bool(enabled),
        "model": str(model or ""),
        "input_rows": int(len(rows or [])),
        "attempted": 0,
        "applied": 0,
        "selected_rows": int(len(rows or [])),
        "dropped_rows": 0,
        "mode": "disabled",
        "error": "",
    }
    base_rows = list(rows or [])
    if not base_rows:
        report["mode"] = "empty"
        return {"rows": [], "report": report}
    if not bool(enabled):
        return {"rows": base_rows, "report": report}

    keep_floor = max(1, int(min_keep or 1))
    wire_hosts = {
        "globenewswire.com",
        "newsfilecorp.com",
        "businesswire.com",
        "newswire.ca",
    }

    def _is_wire_host(host: str) -> bool:
        h = str(host or "").strip().lower()
        if not h:
            return False
        return any(h == x or h.endswith(f".{x}") for x in wire_hosts)

    async def _select_indexes_with_model(
        *,
        candidate_items: List[Dict[str, Any]],
        task_label: str,
    ) -> Dict[str, Any]:
        identity_hints: List[str] = []
        symbol = _normalize_ticker_symbol(str(ticker or ""))
        if symbol:
            identity_hints.append(symbol)
        domain_label = str(official_domain or "").split(".", 1)[0].strip().lower()
        if len(domain_label) >= 3:
            identity_hints.append(domain_label)
        query_words = re.findall(r"[A-Za-z][A-Za-z0-9&.-]{2,}", str(user_query or ""))
        stop = {
            "latest",
            "material",
            "filings",
            "filing",
            "announcement",
            "announcements",
            "news",
            "investor",
            "updates",
            "tsxv",
            "tsx",
            "cse",
            "corp",
            "inc",
            "limited",
            "ltd",
        }
        for token in query_words:
            low = token.strip().lower()
            if len(low) < 4 or low in stop:
                continue
            identity_hints.append(low)
        identity_hints = list(dict.fromkeys(identity_hints))[:8]

        payload = {
            "task": task_label,
            "ticker": str(ticker or ""),
            "exchange": str(exchange_id or "").upper(),
            "official_domain": str(official_domain or ""),
            "identity_hints": identity_hints,
            "rules": [
                "Keep rows clearly about the target issuer and its projects/filings/disclosures.",
                "Drop similarly named but different issuers/entities.",
                "Official-domain rows are strong evidence but are NOT the only valid evidence.",
                "Wire-release rows can be valid primary sources if they refer to the target issuer.",
                "If uncertain and no issuer identity evidence, drop.",
                "Keep all relevant rows; do not force a fixed count.",
            ],
            "candidates": candidate_items,
        }
        prompt = (
            "You are an issuer-identity filter for investment source ingestion.\n"
            "Return STRICT JSON only:\n"
            "{\n"
            '  "selected_indexes": [int, ...],\n'
            '  "drop_indexes": [int, ...],\n'
            '  "notes": "brief rationale"\n'
            "}\n\n"
            f"INPUT:\n{json.dumps(payload, ensure_ascii=True)}"
        )
        response = await query_model(
            str(model),
            [{"role": "user", "content": prompt}],
            timeout=max(10.0, float(timeout_seconds)),
            max_tokens=max(300, int(max_output_tokens)),
        )
        if not response:
            return {"selected_indexes": [], "error": "model_no_response"}
        parsed = _extract_json_object_from_text(str(response.get("content", "")))
        if not parsed:
            return {"selected_indexes": [], "error": "model_parse_failed"}
        selected_idx_raw = parsed.get("selected_indexes", [])
        selected_idx: List[int] = []
        for item in list(selected_idx_raw or []):
            try:
                i = int(item)
            except Exception:
                continue
            if 0 <= i < len(base_rows):
                selected_idx.append(i)
        selected_idx = list(dict.fromkeys(selected_idx))
        if not selected_idx:
            return {"selected_indexes": [], "error": "model_empty_selection"}
        return {"selected_indexes": selected_idx, "error": ""}

    now = datetime.now(timezone.utc)
    indexed_rows: List[Dict[str, Any]] = []
    for idx, row in enumerate(base_rows):
        url = str(row.get("url", "")).strip()
        if not url:
            continue
        dt = _issuer_row_datetime(row)
        age_days = None
        if isinstance(dt, datetime):
            age_days = max(0, int((now - dt).days))
        indexed_rows.append(
            {
                "index": int(idx),
                "url": url,
                "title": str(row.get("title", "")).strip(),
                "published_at": str(row.get("published_at", "")).strip(),
                "host": urlparse(url).netloc.lower().strip(),
                "snippet": str(row.get("content", "")).strip()[:220],
                "age_days": age_days,
                "priority_score": round(
                    _issuer_row_priority_score(
                        row,
                        now=now,
                        official_domain=official_domain,
                    ),
                    3,
                ),
            }
        )

    if not indexed_rows:
        report["mode"] = "no_indexable_rows"
        report["selected_rows"] = 0
        return {"rows": [], "report": report}

    indexed_rows.sort(
        key=lambda item: (
            -float(item.get("priority_score", 0.0)),
            item.get("age_days") if isinstance(item.get("age_days"), int) else 999999,
            str(item.get("url", "")),
        )
    )
    shortlist = indexed_rows[: max(20, int(max_candidates or 0))]
    report["attempted"] = 1
    selected_idx: List[int] = []
    chunk_errors: List[str] = []
    chunk_size = 48
    chunk_count = 0
    for start in range(0, len(shortlist), chunk_size):
        chunk_count += 1
        chunk = shortlist[start : start + chunk_size]
        chunk_selection = await _select_indexes_with_model(
            candidate_items=chunk,
            task_label=(
                "Issuer relevance gate: keep only sources clearly about the target issuer "
                "or its own projects/filings. Drop cross-issuer collisions."
            ),
        )
        chunk_selected = list(chunk_selection.get("selected_indexes", []) or [])
        if chunk_selected:
            selected_idx.extend(chunk_selected)
            continue
        # Retry once with smaller chunk if model parse/output failed.
        if len(chunk) > 20:
            mid = len(chunk) // 2
            for sub in (chunk[:mid], chunk[mid:]):
                sub_selection = await _select_indexes_with_model(
                    candidate_items=sub,
                    task_label=(
                        "Issuer relevance gate retry: keep only sources clearly about the target issuer; "
                        "drop similarly named unrelated issuers."
                    ),
                )
                sub_selected = list(sub_selection.get("selected_indexes", []) or [])
                if sub_selected:
                    selected_idx.extend(sub_selected)
                else:
                    chunk_errors.append(str(sub_selection.get("error", "model_empty_selection")))
        else:
            chunk_errors.append(str(chunk_selection.get("error", "model_empty_selection")))

    selected_idx = list(dict.fromkeys(selected_idx))
    report["chunks_attempted"] = int(chunk_count)
    report["chunks_failed"] = int(len(chunk_errors))
    if not selected_idx:
        report["mode"] = "fallback_keep_all"
        report["error"] = "all_chunks_failed:" + ",".join(chunk_errors[:3])
        return {"rows": base_rows, "report": report}

    # Secondary wire-only pass if first pass dropped all wire rows.
    wire_candidates = [item for item in shortlist if _is_wire_host(str(item.get("host", "")))]
    wire_selected = [
        idx
        for idx in selected_idx
        if _is_wire_host(urlparse(str(base_rows[idx].get("url", "")).strip()).netloc.lower().strip())
    ]
    report["wire_recovery_attempted"] = 0
    report["wire_recovery_added"] = 0
    if wire_candidates and not wire_selected:
        report["wire_recovery_attempted"] = 1
        wire_selection = await _select_indexes_with_model(
            candidate_items=wire_candidates,
            task_label=(
                "Wire-release issuer disambiguation: keep only wire rows that clearly refer to target issuer. "
                "Drop other issuers with similar names/tickers."
            ),
        )
        wire_idx = list(wire_selection.get("selected_indexes", []) or [])
        if wire_idx:
            merged = list(dict.fromkeys(list(selected_idx) + wire_idx))
            report["wire_recovery_added"] = int(max(0, len(merged) - len(selected_idx)))
            selected_idx = merged

    keep_rows: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for idx, row in enumerate(base_rows):
        if idx not in selected_idx:
            continue
        url = str(row.get("url", "")).strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        keep_rows.append(row)

    if len(keep_rows) < keep_floor:
        # Safety rail: top up from highest-priority issuer-domain rows first, then general pool.
        prioritized = [
            item
            for item in indexed_rows
            if official_domain
            and _host_matches_suffix(str(item.get("host", "")).strip().lower(), official_domain)
        ]
        for item in prioritized + indexed_rows:
            idx = int(item.get("index", -1))
            if idx < 0 or idx >= len(base_rows):
                continue
            row = base_rows[idx]
            url = str(row.get("url", "")).strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            keep_rows.append(row)
            if len(keep_rows) >= keep_floor:
                break

    report["applied"] = 1
    report["mode"] = "model"
    report["selected_rows"] = int(len(keep_rows))
    report["dropped_rows"] = max(0, int(len(base_rows) - len(keep_rows)))
    return {"rows": keep_rows, "report": report}


async def _discover_official_site_primary_sources(
    client: httpx.AsyncClient,
    *,
    official_domain: str,
    max_rows: int = 30,
) -> List[Dict[str, Any]]:
    domain = str(official_domain or "").strip().lower()
    if not domain:
        return []

    base = f"https://{domain}"
    seed_urls: List[str] = [
        base,
        f"{base}/news",
        f"{base}/newsroom",
        f"{base}/media",
        f"{base}/press-releases",
        f"{base}/announcements",
        f"{base}/investors",
        f"{base}/investor-centre",
        f"{base}/investor-center",
        f"{base}/investor-relations",
    ]
    keyword_re = re.compile(
        r"(news|press|release|announcement|investor|filing|report|results|presentation|quarterly|annual)",
        flags=re.IGNORECASE,
    )
    rows: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    def _add_row(url: str, title: str, content: str = "") -> None:
        normalized_url = str(url or "").strip()
        if not normalized_url or normalized_url in seen_urls:
            return
        try:
            host = urlparse(normalized_url).netloc.lower().strip()
        except Exception:
            return
        if not _host_matches_suffix(host, domain):
            return
        seen_urls.add(normalized_url)
        dt = _parse_date_from_pdf_url(normalized_url)
        rows.append(
            {
                "url": normalized_url,
                "title": str(title or "").strip() or "Issuer disclosure page",
                "published_at": dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else "",
                "content": str(content or "").strip(),
                "score": 0.0,
            }
        )

    for page_url in seed_urls:
        if len(rows) >= max_rows:
            break
        try:
            resp = await _http_get_with_sec_fallback(client, page_url)
        except Exception:
            continue
        if resp.status_code >= 400:
            continue
        html = str(resp.text or "")
        if not html.strip():
            continue

        title_match = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
        page_title = _clean_html_fragment(title_match.group(1)) if title_match else ""
        _add_row(page_url, page_title or f"{domain} disclosures", "Official issuer disclosure page.")
        if len(rows) >= max_rows:
            break

        hrefs = re.findall(r'(?is)href=["\']([^"\']+)["\']', html)
        for href in hrefs:
            if len(rows) >= max_rows:
                break
            resolved = urljoin(page_url, str(href or "").strip())
            if not resolved:
                continue
            parsed = urlparse(resolved)
            if parsed.scheme not in {"http", "https"}:
                continue
            host = parsed.netloc.lower().strip()
            if not _host_matches_suffix(host, domain):
                continue
            low = resolved.lower()
            path_low = str(parsed.path or "").lower()
            if low.endswith(".pdf"):
                _add_row(resolved, f"{domain} filing PDF")
                continue
            if keyword_re.search(path_low):
                _add_row(resolved, f"{domain} disclosure link")

    rows.sort(
        key=lambda row: (
            _parse_iso_date(str(row.get("published_at", "")).strip())
            or _parse_date_from_pdf_url(str(row.get("url", "")).strip())
            or datetime(1970, 1, 1, tzinfo=timezone.utc),
            str(row.get("url", "")),
        ),
        reverse=True,
    )
    return rows[: max(1, int(max_rows))]


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
    source_snippet: str,
    source_url: str,
    pdf_url: str,
    ii_price_sensitive_marker: bool,
    token_marker: bool,
) -> Dict[str, Any]:
    exchange = str(exchange_id or "").strip().lower()
    is_us = exchange in US_EXCHANGE_IDS
    is_canadian = exchange in CANADIAN_EXCHANGE_IDS
    text = " ".join(
        [
            str(title or ""),
            str(source_title or ""),
            str(source_snippet or ""),
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
        score += 0.34 if is_canadian else 0.30
        reason_codes.append("material_filing_token_match")

    inferred_form = ""
    form_weight = 0.0
    if is_us:
        inferred_form = _infer_us_form_from_text(low)
        form_weight = float(US_PS_FORM_WEIGHTS.get(inferred_form, 0.0))
        if form_weight > 0:
            score += form_weight
            reason_codes.append(f"sec_form:{inferred_form}")

    if is_us:
        pos_hits = [kw for kw in US_PS_POSITIVE_KEYWORDS if kw in low]
        neg_hits = [kw for kw in US_PS_NEGATIVE_KEYWORDS if kw in low]
    elif is_canadian:
        pos_hits = [kw for kw in CANADIAN_PS_POSITIVE_KEYWORDS if kw in low]
        neg_hits = [kw for kw in CANADIAN_PS_NEGATIVE_KEYWORDS if kw in low]
    else:
        pos_hits = []
        neg_hits = []
    if pos_hits:
        score += min(0.35 if is_canadian else 0.35, 0.08 * len(pos_hits))
        reason_codes.append("material_keyword_hit")
    if is_canadian:
        high_impact_hits = [kw for kw in CANADIAN_PS_HIGH_IMPACT_KEYWORDS if kw in low]
    else:
        high_impact_hits = []
    if high_impact_hits:
        score += min(0.36, 0.18 * len(high_impact_hits))
        reason_codes.append("high_impact_keyword_hit")
    if neg_hits:
        score -= min(0.55 if is_canadian else 0.40, (0.15 if is_canadian else 0.12) * len(neg_hits))
        reason_codes.append("low_signal_keyword_hit")

    score = max(0.0, min(1.50, score))
    threshold = 0.72 if is_canadian else 0.72
    if is_canadian and neg_hits and not explicit_hit:
        threshold += 0.06
    is_ps = bool(score >= threshold)
    if is_canadian and is_ps and not explicit_hit and not token_marker and len(pos_hits) < 2:
        is_ps = False
        reason_codes.append("insufficient_canadian_signal_density")
    margin = abs(score - threshold)
    if not reason_codes:
        confidence = 0.45
    elif is_ps:
        confidence = max(0.55, min(0.94, 0.58 + (0.65 * max(0.0, score - threshold))))
    else:
        confidence = max(0.40, min(0.78, 0.46 + (0.45 * max(0.0, threshold - score))))
    if explicit_hit:
        confidence = max(confidence, 0.85)
    if inferred_form in {"8-K", "10-Q", "10-K", "20-F", "6-K"} and is_ps:
        confidence = max(confidence, 0.76)
    if is_canadian and token_marker and is_ps:
        confidence = max(confidence, 0.72)

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
                "high_impact_hits": high_impact_hits[:12],
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
    allow_source_fallback_decode: bool = False,
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
        if _is_allowed_domain(source_url, allowed_domain_suffixes) or bool(allow_source_fallback_decode):
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


def _is_baseline_filing_doc(
    *,
    title: str,
    source_url: str,
    pdf_url: str,
) -> bool:
    text = " ".join(
        [
            str(title or ""),
            str(source_url or ""),
            str(pdf_url or ""),
        ]
    ).lower()
    # Restrict to real filing/report markers (not generic "annual growth" phrasing).
    hard_tokens = (
        "financial statements",
        "interim financial",
        "half-year report",
        "half yearly report",
        "half year report",
        "half-year financial report",
        "full year financial report",
        "condensed consolidated interim",
        "management discussion",
        "md&a",
        "annual information form",
        "technical report",
        "ni 43-101",
        "form 10-k",
        "form 10-q",
        "form 20-f",
        "form 6-k",
        "appendix 4c",
        "appendix 4d",
        "appendix 4e",
        "appendix 5b",
    )
    if any(token in text for token in hard_tokens):
        return True
    if re.search(r"\b(aif)\b", text):
        return True
    if re.search(r"\b(10-k|10-q|20-f|6-k)\b", text):
        return True
    # Annual/interim report needs "report" context.
    if (
        "annual report" in text
        or "interim report" in text
        or "quarterly report" in text
        or "half-year report" in text
        or "half yearly report" in text
        or "half year report" in text
        or "financial report" in text
    ):
        return True
    return False


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
    opposite_backfill_cap = max(
        1,
        min(
            3,
            int(round((max(0, int(target_ps)) + target_non_ps_int) * 0.2)) or 1,
        ),
    )
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
        capped_short_ps = min(short_ps, opposite_backfill_cap)
        taken_from_pref = _take(non_ps_preferred, capped_short_ps, "backfill_non_ps")
        remaining = capped_short_ps - taken_from_pref
        if remaining > 0:
            _take(non_ps_fallback_low, remaining, "backfill_non_ps_low_priority")
    if short_non_ps > 0:
        capped_short_non_ps = min(short_non_ps, opposite_backfill_cap)
        _take(ps_candidates, capped_short_non_ps, "backfill_ps")

    # Final sort for processing determinism.
    selected.sort(key=_overall_rank_tuple, reverse=True)
    return selected


def _select_latest_entries(
    *,
    rows: List[Dict[str, Any]],
    target: int,
    source_quality_priority: Dict[str, int],
) -> List[Dict[str, Any]]:
    """Deterministic latest-by-date selection used for strict Canadian primary lanes."""
    target_int = max(1, int(target))
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    ranked = list(rows or [])
    ranked.sort(
        key=lambda row: (
            1 if isinstance(row.get("published_dt"), datetime) else 0,
            row.get("published_dt") if isinstance(row.get("published_dt"), datetime) else epoch,
            _source_priority_score_for_url(
                str(row.get("pdf_url", "")).strip() or str(row.get("source_url", "")).strip(),
                source_quality_priority=source_quality_priority,
            ),
            float(row.get("score", 0.0) or 0.0),
        ),
        reverse=True,
    )

    selected: List[Dict[str, Any]] = []
    used = set()
    for item in ranked:
        key = str(item.get("pdf_url", "")).strip()
        if not key or key in used:
            continue
        if not isinstance(item.get("published_dt"), datetime):
            continue
        used.add(key)
        selected.append({**item, "selection_bucket": "deterministic_latest"})
        if len(selected) >= target_int:
            return selected

    for item in ranked:
        key = str(item.get("pdf_url", "")).strip()
        if not key or key in used:
            continue
        used.add(key)
        selected.append({**item, "selection_bucket": "deterministic_latest_undated_fallback"})
        if len(selected) >= target_int:
            break

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
        if len(text) < 180:
            return {"ok": False, "error": f"decoded_text_too_short:{len(text)}"}
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
        if len(text) < 120:
            return {"ok": False, "error": f"decoded_text_too_short:{len(text)}"}
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


def _row_to_document_ref(index: int, row: Dict[str, Any]) -> Dict[str, Any]:
    source_url = str(row.get("source_url", "")).strip()
    pdf_url = str(row.get("pdf_url", "")).strip()
    return {
        "doc_id": f"doc_{int(index):03d}",
        "title": str(row.get("title", "")).strip(),
        "source_url": source_url,
        "content_url": pdf_url or source_url,
        "pdf_url": pdf_url,
        "content_type_hint": "pdf" if _looks_like_pdf_url(pdf_url or source_url) else "html",
        "published_at": str(row.get("published_at", "")).strip(),
        "domain": str(row.get("domain", "")).strip(),
        "exchange": str(row.get("exchange", "")).strip(),
        "issuer_hint": str(row.get("company_name", "")).strip(),
        "ticker_hint": str(row.get("ticker", "")).strip(),
        "discovery_method": str(row.get("discovery_method", "")).strip(),
        "discovery_tier": str(row.get("discovery_tier", "")).strip(),
        "selection_bucket": str(row.get("selection_bucket", "")).strip(),
        "retrieval_meta": {
            "score": float(row.get("score", 0.0) or 0.0),
            "price_sensitive_marker": bool(row.get("price_sensitive_marker", False)),
            "price_sensitive_confidence": float(row.get("price_sensitive_confidence", 0.0) or 0.0),
            "ii_price_sensitive_marker": bool(row.get("ii_price_sensitive_marker", False)),
            "price_sensitive_reason_codes": list(
                (row.get("price_sensitivity", {}) or {}).get("reason_codes", []) or []
            ),
            "selection_bucket": str(row.get("selection_bucket", "")).strip(),
            "discovery_tier": str(row.get("discovery_tier", "")).strip(),
            "discovery_method": str(row.get("discovery_method", "")).strip(),
        },
    }


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
        "--skip-perplexity-retrieval",
        action="store_true",
        help="Skip Perplexity retrieval and rely on deterministic/native crawler lanes only",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Legacy total cap for dumped PDFs when bucket targets are zero",
    )
    parser.add_argument(
        "--document-parser",
        default="smart_default",
        help="Document parser backend id (default: smart_default)",
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
        "--disable-official-site-filter",
        action="store_true",
        help="Disable mini-agent filtering for issuer-site crawl candidates",
    )
    parser.add_argument(
        "--official-site-filter-model",
        default=DEFAULT_OFFICIAL_SITE_FILTER_MODEL,
        help=f"Model for issuer-site mini-agent filtering (default: {DEFAULT_OFFICIAL_SITE_FILTER_MODEL})",
    )
    parser.add_argument(
        "--official-site-filter-timeout-seconds",
        type=float,
        default=30.0,
        help="Timeout for issuer-site mini-agent filtering call",
    )
    parser.add_argument(
        "--official-site-filter-max-candidates",
        type=int,
        default=100,
        help="Max issuer-site candidate rows sent to mini-agent filter",
    )
    parser.add_argument(
        "--official-site-filter-max-output-tokens",
        type=int,
        default=1200,
        help="Completion token cap for issuer-site mini-agent filtering call",
    )
    parser.add_argument(
        "--official-site-crawl-max-rows",
        type=int,
        default=220,
        help="Max raw rows gathered from official issuer site crawler lane",
    )
    parser.add_argument(
        "--official-site-min-quota",
        type=int,
        default=0,
        help="Minimum official-site rows to keep after filtering (0=disabled)",
    )
    parser.add_argument(
        "--canadian-official-site-only",
        action="store_true",
        help="For TSX/TSXV/CSE, disable Globe/deterministic wire lanes and crawl issuer site only",
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
        default="openai/gpt-5-mini",
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
        "--worker-stage-timeout-seconds",
        type=float,
        default=0.0,
        help="Optional hard timeout for entire worker stage (0=disabled)",
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
        default="openai/gpt-5-mini",
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
    parser.add_argument(
        "--worker-complex-reasoning-model",
        default="",
        help="Optional escalation model for complex docs in worker stage (e.g., openai/gpt-5.4)",
    )
    parser.add_argument(
        "--worker-complex-reasoning-min-doc-chars",
        type=int,
        default=90000,
        help="Escalate when decoded text chars >= this threshold in worker stage",
    )
    parser.add_argument(
        "--worker-complex-reasoning-min-importance-score",
        type=int,
        default=90,
        help="Escalate when initial importance score >= this threshold in worker stage",
    )
    parser.set_defaults(worker_enable_vision=True)
    return parser


async def _run_pre_stage1_contamination_gate(
    *,
    compact_docs: List[Dict[str, Any]],
    target_ticker: str,
    target_company: str,
    model: str,
) -> Dict[str, Any]:
    total_docs = int(len(compact_docs))
    report: Dict[str, Any] = {
        "enabled": True,
        "model": str(model or ""),
        "target_ticker": str(target_ticker or ""),
        "target_company": str(target_company or ""),
        "total_docs": total_docs,
        "deterministic_dropped_doc_ids": [],
        "deterministic_related_party_doc_ids": [],
        "ambiguous_doc_ids": [],
        "model_reviewed_doc_ids": [],
        "model_dropped_doc_ids": [],
        "model_related_party_doc_ids": [],
        "model_inconclusive_doc_ids": [],
        "kept_doc_ids": [],
        "status": "not_run",
        "reason": "",
        "hard_fail": False,
    }
    if not compact_docs:
        report["status"] = "empty"
        return {"docs": compact_docs, "report": report}

    kept_docs: List[Dict[str, Any]] = []
    ambiguous_docs: List[Dict[str, Any]] = []
    for doc in compact_docs:
        validation = dict((doc.get("source_meta", {}) or {}).get("issuer_validation", {}) or {})
        status = str(validation.get("status", "unclear")).strip().lower()
        doc_id = str(doc.get("doc_id", "")).strip()
        if status == "mismatch":
            report["deterministic_dropped_doc_ids"].append(doc_id)
            continue
        if status == "related_party":
            report["deterministic_related_party_doc_ids"].append(doc_id)
            continue
        importance = int(doc.get("importance_score", 0) or 0)
        if status == "unclear" and importance >= 60:
            ambiguous_docs.append(doc)
            report["ambiguous_doc_ids"].append(doc_id)
            continue
        kept_docs.append(doc)

    if ambiguous_docs and str(model or "").strip():
        candidate_cards: List[Dict[str, Any]] = []
        for doc in ambiguous_docs[:8]:
            candidate_cards.append(
                {
                    "doc_id": str(doc.get("doc_id", "")).strip(),
                    "title": str(doc.get("title", "")).strip(),
                    "source_url": str(doc.get("source_url", "")).strip(),
                    "published_at": str(doc.get("published_at", "")).strip(),
                    "importance_score": int(doc.get("importance_score", 0) or 0),
                    "one_line": str(doc.get("one_line", "")).strip(),
                    "key_points": list(doc.get("key_points", []) or [])[:4],
                    "issuer_validation": dict((doc.get("source_meta", {}) or {}).get("issuer_validation", {}) or {}),
                }
            )
        payload = {
            "target_ticker": str(target_ticker or ""),
            "target_company": str(target_company or ""),
            "rules": [
                "Return keep only if the document is clearly about the target issuer.",
                "Return related_party only if the document is primarily about another issuer but explicitly concerns the target as a counterparty/JV participant.",
                "Return drop if issuer alignment is unclear or points to another issuer.",
            ],
            "docs": candidate_cards,
        }
        prompt = (
            "You are a pre-Stage-1 contamination gate for an investment-analysis packet.\n"
            "Return STRICT JSON only:\n"
            "{\n"
            '  "decisions": [\n'
            '    {"doc_id": "string", "decision": "keep|related_party|drop", "reason": "short"}\n'
            "  ]\n"
            "}\n\n"
            f"INPUT:\n{json.dumps(payload, ensure_ascii=True)}"
        )
        model_parse_ok = False
        try:
            response = await query_model(
                str(model),
                [{"role": "user", "content": prompt}],
                timeout=20.0,
                max_tokens=500,
            )
            parsed = _extract_json_object_from_text(str((response or {}).get("content", "")))
            model_parse_ok = bool(parsed)
        except Exception:
            parsed = {}
        decisions_by_id: Dict[str, str] = {}
        for row in list((parsed or {}).get("decisions", []) or []):
            if not isinstance(row, dict):
                continue
            doc_id = str(row.get("doc_id", "")).strip()
            decision = str(row.get("decision", "")).strip().lower()
            if not doc_id or decision not in {"keep", "related_party", "drop"}:
                continue
            decisions_by_id[doc_id] = decision
        for doc in ambiguous_docs:
            doc_id = str(doc.get("doc_id", "")).strip()
            report["model_reviewed_doc_ids"].append(doc_id)
            decision = decisions_by_id.get(doc_id, "")
            if decision == "keep":
                kept_docs.append(doc)
            elif decision == "related_party":
                report["model_related_party_doc_ids"].append(doc_id)
            elif decision == "drop":
                report["model_dropped_doc_ids"].append(doc_id)
            else:
                kept_docs.append(doc)
                report["model_inconclusive_doc_ids"].append(doc_id)
        if ambiguous_docs and not model_parse_ok:
            report["reason"] = "model_inconclusive_fail_open"
    else:
        for doc in ambiguous_docs:
            kept_docs.append(doc)
            report["model_inconclusive_doc_ids"].append(str(doc.get("doc_id", "")).strip())
        if ambiguous_docs:
            report["reason"] = "no_model_fail_open"

    report["kept_doc_ids"] = [str(doc.get("doc_id", "")).strip() for doc in kept_docs]
    total_dropped = (
        len(report["deterministic_dropped_doc_ids"])
        + len(report["deterministic_related_party_doc_ids"])
        + len(report["model_dropped_doc_ids"])
        + len(report["model_related_party_doc_ids"])
    )
    severe_ratio = (float(total_dropped) / float(total_docs)) if total_docs > 0 else 0.0
    if len(kept_docs) < max(6, min(10, total_docs // 2)):
        report["status"] = "hard_fail"
        report["reason"] = "too_few_docs_after_contamination_clipping"
        report["hard_fail"] = True
    elif severe_ratio >= 0.4 and total_dropped >= 4:
        report["status"] = "hard_fail"
        report["reason"] = "high_contamination_ratio"
        report["hard_fail"] = True
    else:
        report["status"] = "applied"
        if not str(report.get("reason", "")).strip():
            report["reason"] = "ok"
    return {"docs": kept_docs, "report": report}


async def _build_injection_bundle_from_worker_summary(
    *,
    out_dir: Path,
    worker_summary_json: Path,
    exchange_profile: Optional[Dict[str, Any]] = None,
    contamination_gate_model: str = "",
    target_ticker: str = "",
    target_company: str = "",
) -> Dict[str, Any]:
    payload = json.loads(worker_summary_json.read_text(encoding="utf-8"))
    rows = list(payload.get("results", []) or [])
    exchange_profile = dict(exchange_profile or {})
    domain_priority_raw = dict(exchange_profile.get("source_quality_priority", {}) or {})
    domain_priority: Dict[str, int] = {}
    for key, value in domain_priority_raw.items():
        host = str(key or "").strip().lower()
        if not host:
            continue
        try:
            domain_priority[host] = int(value)
        except Exception:
            continue
    def _doc_text(summary: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key in ("one_line", "key_facts_paragraph", "market_impact_assessment"):
            value = str(summary.get(key, "")).strip()
            if value:
                parts.append(value)
        for key in ("key_points", "capital_structure", "catalysts_next_12m", "risks_headwinds"):
            for item in list(summary.get(key, []) or []):
                text = str(item).strip()
                if text:
                    parts.append(text)
        return "\n".join(parts).strip()

    def _family_key(title: str, source_url: str = "") -> str:
        text = str(title or "").lower()
        text = re.sub(r"&amp;", "and", text)
        text = re.sub(r"\s+", " ", text)
        path = urlparse(str(source_url or "")).path.lower()
        if text in {
            "approvals and reporting",
            "announcements",
            "announcement centre",
            "announcement center",
            "investor centre",
            "investor center",
            "reporting hub",
            "media centre",
            "media center",
            "newsroom",
            "press releases",
            "asx releases",
        }:
            return "wrapper_index_page"
        if path and any(
            hint in path
            for hint in (
                "/announcements",
                "/announcement-centre",
                "/announcement-center",
                "/investor-centre",
                "/investor-center",
                "/reporting",
                "/newsroom",
                "/press-releases",
            )
        ) and text in {"updates", "latest updates", "investor updates", "reports", "news"}:
            return "wrapper_index_page"
        if "notification of buy-back" in text:
            return "buyback_notice"
        if "change in substantial holding" in text:
            return "substantial_holder_notice"
        if "cleansing statement" in text:
            return "cleansing_notice"
        if "application for quotation of securities" in text:
            return "quotation_notice"
        if "quarterly activities and cashflow report" in text or "quarterly report" in text:
            return "quarterly_report"
        if "investor presentation" in text or "webinar" in text or "corporate presentation" in text:
            return "presentation_or_webinar"
        text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
        return text[:64] if text else "misc"

    def _source_quality_score(source_url: str, pdf_url: str) -> int:
        host = urlparse(str(pdf_url or source_url or "")).netloc.lower()
        if host and domain_priority:
            for suffix, score in domain_priority.items():
                if host == suffix or host.endswith(f".{suffix}"):
                    return max(0, min(100, int(score)))
        if host.endswith("asx.com.au"):
            return 100
        if host.endswith("marketindex.com.au"):
            return 94
        if host.endswith("intelligentinvestor.com.au"):
            return 92
        if host.endswith("wcsecure.weblink.com.au"):
            return 90
        if host.endswith("aspecthuntley.com.au"):
            return 88
        if host.endswith("sedarplus.ca"):
            return 100
        if host.endswith("tmx.com") or host.endswith("tsx.com"):
            return 97
        if host.endswith("newsfilecorp.com"):
            return 92
        if host.endswith("globenewswire.com"):
            return 90
        if host.endswith("businesswire.com"):
            return 89
        if host.endswith("thenewswire.com"):
            return 88
        if host.endswith("stockhouse.com"):
            return 84
        if host:
            return 78
        return 60

    def _coverage_tags(title: str, text: str) -> List[str]:
        hay = f"{title}\n{text}".lower()
        tags: List[str] = []
        if any(k in hay for k in ("quarterly", "cashflow", "cash flow", "operating cash", "runway")):
            tags.append("cashflow")
        if any(k in hay for k in ("facility", "financing", "placement", "debt", "buy-back", "dilution", "capital")):
            tags.append("funding_capital")
        if any(k in hay for k in ("timeline", "target", "q1", "q2", "q3", "q4", "2026", "2027", "milestone")):
            tags.append("timeline")
        if any(k in hay for k in ("production", "boe", "opex", "capex", "margin", "guidance", "npv", "irr", "aisc")):
            tags.append("valuation_ops")
        if any(k in hay for k in ("board", "ceo", "executive", "management", "director")):
            tags.append("management")
        return sorted(set(tags))

    def _anchor_tags(title: str, text: str) -> List[str]:
        hay = f"{title}\n{text}".lower()
        tags: List[str] = []
        if any(k in hay for k in ("quarterly", "cashflow report", "annual report", "half-year", "interim report")):
            tags.append("periodic_report")
        if any(k in hay for k in ("investor presentation", "corporate presentation", "webinar", "deck")):
            tags.append("presentation")
        if any(k in hay for k in ("facility", "financing", "placement", "buy-back", "capital", "debt")):
            tags.append("capital_structure")
        return sorted(set(tags))

    retrieval_params = dict(exchange_profile or {})
    target_ps_default = _int_config(retrieval_params.get("target_price_sensitive_default"), 10)
    target_non_ps_default = _int_config(retrieval_params.get("target_non_price_sensitive_default"), 10)
    target_bundle_docs = max(20, target_ps_default + target_non_ps_default)

    # Model decides eligibility first; avoid hard-threshold rejection here.
    prefiltered = [
        row
        for row in rows
        if (
            bool((row.get("importance", {}) or {}).get("keep_for_injection", False))
            or bool((row.get("importance", {}) or {}).get("is_important", False))
        )
    ]
    if not prefiltered:
        prefiltered = list(rows)

    def _candidate_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
        source = row.get("source_meta", {}) or {}
        summary = row.get("summary", {}) or {}
        title = str(source.get("title", "")).strip() or str(row.get("doc_id", "")).strip() or "Untitled"
        source_url = str(source.get("source_url", "")).strip()
        pdf_url = str(source.get("pdf_url", "")).strip()
        host = urlparse(str(pdf_url or source_url or "")).netloc.lower()
        published_at = str(source.get("published_at", "")).strip()
        role_tags = [str(item).strip() for item in list(source.get("role_tags", []) or []) if str(item).strip()]
        family_key = _family_key(title, source_url)
        wrapper_page = (
            bool(source.get("wrapper_page", False))
            or "wrapper_or_index_page" in role_tags
            or family_key == "wrapper_index_page"
        )
        importance_score = int((row.get("importance", {}) or {}).get("importance_score", 0) or 0)
        importance_score = max(0, min(100, importance_score))
        price_sensitive = bool((row.get("price_sensitive", {}) or {}).get("is_price_sensitive", False))
        model_keep = bool((row.get("importance", {}) or {}).get("keep_for_injection", False))
        doc_text = _doc_text(summary)
        coverage = _coverage_tags(title, doc_text)
        anchor = _anchor_tags(title, doc_text)
        if "periodic_report" in role_tags and "periodic_report" not in anchor:
            anchor.append("periodic_report")
        if "presentation_or_deck" in role_tags and "presentation" not in anchor:
            anchor.append("presentation")
        if "financing_or_capital" in role_tags and "capital_structure" not in anchor:
            anchor.append("capital_structure")
        if "operational_update" in role_tags and "operational_update" not in anchor:
            anchor.append("operational_update")
        if "regulatory_or_approval" in role_tags and "regulatory_or_approval" not in anchor:
            anchor.append("regulatory_or_approval")
        coverage_score = min(100, len(coverage) * 22)
        source_score = _source_quality_score(source_url=source_url, pdf_url=pdf_url)
        return {
            "row": row,
            "doc_id": str(row.get("doc_id", "")).strip(),
            "title": title,
            "source_url": source_url,
            "pdf_url": pdf_url,
            "host": host,
            "published_at": published_at,
            "importance_score": importance_score,
            "price_sensitive": price_sensitive,
            "model_keep": model_keep,
            "coverage_tags": coverage,
            "coverage_score": int(coverage_score),
            "source_quality_score": int(source_score),
            "anchor_tags": sorted(set(anchor)),
            "family_key": family_key,
            "role_tags": role_tags,
            "wrapper_page": wrapper_page,
            "issuer_validation": dict(source.get("issuer_validation", {}) or {}),
            "corroboration_score": 70,
            "rank_score": 0.0,
        }

    candidates: List[Dict[str, Any]] = []
    for row in prefiltered:
        candidates.append(_candidate_from_row(row))

    all_candidates: List[Dict[str, Any]] = []
    seen_doc_ids_all: set[str] = set()
    for row in rows:
        candidate = _candidate_from_row(row)
        doc_id = str(candidate.get("doc_id", "")).strip()
        if not doc_id or doc_id in seen_doc_ids_all:
            continue
        seen_doc_ids_all.add(doc_id)
        all_candidates.append(candidate)

    def _apply_corroboration_and_rank(items: List[Dict[str, Any]]) -> None:
        family_counts: Dict[str, int] = {}
        family_hosts: Dict[str, set[str]] = {}
        for row in items:
            key = str(row.get("family_key", "misc"))
            family_counts[key] = family_counts.get(key, 0) + 1
            host = str(row.get("host", "")).strip().lower()
            if host:
                family_hosts.setdefault(key, set()).add(host)
        for row in items:
            fam = str(row.get("family_key", "misc"))
            fam_count = family_counts.get(fam, 1)
            unique_hosts = len(family_hosts.get(fam, set()))
            same_host_duplicates = max(0, fam_count - max(1, unique_hosts))

            corroboration_score = 70
            if fam_count <= 1:
                corroboration_score += 8
            else:
                corroboration_score += min(18, 7 * max(0, unique_hosts - 1))
                corroboration_score -= min(28, 10 * same_host_duplicates)
                if unique_hosts <= 1 and fam_count >= 3:
                    corroboration_score -= 10
            if bool(row.get("price_sensitive", False)):
                corroboration_score += 4
            row["corroboration_score"] = int(max(20, min(100, corroboration_score)))
            wrapper_penalty = 18.0 if bool(row.get("wrapper_page", False)) else 0.0
            family_crowding_penalty = 0.0
            if fam_count >= 4:
                family_crowding_penalty += min(8.0, 2.0 * float(fam_count - 3))
            if bool(row.get("wrapper_page", False)) and fam_count >= 2:
                family_crowding_penalty += 6.0
            rank = (
                (0.55 * float(row.get("importance_score", 0)))
                + (0.25 * float(row.get("coverage_score", 0)))
                + (0.10 * float(row.get("source_quality_score", 0)))
                + (0.10 * float(row.get("corroboration_score", 0)))
                + (5.0 if bool(row.get("model_keep", False)) else 0.0)
                + (4.0 if bool(row.get("price_sensitive", False)) else 0.0)
                - wrapper_penalty
                - family_crowding_penalty
            )
            row["rank_score"] = round(rank, 3)

    _apply_corroboration_and_rank(candidates)
    _apply_corroboration_and_rank(all_candidates)

    candidates.sort(
        key=lambda item: (
            float(item.get("rank_score", 0.0)),
            str(item.get("published_at", "")),
            int(item.get("importance_score", 0)),
        ),
        reverse=True,
    )

    # Deduplicate repetitive admin notice families.
    repetitive_families = {
        "buyback_notice",
        "substantial_holder_notice",
        "cleansing_notice",
        "quotation_notice",
    }
    by_family: Dict[str, List[Dict[str, Any]]] = {}
    for item in candidates:
        by_family.setdefault(str(item.get("family_key", "misc")), []).append(item)
    deduped: List[Dict[str, Any]] = []
    for family, items in by_family.items():
        items_sorted = sorted(
            items,
            key=lambda item: (
                float(item.get("rank_score", 0.0)),
                str(item.get("published_at", "")),
            ),
            reverse=True,
        )
        keep_n = len(items_sorted)
        if family in repetitive_families:
            keep_n = 1
            if any(
                int(item.get("importance_score", 0)) >= 75
                or bool(item.get("price_sensitive", False))
                for item in items_sorted
            ):
                keep_n = min(2, len(items_sorted))
        elif len(items_sorted) > 2:
            keep_n = 2
        deduped.extend(items_sorted[:keep_n])

    deduped.sort(
        key=lambda item: (
            float(item.get("rank_score", 0.0)),
            str(item.get("published_at", "")),
            int(item.get("importance_score", 0)),
        ),
        reverse=True,
    )

    min_docs = max(10, min(24, target_bundle_docs))
    max_docs = max(min_docs, min(36, target_bundle_docs + 10))
    context_quota = max(1, min(3, target_bundle_docs // 10))
    def _family_cap(item: Dict[str, Any]) -> int:
        family = str(item.get("family_key", "misc"))
        if bool(item.get("wrapper_page", False)) or family == "wrapper_index_page":
            return 1
        if family in repetitive_families:
            return 2
        return 3

    def _family_count(selected_items: List[Dict[str, Any]], item: Dict[str, Any]) -> int:
        family = str(item.get("family_key", "misc"))
        return sum(1 for row in selected_items if str(row.get("family_key", "misc")) == family)

    def _is_fill_eligible(item: Dict[str, Any]) -> bool:
        importance = int(item.get("importance_score", 0) or 0)
        source_quality = int(item.get("source_quality_score", 0) or 0)
        is_ps = bool(item.get("price_sensitive", False))
        family = str(item.get("family_key", "misc"))
        if (bool(item.get("wrapper_page", False)) or family == "wrapper_index_page") and not is_ps and importance < 75:
            return False
        if family in repetitive_families and not is_ps and importance < 70:
            return False
        if is_ps:
            return True
        if importance >= 40:
            return True
        if source_quality >= 92 and importance >= 30:
            return True
        return False

    selected: List[Dict[str, Any]] = []
    for item in deduped:
        if len(selected) >= max_docs:
            break
        if _family_count(selected, item) >= _family_cap(item):
            continue
        if int(item.get("importance_score", 0)) >= 50 or bool(item.get("price_sensitive", False)):
            selected.append(item)
    if len(selected) < min_docs:
        for item in deduped:
            if item in selected:
                continue
            if _family_count(selected, item) >= _family_cap(item):
                continue
            if not _is_fill_eligible(item):
                continue
            selected.append(item)
            if len(selected) >= min_docs:
                break

    # Context quota: keep at least one medium-signal contextual document.
    context_present = any(35 <= int(item.get("importance_score", 0)) < 60 for item in selected)
    if not context_present and context_quota > 0:
        context_candidates = [
            item
            for item in deduped
            if item not in selected and 35 <= int(item.get("importance_score", 0)) < 60
        ]
        if context_candidates:
            context_pick = context_candidates[0]
            if _family_count(selected, context_pick) < _family_cap(context_pick):
                selected.append(context_pick)

    # Anchor floor: ensure at least one periodic report, one presentation, one capital-structure item if available.
    required_anchors = ("periodic_report", "presentation", "capital_structure")
    for anchor in required_anchors:
        if any(anchor in list(item.get("anchor_tags", []) or []) for item in selected):
            continue
        replacement = next(
            (
                item
                for item in deduped
                if item not in selected and anchor in list(item.get("anchor_tags", []) or [])
            ),
            None,
        )
        if replacement is not None:
            if _family_count(selected, replacement) < _family_cap(replacement):
                selected.append(replacement)

    # If dedupe collapsed too aggressively, pad from ranked candidates.
    if len(selected) < min_docs:
        reserve_candidates = [
            item
            for item in all_candidates
            if item not in selected and str(item.get("family_key", "misc")) not in repetitive_families
            and _is_fill_eligible(item)
        ]
        reserve_candidates.extend(
            [
                item
                for item in all_candidates
                if item not in selected and item not in reserve_candidates
                and _is_fill_eligible(item)
            ]
        )
        for item in reserve_candidates:
            if item in selected:
                continue
            if _family_count(selected, item) >= _family_cap(item):
                continue
            selected.append(item)
            if len(selected) >= min_docs:
                break
    if len(selected) < min_docs:
        # Final robustness fallback: still require baseline eligibility.
        for item in all_candidates:
            if item in selected:
                continue
            if not _is_fill_eligible(item):
                continue
            if _family_count(selected, item) >= _family_cap(item):
                continue
            selected.append(item)
            if len(selected) >= min_docs:
                break

    # De-duplicate final list by doc_id while preserving order.
    seen_doc_ids: set[str] = set()
    final_selected: List[Dict[str, Any]] = []
    for item in selected:
        doc_id = str(item.get("doc_id", ""))
        if not doc_id or doc_id in seen_doc_ids:
            continue
        seen_doc_ids.add(doc_id)
        final_selected.append(item)

    if len(final_selected) < min_docs:
        fill_pool: List[Dict[str, Any]] = []
        fill_pool.extend(
            [
                item
                for item in all_candidates
                if str(item.get("doc_id", "")) not in seen_doc_ids
                and str(item.get("family_key", "misc")) not in repetitive_families
                and _is_fill_eligible(item)
            ]
        )
        fill_pool.extend(
            [
                item
                for item in all_candidates
                if str(item.get("doc_id", "")) not in seen_doc_ids
                and str(item.get("family_key", "misc")) != "buyback_notice"
                and item not in fill_pool
                and _is_fill_eligible(item)
            ]
        )
        fill_pool.extend(
            [
                item
                for item in all_candidates
                if str(item.get("doc_id", "")) not in seen_doc_ids
                and item not in fill_pool
                and _is_fill_eligible(item)
            ]
        )
        for item in fill_pool:
            doc_id = str(item.get("doc_id", ""))
            if not doc_id or doc_id in seen_doc_ids:
                continue
            if _family_count(final_selected, item) >= _family_cap(item):
                continue
            final_selected.append(item)
            seen_doc_ids.add(doc_id)
            if len(final_selected) >= min_docs:
                break

    if len(final_selected) > max_docs:
        final_selected = sorted(
            final_selected,
            key=lambda item: (
                float(item.get("rank_score", 0.0)),
                str(item.get("published_at", "")),
            ),
            reverse=True,
        )[:max_docs]

    fallback_applied = False
    fallback_reason = ""
    if not final_selected and candidates:
        fallback_applied = True
        fallback_reason = "empty_after_selection"
        final_selected = candidates[: min(8, len(candidates))]

    prefiltered_ids = {
        str((row or {}).get("doc_id", "")).strip()
        for row in prefiltered
        if isinstance(row, dict) and str((row or {}).get("doc_id", "")).strip()
    }
    candidate_ids = {
        str(item.get("doc_id", "")).strip()
        for item in candidates
        if str(item.get("doc_id", "")).strip()
    }
    deduped_ids = {
        str(item.get("doc_id", "")).strip()
        for item in deduped
        if str(item.get("doc_id", "")).strip()
    }
    final_ids = {
        str(item.get("doc_id", "")).strip()
        for item in final_selected
        if str(item.get("doc_id", "")).strip()
    }

    dropped_doc_reasons: List[Dict[str, Any]] = []
    for item in all_candidates:
        doc_id = str(item.get("doc_id", "")).strip()
        if not doc_id or doc_id in final_ids:
            continue
        if doc_id not in prefiltered_ids:
            stage = "prefilter"
            reason = "model_marked_not_important"
        elif doc_id in candidate_ids and doc_id not in deduped_ids:
            stage = "dedupe"
            reason = "family_deduplicated"
        elif doc_id in deduped_ids:
            stage = "selection"
            reason = "not_selected_within_target_window"
        else:
            stage = "selection"
            reason = "not_selected"
        dropped_doc_reasons.append(
            {
                "doc_id": doc_id,
                "title": str(item.get("title", "")),
                "published_at": str(item.get("published_at", "")),
                "source_host": str(item.get("host", "")),
                "importance_score": int(item.get("importance_score", 0) or 0),
                "price_sensitive": bool(item.get("price_sensitive", False)),
                "rank_score": float(item.get("rank_score", 0.0) or 0.0),
                "drop_stage": stage,
                "drop_reason": reason,
            }
        )
    dropped_doc_reasons.sort(
        key=lambda row: (
            int(row.get("importance_score", 0) or 0),
            float(row.get("rank_score", 0.0) or 0.0),
        ),
        reverse=True,
    )
    high_importance_dropped = [
        row
        for row in dropped_doc_reasons
        if int(row.get("importance_score", 0) or 0) >= 75
    ]

    compact_docs: List[Dict[str, Any]] = []
    for item in final_selected:
        row = item.get("row", {}) or {}
        source = row.get("source_meta", {}) or {}
        summary = row.get("summary", {}) or {}
        compact_docs.append(
            {
                "doc_id": str(row.get("doc_id", "")),
                "title": str(source.get("title", "")),
                "source_url": str(source.get("source_url", "")),
                "pdf_url": str(source.get("pdf_url", "")),
                "published_at": str(source.get("published_at", "")),
                "price_sensitive": bool((row.get("price_sensitive", {}) or {}).get("is_price_sensitive", False)),
                "price_sensitive_confidence": float((row.get("price_sensitive", {}) or {}).get("confidence", 0.0) or 0.0),
                "price_sensitive_reason": str((row.get("price_sensitive", {}) or {}).get("reason", "")),
                "importance_score": int((row.get("importance", {}) or {}).get("importance_score", 0) or 0),
                "importance_tier": str((row.get("importance", {}) or {}).get("tier", "")),
                "role_tags": list((source.get("role_tags", []) or [])),
                "wrapper_page": bool(source.get("wrapper_page", False)),
                "one_line": str(summary.get("one_line", "")),
                "key_facts_paragraph": str(summary.get("key_facts_paragraph", "")),
                "key_points": list(summary.get("key_points", []) or [])[:30],
                "timeline_milestones": list(summary.get("timeline_milestones", []) or [])[:20],
                "capital_structure": list(summary.get("capital_structure", []) or [])[:10],
                "catalysts_next_12m": list(summary.get("catalysts_next_12m", []) or [])[:10],
                "risks_headwinds": list(summary.get("risks_headwinds", []) or [])[:10],
                "market_impact_assessment": str(summary.get("market_impact_assessment", "")),
                "selection_debug": {
                    "rank_score": float(item.get("rank_score", 0.0)),
                    "coverage_tags": list(item.get("coverage_tags", []) or []),
                    "anchor_tags": list(item.get("anchor_tags", []) or []),
                    "role_tags": list(item.get("role_tags", []) or []),
                    "source_quality_score": int(item.get("source_quality_score", 0) or 0),
                    "corroboration_score": int(item.get("corroboration_score", 0) or 0),
                    "family_key": str(item.get("family_key", "")),
                    "wrapper_page": bool(item.get("wrapper_page", False)),
                },
                "source_meta": {
                    "issuer_validation": dict(item.get("issuer_validation", {}) or {}),
                },
            }
        )

    contamination_gate = await _run_pre_stage1_contamination_gate(
        compact_docs=compact_docs,
        target_ticker=target_ticker,
        target_company=target_company,
        model=contamination_gate_model,
    )
    compact_docs = list(contamination_gate.get("docs", []) or [])
    contamination_report = dict(contamination_gate.get("report", {}) or {})
    if bool(contamination_report.get("hard_fail", False)):
        raise RuntimeError(
            "Pre-Stage-1 contamination gate failed: "
            f"{str(contamination_report.get('reason', 'unknown')).strip()}"
        )

    dropped_unimportant = max(0, len(rows) - len(prefiltered))
    dropped_deduplicated = max(0, len(candidates) - len(deduped))
    dropped_after_selection = max(0, len(deduped) - len(final_selected))
    bundle = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "worker_model": str(payload.get("worker_model", "")),
        "dump_dir": str(out_dir),
        "injection_policy": {
            "min_importance_score": int(INJECTION_MIN_IMPORTANCE_SCORE),
            "include_numeric_facts": False,
            "fallback_applied": bool(fallback_applied),
            "fallback_reason": fallback_reason,
            "ranking_weights": {
                "importance": 0.55,
                "template_coverage": 0.25,
                "source_quality": 0.10,
                "corroboration": 0.10,
            },
            "context_quota": int(context_quota),
            "min_docs": int(min_docs),
            "max_docs": int(max_docs),
            "target_bundle_docs": int(target_bundle_docs),
        },
        "total_processed": int(payload.get("total_processed", len(rows))),
        "kept_for_injection": len(compact_docs),
        "dropped_as_unimportant": int(dropped_unimportant),
        "dropped_deduplicated": int(dropped_deduplicated),
        "dropped_after_selection": int(dropped_after_selection),
        "selection_counts": {
            "rows_total": int(len(rows)),
            "prefiltered_rows": int(len(prefiltered)),
            "candidate_rows": int(len(candidates)),
            "deduped_rows": int(len(deduped)),
            "selected_rows": int(len(final_selected)),
            "kept_rows": int(len(compact_docs)),
        },
        "selection_audit": {
            "high_importance_dropped": high_importance_dropped[:50],
            "dropped_top_ranked": dropped_doc_reasons[:80],
        },
        "pre_stage1_contamination_gate": contamination_report,
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
        f"- dropped_deduplicated: {bundle['dropped_deduplicated']}",
        f"- dropped_after_selection: {bundle['dropped_after_selection']}",
        f"- selection_counts: {json.dumps(bundle['selection_counts'], ensure_ascii=False)}",
        f"- high_importance_dropped_count: {len(bundle['selection_audit']['high_importance_dropped'])}",
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
        "contamination_gate": contamination_report,
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
    official_site = _resolve_official_issuer_site(
        ticker=str(args.ticker or ""),
        exchange_id=exchange_id,
        query=str(args.query or ""),
    )
    official_domain = str(official_site.get("website_domain", "")).strip().lower()
    if not official_domain:
        query_site_domain = _extract_site_domain_from_query(str(args.query or ""))
        if query_site_domain:
            official_domain = query_site_domain
            if not isinstance(official_site, dict):
                official_site = {}
            official_site.setdefault("website_domain", official_domain)
            official_site.setdefault("website_url", f"https://{official_domain}")
    if official_domain:
        if official_domain not in allowed_domain_suffixes:
            allowed_domain_suffixes.append(official_domain)
    lookback_days = (
        int(args.lookback_days)
        if int(args.lookback_days) > 0
        else int(exchange_profile.get("lookback_days_default", 365) or 365)
    )
    retrieval_params = dict(exchange_profile or {})
    target_ps_default = _int_config(retrieval_params.get("target_price_sensitive_default"), 10)
    target_non_ps_default = _int_config(retrieval_params.get("target_non_price_sensitive_default"), 10)
    max_sources_default = int(exchange_profile.get("max_sources_default", 30) or 30)
    requested_max_sources = int(args.max_sources)
    if requested_max_sources <= 0:
        requested_max_sources = max_sources_default
    material_filing_tokens = list(exchange_profile.get("material_filing_tokens", []) or [])
    low_signal_notice_tokens = list(exchange_profile.get("low_signal_notice_tokens", []) or [])
    source_quality_priority = dict(exchange_profile.get("source_quality_priority", {}) or {})
    price_sensitive_strategy = str(exchange_profile.get("price_sensitive_strategy", "none") or "none")

    # Respect exchange profile defaults when explicit bucket targets are not provided.
    target_ps = int(args.target_price_sensitive) if int(args.target_price_sensitive) >= 0 else int(target_ps_default)
    target_non_ps = int(args.target_non_price_sensitive) if int(args.target_non_price_sensitive) >= 0 else int(target_non_ps_default)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max(1, lookback_days))
    extended_filing_cutoff = now - timedelta(days=max(lookback_days, 1095))

    deterministic_canadian_mode = "none"
    deterministic_asx_mode = "none"
    if not bool(args.canadian_official_site_only):
        if (
            exchange_id == "tsxv"
            and any(str(item).strip().lower() == "globenewswire.com" for item in allowed_domain_suffixes)
        ):
            deterministic_canadian_mode = "tsxv_dual_primary"
        elif exchange_id in {"tsx", "cse"}:
            deterministic_canadian_mode = "tsx_cse_multi_adapter"
    canadian_deterministic_primary_active = deterministic_canadian_mode != "none"
    if exchange_id in ASX_EXCHANGE_IDS:
        deterministic_asx_mode = "marketindex_asxpdf"
    asx_deterministic_primary_active = deterministic_asx_mode != "none"

    provider = PerplexityResearchProvider()
    official_focus_brief = ""
    if official_domain:
        official_focus_brief = (
            "Official issuer website focus: prioritize the company website "
            f"({official_domain}) and its investor/news/announcements pages for primary disclosures. "
            "Use this alongside exchange filings and wire releases."
        )
    print(
        "[retrieve] start "
        f"exchange={exchange_id} deterministic_canadian_mode={deterministic_canadian_mode} "
        f"allowed_domains={','.join(allowed_domain_suffixes)} "
        f"max_sources={max(1, int(requested_max_sources))}",
        flush=True,
    )
    async def _run_retrieve_with_progress(label: str) -> Dict[str, Any]:
        return await _await_with_phase_progress(
            provider.gather(
                user_query=str(args.query),
                ticker=str(args.ticker or ""),
                depth=str(args.depth),
                max_sources=max(1, int(requested_max_sources)),
                research_brief=official_focus_brief,
            ),
            label=label,
            interval_seconds=15.0,
        )

    if bool(args.skip_perplexity_retrieval):
        print("[retrieve] skip_perplexity_retrieval=True; native lanes only.", flush=True)
        result: Dict[str, Any] = {"results": []}
        sources: List[Dict[str, Any]] = []
    else:
        result = await _run_retrieve_with_progress("retrieve")
        sources = list(result.get("results", []) or [])
    perplexity_error = str(result.get("error", "")).strip() if isinstance(result, dict) else ""
    perplexity_metadata = dict(result.get("provider_metadata", {}) or {}) if isinstance(result, dict) else {}
    retrieval_lanes: Dict[str, Any] = {
        "perplexity_seed_sources": int(len(sources)),
        "self_scrape_globe_added": 0,
        "self_scrape_official_added": 0,
        "self_scrape_asx_marketindex_added": 0,
        "self_scrape_asx_direct_added": 0,
        "post_merge_source_count": int(len(sources)),
        "perplexity_empty": bool(not sources),
        "perplexity_error": perplexity_error,
        "perplexity_provider_metadata": {
            "request_attempts": int(perplexity_metadata.get("request_attempts", 0) or 0),
            "stream_requested": bool(perplexity_metadata.get("stream_requested", False)),
            "stream_used": bool(perplexity_metadata.get("stream_used", False)),
            "stream_empty_retry_applied": bool(perplexity_metadata.get("stream_empty_retry_applied", False)),
            "transport_retry_applied": str(perplexity_metadata.get("transport_retry_applied", "none") or "none"),
            "timeout_retry_applied": str(perplexity_metadata.get("timeout_retry_applied", "none") or "none"),
            "reasoning_retry_applied": str(perplexity_metadata.get("reasoning_retry_applied", "unchanged") or "unchanged"),
        },
        "issuer_source_filter": {
            "enabled": not bool(args.disable_official_site_filter),
            "attempted": 0,
            "applied": 0,
            "selected_rows": int(len(sources)),
            "dropped_rows": 0,
            "mode": "not_run",
            "error": "",
        },
        "official_site_filter": {
            "enabled": not bool(args.disable_official_site_filter),
            "attempted": 0,
            "applied": 0,
            "selected_rows": 0,
            "mode": "not_run",
            "error": "",
        },
    }
    if not sources:
        print("[retrieve] no Perplexity sources; continuing with native scraper lanes.", flush=True)
        if perplexity_error:
            print(f"[retrieve] Perplexity error detail: {perplexity_error}", flush=True)

    if not official_domain:
        inferred_issuer_domain = _infer_issuer_domain_from_sources(
            sources=sources,
            ticker_symbol=_normalize_ticker_symbol(str(args.ticker or "")),
        )
        if inferred_issuer_domain:
            official_domain = str(inferred_issuer_domain).strip().lower()
            if official_domain and official_domain not in allowed_domain_suffixes:
                allowed_domain_suffixes.append(official_domain)
            official_focus_brief = (
                "Official issuer website focus: prioritize the company website "
                f"({official_domain}) and its investor/news/announcements pages for primary disclosures. "
                "Use this alongside exchange filings and wire releases."
            )
            print(
                "[retrieve] inferred official issuer domain "
                f"domain={official_domain} from_seed_sources=True",
                flush=True,
            )

    # Parallel native scraper lanes for Canadian exchanges:
    # 1) direct Globe keyword scraping (non-Perplexity)
    # 2) issuer official-site disclosure scraping
    # Both are merged with Perplexity seeds.
    if exchange_id in CANADIAN_EXCHANGE_IDS:
        search_terms = _extract_company_term_candidates(
            ticker=str(args.ticker or ""),
            user_query=str(args.query or ""),
            seed_sources=sources,
            official_domain=official_domain,
        )
        guard_terms = _extract_company_guard_terms(
            ticker=str(args.ticker or ""),
            user_query=str(args.query or ""),
            official_domain=official_domain,
        )
        self_scrape_rows: List[Dict[str, Any]] = []
        official_scrape_rows: List[Dict[str, Any]] = []
        if search_terms:
            timeout = httpx.Timeout(35.0, connect=12.0, read=35.0, write=12.0)
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                )
            }
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as scrape_client:
                if not bool(args.canadian_official_site_only):
                    self_scrape_rows = await _await_with_phase_progress(
                        _discover_globenewswire_search_release_sources(
                            scrape_client,
                            terms=search_terms,
                            max_releases=max(40, int(max(target_non_ps, int(args.top))) * 3),
                            max_pages_per_term=4,
                            company_guard_terms=guard_terms,
                        ),
                        label="self-scrape-globe",
                        interval_seconds=10.0,
                    )
                if official_domain:
                    official_target_keep = max(
                        1,
                        int(max(target_non_ps, int(args.top))) * 2,
                        int(args.official_site_min_quota or 0),
                    )
                    official_crawl_cap = max(
                        int(args.official_site_crawl_max_rows or 0),
                        official_target_keep,
                        18,
                    )
                    official_scrape_rows = await _await_with_phase_progress(
                        _discover_official_site_primary_sources(
                            scrape_client,
                            official_domain=official_domain,
                            max_rows=official_crawl_cap,
                        ),
                        label="self-scrape-official",
                        interval_seconds=10.0,
                    )
                    if official_scrape_rows:
                        filtered = await _apply_official_site_mini_filter(
                            rows=official_scrape_rows,
                            ticker=str(args.ticker or ""),
                            exchange_id=exchange_id,
                            official_domain=official_domain,
                            enabled=not bool(args.disable_official_site_filter),
                            model=str(args.official_site_filter_model),
                            timeout_seconds=float(args.official_site_filter_timeout_seconds),
                            max_output_tokens=max(200, int(args.official_site_filter_max_output_tokens)),
                            max_candidates=max(10, int(args.official_site_filter_max_candidates)),
                            max_keep=official_target_keep,
                        )
                        official_scrape_rows = list(filtered.get("rows", []) or [])
                        retrieval_lanes["official_site_filter"] = dict(filtered.get("report", {}) or {})
                    else:
                        retrieval_lanes["official_site_filter"] = {
                            "enabled": not bool(args.disable_official_site_filter),
                            "attempted": 0,
                            "applied": 0,
                            "selected_rows": 0,
                            "mode": "empty_input",
                            "error": "",
                        }

        if self_scrape_rows or official_scrape_rows:
            merged: Dict[str, Dict[str, Any]] = {}
            for row in list(sources) + list(self_scrape_rows) + list(official_scrape_rows):
                row_url = str(row.get("url", "")).strip()
                if not row_url:
                    continue
                existing = merged.get(row_url)
                if not existing:
                    merged[row_url] = row
                    continue
                existing_dt = _parse_iso_date(str(existing.get("published_at", "")).strip()) or _parse_date_from_pdf_url(
                    str(existing.get("url", "")).strip()
                )
                row_dt = _parse_iso_date(str(row.get("published_at", "")).strip()) or _parse_date_from_pdf_url(
                    row_url
                )
                if (row_dt or datetime(1970, 1, 1, tzinfo=timezone.utc)) > (
                    existing_dt or datetime(1970, 1, 1, tzinfo=timezone.utc)
                ):
                    merged[row_url] = row
            before_count = len(sources)
            sources = list(merged.values())
            globe_added = max(0, len([r for r in self_scrape_rows if str(r.get("url", "")).strip()]) )
            official_added = max(0, len([r for r in official_scrape_rows if str(r.get("url", "")).strip()]) )
            retrieval_lanes["self_scrape_globe_added"] = int(globe_added)
            retrieval_lanes["self_scrape_official_added"] = int(official_added)
            retrieval_lanes["post_merge_source_count"] = int(len(sources))
            print(
                "[self-scrape] merged "
                f"total_added={max(0, len(sources) - before_count)} "
                f"globe_candidates={globe_added} "
                f"official_candidates={official_added} "
                f"terms={','.join(search_terms)} "
                f"guards={','.join(guard_terms)}",
                flush=True,
            )

    # Deterministic ASX primary lane:
    # restore the old Market Index -> direct ASX PDF retrieval path and treat it as
    # the primary evidence source for ASX prepass packets.
    if asx_deterministic_primary_active:
        ticker_symbol = _normalize_ticker_symbol(str(args.ticker or ""))
        deterministic_asx_sources: List[Dict[str, Any]] = []
        if ticker_symbol:
            deterministic_asx_rows = await _await_with_phase_progress(
                scrape_marketindex_announcements(
                    ticker_symbol,
                    max_results=max(
                        40,
                        int(requested_max_sources) * 2,
                        int(max(target_ps_default + target_non_ps_default, int(args.top) * 2)),
                    ),
                ),
                label="self-scrape-marketindex",
                interval_seconds=10.0,
            )
            for row in deterministic_asx_rows:
                source_url = str(row.get("url", "")).strip()
                if not source_url:
                    continue
                category = str(row.get("category", "")).strip().lower()
                if category == "ignore":
                    continue
                priority = int(row.get("priority", 3) or 3)
                score = {1: 1.0, 2: 0.85, 3: 0.65}.get(priority, 0.5)
                deterministic_asx_sources.append(
                    {
                        "url": source_url,
                        "title": str(row.get("title", "")).strip() or "ASX announcement",
                        "published_at": str(row.get("published_at", "")).strip(),
                        "content": (
                            "Deterministic ASX Market Index announcement lane. "
                            f"category={category or 'routine'} priority={priority}."
                        ),
                        "score": score,
                        "deterministic_source_kind": "asx_marketindex_pdf",
                        "marketindex_category": category,
                        "marketindex_priority": priority,
                    }
                )
        if not deterministic_asx_sources and ticker_symbol:
            deterministic_asx_sources = await _await_with_phase_progress(
                _discover_direct_asx_primary_sources(
                    symbol=ticker_symbol,
                    lookback_days=lookback_days,
                    max_rows=max(
                        40,
                        int(requested_max_sources),
                        int(max(target_ps_default + target_non_ps_default, int(args.top) * 2)),
                    ),
                ),
                label="self-scrape-asx-direct",
                interval_seconds=10.0,
            )

        if deterministic_asx_sources:
            supplement_sources: List[Dict[str, Any]] = []
            for row in list(sources or []):
                source_url = str(row.get("url", "")).strip()
                if not source_url:
                    continue
                host = urlparse(source_url).netloc.lower().strip()
                if not host:
                    continue
                if official_domain and _host_matches_suffix(host, official_domain):
                    supplement_sources.append(row)

            merged_asx_sources: Dict[str, Dict[str, Any]] = {}
            for row in list(deterministic_asx_sources) + list(supplement_sources):
                row_url = str(row.get("url", "")).strip()
                if not row_url:
                    continue
                existing = merged_asx_sources.get(row_url)
                if not existing:
                    merged_asx_sources[row_url] = row
                    continue
                existing_dt = _parse_iso_date(str(existing.get("published_at", "")).strip()) or _parse_date_from_pdf_url(
                    str(existing.get("url", "")).strip()
                )
                row_dt = _parse_iso_date(str(row.get("published_at", "")).strip()) or _parse_date_from_pdf_url(
                    row_url
                )
                existing_score = float(existing.get("score", 0.0) or 0.0)
                row_score = float(row.get("score", 0.0) or 0.0)
                if (row_dt or datetime(1970, 1, 1, tzinfo=timezone.utc)) > (
                    existing_dt or datetime(1970, 1, 1, tzinfo=timezone.utc)
                ) or row_score > existing_score:
                    merged_asx_sources[row_url] = row

            sources = list(merged_asx_sources.values())
            sources.sort(
                key=lambda row: (
                    _parse_iso_date(str(row.get("published_at", "")).strip())
                    or _parse_date_from_pdf_url(str(row.get("url", "")).strip())
                    or datetime(1970, 1, 1, tzinfo=timezone.utc),
                    float(row.get("score", 0.0) or 0.0),
                ),
                reverse=True,
            )
            retrieval_lanes["self_scrape_asx_marketindex_added"] = int(
                len(
                    [
                        row
                        for row in deterministic_asx_sources
                        if str(row.get("deterministic_source_kind", "")).strip() == "asx_marketindex_pdf"
                    ]
                )
            )
            retrieval_lanes["self_scrape_asx_direct_added"] = int(
                len(
                    [
                        row
                        for row in deterministic_asx_sources
                        if str(row.get("deterministic_source_kind", "")).strip() == "asx_direct_announcement_pdf"
                    ]
                )
            )
            retrieval_lanes["post_merge_source_count"] = int(len(sources))
            print(
                "[asx-deterministic] complete "
                f"mode={deterministic_asx_mode} deterministic_sources={len(deterministic_asx_sources)} "
                f"marketindex={retrieval_lanes['self_scrape_asx_marketindex_added']} "
                f"direct_asx={retrieval_lanes['self_scrape_asx_direct_added']} "
                f"post_merge={len(sources)}",
                flush=True,
            )
            _print_source_preview("asx-deterministic", sources, url_key="url", limit=10)
        else:
            print(
                "[asx-deterministic] no Market Index or direct ASX PDFs found; falling back to existing retrieval sources",
                flush=True,
            )

    # Issuer-identity gate (agent-based): filter merged source pool before deterministic source construction.
    if (exchange_id in CANADIAN_EXCHANGE_IDS or exchange_id in ASX_EXCHANGE_IDS) and sources:
        issuer_filter_result = await _apply_issuer_source_mini_filter(
            rows=sources,
            ticker=str(args.ticker or ""),
            exchange_id=exchange_id,
            official_domain=official_domain,
            user_query=str(args.query or ""),
            enabled=not bool(args.disable_official_site_filter),
            model=str(args.official_site_filter_model),
            timeout_seconds=float(args.official_site_filter_timeout_seconds),
            max_output_tokens=max(300, int(args.official_site_filter_max_output_tokens)),
            max_candidates=max(30, int(args.official_site_filter_max_candidates), len(sources)),
            min_keep=max(10, int(args.top)),
        )
        filtered_sources = list(issuer_filter_result.get("rows", []) or [])
        report = dict(issuer_filter_result.get("report", {}) or {})
        retrieval_lanes["issuer_source_filter"] = report
        if filtered_sources:
            dropped = max(0, len(sources) - len(filtered_sources))
            print(
                "[issuer-source-filter] "
                f"mode={str(report.get('mode', 'n/a'))} "
                f"selected={len(filtered_sources)} dropped={dropped} "
                f"error={str(report.get('error', '')).strip()}",
                flush=True,
            )
            sources = filtered_sources
        else:
            print(
                "[issuer-source-filter] no rows after filter; keeping unfiltered source pool",
                flush=True,
            )

    if (
        canadian_deterministic_primary_active
        and not bool(args.skip_perplexity_retrieval)
        and deterministic_canadian_mode != "tsxv_dual_primary"
    ):
        max_primary_seed_retries = 2
        for retry_idx in range(1, max_primary_seed_retries + 1):
            org_seed_count = sum(
                1
                for row in sources
                if _is_globenewswire_org_search_url(str(row.get("url", "")).strip())
            )
            release_seed_count = sum(
                1
                for row in sources
                if _is_globenewswire_release_url(str(row.get("url", "")).strip())
            )
            if org_seed_count > 0 or release_seed_count > 0:
                break
            print(
                "[retrieve] primary-seed-miss "
                f"retry={retry_idx}/{max_primary_seed_retries} "
                "reason=no_globe_org_or_release_seed",
                flush=True,
            )
            result = await _run_retrieve_with_progress(f"retrieve-retry-{retry_idx}")
            retry_sources = list(result.get("results", []) or [])
            if not retry_sources:
                break
            merged_retry: Dict[str, Dict[str, Any]] = {}
            for row in list(sources) + list(retry_sources):
                row_url = str(row.get("url", "")).strip()
                if not row_url:
                    continue
                existing = merged_retry.get(row_url)
                if not existing:
                    merged_retry[row_url] = row
                    continue
                existing_dt = _parse_iso_date(str(existing.get("published_at", "")).strip()) or _parse_date_from_pdf_url(
                    str(existing.get("url", "")).strip()
                )
                row_dt = _parse_iso_date(str(row.get("published_at", "")).strip()) or _parse_date_from_pdf_url(
                    row_url
                )
                if (row_dt or datetime(1970, 1, 1, tzinfo=timezone.utc)) > (
                    existing_dt or datetime(1970, 1, 1, tzinfo=timezone.utc)
                ):
                    merged_retry[row_url] = row
            sources = list(merged_retry.values())
            if not official_domain:
                inferred_issuer_domain = _infer_issuer_domain_from_sources(
                    sources=sources,
                    ticker_symbol=_normalize_ticker_symbol(str(args.ticker or "")),
                )
                if inferred_issuer_domain:
                    official_domain = str(inferred_issuer_domain).strip().lower()
                    if official_domain and official_domain not in allowed_domain_suffixes:
                        allowed_domain_suffixes.append(official_domain)
                    official_focus_brief = (
                        "Official issuer website focus: prioritize the company website "
                        f"({official_domain}) and its investor/news/announcements pages for primary disclosures. "
                        "Use this alongside exchange filings and wire releases."
                    )
                    print(
                        "[retrieve] inferred official issuer domain "
                        f"domain={official_domain} from_retry_sources=True",
                        flush=True,
                    )
    retrieval_lanes["post_merge_source_count"] = int(len(sources))
    if not sources:
        print("No retrieval sources after Perplexity + native scraper lanes.")
        return 1
    _print_source_preview("retrieve", sources, url_key="url", limit=5)

    # Deterministic Canadian primary lane:
    # - TSXV: strict GlobeNewswire release feed.
    # - TSX/CSE: multi-adapter deterministic set (wire + exchange pages + issuer pages when available).
    if canadian_deterministic_primary_active:
        org_seed_count = sum(
            1 for row in sources if _is_globenewswire_org_search_url(str(row.get("url", "")).strip())
        )
        release_seed_count = sum(
            1 for row in sources if _is_globenewswire_release_url(str(row.get("url", "")).strip())
        )
        print(
            "[canadian-deterministic] start "
            f"mode={deterministic_canadian_mode} "
            f"seed_sources={len(sources)} org_seed_count={org_seed_count} "
            f"release_seed_count={release_seed_count}",
            flush=True,
        )
        timeout = httpx.Timeout(35.0, connect=15.0, read=35.0, write=15.0)
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            )
        }
        deterministic_sources: List[Dict[str, Any]] = []
        approved_wire_hosts = {
            "globenewswire.com",
            "newsfilecorp.com",
            "businesswire.com",
            "newswire.ca",
        }
        exchange_adapter_hosts = {
            "tsx": {"money.tmx.com", "tmx.com", "tsx.com"},
            "cse": {"thecse.com", "www.thecse.com", "cms.thecse.com", "money.tmx.com"},
        }
        ticker_symbol = _normalize_ticker_symbol(str(args.ticker or ""))

        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as org_client:
            deterministic_release_sources = await _await_with_phase_progress(
                _build_strict_canadian_release_sources(
                    org_client,
                    seed_sources=sources,
                    max_releases=50,
                ),
                label="canadian-deterministic",
                interval_seconds=10.0,
            )
            deterministic_sources.extend(deterministic_release_sources)

            official_seed_count = 0
            if official_domain:
                official_seed_count = sum(
                    1
                    for row in list(sources or [])
                    if _host_matches_suffix(
                        urlparse(str(row.get("url", "")).strip()).netloc.lower().strip(),
                        official_domain,
                    )
                )

            needs_search_fallback = (
                deterministic_canadian_mode == "tsxv_dual_primary"
                and len(deterministic_sources) < max(5, target_non_ps, int(args.top))
                and official_seed_count < max(5, target_non_ps, int(args.top))
            ) or (
                deterministic_canadian_mode == "tsx_cse_multi_adapter"
                and len(deterministic_sources) < max(6, target_non_ps, int(args.top))
            )
            if needs_search_fallback:
                term_candidates = _extract_company_term_candidates(
                    ticker=str(args.ticker or ""),
                    user_query=str(args.query or ""),
                    seed_sources=sources,
                    official_domain=official_domain,
                )
                guard_terms = _extract_company_guard_terms(
                    ticker=str(args.ticker or ""),
                    user_query=str(args.query or ""),
                    official_domain=official_domain,
                )
                search_release_sources = await _await_with_phase_progress(
                    _discover_globenewswire_search_release_sources(
                        org_client,
                        terms=term_candidates,
                        max_releases=(
                            max(40, int(max(target_non_ps, int(args.top))) * 2)
                            if deterministic_canadian_mode == "tsxv_dual_primary"
                            else max(20, int(max(target_non_ps, int(args.top))) * 2)
                        ),
                        max_pages_per_term=(4 if deterministic_canadian_mode == "tsxv_dual_primary" else 4),
                        company_guard_terms=guard_terms,
                    ),
                    label="canadian-deterministic-search-fallback",
                    interval_seconds=10.0,
                )
                if search_release_sources:
                    deterministic_sources.extend(search_release_sources)
                    print(
                        "[canadian-deterministic] search fallback "
                        f"terms={','.join(term_candidates)} "
                        f"guards={','.join(guard_terms)} "
                        f"added={len(search_release_sources)}",
                        flush=True,
                    )

        # Add deterministic exchange endpoints for TSX/CSE adapters.
        if deterministic_canadian_mode == "tsx_cse_multi_adapter" and ticker_symbol:
            if exchange_id in {"tsx", "cse"}:
                deterministic_sources.extend(
                    [
                        {
                            "url": f"https://money.tmx.com/en/quote/{ticker_symbol}/news",
                            "title": f"TMX {ticker_symbol} news",
                            "published_at": "",
                            "content": "Deterministic exchange adapter seed.",
                            "score": 0.0,
                        },
                        {
                            "url": f"https://money.tmx.com/en/quote/{ticker_symbol}/filings",
                            "title": f"TMX {ticker_symbol} filings",
                            "published_at": "",
                            "content": "Deterministic exchange adapter seed.",
                            "score": 0.0,
                        },
                        {
                            "url": f"https://www.globenewswire.com/Search?query={quote(ticker_symbol)}",
                            "title": f"GlobeNewswire search {ticker_symbol}",
                            "published_at": "",
                            "content": "Deterministic wire search seed.",
                            "score": 0.0,
                        },
                        {
                            "url": f"https://www.newsfilecorp.com/search/?q={quote(ticker_symbol)}",
                            "title": f"Newsfile search {ticker_symbol}",
                            "published_at": "",
                            "content": "Deterministic wire search seed.",
                            "score": 0.0,
                        },
                    ]
                )

        # Include deterministic adapter-friendly rows from raw retrieval seeds.
        for row in list(sources or []):
            source_url = str(row.get("url", "")).strip()
            if not source_url:
                continue
            title = str(row.get("title", "")).strip() or "Untitled"
            if _is_low_signal_source_page(source_url, title):
                continue
            if not _is_allowed_domain(source_url, allowed_domain_suffixes):
                continue
            host = urlparse(source_url).netloc.lower().strip()
            include = False
            if _is_globenewswire_release_url(source_url):
                include = True
            if official_domain and _host_matches_suffix(host, official_domain):
                include = True
            if any(_host_matches_suffix(host, h) for h in approved_wire_hosts):
                include = True
            if any(
                _host_matches_suffix(host, h)
                for h in exchange_adapter_hosts.get(exchange_id, set())
            ):
                include = True
            if include:
                deterministic_sources.append(
                    {
                        "url": source_url,
                        "title": title,
                        "published_at": str(row.get("published_at", "")).strip(),
                        "content": str(row.get("content", "")).strip(),
                        "score": float(row.get("score", 0.0) or 0.0),
                    }
                )

        # Deduplicate deterministic set by URL, keep newest/highest score.
        deduped_sources: Dict[str, Dict[str, Any]] = {}
        for row in deterministic_sources:
            source_url = str(row.get("url", "")).strip()
            if not source_url:
                continue
            candidate_dt = _parse_iso_date(str(row.get("published_at", "")).strip()) or _parse_date_from_pdf_url(source_url)
            existing = deduped_sources.get(source_url)
            if existing is None:
                deduped_sources[source_url] = row
                continue
            existing_dt = _parse_iso_date(str(existing.get("published_at", "")).strip()) or _parse_date_from_pdf_url(
                str(existing.get("url", "")).strip()
            )
            existing_score = float(existing.get("score", 0.0) or 0.0)
            candidate_score = float(row.get("score", 0.0) or 0.0)
            if (candidate_dt or datetime(1970, 1, 1, tzinfo=timezone.utc)) > (
                existing_dt or datetime(1970, 1, 1, tzinfo=timezone.utc)
            ):
                deduped_sources[source_url] = row
                continue
            if candidate_dt == existing_dt and candidate_score > existing_score:
                deduped_sources[source_url] = row

        if deduped_sources:
            sources = list(deduped_sources.values())
            sources.sort(
                key=lambda row: (
                    _parse_iso_date(str(row.get("published_at", "")).strip())
                    or _parse_date_from_pdf_url(str(row.get("url", "")).strip())
                    or datetime(1970, 1, 1, tzinfo=timezone.utc),
                    float(row.get("score", 0.0) or 0.0),
                ),
                reverse=True,
            )
            print(
                "[canadian-deterministic] complete "
                f"deterministic_sources={len(sources)}",
                flush=True,
            )
            _print_source_preview("canadian-deterministic", sources, url_key="url", limit=10)
        else:
            print(
                "[canadian-deterministic] no deterministic sources built; keeping raw retrieval results",
                flush=True,
            )

    retrieval_lanes["final_source_count"] = int(len(sources))

    candidate_rows: List[Dict[str, Any]] = []
    candidate_backfill_rows: List[Dict[str, Any]] = []
    ticker_symbol = _normalize_ticker_symbol(str(args.ticker or ""))
    for source in sources:
        source_url = str(source.get("url", "")).strip()
        if not source_url:
            continue
        published_at = str(source.get("published_at", "")).strip()
        parsed = _parse_iso_date(published_at)
        title = str(source.get("title", "")).strip() or "Untitled"
        snippet = str(source.get("content", "")).strip()
        if _is_low_signal_source_page(source_url, title):
            continue
        discovery_method = "tier4_existing_perplexity"
        discovery_tier = 4
        if str(source.get("deterministic_source_kind", "")).strip() == "asx_marketindex_pdf":
            discovery_method = "tier1_asx_marketindex_deterministic"
            discovery_tier = 1
        row = _build_candidate_row(
            source_url=source_url,
            title=title,
            published_dt=parsed,
            score=float(source.get("score", 0.0) or 0.0),
            discovery_tier=discovery_tier,
            discovery_method=discovery_method,
            source_snippet=snippet,
        )
        if _is_allowed_domain(source_url, allowed_domain_suffixes):
            candidate_rows.append(row)
            continue
        if _is_canadian_backfill_candidate(
            exchange_id=exchange_id,
            title=title,
            source_url=source_url,
            source_snippet=snippet,
            material_filing_tokens=material_filing_tokens,
            allowed_domain_suffixes=allowed_domain_suffixes,
            ticker_symbol=ticker_symbol,
            source_quality_priority=source_quality_priority,
        ):
            candidate_backfill_rows.append({**row, "discovery_tier": 5, "discovery_method": "tier5_perplexity_backfill"})

    print(
        "[candidate-build] "
        f"allowlisted={len(candidate_rows)} backfill={len(candidate_backfill_rows)} "
        f"from_sources={len(sources)}",
        flush=True,
    )
    _print_source_preview("candidate-build", candidate_rows, url_key="source_url", limit=6)

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
    backfill_added = 0
    backfill_cap_used = 0
    source_page_fallback_used = False
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

        if (
            exchange_id in CANADIAN_EXCHANGE_IDS
            and len(candidate_rows) < 6
            and not canadian_deterministic_primary_active
        ):
            symbol = ticker_symbol
            if symbol:
                existing_sources = {str(row.get("source_url", "")).strip() for row in candidate_rows}
                canadian_seed_urls = [
                    f"https://money.tmx.com/en/quote/{symbol}/news",
                    f"https://money.tmx.com/en/quote/{symbol}/filings",
                ]
                for seed_url in canadian_seed_urls:
                    if seed_url in existing_sources:
                        continue
                    candidate_rows.append(
                        _build_candidate_row(
                            source_url=seed_url,
                            title=f"TMX {symbol} filings/news",
                            published_dt=now,
                            score=0.0,
                            discovery_tier=1,
                            discovery_method="tier1_tmx_quote_pages",
                            source_snippet="Deterministic TMX fallback seed for Canadian exchange filings/news pages.",
                        )
                    )
                    existing_sources.add(seed_url)

        if (
            exchange_id in CANADIAN_EXCHANGE_IDS
            and candidate_backfill_rows
            and not canadian_deterministic_primary_active
        ):
            min_candidate_floor = max(12, int(target_files) * 2)
            if len(candidate_rows) < min_candidate_floor:
                existing_urls = {str(row.get("source_url", "")).strip() for row in candidate_rows}
                candidate_backfill_rows.sort(
                    key=lambda row: (
                        _source_priority_score_for_url(
                            str(row.get("source_url", "")),
                            source_quality_priority=source_quality_priority,
                        ),
                        row.get("published_dt") or datetime(1970, 1, 1, tzinfo=timezone.utc),
                        float(row.get("score", 0.0) or 0.0),
                    ),
                    reverse=True,
                )
                needed = max(0, min_candidate_floor - len(candidate_rows))
                backfill_cap_used = max(4, min(12, int(target_files)))
                allowed_backfill = min(needed, backfill_cap_used)
                for row in candidate_backfill_rows:
                    src = str(row.get("source_url", "")).strip()
                    if not src or src in existing_urls:
                        continue
                    candidate_rows.append(row)
                    existing_urls.add(src)
                    backfill_added += 1
                    if backfill_added >= allowed_backfill:
                        break

        if (
            not candidate_rows
            and candidate_backfill_rows
            and not canadian_deterministic_primary_active
        ):
            candidate_backfill_rows.sort(
                key=lambda row: (
                    _source_priority_score_for_url(
                        str(row.get("source_url", "")),
                        source_quality_priority=source_quality_priority,
                    ),
                    row.get("published_dt") or datetime(1970, 1, 1, tzinfo=timezone.utc),
                    float(row.get("score", 0.0) or 0.0),
                ),
                reverse=True,
            )
            fallback_cap = max(10, int(target_files) * 2)
            candidate_rows = list(candidate_backfill_rows[:fallback_cap])
            print(
                "[candidate-build] fallback_to_backfill "
                f"selected={len(candidate_rows)} from_backfill={len(candidate_backfill_rows)}",
                flush=True,
            )

        if not candidate_rows and canadian_deterministic_primary_active:
            print(
                "[candidate-build] primary_lane_empty_after_retries "
                "strict_canadian_mode=True refusing_backfill_degrade=True",
                flush=True,
            )
            return 1

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
            print(
                f"[pdf-discovery] scanning host={urlparse(str(row.get('source_url', '')).strip()).netloc.lower() or 'no-host'} "
                f"title={str(row.get('title', '')).strip()[:120]}",
                flush=True,
            )
            discovered_entries = await _extract_pdf_entries_from_page(
                client,
                row["source_url"],
                allowed_domain_suffixes,
                allow_source_fallback_decode=bool(exchange_id in CANADIAN_EXCHANGE_IDS),
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
                    is_baseline_exception = _is_baseline_filing_doc(
                        title=title,
                        source_url=str(row.get("source_url", "")),
                        pdf_url=resolved_pdf,
                    )
                    if not (is_baseline_exception and published_dt >= extended_filing_cutoff):
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
                    source_snippet=str(row.get("source_snippet", "")),
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
            source_page_pool: List[Dict[str, Any]] = []
            source_page_seen = set()
            for row in candidate_rows:
                source_url = str(row.get("source_url", "")).strip()
                if not source_url:
                    continue
                if source_url in source_page_seen:
                    continue
                source_page_seen.add(source_url)
                published_dt = row.get("published_dt") if isinstance(row.get("published_dt"), datetime) else now
                title = str(row.get("title", "")).strip() or "Untitled"
                if isinstance(row.get("published_dt"), datetime) and published_dt < cutoff:
                    is_baseline_exception = _is_baseline_filing_doc(
                        title=title,
                        source_url=source_url,
                        pdf_url=source_url,
                    )
                    if not (is_baseline_exception and published_dt >= extended_filing_cutoff):
                        continue
                ps_assessment = _build_price_sensitivity_assessment(
                    exchange_id=exchange_id,
                    title=title,
                    source_title=title,
                    source_snippet=str(row.get("source_snippet", "")),
                    source_url=source_url,
                    pdf_url=source_url,
                    ii_price_sensitive_marker=False,
                    token_marker=_has_material_filing_token(
                        f"{title} {source_url}",
                        material_filing_tokens,
                    ),
                )
                source_page_pool.append(
                    {
                        **row,
                        "exchange": exchange_id,
                        "source_title": title,
                        "source_snippet": str(row.get("source_snippet", "")),
                        "title": title,
                        "pdf_url": source_url,
                        "published_dt": published_dt,
                        "published_at": published_dt.strftime("%Y-%m-%d"),
                        "price_sensitive_marker": bool(ps_assessment.get("is_price_sensitive", False)),
                        "price_sensitive_confidence": round(float(ps_assessment.get("confidence", 0.0) or 0.0), 4),
                        "price_sensitivity": ps_assessment,
                        "ii_price_sensitive_marker": False,
                        "ii_key_points": [],
                        "ii_has_summary_popover": False,
                        "ii_summary_popover_id": "",
                        "published_at_raw": "",
                        "non_ps_priority_boost": 0.0,
                        "non_ps_high_signal": False,
                        "dilution_signal_title": False,
                        "low_signal_admin_title": False,
                        "discovery_source": source_url,
                        "source_page_fallback": True,
                    }
                )
            if source_page_pool:
                pdf_pool = source_page_pool
                source_page_fallback_used = True
                print(
                    "[pdf-pool] no resolvable PDF URLs; using source-page fallback "
                    f"total={len(pdf_pool)}",
                    flush=True,
                )
            else:
                print("No resolvable PDF URLs found from filtered candidates.")
                return 1

        pdf_pool_ps = sum(1 for row in pdf_pool if bool(row.get("price_sensitive_marker", False)))
        print(
            f"[pdf-pool] total={len(pdf_pool)} price_sensitive={pdf_pool_ps} non_price_sensitive={len(pdf_pool) - pdf_pool_ps}",
            flush=True,
        )
        _print_source_preview("pdf-pool", pdf_pool, url_key="pdf_url", limit=8)

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

        if canadian_deterministic_primary_active or asx_deterministic_primary_active:
            selected_primary = _select_latest_entries(
                rows=pdf_pool,
                target=target_files,
                source_quality_priority=source_quality_priority,
            )
        elif target_ps == 0 and target_non_ps == 0:
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

        selected_primary_ps = sum(1 for row in selected_primary if bool(row.get("price_sensitive_marker", False)))
        selected_primary_non_ps = len(selected_primary) - selected_primary_ps
        print(
            "[selection] "
            f"selected_primary={len(selected_primary)} ps={selected_primary_ps} "
            f"non_ps={selected_primary_non_ps} target_ps={target_ps} target_non_ps={target_non_ps} "
            f"deterministic_mode={deterministic_canadian_mode}",
            flush=True,
        )
        _print_source_preview("selection", selected_primary, url_key="pdf_url", limit=8)

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

        document_refs = [
            _row_to_document_ref(idx, row)
            for idx, row in enumerate(decode_queue, start=1)
        ]
        row_by_doc_id = {
            str(ref.get("doc_id", "")): row
            for ref, row in zip(document_refs, decode_queue)
            if str(ref.get("doc_id", "")).strip()
        }
        print(
            f"[parse] parser={str(args.document_parser).strip() or 'smart_default'} "
            f"documents={len(document_refs)}",
            flush=True,
        )
        parsed_documents = await parse_documents(
            document_refs=document_refs,
            parser_id=str(args.document_parser),
            out_dir=out_dir,
            client=client,
        )

        written: List[Dict[str, Any]] = []
        failed: List[Dict[str, Any]] = []
        parsed_documents_for_worker: List[Dict[str, Any]] = []

        for idx, parsed_doc in enumerate(parsed_documents, start=1):
            if len(written) >= target_files:
                break
            doc_id = str(parsed_doc.get("doc_id", "")).strip()
            row = row_by_doc_id.get(doc_id, {})
            title = str(parsed_doc.get("title", "")).strip() or str(row.get("title", "")).strip()
            print(
                f"[parse] {idx}/{len(parsed_documents)} status={str(parsed_doc.get('parse_status', '')).strip() or 'n/a'} "
                f"title={title[:120]}",
                flush=True,
            )
            parse_errors = list(parsed_doc.get("parse_errors", []) or [])
            full_text = str(parsed_doc.get("full_text", "") or "")
            document_type = str(parsed_doc.get("document_type", "") or "")
            min_chars = 180 if document_type == "html" else 120
            if str(parsed_doc.get("parse_status", "")).strip().lower() == "failed" or not full_text:
                failed.append(
                    {
                        "index": idx,
                        "title": title,
                        "source_url": str(parsed_doc.get("source_url", "") or row.get("source_url", "")),
                        "pdf_url": str(parsed_doc.get("pdf_url", "") or row.get("pdf_url", "")),
                        "selection_bucket": row.get("selection_bucket", ""),
                        "price_sensitive_marker": bool(row.get("price_sensitive_marker", False)),
                        "price_sensitive_confidence": float(row.get("price_sensitive_confidence", 0.0) or 0.0),
                        "ii_price_sensitive_marker": bool(row.get("ii_price_sensitive_marker", False)),
                        "price_sensitivity": dict(row.get("price_sensitivity", {}) or {}),
                        "error": "; ".join(parse_errors) if parse_errors else "parse_failed",
                    }
                )
                print(f"[parse] failed error={'; '.join(parse_errors) if parse_errors else 'parse_failed'}", flush=True)
                continue
            if len(full_text) < min_chars:
                failed.append(
                    {
                        "index": idx,
                        "title": title,
                        "source_url": str(parsed_doc.get("source_url", "") or row.get("source_url", "")),
                        "pdf_url": str(parsed_doc.get("pdf_url", "") or row.get("pdf_url", "")),
                        "selection_bucket": row.get("selection_bucket", ""),
                        "price_sensitive_marker": bool(row.get("price_sensitive_marker", False)),
                        "price_sensitive_confidence": float(row.get("price_sensitive_confidence", 0.0) or 0.0),
                        "ii_price_sensitive_marker": bool(row.get("ii_price_sensitive_marker", False)),
                        "price_sensitivity": dict(row.get("price_sensitivity", {}) or {}),
                        "error": f"decoded_text_too_short:{len(full_text)}",
                    }
                )
                print(f"[parse] failed error=decoded_text_too_short:{len(full_text)}", flush=True)
                continue

            published_dt = row.get("published_dt")
            date_part = (
                published_dt.strftime("%Y-%m-%d")
                if isinstance(published_dt, datetime)
                else datetime.now(timezone.utc).strftime("%Y-%m-%d")
            )
            name = _slugify(title, max_len=72)
            file_index = len(written) + 1
            file_name = f"{file_index:02d}_{date_part}_{name}.md"
            file_path = out_dir / file_name
            decoded = {
                "ok": True,
                "text": full_text,
                "page_count": int(parsed_doc.get("page_count", 0) or 0),
                "title": title,
                "content_type": str((parsed_doc.get("trace", {}) or {}).get("content_type", "")),
                "bytes": int((parsed_doc.get("trace", {}) or {}).get("content_bytes", 0) or 0),
                "decode_mode": document_type or "unknown",
            }
            _write_dump_markdown(file_path, row or parsed_doc, decoded)

            parsed_doc["file_name"] = file_name
            parsed_doc["file"] = str(file_path)
            parsed_documents_for_worker.append(parsed_doc)

            written.append(
                {
                    "index": file_index,
                    "file": str(file_path),
                    "title": title,
                    "source_url": str(parsed_doc.get("source_url", "") or row.get("source_url", "")),
                    "pdf_url": str(parsed_doc.get("pdf_url", "") or row.get("pdf_url", "")),
                    "published_at": str(parsed_doc.get("published_at", "") or row.get("published_at", "")),
                    "score": float(row.get("score", 0.0) or 0.0),
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
                    "parse_status": str(parsed_doc.get("parse_status", "")),
                    "parse_method": dict(parsed_doc.get("parse_method", {}) or {}),
                }
            )
            print(f"[parse] wrote file={file_name}", flush=True)

        write_json(
            out_dir / "parsed_documents.json",
            {
                "parser_id": str(args.document_parser),
                "document_count": len(parsed_documents_for_worker),
                "documents": parsed_documents_for_worker,
            },
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
    if canadian_deterministic_primary_active:
        selection_notes.append(
            "Canadian deterministic primary lane active: primary documents selected by latest published date "
            f"(mode={deterministic_canadian_mode})."
        )

    manifest = {
        "query": args.query,
        "ticker": args.ticker,
        "exchange": exchange_id,
        "official_issuer_site": official_site,
        "depth": args.depth,
        "lookback_days": int(lookback_days),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "exchange_retrieval_profile": exchange_profile,
        "deterministic_canadian_mode": deterministic_canadian_mode,
        "us_fallback_chain": us_fallback_chain,
        "price_sensitivity_layer_report": us_ps_model_layer_report,
        "allowed_domains": list(allowed_domain_suffixes),
        "retrieved_sources": len(sources),
        "retrieval_lanes": retrieval_lanes,
        "selection_policy": {
            "target_price_sensitive": int(target_ps),
            "target_non_price_sensitive": int(target_non_ps),
            "fallback_mode_when_zero_targets": int(args.top),
            "notes": selection_notes,
        },
        "target_files": target_files,
        "candidate_sources_considered": len(candidate_rows),
        "candidate_backfill_available": len(candidate_backfill_rows),
        "candidate_backfill_cap": int(backfill_cap_used),
        "candidate_allowlisted_sources": len(
            [row for row in candidate_rows if str(row.get("discovery_method", "")).startswith("tier4")]
        ),
        "candidate_backfill_added": int(backfill_added),
        "candidate_pdfs_in_window": len(pdf_pool),
        "source_page_fallback_used": bool(source_page_fallback_used),
        "selected_primary_candidates": len(selected_primary),
        "document_parser": str(args.document_parser),
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
            "stage_timeout_seconds": float(args.worker_stage_timeout_seconds),
            "hybrid_vision_enabled": bool(args.worker_enable_vision),
            "hybrid_vision_model": str(args.worker_vision_model),
            "hybrid_vision_max_pages": int(args.worker_vision_max_pages),
            "hybrid_vision_page_batch_size": int(args.worker_vision_page_batch_size),
            "hybrid_vision_max_page_facts": int(args.worker_vision_max_page_facts),
            "hybrid_vision_timeout_seconds": float(args.worker_vision_timeout_seconds),
            "hybrid_vision_max_tokens": int(args.worker_vision_max_tokens),
            "complex_reasoning_model": str(args.worker_complex_reasoning_model),
            "complex_reasoning_min_doc_chars": int(args.worker_complex_reasoning_min_doc_chars),
            "complex_reasoning_min_importance_score": int(
                args.worker_complex_reasoning_min_importance_score
            ),
        },
    }

    worker_summary_markdown = ""
    worker_summary_json = ""
    injection_bundle_json = ""
    injection_bundle_markdown = ""
    contamination_gate_info: Dict[str, Any] = {}
    worker_stage_log = ""
    if not bool(args.skip_worker_summaries) and written:
        worker_cmd = [
            "uv",
            "run",
            "python",
            "-u",
            "test_pdf_dump_worker_summaries.py",
            "--dump-dir",
            str(out_dir),
            "--parsed-documents-json",
            str(out_dir / "parsed_documents.json"),
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
        if str(args.worker_complex_reasoning_model or "").strip():
            worker_cmd.extend(
                [
                    "--complex-reasoning-model",
                    str(args.worker_complex_reasoning_model),
                    "--complex-reasoning-min-doc-chars",
                    str(max(10000, int(args.worker_complex_reasoning_min_doc_chars))),
                    "--complex-reasoning-min-importance-score",
                    str(
                        max(
                            0,
                            min(100, int(args.worker_complex_reasoning_min_importance_score)),
                        )
                    ),
                ]
            )
        if bool(args.worker_enable_vision):
            worker_cmd.append("--worker-enable-vision")
        else:
            worker_cmd.append("--worker-disable-vision")
        print(f"Worker summaries: running {' '.join(worker_cmd)}")
        worker_stage_timeout_seconds = max(0.0, float(args.worker_stage_timeout_seconds))
        worker_log_path = out_dir / "worker_stage.log"
        worker_stage_log = str(worker_log_path)
        worker_returncode = 1
        worker_timed_out = False
        with worker_log_path.open("w", encoding="utf-8") as worker_log:
            worker_log.write(f"command: {' '.join(worker_cmd)}\n")
            worker_log.flush()
            start_ts = time.monotonic()
            proc = subprocess.Popen(
                worker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            assert proc.stdout is not None
            while True:
                try:
                    ready, _, _ = select.select([proc.stdout], [], [], 0.2)
                except Exception:
                    ready = []
                if ready:
                    line = proc.stdout.readline()
                    if line:
                        print(line.rstrip())
                        worker_log.write(line)
                        worker_log.flush()
                        continue
                if proc.poll() is not None:
                    while True:
                        tail_line = proc.stdout.readline()
                        if not tail_line:
                            break
                        print(tail_line.rstrip())
                        worker_log.write(tail_line)
                    worker_log.flush()
                    worker_returncode = int(proc.returncode or 0)
                    break
                if worker_stage_timeout_seconds > 0 and (
                    time.monotonic() - start_ts
                ) > worker_stage_timeout_seconds:
                    worker_timed_out = True
                    proc.kill()
                    worker_returncode = 124
                    print(
                        "Worker summaries timeout: "
                        f"exceeded {worker_stage_timeout_seconds:.0f}s "
                        f"(see {worker_log_path})"
                    )
                    worker_log.write(
                        f"\nworker_stage_timeout_seconds exceeded: {worker_stage_timeout_seconds}\n"
                    )
                    worker_log.flush()
                    break
                time.sleep(0.2)
        if worker_returncode != 0:
            print("Worker summaries failed:")
            if worker_timed_out:
                print(f"- reason: timeout (see {worker_log_path})")
            else:
                print(f"- reason: non-zero return code {worker_returncode} (see {worker_log_path})")
        else:
            worker_summary_markdown = str(out_dir / "announcement_summaries.md")
            worker_summary_json = str(out_dir / "announcement_summaries.json")
            try:
                bundle_info = await _build_injection_bundle_from_worker_summary(
                    out_dir=out_dir,
                    worker_summary_json=Path(worker_summary_json),
                    exchange_profile=exchange_profile,
                    contamination_gate_model=str(args.official_site_filter_model or ""),
                    target_ticker=str(args.ticker or ""),
                    target_company=str(manifest.get("company_name", "") or ""),
                )
                injection_bundle_json = str(bundle_info.get("bundle_json", ""))
                injection_bundle_markdown = str(bundle_info.get("bundle_markdown", ""))
                print(
                    "Injection bundle: "
                    f"kept={bundle_info.get('kept_for_injection', 0)} "
                    f"json={injection_bundle_json}"
                )
                contamination_gate_info = dict(bundle_info.get("contamination_gate", {}) or {})
                if contamination_gate_info:
                    print(
                        "Pre-Stage-1 contamination gate: "
                        f"status={contamination_gate_info.get('status', 'n/a')} "
                        f"dropped={len(contamination_gate_info.get('deterministic_dropped_doc_ids', []) or []) + len(contamination_gate_info.get('model_dropped_doc_ids', []) or [])} "
                        f"related_party={len(contamination_gate_info.get('deterministic_related_party_doc_ids', []) or []) + len(contamination_gate_info.get('model_related_party_doc_ids', []) or [])}"
                    )
            except Exception as exc:
                print(f"Injection bundle generation failed: {type(exc).__name__}: {exc}")

    manifest["worker_summary_markdown"] = worker_summary_markdown
    manifest["worker_summary_json"] = worker_summary_json
    manifest["injection_bundle_json"] = injection_bundle_json
    manifest["injection_bundle_markdown"] = injection_bundle_markdown
    manifest["pre_stage1_contamination_gate"] = contamination_gate_info
    manifest["worker_stage_log"] = worker_stage_log
    manifest["document_refs_json"] = str(out_dir / "document_refs.json")
    manifest["parsed_documents_json"] = str(out_dir / "parsed_documents.json")

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    index_lines = [
        "# PDF Dump Index",
        "",
        f"- query: {args.query}",
        f"- ticker: {args.ticker}",
        f"- exchange: {exchange_id}",
        f"- official_issuer_site: {official_site.get('website_url', '')}",
        f"- depth: {args.depth}",
        f"- lookback_days: {lookback_days}",
        f"- retrieved_sources: {len(sources)}",
        f"- retrieval_lane_perplexity_seed_sources: {int(retrieval_lanes.get('perplexity_seed_sources', 0) or 0)}",
        f"- retrieval_lane_self_scrape_globe_candidates: {int(retrieval_lanes.get('self_scrape_globe_added', 0) or 0)}",
        f"- retrieval_lane_self_scrape_official_candidates: {int(retrieval_lanes.get('self_scrape_official_added', 0) or 0)}",
        f"- retrieval_lane_post_merge_sources: {int(retrieval_lanes.get('post_merge_source_count', 0) or 0)}",
        f"- retrieval_lane_final_sources: {int(retrieval_lanes.get('final_source_count', len(sources)) or 0)}",
        f"- retrieval_lane_perplexity_empty: {bool(retrieval_lanes.get('perplexity_empty', False))}",
        f"- issuer_source_filter_mode: {str((retrieval_lanes.get('issuer_source_filter', {}) or {}).get('mode', 'n/a'))}",
        f"- issuer_source_filter_selected_rows: {int((retrieval_lanes.get('issuer_source_filter', {}) or {}).get('selected_rows', 0) or 0)}",
        f"- issuer_source_filter_dropped_rows: {int((retrieval_lanes.get('issuer_source_filter', {}) or {}).get('dropped_rows', 0) or 0)}",
        f"- issuer_source_filter_error: {str((retrieval_lanes.get('issuer_source_filter', {}) or {}).get('error', '')).strip()}",
        f"- official_site_filter_mode: {str((retrieval_lanes.get('official_site_filter', {}) or {}).get('mode', 'n/a'))}",
        f"- official_site_filter_selected_rows: {int((retrieval_lanes.get('official_site_filter', {}) or {}).get('selected_rows', 0) or 0)}",
        f"- official_site_filter_error: {str((retrieval_lanes.get('official_site_filter', {}) or {}).get('error', '')).strip()}",
        f"- us_fallback_enabled: {bool(us_fallback_chain.get('enabled', False))}",
        f"- us_fallback_tier_used: {us_fallback_chain.get('tier_used', '')}",
        f"- us_ps_model_layer_enabled: {bool(us_ps_model_layer_report.get('enabled', False))}",
        f"- us_ps_model_layer_attempted: {int(us_ps_model_layer_report.get('attempted', 0) or 0)}",
        f"- us_ps_model_layer_updated: {int(us_ps_model_layer_report.get('updated', 0) or 0)}",
        f"- target_price_sensitive: {target_ps}",
        f"- target_non_price_sensitive: {target_non_ps}",
        f"- candidate_sources_considered: {len(candidate_rows)}",
        f"- candidate_backfill_available: {len(candidate_backfill_rows)}",
        f"- candidate_backfill_cap: {int(backfill_cap_used)}",
        f"- candidate_allowlisted_sources: {len([row for row in candidate_rows if str(row.get('discovery_method', '')).startswith('tier4')])}",
        f"- candidate_backfill_added: {int(backfill_added)}",
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
    if official_site.get("website_url"):
        print(
            "Official issuer site focus: "
            f"{official_site.get('website_url')} (source={official_site.get('source')})"
        )
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
    issuer_filter_report = retrieval_lanes.get("issuer_source_filter", {}) or {}
    if issuer_filter_report:
        print(
            "Issuer source filter: "
            f"mode={issuer_filter_report.get('mode', 'n/a')} "
            f"selected={int(issuer_filter_report.get('selected_rows', 0) or 0)} "
            f"dropped={int(issuer_filter_report.get('dropped_rows', 0) or 0)} "
            f"error={str(issuer_filter_report.get('error', '')).strip()}",
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
