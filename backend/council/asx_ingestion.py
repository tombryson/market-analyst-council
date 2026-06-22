"""ASX deterministic source ingestion.

Fetches, parses, and caches ASX announcement listings; decodes PDF URLs;
and merges ASX sources into the Stage-1 source-row set.
"""

import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx

from ..config import (
    ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED,
    ASX_DETERMINISTIC_FETCH_TIMEOUT_SECONDS,
    ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL,
    ASX_DETERMINISTIC_LOOKBACK_YEARS,
    ASX_DETERMINISTIC_MAX_DECODE,
    ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY,
    ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS,
)
from .source_analysis import _excerpt_material_signal_score
from .stage1_attempt import _progress_log
import copy
from urllib.parse import parse_qs
from .fact_digest import _ASX_ANNOUNCEMENT_SEARCH_URL, _ASX_DETERMINISTIC_CACHE

logger = logging.getLogger(__name__)

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

    from ..research.providers.perplexity import PerplexityResearchProvider

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

