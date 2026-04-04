#!/usr/bin/env python3
"""
Worker summarizer for PDF dump announcements.

Reads markdown files produced by test_perplexity_pdf_dump.py, applies a rubric
with a lightweight model, and emits structured JSON summaries.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx

from dotenv import load_dotenv

from backend.config import OPENROUTER_API_KEY, OPENROUTER_API_URL
from backend.openrouter import query_model

try:
    import pymupdf  # type: ignore

    PYMUPDF_AVAILABLE = True
    PYMUPDF_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover
    PYMUPDF_AVAILABLE = False
    PYMUPDF_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

DEFAULT_WORKER_MODEL = "openai/gpt-5-mini"
OUTPUT_MIN_IMPORTANCE_SCORE = 80
OUTPUT_INCLUDE_NUMERIC_FACTS = False
WORKER_MAX_OUTPUT_TOKENS_CAP = 7200
VISION_FORCE_ON_TITLE_TERMS = (
    "presentation",
    "investor presentation",
    "results presentation",
    "conference presentation",
    "webinar presentation",
    "corporate presentation",
    "deck",
    "fact sheet",
    "factsheet",
    "roadshow",
)
VISION_FORCE_OFF_TITLE_TERMS = (
    "annual report",
    "half yearly report",
    "half-year report",
    "half year report",
    "interim report",
    "financial report",
    "quarterly",
    "quarterly activities",
    "quarterly report",
    "cash flow report",
    "appendix 4c",
    "appendix 4d",
    "appendix 4e",
    "appendix 5b",
    "appendix 5c",
    "notice of meeting",
    "cleansing notice",
    "application for quotation",
    "quotation of securities",
    "director's interest",
    "governance",
    "tenement table",
    "teleconference",
    "prospectus",
)
VISION_FORCE_OFF_PREFIXES = (
    "approvals and reporting",
)
WRAPPER_TITLE_EXACT_TERMS = {
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
}
WRAPPER_URL_HINTS = (
    "/announcements",
    "/announcement-centre",
    "/announcement-center",
    "/investor-centre",
    "/investor-center",
    "/reporting",
    "/reports",
    "/newsroom",
    "/press-releases",
)
_GENERIC_ISSUER_TERMS = {
    "limited",
    "ltd",
    "inc",
    "corp",
    "corporation",
    "company",
    "co",
    "group",
    "holdings",
    "holding",
    "resources",
    "resource",
    "minerals",
    "mining",
    "energy",
    "metals",
    "gold",
    "silver",
    "uranium",
    "copper",
    "lithium",
    "nickel",
    "lead",
    "zinc",
    "iron",
    "ore",
    "projects",
    "project",
}
_SUPPORTING_CONTEXT_TERMS = (
    "counterparty",
    "related party",
    "related-party",
    "joint venture",
    "joint-venture",
    "jv",
    "processing agreement",
    "toll treatment",
    "scheme booklet",
    "scheme implementation",
    "bidder's statement",
    "target's statement",
    "farm-in",
    "farmout",
    "option agreement",
    "sale and purchase agreement",
    "acquisition agreement",
)


def _normalize_ticker_symbol(raw: str) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        return ""
    text = text.split(":", 1)[-1].strip()
    match = re.search(r"\b([A-Z]{2,6})\b", text)
    return str(match.group(1)).strip() if match else text


def _distinctive_issuer_terms(name: str) -> List[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9&.-]{2,}", str(name or "").lower())
    out: List[str] = []
    for token in tokens:
        norm = re.sub(r"[^a-z0-9]+", "", token)
        if len(norm) < 4 or norm in _GENERIC_ISSUER_TERMS:
            continue
        out.append(norm)
    return list(dict.fromkeys(out))


def _extract_exchange_ticker_mentions(text: str) -> List[str]:
    blob = str(text or "").upper()
    mentions = re.findall(r"\b(?:ASX|TSXV|TSX|NYSE|NASDAQ|LSE|CSE|OTCQX|OTC)[:\s]+([A-Z]{2,6})\b", blob)
    return list(dict.fromkeys(str(item).strip().upper() for item in mentions if str(item).strip()))


def _detect_issuer_alignment(doc: Dict[str, Any]) -> Dict[str, Any]:
    document_ref = dict(doc.get("document_ref", {}) or {})
    issuer_signals = dict(doc.get("issuer_signals", {}) or {})
    ticker_mentions_raw = issuer_signals.get("ticker_mentions", [])
    ticker_mentions = ticker_mentions_raw if isinstance(ticker_mentions_raw, list) else []
    fallback_ticker = ""
    for item in ticker_mentions:
        candidate = str(item or "").strip()
        if candidate:
            fallback_ticker = candidate
            break
    expected_symbol = _normalize_ticker_symbol(
        str(document_ref.get("ticker_hint") or fallback_ticker or "")
    )
    expected_name = str(document_ref.get("issuer_hint", "") or "").strip()
    expected_terms = _distinctive_issuer_terms(expected_name)

    title = str(doc.get("title", "")).strip()
    full_text = str(doc.get("full_text", "") or "")
    header_lines = [line.strip() for line in full_text.splitlines()[:40] if str(line).strip()]
    header_blob = "\n".join(([title] if title else []) + header_lines[:20])
    search_blob = f"{header_blob}\n{full_text[:8000]}".lower()

    observed_symbols = _extract_exchange_ticker_mentions(header_blob)
    expected_term_hits = [term for term in expected_terms if term in search_blob]
    support_context = any(term in search_blob for term in _SUPPORTING_CONTEXT_TERMS)

    other_symbol_mentions = [sym for sym in observed_symbols if expected_symbol and sym != expected_symbol]
    if expected_symbol and expected_symbol in observed_symbols and other_symbol_mentions and support_context:
        return {
            "status": "related_party",
            "expected_symbol": expected_symbol,
            "observed_symbols": observed_symbols,
            "expected_terms": expected_terms,
            "matched_terms": expected_term_hits,
            "support_context": support_context,
            "reason": f"supporting_context_foreign_ticker={','.join(other_symbol_mentions[:3])}",
        }
    if expected_symbol and observed_symbols and expected_symbol not in observed_symbols and other_symbol_mentions:
        status = "related_party" if support_context and expected_term_hits else "mismatch"
        return {
            "status": status,
            "expected_symbol": expected_symbol,
            "observed_symbols": observed_symbols,
            "expected_terms": expected_terms,
            "matched_terms": expected_term_hits,
            "support_context": support_context,
            "reason": f"observed_foreign_ticker={','.join(other_symbol_mentions[:3])}",
        }

    if expected_terms:
        capitalized_terms = re.findall(r"\b[A-Z][A-Za-z0-9&.-]{2,}\b", header_blob)
        observed_name_terms = [
            re.sub(r"[^a-z0-9]+", "", token.lower())
            for token in capitalized_terms
            if len(re.sub(r"[^a-z0-9]+", "", token.lower())) >= 4
        ]
        observed_name_terms = [
            token for token in observed_name_terms if token and token not in _GENERIC_ISSUER_TERMS
        ]
        observed_name_terms = list(dict.fromkeys(observed_name_terms))
        foreign_name_terms = [term for term in observed_name_terms if term not in expected_terms]
        if foreign_name_terms and not expected_term_hits:
            status = "related_party" if support_context else "mismatch"
            return {
                "status": status,
                "expected_symbol": expected_symbol,
                "observed_symbols": observed_symbols,
                "expected_terms": expected_terms,
                "matched_terms": expected_term_hits,
                "support_context": support_context,
                "reason": f"observed_foreign_issuer_terms={','.join(foreign_name_terms[:4])}",
            }

    if expected_term_hits or (expected_symbol and expected_symbol in observed_symbols):
        return {
            "status": "match",
            "expected_symbol": expected_symbol,
            "observed_symbols": observed_symbols,
            "expected_terms": expected_terms,
            "matched_terms": expected_term_hits,
            "support_context": support_context,
            "reason": "target_issuer_detected",
        }

    return {
        "status": "unclear",
        "expected_symbol": expected_symbol,
        "observed_symbols": observed_symbols,
        "expected_terms": expected_terms,
        "matched_terms": expected_term_hits,
        "support_context": support_context,
        "reason": "no_clear_issuer_signal",
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize pdf_dump announcements with rubric")
    parser.add_argument(
        "--dump-dir",
        required=True,
        help="Directory containing pdf dump markdown files",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_WORKER_MODEL,
        help=f"Worker model (default: {DEFAULT_WORKER_MODEL})",
    )
    parser.add_argument(
        "--max-key-points",
        type=int,
        default=30,
        help="Maximum key points per kept document",
    )
    parser.add_argument(
        "--max-doc-chars",
        type=int,
        default=120000,
        help="Maximum decoded-text chars sent to model per doc",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=180.0,
        help="OpenRouter request timeout per document",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help="Parallel worker calls",
    )
    parser.add_argument(
        "--max-docs",
        type=int,
        default=0,
        help="Optional cap for number of docs to process (0=all)",
    )
    parser.add_argument(
        "--output-markdown",
        default="",
        help="Optional output markdown path (default: <dump-dir>/announcement_summaries.md)",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional output json path (default: <dump-dir>/announcement_summaries.json)",
    )
    parser.add_argument(
        "--parsed-documents-json",
        default="",
        help="Optional parsed documents JSON path (default: <dump-dir>/parsed_documents.json)",
    )
    parser.add_argument(
        "--worker-enable-vision",
        action="store_true",
        help="Enable hybrid vision extraction before text summarization (default: enabled)",
    )
    parser.add_argument(
        "--worker-disable-vision",
        dest="worker_enable_vision",
        action="store_false",
        help="Disable hybrid vision extraction and run text-only worker",
    )
    parser.add_argument(
        "--vision-model",
        default=DEFAULT_WORKER_MODEL,
        help=f"Vision extraction model (default: {DEFAULT_WORKER_MODEL})",
    )
    parser.add_argument(
        "--vision-max-pages",
        type=int,
        default=50,
        help="Max visual pages per document (0 = all pages, default=50 soft cap)",
    )
    parser.add_argument(
        "--vision-page-batch-size",
        type=int,
        default=4,
        help="Number of PDF pages to send per vision call",
    )
    parser.add_argument(
        "--vision-max-page-facts",
        type=int,
        default=12,
        help="Max key facts extracted per page in vision stage",
    )
    parser.add_argument(
        "--vision-zoom",
        type=float,
        default=1.8,
        help="PDF render zoom for page images",
    )
    parser.add_argument(
        "--vision-timeout-seconds",
        type=float,
        default=180.0,
        help="Timeout for each vision extraction call",
    )
    parser.add_argument(
        "--vision-max-tokens",
        type=int,
        default=1200,
        help="Completion token cap for each vision extraction call",
    )
    parser.add_argument(
        "--complex-reasoning-model",
        default="",
        help=(
            "Optional escalation model for complex docs (e.g., openai/gpt-5.4). "
            "If empty, no escalation is performed."
        ),
    )
    parser.add_argument(
        "--complex-reasoning-min-doc-chars",
        type=int,
        default=90000,
        help="Escalate when decoded text chars >= this threshold",
    )
    parser.add_argument(
        "--complex-reasoning-min-importance-score",
        type=int,
        default=90,
        help="Escalate when initial importance score >= this threshold",
    )
    parser.set_defaults(worker_enable_vision=True)
    return parser.parse_args()


def _extract_fenced_json(text: str) -> Optional[Dict[str, Any]]:
    payload = str(text or "").strip()
    if not payload:
        return None
    obj = _try_load_json_obj(payload)
    if isinstance(obj, dict):
        return obj

    fence = re.search(r"```json\s*(\{.*?\})\s*```", payload, flags=re.DOTALL | re.IGNORECASE)
    if fence:
        obj = _try_load_json_obj(fence.group(1))
        if isinstance(obj, dict):
            return obj

    candidate = _extract_balanced_json_object(payload)
    if candidate:
        obj = _try_load_json_obj(candidate)
        if isinstance(obj, dict):
            return obj
    return None


def _try_load_json_obj(payload: str) -> Optional[Dict[str, Any]]:
    raw = str(payload or "").strip()
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def _extract_balanced_json_object(payload: str) -> Optional[str]:
    text = str(payload or "")
    start = text.find("{")
    if start < 0:
        return None

    depth = 0
    in_str = False
    esc = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
            continue
    return None


def _adaptive_worker_max_tokens(decoded_chars: int) -> int:
    n = max(0, int(decoded_chars))
    if n >= 140_000:
        return 6200
    if n >= 100_000:
        return 5600
    if n >= 70_000:
        return 5000
    if n >= 45_000:
        return 4400
    if n >= 25_000:
        return 3800
    if n >= 12_000:
        return 3400
    return 3000


def _sample_page_indices(total_pages: int, max_samples: int = 10) -> List[int]:
    total = max(0, int(total_pages))
    sample_cap = max(1, int(max_samples))
    if total <= sample_cap:
        return list(range(total))
    if sample_cap == 1:
        return [0]
    chosen = {
        int(round(i * (total - 1) / (sample_cap - 1)))
        for i in range(sample_cap)
    }
    return sorted(chosen)


def _compute_text_layout_metrics(text: str) -> Dict[str, Any]:
    nonempty_lines: List[str] = []
    bullet_like = 0
    short_lines = 0
    total_len = 0
    for raw_line in str(text or "").splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line:
            continue
        nonempty_lines.append(line)
        total_len += len(line)
        if len(line) <= 80:
            short_lines += 1
        if re.match(r"^(?:[-*•▪◦–]|\d+[\.\)])\s+", line):
            bullet_like += 1

    line_count = len(nonempty_lines)
    avg_line_len = (total_len / line_count) if line_count else 0.0
    short_line_ratio = (short_lines / line_count) if line_count else 0.0
    bullet_like_ratio = (bullet_like / line_count) if line_count else 0.0
    return {
        "line_count": int(line_count),
        "avg_line_len": round(float(avg_line_len), 2),
        "short_line_ratio": round(float(short_line_ratio), 4),
        "bullet_like_ratio": round(float(bullet_like_ratio), 4),
    }


async def _fetch_pdf_bytes(pdf_url: str) -> Tuple[Optional[bytes], str]:
    url = str(pdf_url or "").strip()
    if not url:
        return None, "missing_pdf_url"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )
    }
    try:
        timeout = httpx.Timeout(60.0, connect=30.0, read=60.0, write=30.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return bytes(resp.content), ""
    except Exception as exc:
        return None, f"pdf_download_failed:{type(exc).__name__}:{exc}"


def _probe_pdf_structure(pdf_bytes: bytes, *, max_sample_pages: int = 10) -> Dict[str, Any]:
    if not PYMUPDF_AVAILABLE:
        return {
            "status": "failed",
            "reason": f"pymupdf_unavailable:{PYMUPDF_IMPORT_ERROR}",
        }

    try:
        pdf_doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    except Exception as exc:
        return {
            "status": "failed",
            "reason": f"pdf_open_failed:{type(exc).__name__}:{exc}",
        }

    try:
        total_pages = int(pdf_doc.page_count or 0)
        sampled_indices = _sample_page_indices(total_pages, max_samples=max_sample_pages)
        text_chars: List[int] = []
        image_counts: List[int] = []
        low_text_pages = 0
        short_lines = 0
        total_lines = 0
        total_line_len = 0
        for page_index in sampled_indices:
            page = pdf_doc.load_page(page_index)
            text = str(page.get_text("text") or "")
            text_len = len(text.strip())
            text_chars.append(text_len)
            if text_len < 350:
                low_text_pages += 1
            images = page.get_images(full=False) or []
            image_counts.append(len(images))
            for raw_line in text.splitlines():
                line = re.sub(r"\s+", " ", raw_line).strip()
                if not line:
                    continue
                total_lines += 1
                total_line_len += len(line)
                if len(line) <= 80:
                    short_lines += 1

        sampled_pages = len(sampled_indices)
        avg_text_chars = (sum(text_chars) / sampled_pages) if sampled_pages else 0.0
        avg_images = (sum(image_counts) / sampled_pages) if sampled_pages else 0.0
        short_ratio = (short_lines / total_lines) if total_lines else 0.0
        low_text_ratio = (low_text_pages / sampled_pages) if sampled_pages else 0.0
        return {
            "status": "ok",
            "reason": "",
            "total_pages": int(total_pages),
            "sampled_pages": int(sampled_pages),
            "avg_sample_text_chars": round(float(avg_text_chars), 2),
            "low_text_page_ratio": round(float(low_text_ratio), 4),
            "avg_image_count_per_page": round(float(avg_images), 4),
            "short_line_ratio": round(float(short_ratio), 4),
            "text_extraction_sparse": bool(avg_text_chars < 300),
        }
    finally:
        try:
            pdf_doc.close()
        except Exception:
            pass


async def _decide_vision_policy(
    *,
    doc: Dict[str, Any],
    vision_enabled: bool,
) -> Tuple[Dict[str, Any], Optional[bytes]]:
    title = str(doc.get("title", "") or "").strip()
    title_lower = title.lower()
    decoded_chars = int(doc.get("decoded_chars", 0) or 0)
    full_text = str(doc.get("full_text", "") or "")
    text_metrics = _compute_text_layout_metrics(full_text)
    policy: Dict[str, Any] = {
        "use_vision": False,
        "policy": "disabled_by_flag",
        "reason": "worker_disable_vision",
        "heuristic_score": 0,
        "decision_reasons": ["worker_disable_vision"],
        "probe": {
            "decoded_chars": int(decoded_chars),
            "title": title,
            "line_count": int(text_metrics.get("line_count", 0) or 0),
            "avg_line_len": float(text_metrics.get("avg_line_len", 0.0) or 0.0),
            "short_line_ratio": float(text_metrics.get("short_line_ratio", 0.0) or 0.0),
            "bullet_like_ratio": float(text_metrics.get("bullet_like_ratio", 0.0) or 0.0),
        },
    }
    if not bool(vision_enabled):
        return policy, None

    if any(term in title_lower for term in VISION_FORCE_ON_TITLE_TERMS):
        policy.update(
            {
                "use_vision": True,
                "policy": "force_on_title_match",
                "reason": "presentation_title_match",
                "heuristic_score": 8,
                "decision_reasons": ["title_matches_presentation_rule"],
            }
        )
        return policy, None

    if any(title_lower.startswith(prefix) for prefix in VISION_FORCE_OFF_PREFIXES):
        policy.update(
            {
                "policy": "force_off_title_prefix",
                "reason": "text_filing_title_prefix_match",
                "decision_reasons": ["title_matches_text_filing_prefix_rule"],
            }
        )
        return policy, None

    if any(term in title_lower for term in VISION_FORCE_OFF_TITLE_TERMS):
        policy.update(
            {
                "policy": "force_off_title_match",
                "reason": "text_filing_title_match",
                "decision_reasons": ["title_matches_text_filing_rule"],
            }
        )
        return policy, None

    avg_line_len = float(text_metrics.get("avg_line_len", 0.0) or 0.0)
    short_line_ratio = float(text_metrics.get("short_line_ratio", 0.0) or 0.0)
    bullet_like_ratio = float(text_metrics.get("bullet_like_ratio", 0.0) or 0.0)
    if decoded_chars >= 80000 and avg_line_len >= 72 and short_line_ratio <= 0.72:
        policy.update(
            {
                "policy": "text_dense_short_circuit",
                "reason": "decoded_text_is_dense",
                "decision_reasons": ["high_decoded_chars", "long_average_line_length"],
            }
        )
        return policy, None

    if decoded_chars <= 18000 and avg_line_len <= 48 and short_line_ratio >= 0.82:
        policy.update(
            {
                "use_vision": True,
                "policy": "slide_like_text_layout",
                "reason": "short_line_layout_detected",
                "heuristic_score": 5,
                "decision_reasons": ["low_decoded_chars", "high_short_line_ratio", "short_average_line_length"],
            }
        )
        return policy, None

    def _apply_text_only_fallback(reason_prefix: str) -> Tuple[Dict[str, Any], Optional[bytes]]:
        score = 0
        reasons: List[str] = []
        if decoded_chars <= 22000:
            score += 1
            reasons.append("lower_decoded_chars")
        if avg_line_len <= 48:
            score += 1
            reasons.append("short_average_line_length")
        if short_line_ratio >= 0.82:
            score += 2
            reasons.append("high_short_line_ratio")
        elif short_line_ratio >= 0.72:
            score += 1
            reasons.append("moderate_short_line_ratio")
        if bullet_like_ratio >= 0.15:
            score += 1
            reasons.append("bullet_like_layout")
        if decoded_chars >= 50000 and avg_line_len >= 68:
            score -= 2
            reasons.append("dense_text_body")
        use_vision = score >= 4
        policy.update(
            {
                "use_vision": bool(use_vision),
                "policy": "text_layout_fallback",
                "reason": (
                    f"{reason_prefix}_supports_vision"
                    if use_vision
                    else f"{reason_prefix}_supports_text_only"
                ),
                "heuristic_score": int(score),
                "decision_reasons": reasons[:8] or ["default_text_only"],
            }
        )
        return policy, None

    if not PYMUPDF_AVAILABLE:
        policy["probe"]["pdf_probe_error"] = f"pymupdf_unavailable:{PYMUPDF_IMPORT_ERROR}"
        return _apply_text_only_fallback("text_layout_only")

    pdf_url = str(doc.get("pdf_url", "") or "").strip()
    pdf_bytes, fetch_error = await _fetch_pdf_bytes(pdf_url)
    if not pdf_bytes:
        if fetch_error:
            policy["probe"]["pdf_fetch_error"] = fetch_error
        return _apply_text_only_fallback("pdf_probe_unavailable")

    probe = _probe_pdf_structure(pdf_bytes, max_sample_pages=10)
    policy["probe"].update(probe)
    if str(probe.get("status", "")) != "ok":
        policy.update(
            {
                "policy": "pdf_probe_failed",
                "reason": str(probe.get("reason", "") or "pdf_probe_failed"),
                "decision_reasons": ["pdf_probe_failed", "default_text_only"],
            }
        )
        return policy, None

    score = 0
    reasons: List[str] = []
    avg_sample_text_chars = float(probe.get("avg_sample_text_chars", 0.0) or 0.0)
    low_text_page_ratio = float(probe.get("low_text_page_ratio", 0.0) or 0.0)
    avg_image_count_per_page = float(probe.get("avg_image_count_per_page", 0.0) or 0.0)
    probe_short_line_ratio = float(probe.get("short_line_ratio", short_line_ratio) or 0.0)
    total_pages = int(probe.get("total_pages", 0) or 0)

    if avg_sample_text_chars < 600:
        score += 3
        reasons.append("low_text_per_sampled_page")
    elif avg_sample_text_chars < 1200:
        score += 2
        reasons.append("moderate_text_per_sampled_page")
    if low_text_page_ratio >= 0.40:
        score += 2
        reasons.append("many_low_text_pages")
    if avg_image_count_per_page >= 1.0:
        score += 2
        reasons.append("image_heavy_pages")
    elif avg_image_count_per_page >= 0.35:
        score += 1
        reasons.append("some_image_density")
    if probe_short_line_ratio >= 0.82:
        score += 2
        reasons.append("short_line_layout")
    elif probe_short_line_ratio >= 0.72:
        score += 1
        reasons.append("semi_slide_layout")
    if bool(probe.get("text_extraction_sparse", False)):
        score += 2
        reasons.append("sparse_text_extraction")
    if total_pages >= 8 and avg_sample_text_chars < 1600:
        score += 1
        reasons.append("multi_page_low_text_doc")
    if decoded_chars >= 90000 and avg_sample_text_chars > 1800:
        score -= 2
        reasons.append("very_text_dense_doc")
    if avg_line_len >= 78 and short_line_ratio <= 0.68:
        score -= 1
        reasons.append("long_line_text_layout")

    use_vision = score >= 4
    policy.update(
        {
            "use_vision": bool(use_vision),
            "policy": "pdf_structure_heuristic",
            "reason": "pdf_structure_supports_vision" if use_vision else "pdf_structure_supports_text_only",
            "heuristic_score": int(score),
            "decision_reasons": reasons[:8] or ["default_text_only"],
        }
    )
    return policy, (pdf_bytes if use_vision else None)


async def _query_openrouter_multimodal(
    *,
    model: str,
    content: List[Dict[str, Any]],
    timeout_seconds: float,
    max_tokens: int,
) -> Optional[str]:
    if not OPENROUTER_API_KEY:
        return None
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "model": str(model),
        "messages": [
            {
                "role": "user",
                "content": content,
            }
        ],
        "max_tokens": int(max_tokens),
    }
    timeout = httpx.Timeout(float(timeout_seconds), connect=30.0, read=float(timeout_seconds), write=30.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(OPENROUTER_API_URL, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        return f"vision_http_error:{type(exc).__name__}:{exc}"

    choices = data.get("choices") or []
    if not choices:
        return ""
    message = (choices[0] or {}).get("message") or {}
    raw = message.get("content")
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: List[str] = []
        for item in raw:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
        return "\n".join(parts).strip()
    if raw is None:
        return ""
    return str(raw)


def _build_vision_batch_prompt(
    *,
    title: str,
    doc_id: str,
    page_numbers: List[int],
    max_page_facts: int,
) -> str:
    return (
        "You are extracting investment-relevant facts from PDF slide/page images.\n"
        "Return STRICT JSON only.\n\n"
        "Schema:\n"
        "{\n"
        '  "doc_id": "",\n'
        '  "page_batch": [1,2],\n'
        '  "pages": [\n'
        "    {\n"
        '      "page_number": 1,\n'
        '      "is_investment_relevant": true,\n'
        '      "key_facts": ["..."],\n'
        '      "numeric_facts": [{"metric":"","value":"","unit":"","context":"","confidence":0.0}],\n'
        '      "timeline_facts": [{"milestone":"","target_window":"","direction":"new|reconfirmed|delayed|accelerated|unclear","confidence":0.0}],\n'
        '      "capital_structure_facts": ["..."],\n'
        '      "risks_or_caveats": ["..."],\n'
        '      "confidence": 0.0\n'
        "    }\n"
        "  ],\n"
        '  "notes": ["..."]\n'
        "}\n\n"
        "Rules:\n"
        f"- Document title: {title}\n"
        f"- Document id: {doc_id}\n"
        f"- This batch includes pages: {page_numbers}\n"
        f"- Max key_facts per page: {max(1, int(max_page_facts))}\n"
        "- Focus on valuation, production, capex/opex, NPV/IRR/AISC, financing, dilution, milestones, catalysts, and risks.\n"
        "- Ignore decorative text.\n"
        "- If uncertain, lower confidence and note uncertainty.\n"
        "- Do not output markdown, prose, or code fences."
    )


def _render_page_to_data_url(
    *,
    doc: Any,
    page_index: int,
    zoom: float,
) -> str:
    page = doc.load_page(page_index)
    mat = pymupdf.Matrix(float(zoom), float(zoom))
    pix = page.get_pixmap(matrix=mat, alpha=False)
    raw = pix.tobytes("png")
    b64 = base64.b64encode(raw).decode("ascii")
    return f"data:image/png;base64,{b64}"


def _normalize_vision_page_obj(page_obj: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "page_number": int(page_obj.get("page_number", 0) or 0),
        "is_investment_relevant": bool(page_obj.get("is_investment_relevant", False)),
        "key_facts": [],
        "numeric_facts": [],
        "timeline_facts": [],
        "capital_structure_facts": [],
        "risks_or_caveats": [],
        "confidence": 0.0,
    }
    key_facts = page_obj.get("key_facts", [])
    if isinstance(key_facts, list):
        out["key_facts"] = [str(x).strip() for x in key_facts if str(x).strip()]
    numeric = page_obj.get("numeric_facts", [])
    if isinstance(numeric, list):
        clean_numeric: List[Dict[str, Any]] = []
        for row in numeric:
            if not isinstance(row, dict):
                continue
            clean_numeric.append(
                {
                    "metric": str(row.get("metric", "")).strip(),
                    "value": str(row.get("value", "")).strip(),
                    "unit": str(row.get("unit", "")).strip(),
                    "context": str(row.get("context", "")).strip(),
                    "confidence": float(row.get("confidence", 0.0) or 0.0),
                }
            )
        out["numeric_facts"] = clean_numeric
    timeline = page_obj.get("timeline_facts", [])
    if isinstance(timeline, list):
        clean_timeline: List[Dict[str, Any]] = []
        valid = {"new", "reconfirmed", "delayed", "accelerated", "unclear"}
        for row in timeline:
            if not isinstance(row, dict):
                continue
            direction = str(row.get("direction", "unclear")).strip().lower()
            if direction not in valid:
                direction = "unclear"
            clean_timeline.append(
                {
                    "milestone": str(row.get("milestone", "")).strip(),
                    "target_window": str(row.get("target_window", "")).strip(),
                    "direction": direction,
                    "confidence": float(row.get("confidence", 0.0) or 0.0),
                }
            )
        out["timeline_facts"] = clean_timeline
    cap_facts = page_obj.get("capital_structure_facts", [])
    if isinstance(cap_facts, list):
        out["capital_structure_facts"] = [str(x).strip() for x in cap_facts if str(x).strip()]
    risks = page_obj.get("risks_or_caveats", [])
    if isinstance(risks, list):
        out["risks_or_caveats"] = [str(x).strip() for x in risks if str(x).strip()]
    out["confidence"] = max(0.0, min(1.0, float(page_obj.get("confidence", 0.0) or 0.0)))
    return out


async def _extract_vision_bundle(
    *,
    doc: Dict[str, Any],
    model: str,
    max_pages: int,
    batch_size: int,
    max_page_facts: int,
    zoom: float,
    timeout_seconds: float,
    max_tokens: int,
    pdf_bytes: Optional[bytes] = None,
) -> Dict[str, Any]:
    if not PYMUPDF_AVAILABLE:
        return {
            "enabled": False,
            "status": "skipped",
            "reason": f"pymupdf_unavailable:{PYMUPDF_IMPORT_ERROR}",
            "total_pages": 0,
            "pages_processed": 0,
            "page_cap": int(max_pages),
            "relevant_pages": 0,
            "aggregated": {},
        }
    pdf_url = str(doc.get("pdf_url", "")).strip()
    if not pdf_url:
        return {
            "enabled": False,
            "status": "skipped",
            "reason": "missing_pdf_url",
            "total_pages": 0,
            "pages_processed": 0,
            "page_cap": int(max_pages),
            "relevant_pages": 0,
            "aggregated": {},
        }

    if pdf_bytes is None:
        fetched_pdf_bytes, fetch_error = await _fetch_pdf_bytes(pdf_url)
        if not fetched_pdf_bytes:
            return {
                "enabled": True,
                "status": "failed",
                "reason": fetch_error or "pdf_download_failed",
                "total_pages": 0,
                "pages_processed": 0,
                "page_cap": int(max_pages),
                "relevant_pages": 0,
                "aggregated": {},
            }
        pdf_bytes = fetched_pdf_bytes

    try:
        pdf_doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    except Exception as exc:
        return {
            "enabled": True,
            "status": "failed",
            "reason": f"pdf_open_failed:{type(exc).__name__}:{exc}",
            "total_pages": 0,
            "pages_processed": 0,
            "page_cap": int(max_pages),
            "relevant_pages": 0,
            "aggregated": {},
        }

    try:
        total_pages = int(pdf_doc.page_count)
        if total_pages <= 0:
            return {
                "enabled": True,
                "status": "failed",
                "reason": "zero_pages",
                "total_pages": 0,
                "pages_processed": 0,
                "page_cap": int(max_pages),
                "relevant_pages": 0,
                "aggregated": {},
            }
        page_cap = int(max_pages)
        if page_cap <= 0:
            selected_pages = list(range(1, total_pages + 1))
        else:
            selected_pages = list(range(1, min(total_pages, page_cap) + 1))

        batch_n = max(1, int(batch_size))
        pages_out: List[Dict[str, Any]] = []
        notes: List[str] = []
        for start in range(0, len(selected_pages), batch_n):
            batch_pages = selected_pages[start : start + batch_n]
            content: List[Dict[str, Any]] = [
                {
                    "type": "text",
                    "text": _build_vision_batch_prompt(
                        title=str(doc.get("title", "")),
                        doc_id=str(doc.get("file_name", "")),
                        page_numbers=batch_pages,
                        max_page_facts=max_page_facts,
                    ),
                }
            ]
            for page_number in batch_pages:
                data_url = _render_page_to_data_url(
                    doc=pdf_doc,
                    page_index=int(page_number) - 1,
                    zoom=float(zoom),
                )
                content.append({"type": "image_url", "image_url": {"url": data_url}})

            resp_text = await _query_openrouter_multimodal(
                model=str(model),
                content=content,
                timeout_seconds=float(timeout_seconds),
                max_tokens=max(400, int(max_tokens)),
            )
            if str(resp_text or "").startswith("vision_http_error:"):
                notes.append(str(resp_text)[:220])
                continue
            resp_obj = _extract_fenced_json(str(resp_text or ""))
            if not isinstance(resp_obj, dict):
                notes.append(f"batch_{batch_pages[0]}_{batch_pages[-1]}:json_parse_failed")
                continue

            rows = resp_obj.get("pages", [])
            if not isinstance(rows, list):
                notes.append(f"batch_{batch_pages[0]}_{batch_pages[-1]}:missing_pages_array")
                continue
            for row in rows:
                if isinstance(row, dict):
                    page_norm = _normalize_vision_page_obj(row)
                    if page_norm.get("page_number", 0) <= 0:
                        continue
                    pages_out.append(page_norm)

        by_page: Dict[int, Dict[str, Any]] = {}
        for row in pages_out:
            page_number = int(row.get("page_number", 0) or 0)
            if page_number <= 0:
                continue
            if page_number not in by_page:
                by_page[page_number] = row
                continue
            # Merge duplicates by extending unique facts.
            cur = by_page[page_number]
            for key in ("key_facts", "capital_structure_facts", "risks_or_caveats"):
                combined = list(cur.get(key, []) or [])
                for item in row.get(key, []) or []:
                    if item not in combined:
                        combined.append(item)
                cur[key] = combined
            for key in ("numeric_facts", "timeline_facts"):
                combined = list(cur.get(key, []) or [])
                for item in row.get(key, []) or []:
                    if item not in combined:
                        combined.append(item)
                cur[key] = combined
            cur["is_investment_relevant"] = bool(cur.get("is_investment_relevant", False)) or bool(
                row.get("is_investment_relevant", False)
            )
            cur["confidence"] = max(
                float(cur.get("confidence", 0.0) or 0.0),
                float(row.get("confidence", 0.0) or 0.0),
            )
            by_page[page_number] = cur

        page_rows = [by_page[p] for p in sorted(by_page.keys())]
        key_facts: List[str] = []
        numeric_facts: List[Dict[str, Any]] = []
        timeline_facts: List[Dict[str, Any]] = []
        capital_facts: List[str] = []
        risk_facts: List[str] = []
        relevant_pages = 0
        for row in page_rows:
            if bool(row.get("is_investment_relevant", False)):
                relevant_pages += 1
            for item in row.get("key_facts", []) or []:
                if item not in key_facts:
                    key_facts.append(item)
            for item in row.get("numeric_facts", []) or []:
                if item not in numeric_facts:
                    numeric_facts.append(item)
            for item in row.get("timeline_facts", []) or []:
                if item not in timeline_facts:
                    timeline_facts.append(item)
            for item in row.get("capital_structure_facts", []) or []:
                if item not in capital_facts:
                    capital_facts.append(item)
            for item in row.get("risks_or_caveats", []) or []:
                if item not in risk_facts:
                    risk_facts.append(item)

        return {
            "enabled": True,
            "status": "ok",
            "reason": "",
            "total_pages": int(total_pages),
            "pages_processed": len(selected_pages),
            "page_cap": int(max_pages),
            "page_cap_applied": bool(int(max_pages) > 0 and int(total_pages) > int(max_pages)),
            "relevant_pages": int(relevant_pages),
            "notes": notes,
            "aggregated": {
                "key_facts": key_facts[:120],
                "numeric_facts": numeric_facts[:80],
                "timeline_facts": timeline_facts[:80],
                "capital_structure_facts": capital_facts[:60],
                "risks_or_caveats": risk_facts[:60],
            },
        }
    finally:
        try:
            pdf_doc.close()
        except Exception:
            pass


def _read_dump_markdown(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    lines = raw.splitlines()

    title = ""
    source_url = ""
    pdf_url = ""
    domain = ""
    published_at = ""
    page_count = 0
    decoded_chars = 0

    for line in lines[:80]:
        stripped = line.strip()
        if stripped.startswith("# PDF Dump:"):
            title = stripped.replace("# PDF Dump:", "", 1).strip()
        elif stripped.startswith("- source_url:"):
            source_url = stripped.replace("- source_url:", "", 1).strip()
        elif stripped.startswith("- pdf_url:"):
            pdf_url = stripped.replace("- pdf_url:", "", 1).strip()
        elif stripped.startswith("- domain:"):
            domain = stripped.replace("- domain:", "", 1).strip()
        elif stripped.startswith("- published_at:"):
            published_at = stripped.replace("- published_at:", "", 1).strip()
        elif stripped.startswith("- page_count:"):
            try:
                page_count = int(stripped.replace("- page_count:", "", 1).strip())
            except Exception:
                page_count = 0
        elif stripped.startswith("- decoded_chars:"):
            try:
                decoded_chars = int(stripped.replace("- decoded_chars:", "", 1).strip())
            except Exception:
                decoded_chars = 0

    marker = "## Full Decoded Text"
    full_text = raw
    if marker in raw:
        after = raw.split(marker, 1)[1]
        full_text = after.strip()

    return {
        "file": str(path),
        "file_name": path.name,
        "title": title or path.stem,
        "source_url": source_url,
        "pdf_url": pdf_url,
        "domain": domain,
        "published_at": published_at,
        "page_count": page_count,
        "decoded_chars": decoded_chars,
        "full_text": full_text.strip(),
    }


def _read_parsed_documents(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = list(payload.get("documents", []) or [])
    docs: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        file_name = str(row.get("file_name", "")).strip() or f"doc_{idx:03d}.md"
        full_text = str(row.get("full_text", "") or row.get("raw_text", "") or "").strip()
        docs.append(
            {
                "file": str(row.get("file", "")).strip(),
                "file_name": file_name,
                "title": str(row.get("title", "")).strip() or Path(file_name).stem,
                "source_url": str(row.get("source_url", "")).strip(),
                "pdf_url": str(row.get("pdf_url", "")).strip(),
                "domain": str(row.get("domain", "")).strip(),
                "published_at": str(row.get("published_at", "")).strip(),
                "page_count": int(row.get("page_count", 0) or 0),
                "decoded_chars": int(row.get("decoded_chars", 0) or len(full_text)),
                "full_text": full_text,
                "parse_status": str(row.get("parse_status", "")).strip(),
                "parse_method": dict(row.get("parse_method", {}) or {}),
                "parse_quality": dict(row.get("parse_quality", {}) or {}),
                "visual_fact_pack": dict(row.get("visual_fact_pack", {}) or {}),
                "issuer_signals": dict(row.get("issuer_signals", {}) or {}),
                "document_ref": dict(row.get("document_ref", {}) or {}),
            }
        )
    return docs


def _tier_from_score(score: int) -> str:
    s = int(max(0, min(100, score)))
    if s >= 85:
        return "critical"
    if s >= 70:
        return "high"
    if s >= 50:
        return "medium"
    if s >= 30:
        return "low"
    return "ignore"


def _compute_relevance_lanes(title: str, text: str) -> Dict[str, Any]:
    hay = f"{title}\n{text}".lower()
    lane_terms: Dict[str, List[str]] = {
        "financing_capital": [
            "financing", "project financing", "bought deal", "private placement", "credit facility",
            "debt facility", "loan", "refinancing", "royalty", "stream", "warrant", "proceeds",
            "dilution", "equity raise", "liquidity", "cash runway", "working capital",
        ],
        "growth_pipeline": [
            "drill", "drilling", "assay", "intersects", "resource estimate", "reserve estimate",
            "pipeline", "trial", "phase 1", "phase 2", "phase 3", "enrollment", "enrolment",
            "launch", "rollout", "new contract", "backlog", "customer win", "infill", "step-out",
        ],
        "operations_execution": [
            "production", "throughput", "commissioning", "construction", "ramp-up", "ramp up",
            "cash flow", "cashflow", "revenue", "gross margin", "ebitda", "capex", "opex",
            "cost", "guidance", "npv", "irr", "aisc", "utilization", "utilisation",
        ],
        "timeline_milestones": [
            "milestone", "timeline", "target", "by q", "by h1", "by h2", "on track", "delayed",
            "accelerated", "expected in", "expected by", "planned in", "schedule",
        ],
        "regulatory_legal": [
            "permit", "permitting", "approval", "authorization", "authorisation", "licence",
            "license", "fda", "ema", "regulatory", "litigation", "consent", "compliance",
            "federal", "environmental",
        ],
        "market_demand_pricing": [
            "pricing", "price", "tariff", "demand", "supply", "inventory", "opec", "brent",
            "wti", "henry hub", "commodity", "fx", "exchange rate", "spot",
        ],
        "management_governance": [
            "board", "director", "ceo", "cfo", "executive", "management", "appointment",
            "resignation", "leadership", "governance",
        ],
    }
    lane_regexes: Dict[str, List[str]] = {
        "growth_pipeline": [
            r"\b\d+(?:\.\d+)?\s*g\/t\b",
            r"\b\d+(?:\.\d+)?\s*m(?:etres?|eters?)\b",
            r"\bintersect(?:s|ed)?\b",
        ],
        "financing_capital": [
            r"\b(?:c\$|a\$|us\$)\s*\d",
            r"\b(?:million|billion)\b",
        ],
        "timeline_milestones": [
            r"\bq[1-4]\b",
            r"\b20\d{2}\b",
            r"\bwithin\s+\d+\s+(?:weeks?|months?)\b",
        ],
        "operations_execution": [
            r"\b\d+(?:\.\d+)?\s*(?:koz|moz|boe\/d|oz)\b",
            r"\b\d+(?:\.\d+)?\s*%\b",
        ],
    }

    scores: Dict[str, int] = {}
    hits_out: Dict[str, List[str]] = {}
    signal_flags: Dict[str, bool] = {}
    for lane, terms in lane_terms.items():
        hits = [term for term in terms if term in hay]
        regex_hits = 0
        for pattern in lane_regexes.get(lane, []):
            if re.search(pattern, hay):
                regex_hits += 1
        boost = 10 if len(hits) >= 3 else 0
        score = min(100, (len(hits) * 12) + (regex_hits * 16) + boost)
        scores[lane] = int(score)
        hits_out[lane] = hits[:12]
        signal_flags[f"{lane}_regex"] = bool(regex_hits > 0)

    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    primary_lane = ordered[0][0] if ordered else "operations_execution"
    primary_score = ordered[0][1] if ordered else 0
    second_score = ordered[1][1] if len(ordered) > 1 else 0
    return {
        "scores": scores,
        "hits": hits_out,
        "primary_lane": primary_lane,
        "primary_score": int(primary_score),
        "second_score": int(second_score),
        "signals": signal_flags,
    }


def _compute_noise_profile(title: str, text: str) -> Dict[str, Any]:
    hay = f"{title}\n{text}".lower()
    buckets: Dict[str, List[str]] = {
        "procedural_admin": [
            "notice of meeting",
            "change in director",
            "director's interest",
            "appendix 3x",
            "appendix 3y",
            "appendix 3z",
            "change in substantial holding",
            "ceasing to be a substantial holder",
            "top 20 holders",
            "distribution schedule",
            "annual general meeting results",
            "agm results",
            "appendix 2a",
            "appendix 3b",
            "appendix 3c",
        ],
        "listing_housekeeping": [
            "application for quotation",
            "quotation of securities",
            "quotation notice",
            "cleansing notice",
            "cleansing statement",
            "issuance of securities",
            "listing rule",
            "appendix 2a",
            "appendix 3g",
        ],
        "duplicate_or_broadcast_copy": [
            "forward-looking statements",
            "safe harbor statement",
            "for immediate release",
            "view source version",
            "distributed by",
            "news release",
        ],
    }
    regexes: Dict[str, List[str]] = {
        "procedural_admin": [
            r"\bappendix\s+[23][a-z0-9]+\b",
            r"\bform\s+604\b",
            r"\bform\s+605\b",
        ],
        "listing_housekeeping": [
            r"\bapplication\s+for\s+quotation\b",
            r"\bnotification\s+regarding\s+unquoted\s+securities\b",
        ],
    }
    hits_by_bucket: Dict[str, List[str]] = {}
    bucket_scores: Dict[str, int] = {}
    regex_total = 0
    for bucket, terms in buckets.items():
        hits = [term for term in terms if term in hay]
        rx_hits = 0
        for pattern in regexes.get(bucket, []):
            if re.search(pattern, hay):
                rx_hits += 1
        hits_by_bucket[bucket] = hits[:12]
        regex_total += rx_hits
        bucket_scores[bucket] = int(min(100, (len(hits) * 12) + (rx_hits * 16)))
    total_hits = sum(len(v) for v in hits_by_bucket.values())
    bucket_count = sum(1 for v in hits_by_bucket.values() if v)
    noise_score = min(100, (total_hits * 8) + (bucket_count * 10) + (regex_total * 8))
    return {
        "noise_score": int(max(0, min(100, noise_score))),
        "bucket_scores": bucket_scores,
        "hits": hits_by_bucket,
        "bucket_count": int(bucket_count),
    }


def _is_wrapper_or_index_page(title: str, text: str, source_url: str = "") -> bool:
    title_norm = re.sub(r"\s+", " ", str(title or "").strip().lower())
    if title_norm in WRAPPER_TITLE_EXACT_TERMS:
        return True
    low = f"{title}\n{text}".lower()
    if (
        title_norm
        and len(title_norm) <= 40
        and any(token in title_norm for token in ("announcements", "investor centre", "investor center", "reporting"))
        and "quarterly" not in low
        and "annual report" not in low
        and "half year" not in low
        and "half-year" not in low
        and "presentation" not in low
    ):
        return True
    path = urlparse(str(source_url or "")).path.lower()
    if path and any(hint in path for hint in WRAPPER_URL_HINTS) and title_norm in {
        "updates",
        "latest updates",
        "announcements",
        "news",
        "investor updates",
        "reports",
    }:
        return True
    return False


def _derive_document_role_tags(
    title: str,
    text: str,
    lane_meta: Dict[str, Any],
    *,
    wrapper_page: bool = False,
) -> List[str]:
    hay = f"{title}\n{text}".lower()
    scores = dict(lane_meta.get("scores", {}) or {})
    roles: set[str] = set()

    if wrapper_page:
        roles.add("wrapper_or_index_page")

    if any(
        token in hay
        for token in (
            "annual report",
            "annual results",
            "half yearly",
            "half-year",
            "half year",
            "interim report",
            "interim results",
            "quarterly",
            "cash flow report",
            "cashflow report",
            "10-k",
            "10-q",
            "20-f",
            "6-k",
            "appendix 4d",
            "appendix 4e",
            "appendix 5b",
        )
    ):
        roles.add("periodic_report")

    if any(
        token in hay
        for token in (
            "presentation",
            "deck",
            "webinar",
            "investor day",
            "fact sheet",
            "factsheet",
            "roadshow",
        )
    ):
        roles.add("presentation_or_deck")

    if int(scores.get("financing_capital", 0)) >= 50:
        roles.add("financing_or_capital")
    if int(scores.get("growth_pipeline", 0)) >= 50:
        roles.add("technical_or_product_update")
    if int(scores.get("operations_execution", 0)) >= 50:
        roles.add("operational_update")
    if int(scores.get("regulatory_legal", 0)) >= 50:
        roles.add("regulatory_or_approval")
    if int(scores.get("management_governance", 0)) >= 50:
        roles.add("management_or_governance")
    if int(scores.get("market_demand_pricing", 0)) >= 50:
        roles.add("market_or_macro_context")
    if int(scores.get("timeline_milestones", 0)) >= 50:
        roles.add("timeline_or_milestone")

    if not roles:
        primary_lane = str(lane_meta.get("primary_lane", "")).strip().lower()
        lane_to_role = {
            "financing_capital": "financing_or_capital",
            "growth_pipeline": "technical_or_product_update",
            "operations_execution": "operational_update",
            "timeline_milestones": "timeline_or_milestone",
            "regulatory_legal": "regulatory_or_approval",
            "market_demand_pricing": "market_or_macro_context",
            "management_governance": "management_or_governance",
        }
        fallback_role = lane_to_role.get(primary_lane)
        if fallback_role:
            roles.add(fallback_role)

    return sorted(roles)


def _auto_importance_floor_from_lanes(lane_meta: Dict[str, Any]) -> int:
    scores = dict(lane_meta.get("scores", {}) or {})
    floor = 0
    if int(scores.get("financing_capital", 0)) >= 70:
        floor = max(floor, 78)
    elif int(scores.get("financing_capital", 0)) >= 55:
        floor = max(floor, 64)

    if int(scores.get("regulatory_legal", 0)) >= 65:
        floor = max(floor, 72)

    if int(scores.get("growth_pipeline", 0)) >= 72:
        floor = max(floor, 68)
    elif int(scores.get("growth_pipeline", 0)) >= 56:
        floor = max(floor, 58)

    if int(scores.get("operations_execution", 0)) >= 70:
        floor = max(floor, 67)

    if (
        int(scores.get("timeline_milestones", 0)) >= 58
        and int(scores.get("operations_execution", 0)) >= 48
    ):
        floor = max(floor, 62)

    strong_lanes = sum(1 for v in scores.values() if int(v) >= 60)
    if strong_lanes >= 2:
        floor = max(floor, 70)
    return int(max(0, min(95, floor)))


def _auto_price_sensitivity_from_lanes(lane_meta: Dict[str, Any]) -> bool:
    scores = dict(lane_meta.get("scores", {}) or {})
    if int(scores.get("financing_capital", 0)) >= 70:
        return True
    if int(scores.get("regulatory_legal", 0)) >= 72:
        return True
    if (
        int(scores.get("growth_pipeline", 0)) >= 78
        and int(scores.get("timeline_milestones", 0)) >= 40
    ):
        return True
    if int(scores.get("operations_execution", 0)) >= 80:
        return True
    return False


def _fallback_market_impact_assessment(
    *,
    primary_lane: str,
    lane_meta: Dict[str, Any],
    importance_score: int,
    one_line: str,
    key_facts_paragraph: str,
) -> str:
    lane_msg = {
        "growth_pipeline": "Could materially affect growth assumptions and forward valuation inputs if follow-through confirms the signal.",
        "financing_capital": "Material for funding runway, financing certainty, and dilution/capital-structure outcomes.",
        "timeline_milestones": "Material for schedule risk and milestone timing used in scenario calibration.",
        "regulatory_legal": "Directly de-risks (or increases risk to) regulatory pathway and execution certainty.",
        "operations_execution": "May move operating and valuation assumptions (costs, throughput, margins, cash generation).",
        "market_demand_pricing": "Can shift realized pricing assumptions and external-demand sensitivity in valuation.",
        "management_governance": "Potentially relevant to execution quality and governance risk.",
    }
    line1 = lane_msg.get(primary_lane, "Potentially material for investment thesis updates.")
    if int(importance_score) >= 75:
        line2 = "Likely to require explicit model update in the next analysis cycle."
    elif int(importance_score) >= 55:
        line2 = "Should be monitored and validated against subsequent disclosures."
    else:
        line2 = "Contextual signal; monitor for corroboration in future announcements."
    basis = one_line.strip() or key_facts_paragraph.strip()
    if basis:
        basis = re.sub(r"\s+", " ", basis).strip()
        basis = basis[:220]
        return f"{line1} {line2} Basis: {basis}"
    return f"{line1} {line2}"


def _split_sentences(text: str) -> List[str]:
    raw = str(text or "")
    if not raw:
        return []
    parts = re.split(r"(?<=[.!?])\s+", raw)
    out: List[str] = []
    for part in parts:
        clean = re.sub(r"\s+", " ", str(part)).strip()
        if clean:
            out.append(clean)
    return out


def _extract_target_window(sentence: str) -> str:
    s = str(sentence or "")
    patterns = [
        r"\bQ[1-4]\s+20\d{2}\b",
        r"\bH[1-2]\s+20\d{2}\b",
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+20\d{2}\b",
        r"\b20\d{2}\b",
        r"\bwithin\s+\d+\s+(?:days?|weeks?|months?|years?)\b",
        r"\bon or about\s+[A-Za-z]+\s+\d{1,2},?\s+20\d{2}\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, s, flags=re.IGNORECASE)
        if m:
            return m.group(0)
    return ""


def _infer_timeline_direction(sentence: str) -> str:
    s = str(sentence or "").lower()
    if any(tok in s for tok in ("delay", "deferred", "postponed", "slipped")):
        return "delayed"
    if any(tok in s for tok in ("accelerat", "ahead of schedule", "earlier than")):
        return "accelerated"
    if any(tok in s for tok in ("reconfirm", "on track", "unchanged", "maintain")):
        return "reconfirmed"
    if any(tok in s for tok in ("announc", "commenc", "initiat", "received", "granted", "closed", "approved")):
        return "new"
    return "unclear"


def _extract_timeline_milestones_from_text(text: str, max_items: int = 6) -> List[Dict[str, str]]:
    sentences = _split_sentences(text)
    if not sentences:
        return []
    milestone_terms = re.compile(
        r"\b(milestone|target|expected|schedule|construction|commission|first gold|production|drill program|financing|permit|approval|study|decision)\b",
        flags=re.IGNORECASE,
    )
    time_terms = re.compile(
        r"\b(Q[1-4]|H[1-2]|20\d{2}|within\s+\d+\s+(?:days?|weeks?|months?|years?)|on or about)\b",
        flags=re.IGNORECASE,
    )
    rows: List[Dict[str, str]] = []
    seen: set[str] = set()
    for sent in sentences:
        if len(rows) >= max(1, int(max_items)):
            break
        if not milestone_terms.search(sent):
            continue
        if not time_terms.search(sent):
            continue
        snippet = sent[:260]
        key = snippet.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "milestone": snippet[:120],
                "target_window": _extract_target_window(sent),
                "direction": _infer_timeline_direction(sent),
                "source_snippet": snippet,
            }
        )
    return rows


def _extract_catalysts_from_text(text: str, max_items: int = 6) -> List[str]:
    sentences = _split_sentences(text)
    if not sentences:
        return []
    catalyst_terms = re.compile(
        r"\b(catalyst|expected|target|construction decision|financing|permit|approval|drill program|resource|study|commission|production)\b",
        flags=re.IGNORECASE,
    )
    rows: List[str] = []
    seen: set[str] = set()
    for sent in sentences:
        if len(rows) >= max(1, int(max_items)):
            break
        if not catalyst_terms.search(sent):
            continue
        clean = sent[:220]
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(clean)
    return rows


def _extract_risks_from_text(text: str, max_items: int = 6) -> List[str]:
    sentences = _split_sentences(text)
    if not sentences:
        return []
    risk_terms = re.compile(
        r"\b(risk|uncertaint|delay|subject to|approval|regulatory|financing|dilution|forward-looking|no assurance|could differ)\b",
        flags=re.IGNORECASE,
    )
    rows: List[str] = []
    seen: set[str] = set()
    for sent in sentences:
        if len(rows) >= max(1, int(max_items)):
            break
        if not risk_terms.search(sent):
            continue
        clean = sent[:220]
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(clean)
    return rows


def _build_worker_prompt(
    doc: Dict[str, Any],
    max_key_points: int,
    max_doc_chars: int,
    vision_bundle: Optional[Dict[str, Any]] = None,
) -> str:
    text = str(doc.get("full_text", ""))
    truncated = False
    if len(text) > max_doc_chars:
        text = text[:max_doc_chars]
        truncated = True

    rubric = f"""
You are a strict financial-announcement triage worker.
You are operating in HYBRID mode: use full decoded text plus visual fact pack when available.

Task:
1) Determine if the document is price-sensitive and estimate importance for investment analysis.
2) Always return a compact structured summary (even when low importance) so downstream filtering can happen after analysis.
3) Return at most {max_key_points} key points.

Materiality/importance checks (exclusion-first):
1) First classify low-signal/noise patterns:
   - procedural/admin updates (holder notices, governance forms, routine meeting docs)
   - listing housekeeping without economics change (quotation/cleansing boilerplate)
   - duplicated broadcast copy with no new facts
2) Then classify material signals:
   - funding/capital structure changes (debt/equity terms, dilution, runway)
   - operations and economics (revenue, margin, cash flow, capex/opex, guidance)
   - growth/pipeline outcomes (resources, assays, product/clinical phases, launches, contracts)
   - timeline milestones (new target windows, delays, accelerations, on-track confirmations)
   - regulatory/legal outcomes that change execution risk
   - management/strategy changes with likely execution impact
   - market/demand/pricing shifts with quantified sensitivity where available

If a document is mostly noise and lacks material signals, score it low and set keep_for_injection=false.
Issuer relevance rules (hard guardrails):
1) The target issuer is provided in the metadata. A document for a different company is irrelevant by default.
2) If the title/header/front page clearly belongs to another issuer or ticker, mark it low importance and set keep_for_injection=false.
3) Only treat a non-issuer document as supporting context when it explicitly refers to the target issuer as a counterparty / JV / agreement participant.
4) Supporting-context documents are not primary issuer evidence. Do not set keep_for_injection=true for them.

Return STRICT JSON only with this schema:
{{
  "doc_id": "<file name>",
  "price_sensitive": {{
    "is_price_sensitive": true/false,
    "confidence": 0.0-1.0,
    "reason": "<short reason>"
  }},
  "importance": {{
    "is_important": true/false,
    "importance_score": 0-100,
    "tier": "critical|high|medium|low|ignore",
    "keep_for_injection": true/false,
    "reason": "<short reason>"
  }},
  "summary": {{
    "one_line": "<single line summary>",
    "key_facts_paragraph": "<120-220 words, factual paragraph summarizing the most relevant facts and implications>",
    "key_points": ["... up to {max_key_points} ..."],
    "numeric_facts": [
      {{"metric":"", "value":"", "unit":"", "context":"", "source_snippet":""}}
    ],
    "timeline_milestones": [
      {{"milestone":"", "target_window":"", "direction":"new|reconfirmed|delayed|accelerated|unclear", "source_snippet":""}}
    ],
    "capital_structure": ["..."],
    "catalysts_next_12m": ["..."],
    "risks_headwinds": ["..."],
    "market_impact_assessment": "<2-4 lines>"
  }},
  "extraction_quality": {{
    "text_truncated_for_model": true/false,
    "signal_quality": "high|medium|low",
    "notes": ["..."]
  }}
}}

Rules:
- keep_for_injection should only be true when evidence is genuinely material; use importance score + signal strength.
- Never exceed {max_key_points} key points.
- Prefer concrete numeric/timeline facts and avoid fluff.
- Do not output markdown, code fences, or prose outside JSON.
""".strip()

    vision_status = dict(vision_bundle or {})
    aggregated = dict(vision_status.get("aggregated", {}) or {})
    vision_prompt_pack = {
        "status": str(vision_status.get("status", "disabled")),
        "reason": str(vision_status.get("reason", "")),
        "total_pages": int(vision_status.get("total_pages", 0) or 0),
        "pages_processed": int(vision_status.get("pages_processed", 0) or 0),
        "page_cap": int(vision_status.get("page_cap", 0) or 0),
        "page_cap_applied": bool(vision_status.get("page_cap_applied", False)),
        "relevant_pages": int(vision_status.get("relevant_pages", 0) or 0),
        "notes": list(vision_status.get("notes", []) or [])[:12],
        "aggregated": {
            "key_facts": list(aggregated.get("key_facts", []) or [])[:120],
            "numeric_facts": list(aggregated.get("numeric_facts", []) or [])[:80],
            "timeline_facts": list(aggregated.get("timeline_facts", []) or [])[:80],
            "capital_structure_facts": list(aggregated.get("capital_structure_facts", []) or [])[:60],
            "risks_or_caveats": list(aggregated.get("risks_or_caveats", []) or [])[:60],
        },
    }

    metadata = {
        "doc_id": doc.get("file_name", ""),
        "title": doc.get("title", ""),
        "source_url": doc.get("source_url", ""),
        "pdf_url": doc.get("pdf_url", ""),
        "domain": doc.get("domain", ""),
        "published_at": doc.get("published_at", ""),
        "decoded_chars_in_file": int(doc.get("decoded_chars", 0) or 0),
        "text_chars_sent_to_model": len(text),
        "text_truncated_for_model": truncated,
        "target_issuer": str((doc.get("document_ref", {}) or {}).get("issuer_hint", "")).strip(),
        "target_ticker": str((doc.get("document_ref", {}) or {}).get("ticker_hint", "")).strip(),
    }

    return (
        f"{rubric}\n\n"
        f"DOCUMENT METADATA:\n{json.dumps(metadata, ensure_ascii=True)}\n\n"
        f"VISION FACT PACK:\n{json.dumps(vision_prompt_pack, ensure_ascii=True)}\n\n"
        f"DOCUMENT TEXT:\n{text}"
    )


def _normalize_summary_object(
    obj: Dict[str, Any],
    doc: Dict[str, Any],
    max_key_points: int,
    text_truncated: bool,
    vision_bundle: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(obj or {})
    out["doc_id"] = str(out.get("doc_id") or doc.get("file_name", ""))
    title_for_lanes = str(doc.get("title", "") or "")
    text_for_lanes = str(doc.get("full_text", "") or "")[:120000]
    lane_meta = _compute_relevance_lanes(title_for_lanes, text_for_lanes)
    noise_meta = _compute_noise_profile(title_for_lanes, text_for_lanes)
    issuer_alignment = _detect_issuer_alignment(doc)
    source_url = str(doc.get("source_url", "") or "")
    wrapper_page = _is_wrapper_or_index_page(title_for_lanes, text_for_lanes, source_url)
    role_tags = _derive_document_role_tags(
        title_for_lanes,
        text_for_lanes,
        lane_meta,
        wrapper_page=wrapper_page,
    )

    ps = out.get("price_sensitive", {})
    if not isinstance(ps, dict):
        ps = {}
    is_ps_raw = bool(ps.get("is_price_sensitive", False))
    ps_conf_raw = float(ps.get("confidence", 0.0) or 0.0)
    ps_reason_raw = str(ps.get("reason", "")).strip()

    imp = out.get("importance", {})
    if not isinstance(imp, dict):
        imp = {}
    importance_score = int(imp.get("importance_score", 0) or 0)
    importance_score = max(0, min(100, importance_score))
    is_important = bool(imp.get("is_important", False))
    keep_requested = bool(imp.get("keep_for_injection", is_important))
    reason_text = str(imp.get("reason", "")).strip()

    summary = out.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    key_points = summary.get("key_points", [])
    if not isinstance(key_points, list):
        key_points = []
    key_points = [str(item).strip() for item in key_points if str(item).strip()][:max_key_points]

    def _normalize_list(key: str) -> List[str]:
        val = summary.get(key, [])
        if not isinstance(val, list):
            return []
        return [str(item).strip() for item in val if str(item).strip()]

    numeric_facts = summary.get("numeric_facts", [])
    if not isinstance(numeric_facts, list):
        numeric_facts = []
    numeric_out: List[Dict[str, str]] = []
    for row in numeric_facts[:20]:
        if not isinstance(row, dict):
            continue
        numeric_out.append(
            {
                "metric": str(row.get("metric", "")).strip(),
                "value": str(row.get("value", "")).strip(),
                "unit": str(row.get("unit", "")).strip(),
                "context": str(row.get("context", "")).strip(),
                "source_snippet": str(row.get("source_snippet", "")).strip(),
            }
        )

    timeline_rows = summary.get("timeline_milestones", [])
    if not isinstance(timeline_rows, list):
        timeline_rows = []
    timeline_out: List[Dict[str, str]] = []
    valid_directions = {"new", "reconfirmed", "delayed", "accelerated", "unclear"}
    for row in timeline_rows[:20]:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction", "unclear")).strip().lower()
        if direction not in valid_directions:
            direction = "unclear"
        timeline_out.append(
            {
                "milestone": str(row.get("milestone", "")).strip(),
                "target_window": str(row.get("target_window", "")).strip(),
                "direction": direction,
                "source_snippet": str(row.get("source_snippet", "")).strip(),
            }
        )

    key_facts_paragraph = str(summary.get("key_facts_paragraph", "")).strip()
    if not key_facts_paragraph and key_points:
        key_facts_paragraph = " ".join(key_points[:6]).strip()
    market_impact_assessment = str(summary.get("market_impact_assessment", "")).strip()
    floor = _auto_importance_floor_from_lanes(lane_meta)
    noise_score = int(noise_meta.get("noise_score", 0) or 0)
    primary_score = int(lane_meta.get("primary_score", 0) or 0)
    second_score = int(lane_meta.get("second_score", 0) or 0)
    strong_material = bool(primary_score >= 70 or (primary_score >= 58 and second_score >= 48))
    auto_ps = _auto_price_sensitivity_from_lanes(lane_meta)
    adjusted_score = int(importance_score)
    if floor > adjusted_score and noise_score < 70:
        adjusted_score = floor
    if noise_score >= 75 and not strong_material:
        adjusted_score = min(adjusted_score, 34)
    elif noise_score >= 55 and primary_score < 55:
        adjusted_score = min(adjusted_score, 48)
    if wrapper_page and not strong_material and primary_score < 65:
        adjusted_score = min(adjusted_score, 45)
    adjusted_score = max(0, min(100, adjusted_score))
    adjusted_tier = _tier_from_score(adjusted_score)
    adjusted_is_important = bool(is_important or adjusted_score >= 50 or strong_material)
    if noise_score >= 75 and not strong_material:
        adjusted_is_important = False
    adjusted_ps = bool(is_ps_raw or auto_ps)
    if noise_score >= 75 and primary_score < 55:
        adjusted_ps = False
    adjusted_keep = bool(
        keep_requested
        or adjusted_score >= int(OUTPUT_MIN_IMPORTANCE_SCORE)
        or (adjusted_ps and adjusted_score >= 68)
    )
    if noise_score >= 75 and not strong_material:
        adjusted_keep = False
    if wrapper_page and not strong_material and adjusted_score < 70 and not adjusted_ps:
        adjusted_keep = False
    ps_conf = max(0.0, min(1.0, ps_conf_raw))
    if auto_ps and ps_conf < 0.55:
        ps_conf = 0.55
    if not adjusted_ps:
        ps_conf = min(ps_conf, 0.45)
    reason_suffix: List[str] = []
    if noise_score >= 55 and not strong_material:
        reason_suffix.append(f"noise_score={noise_score}")
    if floor > importance_score and noise_score < 70:
        reason_suffix.append(f"lane_floor={floor}")
    if reason_suffix:
        base_reason = reason_text or "model_assessment"
        reason_text = f"{base_reason}; {'; '.join(reason_suffix)}"
    if wrapper_page:
        base_reason = reason_text or "model_assessment"
        reason_text = f"{base_reason}; wrapper_page"
    if not market_impact_assessment:
        market_impact_assessment = _fallback_market_impact_assessment(
            primary_lane=str(lane_meta.get("primary_lane", "operations_execution")),
            lane_meta=lane_meta,
            importance_score=adjusted_score,
            one_line=str(summary.get("one_line", "")).strip(),
            key_facts_paragraph=key_facts_paragraph,
        )

    auto_fill_notes: List[str] = []
    if adjusted_keep:
        if not timeline_out:
            timeline_out = _extract_timeline_milestones_from_text(text_for_lanes, max_items=6)
            if timeline_out:
                auto_fill_notes.append("auto_fill:timeline_milestones")
        catalysts_auto = _normalize_list("catalysts_next_12m")
        if not catalysts_auto:
            catalysts_auto = _extract_catalysts_from_text(text_for_lanes, max_items=6)
            if catalysts_auto:
                auto_fill_notes.append("auto_fill:catalysts_next_12m")
        risks_auto = _normalize_list("risks_headwinds")
        if not risks_auto:
            risks_auto = _extract_risks_from_text(text_for_lanes, max_items=6)
            if risks_auto:
                auto_fill_notes.append("auto_fill:risks_headwinds")
    else:
        catalysts_auto = _normalize_list("catalysts_next_12m")
        risks_auto = _normalize_list("risks_headwinds")

    issuer_status = str(issuer_alignment.get("status", "unclear")).strip().lower()
    if issuer_status == "mismatch":
        adjusted_score = min(adjusted_score, 12)
        adjusted_tier = "ignore"
        adjusted_is_important = False
        adjusted_ps = False
        adjusted_keep = False
        market_impact_assessment = (
            "Issuer mismatch: document appears to belong to another company and should be excluded."
        )
        base_reason = reason_text or "model_assessment"
        reason_text = f"{base_reason}; issuer_mismatch:{issuer_alignment.get('reason', 'header_mismatch')}"
        auto_fill_notes.append("issuer_mismatch_forced_drop")
    elif issuer_status == "related_party":
        adjusted_score = min(adjusted_score, 28)
        adjusted_tier = _tier_from_score(adjusted_score)
        adjusted_is_important = False
        adjusted_keep = False
        adjusted_ps = False
        base_reason = reason_text or "model_assessment"
        reason_text = f"{base_reason}; related_party_supporting_context_only"
        auto_fill_notes.append("related_party_supporting_context_only")

    out["price_sensitive"] = {
        "is_price_sensitive": adjusted_ps,
        "confidence": round(ps_conf, 4),
        "reason": ps_reason_raw or ("auto_lane_signal" if auto_ps else ""),
    }
    out["importance"] = {
        "is_important": adjusted_is_important,
        "importance_score": int(adjusted_score),
        "tier": adjusted_tier,
        "keep_for_injection": adjusted_keep,
        "reason": reason_text,
        "relevance_mode": "exclusion_first",
        "lane_meta": lane_meta,
        "noise_meta": noise_meta,
    }

    out["summary"] = {
        "one_line": str(summary.get("one_line", "")).strip(),
        "key_facts_paragraph": key_facts_paragraph,
        "key_points": key_points,
        "numeric_facts": numeric_out if bool(OUTPUT_INCLUDE_NUMERIC_FACTS) else [],
        "timeline_milestones": timeline_out,
        "capital_structure": _normalize_list("capital_structure"),
        "catalysts_next_12m": catalysts_auto,
        "risks_headwinds": risks_auto,
        "market_impact_assessment": market_impact_assessment,
    }

    quality = out.get("extraction_quality", {})
    if not isinstance(quality, dict):
        quality = {}
    signal_quality = str(quality.get("signal_quality", "low")).strip().lower()
    if signal_quality not in {"high", "medium", "low"}:
        signal_quality = "low"
    notes = quality.get("notes", [])
    if not isinstance(notes, list):
        notes = []
    notes_clean = [str(item).strip() for item in notes if str(item).strip()]
    notes_clean.extend(auto_fill_notes)
    if not OUTPUT_INCLUDE_NUMERIC_FACTS:
        notes_clean.append("numeric_facts_disabled_by_policy")

    out["extraction_quality"] = {
        "text_truncated_for_model": bool(quality.get("text_truncated_for_model", text_truncated)),
        "signal_quality": signal_quality,
        "notes": notes_clean[:30],
    }

    out["source_meta"] = {
        "file_name": str(doc.get("file_name", "")),
        "file": str(doc.get("file", "")),
        "title": str(doc.get("title", "")),
        "source_url": source_url,
        "pdf_url": str(doc.get("pdf_url", "")),
        "domain": str(doc.get("domain", "")),
        "published_at": str(doc.get("published_at", "")),
        "decoded_chars_in_file": int(doc.get("decoded_chars", 0) or 0),
        "wrapper_page": bool(wrapper_page),
        "role_tags": role_tags,
        "issuer_validation": issuer_alignment,
    }
    if isinstance(vision_bundle, dict):
        out["source_meta"]["vision_meta"] = {
            "enabled": bool(vision_bundle.get("enabled", False)),
            "status": str(vision_bundle.get("status", "")),
            "reason": str(vision_bundle.get("reason", "")),
            "use_vision": bool(vision_bundle.get("use_vision", False)),
            "policy": str(vision_bundle.get("policy", "")),
            "heuristic_score": int(vision_bundle.get("heuristic_score", 0) or 0),
            "decision_reasons": list(vision_bundle.get("decision_reasons", []) or [])[:8],
            "total_pages": int(vision_bundle.get("total_pages", 0) or 0),
            "pages_processed": int(vision_bundle.get("pages_processed", 0) or 0),
            "page_cap": int(vision_bundle.get("page_cap", 0) or 0),
            "page_cap_applied": bool(vision_bundle.get("page_cap_applied", False)),
            "relevant_pages": int(vision_bundle.get("relevant_pages", 0) or 0),
            "sampled_pages": int(((vision_bundle.get("probe", {}) or {}).get("sampled_pages", 0) or 0)),
            "avg_sample_text_chars": float(
                ((vision_bundle.get("probe", {}) or {}).get("avg_sample_text_chars", 0.0) or 0.0)
            ),
            "low_text_page_ratio": float(
                ((vision_bundle.get("probe", {}) or {}).get("low_text_page_ratio", 0.0) or 0.0)
            ),
            "avg_image_count_per_page": float(
                ((vision_bundle.get("probe", {}) or {}).get("avg_image_count_per_page", 0.0) or 0.0)
            ),
            "short_line_ratio": float(
                ((vision_bundle.get("probe", {}) or {}).get("short_line_ratio", 0.0) or 0.0)
            ),
        }

    return out


def _heuristic_summary_from_doc(
    *,
    doc: Dict[str, Any],
    full_text: str,
    max_key_points: int,
    text_truncated: bool,
    vision_bundle: Optional[Dict[str, Any]] = None,
    notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    title = str(doc.get("title", "")).strip()
    haystack = f"{title}\n{full_text}".lower()

    lane_meta = _compute_relevance_lanes(title, full_text)
    noise_meta = _compute_noise_profile(title, full_text)
    issuer_alignment = _detect_issuer_alignment(doc)
    primary = int(lane_meta.get("primary_score", 0) or 0)
    second = int(lane_meta.get("second_score", 0) or 0)
    noise_score = int(noise_meta.get("noise_score", 0) or 0)
    score = max(primary, int((0.7 * primary) + (0.3 * second)))
    if sum(1 for v in (lane_meta.get("scores", {}) or {}).values() if int(v) >= 60) >= 2:
        score += 8
    floor = _auto_importance_floor_from_lanes(lane_meta)
    if floor > score and noise_score < 70:
        score = floor
    if noise_score >= 75 and primary < 60:
        score = min(score, 34)
    elif noise_score >= 55 and primary < 55:
        score = min(score, 48)
    score = max(0, min(100, int(score)))
    tier = _tier_from_score(score)
    issuer_status = str(issuer_alignment.get("status", "unclear")).strip().lower()
    if issuer_status == "mismatch":
        score = min(score, 12)
        tier = "ignore"
    elif issuer_status == "related_party":
        score = min(score, 28)
        tier = _tier_from_score(score)

    sentence_candidates = re.split(r"(?<=[.!?])\s+", str(full_text or ""))
    key_points: List[str] = []
    lane_hit_terms = {
        term
        for terms in dict(lane_meta.get("hits", {}) or {}).values()
        for term in list(terms or [])
    }
    for sent in sentence_candidates:
        s = re.sub(r"\s+", " ", sent).strip()
        if not s:
            continue
        if len(s) < 45:
            continue
        s_low = s.lower()
        if any(tok in s_low for tok in lane_hit_terms) or re.search(r"\d", s):
            key_points.append(s[:260])
        if len(key_points) >= max(1, int(max_key_points)):
            break

    if not key_points:
        short = re.sub(r"\s+", " ", str(full_text or "")).strip()
        if short:
            key_points = [short[:220]]
        elif title:
            key_points = [title]

    paragraph = " ".join(key_points[: min(6, len(key_points))]).strip()
    paragraph = re.sub(r"\s+", " ", paragraph)
    if len(paragraph) > 850:
        paragraph = paragraph[:847].rstrip() + "..."

    is_price_sensitive = bool(_auto_price_sensitivity_from_lanes(lane_meta) or score >= 68)
    if noise_score >= 75 and primary < 55:
        is_price_sensitive = False
    if issuer_status in {"mismatch", "related_party"}:
        is_price_sensitive = False
    ps_conf = max(0.2, min(0.85, (max(primary, second) / 100.0) + 0.05))
    if not is_price_sensitive:
        ps_conf = min(ps_conf, 0.45)
    market_impact = _fallback_market_impact_assessment(
        primary_lane=str(lane_meta.get("primary_lane", "operations_execution")),
        lane_meta=lane_meta,
        importance_score=score,
        one_line=(key_points[0][:180] if key_points else title[:180]),
        key_facts_paragraph=paragraph,
    )

    timeline_rows = _extract_timeline_milestones_from_text(full_text, max_items=6)
    catalysts_rows = _extract_catalysts_from_text(full_text, max_items=6)
    risks_rows = _extract_risks_from_text(full_text, max_items=6)

    payload = {
        "doc_id": str(doc.get("file_name", "")),
        "price_sensitive": {
            "is_price_sensitive": bool(is_price_sensitive),
            "confidence": round(ps_conf, 3),
            "reason": "heuristic_recovery_from_worker_failure",
        },
        "importance": {
            "is_important": bool(score >= 45 and issuer_status not in {"mismatch", "related_party"}),
            "importance_score": int(score),
            "tier": tier,
            "keep_for_injection": bool(score >= 50 and issuer_status == "match"),
            "reason": (
                f"heuristic_recovery_from_worker_failure; issuer_status={issuer_status}"
                if issuer_status != "unclear"
                else "heuristic_recovery_from_worker_failure"
            ),
        },
        "summary": {
            "one_line": (key_points[0][:180] if key_points else title[:180]),
            "key_facts_paragraph": paragraph,
            "key_points": key_points[: max(1, int(max_key_points))],
            "numeric_facts": [],
            "timeline_milestones": timeline_rows,
            "capital_structure": [],
            "catalysts_next_12m": catalysts_rows,
            "risks_headwinds": risks_rows,
            "market_impact_assessment": market_impact,
        },
        "extraction_quality": {
            "text_truncated_for_model": bool(text_truncated),
            "signal_quality": "low",
            "notes": (list(notes or []) + ([f"issuer_status:{issuer_status}"] if issuer_status != "unclear" else []))[:12],
        },
        "source_meta": {
            "title": title,
            "source_url": str(doc.get("source_url", "")).strip(),
            "issuer_validation": issuer_alignment,
        },
    }
    return _normalize_summary_object(
        payload,
        doc=doc,
        max_key_points=max_key_points,
        text_truncated=text_truncated,
        vision_bundle=vision_bundle,
    )


async def _summarize_one(
    *,
    doc: Dict[str, Any],
    model: str,
    max_key_points: int,
    max_doc_chars: int,
    timeout_seconds: float,
    vision_enabled: bool,
    vision_model: str,
    vision_max_pages: int,
    vision_page_batch_size: int,
    vision_max_page_facts: int,
    vision_zoom: float,
    vision_timeout_seconds: float,
    vision_max_tokens: int,
    complex_reasoning_model: str,
    complex_reasoning_min_doc_chars: int,
    complex_reasoning_min_importance_score: int,
) -> Dict[str, Any]:
    text = str(doc.get("full_text", "") or "")
    decoded_chars = int(doc.get("decoded_chars", 0) or len(text))
    vision_policy, vision_pdf_bytes = await _decide_vision_policy(
        doc=doc,
        vision_enabled=bool(vision_enabled),
    )
    vision_bundle: Dict[str, Any] = {
        "enabled": False,
        "status": "disabled" if not bool(vision_policy.get("use_vision", False)) else "ok",
        "reason": str(vision_policy.get("reason", "worker_disable_vision")),
        "use_vision": bool(vision_policy.get("use_vision", False)),
        "policy": str(vision_policy.get("policy", "")),
        "heuristic_score": int(vision_policy.get("heuristic_score", 0) or 0),
        "decision_reasons": list(vision_policy.get("decision_reasons", []) or [])[:8],
        "probe": dict(vision_policy.get("probe", {}) or {}),
        "total_pages": 0,
        "pages_processed": 0,
        "page_cap": int(vision_max_pages),
        "page_cap_applied": False,
        "relevant_pages": 0,
        "notes": [],
        "aggregated": {},
    }
    if bool(vision_policy.get("use_vision", False)):
        extracted_vision_bundle = await _extract_vision_bundle(
            doc=doc,
            model=str(vision_model),
            max_pages=int(vision_max_pages),
            batch_size=int(vision_page_batch_size),
            max_page_facts=int(vision_max_page_facts),
            zoom=float(vision_zoom),
            timeout_seconds=float(vision_timeout_seconds),
            max_tokens=int(vision_max_tokens),
            pdf_bytes=vision_pdf_bytes,
        )
        vision_bundle.update(extracted_vision_bundle)
        vision_bundle["enabled"] = True
    else:
        vision_bundle["status"] = "skipped"

    prompt = _build_worker_prompt(
        doc,
        max_key_points=max_key_points,
        max_doc_chars=max_doc_chars,
        vision_bundle=vision_bundle,
    )
    text_truncated = len(text) > max_doc_chars

    errors: List[str] = []
    last_obj: Optional[Dict[str, Any]] = None
    base_max_tokens = _adaptive_worker_max_tokens(decoded_chars)
    attempt_caps = [
        min(WORKER_MAX_OUTPUT_TOKENS_CAP, base_max_tokens + (step * 700))
        for step in range(0, 4)
    ]
    attempts = len(attempt_caps)
    for attempt in range(1, attempts + 1):
        cap = int(attempt_caps[attempt - 1])
        response = await query_model(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            timeout=float(timeout_seconds),
            max_tokens=cap,
            reasoning_effort="low",
        )
        if not response or not response.get("content"):
            errors.append(f"attempt_{attempt}:empty_response:max_tokens={cap}")
            await asyncio.sleep(min(2.0 * attempt, 6.0))
            continue

        finish_reason = str(response.get("finish_reason", "")).strip().lower()
        obj = _extract_fenced_json(str(response.get("content", "")))
        if isinstance(obj, dict):
            last_obj = obj
            errors.append(f"attempt_{attempt}:json_parse_ok:finish_reason={finish_reason or 'unknown'}:max_tokens={cap}")
            break

        errors.append(
            f"attempt_{attempt}:json_parse_failed:finish_reason={finish_reason or 'unknown'}:max_tokens={cap}"
        )
        if finish_reason == "length":
            # Keep original prompt and increase token cap on next attempt.
            await asyncio.sleep(min(1.0 * attempt, 4.0))
            continue

        repair_prompt = (
            "Return STRICT JSON only for the same schema. "
            "No markdown, no prose, no code fences.\n\n"
            f"Previous output (possibly invalid/truncated):\n{str(response.get('content', ''))[:5000]}"
        )
        prompt = repair_prompt
        await asyncio.sleep(min(1.0 * attempt, 4.0))

    if last_obj is None:
        # Degraded retry path: drop vision context and shorten text before giving up.
        degraded_doc = dict(doc)
        degraded_doc["full_text"] = str(text or "")[: max(30000, min(90000, int(max_doc_chars)))]
        degraded_prompt = _build_worker_prompt(
            degraded_doc,
            max_key_points=max_key_points,
            max_doc_chars=max(30000, min(90000, int(max_doc_chars))),
            vision_bundle={
                "status": "disabled",
                "reason": "degraded_retry_text_only",
                "total_pages": 0,
                "pages_processed": 0,
                "page_cap": 0,
                "page_cap_applied": False,
                "relevant_pages": 0,
                "notes": [],
                "aggregated": {},
            },
        )
        degraded_base_cap = min(
            WORKER_MAX_OUTPUT_TOKENS_CAP,
            max(4200, base_max_tokens + 900),
        )
        for retry_idx in range(1, 3):
            retry_cap = min(WORKER_MAX_OUTPUT_TOKENS_CAP, degraded_base_cap + ((retry_idx - 1) * 700))
            retry_response = await query_model(
                model=model,
                messages=[{"role": "user", "content": degraded_prompt}],
                timeout=float(timeout_seconds),
                max_tokens=int(retry_cap),
                reasoning_effort="low",
            )
            retry_finish = str((retry_response or {}).get("finish_reason", "")).strip().lower()
            retry_obj = _extract_fenced_json(str((retry_response or {}).get("content", "")))
            if isinstance(retry_obj, dict):
                recovered = _normalize_summary_object(
                    retry_obj,
                    doc=doc,
                    max_key_points=max_key_points,
                    text_truncated=text_truncated,
                    vision_bundle=vision_bundle,
                )
                notes = list((recovered.get("extraction_quality", {}) or {}).get("notes", []) or [])
                notes.extend(errors)
                notes.append(
                    f"degraded_retry_success_{retry_idx}:finish_reason={retry_finish or 'unknown'}:max_tokens={retry_cap}"
                )
                recovered["extraction_quality"]["notes"] = notes[:20]
                return recovered
            errors.append(
                f"degraded_retry_{retry_idx}:json_parse_failed:finish_reason={retry_finish or 'unknown'}:max_tokens={retry_cap}"
            )
            await asyncio.sleep(float(retry_idx))

        # Final safety rail: produce heuristic structured summary instead of hard worker_failed.
        heuristic_notes = list(errors or [])
        heuristic_notes.append("heuristic_recovery_applied")
        return _heuristic_summary_from_doc(
            doc=doc,
            full_text=str(text or ""),
            max_key_points=max_key_points,
            text_truncated=text_truncated,
            vision_bundle=vision_bundle,
            notes=heuristic_notes,
        )

    normalized = _normalize_summary_object(
        last_obj,
        doc=doc,
        max_key_points=max_key_points,
        text_truncated=text_truncated,
        vision_bundle=vision_bundle,
    )
    normalized.setdefault("source_meta", {})
    normalized["source_meta"]["analysis_model_used"] = str(model)

    escalation_model = str(complex_reasoning_model or "").strip()
    if escalation_model and escalation_model != str(model):
        importance_score = int((normalized.get("importance", {}) or {}).get("importance_score", 0) or 0)
        should_escalate = (
            len(text) >= int(max(10000, complex_reasoning_min_doc_chars))
            or importance_score >= int(max(0, min(100, complex_reasoning_min_importance_score)))
        )
        if should_escalate:
            esc_response = await query_model(
                model=escalation_model,
                messages=[{"role": "user", "content": prompt}],
                timeout=float(timeout_seconds),
                max_tokens=2200,
                reasoning_effort="medium",
            )
            esc_obj = _extract_fenced_json(str((esc_response or {}).get("content", "")))
            if isinstance(esc_obj, dict):
                esc_normalized = _normalize_summary_object(
                    esc_obj,
                    doc=doc,
                    max_key_points=max_key_points,
                    text_truncated=text_truncated,
                    vision_bundle=vision_bundle,
                )
                esc_normalized.setdefault("source_meta", {})
                esc_normalized["source_meta"]["analysis_model_used"] = escalation_model
                esc_normalized["source_meta"]["analysis_model_fallback"] = str(model)
                notes = list((esc_normalized.get("extraction_quality", {}) or {}).get("notes", []) or [])
                notes.append(f"complex_reasoning_escalation_applied:{model}->{escalation_model}")
                esc_normalized["extraction_quality"]["notes"] = notes
                normalized = esc_normalized
            else:
                notes = list((normalized.get("extraction_quality", {}) or {}).get("notes", []) or [])
                notes.append(f"complex_reasoning_escalation_failed:{model}->{escalation_model}")
                normalized["extraction_quality"]["notes"] = notes

    if errors:
        notes = normalized["extraction_quality"].get("notes", [])
        normalized["extraction_quality"]["notes"] = [*notes, *errors]
    return normalized


def _render_markdown_report(
    *,
    model: str,
    dump_dir: Path,
    processed: List[Dict[str, Any]],
    kept: List[Dict[str, Any]],
    dropped: List[Dict[str, Any]],
    hybrid_vision: Dict[str, Any],
) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    lines: List[str] = [
        "# Announcement Summaries",
        "",
        f"- generated_at_utc: {generated_at}",
        f"- worker_model: {model}",
        f"- output_min_importance_score: {int(OUTPUT_MIN_IMPORTANCE_SCORE)}",
        f"- output_include_numeric_facts: {bool(OUTPUT_INCLUDE_NUMERIC_FACTS)}",
        f"- hybrid_vision_enabled: {bool(hybrid_vision.get('enabled', False))}",
        f"- hybrid_vision_model: {hybrid_vision.get('vision_model', '')}",
        f"- hybrid_vision_max_pages: {hybrid_vision.get('vision_max_pages', 0)} (0=all; default soft cap=50)",
        f"- complex_reasoning_model: {hybrid_vision.get('complex_reasoning_model', '')}",
        f"- complex_reasoning_min_doc_chars: {hybrid_vision.get('complex_reasoning_min_doc_chars', 0)}",
        f"- complex_reasoning_min_importance_score: {hybrid_vision.get('complex_reasoning_min_importance_score', 0)}",
        f"- dump_dir: {dump_dir}",
        f"- total_processed: {len(processed)}",
        f"- kept_for_injection: {len(kept)}",
        f"- dropped_as_unimportant: {len(dropped)}",
        "",
        "## Kept Documents (JSON Elements)",
        "",
    ]

    for item in kept:
        lines.append(f"### {item.get('source_meta', {}).get('file_name', item.get('doc_id', 'document'))}")
        lines.append("```json")
        lines.append(json.dumps(item, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")

    lines.extend(["## Dropped Documents (Classification Only)", ""])
    for item in dropped:
        compact = {
            "doc_id": item.get("doc_id", ""),
            "price_sensitive": item.get("price_sensitive", {}),
            "importance": item.get("importance", {}),
            "summary": {
                "one_line": ((item.get("summary", {}) or {}).get("one_line", "")),
                "key_facts_paragraph": ((item.get("summary", {}) or {}).get("key_facts_paragraph", "")),
                "key_points": ((item.get("summary", {}) or {}).get("key_points", []))[:8],
            },
            "source_meta": item.get("source_meta", {}),
            "extraction_quality": item.get("extraction_quality", {}),
        }
        lines.append(f"### {compact.get('source_meta', {}).get('file_name', compact.get('doc_id', 'document'))}")
        lines.append("```json")
        lines.append(json.dumps(compact, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _build_worker_doc_audit_rows(
    *,
    docs: List[Dict[str, Any]],
    results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_file = {str(doc.get("file_name", "")): doc for doc in docs}
    rows: List[Dict[str, Any]] = []

    for item in results:
        meta = item.get("source_meta", {}) or {}
        file_name = str(meta.get("file_name", "")).strip()
        src_doc = by_file.get(file_name, {})
        full_text = str(src_doc.get("full_text", "") or "")
        notes = list((item.get("extraction_quality", {}) or {}).get("notes", []) or [])
        keep = bool((item.get("importance", {}) or {}).get("keep_for_injection", False))

        parse_fail_notes = [n for n in notes if "json_parse_failed" in str(n)]
        truncation_notes = [n for n in notes if "finish_reason=length" in str(n)]
        heuristic_applied = any("heuristic_recovery_applied" == str(n) for n in notes)

        summary = item.get("summary", {}) or {}
        timeline_rows = list(summary.get("timeline_milestones", []) or [])
        catalysts_rows = list(summary.get("catalysts_next_12m", []) or [])
        risks_rows = list(summary.get("risks_headwinds", []) or [])
        numeric_rows = list(summary.get("numeric_facts", []) or [])

        source_timeline_signal = bool(_extract_timeline_milestones_from_text(full_text, max_items=3))
        source_catalyst_signal = bool(_extract_catalysts_from_text(full_text, max_items=3))
        source_risk_signal = bool(_extract_risks_from_text(full_text, max_items=3))

        path = "direct_model_json"
        if heuristic_applied:
            path = "heuristic_recovery"
        elif parse_fail_notes:
            path = "model_json_after_retry"

        cause_codes: List[str] = []
        if parse_fail_notes:
            cause_codes.append("json_parse_failures")
        if truncation_notes:
            cause_codes.append("output_truncation")
        if heuristic_applied:
            cause_codes.append("heuristic_recovery_applied")
        if keep and source_timeline_signal and not timeline_rows:
            cause_codes.append("timeline_loss_vs_source")
        if keep and source_catalyst_signal and not catalysts_rows:
            cause_codes.append("catalyst_loss_vs_source")
        if keep and source_risk_signal and not risks_rows:
            cause_codes.append("risk_loss_vs_source")
        if keep and not OUTPUT_INCLUDE_NUMERIC_FACTS:
            cause_codes.append("numeric_disabled_by_policy")

        rows.append(
            {
                "doc_id": str(item.get("doc_id", "")),
                "title": str(meta.get("title", "")),
                "source_url": str(meta.get("source_url", "")),
                "published_at": str(meta.get("published_at", "")),
                "decoded_chars_in_file": int(meta.get("decoded_chars_in_file", 0) or 0),
                "analysis_model_used": str(meta.get("analysis_model_used", "")),
                "keep_for_injection": keep,
                "importance_score": int((item.get("importance", {}) or {}).get("importance_score", 0) or 0),
                "importance_tier": str((item.get("importance", {}) or {}).get("tier", "")),
                "signal_quality": str((item.get("extraction_quality", {}) or {}).get("signal_quality", "")),
                "parser_path": path,
                "parse_fail_count": len(parse_fail_notes),
                "truncation_note_count": len(truncation_notes),
                "notes": notes[:40],
                "extracted_counts": {
                    "timeline_milestones": len(timeline_rows),
                    "catalysts_next_12m": len(catalysts_rows),
                    "risks_headwinds": len(risks_rows),
                    "numeric_facts": len(numeric_rows),
                    "key_points": len(list(summary.get("key_points", []) or [])),
                },
                "source_signal_flags": {
                    "timeline_signal": source_timeline_signal,
                    "catalyst_signal": source_catalyst_signal,
                    "risk_signal": source_risk_signal,
                },
                "cause_codes": cause_codes,
            }
        )
    return rows


async def _run(args: argparse.Namespace) -> int:
    load_dotenv()

    dump_dir = Path(args.dump_dir).resolve()
    if not dump_dir.exists() or not dump_dir.is_dir():
        raise FileNotFoundError(f"Dump directory not found: {dump_dir}")

    parsed_documents_path = (
        Path(args.parsed_documents_json).resolve()
        if str(args.parsed_documents_json or "").strip()
        else (dump_dir / "parsed_documents.json")
    )

    docs: List[Dict[str, Any]] = []
    if parsed_documents_path.exists() and parsed_documents_path.is_file():
        docs = _read_parsed_documents(parsed_documents_path)
        if docs:
            print(f"Loaded parsed documents: {parsed_documents_path}")

    if not docs:
        md_files = sorted(
            [
                path
                for path in dump_dir.glob("*.md")
                if path.is_file() and path.name != "index.md"
            ]
        )
        if not md_files:
            print(f"No parsed documents or markdown dump files found in: {dump_dir}")
            return 1
        docs = [_read_dump_markdown(path) for path in md_files]

    if int(args.max_docs) > 0:
        docs = docs[: int(args.max_docs)]

    sem = asyncio.Semaphore(max(1, int(args.concurrency)))
    results: List[Dict[str, Any]] = []

    async def _worker(doc: Dict[str, Any]) -> Dict[str, Any]:
        async with sem:
            return await _summarize_one(
                doc=doc,
                model=str(args.model),
                max_key_points=max(1, int(args.max_key_points)),
                max_doc_chars=max(5000, int(args.max_doc_chars)),
                timeout_seconds=float(args.timeout_seconds),
                vision_enabled=bool(args.worker_enable_vision),
                vision_model=str(args.vision_model),
                vision_max_pages=int(args.vision_max_pages),
                vision_page_batch_size=max(1, int(args.vision_page_batch_size)),
                vision_max_page_facts=max(1, int(args.vision_max_page_facts)),
                vision_zoom=float(args.vision_zoom),
                vision_timeout_seconds=max(30.0, float(args.vision_timeout_seconds)),
                vision_max_tokens=max(400, int(args.vision_max_tokens)),
                complex_reasoning_model=str(args.complex_reasoning_model),
                complex_reasoning_min_doc_chars=max(10000, int(args.complex_reasoning_min_doc_chars)),
                complex_reasoning_min_importance_score=max(
                    0, min(100, int(args.complex_reasoning_min_importance_score))
                ),
            )

    tasks = [_worker(doc) for doc in docs]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
        meta = result.get("source_meta", {}) or {}
        imp = result.get("importance", {}) or {}
        vision_meta = meta.get("vision_meta", {}) or {}
        completed = len(results)
        total = len(docs)
        vision_label = "used" if bool(vision_meta.get("use_vision", False)) else str(
            vision_meta.get("status", "n/a")
        )
        print(
            f"worker progress: completed={completed}/{total} "
            f"file={meta.get('file_name', result.get('doc_id', 'doc'))} "
            f"keep={imp.get('keep_for_injection', False)} "
            f"score={imp.get('importance_score', 0)} "
            f"tier={imp.get('tier', 'ignore')} "
            f"vision={vision_label} "
            f"policy={str(vision_meta.get('policy', '')).strip() or 'n/a'}",
            flush=True,
        )

    # Preserve input order in output.
    order_map = {doc.get("file_name", ""): idx for idx, doc in enumerate(docs)}
    results.sort(key=lambda item: order_map.get((item.get("source_meta", {}) or {}).get("file_name", ""), 10**9))

    kept = [item for item in results if bool((item.get("importance", {}) or {}).get("keep_for_injection", False))]
    dropped = [item for item in results if item not in kept]

    out_md = Path(args.output_markdown).resolve() if args.output_markdown else (dump_dir / "announcement_summaries.md")
    out_json = Path(args.output_json).resolve() if args.output_json else (dump_dir / "announcement_summaries.json")

    report = _render_markdown_report(
        model=str(args.model),
        dump_dir=dump_dir,
        processed=results,
        kept=kept,
        dropped=dropped,
        hybrid_vision={
            "enabled": bool(args.worker_enable_vision),
            "vision_model": str(args.vision_model),
            "vision_max_pages": int(args.vision_max_pages),
            "complex_reasoning_model": str(args.complex_reasoning_model),
            "complex_reasoning_min_doc_chars": int(args.complex_reasoning_min_doc_chars),
            "complex_reasoning_min_importance_score": int(args.complex_reasoning_min_importance_score),
        },
    )
    out_md.write_text(report, encoding="utf-8")

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "worker_model": str(args.model),
        "output_policy": {
            "min_importance_score": int(OUTPUT_MIN_IMPORTANCE_SCORE),
            "include_numeric_facts": bool(OUTPUT_INCLUDE_NUMERIC_FACTS),
        },
        "hybrid_vision": {
            "enabled": bool(args.worker_enable_vision),
            "vision_model": str(args.vision_model),
            "vision_max_pages": int(args.vision_max_pages),
            "vision_page_batch_size": int(args.vision_page_batch_size),
            "vision_max_page_facts": int(args.vision_max_page_facts),
            "vision_zoom": float(args.vision_zoom),
            "vision_timeout_seconds": float(args.vision_timeout_seconds),
            "vision_max_tokens": int(args.vision_max_tokens),
        },
        "complex_reasoning": {
            "model": str(args.complex_reasoning_model),
            "min_doc_chars": int(args.complex_reasoning_min_doc_chars),
            "min_importance_score": int(args.complex_reasoning_min_importance_score),
        },
        "dump_dir": str(dump_dir),
        "total_processed": len(results),
        "kept_for_injection": len(kept),
        "dropped_as_unimportant": len(dropped),
        "results": results,
    }

    doc_audit_rows = _build_worker_doc_audit_rows(docs=docs, results=results)
    doc_audit_path = dump_dir / "worker_doc_audit.json"
    doc_audit_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dump_dir": str(dump_dir),
        "worker_model": str(args.model),
        "rows": doc_audit_rows,
    }
    doc_audit_path.write_text(json.dumps(doc_audit_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["doc_audit_json"] = str(doc_audit_path)

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Output markdown: {out_md}", flush=True)
    print(f"Output json: {out_json}", flush=True)
    print(f"Doc audit json: {doc_audit_path}", flush=True)
    print(f"Processed={len(results)} kept={len(kept)} dropped={len(dropped)}", flush=True)
    return 0


def main() -> None:
    args = _parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
