from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse

import httpx

from ...pdf_processor import extract_text_from_pdf
from ..contracts import DocumentRef, ParsedDocument
from ..parser_base import DocumentParser

try:
    from bs4 import BeautifulSoup  # type: ignore

    BS4_AVAILABLE = True
except Exception:
    BeautifulSoup = None  # type: ignore
    BS4_AVAILABLE = False


class LocalDocumentParser(DocumentParser):
    @property
    def parser_id(self) -> str:
        return "local_default"

    async def parse_document(
        self,
        doc_ref: DocumentRef,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> ParsedDocument:
        own_client = client is None
        active_client = client or httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(60.0, connect=20.0),
        )
        try:
            return await _parse_document_with_client(doc_ref, active_client)
        finally:
            if own_client:
                await active_client.aclose()


def _looks_like_pdf_url(url: str) -> bool:
    raw = str(url or "").strip().lower()
    if not raw:
        return False
    if raw.endswith(".pdf"):
        return True
    path = urlparse(raw).path.lower()
    return path.endswith(".pdf") or "/asxpdf/" in raw


def _html_to_text(html: str) -> str:
    raw = str(html or "").strip()
    if not raw:
        return ""
    if BS4_AVAILABLE and BeautifulSoup is not None:
        try:
            soup = BeautifulSoup(raw, "html.parser")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            text = soup.get_text("\n")
            text = re.sub(r"\n{3,}", "\n\n", text)
            return re.sub(r"[ \t]+", " ", text).strip()
        except Exception:
            pass
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_quality_from_text(text: str, *, page_count: int, document_type: str) -> Dict[str, object]:
    chars = len(text)
    avg_chars_per_page = float(chars / max(1, int(page_count or 1)))
    line_lengths = [len(line.strip()) for line in str(text or "").splitlines() if line.strip()]
    avg_line_len = float(sum(line_lengths) / max(1, len(line_lengths)))
    short_lines = sum(1 for value in line_lengths if value <= 60)
    short_line_ratio = float(short_lines / max(1, len(line_lengths)))

    if document_type == "html":
        layout_type = "mixed"
    elif avg_line_len <= 55 and short_line_ratio >= 0.75:
        layout_type = "slide_like"
    elif avg_line_len >= 70:
        layout_type = "text_report"
    else:
        layout_type = "mixed"

    if avg_chars_per_page >= 2200:
        text_density = "high"
    elif avg_chars_per_page >= 900:
        text_density = "medium"
    else:
        text_density = "low"

    sparse = bool(chars > 0 and avg_chars_per_page < 500)
    image_heavy = bool(layout_type == "slide_like" and avg_chars_per_page < 900)
    confidence = 0.9 if chars >= 2000 else (0.7 if chars >= 600 else 0.45)
    return {
        "text_density": text_density,
        "text_extraction_sparse": sparse,
        "image_heavy": image_heavy,
        "layout_type": layout_type,
        "avg_chars_per_page": round(avg_chars_per_page, 2),
        "avg_line_len": round(avg_line_len, 2),
        "short_line_ratio": round(short_line_ratio, 3),
        "confidence": confidence,
    }


def _blank_visual_fact_pack() -> Dict[str, object]:
    return {
        "status": "unused",
        "reason": "not_run_in_parse_stage",
        "pages_processed": 0,
        "relevant_pages": 0,
        "key_facts": [],
        "numeric_facts": [],
        "timeline_facts": [],
        "capital_structure_facts": [],
        "risks_or_caveats": [],
    }


def _issuer_signals(doc_ref: DocumentRef) -> Dict[str, object]:
    issuer = str(doc_ref.get("issuer_hint", "") or "").strip()
    ticker = str(doc_ref.get("ticker_hint", "") or "").strip()
    return {
        "issuer_fit": "unclear",
        "issuer_mentions": [issuer] if issuer else [],
        "ticker_mentions": [ticker] if ticker else [],
        "project_mentions": [],
    }


def _empty_failure(
    doc_ref: DocumentRef,
    *,
    document_type: str,
    content_type: str,
    content_bytes: int,
    error: str,
) -> ParsedDocument:
    return {
        "doc_id": str(doc_ref.get("doc_id", "")),
        "file_name": str(doc_ref.get("doc_id", "")),
        "title": str(doc_ref.get("title", "")),
        "source_url": str(doc_ref.get("source_url", "")),
        "content_url": str(doc_ref.get("content_url", "")),
        "pdf_url": str(doc_ref.get("pdf_url", "")),
        "domain": str(doc_ref.get("domain", "")),
        "published_at": str(doc_ref.get("published_at", "")),
        "document_type": document_type,
        "parse_status": "failed",
        "parse_errors": [error] if error else ["parse_failed"],
        "raw_text": "",
        "full_text": "",
        "decoded_chars": 0,
        "page_count": 0,
        "parse_method": {
            "primary": "none",
            "fallback_used": False,
            "visual_extraction_used": False,
        },
        "parse_quality": {
            "text_density": "low",
            "text_extraction_sparse": True,
            "image_heavy": False,
            "layout_type": "unknown",
            "confidence": 0.0,
        },
        "visual_fact_pack": _blank_visual_fact_pack(),
        "issuer_signals": _issuer_signals(doc_ref),
        "trace": {
            "content_bytes": int(content_bytes),
            "content_type": content_type,
        },
        "document_ref": dict(doc_ref),
    }


async def _parse_document_with_client(
    doc_ref: DocumentRef,
    client: httpx.AsyncClient,
) -> ParsedDocument:
    content_url = str(doc_ref.get("content_url") or doc_ref.get("pdf_url") or doc_ref.get("source_url") or "").strip()
    if not content_url:
        return _empty_failure(
            doc_ref,
            document_type="unknown",
            content_type="",
            content_bytes=0,
            error="missing_content_url",
        )

    try:
        response = await client.get(content_url)
    except Exception as exc:
        return _empty_failure(
            doc_ref,
            document_type="unknown",
            content_type="",
            content_bytes=0,
            error=f"http_error:{type(exc).__name__}:{exc}",
        )

    if response.status_code >= 400:
        return _empty_failure(
            doc_ref,
            document_type="unknown",
            content_type=str(response.headers.get("content-type", "")),
            content_bytes=len(response.content),
            error=f"http_{response.status_code}",
        )

    content_type = str(response.headers.get("content-type", "") or "")
    looks_pdf = "application/pdf" in content_type.lower() or _looks_like_pdf_url(content_url)
    if looks_pdf:
        return await _parse_pdf(doc_ref, response.content, content_type)
    return _parse_html(doc_ref, response.text, content_type, len(response.content))


async def _parse_pdf(doc_ref: DocumentRef, pdf_bytes: bytes, content_type: str) -> ParsedDocument:
    tmp_path = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        extracted = await extract_text_from_pdf(tmp_path)
        error = str(extracted.get("error", "")).strip()
        text = str(extracted.get("text", "") or "")
        metadata = dict(extracted.get("metadata", {}) or {})
        page_count = int(extracted.get("page_count", 0) or 0)
        title = str(metadata.get("title", "") or "").strip() or str(doc_ref.get("title", "")).strip()
        if error:
            return _empty_failure(
                doc_ref,
                document_type="pdf",
                content_type=content_type,
                content_bytes=len(pdf_bytes),
                error=error,
            )
        quality = _parse_quality_from_text(text, page_count=page_count, document_type="pdf")
        return {
            "doc_id": str(doc_ref.get("doc_id", "")),
            "file_name": str(doc_ref.get("doc_id", "")),
            "title": title,
            "source_url": str(doc_ref.get("source_url", "")),
            "content_url": str(doc_ref.get("content_url", "")),
            "pdf_url": str(doc_ref.get("pdf_url") or doc_ref.get("content_url") or ""),
            "domain": str(doc_ref.get("domain", "")),
            "published_at": str(doc_ref.get("published_at", "")),
            "document_type": "pdf",
            "parse_status": "ok" if text else "failed",
            "parse_errors": [] if text else ["empty_extracted_text"],
            "raw_text": text,
            "full_text": text,
            "decoded_chars": len(text),
            "page_count": page_count,
            "parse_method": {
                "primary": str(extracted.get("extraction_method", "pymupdf")),
                "fallback_used": str(extracted.get("extraction_method", "")) != "pymupdf",
                "visual_extraction_used": False,
            },
            "parse_quality": quality,
            "visual_fact_pack": _blank_visual_fact_pack(),
            "issuer_signals": _issuer_signals(doc_ref),
            "trace": {
                "content_bytes": len(pdf_bytes),
                "content_type": content_type,
            },
            "document_ref": dict(doc_ref),
        }
    finally:
        if tmp_path:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass


def _parse_html(doc_ref: DocumentRef, html: str, content_type: str, content_bytes: int) -> ParsedDocument:
    text = _html_to_text(html)
    title = str(doc_ref.get("title", "")).strip()
    quality = _parse_quality_from_text(text, page_count=0, document_type="html")
    status = "ok" if text else "failed"
    return {
        "doc_id": str(doc_ref.get("doc_id", "")),
        "file_name": str(doc_ref.get("doc_id", "")),
        "title": title,
        "source_url": str(doc_ref.get("source_url", "")),
        "content_url": str(doc_ref.get("content_url", "")),
        "pdf_url": str(doc_ref.get("pdf_url", "")),
        "domain": str(doc_ref.get("domain", "")),
        "published_at": str(doc_ref.get("published_at", "")),
        "document_type": "html",
        "parse_status": status,
        "parse_errors": [] if text else ["empty_extracted_text"],
        "raw_text": text,
        "full_text": text,
        "decoded_chars": len(text),
        "page_count": 0,
        "parse_method": {
            "primary": "html_extract",
            "fallback_used": False,
            "visual_extraction_used": False,
        },
        "parse_quality": quality,
        "visual_fact_pack": _blank_visual_fact_pack(),
        "issuer_signals": _issuer_signals(doc_ref),
        "trace": {
            "content_bytes": int(content_bytes),
            "content_type": content_type,
        },
        "document_ref": dict(doc_ref),
    }
