from __future__ import annotations

from typing import Optional

import httpx
from liteparse import LiteParse
from liteparse.types import ParseError as LiteParseError

from ...config import LITEPARSE_OCR_ENABLED, LITEPARSE_TIMEOUT_SECONDS
from ..contracts import DocumentRef, ParsedDocument
from ..parser_base import DocumentParser
from .local_parser import (
    _blank_visual_fact_pack,
    _empty_failure,
    _issuer_signals,
    _looks_like_pdf_url,
    _parse_html,
    _parse_quality_from_text,
)


class LiteParseDocumentParser(DocumentParser):
    @property
    def parser_id(self) -> str:
        return "liteparse"

    async def parse_document(
        self,
        doc_ref: DocumentRef,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> ParsedDocument:
        own_client = client is None
        active_client = client or httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(LITEPARSE_TIMEOUT_SECONDS, connect=20.0),
        )
        try:
            return await _parse_document_with_client(doc_ref, active_client)
        finally:
            if own_client:
                await active_client.aclose()


async def _parse_document_with_client(
    doc_ref: DocumentRef,
    client: httpx.AsyncClient,
) -> ParsedDocument:
    content_url = str(
        doc_ref.get("content_url")
        or doc_ref.get("pdf_url")
        or doc_ref.get("source_url")
        or ""
    ).strip()
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

    content_type = str(response.headers.get("content-type", "") or "")
    content_bytes = response.content
    if response.status_code >= 400:
        return _empty_failure(
            doc_ref,
            document_type="unknown",
            content_type=content_type,
            content_bytes=len(content_bytes),
            error=f"http_{response.status_code}",
        )

    looks_pdf = "application/pdf" in content_type.lower() or _looks_like_pdf_url(content_url)
    if not looks_pdf and "html" in content_type.lower():
        return _parse_html(doc_ref, response.text, content_type, len(content_bytes))

    parser = LiteParse()
    try:
        result = parser.parse(
            content_bytes,
            ocr_enabled=LITEPARSE_OCR_ENABLED,
            timeout=LITEPARSE_TIMEOUT_SECONDS,
        )
    except LiteParseError as exc:
        return _empty_failure(
            doc_ref,
            document_type="pdf" if looks_pdf else "other",
            content_type=content_type,
            content_bytes=len(content_bytes),
            error=f"liteparse_error:{exc}",
        )
    except Exception as exc:
        return _empty_failure(
            doc_ref,
            document_type="pdf" if looks_pdf else "other",
            content_type=content_type,
            content_bytes=len(content_bytes),
            error=f"liteparse_error:{type(exc).__name__}:{exc}",
        )

    text = str(getattr(result, "text", "") or "")
    pages = getattr(result, "pages", None) or []
    page_count = len(pages)
    quality = _parse_quality_from_text(
        text,
        page_count=page_count,
        document_type="pdf" if looks_pdf else "other",
    )
    status = "ok" if text else "failed"
    return {
        "doc_id": str(doc_ref.get("doc_id", "")),
        "file_name": str(doc_ref.get("doc_id", "")),
        "title": str(doc_ref.get("title", "")),
        "source_url": str(doc_ref.get("source_url", "")),
        "content_url": str(doc_ref.get("content_url", "")),
        "pdf_url": str(doc_ref.get("pdf_url") or doc_ref.get("content_url") or ""),
        "domain": str(doc_ref.get("domain", "")),
        "published_at": str(doc_ref.get("published_at", "")),
        "document_type": "pdf" if looks_pdf else "other",
        "parse_status": status,
        "parse_errors": [] if text else ["empty_liteparse_text"],
        "raw_text": text,
        "full_text": text,
        "decoded_chars": len(text),
        "page_count": page_count,
        "parse_method": {
            "primary": "liteparse",
            "fallback_used": False,
            "visual_extraction_used": False,
            "ocr_enabled": LITEPARSE_OCR_ENABLED,
        },
        "parse_quality": quality,
        "visual_fact_pack": _blank_visual_fact_pack(),
        "issuer_signals": _issuer_signals(doc_ref),
        "trace": {
            "content_bytes": len(content_bytes),
            "content_type": content_type,
        },
        "document_ref": dict(doc_ref),
    }
