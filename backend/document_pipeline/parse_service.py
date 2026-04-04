from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx

from .contracts import DocumentRef, ParsedDocument
from .io import write_json
from .parser_registry import get_document_parser

AUTO_PARSER_IDS = {"auto", "smart", "smart_default", "default"}

LLAMA_FIRST_TITLE_TERMS = (
    "annual report",
    "annual report and accounts",
    "half year",
    "half-year",
    "half yearly",
    "interim report",
    "quarterly report",
    "appendix 4d",
    "appendix 4e",
    "appendix 5b",
    "10-k",
    "10-q",
    "20-f",
    "40-f",
    "financial statements",
    "feasibility study",
    "pre-feasibility",
    "pre feasibility",
    "definitive feasibility",
    "scoping study",
    "technical report",
    "resource estimate",
    "reserves statement",
)


def _looks_like_pdf_doc(ref: DocumentRef) -> bool:
    content_hint = str(ref.get("content_type_hint", "") or "").strip().lower()
    if "pdf" in content_hint:
        return True
    url = str(
        ref.get("content_url")
        or ref.get("pdf_url")
        or ref.get("source_url")
        or ""
    ).strip()
    lower = url.lower()
    if lower.endswith(".pdf") or "/asxpdf/" in lower:
        return True
    return urlparse(lower).path.endswith(".pdf")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _prefer_llamaparse_first(ref: DocumentRef) -> bool:
    title = _normalize_text(ref.get("title"))
    return any(term in title for term in LLAMA_FIRST_TITLE_TERMS)


def _needs_llamaparse_retry(ref: DocumentRef, parsed: ParsedDocument) -> bool:
    if _normalize_text(parsed.get("parse_status")) == "failed":
        return True
    chars = int(parsed.get("decoded_chars", 0) or 0)
    pages = int(parsed.get("page_count", 0) or 0)
    if chars <= 0:
        return True
    quality = dict(parsed.get("parse_quality", {}) or {})
    if bool(quality.get("text_extraction_sparse")) and pages >= 3:
        return True
    avg_chars_per_page = float(chars / max(1, pages))
    if pages >= 15 and avg_chars_per_page < 900:
        return True
    if pages >= 30 and avg_chars_per_page < 1200:
        return True
    title = _normalize_text(ref.get("title"))
    if pages >= 15 and any(term in title for term in LLAMA_FIRST_TITLE_TERMS) and avg_chars_per_page < 1600:
        return True
    return False


def _prefer_candidate(candidate: ParsedDocument, current: ParsedDocument) -> bool:
    candidate_status = _normalize_text(candidate.get("parse_status"))
    current_status = _normalize_text(current.get("parse_status"))
    if candidate_status == "ok" and current_status != "ok":
        return True
    if candidate_status != "ok":
        return False
    current_chars = int(current.get("decoded_chars", 0) or 0)
    candidate_chars = int(candidate.get("decoded_chars", 0) or 0)
    if current_chars <= 0 and candidate_chars > 0:
        return True
    if candidate_chars >= max(int(current_chars * 1.12), current_chars + 1200):
        return True
    current_quality = dict(current.get("parse_quality", {}) or {})
    candidate_quality = dict(candidate.get("parse_quality", {}) or {})
    if bool(current_quality.get("text_extraction_sparse")) and not bool(candidate_quality.get("text_extraction_sparse")):
        return True
    return False


def _annotate_route(
    parsed: ParsedDocument,
    *,
    selected_backend: str,
    attempted_backends: List[str],
    route_reason: str,
) -> ParsedDocument:
    payload = dict(parsed)
    parse_method = dict(payload.get("parse_method", {}) or {})
    parse_method["route_mode"] = "smart_default"
    parse_method["selected_backend"] = selected_backend
    parse_method["attempted_backends"] = attempted_backends
    parse_method["route_reason"] = route_reason
    payload["parse_method"] = parse_method
    return payload


async def _parse_document_smart(
    *,
    ref: DocumentRef,
    client: Optional[httpx.AsyncClient],
) -> ParsedDocument:
    local_parser = get_document_parser("local_default")
    lite_parser = get_document_parser("liteparse")
    llama_parser = get_document_parser("llamaparse")

    if not _looks_like_pdf_doc(ref):
        parsed = await local_parser.parse_document(ref, client=client)
        return _annotate_route(
            parsed,
            selected_backend="local_default",
            attempted_backends=["local_default"],
            route_reason="non_pdf_document",
        )

    attempted: List[str] = []
    if _prefer_llamaparse_first(ref):
        attempted.append("llamaparse")
        llama_doc = await llama_parser.parse_document(ref, client=client)
        if _normalize_text(llama_doc.get("parse_status")) == "ok":
            return _annotate_route(
                llama_doc,
                selected_backend="llamaparse",
                attempted_backends=attempted,
                route_reason="title_matched_hard_pdf_profile",
            )
        attempted.append("liteparse")
        lite_doc = await lite_parser.parse_document(ref, client=client)
        if _normalize_text(lite_doc.get("parse_status")) == "ok":
            return _annotate_route(
                lite_doc,
                selected_backend="liteparse",
                attempted_backends=attempted,
                route_reason="llamaparse_failed_fell_back_to_liteparse",
            )
        attempted.append("local_default")
        local_doc = await local_parser.parse_document(ref, client=client)
        return _annotate_route(
            local_doc,
            selected_backend="local_default",
            attempted_backends=attempted,
            route_reason="llamaparse_and_liteparse_failed_fell_back_to_local",
        )

    attempted.append("liteparse")
    lite_doc = await lite_parser.parse_document(ref, client=client)
    if _needs_llamaparse_retry(ref, lite_doc):
        attempted.append("llamaparse")
        llama_doc = await llama_parser.parse_document(ref, client=client)
        if _prefer_candidate(llama_doc, lite_doc):
            return _annotate_route(
                llama_doc,
                selected_backend="llamaparse",
                attempted_backends=attempted,
                route_reason="liteparse_parse_quality_triggered_llamaparse_escalation",
            )

    if _normalize_text(lite_doc.get("parse_status")) == "ok":
        return _annotate_route(
            lite_doc,
            selected_backend="liteparse",
            attempted_backends=attempted,
            route_reason="pdf_defaulted_to_liteparse",
        )

    attempted.append("local_default")
    local_doc = await local_parser.parse_document(ref, client=client)
    return _annotate_route(
        local_doc,
        selected_backend="local_default",
        attempted_backends=attempted,
        route_reason="liteparse_failed_fell_back_to_local",
    )


async def parse_documents(
    *,
    document_refs: List[DocumentRef],
    parser_id: str = "smart_default",
    out_dir: Optional[Path] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> List[ParsedDocument]:
    parser_key = str(parser_id or "smart_default").strip().lower()
    refs = list(document_refs or [])
    parsed: List[ParsedDocument] = []
    if parser_key in AUTO_PARSER_IDS:
        for ref in refs:
            parsed.append(await _parse_document_smart(ref=ref, client=client))
        effective_parser_id = "smart_default"
    else:
        parser = get_document_parser(parser_key)
        for ref in refs:
            parsed.append(await parser.parse_document(ref, client=client))
        effective_parser_id = parser.parser_id

    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        write_json(out_dir / "document_refs.json", refs)
        write_json(
            out_dir / "parsed_documents.json",
            {
                "parser_id": effective_parser_id,
                "document_count": len(parsed),
                "documents": parsed,
            },
        )
    return parsed
