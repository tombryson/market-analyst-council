from __future__ import annotations

from typing import Any, Dict, List, Literal, TypedDict

DocumentType = Literal["pdf", "html", "other", "unknown"]
ParseStatus = Literal["ok", "partial", "failed"]
LayoutType = Literal["text_report", "slide_like", "mixed", "unknown"]
IssuerFit = Literal["match", "related_party", "unclear", "mismatch"]


class DocumentRef(TypedDict, total=False):
    doc_id: str
    title: str
    source_url: str
    content_url: str
    pdf_url: str
    content_type_hint: str
    published_at: str
    domain: str
    exchange: str
    issuer_hint: str
    ticker_hint: str
    discovery_method: str
    discovery_tier: str
    selection_bucket: str
    retrieval_meta: Dict[str, Any]


class ParsedDocument(TypedDict, total=False):
    doc_id: str
    file: str
    file_name: str
    title: str
    source_url: str
    content_url: str
    pdf_url: str
    domain: str
    published_at: str

    document_type: DocumentType
    parse_status: ParseStatus
    parse_errors: List[str]

    raw_text: str
    full_text: str
    decoded_chars: int
    page_count: int

    parse_method: Dict[str, Any]
    parse_quality: Dict[str, Any]
    visual_fact_pack: Dict[str, Any]
    issuer_signals: Dict[str, Any]
    trace: Dict[str, Any]

    document_ref: DocumentRef
