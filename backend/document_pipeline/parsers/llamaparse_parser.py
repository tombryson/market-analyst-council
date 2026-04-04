from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import httpx

from ...config import (
    LLAMAPARSE_API_KEY,
    LLAMAPARSE_API_URL,
    LLAMAPARSE_COST_OPTIMIZER_ENABLED,
    LLAMAPARSE_POLL_INTERVAL_SECONDS,
    LLAMAPARSE_TIMEOUT_SECONDS,
    LLAMAPARSE_TIER,
    LLAMAPARSE_UPLOAD_URL,
    LLAMAPARSE_VERSION,
)
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


class LlamaParseDocumentParser(DocumentParser):
    @property
    def parser_id(self) -> str:
        return "llamaparse"

    async def parse_document(
        self,
        doc_ref: DocumentRef,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> ParsedDocument:
        if not LLAMAPARSE_API_KEY:
            return _empty_failure(
                doc_ref,
                document_type="unknown",
                content_type="",
                content_bytes=0,
                error="missing_llamaparse_api_key",
            )

        own_client = client is None
        active_client = client or httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(LLAMAPARSE_TIMEOUT_SECONDS, connect=20.0),
        )
        try:
            return await _parse_document_with_client(doc_ref, active_client)
        finally:
            if own_client:
                await active_client.aclose()


def _infer_filename(doc_ref: DocumentRef, content_type: str) -> str:
    content_url = str(
        doc_ref.get("content_url")
        or doc_ref.get("pdf_url")
        or doc_ref.get("source_url")
        or ""
    ).strip()
    path_name = Path(urlparse(content_url).path).name
    if path_name:
        return path_name
    suffix = ".pdf" if "pdf" in str(content_type or "").lower() else ".bin"
    doc_id = str(doc_ref.get("doc_id", "") or "document").strip() or "document"
    return f"{doc_id}{suffix}"


def _configuration_payload() -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "tier": LLAMAPARSE_TIER or "agentic",
        "version": LLAMAPARSE_VERSION or "latest",
    }
    if LLAMAPARSE_COST_OPTIMIZER_ENABLED:
        payload["processing_options"] = {
            "cost_optimizer": {"enable": True},
        }
    return payload


def _extract_job_id(payload: Dict[str, Any]) -> str:
    candidates = [
        payload.get("job_id"),
        payload.get("id"),
        (payload.get("job") or {}).get("id") if isinstance(payload.get("job"), dict) else None,
        (payload.get("data") or {}).get("id") if isinstance(payload.get("data"), dict) else None,
    ]
    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value
    return ""


def _extract_text_blob(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        pages = value.get("pages")
        if isinstance(pages, list):
            ordered_pages = []
            for page in pages:
                if not isinstance(page, dict):
                    continue
                page_number = page.get("page_number")
                try:
                    sort_key = int(page_number or 0)
                except (TypeError, ValueError):
                    sort_key = 0
                blob = ""
                for key in ("markdown", "text", "content", "value"):
                    candidate = page.get(key)
                    if isinstance(candidate, str) and candidate.strip():
                        blob = candidate.strip()
                        break
                if blob:
                    ordered_pages.append((sort_key, blob))
            if ordered_pages:
                ordered_pages.sort(key=lambda item: item[0])
                return "\n\n".join(blob for _, blob in ordered_pages).strip()
        for key in ("content", "text", "markdown", "value"):
            blob = value.get(key)
            if isinstance(blob, str) and blob.strip():
                return blob.strip()
    return ""


def _extract_page_count(payload: Dict[str, Any]) -> int:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        pages = metadata.get("pages")
        if isinstance(pages, list):
            return len(pages)
        for key in ("page_count", "pages_count", "num_pages"):
            try:
                value = int(metadata.get(key, 0) or 0)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                return value
    return 0


def _extract_cost_optimizer_stats(payload: Dict[str, Any]) -> Dict[str, int]:
    metadata = payload.get("metadata")
    optimized = 0
    total = 0
    if isinstance(metadata, dict):
        pages = metadata.get("pages")
        if isinstance(pages, list):
            total = len(pages)
            optimized = sum(
                1
                for page in pages
                if isinstance(page, dict) and bool(page.get("cost_optimized"))
            )
    return {
        "total_pages": total,
        "cost_optimized_pages": optimized,
    }


async def _poll_until_complete(client: httpx.AsyncClient, job_id: str) -> Dict[str, Any]:
    deadline = asyncio.get_event_loop().time() + max(10.0, float(LLAMAPARSE_TIMEOUT_SECONDS))
    url = f"{LLAMAPARSE_API_URL.rstrip('/')}/{job_id}"
    params = {"expand": "markdown,text,metadata"}
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {LLAMAPARSE_API_KEY}",
    }

    last_payload: Dict[str, Any] = {}
    while True:
        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()
        payload = response.json()
        last_payload = payload if isinstance(payload, dict) else {}
        job = last_payload.get("job")
        status = ""
        if isinstance(job, dict):
            status = str(job.get("status", "") or "").strip().upper()
        if status == "COMPLETED":
            return last_payload
        if status in {"FAILED", "CANCELLED"}:
            message = ""
            if isinstance(job, dict):
                message = str(job.get("error_message", "") or "").strip()
            raise RuntimeError(message or f"llamaparse_job_{status.lower()}")
        if asyncio.get_event_loop().time() >= deadline:
            raise TimeoutError(f"llamaparse_poll_timeout:{job_id}")
        await asyncio.sleep(max(0.5, float(LLAMAPARSE_POLL_INTERVAL_SECONDS)))


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
        source_response = await client.get(content_url)
    except Exception as exc:
        return _empty_failure(
            doc_ref,
            document_type="unknown",
            content_type="",
            content_bytes=0,
            error=f"http_error:{type(exc).__name__}:{exc}",
        )

    content_type = str(source_response.headers.get("content-type", "") or "")
    content_bytes = source_response.content
    if source_response.status_code >= 400:
        return _empty_failure(
            doc_ref,
            document_type="unknown",
            content_type=content_type,
            content_bytes=len(content_bytes),
            error=f"http_{source_response.status_code}",
        )

    looks_pdf = "application/pdf" in content_type.lower() or _looks_like_pdf_url(content_url)
    if not looks_pdf and "html" in content_type.lower():
        return _parse_html(doc_ref, source_response.text, content_type, len(content_bytes))

    filename = _infer_filename(doc_ref, content_type)
    config_json = json.dumps(_configuration_payload())
    upload_headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {LLAMAPARSE_API_KEY}",
    }

    try:
        upload_response = await client.post(
            LLAMAPARSE_UPLOAD_URL,
            headers=upload_headers,
            data={"configuration": config_json},
            files={
                "file": (
                    filename,
                    content_bytes,
                    content_type or "application/octet-stream",
                ),
            },
        )
        upload_response.raise_for_status()
        upload_payload = upload_response.json()
        if not isinstance(upload_payload, dict):
            raise RuntimeError("llamaparse_invalid_upload_response")
        job_id = _extract_job_id(upload_payload)
        if not job_id:
            raise RuntimeError("llamaparse_missing_job_id")
        result_payload = await _poll_until_complete(client, job_id)
    except Exception as exc:
        return _empty_failure(
            doc_ref,
            document_type="pdf" if looks_pdf else "other",
            content_type=content_type,
            content_bytes=len(content_bytes),
            error=f"llamaparse_error:{type(exc).__name__}:{exc}",
        )

    markdown = _extract_text_blob(result_payload.get("markdown"))
    text = _extract_text_blob(result_payload.get("text"))
    full_text = markdown or text
    raw_text = text or full_text
    page_count = _extract_page_count(result_payload)
    quality = _parse_quality_from_text(
        full_text,
        page_count=page_count,
        document_type="pdf" if looks_pdf else "other",
    )
    status = "ok" if full_text else "failed"
    stats = _extract_cost_optimizer_stats(result_payload)

    job = result_payload.get("job")
    job_status = ""
    if isinstance(job, dict):
        job_status = str(job.get("status", "") or "").strip().upper()

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
        "parse_errors": [] if full_text else ["empty_llamaparse_text"],
        "raw_text": raw_text,
        "full_text": full_text,
        "decoded_chars": len(full_text),
        "page_count": page_count,
        "parse_method": {
            "primary": "llamaparse_v2",
            "fallback_used": False,
            "visual_extraction_used": False,
            "provider_job_id": _extract_job_id(result_payload),
            "provider_status": job_status,
            "tier": LLAMAPARSE_TIER,
            "version": LLAMAPARSE_VERSION,
            "cost_optimizer_enabled": LLAMAPARSE_COST_OPTIMIZER_ENABLED,
            **stats,
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
