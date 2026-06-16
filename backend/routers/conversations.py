"""Conversation CRUD and council streaming endpoints."""

import asyncio
import json
import logging
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .. import storage
from ..config import (
    CHAIRMAN_MODEL,
    COUNCIL_EXECUTION_MODE,
    ENABLE_MARKET_FACTS_PREPASS,
    ENABLE_RESEARCH_SERVICE,
    PROGRESS_LOGGING,
    RESEARCH_DEPTH,
    STAGE2_RECONCILIATION_ENABLED,
    STAGE2_REVISION_PASS_ENABLED,
)
from ..council import (
    _is_openrouter_compatible_model,
    apply_stage2_revision_deltas,
    calculate_aggregate_rankings,
    generate_conversation_title,
    run_full_council,
    stage1_collect_perplexity_research_responses,
    stage1_collect_responses,
    stage2_collect_rankings,
    stage2_collect_reconciliation,
    stage2_collect_revision_deltas,
    stage3_synthesize_final,
)
from ..jobs.prepass import _prepare_stage1_authoritative_prepass_bundle
from ..jobs.state import (
    SUPPLEMENTARY_DOC_ALLOWED_EXTENSIONS,
    SUPPLEMENTARY_DOC_MAX_CHARS,
    research_service,
)
from ..market_facts import (
    format_market_facts_query_prefix,
    gather_market_facts_prepass,
    prepend_market_facts_to_query,
)
from ..pdf_processor import format_pdf_context_for_prompt, process_pdf_attachment, save_attachment
from ..research import format_evidence_pack_for_prompt
from ..search import (
    extract_ticker_from_query,
    format_search_results_for_prompt,
    perform_financial_search,
    perform_search,
    reformulate_query_for_search,
)
from ..utils import _ensure_system_enabled

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/conversations")


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class CreateConversationRequest(BaseModel):
    """Request to create a new conversation."""
    pass


class SendMessageRequest(BaseModel):
    """Request to send a message in a conversation."""
    content: str


class ConversationMetadata(BaseModel):
    """Conversation metadata for list view."""
    id: str
    created_at: str
    title: str
    message_count: int


class Conversation(BaseModel):
    """Full conversation with all messages."""
    id: str
    created_at: str
    title: str
    messages: List[Dict[str, Any]]


# ---------------------------------------------------------------------------
# Helper: supplementary document context
# ---------------------------------------------------------------------------

async def _build_supplementary_document_context(
    supplementary_file: Optional[UploadFile],
    *,
    conversation_id: str,
    message_id: str,
) -> str:
    """Build one bounded supplementary text block from an optional user upload."""
    if supplementary_file is None:
        return ""

    filename = str(getattr(supplementary_file, "filename", "") or "").strip() or "supplementary_document"
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPLEMENTARY_DOC_ALLOWED_EXTENSIONS:
        return ""

    try:
        file_content = await supplementary_file.read()
    except Exception:
        return ""
    if not file_content:
        return ""

    extracted_text = ""
    if suffix == ".pdf":
        try:
            storage_path = await save_attachment(file_content, conversation_id, message_id, filename)
            processed = await process_pdf_attachment(storage_path, filename)
            if processed.get("status") == "success":
                extracted_text = str(
                    processed.get("full_text")
                    or processed.get("summary")
                    or ""
                ).strip()
        except Exception:
            extracted_text = ""
    else:
        try:
            text = file_content.decode("utf-8", errors="replace").strip()
            if suffix == ".json":
                try:
                    parsed = json.loads(text)
                    text = json.dumps(parsed, indent=2, ensure_ascii=False)
                except Exception:
                    pass
            extracted_text = text
        except Exception:
            extracted_text = ""

    if not extracted_text:
        return ""

    bounded_text = extracted_text[:SUPPLEMENTARY_DOC_MAX_CHARS].strip()
    if len(extracted_text) > SUPPLEMENTARY_DOC_MAX_CHARS:
        bounded_text += "\n\n[Supplementary document truncated]"

    return (
        "SUPPLEMENTARY USER-PROVIDED DOCUMENT\n"
        "Use this as optional additional context only.\n"
        "Do not treat it as higher priority than filings, market facts, or company announcements.\n"
        f"Filename: {filename}\n\n"
        f"{bounded_text}"
    )


# ---------------------------------------------------------------------------
# Helper: context builders + normalizers
# ---------------------------------------------------------------------------

def build_enhanced_context(
    user_query: str,
    search_results: Optional[Dict[str, Any]],
    attachments_processed: List[Dict[str, Any]],
    template_context: str = "",
    market_facts: Optional[Dict[str, Any]] = None,
    supplementary_context: str = "",
) -> str:
    """Build enhanced query context with search results and PDF content."""
    parts = [f"User Question: {user_query}"]

    if template_context:
        parts.append("\n\n--- ANALYSIS FRAMEWORK ---")
        parts.append(template_context)

    market_facts_text = format_market_facts_query_prefix(market_facts)
    if market_facts_text:
        parts.append("\n\n--- MARKET FACTS PREPASS ---")
        parts.append(market_facts_text)

    if search_results:
        parts.append("\n\n--- INTERNET SEARCH RESULTS ---")
        formatted = format_search_results_for_prompt(search_results)
        parts.append(formatted)
        logger.debug("Search context added to prompt: %d chars", len(formatted))

        evidence_pack = search_results.get("evidence_pack")
        if evidence_pack:
            evidence_text = format_evidence_pack_for_prompt(evidence_pack)
            if evidence_text:
                parts.append("\n\n--- NORMALIZED EVIDENCE PACK ---")
                parts.append(evidence_text)
                logger.debug("Evidence pack context added: %d chars", len(evidence_text))

    if attachments_processed:
        pdf_context = format_pdf_context_for_prompt(attachments_processed)
        if pdf_context:
            parts.append("\n\n--- ATTACHED DOCUMENTS ---")
            parts.append(pdf_context)

    if supplementary_context:
        parts.append("\n\n--- SUPPLEMENTARY USER DOCUMENT ---")
        parts.append(str(supplementary_context).strip())

    enhanced = "\n".join(parts)
    logger.debug("Enhanced context total length: %d chars", len(enhanced))
    return enhanced


def build_template_context_for_prompt(
    template_id: str,
    template_data: Dict[str, Any],
    company_name: Optional[str] = None,
    company_type: Optional[str] = None,
    exchange: Optional[str] = None,
    exchange_assumptions: str = "",
    max_rubric_chars: int = 0,
) -> str:
    """Build concise template context for Stage 1/2 prompts."""
    if not template_data:
        return ""

    rubric = (template_data.get("stage1_focus_prompt") or template_data.get("rubric") or "").strip()
    if rubric:
        try:
            from ..template_loader import get_template_loader

            loader = get_template_loader()
            rubric = loader.apply_prompt_substitutions(
                rubric,
                company_name=company_name,
                exchange=exchange,
            )
        except Exception:
            if company_name:
                rubric = rubric.replace("[Company Name]", company_name)
            if exchange:
                rubric = rubric.replace("[Exchange]", exchange.upper())
    if max_rubric_chars > 0:
        rubric = rubric[:max_rubric_chars]

    lines = [
        f"Template ID: {template_id}",
        f"Template Name: {template_data.get('name', template_id)}",
    ]

    if company_name:
        lines.append(f"Company Name: {company_name}")
    if company_type:
        lines.append(f"Company Type: {company_type}")
    if exchange:
        lines.append(f"Exchange: {exchange}")

    description = (template_data.get("description") or "").strip()
    if description:
        lines.append(f"Template Description: {description}")

    if exchange_assumptions:
        lines.append("Exchange Assumptions:")
        lines.append(exchange_assumptions.strip())

    if rubric:
        lines.append("Rubric:")
        lines.append(rubric)

    return "\n".join(lines).strip()


def normalize_council_mode(mode: Optional[str]) -> str:
    """Normalize council mode aliases to supported values."""
    normalized = (mode or "local").strip().lower()
    if normalized in {
        "perplexity",
        "perplexity_emulated",
        "perplexity_council_emulated",
        "hybrid_mixed",
        "perplexity_mixed",
    }:
        return "perplexity_emulated"
    return "local"


def normalize_research_depth(depth: Optional[str]) -> str:
    """Normalize retrieval depth to supported values."""
    normalized = (depth or "basic").strip().lower()
    return "deep" if normalized == "deep" else "basic"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("", response_model=List[ConversationMetadata])
async def list_conversations():
    """List all conversations (metadata only)."""
    return storage.list_conversations()


@router.post("", response_model=Conversation)
async def create_conversation(request: CreateConversationRequest):
    """Create a new conversation."""
    _ensure_system_enabled()
    conversation_id = str(uuid.uuid4())
    conversation = storage.create_conversation(conversation_id)
    return conversation


@router.get("/{conversation_id}", response_model=Conversation)
async def get_conversation(conversation_id: str):
    """Get a specific conversation with all its messages."""
    conversation = storage.get_conversation(conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conversation


@router.post("/{conversation_id}/message")
async def send_message(conversation_id: str, request: SendMessageRequest):
    """
    Send a message and run the 3-stage council process.
    Returns the complete response with all stages.
    """
    _ensure_system_enabled()
    conversation = storage.get_conversation(conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    is_first_message = len(conversation["messages"]) == 0
    storage.add_user_message(conversation_id, request.content)

    if is_first_message:
        title = await generate_conversation_title(request.content)
        storage.update_conversation_title(conversation_id, title)

    stage1_results, stage2_results, stage3_result, metadata = await run_full_council(
        request.content
    )

    storage.add_assistant_message_with_metadata(
        conversation_id,
        stage1_results,
        stage2_results,
        stage3_result,
        search_results=None,
        attachments_processed=[],
    )

    return {
        "stage1": stage1_results,
        "stage2": stage2_results,
        "stage3": stage3_result,
        "metadata": metadata,
    }


@router.post("/{conversation_id}/message/stream")
async def send_message_stream(
    conversation_id: str,
    content: str = Form(...),
    enable_search: bool = Form(True),
    ticker: str = Form(None),
    exchange: str = Form(None),
    research_depth: str = Form(None),
    council_mode: str = Form(None),
    template_id: str = Form(None),
    company_type: str = Form(None),
    files: List[UploadFile] = File(None),
    supplementary_file: UploadFile = File(None),
):
    """
    Send a message and stream the 3-stage council process.
    Supports optional PDF attachments and internet search.
    Returns Server-Sent Events as each stage completes.
    """
    _ensure_system_enabled()
    conversation = storage.get_conversation(conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    is_first_message = len(conversation["messages"]) == 0
    selected_council_mode = normalize_council_mode(council_mode or COUNCIL_EXECUTION_MODE)
    selected_research_depth = normalize_research_depth(research_depth or RESEARCH_DEPTH)
    effective_enable_search_for_storage = enable_search or selected_council_mode == "perplexity_emulated"

    storage.add_user_message_with_metadata(
        conversation_id,
        content,
        effective_enable_search_for_storage,
        [],
        selected_council_mode,
        template_id=template_id or None,
        company_name=None,
        company_type=company_type or None,
        exchange=exchange or None,
    )

    async def event_generator():
        try:
            yield (
                "data: "
                f"{json.dumps({'type': 'council_mode', 'data': {'mode': selected_council_mode, 'research_depth': selected_research_depth}})}\n\n"
            )

            storage.add_assistant_placeholder_message(
                conversation_id,
                metadata={
                    "council_mode": selected_council_mode,
                    "research_depth": selected_research_depth,
                    "template_id": template_id or None,
                    "company_type": company_type or None,
                    "exchange": exchange or None,
                },
            )

            def _persist_assistant_patch(patch: Dict[str, Any]) -> None:
                try:
                    storage.update_last_assistant_message(conversation_id, patch)
                except Exception:
                    pass

            message_id = str(uuid.uuid4())

            attachments_metadata = []
            attachments_processed = []

            if files:
                yield f"data: {json.dumps({'type': 'attachments_start', 'count': len(files)})}\n\n"
                _persist_assistant_patch({"loading": {"attachments": True, "stage1Message": "Processing attachments..."}})

                for file in files:
                    if not file.filename.endswith('.pdf'):
                        continue

                    file_content = await file.read()
                    storage_path = await save_attachment(
                        file_content, conversation_id, message_id, file.filename
                    )

                    processed = await process_pdf_attachment(storage_path, file.filename)
                    attachments_processed.append(processed)

                    attachments_metadata.append({
                        "filename": file.filename,
                        "size": len(file_content),
                        "uploaded_at": asyncio.get_event_loop().time(),
                        "storage_path": storage_path,
                        "page_count": processed.get('page_count', 0),
                        "processing_status": processed.get('status', 'failed')
                    })

                yield f"data: {json.dumps({'type': 'attachments_complete', 'data': attachments_processed})}\n\n"
                _persist_assistant_patch(
                    {
                        "attachments_processed": attachments_processed,
                        "loading": {"attachments": False},
                    }
                )

            supplementary_context = await _build_supplementary_document_context(
                supplementary_file,
                conversation_id=conversation_id,
                message_id=message_id,
            )

            use_perplexity_emulated_stage1 = selected_council_mode == "perplexity_emulated"
            effective_enable_search = enable_search or use_perplexity_emulated_stage1
            search_results = None
            search_ticker = ticker

            if not search_ticker:
                search_ticker = extract_ticker_from_query(content)

            from ..template_loader import get_template_loader, resolve_template_selection
            loader = get_template_loader()
            auto_company_name = loader.infer_company_name(content, ticker=search_ticker)
            template_selection = resolve_template_selection(
                user_query=content,
                ticker=search_ticker,
                explicit_template_id=template_id,
                company_type=(company_type or None),
                exchange=exchange,
            )
            selected_template_id = template_selection["template_id"]
            selected_company_type = template_selection.get("company_type")
            selected_company_name = template_selection.get("company_name")
            selected_exchange = template_selection.get("exchange")
            template_selection_source = template_selection.get("selection_source", "auto")
            exchange_selection_source = template_selection.get("exchange_selection_source", "auto_exchange")
            use_structured_analysis = loader.is_structured_template(selected_template_id)
            template_data = loader.get_template(selected_template_id) or {}
            stage1_research_brief = loader.get_stage1_research_brief(
                selected_template_id,
                selected_company_type,
                selected_exchange,
                selected_company_name,
                include_rubric=False,
            )
            template_context = build_template_context_for_prompt(
                selected_template_id,
                template_data,
                selected_company_name,
                selected_company_type,
                selected_exchange,
                template_selection.get("exchange_assumptions", ""),
            )

            logger.info(
                "Template selection: template=%s company=%s company_type=%s "
                "exchange=%s source=%s exchange_source=%s structured=%s",
                selected_template_id, selected_company_name, selected_company_type,
                selected_exchange, template_selection_source, exchange_selection_source,
                use_structured_analysis,
            )
            yield (
                "data: "
                f"{json.dumps({'type': 'template_selected', 'data': template_selection})}\n\n"
            )
            _persist_assistant_patch(
                {
                    "metadata": {
                        "template_id": selected_template_id,
                        "template_name": template_selection.get("template_name"),
                        "company_name": selected_company_name,
                        "company_type": selected_company_type,
                        "template_selection_source": template_selection_source,
                        "exchange": selected_exchange,
                        "exchange_selection_source": exchange_selection_source,
                    }
                }
            )

            if use_structured_analysis and ENABLE_MARKET_FACTS_PREPASS and not search_ticker:
                msg = (
                    "Ticker unresolved for structured analysis while market-facts prepass is enabled. "
                    "Provide EXCHANGE:SYMBOL (e.g., ASX:BRK)."
                )
                yield f"data: {json.dumps({'type': 'error', 'error': msg})}\n\n"
                return

            market_facts = None
            if ENABLE_MARKET_FACTS_PREPASS and search_ticker:
                yield f"data: {json.dumps({'type': 'market_facts_start'})}\n\n"
                _persist_assistant_patch({"loading": {"stage1": True, "stage1Message": "Gathering market facts..."}})
                market_facts = await gather_market_facts_prepass(
                    ticker=search_ticker,
                    company_name=selected_company_name,
                    exchange=selected_exchange,
                    template_id=selected_template_id,
                    company_type=selected_company_type,
                )
                yield f"data: {json.dumps({'type': 'market_facts_complete', 'data': market_facts})}\n\n"
                _persist_assistant_patch({"loading": {"stage1Message": "Market facts prepared"}})
                if use_structured_analysis:
                    market_status = str((market_facts or {}).get("status") or "").strip().lower()
                    market_prefix = format_market_facts_query_prefix(market_facts)
                    if market_status in {"skipped", "error", "empty"} or not market_prefix:
                        msg = (
                            "Market-facts prepass failed for structured analysis. "
                            f"status={market_status or 'unknown'} "
                            f"reason={str((market_facts or {}).get('reason') or '').strip() or 'n/a'}"
                        )
                        yield f"data: {json.dumps({'type': 'error', 'error': msg})}\n\n"
                        return

            if effective_enable_search and not use_perplexity_emulated_stage1:
                yield f"data: {json.dumps({'type': 'search_start'})}\n\n"
                _persist_assistant_patch({"loading": {"search": True, "stage1Message": "Gathering supporting sources..."}})

                try:
                    if ENABLE_RESEARCH_SERVICE:
                        yield f"data: {json.dumps({'type': 'evidence_start'})}\n\n"
                        _persist_assistant_patch({"loading": {"evidence": True}})

                        search_results = await research_service.gather_research(
                            user_query=content,
                            ticker=search_ticker,
                        )

                        evidence_pack = search_results.get("evidence_pack")
                        if evidence_pack:
                            yield f"data: {json.dumps({'type': 'evidence_complete', 'data': evidence_pack})}\n\n"
                            _persist_assistant_patch(
                                {
                                    "evidence_pack": evidence_pack,
                                    "loading": {"evidence": False},
                                }
                            )

                        logger.info(
                            "Research service complete (provider=%s): %d results",
                            search_results.get("provider", "unknown"),
                            search_results.get("result_count", 0),
                        )
                    else:
                        if search_ticker:
                            logger.info("Using ticker: %s, performing targeted financial search", search_ticker)
                            search_results = await perform_financial_search(search_ticker)
                            logger.info(
                                "Financial search complete: %d results, %d PDFs downloaded",
                                search_results.get("result_count", 0),
                                len(search_results.get("pdfs_processed", [])),
                            )
                        else:
                            search_query = await reformulate_query_for_search(content)
                            logger.info(
                                "Search query reformulated: '%s...' -> '%s'",
                                content[:50], search_query,
                            )
                            search_results = await perform_search(search_query)
                            logger.info("Search results: %d results", search_results.get("result_count", 0))

                    yield f"data: {json.dumps({'type': 'search_complete', 'data': search_results})}\n\n"
                    _persist_assistant_patch(
                        {
                            "search_results": search_results,
                            "loading": {"search": False},
                        }
                    )
                except Exception as e:
                    import logging as _logging
                    _logging.getLogger(__name__).error("Search error: %s", e, exc_info=True)
                    search_results = {
                        "error": f"Search failed: {str(e)}",
                        "results": [],
                        "result_count": 0
                    }
                    yield f"data: {json.dumps({'type': 'search_complete', 'data': search_results})}\n\n"
                    _persist_assistant_patch(
                        {
                            "search_results": search_results,
                            "loading": {"search": False, "evidence": False},
                        }
                    )

            all_attachments = attachments_processed.copy()
            if search_results and search_results.get("pdfs_processed"):
                all_attachments.extend(search_results["pdfs_processed"])

            if attachments_metadata:
                _persist_assistant_patch(
                    {
                        "metadata": {
                            "attachments": attachments_metadata,
                        }
                    }
                )

            title_task = None
            if is_first_message:
                title_task = asyncio.create_task(generate_conversation_title(content))

            yield f"data: {json.dumps({'type': 'stage1_start'})}\n\n"
            _persist_assistant_patch(
                {
                    "status": "running",
                    "loading": {
                        "stage1": True,
                        "stage1Progress": 0,
                        "stage1Completed": 0,
                        "stage1Total": 0,
                        "stage1Model": "",
                        "stage1Message": "Stage 1 starting...",
                    }
                }
            )
            stage2_ranking_models = None
            stage3_chairman_model = None
            prepass_source_rows: List[Dict[str, Any]] = []
            prepass_bundle_meta: Dict[str, Any] = {}
            stage1_progress_queue: asyncio.Queue = asyncio.Queue()

            def _push_stage1_progress(payload: Dict[str, Any]) -> None:
                try:
                    stage1_progress_queue.put_nowait(payload)
                except Exception:
                    pass

            async def _drain_stage1_progress(stage1_task: asyncio.Task):
                while True:
                    if stage1_task.done() and stage1_progress_queue.empty():
                        break
                    try:
                        payload = await asyncio.wait_for(stage1_progress_queue.get(), timeout=0.25)
                    except asyncio.TimeoutError:
                        continue
                    if payload.get("type") == "stage1_progress":
                        data = payload.get("data") or {}
                        try:
                            progress_pct = int(data.get("progress_pct") or 0)
                        except Exception:
                            progress_pct = 0
                        try:
                            completed = int(data.get("completed") or 0)
                        except Exception:
                            completed = 0
                        try:
                            total = int(data.get("total") or 0)
                        except Exception:
                            total = 0
                        _persist_assistant_patch(
                            {
                                "loading": {
                                    "stage1": True,
                                    "stage1Progress": progress_pct,
                                    "stage1Completed": completed,
                                    "stage1Total": total,
                                    "stage1Model": str(data.get("model") or ""),
                                    "stage1Message": str(data.get("stage_message") or ""),
                                }
                            }
                        )
                    yield f"data: {json.dumps(payload)}\n\n"

            if use_perplexity_emulated_stage1:
                yield f"data: {json.dumps({'type': 'search_start'})}\n\n"
                yield f"data: {json.dumps({'type': 'evidence_start'})}\n\n"

                attachment_context = format_pdf_context_for_prompt(all_attachments)
                if supplementary_context:
                    attachment_context = (
                        f"{attachment_context}\n\n{supplementary_context}".strip()
                        if attachment_context
                        else supplementary_context
                    )
                stage1_effective_research_brief = stage1_research_brief
                stage1_query_core = (content or "").strip()
                if use_structured_analysis:
                    rendered_stage1_prompt = loader.render_template_rubric(
                        selected_template_id,
                        company_name=selected_company_name,
                        exchange=selected_exchange,
                    ).strip()
                    if not rendered_stage1_prompt:
                        rendered_stage1_prompt = loader.render_stage1_query_prompt(
                            selected_template_id,
                            company_name=selected_company_name,
                            exchange=selected_exchange,
                        ).strip()
                    if rendered_stage1_prompt:
                        stage1_query_core = rendered_stage1_prompt

                stage1_effective_query = prepend_market_facts_to_query(
                    stage1_query_core,
                    market_facts,
                )
                if PROGRESS_LOGGING:
                    logger.debug(
                        "Stage1 prompt assembly: structured_template=%s template_id=%s "
                        "market_facts_prefixed=%s query_core_chars=%d query_sent_chars=%d brief_chars=%d",
                        use_structured_analysis, selected_template_id, bool(market_facts),
                        len(stage1_query_core), len(stage1_effective_query), len(stage1_effective_research_brief),
                    )
                yield f"data: {json.dumps({'type': 'prepass_start'})}\n\n"
                try:
                    exchange_retrieval_params = loader.get_exchange_retrieval_params(
                        selected_exchange
                    )
                    prepass_source_rows, prepass_bundle_meta = await _prepare_stage1_authoritative_prepass_bundle(
                        ticker=search_ticker or "",
                        query_hint=stage1_effective_query,
                        exchange=selected_exchange or "",
                        exchange_retrieval_params=exchange_retrieval_params,
                        company_name=selected_company_name,
                        template_id=selected_template_id or "",
                    )
                except Exception as prepass_exc:
                    msg = (
                        "Authoritative prepass failed; Stage 1 retrieval fallback is disabled. "
                        f"error={str(prepass_exc)}"
                    )
                    yield f"data: {json.dumps({'type': 'error', 'error': msg})}\n\n"
                    return
                if not prepass_source_rows:
                    msg = (
                        "Authoritative prepass produced zero source rows; "
                        "Stage 1 retrieval fallback is disabled."
                    )
                    yield f"data: {json.dumps({'type': 'error', 'error': msg})}\n\n"
                    return
                yield f"data: {json.dumps({'type': 'prepass_complete', 'data': prepass_bundle_meta})}\n\n"
                stage1_task = asyncio.create_task(
                    stage1_collect_perplexity_research_responses(
                        user_query=stage1_effective_query,
                        ticker=search_ticker,
                        attachment_context=attachment_context,
                        prepass_source_rows=prepass_source_rows,
                        depth=selected_research_depth,
                        research_brief=stage1_effective_research_brief,
                        template_id=selected_template_id,
                        progress_callback=_push_stage1_progress,
                    )
                )
                async for progress_event in _drain_stage1_progress(stage1_task):
                    yield progress_event
                stage1_results, emulated_metadata = await stage1_task
                if isinstance(emulated_metadata, dict):
                    emulated_metadata["stage1_prepass_bundle_meta"] = dict(prepass_bundle_meta)
                stage2_ranking_models = [
                    item.get("model")
                    for item in stage1_results
                    if item.get("model") and _is_openrouter_compatible_model(item.get("model"))
                ]
                excluded_stage2_models = [
                    item.get("model")
                    for item in stage1_results
                    if item.get("model") and not _is_openrouter_compatible_model(item.get("model"))
                ]
                if excluded_stage2_models and PROGRESS_LOGGING:
                    logger.debug(
                        "Stage2 judge-model filter excluded non-OpenRouter models: %s",
                        excluded_stage2_models,
                    )
                if stage2_ranking_models:
                    stage3_chairman_model = (
                        CHAIRMAN_MODEL
                        if CHAIRMAN_MODEL in stage2_ranking_models
                        else stage2_ranking_models[0]
                    )
                else:
                    stage3_chairman_model = CHAIRMAN_MODEL

                search_results = emulated_metadata.get("aggregated_search_results", {})
                if isinstance(search_results, dict):
                    search_meta = search_results.setdefault("metadata", {})
                    if isinstance(search_meta, dict):
                        search_meta["stage1_prepass_bundle_meta"] = dict(prepass_bundle_meta)
                if excluded_stage2_models:
                    search_meta = search_results.setdefault("metadata", {})
                    if isinstance(search_meta, dict):
                        search_meta["stage2_excluded_non_openrouter_models"] = excluded_stage2_models
                evidence_pack = search_results.get("evidence_pack")
                if evidence_pack:
                    yield f"data: {json.dumps({'type': 'evidence_complete', 'data': evidence_pack})}\n\n"
                yield f"data: {json.dumps({'type': 'search_complete', 'data': search_results})}\n\n"
            else:
                enhanced_context = build_enhanced_context(
                    content,
                    search_results,
                    all_attachments,
                    template_context=template_context,
                    market_facts=market_facts,
                    supplementary_context=supplementary_context,
                )
                stage1_task = asyncio.create_task(
                    stage1_collect_responses(
                        enhanced_context,
                        progress_callback=_push_stage1_progress,
                    )
                )
                async for progress_event in _drain_stage1_progress(stage1_task):
                    yield progress_event
                stage1_results = await stage1_task
                emulated_metadata = {}

            if not stage1_results:
                yield f"data: {json.dumps({'type': 'error', 'message': 'No Stage 1 responses were generated. Please try again.'})}\n\n"
                return

            enhanced_context = build_enhanced_context(
                content,
                search_results,
                all_attachments,
                template_context=template_context,
                market_facts=market_facts,
                supplementary_context=supplementary_context,
            )
            yield f"data: {json.dumps({'type': 'stage1_complete', 'data': stage1_results})}\n\n"
            _persist_assistant_patch(
                {
                    "stage1": stage1_results,
                    "search_results": search_results,
                    "attachments_processed": attachments_processed,
                    "loading": {
                        "search": False,
                        "evidence": False,
                        "attachments": False,
                        "stage1": False,
                        "stage1Progress": 100,
                        "stage1Message": "Stage 1 complete",
                    },
                }
            )

            yield f"data: {json.dumps({'type': 'stage2_start'})}\n\n"
            _persist_assistant_patch({"loading": {"stage2": True}})
            stage2_results, label_to_model = await stage2_collect_rankings(
                enhanced_context,
                stage1_results,
                ranking_models=stage2_ranking_models,
            )
            aggregate_rankings = calculate_aggregate_rankings(stage2_results, label_to_model)
            stage1_results_for_stage3 = stage1_results
            stage2_revision_summary: Dict[str, Any] = {"enabled": False}
            stage2_revision_results: List[Dict[str, Any]] = []
            if STAGE2_REVISION_PASS_ENABLED:
                yield f"data: {json.dumps({'type': 'stage2_revision_start'})}\n\n"
                stage2_revision_results, stage2_revision_summary = await stage2_collect_revision_deltas(
                    enhanced_context,
                    stage1_results,
                    stage2_results,
                    label_to_model,
                    revision_models=stage2_ranking_models,
                )
                stage1_results_for_stage3, apply_summary = apply_stage2_revision_deltas(
                    stage1_results,
                    stage2_revision_results,
                )
                stage2_revision_summary["apply"] = apply_summary
                yield (
                    "data: "
                    f"{json.dumps({'type': 'stage2_revision_complete', 'data': stage2_revision_results, 'summary': stage2_revision_summary})}\n\n"
                )
            stage2_reconciliation: Dict[str, Any] = {"enabled": False, "accepted": False}
            if STAGE2_RECONCILIATION_ENABLED:
                yield f"data: {json.dumps({'type': 'stage2_reconciliation_start'})}\n\n"
                stage2_reconciliation = await stage2_collect_reconciliation(
                    enhanced_context,
                    stage1_results_for_stage3,
                    stage2_results,
                    label_to_model,
                    source_evidence_pack=emulated_metadata,
                )
                yield (
                    "data: "
                    f"{json.dumps({'type': 'stage2_reconciliation_complete', 'data': stage2_reconciliation})}\n\n"
                )
            yield (
                "data: "
                f"{json.dumps({'type': 'stage2_complete', 'data': stage2_results, 'metadata': {'label_to_model': label_to_model, 'aggregate_rankings': aggregate_rankings, 'council_mode': selected_council_mode, 'research_depth': selected_research_depth, 'ranking_models': stage2_ranking_models or [], 'chairman_model': stage3_chairman_model or CHAIRMAN_MODEL, 'template_id': selected_template_id, 'company_name': selected_company_name, 'company_type': selected_company_type, 'template_selection_source': template_selection_source, 'exchange': selected_exchange, 'exchange_selection_source': exchange_selection_source, 'stage2_revision_pass_enabled': bool(STAGE2_REVISION_PASS_ENABLED), 'stage2_revision_summary': stage2_revision_summary, 'stage2_reconciliation_enabled': bool(STAGE2_RECONCILIATION_ENABLED), 'stage2_reconciliation': stage2_reconciliation}})}\n\n"
            )
            _persist_assistant_patch(
                {
                    "stage2": stage2_results,
                    "metadata": {
                        "label_to_model": label_to_model,
                        "aggregate_rankings": aggregate_rankings,
                        "council_mode": selected_council_mode,
                        "research_depth": selected_research_depth,
                        "ranking_models": stage2_ranking_models or [],
                        "chairman_model": stage3_chairman_model or CHAIRMAN_MODEL,
                        "template_id": selected_template_id,
                        "company_name": selected_company_name,
                        "company_type": selected_company_type,
                        "template_selection_source": template_selection_source,
                        "exchange": selected_exchange,
                        "exchange_selection_source": exchange_selection_source,
                        "stage2_revision_pass_enabled": bool(STAGE2_REVISION_PASS_ENABLED),
                        "stage2_revision_summary": stage2_revision_summary,
                    },
                    "loading": {"stage2": False},
                }
            )

            yield f"data: {json.dumps({'type': 'stage3_start'})}\n\n"
            _persist_assistant_patch({"loading": {"stage3": True}})
            stage3_evidence_pack = (search_results or {}).get("evidence_pack")
            if isinstance(stage3_evidence_pack, dict) and isinstance(emulated_metadata, dict):
                stage3_evidence_pack = {
                    **stage3_evidence_pack,
                    "stage1_emulated_metadata": emulated_metadata,
                    "per_model_research_runs": emulated_metadata.get("per_model_research_runs", []),
                }
            elif isinstance(emulated_metadata, dict) and emulated_metadata:
                stage3_evidence_pack = emulated_metadata

            stage3_result = await stage3_synthesize_final(
                enhanced_context,
                stage1_results_for_stage3,
                stage2_results,
                label_to_model=label_to_model,
                use_structured_analysis=use_structured_analysis,
                template_id=selected_template_id,
                ticker=search_ticker,
                company_name=selected_company_name,
                exchange=selected_exchange,
                chairman_model=stage3_chairman_model,
                market_facts=market_facts,
                evidence_pack=stage3_evidence_pack,
                stage2_reconciliation=stage2_reconciliation,
            )
            yield f"data: {json.dumps({'type': 'stage3_complete', 'data': stage3_result})}\n\n"
            _persist_assistant_patch(
                {
                    "stage3": stage3_result,
                    "loading": {"stage3": False},
                }
            )

            if title_task:
                title = await title_task
                storage.update_conversation_title(conversation_id, title)
                yield f"data: {json.dumps({'type': 'title_complete', 'data': {'title': title}})}\n\n"

            storage.add_assistant_message_with_metadata(
                conversation_id,
                stage1_results_for_stage3,
                stage2_results,
                stage3_result,
                search_results,
                attachments_processed
            )

            yield f"data: {json.dumps({'type': 'complete'})}\n\n"

        except Exception as e:
            import traceback
            traceback.print_exc()
            _persist_assistant_patch(
                {
                    "status": "failed",
                    "error": str(e),
                    "loading": {
                        "search": False,
                        "evidence": False,
                        "attachments": False,
                        "stage1": False,
                        "stage2": False,
                        "stage3": False,
                    },
                }
            )
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )
