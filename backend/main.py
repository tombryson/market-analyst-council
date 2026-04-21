"""FastAPI backend for LLM Council."""

from fastapi import FastAPI, HTTPException, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uuid
import json
import asyncio

from . import storage
from .council import (
    run_full_council,
    generate_conversation_title,
    stage1_collect_responses,
    stage1_collect_perplexity_research_responses,
    stage2_collect_rankings,
    stage2_collect_revision_deltas,
    stage2_collect_reconciliation,
    apply_stage2_revision_deltas,
    stage3_synthesize_final,
    calculate_aggregate_rankings,
    _is_openrouter_compatible_model,
)
from .search import (
    perform_search,
    reformulate_query_for_search,
    format_search_results_for_prompt,
    perform_financial_search,
    extract_ticker_from_query,
)
from .pdf_processor import save_attachment, process_pdf_attachment, format_pdf_context_for_prompt
from .config import (
    ENABLE_RESEARCH_SERVICE,
    COUNCIL_EXECUTION_MODE,
    RESEARCH_DEPTH,
    CHAIRMAN_MODEL,
    ENABLE_MARKET_FACTS_PREPASS,
    STAGE2_REVISION_PASS_ENABLED,
    STAGE2_RECONCILIATION_ENABLED,
    PROGRESS_LOGGING,
    SYSTEM_ENABLED,
    SYSTEM_SHUTDOWN_REASON,
)
from .research import ResearchService, format_evidence_pack_for_prompt
from .market_facts import (
    gather_market_facts_prepass,
    format_market_facts_query_prefix,
    prepend_market_facts_to_query,
)
from .company_type_detector import detect_company_type_via_api

app = FastAPI(title="LLM Council API")
research_service = ResearchService()


def _ensure_system_enabled() -> None:
    """Block runtime execution while global shutdown is active."""
    if SYSTEM_ENABLED:
        return
    raise HTTPException(
        status_code=503,
        detail=(
            f"System disabled: {SYSTEM_SHUTDOWN_REASON or 'maintenance mode active'}"
        ),
    )

# Enable CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CreateConversationRequest(BaseModel):
    """Request to create a new conversation."""
    pass


class SendMessageRequest(BaseModel):
    """Request to send a message in a conversation."""
    content: str


class CompanyTypeDetectRequest(BaseModel):
    """Request payload for company-type detection prepass."""
    content: str = ""
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    exchange: Optional[str] = None


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


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "LLM Council API",
        "system_enabled": bool(SYSTEM_ENABLED),
        "shutdown_reason": SYSTEM_SHUTDOWN_REASON if not SYSTEM_ENABLED else "",
    }


@app.get("/api/templates")
async def list_templates():
    """List all available analysis templates."""
    from .template_loader import list_available_templates
    return list_available_templates()


@app.get("/api/company-types")
async def list_company_types():
    """List predefined company types and mapped templates."""
    from .template_loader import list_company_types as list_available_company_types
    return list_available_company_types()


@app.get("/api/exchanges")
async def list_exchanges():
    """List predefined exchange profiles used for assumption substitution."""
    from .template_loader import list_exchanges as list_available_exchanges
    return list_available_exchanges()


@app.post("/api/company-types/detect")
async def detect_company_type(request: CompanyTypeDetectRequest):
    """
    Lightweight API-assisted company-type detection for template routing.
    """
    result = await detect_company_type_via_api(
        user_query=request.content,
        ticker=request.ticker,
        company_name=request.company_name,
        exchange=request.exchange,
    )
    return result


@app.get("/api/conversations", response_model=List[ConversationMetadata])
async def list_conversations():
    """List all conversations (metadata only)."""
    return storage.list_conversations()


@app.post("/api/conversations", response_model=Conversation)
async def create_conversation(request: CreateConversationRequest):
    """Create a new conversation."""
    _ensure_system_enabled()
    conversation_id = str(uuid.uuid4())
    conversation = storage.create_conversation(conversation_id)
    return conversation


@app.get("/api/conversations/{conversation_id}", response_model=Conversation)
async def get_conversation(conversation_id: str):
    """Get a specific conversation with all its messages."""
    conversation = storage.get_conversation(conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conversation


@app.post("/api/conversations/{conversation_id}/message")
async def send_message(conversation_id: str, request: SendMessageRequest):
    """
    Send a message and run the 3-stage council process.
    Returns the complete response with all stages.
    """
    _ensure_system_enabled()
    # Check if conversation exists
    conversation = storage.get_conversation(conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Check if this is the first message
    is_first_message = len(conversation["messages"]) == 0

    # Add user message
    storage.add_user_message(conversation_id, request.content)

    # If this is the first message, generate a title
    if is_first_message:
        title = await generate_conversation_title(request.content)
        storage.update_conversation_title(conversation_id, title)

    # Run the 3-stage council process
    stage1_results, stage2_results, stage3_result, metadata = await run_full_council(
        request.content
    )

    # Add assistant message with all stages
    storage.add_assistant_message(
        conversation_id,
        stage1_results,
        stage2_results,
        stage3_result
    )

    # Return the complete response with metadata
    return {
        "stage1": stage1_results,
        "stage2": stage2_results,
        "stage3": stage3_result,
        "metadata": metadata
    }


@app.post("/api/conversations/{conversation_id}/message/stream")
async def send_message_stream(
    conversation_id: str,
    content: str = Form(...),
    enable_search: bool = Form(True),
    ticker: str = Form(None),
    exchange: str = Form(None),
    research_depth: str = Form(None),
    council_mode: str = Form(None),
    template_id: str = Form(None),  # NEW: Optional template selection
    company_type: str = Form(None),
    files: List[UploadFile] = File(None)
):
    """
    Send a message and stream the 3-stage council process.
    Supports optional PDF attachments and internet search.
    Returns Server-Sent Events as each stage completes.
    """
    _ensure_system_enabled()
    # Check if conversation exists
    conversation = storage.get_conversation(conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Check if this is the first message
    is_first_message = len(conversation["messages"]) == 0
    selected_council_mode = normalize_council_mode(council_mode or COUNCIL_EXECUTION_MODE)
    selected_research_depth = normalize_research_depth(research_depth or RESEARCH_DEPTH)

    async def event_generator():
        try:
            yield (
                "data: "
                f"{json.dumps({'type': 'council_mode', 'data': {'mode': selected_council_mode, 'research_depth': selected_research_depth}})}\n\n"
            )

            # Generate message ID for attachment storage
            message_id = str(uuid.uuid4())

            # Process PDF attachments if present
            attachments_metadata = []
            attachments_processed = []

            if files:
                yield f"data: {json.dumps({'type': 'attachments_start', 'count': len(files)})}\n\n"

                for file in files:
                    # Validate file type
                    if not file.filename.endswith('.pdf'):
                        continue

                    # Save file
                    file_content = await file.read()
                    storage_path = await save_attachment(
                        file_content, conversation_id, message_id, file.filename
                    )

                    # Process PDF
                    processed = await process_pdf_attachment(storage_path, file.filename)
                    attachments_processed.append(processed)

                    # Create metadata for storage
                    attachments_metadata.append({
                        "filename": file.filename,
                        "size": len(file_content),
                        "uploaded_at": asyncio.get_event_loop().time(),
                        "storage_path": storage_path,
                        "page_count": processed.get('page_count', 0),
                        "processing_status": processed.get('status', 'failed')
                    })

                yield f"data: {json.dumps({'type': 'attachments_complete', 'data': attachments_processed})}\n\n"

            # Perform shared internet search if enabled and not using emulated Perplexity council mode.
            use_perplexity_emulated_stage1 = selected_council_mode == "perplexity_emulated"
            effective_enable_search = enable_search or use_perplexity_emulated_stage1
            search_results = None
            search_ticker = ticker  # Initialize ticker outside search block

            if not search_ticker:
                search_ticker = extract_ticker_from_query(content)

            from .template_loader import get_template_loader, resolve_template_selection
            loader = get_template_loader()
            auto_company_name = loader.infer_company_name(content, ticker=search_ticker)
            auto_detected_company_type = None
            company_type_detection: Dict[str, Any] = {}
            if not company_type:
                yield f"data: {json.dumps({'type': 'company_type_detection_start'})}\n\n"
                company_type_detection = await detect_company_type_via_api(
                    user_query=content,
                    ticker=search_ticker,
                    company_name=auto_company_name,
                    exchange=exchange,
                )
                auto_detected_company_type = str(
                    company_type_detection.get("selected_company_type") or ""
                ).strip()
                yield (
                    "data: "
                    f"{json.dumps({'type': 'company_type_detection_complete', 'data': company_type_detection})}\n\n"
                )

            template_selection = resolve_template_selection(
                user_query=content,
                ticker=search_ticker,
                explicit_template_id=template_id,
                company_type=(company_type or auto_detected_company_type or None),
                exchange=exchange,
            )
            if company_type_detection:
                template_selection["company_type_detection"] = company_type_detection
            selected_template_id = template_selection["template_id"]
            selected_company_type = template_selection.get("company_type")
            selected_company_name = template_selection.get("company_name")
            selected_exchange = template_selection.get("exchange")
            template_selection_source = template_selection.get("selection_source", "auto")
            if auto_detected_company_type and not company_type:
                template_selection_source = "api_company_type_detected"
                template_selection["selection_source"] = template_selection_source
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

            print(
                "Template selection: "
                f"template={selected_template_id} "
                f"company={selected_company_name} "
                f"company_type={selected_company_type} "
                f"exchange={selected_exchange} "
                f"source={template_selection_source} "
                f"exchange_source={exchange_selection_source} "
                f"structured={use_structured_analysis}"
            )
            yield (
                "data: "
                f"{json.dumps({'type': 'template_selected', 'data': template_selection})}\n\n"
            )

            # Deterministic market-facts prepass used to anchor valuation/share-structure fields.
            market_facts = None
            if ENABLE_MARKET_FACTS_PREPASS and search_ticker:
                yield f"data: {json.dumps({'type': 'market_facts_start'})}\n\n"
                market_facts = await gather_market_facts_prepass(
                    ticker=search_ticker,
                    company_name=selected_company_name,
                    exchange=selected_exchange,
                )
                yield f"data: {json.dumps({'type': 'market_facts_complete', 'data': market_facts})}\n\n"

            if effective_enable_search and not use_perplexity_emulated_stage1:
                yield f"data: {json.dumps({'type': 'search_start'})}\n\n"

                try:
                    if ENABLE_RESEARCH_SERVICE:
                        yield f"data: {json.dumps({'type': 'evidence_start'})}\n\n"

                        search_results = await research_service.gather_research(
                            user_query=content,
                            ticker=search_ticker,
                        )

                        evidence_pack = search_results.get("evidence_pack")
                        if evidence_pack:
                            yield f"data: {json.dumps({'type': 'evidence_complete', 'data': evidence_pack})}\n\n"

                        print(
                            "Research service complete "
                            f"(provider={search_results.get('provider', 'unknown')}): "
                            f"{search_results.get('result_count', 0)} results"
                        )
                    else:
                        if search_ticker:
                            # If we have a ticker, do financial search on JUST the ticker
                            print(f"Using ticker: {search_ticker}, performing targeted financial search")
                            search_results = await perform_financial_search(search_ticker)
                            print(f"Financial search complete: {search_results.get('result_count', 0)} results, {len(search_results.get('pdfs_processed', []))} PDFs downloaded")
                        else:
                            # No ticker found, do standard search with reformulated query
                            search_query = await reformulate_query_for_search(content)
                            print(f"Search query reformulated: '{content[:50]}...' -> '{search_query}'")
                            search_results = await perform_search(search_query)
                            print(f"Search results: {search_results.get('result_count', 0)} results")

                    yield f"data: {json.dumps({'type': 'search_complete', 'data': search_results})}\n\n"
                except Exception as e:
                    print(f"Search error: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    search_results = {
                        "error": f"Search failed: {str(e)}",
                        "results": [],
                        "result_count": 0
                    }
                    yield f"data: {json.dumps({'type': 'search_complete', 'data': search_results})}\n\n"

            # Include PDFs from search if available
            all_attachments = attachments_processed.copy()
            if search_results and search_results.get("pdfs_processed"):
                all_attachments.extend(search_results["pdfs_processed"])

            # Add user message with metadata
            storage.add_user_message_with_metadata(
                conversation_id,
                content,
                effective_enable_search,
                attachments_metadata,
                selected_council_mode,
                template_id=selected_template_id,
                company_name=selected_company_name,
                company_type=selected_company_type,
                exchange=selected_exchange,
                template_selection_source=template_selection_source,
                exchange_selection_source=exchange_selection_source,
            )

            # Start title generation in parallel (don't await yet)
            title_task = None
            if is_first_message:
                title_task = asyncio.create_task(generate_conversation_title(content))

            # Stage 1: Collect responses
            yield f"data: {json.dumps({'type': 'stage1_start'})}\n\n"
            stage2_ranking_models = None
            stage3_chairman_model = None
            if use_perplexity_emulated_stage1:
                yield f"data: {json.dumps({'type': 'search_start'})}\n\n"
                yield f"data: {json.dumps({'type': 'evidence_start'})}\n\n"

                attachment_context = format_pdf_context_for_prompt(all_attachments)
                stage1_effective_research_brief = stage1_research_brief
                stage1_query_core = (content or "").strip()
                if use_structured_analysis:
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
                    print(
                        "Stage1 prompt assembly: "
                        f"structured_template={use_structured_analysis}, "
                        f"template_id={selected_template_id}, "
                        f"market_facts_prefixed={bool(market_facts)}, "
                        f"query_core_chars={len(stage1_query_core)}, "
                        f"query_sent_chars={len(stage1_effective_query)}, "
                        f"brief_chars={len(stage1_effective_research_brief)}"
                    )
                stage1_results, emulated_metadata = await stage1_collect_perplexity_research_responses(
                    user_query=stage1_effective_query,
                    ticker=search_ticker,
                    attachment_context=attachment_context,
                    depth=selected_research_depth,
                    research_brief=stage1_effective_research_brief,
                    template_id=selected_template_id,
                )
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
                    print(
                        "Stage2 judge-model filter excluded non-OpenRouter models: "
                        f"{excluded_stage2_models}"
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
                )
                stage1_results = await stage1_collect_responses(enhanced_context)

            if not stage1_results:
                yield f"data: {json.dumps({'type': 'error', 'message': 'No Stage 1 responses were generated. Please try again.'})}\n\n"
                return

            enhanced_context = build_enhanced_context(
                content,
                search_results,
                all_attachments,
                template_context=template_context,
                market_facts=market_facts,
            )
            yield f"data: {json.dumps({'type': 'stage1_complete', 'data': stage1_results})}\n\n"

            # Stage 2: Collect rankings
            yield f"data: {json.dumps({'type': 'stage2_start'})}\n\n"
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
                )
                yield (
                    "data: "
                    f"{json.dumps({'type': 'stage2_reconciliation_complete', 'data': stage2_reconciliation})}\n\n"
                )
            yield (
                "data: "
                f"{json.dumps({'type': 'stage2_complete', 'data': stage2_results, 'metadata': {'label_to_model': label_to_model, 'aggregate_rankings': aggregate_rankings, 'council_mode': selected_council_mode, 'research_depth': selected_research_depth, 'ranking_models': stage2_ranking_models or [], 'chairman_model': stage3_chairman_model or CHAIRMAN_MODEL, 'template_id': selected_template_id, 'company_name': selected_company_name, 'company_type': selected_company_type, 'template_selection_source': template_selection_source, 'exchange': selected_exchange, 'exchange_selection_source': exchange_selection_source, 'stage2_revision_pass_enabled': bool(STAGE2_REVISION_PASS_ENABLED), 'stage2_revision_summary': stage2_revision_summary, 'stage2_reconciliation_enabled': bool(STAGE2_RECONCILIATION_ENABLED), 'stage2_reconciliation': stage2_reconciliation}})}\n\n"
            )

            # Stage 3: Synthesize final answer (with optional structured analysis)
            yield f"data: {json.dumps({'type': 'stage3_start'})}\n\n"
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
                evidence_pack=(search_results or {}).get("evidence_pack"),
                stage2_reconciliation=stage2_reconciliation,
            )
            yield f"data: {json.dumps({'type': 'stage3_complete', 'data': stage3_result})}\n\n"

            # Wait for title generation if it was started
            if title_task:
                title = await title_task
                storage.update_conversation_title(conversation_id, title)
                yield f"data: {json.dumps({'type': 'title_complete', 'data': {'title': title}})}\n\n"

            # Save complete assistant message with search/attachment metadata
            storage.add_assistant_message_with_metadata(
                conversation_id,
                stage1_results_for_stage3,
                stage2_results,
                stage3_result,
                search_results,
                attachments_processed
            )

            # Send completion event
            yield f"data: {json.dumps({'type': 'complete'})}\n\n"

        except Exception as e:
            # Send error event
            import traceback
            traceback.print_exc()
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


def build_enhanced_context(
    user_query: str,
    search_results: Optional[Dict[str, Any]],
    attachments_processed: List[Dict[str, Any]],
    template_context: str = "",
    market_facts: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Build enhanced query context with search results and PDF content.

    Returns:
        Enhanced prompt string combining all context
    """
    parts = [f"User Question: {user_query}"]

    if template_context:
        parts.append("\n\n--- ANALYSIS FRAMEWORK ---")
        parts.append(template_context)

    market_facts_text = format_market_facts_query_prefix(market_facts)
    if market_facts_text:
        parts.append("\n\n--- MARKET FACTS PREPASS ---")
        parts.append(market_facts_text)

    # Include search results even if there's an error (format_search_results_for_prompt handles this)
    if search_results:
        parts.append("\n\n--- INTERNET SEARCH RESULTS ---")
        formatted = format_search_results_for_prompt(search_results)
        parts.append(formatted)
        print(f"Search context added to prompt: {len(formatted)} chars")

        evidence_pack = search_results.get("evidence_pack")
        if evidence_pack:
            evidence_text = format_evidence_pack_for_prompt(evidence_pack)
            if evidence_text:
                parts.append("\n\n--- NORMALIZED EVIDENCE PACK ---")
                parts.append(evidence_text)
                print(f"Evidence pack context added: {len(evidence_text)} chars")

    if attachments_processed:
        pdf_context = format_pdf_context_for_prompt(attachments_processed)
        if pdf_context:
            parts.append("\n\n--- ATTACHED DOCUMENTS ---")
            parts.append(pdf_context)

    enhanced = "\n".join(parts)
    print(f"Enhanced context total length: {len(enhanced)} chars")
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
