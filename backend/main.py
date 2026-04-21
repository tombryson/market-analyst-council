"""FastAPI backend for LLM Council."""

import hashlib
import hmac

from fastapi import FastAPI, HTTPException, File, UploadFile, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple
import uuid
import json
import asyncio
import re
import sys
import os
import socket
import time
from pathlib import Path
from datetime import datetime, timezone

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
    SUPPLEMENTARY_API_PIPELINES_ENABLED,
    SCENARIO_ROUTER_WEBHOOK_SECRET,
    SCENARIO_ROUTER_WEBHOOK_REQUIRE_SECRET,
)
from .research import ResearchService, format_evidence_pack_for_prompt
from .research.supplementary_registry import (
    resolve_pipeline_id_for_template,
    resolve_pipeline_spec_for_template,
)
from .market_facts import (
    gather_market_facts_prepass,
    format_market_facts_query_prefix,
    prepend_market_facts_to_query,
)
from .delta_monitor import (
    get_latest_delta,
    run_delta_check,
)

app = FastAPI(title="LLM Council API")
research_service = ResearchService()
OUTPUTS_DIR = Path(
    os.getenv("ANALYSIS_OUTPUTS_DIR", str(Path(__file__).resolve().parents[1] / "outputs"))
)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOBS_OUTPUTS_DIR = Path(os.getenv("ANALYSIS_JOBS_DIR", str(OUTPUTS_DIR / "jobs")))
JOBS_META_DIR = JOBS_OUTPUTS_DIR / "meta"
PREPASS_OUTPUTS_DIR = Path(
    os.getenv("ANALYSIS_PREPASS_DIR", str(JOBS_OUTPUTS_DIR / "prepass"))
)
ANALYSIS_JOB_LOG_TAIL_CHARS = 24000
ANALYSIS_JOBS: Dict[str, Dict[str, Any]] = {}
ANALYSIS_JOBS_LOCK = asyncio.Lock()
SYNTHETIC_RUN_JOB_PREFIX = "run::"
GANTT_RUN_LIST_CACHE_TTL_SEC = max(
    1,
    int(os.getenv("GANTT_RUN_LIST_CACHE_TTL_SEC", "15")),
)
_GANTT_RUN_LIST_CACHE: Dict[str, Any] = {
    "expires_at": 0.0,
    "key": None,
    "runs": None,
}
INSTANCE_ID = (
    str(os.getenv("FLY_MACHINE_ID") or "").strip()
    or str(os.getenv("HOSTNAME") or "").strip()
    or socket.gethostname()
)
SUPPLEMENTARY_DOC_MAX_CHARS = 12000
SUPPLEMENTARY_DOC_ALLOWED_EXTENSIONS = {".pdf", ".md", ".txt", ".json"}
SCENARIO_ROUTER_EVENTS_DIR = OUTPUTS_DIR / "scenario_router_events"
SCENARIO_ROUTER_DEDUPE_DIR = SCENARIO_ROUTER_EVENTS_DIR / "dedupe"
LEGACY_FRESHNESS_EVENTS_DIR = OUTPUTS_DIR / "freshness_events"
LEGACY_FRESHNESS_DEDUPE_DIR = LEGACY_FRESHNESS_EVENTS_DIR / "dedupe"

_ANALYSIS_PROGRESS_MARKERS: List[Tuple[str, str, int]] = [
    ("market facts prepass start", "prepass", 4),
    ("market facts prepass done", "prepass", 8),
    ("primary injection prepass start", "prepass", 10),
    ("primary injection bundle ready", "prepass", 16),
    ("stage 1 start", "stage1", 18),
    ("stage 1 done", "stage1", 55),
    ("stage 2 start", "stage2", 60),
    ("stage 2 done", "stage2", 72),
    ("stage 2.5 revision pass start", "stage2_5", 76),
    ("stage 2.5 revision pass done", "stage2_5", 84),
    ("stage 3 start", "stage3", 88),
    ("stage 3 primary done", "stage3", 95),
    ("stage 4 start", "stage4", 96),
    ("stage 4 done", "stage4", 98),
    ("stage 3 secondary start", "stage3_secondary", 96),
    ("stage 3 secondary done", "stage3_secondary", 98),
    ("run complete", "complete", 100),
    ("mvp quality test complete", "complete", 100),
]

_ANALYSIS_STAGE_ORDER: Dict[str, int] = {
    "queued": 0,
    "initializing": 1,
    "prepass": 2,
    "stage1": 3,
    "stage2": 4,
    "stage2_5": 5,
    "stage3": 6,
    "stage4": 7,
    "stage3_secondary": 8,
    "complete": 9,
    "failed": 10,
}

_ANALYSIS_STAGE_RANGES: Dict[str, Tuple[int, int]] = {
    "prepass": (4, 16),
    "stage1": (18, 55),
    "stage2": (60, 72),
    "stage2_5": (76, 84),
    "stage3": (88, 95),
    "stage4": (96, 98),
    "stage3_secondary": (96, 98),
}


def _build_scenario_router_service():
    from .scenario_router.document_reader import DocumentReader
    from .scenario_router.lab_scribe import LabScribe
    from .scenario_router.market_facts_resolver import ScenarioMarketFactsResolver
    from .scenario_router.run_selector import LatestRunSelector
    from .scenario_router.source_resolver import SourceResolver
    from .scenario_router.thesis_comparator import ThesisComparator
    from .scenario_router.service import (
        ScenarioRouterDependencies,
        ScenarioRouterService,
    )

    return ScenarioRouterService(
        ScenarioRouterDependencies(
            source_resolver=SourceResolver().resolve,
            document_reader=DocumentReader().read,
            run_selector=LatestRunSelector(limit=25).select_latest,
            market_facts_resolver=ScenarioMarketFactsResolver().resolve,
            thesis_comparator=ThesisComparator().compare,
            lab_scribe=LabScribe().persist,
        )
    )


def _safe_scenario_router_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _scenario_router_dedupe_paths(event_key: str) -> Tuple[Path, Path]:
    safe_key = _safe_scenario_router_key(event_key)
    if not safe_key:
        return Path(""), Path("")
    prefix = safe_key[:2]
    directory = SCENARIO_ROUTER_DEDUPE_DIR / prefix
    return directory, directory / f"{safe_key}.json"


def _load_scenario_router_dedupe(event_key: str) -> Dict[str, Any]:
    directory, marker_path = _scenario_router_dedupe_paths(event_key)
    if not directory:
        return {}
    if not marker_path.exists():
        legacy_path = LEGACY_FRESHNESS_DEDUPE_DIR / marker_path.parent.name / marker_path.name
        if legacy_path.exists():
            marker_path = legacy_path
        else:
            return {}
    try:
        return json.loads(marker_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _persist_scenario_router_dedupe(event_key: str, payload: Dict[str, Any]) -> None:
    directory, marker_path = _scenario_router_dedupe_paths(event_key)
    if not directory:
        return
    directory.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


def _choose_scenario_router_event_key(payload: Dict[str, Any]) -> str:
    for field in ("gmail_message_id", "event_id", "message_id"):
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    subject = str(payload.get("subject") or "").strip()
    sender = str(payload.get("sender") or "").strip().lower()
    received_at = str(payload.get("received_at_utc") or "").strip()
    ticker = str(payload.get("ticker") or "").strip().upper()
    if subject or sender or received_at:
        return f"{ticker}|{sender}|{received_at}|{subject}"
    return ""


def _check_scenario_router_webhook_secret(request: Request) -> None:
    provided = str(
        request.headers.get("x-scenario-router-secret")
        or request.headers.get("x-freshness-secret")
        or ""
    ).strip()
    configured = str(SCENARIO_ROUTER_WEBHOOK_SECRET or "").strip()
    if not configured and not SCENARIO_ROUTER_WEBHOOK_REQUIRE_SECRET:
        return
    if not configured:
        raise HTTPException(
            status_code=503,
            detail="Scenario router webhook secret is required but not configured.",
        )
    if not provided or not hmac.compare_digest(provided, configured):
        raise HTTPException(status_code=401, detail="Invalid scenario router webhook secret.")


def _analysis_stage_rank(stage: Any) -> int:
    return int(_ANALYSIS_STAGE_ORDER.get(str(stage or "").strip().lower(), -1))


def _scale_stage_progress(stage: str, local_pct: int) -> int:
    stage_key = str(stage or "").strip().lower()
    start_end = _ANALYSIS_STAGE_RANGES.get(stage_key)
    pct = max(0, min(100, int(local_pct)))
    if not start_end:
        return pct
    start, end = start_end
    if end <= start:
        return end
    return start + int(round((pct / 100.0) * (end - start)))


def _manifest_count(value: Any) -> int:
    """Accept either a scalar count or a collection stored in manifest fields."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        try:
            return int(float(text))
        except Exception:
            return 0
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    return 0


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


async def _store_supplementary_upload_for_job(
    supplementary_file: Optional[UploadFile],
    *,
    job_id: str,
) -> Tuple[Optional[Path], str]:
    """Persist the raw supplementary upload quickly so the async worker can process it later."""
    if supplementary_file is None:
        return None, ""

    filename = str(getattr(supplementary_file, "filename", "") or "").strip() or "supplementary_document"
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPLEMENTARY_DOC_ALLOWED_EXTENSIONS:
        return None, ""

    try:
        file_content = await supplementary_file.read()
    except Exception:
        return None, ""
    if not file_content:
        return None, ""

    uploads_dir = JOBS_OUTPUTS_DIR / "supplementary"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    safe_stem = re.sub(r"[^\w\s\-\.]", "_", Path(filename).stem).strip() or "supplementary_document"
    target = uploads_dir / f"{job_id}_{safe_stem}{suffix}"
    target.write_bytes(file_content)
    return target, filename


def _store_portfolio_context_for_job(
    portfolio_context: Optional[Dict[str, Any]],
    *,
    job_id: str,
) -> Optional[Path]:
    """Persist normalized portfolio context for async portfolio-positioning jobs."""
    if not isinstance(portfolio_context, dict) or not portfolio_context:
        return None

    contexts_dir = JOBS_OUTPUTS_DIR / "portfolio_context"
    contexts_dir.mkdir(parents=True, exist_ok=True)
    target = contexts_dir / f"{job_id}_portfolio_context.json"
    target.write_text(json.dumps(portfolio_context, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


async def _prepare_generated_supplementary_for_job(
    *,
    job_id: str,
    request_payload: Dict[str, Any],
) -> Tuple[Optional[Path], List[Path], Dict[str, Any]]:
    mode = _validate_supplementary_mode(request_payload.get("supplementary_mode"))
    if mode not in {"mining_pipeline", "api_pipeline"}:
        return None, [], {"mode": mode or "", "generated": False}
    if not SUPPLEMENTARY_API_PIPELINES_ENABLED:
        raise RuntimeError(
            "Supplementary API pipelines are disabled. Use an uploaded supplementary document instead."
        )
    if str(request_payload.get("supplementary_file") or "").strip():
        return None, [], {"mode": mode, "generated": False, "reason": "uploaded_file_precedence"}
    if str(request_payload.get("reuse_supplementary_from_job_id") or "").strip():
        return None, [], {"mode": mode, "generated": False, "reason": "reused_supplementary_precedence"}

    from .template_loader import get_template_loader

    loader = get_template_loader()
    ticker = str(request_payload.get("ticker") or "").strip().upper()
    user_query = str(request_payload.get("query") or "").strip()
    explicit_company_name = str(request_payload.get("company_name") or "").strip()
    explicit_exchange = str(request_payload.get("exchange") or "").strip()
    explicit_template_id = str(request_payload.get("template_id") or "").strip()
    explicit_company_type = str(request_payload.get("company_type") or "").strip()

    selection = loader.resolve_template_selection(
        user_query or explicit_company_name or ticker,
        ticker=ticker or None,
        explicit_template_id=explicit_template_id or None,
        company_type=explicit_company_type or None,
        exchange=explicit_exchange or None,
    )
    selected_template_id = str(selection.get("template_id") or explicit_template_id or "").strip()
    selected_company_type = str(selection.get("company_type") or explicit_company_type or "").strip()
    resolved_pipeline_id = resolve_pipeline_id_for_template(selected_template_id)
    pipeline_spec = resolve_pipeline_spec_for_template(selected_template_id)
    if mode == "mining_pipeline":
        if resolved_pipeline_id != "resources_supplementary":
            raise RuntimeError(
                f"Supplementary mode 'mining_pipeline' requested for non-mining context "
                f"(template_id={selected_template_id or 'unknown'}, company_type={selected_company_type or 'unknown'})."
            )
    elif not resolved_pipeline_id or pipeline_spec is None:
        raise RuntimeError(
            f"Supplementary mode 'api_pipeline' requested for unsupported context "
            f"(template_id={selected_template_id or 'unknown'}, company_type={selected_company_type or 'unknown'})."
        )

    selected_company_name = str(selection.get("company_name") or explicit_company_name or "").strip()
    selected_exchange = str(selection.get("exchange") or explicit_exchange or "").strip()
    if not ticker:
        raise RuntimeError("Supplementary generation requires a canonical ticker.")

    generated = await research_service.gather_supplementary_facts(
        pipeline_id=resolved_pipeline_id or "",
        user_query=user_query,
        company=selected_company_name,
        ticker=ticker,
        exchange=selected_exchange,
        commodity="",
        template_id=selected_template_id,
        company_type=selected_company_type,
    )
    final_json = generated.get("final_json") if isinstance(generated, dict) else None
    if not isinstance(final_json, dict) or not final_json:
        raise RuntimeError("Supplementary generation returned no final_json payload.")

    uploads_dir = JOBS_OUTPUTS_DIR / "supplementary"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    base_stem = f"{job_id}_{resolved_pipeline_id or mode}"
    context_path = uploads_dir / f"{base_stem}.json"
    debug_path = uploads_dir / f"{base_stem}.debug.json"
    context_path.write_text(json.dumps(final_json, indent=2, ensure_ascii=False), encoding="utf-8")
    debug_path.write_text(json.dumps(generated, indent=2, ensure_ascii=False), encoding="utf-8")
    return context_path, [context_path, debug_path], {
        "mode": mode,
        "generated": True,
        "context_path": str(context_path),
        "debug_path": str(debug_path),
        "template_id": selected_template_id,
        "company_type": selected_company_type,
        "company_name": selected_company_name,
        "exchange": selected_exchange,
        "pipeline_id": resolved_pipeline_id,
        "pipeline_label": getattr(pipeline_spec, "industry_label", "") if pipeline_spec else "",
    }

# Enable CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    # Allow localhost, loopback, and LAN dev hosts for browser-based testing.
    allow_origin_regex=(
        r"^https?://("
        r"localhost|127\.0\.0\.1|0\.0\.0\.0|"
        r"192\.168\.\d{1,3}\.\d{1,3}|"
        r"10\.\d{1,3}\.\d{1,3}\.\d{1,3}|"
        r"172\.(1[6-9]|2\d|3[0-1])\.\d{1,3}\.\d{1,3}"
        r")(:\d+)?$"
    ),
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


class CreateAnalysisJobRequest(BaseModel):
    """Request payload for async full-analysis job submission."""
    job_type: Optional[str] = None
    query: Optional[str] = None
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    template_id: Optional[str] = None
    company_type: Optional[str] = None
    exchange: Optional[str] = None
    stage1_only: bool = False
    stage2_revision_pass: str = "on"  # on | off | auto
    secondary_chairman_model: Optional[str] = None
    run_label: Optional[str] = None
    diagnostic_mode: bool = False
    reuse_recent_bundle: bool = False
    reuse_supplementary_from_job_id: Optional[str] = None
    supplementary_mode: Optional[str] = None
    portfolio_context: Optional[Dict[str, Any]] = None
    portfolio_positioning_mode: Optional[str] = None


class ScenarioRouterAttachmentPayload(BaseModel):
    filename: str
    content_type: Optional[str] = None
    local_path: Optional[str] = None


class ProcessScenarioRouterAnnouncementRequest(BaseModel):
    event_id: Optional[str] = None
    gmail_message_id: Optional[str] = None
    message_id: Optional[str] = None
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    company_hint: Optional[str] = None
    company_name: Optional[str] = None
    subject: Optional[str] = None
    sender: Optional[str] = None
    body_text: Optional[str] = None
    source_channel: Optional[str] = None
    received_at_utc: Optional[str] = None
    urls: List[str] = []
    attachments: List[ScenarioRouterAttachmentPayload] = []


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


@app.on_event("startup")
async def _hydrate_analysis_jobs_from_disk() -> None:
    """Load persisted async analysis jobs when API process starts."""
    loaded = _load_analysis_jobs_from_disk()
    async with ANALYSIS_JOBS_LOCK:
        ANALYSIS_JOBS.clear()
        ANALYSIS_JOBS.update(loaded)


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


def _extract_stage3_structured_from_artifact(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract Stage 3 structured JSON from known artifact shapes."""
    if not isinstance(payload, dict):
        return None

    # json_runner_regen artifacts
    structured = payload.get("structured_data")
    if isinstance(structured, dict) and structured:
        return structured

    # Full quality run artifacts
    primary = payload.get("stage3_result_primary")
    if isinstance(primary, dict):
        structured = primary.get("structured_data")
        if isinstance(structured, dict) and structured:
            return structured

    fallback = payload.get("stage3_result")
    if isinstance(fallback, dict):
        structured = fallback.get("structured_data")
        if isinstance(structured, dict) and structured:
            return structured

    return None


def _extract_stage3_result_from_artifact(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract full Stage 3 result object from known artifact shapes."""
    if not isinstance(payload, dict):
        return None

    primary = payload.get("stage3_result_primary")
    if isinstance(primary, dict) and isinstance(primary.get("structured_data"), dict):
        return primary

    fallback = payload.get("stage3_result")
    if isinstance(fallback, dict) and isinstance(fallback.get("structured_data"), dict):
        return fallback

    structured = payload.get("structured_data")
    if isinstance(structured, dict) and structured:
        return {
            "structured_data": structured,
            "chairman_document": payload.get("chairman_document") or {},
            "analyst_document": payload.get("analyst_document") or {},
        }

    return None


def _read_text_if_exists(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _extract_memo_content(
    payload: Dict[str, Any],
    stage3_result: Dict[str, Any],
    artifact_path: Path,
) -> Dict[str, str]:
    """
    Extract analyst/chairman memo text from inline stage3_result docs or sidecar .md files.
    Prefer inline docs from the current Stage 3 payload; sidecars are a fallback for older runs.
    """
    analyst_doc = stage3_result.get("analyst_document") if isinstance(stage3_result, dict) else {}
    chairman_doc = stage3_result.get("chairman_document") if isinstance(stage3_result, dict) else {}

    analyst_markdown = (
        str(analyst_doc.get("content_markdown") or "").strip()
        if isinstance(analyst_doc, dict)
        else ""
    )
    chairman_markdown = (
        str(chairman_doc.get("content") or "").strip()
        if isinstance(chairman_doc, dict)
        else ""
    )
    if not analyst_markdown:
        analyst_markdown = str(payload.get("analyst_memo_markdown") or "").strip()
    if not chairman_markdown:
        chairman_markdown = str(payload.get("chairman_memo_markdown") or "").strip()

    def _memo_priority(path: Path, *, analyst: bool) -> Tuple[int, float]:
        """
        Prefer canonical stage3_primary sidecars for the selected run.
        Replay/experimental sidecars are lower priority even if newer.
        """
        name = path.name.lower()
        if "_override" in name:
            return (99, -path.stat().st_mtime)

        replay_penalty = 10 if "replay" in name else 0
        if analyst:
            if ".stage3_primary_" in name and "analyst" in name:
                base = 0
            elif ".stage3_secondary_" in name and "analyst" in name:
                base = 1
            elif "analyst" in name:
                base = 2
            else:
                base = 50
        else:
            if ".stage3_primary_" in name and "analyst" not in name:
                base = 0
            elif ".stage3_secondary_" in name and "analyst" not in name:
                base = 1
            elif ".stage3_" in name and "analyst" not in name:
                base = 2
            else:
                base = 50

        return (base + replay_penalty, -path.stat().st_mtime)

    memo_files = payload.get("stage3_memo_files")
    if isinstance(memo_files, list):
        existing_candidates: List[Path] = []
        for raw_path in memo_files:
            candidate = Path(str(raw_path))
            if candidate.exists() and candidate.is_file():
                existing_candidates.append(candidate)

        for candidate in sorted(existing_candidates, key=lambda p: _memo_priority(p, analyst=True)):
            lower_name = candidate.name.lower()
            if "_override" in lower_name:
                continue
            if not analyst_markdown and "analyst" in lower_name:
                analyst_markdown = _read_text_if_exists(candidate).strip()
                break

        for candidate in sorted(existing_candidates, key=lambda p: _memo_priority(p, analyst=False)):
            lower_name = candidate.name.lower()
            if "_override" in lower_name:
                continue
            if "analyst" in lower_name:
                continue
            if not chairman_markdown:
                chairman_markdown = _read_text_if_exists(candidate).strip()
                if chairman_markdown:
                    break

    if not analyst_markdown:
        analyst_candidates = sorted(
            artifact_path.parent.glob(f"{artifact_path.stem}.stage3_*_analyst_*.md"),
            key=lambda p: _memo_priority(p, analyst=True),
        )
        for candidate in analyst_candidates:
            if "_override" in candidate.name.lower():
                continue
            text = _read_text_if_exists(candidate).strip()
            if text:
                analyst_markdown = text
                break

    if not chairman_markdown:
        chairman_candidates = sorted(
            artifact_path.parent.glob(f"{artifact_path.stem}.stage3_*.md"),
            key=lambda p: _memo_priority(p, analyst=False),
        )
        for candidate in chairman_candidates:
            lower_name = candidate.name.lower()
            if "_override" in lower_name:
                continue
            if "analyst" in lower_name:
                continue
            text = _read_text_if_exists(candidate).strip()
            if text:
                chairman_markdown = text
                break

    return {
        "analyst_memo_markdown": _sanitize_memo_markdown(analyst_markdown),
        "chairman_memo_markdown": _sanitize_memo_markdown(chairman_markdown),
    }


def _sanitize_memo_markdown(markdown: str) -> str:
    """
    Remove legacy Stage 3 metadata preambles from memo markdown so UI starts
    directly with title/content.
    """
    text = str(markdown or "")
    if not text.strip():
        return ""
    lines = text.replace("\r\n", "\n").split("\n")
    first_non_empty = next((line.strip() for line in lines if line.strip()), "")
    if first_non_empty not in {"# Stage 3 Analyst Memo", "# Stage 3 Chairman Memo"}:
        cleaned = text
    else:
        memo_idx = None
        for idx, line in enumerate(lines):
            if line.strip().lower() == "## memo":
                memo_idx = idx
                break
        if memo_idx is None:
            cleaned = text
        else:
            content_lines = lines[memo_idx + 1 :]
            while content_lines and not content_lines[0].strip():
                content_lines = content_lines[1:]
            cleaned = "\n".join(content_lines).strip()

    # Remove legacy stage-1 snapshot appendices from analyst memos.
    # This keeps the memo narrative focused on the synthesized analysis only.
    cleaned = re.sub(
        r"(?ms)^#{2,3}\s*Stage 1 (?:Model Score & Target Reference|Council Snapshot)\s*\n.*?(?=^#{1,6}\s+|\Z)",
        "",
        cleaned,
    )
    return cleaned.strip()


def _build_gantt_run_label(filename: str, structured: Dict[str, Any]) -> str:
    ticker = _normalize_ticker_for_label(structured)
    company = str(structured.get("company_name") or structured.get("company") or "").strip()
    analysis_date = str(structured.get("analysis_date") or "").strip()
    date_label = ""
    if analysis_date:
        try:
            dt = datetime.fromisoformat(analysis_date.replace("Z", "+00:00"))
            date_label = dt.strftime("%Y-%m-%d")
        except Exception:
            date_label = analysis_date[:10]

    head = " ".join(x for x in [ticker, company] if x).strip() or filename
    if date_label:
        return f"{head} ({date_label})"
    return head


def _normalize_ticker_for_label(structured: Dict[str, Any]) -> str:
    """
    Prefer canonical EXCHANGE:TICKER display where possible.
    Falls back safely for legacy artifacts.
    """
    if not isinstance(structured, dict):
        return ""

    raw_ticker = str(structured.get("ticker") or "").strip()
    exchange = str(
        structured.get("exchange")
        or structured.get("exchange_id")
        or ""
    ).strip().upper()
    market_meta = structured.get("market_data_provenance")
    if not isinstance(market_meta, dict):
        market_meta = {}
    prepass_ticker = str(market_meta.get("prepass_ticker") or "").strip()

    candidate = raw_ticker or prepass_ticker
    if not candidate:
        return ""

    if ":" in candidate:
        prefix, symbol = candidate.split(":", 1)
        prefix = prefix.strip().upper()
        symbol = symbol.strip().upper()
        if prefix and symbol:
            return f"{prefix}:{symbol}"
        return candidate.strip().upper()

    symbol = candidate.strip().upper()
    # If symbol already includes venue suffix (e.g. ".AX"), preserve as-is.
    if "." in symbol:
        return symbol
    if exchange:
        return f"{exchange}:{symbol}"
    return symbol


def _parse_iso_datetime_utc(value: Any) -> Optional[datetime]:
    """Best-effort parse of ISO-like timestamp values into UTC datetimes."""
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_utc_iso(value: Optional[datetime]) -> str:
    if not isinstance(value, datetime):
        return ""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compute_run_freshness(structured: Dict[str, Any], artifact_mtime_utc: str) -> Dict[str, Any]:
    """
    Compute staleness/freshness from existing run artifact only.
    No retrieval, no external side effects.
    """
    now_utc = datetime.now(timezone.utc)
    market_meta = structured.get("market_data_provenance")
    if not isinstance(market_meta, dict):
        market_meta = {}

    analysis_dt = _parse_iso_datetime_utc(structured.get("analysis_date"))
    market_dt = _parse_iso_datetime_utc(market_meta.get("prepass_as_of_utc"))
    artifact_dt = _parse_iso_datetime_utc(artifact_mtime_utc)

    baseline_dt = analysis_dt or market_dt or artifact_dt or now_utc
    age_days = max(0, int((now_utc - baseline_dt).total_seconds() // 86400))

    if age_days <= 7:
        status = "fresh"
        recommended_action = "reuse"
    elif age_days <= 21:
        status = "watch"
        recommended_action = "review_soon"
    else:
        status = "stale"
        recommended_action = "full_rerun_recommended"

    baseline_source = "analysis_date"
    if analysis_dt is None and market_dt is not None:
        baseline_source = "market_data_provenance.prepass_as_of_utc"
    elif analysis_dt is None and market_dt is None:
        baseline_source = "artifact_updated_at"

    return {
        "analysis_as_of_utc": _to_utc_iso(analysis_dt),
        "market_as_of_utc": _to_utc_iso(market_dt),
        "baseline_as_of_utc": _to_utc_iso(baseline_dt),
        "baseline_source": baseline_source,
        "age_days": age_days,
        "status": status,
        "recommended_action": recommended_action,
        "reason": f"baseline from {baseline_source}; age={age_days} day(s)",
    }


def _resolve_run_artifact_path(run_id: str) -> Path:
    safe_name = Path(run_id).name
    primary = OUTPUTS_DIR / safe_name
    if primary.exists() and primary.is_file():
        return primary
    jobs_path = JOBS_OUTPUTS_DIR / safe_name
    if jobs_path.exists() and jobs_path.is_file():
        return jobs_path
    return primary


def _invalidate_gantt_run_cache() -> None:
    _GANTT_RUN_LIST_CACHE["expires_at"] = 0.0
    _GANTT_RUN_LIST_CACHE["key"] = None
    _GANTT_RUN_LIST_CACHE["runs"] = None


def _normalize_run_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _run_ticker_aliases(value: Any) -> set[str]:
    raw = _normalize_run_ticker(value)
    if not raw:
        return set()
    aliases = {raw}
    if ':' in raw:
        suffix = raw.split(':', 1)[1].strip()
        if suffix:
            aliases.add(suffix)
    return aliases


def _collect_run_related_paths(run_id: str) -> List[Path]:
    canonical_id = _canonical_run_id_for_listing(str(run_id or ""))
    canonical_path = _resolve_run_artifact_path(canonical_id)
    if not canonical_path.exists() or not canonical_path.is_file():
        return []

    related: Dict[str, Path] = {}
    stem = Path(canonical_id).stem

    for candidate in canonical_path.parent.iterdir():
        if not candidate.is_file():
            continue
        name = candidate.name
        if name == canonical_id or name.startswith(f"{stem}."):
            related[str(candidate.resolve())] = candidate

    delta_root = OUTPUTS_DIR / "delta_monitor"
    if delta_root.exists():
        for candidate in delta_root.glob(f"{canonical_id}__*.json"):
            if candidate.is_file():
                related[str(candidate.resolve())] = candidate

    if JOBS_META_DIR.exists():
        for meta_path in JOBS_META_DIR.glob("*.json"):
            try:
                meta_payload = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            meta_run_id = _canonical_run_id_for_listing(str(meta_payload.get("run_id") or ""))
            meta_output_id = _canonical_run_id_for_listing(Path(str(meta_payload.get("output_path") or "")).name)
            if canonical_id and canonical_id in {meta_run_id, meta_output_id}:
                related[str(meta_path.resolve())] = meta_path

    return sorted(related.values(), key=lambda path: str(path))


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _ensure_analysis_job_dirs() -> None:
    JOBS_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    JOBS_META_DIR.mkdir(parents=True, exist_ok=True)
    PREPASS_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


def _build_analysis_job_env() -> Dict[str, str]:
    env = dict(os.environ)
    env.setdefault("ANALYSIS_JOBS_DIR", str(JOBS_OUTPUTS_DIR))
    env.setdefault("ANALYSIS_PREPASS_DIR", str(PREPASS_OUTPUTS_DIR))
    return env


def _job_meta_path(job_id: str) -> Path:
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(job_id))
    return JOBS_META_DIR / f"{safe_id}.json"


def _display_date_from_iso(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return raw[:10]


def _build_job_label(
    *,
    ticker: Any = None,
    company_name: Any = None,
    analysis_date: Any = None,
    created_at: Any = None,
    fallback: str = "",
) -> str:
    ticker_text = str(ticker or "").strip()
    company_text = str(company_name or "").strip()
    date_text = (
        _display_date_from_iso(analysis_date)
        or _display_date_from_iso(created_at)
    )
    if ticker_text and company_text and date_text:
        return f"{ticker_text} {company_text} ({date_text})"
    if ticker_text and company_text:
        return f"{ticker_text} {company_text}"
    if ticker_text and date_text:
        return f"{ticker_text} ({date_text})"
    if ticker_text:
        return ticker_text
    if company_text and date_text:
        return f"{company_text} ({date_text})"
    if company_text:
        return company_text
    if date_text:
        return f"Analysis ({date_text})"
    return str(fallback or "").strip()


def _extract_run_metadata_for_job(run_id: str) -> Dict[str, Any]:
    safe_name = Path(str(run_id or "")).name
    if not safe_name:
        return {}
    path = _resolve_run_artifact_path(safe_name)
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    structured = payload.get("structured_data") if isinstance(payload.get("structured_data"), dict) else {}
    ticker = str(
        structured.get("ticker")
        or payload.get("ticker")
        or ""
    ).strip()
    company_name = str(
        structured.get("company_name")
        or structured.get("company")
        or payload.get("company_name")
        or ""
    ).strip()
    analysis_date = str(
        structured.get("analysis_date")
        or payload.get("analysis_date")
        or payload.get("updated_at")
        or ""
    ).strip()
    label = str(payload.get("label") or "").strip()
    council_meta = structured.get("council_metadata") if isinstance(structured.get("council_metadata"), dict) else {}
    council_contract = council_meta.get("template_contract") if isinstance(council_meta.get("template_contract"), dict) else {}
    top_level_contract = structured.get("template_contract") if isinstance(structured.get("template_contract"), dict) else {}
    template_id = (
        structured.get("template_id")
        or top_level_contract.get("id")
        or council_contract.get("id")
    )
    company_type = structured.get("company_type")
    exchange = str(
        structured.get("exchange")
        or (ticker.split(":", 1)[0] if ":" in ticker else "")
    ).strip()
    return {
        "ticker": ticker,
        "company_name": company_name,
        "analysis_date": analysis_date,
        "label": label,
        "template_id": template_id,
        "company_type": company_type,
        "exchange": exchange,
    }


def _backfill_job_record_metadata(job: Dict[str, Any]) -> bool:
    if not isinstance(job, dict):
        return False

    changed = False
    request_payload = dict(job.get("request") or {})
    output_path = Path(str(job.get("output_path") or ""))
    run_id = Path(str(job.get("run_id") or "")).name

    if not run_id and output_path.exists() and output_path.is_file():
        run_id = output_path.name
        job["run_id"] = run_id
        changed = True

    run_meta = _extract_run_metadata_for_job(run_id) if run_id else {}

    for field in ("ticker", "company_name", "template_id", "company_type", "exchange", "analysis_date"):
        current = job.get(field)
        candidate = run_meta.get(field)
        if candidate in (None, ""):
            candidate = request_payload.get(field)
        if current in (None, "") and candidate not in (None, ""):
            job[field] = candidate
            changed = True

    if request_payload != dict(job.get("request") or {}):
        job["request"] = request_payload

    for field in ("ticker", "company_name", "template_id", "company_type", "exchange"):
        if request_payload.get(field) in (None, "") and job.get(field) not in (None, ""):
            request_payload[field] = job.get(field)
            changed = True

    if request_payload != dict(job.get("request") or {}):
        job["request"] = request_payload

    if str(job.get("label") or "").strip() == "":
        derived_label = (
            str(run_meta.get("label") or "").strip()
            or _build_job_label(
                ticker=job.get("ticker"),
                company_name=job.get("company_name"),
                analysis_date=job.get("analysis_date"),
                created_at=job.get("created_at"),
                fallback=str(run_id or job.get("job_id") or ""),
            )
        )
        if derived_label:
            job["label"] = derived_label
            changed = True

    return changed


def _persist_job_record(job: Dict[str, Any]) -> None:
    if not isinstance(job, dict):
        return
    job_id = str(job.get("job_id") or "").strip()
    if not job_id:
        return
    _ensure_analysis_job_dirs()
    target = _job_meta_path(job_id)
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(job, indent=2), encoding="utf-8")
    tmp.replace(target)


def _load_analysis_jobs_from_disk() -> Dict[str, Dict[str, Any]]:
    _ensure_analysis_job_dirs()
    loaded: Dict[str, Dict[str, Any]] = {}
    now_iso = _utc_now_iso()
    for path in sorted(JOBS_META_DIR.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            continue

        status = str(payload.get("status") or "").strip().lower()
        output_path = Path(str(payload.get("output_path") or ""))
        if status in {"queued", "running"}:
            if output_path.exists():
                payload["status"] = "succeeded"
                payload["run_id"] = str(payload.get("run_id") or output_path.name)
                payload["finished_at"] = str(payload.get("finished_at") or now_iso)
                payload["stage"] = str(payload.get("stage") or "complete")
                payload["stage_message"] = str(payload.get("stage_message") or "Recovered completed run")
                payload["progress_pct"] = int(payload.get("progress_pct") or 100)
            else:
                payload["status"] = "failed"
                payload["finished_at"] = str(payload.get("finished_at") or now_iso)
                payload["error"] = (
                    str(payload.get("error") or "").strip()
                    or "analysis job interrupted during process restart"
                )
                payload["stage"] = str(payload.get("stage") or "failed")
                payload["stage_message"] = str(payload.get("stage_message") or "Interrupted during restart")
            _persist_job_record(payload)

        payload.setdefault("stage", "queued")
        payload.setdefault("stage_message", "")
        payload.setdefault("progress_pct", 0)
        payload.setdefault("last_output_at", "")
        payload.setdefault("instance_id", INSTANCE_ID)
        if _backfill_job_record_metadata(payload):
            _persist_job_record(payload)
        loaded[job_id] = payload
    return loaded


async def _set_job_fields(job_id: str, **fields: Any) -> Optional[Dict[str, Any]]:
    async with ANALYSIS_JOBS_LOCK:
        job = ANALYSIS_JOBS.get(str(job_id))
        if not isinstance(job, dict):
            return None
        for key, value in fields.items():
            job[key] = value
        _persist_job_record(job)
        return job


def _slugify_label(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    return cleaned[:48] if cleaned else fallback


def _validate_job_type(value: Any) -> str:
    job_type = str(value or "company_analysis").strip().lower()
    if job_type not in {"company_analysis", "portfolio_positioning"}:
        raise HTTPException(
            status_code=400,
            detail="job_type must be one of: company_analysis, portfolio_positioning",
        )
    return job_type


def _validate_portfolio_positioning_mode(value: Any) -> str:
    mode = str(value or "fast").strip().lower()
    if mode not in {"fast", "deep"}:
        raise HTTPException(
            status_code=400,
            detail="portfolio_positioning_mode must be one of: fast, deep",
        )
    return mode


def _build_job_run_filename(request: CreateAnalysisJobRequest) -> str:
    job_type = _validate_job_type(getattr(request, "job_type", None))
    ticker = str(request.ticker or "").strip()
    query = str(request.query or "").strip()
    base_hint = str(request.run_label or "").strip() or ticker or query or "analysis"
    fallback = "portfolio_positioning" if job_type == "portfolio_positioning" else "analysis"
    base = _slugify_label(base_hint, fallback=fallback)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    prefix = "portfolio_positioning_job" if job_type == "portfolio_positioning" else "quality_job"
    return f"{prefix}_{base}_{timestamp}.json"


def _validate_stage2_revision_mode(value: str) -> str:
    mode = str(value or "on").strip().lower()
    if mode not in {"on", "off", "auto"}:
        raise HTTPException(
            status_code=400,
            detail="stage2_revision_pass must be one of: on, off, auto",
        )
    return mode


def _build_analysis_job_command(
    request: CreateAnalysisJobRequest,
    output_path: Path,
    *,
    supplementary_context_path: Optional[Path] = None,
    portfolio_context_path: Optional[Path] = None,
) -> List[str]:
    job_type = _validate_job_type(getattr(request, "job_type", None))
    if job_type == "portfolio_positioning":
        if not portfolio_context_path:
            raise HTTPException(status_code=400, detail="Portfolio positioning requires portfolio_context")
        cmd = [sys.executable, str(PROJECT_ROOT / "portfolio_positioning_memo.py")]
        request_query = str(request.query or "").strip()
        if request_query:
            cmd.extend(["--query", request_query])
        cmd.extend(["--portfolio-context-file", str(portfolio_context_path)])
        cmd.extend(["--mode", _validate_portfolio_positioning_mode(request.portfolio_positioning_mode)])
        if request.run_label:
            cmd.extend(["--run-label", str(request.run_label)])
        cmd.extend(["--dump-json", str(output_path)])
        return cmd

    cmd: List[str] = [sys.executable, str(PROJECT_ROOT / "test_quality_mvp.py")]

    request_query = str(request.query or "").strip()
    request_ticker = str(request.ticker or "").strip()
    request_company_name = str(request.company_name or "").strip()
    if not request_query and request_company_name:
        if request_ticker:
            request_query = f"Run full analysis on {request_company_name} ({request_ticker})"
        else:
            request_query = f"Run full analysis on {request_company_name}"

    if request_query:
        cmd.extend(["--query", request_query])
    if request.ticker:
        cmd.extend(["--ticker", str(request.ticker)])
    if request.template_id:
        cmd.extend(["--template-id", str(request.template_id)])
    if request.company_type:
        cmd.extend(["--company-type", str(request.company_type)])
    if request.exchange:
        cmd.extend(["--exchange", str(request.exchange)])
    if request.secondary_chairman_model:
        cmd.extend(["--secondary-chairman-model", str(request.secondary_chairman_model)])

    cmd.extend(["--stage2-revision-pass", _validate_stage2_revision_mode(request.stage2_revision_pass)])
    if request.stage1_only:
        cmd.append("--stage1-only")
    if request.diagnostic_mode:
        cmd.append("--diagnostic-mode")
    if request.reuse_recent_bundle:
        cmd.append("--reuse-recent-bundle")
    if supplementary_context_path:
        cmd.extend(["--supplementary-context-file", str(supplementary_context_path)])

    cmd.extend(["--dump-json", str(output_path)])
    return cmd


def _coerce_form_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _validate_supplementary_mode(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        return None
    allowed = {"upload", "mining_pipeline", "api_pipeline"}
    if text not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid supplementary_mode: {text}",
        )
    return text


def _tail_text(value: str, max_chars: int = ANALYSIS_JOB_LOG_TAIL_CHARS) -> str:
    text = str(value or "")
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _sanitize_ticker_for_dir(ticker: str) -> str:
    text = str(ticker or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_") or "unknown"


def _flatten_prepass_bundle_milestone(item: Any) -> str:
    if isinstance(item, dict):
        milestone = str(item.get("milestone", "")).strip()
        target_window = str(item.get("target_window", "")).strip()
        direction = str(item.get("direction", "")).strip()
        parts = [part for part in [milestone, target_window, direction] if part]
        return " | ".join(parts).strip()
    return str(item or "").strip()


def _build_stage1_prepass_source_rows_from_bundle(
    bundle_path: Path,
    *,
    max_sources: int = 24,
    max_chars_per_source: int = 1600,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    docs = list(payload.get("docs", []) or [])
    docs_sorted = sorted(
        [doc for doc in docs if isinstance(doc, dict)],
        key=lambda row: (
            1 if bool(row.get("price_sensitive", False)) else 0,
            int(row.get("importance_score", 0) or 0),
            str(row.get("published_at", "")),
        ),
        reverse=True,
    )
    rows: List[Dict[str, Any]] = []
    max_sources_safe = max(1, int(max_sources))
    max_chars_safe = max(300, int(max_chars_per_source))
    for idx, doc in enumerate(docs_sorted[:max_sources_safe], 1):
        lines: List[str] = []
        one_line = str(doc.get("one_line", "")).strip()
        if one_line:
            lines.append(one_line)
        key_facts_paragraph = str(doc.get("key_facts_paragraph", "")).strip()
        if key_facts_paragraph:
            lines.append(key_facts_paragraph)
        for point in list(doc.get("key_points", []) or [])[:20]:
            text = str(point or "").strip()
            if text:
                lines.append(f"- {text}")
        for point in list(doc.get("timeline_milestones", []) or [])[:10]:
            text = _flatten_prepass_bundle_milestone(point)
            if text:
                lines.append(f"- Timeline: {text}")
        for point in list(doc.get("catalysts_next_12m", []) or [])[:8]:
            text = str(point or "").strip()
            if text:
                lines.append(f"- Catalyst: {text}")
        for point in list(doc.get("capital_structure", []) or [])[:8]:
            text = str(point or "").strip()
            if text:
                lines.append(f"- Capital: {text}")
        for point in list(doc.get("risks_headwinds", []) or [])[:8]:
            text = str(point or "").strip()
            if text:
                lines.append(f"- Risk: {text}")

        excerpt = "\n".join(lines).strip()
        if not excerpt:
            continue
        if len(excerpt) > max_chars_safe:
            excerpt = excerpt[: max_chars_safe - 3].rstrip() + "..."

        importance_score = int(doc.get("importance_score", 0) or 0)
        material_signal_score = max(0, min(8, int(round(importance_score / 12.5))))
        rows.append(
            {
                "source_id": f"S{len(rows) + 1}",
                "title": str(doc.get("title", "")).strip() or f"Bundled Source {idx}",
                "url": str(doc.get("pdf_url", "")).strip() or str(doc.get("url", "")).strip(),
                "published_at": str(doc.get("published_at", "")).strip(),
                "decode_status": "prepass_bundle",
                "decoded": True,
                "excerpt": excerpt,
                "material_signal_score": material_signal_score,
                "bundle_importance_score": importance_score,
                "bundle_price_sensitive": bool(doc.get("price_sensitive", False)),
            }
        )

    selection_audit = payload.get("selection_audit", {}) or {}
    meta = {
        "bundle_path": str(bundle_path),
        "generated_at_utc": str(payload.get("generated_at_utc", "")),
        "docs_in_bundle": int(len(docs)),
        "rows_built": int(len(rows)),
        "min_importance_score": int(
            ((payload.get("injection_policy", {}) or {}).get("min_importance_score", 0) or 0)
        ),
        "kept_for_injection": int(payload.get("kept_for_injection", 0) or 0),
        "dropped_as_unimportant": int(payload.get("dropped_as_unimportant", 0) or 0),
        "dropped_deduplicated": int(payload.get("dropped_deduplicated", 0) or 0),
        "dropped_after_selection": int(payload.get("dropped_after_selection", 0) or 0),
        "selection_counts": payload.get("selection_counts", {}) or {},
        "selection_audit_high_importance_dropped_count": int(
            len(list(selection_audit.get("high_importance_dropped", []) or []))
        ),
    }
    return rows, meta


async def _run_subprocess_capture(
    *,
    cmd: List[str],
    cwd: Path,
) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_bytes, stderr_bytes = await proc.communicate()
    stdout_text = (stdout_bytes or b"").decode("utf-8", errors="replace")
    stderr_text = (stderr_bytes or b"").decode("utf-8", errors="replace")
    return int(proc.returncode or 0), stdout_text, stderr_text


async def _prepare_stage1_authoritative_prepass_bundle(
    *,
    ticker: str,
    query_hint: str,
    exchange: str,
    exchange_retrieval_params: Optional[Dict[str, Any]] = None,
    company_name: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not str(ticker or "").strip():
        raise RuntimeError("ticker_required_for_authoritative_prepass")

    PREPASS_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = (
        PREPASS_OUTPUTS_DIR
        / f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{_sanitize_ticker_for_dir(ticker)}_api_prepass"
    )

    query_seed = str(company_name or "").strip() or str(query_hint or "").strip() or str(ticker or "").strip()
    query_seed = re.sub(r"\s+", " ", query_seed).strip()[:120]
    retrieval_query = f"Latest material filings, announcements, and investor updates for {query_seed}"

    retrieval_params = dict(exchange_retrieval_params or {})
    target_price_sensitive = int(retrieval_params.get("target_price_sensitive_default", 10) or 10)
    target_non_price_sensitive = int(
        retrieval_params.get("target_non_price_sensitive_default", 10) or 10
    )
    if (target_price_sensitive + target_non_price_sensitive) < 20:
        target_non_price_sensitive = max(
            target_non_price_sensitive,
            20 - max(0, target_price_sensitive),
        )
    top_default = max(1, target_price_sensitive + target_non_price_sensitive)
    max_sources_default = int(retrieval_params.get("max_sources_default", 0) or 0)
    lookback_days_default = int(retrieval_params.get("lookback_days_default", 0) or 0)

    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "test_perplexity_pdf_dump.py"),
        "--query",
        retrieval_query,
        "--ticker",
        str(ticker),
        "--output-dir",
        str(output_dir),
        "--depth",
        "deep",
        "--top",
        str(top_default),
        "--target-price-sensitive",
        str(target_price_sensitive),
        "--target-non-price-sensitive",
        str(target_non_price_sensitive),
    ]
    if max_sources_default > 0:
        cmd.extend(["--max-sources", str(max_sources_default)])
    if lookback_days_default > 0:
        cmd.extend(["--lookback-days", str(lookback_days_default)])
    if str(exchange or "").strip():
        cmd.extend(["--exchange", str(exchange).strip().lower()])

    returncode, stdout_text, stderr_text = await _run_subprocess_capture(
        cmd=cmd,
        cwd=PROJECT_ROOT,
    )
    if returncode != 0:
        raise RuntimeError(
            "authoritative_prepass_failed "
            f"rc={returncode} "
            f"stderr_tail={_tail_text(str(stderr_text or '').strip(), max_chars=1200)} "
            f"stdout_tail={_tail_text(str(stdout_text or '').strip(), max_chars=1200)}"
        )

    manifest_path = output_dir / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(f"authoritative_prepass_manifest_missing:{manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    bundle_path_raw = str(manifest.get("injection_bundle_json", "")).strip()
    if bundle_path_raw:
        bundle_path_candidate = Path(bundle_path_raw)
        bundle_path = (
            bundle_path_candidate.resolve()
            if bundle_path_candidate.is_absolute()
            else (PROJECT_ROOT / bundle_path_candidate).resolve()
        )
    else:
        bundle_path = (output_dir / "injection_bundle.json").resolve()
    if not bundle_path.exists() or not bundle_path.is_file():
        raise RuntimeError(f"authoritative_prepass_bundle_missing:{bundle_path}")

    rows, meta = _build_stage1_prepass_source_rows_from_bundle(bundle_path)
    meta["strategy"] = "authoritative_prepass_bundle"
    meta["output_dir"] = str(output_dir)
    meta["prepass_top"] = top_default
    meta["prepass_target_price_sensitive"] = target_price_sensitive
    meta["prepass_target_non_price_sensitive"] = target_non_price_sensitive
    meta["prepass_max_sources"] = max_sources_default
    meta["prepass_lookback_days"] = lookback_days_default
    meta["prepass_retrieved_sources"] = _manifest_count(
        manifest.get("retrieved_sources", 0)
    )
    meta["prepass_candidate_sources_considered"] = _manifest_count(
        manifest.get("candidate_sources_considered", 0)
    )
    meta["prepass_candidate_allowlisted_sources"] = _manifest_count(
        manifest.get("candidate_allowlisted_sources", 0)
    )
    meta["prepass_candidate_pdfs_in_window"] = _manifest_count(
        manifest.get("candidate_pdfs_in_window", 0)
    )
    meta["prepass_selected_primary_candidates"] = _manifest_count(
        manifest.get("selected_primary_candidates", 0)
    )
    meta["prepass_written_files"] = _manifest_count(manifest.get("written_files", 0))
    return rows, meta


def _extract_progress_update_from_line(line: str) -> Optional[Dict[str, Any]]:
    raw = str(line or "").strip()
    if not raw:
        return None
    message = raw
    if "[test_quality_mvp]" in message:
        message = message.split("[test_quality_mvp]", 1)[1].strip()
    elif "[council]" in message:
        message = message.split("[council]", 1)[1].strip()
    lower = message.lower()

    if lower.startswith("worker progress:"):
        completed_match = re.search(r"completed=(\d+)/(\d+)", message, flags=re.IGNORECASE)
        if completed_match:
            completed = max(0, int(completed_match.group(1)))
            total = max(1, int(completed_match.group(2)))
            worker_local_pct = int(round((completed / total) * 100))
            # Prepass worker progress should live between the bundle-start marker (10)
            # and the bundle-ready marker (16) so the UI advances smoothly.
            progress_pct = 10 + int(round((worker_local_pct / 100.0) * 5))
            return {
                "stage": "prepass",
                "progress_pct": progress_pct,
                "stage_message": f"Prepass: {completed}/{total} docs processed",
            }

    if "stage1 progress" in lower:
        pct_match = re.search(r"pct=(\d+)", message, flags=re.IGNORECASE)
        completed_match = re.search(r"completed=(\d+)/(\d+)", message, flags=re.IGNORECASE)
        model_match = re.search(r"model=([^,]+)", message, flags=re.IGNORECASE)
        status_match = re.search(r"status=([a-z_]+)", message, flags=re.IGNORECASE)
        local_pct = int(pct_match.group(1)) if pct_match else 0
        stage_message = message
        if completed_match and not pct_match:
            try:
                completed = int(completed_match.group(1))
                total = max(1, int(completed_match.group(2)))
                local_pct = int(round((completed / total) * 100))
            except Exception:
                local_pct = 0
        return {
            "stage": "stage1",
            "progress_pct": int(_scale_stage_progress("stage1", local_pct)),
            "stage_message": stage_message,
            "stage1_model": model_match.group(1).strip() if model_match else "",
            "stage1_status": status_match.group(1).strip() if status_match else "",
        }

    for marker, stage_key, pct in _ANALYSIS_PROGRESS_MARKERS:
        if marker in lower:
            return {
                "stage": stage_key,
                "progress_pct": int(pct),
                "stage_message": message,
            }
    return None


async def _append_job_stream_line(job_id: str, key: str, text: str) -> None:
    progress_update = _extract_progress_update_from_line(text)
    async with ANALYSIS_JOBS_LOCK:
        job = ANALYSIS_JOBS.get(str(job_id))
        if not isinstance(job, dict):
            return
        current = str(job.get(key) or "")
        job[key] = _tail_text(current + str(text or ""))
        job["last_output_at"] = _utc_now_iso()
        if progress_update:
            old_pct = 0
            try:
                old_pct = int(job.get("progress_pct") or 0)
            except Exception:
                old_pct = 0
            current_stage = str(job.get("stage") or "")
            new_stage = str(progress_update.get("stage") or current_stage)
            current_rank = _analysis_stage_rank(current_stage)
            new_rank = _analysis_stage_rank(new_stage)
            new_pct = int(progress_update.get("progress_pct") or old_pct)
            if new_rank > current_rank:
                job["stage"] = new_stage
                if old_pct >= 100 and new_stage != "complete":
                    job["progress_pct"] = new_pct
                else:
                    job["progress_pct"] = max(old_pct, new_pct)
            elif new_pct >= old_pct:
                job["progress_pct"] = new_pct
                job["stage"] = new_stage
            job["stage_message"] = str(progress_update.get("stage_message") or "")
            _persist_job_record(job)


async def _consume_process_stream(
    *,
    job_id: str,
    stream: Optional[asyncio.StreamReader],
    key: str,
) -> None:
    if stream is None:
        return
    while True:
        line = await stream.readline()
        if not line:
            break
        text = line.decode("utf-8", errors="replace")
        await _append_job_stream_line(job_id=job_id, key=key, text=text)


async def _run_analysis_job(
    *,
    job_id: str,
    command: List[str],
    output_path: Path,
    request_payload: Optional[Dict[str, Any]] = None,
    cleanup_paths: Optional[List[Path]] = None,
) -> None:
    if await _set_job_fields(
        job_id,
        status="running",
        started_at=_utc_now_iso(),
        stage="initializing",
        stage_message="Subprocess started",
        progress_pct=2,
    ) is None:
        return

    process = None
    command_to_run = list(command)
    cleanup_list = list(cleanup_paths or [])
    try:
        supplementary_mode = _validate_supplementary_mode((request_payload or {}).get("supplementary_mode"))
        if supplementary_mode in {"mining_pipeline", "api_pipeline"}:
            await _set_job_fields(
                job_id,
                stage="initializing",
                stage_message="Generating supplementary packet",
                progress_pct=4,
            )
        generated_context_path, generated_cleanup_paths, generation_meta = await _prepare_generated_supplementary_for_job(
            job_id=job_id,
            request_payload=request_payload or {},
        )
        if generated_context_path:
            command_to_run.extend(["--supplementary-context-file", str(generated_context_path)])
            cleanup_list.extend(generated_cleanup_paths)
            await _set_job_fields(
                job_id,
                stage="initializing",
                stage_message="Generated supplementary packet",
                progress_pct=6,
            )
        process = await asyncio.create_subprocess_exec(
            *command_to_run,
            cwd=str(PROJECT_ROOT),
            env=_build_analysis_job_env(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await _set_job_fields(job_id, pid=int(process.pid or 0))

        stdout_task = asyncio.create_task(
            _consume_process_stream(
                job_id=job_id,
                stream=process.stdout,
                key="stdout_tail",
            )
        )
        stderr_task = asyncio.create_task(
            _consume_process_stream(
                job_id=job_id,
                stream=process.stderr,
                key="stderr_tail",
            )
        )

        await process.wait()
        await asyncio.gather(stdout_task, stderr_task)
        returncode = int(process.returncode or 0)

        status = "succeeded" if returncode == 0 and output_path.exists() else "failed"
        current_progress = 0
        async with ANALYSIS_JOBS_LOCK:
            current_job = ANALYSIS_JOBS.get(str(job_id))
            if isinstance(current_job, dict):
                try:
                    current_progress = int(current_job.get("progress_pct") or 0)
                except Exception:
                    current_progress = 0
        fields: Dict[str, Any] = {
            "status": status,
            "finished_at": _utc_now_iso(),
            "returncode": returncode,
            "run_id": output_path.name if output_path.exists() else "",
            "progress_pct": 100 if status == "succeeded" else current_progress,
            "stage": "complete" if status == "succeeded" else "failed",
            "stage_message": (
                "Run completed successfully"
                if status == "succeeded"
                else "Run failed"
            ),
        }
        if status == "succeeded":
            fields["error"] = ""
        else:
            fields["error"] = (
                f"analysis subprocess failed (returncode={returncode})"
                if returncode != 0
                else "analysis subprocess did not produce output artifact"
            )
        await _set_job_fields(job_id, **fields)
    except Exception as exc:
        fields: Dict[str, Any] = {
            "status": "failed",
            "finished_at": _utc_now_iso(),
            "error": f"analysis job execution error: {exc}",
        }
        if process is not None and process.returncode is not None:
            fields["returncode"] = int(process.returncode)
        await _set_job_fields(job_id, **fields)
    finally:
        for path in cleanup_list:
            try:
                if isinstance(path, Path) and path.exists():
                    path.unlink()
            except Exception:
                pass


def _public_job_view(job: Dict[str, Any]) -> Dict[str, Any]:
    status = str(job.get("status") or "")
    stage = str(job.get("stage") or "")
    stage_message = str(job.get("stage_message") or "")
    try:
        progress_pct = int(job.get("progress_pct") or 0)
    except Exception:
        progress_pct = 0

    if status in {"queued", "running"}:
        inferred = _extract_progress_update_from_line(stage_message)
        if inferred:
            inferred_stage = str(inferred.get("stage") or stage)
            inferred_pct = int(inferred.get("progress_pct") or progress_pct)
            if _analysis_stage_rank(inferred_stage) > _analysis_stage_rank(stage):
                stage = inferred_stage
                progress_pct = inferred_pct
            elif progress_pct >= 100 and stage != "complete":
                stage = inferred_stage
                progress_pct = inferred_pct

        if status == "running" and stage != "complete":
            progress_pct = min(progress_pct, 99)
        stage_range = _ANALYSIS_STAGE_RANGES.get(stage)
        if stage_range:
            progress_pct = min(progress_pct, stage_range[1])

    request_payload = dict(job.get("request") or {})
    display_ticker = str(job.get("ticker") or request_payload.get("ticker") or "")
    display_company_name = str(job.get("company_name") or request_payload.get("company_name") or "")
    display_analysis_date = str(job.get("analysis_date") or "")
    display_label = (
        str(job.get("label") or "").strip()
        or _build_job_label(
            ticker=display_ticker,
            company_name=display_company_name,
            analysis_date=display_analysis_date,
            created_at=job.get("created_at"),
            fallback=str(job.get("run_id") or job.get("job_id") or ""),
        )
    )
    return {
        "id": str(job.get("job_id") or ""),
        "job_id": str(job.get("job_id") or ""),
        "status": status,
        "stage": stage,
        "stage_message": stage_message,
        "progress_pct": progress_pct,
        "instance_id": str(job.get("instance_id") or INSTANCE_ID),
        "created_at": str(job.get("created_at") or ""),
        "started_at": str(job.get("started_at") or ""),
        "finished_at": str(job.get("finished_at") or ""),
        "last_output_at": str(job.get("last_output_at") or ""),
        "run_id": str(job.get("run_id") or ""),
        "output_path": str(job.get("output_path") or ""),
        "returncode": job.get("returncode"),
        "pid": job.get("pid"),
        "error": str(job.get("error") or ""),
        "stdout_tail": str(job.get("stdout_tail") or ""),
        "stderr_tail": str(job.get("stderr_tail") or ""),
        "request": request_payload,
        "ticker": display_ticker,
        "company_name": display_company_name,
        "template_id": (
            job.get("template_id")
            if job.get("template_id") is not None
            else request_payload.get("template_id")
        ),
        "company_type": (
            job.get("company_type")
            if job.get("company_type") is not None
            else request_payload.get("company_type")
        ),
        "exchange": str(job.get("exchange") or request_payload.get("exchange") or ""),
        "label": display_label,
        "analysis_date": display_analysis_date,
        "is_synthetic": bool(job.get("is_synthetic")),
    }


def _synthetic_job_id_for_run(run_id: str) -> str:
    return f"{SYNTHETIC_RUN_JOB_PREFIX}{Path(str(run_id or '')).name}"


def _run_id_from_synthetic_job_id(job_id: str) -> str:
    raw = str(job_id or "")
    if not raw.startswith(SYNTHETIC_RUN_JOB_PREFIX):
        return ""
    return Path(raw[len(SYNTHETIC_RUN_JOB_PREFIX):]).name


def _build_synthetic_job_record_from_run(run_meta: Dict[str, Any]) -> Dict[str, Any]:
    run_id = Path(str(run_meta.get("id") or "")).name
    ticker = str(run_meta.get("ticker") or "")
    exchange = ticker.split(":", 1)[0] if ":" in ticker else ""
    ts = str(run_meta.get("updated_at") or run_meta.get("analysis_date") or _utc_now_iso())
    return {
        "job_id": _synthetic_job_id_for_run(run_id),
        "status": "succeeded",
        "stage": "complete",
        "stage_message": "Recovered from run artifact",
        "progress_pct": 100,
        "instance_id": INSTANCE_ID,
        "created_at": ts,
        "started_at": ts,
        "finished_at": ts,
        "last_output_at": ts,
        "output_path": str(_resolve_run_artifact_path(run_id)),
        "returncode": 0,
        "pid": None,
        "run_id": run_id,
        "error": "",
        "stdout_tail": "",
        "stderr_tail": "",
        "request": {
            "ticker": ticker,
            "company_name": str(run_meta.get("company_name") or ""),
            "template_id": None,
            "company_type": None,
            "exchange": exchange,
        },
        "ticker": ticker,
        "company_name": str(run_meta.get("company_name") or ""),
        "exchange": exchange,
        "label": str(run_meta.get("label") or run_id),
        "analysis_date": str(run_meta.get("analysis_date") or ""),
        "is_synthetic": True,
    }


def _extract_current_price_candidates_from_text(text: str) -> List[float]:
    """
    Extract plausible current-share-price mentions from markdown/text blobs.
    Targets lines containing "current price"/"share price" and AUD values.
    """
    if not text:
        return []
    candidates: List[float] = []
    trigger = re.compile(r"\b(current\s+(?:share\s+)?price|share\s+price)\b", re.IGNORECASE)
    aud_price = re.compile(r"A\$\s*([0-9]+(?:\.[0-9]+)?)")
    for raw_line in str(text).splitlines():
        line = raw_line.strip()
        if not line or not trigger.search(line):
            continue
        for match in aud_price.finditer(line):
            try:
                price = float(match.group(1))
            except Exception:
                continue
            if 0 < price < 1000:
                candidates.append(price)
    return candidates


def _extract_normalized_facts_from_prefixed_query(query_text: str) -> Dict[str, Any]:
    """
    Parse a leading normalized_facts JSON block from query text.
    Expected prefix shape:
      { "normalized_facts": { ... } }
      <template prompt text...>
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
    facts = parsed.get("normalized_facts")
    if not isinstance(facts, dict):
        return {}
    return dict(facts)


def _extract_market_facts_from_artifact_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recover market_facts robustly from run/checkpoint artifacts.
    Fallback order:
    1) input_audit.market_facts
    2) metadata.market_facts
    3) normalized_facts parsed from effective_query
    4) normalized_facts parsed from per_model_research_runs[*].result.query
    """
    if not isinstance(payload, dict):
        return {}

    def _valid_market_facts(candidate: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(candidate, dict):
            return None
        normalized = candidate.get("normalized_facts")
        if isinstance(normalized, dict) and any(v is not None for v in normalized.values()):
            return dict(candidate)
        return None

    input_audit = payload.get("input_audit")
    if isinstance(input_audit, dict):
        found = _valid_market_facts(input_audit.get("market_facts"))
        if found:
            return found

    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        found = _valid_market_facts(metadata.get("market_facts"))
        if found:
            return found

    # Parse normalized_facts directly from prefixed query text.
    effective_query = str(payload.get("effective_query") or "")
    parsed_from_query = _extract_normalized_facts_from_prefixed_query(effective_query)
    if parsed_from_query:
        return {
            "status": "reconstructed",
            "reason": "reconstructed_from_effective_query",
            "normalized_facts": parsed_from_query,
        }

    # Parse from per-model research query payloads, keeping the richest block.
    best_facts: Dict[str, Any] = {}
    best_score = -1
    per_model_runs = (metadata or {}).get("per_model_research_runs") if isinstance(metadata, dict) else None
    if isinstance(per_model_runs, list):
        for row in per_model_runs:
            if not isinstance(row, dict):
                continue
            result = row.get("result")
            if not isinstance(result, dict):
                continue
            candidate_query = str(result.get("query") or "")
            parsed = _extract_normalized_facts_from_prefixed_query(candidate_query)
            if not parsed:
                continue
            score = len([k for k, v in parsed.items() if v is not None])
            if score > best_score:
                best_score = score
                best_facts = parsed

    if best_facts:
        return {
            "status": "reconstructed",
            "reason": "reconstructed_from_per_model_query",
            "normalized_facts": best_facts,
        }

    return {}


def _infer_current_price_from_artifact(payload: Dict[str, Any]) -> Optional[float]:
    """
    Infer current share price from Stage 1 responses when Stage 3 omitted it.
    Uses median across candidate model outputs to reduce outlier impact.
    """
    if not isinstance(payload, dict):
        return None

    def _as_price(value: Any) -> Optional[float]:
        try:
            n = float(value)
        except Exception:
            return None
        if not (0 < n < 1000):
            return None
        return round(n, 6)

    # 1) Strongest source: deterministic prepass/reconstructed normalized facts.
    market_facts = _extract_market_facts_from_artifact_payload(payload)
    normalized = market_facts.get("normalized_facts") if isinstance(market_facts, dict) else None
    if isinstance(normalized, dict):
        price = _as_price(normalized.get("current_price"))
        if price is not None:
            return price

    # 2) Stage 3 provenance, if present.
    stage3 = payload.get("stage3_result") if isinstance(payload.get("stage3_result"), dict) else {}
    if stage3:
        structured = stage3.get("structured_data")
        if isinstance(structured, dict):
            provenance = structured.get("market_data_provenance")
            if isinstance(provenance, dict):
                price = _as_price(provenance.get("prepass_current_price"))
                if price is not None:
                    return price

    stage1_rows = payload.get("stage1_results")
    if not isinstance(stage1_rows, list):
        return None

    values: List[float] = []
    for row in stage1_rows:
        if not isinstance(row, dict):
            continue
        values.extend(_extract_current_price_candidates_from_text(str(row.get("response") or "")))

    if not values:
        return None

    values = sorted(values)
    n = len(values)
    if n % 2 == 1:
        median_val = values[n // 2]
    else:
        median_val = (values[(n // 2) - 1] + values[n // 2]) / 2.0
    return round(float(median_val), 6)


def _normalize_timeline_rows_for_api(raw_timeline: Any) -> List[Dict[str, Any]]:
    """
    Normalize timeline rows for UI consumers.
    Accepts either structured objects or plain strings like:
    "Q1-Q2 2026: Initial Drawdown on US$25M Facility"
    """
    if not isinstance(raw_timeline, list):
        return []

    out: List[Dict[str, Any]] = []
    period_pattern = re.compile(
        r"\b(Q[1-4](?:\s*[-/]\s*Q[1-4])?\s*20\d{2}|H[12]\s*20\d{2}|20\d{2})\b",
        re.IGNORECASE,
    )
    for idx, item in enumerate(raw_timeline):
        if isinstance(item, dict):
            milestone = str(
                item.get("milestone")
                or item.get("event")
                or item.get("name")
                or item.get("goal")
                or item.get("title")
                or ""
            ).strip()
            target_period = str(
                item.get("target_period")
                or item.get("targetPeriod")
                or item.get("period")
                or item.get("when")
                or item.get("date")
                or ""
            ).strip()
            status = str(item.get("status") or item.get("current_status") or item.get("state") or "unspecified").strip()
            confidence = item.get("confidence_pct")
            if confidence is None:
                confidence = item.get("certainty_pct")
            out.append(
                {
                    "milestone": milestone or f"Milestone {idx + 1}",
                    "target_period": target_period,
                    "status": status or "unspecified",
                    "confidence_pct": confidence,
                    "primary_risk": str(item.get("primary_risk") or item.get("risk") or "").strip(),
                }
            )
            continue

        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            milestone = text
            target_period = ""

            colon_split = re.match(r"^([^:]{2,40}):\s*(.+)$", text)
            if colon_split:
                lhs = str(colon_split.group(1) or "").strip()
                rhs = str(colon_split.group(2) or "").strip()
                if lhs and period_pattern.search(lhs):
                    target_period = lhs
                    milestone = rhs or text

            if not target_period:
                period_match = period_pattern.search(text)
                if period_match:
                    target_period = str(period_match.group(1) or "").strip()
                    stripped = re.sub(r"^[:\-\s]+", "", text.replace(period_match.group(0), "")).strip()
                    milestone = stripped or text

            out.append(
                {
                    "milestone": milestone or f"Milestone {idx + 1}",
                    "target_period": target_period,
                    "status": "unspecified",
                    "confidence_pct": None,
                    "primary_risk": "",
                }
            )
            continue

    return _cap_previous_timeline_rows(out, max_previous=1)


def _timeline_period_to_quarter_index(period: Any) -> Optional[int]:
    text = str(period or "").strip().upper()
    if not text:
        return None

    q_range = re.search(r"\bQ([1-4])\s*[-/]\s*Q([1-4])\s*(20\d{2})\b", text)
    if q_range:
        q1 = int(q_range.group(1))
        q2 = int(q_range.group(2))
        year = int(q_range.group(3))
        return (year * 4) + max(q1, q2)

    q_single = re.search(r"\bQ([1-4])\s*(20\d{2})\b", text)
    if q_single:
        quarter = int(q_single.group(1))
        year = int(q_single.group(2))
        return (year * 4) + quarter

    half = re.search(r"\bH([12])\s*(20\d{2})\b", text)
    if half:
        h = int(half.group(1))
        year = int(half.group(2))
        quarter = 2 if h == 1 else 4
        return (year * 4) + quarter

    year_only = re.search(r"\b(20\d{2})\b", text)
    if year_only:
        year = int(year_only.group(1))
        return (year * 4) + 4

    return None


def _current_quarter_index(now_utc: Optional[datetime] = None) -> int:
    now = now_utc or datetime.now(timezone.utc)
    quarter = ((now.month - 1) // 3) + 1
    return (now.year * 4) + quarter


def _status_indicates_past(status: Any) -> bool:
    low = str(status or "").strip().lower()
    if not low:
        return False
    return any(
        token in low
        for token in (
            "achieved",
            "completed",
            "done",
            "delivered",
            "closed",
            "finished",
            "met",
            "launched",
            "commissioned",
            "first gold",
        )
    )


def _status_indicates_future(status: Any) -> bool:
    low = str(status or "").strip().lower()
    if not low:
        return False
    return any(
        token in low
        for token in (
            "planned",
            "at_risk",
            "at risk",
            "pending",
            "upcoming",
            "target",
            "on track",
            "on_track",
            "current",
            "in progress",
            "in_progress",
            "speculative",
            "proposed",
        )
    )


def _timeline_row_is_previous(row: Dict[str, Any], now_utc: Optional[datetime] = None) -> bool:
    status = row.get("status")
    if _status_indicates_past(status):
        return True
    if _status_indicates_future(status):
        return False

    quarter_idx = _timeline_period_to_quarter_index(
        row.get("target_period")
        or row.get("targetPeriod")
        or row.get("period")
        or row.get("date")
    )
    if quarter_idx is None:
        return False
    return quarter_idx < _current_quarter_index(now_utc)


def _cap_previous_timeline_rows(
    rows: List[Dict[str, Any]],
    *,
    max_previous: int = 1,
    now_utc: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(rows, list) or max_previous < 0:
        return []
    if not rows:
        return []

    indexed: List[Tuple[int, Dict[str, Any], bool, Optional[int]]] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        is_previous = _timeline_row_is_previous(row, now_utc=now_utc)
        quarter_idx = _timeline_period_to_quarter_index(
            row.get("target_period")
            or row.get("targetPeriod")
            or row.get("period")
            or row.get("date")
        )
        indexed.append((idx, row, is_previous, quarter_idx))

    previous_rows = [item for item in indexed if item[2]]
    if len(previous_rows) <= max_previous:
        return [item[1] for item in indexed]

    previous_rows_sorted = sorted(
        previous_rows,
        key=lambda item: (
            item[3] is not None,
            item[3] if item[3] is not None else -1,
            item[0],
        ),
        reverse=True,
    )
    keep_previous_idx = {item[0] for item in previous_rows_sorted[:max_previous]}
    filtered: List[Dict[str, Any]] = []
    for idx, row, is_previous, _ in indexed:
        if not is_previous or idx in keep_previous_idx:
            filtered.append(row)
    return filtered


def _extract_period_from_text(text: str) -> str:
    if not text:
        return ""
    match = re.search(
        r"\b(Q[1-4](?:\s*[-/]\s*Q[1-4])?\s*20\d{2}|H[12]\s*20\d{2}|20\d{2})\b",
        str(text),
        re.IGNORECASE,
    )
    return str(match.group(1) or "").strip() if match else ""


def _cap_previous_catalyst_rows(
    rows: List[Any],
    *,
    max_previous: int = 1,
    now_utc: Optional[datetime] = None,
) -> List[Any]:
    if not isinstance(rows, list) or max_previous < 0:
        return []
    if not rows:
        return []

    indexed: List[Tuple[int, Any, bool, Optional[int]]] = []
    for idx, row in enumerate(rows):
        status = ""
        period = ""
        if isinstance(row, dict):
            status = str(
                row.get("status")
                or row.get("state")
                or row.get("current_status")
                or ""
            ).strip()
            period = str(
                row.get("target_period")
                or row.get("targetPeriod")
                or row.get("period")
                or row.get("when")
                or row.get("date")
                or ""
            ).strip()
            if not period:
                period = _extract_period_from_text(
                    str(row.get("name") or row.get("title") or row.get("milestone") or row.get("catalyst") or "")
                )
        elif isinstance(row, str):
            status = row
            period = _extract_period_from_text(row)
        else:
            continue

        tmp = {
            "status": status,
            "target_period": period,
        }
        is_previous = _timeline_row_is_previous(tmp, now_utc=now_utc)
        quarter_idx = _timeline_period_to_quarter_index(period)
        indexed.append((idx, row, is_previous, quarter_idx))

    previous_rows = [item for item in indexed if item[2]]
    if len(previous_rows) <= max_previous:
        return [item[1] for item in indexed]

    previous_rows_sorted = sorted(
        previous_rows,
        key=lambda item: (
            item[3] is not None,
            item[3] if item[3] is not None else -1,
            item[0],
        ),
        reverse=True,
    )
    keep_previous_idx = {item[0] for item in previous_rows_sorted[:max_previous]}
    filtered: List[Any] = []
    for idx, row, is_previous, _ in indexed:
        if not is_previous or idx in keep_previous_idx:
            filtered.append(row)
    return filtered


def _to_float(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    if not (num == num):  # NaN
        return None
    return num


def _safe_float(value: Any) -> Optional[float]:
    num = _to_float(value)
    if num is None:
        return None
    if abs(num) > 1e18:
        return None
    return num


def _pick_score_total(score_obj: Any) -> Optional[float]:
    if isinstance(score_obj, dict):
        return _safe_float(score_obj.get("total"))
    return _safe_float(score_obj)


def _pick_nested(mapping: Dict[str, Any], *keys: str) -> Optional[float]:
    cur: Any = mapping
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return _safe_float(cur)


def _compute_prob_weighted_target(
    scenario_targets: Dict[str, Any],
    scenario_probabilities: Dict[str, Any],
) -> Optional[float]:
    weighted = 0.0
    prob_sum = 0.0
    for key in ("bear", "base", "bull"):
        target = _safe_float(scenario_targets.get(key))
        prob = _safe_float(scenario_probabilities.get(key))
        if target is None or prob is None:
            continue
        if prob <= 1.0:
            prob = prob * 100.0
        if prob <= 0.0:
            continue
        weighted += target * prob
        prob_sum += prob
    if prob_sum <= 0.0:
        return _safe_float(scenario_targets.get("base"))
    return weighted / prob_sum


def _build_summary_fields(structured: Dict[str, Any], freshness: Dict[str, Any]) -> Dict[str, Any]:
    if str(structured.get("analysis_kind") or "").strip() == "portfolio_positioning":
        diagnosis = structured.get("portfolio_diagnosis") if isinstance(structured.get("portfolio_diagnosis"), dict) else {}
        strategic_view = structured.get("strategic_view") if isinstance(structured.get("strategic_view"), dict) else {}
        current_vs_ideal = structured.get("current_vs_ideal") if isinstance(structured.get("current_vs_ideal"), dict) else {}
        return {
            "analysis_kind": "portfolio_positioning",
            "analysis_date": str(structured.get("analysis_date") or "").strip(),
            "current_cash_pct": _safe_float(diagnosis.get("current_cash_pct")),
            "cash_target_pct": _safe_float(strategic_view.get("cash_target_pct")),
            "primary_theme": str(strategic_view.get("primary_theme") or "").strip(),
            "secondary_theme": str(strategic_view.get("secondary_theme") or "").strip(),
            "main_overweights": current_vs_ideal.get("main_overweights") if isinstance(current_vs_ideal.get("main_overweights"), list) else [],
            "main_underweights": current_vs_ideal.get("main_underweights") if isinstance(current_vs_ideal.get("main_underweights"), list) else [],
        }

    market_data = structured.get("market_data") if isinstance(structured.get("market_data"), dict) else {}
    council_meta = structured.get("council_metadata") if isinstance(structured.get("council_metadata"), dict) else {}
    council_contract = council_meta.get("template_contract") if isinstance(council_meta.get("template_contract"), dict) else {}
    top_level_contract = structured.get("template_contract") if isinstance(structured.get("template_contract"), dict) else {}
    price_targets = structured.get("price_targets") if isinstance(structured.get("price_targets"), dict) else {}
    scenario_targets = price_targets.get("scenario_targets") if isinstance(price_targets.get("scenario_targets"), dict) else {}
    scenario_probabilities = (
        price_targets.get("scenario_probabilities")
        if isinstance(price_targets.get("scenario_probabilities"), dict)
        else {}
    )
    scenario_12m = scenario_targets.get("12m") if isinstance(scenario_targets.get("12m"), dict) else {}
    scenario_24m = scenario_targets.get("24m") if isinstance(scenario_targets.get("24m"), dict) else {}
    probs_12m = scenario_probabilities.get("12m") if isinstance(scenario_probabilities.get("12m"), dict) else {}
    probs_24m = scenario_probabilities.get("24m") if isinstance(scenario_probabilities.get("24m"), dict) else {}

    quality_total = _pick_score_total(structured.get("quality_score"))
    value_total = _pick_score_total(structured.get("value_score"))
    rating = str(
        (
            (structured.get("investment_recommendation") or {}).get("rating")
            if isinstance(structured.get("investment_recommendation"), dict)
            else ""
        )
        or (
            (structured.get("investment_verdict") or {}).get("rating")
            if isinstance(structured.get("investment_verdict"), dict)
            else ""
        )
        or ""
    ).strip()
    conviction = str(
        (
            (structured.get("investment_recommendation") or {}).get("conviction")
            if isinstance(structured.get("investment_recommendation"), dict)
            else ""
        )
        or (
            (structured.get("investment_verdict") or {}).get("conviction")
            if isinstance(structured.get("investment_verdict"), dict)
            else ""
        )
        or ""
    ).strip()

    current_price = _safe_float(market_data.get("current_price"))
    if current_price is None:
        current_price = _safe_float(price_targets.get("current_price"))

    target_12m_base = _safe_float(price_targets.get("target_12m"))
    if target_12m_base is None:
        target_12m_base = _safe_float(scenario_12m.get("base"))
    target_24m_base = _safe_float(price_targets.get("target_24m"))
    if target_24m_base is None:
        target_24m_base = _safe_float(scenario_24m.get("base"))

    prob_weighted_12m = _safe_float(price_targets.get("prob_weighted_target_12m"))
    if prob_weighted_12m is None:
        prob_weighted_12m = _compute_prob_weighted_target(scenario_12m, probs_12m)

    prob_weighted_24m = _safe_float(price_targets.get("prob_weighted_target_24m"))
    if prob_weighted_24m is None:
        prob_weighted_24m = _compute_prob_weighted_target(scenario_24m, probs_24m)

    current_stage = str(structured.get("current_development_stage") or "").strip()
    if not current_stage:
        timeline_raw = structured.get("development_timeline")
        if isinstance(timeline_raw, list) and timeline_raw:
            first_row = timeline_raw[0]
            if isinstance(first_row, dict):
                current_stage = str(first_row.get("status") or first_row.get("stage") or "").strip()

    return {
        "ticker": str(structured.get("ticker") or "").strip(),
        "company_name": str(structured.get("company_name") or structured.get("company") or "").strip(),
        "analysis_date": str(structured.get("analysis_date") or "").strip(),
        "template_id": str(
            structured.get("template_id")
            or top_level_contract.get("id")
            or council_contract.get("id")
            or ""
        ).strip(),
        "quality_score": quality_total,
        "value_score": value_total,
        "rating": rating,
        "conviction": conviction,
        "current_price": current_price,
        "target_12m_base": target_12m_base,
        "target_24m_base": target_24m_base,
        "prob_weighted_target_12m": prob_weighted_12m,
        "prob_weighted_target_24m": prob_weighted_24m,
        "current_stage": current_stage,
        "freshness_status": str((freshness or {}).get("status") or ""),
        "freshness_age_days": (freshness or {}).get("age_days"),
        "freshness_recommended_action": str((freshness or {}).get("recommended_action") or ""),
    }


def _load_latest_scenario_router_state(run_id: str) -> Dict[str, Any]:
    try:
        from .scenario_router.lab_scribe import LabScribe

        return LabScribe.load_latest_for_run(run_id)
    except Exception:
        return {}


def _build_scenario_router_summary(router_state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(router_state, dict) or not router_state:
        return {}

    try:
        from .scenario_router.artifact_replay import replay_comparison_from_artifact

        comparison, action = replay_comparison_from_artifact(router_state)
    except Exception:
        comparison = (
            router_state.get("comparison_report")
            if isinstance(router_state.get("comparison_report"), dict)
            else {}
        )
        action = (
            router_state.get("action_decision")
            if isinstance(router_state.get("action_decision"), dict)
            else {}
        )
    facts = (
        router_state.get("announcement_facts")
        if isinstance(router_state.get("announcement_facts"), dict)
        else {}
    )
    event = (
        router_state.get("event")
        if isinstance(router_state.get("event"), dict)
        else {}
    )
    condition_evaluations = (
        comparison.get("condition_evaluations")
        if isinstance(comparison.get("condition_evaluations"), list)
        else []
    )
    matched_conditions = [
        str(item.get("label") or item.get("condition_id") or "").strip()
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "matched"
        and str(item.get("group") or "").strip() in {"required", "failure"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    triggered_watchlist = [
        str(item.get("label") or item.get("condition_id") or "").strip()
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "matched"
        and str(item.get("group") or "").strip() in {"red_flag", "confirmatory"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    market_context_conditions = [
        {
            "label": str(item.get("label") or item.get("condition_id") or "").strip(),
            "scenario": str(item.get("scenario") or "").strip(),
            "group": str(item.get("group") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "matched_via": str(item.get("matched_via") or "").strip(),
            "field": str(item.get("market_field") or "").strip(),
            "observed_value": item.get("observed_value"),
            "comparator": str(item.get("comparator") or "").strip(),
            "threshold_value": item.get("threshold_value"),
            "status": str(item.get("status") or "").strip(),
        }
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("matched_via") or "").strip() == "market_facts"
        and str(item.get("status") or "").strip() in {"matched", "contradicted"}
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    matched_condition_details = [
        {
            "label": str(item.get("label") or item.get("condition_id") or "").strip(),
            "scenario": str(item.get("scenario") or "").strip(),
            "group": str(item.get("group") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "matched_via": str(item.get("matched_via") or "").strip(),
            "confidence": item.get("confidence"),
        }
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "matched"
        and str(item.get("group") or "").strip() in {"required", "failure"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    triggered_watchlist_details = [
        {
            "label": str(item.get("label") or item.get("condition_id") or "").strip(),
            "scenario": str(item.get("scenario") or "").strip(),
            "group": str(item.get("group") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "matched_via": str(item.get("matched_via") or "").strip(),
            "confidence": item.get("confidence"),
        }
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "matched"
        and str(item.get("group") or "").strip() in {"red_flag", "confirmatory"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    key_findings = [
        {
            "type": str(item.get("type") or "").strip(),
            "summary": str(item.get("summary") or "").strip(),
            "severity": str(item.get("severity") or "").strip(),
        }
        for item in (comparison.get("key_findings") or [])
        if isinstance(item, dict) and str(item.get("summary") or "").strip()
    ][:8]
    conflicts_with_run = [
        {
            "type": str(item.get("type") or "").strip(),
            "summary": str(item.get("summary") or "").strip(),
            "severity": str(item.get("severity") or "").strip(),
        }
        for item in (comparison.get("conflicts_with_run") or [])
        if isinstance(item, dict) and str(item.get("summary") or "").strip()
    ][:8]
    packet = (
        router_state.get("announcement_packet")
        if isinstance(router_state.get("announcement_packet"), dict)
        else {}
    )
    raw_action = str(action.get("action") or "").strip()
    raw_current_path = str(comparison.get("current_path") or "").strip()
    raw_baseline_path = str(comparison.get("baseline_path") or "").strip()
    raw_impact = str(comparison.get("impact_level") or "").strip()
    has_direct_announcement_hit = bool(matched_conditions or triggered_watchlist)
    suppress_stale_market_only_reroute = (
        not has_direct_announcement_hit
        and raw_action in {"full_rerun", "rerun_stage1", "run_delta_only"}
    )
    display_current_path = (
        raw_baseline_path or raw_current_path
        if suppress_stale_market_only_reroute
        else raw_current_path
    )
    display_action = "watch" if suppress_stale_market_only_reroute else raw_action
    display_impact = "low" if suppress_stale_market_only_reroute and raw_impact != "critical" else raw_impact
    return {
        "current_path": display_current_path,
        "baseline_path": raw_baseline_path,
        "path_transition": "" if suppress_stale_market_only_reroute else str(comparison.get("path_transition") or "").strip(),
        "path_confidence": comparison.get("path_confidence"),
        "run_validity": str(comparison.get("run_validity") or "").strip(),
        "impact_level": display_impact,
        "action": display_action,
        "action_confidence": action.get("confidence"),
        "reason": str(action.get("reason") or "").strip(),
        "announcement_title": str(
            comparison.get("announcement_title") or facts.get("title") or event.get("subject") or ""
        ).strip(),
        "matched_conditions": matched_conditions,
        "matched_condition_details": matched_condition_details,
        "triggered_watchlist": triggered_watchlist,
        "triggered_watchlist_details": triggered_watchlist_details,
        "market_context_conditions": market_context_conditions,
        "key_findings": key_findings,
        "conflicts_with_run": conflicts_with_run,
        "affected_domains": (
            comparison.get("affected_domains")
            if isinstance(comparison.get("affected_domains"), list)
            else []
        ),
        "thesis_effect": str(comparison.get("thesis_effect") or "").strip(),
        "run_validity": str(comparison.get("run_validity") or "").strip(),
        "source_type": str(packet.get("source_type") or "").strip(),
        "source_url": str(packet.get("source_url") or "").strip(),
        "market_facts_used": (
            comparison.get("market_facts_used")
            if isinstance(comparison.get("market_facts_used"), dict)
            else {}
        ),
        "invalidated_sections": [
            str(item or "").strip()
            for item in (action.get("invalidated_sections") or [])
            if str(item or "").strip()
        ][:8],
        "follow_up_steps": [
            str(item or "").strip()
            for item in (action.get("follow_up_steps") or [])
            if str(item or "").strip()
        ][:5],
        "received_at_utc": str(event.get("received_at_utc") or "").strip(),
        "saved_at_utc": str(router_state.get("saved_at_utc") or "").strip(),
    }


def _build_integration_packet(
    *,
    run_id: str,
    run_payload: Dict[str, Any],
) -> Dict[str, Any]:
    structured = run_payload.get("structured_data") if isinstance(run_payload.get("structured_data"), dict) else {}
    freshness = run_payload.get("freshness") if isinstance(run_payload.get("freshness"), dict) else {}
    scenario_router = (
        run_payload.get("scenario_router")
        if isinstance(run_payload.get("scenario_router"), dict)
        else {}
    )
    timeline_rows = _normalize_timeline_rows_for_api(structured.get("development_timeline"))
    summary_fields = _build_summary_fields(structured, freshness)
    summary_fields.update(
        {
            "current_path": str(scenario_router.get("current_path") or "").strip(),
            "path_transition": str(scenario_router.get("path_transition") or "").strip(),
            "scenario_router_action": str(scenario_router.get("action") or "").strip(),
            "scenario_router_impact": str(scenario_router.get("impact_level") or "").strip(),
        }
    )

    return {
        "contract": "analysis_report_packet_v1",
        "run_id": str(run_id),
        "summary_fields": summary_fields,
        "lab_payload": {
            "id": run_payload.get("id"),
            "file": run_payload.get("file"),
            "label": run_payload.get("label"),
            "updated_at": run_payload.get("updated_at"),
            "structured_data": structured,
            "freshness": freshness,
            "scenario_router": scenario_router,
            "delta_check": run_payload.get("delta_check") or {},
            "analyst_memo_markdown": run_payload.get("analyst_memo_markdown") or "",
            "chairman_memo_markdown": run_payload.get("chairman_memo_markdown") or "",
        },
        "timeline_rows": timeline_rows,
        "scenario_router": scenario_router,
        "memos": {
            "analyst_memo_markdown": run_payload.get("analyst_memo_markdown") or "",
            "chairman_memo_markdown": run_payload.get("chairman_memo_markdown") or "",
        },
    }


def _canonical_run_id_for_listing(filename: str) -> str:
    """
    Collapse stage/checkpoint/preview JSON variants into one canonical run artifact id.
    """
    safe_name = Path(filename).name

    # quality_run_xxx.stage3_primary.checkpoint.json -> quality_run_xxx.json
    if safe_name.endswith(".checkpoint.json"):
        base = safe_name[: -len(".checkpoint.json")]
        if ".stage" in base:
            base = base.split(".stage", 1)[0]
        return f"{base}.json"

    # stage3_replay_batch_xxx.normalized_preview_yyy.json -> stage3_replay_batch_xxx.json
    preview_marker = ".normalized_preview_"
    if preview_marker in safe_name and safe_name.endswith(".json"):
        base = safe_name.split(preview_marker, 1)[0]
        return f"{base}.json"

    # Any other stage-suffixed sidecar JSON should map to base run artifact.
    if ".stage" in safe_name and safe_name.endswith(".json"):
        base = safe_name.split(".stage", 1)[0]
        return f"{base}.json"

    return safe_name


@app.get("/api/gantt-runs")
async def list_gantt_runs(limit: int = 20, ticker: Optional[str] = None):
    """List recent output artifacts that contain Stage 3 structured data."""
    safe_limit = max(1, int(limit))
    ticker_filter = _normalize_run_ticker(ticker)
    ticker_filter_aliases = _run_ticker_aliases(ticker_filter)
    now_ts = time.time()
    cache_key = _GANTT_RUN_LIST_CACHE.get("key")
    cache_runs = _GANTT_RUN_LIST_CACHE.get("runs")
    cache_expiry = float(_GANTT_RUN_LIST_CACHE.get("expires_at") or 0.0)
    requested_cache_key = f"{safe_limit}|{ticker_filter}"
    if (
        isinstance(cache_runs, list)
        and cache_key == requested_cache_key
        and now_ts <= cache_expiry
    ):
        return {"runs": cache_runs}

    search_roots = [OUTPUTS_DIR]
    if JOBS_OUTPUTS_DIR != OUTPUTS_DIR:
        search_roots.append(JOBS_OUTPUTS_DIR)
    if not any(root.exists() for root in search_roots):
        return {"runs": []}

    all_json: List[Path] = []
    for root in search_roots:
        if not root.exists():
            continue
        all_json.extend(root.glob("*.json"))
    all_json = sorted(all_json, key=lambda p: p.stat().st_mtime, reverse=True)
    # Keep exactly one artifact per run id (prefer canonical base .json if present).
    selected: Dict[str, Path] = {}
    for path in all_json:
        canonical_id = _canonical_run_id_for_listing(path.name)
        if canonical_id in selected:
            continue
        # Prefer canonical base .json in the same directory when available.
        canonical_path_same_dir = path.parent / canonical_id
        selected[canonical_id] = (
            canonical_path_same_dir if canonical_path_same_dir.exists() else path
        )

    candidates = sorted(
        selected.values(),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    runs: List[Dict[str, Any]] = []
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        structured = _extract_stage3_structured_from_artifact(payload)
        if not isinstance(structured, dict) or not structured:
            continue
        run_ticker = _normalize_run_ticker(structured.get("ticker"))
        run_ticker_aliases = _run_ticker_aliases(run_ticker)
        if ticker_filter_aliases and not (run_ticker_aliases & ticker_filter_aliases):
            continue

        run_id = path.name
        updated_at = datetime.utcfromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat() + "Z"
        scenario_router = _build_scenario_router_summary(
            _load_latest_scenario_router_state(run_id)
        )
        runs.append(
            {
                "id": run_id,
                "file": path.name,
                "label": _build_gantt_run_label(path.stem, structured),
                "ticker": structured.get("ticker"),
                "company_name": structured.get("company_name") or structured.get("company"),
                "analysis_date": structured.get("analysis_date"),
                "updated_at": updated_at,
                "freshness": _compute_run_freshness(structured, updated_at),
                "scenario_router": scenario_router,
            }
        )
        if len(runs) >= safe_limit:
            break

    _GANTT_RUN_LIST_CACHE["key"] = requested_cache_key
    _GANTT_RUN_LIST_CACHE["runs"] = runs
    _GANTT_RUN_LIST_CACHE["expires_at"] = now_ts + float(GANTT_RUN_LIST_CACHE_TTL_SEC)
    return {"runs": runs}


@app.get("/api/gantt-runs/{run_id}")
async def get_gantt_run(run_id: str):
    """Load one output artifact and return Stage 3 structured data for gantt-lab."""
    safe_name = Path(run_id).name
    path = _resolve_run_artifact_path(safe_name)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Run artifact not found")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse artifact: {exc}") from exc

    stage3_result = _extract_stage3_result_from_artifact(payload) or {}
    structured = stage3_result.get("structured_data")
    if not isinstance(structured, dict) or not structured:
        raise HTTPException(status_code=400, detail="Artifact does not contain Stage 3 structured data")

    # Backfill current share price for charting if Stage 3/jsonifier omitted it.
    market_data = structured.get("market_data")
    if not isinstance(market_data, dict):
        market_data = {}
        structured["market_data"] = market_data
    current_price = market_data.get("current_price")
    if current_price in (None, "", "n/a", "N/A"):
        inferred_current_price = _infer_current_price_from_artifact(payload)
        if inferred_current_price is not None:
            market_data["current_price"] = inferred_current_price
    # Keep price_targets.current_price aligned with market_data.current_price.
    price_targets = structured.get("price_targets")
    if not isinstance(price_targets, dict):
        price_targets = {}
        structured["price_targets"] = price_targets
    if (
        market_data.get("current_price") not in (None, "", "n/a", "N/A")
        and price_targets.get("current_price") in (None, "", "n/a", "N/A")
    ):
        price_targets["current_price"] = market_data.get("current_price")

    # Normalize timeline rows for frontend charting (avoid TBD when chairman/jsonifier emits strings).
    structured["development_timeline"] = _normalize_timeline_rows_for_api(
        structured.get("development_timeline")
    )
    # Keep historical catalyst references lightweight: max one prior catalyst, rest future/current.
    extended_analysis = structured.get("extended_analysis")
    if isinstance(extended_analysis, dict) and isinstance(extended_analysis.get("next_major_catalysts"), list):
        extended_analysis["next_major_catalysts"] = _cap_previous_catalyst_rows(
            extended_analysis.get("next_major_catalysts") or [],
            max_previous=1,
        )

    memo_payload = _extract_memo_content(payload, stage3_result, path)
    analyst_document = stage3_result.get("analyst_document") if isinstance(stage3_result, dict) else {}
    chairman_document = stage3_result.get("chairman_document") if isinstance(stage3_result, dict) else {}

    artifact_updated_at = datetime.utcfromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat() + "Z"
    delta_latest = get_latest_delta(safe_name)
    freshness = _compute_run_freshness(structured, artifact_updated_at)
    router_state = _build_scenario_router_summary(
        _load_latest_scenario_router_state(safe_name)
    )
    summary_fields = _build_summary_fields(structured, freshness)
    summary_fields.update(
        {
            "current_path": str(router_state.get("current_path") or "").strip(),
            "path_transition": str(router_state.get("path_transition") or "").strip(),
            "scenario_router_action": str(router_state.get("action") or "").strip(),
            "scenario_router_impact": str(router_state.get("impact_level") or "").strip(),
        }
    )
    return {
        "id": safe_name,
        "file": safe_name,
        "label": _build_gantt_run_label(path.stem, structured),
        "structured_data": structured,
        "updated_at": artifact_updated_at,
        "freshness": freshness,
        "summary_fields": summary_fields,
        "scenario_router": router_state,
        "delta_check": delta_latest or {},
        "analyst_memo_markdown": memo_payload.get("analyst_memo_markdown", ""),
        "chairman_memo_markdown": memo_payload.get("chairman_memo_markdown", ""),
        "analyst_document": analyst_document if isinstance(analyst_document, dict) else {},
        "chairman_document": chairman_document if isinstance(chairman_document, dict) else {},
    }


@app.delete("/api/gantt-runs/{run_id}")
async def delete_gantt_run(run_id: str):
    safe_name = Path(run_id).name
    canonical_id = _canonical_run_id_for_listing(safe_name)
    related_paths = _collect_run_related_paths(canonical_id)
    if not related_paths:
        raise HTTPException(status_code=404, detail="Run artifact not found")

    deleted: List[str] = []
    failed: List[Dict[str, str]] = []
    for path in related_paths:
        try:
            path.unlink(missing_ok=True)
            try:
                deleted.append(str(path.relative_to(PROJECT_ROOT)))
            except Exception:
                deleted.append(str(path))
        except Exception as exc:
            failed.append({"path": str(path), "error": str(exc)})

    _invalidate_gantt_run_cache()

    status = "deleted" if not failed else ("partial" if deleted else "failed")
    if not deleted and failed:
        raise HTTPException(
            status_code=500,
            detail={
                "status": status,
                "run_id": canonical_id,
                "deleted_count": 0,
                "failed": failed,
            },
        )

    return {
        "status": status,
        "run_id": canonical_id,
        "deleted_count": len(deleted),
        "deleted_files": deleted,
        "failed": failed,
    }


@app.get("/api/gantt-runs/{run_id}/report-packet")
async def get_gantt_run_report_packet(run_id: str):
    """
    Return a single integration packet for external apps:
    summary fields + full gantt-lab payload + memo markdown.
    """
    run_payload = await get_gantt_run(run_id)
    return _build_integration_packet(run_id=str(run_payload.get("id") or run_id), run_payload=run_payload)


@app.get("/api/scenario-router/overview")
async def get_scenario_router_overview(limit: int = 100, ticker: str = ""):
    from .scenario_router.observability import ScenarioRouterObservability

    observer = ScenarioRouterObservability()
    return observer.build_overview(recent_limit=max(1, min(int(limit or 100), 500)), ticker=str(ticker or "").strip())


@app.get("/api/scenario-router/events")
async def list_scenario_router_events(limit: int = 50, ticker: str = ""):
    from .scenario_router.observability import ScenarioRouterObservability

    observer = ScenarioRouterObservability()
    return {
        "events": observer.list_recent_events(limit=max(1, min(int(limit or 50), 500)), ticker=str(ticker or "").strip()),
    }


@app.get("/api/scenario-router/evaluations")
async def get_scenario_router_evaluations():
    from .scenario_router.observability import ScenarioRouterObservability

    observer = ScenarioRouterObservability()
    return observer.run_evaluation_suite()


@app.get("/api/gantt-runs/{run_id}/delta-check/latest")
async def get_latest_delta_check(run_id: str):
    """Return latest delta-check result for a run, if available."""
    safe_name = Path(run_id).name
    path = _resolve_run_artifact_path(safe_name)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Run artifact not found")

    latest = get_latest_delta(safe_name)
    if not latest:
        raise HTTPException(status_code=404, detail="No delta-check artifact found for run")
    return latest


@app.post("/api/scenario-router/process-announcement")
@app.post("/api/freshness/process-announcement")
async def process_scenario_router_announcement(
    request: ProcessScenarioRouterAnnouncementRequest,
    raw_request: Request,
):
    """Process one inbound announcement event against the latest saved lab run."""
    _check_scenario_router_webhook_secret(raw_request)
    payload = request.model_dump()
    dedupe_key = _choose_scenario_router_event_key(payload)
    if dedupe_key:
        existing = _load_scenario_router_dedupe(dedupe_key)
        if existing:
            return {
                "status": "duplicate",
                "ticker": str(existing.get("ticker") or payload.get("ticker") or "").strip(),
                "baseline_run_id": str(existing.get("baseline_run_id") or "").strip(),
                "current_path": str(existing.get("current_path") or "").strip(),
                "path_transition": str(existing.get("path_transition") or "").strip(),
                "action": str(existing.get("action") or "").strip(),
                "dedupe": {
                    "event_key": dedupe_key,
                    "processed_at_utc": str(existing.get("processed_at_utc") or "").strip(),
                },
            }
    from .scenario_router.inbox_sentinel import InboxSentinel
    from .scenario_router.lab_scribe import LabScribe

    sentinel = InboxSentinel()
    scribe = LabScribe()
    event = None
    try:
        event = sentinel.ingest_email_payload(payload)
        if not str(event.ticker or "").strip():
            raise HTTPException(
                status_code=400,
                detail="Could not determine ticker from announcement payload.",
            )

        service = _build_scenario_router_service()
        decision = await service.process_announcement_event(event)
    except RuntimeError as exc:
        reason = str(exc or "").strip()
        if event is not None and reason.startswith("No saved lab runs found for "):
            processed_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            if dedupe_key:
                _persist_scenario_router_dedupe(
                    dedupe_key,
                    {
                        "event_key": dedupe_key,
                        "processed_at_utc": processed_at_utc,
                        "event_id": event.event_id,
                        "ticker": event.ticker,
                        "baseline_run_id": "",
                        "current_path": "",
                        "path_transition": "",
                        "action": "",
                        "status": "skipped_no_baseline_run",
                        "detail": reason,
                    },
                )
            return {
                "status": "skipped_no_baseline_run",
                "ticker": event.ticker,
                "baseline_run_id": "",
                "current_path": "",
                "path_transition": "",
                "action": "",
                "detail": reason,
            }
        raise HTTPException(
            status_code=500,
            detail=f"Scenario router announcement processing failed: {exc}",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        if event is not None:
            await scribe.persist_status(
                event=event,
                status="processing_error",
                reason=str(exc or "").strip(),
            )
        raise HTTPException(
            status_code=500,
            detail=f"Scenario router announcement processing failed: {exc}",
        ) from exc

    processed_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if dedupe_key:
        _persist_scenario_router_dedupe(
            dedupe_key,
            {
                "event_key": dedupe_key,
                "processed_at_utc": processed_at_utc,
                "event_id": decision.event.event_id,
                "ticker": decision.event.ticker,
                "baseline_run_id": decision.baseline_run.run_id,
                "current_path": decision.comparison_report.current_path,
                "path_transition": decision.comparison_report.path_transition,
                "action": decision.action_decision.action,
                "status": "ok",
            },
        )

    return {
        "status": "ok",
        "ticker": decision.event.ticker,
        "baseline_run_id": decision.baseline_run.run_id,
        "current_path": decision.comparison_report.current_path,
        "path_transition": decision.comparison_report.path_transition,
        "action": decision.action_decision.action,
        "dedupe": {
            "event_key": dedupe_key,
            "processed_at_utc": processed_at_utc,
        },
        "decision": decision.to_dict(),
    }


@app.post("/api/gantt-runs/{run_id}/delta-check")
async def post_delta_check(
    run_id: str,
    force: bool = False,
    max_sources: int = 12,
    lookback_days: int = 14,
):
    """Run lightweight delta monitor against the selected run artifact."""
    safe_name = Path(run_id).name
    path = _resolve_run_artifact_path(safe_name)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Run artifact not found")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse artifact: {exc}") from exc

    stage3_result = _extract_stage3_result_from_artifact(payload) or {}
    structured = stage3_result.get("structured_data")
    if not isinstance(structured, dict) or not structured:
        raise HTTPException(status_code=400, detail="Artifact does not contain Stage 3 structured data")

    artifact_updated_at = datetime.utcfromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat() + "Z"
    try:
        result = await run_delta_check(
            run_id=safe_name,
            structured=structured,
            artifact_updated_at=artifact_updated_at,
            force=bool(force),
            max_sources=max(1, int(max_sources)),
            lookback_days=max(1, int(lookback_days)),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delta-check failed: {exc}") from exc

    return result


@app.get("/api/memos/{memo_name}")
async def get_memo_file(memo_name: str):
    """
    Load a markdown memo artifact from outputs/ by filename.
    Example: /api/memos/analyst_memo_regen_20260306_115115
    """
    safe_name = Path(memo_name).name
    if not safe_name.endswith(".md"):
        safe_name = f"{safe_name}.md"

    path = OUTPUTS_DIR / safe_name
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Memo artifact not found")

    try:
        markdown = path.read_text(encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read memo artifact: {exc}") from exc

    return {
        "id": safe_name,
        "markdown": markdown,
    }


@app.post("/api/analysis-jobs", status_code=202)
async def create_analysis_job(
    request: Request,
    job_type: Optional[str] = Form(None),
    query: Optional[str] = Form(None),
    ticker: Optional[str] = Form(None),
    company_name: Optional[str] = Form(None),
    template_id: Optional[str] = Form(None),
    company_type: Optional[str] = Form(None),
    exchange: Optional[str] = Form(None),
    stage1_only: Optional[str] = Form(None),
    stage2_revision_pass: Optional[str] = Form(None),
    secondary_chairman_model: Optional[str] = Form(None),
    run_label: Optional[str] = Form(None),
    diagnostic_mode: Optional[str] = Form(None),
    reuse_recent_bundle: Optional[str] = Form(None),
    reuse_supplementary_from_job_id: Optional[str] = Form(None),
    supplementary_mode: Optional[str] = Form(None),
    portfolio_positioning_mode: Optional[str] = Form(None),
    supplementary_file: UploadFile = File(None),
):
    """
    Submit an async full-analysis job.
    Produces a run artifact under outputs/jobs/ and returns a stable job_id.
    """
    _ensure_system_enabled()
    content_type = str(request.headers.get("content-type") or "").lower()
    if "multipart/form-data" in content_type:
        job_request = CreateAnalysisJobRequest(
            job_type=job_type,
            query=query,
            ticker=ticker,
            company_name=company_name,
            template_id=template_id,
            company_type=company_type,
            exchange=exchange,
            stage1_only=_coerce_form_bool(stage1_only, default=False),
            stage2_revision_pass=str(stage2_revision_pass or "on"),
            secondary_chairman_model=secondary_chairman_model,
            run_label=run_label,
            diagnostic_mode=_coerce_form_bool(diagnostic_mode, default=False),
            reuse_recent_bundle=_coerce_form_bool(reuse_recent_bundle, default=False),
            reuse_supplementary_from_job_id=str(reuse_supplementary_from_job_id or "").strip() or None,
            supplementary_mode=_validate_supplementary_mode(supplementary_mode),
            portfolio_positioning_mode=_validate_portfolio_positioning_mode(portfolio_positioning_mode),
        )
    else:
        try:
            body = await request.json()
        except Exception:
            body = {}
        job_request = CreateAnalysisJobRequest(**(body or {}))
        supplementary_file = None

    job_kind = _validate_job_type(job_request.job_type)
    if job_kind == "portfolio_positioning":
        if not isinstance(job_request.portfolio_context, dict) or not job_request.portfolio_context:
            raise HTTPException(
                status_code=400,
                detail="Portfolio positioning requires portfolio_context",
            )
        job_request.portfolio_positioning_mode = _validate_portfolio_positioning_mode(
            job_request.portfolio_positioning_mode
        )
    elif not str(job_request.query or "").strip() and not str(job_request.ticker or "").strip():
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: query, ticker",
        )

    from .template_loader import get_template_loader
    loader = get_template_loader()
    requested_exchange = str(job_request.exchange or "").strip()
    requested_ticker = str(job_request.ticker or "").strip().upper()
    if job_kind != "portfolio_positioning" and requested_exchange and not loader.normalize_exchange(requested_exchange):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid exchange value: {requested_exchange}",
        )
    if job_kind != "portfolio_positioning" and requested_ticker and ":" not in requested_ticker and not requested_exchange:
        raise HTTPException(
            status_code=400,
            detail="Ticker must include an exchange prefix (e.g. ASX:BHM) or provide a valid exchange.",
        )

    _ensure_analysis_job_dirs()
    output_name = _build_job_run_filename(job_request)
    output_path = JOBS_OUTPUTS_DIR / output_name
    job_id = str(uuid.uuid4())
    supplementary_upload_path: Optional[Path] = None
    portfolio_context_path: Optional[Path] = None
    supplementary_filename = ""
    if supplementary_file is not None:
        supplementary_upload_path, supplementary_filename = await _store_supplementary_upload_for_job(
            supplementary_file,
            job_id=job_id,
        )
    elif job_request.reuse_supplementary_from_job_id:
        prev_job_id = str(job_request.reuse_supplementary_from_job_id or "").strip()
        if prev_job_id:
            supplementary_dir = JOBS_OUTPUTS_DIR / "supplementary"
            matches = sorted(supplementary_dir.glob(f"{prev_job_id}_*"))
            if not matches:
                raise HTTPException(
                    status_code=404,
                    detail=f"No saved supplementary file found for job: {prev_job_id}",
                )
            supplementary_upload_path = matches[0]
            supplementary_filename = supplementary_upload_path.name

    if job_kind == "portfolio_positioning":
        portfolio_context_path = _store_portfolio_context_for_job(
            job_request.portfolio_context,
            job_id=job_id,
        )
        if portfolio_context_path is None:
            raise HTTPException(status_code=400, detail="Failed to persist portfolio_context")

    command = _build_analysis_job_command(
        job_request,
        output_path,
        supplementary_context_path=supplementary_upload_path,
        portfolio_context_path=portfolio_context_path,
    )
    request_payload = (
        job_request.model_dump()
        if hasattr(job_request, "model_dump")
        else job_request.dict()
    )
    if supplementary_upload_path is not None:
        request_payload["supplementary_file"] = supplementary_filename or "supplementary_document"
    job_record = {
        "job_id": job_id,
        "status": "queued",
        "stage": "queued",
        "stage_message": "Queued",
        "progress_pct": 0,
        "instance_id": INSTANCE_ID,
        "created_at": _utc_now_iso(),
        "started_at": "",
        "finished_at": "",
        "last_output_at": "",
        "output_path": str(output_path),
        "returncode": None,
        "pid": None,
        "run_id": "",
        "error": "",
        "stdout_tail": "",
        "stderr_tail": "",
        "request": request_payload,
        "command": command,
    }

    async with ANALYSIS_JOBS_LOCK:
        ANALYSIS_JOBS[job_id] = job_record
        _persist_job_record(job_record)

    asyncio.create_task(
        _run_analysis_job(
            job_id=job_id,
            command=command,
            output_path=output_path,
            request_payload=request_payload,
            cleanup_paths=[
                path
                for path in [supplementary_upload_path, portfolio_context_path]
                if path is not None
            ],
        )
    )
    return _public_job_view(job_record)


@app.get("/api/analysis-jobs")
async def list_analysis_jobs(limit: int = 20):
    """List most recent async analysis jobs."""
    async with ANALYSIS_JOBS_LOCK:
        rows = list(ANALYSIS_JOBS.values())
    run_ids_with_jobs = {
        Path(str(row.get("run_id") or "")).name
        for row in rows
        if str(row.get("run_id") or "").strip()
    }
    try:
        gantt_payload = await list_gantt_runs(limit=max(50, max(1, int(limit)) * 3))
        for run in list(gantt_payload.get("runs") or []):
            run_id = Path(str(run.get("id") or "")).name
            if not run_id or run_id in run_ids_with_jobs:
                continue
            rows.append(_build_synthetic_job_record_from_run(run))
    except Exception:
        pass
    rows.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return {"jobs": [_public_job_view(row) for row in rows[: max(1, int(limit))]]}


@app.get("/api/analysis-jobs/{job_id}")
async def get_analysis_job(job_id: str):
    """Fetch status/log tails for one async analysis job."""
    async with ANALYSIS_JOBS_LOCK:
        job = ANALYSIS_JOBS.get(str(job_id))
    if isinstance(job, dict):
        return _public_job_view(job)

    synthetic_run_id = _run_id_from_synthetic_job_id(job_id)
    if synthetic_run_id:
        run_payload = await get_gantt_run(synthetic_run_id)
        run_meta = {
            "id": run_payload.get("id") or synthetic_run_id,
            "label": run_payload.get("label") or synthetic_run_id,
            "ticker": ((run_payload.get("structured_data") or {}).get("ticker") if isinstance(run_payload.get("structured_data"), dict) else ""),
            "company_name": ((run_payload.get("structured_data") or {}).get("company_name") if isinstance(run_payload.get("structured_data"), dict) else ""),
            "analysis_date": ((run_payload.get("structured_data") or {}).get("analysis_date") if isinstance(run_payload.get("structured_data"), dict) else ""),
            "updated_at": run_payload.get("updated_at") or "",
        }
        return _public_job_view(_build_synthetic_job_record_from_run(run_meta))

    raise HTTPException(
        status_code=404,
        detail={
            "message": "Analysis job not found on this instance",
            "instance_id": INSTANCE_ID,
        },
    )


@app.get("/api/analysis-jobs/{job_id}/result")
async def get_analysis_job_result(job_id: str):
    """Return normalized run payload once the async analysis job succeeds."""
    async with ANALYSIS_JOBS_LOCK:
        job = ANALYSIS_JOBS.get(str(job_id))
    synthetic_run_id = _run_id_from_synthetic_job_id(job_id)
    if not isinstance(job, dict):
        if not synthetic_run_id:
            raise HTTPException(status_code=404, detail="Analysis job not found")
        run_payload = await get_gantt_run(synthetic_run_id)
        structured = run_payload.get("structured_data") if isinstance(run_payload.get("structured_data"), dict) else {}
        synthetic_job = _build_synthetic_job_record_from_run(
            {
                "id": run_payload.get("id") or synthetic_run_id,
                "label": run_payload.get("label") or synthetic_run_id,
                "ticker": structured.get("ticker") or "",
                "company_name": structured.get("company_name") or structured.get("company") or "",
                "analysis_date": structured.get("analysis_date") or "",
                "updated_at": run_payload.get("updated_at") or "",
            }
        )
        report_packet = _build_integration_packet(run_id=synthetic_run_id, run_payload=run_payload)
        return {
            "job": _public_job_view(synthetic_job),
            "run": run_payload,
            "report_packet": report_packet,
        }

    status = str(job.get("status") or "")
    run_id = str(job.get("run_id") or "")
    if status == "failed":
        raise HTTPException(
            status_code=409,
            detail=str(job.get("error") or "Analysis job failed"),
        )
    if status != "succeeded" or not run_id:
        raise HTTPException(status_code=409, detail=f"Analysis job not completed (status={status})")

    run_payload = await get_gantt_run(run_id)
    report_packet = _build_integration_packet(run_id=run_id, run_payload=run_payload)
    return {
        "job": _public_job_view(job),
        "run": run_payload,
        "report_packet": report_packet,
    }


@app.get("/api/analysis-jobs/{job_id}/events")
async def stream_analysis_job_events(job_id: str, poll_ms: int = 1000):
    """
    Stream async job status updates (SSE) for progress bars and live UX feedback.
    Emits `analysis_job` events when status/stage/progress changes and exits on terminal state.
    """
    interval_s = max(0.25, min(5.0, float(poll_ms) / 1000.0))

    async def _event_stream():
        last_signature: Optional[Tuple[Any, ...]] = None
        while True:
            async with ANALYSIS_JOBS_LOCK:
                job = ANALYSIS_JOBS.get(str(job_id))
                payload = _public_job_view(job) if isinstance(job, dict) else None

            if payload is None:
                data = {
                    "type": "analysis_job_not_found",
                    "job_id": str(job_id),
                    "instance_id": INSTANCE_ID,
                }
                yield f"event: error\ndata: {json.dumps(data)}\n\n"
                break

            signature = (
                payload.get("status"),
                payload.get("stage"),
                payload.get("progress_pct"),
                payload.get("stage_message"),
                payload.get("last_output_at"),
                payload.get("run_id"),
                payload.get("error"),
            )
            if signature != last_signature:
                yield f"event: analysis_job\ndata: {json.dumps(payload)}\n\n"
                last_signature = signature

            status = str(payload.get("status") or "").lower()
            if status in {"succeeded", "failed"}:
                break
            await asyncio.sleep(interval_s)

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/company-types/detect")
async def detect_company_type(request: CompanyTypeDetectRequest):
    """
    Deterministic company-type detection for template routing.
    """
    from .template_loader import get_template_loader

    loader = get_template_loader()
    selected = loader.detect_company_type(
        user_query=request.content,
        ticker=request.ticker,
    )
    return {
        "status": "ok" if selected else "unresolved",
        "provider": "deterministic_resolver",
        "selected_company_type": selected,
        "candidate_company_type": selected,
        "applied": bool(selected),
        "confidence": 1.0 if selected else 0.0,
        "company_name": loader.infer_company_name(request.content, ticker=request.ticker),
        "exchange": loader.normalize_exchange(request.exchange) or "unknown",
    }


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
    files: List[UploadFile] = File(None),
    supplementary_file: UploadFile = File(None),
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
    effective_enable_search_for_storage = enable_search or selected_council_mode == "perplexity_emulated"

    # Persist the user message immediately so reloads do not hide an in-flight run.
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

            # Generate message ID for attachment storage
            message_id = str(uuid.uuid4())

            # Process PDF attachments if present
            attachments_metadata = []
            attachments_processed = []

            if files:
                yield f"data: {json.dumps({'type': 'attachments_start', 'count': len(files)})}\n\n"
                _persist_assistant_patch({"loading": {"attachments": True, "stage1Message": "Processing attachments..."}})

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

            # Structured analyses require deterministic ticker anchoring. Prevent
            # silent degraded runs (no market-facts, no deterministic ASX ingest).
            if use_structured_analysis and ENABLE_MARKET_FACTS_PREPASS and not search_ticker:
                msg = (
                    "Ticker unresolved for structured analysis while market-facts prepass is enabled. "
                    "Provide EXCHANGE:SYMBOL (e.g., ASX:BRK)."
                )
                yield f"data: {json.dumps({'type': 'error', 'error': msg})}\n\n"
                return

            # Deterministic market-facts prepass used to anchor valuation/share-structure fields.
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
                    _persist_assistant_patch(
                        {
                            "search_results": search_results,
                            "loading": {"search": False},
                        }
                    )
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
                    _persist_assistant_patch(
                        {
                            "search_results": search_results,
                            "loading": {"search": False, "evidence": False},
                        }
                    )

            # Include PDFs from search if available
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

            # Start title generation in parallel (don't await yet)
            title_task = None
            if is_first_message:
                title_task = asyncio.create_task(generate_conversation_title(content))

            # Stage 1: Collect responses
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
                    # Stage 1 should run against the full template rubric by default.
                    # Fallback to stage1_focus_prompt only when rubric is absent.
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
                    print(
                        "Stage1 prompt assembly: "
                        f"structured_template={use_structured_analysis}, "
                        f"template_id={selected_template_id}, "
                        f"market_facts_prefixed={bool(market_facts)}, "
                        f"query_core_chars={len(stage1_query_core)}, "
                        f"query_sent_chars={len(stage1_effective_query)}, "
                        f"brief_chars={len(stage1_effective_research_brief)}"
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

            # Stage 2: Collect rankings
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

            # Stage 3: Synthesize final answer (with optional structured analysis)
            yield f"data: {json.dumps({'type': 'stage3_start'})}\n\n"
            _persist_assistant_patch({"loading": {"stage3": True}})
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
            _persist_assistant_patch(
                {
                    "stage3": stage3_result,
                    "loading": {"stage3": False},
                }
            )

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


def build_enhanced_context(
    user_query: str,
    search_results: Optional[Dict[str, Any]],
    attachments_processed: List[Dict[str, Any]],
    template_context: str = "",
    market_facts: Optional[Dict[str, Any]] = None,
    supplementary_context: str = "",
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

    if supplementary_context:
        parts.append("\n\n--- SUPPLEMENTARY USER DOCUMENT ---")
        parts.append(str(supplementary_context).strip())

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
    if rubric:
        try:
            from .template_loader import get_template_loader

            loader = get_template_loader()
            rubric = loader.apply_prompt_substitutions(
                rubric,
                company_name=company_name,
                exchange=exchange,
            )
        except Exception:
            # Keep lightweight fallback behavior if template loader is unavailable.
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


# Serve the Vite frontend SPA (built into frontend/dist by Dockerfile)
_FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"
if _FRONTEND_DIST.exists():
    _assets_dir = _FRONTEND_DIST / "assets"
    if _assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(_assets_dir)), name="frontend-assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        """Catch-all: serve static files from dist root, fall back to index.html for SPA routing."""
        candidate = _FRONTEND_DIST / full_path
        if full_path and candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(_FRONTEND_DIST / "index.html"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
