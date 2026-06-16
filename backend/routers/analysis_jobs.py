"""HTTP routes for long-running analysis jobs.

Handles job creation, status polling, streaming log output, result
retrieval, and admin endpoints.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse

from ..jobs.executor import (
    ANALYSIS_JOB_HEARTBEAT_SECONDS,
    CreateAnalysisJobRequest,
    _build_analysis_job_command,
    _build_job_run_filename,
    _build_synthetic_job_record_from_run,
    _coerce_form_bool,
    _ensure_analysis_job_dirs,
    _persist_job_record,
    _public_job_view,
    _run_analysis_job,
    _run_id_from_synthetic_job_id,
    _sanitize_analysis_request_payload,
    _store_portfolio_context_for_job,
    _store_supplementary_upload_for_job,
    _utc_now_iso,
    _validate_job_type,
    _validate_portfolio_positioning_mode,
    _validate_supplementary_mode,
)
from ..jobs.structured import _build_integration_packet
from ..jobs.state import (
    ANALYSIS_JOBS,
    ANALYSIS_JOBS_LOCK,
    INSTANCE_ID,
    JOBS_OUTPUTS_DIR,
    PORTFOLIO_POSITIONING_OUTPUTS_DIR,
)
from ..utils import _ensure_system_enabled

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/api/analysis-jobs", status_code=202)
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
    output_path = (
        PORTFOLIO_POSITIONING_OUTPUTS_DIR / output_name
        if job_kind == "portfolio_positioning"
        else JOBS_OUTPUTS_DIR / output_name
    )
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
    request_payload = _sanitize_analysis_request_payload(request_payload)
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


@router.get("/api/analysis-jobs")
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


@router.get("/api/analysis-jobs/{job_id}")
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


@router.get("/api/analysis-jobs/{job_id}/result")
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

    if str((job.get("request") or {}).get("job_type") or "").strip().lower() == "portfolio_positioning":
        run_payload = await get_portfolio_positioning_run(run_id)
        return {
            "job": _public_job_view(job),
            "run": run_payload,
            "report_packet": None,
        }

    run_payload = await get_gantt_run(run_id)
    report_packet = _build_integration_packet(run_id=run_id, run_payload=run_payload)
    return {
        "job": _public_job_view(job),
        "run": run_payload,
        "report_packet": report_packet,
    }


@router.get("/api/analysis-jobs/{job_id}/events")
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
                payload.get("heartbeat_at"),
                payload.get("heartbeat_count"),
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



