"""Analysis-job execution infrastructure.

Contains helpers for building, running, persisting, and monitoring async
full-analysis jobs.  Shared by the analysis_jobs router and the lifespan
startup loader in main.py.
"""

import asyncio
import contextlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, UploadFile
from pydantic import BaseModel

from ..config import SUPPLEMENTARY_API_PIPELINES_ENABLED
from .state import (
    ANALYSIS_JOB_LOG_TAIL_CHARS,
    ANALYSIS_JOBS,
    ANALYSIS_JOBS_LOCK,
    INSTANCE_ID,
    JOBS_META_DIR,
    JOBS_OUTPUTS_DIR,
    OUTPUTS_DIR,
    PORTFOLIO_POSITIONING_OUTPUTS_DIR,
    PREPASS_OUTPUTS_DIR,
    PROJECT_ROOT,
    SUPPLEMENTARY_DOC_ALLOWED_EXTENSIONS,
    SYNTHETIC_RUN_JOB_PREFIX,
    _GANTT_RUN_LIST_CACHE,
    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE,
    _ANALYSIS_PROGRESS_MARKERS,
    _ANALYSIS_STAGE_ORDER,
    _ANALYSIS_STAGE_RANGES,
    research_service,
)
from .prepass import _tail_text
from .runs import (
    _invalidate_gantt_run_cache,
    _invalidate_portfolio_positioning_run_cache,
    _resolve_run_artifact_path,
)
from .structured import (
    _extract_stage3_result_from_artifact,
    _extract_stage3_structured_from_artifact,
)

logger = logging.getLogger(__name__)

ANALYSIS_JOB_HEARTBEAT_SECONDS = max(
    5.0,
    float(os.getenv("ANALYSIS_JOB_HEARTBEAT_SECONDS", "10") or 10),
)

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


def _summarize_portfolio_context_for_job(portfolio_context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Keep job metadata readable; the raw context is stored only as a temporary worker input."""
    if not isinstance(portfolio_context, dict):
        return {}

    asset_classes = portfolio_context.get("asset_classes") if isinstance(portfolio_context.get("asset_classes"), list) else []
    available_asset_classes = (
        portfolio_context.get("available_asset_classes")
        if isinstance(portfolio_context.get("available_asset_classes"), list)
        else []
    )
    positions = portfolio_context.get("positions") if isinstance(portfolio_context.get("positions"), list) else []
    portfolio = portfolio_context.get("portfolio") if isinstance(portfolio_context.get("portfolio"), dict) else {}
    overlay = portfolio_context.get("overlay") if isinstance(portfolio_context.get("overlay"), dict) else {}

    def _asset_label(row: Any) -> str:
        if isinstance(row, dict):
            return str(row.get("asset_class") or row.get("display_name") or "").strip()
        return str(row or "").strip()

    return {
        "as_of": str(portfolio_context.get("as_of") or "").strip(),
        "asset_class_count": len(asset_classes),
        "available_asset_class_count": len(available_asset_classes),
        "position_count": len(positions),
        "available_asset_classes": [
            _asset_label(row)
            for row in available_asset_classes
            if _asset_label(row)
        ][:80],
        "portfolio": {
            "total_value": portfolio.get("total_value"),
            "cash_pct": portfolio.get("cash_pct") if portfolio.get("cash_pct") is not None else portfolio.get("cash_on_hand_pct"),
        },
        "overlay": {
            "q1_exposure_pct": overlay.get("q1_exposure_pct") if overlay.get("q1_exposure_pct") is not None else overlay.get("effective_q1_pct"),
            "status": overlay.get("status"),
        },
    }


def _sanitize_analysis_request_payload(request_payload: Dict[str, Any]) -> Dict[str, Any]:
    sanitized = dict(request_payload or {})
    if isinstance(sanitized.get("portfolio_context"), dict):
        sanitized["portfolio_context_summary"] = _summarize_portfolio_context_for_job(sanitized.get("portfolio_context"))
        sanitized.pop("portfolio_context", None)
    return sanitized


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

# ---------------------------------------------------------------------------
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

def _analysis_job_output_error(output_path: Path, request_payload: Optional[Dict[str, Any]]) -> str:
    """Return a user-facing error if a completed subprocess artifact is not usable."""
    if not output_path.exists() or not output_path.is_file():
        return "analysis subprocess did not produce output artifact"

    if str((request_payload or {}).get("job_type") or "").strip().lower() == "portfolio_positioning":
        return ""

    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return f"analysis subprocess produced unreadable output artifact: {exc}"

    structured = _extract_stage3_structured_from_artifact(payload)
    if isinstance(structured, dict) and structured:
        return ""

    stage3_result = _extract_stage3_result_from_artifact(payload) or {}
    if not stage3_result:
        primary = payload.get("stage3_result_primary")
        if isinstance(primary, dict):
            stage3_result = primary
        else:
            fallback = payload.get("stage3_result")
            stage3_result = fallback if isinstance(fallback, dict) else {}

    parse_error = str(stage3_result.get("parse_error") or "").strip()
    model = str(stage3_result.get("model") or payload.get("stage3_primary_model") or "").strip()
    detail = "Stage 3 did not produce structured data"
    if model:
        detail += f" (model={model})"
    if parse_error:
        detail += f": {parse_error}"
    return detail


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _ensure_analysis_job_dirs() -> None:
    JOBS_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    JOBS_META_DIR.mkdir(parents=True, exist_ok=True)
    PORTFOLIO_POSITIONING_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    PREPASS_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


def _build_analysis_job_env() -> Dict[str, str]:
    env = dict(os.environ)
    env.setdefault("ANALYSIS_JOBS_DIR", str(JOBS_OUTPUTS_DIR))
    env.setdefault("ANALYSIS_PREPASS_DIR", str(PREPASS_OUTPUTS_DIR))
    return env


ANALYSIS_JOB_HEARTBEAT_SECONDS = max(
    5.0,
    float(os.getenv("ANALYSIS_JOB_HEARTBEAT_SECONDS", "10") or 10),
)


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
    request_payload = _sanitize_analysis_request_payload(dict(job.get("request") or {}))
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
                payload["finished_at"] = str(payload.get("finished_at") or now_iso)
                output_error = _analysis_job_output_error(
                    output_path,
                    payload.get("request") if isinstance(payload.get("request"), dict) else {},
                )
                if output_error:
                    payload["status"] = "failed"
                    payload["error"] = output_error
                    payload["stage"] = "failed"
                    payload["stage_message"] = "Recovered failed run"
                else:
                    payload["status"] = "succeeded"
                    payload["run_id"] = str(payload.get("run_id") or output_path.name)
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
        elif status == "succeeded":
            output_error = _analysis_job_output_error(
                output_path,
                payload.get("request") if isinstance(payload.get("request"), dict) else {},
            )
            if output_error:
                payload["status"] = "failed"
                payload["error"] = output_error
                payload["stage"] = "failed"
                payload["stage_message"] = "Output artifact is not renderable"
                payload["finished_at"] = str(payload.get("finished_at") or now_iso)
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


async def _heartbeat_analysis_job(
    *,
    job_id: str,
    process: asyncio.subprocess.Process,
) -> None:
    """Keep SSE clients alive during long model calls with no subprocess stdout."""
    heartbeat_count = 0
    while process.returncode is None:
        await asyncio.sleep(float(ANALYSIS_JOB_HEARTBEAT_SECONDS))
        if process.returncode is not None:
            break
        heartbeat_count += 1
        now = _utc_now_iso()
        async with ANALYSIS_JOBS_LOCK:
            job = ANALYSIS_JOBS.get(str(job_id))
            if not isinstance(job, dict):
                break
            if str(job.get("status") or "").lower() != "running":
                break
            job["last_output_at"] = now
            job["heartbeat_at"] = now
            job["heartbeat_count"] = heartbeat_count
            _persist_job_record(job)


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

        heartbeat_task = asyncio.create_task(
            _heartbeat_analysis_job(
                job_id=job_id,
                process=process,
            )
        )
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
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task
        await asyncio.gather(stdout_task, stderr_task)
        returncode = int(process.returncode or 0)

        output_error = (
            _analysis_job_output_error(output_path, request_payload)
            if returncode == 0
            else ""
        )
        status = "succeeded" if returncode == 0 and not output_error else "failed"
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
            if str((request_payload or {}).get("job_type") or "").strip().lower() == "portfolio_positioning":
                _invalidate_portfolio_positioning_run_cache()
            else:
                _invalidate_gantt_run_cache()
        else:
            fields["error"] = (
                f"analysis subprocess failed (returncode={returncode})"
                if returncode != 0
                else output_error
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
        "heartbeat_at": str(job.get("heartbeat_at") or ""),
        "heartbeat_count": int(job.get("heartbeat_count") or 0),
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
