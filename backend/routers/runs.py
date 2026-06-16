"""HTTP routes for gantt-runs and portfolio-positioning-runs.

Includes listing, retrieval, deletion, report-packet generation, and
delta-check endpoints.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

from ..delta_monitor import get_latest_delta, run_delta_check
from ..jobs.runs import (
    _build_gantt_run_label,
    _collect_portfolio_positioning_related_paths,
    _collect_run_related_paths,
    _compute_run_freshness,
    _extract_memo_content,
    _invalidate_gantt_run_cache,
    _invalidate_portfolio_positioning_run_cache,
    _is_portfolio_positioning_artifact,
    _normalize_run_ticker,
    _portfolio_positioning_search_roots,
    _resolve_portfolio_positioning_artifact_path,
    _resolve_run_artifact_path,
    _run_ticker_aliases,
)
from ..jobs.structured import (
    _backfill_stage2_ranking_telemetry,
    _build_integration_packet,
    _build_portfolio_positioning_run_label,
    _build_scenario_router_summary,
    _build_summary_fields,
    _canonical_run_id_for_listing,
    _cap_previous_catalyst_rows,
    _extract_stage3_result_from_artifact,
    _extract_stage3_structured_from_artifact,
    _infer_current_price_from_artifact,
    _load_latest_scenario_router_state,
    _normalize_timeline_rows_for_api,
)
from ..jobs.state import (
    GANTT_RUN_LIST_CACHE_TTL_SEC,
    JOBS_OUTPUTS_DIR,
    OUTPUTS_DIR,
    PROJECT_ROOT,
    _GANTT_RUN_LIST_CACHE,
    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE,
)
from ..utils import _ensure_system_enabled

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/api/portfolio-positioning-runs")
async def list_portfolio_positioning_runs(limit: int = 20):
    """List portfolio-positioning memo artifacts without mixing them into stock analysis runs."""
    safe_limit = max(1, int(limit))
    now_ts = time.time()
    cache_key = _PORTFOLIO_POSITIONING_RUN_LIST_CACHE.get("key")
    cache_runs = _PORTFOLIO_POSITIONING_RUN_LIST_CACHE.get("runs")
    cache_expiry = float(_PORTFOLIO_POSITIONING_RUN_LIST_CACHE.get("expires_at") or 0.0)
    requested_cache_key = str(safe_limit)
    if (
        isinstance(cache_runs, list)
        and cache_key == requested_cache_key
        and now_ts <= cache_expiry
    ):
        return {"runs": cache_runs}

    all_json: List[Path] = []
    for root in _portfolio_positioning_search_roots():
        if root.exists():
            all_json.extend(root.glob("*.json"))
    all_json = sorted({str(path.resolve()): path for path in all_json}.values(), key=lambda p: p.stat().st_mtime, reverse=True)

    runs: List[Dict[str, Any]] = []
    for path in all_json:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        structured = _extract_stage3_structured_from_artifact(payload)
        if not isinstance(structured, dict) or not _is_portfolio_positioning_artifact(payload, structured):
            continue

        updated_at = datetime.utcfromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat() + "Z"
        runs.append(
            {
                "id": path.name,
                "file": path.name,
                "label": _build_portfolio_positioning_run_label(path.stem, structured, payload),
                "analysis_kind": "portfolio_positioning",
                "analysis_date": structured.get("analysis_date"),
                "updated_at": updated_at,
                "mode": structured.get("mode") or payload.get("mode"),
                "summary_fields": _build_summary_fields(structured, {}),
            }
        )
        if len(runs) >= safe_limit:
            break

    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE["key"] = requested_cache_key
    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE["runs"] = runs
    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE["expires_at"] = now_ts + float(GANTT_RUN_LIST_CACHE_TTL_SEC)
    return {"runs": runs}


@router.get("/api/portfolio-positioning-runs/{run_id}")
async def get_portfolio_positioning_run(run_id: str):
    """Load one portfolio-positioning artifact for the dedicated portfolio memo UI."""
    safe_name = Path(run_id).name
    path = _resolve_portfolio_positioning_artifact_path(safe_name)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Portfolio positioning artifact not found")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to parse artifact: {exc}") from exc

    stage3_result = _extract_stage3_result_from_artifact(payload) or {}
    structured = stage3_result.get("structured_data")
    if not isinstance(structured, dict) or not _is_portfolio_positioning_artifact(payload, structured):
        raise HTTPException(status_code=400, detail="Artifact is not a portfolio positioning memo")

    artifact_updated_at = datetime.utcfromtimestamp(path.stat().st_mtime).replace(microsecond=0).isoformat() + "Z"
    analyst_document = stage3_result.get("analyst_document") if isinstance(stage3_result, dict) else {}
    chairman_document = stage3_result.get("chairman_document") if isinstance(stage3_result, dict) else {}
    if not isinstance(analyst_document, dict):
        analyst_document = {}
    if not isinstance(chairman_document, dict):
        chairman_document = {}
    memo_markdown = (
        str(payload.get("chairman_memo_markdown") or "").strip()
        or str(payload.get("analyst_memo_markdown") or "").strip()
        or str((chairman_document or {}).get("content_markdown") or "").strip()
        or str((analyst_document or {}).get("content_markdown") or "").strip()
    )

    return {
        "id": safe_name,
        "file": safe_name,
        "label": _build_portfolio_positioning_run_label(path.stem, structured, payload),
        "analysis_kind": "portfolio_positioning",
        "structured_data": structured,
        "portfolio_snapshot": payload.get("portfolio_snapshot") if isinstance(payload.get("portfolio_snapshot"), dict) else {},
        "evidence_brief": payload.get("evidence_brief") if isinstance(payload.get("evidence_brief"), dict) else {},
        "allocator_council_runs": payload.get("allocator_council_runs") if isinstance(payload.get("allocator_council_runs"), list) else [],
        "macro_positioning": payload.get("macro_positioning") if isinstance(payload.get("macro_positioning"), dict) else {},
        "allocator_commentary": payload.get("allocator_commentary") if isinstance(payload.get("allocator_commentary"), dict) else {},
        "updated_at": artifact_updated_at,
        "summary_fields": _build_summary_fields(structured, {}),
        "analyst_memo_markdown": str(payload.get("analyst_memo_markdown") or "").strip(),
        "chairman_memo_markdown": str(payload.get("chairman_memo_markdown") or "").strip() or memo_markdown,
        "memo_markdown": memo_markdown,
        "analyst_document": analyst_document,
        "chairman_document": chairman_document,
    }


@router.delete("/api/portfolio-positioning-runs/{run_id}")
async def delete_portfolio_positioning_run(run_id: str):
    safe_name = Path(run_id).name
    related_paths = _collect_portfolio_positioning_related_paths(safe_name)
    if not related_paths:
        raise HTTPException(status_code=404, detail="Portfolio positioning artifact not found")

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

    _invalidate_portfolio_positioning_run_cache()

    status = "deleted" if not failed else ("partial" if deleted else "failed")
    if not deleted and failed:
        raise HTTPException(
            status_code=500,
            detail={
                "status": status,
                "run_id": safe_name,
                "deleted_count": 0,
                "failed": failed,
            },
        )
    return {
        "status": status,
        "run_id": safe_name,
        "deleted_count": len(deleted),
        "deleted": deleted,
        "failed": failed,
    }


@router.get("/api/gantt-runs")
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
        if _is_portfolio_positioning_artifact(payload, structured):
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


@router.get("/api/gantt-runs/{run_id}")
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
    if _is_portfolio_positioning_artifact(payload, structured):
        raise HTTPException(
            status_code=400,
            detail="Portfolio positioning artifacts must be loaded via /api/portfolio-positioning-runs/{run_id}",
        )
    _backfill_stage2_ranking_telemetry(structured, payload)

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


@router.delete("/api/gantt-runs/{run_id}")
async def delete_gantt_run(run_id: str):
    safe_name = Path(run_id).name
    canonical_id = _canonical_run_id_for_listing(safe_name)
    existing_path = _resolve_run_artifact_path(canonical_id)
    if existing_path.exists() and existing_path.is_file():
        try:
            existing_payload = json.loads(existing_path.read_text(encoding="utf-8"))
            existing_structured = _extract_stage3_structured_from_artifact(existing_payload)
            if _is_portfolio_positioning_artifact(existing_payload, existing_structured):
                raise HTTPException(
                    status_code=400,
                    detail="Portfolio positioning artifacts must be deleted via /api/portfolio-positioning-runs/{run_id}",
                )
        except HTTPException:
            raise
        except Exception:
            pass
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


@router.get("/api/gantt-runs/{run_id}/report-packet")
async def get_gantt_run_report_packet(run_id: str):
    """
    Return a single integration packet for external apps:
    summary fields + full gantt-lab payload + memo markdown.
    """
    run_payload = await get_gantt_run(run_id)
    return _build_integration_packet(run_id=str(run_payload.get("id") or run_id), run_payload=run_payload)

@router.get("/api/gantt-runs/{run_id}/delta-check/latest")
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


@router.post("/api/gantt-runs/{run_id}/delta-check")
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



