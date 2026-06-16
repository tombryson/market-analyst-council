"""Authoritative prepass bundle infrastructure for Stage-1 source retrieval.

Shared by the conversations streaming handler and the analysis-jobs executor.
"""

import asyncio
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .state import ANALYSIS_JOB_LOG_TAIL_CHARS, PREPASS_OUTPUTS_DIR, PROJECT_ROOT
from ..prepass_cache import (
    delta_lookback_days_from_cache,
    load_cached_prepass_rows,
    merge_prepass_source_rows,
    prepass_cache_enabled,
    prepass_cache_max_age_days,
    prepass_delta_enabled,
    prepass_delta_max_sources,
    prepass_delta_target_non_price_sensitive,
    prepass_delta_target_price_sensitive,
    resolve_prepass_cache_root,
    save_cached_prepass_rows,
)

logger = logging.getLogger(__name__)


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


async def _run_authoritative_prepass_bundle_subprocess(
    *,
    ticker: str,
    query_hint: str,
    exchange: str,
    exchange_retrieval_params: Optional[Dict[str, Any]],
    company_name: Optional[str],
    output_suffix: str,
    target_price_sensitive: int,
    target_non_price_sensitive: int,
    max_sources: int,
    lookback_days: int,
    strategy: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    PREPASS_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = (
        PREPASS_OUTPUTS_DIR
        / f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{_sanitize_ticker_for_dir(ticker)}_{output_suffix}"
    )

    query_seed = str(company_name or "").strip() or str(query_hint or "").strip() or str(ticker or "").strip()
    query_seed = re.sub(r"\s+", " ", query_seed).strip()[:120]
    retrieval_query = f"Latest material filings, announcements, and investor updates for {query_seed}"

    top_default = max(1, int(target_price_sensitive) + int(target_non_price_sensitive))
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
    if str(company_name or "").strip():
        cmd.extend(["--company-name", str(company_name).strip()])
    if max_sources > 0:
        cmd.extend(["--max-sources", str(max_sources)])
    if lookback_days > 0:
        cmd.extend(["--lookback-days", str(lookback_days)])
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
    meta["strategy"] = strategy
    meta["output_dir"] = str(output_dir)
    meta["prepass_top"] = top_default
    meta["prepass_target_price_sensitive"] = target_price_sensitive
    meta["prepass_target_non_price_sensitive"] = target_non_price_sensitive
    meta["prepass_max_sources"] = max_sources
    meta["prepass_lookback_days"] = lookback_days
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
    meta["exchange_retrieval_params"] = dict(exchange_retrieval_params or {})
    return rows, meta


async def _prepare_stage1_authoritative_prepass_bundle(
    *,
    ticker: str,
    query_hint: str,
    exchange: str,
    exchange_retrieval_params: Optional[Dict[str, Any]] = None,
    company_name: Optional[str] = None,
    template_id: str = "",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not str(ticker or "").strip():
        raise RuntimeError("ticker_required_for_authoritative_prepass")

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
    max_sources_default = int(retrieval_params.get("max_sources_default", 0) or 0)
    lookback_days_default = int(retrieval_params.get("lookback_days_default", 0) or 0)

    cache_root = resolve_prepass_cache_root(PREPASS_OUTPUTS_DIR)
    if prepass_cache_enabled():
        cached_rows, cache_meta = load_cached_prepass_rows(
            cache_root=cache_root,
            ticker=ticker,
            exchange=exchange,
            template_id=template_id,
            max_age_days=prepass_cache_max_age_days(),
        )
        if cached_rows:
            try:
                delta_rows: List[Dict[str, Any]] = []
                delta_meta: Dict[str, Any] = {"strategy": "prepass_cache_delta_skipped"}
                if prepass_delta_enabled():
                    delta_lookback_days = delta_lookback_days_from_cache(cache_meta)
                    delta_rows, delta_meta = await _run_authoritative_prepass_bundle_subprocess(
                        ticker=ticker,
                        query_hint=query_hint,
                        exchange=exchange,
                        exchange_retrieval_params=exchange_retrieval_params,
                        company_name=company_name,
                        output_suffix="api_delta",
                        target_price_sensitive=prepass_delta_target_price_sensitive(),
                        target_non_price_sensitive=prepass_delta_target_non_price_sensitive(),
                        max_sources=prepass_delta_max_sources(),
                        lookback_days=delta_lookback_days,
                        strategy="built_delta_bundle",
                    )
                merged_rows, merge_meta = merge_prepass_source_rows(
                    cached_rows=cached_rows,
                    delta_rows=delta_rows,
                    max_rows=max(24, len(cached_rows), len(delta_rows)),
                )
                save_meta = save_cached_prepass_rows(
                    cache_root=cache_root,
                    ticker=ticker,
                    exchange=exchange,
                    template_id=template_id,
                    company_name=str(company_name or ""),
                    source_rows=merged_rows,
                    source_meta={
                        "strategy": "reused_cached_prepass_with_delta",
                        "prepass_top": len(merged_rows),
                        "prepass_lookback_days": cache_meta.get("prepass_lookback_days"),
                        "bundle_path": delta_meta.get("bundle_path") or cache_meta.get("bundle_path"),
                        "output_dir": delta_meta.get("output_dir") or "",
                    },
                    preserve_created_at=True,
                )
                return merged_rows, {
                    "strategy": "reused_cached_prepass_with_delta",
                    "cache": cache_meta,
                    "delta": delta_meta,
                    "merge": merge_meta,
                    "cache_save": save_meta,
                    "bundle_path": str(cache_meta.get("cache_path") or ""),
                    "rows_built": len(merged_rows),
                    "prepass_top": len(merged_rows),
                    "prepass_target_price_sensitive": prepass_delta_target_price_sensitive(),
                    "prepass_target_non_price_sensitive": prepass_delta_target_non_price_sensitive(),
                    "prepass_max_sources": prepass_delta_max_sources(),
                    "prepass_lookback_days": delta_meta.get(
                        "prepass_lookback_days",
                        delta_lookback_days_from_cache(cache_meta),
                    ),
                }
            except Exception:
                # Cache reuse is an optimisation. Fall through to the full
                # authoritative prepass if the delta pass cannot be built.
                pass

    rows, meta = await _run_authoritative_prepass_bundle_subprocess(
        ticker=ticker,
        query_hint=query_hint,
        exchange=exchange,
        exchange_retrieval_params=exchange_retrieval_params,
        company_name=company_name,
        output_suffix="api_prepass",
        target_price_sensitive=target_price_sensitive,
        target_non_price_sensitive=target_non_price_sensitive,
        max_sources=max_sources_default,
        lookback_days=lookback_days_default,
        strategy="authoritative_prepass_bundle",
    )
    if prepass_cache_enabled() and rows:
        meta["cache_save"] = save_cached_prepass_rows(
            cache_root=cache_root,
            ticker=ticker,
            exchange=exchange,
            template_id=template_id,
            company_name=str(company_name or ""),
            source_rows=rows,
            source_meta=meta,
        )
    return rows, meta
