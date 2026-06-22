"""Run artifact helpers for gantt-runs and portfolio-positioning-runs.

Includes memo extraction, path resolution, cache invalidation, and
ticker normalisation utilities.
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .state import (
    JOBS_META_DIR,
    JOBS_OUTPUTS_DIR,
    OUTPUTS_DIR,
    PORTFOLIO_POSITIONING_OUTPUTS_DIR,
    _GANTT_RUN_LIST_CACHE,
    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE,
)

logger = logging.getLogger(__name__)

def _is_portfolio_positioning_artifact(
    payload: Dict[str, Any],
    structured: Optional[Dict[str, Any]] = None,
) -> bool:
    """Identify portfolio-level artifacts so they do not leak into stock-analysis views."""
    if not isinstance(payload, dict):
        return False
    if str(payload.get("analysis_kind") or "").strip() == "portfolio_positioning":
        return True
    if isinstance(structured, dict) and str(structured.get("analysis_kind") or "").strip() == "portfolio_positioning":
        return True
    top_level_structured = payload.get("structured_data")
    if (
        isinstance(top_level_structured, dict)
        and str(top_level_structured.get("analysis_kind") or "").strip() == "portfolio_positioning"
    ):
        return True
    return False


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


def _portfolio_positioning_search_roots() -> List[Path]:
    roots = [PORTFOLIO_POSITIONING_OUTPUTS_DIR, JOBS_OUTPUTS_DIR, OUTPUTS_DIR]
    unique: Dict[str, Path] = {}
    for root in roots:
        try:
            unique[str(root.resolve())] = root
        except Exception:
            unique[str(root)] = root
    return list(unique.values())


def _resolve_portfolio_positioning_artifact_path(run_id: str) -> Path:
    safe_name = Path(run_id).name
    for root in _portfolio_positioning_search_roots():
        candidate = root / safe_name
        if candidate.exists() and candidate.is_file():
            return candidate
    return PORTFOLIO_POSITIONING_OUTPUTS_DIR / safe_name


def _invalidate_gantt_run_cache() -> None:
    _GANTT_RUN_LIST_CACHE["expires_at"] = 0.0
    _GANTT_RUN_LIST_CACHE["key"] = None
    _GANTT_RUN_LIST_CACHE["runs"] = None


def _invalidate_portfolio_positioning_run_cache() -> None:
    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE["expires_at"] = 0.0
    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE["key"] = None
    _PORTFOLIO_POSITIONING_RUN_LIST_CACHE["runs"] = None


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


def _collect_portfolio_positioning_related_paths(run_id: str) -> List[Path]:
    safe_name = Path(run_id).name
    canonical_path = _resolve_portfolio_positioning_artifact_path(safe_name)
    if not canonical_path.exists() or not canonical_path.is_file():
        return []

    related: Dict[str, Path] = {}
    stem = canonical_path.stem

    for candidate in canonical_path.parent.iterdir():
        if not candidate.is_file():
            continue
        if candidate.name == canonical_path.name or candidate.name.startswith(f"{stem}."):
            related[str(candidate.resolve())] = candidate

    if JOBS_META_DIR.exists():
        for meta_path in JOBS_META_DIR.glob("*.json"):
            try:
                meta_payload = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            meta_run_id = Path(str(meta_payload.get("run_id") or "")).name
            meta_output_id = Path(str(meta_payload.get("output_path") or "")).name
            if safe_name and safe_name in {meta_run_id, meta_output_id}:
                related[str(meta_path.resolve())] = meta_path

    return sorted(related.values(), key=lambda path: str(path))

