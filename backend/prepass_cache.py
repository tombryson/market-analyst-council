from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PREPASS_CACHE_SCHEMA_VERSION = 1
PREPASS_CACHE_VERSION = "stage1_prepass_rows_v1"


def prepass_cache_enabled() -> bool:
    value = str(os.getenv("PREPASS_CACHE_ENABLED", "true") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def prepass_cache_max_age_days() -> int:
    raw = str(os.getenv("PREPASS_CACHE_MAX_AGE_DAYS", "90") or "90").strip()
    try:
        return max(1, int(float(raw)))
    except Exception:
        return 90


def prepass_delta_enabled() -> bool:
    value = str(os.getenv("PREPASS_CACHE_DELTA_ENABLED", "true") or "").strip().lower()
    return value not in {"0", "false", "no", "off"}


def prepass_delta_target_price_sensitive() -> int:
    return _env_int("PREPASS_CACHE_DELTA_TARGET_PRICE_SENSITIVE", 8, minimum=1)


def prepass_delta_target_non_price_sensitive() -> int:
    return _env_int("PREPASS_CACHE_DELTA_TARGET_NON_PRICE_SENSITIVE", 4, minimum=0)


def prepass_delta_max_sources() -> int:
    return _env_int("PREPASS_CACHE_DELTA_MAX_SOURCES", 12, minimum=1)


def resolve_prepass_cache_root(prepass_root: Path) -> Path:
    configured = str(os.getenv("PREPASS_CACHE_DIR", "") or "").strip()
    if configured:
        return Path(configured)
    return Path(prepass_root) / "cache"


def load_cached_prepass_rows(
    *,
    cache_root: Path,
    ticker: str,
    exchange: str = "",
    template_id: str = "",
    max_age_days: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cache_path = _cache_path(
        cache_root=cache_root,
        ticker=ticker,
        exchange=exchange,
        template_id=template_id,
    )
    if not cache_path.exists() or not cache_path.is_file():
        return [], {"cache_status": "miss", "cache_path": str(cache_path)}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [], {"cache_status": "unreadable", "cache_path": str(cache_path), "cache_error": str(exc)}

    status, reason = _validate_cache_payload(
        payload,
        ticker=ticker,
        exchange=exchange,
        template_id=template_id,
        max_age_days=max_age_days if max_age_days is not None else prepass_cache_max_age_days(),
    )
    meta = dict(payload.get("provenance") or {})
    meta.update(
        {
            "cache_status": status,
            "cache_reason": reason,
            "cache_path": str(cache_path),
            "cache_created_at_utc": str(payload.get("created_at_utc") or ""),
            "cache_updated_at_utc": str(payload.get("updated_at_utc") or ""),
            "cache_rows_count": len(payload.get("source_rows") or []),
        }
    )
    if status != "hit":
        return [], meta

    rows = [dict(row) for row in (payload.get("source_rows") or []) if isinstance(row, dict)]
    return renumber_source_rows(rows), meta


def save_cached_prepass_rows(
    *,
    cache_root: Path,
    ticker: str,
    exchange: str = "",
    template_id: str = "",
    company_name: str = "",
    source_rows: List[Dict[str, Any]],
    source_meta: Optional[Dict[str, Any]] = None,
    preserve_created_at: bool = False,
) -> Dict[str, Any]:
    rows = renumber_source_rows(source_rows or [])
    cache_path = _cache_path(
        cache_root=cache_root,
        ticker=ticker,
        exchange=exchange,
        template_id=template_id,
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    now = _utc_now()

    existing_created_at = ""
    if preserve_created_at and cache_path.exists() and cache_path.is_file():
        try:
            existing = json.loads(cache_path.read_text(encoding="utf-8"))
            existing_created_at = str(existing.get("created_at_utc") or "")
        except Exception:
            existing_created_at = ""

    payload = {
        "schema_version": PREPASS_CACHE_SCHEMA_VERSION,
        "prepass_version": PREPASS_CACHE_VERSION,
        "ticker": str(ticker or "").strip(),
        "ticker_norm": _normalize_ticker(ticker),
        "exchange": str(exchange or "").strip().upper(),
        "template_id": str(template_id or "").strip(),
        "company_name": str(company_name or "").strip(),
        "created_at_utc": existing_created_at or now,
        "updated_at_utc": now,
        "source_rows": rows,
        "provenance": build_prepass_cache_provenance(source_rows=rows, source_meta=source_meta or {}),
    }
    cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return {
        "cache_status": "saved",
        "cache_path": str(cache_path),
        "cache_rows_count": len(rows),
        "cache_created_at_utc": payload["created_at_utc"],
        "cache_updated_at_utc": now,
    }


def merge_prepass_source_rows(
    *,
    cached_rows: List[Dict[str, Any]],
    delta_rows: List[Dict[str, Any]],
    max_rows: int = 24,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()
    duplicate_count = 0

    for origin, rows in (("delta", delta_rows or []), ("cache", cached_rows or [])):
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = source_row_identity(row)
            if key in seen:
                duplicate_count += 1
                continue
            seen.add(key)
            next_row = dict(row)
            next_row["prepass_row_origin"] = origin
            merged.append(next_row)
            if len(merged) >= max(1, int(max_rows or 24)):
                return renumber_source_rows(merged), {
                    "cached_rows_count": len(cached_rows or []),
                    "delta_rows_count": len(delta_rows or []),
                    "merged_rows_count": len(merged),
                    "deduplicated_rows_count": duplicate_count,
                }

    return renumber_source_rows(merged), {
        "cached_rows_count": len(cached_rows or []),
        "delta_rows_count": len(delta_rows or []),
        "merged_rows_count": len(merged),
        "deduplicated_rows_count": duplicate_count,
    }


def renumber_source_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, row in enumerate([item for item in rows if isinstance(item, dict)], 1):
        next_row = dict(row)
        next_row["source_id"] = f"S{idx}"
        out.append(next_row)
    return out


def source_row_identity(row: Dict[str, Any]) -> str:
    url = _normalize_url(row.get("url"))
    if url:
        return f"url:{url}"
    title = re.sub(r"\s+", " ", str(row.get("title") or "").strip().lower())
    date = str(row.get("published_at") or "").strip()[:10]
    if title or date:
        return f"title_date:{title}|{date}"
    excerpt = str(row.get("excerpt") or "").strip()
    return "excerpt:" + hashlib.sha256(excerpt.encode("utf-8", errors="ignore")).hexdigest()


def build_prepass_cache_provenance(
    *,
    source_rows: List[Dict[str, Any]],
    source_meta: Dict[str, Any],
) -> Dict[str, Any]:
    rows = [row for row in source_rows or [] if isinstance(row, dict)]
    return {
        "strategy": str(source_meta.get("strategy") or "").strip(),
        "rows_count": len(rows),
        "source_urls": [str(row.get("url") or "").strip() for row in rows if str(row.get("url") or "").strip()][:80],
        "document_titles": [str(row.get("title") or "").strip() for row in rows if str(row.get("title") or "").strip()][:80],
        "price_sensitive_count": sum(1 for row in rows if bool(row.get("bundle_price_sensitive"))),
        "prepass_top": source_meta.get("prepass_top"),
        "prepass_lookback_days": source_meta.get("prepass_lookback_days"),
        "prepass_selected_primary_candidates": source_meta.get("prepass_selected_primary_candidates"),
        "prepass_retrieved_sources": source_meta.get("prepass_retrieved_sources"),
        "bundle_path": str(source_meta.get("bundle_path") or "").strip(),
        "output_dir": str(source_meta.get("output_dir") or "").strip(),
    }


def delta_lookback_days_from_cache(cache_meta: Dict[str, Any]) -> int:
    raw = str(
        cache_meta.get("cache_updated_at_utc")
        or cache_meta.get("cache_created_at_utc")
        or ""
    ).strip()
    if not raw:
        return 14
    try:
        cache_dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return 14
    age_days = max(1, (datetime.utcnow() - cache_dt).days + 3)
    return max(7, min(prepass_cache_max_age_days() + 7, age_days))


def _cache_path(
    *,
    cache_root: Path,
    ticker: str,
    exchange: str = "",
    template_id: str = "",
) -> Path:
    ticker_key = _safe_key(_normalize_ticker(ticker) or "unknown")
    exchange_key = _safe_key(str(exchange or "").strip().upper() or "default")
    template_key = _safe_key(str(template_id or "").strip() or "default")
    return Path(cache_root) / ticker_key / exchange_key / template_key / "latest.json"


def _validate_cache_payload(
    payload: Dict[str, Any],
    *,
    ticker: str,
    exchange: str = "",
    template_id: str = "",
    max_age_days: int,
) -> Tuple[str, str]:
    if not isinstance(payload, dict):
        return "invalid", "payload_not_object"
    if int(payload.get("schema_version") or 0) != PREPASS_CACHE_SCHEMA_VERSION:
        return "stale", "schema_version_changed"
    if str(payload.get("prepass_version") or "") != PREPASS_CACHE_VERSION:
        return "stale", "prepass_version_changed"
    if _normalize_ticker(payload.get("ticker_norm") or payload.get("ticker")) != _normalize_ticker(ticker):
        return "miss", "ticker_mismatch"
    exchange_norm = str(exchange or "").strip().upper()
    payload_exchange = str(payload.get("exchange") or "").strip().upper()
    if exchange_norm and payload_exchange and payload_exchange != exchange_norm:
        return "miss", "exchange_mismatch"
    template_norm = str(template_id or "").strip()
    payload_template = str(payload.get("template_id") or "").strip()
    if template_norm and payload_template and payload_template != template_norm:
        return "miss", "template_mismatch"
    rows = payload.get("source_rows")
    if not isinstance(rows, list) or not rows:
        return "invalid", "empty_source_rows"

    timestamp = str(payload.get("created_at_utc") or "").strip()
    if not timestamp:
        return "stale", "missing_cache_timestamp"
    try:
        cache_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return "stale", "unparseable_cache_timestamp"
    if cache_dt < datetime.utcnow() - timedelta(days=max(1, int(max_age_days))):
        return "stale", "cache_too_old"
    return "hit", "cache_valid"


def _normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_url(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"#.*$", "", text)
    text = re.sub(r"\?.*$", "", text)
    return text.rstrip("/")


def _safe_key(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^A-Za-z0-9._:-]+", "_", text)
    return text.strip("_")[:180] or "unknown"


def _utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.getenv(name, str(default)) or str(default)).strip()
    try:
        return max(minimum, int(float(raw)))
    except Exception:
        return max(minimum, int(default))
