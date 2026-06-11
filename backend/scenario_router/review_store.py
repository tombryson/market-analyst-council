from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .lab_scribe import SCENARIO_ROUTER_EVENTS_DIR

REVIEWS_DIR = SCENARIO_ROUTER_EVENTS_DIR / "reviews"
VALID_REVIEW_STATUSES = {"open", "reviewed", "dismissed"}


def load_review(event_id: str, *, base_dir: Path = REVIEWS_DIR) -> Dict[str, Any]:
    key = _safe_key(event_id)
    if not key:
        return {}
    path = Path(base_dir) / f"{key}.json"
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def save_review(
    event_id: str,
    *,
    status: str,
    note: str = "",
    actor: str = "analyst",
    owner: str = "",
    base_dir: Path = REVIEWS_DIR,
) -> Dict[str, Any]:
    event_key = _safe_key(event_id)
    status_key = str(status or "").strip().lower()
    if not event_key:
        raise ValueError("event_id is required")
    if status_key not in VALID_REVIEW_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(sorted(VALID_REVIEW_STATUSES))}")

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    existing = load_review(event_key, base_dir=base_dir)
    payload = {
        **existing,
        "event_id": event_key,
        "review_status": status_key,
        "review_note": str(note or "").strip(),
        "reviewed_by": str(actor or "analyst").strip() or "analyst",
        "review_owner": str(owner or actor or "analyst").strip() or "analyst",
        "reviewed_at_utc": now,
        "updated_at_utc": now,
    }
    if not existing.get("created_at_utc"):
        payload["created_at_utc"] = now

    directory = Path(base_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{event_key}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    return payload


def apply_review_overlay(row: Dict[str, Any], review: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(row, dict) or not isinstance(review, dict) or not review:
        return row
    status = str(review.get("review_status") or "").strip().lower()
    if status == "escalated":
        status = "open"
    if status not in VALID_REVIEW_STATUSES or status == "open":
        if status == "open":
            review = {**review, "review_status": "open"}
        row["review_overlay"] = review
        return row

    display = row.get("display") if isinstance(row.get("display"), dict) else {}
    label = {
        "reviewed": "Reviewed",
        "dismissed": "Cleared",
    }.get(status, status.title())
    current_tone = str(display.get("tone") or "").strip()
    row["display"] = {
        **display,
        "queue_bucket": str(display.get("queue_bucket") or "").strip(),
        "queue_label": str(display.get("queue_label") or "").strip(),
        "review_status": status,
        "review_label": label,
        "review_owner": str(review.get("review_owner") or "").strip(),
        "is_user_action_required": False,
        "tone": current_tone or str(display.get("tone") or "").strip(),
    }
    row["review_overlay"] = review
    return row


def _safe_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._:-]+", "_", str(value or "").strip())[:180]
