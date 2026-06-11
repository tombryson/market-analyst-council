from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from .lab_scribe import SCENARIO_ROUTER_EVENTS_DIR

REVIEWS_DIR = SCENARIO_ROUTER_EVENTS_DIR / "reviews"
VALID_REVIEW_STATUSES = {"open", "reviewed", "dismissed", "escalated"}
VALID_ESCALATION_REASONS = {
    "router_misclassified",
    "thesis_map_gap",
    "timeline_changed",
    "valuation_changed",
    "contradicts_saved_thesis",
    "needs_evidence_refresh",
    "needs_full_rerun",
    "source_verification",
    "other",
}

ESCALATION_REASON_LABELS = {
    "router_misclassified": "Check router classification",
    "thesis_map_gap": "Create thesis-map update task",
    "timeline_changed": "Review timeline assumptions",
    "valuation_changed": "Review valuation assumptions",
    "contradicts_saved_thesis": "Review thesis conflict",
    "needs_evidence_refresh": "Add filing to saved analysis",
    "needs_full_rerun": "Rebuild council analysis",
    "source_verification": "Verify source and extraction",
    "other": "Other analyst concern",
}

NEXT_ACTION_LABELS = {
    "add_note": "Add note",
    "update_thesis_map": "Add thesis-map condition",
    "refresh_evidence": "Update saved analysis",
    "rebuild_analysis": "Rebuild analysis",
    "verify_source": "Verify source",
    "label_filing": "Label filing",
    "none": "No system action",
}

NEXT_ACTION_QUEUE_LABELS = {
    "add_note": "Queued note",
    "update_thesis_map": "Queued for thesis-map update",
    "refresh_evidence": "Analysis update queue",
    "rebuild_analysis": "Council rebuild queue",
    "verify_source": "Source check queue",
    "label_filing": "Classification queue",
}

REASON_TO_NEXT_ACTION = {
    "router_misclassified": "label_filing",
    "thesis_map_gap": "update_thesis_map",
    "timeline_changed": "refresh_evidence",
    "valuation_changed": "refresh_evidence",
    "contradicts_saved_thesis": "rebuild_analysis",
    "needs_evidence_refresh": "refresh_evidence",
    "needs_full_rerun": "rebuild_analysis",
    "source_verification": "verify_source",
    "other": "add_note",
}


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
    escalation_reason: str = "",
    next_action: str = "",
    owner: str = "",
    base_dir: Path = REVIEWS_DIR,
) -> Dict[str, Any]:
    event_key = _safe_key(event_id)
    status_key = str(status or "").strip().lower()
    reason_key = str(escalation_reason or "").strip().lower()
    next_action_key = str(next_action or "").strip().lower()
    if not event_key:
        raise ValueError("event_id is required")
    if status_key not in VALID_REVIEW_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(sorted(VALID_REVIEW_STATUSES))}")
    if reason_key and reason_key not in VALID_ESCALATION_REASONS:
        raise ValueError(f"escalation_reason must be one of: {', '.join(sorted(VALID_ESCALATION_REASONS))}")
    if next_action_key and next_action_key not in NEXT_ACTION_LABELS:
        raise ValueError(f"next_action must be one of: {', '.join(sorted(NEXT_ACTION_LABELS))}")
    if status_key == "escalated" and not reason_key:
        raise ValueError("escalation_reason is required when status is escalated")
    if not next_action_key:
        next_action_key = REASON_TO_NEXT_ACTION.get(reason_key, "none" if status_key in {"reviewed", "dismissed"} else "add_note")

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    existing = load_review(event_key, base_dir=base_dir)
    payload = {
        **existing,
        "event_id": event_key,
        "review_status": status_key,
        "review_note": str(note or "").strip(),
        "reviewed_by": str(actor or "analyst").strip() or "analyst",
        "review_owner": str(owner or actor or "analyst").strip() or "analyst",
        "escalation_reason": reason_key,
        "escalation_reason_label": ESCALATION_REASON_LABELS.get(reason_key, ""),
        "next_action": next_action_key,
        "next_action_label": NEXT_ACTION_LABELS.get(next_action_key, _titleize(next_action_key)),
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
    if status not in VALID_REVIEW_STATUSES or status == "open":
        row["review_overlay"] = review
        return row

    display = row.get("display") if isinstance(row.get("display"), dict) else {}
    label = {
        "reviewed": "Reviewed",
        "dismissed": "Dismissed",
        "escalated": "Queued task",
    }.get(status, status.title())
    reason_label = str(review.get("escalation_reason_label") or "").strip()
    next_action_label = str(review.get("next_action_label") or "").strip()
    next_action = str(review.get("next_action") or "").strip()
    queued_label = NEXT_ACTION_QUEUE_LABELS.get(next_action, "Queued follow-up")
    current_tone = str(display.get("tone") or "").strip()
    row["display"] = {
        **display,
        "queue_bucket": str(display.get("queue_bucket") or "").strip(),
        "queue_label": str(display.get("queue_label") or "").strip(),
        "review_queue_label": queued_label if status == "escalated" else "",
        "review_status": status,
        "review_label": label,
        "review_reason": str(review.get("escalation_reason") or display.get("review_reason") or "").strip(),
        "review_reason_label": reason_label,
        "review_owner": str(review.get("review_owner") or "").strip(),
        "next_action": next_action,
        "next_action_label": next_action_label,
        "is_user_action_required": False,
        "tone": current_tone or str(display.get("tone") or "").strip(),
    }
    row["review_overlay"] = review
    return row


def _safe_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._:-]+", "_", str(value or "").strip())[:180]


def _titleize(value: str) -> str:
    return " ".join(part[:1].upper() + part[1:] for part in str(value or "").replace("_", " ").split())
