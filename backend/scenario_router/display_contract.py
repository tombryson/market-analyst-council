from __future__ import annotations

from typing import Any, Dict

RERUN_ACTIONS = {"full_rerun", "rerun_stage1", "run_delta_only"}
MARKET_ONLY_WATCH_REASON = (
    "The announcement did not match any saved thesis condition. "
    "Market context was reviewed separately, so the saved run stays on watch unless new announcement evidence warrants a refresh."
)
MARKET_ONLY_WATCH_FOLLOW_UP_STEPS = [
    "Keep the current lab run active.",
    "Review the next primary-source announcement for a thesis-condition match.",
]

NEGATIVE_TRAJECTORY_STATES = {"thesis_weakened", "timeline_delayed", "risk_increased"}
POSITIVE_TRAJECTORY_STATES = {"thesis_strengthened", "timeline_accelerated", "risk_reduced"}
OPEN_REVIEW_STATES = NEGATIVE_TRAJECTORY_STATES | {"needs_classification", "material_unmapped"}
CLEARED_STATES = {"market_backdrop_only", "no_thesis_change"}
ADMINISTRATIVE_STATES = {"administrative_filing"}

TRAJECTORY_LABELS = {
    "thesis_strengthened": "Thesis strengthened",
    "thesis_weakened": "Thesis weakened",
    "timeline_accelerated": "Timeline accelerated",
    "timeline_delayed": "Timeline delayed",
    "risk_reduced": "Risk reduced",
    "risk_increased": "Risk increased",
    "material_unmapped": "Material filing outside thesis map",
    "market_backdrop_only": "Market backdrop only",
    "administrative_filing": "Administrative filing",
    "no_thesis_change": "No thesis change",
    "needs_classification": "Needs classification",
}

SYSTEM_ACTION_LABELS = {
    "ignore": "No system action",
    "watch": "Monitor only",
    "annotate_run": "Add note",
    "run_delta_only": "Update section",
    "rerun_stage1": "Refresh evidence",
    "full_rerun": "Rebuild analysis",
    "urgent_human_review": "Urgent thesis review",
}

QUEUE_BUCKET_LABELS = {
    "open_review": "Needs thesis decision",
    "positive_movement": "Thesis improved",
    "cleared": "Cleared",
    "administrative": "Administrative",
    "all": "All filings",
}

REVIEW_LABELS = {
    "open": "Needs thesis decision",
    "tracking": "Track update",
    "auto_cleared": "Auto-cleared",
}

PRIMARY_REASONS = {
    "needs_classification": "The filing was captured, but the router could not confidently classify its thesis impact.",
    "material_unmapped": "No saved bull/base/bear condition covers this material filing.",
    "thesis_weakened": "The filing weakens the saved thesis path.",
    "timeline_delayed": "The filing pushes the saved timeline out.",
    "risk_increased": "The filing increases risk against the saved thesis path.",
    "thesis_strengthened": "The filing strengthens the saved thesis path.",
    "timeline_accelerated": "The filing pulls the saved timeline forward.",
    "risk_reduced": "The filing reduces risk against the saved thesis path.",
    "market_backdrop_only": "Market facts were checked as backdrop only; no filing-led thesis movement was found.",
    "no_thesis_change": "The filing was checked against the saved thesis and watchlist conditions with no mapped change.",
    "administrative_filing": "Administrative filing. Recorded without a thesis trajectory change.",
}


def should_project_market_only_watch(
    *,
    matched_conditions_count: int,
    triggered_watchlist_count: int,
    market_conditions_count: int,
    raw_action: str,
    materiality: str = "",
    trajectory_state: str = "",
) -> bool:
    """True when a rerun-looking result is only caused by market context, not announcement evidence."""
    materiality_norm = str(materiality or "").strip().lower()
    trajectory_norm = str(trajectory_state or "").strip().lower()
    if materiality_norm in {"medium", "high", "critical"}:
        return False
    if trajectory_norm in {
        "material_unmapped",
        "thesis_strengthened",
        "thesis_weakened",
        "timeline_accelerated",
        "timeline_delayed",
        "risk_reduced",
        "risk_increased",
        "needs_classification",
    }:
        return False
    return (
        int(matched_conditions_count or 0) == 0
        and int(triggered_watchlist_count or 0) == 0
        and int(market_conditions_count or 0) > 0
        and str(raw_action or "").strip() in RERUN_ACTIONS
    )


def build_router_display_contract(
    report: Dict[str, Any],
    action: Dict[str, Any],
    *,
    matched_conditions_count: int = 0,
    triggered_watchlist_count: int = 0,
    triggered_verification_count: int = 0,
) -> Dict[str, Any]:
    """Normalize router output into user-facing display axes.

    The core router verdict is the thesis trajectory state. Case state and
    system action are kept separate so the UI does not present workflow labels
    as case types.
    """

    report = report if isinstance(report, dict) else {}
    action = action if isinstance(action, dict) else {}
    state = _norm(report.get("trajectory_state"))
    action_key = _norm(action.get("action"))
    status = _norm(report.get("status"))
    direct_hit_count = (
        int(matched_conditions_count or 0)
        + int(triggered_watchlist_count or 0)
        + int(triggered_verification_count or 0)
    )
    has_verification_hit = int(triggered_verification_count or 0) > 0

    queue_bucket = _queue_bucket(
        state=state,
        action=action_key,
        has_verification_hit=has_verification_hit,
    )
    review_status = _review_status(queue_bucket)
    review_reason = _review_reason(
        state=state,
        queue_bucket=queue_bucket,
        has_verification_hit=has_verification_hit,
        direct_hit_count=direct_hit_count,
    )
    tone = _tone(
        state=state,
        action=action_key,
        status=status,
        queue_bucket=queue_bucket,
        has_direct_hit=direct_hit_count > 0,
    )

    return {
        "trajectory_state": state,
        "trajectory_label": TRAJECTORY_LABELS.get(state) or _titleize(state) or "Not assessed",
        "queue_bucket": queue_bucket,
        "queue_label": QUEUE_BUCKET_LABELS.get(queue_bucket, "All filings"),
        "review_status": review_status,
        "review_label": REVIEW_LABELS.get(review_status, "Status unknown"),
        "review_reason": review_reason,
        "is_user_action_required": review_status == "open",
        "system_action": action_key,
        "system_action_label": SYSTEM_ACTION_LABELS.get(action_key) or _titleize(action_key) or "No system action",
        "primary_reason": _primary_reason(state=state, action=action, direct_hit_count=direct_hit_count),
        "tone": tone,
    }


def market_only_watch_projection(
    *,
    baseline_path: str,
    current_path: str,
    raw_impact: str,
) -> Dict[str, Any]:
    current = str(baseline_path or current_path or "").strip()
    impact = str(raw_impact or "").strip()
    return {
        "action": "watch",
        "impact_level": "low" if impact != "critical" else impact,
        "current_path": current,
        "path_transition": "",
        "action_reason": MARKET_ONLY_WATCH_REASON,
        "reason": MARKET_ONLY_WATCH_REASON,
        "follow_up_steps": list(MARKET_ONLY_WATCH_FOLLOW_UP_STEPS),
        "invalidated_sections": [],
        "affected_domains": [],
        "thesis_effect": "no_change",
        "run_validity": "watch",
        "trajectory_state": "market_backdrop_only",
        "trajectory_effect": "no_clear_change",
        "price_time_effect": "Market-only context; no direct announcement-led trajectory change identified.",
        "display_adjustment": "market_context_only_watch",
    }


def _queue_bucket(*, state: str, action: str, has_verification_hit: bool) -> str:
    if state in ADMINISTRATIVE_STATES:
        return "administrative"
    if state in OPEN_REVIEW_STATES or has_verification_hit or action == "urgent_human_review":
        return "open_review"
    if state in POSITIVE_TRAJECTORY_STATES:
        return "positive_movement"
    if state in CLEARED_STATES:
        return "cleared"
    if action in {"full_rerun", "rerun_stage1", "run_delta_only"}:
        return "open_review"
    return "all"


def _review_status(queue_bucket: str) -> str:
    if queue_bucket == "open_review":
        return "open"
    if queue_bucket == "positive_movement":
        return "tracking"
    return "auto_cleared"


def _review_reason(
    *,
    state: str,
    queue_bucket: str,
    has_verification_hit: bool,
    direct_hit_count: int,
) -> str:
    if state == "needs_classification":
        return "classification_unresolved"
    if state == "material_unmapped":
        return "thesis_map_gap"
    if state in NEGATIVE_TRAJECTORY_STATES:
        return "negative_trajectory"
    if has_verification_hit:
        return "verification_hit"
    if state in POSITIVE_TRAJECTORY_STATES:
        return "positive_material_update"
    if direct_hit_count > 0 and queue_bucket == "open_review":
        return "mapped_evidence_hit"
    return "none"


def _tone(
    *,
    state: str,
    action: str,
    status: str,
    queue_bucket: str,
    has_direct_hit: bool,
) -> str:
    if status == "error" or action in {"urgent_human_review", "full_rerun"}:
        return "urgent"
    if state in NEGATIVE_TRAJECTORY_STATES:
        return "urgent"
    if state in {"needs_classification", "material_unmapped"} or queue_bucket == "open_review":
        return "warn"
    if state in POSITIVE_TRAJECTORY_STATES:
        return "positive"
    if action in {"rerun_stage1", "run_delta_only"}:
        return "alarm"
    if has_direct_hit or action == "annotate_run":
        return "warn"
    return "neutral"


def _primary_reason(*, state: str, action: Dict[str, Any], direct_hit_count: int) -> str:
    if direct_hit_count > 0:
        return f"Matched {direct_hit_count} saved thesis, watchlist, or verification item{'' if direct_hit_count == 1 else 's'}."
    if state in PRIMARY_REASONS:
        return PRIMARY_REASONS[state]
    return str(action.get("reason") or "").strip()


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _titleize(value: str) -> str:
    text = str(value or "").replace("_", " ").strip()
    return " ".join(part[:1].upper() + part[1:] for part in text.split()) if text else ""
