from __future__ import annotations

from typing import Any, Dict

from .trajectory_scoring import baseline_path_score, position_label, score_band

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
    "ignore": "No maintenance",
    "watch": "Monitor only",
    "annotate_run": "Attach to thesis log",
    "run_delta_only": "Update thesis note",
    "rerun_stage1": "Refresh evidence pack",
    "full_rerun": "Rebuild council run",
    "urgent_human_review": "Human review now",
}

QUEUE_BUCKET_LABELS = {
    "open_review": "Needs thesis decision",
    "positive_movement": "Thesis improved",
    "negative_movement": "Thesis weakened",
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
    "material_unmapped": "No saved thesis condition covers this filing.",
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

RELATIONSHIP_LABELS = {
    "administrative": "Administrative filing",
    "no_relation": "No saved relationship",
    "market_backdrop_only": "Market backdrop only",
    "material_unmapped": "Material outside thesis map",
    "needs_classification": "Needs classification",
    "saved_thesis_condition": "Saved thesis condition",
    "saved_thesis_failure": "Saved thesis failure",
    "watchlist_red_flag": "Watchlist red flag",
    "watchlist_confirmatory": "Watchlist signal",
    "verification_queue": "Verification item",
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
    checked_watchlist_count: int = 0,
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
    relationship_kind = _norm(report.get("relationship_kind"))
    relationship_strength = _norm(report.get("relationship_strength"))
    relationship_priority = report.get("relationship_priority", 0)
    filing_type = _norm(report.get("filing_type"))
    evidence_scope = _norm(report.get("evidence_scope"))
    thesis_relationship = _norm(report.get("thesis_relationship"))
    impact_verdict = _norm(report.get("impact_verdict"))
    impact_dimension = _norm(report.get("impact_dimension"))
    unvalidated_positive_support = _is_unvalidated_positive_support(
        report=report,
        impact_verdict=impact_verdict,
        direct_hit_count=direct_hit_count,
    )

    queue_bucket = _queue_bucket(
        state=state,
        action=action_key,
        has_verification_hit=has_verification_hit,
        filing_type=filing_type,
        thesis_relationship=thesis_relationship,
        impact_verdict=impact_verdict,
        unvalidated_positive_support=unvalidated_positive_support,
    )
    review_status = _review_status(queue_bucket)
    review_reason = _review_reason(
        state=state,
        queue_bucket=queue_bucket,
        has_verification_hit=has_verification_hit,
        direct_hit_count=direct_hit_count,
        thesis_relationship=thesis_relationship,
        impact_verdict=impact_verdict,
        unvalidated_positive_support=unvalidated_positive_support,
    )
    tone = _tone(
        state=state,
        action=action_key,
        status=status,
        queue_bucket=queue_bucket,
        impact_verdict=impact_verdict,
    )

    return {
        "filing_type": filing_type,
        "evidence_scope": evidence_scope,
        "thesis_relationship": thesis_relationship,
        "impact_verdict": impact_verdict,
        "impact_dimension": impact_dimension,
        "trajectory_state": state,
        "trajectory_label": _trajectory_label(
            state=state,
            filing_type=filing_type,
            thesis_relationship=thesis_relationship,
            impact_verdict=impact_verdict,
            unvalidated_positive_support=unvalidated_positive_support,
        ),
        "queue_bucket": queue_bucket,
        "queue_label": QUEUE_BUCKET_LABELS.get(queue_bucket, "All filings"),
        "review_status": review_status,
        "review_label": REVIEW_LABELS.get(review_status, "Status unknown"),
        "review_reason": review_reason,
        "is_user_action_required": review_status == "open",
        "system_action": action_key,
        "system_action_label": SYSTEM_ACTION_LABELS.get(action_key) or _titleize(action_key) or "No maintenance",
        "evidence_label": _evidence_label(
            state=state,
            thesis_relationship=thesis_relationship,
            relationship_kind=relationship_kind,
            matched_conditions_count=matched_conditions_count,
            triggered_watchlist_count=triggered_watchlist_count,
            triggered_verification_count=triggered_verification_count,
            checked_watchlist_count=checked_watchlist_count,
        ),
        "relationship_label": _relationship_label(
            kind=relationship_kind,
            strength=relationship_strength,
            priority=relationship_priority,
        ),
        "primary_reason": _primary_reason(
            state=state,
            action=action,
            direct_hit_count=direct_hit_count,
            unvalidated_positive_support=unvalidated_positive_support,
        ),
        "tone": "warn" if unvalidated_positive_support else tone,
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
        "filing_type": "market_context",
        "evidence_scope": "market_backdrop_only",
        "thesis_relationship": "unrelated",
        "impact_verdict": "neutral",
        "impact_dimension": "general",
        "trajectory_state": "market_backdrop_only",
        "trajectory_effect": "no_clear_change",
        "price_time_effect": "Market-only context; no direct announcement-led trajectory change identified.",
        "display_adjustment": "market_context_only_watch",
    }


def watchlist_engagement_projection(
    report: Dict[str, Any],
    action: Dict[str, Any],
    *,
    triggered_watchlist_count: int = 0,
) -> Dict[str, Any]:
    """Repair stale rows where watchlist engagement was saved as no-change."""

    report = report if isinstance(report, dict) else {}
    action = action if isinstance(action, dict) else {}
    if _norm(report.get("trajectory_state")) != "no_thesis_change":
        return {}
    if int(triggered_watchlist_count or 0) <= 0:
        return {}
    score = report.get("trajectory_score") if isinstance(report.get("trajectory_score"), dict) else {}
    validation_type = _norm(score.get("validation_type"))
    score_direction = _norm(score.get("direction"))
    event_delta = _to_float(score.get("event_delta"))
    if not validation_type.startswith("watchlist_"):
        return {}
    if score_direction not in {"positive", "negative"}:
        return {}
    if event_delta is None or abs(event_delta) <= 0.0001:
        return {}

    action_key = _norm(action.get("action"))
    impact = _norm(report.get("impact_level"))
    materiality = _norm(report.get("materiality"))
    if (
        action_key not in RERUN_ACTIONS
        and impact not in {"medium", "high", "critical"}
        and materiality not in {"medium", "high", "critical"}
    ):
        return {}

    is_red_flag = "red_flag" in validation_type
    is_confirmatory = "confirmatory" in validation_type
    if not is_red_flag and not is_confirmatory:
        return {}

    is_partial = "partial" in validation_type
    direction = "negative" if is_red_flag else "positive"
    trajectory_state = "risk_increased" if is_red_flag else "thesis_strengthened"
    thesis_effect = "undermines" if is_red_flag else "confirms"
    validation_type = (
        "watchlist_red_flag_partial"
        if is_red_flag and is_partial
        else "watchlist_red_flag_full"
        if is_red_flag
        else "watchlist_confirmatory_partial"
        if is_partial
        else "watchlist_confirmatory_full"
    )
    validation_weight = {
        "watchlist_red_flag_partial": 2.0,
        "watchlist_red_flag_full": 3.0,
        "watchlist_confirmatory_partial": 2.0,
        "watchlist_confirmatory_full": 2.5,
    }[validation_type]
    event_delta = -validation_weight if direction == "negative" else validation_weight
    baseline_path = _norm(report.get("baseline_path"))
    baseline_score = baseline_path_score(baseline_path)
    score_after_event = round(baseline_score + event_delta, 2)
    intensity = impact if impact in {"low", "medium", "high", "critical"} else "medium"
    scope = "red-flag watchlist hit" if is_red_flag else "confirmatory watchlist hit"
    if is_partial:
        scope = f"partial {scope}"
    direction_text = "Bear-leaning" if direction == "negative" else "Bull-leaning"
    price_time_effect = str(report.get("price_time_effect") or "").strip()
    reason = f"{direction_text} {intensity} evidence from a {scope}."
    if price_time_effect:
        reason = f"{direction_text} {intensity} evidence from a {scope}: {price_time_effect}"

    return {
        "trajectory_state": trajectory_state,
        "trajectory_effect": "weakens" if is_red_flag else "strengthens",
        "thesis_effect": thesis_effect,
        "run_validity": str(report.get("run_validity") or "watch").strip() or "watch",
        "filing_type": str(report.get("filing_type") or "company_event").strip() or "company_event",
        "evidence_scope": "saved_watchlist_evidence",
        "thesis_relationship": "watchlist_match",
        "impact_verdict": direction,
        "impact_dimension": str(report.get("impact_dimension") or "general").strip() or "general",
        "display_adjustment": "watchlist_engagement_projection",
        "trajectory_score": {
            "direction": direction,
            "intensity": intensity,
            "event_delta": round(event_delta, 2),
            "baseline_score": baseline_score,
            "score_after_event": score_after_event,
            "position_band": score_band(score_after_event),
            "position_label": position_label(baseline_path, score_after_event, validated_delta=event_delta),
            "confidence": report.get("thesis_match_confidence") or report.get("classification_confidence") or 0.0,
            "mapped_condition": True,
            "validation_type": validation_type,
            "validation_weight": validation_weight,
            "reason": reason,
        },
    }


def _queue_bucket(
    *,
    state: str,
    action: str,
    has_verification_hit: bool,
    filing_type: str = "",
    thesis_relationship: str = "",
    impact_verdict: str = "",
    unvalidated_positive_support: bool = False,
) -> str:
    if filing_type == "administrative" or state in ADMINISTRATIVE_STATES:
        return "administrative"
    if unvalidated_positive_support:
        return "open_review"
    if thesis_relationship == "related_unmapped":
        return "open_review"
    if state == "material_unmapped":
        return "open_review"
    if impact_verdict in {"negative", "mixed", "uncertain"}:
        return "open_review"
    if has_verification_hit or action == "urgent_human_review":
        return "open_review"
    if impact_verdict == "positive" or state in POSITIVE_TRAJECTORY_STATES:
        return "positive_movement"
    if state in NEGATIVE_TRAJECTORY_STATES:
        return "open_review"
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
    thesis_relationship: str = "",
    impact_verdict: str = "",
    unvalidated_positive_support: bool = False,
) -> str:
    if unvalidated_positive_support:
        return "thesis_map_gap"
    if state == "needs_classification":
        return "classification_unresolved"
    if thesis_relationship == "related_unmapped" or state == "material_unmapped":
        return "thesis_map_gap"
    if impact_verdict in {"mixed", "uncertain"}:
        return "classification_unresolved"
    if state in NEGATIVE_TRAJECTORY_STATES:
        return "negative_trajectory"
    if has_verification_hit:
        return "verification_hit"
    if state in POSITIVE_TRAJECTORY_STATES:
        return "positive_material_update"
    if direct_hit_count > 0 and queue_bucket == "open_review":
        return "mapped_evidence_hit"
    return "none"


def _trajectory_label(
    *,
    state: str,
    filing_type: str = "",
    thesis_relationship: str = "",
    impact_verdict: str = "",
    unvalidated_positive_support: bool = False,
) -> str:
    if filing_type == "administrative":
        return "Administrative"
    if unvalidated_positive_support:
        return "Related thesis evidence"
    if state in {
        "thesis_strengthened",
        "thesis_weakened",
        "timeline_accelerated",
        "timeline_delayed",
        "risk_reduced",
        "risk_increased",
    }:
        return TRAJECTORY_LABELS.get(state) or _titleize(state)
    if impact_verdict == "positive":
        return "Thesis improved"
    if impact_verdict == "negative":
        return "Thesis weakened"
    if thesis_relationship == "related_unmapped":
        return "Needs assessment"
    if impact_verdict in {"mixed", "uncertain", "unclear"}:
        return "Needs assessment"
    if impact_verdict == "neutral":
        return "No thesis impact"
    if state == "material_unmapped":
        return "Needs assessment"
    if state in {"material_unmapped", "market_backdrop_only", "no_thesis_change"}:
        return "No thesis impact"
    if state == "needs_classification":
        return "Needs assessment"
    return TRAJECTORY_LABELS.get(state) or _titleize(state) or "Not assessed"


def _tone(
    *,
    state: str,
    action: str,
    status: str,
    queue_bucket: str,
    impact_verdict: str = "",
) -> str:
    if status == "error" or action in {"urgent_human_review", "full_rerun"}:
        return "urgent"
    if impact_verdict == "negative":
        return "urgent"
    if impact_verdict == "positive":
        return "positive"
    if impact_verdict in {"mixed", "uncertain", "unclear"}:
        return "warn"
    if impact_verdict == "neutral":
        return "neutral"
    if state in NEGATIVE_TRAJECTORY_STATES:
        return "urgent"
    if state in {"needs_classification", "material_unmapped"} or queue_bucket == "open_review":
        return "warn"
    if state in POSITIVE_TRAJECTORY_STATES:
        return "positive"
    if action in {"rerun_stage1", "run_delta_only"}:
        return "alarm"
    return "neutral"


def _primary_reason(
    *,
    state: str,
    action: Dict[str, Any],
    direct_hit_count: int,
    unvalidated_positive_support: bool = False,
) -> str:
    if unvalidated_positive_support:
        return "Related to the saved thesis, but no saved condition was achieved."
    if direct_hit_count > 0:
        return f"Engaged {direct_hit_count} saved thesis, watchlist, or verification item{'' if direct_hit_count == 1 else 's'}."
    if state in PRIMARY_REASONS:
        return PRIMARY_REASONS[state]
    return str(action.get("reason") or "").strip()


def _evidence_label(
    *,
    state: str,
    thesis_relationship: str = "",
    relationship_kind: str = "",
    matched_conditions_count: int,
    triggered_watchlist_count: int,
    triggered_verification_count: int,
    checked_watchlist_count: int = 0,
) -> str:
    if int(matched_conditions_count or 0) > 0:
        return "Thesis condition matched"
    if int(triggered_watchlist_count or 0) > 0:
        return "Watchlist condition matched"
    if int(triggered_verification_count or 0) > 0:
        return "Verification item matched"
    if state in {"risk_increased", "thesis_weakened", "timeline_delayed"} and (
        thesis_relationship == "related_unmapped" or relationship_kind == "material_unmapped"
    ):
        return "Risk event outside thesis map"
    if int(checked_watchlist_count or 0) > 0:
        return "Watchlist checked, not triggered"
    if thesis_relationship == "related_unmapped" or state == "material_unmapped":
        return "No saved condition match"
    if state == "needs_classification":
        return "Classification unresolved"
    if state == "market_backdrop_only":
        return "Market backdrop only"
    if state == "administrative_filing":
        return "Administrative only"
    return "No condition match"


def _is_unvalidated_positive_support(*, report: Dict[str, Any], impact_verdict: str, direct_hit_count: int) -> bool:
    if impact_verdict != "positive" or int(direct_hit_count or 0) > 0:
        return False
    score = report.get("trajectory_score") if isinstance(report.get("trajectory_score"), dict) else {}
    validation_type = _norm(score.get("validation_type"))
    event_delta = _to_float(score.get("event_delta"))
    mapped = bool(score.get("mapped_condition"))
    if validation_type not in {"", "none", "related_unmapped", "material_unmapped"}:
        return False
    if mapped:
        return False
    return event_delta is None or abs(event_delta) <= 0.0001


def _relationship_label(*, kind: str, strength: str, priority: Any) -> str:
    label = RELATIONSHIP_LABELS.get(kind) or _titleize(kind) or "Not assessed"
    strength_label = "Partial " if strength == "partial" else "Full " if strength == "full" else ""
    priority_num = _to_int(priority)
    if priority_num and priority_num >= 4 and strength_label:
        return f"{strength_label}{label}".strip()
    return label


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _titleize(value: str) -> str:
    text = str(value or "").replace("_", " ").strip()
    return " ".join(part[:1].upper() + part[1:] for part in text.split()) if text else ""


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0
