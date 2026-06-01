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
