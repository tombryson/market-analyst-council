from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


BASELINE_PATH_SCORES = {
    "bear": -4.0,
    "base": 0.0,
    "bull": 4.0,
    "mixed": 0.0,
    "unknown": 0.0,
}

INTENSITY_DELTAS = {
    "none": 0.0,
    "low": 0.5,
    "medium": 2.0,
    "high": 3.0,
    "critical": 3.0,
}


def build_trajectory_score(
    *,
    baseline_path: str,
    current_path: str,
    trajectory_state: str,
    trajectory_effect: str,
    thesis_effect: str,
    timeline_effect: str,
    impact_level: str,
    materiality: str,
    classification_confidence: float,
    thesis_match_confidence: float,
    direct_match_count: int,
    red_flag_hits: int,
    confirmatory_hits: int,
    verification_hits: int,
    positive: bool,
    negative: bool,
    price_time_effect: str = "",
    thesis_required_hits: int = 0,
    thesis_failure_hits: int = 0,
    red_flag_partial_hits: int = 0,
    confirmatory_partial_hits: int = 0,
) -> Dict[str, Any]:
    direction = _direction(
        trajectory_state=trajectory_state,
        trajectory_effect=trajectory_effect,
        thesis_effect=thesis_effect,
        timeline_effect=timeline_effect,
        red_flag_hits=red_flag_hits,
        confirmatory_hits=confirmatory_hits,
        verification_hits=verification_hits,
        positive=positive,
        negative=negative,
    )
    intensity = _intensity(
        trajectory_state=trajectory_state,
        impact_level=impact_level,
        materiality=materiality,
        direct_match_count=direct_match_count,
        red_flag_hits=red_flag_hits,
        confirmatory_hits=confirmatory_hits,
    )
    validation_type, validation_weight = _validation_weight(
        direction=direction,
        trajectory_state=trajectory_state,
        thesis_required_hits=thesis_required_hits,
        thesis_failure_hits=thesis_failure_hits,
        red_flag_hits=red_flag_hits,
        confirmatory_hits=confirmatory_hits,
        red_flag_partial_hits=red_flag_partial_hits,
        confirmatory_partial_hits=confirmatory_partial_hits,
        verification_hits=verification_hits,
        direct_match_count=direct_match_count,
    )
    magnitude = max(INTENSITY_DELTAS.get(intensity, 0.0), validation_weight)
    if direction == "negative":
        event_delta = -magnitude
    elif direction == "positive":
        event_delta = magnitude
    else:
        event_delta = 0.0

    baseline_score = baseline_path_score(baseline_path)
    score_after_event = round(baseline_score + event_delta, 2)
    confidence = _confidence(
        classification_confidence=classification_confidence,
        thesis_match_confidence=thesis_match_confidence,
        direct_match_count=direct_match_count,
    )
    mapped = int(direct_match_count or 0) > 0
    return {
        "direction": direction,
        "intensity": intensity,
        "event_delta": round(event_delta, 2),
        "baseline_score": baseline_score,
        "score_after_event": score_after_event,
        "position_band": score_band(score_after_event),
        "position_label": position_label(baseline_path, score_after_event),
        "confidence": confidence,
        "mapped_condition": mapped,
        "validation_type": validation_type,
        "validation_weight": validation_weight,
        "reason": _reason(
            direction=direction,
            intensity=intensity,
            trajectory_state=trajectory_state,
            mapped=mapped,
            validation_type=validation_type,
            price_time_effect=price_time_effect,
        ),
    }


def apply_cumulative_scores(rows: List[Dict[str, Any]]) -> None:
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        score = row.get("trajectory_score")
        if not isinstance(score, dict):
            continue
        key = (
            str(row.get("ticker") or "").strip().upper(),
            str(row.get("run_id") or "").strip(),
        )
        groups.setdefault(key, []).append(row)

    for group_rows in groups.values():
        cumulative = 0.0
        for row in sorted(group_rows, key=_row_time_key):
            score = row.get("trajectory_score") if isinstance(row.get("trajectory_score"), dict) else {}
            event_delta = _to_float(score.get("event_delta"))
            if event_delta is None:
                continue
            cumulative = round(cumulative + event_delta, 2)
            baseline_path = str(row.get("baseline_path") or "").strip().lower()
            baseline_score = _to_float(score.get("baseline_score"))
            if baseline_score is None:
                baseline_score = baseline_path_score(baseline_path)
            score_after_cumulative = round(baseline_score + cumulative, 2)
            score.update(
                {
                    "cumulative_delta": cumulative,
                    "score_after_cumulative": score_after_cumulative,
                    "cumulative_position_band": score_band(score_after_cumulative),
                    "cumulative_position_label": position_label(baseline_path, score_after_cumulative),
                }
            )


def baseline_path_score(path: str) -> float:
    return BASELINE_PATH_SCORES.get(str(path or "").strip().lower(), 0.0)


def score_band(score: Any) -> str:
    value = _to_float(score)
    if value is None:
        return "unknown"
    if value <= -4:
        return "bear"
    if value <= -2:
        return "bear_leaning"
    if value < 2:
        return "base"
    if value < 4:
        return "bull_leaning"
    return "bull"


def position_label(baseline_path: str, score: Any) -> str:
    band = score_band(score)
    labels = {
        "bear": "Bear case",
        "bear_leaning": "Bear-leaning",
        "base": "Base case",
        "bull_leaning": "Bull-leaning",
        "bull": "Bull case",
        "unknown": "Not assessed",
    }
    baseline = str(baseline_path or "").strip().lower()
    label = labels.get(band, "Not assessed")
    if baseline == "base" and band in {"bear_leaning", "bull_leaning"}:
        return f"Base, {label.lower()}"
    if baseline == "bull" and band == "bull_leaning":
        return "Bull case, weakening"
    if baseline == "bear" and band == "bear_leaning":
        return "Bear case, improving"
    return label


def _direction(
    *,
    trajectory_state: str,
    trajectory_effect: str,
    thesis_effect: str,
    timeline_effect: str,
    red_flag_hits: int,
    confirmatory_hits: int,
    verification_hits: int,
    positive: bool,
    negative: bool,
) -> str:
    state = str(trajectory_state or "").strip().lower()
    effect = str(trajectory_effect or "").strip().lower()
    thesis = str(thesis_effect or "").strip().lower()
    timeline = str(timeline_effect or "").strip().lower()
    if state in {"thesis_weakened", "timeline_delayed", "risk_increased"}:
        return "negative"
    if int(red_flag_hits or 0) > 0 or thesis in {"undermines", "invalidates"} or effect in {"weakens", "delays"} or timeline == "delayed":
        return "negative"
    if state in {"thesis_strengthened", "timeline_accelerated", "risk_reduced"}:
        return "positive"
    if int(confirmatory_hits or 0) > 0 or int(verification_hits or 0) > 0:
        return "positive"
    if thesis in {"confirms", "accelerates"} or effect in {"strengthens", "risk_reduced", "accelerates"} or timeline == "accelerated":
        return "positive"
    if positive and not negative:
        return "positive"
    if negative and not positive:
        return "negative"
    if positive and negative:
        return "mixed"
    return "neutral"


def _intensity(
    *,
    trajectory_state: str,
    impact_level: str,
    materiality: str,
    direct_match_count: int,
    red_flag_hits: int,
    confirmatory_hits: int,
) -> str:
    state = str(trajectory_state or "").strip().lower()
    if state in {"no_thesis_change", "administrative_filing", "market_backdrop_only"}:
        return "none"
    impact = str(impact_level or "").strip().lower()
    material = str(materiality or "").strip().lower()
    if impact in {"critical", "high", "medium", "low", "none"}:
        intensity = impact
    elif material in {"critical", "high", "medium", "low", "none"}:
        intensity = material
    else:
        intensity = "low" if int(direct_match_count or 0) > 0 else "none"
    if int(red_flag_hits or 0) > 0 and intensity in {"none", "low", "medium"}:
        return "high"
    if int(confirmatory_hits or 0) > 0 and intensity in {"none", "low"}:
        return "medium"
    return intensity


def _confidence(
    *,
    classification_confidence: float,
    thesis_match_confidence: float,
    direct_match_count: int,
) -> float:
    values = [_to_float(classification_confidence)]
    if int(direct_match_count or 0) > 0:
        values.append(_to_float(thesis_match_confidence))
    clean = [value for value in values if value is not None]
    if not clean:
        return 0.0
    return round(max(0.0, min(1.0, max(clean))), 2)


def _validation_weight(
    *,
    direction: str,
    trajectory_state: str,
    thesis_required_hits: int,
    thesis_failure_hits: int,
    red_flag_hits: int,
    confirmatory_hits: int,
    red_flag_partial_hits: int,
    confirmatory_partial_hits: int,
    verification_hits: int,
    direct_match_count: int,
) -> Tuple[str, float]:
    state = str(trajectory_state or "").strip().lower()
    if direction == "positive":
        if int(thesis_required_hits or 0) > 0:
            return "saved_thesis_condition", 3.0
        if int(confirmatory_hits or 0) > 0:
            return "watchlist_confirmatory_full", 2.5
        if int(confirmatory_partial_hits or 0) > 0:
            return "watchlist_confirmatory_partial", 2.0
        if int(verification_hits or 0) > 0:
            return "verification_queue", 1.5
    if direction == "negative":
        if int(thesis_failure_hits or 0) > 0:
            return "saved_thesis_failure", 4.0
        if int(red_flag_hits or 0) > 0:
            return "watchlist_red_flag_full", 3.0
        if int(red_flag_partial_hits or 0) > 0:
            return "watchlist_red_flag_partial", 2.0
    if state == "material_unmapped" and int(direct_match_count or 0) <= 0:
        return "material_unmapped", 0.0
    if int(direct_match_count or 0) > 0:
        return "mapped_condition", 1.0
    return "none", 0.0


def _reason(
    *,
    direction: str,
    intensity: str,
    trajectory_state: str,
    mapped: bool,
    validation_type: str,
    price_time_effect: str,
) -> str:
    state = str(trajectory_state or "").strip().lower()
    if direction == "neutral":
        return "No price/time thesis movement was scored."
    if direction == "mixed":
        return "The filing has mixed directional evidence, so no clean scenario move was scored."
    direction_text = "Bull-leaning" if direction == "positive" else "Bear-leaning"
    scope = _validation_scope(validation_type, mapped)
    if state == "material_unmapped" and not mapped:
        scope = "material filing outside the saved thesis map"
    effect = str(price_time_effect or "").strip()
    if effect:
        return f"{direction_text} {intensity} evidence from a {scope}: {effect}"
    return f"{direction_text} {intensity} evidence from a {scope}."


def _validation_scope(validation_type: str, mapped: bool) -> str:
    labels = {
        "saved_thesis_condition": "saved thesis condition",
        "saved_thesis_failure": "saved thesis failure condition",
        "watchlist_confirmatory_full": "full confirmatory watchlist hit",
        "watchlist_confirmatory_partial": "partial confirmatory watchlist hit",
        "watchlist_red_flag_full": "red-flag watchlist hit",
        "watchlist_red_flag_partial": "partial red-flag watchlist hit",
        "verification_queue": "verification queue hit",
        "material_unmapped": "material filing outside the saved thesis map",
        "mapped_condition": "mapped condition",
    }
    return labels.get(str(validation_type or "").strip().lower(), "mapped condition" if mapped else "outside saved conditions")


def _row_time_key(row: Dict[str, Any]) -> str:
    return str(row.get("saved_at_utc") or row.get("received_at_utc") or "").strip()


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None
