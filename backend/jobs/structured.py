"""Structured-analysis artifact helpers.

Parses Stage-3 JSON output shapes, extracts market facts, normalises
timeline / catalyst rows, builds per-run summary fields, and produces the
integration packet consumed by the frontend report view.
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..council import (
    calculate_aggregate_rankings,
    compact_stage2_rankings_for_telemetry,
)
from ..timeline_normalization import normalize_timeline_rows as _standardize_timeline_rows

logger = logging.getLogger(__name__)

def _extract_stage3_structured_from_artifact(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract Stage 3 structured JSON from known artifact shapes."""
    if not isinstance(payload, dict):
        return None

    # json_runner_regen artifacts
    structured = payload.get("structured_data")
    if isinstance(structured, dict) and structured:
        return structured

    # Full quality run artifacts
    primary = payload.get("stage3_result_primary")
    if isinstance(primary, dict):
        structured = primary.get("structured_data")
        if isinstance(structured, dict) and structured:
            return structured

    fallback = payload.get("stage3_result")
    if isinstance(fallback, dict):
        structured = fallback.get("structured_data")
        if isinstance(structured, dict) and structured:
            return structured

    return None


def _extract_stage3_result_from_artifact(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract full Stage 3 result object from known artifact shapes."""
    if not isinstance(payload, dict):
        return None

    primary = payload.get("stage3_result_primary")
    if isinstance(primary, dict) and isinstance(primary.get("structured_data"), dict):
        return primary

    fallback = payload.get("stage3_result")
    if isinstance(fallback, dict) and isinstance(fallback.get("structured_data"), dict):
        return fallback

    structured = payload.get("structured_data")
    if isinstance(structured, dict) and structured:
        return {
            "structured_data": structured,
            "chairman_document": payload.get("chairman_document") or {},
            "analyst_document": payload.get("analyst_document") or {},
        }

    return None


def _extract_current_price_candidates_from_text(text: str) -> List[float]:
    """
    Extract plausible current-share-price mentions from markdown/text blobs.
    Targets lines containing "current price"/"share price" and AUD values.
    """
    if not text:
        return []
    candidates: List[float] = []
    trigger = re.compile(r"\b(current\s+(?:share\s+)?price|share\s+price)\b", re.IGNORECASE)
    aud_price = re.compile(r"A\$\s*([0-9]+(?:\.[0-9]+)?)")
    for raw_line in str(text).splitlines():
        line = raw_line.strip()
        if not line or not trigger.search(line):
            continue
        for match in aud_price.finditer(line):
            try:
                price = float(match.group(1))
            except Exception:
                continue
            if 0 < price < 1000:
                candidates.append(price)
    return candidates


def _extract_normalized_facts_from_prefixed_query(query_text: str) -> Dict[str, Any]:
    """
    Parse a leading normalized_facts JSON block from query text.
    Expected prefix shape:
      { "normalized_facts": { ... } }
      <template prompt text...>
    """
    raw = str(query_text or "")
    if not raw.strip():
        return {}
    match = re.search(r"\{\s*\"normalized_facts\"\s*:", raw)
    if not match:
        return {}
    try:
        parsed, _ = json.JSONDecoder().raw_decode(raw[match.start():].lstrip())
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    facts = parsed.get("normalized_facts")
    if not isinstance(facts, dict):
        return {}
    return dict(facts)


def _extract_market_facts_from_artifact_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recover market_facts robustly from run/checkpoint artifacts.
    Fallback order:
    1) input_audit.market_facts
    2) metadata.market_facts
    3) normalized_facts parsed from effective_query
    4) normalized_facts parsed from per_model_research_runs[*].result.query
    """
    if not isinstance(payload, dict):
        return {}

    def _valid_market_facts(candidate: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(candidate, dict):
            return None
        normalized = candidate.get("normalized_facts")
        if isinstance(normalized, dict) and any(v is not None for v in normalized.values()):
            return dict(candidate)
        return None

    input_audit = payload.get("input_audit")
    if isinstance(input_audit, dict):
        found = _valid_market_facts(input_audit.get("market_facts"))
        if found:
            return found

    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        found = _valid_market_facts(metadata.get("market_facts"))
        if found:
            return found

    # Parse normalized_facts directly from prefixed query text.
    effective_query = str(payload.get("effective_query") or "")
    parsed_from_query = _extract_normalized_facts_from_prefixed_query(effective_query)
    if parsed_from_query:
        return {
            "status": "reconstructed",
            "reason": "reconstructed_from_effective_query",
            "normalized_facts": parsed_from_query,
        }

    # Parse from per-model research query payloads, keeping the richest block.
    best_facts: Dict[str, Any] = {}
    best_score = -1
    per_model_runs = (metadata or {}).get("per_model_research_runs") if isinstance(metadata, dict) else None
    if isinstance(per_model_runs, list):
        for row in per_model_runs:
            if not isinstance(row, dict):
                continue
            result = row.get("result")
            if not isinstance(result, dict):
                continue
            candidate_query = str(result.get("query") or "")
            parsed = _extract_normalized_facts_from_prefixed_query(candidate_query)
            if not parsed:
                continue
            score = len([k for k, v in parsed.items() if v is not None])
            if score > best_score:
                best_score = score
                best_facts = parsed

    if best_facts:
        return {
            "status": "reconstructed",
            "reason": "reconstructed_from_per_model_query",
            "normalized_facts": best_facts,
        }

    return {}


def _infer_current_price_from_artifact(payload: Dict[str, Any]) -> Optional[float]:
    """
    Infer current share price from Stage 1 responses when Stage 3 omitted it.
    Uses median across candidate model outputs to reduce outlier impact.
    """
    if not isinstance(payload, dict):
        return None

    def _as_price(value: Any) -> Optional[float]:
        try:
            n = float(value)
        except Exception:
            return None
        if not (0 < n < 1000):
            return None
        return round(n, 6)

    # 1) Strongest source: deterministic prepass/reconstructed normalized facts.
    market_facts = _extract_market_facts_from_artifact_payload(payload)
    normalized = market_facts.get("normalized_facts") if isinstance(market_facts, dict) else None
    if isinstance(normalized, dict):
        price = _as_price(normalized.get("current_price"))
        if price is not None:
            return price

    # 2) Stage 3 provenance, if present.
    stage3 = payload.get("stage3_result") if isinstance(payload.get("stage3_result"), dict) else {}
    if stage3:
        structured = stage3.get("structured_data")
        if isinstance(structured, dict):
            provenance = structured.get("market_data_provenance")
            if isinstance(provenance, dict):
                price = _as_price(provenance.get("prepass_current_price"))
                if price is not None:
                    return price

    stage1_rows = payload.get("stage1_results")
    if not isinstance(stage1_rows, list):
        return None

    values: List[float] = []
    for row in stage1_rows:
        if not isinstance(row, dict):
            continue
        values.extend(_extract_current_price_candidates_from_text(str(row.get("response") or "")))

    if not values:
        return None

    values = sorted(values)
    n = len(values)
    if n % 2 == 1:
        median_val = values[n // 2]
    else:
        median_val = (values[(n // 2) - 1] + values[n // 2]) / 2.0
    return round(float(median_val), 6)


def _backfill_stage2_ranking_telemetry(
    structured: Dict[str, Any],
    payload: Dict[str, Any],
) -> None:
    """Expose compact full Stage 2 rankings for existing and new Gantt artifacts."""
    if not isinstance(structured, dict) or not isinstance(payload, dict):
        return
    stage2_results = payload.get("stage2_results")
    label_to_model = payload.get("label_to_model")
    if not isinstance(stage2_results, list) or not isinstance(label_to_model, dict):
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        if not isinstance(stage2_results, list):
            stage2_results = metadata.get("stage2_results")
        if not isinstance(label_to_model, dict):
            label_to_model = metadata.get("label_to_model")
    if not isinstance(stage2_results, list):
        return
    if not isinstance(label_to_model, dict):
        stage1_results = payload.get("stage1_results_for_stage3")
        if not isinstance(stage1_results, list):
            stage1_results = payload.get("stage1_results")
        if isinstance(stage1_results, list):
            label_to_model = {
                f"Response {chr(65 + idx)}": str(row.get("model") or "").strip()
                for idx, row in enumerate(stage1_results)
                if isinstance(row, dict) and str(row.get("model") or "").strip()
            }
        else:
            label_to_model = {}

    council_meta = structured.get("council_metadata")
    if not isinstance(council_meta, dict):
        council_meta = {}
        structured["council_metadata"] = council_meta

    aggregate = payload.get("stage2_aggregate_rankings")
    if not isinstance(aggregate, list):
        aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)
    if aggregate and not isinstance(council_meta.get("stage2_aggregate_rankings"), list):
        council_meta["stage2_aggregate_rankings"] = aggregate

    if not isinstance(council_meta.get("stage2_judge_rankings"), list):
        council_meta["stage2_judge_rankings"] = compact_stage2_rankings_for_telemetry(
            stage2_results,
            label_to_model,
        )


def _normalize_timeline_rows_for_api(raw_timeline: Any) -> List[Dict[str, Any]]:
    """
    Normalize timeline rows for UI consumers.
    Accepts either structured objects or plain strings like:
    "Q1-Q2 2026: Initial Drawdown on US$25M Facility"
    """
    out = _standardize_timeline_rows(raw_timeline)
    return _cap_previous_timeline_rows(out, max_previous=1)


def _normalize_catalyst_rows_for_api(raw_catalysts: Any) -> List[Dict[str, Any]]:
    rows = _cap_previous_catalyst_rows(raw_catalysts if isinstance(raw_catalysts, list) else [], max_previous=1)
    out: List[Dict[str, Any]] = []
    for item in rows:
        if isinstance(item, dict):
            title = str(
                item.get("title")
                or item.get("name")
                or item.get("milestone")
                or item.get("catalyst")
                or item.get("event")
                or ""
            ).strip()
            target_period = str(
                item.get("target_period")
                or item.get("targetPeriod")
                or item.get("period")
                or item.get("when")
                or item.get("date")
                or ""
            ).strip()
            status = str(item.get("status") or item.get("current_status") or item.get("state") or "").strip()
        elif isinstance(item, str):
            title = item.strip()
            target_period = _extract_period_from_text(title)
            status = ""
        else:
            continue

        if not title:
            continue
        out.append(
            {
                "title": title,
                "target_period": target_period,
                "status": status,
            }
        )
    return out[:8]


def _timeline_period_to_quarter_index(period: Any) -> Optional[int]:
    text = str(period or "").strip().upper()
    if not text:
        return None

    q_range = re.search(r"\bQ([1-4])\s*[-/]\s*Q([1-4])\s*(20\d{2})\b", text)
    if q_range:
        q1 = int(q_range.group(1))
        q2 = int(q_range.group(2))
        year = int(q_range.group(3))
        return (year * 4) + max(q1, q2)

    q_single = re.search(r"\bQ([1-4])\s*(20\d{2})\b", text)
    if q_single:
        quarter = int(q_single.group(1))
        year = int(q_single.group(2))
        return (year * 4) + quarter

    half = re.search(r"\bH([12])\s*(20\d{2})\b", text)
    if half:
        h = int(half.group(1))
        year = int(half.group(2))
        quarter = 2 if h == 1 else 4
        return (year * 4) + quarter

    year_only = re.search(r"\b(20\d{2})\b", text)
    if year_only:
        year = int(year_only.group(1))
        return (year * 4) + 4

    return None


def _current_quarter_index(now_utc: Optional[datetime] = None) -> int:
    now = now_utc or datetime.now(timezone.utc)
    quarter = ((now.month - 1) // 3) + 1
    return (now.year * 4) + quarter


def _status_indicates_past(status: Any) -> bool:
    low = str(status or "").strip().lower()
    if not low:
        return False
    return any(
        token in low
        for token in (
            "achieved",
            "completed",
            "done",
            "delivered",
            "closed",
            "finished",
            "met",
            "launched",
            "commissioned",
            "first gold",
        )
    )


def _status_indicates_future(status: Any) -> bool:
    low = str(status or "").strip().lower()
    if not low:
        return False
    return any(
        token in low
        for token in (
            "planned",
            "at_risk",
            "at risk",
            "pending",
            "upcoming",
            "target",
            "on track",
            "on_track",
            "current",
            "in progress",
            "in_progress",
            "speculative",
            "proposed",
        )
    )


def _timeline_row_is_previous(row: Dict[str, Any], now_utc: Optional[datetime] = None) -> bool:
    status = row.get("status")
    if _status_indicates_past(status):
        return True
    if _status_indicates_future(status):
        return False

    quarter_idx = _timeline_period_to_quarter_index(
        row.get("target_period")
        or row.get("targetPeriod")
        or row.get("period")
        or row.get("date")
    )
    if quarter_idx is None:
        return False
    return quarter_idx < _current_quarter_index(now_utc)


def _cap_previous_timeline_rows(
    rows: List[Dict[str, Any]],
    *,
    max_previous: int = 1,
    now_utc: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(rows, list) or max_previous < 0:
        return []
    if not rows:
        return []

    indexed: List[Tuple[int, Dict[str, Any], bool, Optional[int]]] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        is_previous = _timeline_row_is_previous(row, now_utc=now_utc)
        quarter_idx = _timeline_period_to_quarter_index(
            row.get("target_period")
            or row.get("targetPeriod")
            or row.get("period")
            or row.get("date")
        )
        indexed.append((idx, row, is_previous, quarter_idx))

    previous_rows = [item for item in indexed if item[2]]
    if len(previous_rows) <= max_previous:
        return [item[1] for item in indexed]

    previous_rows_sorted = sorted(
        previous_rows,
        key=lambda item: (
            item[3] is not None,
            item[3] if item[3] is not None else -1,
            item[0],
        ),
        reverse=True,
    )
    keep_previous_idx = {item[0] for item in previous_rows_sorted[:max_previous]}
    filtered: List[Dict[str, Any]] = []
    for idx, row, is_previous, _ in indexed:
        if not is_previous or idx in keep_previous_idx:
            filtered.append(row)
    return filtered


def _extract_period_from_text(text: str) -> str:
    if not text:
        return ""
    match = re.search(
        r"\b(Q[1-4](?:\s*[-/]\s*Q[1-4])?\s*20\d{2}|H[12]\s*20\d{2}|20\d{2})\b",
        str(text),
        re.IGNORECASE,
    )
    return str(match.group(1) or "").strip() if match else ""


def _cap_previous_catalyst_rows(
    rows: List[Any],
    *,
    max_previous: int = 1,
    now_utc: Optional[datetime] = None,
) -> List[Any]:
    if not isinstance(rows, list) or max_previous < 0:
        return []
    if not rows:
        return []

    indexed: List[Tuple[int, Any, bool, Optional[int]]] = []
    for idx, row in enumerate(rows):
        status = ""
        period = ""
        if isinstance(row, dict):
            status = str(
                row.get("status")
                or row.get("state")
                or row.get("current_status")
                or ""
            ).strip()
            period = str(
                row.get("target_period")
                or row.get("targetPeriod")
                or row.get("period")
                or row.get("when")
                or row.get("date")
                or ""
            ).strip()
            if not period:
                period = _extract_period_from_text(
                    str(row.get("name") or row.get("title") or row.get("milestone") or row.get("catalyst") or "")
                )
        elif isinstance(row, str):
            status = row
            period = _extract_period_from_text(row)
        else:
            continue

        tmp = {
            "status": status,
            "target_period": period,
        }
        is_previous = _timeline_row_is_previous(tmp, now_utc=now_utc)
        quarter_idx = _timeline_period_to_quarter_index(period)
        indexed.append((idx, row, is_previous, quarter_idx))

    previous_rows = [item for item in indexed if item[2]]
    if len(previous_rows) <= max_previous:
        return [item[1] for item in indexed]

    previous_rows_sorted = sorted(
        previous_rows,
        key=lambda item: (
            item[3] is not None,
            item[3] if item[3] is not None else -1,
            item[0],
        ),
        reverse=True,
    )
    keep_previous_idx = {item[0] for item in previous_rows_sorted[:max_previous]}
    filtered: List[Any] = []
    for idx, row, is_previous, _ in indexed:
        if not is_previous or idx in keep_previous_idx:
            filtered.append(row)
    return filtered


def _to_float(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except Exception:
        return None
    if not (num == num):  # NaN
        return None
    return num


def _safe_float(value: Any) -> Optional[float]:
    num = _to_float(value)
    if num is None:
        return None
    if abs(num) > 1e18:
        return None
    return num


def _pick_score_total(score_obj: Any) -> Optional[float]:
    if isinstance(score_obj, dict):
        return _safe_float(score_obj.get("total"))
    return _safe_float(score_obj)


def _pick_nested(mapping: Dict[str, Any], *keys: str) -> Optional[float]:
    cur: Any = mapping
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return _safe_float(cur)


def _compute_prob_weighted_target(
    scenario_targets: Dict[str, Any],
    scenario_probabilities: Dict[str, Any],
) -> Optional[float]:
    weighted = 0.0
    prob_sum = 0.0
    for key in ("bear", "base", "bull"):
        target = _safe_float(scenario_targets.get(key))
        prob = _safe_float(scenario_probabilities.get(key))
        if target is None or prob is None:
            continue
        if prob <= 1.0:
            prob = prob * 100.0
        if prob <= 0.0:
            continue
        weighted += target * prob
        prob_sum += prob
    if prob_sum <= 0.0:
        return _safe_float(scenario_targets.get("base"))
    return weighted / prob_sum


def _build_summary_fields(structured: Dict[str, Any], freshness: Dict[str, Any]) -> Dict[str, Any]:
    if str(structured.get("analysis_kind") or "").strip() == "portfolio_positioning":
        diagnosis = structured.get("portfolio_diagnosis") if isinstance(structured.get("portfolio_diagnosis"), dict) else {}
        strategic_view = structured.get("strategic_view") if isinstance(structured.get("strategic_view"), dict) else {}
        current_vs_ideal = structured.get("current_vs_ideal") if isinstance(structured.get("current_vs_ideal"), dict) else {}
        return {
            "analysis_kind": "portfolio_positioning",
            "analysis_date": str(structured.get("analysis_date") or "").strip(),
            "current_cash_pct": _safe_float(diagnosis.get("current_cash_pct")),
            "cash_target_pct": _safe_float(strategic_view.get("cash_target_pct")),
            "primary_theme": str(strategic_view.get("primary_theme") or "").strip(),
            "secondary_theme": str(strategic_view.get("secondary_theme") or "").strip(),
            "main_overweights": current_vs_ideal.get("main_overweights") if isinstance(current_vs_ideal.get("main_overweights"), list) else [],
            "main_underweights": current_vs_ideal.get("main_underweights") if isinstance(current_vs_ideal.get("main_underweights"), list) else [],
        }

    market_data = structured.get("market_data") if isinstance(structured.get("market_data"), dict) else {}
    council_meta = structured.get("council_metadata") if isinstance(structured.get("council_metadata"), dict) else {}
    council_contract = council_meta.get("template_contract") if isinstance(council_meta.get("template_contract"), dict) else {}
    top_level_contract = structured.get("template_contract") if isinstance(structured.get("template_contract"), dict) else {}
    price_targets = structured.get("price_targets") if isinstance(structured.get("price_targets"), dict) else {}
    scenario_targets = price_targets.get("scenario_targets") if isinstance(price_targets.get("scenario_targets"), dict) else {}
    scenario_probabilities = (
        price_targets.get("scenario_probabilities")
        if isinstance(price_targets.get("scenario_probabilities"), dict)
        else {}
    )
    scenario_12m = scenario_targets.get("12m") if isinstance(scenario_targets.get("12m"), dict) else {}
    scenario_24m = scenario_targets.get("24m") if isinstance(scenario_targets.get("24m"), dict) else {}
    probs_12m = scenario_probabilities.get("12m") if isinstance(scenario_probabilities.get("12m"), dict) else {}
    probs_24m = scenario_probabilities.get("24m") if isinstance(scenario_probabilities.get("24m"), dict) else {}

    quality_total = _pick_score_total(structured.get("quality_score"))
    value_total = _pick_score_total(structured.get("value_score"))
    rating = str(
        (
            (structured.get("investment_recommendation") or {}).get("rating")
            if isinstance(structured.get("investment_recommendation"), dict)
            else ""
        )
        or (
            (structured.get("investment_verdict") or {}).get("rating")
            if isinstance(structured.get("investment_verdict"), dict)
            else ""
        )
        or ""
    ).strip()
    conviction = str(
        (
            (structured.get("investment_recommendation") or {}).get("conviction")
            if isinstance(structured.get("investment_recommendation"), dict)
            else ""
        )
        or (
            (structured.get("investment_verdict") or {}).get("conviction")
            if isinstance(structured.get("investment_verdict"), dict)
            else ""
        )
        or ""
    ).strip()

    current_price = _safe_float(market_data.get("current_price"))
    if current_price is None:
        current_price = _safe_float(price_targets.get("current_price"))

    target_12m_base = _safe_float(price_targets.get("target_12m"))
    if target_12m_base is None:
        target_12m_base = _safe_float(scenario_12m.get("base"))
    target_24m_base = _safe_float(price_targets.get("target_24m"))
    if target_24m_base is None:
        target_24m_base = _safe_float(scenario_24m.get("base"))

    prob_weighted_12m = _safe_float(price_targets.get("prob_weighted_target_12m"))
    if prob_weighted_12m is None:
        prob_weighted_12m = _compute_prob_weighted_target(scenario_12m, probs_12m)

    prob_weighted_24m = _safe_float(price_targets.get("prob_weighted_target_24m"))
    if prob_weighted_24m is None:
        prob_weighted_24m = _compute_prob_weighted_target(scenario_24m, probs_24m)

    current_stage = str(structured.get("current_development_stage") or "").strip()
    if not current_stage:
        timeline_raw = structured.get("development_timeline")
        if isinstance(timeline_raw, list) and timeline_raw:
            first_row = timeline_raw[0]
            if isinstance(first_row, dict):
                current_stage = str(first_row.get("status") or first_row.get("stage") or "").strip()

    return {
        "ticker": str(structured.get("ticker") or "").strip(),
        "company_name": str(structured.get("company_name") or structured.get("company") or "").strip(),
        "analysis_date": str(structured.get("analysis_date") or "").strip(),
        "template_id": str(
            structured.get("template_id")
            or top_level_contract.get("id")
            or council_contract.get("id")
            or ""
        ).strip(),
        "quality_score": quality_total,
        "value_score": value_total,
        "rating": rating,
        "conviction": conviction,
        "current_price": current_price,
        "target_12m_base": target_12m_base,
        "target_24m_base": target_24m_base,
        "prob_weighted_target_12m": prob_weighted_12m,
        "prob_weighted_target_24m": prob_weighted_24m,
        "current_stage": current_stage,
        "freshness_status": str((freshness or {}).get("status") or ""),
        "freshness_age_days": (freshness or {}).get("age_days"),
        "freshness_recommended_action": str((freshness or {}).get("recommended_action") or ""),
    }


def _load_latest_scenario_router_state(run_id: str) -> Dict[str, Any]:
    try:
        from ..scenario_router.lab_scribe import LabScribe

        return LabScribe.load_latest_for_run(run_id)
    except Exception:
        return {}


def _build_scenario_router_summary(router_state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(router_state, dict) or not router_state:
        return {}

    build_router_display_contract = None
    market_only_watch_projection = None
    should_project_market_only_watch = None
    watchlist_engagement_projection = None
    router_condition_details = None
    router_thesis_snapshot = None
    try:
        from ..scenario_router.artifact_replay import replay_comparison_from_artifact
        from ..scenario_router.display_contract import (
            build_router_display_contract,
            market_only_watch_projection,
            should_project_market_only_watch,
            watchlist_engagement_projection,
        )
        from ..scenario_router.observability import (
            _condition_details as router_condition_details,
            _thesis_snapshot as router_thesis_snapshot,
        )

        comparison, action = replay_comparison_from_artifact(router_state)
    except Exception:
        comparison = (
            router_state.get("comparison_report")
            if isinstance(router_state.get("comparison_report"), dict)
            else {}
        )
        action = (
            router_state.get("action_decision")
            if isinstance(router_state.get("action_decision"), dict)
            else {}
        )
    facts = (
        router_state.get("announcement_facts")
        if isinstance(router_state.get("announcement_facts"), dict)
        else {}
    )
    event = (
        router_state.get("event")
        if isinstance(router_state.get("event"), dict)
        else {}
    )
    condition_evaluations = (
        comparison.get("condition_evaluations")
        if isinstance(comparison.get("condition_evaluations"), list)
        else []
    )
    matched_conditions = [
        str(item.get("label") or item.get("condition_id") or "").strip()
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "matched"
        and str(item.get("group") or "").strip() in {"required", "failure"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    triggered_watchlist = [
        str(item.get("label") or item.get("condition_id") or "").strip()
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() in {"matched", "partial_match"}
        and str(item.get("group") or "").strip() in {"red_flag", "confirmatory"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    checked_watchlist = [
        str(item.get("label") or item.get("condition_id") or "").strip()
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "checked_not_triggered"
        and str(item.get("group") or "").strip() in {"red_flag", "confirmatory"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    triggered_verification = [
        str(item.get("label") or item.get("condition_id") or "").strip()
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "matched"
        and str(item.get("group") or "").strip() == "verification"
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    market_context_conditions = [
        {
            "label": str(item.get("label") or item.get("condition_id") or "").strip(),
            "scenario": str(item.get("scenario") or "").strip(),
            "group": str(item.get("group") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "matched_via": str(item.get("matched_via") or "").strip(),
            "field": str(item.get("market_field") or "").strip(),
            "observed_value": item.get("observed_value"),
            "comparator": str(item.get("comparator") or "").strip(),
            "threshold_value": item.get("threshold_value"),
            "status": str(item.get("status") or "").strip(),
        }
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("matched_via") or "").strip() == "market_facts"
        and str(item.get("status") or "").strip() in {"matched", "contradicted"}
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    matched_condition_details = [
        {
            "label": str(item.get("label") or item.get("condition_id") or "").strip(),
            "scenario": str(item.get("scenario") or "").strip(),
            "group": str(item.get("group") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "matched_via": str(item.get("matched_via") or "").strip(),
            "confidence": item.get("confidence"),
        }
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "matched"
        and str(item.get("group") or "").strip() in {"required", "failure"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    triggered_watchlist_details = [
        {
            "label": str(item.get("label") or item.get("condition_id") or "").strip(),
            "scenario": str(item.get("scenario") or "").strip(),
            "group": str(item.get("group") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "matched_via": str(item.get("matched_via") or "").strip(),
            "confidence": item.get("confidence"),
        }
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() in {"matched", "partial_match"}
        and str(item.get("group") or "").strip() in {"red_flag", "confirmatory"}
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    triggered_verification_details = [
        {
            "label": str(item.get("label") or item.get("condition_id") or "").strip(),
            "scenario": str(item.get("scenario") or "").strip(),
            "group": str(item.get("group") or "").strip(),
            "reason": str(item.get("reason") or "").strip(),
            "matched_via": str(item.get("matched_via") or "").strip(),
            "confidence": item.get("confidence"),
        }
        for item in condition_evaluations
        if isinstance(item, dict)
        and str(item.get("status") or "").strip() == "matched"
        and str(item.get("group") or "").strip() == "verification"
        and str(item.get("matched_via") or "").strip() != "market_facts"
        and str(item.get("label") or item.get("condition_id") or "").strip()
    ][:8]
    key_findings = [
        {
            "type": str(item.get("type") or "").strip(),
            "summary": str(item.get("summary") or "").strip(),
            "severity": str(item.get("severity") or "").strip(),
        }
        for item in (comparison.get("key_findings") or [])
        if isinstance(item, dict) and str(item.get("summary") or "").strip()
    ][:8]
    conflicts_with_run = [
        {
            "type": str(item.get("type") or "").strip(),
            "summary": str(item.get("summary") or "").strip(),
            "severity": str(item.get("severity") or "").strip(),
        }
        for item in (comparison.get("conflicts_with_run") or [])
        if isinstance(item, dict) and str(item.get("summary") or "").strip()
    ][:8]
    packet = (
        router_state.get("announcement_packet")
        if isinstance(router_state.get("announcement_packet"), dict)
        else {}
    )
    raw_action = str(action.get("action") or "").strip()
    raw_current_path = str(comparison.get("current_path") or "").strip()
    raw_baseline_path = str(comparison.get("baseline_path") or "").strip()
    raw_impact = str(comparison.get("impact_level") or "").strip()
    suppress_stale_market_only_reroute = False
    if should_project_market_only_watch:
        suppress_stale_market_only_reroute = should_project_market_only_watch(
            matched_conditions_count=len(matched_conditions),
            triggered_watchlist_count=len(triggered_watchlist) + len(triggered_verification),
            market_conditions_count=len(market_context_conditions),
            raw_action=raw_action,
            materiality=str(comparison.get("materiality") or facts.get("materiality") or "").strip(),
            trajectory_state=str(comparison.get("trajectory_state") or "").strip(),
        )
    if suppress_stale_market_only_reroute and market_only_watch_projection:
        display_projection = market_only_watch_projection(
            baseline_path=raw_baseline_path,
            current_path=raw_current_path,
            raw_impact=raw_impact,
        )
    elif watchlist_engagement_projection:
        display_projection = watchlist_engagement_projection(
            comparison,
            action,
            triggered_watchlist_count=len(triggered_watchlist),
        )
    else:
        display_projection = {}
    display_current_path = str(display_projection.get("current_path") or raw_current_path).strip()
    display_action = str(display_projection.get("action") or raw_action).strip()
    display_impact = str(display_projection.get("impact_level") or raw_impact).strip()
    display_trajectory_state = str(display_projection.get("trajectory_state") or comparison.get("trajectory_state") or "").strip()
    display_contract = (
        build_router_display_contract(
            {
                **comparison,
                "trajectory_state": display_trajectory_state,
                "impact_level": display_impact,
                "status": str(router_state.get("status") or "ok").strip() or "ok",
            },
            {
                **action,
                "action": display_action,
                "reason": str(display_projection.get("reason") or action.get("reason") or "").strip(),
            },
            matched_conditions_count=len(matched_conditions),
            triggered_watchlist_count=len(triggered_watchlist),
            triggered_verification_count=len(triggered_verification),
            checked_watchlist_count=len(checked_watchlist),
        )
        if build_router_display_contract
        else {}
    )
    return {
        "current_path": display_current_path,
        "baseline_path": raw_baseline_path,
        "path_transition": str(
            display_projection.get("path_transition")
            if display_projection
            else comparison.get("path_transition") or ""
        ).strip(),
        "path_confidence": comparison.get("path_confidence"),
        "run_validity": str(display_projection.get("run_validity") or comparison.get("run_validity") or "").strip(),
        "impact_level": display_impact,
        "announcement_class": str(comparison.get("announcement_class") or facts.get("announcement_class") or "").strip(),
        "materiality": str(comparison.get("materiality") or facts.get("materiality") or "").strip(),
        "relationship_priority": comparison.get("relationship_priority", 0),
        "relationship_kind": str(comparison.get("relationship_kind") or "").strip(),
        "relationship_strength": str(comparison.get("relationship_strength") or "").strip(),
        "relationship_direction": str(comparison.get("relationship_direction") or "").strip(),
        "relationship_summary": str(comparison.get("relationship_summary") or "").strip(),
        "trajectory_state": display_trajectory_state,
        "trajectory_effect": str(display_projection.get("trajectory_effect") or comparison.get("trajectory_effect") or facts.get("trajectory_effect") or "").strip(),
        "price_time_effect": str(display_projection.get("price_time_effect") or comparison.get("price_time_effect") or facts.get("price_time_effect") or "").strip(),
        "semantic_summary": str(comparison.get("semantic_summary") or facts.get("semantic_summary") or "").strip(),
        "filing_summary": str(comparison.get("filing_summary") or facts.get("filing_summary") or "").strip(),
        "parser_confidence": comparison.get("parser_confidence", facts.get("semantic_confidence")),
        "source_confidence": comparison.get("source_confidence", facts.get("source_confidence")),
        "extraction_confidence": comparison.get("extraction_confidence", facts.get("extraction_confidence")),
        "classification_confidence": comparison.get("classification_confidence", facts.get("classification_confidence", facts.get("semantic_confidence"))),
        "thesis_match_confidence": comparison.get("thesis_match_confidence", facts.get("thesis_match_confidence")),
        "classification_reason": str(comparison.get("classification_reason") or facts.get("classification_reason") or "").strip(),
        "confidence_breakdown": (
            comparison.get("confidence_breakdown")
            if isinstance(comparison.get("confidence_breakdown"), dict)
            else facts.get("confidence_breakdown") if isinstance(facts.get("confidence_breakdown"), dict) else {}
        ),
        "domain_profile": str(facts.get("domain_profile") or "").strip(),
        "parser_warnings": [str(item or "").strip() for item in (facts.get("parser_warnings") or []) if str(item or "").strip()][:8],
        "classification_basis": [str(item or "").strip() for item in (facts.get("classification_basis") or []) if str(item or "").strip()][:8],
        "action": display_action,
        "action_confidence": action.get("confidence"),
        "reason": str(display_projection.get("reason") or action.get("reason") or "").strip(),
        "announcement_title": str(
            comparison.get("announcement_title") or facts.get("title") or event.get("subject") or ""
        ).strip(),
        "matched_conditions": matched_conditions,
        "matched_condition_details": matched_condition_details,
        "triggered_watchlist": triggered_watchlist,
        "checked_watchlist": checked_watchlist,
        "triggered_watchlist_details": triggered_watchlist_details,
        "triggered_verification": triggered_verification,
        "triggered_verification_details": triggered_verification_details,
        "market_context_conditions": market_context_conditions,
        "announcement_condition_checks": (
            router_condition_details(
                condition_evaluations,
                groups={"required", "failure"},
                statuses={"matched", "partial_match", "checked_not_triggered", "not_matched", "contradicted", "unclear"},
                exclude_market=True,
                limit=40,
            )
            if router_condition_details
            else matched_condition_details
        ),
        "watchlist_condition_checks": (
            router_condition_details(
                condition_evaluations,
                groups={"red_flag", "confirmatory"},
                statuses={"matched", "partial_match", "checked_not_triggered", "not_matched", "contradicted", "unclear"},
                exclude_market=True,
                limit=30,
            )
            if router_condition_details
            else triggered_watchlist_details
        ),
        "verification_condition_checks": (
            router_condition_details(
                condition_evaluations,
                groups={"verification"},
                statuses={"matched", "partial_match", "checked_not_triggered", "not_matched", "contradicted", "unclear"},
                exclude_market=True,
                limit=30,
            )
            if router_condition_details
            else triggered_verification_details
        ),
        "triggered_verification_count": len(triggered_verification),
        "checked_watchlist_count": len(checked_watchlist),
        "trajectory_projection": (
            comparison.get("trajectory_projection")
            if isinstance(comparison.get("trajectory_projection"), dict)
            else {}
        ),
        "trajectory_score": (
            display_projection.get("trajectory_score")
            if isinstance(display_projection.get("trajectory_score"), dict)
            else
            comparison.get("trajectory_score")
            if isinstance(comparison.get("trajectory_score"), dict)
            else {}
        ),
        "thesis_snapshot": router_thesis_snapshot(router_state.get("baseline_run")) if router_thesis_snapshot else {},
        "key_findings": key_findings,
        "conflicts_with_run": conflicts_with_run,
        "affected_domains": (
            display_projection.get("affected_domains")
            if display_projection
            else comparison.get("affected_domains")
            if isinstance(comparison.get("affected_domains"), list)
            else []
        ),
        "thesis_effect": str(display_projection.get("thesis_effect") or comparison.get("thesis_effect") or "").strip(),
        "run_validity": str(display_projection.get("run_validity") or comparison.get("run_validity") or "").strip(),
        "source_type": str(packet.get("source_type") or "").strip(),
        "source_url": str(packet.get("source_url") or "").strip(),
        "market_facts_used": (
            comparison.get("market_facts_used")
            if isinstance(comparison.get("market_facts_used"), dict)
            else {}
        ),
        "invalidated_sections": [
            str(item or "").strip()
            for item in (
                (display_projection.get("invalidated_sections") if display_projection else action.get("invalidated_sections"))
                or []
            )
            if str(item or "").strip()
        ][:8],
        "follow_up_steps": [
            str(item or "").strip()
            for item in (
                (display_projection.get("follow_up_steps") if display_projection else action.get("follow_up_steps"))
                or []
            )
            if str(item or "").strip()
        ][:5],
        "display_adjustment": str(display_projection.get("display_adjustment") or "").strip(),
        "display": display_contract,
        "received_at_utc": str(event.get("received_at_utc") or "").strip(),
        "saved_at_utc": str(router_state.get("saved_at_utc") or "").strip(),
    }


def _build_integration_packet(
    *,
    run_id: str,
    run_payload: Dict[str, Any],
) -> Dict[str, Any]:
    structured = run_payload.get("structured_data") if isinstance(run_payload.get("structured_data"), dict) else {}
    freshness = run_payload.get("freshness") if isinstance(run_payload.get("freshness"), dict) else {}
    scenario_router = (
        run_payload.get("scenario_router")
        if isinstance(run_payload.get("scenario_router"), dict)
        else {}
    )
    timeline_rows = _normalize_timeline_rows_for_api(structured.get("development_timeline"))
    extended_analysis = structured.get("extended_analysis") if isinstance(structured.get("extended_analysis"), dict) else {}
    catalyst_rows = _normalize_catalyst_rows_for_api(extended_analysis.get("next_major_catalysts"))
    summary_fields = _build_summary_fields(structured, freshness)
    summary_fields.update(
        {
            "current_path": str(scenario_router.get("current_path") or "").strip(),
            "path_transition": str(scenario_router.get("path_transition") or "").strip(),
            "scenario_router_action": str(scenario_router.get("action") or "").strip(),
            "scenario_router_impact": str(scenario_router.get("impact_level") or "").strip(),
        }
    )

    return {
        "contract": "analysis_report_packet_v1",
        "run_id": str(run_id),
        "summary_fields": summary_fields,
        "lab_payload": {
            "id": run_payload.get("id"),
            "file": run_payload.get("file"),
            "label": run_payload.get("label"),
            "updated_at": run_payload.get("updated_at"),
            "structured_data": structured,
            "freshness": freshness,
            "scenario_router": scenario_router,
            "delta_check": run_payload.get("delta_check") or {},
            "analyst_memo_markdown": run_payload.get("analyst_memo_markdown") or "",
            "chairman_memo_markdown": run_payload.get("chairman_memo_markdown") or "",
        },
        "timeline_rows": timeline_rows,
        "catalyst_rows": catalyst_rows,
        "scenario_router": scenario_router,
        "memos": {
            "analyst_memo_markdown": run_payload.get("analyst_memo_markdown") or "",
            "chairman_memo_markdown": run_payload.get("chairman_memo_markdown") or "",
        },
    }


def _canonical_run_id_for_listing(filename: str) -> str:
    """
    Collapse stage/checkpoint/preview JSON variants into one canonical run artifact id.
    """
    safe_name = Path(filename).name

    # quality_run_xxx.stage3_primary.checkpoint.json -> quality_run_xxx.json
    if safe_name.endswith(".checkpoint.json"):
        base = safe_name[: -len(".checkpoint.json")]
        if ".stage" in base:
            base = base.split(".stage", 1)[0]
        return f"{base}.json"

    # stage3_replay_batch_xxx.normalized_preview_yyy.json -> stage3_replay_batch_xxx.json
    preview_marker = ".normalized_preview_"
    if preview_marker in safe_name and safe_name.endswith(".json"):
        base = safe_name.split(preview_marker, 1)[0]
        return f"{base}.json"

    # Any other stage-suffixed sidecar JSON should map to base run artifact.
    if ".stage" in safe_name and safe_name.endswith(".json"):
        base = safe_name.split(".stage", 1)[0]
        return f"{base}.json"

    return safe_name


def _build_portfolio_positioning_run_label(filename: str, structured: Dict[str, Any], payload: Dict[str, Any]) -> str:
    label = str(payload.get("label") or payload.get("run_label") or "").strip()
    analysis_date = str(structured.get("analysis_date") or payload.get("analysis_date") or "").strip()
    date_label = ""
    if analysis_date:
        try:
            dt = datetime.fromisoformat(analysis_date.replace("Z", "+00:00"))
            date_label = dt.strftime("%Y-%m-%d")
        except Exception:
            date_label = analysis_date[:10]
    head = label or "Portfolio Positioning Memo"
    if date_label:
        return f"{head} ({date_label})"
    return head or filename
