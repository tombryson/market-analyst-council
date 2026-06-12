from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from .artifact_replay import replay_comparison_from_artifact
from .display_contract import (
    build_router_display_contract,
    market_only_watch_projection,
    should_project_market_only_watch,
    watchlist_engagement_projection,
)
from .lab_scribe import SCENARIO_ROUTER_EVENTS_DIR
from .mock_harness import run_mock_router_case
from .review_store import apply_review_overlay, load_review
from .trajectory_scoring import apply_cumulative_scores

EVALUATION_CASES_PATH = Path(__file__).with_name("evaluation_cases.json")
EVIDENCE_ENGAGED_STATUSES = {"matched", "partial_match"}


@dataclass
class ScenarioRouterObservability:
    base_dir: Path = SCENARIO_ROUTER_EVENTS_DIR

    def list_recent_events(self, *, limit: int = 50, ticker: str = "") -> List[Dict[str, Any]]:
        wanted = str(ticker or "").strip().upper()
        rows: List[Dict[str, Any]] = []
        for path in self._event_artifact_paths():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            row = self._summarize_event_payload(payload, path=path)
            if self._is_skipped_status(row.get("status")):
                continue
            row = apply_review_overlay(row, load_review(str(row.get("event_id") or "")))
            if wanted and str(row.get("ticker") or "").strip().upper() != wanted:
                continue
            rows.append(row)

        apply_cumulative_scores(rows)
        rows.sort(
            key=lambda item: (
                str(item.get("saved_at_utc") or "").strip(),
                str(item.get("received_at_utc") or "").strip(),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit))]

    @staticmethod
    def _is_skipped_status(status: Any) -> bool:
        normalized = str(status or "").strip().lower()
        return normalized in {"no_baseline_run", "skipped_no_baseline_run"}

    def build_overview(self, *, recent_limit: int = 100, ticker: str = "") -> Dict[str, Any]:
        rows = self.list_recent_events(limit=max(1, int(recent_limit)), ticker=ticker)
        total = len(rows)
        status_counts = Counter(str(row.get("status") or "").strip() for row in rows if str(row.get("status") or "").strip())
        action_counts = Counter(str(row.get("action") or "").strip() for row in rows if str(row.get("action") or "").strip())
        impact_counts = Counter(str(row.get("impact_level") or "").strip() for row in rows if str(row.get("impact_level") or "").strip())
        current_path_counts = Counter(str(row.get("current_path") or "").strip() for row in rows if str(row.get("current_path") or "").strip())
        transition_counts = Counter(str(row.get("path_transition") or "").strip() for row in rows if str(row.get("path_transition") or "").strip())
        source_type_counts = Counter(str(row.get("source_type") or "").strip() for row in rows if str(row.get("source_type") or "").strip())
        unique_tickers = sorted({str(row.get("ticker") or "").strip() for row in rows if str(row.get("ticker") or "").strip()})
        processing_durations = [int(row.get("processing_duration_ms") or 0) for row in rows if int(row.get("processing_duration_ms") or 0) > 0]
        avg_processing_ms = round(sum(processing_durations) / len(processing_durations), 1) if processing_durations else 0.0
        official_source_count = sum(
            1
            for row in rows
            if str(row.get("source_type") or "").strip().lower() == "exchange_filing"
            and str(row.get("source_url") or "").strip()
        )

        return {
            "total_events": total,
            "unique_tickers": len(unique_tickers),
            "status_counts": dict(status_counts),
            "official_source_rate_pct": round((official_source_count / total) * 100.0, 1) if total else 0.0,
            "average_processing_ms": avg_processing_ms,
            "action_counts": dict(action_counts),
            "impact_counts": dict(impact_counts),
            "current_path_counts": dict(current_path_counts),
            "path_transition_counts": dict(transition_counts),
            "source_type_counts": dict(source_type_counts),
            "recent_events": rows[:12],
        }

    def build_signal_map(self, *, limit: int = 500, ticker: str = "") -> Dict[str, int | float]:
        wanted = str(ticker or "").strip().upper()
        rows = self.list_recent_events(limit=max(1, int(limit)), ticker=wanted)
        signals: Dict[str, int | float] = {}
        for row in rows:
            row_ticker = str(row.get("ticker") or "").strip().upper()
            if not row_ticker or row_ticker in signals:
                continue
            signals[row_ticker] = _compact_number(_validated_signal_score(row))
        if wanted and wanted not in signals:
            signals[wanted] = 0
        return signals

    def run_evaluation_suite(self) -> Dict[str, Any]:
        cases = self._load_evaluation_cases()
        results: List[Dict[str, Any]] = []

        for case in cases:
            result = run_mock_router_case(case)
            expected = case.get("expected") or {}

            results.append(
                {
                    "case_id": str(case.get("case_id") or ""),
                    "category": str(case.get("category") or ""),
                    "label": str(case.get("label") or ""),
                    "expected": expected,
                    "actual": result.get("actual") or {},
                    "scenario_results": result.get("scenario_results") or {},
                    "assertions": result.get("assertions") or [],
                    "passed": bool(result.get("passed")),
                }
            )

        passed_count = sum(1 for item in results if item.get("passed"))
        return {
            "total_cases": len(results),
            "passed_cases": passed_count,
            "failed_cases": max(0, len(results) - passed_count),
            "pass_rate_pct": round((passed_count / len(results)) * 100.0, 1) if results else 0.0,
            "results": results,
        }

    def _event_artifact_paths(self) -> List[Path]:
        base = Path(self.base_dir)
        if not base.exists() or not base.is_dir():
            return []
        out: List[Path] = []
        for child in base.iterdir():
            if not child.is_dir():
                continue
            if child.name in {"by_run", "dedupe", "reviews"}:
                continue
            for path in child.glob("*.json"):
                if path.name == "latest.json":
                    continue
                out.append(path)
        return out

    @staticmethod
    def _summarize_event_payload(payload: Dict[str, Any], *, path: Path) -> Dict[str, Any]:
        event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
        packet = payload.get("announcement_packet") if isinstance(payload.get("announcement_packet"), dict) else {}
        facts = payload.get("announcement_facts") if isinstance(payload.get("announcement_facts"), dict) else {}
        report, action = replay_comparison_from_artifact(payload)
        baseline_run = payload.get("baseline_run") if isinstance(payload.get("baseline_run"), dict) else {}
        trace = payload.get("processing_trace") if isinstance(payload.get("processing_trace"), list) else []
        evaluations = report.get("condition_evaluations") if isinstance(report.get("condition_evaluations"), list) else []
        matched_conditions = sum(
            1
            for item in evaluations
            if isinstance(item, dict)
            and str(item.get("status") or "").strip() == "matched"
            and str(item.get("group") or "").strip() in {"required", "failure"}
            and str(item.get("matched_via") or "").strip() != "market_facts"
        )
        triggered_watchlist = sum(
            1
            for item in evaluations
            if isinstance(item, dict)
            and str(item.get("status") or "").strip() in EVIDENCE_ENGAGED_STATUSES
            and str(item.get("group") or "").strip() in {"red_flag", "confirmatory"}
            and str(item.get("matched_via") or "").strip() != "market_facts"
        )
        checked_watchlist = sum(
            1
            for item in evaluations
            if isinstance(item, dict)
            and str(item.get("status") or "").strip() == "checked_not_triggered"
            and str(item.get("group") or "").strip() in {"red_flag", "confirmatory"}
            and str(item.get("matched_via") or "").strip() != "market_facts"
        )
        triggered_verification = sum(
            1
            for item in evaluations
            if isinstance(item, dict)
            and str(item.get("status") or "").strip() in EVIDENCE_ENGAGED_STATUSES
            and str(item.get("group") or "").strip() == "verification"
            and str(item.get("matched_via") or "").strip() != "market_facts"
        )
        market_conditions = sum(
            1
            for item in evaluations
            if isinstance(item, dict)
            and str(item.get("matched_via") or "").strip() == "market_facts"
            and str(item.get("status") or "").strip() in {"matched", "contradicted"}
        )
        raw_action = str(action.get("action") or "").strip()
        raw_current_path = str(report.get("current_path") or "").strip()
        raw_baseline_path = str(report.get("baseline_path") or "").strip()
        suppress_stale_market_only_reroute = should_project_market_only_watch(
            matched_conditions_count=matched_conditions,
            triggered_watchlist_count=triggered_watchlist + triggered_verification,
            market_conditions_count=market_conditions,
            raw_action=raw_action,
            materiality=str(report.get("materiality") or facts.get("materiality") or "").strip(),
            trajectory_state=str(report.get("trajectory_state") or "").strip(),
        )
        display_projection = (
            market_only_watch_projection(
                baseline_path=raw_baseline_path,
                current_path=raw_current_path,
                raw_impact=str(report.get("impact_level") or "").strip(),
            )
            if suppress_stale_market_only_reroute
            else watchlist_engagement_projection(
                report,
                action,
                triggered_watchlist_count=triggered_watchlist,
            )
        )
        display_action = str(display_projection.get("action") or raw_action).strip()
        display_report = {
            **report,
            "trajectory_state": str(display_projection.get("trajectory_state") or report.get("trajectory_state") or "").strip(),
            "impact_level": str(display_projection.get("impact_level") or report.get("impact_level") or "").strip(),
            "filing_type": str(display_projection.get("filing_type") or report.get("filing_type") or "").strip(),
            "evidence_scope": str(display_projection.get("evidence_scope") or report.get("evidence_scope") or "").strip(),
            "thesis_relationship": str(display_projection.get("thesis_relationship") or report.get("thesis_relationship") or "").strip(),
            "impact_verdict": str(display_projection.get("impact_verdict") or report.get("impact_verdict") or "").strip(),
            "impact_dimension": str(display_projection.get("impact_dimension") or report.get("impact_dimension") or "").strip(),
            "status": str(payload.get("status") or "ok").strip() or "ok",
        }
        display_decision = {
            **action,
            "action": display_action,
            "reason": str(display_projection.get("reason") or action.get("reason") or "").strip(),
        }
        display_contract = build_router_display_contract(
            display_report,
            display_decision,
            matched_conditions_count=matched_conditions,
            triggered_watchlist_count=triggered_watchlist,
            triggered_verification_count=triggered_verification,
            checked_watchlist_count=checked_watchlist,
        )

        return {
            "status": str(payload.get("status") or "ok").strip() or "ok",
            "event_id": str(event.get("event_id") or packet.get("event_id") or path.stem).strip(),
            "ticker": str(event.get("ticker") or packet.get("ticker") or "").strip(),
            "title": str(packet.get("title") or report.get("announcement_title") or "").strip(),
            "company_name": str(packet.get("company_name") or "").strip(),
            "saved_at_utc": str(payload.get("saved_at_utc") or "").strip(),
            "received_at_utc": str(event.get("received_at_utc") or "").strip(),
            "action": display_action,
            "impact_level": str(display_projection.get("impact_level") or report.get("impact_level") or "").strip(),
            "announcement_class": str(report.get("announcement_class") or facts.get("announcement_class") or "").strip(),
            "filing_type": str(display_projection.get("filing_type") or report.get("filing_type") or "").strip(),
            "materiality": str(report.get("materiality") or facts.get("materiality") or "").strip(),
            "evidence_scope": str(display_projection.get("evidence_scope") or report.get("evidence_scope") or "").strip(),
            "thesis_relationship": str(display_projection.get("thesis_relationship") or report.get("thesis_relationship") or "").strip(),
            "impact_verdict": str(display_projection.get("impact_verdict") or report.get("impact_verdict") or "").strip(),
            "impact_dimension": str(display_projection.get("impact_dimension") or report.get("impact_dimension") or "").strip(),
            "relationship_priority": report.get("relationship_priority", 0),
            "relationship_kind": str(report.get("relationship_kind") or "").strip(),
            "relationship_strength": str(report.get("relationship_strength") or "").strip(),
            "relationship_direction": str(report.get("relationship_direction") or "").strip(),
            "relationship_summary": str(report.get("relationship_summary") or "").strip(),
            "trajectory_state": str(display_projection.get("trajectory_state") or report.get("trajectory_state") or "").strip(),
            "trajectory_effect": str(display_projection.get("trajectory_effect") or report.get("trajectory_effect") or facts.get("trajectory_effect") or "").strip(),
            "price_time_effect": str(display_projection.get("price_time_effect") or report.get("price_time_effect") or facts.get("price_time_effect") or "").strip(),
            "semantic_summary": str(report.get("semantic_summary") or facts.get("semantic_summary") or "").strip(),
            "filing_summary": str(report.get("filing_summary") or facts.get("filing_summary") or "").strip(),
            "parser_confidence": report.get("parser_confidence", facts.get("semantic_confidence")),
            "source_confidence": report.get("source_confidence", facts.get("source_confidence")),
            "extraction_confidence": report.get("extraction_confidence", facts.get("extraction_confidence")),
            "classification_confidence": report.get("classification_confidence", facts.get("classification_confidence", facts.get("semantic_confidence"))),
            "thesis_match_confidence": report.get("thesis_match_confidence", facts.get("thesis_match_confidence")),
            "classification_reason": str(report.get("classification_reason") or facts.get("classification_reason") or "").strip(),
            "confidence_breakdown": (
                report.get("confidence_breakdown")
                if isinstance(report.get("confidence_breakdown"), dict)
                else facts.get("confidence_breakdown") if isinstance(facts.get("confidence_breakdown"), dict) else {}
            ),
            "domain_profile": str(facts.get("domain_profile") or "").strip(),
            "parser_warnings": [str(item or "").strip() for item in (facts.get("parser_warnings") or []) if str(item or "").strip()][:8],
            "classification_basis": [str(item or "").strip() for item in (facts.get("classification_basis") or []) if str(item or "").strip()][:8],
            "current_path": str(display_projection.get("current_path") or raw_current_path).strip(),
            "baseline_path": raw_baseline_path,
            "path_transition": str(display_projection.get("path_transition") if display_projection else report.get("path_transition") or "").strip(),
            "source_type": str(packet.get("source_type") or "").strip(),
            "source_url": str(packet.get("source_url") or "").strip(),
            "run_id": str(baseline_run.get("run_id") or "").strip(),
            "processing_duration_ms": int(payload.get("processing_duration_ms") or 0),
            "matched_conditions_count": matched_conditions,
            "triggered_watchlist_count": triggered_watchlist,
            "checked_watchlist_count": checked_watchlist,
            "triggered_verification_count": triggered_verification,
            "market_conditions_count": market_conditions,
            "matched_conditions": _condition_details(
                evaluations,
                groups={"required", "failure"},
                statuses={"matched"},
                exclude_market=True,
            ),
            "triggered_watchlist": _condition_details(evaluations, groups={"red_flag", "confirmatory"}, exclude_market=True),
            "triggered_verification": _condition_details(evaluations, groups={"verification"}, exclude_market=True),
            "market_context_conditions": _condition_details(
                evaluations,
                matched_via="market_facts",
                statuses={"matched", "contradicted", "unclear"},
                limit=12,
            ),
            "announcement_condition_checks": _condition_details(
                evaluations,
                groups={"required", "failure"},
                statuses={"matched", "partial_match", "checked_not_triggered", "not_matched", "contradicted", "unclear"},
                exclude_market=True,
                limit=40,
            ),
            "watchlist_condition_checks": _condition_details(
                evaluations,
                groups={"red_flag", "confirmatory"},
                statuses={"matched", "partial_match", "checked_not_triggered", "not_matched", "contradicted", "unclear"},
                exclude_market=True,
                limit=30,
            ),
            "verification_condition_checks": _condition_details(
                evaluations,
                groups={"verification"},
                statuses={"matched", "partial_match", "checked_not_triggered", "not_matched", "contradicted", "unclear"},
                exclude_market=True,
                limit=30,
            ),
            "trajectory_projection": (
                report.get("trajectory_projection")
                if isinstance(report.get("trajectory_projection"), dict)
                else {}
            ),
            "trajectory_score": (
                display_projection.get("trajectory_score")
                if isinstance(display_projection.get("trajectory_score"), dict)
                else
                report.get("trajectory_score")
                if isinstance(report.get("trajectory_score"), dict)
                else {}
            ),
            "thesis_snapshot": _thesis_snapshot(baseline_run),
            "key_findings": _finding_details(report.get("key_findings")),
            "conflicts_with_run": _finding_details(report.get("conflicts_with_run")),
            "action_reason": str(display_projection.get("action_reason") or action.get("reason") or "").strip(),
            "action_confidence": action.get("confidence"),
            "follow_up_steps": (
                [str(item or "").strip() for item in (display_projection.get("follow_up_steps") or []) if str(item or "").strip()]
                if display_projection
                else [str(item or "").strip() for item in (action.get("follow_up_steps") or []) if str(item or "").strip()]
            )[:5],
            "invalidated_sections": (
                [str(item or "").strip() for item in (display_projection.get("invalidated_sections") or []) if str(item or "").strip()]
                if display_projection
                else [
                str(item or "").strip()
                for item in (action.get("invalidated_sections") or [])
                if str(item or "").strip()
                ]
            )[:8],
            "affected_domains": (
                [str(item or "").strip() for item in (display_projection.get("affected_domains") or []) if str(item or "").strip()]
                if display_projection
                else [
                str(item or "").strip()
                for item in (report.get("affected_domains") or [])
                if str(item or "").strip()
                ]
            )[:8],
            "thesis_effect": str(display_projection.get("thesis_effect") or report.get("thesis_effect") or "").strip(),
            "run_validity": str(display_projection.get("run_validity") or report.get("run_validity") or "").strip(),
            "display_adjustment": str(display_projection.get("display_adjustment") or "").strip(),
            "display": display_contract,
            "market_facts_used": report.get("market_facts_used") if isinstance(report.get("market_facts_used"), dict) else {},
            "error_reason": str(((payload.get("error") or {}) if isinstance(payload.get("error"), dict) else {}).get("reason") or "").strip(),
            "processing_trace": trace,
            "artifact_path": str(path),
        }

    @staticmethod
    def _load_evaluation_cases() -> List[Dict[str, Any]]:
        if not EVALUATION_CASES_PATH.exists() or not EVALUATION_CASES_PATH.is_file():
            return []
        try:
            payload = json.loads(EVALUATION_CASES_PATH.read_text(encoding="utf-8"))
        except Exception:
            return []
        return payload if isinstance(payload, list) else []


def _condition_details(
    evaluations: List[Dict[str, Any]],
    *,
    groups: set[str] | None = None,
    matched_via: str = "",
    statuses: set[str] | None = None,
    exclude_market: bool = False,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in evaluations:
        if not isinstance(item, dict):
            continue
        group = str(item.get("group") or "").strip()
        status = str(item.get("status") or "").strip()
        via = str(item.get("matched_via") or "").strip()
        if groups is not None and group not in groups:
            continue
        if statuses is not None and status not in statuses:
            continue
        if statuses is None and status not in EVIDENCE_ENGAGED_STATUSES:
            continue
        if matched_via and via != matched_via:
            continue
        if exclude_market and via == "market_facts":
            continue
        label = str(item.get("label") or item.get("condition_id") or "").strip()
        if not label:
            continue
        evidence = item.get("evidence") if isinstance(item.get("evidence"), dict) else {}
        rows.append(
            {
                "label": label,
                "condition_id": str(item.get("condition_id") or "").strip(),
                "scenario": str(item.get("scenario") or "").strip(),
                "group": group,
                "status": status,
                "reason": str(item.get("reason") or "").strip(),
                "matched_via": via,
                "relationship": str(item.get("relationship") or "").strip(),
                "satisfies_condition": bool(item.get("satisfies_condition")),
                "missing_for_full_match": [
                    str(value or "").strip()
                    for value in (item.get("missing_for_full_match") or [])
                    if str(value or "").strip()
                ][:6],
                "confidence": item.get("confidence"),
                "market_field": str(item.get("market_field") or "").strip(),
                "observed_value": item.get("observed_value"),
                "comparator": str(item.get("comparator") or "").strip(),
                "threshold_value": item.get("threshold_value"),
                "evidence_quote": str(evidence.get("quote_excerpt") or "").strip(),
                "source_url": str(evidence.get("source_url") or "").strip(),
                "source_title": str(evidence.get("source_title") or "").strip(),
            }
        )
        if len(rows) >= max(1, int(limit or 10)):
            break
    return rows


def _validated_signal_score(row: Dict[str, Any]) -> float:
    score = row.get("trajectory_score") if isinstance(row.get("trajectory_score"), dict) else {}
    raw = score.get("cumulative_validated_delta")
    if raw is None:
        raw = score.get("cumulative_delta")
    try:
        return round(float(raw or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _compact_number(value: float) -> int | float:
    rounded = round(float(value or 0), 2)
    return int(rounded) if rounded.is_integer() else rounded


def _thesis_snapshot(baseline_run: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(baseline_run, dict):
        return {}
    lab_payload = baseline_run.get("lab_payload") if isinstance(baseline_run.get("lab_payload"), dict) else {}
    structured = lab_payload.get("structured_data") if isinstance(lab_payload.get("structured_data"), dict) else {}
    thesis_map = structured.get("thesis_map") if isinstance(structured.get("thesis_map"), dict) else {}
    watchlist = structured.get("monitoring_watchlist") if isinstance(structured.get("monitoring_watchlist"), dict) else {}
    extended = structured.get("extended_analysis") if isinstance(structured.get("extended_analysis"), dict) else {}
    current_state = extended.get("current_thesis_state") if isinstance(extended.get("current_thesis_state"), dict) else {}
    verification = structured.get("verification_queue") if isinstance(structured.get("verification_queue"), list) else []

    scenarios: Dict[str, Any] = {}
    for name in ("bull", "base", "bear"):
        block = thesis_map.get(name) if isinstance(thesis_map.get(name), dict) else {}
        scenarios[name] = {
            "target_12m": block.get("target_12m"),
            "target_24m": block.get("target_24m"),
            "probability_pct": block.get("probability_24m_pct", block.get("probability_pct")),
            "summary": str(block.get("summary") or "").strip(),
            "current_positioning": str(block.get("current_positioning") or "").strip(),
            "why_current_positioning": str(block.get("why_current_positioning") or "").strip(),
            "condition_logic": block.get("condition_logic") if isinstance(block.get("condition_logic"), dict) else {},
            "required_conditions": _snapshot_conditions(block.get("required_conditions")),
            "failure_conditions": _snapshot_conditions(block.get("failure_conditions")),
        }

    return {
        "current_thesis_state": {
            "leaning": str(current_state.get("leaning") or "").strip(),
            "status": str(current_state.get("status") or "").strip(),
            "basis": str(current_state.get("basis") or "").strip(),
        },
        "scenarios": scenarios,
        "monitoring_watchlist": {
            "red_flags": _snapshot_watch_items(watchlist.get("red_flags")),
            "confirmatory_signals": _snapshot_watch_items(watchlist.get("confirmatory_signals")),
        },
        "verification_queue": _snapshot_verification_items(verification),
    }


def _snapshot_conditions(value: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(value, list):
        return rows
    for item in value:
        if isinstance(item, str):
            text = item.strip()
            if not text:
                continue
            rows.append({"condition_id": "", "condition": text, "status": "", "target_period": "", "severity": ""})
            continue
        if not isinstance(item, dict):
            continue
        condition = str(item.get("condition") or item.get("title") or item.get("condition_id") or "").strip()
        if not condition:
            continue
        rows.append(
            {
                "condition_id": str(item.get("condition_id") or item.get("watch_id") or "").strip(),
                "condition": condition,
                "status": str(item.get("status") or "").strip(),
                "target_period": str(item.get("target_period") or item.get("trigger_window") or "").strip(),
                "severity": str(item.get("severity") or "").strip(),
                "source_to_monitor": str(item.get("source_to_monitor") or "").strip(),
            }
        )
    return rows


def _snapshot_watch_items(value: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(value, list):
        return rows
    for item in value:
        if isinstance(item, str):
            text = item.strip()
            if text:
                rows.append({"watch_id": "", "condition": text, "source_to_monitor": "", "severity": ""})
            continue
        if not isinstance(item, dict):
            continue
        condition = str(item.get("condition") or item.get("title") or item.get("watch_id") or "").strip()
        if not condition:
            continue
        rows.append(
            {
                "watch_id": str(item.get("watch_id") or "").strip(),
                "condition": condition,
                "source_to_monitor": str(item.get("source_to_monitor") or "").strip(),
                "trigger_window": str(item.get("trigger_window") or "").strip(),
                "severity": str(item.get("severity") or "").strip(),
            }
        )
    return rows


def _snapshot_verification_items(value: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(value, list):
        return rows
    for item in value[:12]:
        if isinstance(item, str):
            field = item.strip()
            if field:
                rows.append({"field": field, "priority": "", "reason": "", "required_source": ""})
            continue
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or item.get("field_path") or "").strip()
        if not field:
            continue
        rows.append(
            {
                "field": field,
                "priority": str(item.get("priority") or "").strip(),
                "reason": str(item.get("reason") or "").strip(),
                "required_source": str(item.get("required_source") or "").strip(),
            }
        )
    return rows


def _finding_details(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    rows: List[Dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        summary = str(item.get("summary") or "").strip()
        if not summary:
            continue
        rows.append(
            {
                "type": str(item.get("type") or "").strip(),
                "summary": summary,
                "severity": str(item.get("severity") or "").strip(),
            }
        )
        if len(rows) >= 8:
            break
    return rows
