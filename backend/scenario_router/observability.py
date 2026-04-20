from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from .action_judge import ActionJudge
from .lab_scribe import SCENARIO_ROUTER_EVENTS_DIR
from .models import AnnouncementFacts, BaselineRunPacket, EvidenceRef
from .thesis_comparator import ThesisComparator

EVALUATION_CASES_PATH = Path(__file__).with_name("evaluation_cases.json")


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
            if wanted and str(row.get("ticker") or "").strip().upper() != wanted:
                continue
            rows.append(row)

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

    def run_evaluation_suite(self) -> Dict[str, Any]:
        cases = self._load_evaluation_cases()
        comparator = ThesisComparator()
        judge = ActionJudge()
        results: List[Dict[str, Any]] = []

        for case in cases:
            baseline = _build_baseline_run(case)
            facts = AnnouncementFacts(
                event_id=str(case.get("case_id") or ""),
                ticker=str(case.get("ticker") or ""),
                company_name="Scenario Router Fixture Co",
                title=str(case.get("title") or ""),
                summary=str(case.get("summary") or ""),
                extracted_facts=[str(item or "") for item in (case.get("extracted_facts") or [])],
                material_topics=[str(item or "") for item in (case.get("material_topics") or [])],
                evidence=[EvidenceRef(source_url="https://announcements.asx.com.au/asxpdf/example.pdf")],
                raw_text_excerpt="\n".join([str(case.get("summary") or "")] + [str(item or "") for item in (case.get("extracted_facts") or [])]),
            )
            report = comparator.compare(facts, baseline)
            action = judge.judge(report)
            expected = case.get("expected") or {}
            pass_current_path = str(report.current_path or "") == str(expected.get("current_path") or "")
            pass_action = str(action.action or "") == str(expected.get("action") or "")
            pass_impact = str(report.impact_level or "") == str(expected.get("impact_level") or "")
            passed = pass_current_path and pass_action and pass_impact

            results.append(
                {
                    "case_id": str(case.get("case_id") or ""),
                    "category": str(case.get("category") or ""),
                    "label": str(case.get("label") or ""),
                    "expected": expected,
                    "actual": {
                        "current_path": report.current_path,
                        "action": action.action,
                        "impact_level": report.impact_level,
                    },
                    "passed": passed,
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
            if child.name in {"by_run", "dedupe"}:
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
        report = payload.get("comparison_report") if isinstance(payload.get("comparison_report"), dict) else {}
        action = payload.get("action_decision") if isinstance(payload.get("action_decision"), dict) else {}
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
            and str(item.get("status") or "").strip() == "matched"
            and str(item.get("group") or "").strip() in {"red_flag", "confirmatory"}
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
        suppress_stale_market_only_reroute = (
            matched_conditions == 0
            and triggered_watchlist == 0
            and market_conditions > 0
            and raw_action in {"full_rerun", "rerun_stage1", "run_delta_only"}
        )

        return {
            "status": str(payload.get("status") or "ok").strip() or "ok",
            "event_id": str(event.get("event_id") or packet.get("event_id") or path.stem).strip(),
            "ticker": str(event.get("ticker") or packet.get("ticker") or "").strip(),
            "title": str(packet.get("title") or report.get("announcement_title") or "").strip(),
            "company_name": str(packet.get("company_name") or "").strip(),
            "saved_at_utc": str(payload.get("saved_at_utc") or "").strip(),
            "received_at_utc": str(event.get("received_at_utc") or "").strip(),
            "action": "watch" if suppress_stale_market_only_reroute else raw_action,
            "impact_level": "low" if suppress_stale_market_only_reroute else str(report.get("impact_level") or "").strip(),
            "current_path": (raw_baseline_path or raw_current_path) if suppress_stale_market_only_reroute else raw_current_path,
            "baseline_path": raw_baseline_path,
            "path_transition": "" if suppress_stale_market_only_reroute else str(report.get("path_transition") or "").strip(),
            "source_type": str(packet.get("source_type") or "").strip(),
            "source_url": str(packet.get("source_url") or "").strip(),
            "run_id": str(baseline_run.get("run_id") or "").strip(),
            "processing_duration_ms": int(payload.get("processing_duration_ms") or 0),
            "matched_conditions_count": matched_conditions,
            "triggered_watchlist_count": triggered_watchlist,
            "market_conditions_count": market_conditions,
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


def _build_baseline_run(case: Dict[str, Any]) -> BaselineRunPacket:
    baseline_path = str(case.get("baseline_path") or "base").strip().lower() or "base"
    ticker = str(case.get("ticker") or "ASX:TEST").strip().upper()
    return BaselineRunPacket(
        run_id=f"fixture-{case.get('case_id')}",
        ticker=ticker,
        exchange=ticker.split(":", 1)[0] if ":" in ticker else "ASX",
        company_name="Scenario Router Fixture Co",
        template_id="resources_gold_monometallic",
        freshness_status="watch",
        freshness_age_days=2,
        lab_payload={
            "structured_data": {
                "extended_analysis": {
                    "current_thesis_state": {
                        "leaning": baseline_path,
                        "status": "on-track",
                        "basis": "Fixture baseline state.",
                    }
                },
                "thesis_map": {
                    "bull": {
                        "required_conditions": [
                            {
                                "condition_id": "bull_permit_fast",
                                "condition": "Permitting approvals arrive ahead of plan",
                                "evidence_hooks": ["permit approval ahead of schedule"],
                                "linked_milestones": ["permit approval"],
                            },
                            {
                                "condition_id": "bull_milestone_fast",
                                "condition": "Project milestone is achieved ahead of schedule",
                                "evidence_hooks": ["milestone was achieved ahead of schedule"],
                                "linked_milestones": ["project milestone"],
                            },
                            {
                                "condition_id": "bull_funding_secure",
                                "condition": "Funding remains sufficient for planned milestones",
                                "evidence_hooks": ["funding remains sufficient"],
                                "linked_milestones": ["funding"],
                            },
                        ],
                        "failure_conditions": [],
                    },
                    "base": {
                        "required_conditions": [
                            {
                                "condition_id": "base_funding_secure",
                                "condition": "Funding remains sufficient for planned milestones",
                                "evidence_hooks": ["funding remains sufficient"],
                                "linked_milestones": ["funding"],
                            }
                        ],
                        "failure_conditions": [
                            {
                                "condition_id": "base_funding_break",
                                "condition": "Funding pathway breaks before key milestones",
                                "evidence_hooks": ["funding shortfall", "capital raise under pressure"],
                                "linked_milestones": ["funding"],
                            }
                        ],
                    },
                    "bear": {
                        "required_conditions": [
                            {
                                "condition_id": "bear_delay_and_shortfall",
                                "condition": "Project delays and funding shortfall emerge",
                                "evidence_hooks": ["delay", "funding shortfall"],
                                "linked_milestones": ["project timeline"],
                            },
                            {
                                "condition_id": "bear_permit_withdrawn",
                                "condition": "Permit approval is withdrawn",
                                "evidence_hooks": ["permit approval was withdrawn"],
                                "linked_milestones": ["permit approval"],
                            },
                        ],
                        "failure_conditions": [],
                    },
                },
            }
        },
    )
