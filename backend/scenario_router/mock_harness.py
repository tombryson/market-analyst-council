from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from .action_judge import ActionJudge
from .announcement_interpreter import AnnouncementInterpreter
from .models import AnnouncementFacts, BaselineRunPacket, EvidenceRef
from .thesis_comparator import ThesisComparator


def run_mock_router_case(case: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate one mock announcement against one mock thesis map.

    This is the deterministic test harness for the router. It does not fetch
    external data, persist artifacts, or call an LLM. The caller supplies both
    sides of the test: a mock announcement and the saved thesis map it should be
    checked against.
    """

    baseline = build_mock_baseline_run(case)
    facts = build_mock_announcement_facts(case)
    if case.get("use_interpreter", True):
        facts = AnnouncementInterpreter().interpret(facts, baseline)
    report = ThesisComparator().compare(facts, baseline)
    action = ActionJudge().judge(report)
    actual = _actual_result(facts, report, action)
    assertions = _build_assertions(case.get("expected") or {}, actual, report)

    return {
        "status": "ok",
        "mock": True,
        "case_id": str(case.get("case_id") or case.get("id") or "").strip(),
        "label": str(case.get("label") or "").strip(),
        "ticker": facts.ticker,
        "expected": case.get("expected") if isinstance(case.get("expected"), dict) else {},
        "actual": actual,
        "passed": all(item["passed"] for item in assertions) if assertions else None,
        "assertions": assertions,
        "scenario_results": _scenario_results(report),
        "watchlist_results": _watchlist_results(report),
        "announcement_facts": facts.to_dict(),
        "comparison_report": report.to_dict(),
        "action_decision": action.to_dict(),
        "input": {
            "title": facts.title,
            "summary": facts.summary,
            "baseline_path": baseline.lab_payload.get("structured_data", {})
            .get("extended_analysis", {})
            .get("current_thesis_state", {})
            .get("leaning", ""),
            "template_id": baseline.template_id,
        },
    }


def run_mock_router_cases(cases: List[Dict[str, Any]]) -> Dict[str, Any]:
    results = [run_mock_router_case(case) for case in cases if isinstance(case, dict)]
    asserted = [item for item in results if item.get("passed") is not None]
    passed = sum(1 for item in asserted if item.get("passed"))
    return {
        "status": "ok",
        "total_cases": len(results),
        "asserted_cases": len(asserted),
        "passed_cases": passed,
        "failed_cases": max(0, len(asserted) - passed),
        "pass_rate_pct": round((passed / len(asserted)) * 100.0, 1) if asserted else 0.0,
        "results": results,
    }


def build_mock_announcement_facts(case: Dict[str, Any]) -> AnnouncementFacts:
    ticker = _normalize_ticker(case.get("ticker") or "ASX:MOCK")
    title = str(case.get("title") or case.get("announcement_title") or "Mock Announcement").strip()
    body = str(case.get("body_text") or case.get("text") or "").strip()
    summary = str(case.get("summary") or body or title).strip()
    extracted_facts = _string_list(case.get("extracted_facts") or case.get("facts") or [])
    if not extracted_facts and body:
        extracted_facts = _sentences(body)[:8]
    if not extracted_facts and summary and summary != title:
        extracted_facts = [summary]

    raw_text = str(case.get("raw_text_excerpt") or "").strip()
    if not raw_text:
        raw_text = "\n".join([summary] + extracted_facts)
    raw_text = raw_text[:12000]

    source_url = str(case.get("source_url") or "mock://scenario-router/announcement").strip()
    evidence_quotes = _string_list(case.get("evidence_quotes") or [])
    if not evidence_quotes:
        evidence_quotes = extracted_facts[:3] or [summary]
    evidence = [
        EvidenceRef(
            source_url=source_url,
            quote_excerpt=quote,
            source_title=title,
            source_date_utc=str(case.get("published_at_utc") or ""),
        )
        for quote in evidence_quotes[:6]
        if str(quote or "").strip()
    ]

    parse_quality = {
        "decoded_chars": len(raw_text),
        "fact_count": len(extracted_facts),
        "evidence_excerpt_count": len(evidence),
        "reader": "mock_harness",
    }
    market_facts = case.get("market_facts") if isinstance(case.get("market_facts"), dict) else {}
    if market_facts and "normalized_facts" not in market_facts:
        market_facts = {"normalized_facts": market_facts}

    source_confidence = 1.0 if source_url.startswith("https://announcements.asx.com.au/") else 0.8
    extraction_confidence = _mock_extraction_confidence(parse_quality)
    return AnnouncementFacts(
        event_id=str(case.get("event_id") or case.get("case_id") or "mock-announcement").strip(),
        ticker=ticker,
        company_name=str(case.get("company_name") or "Mock Company").strip(),
        title=title,
        summary=summary,
        extracted_facts=extracted_facts,
        material_topics=_string_list(case.get("material_topics") or []),
        market_facts=market_facts,
        evidence=evidence,
        raw_text_excerpt=raw_text,
        parse_quality=parse_quality,
        source_confidence=source_confidence,
        extraction_confidence=extraction_confidence,
        confidence_breakdown={
            "source_confidence": source_confidence,
            "extraction_confidence": extraction_confidence,
            "source": {
                "source_type": str(case.get("source_type") or "mock").strip(),
                "source_url_resolved": bool(source_url),
            },
            "extraction": parse_quality,
        },
    )


def build_mock_baseline_run(case: Dict[str, Any]) -> BaselineRunPacket:
    ticker = _normalize_ticker(case.get("ticker") or "ASX:MOCK")
    baseline_path = str(case.get("baseline_path") or case.get("current_path") or "base").strip().lower() or "base"
    template_id = str(case.get("template_id") or case.get("template_family") or "general").strip() or "general"
    structured = case.get("structured_data") if isinstance(case.get("structured_data"), dict) else {}
    thesis_map = _coerce_thesis_map(
        case.get("thesis_map")
        if isinstance(case.get("thesis_map"), dict)
        else case.get("theses") if isinstance(case.get("theses"), dict) else structured.get("thesis_map")
    )
    if not any(thesis_map.get(name, {}).get("required_conditions") or thesis_map.get(name, {}).get("failure_conditions") for name in ("bull", "base", "bear")):
        thesis_map = _default_thesis_map()
    watchlist = _coerce_watchlist(
        case.get("monitoring_watchlist")
        if isinstance(case.get("monitoring_watchlist"), dict)
        else case.get("watchlist") if isinstance(case.get("watchlist"), dict) else structured.get("monitoring_watchlist")
    )
    verification_queue = _coerce_verification_items(
        case.get("verification_queue")
        if isinstance(case.get("verification_queue"), list)
        else structured.get("verification_queue")
    )
    price_targets = (
        case.get("price_targets")
        if isinstance(case.get("price_targets"), dict)
        else structured.get("price_targets") if isinstance(structured.get("price_targets"), dict) else {}
    )
    market_data = (
        case.get("market_data")
        if isinstance(case.get("market_data"), dict)
        else structured.get("market_data") if isinstance(structured.get("market_data"), dict) else {}
    )
    development_timeline = (
        case.get("development_timeline")
        if isinstance(case.get("development_timeline"), list)
        else structured.get("development_timeline") if isinstance(structured.get("development_timeline"), list) else []
    )
    summary_fields = case.get("summary_fields") if isinstance(case.get("summary_fields"), dict) else {}
    summary_fields = {
        **summary_fields,
        "template_family": summary_fields.get("template_family") or template_id,
        "sector": summary_fields.get("sector") or case.get("sector") or "",
        "industry": summary_fields.get("industry") or case.get("industry") or "",
    }

    lab_payload = {
        "structured_data": {
            "extended_analysis": {
                "current_thesis_state": {
                    "leaning": baseline_path,
                    "status": str(case.get("baseline_status") or "on-track").strip(),
                    "basis": str(case.get("baseline_basis") or "Mock baseline state.").strip(),
                }
            },
            "thesis_map": thesis_map,
            "monitoring_watchlist": watchlist,
            "verification_queue": verification_queue,
            "price_targets": price_targets,
            "market_data": market_data,
            "development_timeline": development_timeline,
        }
    }
    return BaselineRunPacket(
        run_id=str(case.get("run_id") or f"mock-{case.get('case_id') or 'case'}").strip(),
        ticker=ticker,
        exchange=ticker.split(":", 1)[0] if ":" in ticker else str(case.get("exchange") or "ASX").strip(),
        company_name=str(case.get("company_name") or "Mock Company").strip(),
        template_id=template_id,
        freshness_status=str(case.get("freshness_status") or "mock").strip(),
        freshness_age_days=int(case.get("freshness_age_days") or 0),
        summary_fields=summary_fields,
        lab_payload=lab_payload,
        timeline_rows=development_timeline,
    )


def _actual_result(facts: AnnouncementFacts, report, action) -> Dict[str, Any]:
    return {
        "announcement_class": facts.announcement_class,
        "materiality": report.materiality,
        "trajectory_state": report.trajectory_state,
        "trajectory_effect": report.trajectory_effect,
        "current_path": report.current_path,
        "baseline_path": report.baseline_path,
        "path_transition": report.path_transition,
        "impact_level": report.impact_level,
        "action": action.action,
        "thesis_effect": report.thesis_effect,
        "timeline_effect": report.timeline_effect,
        "capital_effect": report.capital_effect,
        "run_validity": report.run_validity,
        "matched_condition_ids": list(report.matched_condition_ids or []),
        "triggered_watchlist_ids": list(report.triggered_watchlist_ids or []),
        "triggered_verification_ids": list(getattr(report, "triggered_verification_ids", []) or []),
        "trajectory_projection": dict(getattr(report, "trajectory_projection", {}) or {}),
        "classification_confidence": report.classification_confidence,
        "thesis_match_confidence": report.thesis_match_confidence,
        "filing_summary": report.filing_summary,
    }


def _build_assertions(expected: Dict[str, Any], actual: Dict[str, Any], report) -> List[Dict[str, Any]]:
    if not isinstance(expected, dict) or not expected:
        return []
    assertions: List[Dict[str, Any]] = []
    scalar_fields = [
        "announcement_class",
        "materiality",
        "trajectory_state",
        "trajectory_effect",
        "current_path",
        "baseline_path",
        "path_transition",
        "impact_level",
        "action",
        "thesis_effect",
        "timeline_effect",
        "capital_effect",
        "run_validity",
    ]
    for field in scalar_fields:
        if field in expected:
            _append_assertion(assertions, field, expected.get(field), actual.get(field))

    for field in ("matched_condition_ids", "triggered_watchlist_ids", "triggered_verification_ids"):
        if field in expected:
            _append_assertion(
                assertions,
                field,
                sorted(_string_list(expected.get(field))),
                sorted(_string_list(actual.get(field))),
            )

    confidence_min = expected.get("confidence_min") if isinstance(expected.get("confidence_min"), dict) else {}
    for field, minimum in confidence_min.items():
        value = actual.get(field)
        passed = _number(value) >= _number(minimum)
        assertions.append({"field": f"confidence_min.{field}", "expected": minimum, "actual": value, "passed": passed})

    condition_statuses = expected.get("condition_statuses") if isinstance(expected.get("condition_statuses"), dict) else {}
    if condition_statuses:
        by_id = {
            str(item.condition_id or "").strip(): str(item.status or "").strip()
            for item in report.condition_evaluations
            if str(item.condition_id or "").strip()
        }
        for condition_id, status in condition_statuses.items():
            _append_assertion(assertions, f"condition_statuses.{condition_id}", status, by_id.get(str(condition_id)))

    scenario_hits = expected.get("scenario_hits") if isinstance(expected.get("scenario_hits"), dict) else {}
    if scenario_hits:
        actual_hits = _scenario_hit_ids(report)
        for scenario, groups in scenario_hits.items():
            if not isinstance(groups, dict):
                continue
            for group, expected_ids in groups.items():
                field = f"scenario_hits.{scenario}.{group}"
                actual_ids = actual_hits.get(str(scenario), {}).get(str(group), [])
                _append_assertion(assertions, field, sorted(_string_list(expected_ids)), sorted(actual_ids))

    return assertions


def _append_assertion(rows: List[Dict[str, Any]], field: str, expected: Any, actual: Any) -> None:
    rows.append({"field": field, "expected": expected, "actual": actual, "passed": expected == actual})


def _scenario_results(report) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        name: {"required": [], "failure": [], "matched_required": [], "matched_failure": []}
        for name in ("bull", "base", "bear")
    }
    for item in report.condition_evaluations or []:
        scenario = str(item.scenario or "").strip().lower()
        group = str(item.group or "").strip().lower()
        if scenario not in out or group not in {"required", "failure"}:
            continue
        row = item.to_dict()
        out[scenario][group].append(row)
        if item.status == "matched":
            out[scenario][f"matched_{group}"].append(item.condition_id)
    return out


def _watchlist_results(report) -> Dict[str, Any]:
    out: Dict[str, Any] = {"red_flag": [], "confirmatory": [], "matched_red_flag": [], "matched_confirmatory": []}
    for item in report.condition_evaluations or []:
        group = str(item.group or "").strip().lower()
        if group not in {"red_flag", "confirmatory"}:
            continue
        out[group].append(item.to_dict())
        if item.status == "matched":
            out[f"matched_{group}"].append(item.condition_id)
    return out


def _scenario_hit_ids(report) -> Dict[str, Dict[str, List[str]]]:
    out: Dict[str, Dict[str, List[str]]] = {
        name: {"required": [], "failure": []}
        for name in ("bull", "base", "bear")
    }
    for item in report.condition_evaluations or []:
        scenario = str(item.scenario or "").strip().lower()
        group = str(item.group or "").strip().lower()
        if scenario in out and group in out[scenario] and item.status == "matched":
            out[scenario][group].append(str(item.condition_id or "").strip())
    return out


def _coerce_thesis_map(value: Any) -> Dict[str, Dict[str, Any]]:
    payload = value if isinstance(value, dict) else {}
    out: Dict[str, Dict[str, Any]] = {}
    for scenario in ("bull", "base", "bear"):
        block = payload.get(scenario) if isinstance(payload.get(scenario), dict) else {}
        out[scenario] = {
            "summary": str(block.get("summary") or "").strip(),
            "required_conditions": _coerce_conditions(block.get("required_conditions") or block.get("required") or []),
            "failure_conditions": _coerce_conditions(block.get("failure_conditions") or block.get("failure") or []),
        }
    return out


def _coerce_watchlist(value: Any) -> Dict[str, List[Any]]:
    payload = value if isinstance(value, dict) else {}
    return {
        "red_flags": _coerce_watch_items(payload.get("red_flags") or payload.get("red_flag") or []),
        "confirmatory_signals": _coerce_watch_items(payload.get("confirmatory_signals") or payload.get("confirmatory") or []),
    }


def _coerce_conditions(value: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, item in enumerate(value if isinstance(value, list) else []):
        if isinstance(item, str):
            text = item.strip()
            if text:
                rows.append({"condition_id": _condition_id(text, idx), "condition": text, "evidence_hooks": [text]})
            continue
        if not isinstance(item, dict):
            continue
        condition = str(item.get("condition") or item.get("title") or item.get("label") or item.get("condition_id") or "").strip()
        if not condition:
            continue
        row = dict(item)
        row["condition"] = condition
        row["condition_id"] = str(row.get("condition_id") or _condition_id(condition, idx)).strip()
        if "evidence_hooks" not in row and item.get("evidence_hook"):
            row["evidence_hooks"] = [str(item.get("evidence_hook"))]
        rows.append(row)
    return rows


def _coerce_watch_items(value: Any) -> List[Any]:
    rows: List[Any] = []
    for idx, item in enumerate(value if isinstance(value, list) else []):
        if isinstance(item, str):
            text = item.strip()
            if text:
                rows.append({"watch_id": _condition_id(text, idx), "condition": text})
            continue
        if isinstance(item, dict):
            condition = str(item.get("condition") or item.get("title") or item.get("label") or item.get("watch_id") or "").strip()
            if condition:
                row = dict(item)
                row["condition"] = condition
                row["watch_id"] = str(row.get("watch_id") or _condition_id(condition, idx)).strip()
                rows.append(row)
    return rows


def _coerce_verification_items(value: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, item in enumerate(value if isinstance(value, list) else []):
        if isinstance(item, str):
            text = item.strip()
            if text:
                rows.append({"verification_id": _condition_id(text, idx), "field": text, "reason": text})
            continue
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or item.get("field_path") or item.get("title") or "").strip()
        reason = str(item.get("reason") or item.get("condition") or "").strip()
        if not field and not reason:
            continue
        row = dict(item)
        row["field"] = field or reason
        row["verification_id"] = str(
            row.get("verification_id")
            or row.get("condition_id")
            or row.get("field_path")
            or _condition_id(row["field"], idx)
        ).strip()
        if "evidence_hooks" not in row and item.get("evidence_hook"):
            row["evidence_hooks"] = [str(item.get("evidence_hook"))]
        rows.append(row)
    return rows


def _default_thesis_map() -> Dict[str, Dict[str, Any]]:
    return {
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
    }


def _mock_extraction_confidence(parse_quality: Dict[str, Any]) -> float:
    decoded = int(parse_quality.get("decoded_chars") or 0)
    facts = int(parse_quality.get("fact_count") or 0)
    if decoded >= 1200:
        base = 0.9
    elif decoded >= 400:
        base = 0.82
    elif decoded > 0:
        base = 0.62
    else:
        base = 0.1
    return round(min(0.98, base + min(0.06, facts * 0.01)), 3)


def _normalize_ticker(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if text else "ASX:MOCK"


def _condition_id(text: str, idx: int) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(text or "").strip().lower()).strip("_")
    return f"mock_{slug[:48] or idx}"


def _sentences(value: str) -> List[str]:
    return [
        text.strip()
        for text in re.split(r"(?<=[.!?])\s+", str(value or ""))
        if len(text.strip()) >= 20
    ]


def _string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _number(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0
