from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from .mock_harness import run_mock_router_case

FIXTURES_DIR = Path(__file__).with_name("test_fixtures")
COMPANY_FIXTURES_DIR = FIXTURES_DIR / "companies"
SUITE_FIXTURES_DIR = FIXTURES_DIR / "suites"


class ScenarioRouterFixtureError(ValueError):
    """Raised when a backwards-designed router fixture is malformed."""


def load_router_fixture_suite(suite_name_or_path: str | Path) -> Dict[str, Any]:
    suite_path = _resolve_suite_path(suite_name_or_path)
    suite = _read_yaml(suite_path)
    company_ref = str(suite.get("company_fixture") or "").strip()
    if not company_ref:
        raise ScenarioRouterFixtureError(f"{suite_path.name} is missing company_fixture.")
    company_path = _resolve_company_path(company_ref, suite_path)
    company = _read_yaml(company_path)
    return compile_router_fixture_suite(suite, company, suite_path=suite_path, company_path=company_path)


def compile_router_fixture_suite(
    suite: Dict[str, Any],
    company: Dict[str, Any],
    *,
    suite_path: Path | None = None,
    company_path: Path | None = None,
) -> Dict[str, Any]:
    _validate_company_fixture(company, company_path=company_path)
    _validate_suite_fixture(suite, suite_path=suite_path)

    suite_id = str(suite.get("id") or (suite_path.stem if suite_path else "router_fixture_suite")).strip()
    company_base = _company_case_base(company)
    cases: List[Dict[str, Any]] = []
    expected_user_buckets: Dict[str, str] = {}

    for announcement in suite.get("announcements") or []:
        ann_id = str(announcement.get("id") or "").strip()
        case = {
            **company_base,
            "case_id": f"{suite_id}__{ann_id}",
            "label": str(announcement.get("label") or ann_id).strip(),
            "title": str(announcement.get("title") or "").strip(),
            "summary": str(announcement.get("summary") or "").strip(),
            "body_text": str(announcement.get("body_text") or "").strip(),
            "extracted_facts": _string_list(announcement.get("extracted_facts") or []),
            "evidence_quotes": _string_list(announcement.get("evidence_quotes") or []),
            "source_url": str(announcement.get("source_url") or "mock://scenario-router/backwards-fixture").strip(),
            "source_type": str(announcement.get("source_type") or "mock").strip(),
            "published_at_utc": str(announcement.get("published_at_utc") or "").strip(),
            "use_legacy_interpreter": bool(
                announcement.get("use_legacy_interpreter", announcement.get("use_interpreter", True))
            ),
            "expected": {
                key: value
                for key, value in dict(announcement.get("expected") or {}).items()
                if key != "user_bucket"
            },
        }
        expected_bucket = str((announcement.get("expected") or {}).get("user_bucket") or "").strip()
        if expected_bucket:
            expected_user_buckets[case["case_id"]] = expected_bucket
        cases.append(case)

    return {
        "suite_id": suite_id,
        "description": str(suite.get("description") or "").strip(),
        "company_id": str((company.get("company") or {}).get("id") or "").strip(),
        "company_path": str(company_path or ""),
        "suite_path": str(suite_path or ""),
        "cases": cases,
        "expected_user_buckets": expected_user_buckets,
    }


def run_router_fixture_suite(suite_name_or_path: str | Path) -> Dict[str, Any]:
    compiled = load_router_fixture_suite(suite_name_or_path)
    results: List[Dict[str, Any]] = []
    expected_buckets = compiled.get("expected_user_buckets") or {}

    for case in compiled.get("cases") or []:
        result = run_mock_router_case(case)
        actual_bucket = _user_bucket(result.get("actual") or {})
        result.setdefault("actual", {})["user_bucket"] = actual_bucket
        expected_bucket = expected_buckets.get(case.get("case_id"))
        if expected_bucket:
            assertion = {
                "field": "user_bucket",
                "expected": expected_bucket,
                "actual": actual_bucket,
                "passed": expected_bucket == actual_bucket,
            }
            result.setdefault("assertions", []).append(assertion)
            result["passed"] = all(item.get("passed") for item in result.get("assertions") or [])
        results.append(result)

    asserted = [item for item in results if item.get("passed") is not None]
    passed = sum(1 for item in asserted if item.get("passed"))
    return {
        "status": "ok",
        "fixture_suite": True,
        "suite_id": compiled.get("suite_id"),
        "company_id": compiled.get("company_id"),
        "total_cases": len(results),
        "asserted_cases": len(asserted),
        "passed_cases": passed,
        "failed_cases": max(0, len(asserted) - passed),
        "pass_rate_pct": round((passed / len(asserted)) * 100.0, 1) if asserted else 0.0,
        "results": results,
    }


def _company_case_base(company_fixture: Dict[str, Any]) -> Dict[str, Any]:
    company = company_fixture.get("company") if isinstance(company_fixture.get("company"), dict) else {}
    current = company_fixture.get("current_thesis") if isinstance(company_fixture.get("current_thesis"), dict) else {}
    catalysts = company_fixture.get("catalysts") if isinstance(company_fixture.get("catalysts"), list) else []
    thesis_map = _compile_thesis_map(catalysts)
    timeline = company_fixture.get("timeline") if isinstance(company_fixture.get("timeline"), list) else []
    watchlist = company_fixture.get("watchlist") if isinstance(company_fixture.get("watchlist"), dict) else {}
    verification_queue = (
        company_fixture.get("verification_queue")
        if isinstance(company_fixture.get("verification_queue"), list)
        else []
    )

    return {
        "ticker": str(company.get("ticker") or "TEST:MOCK").strip(),
        "company_name": str(company.get("name") or "Mock Company").strip(),
        "template_id": str(company.get("template_id") or "general_equity").strip(),
        "baseline_path": str(current.get("path") or "base").strip().lower(),
        "baseline_status": str(current.get("status") or "on-track").strip(),
        "baseline_basis": str(current.get("basis") or "Backwards-designed fixture baseline.").strip(),
        "summary_fields": {
            "template_family": str(company.get("template_id") or "general_equity").strip(),
            "sector": str(company.get("sector") or "").strip(),
            "industry": str(company.get("industry") or "").strip(),
        },
        "thesis_map": thesis_map,
        "monitoring_watchlist": watchlist,
        "verification_queue": verification_queue,
        "development_timeline": timeline,
        "catalyst_rows": _catalyst_rows(catalysts),
        "price_targets": company_fixture.get("price_targets") if isinstance(company_fixture.get("price_targets"), dict) else {},
        "market_data": company_fixture.get("market_data") if isinstance(company_fixture.get("market_data"), dict) else {},
    }


def _compile_thesis_map(catalysts: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {
        scenario: {"summary": "", "required_conditions": [], "failure_conditions": []}
        for scenario in ("bull", "base", "bear")
    }
    for catalyst in catalysts:
        if not isinstance(catalyst, dict):
            continue
        catalyst_id = str(catalyst.get("id") or "").strip()
        linked = [value for value in [catalyst_id, str(catalyst.get("title") or "").strip()] if value]
        for scenario in ("bull", "base", "bear"):
            block = catalyst.get(scenario) if isinstance(catalyst.get(scenario), dict) else {}
            for source_key, target_key in (("required", "required_conditions"), ("failure", "failure_conditions")):
                for condition in block.get(source_key) or []:
                    row = _condition_row(
                        condition,
                        scenario=scenario,
                        group=source_key,
                        catalyst_id=catalyst_id,
                        linked_milestones=linked,
                    )
                    out[scenario][target_key].append(row)
    return out


def _condition_row(
    condition: Dict[str, Any],
    *,
    scenario: str,
    group: str,
    catalyst_id: str,
    linked_milestones: List[str],
) -> Dict[str, Any]:
    condition_id = str(condition.get("id") or condition.get("condition_id") or "").strip()
    if not condition_id:
        condition_id = f"{catalyst_id}_{scenario}_{group}_{len(linked_milestones)}"
    hooks = _string_list(condition.get("evidence_hooks") or condition.get("evidence_hook") or [])
    label = str(condition.get("condition") or condition.get("title") or "").strip()
    row = {
        "condition_id": condition_id,
        "condition": label,
        "evidence_hooks": hooks or [label],
        "severity": str(condition.get("severity") or ("high" if scenario == "bear" or group == "failure" else "medium")).strip(),
        "linked_milestones": linked_milestones,
    }
    if condition.get("driver"):
        row["driver"] = str(condition.get("driver") or "").strip()
    return row


def _catalyst_rows(catalysts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for catalyst in catalysts:
        if not isinstance(catalyst, dict):
            continue
        title = str(catalyst.get("title") or catalyst.get("id") or "").strip()
        if not title:
            continue
        rows.append(
            {
                "title": title,
                "target_period": str(catalyst.get("target_period") or "").strip(),
                "status": str(catalyst.get("status") or "planned").strip(),
            }
        )
    return rows


def _validate_company_fixture(company: Dict[str, Any], *, company_path: Path | None = None) -> None:
    label = str(company_path or "company fixture")
    company_block = company.get("company") if isinstance(company.get("company"), dict) else {}
    if not str(company_block.get("ticker") or "").strip():
        raise ScenarioRouterFixtureError(f"{label} is missing company.ticker.")
    catalysts = company.get("catalysts") if isinstance(company.get("catalysts"), list) else []
    if not catalysts:
        raise ScenarioRouterFixtureError(f"{label} must define at least one catalyst.")
    for catalyst in catalysts:
        catalyst_id = str((catalyst or {}).get("id") or "").strip()
        if not catalyst_id:
            raise ScenarioRouterFixtureError(f"{label} has a catalyst without id.")
        for scenario in ("bull", "base", "bear"):
            block = catalyst.get(scenario) if isinstance(catalyst.get(scenario), dict) else {}
            for group in ("required", "failure"):
                for condition in block.get(group) or []:
                    if not str(condition.get("condition") or "").strip():
                        raise ScenarioRouterFixtureError(f"{label} {catalyst_id}.{scenario}.{group} has a condition without text.")
                    if not _string_list(condition.get("evidence_hooks") or []):
                        raise ScenarioRouterFixtureError(f"{label} {catalyst_id}.{scenario}.{group} is missing evidence_hooks.")


def _validate_suite_fixture(suite: Dict[str, Any], *, suite_path: Path | None = None) -> None:
    label = str(suite_path or "suite fixture")
    announcements = suite.get("announcements") if isinstance(suite.get("announcements"), list) else []
    if not announcements:
        raise ScenarioRouterFixtureError(f"{label} must define announcements.")
    for announcement in announcements:
        ann_id = str((announcement or {}).get("id") or "").strip()
        if not ann_id:
            raise ScenarioRouterFixtureError(f"{label} has an announcement without id.")
        if not str(announcement.get("title") or "").strip():
            raise ScenarioRouterFixtureError(f"{label} {ann_id} is missing title.")
        expected = announcement.get("expected") if isinstance(announcement.get("expected"), dict) else {}
        for required in ("current_path", "trajectory_state", "action", "user_bucket"):
            if required not in expected:
                raise ScenarioRouterFixtureError(f"{label} {ann_id} expected is missing {required}.")


def _user_bucket(actual: Dict[str, Any]) -> str:
    state = str(actual.get("trajectory_state") or "").strip().lower()
    action = str(actual.get("action") or "").strip().lower()
    if state in {"needs_classification", "material_unmapped", "thesis_weakened", "timeline_delayed", "risk_increased"}:
        return "needs_review"
    if actual.get("triggered_verification_ids"):
        return "needs_review"
    if state in {"thesis_strengthened", "timeline_accelerated", "risk_reduced"}:
        return "thesis_moving"
    if state in {"market_backdrop_only", "no_thesis_change"}:
        return "no_thesis_change"
    if state == "administrative_filing":
        return "administrative"
    if action in {"full_rerun", "rerun_stage1", "run_delta_only", "urgent_human_review"}:
        return "needs_review"
    return "all"


def _resolve_suite_path(value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    if path.suffix:
        return (SUITE_FIXTURES_DIR / path).resolve()
    return (SUITE_FIXTURES_DIR / f"{path}.yaml").resolve()


def _resolve_company_path(value: str, suite_path: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] in {"..", "."}:
        return (suite_path.parent / path).resolve()
    return (COMPANY_FIXTURES_DIR / path).resolve()


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ScenarioRouterFixtureError(f"Fixture file not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ScenarioRouterFixtureError(f"Fixture must be a mapping: {path}")
    return payload


def _string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item or "").strip() for item in value if str(item or "").strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []
