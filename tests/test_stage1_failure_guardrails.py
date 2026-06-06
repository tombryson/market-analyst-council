import json

from backend.council import _validate_stage1_prompt_mandatory_fact_coverage
from test_quality_mvp import _write_failure_artifact


def test_mandatory_fact_coverage_accepts_json_escaped_unicode() -> None:
    fact = (
        "Metallurgy indicates non-refractory ore with historical/previous "
        "test recoveries \u224888\u201391% used in studies."
    )
    ledger = {
        "facts": [
            {
                "fact_id": "MF4",
                "family": "metallurgy_recovery",
                "source_id": "S2",
                "fact": fact,
                "mandatory": True,
            }
        ]
    }
    prompt = json.dumps({"mandatory_fact_ledger": ledger}, ensure_ascii=True)

    coverage = _validate_stage1_prompt_mandatory_fact_coverage(prompt, ledger)

    assert coverage["passed"] is True
    assert coverage["missing_fact_ids"] == []


def test_mandatory_fact_coverage_still_fails_when_fact_missing() -> None:
    ledger = {
        "facts": [
            {
                "fact_id": "MF1",
                "family": "resource_reserve",
                "source_id": "S1",
                "fact": "Updated Mt York MRE: 61.7 Mt @ 1.05 g/t Au = 2.08 Moz.",
                "mandatory": True,
            }
        ]
    }

    coverage = _validate_stage1_prompt_mandatory_fact_coverage("No resource fact here.", ledger)

    assert coverage["passed"] is False
    assert coverage["missing_fact_ids"] == ["MF1"]


def test_failure_artifact_marks_run_failed(tmp_path) -> None:
    output_path = tmp_path / "quality_job.json"

    written = _write_failure_artifact(
        str(output_path),
        failure_stage="stage1_audit_gate",
        failure_reason="no_stage1_responses_passed_audit_gate",
        payload={"stage1_results": []},
    )

    assert written == output_path.resolve()
    payload = json.loads(output_path.read_text())
    assert payload["status"] == "failed"
    assert payload["failure_stage"] == "stage1_audit_gate"
    assert payload["failure_reason"] == "no_stage1_responses_passed_audit_gate"
    assert payload["stage1_results"] == []
