import asyncio

from backend import council
from backend.investment_synthesis import _build_stage3_source_fact_guardrails
from backend.source_fact_context import build_source_fact_context
from test_quality_mvp import _build_stage1_source_evidence_pack


def _sample_stage1_metadata():
    return {
        "per_model_research_runs": [
            {
                "model": "example/model",
                "result": {
                    "stage1_second_pass_source_rows": [
                        {
                            "source_id": "S1",
                            "title": "PFS and Ore Reserve Update",
                            "url": "https://example.com/pfs.pdf",
                            "published_at": "2026-03-13",
                            "decoded": True,
                            "excerpt": "The company completed a PFS, published a maiden ore reserve, and targeted FID in H2 2026.",
                        }
                    ],
                    "stage1_second_pass_mandatory_fact_ledger": {
                        "schema": "stage1_mandatory_source_fact_ledger_v1",
                        "facts": [
                            {
                                "fact_id": "MF1",
                                "family": "resource_reserve",
                                "source_id": "S1",
                                "fact": "Maiden ore reserve was published with the PFS.",
                                "mandatory": True,
                            }
                        ],
                    },
                    "stage1_second_pass_fact_pack": {
                        "schema": "rubric_fact_pack_v1",
                        "sections": {
                            "project_economics": [
                                {
                                    "source_id": "S1",
                                    "fact": "PFS completed with NPV8 of US$1.41bn and IRR of 43%.",
                                }
                            ],
                            "resource_and_reserve": [
                                {
                                    "source_id": "S1",
                                    "fact": "Maiden ore reserve was published with the PFS.",
                                }
                            ],
                        },
                    },
                },
            }
        ]
    }


def test_source_fact_context_renders_second_pass_rows_and_fact_pack():
    rendered = build_source_fact_context(_sample_stage1_metadata())

    assert "PRIMARY/PREPASS SOURCE FACT PACKET" in rendered
    assert "second_pass_source_rows: 1" in rendered
    assert "mandatory_fact_ledger_facts: 1" in rendered
    assert "Mandatory source fact ledger" in rendered
    assert "[S1] PFS and Ore Reserve Update" in rendered
    assert "PFS completed with NPV8 of US$1.41bn and IRR of 43%" in rendered
    assert "Maiden ore reserve was published" in rendered


def test_stage3_source_guardrails_are_not_energy_template_only():
    rendered = _build_stage3_source_fact_guardrails(
        "fallback enhanced context",
        template_id="resources_gold_monometallic",
        evidence_pack=_sample_stage1_metadata(),
    )

    assert "PRIMARY/PREPASS SOURCE FACT PACKET" in rendered
    assert "PFS completed with NPV8 of US$1.41bn" in rendered


def test_stage2_reconciliation_prompt_receives_source_fact_packet(monkeypatch):
    captured = {}

    async def fake_query_model(model, messages, **kwargs):
        captured["prompt"] = messages[0]["content"]
        return {
            "content": (
                '{"status":"no_material_issues","blocking":[],"material":[],'
                '"minor":[],"unresolved":[],"topic_overrides":[],'
                '"stage3_constraints":[],"summary":"ok"}'
            )
        }

    monkeypatch.setattr(council, "query_model", fake_query_model)

    result = asyncio.run(
        council.stage2_collect_reconciliation(
            "Enhanced context only.",
            [{"model": "model-a", "response": "The company lacks verified resource data."}],
            [],
            {},
            reconciliation_model="test/model",
            enabled=True,
            source_evidence_pack=_sample_stage1_metadata(),
        )
    )

    assert result["accepted"] is True
    assert result["source_fact_context_chars"] > 0
    assert "PRIMARY/PREPASS SOURCE FACT PACKET" in captured["prompt"]
    assert "PFS completed with NPV8 of US$1.41bn" in captured["prompt"]


def test_stage2_reconciliation_self_recovers_source_packet_from_stage1_results(monkeypatch):
    captured = {}

    async def fake_query_model(model, messages, **kwargs):
        captured["prompt"] = messages[0]["content"]
        return {
            "content": (
                '{"status":"no_material_issues","blocking":[],"material":[],'
                '"minor":[],"unresolved":[],"topic_overrides":[],'
                '"stage3_constraints":[],"summary":"ok"}'
            )
        }

    monkeypatch.setattr(council, "query_model", fake_query_model)
    stage1_result = dict(_sample_stage1_metadata()["per_model_research_runs"][0]["result"])
    stage1_result["model"] = "model-a"
    stage1_result["response"] = "The company lacks verified resource data."

    result = asyncio.run(
        council.stage2_collect_reconciliation(
            "Enhanced context only.",
            [stage1_result],
            [],
            {},
            reconciliation_model="test/model",
            enabled=True,
        )
    )

    assert result["accepted"] is True
    assert result["source_fact_context_chars"] > 0
    assert "PRIMARY/PREPASS SOURCE FACT PACKET" in captured["prompt"]
    assert "PFS completed with NPV8 of US$1.41bn" in captured["prompt"]


def test_quality_job_evidence_pack_merges_stage1_metadata_for_source_context():
    merged = _build_stage1_source_evidence_pack(
        {"evidence_pack": {"claim_ledger": {"raw_claims": []}}},
        _sample_stage1_metadata(),
    )

    assert "claim_ledger" in merged
    assert "stage1_emulated_metadata" in merged
    assert len(merged["per_model_research_runs"]) == 1

    rendered = build_source_fact_context(merged)
    assert "PRIMARY/PREPASS SOURCE FACT PACKET" in rendered
    assert "PFS completed with NPV8 of US$1.41bn" in rendered
