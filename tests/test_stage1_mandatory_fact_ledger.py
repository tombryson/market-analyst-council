import json

from backend.council import (
    _build_stage1_mandatory_fact_ledger,
    _build_stage1_second_pass_prompt,
    _validate_stage1_prompt_mandatory_fact_coverage,
)


def _vmm_like_source_rows():
    return [
        {
            "source_id": "S4",
            "title": "COLOSSUS IONIC CLAY PROJECT",
            "url": "https://viridismining.com.au/presentation.pdf",
            "published_at": "2026-01-01",
            "decoded": True,
            "material_signal_score": 7,
            "excerpt": (
                "The presentation presents strong project economics from a July 2025 PFS "
                "(NPV8 US$1.41bn pre-tax, IRR 43%, CAPEX US$358m incl. 25% contingency). "
                "The project has a large high-grade resource of 493Mt @ 2,508ppm TREO, "
                "601ppm MREO; M+I 329Mt @ 659ppm MREO and a 20-year base case Life of Mine."
            ),
        },
        {
            "source_id": "S7",
            "title": "Viridis Completes Key Infill Drilling Milestone",
            "url": "https://announcements.asx.com.au/asxpdf/20260515/pdf/example.pdf",
            "published_at": "2026-05-15",
            "decoded": True,
            "asx_price_sensitive": True,
            "material_signal_score": 9,
            "excerpt": (
                "The company reports a Maiden Ore Reserve of 200.6 Mt at 2,640 ppm TREO "
                "(100% Probable, mining recovery 95%, 1,000 ppm TREO cut-off, 5% dilution) "
                "and a Global Resource of 493 Mt at 2,508 ppm TREO."
            ),
        },
        {
            "source_id": "S13",
            "title": "Investor Presentation",
            "url": "https://announcements.asx.com.au/asxpdf/20240813/pdf/example.pdf",
            "published_at": "2025-08-13",
            "decoded": True,
            "material_signal_score": 5,
            "excerpt": (
                "Metallurgical testwork returned ANSTO 11.9m at 80% Nd-Pr recovery and "
                "bulk composite ionic recoveries across concessions with Nd+Pr averaging "
                "59-67% and Dy+Tb 49-65%."
            ),
        },
    ]


def test_mandatory_fact_ledger_extracts_resource_reserve_and_recovery_facts():
    ledger = _build_stage1_mandatory_fact_ledger(
        _vmm_like_source_rows(),
        template_id="rare_earths_critical_minerals",
    )

    facts = " ".join(row["fact"] for row in ledger["facts"])

    assert ledger["counts"]["fact_count"] >= 3
    assert "493Mt @ 2,508ppm TREO" in facts or "493 Mt at 2,508 ppm TREO" in facts
    assert "200.6 Mt at 2,640 ppm TREO" in facts
    assert "95%" in facts
    assert "59-67%" in facts


def test_mandatory_ledger_survives_prompt_when_compact_bundle_drops_resource_row():
    ledger = _build_stage1_mandatory_fact_ledger(
        _vmm_like_source_rows(),
        template_id="rare_earths_critical_minerals",
    )
    compact_bundle_missing_resource = {
        "schema": "compact_fact_bundle_v1",
        "categories": {
            "project_economics_resource": [
                {
                    "source_id": "S4",
                    "fact": (
                        "The presentation presents strong project economics from a July "
                        "2025 PFS (NPV8 US$1.41bn pre-tax, IRR 43%, CAPEX US$358m incl."
                    ),
                }
            ]
        },
        "critical_gaps": [],
        "counts": {"total_facts": 1},
    }

    prompt = _build_stage1_second_pass_prompt(
        user_query="Run an investment analysis on ASX:VMM.",
        research_brief="Rare earths and critical minerals rubric.",
        run={"model": "test/model", "ticker": "ASX:VMM", "depth": "deep"},
        mandatory_fact_ledger_json=json.dumps(ledger, ensure_ascii=True, separators=(",", ":")),
        source_key_points_json="",
        supplementary_macro_news_json="",
        compact_fact_bundle_json=json.dumps(
            compact_bundle_missing_resource,
            ensure_ascii=True,
            separators=(",", ":"),
        ),
        fact_digest_json='{"schema":"fact_digest_v2","counts":{}}',
        fact_pack_json='{"schema":"rubric_fact_pack_v1","counts":{}}',
        evidence_appendix="(omitted due to prompt budget; represented in Source Key Points)",
        timeline_digest="",
        cashflow_schema_contract="",
    )

    assert "MANDATORY_SOURCE_FACT_LEDGER_JSON" in prompt
    assert "493Mt @ 2,508ppm TREO" in prompt or "493 Mt at 2,508 ppm TREO" in prompt
    assert "200.6 Mt at 2,640 ppm TREO" in prompt
    assert _validate_stage1_prompt_mandatory_fact_coverage(prompt, ledger)["passed"] is True


def test_prompt_coverage_validator_fails_when_ledger_facts_are_missing():
    ledger = _build_stage1_mandatory_fact_ledger(
        _vmm_like_source_rows(),
        template_id="rare_earths_critical_minerals",
    )
    prompt_without_ledger = "COMPACT_FACT_BUNDLE_JSON only includes PFS NPV and capex."

    coverage = _validate_stage1_prompt_mandatory_fact_coverage(
        prompt_without_ledger,
        ledger,
    )

    assert coverage["passed"] is False
    assert coverage["missing_fact_ids"]
