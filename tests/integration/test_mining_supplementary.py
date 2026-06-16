import unittest

from backend.research.mining_supplementary import (
    apply_contamination_review,
    apply_deterministic_adjudication,
    build_contamination_review_prompt,
    canonicalize_checklist_name,
    checklist_map,
    flatten_packet_rows,
    merge_segment_outputs,
    resolve_mining_enricher_context,
    segment_repairs_for_missing_items,
)


class MiningSupplementaryTests(unittest.TestCase):
    def test_canonicalize_checklist_aliases(self):
        self.assertEqual(
            canonicalize_checklist_name(
                "power supply arrangement: named contract type (boo/bot/ppa), named counterparty, and whether it eliminates a capex line"
            ),
            "Power supply arrangement: named contract type (BOO/BOT/PPA), named counterparty, and whether it eliminates a capex line",
        )

    def test_merge_segment_outputs_prefers_found_and_dedupes_facts(self):
        segment_a = {
            "checklist_results": {
                "items": [
                    {
                        "checklist_item": "Water supply arrangement: named source, permit reference, named contractor if any",
                        "status": "not_found",
                        "category_populated": None,
                        "not_found_reason": "Not found in this slice.",
                    }
                ]
            },
            "infrastructure_and_project_structure": [
                {
                    "fact": "Water supply via raw-water pipeline from named utility.",
                    "capex_impact": "reduces camp services",
                    "date": "2025-01",
                    "source": "Company presentation",
                    "filing_ref": "not yet cross-checked",
                    "confidence": "SECONDARY",
                    "corroborated_by": 1,
                    "conflict": False,
                    "conflict_detail": None,
                }
            ],
        }
        segment_b = {
            "checklist_results": {
                "items": [
                    {
                        "checklist_item": "Water supply arrangement: named source, permit reference, named contractor if any",
                        "status": "found",
                        "category_populated": "infrastructure_and_project_structure",
                        "not_found_reason": None,
                    }
                ]
            },
            "infrastructure_and_project_structure": [
                {
                    "fact": "Water supply via raw-water pipeline from named utility.",
                    "capex_impact": "reduces camp services",
                    "date": "2025-01",
                    "source": "Company presentation",
                    "filing_ref": "not yet cross-checked",
                    "confidence": "SECONDARY",
                    "corroborated_by": 1,
                    "conflict": False,
                    "conflict_detail": None,
                }
            ],
        }
        merged = merge_segment_outputs(
            company="Brightstar Resources Limited",
            ticker="BTR",
            exchange="ASX",
            commodity="gold",
            discovery_json={"priority_sources": [{"url": "https://example.com"}]},
            segment_outputs=[segment_a, segment_b],
        )
        ck = checklist_map(merged)
        self.assertEqual(
            ck["Water supply arrangement: named source, permit reference, named contractor if any"],
            "found",
        )
        self.assertEqual(len(merged["infrastructure_and_project_structure"]), 1)

    def test_adjudication_marks_water_brokers_and_overlap(self):
        payload = {
            "company": "Brightstar Resources Limited",
            "checklist_results": {"items": []},
            "infrastructure_and_project_structure": [
                {
                    "fact": "Potable water pipeline installed from named utility source.",
                    "capex_impact": "reduces camp services",
                    "date": "2025-01",
                    "source": "Project update",
                }
            ],
            "named_advisors_and_counterparties": [
                {
                    "fact": "Canaccord Genuity acted as placement manager.",
                    "role": "placement manager",
                    "date": "2025-02",
                    "source": "Placement announcement",
                }
            ],
            "broker_and_analyst_references": [
                {
                    "fact": "Canaccord Genuity maintained Buy with A$0.80 target.",
                    "broker": "Canaccord Genuity",
                    "target": "A$0.80",
                    "basis": "NAV",
                    "date": "2025-03",
                    "source": "Canaccord research",
                }
            ],
        }
        adjudicated, meta = apply_deterministic_adjudication(payload)
        ck = checklist_map(adjudicated)
        self.assertEqual(
            ck["Water supply arrangement: named source, permit reference, named contractor if any"],
            "found",
        )
        self.assertEqual(
            ck["All named brokers with stated price targets and ratings, including initiation and all subsequent revisions with dates"],
            "found",
        )
        self.assertEqual(
            ck["For any named party in the placement manager or advisor sections: check whether that same firm also published research coverage and extract any stated target if found"],
            "found",
        )
        self.assertGreaterEqual(meta["rule_count"], 3)

    def test_adjudication_downgrades_invalid_self_referential_benchmark(self):
        payload = {
            "company": "West Wits Mining Limited",
            "checklist_results": {
                "items": [
                    {
                        "checklist_item": "Named transactions used to benchmark EV/oz or premium to NAV",
                        "status": "found",
                        "category_populated": "peer_and_ma_comparables",
                        "not_found_reason": None,
                    }
                ]
            },
            "peer_and_ma_comparables": [
                {
                    "fact": "West Wits trades at EV/oz discount to peers.",
                    "acquirer": "West Wits Mining",
                    "target": "West Wits Mining",
                    "exchange_jurisdiction": "South Africa",
                    "metric": "EV/oz",
                    "date": "2025-01",
                    "source": "Company presentation",
                }
            ],
        }
        adjudicated, _ = apply_deterministic_adjudication(payload)
        ck = checklist_map(adjudicated)
        self.assertEqual(
            ck["Named transactions used to benchmark EV/oz or premium to NAV"],
            "not_found",
        )

    def test_segment_repairs_groups_missing_items_by_segment(self):
        repairs = segment_repairs_for_missing_items(
            [
                "Water supply arrangement: named source, permit reference, named contractor if any",
                "All named brokers with stated price targets and ratings, including initiation and all subsequent revisions with dates",
            ]
        )
        repair_names = {entry["name"] for entry in repairs}
        self.assertIn("core_finance_ops", repair_names)
        self.assertIn("technical_broker_misc", repair_names)

    def test_resolve_mining_enricher_context_uses_canonical_assignment(self):
        resolved = resolve_mining_enricher_context(
            user_query="Run full analysis on Brightstar Resources",
            ticker="ASX:BTR",
        )
        self.assertEqual(resolved["exchange"], "ASX")
        self.assertEqual(resolved["ticker_symbol"], "BTR")
        self.assertEqual(resolved["display_ticker"], "ASX:BTR")
        self.assertEqual(resolved["company"], "Brightstar Resources Limited")
        self.assertEqual(resolved["commodity"], "gold")

    def test_resolve_mining_enricher_context_handles_bare_ticker_with_exchange(self):
        resolved = resolve_mining_enricher_context(
            user_query="West Wits Mining",
            ticker="WWI",
            exchange="ASX",
        )
        self.assertEqual(resolved["exchange"], "ASX")
        self.assertEqual(resolved["ticker_symbol"], "WWI")
        self.assertEqual(resolved["company"], "West Wits Mining Limited")
        self.assertEqual(resolved["commodity"], "gold")

    def test_resolve_mining_enricher_context_does_not_default_unknowns_to_gold(self):
        resolved = resolve_mining_enricher_context(
            user_query="Run full analysis on Example Holdings",
            ticker="NASDAQ:EXMP",
            company="Example Holdings Inc.",
            exchange="NASDAQ",
        )
        self.assertEqual(resolved["exchange"], "NASDAQ")
        self.assertEqual(resolved["ticker_symbol"], "EXMP")
        self.assertEqual(resolved["company"], "Example Holdings Inc.")
        self.assertEqual(resolved["commodity"], "")

    def test_contamination_prompt_omits_commodity_header(self):
        prompt = build_contamination_review_prompt(
            company="Brightstar Resources Limited",
            ticker="ASX:BTR",
            exchange="ASX",
            packet_rows=[
                {
                    "row_id": "R1",
                    "category": "named_advisors_and_counterparties",
                    "fact": "Taurus provided the debt facility.",
                    "source": "ASX announcement",
                    "filing_ref": "ASX",
                    "confidence": "PRIMARY",
                }
            ],
        )
        self.assertNotIn("COMMODITY:", prompt)

    def test_apply_contamination_review_drops_flagged_row_only(self):
        payload = {
            "asset_class": "mining",
            "exchange": "ASX",
            "ticker": "ASX:BTR",
            "company": "Brightstar Resources Limited",
            "checklist_results": {
                "items": [
                    {
                        "checklist_item": "Named debt advisor or financial advisor for project financing",
                        "status": "found",
                        "category_populated": "named_advisors_and_counterparties",
                        "not_found_reason": None,
                    },
                    {
                        "checklist_item": "All named brokers with stated price targets and ratings, including initiation and all subsequent revisions with dates",
                        "status": "found",
                        "category_populated": "broker_and_analyst_references",
                        "not_found_reason": None,
                    },
                ]
            },
            "named_advisors_and_counterparties": [
                {
                    "fact": "Taurus provided the debt facility.",
                    "role": "debt provider",
                    "date": "2025-01",
                    "source": "ASX announcement",
                    "filing_ref": "ASX",
                    "confidence": "PRIMARY",
                }
            ],
            "broker_and_analyst_references": [
                {
                    "fact": "Northern Star upgraded target on NST to A$20.00.",
                    "broker": "Example Broker",
                    "target": "A$20.00",
                    "basis": "not disclosed",
                    "date": "2025-02",
                    "source": "Wrong-company note",
                    "confidence": "SECONDARY",
                }
            ],
        }
        rows = flatten_packet_rows(payload)
        review = {
            "packet_decision": "keep",
            "packet_confidence_pct": 99,
            "packet_reason": "One row is clearly about a different issuer.",
            "wrong_entity_detected": None,
            "row_decisions": [
                {
                    "row_id": rows[0]["row_id"],
                    "decision": "keep",
                    "confidence_pct": 99,
                    "reason": "Matches target issuer context.",
                    "wrong_entity_detected": None,
                },
                {
                    "row_id": rows[1]["row_id"],
                    "decision": "drop_row",
                    "confidence_pct": 99,
                    "reason": "Explicitly about Northern Star / NST.",
                    "wrong_entity_detected": "Northern Star Resources",
                },
            ],
        }
        adjusted, meta = apply_contamination_review(payload, review, min_confidence_pct=95)
        self.assertEqual(len(adjusted["named_advisors_and_counterparties"]), 1)
        self.assertNotIn("broker_and_analyst_references", adjusted)
        ck = checklist_map(adjusted)
        self.assertEqual(
            ck["All named brokers with stated price targets and ratings, including initiation and all subsequent revisions with dates"],
            "not_found",
        )
        self.assertEqual(meta["dropped_row_count"], 1)
        self.assertFalse(meta["drop_packet_applied"])

    def test_apply_contamination_review_can_drop_entire_packet(self):
        payload = {
            "asset_class": "mining",
            "exchange": "ASX",
            "ticker": "ASX:BTR",
            "company": "Brightstar Resources Limited",
            "warning": "Supplementary facts only.",
            "checklist_results": {"items": []},
            "named_advisors_and_counterparties": [
                {
                    "fact": "West Wits secured debt package.",
                    "role": "debt provider",
                    "date": "2025-01",
                    "source": "Wrong-company note",
                    "filing_ref": "not yet cross-checked",
                    "confidence": "SECONDARY",
                }
            ],
        }
        adjusted, meta = apply_contamination_review(
            payload,
            {
                "packet_decision": "drop_packet",
                "packet_confidence_pct": 99,
                "packet_reason": "Packet is clearly about West Wits Mining, not Brightstar.",
                "wrong_entity_detected": "West Wits Mining Limited",
                "row_decisions": [],
            },
            min_confidence_pct=95,
        )
        self.assertTrue(meta["drop_packet_applied"])
        self.assertNotIn("named_advisors_and_counterparties", adjusted)
        self.assertEqual(len(adjusted["checklist_results"]["items"]), 32)
        self.assertIn("Contamination guard dropped the packet", adjusted["warning"])

    def test_btr_packet_with_injected_wwi_row_drops_only_wrong_row(self):
        payload = {
            "asset_class": "mining",
            "exchange": "ASX",
            "ticker": "ASX:BTR",
            "company": "Brightstar Resources Limited",
            "warning": "Supplementary facts only.",
            "checklist_results": {
                "items": [
                    {
                        "checklist_item": "Project finance debt sizing assumptions from named advisors or lenders (not analyst estimates)",
                        "status": "found",
                        "category_populated": "tax_and_financial_structure",
                        "not_found_reason": None,
                    },
                    {
                        "checklist_item": "Named debt advisor or financial advisor for project financing",
                        "status": "found",
                        "category_populated": "named_advisors_and_counterparties",
                        "not_found_reason": None,
                    },
                    {
                        "checklist_item": "Processing infrastructure: any toll milling, shared plant, or existing facility arrangement with named counterparty",
                        "status": "found",
                        "category_populated": "infrastructure_and_project_structure",
                        "not_found_reason": None,
                    },
                ]
            },
            "tax_and_financial_structure": [
                {
                    "fact": "Brightstar secured a US$120m senior secured bond facility.",
                    "tax_regime": "Australia",
                    "quantum": "US$120m",
                    "trigger": None,
                    "date": "2025-01",
                    "source": "ASX funding announcement",
                    "filing_ref": "ASX announcement",
                    "confidence": "PRIMARY",
                    "corroborated_by": 1,
                    "conflict": False,
                    "conflict_detail": None,
                }
            ],
            "named_advisors_and_counterparties": [
                {
                    "fact": "Taurus acted as lender for the Brightstar bond package.",
                    "role": "lender",
                    "date": "2025-01",
                    "source": "ASX funding announcement",
                    "filing_ref": "ASX announcement",
                    "confidence": "PRIMARY",
                    "corroborated_by": 1,
                    "conflict": False,
                    "conflict_detail": None,
                },
                {
                    "fact": "West Wits Mining secured IDC and Absa debt support for Qala Shallows.",
                    "role": "lender package",
                    "date": "2025-02",
                    "source": "WWI financing note",
                    "filing_ref": "not yet cross-checked",
                    "confidence": "SECONDARY",
                    "corroborated_by": 1,
                    "conflict": False,
                    "conflict_detail": None,
                },
            ],
            "infrastructure_and_project_structure": [
                {
                    "fact": "Genesis Minerals provides Laverton mill processing capacity for ore treatment.",
                    "capex_impact": "reduces plant capex",
                    "date": "2025-01",
                    "source": "ASX processing announcement",
                    "filing_ref": "ASX announcement",
                    "confidence": "PRIMARY",
                    "corroborated_by": 1,
                    "conflict": False,
                    "conflict_detail": None,
                }
            ],
        }
        rows = flatten_packet_rows(payload)
        wrong_row = next(
            row for row in rows if "West Wits Mining secured IDC and Absa debt support" in row["fact"]
        )
        review = {
            "packet_decision": "keep",
            "packet_confidence_pct": 98,
            "packet_reason": "Packet is mostly Brightstar, with one explicit West Wits row.",
            "wrong_entity_detected": None,
            "row_decisions": [
                {
                    "row_id": row["row_id"],
                    "decision": "drop_row" if row["row_id"] == wrong_row["row_id"] else "keep",
                    "confidence_pct": 99 if row["row_id"] == wrong_row["row_id"] else 97,
                    "reason": (
                        "Explicitly names West Wits Mining and Qala Shallows."
                        if row["row_id"] == wrong_row["row_id"]
                        else "Matches Brightstar context."
                    ),
                    "wrong_entity_detected": (
                        "West Wits Mining Limited" if row["row_id"] == wrong_row["row_id"] else None
                    ),
                }
                for row in rows
            ],
        }
        adjusted, meta = apply_contamination_review(payload, review, min_confidence_pct=95)
        advisors = adjusted.get("named_advisors_and_counterparties") or []
        advisor_facts = [str(entry.get("fact") or "") for entry in advisors]
        self.assertEqual(len(advisors), 1)
        self.assertIn("Taurus acted as lender for the Brightstar bond package.", advisor_facts)
        self.assertTrue(
            all("West Wits Mining" not in fact for fact in advisor_facts),
            msg=f"Unexpected contamination survivor: {advisor_facts}",
        )
        self.assertEqual(meta["dropped_row_count"], 1)
        self.assertFalse(meta["drop_packet_applied"])
        self.assertIn("Genesis Minerals provides Laverton mill processing capacity for ore treatment.", [
            str(entry.get("fact") or "")
            for entry in (adjusted.get("infrastructure_and_project_structure") or [])
        ])


if __name__ == "__main__":
    unittest.main()
