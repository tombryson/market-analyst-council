import unittest

from backend.main import _build_integration_packet
from backend.scenario_router.action_judge import ActionJudge
from backend.scenario_router.announcement_interpreter import AnnouncementInterpreter
from backend.scenario_router.models import AnnouncementFacts, BaselineRunPacket, EvidenceRef
from backend.scenario_router.run_selector import LatestRunSelector
from backend.scenario_router.thesis_comparator import ThesisComparator


def baseline(template_id="software_saas", leaning="base", conditions=None):
    return BaselineRunPacket(
        run_id="run-1",
        ticker="ASX:TST",
        template_id=template_id,
        summary_fields={"template_family": template_id},
        lab_payload={
            "structured_data": {
                "extended_analysis": {"current_thesis_state": {"leaning": leaning}},
                "thesis_map": {
                    "bull": {"required_conditions": conditions or [], "failure_conditions": []},
                    "base": {"required_conditions": [], "failure_conditions": []},
                    "bear": {"required_conditions": [], "failure_conditions": []},
                },
                "monitoring_watchlist": {"red_flags": [], "confirmatory_signals": []},
            }
        },
    )


def facts(title, text):
    return AnnouncementFacts(
        event_id="evt-1",
        ticker="ASX:TST",
        title=title,
        summary=text,
        extracted_facts=[text],
        evidence=[EvidenceRef(source_title=title, quote_excerpt=text)],
        raw_text_excerpt=text,
        parse_quality={"decoded_chars": len(text), "fact_count": 1},
    )


class ScenarioRouterTrajectoryTests(unittest.TestCase):
    def test_resource_language_is_profile_specific_not_generic_parser(self):
        announcement = facts(
            "Gold Mineralisation at Theia Extended 210 Vertical Metres",
            "Assays confirmed mineralisation extended 210 vertical metres with new drilling intercepts and higher grade zones.",
        )

        interpreted = AnnouncementInterpreter().interpret(
            announcement,
            baseline(template_id="rare_earths_critical_minerals"),
        )
        report = ThesisComparator().compare(interpreted, baseline(template_id="rare_earths_critical_minerals"))
        action = ActionJudge().judge(report)

        self.assertEqual(interpreted.domain_profile, "resources")
        self.assertIn("drilling_exploration", interpreted.affected_drivers)
        self.assertEqual(report.trajectory_state, "material_unmapped")
        self.assertEqual(action.action, "annotate_run")
        self.assertIn("thesis-map coverage gap", action.reason)

    def test_generic_customer_contract_classifies_without_resource_topics(self):
        announcement = facts(
            "Three-Year Enterprise Customer Contract Signed",
            "The company signed a three-year enterprise customer contract expected to expand annual recurring revenue.",
        )

        interpreted = AnnouncementInterpreter().interpret(announcement, baseline(template_id="software_saas"))

        self.assertEqual(interpreted.domain_profile, "software")
        self.assertEqual(interpreted.announcement_class, "commercial_customer")
        self.assertIn("commercial_customer", interpreted.affected_drivers)
        self.assertNotIn("resource", interpreted.affected_drivers)
        self.assertEqual(interpreted.materiality, "medium")

    def test_interpreter_summary_does_not_expose_classifier_internals(self):
        announcement = facts(
            "VMM Signs Strategic Offtake/Tech Partnership LoI with Solvay",
            "The company signed a strategic offtake and technology partnership letter of intent with Solvay for its project development pathway.",
        )

        interpreted = AnnouncementInterpreter().interpret(
            announcement,
            baseline(template_id="rare_earths_critical_minerals"),
        )

        summary = interpreted.semantic_summary.lower()
        self.assertIn("filing", summary)
        self.assertIn("solvay", summary)
        self.assertNotIn("classified as", summary)
        self.assertNotIn("; effect:", summary)
        self.assertNotIn("; drivers:", summary)

    def test_administrative_filing_stays_administrative_not_watch(self):
        announcement = facts(
            "Cleansing Notice",
            "The company issued a cleansing notice in connection with quoted securities.",
        )

        interpreted = AnnouncementInterpreter().interpret(announcement, baseline(template_id="software_saas"))
        report = ThesisComparator().compare(interpreted, baseline(template_id="software_saas"))
        action = ActionJudge().judge(report)

        self.assertEqual(interpreted.announcement_class, "administrative")
        self.assertEqual(report.trajectory_state, "administrative_filing")
        self.assertEqual(action.action, "ignore")

    def test_material_unmapped_beats_market_backdrop_projection(self):
        state = ThesisComparator._trajectory_state(
            announcement_class="capital_financing",
            semantic_materiality="medium",
            trajectory_effect="strengthens",
            thesis_effect="confirms",
            timeline_effect="unknown",
            direct_match_count=0,
            market_match_count=1,
            conflicts=[],
            path_transition="",
        )

        self.assertEqual(state, "material_unmapped")

    def test_buyback_update_is_capital_management_not_low_confidence_unknown(self):
        announcement = facts(
            "Update - Notification of buy-back - BRK",
            "The company lodged an update to its on-market share buy-back notification under its capital management program. " * 6,
        )

        interpreted = AnnouncementInterpreter().interpret(
            announcement,
            baseline(template_id="energy_oil_gas"),
        )
        report = ThesisComparator().compare(interpreted, baseline(template_id="energy_oil_gas"))
        action = ActionJudge().judge(report)

        self.assertEqual(interpreted.domain_profile, "oil_gas")
        self.assertEqual(interpreted.announcement_class, "capital_management")
        self.assertEqual(interpreted.materiality, "low")
        self.assertIn("buy-back", interpreted.filing_summary.lower())
        self.assertGreater(interpreted.classification_confidence, 0.45)
        self.assertEqual(report.trajectory_state, "no_thesis_change")
        self.assertEqual(action.action, "annotate_run")

    def test_unknown_filing_exposes_confidence_breakdown(self):
        announcement = facts(
            "Corporate Update",
            "The company provides an update for shareholders. " * 20,
        )

        interpreted = AnnouncementInterpreter().interpret(announcement, baseline(template_id="general"))
        report = ThesisComparator().compare(interpreted, baseline(template_id="general"))

        self.assertEqual(interpreted.announcement_class, "needs_classification")
        self.assertEqual(interpreted.classification_confidence, 0.45)
        self.assertIn("could not classify", interpreted.filing_summary.lower())
        self.assertIn("classification_components", interpreted.confidence_breakdown)
        self.assertEqual(report.confidence_breakdown["classification_confidence"], 0.45)
        self.assertEqual(report.thesis_match_confidence, 0.0)

    def test_24m_projection_places_current_market_on_saved_path(self):
        announcement = facts(
            "Quarterly Activities Report",
            "The company said the development timetable remains on track.",
        )
        announcement.evidence[0].source_date_utc = "2026-03-01T00:00:00Z"
        run = baseline(leaning="base")
        run.summary_fields["analysis_date"] = "2026-01-01"
        run.lab_payload["structured_data"]["price_targets"] = {
            "current_price": 1.8,
            "scenario_targets": {
                "24m": {"bear": 0.8, "base": 1.5, "bull": 2.4},
                "12m": {"bear": 0.7, "base": 1.2, "bull": 1.9},
            },
            "scenario_probabilities": {"24m": {"bear": 20, "base": 50, "bull": 30}},
        }
        run.lab_payload["structured_data"]["development_timeline"] = [
            {"title": "PFS delivery", "timing": "Q3 2026", "status": "planned"}
        ]

        report = ThesisComparator().compare(announcement, run)

        projection = report.trajectory_projection
        self.assertTrue(projection["available"])
        self.assertEqual(projection["market_implied_path_24m"], "base")
        self.assertEqual(projection["baseline_started_at_utc"], "2026-01-01T00:00:00Z")
        self.assertEqual(projection["elapsed_days"], 59)
        self.assertEqual(projection["target_24m"]["bull"], 2.4)
        self.assertAlmostEqual(projection["prob_weighted_target_24m"], 1.63)
        self.assertEqual(projection["timeline_rows"][0]["title"], "PFS delivery")

    def test_projection_preserves_target_period_and_imports_catalysts(self):
        run = baseline(leaning="base")
        run.timeline_rows = [
            {"milestone": "DFS Targeted Completion", "target_period": "Q2 2026", "status": "planned"}
        ]
        run.catalyst_rows = [
            {"title": "Projected First Commercial Production", "target_period": "2028", "status": "planned"}
        ]

        rows = ThesisComparator._projection_timeline_rows({}, run)

        self.assertEqual(rows[0]["title"], "DFS Targeted Completion")
        self.assertEqual(rows[0]["timing"], "Q2 2026")
        self.assertEqual(rows[0]["target_period"], "Q2 2026")
        self.assertEqual(rows[1]["title"], "Projected First Commercial Production")
        self.assertEqual(rows[1]["timing"], "2028")
        self.assertEqual(rows[1]["source"], "next_major_catalysts")

    def test_projection_falls_back_to_lab_payload_catalysts(self):
        run = baseline(leaning="base")
        structured = run.lab_payload["structured_data"]
        structured["extended_analysis"]["next_major_catalysts"] = ["Q4 2026: Target Final Investment Decision"]

        rows = ThesisComparator._projection_timeline_rows(structured, run)

        self.assertEqual(rows[0]["title"], "Target Final Investment Decision")
        self.assertEqual(rows[0]["timing"], "Q4 2026")

    def test_latest_run_selector_carries_council_timeline_and_catalysts(self):
        packet = {
            "run_id": "quality_job_asx_vmm.json",
            "summary_fields": {"ticker": "ASX:VMM"},
            "lab_payload": {},
            "timeline_rows": [
                {"milestone": "Preliminary Licence Granted", "target_period": "Q4 2025", "status": "planned"}
            ],
            "catalyst_rows": [
                {"title": "Target Final Investment Decision", "target_period": "2H 2026", "status": "planned"}
            ],
        }

        run = LatestRunSelector()._coerce_report_packet(packet)

        self.assertEqual(run.timeline_rows[0]["target_period"], "Q4 2025")
        self.assertEqual(run.catalyst_rows[0]["target_period"], "2H 2026")

    def test_integration_packet_exports_timeline_and_catalysts(self):
        packet = _build_integration_packet(
            run_id="quality_job_asx_vmm.json",
            run_payload={
                "structured_data": {
                    "ticker": "ASX:VMM",
                    "development_timeline": [
                        {"milestone": "Preliminary Licence Granted", "target_period": "Q4 2025", "status": "planned"}
                    ],
                    "extended_analysis": {
                        "next_major_catalysts": [
                            {"title": "Target Final Investment Decision", "target_period": "2H 2026", "status": "planned"}
                        ]
                    },
                }
            },
        )

        self.assertEqual(packet["timeline_rows"][0]["target_period"], "Q4 2025")
        self.assertEqual(packet["catalyst_rows"][0]["target_period"], "2H 2026")

    def test_verification_queue_is_checked_as_router_evidence(self):
        announcement = facts(
            "JORC Resource Data Released",
            "The announcement includes JORC-compliant resource data and an independent reserve table.",
        )
        run = baseline(leaning="base")
        run.lab_payload["structured_data"]["verification_queue"] = [
            {
                "verification_id": "verify_jorc_resource",
                "field": "JORC Resource Data",
                "reason": "JORC-compliant resource data required",
                "required_source": "ASX filing",
                "priority": "high",
                "evidence_hooks": ["JORC-compliant resource data"],
            }
        ]

        report = ThesisComparator().compare(announcement, run)
        action = ActionJudge().judge(report)

        self.assertEqual(report.triggered_verification_ids, ["verify_jorc_resource"])
        self.assertIn("verify_jorc_resource", [item.condition_id for item in report.condition_evaluations if item.status == "matched"])
        self.assertEqual(action.action, "annotate_run")
        self.assertEqual(report.trajectory_projection["rerun_signal"], "annotate_evidence")


if __name__ == "__main__":
    unittest.main()
