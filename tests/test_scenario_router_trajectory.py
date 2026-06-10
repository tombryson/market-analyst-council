import asyncio
import unittest

from backend.main import _build_integration_packet
from backend.scenario_router.action_judge import ActionJudge
from backend.scenario_router.announcement_interpreter import AnnouncementInterpreter
from backend.scenario_router.models import AnnouncementFacts, BaselineRunPacket, EvidenceRef
from backend.scenario_router.run_selector import LatestRunSelector
from backend.scenario_router.semantic_adjudicator import parse_adjudicator_json
from backend.scenario_router.thesis_comparator import ThesisComparator
from backend.scenario_router.trajectory_scoring import apply_cumulative_scores


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
        self.assertEqual(report.relationship_priority, 3)
        self.assertEqual(report.relationship_kind, "material_unmapped")
        self.assertEqual(action.action, "annotate_run")
        self.assertIn("saved thesis evidence set", action.reason)

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

    def test_positive_material_unmapped_does_not_move_validated_path(self):
        announcement = facts(
            "Viridis Executes First Major Project Delivery Contract",
            (
                "The company signed a binding agreement for dedicated 138kV power transmission infrastructure. "
                "The agreement secures critical electrical infrastructure and provides a de-risked pathway to "
                "Stage 1 operations and first production."
            ),
        )

        interpreted = AnnouncementInterpreter().interpret(
            announcement,
            baseline(template_id="rare_earths_critical_minerals", leaning="base"),
        )
        report = ThesisComparator().compare(interpreted, baseline(template_id="rare_earths_critical_minerals", leaning="base"))

        self.assertEqual(report.trajectory_state, "material_unmapped")
        self.assertEqual(report.trajectory_score["direction"], "neutral")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertEqual(report.trajectory_score["position_label"], "Base evidence zone")
        self.assertFalse(report.trajectory_score["mapped_condition"])
        self.assertIn("No price/time thesis movement", report.trajectory_score["reason"])

    def test_saved_thesis_condition_scores_above_unmapped_confirmation(self):
        announcement = facts(
            "Power Transmission Infrastructure Secured",
            "The company confirmed dedicated power transmission infrastructure secured for Stage 1 operations.",
        )
        run = baseline(
            template_id="rare_earths_critical_minerals",
            leaning="base",
            conditions=[
                {
                    "condition_id": "bull_power_transmission",
                    "condition": "dedicated power transmission infrastructure secured",
                }
            ],
        )

        interpreted = AnnouncementInterpreter().interpret(announcement, run)
        report = ThesisComparator().compare(interpreted, run)

        self.assertEqual(report.trajectory_score["validation_type"], "saved_thesis_condition")
        self.assertEqual(report.trajectory_score["validation_weight"], 3.0)
        self.assertEqual(report.trajectory_score["event_delta"], 3.0)
        self.assertEqual(report.trajectory_score["position_label"], "Base, bull-leaning")
        self.assertTrue(report.trajectory_score["mapped_condition"])

    def test_negative_material_unmapped_does_not_move_validated_path(self):
        announcement = facts(
            "Key Permit Decision Delayed",
            "The regulator delayed the key operating permit decision and management said the project schedule is at risk.",
        )

        interpreted = AnnouncementInterpreter().interpret(
            announcement,
            baseline(template_id="rare_earths_critical_minerals", leaning="base"),
        )
        report = ThesisComparator().compare(interpreted, baseline(template_id="rare_earths_critical_minerals", leaning="base"))

        self.assertEqual(report.trajectory_state, "material_unmapped")
        self.assertEqual(report.trajectory_score["direction"], "neutral")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertEqual(report.trajectory_score["position_band"], "base")

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
        self.assertEqual(report.relationship_priority, 1)
        self.assertEqual(report.relationship_kind, "no_relation")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertEqual(action.action, "ignore")

    def test_cumulative_scores_roll_forward_by_ticker_and_run(self):
        rows = [
            {
                "ticker": "ASX:TST",
                "run_id": "run-1",
                "saved_at_utc": "2026-01-01T00:00:00Z",
                "baseline_path": "base",
                "trajectory_score": {"event_delta": 2.0, "baseline_score": 0.0, "validation_type": "mapped_condition"},
            },
            {
                "ticker": "ASX:TST",
                "run_id": "run-1",
                "saved_at_utc": "2026-02-01T00:00:00Z",
                "baseline_path": "base",
                "trajectory_score": {"event_delta": 1.5, "baseline_score": 0.0, "validation_type": "mapped_condition"},
            },
            {
                "ticker": "ASX:TST",
                "run_id": "run-1",
                "saved_at_utc": "2026-03-01T00:00:00Z",
                "baseline_path": "base",
                "trajectory_score": {"event_delta": -1.0, "baseline_score": 0.0, "validation_type": "mapped_condition"},
            },
        ]

        apply_cumulative_scores(rows)

        self.assertEqual(rows[0]["trajectory_score"]["cumulative_delta"], 2.0)
        self.assertEqual(rows[1]["trajectory_score"]["cumulative_delta"], 3.5)
        self.assertEqual(rows[2]["trajectory_score"]["cumulative_delta"], 2.5)
        self.assertEqual(rows[2]["trajectory_score"]["cumulative_position_label"], "Base, bull-leaning")

    def test_unmapped_cumulative_score_does_not_move_validated_path(self):
        rows = [
            {
                "ticker": "ASX:TST",
                "run_id": "run-1",
                "saved_at_utc": "2026-01-01T00:00:00Z",
                "baseline_path": "base",
                "trajectory_score": {
                    "direction": "positive",
                    "event_delta": 2.0,
                    "baseline_score": 0.0,
                    "validation_type": "material_unmapped",
                },
            },
            {
                "ticker": "ASX:TST",
                "run_id": "run-1",
                "saved_at_utc": "2026-02-01T00:00:00Z",
                "baseline_path": "base",
                "trajectory_score": {
                    "direction": "positive",
                    "event_delta": 2.0,
                    "baseline_score": 0.0,
                    "validation_type": "material_unmapped",
                },
            },
            {
                "ticker": "ASX:TST",
                "run_id": "run-1",
                "saved_at_utc": "2026-03-01T00:00:00Z",
                "baseline_path": "base",
                "trajectory_score": {
                    "direction": "positive",
                    "event_delta": 6.0,
                    "baseline_score": 0.0,
                    "validation_type": "material_unmapped",
                },
            },
        ]

        apply_cumulative_scores(rows)

        self.assertEqual(rows[2]["trajectory_score"]["cumulative_delta"], 0.0)
        self.assertEqual(rows[2]["trajectory_score"]["cumulative_validated_delta"], 0.0)
        self.assertEqual(rows[2]["trajectory_score"]["cumulative_position_label"], "Base evidence zone")

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
        self.assertEqual(report.relationship_priority, 5)
        self.assertEqual(report.relationship_kind, "verification_queue")
        self.assertEqual(action.action, "run_delta_only")
        self.assertEqual(report.trajectory_projection["rerun_signal"], "annotate_evidence")

    def test_watchlist_semantic_adjudication_flags_offtake_loi_as_partial_not_unmapped(self):
        announcement = facts(
            "VMM Signs Strategic Offtake/Tech Partnership LoI with Solvay",
            "The company signed a strategic offtake and technology partnership letter of intent with Solvay.",
        )
        run = baseline(template_id="rare_earths_critical_minerals", leaning="base")
        run.lab_payload["structured_data"]["monitoring_watchlist"] = {
            "red_flags": [],
            "confirmatory_signals": [
                {
                    "watch_id": "watch_binding_offtake",
                    "condition": "Binding Offtake Announcement",
                    "severity": "high",
                }
            ],
        }

        interpreted = AnnouncementInterpreter().interpret(announcement, run)
        report = ThesisComparator().compare(interpreted, run)
        action = ActionJudge().judge(report)
        watch_eval = next(item for item in report.condition_evaluations if item.condition_id == "watch_binding_offtake")

        self.assertEqual(watch_eval.status, "partial_match")
        self.assertEqual(watch_eval.relationship, "precursor_partial_match")
        self.assertFalse(watch_eval.satisfies_condition)
        self.assertIn("binding or definitive offtake terms", watch_eval.missing_for_full_match)
        self.assertEqual(report.triggered_watchlist_ids, ["watch_binding_offtake"])
        self.assertEqual(report.trajectory_state, "thesis_strengthened")
        self.assertEqual(report.relationship_priority, 4)
        self.assertEqual(report.relationship_kind, "watchlist_confirmatory")
        self.assertEqual(report.relationship_strength, "partial")
        self.assertEqual(report.trajectory_score["validation_type"], "watchlist_confirmatory_partial")
        self.assertEqual(report.trajectory_score["event_delta"], 2.0)
        self.assertEqual(action.action, "run_delta_only")

    def test_model_adjudicator_reviews_deterministic_offtake_partial(self):
        calls = []

        async def fake_adjudicator(request):
            calls.append(request)
            return {
                "status": "partial_match",
                "relationship": "precursor_partial_match",
                "satisfies_condition": False,
                "confidence": 0.82,
                "reason": "The filing is an offtake-related LoI, but not a binding offtake agreement.",
                "missing_for_full_match": ["binding or definitive offtake terms"],
            }

        announcement = facts(
            "VMM Signs Strategic Offtake/Tech Partnership LoI with Solvay",
            "The company signed a strategic offtake and technology partnership letter of intent with Solvay.",
        )
        run = baseline(template_id="rare_earths_critical_minerals", leaning="base")
        run.lab_payload["structured_data"]["monitoring_watchlist"] = {
            "red_flags": [],
            "confirmatory_signals": [
                {
                    "watch_id": "watch_binding_offtake",
                    "condition": "Binding Offtake Announcement",
                    "severity": "high",
                }
            ],
        }

        interpreted = AnnouncementInterpreter().interpret(announcement, run)
        report = asyncio.run(ThesisComparator(semantic_adjudicator=fake_adjudicator).compare_async(interpreted, run))
        watch_eval = next(item for item in report.condition_evaluations if item.condition_id == "watch_binding_offtake")

        self.assertEqual(len(calls), 1)
        self.assertEqual(watch_eval.status, "partial_match")
        self.assertEqual(watch_eval.matched_via, "model_semantic_adjudication")
        self.assertFalse(watch_eval.satisfies_condition)
        self.assertIn("binding or definitive offtake terms", watch_eval.missing_for_full_match)

    def test_watchlist_semantic_adjudication_satisfies_binding_offtake_when_terms_are_binding(self):
        announcement = facts(
            "Binding Offtake Agreement Signed with Solvay",
            "The company signed a binding offtake agreement with Solvay for committed product supply.",
        )
        run = baseline(template_id="rare_earths_critical_minerals", leaning="base")
        run.lab_payload["structured_data"]["monitoring_watchlist"] = {
            "red_flags": [],
            "confirmatory_signals": [
                {
                    "watch_id": "watch_binding_offtake",
                    "condition": "Binding Offtake Announcement",
                    "severity": "high",
                }
            ],
        }

        interpreted = AnnouncementInterpreter().interpret(announcement, run)
        report = ThesisComparator().compare(interpreted, run)
        watch_eval = next(item for item in report.condition_evaluations if item.condition_id == "watch_binding_offtake")

        self.assertEqual(watch_eval.status, "matched")
        self.assertEqual(watch_eval.relationship, "full_match")
        self.assertTrue(watch_eval.satisfies_condition)
        self.assertEqual(report.triggered_watchlist_ids, ["watch_binding_offtake"])
        self.assertEqual(report.trajectory_state, "thesis_strengthened")
        self.assertEqual(report.trajectory_score["validation_type"], "watchlist_confirmatory_full")
        self.assertEqual(report.trajectory_score["validation_weight"], 2.5)
        self.assertEqual(report.trajectory_score["event_delta"], 2.5)

    def test_saved_failure_condition_scores_as_bear_case_break(self):
        announcement = facts(
            "Permit Revoked",
            "The regulator confirmed the key operating permit was revoked for the project.",
        )
        run = baseline(template_id="rare_earths_critical_minerals", leaning="bull")
        run.lab_payload["structured_data"]["thesis_map"]["bull"]["failure_conditions"] = [
            {
                "condition_id": "bull_permit_revoked",
                "condition": "key operating permit was revoked",
            }
        ]

        interpreted = AnnouncementInterpreter().interpret(announcement, run)
        report = ThesisComparator().compare(interpreted, run)

        self.assertEqual(report.trajectory_score["validation_type"], "saved_thesis_failure")
        self.assertEqual(report.trajectory_score["validation_weight"], 4.0)
        self.assertEqual(report.trajectory_score["event_delta"], -4.0)
        self.assertEqual(report.trajectory_score["position_label"], "Base evidence zone")

    def test_model_adjudicator_handles_ambiguous_watchlist_candidate_without_literal_match(self):
        calls = []

        async def fake_adjudicator(request):
            calls.append(request)
            return {
                "status": "partial_match",
                "relationship": "related_partial_match",
                "satisfies_condition": False,
                "confidence": 0.74,
                "reason": "The filing names a strategic partner, but does not disclose a final commercial agreement.",
                "missing_for_full_match": ["final commercial agreement terms"],
            }

        announcement = facts(
            "Strategic Technology Partner Signed",
            "The company signed a strategic technology partner for its customer rollout program.",
        )
        run = baseline(template_id="software_saas", leaning="base")
        run.lab_payload["structured_data"]["monitoring_watchlist"] = {
            "red_flags": [],
            "confirmatory_signals": [
                {
                    "watch_id": "watch_commercial_partner",
                    "condition": "Strategic partner agreement converted into commercial customer rollout",
                    "severity": "medium",
                }
            ],
        }

        interpreted = AnnouncementInterpreter().interpret(announcement, run)
        report = asyncio.run(ThesisComparator(semantic_adjudicator=fake_adjudicator).compare_async(interpreted, run))
        watch_eval = next(item for item in report.condition_evaluations if item.condition_id == "watch_commercial_partner")

        self.assertEqual(len(calls), 1)
        self.assertEqual(watch_eval.status, "partial_match")
        self.assertEqual(watch_eval.matched_via, "model_semantic_adjudication")
        self.assertEqual(watch_eval.relationship, "related_partial_match")
        self.assertEqual(report.triggered_watchlist_ids, ["watch_commercial_partner"])
        self.assertEqual(report.trajectory_state, "thesis_strengthened")

    def test_model_adjudicator_low_confidence_match_is_ignored(self):
        async def weak_adjudicator(_request):
            return {
                "status": "matched",
                "relationship": "full_match",
                "satisfies_condition": True,
                "confidence": 0.3,
                "reason": "Weak match.",
            }

        announcement = facts(
            "Strategic Technology Partner Signed",
            "The company signed a strategic technology partner for its customer rollout program.",
        )
        run = baseline(template_id="software_saas", leaning="base")
        run.lab_payload["structured_data"]["monitoring_watchlist"] = {
            "red_flags": [],
            "confirmatory_signals": [
                {
                    "watch_id": "watch_commercial_partner",
                    "condition": "Strategic partner agreement converted into commercial customer rollout",
                    "severity": "medium",
                }
            ],
        }

        interpreted = AnnouncementInterpreter().interpret(announcement, run)
        report = asyncio.run(ThesisComparator(semantic_adjudicator=weak_adjudicator).compare_async(interpreted, run))
        watch_eval = next(item for item in report.condition_evaluations if item.condition_id == "watch_commercial_partner")

        self.assertEqual(watch_eval.status, "not_matched")
        self.assertEqual(report.triggered_watchlist_ids, [])

    def test_model_adjudicator_not_called_for_administrative_filing(self):
        calls = []

        async def fake_adjudicator(request):
            calls.append(request)
            return {"status": "matched", "relationship": "full_match", "satisfies_condition": True, "confidence": 0.9}

        announcement = facts(
            "Cleansing Notice",
            "The company issued a cleansing notice in connection with quoted securities.",
        )
        run = baseline(template_id="software_saas", leaning="base")
        run.lab_payload["structured_data"]["monitoring_watchlist"] = {
            "red_flags": [],
            "confirmatory_signals": [
                {
                    "watch_id": "watch_contract",
                    "condition": "Major customer contract signed",
                    "severity": "medium",
                }
            ],
        }

        interpreted = AnnouncementInterpreter().interpret(announcement, run)
        report = asyncio.run(ThesisComparator(semantic_adjudicator=fake_adjudicator).compare_async(interpreted, run))

        self.assertEqual(calls, [])
        self.assertEqual(report.triggered_watchlist_ids, [])

    def test_adjudicator_json_parser_extracts_fenced_json(self):
        parsed = parse_adjudicator_json(
            '```json\n{"status":"partial_match","relationship":"related_partial_match","confidence":0.7}\n```'
        )

        self.assertEqual(parsed["status"], "partial_match")
        self.assertEqual(parsed["relationship"], "related_partial_match")


if __name__ == "__main__":
    unittest.main()
