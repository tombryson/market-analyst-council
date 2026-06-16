import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from backend.scenario_router.action_judge import ActionJudge
from backend.scenario_router.document_reader import DocumentReader
from backend.scenario_router.model_thesis_judge import ModelAnnouncementThesisJudge
from backend.scenario_router.models import AnnouncementFacts, BaselineRunPacket, EvidenceRef
from backend.scenario_router.rejudge_artifacts import needs_model_rejudge, scan_and_rejudge
from backend.scenario_router.thesis_comparator import ThesisComparator


def baseline(leaning="base"):
    return BaselineRunPacket(
        run_id="run-1",
        ticker="ASX:TST",
        template_id="rare_earths_critical_minerals",
        summary_fields={"template_family": "rare_earths_critical_minerals"},
        lab_payload={
            "structured_data": {
                "extended_analysis": {"current_thesis_state": {"leaning": leaning}},
                "thesis_map": {
                    "bull": {"required_conditions": [], "failure_conditions": []},
                    "base": {"required_conditions": [], "failure_conditions": []},
                    "bear": {"required_conditions": [], "failure_conditions": []},
                },
                "monitoring_watchlist": {
                    "red_flags": [],
                    "confirmatory_signals": [
                        {
                            "watch_id": "watch_binding_offtake",
                            "condition": "Binding offtake announcement",
                            "severity": "high",
                        }
                    ],
                },
                "verification_queue": [],
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
        source_confidence=1.0,
        extraction_confidence=0.98,
    )


async def run_judge(payload, announcement, run):
    async def fake_query(_model, _messages, _timeout, _max_tokens, _reasoning_effort):
        return {"content": json.dumps(payload)}

    return await ModelAnnouncementThesisJudge(model="test/model", query_fn=fake_query).interpret(announcement, run)


class ScenarioRouterModelThesisJudgeTests(unittest.TestCase):
    def test_incident_report_without_saved_reference_does_not_become_bull_evidence(self):
        announcement = facts(
            "Incident Report",
            (
                "West Wits regrets to inform of an accident resulting in a contractor fatality. "
                "The incident was notified to the regulator. Approved for release by the Managing Director."
            ),
        )
        payload = {
            "one_sentence_summary": "A contractor fatality occurred and the regulator was notified.",
            "document_type": "incident_report",
            "core_claims": [
                {
                    "claim": "A contractor fatality occurred at the mine.",
                    "evidence_quote": "accident resulting in a contractor fatality",
                    "claim_type": "safety_incident",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [{"type": "footer", "reason": "Release authorisation is not a thesis catalyst."}],
            "thesis_relationships": [],
            "trajectory_verdict": {
                "state": "material_unmapped",
                "direction": "neutral",
                "materiality": "medium",
                "intensity": "medium",
                "recommended_case": "unchanged",
                "confidence": 0.82,
                "reason": "The incident is material but no saved thesis reference covers safety incidents.",
            },
            "maintenance_action": {"action": "add_thesis_condition", "reason": "Add safety incident handling."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline(leaning="bull")))
        report = ThesisComparator().compare(interpreted, baseline(leaning="bull"))

        self.assertEqual(interpreted.announcement_class, "incident_report")
        self.assertEqual(report.trajectory_state, "risk_increased")
        self.assertEqual(report.thesis_relationship, "related_unmapped")
        self.assertEqual(report.impact_verdict, "negative")
        self.assertEqual(report.relationship_direction, "neutral")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertLess(report.trajectory_score["unvalidated_event_delta"], 0.0)
        self.assertEqual(report.trajectory_score["position_label"], "Bull evidence zone")

    def test_serious_incident_guardrail_does_not_depend_on_document_type(self):
        announcement = facts(
            "Operations Update",
            (
                "The company reported an underground mine accident involving a contractor fatality. "
                "The regulator has opened a formal investigation."
            ),
        )
        payload = {
            "one_sentence_summary": "A contractor fatality occurred and a regulator investigation has opened.",
            "document_type": "operational_update",
            "core_claims": [
                {
                    "claim": "An underground mine accident caused a contractor fatality.",
                    "evidence_quote": "underground mine accident involving a contractor fatality",
                    "claim_type": "safety_incident",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [],
            "trajectory_verdict": {
                "state": "no_thesis_change",
                "direction": "neutral",
                "materiality": "low",
                "intensity": "low",
                "recommended_case": "unchanged",
                "confidence": 0.8,
                "reason": "No production impact was announced.",
            },
            "maintenance_action": {"action": "none", "reason": "No thesis update required."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline(leaning="bull")))
        report = ThesisComparator().compare(interpreted, baseline(leaning="bull"))
        action = ActionJudge().judge(report)

        self.assertEqual(interpreted.announcement_class, "operational_update")
        self.assertEqual(report.trajectory_state, "risk_increased")
        self.assertEqual(report.impact_verdict, "negative")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertLess(report.trajectory_score["unvalidated_event_delta"], 0.0)
        self.assertEqual(action.action, "annotate_run")
        self.assertTrue(action.requires_human_ack)
        self.assertTrue(any("safety" in step.lower() for step in action.follow_up_steps))

    def test_agm_disclaimer_delay_without_saved_reference_is_gated_to_unmapped_neutral(self):
        announcement = facts(
            "AGM Presentation",
            (
                "Forward-looking statements are subject to project delay or advancement and approvals. "
                "The company also outlined production growth and reserve data."
            ),
        )
        payload = {
            "one_sentence_summary": "The company released an AGM presentation with operational background but no new delay claim.",
            "document_type": "agm_presentation",
            "core_claims": [
                {
                    "claim": "The presentation includes operational background and reserve data.",
                    "evidence_quote": "outlined production growth and reserve data",
                    "claim_type": "investor_presentation",
                    "is_new_information": False,
                }
            ],
            "ignored_text": [{"type": "disclaimer", "reason": "Forward-looking risk text is boilerplate."}],
            "thesis_relationships": [],
            "trajectory_verdict": {
                "state": "material_unmapped",
                "direction": "neutral",
                "materiality": "medium",
                "intensity": "medium",
                "recommended_case": "unchanged",
                "confidence": 0.76,
                "reason": "No core filing claim shows a project delay.",
            },
            "maintenance_action": {"action": "none", "reason": "No thesis movement."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline()))
        report = ThesisComparator().compare(interpreted, baseline())

        self.assertEqual(interpreted.announcement_class, "agm_presentation")
        self.assertEqual(report.trajectory_state, "no_thesis_change")
        self.assertEqual(report.thesis_relationship, "related_unmapped")
        self.assertEqual(report.impact_verdict, "neutral")
        self.assertEqual(report.trajectory_score["direction"], "neutral")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertIn("saved thesis evidence set", report.relationship_summary)

    def test_model_watchlist_relationship_with_quote_moves_validated_path(self):
        announcement = facts(
            "Strategic Offtake LoI",
            "The company signed an offtake-related letter of intent with Solvay.",
        )
        payload = {
            "one_sentence_summary": "The company signed an offtake-related LoI with Solvay.",
            "document_type": "project_development",
            "core_claims": [
                {
                    "claim": "The company signed an offtake-related LoI.",
                    "evidence_quote": "signed an offtake-related letter of intent with Solvay",
                    "claim_type": "commercial",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "watchlist",
                    "reference_id": "watch_binding_offtake",
                    "reference_label": "Binding offtake announcement",
                    "scenario": "",
                    "relationship": "partially_confirms",
                    "direction": "positive",
                    "evidence_quote": "signed an offtake-related letter of intent with Solvay",
                    "reason": "The LoI is an offtake precursor, but not a final binding offtake.",
                    "confidence": 0.78,
                }
            ],
            "trajectory_verdict": {
                "state": "thesis_strengthened",
                "direction": "positive",
                "materiality": "medium",
                "intensity": "medium",
                "recommended_case": "bull",
                "confidence": 0.78,
                "reason": "Partially confirms the saved offtake watchlist signal.",
            },
            "maintenance_action": {"action": "refresh_evidence", "reason": "Attach the new source."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline()))
        report = ThesisComparator().compare(interpreted, baseline())

        self.assertEqual(report.trajectory_state, "thesis_strengthened")
        self.assertEqual(report.triggered_watchlist_ids, ["watch_binding_offtake"])
        self.assertEqual(report.trajectory_score["validation_type"], "watchlist_confirmatory_partial")
        self.assertEqual(report.trajectory_score["event_delta"], 2.0)

    def test_neutral_red_flag_reference_is_checked_not_triggered_not_confirmatory(self):
        announcement = facts(
            "Incident Report",
            (
                "West Wits reported a contractor fatality at the Qala Shallows underground mine "
                "and stated it does not anticipate a material impact to mining operations."
            ),
        )
        run = baseline(leaning="bull")
        run.lab_payload["structured_data"]["monitoring_watchlist"] = {
            "red_flags": [
                {
                    "watch_id": "unexplained_delays_in_quarterly_production_ramp",
                    "condition": "Unexplained delays in quarterly production ramp",
                    "severity": "high",
                }
            ],
            "confirmatory_signals": [],
        }
        payload = {
            "one_sentence_summary": (
                "West Wits reported a contractor fatality but stated no material operational impact is expected."
            ),
            "document_type": "incident_report",
            "core_claims": [
                {
                    "claim": "A contractor fatality occurred, with no expected material operational impact.",
                    "evidence_quote": "does not anticipate a material impact to mining operations",
                    "claim_type": "safety_incident",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "watchlist",
                    "reference_id": "unexplained_delays_in_quarterly_production_ramp",
                    "reference_label": "Unexplained delays in quarterly production ramp",
                    "scenario": "",
                    "relationship": "partially_confirms",
                    "direction": "neutral",
                    "evidence_quote": "does not anticipate a material impact to mining operations",
                    "reason": "The production-delay risk area was checked, but the company guided no operational impact.",
                    "confidence": 0.8,
                }
            ],
            "trajectory_verdict": {
                "state": "no_thesis_change",
                "direction": "neutral",
                "materiality": "low",
                "intensity": "none",
                "recommended_case": "unchanged",
                "confidence": 0.9,
                "reason": "Safety incident disclosed, but no expected operational impact.",
            },
            "maintenance_action": {"action": "none", "reason": "No thesis movement."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, run))
        report = ThesisComparator().compare(interpreted, run)
        watch_eval = next(item for item in report.condition_evaluations if item.condition_id)

        self.assertEqual(watch_eval.group, "red_flag")
        self.assertEqual(watch_eval.status, "checked_not_triggered")
        self.assertEqual(watch_eval.relationship, "checked_not_triggered")
        self.assertFalse(watch_eval.satisfies_condition)
        self.assertEqual(report.triggered_watchlist_ids, [])
        self.assertTrue(any(item.type == "unmapped_material_filing" for item in report.key_findings))
        self.assertEqual(report.relationship_kind, "material_unmapped")
        self.assertEqual(report.trajectory_state, "risk_increased")
        self.assertEqual(report.impact_verdict, "negative")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertLess(report.trajectory_score["unvalidated_event_delta"], 0.0)

    def test_triggered_red_flag_reference_scores_negative(self):
        announcement = facts(
            "Production Ramp Delayed",
            "West Wits reported unexpected geotechnical issues causing a delay to the Qala Shallows production ramp.",
        )
        run = baseline(leaning="bull")
        run.lab_payload["structured_data"]["monitoring_watchlist"] = {
            "red_flags": [
                {
                    "watch_id": "unexplained_delays_in_quarterly_production_ramp",
                    "condition": "Unexplained delays in quarterly production ramp",
                    "severity": "high",
                }
            ],
            "confirmatory_signals": [],
        }
        payload = {
            "one_sentence_summary": "Unexpected geotechnical issues delayed the Qala Shallows production ramp.",
            "document_type": "operations_update",
            "core_claims": [
                {
                    "claim": "Production ramp timing has been delayed by unexpected geotechnical issues.",
                    "evidence_quote": "causing a delay to the Qala Shallows production ramp",
                    "claim_type": "operations",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "watchlist",
                    "reference_id": "unexplained_delays_in_quarterly_production_ramp",
                    "reference_label": "Unexplained delays in quarterly production ramp",
                    "scenario": "",
                    "relationship": "confirms",
                    "direction": "negative",
                    "evidence_quote": "delay to the Qala Shallows production ramp",
                    "reason": "The filing reports the watched production-ramp delay.",
                    "confidence": 0.86,
                }
            ],
            "trajectory_verdict": {
                "state": "timeline_delayed",
                "direction": "negative",
                "materiality": "medium",
                "intensity": "medium",
                "recommended_case": "bear",
                "confidence": 0.86,
                "reason": "The watched ramp-delay risk was triggered.",
            },
            "maintenance_action": {"action": "refresh_evidence", "reason": "Add the delay filing."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, run))
        report = ThesisComparator().compare(interpreted, run)
        watch_eval = next(item for item in report.condition_evaluations if item.condition_id)

        self.assertEqual(watch_eval.group, "red_flag")
        self.assertEqual(watch_eval.status, "matched")
        self.assertTrue(watch_eval.satisfies_condition)
        self.assertEqual(report.triggered_watchlist_ids, ["unexplained_delays_in_quarterly_production_ramp"])
        self.assertEqual(report.relationship_kind, "watchlist_red_flag")
        self.assertLess(report.trajectory_score["event_delta"], 0)
        self.assertEqual(report.trajectory_score["validation_type"], "watchlist_red_flag_full")

    def test_model_partial_verification_relationship_scores_as_lower_authority_evidence(self):
        announcement = facts(
            "DFS Tax Assumption Clarified",
            "The company disclosed a preliminary tax assumption that partially clarifies post-tax project economics.",
        )
        run = baseline()
        run.lab_payload["structured_data"]["verification_queue"] = [
            {
                "verification_id": "verify_post_tax_npv",
                "field": "Post-tax NPV assumptions",
                "reason": "Confirm the tax treatment behind post-tax project economics.",
                "priority": "high",
            }
        ]
        payload = {
            "one_sentence_summary": "The filing partially clarifies the tax assumptions behind project economics.",
            "document_type": "project_development",
            "core_claims": [
                {
                    "claim": "The filing discloses preliminary tax assumptions.",
                    "evidence_quote": "preliminary tax assumption that partially clarifies post-tax project economics",
                    "claim_type": "project_economics",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "verification",
                    "reference_id": "verify_post_tax_npv",
                    "reference_label": "Post-tax NPV assumptions",
                    "scenario": "",
                    "relationship": "partially_confirms",
                    "direction": "positive",
                    "evidence_quote": "preliminary tax assumption that partially clarifies post-tax project economics",
                    "reason": "The disclosure helps resolve the verification item, but does not fully close it.",
                    "missing_for_full_match": ["final post-tax NPV calculation"],
                    "confidence": 0.82,
                }
            ],
            "trajectory_verdict": {
                "state": "risk_reduced",
                "direction": "positive",
                "materiality": "medium",
                "intensity": "medium",
                "recommended_case": "unchanged",
                "confidence": 0.82,
                "reason": "The filing reduces an evidence gap without proving a saved catalyst.",
            },
            "maintenance_action": {"action": "refresh_evidence", "reason": "Attach the source to the evidence pack."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, run))
        report = ThesisComparator().compare(interpreted, run)

        self.assertEqual(report.relationship_kind, "verification_queue")
        self.assertEqual(report.relationship_strength, "partial")
        self.assertEqual(report.triggered_verification_ids, ["verify_post_tax_npv"])
        self.assertEqual(report.trajectory_score["validation_type"], "verification_queue_partial")
        self.assertEqual(report.trajectory_score["validation_weight"], 1.0)
        self.assertEqual(report.trajectory_score["event_delta"], 1.0)
        self.assertEqual(report.trajectory_projection["rerun_signal"], "annotate_evidence")

    def test_model_neutral_verdict_overrides_positive_relationship_match(self):
        announcement = facts(
            "Update - Notification of buy-back - BRK",
            "The company bought back 37,893 shares under an existing on-market buy-back program.",
        )
        payload = {
            "one_sentence_summary": "The company provided a routine daily update on an existing buy-back program.",
            "document_type": "capital_management",
            "core_claims": [
                {
                    "claim": "The company bought back 37,893 shares.",
                    "evidence_quote": "bought back 37,893 shares",
                    "claim_type": "capital_management",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "verification",
                    "reference_id": "base_required_share_buyback_support",
                    "reference_label": "Share buyback support",
                    "scenario": "base",
                    "relationship": "confirms",
                    "direction": "positive",
                    "evidence_quote": "bought back 37,893 shares",
                    "reason": "The buy-back confirms existing capital management activity.",
                    "confidence": 0.9,
                }
            ],
            "trajectory_verdict": {
                "state": "no_thesis_change",
                "direction": "neutral",
                "materiality": "low",
                "intensity": "low",
                "recommended_case": "unchanged",
                "confidence": 0.9,
                "reason": "The filing confirms existing activity but does not change the thesis trajectory.",
            },
            "maintenance_action": {"action": "none", "reason": "No thesis update required."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline()))
        report = ThesisComparator().compare(interpreted, baseline())

        self.assertEqual(report.trajectory_state, "no_thesis_change")
        self.assertEqual(report.relationship_kind, "verification_queue")
        self.assertEqual(report.relationship_direction, "neutral")
        self.assertEqual(report.triggered_verification_ids, ["base_required_share_buyback_support"])
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertEqual(report.trajectory_score["direction"], "neutral")
        self.assertEqual(ActionJudge().judge(report).action, "ignore")

    def test_low_materiality_same_case_confirmation_does_not_upgrade_path(self):
        announcement = facts(
            "Daily Capital Management Update",
            "The company bought back 30,000 shares under an existing on-market buy-back program.",
        )
        payload = {
            "one_sentence_summary": "The company provided a routine update on an existing buy-back program.",
            "document_type": "capital_management",
            "core_claims": [
                {
                    "claim": "The company bought back 30,000 shares.",
                    "evidence_quote": "bought back 30,000 shares",
                    "claim_type": "capital_management",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "thesis_map",
                    "reference_id": "base_required_share_buyback_support",
                    "reference_label": "Share buyback support",
                    "scenario": "base",
                    "relationship": "confirms",
                    "direction": "positive",
                    "evidence_quote": "bought back 30,000 shares",
                    "reason": "The buy-back confirms existing capital discipline.",
                    "confidence": 0.9,
                }
            ],
            "trajectory_verdict": {
                "state": "thesis_strengthened",
                "direction": "positive",
                "materiality": "low",
                "intensity": "low",
                "recommended_case": "base",
                "confidence": 0.9,
                "reason": "Confirms existing base-case capital management.",
            },
            "maintenance_action": {"action": "none", "reason": "Routine update."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline()))
        report = ThesisComparator().compare(interpreted, baseline())

        self.assertEqual(report.trajectory_state, "no_thesis_change")
        self.assertEqual(report.relationship_kind, "saved_thesis_condition")
        self.assertEqual(report.relationship_direction, "neutral")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertEqual(report.trajectory_score["direction"], "neutral")
        self.assertEqual(ActionJudge().judge(report).action, "ignore")

    def test_neutral_base_condition_limits_positive_model_verdict(self):
        announcement = facts(
            "Update - Notification of buy-back - BRK",
            "The company bought back 37,893 shares under an existing on-market buy-back program.",
        )
        payload = {
            "one_sentence_summary": "The company provided a routine update on an existing share buy-back.",
            "document_type": "capital_management",
            "core_claims": [
                {
                    "claim": "The company bought back 37,893 shares.",
                    "evidence_quote": "bought back 37,893 shares",
                    "claim_type": "capital_management",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "thesis_map",
                    "reference_id": "base_required_share_buyback_support",
                    "reference_label": "Share buyback support",
                    "scenario": "base",
                    "relationship": "confirms",
                    "direction": "positive",
                    "evidence_quote": "bought back 37,893 shares",
                    "reason": "The buy-back confirms an existing base-case capital-management condition.",
                    "confidence": 0.9,
                }
            ],
            "trajectory_verdict": {
                "state": "thesis_strengthened",
                "direction": "positive",
                "materiality": "low",
                "intensity": "low",
                "recommended_case": "bull",
                "confidence": 0.9,
                "reason": "The buy-back provides incremental support.",
            },
            "maintenance_action": {"action": "none", "reason": "No follow-up needed."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline(leaning="base")))
        report = ThesisComparator().compare(interpreted, baseline(leaning="base"))

        self.assertEqual(report.relationship_kind, "saved_thesis_condition")
        self.assertEqual(report.relationship_direction, "neutral")
        self.assertEqual(report.impact_verdict, "neutral")
        self.assertEqual(report.trajectory_state, "no_thesis_change")
        self.assertEqual(report.trajectory_score["direction"], "neutral")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertEqual(ActionJudge().judge(report).action, "ignore")

    def test_material_base_condition_can_still_score_positive_drift(self):
        announcement = facts(
            "Major Project Delivery Contract Signed",
            "The company signed a binding project delivery contract that de-risks the path to first production.",
        )
        payload = {
            "one_sentence_summary": "The company signed a material project delivery contract.",
            "document_type": "project_development",
            "core_claims": [
                {
                    "claim": "A binding project delivery contract was signed.",
                    "evidence_quote": "signed a binding project delivery contract",
                    "claim_type": "project_development",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "thesis_map",
                    "reference_id": "base_required_project_execution",
                    "reference_label": "Binding project delivery contract signed",
                    "scenario": "base",
                    "relationship": "confirms",
                    "direction": "positive",
                    "evidence_quote": "signed a binding project delivery contract",
                    "reason": "The contract confirms a material base-case project execution condition.",
                    "confidence": 0.88,
                }
            ],
            "trajectory_verdict": {
                "state": "timeline_accelerated",
                "direction": "positive",
                "materiality": "medium",
                "intensity": "medium",
                "recommended_case": "bull",
                "confidence": 0.88,
                "reason": "The contract de-risks the project delivery path.",
            },
            "maintenance_action": {"action": "refresh_evidence", "reason": "Attach the source to the evidence pack."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline(leaning="base")))
        report = ThesisComparator().compare(interpreted, baseline(leaning="base"))

        self.assertEqual(report.relationship_kind, "saved_thesis_condition")
        self.assertEqual(report.relationship_direction, "neutral")
        self.assertEqual(report.impact_verdict, "positive")
        self.assertEqual(report.trajectory_state, "timeline_accelerated")
        self.assertEqual(report.trajectory_score["direction"], "positive")
        self.assertGreater(report.trajectory_score["event_delta"], 0.0)

    def test_decline_completion_does_not_fulfil_stockpile_processing_condition(self):
        announcement = facts(
            "Qala Shallows 1 West Decline Completed",
            "The company successfully breached the 1 West decline.",
        )
        payload = {
            "one_sentence_summary": "The company completed the 1 West decline.",
            "document_type": "project_development",
            "core_claims": [
                {
                    "claim": "The 1 West decline was completed.",
                    "evidence_quote": "successfully breached the 1 West decline",
                    "claim_type": "project_development",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "thesis_map",
                    "reference_id": "base_stockpile_tolling",
                    "reference_label": "Processing Initial 30,000t Stockpile and Establishing Continuous Tolling Rhythm",
                    "scenario": "base",
                    "relationship": "confirms",
                    "direction": "positive",
                    "evidence_quote": "successfully breached the 1 West decline",
                    "reason": (
                        "The successful breach of the 1 West decline is a critical validator of the "
                        "development timeline leading into Q4 2026 objectives."
                    ),
                    "confidence": 0.82,
                }
            ],
            "trajectory_verdict": {
                "state": "timeline_accelerated",
                "direction": "positive",
                "materiality": "medium",
                "intensity": "medium",
                "recommended_case": "base",
                "confidence": 0.82,
                "reason": "The decline completion supports the development path.",
            },
            "maintenance_action": {"action": "refresh_evidence", "reason": "Attach the source to the evidence pack."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline(leaning="base")))
        report = ThesisComparator().compare(interpreted, baseline(leaning="base"))
        required_rows = [item for item in report.condition_evaluations if item.group == "required"]

        self.assertEqual(report.matched_condition_ids, [])
        self.assertEqual(required_rows[0].status, "partial_match")
        self.assertFalse(required_rows[0].satisfies_condition)
        self.assertNotEqual(report.trajectory_score["validation_type"], "saved_thesis_condition")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)

    def test_power_contract_does_not_fulfil_first_commercial_production_condition(self):
        announcement = facts(
            "Viridis Executes First Major Project Delivery Contract",
            (
                "Viridis signed a binding contract for licensing, engineering and construction of "
                "dedicated high-voltage power infrastructure for the Colossus project."
            ),
        )
        payload = {
            "one_sentence_summary": "Viridis signed a binding power infrastructure delivery contract.",
            "document_type": "project_development",
            "core_claims": [
                {
                    "claim": "A binding power infrastructure contract was signed.",
                    "evidence_quote": (
                        "Viridis has secured reserved grid capacity commencing from December 2027, "
                        "aligning with the Company's development schedule and supporting its targeted "
                        "pathway to first production in H1 2028."
                    ),
                    "claim_type": "project_development",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [
                {
                    "reference_type": "timeline",
                    "reference_id": "base_first_commercial_production_2028",
                    "reference_label": "Projected First Commercial Production (2028)",
                    "scenario": "base",
                    "relationship": "confirms",
                    "direction": "positive",
                    "evidence_quote": (
                        "Viridis has secured reserved grid capacity commencing from December 2027, "
                        "aligning with the Company's development schedule and supporting its targeted "
                        "pathway to first production in H1 2028."
                    ),
                    "reason": (
                        "The filing confirms the power infrastructure timeline, which is a critical path item "
                        "for the 2028 production goal."
                    ),
                    "confidence": 0.84,
                }
            ],
            "trajectory_verdict": {
                "state": "risk_reduced",
                "direction": "positive",
                "materiality": "medium",
                "intensity": "medium",
                "recommended_case": "base",
                "confidence": 0.84,
                "reason": "The contract de-risks the path to production but does not announce production.",
            },
            "maintenance_action": {"action": "refresh_evidence", "reason": "Attach the source to the evidence pack."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, baseline(leaning="base")))
        report = ThesisComparator().compare(interpreted, baseline(leaning="base"))
        required_rows = [item for item in report.condition_evaluations if item.group == "required"]

        self.assertEqual(report.matched_condition_ids, [])
        self.assertEqual(required_rows[0].status, "partial_match")
        self.assertFalse(required_rows[0].satisfies_condition)
        self.assertNotEqual(report.trajectory_score["validation_type"], "saved_thesis_condition")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)

    def test_invalid_model_json_abstains_without_keyword_direction(self):
        async def fake_query(_model, _messages, _timeout, _max_tokens, _reasoning_effort):
            return {"content": "not json"}

        announcement = facts("Corporate Update", "The company provides a short corporate update.")
        interpreted = asyncio.run(
            ModelAnnouncementThesisJudge(model="test/model", query_fn=fake_query).interpret(announcement, baseline())
        )

        self.assertEqual(interpreted.announcement_class, "needs_classification")
        self.assertEqual(interpreted.trajectory_effect, "no_clear_change")
        self.assertEqual(interpreted.classification_confidence, 0.0)
        self.assertIn("abstained", interpreted.classification_reason)

    def test_invalid_first_model_json_is_retried_once(self):
        calls = []
        payload = {
            "one_sentence_summary": "The company released a routine update.",
            "document_type": "operational_update",
            "core_claims": [
                {
                    "claim": "The company released a routine update.",
                    "evidence_quote": "routine update",
                    "claim_type": "operations",
                    "is_new_information": True,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [],
            "trajectory_verdict": {
                "state": "no_thesis_change",
                "direction": "neutral",
                "materiality": "low",
                "intensity": "low",
                "recommended_case": "unchanged",
                "confidence": 0.72,
                "reason": "No saved thesis relationship changed.",
            },
            "maintenance_action": {"action": "none", "reason": "No follow-up needed."},
        }

        async def fake_query(_model, messages, _timeout, _max_tokens, _reasoning_effort):
            calls.append(messages)
            if len(calls) == 1:
                return {"content": "not json"}
            return {"content": json.dumps(payload)}

        announcement = facts("Routine Update", "The company released a routine update.")
        interpreted = asyncio.run(
            ModelAnnouncementThesisJudge(model="test/model", query_fn=fake_query).interpret(announcement, baseline())
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual(interpreted.announcement_class, "operational_update")
        self.assertEqual(interpreted.trajectory_effect, "no_clear_change")

    def test_filing_packet_prefers_segmented_body_and_includes_sections(self):
        announcement = facts("Segmented Filing", "Short summary.")
        announcement.raw_text_excerpt = "raw text should be secondary"
        announcement.document_sections = {
            "highlights": ["Highlights line"],
            "body": "The core filing body says the binding contract was signed.",
            "disclaimers": ["Forward-looking statements are subject to risks."],
        }

        packet = ModelAnnouncementThesisJudge._filing_packet(announcement)

        self.assertIn("document_sections", packet)
        self.assertIn("The core filing body", packet["filing_text"])
        self.assertIn("Forward-looking statements", json.dumps(packet["document_sections"]))

    def test_document_segmentation_separates_disclaimer_and_release_authority(self):
        text = """
Highlights
The company signed a binding grid connection contract.
Forward-looking statements are subject to risks and uncertainties.
This report may contain forecasts and assumptions.
Approved for release by the Board.
"""
        sections = DocumentReader._segment_document(text)

        self.assertIn("The company signed a binding grid connection contract.", "\n".join(sections["highlights"]))
        self.assertTrue(sections["disclaimers"])
        self.assertTrue(sections["release_authority"])

    def test_model_judgement_is_authoritative_over_raw_text_keyword_match(self):
        run = baseline()
        run.lab_payload["structured_data"]["thesis_map"]["bull"]["required_conditions"] = [
            {
                "condition_id": "bull_permit",
                "condition": "Mining permit approval received ahead of schedule",
                "severity": "high",
            }
        ]
        announcement = facts(
            "AGM Presentation",
            "Mining permit approval received ahead of schedule is mentioned only as background text.",
        )
        payload = {
            "one_sentence_summary": "The filing has background text but no thesis movement.",
            "document_type": "agm_presentation",
            "core_claims": [
                {
                    "claim": "The filing repeats background text.",
                    "evidence_quote": "mentioned only as background text",
                    "claim_type": "investor_presentation",
                    "is_new_information": False,
                }
            ],
            "ignored_text": [],
            "thesis_relationships": [],
            "trajectory_verdict": {
                "state": "no_thesis_change",
                "direction": "neutral",
                "materiality": "low",
                "intensity": "low",
                "recommended_case": "unchanged",
                "confidence": 0.8,
                "reason": "No saved condition was engaged by a new core claim.",
            },
            "maintenance_action": {"action": "none", "reason": "No follow-up needed."},
        }

        interpreted = asyncio.run(run_judge(payload, announcement, run))
        report = ThesisComparator().compare(interpreted, run)

        self.assertEqual(report.matched_condition_ids, [])
        self.assertEqual(report.trajectory_state, "no_thesis_change")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)

    def test_rejudge_artifact_dry_run_finds_legacy_candidate(self):
        payload = {
            "announcement_facts": facts("Legacy Filing", "Legacy text.").to_dict(),
            "baseline_run": baseline().to_dict(),
        }
        self.assertTrue(needs_model_rejudge(payload))

        with tempfile.TemporaryDirectory() as tmp:
            artifact_path = Path(tmp) / "ASX:TST" / "event.json"
            artifact_path.parent.mkdir(parents=True)
            artifact_path.write_text(json.dumps(payload), encoding="utf-8")
            result = asyncio.run(scan_and_rejudge(Path(tmp), write=False))

        self.assertEqual(result["candidates"], 1)
        self.assertEqual(result["rewritten"], 0)


if __name__ == "__main__":
    unittest.main()
