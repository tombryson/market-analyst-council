import asyncio
import json
import tempfile
import unittest
from pathlib import Path

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
        self.assertEqual(report.trajectory_state, "material_unmapped")
        self.assertEqual(report.relationship_direction, "neutral")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertEqual(report.trajectory_score["position_label"], "Bull evidence zone")

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
        self.assertEqual(report.trajectory_state, "material_unmapped")
        self.assertEqual(report.trajectory_score["direction"], "neutral")
        self.assertEqual(report.trajectory_score["event_delta"], 0.0)
        self.assertIn("no validated scenario movement", report.relationship_summary)

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
