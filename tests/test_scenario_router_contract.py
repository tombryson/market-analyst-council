from pathlib import Path
import unittest

from backend.scenario_router.inbox_sentinel import InboxSentinel
from backend.scenario_router.artifact_replay import replay_comparison_from_artifact
from backend.scenario_router.observability import ScenarioRouterObservability
from backend.scenario_router.display_contract import MARKET_ONLY_WATCH_REASON, build_router_display_contract
from backend.scenario_router.review_store import apply_review_overlay, save_review


class InboxSentinelCompanyHintTests(unittest.TestCase):
    def test_hotcopper_boilerplate_is_not_used_as_company_hint(self):
        body = """
Welcome from HotCopper Holdings Ltd.

9 April 2026 08:53am (AEST)
PEN: Central Processing Plant Recommences Production

Peninsula Energy Limited released an announcement at 08:52am on 9 April 2026.

Kind regards,
The HotCopper Team
"""
        event = InboxSentinel().ingest_email_payload(
            {
                "subject": "PEN (ASX) announcement on HotCopper",
                "body_text": body,
                "sender": "HotCopper Team <no-reply@hotcopper.com.au>",
            }
        )

        self.assertEqual(event.ticker, "ASX:PEN")
        self.assertEqual(event.company_hint, "Peninsula Energy Limited")


class ScenarioRouterDisplayContractTests(unittest.TestCase):
    def test_legacy_material_unmapped_artifact_suppresses_stale_positive_path_movement(self):
        payload = {
            "status": "ok",
            "event": {"event_id": "evt-wwi", "ticker": "ASX:WWI"},
            "announcement_packet": {
                "event_id": "evt-wwi",
                "ticker": "ASX:WWI",
                "title": "Incident Report",
                "source_type": "exchange_filing",
            },
            "announcement_facts": {
                "event_id": "evt-wwi",
                "ticker": "ASX:WWI",
                "title": "Incident Report",
                "summary": "Incident Report",
                "raw_text_excerpt": "The company reported a fatal incident and regulator notification.",
                "announcement_class": "regulatory_legal",
                "materiality": "medium",
                "trajectory_effect": "risk_reduced",
                "price_time_effect": "Likely improves the price/time path through project delivery.",
                "semantic_summary": "The filing appears to support project delivery and regulatory or legal matters.",
                "model_judgement": {},
            },
            "baseline_run": {
                "run_id": "run-wwi",
                "ticker": "ASX:WWI",
                "lab_payload": {
                    "structured_data": {
                        "extended_analysis": {"current_thesis_state": {"leaning": "bull"}},
                        "thesis_map": {},
                    }
                },
            },
            "comparison_report": {
                "ticker": "ASX:WWI",
                "baseline_run_id": "run-wwi",
                "announcement_title": "Incident Report",
                "baseline_path": "bull",
                "current_path": "bull",
                "impact_level": "medium",
                "announcement_class": "regulatory_legal",
                "materiality": "medium",
                "relationship_kind": "",
                "relationship_direction": "",
                "trajectory_state": "material_unmapped",
                "trajectory_effect": "risk_reduced",
                "price_time_effect": "Likely improves the price/time path through project delivery.",
                "semantic_summary": "The filing appears to support project delivery and regulatory or legal matters.",
                "trajectory_score": {
                    "direction": "positive",
                    "event_delta": 2.0,
                    "validation_type": "material_unmapped",
                    "position_label": "Bull-leaning, unvalidated",
                    "reason": "Positive medium evidence from a material filing outside the saved thesis map.",
                },
            },
            "action_decision": {"action": "annotate_run", "reason": "Old directional reason."},
        }

        report, action = replay_comparison_from_artifact(payload)

        self.assertEqual(report["trajectory_state"], "needs_classification")
        self.assertEqual(report["thesis_relationship"], "related_unmapped")
        self.assertEqual(report["impact_verdict"], "uncertain")
        self.assertEqual(report["relationship_direction"], "neutral")
        self.assertEqual(report["trajectory_score"]["direction"], "neutral")
        self.assertEqual(report["trajectory_score"]["event_delta"], 0.0)
        self.assertNotIn("improves", report["price_time_effect"].lower())
        self.assertIn("model thesis judgement", report["classification_reason"])
        self.assertEqual(action["action"], "annotate_run")

    def test_replay_normalizes_stale_valid_fatality_judgement_before_display(self):
        payload = {
            "status": "ok",
            "saved_at_utc": "2026-06-10T00:00:00Z",
            "event": {
                "event_id": "evt-fatality",
                "ticker": "ASX:WWI",
                "received_at_utc": "2026-06-10T00:00:00Z",
            },
            "announcement_packet": {
                "event_id": "evt-fatality",
                "ticker": "ASX:WWI",
                "title": "Incident Report",
                "source_type": "exchange_filing",
                "source_url": "https://announcements.asx.com.au/asxpdf/example.pdf",
            },
            "announcement_facts": {
                "event_id": "evt-fatality",
                "ticker": "ASX:WWI",
                "title": "Incident Report",
                "summary": "A contractor fatality occurred, with no expected material impact to operations.",
                "raw_text_excerpt": (
                    "West Wits reported a contractor fatality at Qala Shallows and stated it does not "
                    "anticipate a material impact to mining operations."
                ),
                "source_confidence": 1.0,
                "extraction_confidence": 0.98,
                "model_judgement": {
                    "status": "valid",
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
                            "relationship": "partially_confirms",
                            "direction": "neutral",
                            "evidence_quote": "does not anticipate a material impact to mining operations",
                            "reason": "The production-delay risk area was checked, but not triggered.",
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
                },
            },
            "baseline_run": {
                "run_id": "run-wwi",
                "ticker": "ASX:WWI",
                "lab_payload": {
                    "structured_data": {
                        "extended_analysis": {"current_thesis_state": {"leaning": "bull", "status": "on-track"}},
                        "monitoring_watchlist": {
                            "red_flags": [
                                {
                                    "watch_id": "unexplained_delays_in_quarterly_production_ramp",
                                    "condition": "Unexplained delays in quarterly production ramp",
                                    "severity": "high",
                                }
                            ],
                            "confirmatory_signals": [],
                        },
                    }
                },
            },
            "comparison_report": {
                "trajectory_state": "no_thesis_change",
                "impact_verdict": "neutral",
            },
            "action_decision": {"action": "ignore", "reason": "Old stale action."},
        }

        report, action = replay_comparison_from_artifact(payload)
        row = ScenarioRouterObservability._summarize_event_payload(
            payload,
            path=Path("20260610_000000__evt-fatality.json"),
        )

        self.assertEqual(report["trajectory_state"], "risk_increased")
        self.assertEqual(report["impact_verdict"], "negative")
        self.assertEqual(report["relationship_kind"], "material_unmapped")
        self.assertEqual(report["trajectory_score"]["event_delta"], 0.0)
        self.assertLess(report["trajectory_score"]["unvalidated_event_delta"], 0.0)
        self.assertEqual(action["action"], "annotate_run")
        self.assertTrue(action["requires_human_ack"])
        self.assertEqual(row["display"]["trajectory_label"], "Risk increased")
        self.assertEqual(row["display"]["evidence_label"], "Risk event outside thesis map")
        self.assertEqual(row["display"]["queue_bucket"], "open_review")
        self.assertEqual(row["checked_watchlist_count"], 1)
        self.assertEqual(row["watchlist_condition_checks"][0]["status"], "checked_not_triggered")

    def test_review_overlay_preserves_case_bucket_for_reviewed_item(self):
        row = {
            "event_id": "evt-review",
            "display": {
                "queue_bucket": "open_review",
                "queue_label": "Review required",
                "review_status": "open",
                "review_label": "Analyst review required",
                "is_user_action_required": True,
                "tone": "warn",
            },
        }
        review = {
            "event_id": "evt-review",
            "review_status": "reviewed",
            "review_note": "Handled.",
        }

        updated = apply_review_overlay(row, review)

        self.assertEqual(updated["display"]["queue_bucket"], "open_review")
        self.assertEqual(updated["display"]["queue_label"], "Review required")
        self.assertEqual(updated["display"]["review_status"], "reviewed")
        self.assertEqual(updated["display"]["review_label"], "Reviewed")
        self.assertFalse(updated["display"]["is_user_action_required"])

    def test_review_store_persists_review_status(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            review = save_review(
                "evt-store",
                status="dismissed",
                note="Not relevant.",
                actor="test",
                base_dir=Path(tmp),
            )

            self.assertEqual(review["event_id"], "evt-store")
            self.assertEqual(review["review_status"], "dismissed")
            self.assertEqual(review["review_note"], "Not relevant.")
            self.assertEqual(review["next_action"], "none")

    def test_escalated_review_requires_reason_without_erasing_case_bucket(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                save_review("evt-escalate", status="escalated", base_dir=Path(tmp))

            review = save_review(
                "evt-escalate",
                status="escalated",
                escalation_reason="thesis_map_gap",
                note="Project delivery contract is missing from mapped catalysts.",
                actor="test",
                base_dir=Path(tmp),
            )

        row = {
            "event_id": "evt-escalate",
            "display": {
                "queue_bucket": "cleared",
                "queue_label": "Cleared",
                "review_status": "auto_cleared",
                "review_label": "Auto-cleared",
                "is_user_action_required": False,
                "tone": "neutral",
            },
        }
        updated = apply_review_overlay(row, review)

        self.assertEqual(review["escalation_reason_label"], "Create thesis-map update task")
        self.assertEqual(review["next_action"], "update_thesis_map")
        self.assertEqual(updated["display"]["queue_bucket"], "cleared")
        self.assertEqual(updated["display"]["review_status"], "escalated")
        self.assertEqual(updated["display"]["review_label"], "Queued task")
        self.assertEqual(updated["display"]["queue_label"], "Cleared")
        self.assertEqual(updated["display"]["review_queue_label"], "Queued for thesis-map update")
        self.assertEqual(updated["display"]["review_reason_label"], "Create thesis-map update task")
        self.assertEqual(updated["display"]["next_action_label"], "Add thesis-map condition")
        self.assertFalse(updated["display"]["is_user_action_required"])
        self.assertEqual(updated["display"]["tone"], "neutral")

    def test_display_contract_separates_trajectory_review_and_system_action(self):
        display = build_router_display_contract(
            {"trajectory_state": "material_unmapped", "impact_level": "medium"},
            {"action": "annotate_run", "reason": "Material filing without a mapped thesis condition."},
        )

        self.assertEqual(display["trajectory_label"], "Needs assessment")
        self.assertEqual(display["queue_bucket"], "open_review")
        self.assertEqual(display["queue_label"], "Needs thesis decision")
        self.assertEqual(display["review_status"], "open")
        self.assertEqual(display["review_label"], "Needs thesis decision")
        self.assertEqual(display["system_action_label"], "Attach to thesis log")
        self.assertEqual(display["evidence_label"], "No saved condition match")
        self.assertEqual(display["relationship_label"], "Not assessed")
        self.assertTrue(display["is_user_action_required"])

    def test_display_contract_prioritises_unmapped_evidence_over_market_context(self):
        display = build_router_display_contract(
            {
                "trajectory_state": "material_unmapped",
                "impact_level": "medium",
                "relationship_priority": 3,
                "relationship_kind": "material_unmapped",
                "relationship_strength": "none",
            },
            {"action": "annotate_run"},
        )

        self.assertEqual(display["evidence_label"], "No saved condition match")
        self.assertEqual(display["relationship_label"], "Material outside thesis map")

    def test_display_contract_tracks_positive_movement_without_calling_it_review_required(self):
        display = build_router_display_contract(
            {"trajectory_state": "timeline_accelerated", "impact_level": "medium"},
            {"action": "rerun_stage1"},
            matched_conditions_count=1,
        )

        self.assertEqual(display["trajectory_label"], "Timeline accelerated")
        self.assertEqual(display["queue_bucket"], "positive_movement")
        self.assertEqual(display["queue_label"], "Thesis improved")
        self.assertEqual(display["review_status"], "tracking")
        self.assertFalse(display["is_user_action_required"])

    def test_display_contract_does_not_call_unvalidated_positive_support_thesis_improved(self):
        display = build_router_display_contract(
            {
                "trajectory_state": "timeline_accelerated",
                "impact_level": "medium",
                "relationship_kind": "saved_thesis_condition",
                "relationship_strength": "partial",
                "thesis_relationship": "direct_match",
                "impact_verdict": "positive",
                "trajectory_score": {
                    "direction": "positive",
                    "event_delta": 0.0,
                    "validation_type": "none",
                    "mapped_condition": False,
                },
            },
            {"action": "rerun_stage1"},
            matched_conditions_count=0,
            triggered_watchlist_count=0,
            triggered_verification_count=0,
        )

        self.assertEqual(display["trajectory_label"], "Related thesis evidence")
        self.assertEqual(display["queue_bucket"], "open_review")
        self.assertEqual(display["queue_label"], "Needs thesis decision")
        self.assertEqual(display["review_status"], "open")
        self.assertEqual(display["primary_reason"], "Related to the saved thesis, but no saved condition was achieved.")
        self.assertEqual(display["evidence_label"], "No condition match")

    def test_display_contract_auto_clears_no_change_and_administrative_filings(self):
        no_change = build_router_display_contract(
            {"trajectory_state": "no_thesis_change", "impact_level": "low"},
            {"action": "ignore"},
        )
        administrative = build_router_display_contract(
            {"trajectory_state": "administrative_filing", "impact_level": "none"},
            {"action": "ignore"},
        )

        self.assertEqual(no_change["queue_bucket"], "cleared")
        self.assertEqual(no_change["review_status"], "auto_cleared")
        self.assertEqual(administrative["queue_bucket"], "administrative")
        self.assertEqual(administrative["review_status"], "auto_cleared")

    def test_market_only_rerun_projection_rewrites_reason_and_followups(self):
        payload = {
            "status": "ok",
            "saved_at_utc": "2026-04-09T05:57:08Z",
            "event": {
                "event_id": "evt-1",
                "ticker": "ASX:WWI",
                "received_at_utc": "2026-04-09T05:55:00Z",
            },
            "announcement_packet": {
                "event_id": "evt-1",
                "ticker": "ASX:WWI",
                "title": "Completion of Consolidation",
                "source_type": "exchange_filing",
                "source_url": "https://announcements.asx.com.au/asxpdf/example.pdf",
            },
            "baseline_run": {
                "run_id": "run-1",
                "lab_payload": {
                    "structured_data": {
                        "extended_analysis": {
                            "current_thesis_state": {"leaning": "bull", "status": "on-track"}
                        },
                        "thesis_map": {
                            "bull": {
                                "target_12m": "A$0.48",
                                "target_24m": "A$0.85",
                                "probability_24m_pct": 25,
                                "summary": "Bull case summary.",
                                "required_conditions": [
                                    {"condition_id": "bull_dfs", "condition": "DFS confirms capex"}
                                ],
                                "failure_conditions": [],
                            }
                        },
                    }
                },
            },
            "comparison_report": {
                "ticker": "ASX:WWI",
                "baseline_run_id": "run-1",
                "announcement_title": "Completion of Consolidation",
                "baseline_path": "bull",
                "current_path": "base",
                "path_transition": "bull->base",
                "impact_level": "medium",
                "thesis_effect": "undermines",
                "run_validity": "partial_invalidation",
                "affected_domains": ["valuation", "scenario"],
                "market_facts_used": {"gold_price_usd_oz": 4846},
                "condition_evaluations": [
                    {
                        "condition_id": "base_gold",
                        "scenario": "base",
                        "group": "required",
                        "label": "Gold >US$4,200/oz",
                        "status": "matched",
                        "matched_via": "market_facts",
                        "market_field": "gold_price_usd_oz",
                        "observed_value": 4846,
                        "comparator": ">",
                        "threshold_value": 4200,
                    },
                    {
                        "condition_id": "bull_dfs",
                        "scenario": "bull",
                        "group": "required",
                        "label": "DFS confirms capex",
                        "status": "not_matched",
                        "matched_via": "",
                        "reason": "No filing text matched this condition.",
                    }
                ],
            },
            "action_decision": {
                "action": "full_rerun",
                "confidence": 0.8,
                "reason": "High-impact, conflicting, or scenario-breaking announcement likely invalidates parts of the current run.",
                "invalidated_sections": ["valuation"],
                "follow_up_steps": ["Queue full rerun immediately."],
            },
        }

        row = ScenarioRouterObservability._summarize_event_payload(
            payload,
            path=Path("20260409_055708__evt-1.json"),
        )

        self.assertEqual(row["action"], "watch")
        self.assertEqual(row["display"]["queue_bucket"], "cleared")
        self.assertEqual(row["display"]["review_status"], "auto_cleared")
        self.assertEqual(row["display"]["trajectory_label"], "No thesis impact")
        self.assertEqual(row["impact_level"], "low")
        self.assertEqual(row["current_path"], "bull")
        self.assertEqual(row["path_transition"], "")
        self.assertEqual(row["action_reason"], MARKET_ONLY_WATCH_REASON)
        self.assertEqual(row["invalidated_sections"], [])

    def test_stale_partial_watchlist_no_change_projects_to_thesis_movement(self):
        payload = {
            "status": "ok",
            "saved_at_utc": "2026-06-04T00:00:00Z",
            "event": {
                "event_id": "evt-offtake",
                "ticker": "ASX:VMM",
                "received_at_utc": "2026-06-03T23:55:00Z",
            },
            "announcement_packet": {
                "event_id": "evt-offtake",
                "ticker": "ASX:VMM",
                "title": "VMM Signs Strategic Offtake/Tech Partnership LoI with Solvay",
                "source_type": "exchange_filing",
                "source_url": "https://announcements.asx.com.au/asxpdf/example.pdf",
            },
            "baseline_run": {
                "run_id": "run-vmm",
                "lab_payload": {
                    "structured_data": {
                        "extended_analysis": {
                            "current_thesis_state": {"leaning": "base", "status": "on-track"}
                        }
                    }
                },
            },
            "comparison_report": {
                "ticker": "ASX:VMM",
                "baseline_run_id": "run-vmm",
                "announcement_title": "VMM Signs Strategic Offtake/Tech Partnership LoI with Solvay",
                "baseline_path": "base",
                "current_path": "base",
                "impact_level": "medium",
                "materiality": "medium",
                "trajectory_state": "no_thesis_change",
                "trajectory_effect": "material_update",
                "price_time_effect": "Direction depends on final binding terms.",
                "thesis_effect": "no_change",
                "thesis_match_confidence": 0.68,
                "key_findings": [
                    {
                        "type": "confirmatory_partial_match",
                        "summary": "Partially engaged confirmatory: Binding Offtake Announcement",
                        "severity": "low",
                    }
                ],
                "condition_evaluations": [
                    {
                        "condition_id": "watch_binding_offtake",
                        "scenario": "",
                        "group": "confirmatory",
                        "label": "Binding Offtake Announcement",
                        "status": "partial_match",
                        "matched_via": "semantic",
                        "relationship": "precursor_partial_match",
                        "reason": "Announcement is an offtake-related precursor, but not yet a final agreement.",
                    }
                ],
                "trajectory_score": {
                    "direction": "positive",
                    "event_delta": 2.0,
                    "validation_type": "watchlist_confirmatory_partial",
                },
            },
            "action_decision": {
                "action": "run_delta_only",
                "confidence": 0.8,
                "reason": "Meaningful update detected, but not enough to justify a full rerun yet.",
            },
        }

        row = ScenarioRouterObservability._summarize_event_payload(
            payload,
            path=Path("20260604_000000__evt-offtake.json"),
        )

        self.assertEqual(row["trajectory_state"], "thesis_strengthened")
        self.assertEqual(row["display"]["trajectory_label"], "Thesis strengthened")
        self.assertEqual(row["display"]["queue_bucket"], "positive_movement")
        self.assertEqual(row["display"]["review_status"], "tracking")
        self.assertEqual(row["display_adjustment"], "watchlist_engagement_projection")
        self.assertEqual(row["triggered_watchlist_count"], 1)
        self.assertEqual(row["trajectory_score"]["validation_type"], "watchlist_confirmatory_partial")
        self.assertEqual(row["trajectory_score"]["event_delta"], 2.0)
        self.assertEqual(row["watchlist_condition_checks"][0]["status"], "partial_match")
        self.assertIn("offtake", row["watchlist_condition_checks"][0]["label"].lower())

    def test_watchlist_projection_requires_structured_score_not_finding_prose(self):
        payload = {
            "status": "ok",
            "saved_at_utc": "2026-06-04T00:00:00Z",
            "event": {"event_id": "evt-offtake-stale", "ticker": "ASX:VMM"},
            "announcement_packet": {
                "event_id": "evt-offtake-stale",
                "ticker": "ASX:VMM",
                "title": "Offtake LoI",
                "source_type": "exchange_filing",
            },
            "baseline_run": {
                "run_id": "run-vmm",
                "lab_payload": {"structured_data": {"extended_analysis": {"current_thesis_state": {"leaning": "base"}}}},
            },
            "comparison_report": {
                "announcement_title": "Offtake LoI",
                "baseline_path": "base",
                "current_path": "base",
                "impact_level": "medium",
                "materiality": "medium",
                "trajectory_state": "no_thesis_change",
                "trajectory_effect": "no_clear_change",
                "thesis_effect": "no_change",
                "key_findings": [
                    {
                        "type": "confirmatory_partial_match",
                        "summary": "Partially engaged confirmatory: Binding Offtake Announcement",
                        "severity": "low",
                    }
                ],
                "condition_evaluations": [
                    {
                        "condition_id": "watch_binding_offtake",
                        "group": "confirmatory",
                        "label": "Binding Offtake Announcement",
                        "status": "partial_match",
                        "matched_via": "semantic",
                    }
                ],
            },
            "action_decision": {"action": "run_delta_only", "reason": "Old row text."},
        }

        row = ScenarioRouterObservability._summarize_event_payload(
            payload,
            path=Path("20260604_000000__evt-offtake-stale.json"),
        )

        self.assertEqual(row["trajectory_state"], "no_thesis_change")
        self.assertEqual(row["display_adjustment"], "")
        self.assertEqual(row["display"]["trajectory_label"], "No thesis impact")

    def test_explicit_neutral_watchlist_score_is_not_projected_to_thesis_movement(self):
        payload = {
            "status": "ok",
            "saved_at_utc": "2026-06-10T00:00:00Z",
            "event": {
                "event_id": "evt-incident",
                "ticker": "ASX:WWI",
                "received_at_utc": "2026-06-10T00:00:00Z",
            },
            "announcement_packet": {
                "event_id": "evt-incident",
                "ticker": "ASX:WWI",
                "title": "Incident Report",
                "source_type": "exchange_filing",
                "source_url": "https://announcements.asx.com.au/asxpdf/example.pdf",
            },
            "baseline_run": {
                "run_id": "run-wwi",
                "lab_payload": {
                    "structured_data": {
                        "extended_analysis": {"current_thesis_state": {"leaning": "bull"}}
                    }
                },
            },
            "comparison_report": {
                "ticker": "ASX:WWI",
                "baseline_run_id": "run-wwi",
                "announcement_title": "Incident Report",
                "baseline_path": "bull",
                "current_path": "bull",
                "impact_level": "medium",
                "materiality": "medium",
                "trajectory_state": "risk_increased",
                "trajectory_effect": "weakens",
                "price_time_effect": "The company does not anticipate a material impact to operations.",
                "thesis_effect": "undermines",
                "thesis_match_confidence": 0.9,
                "relationship_kind": "material_unmapped",
                "relationship_direction": "neutral",
                "key_findings": [],
                "condition_evaluations": [
                    {
                        "condition_id": "unexplained_delays_in_quarterly_production_ramp",
                        "scenario": "",
                        "group": "red_flag",
                        "label": "Unexplained delays in quarterly production ramp",
                        "status": "checked_not_triggered",
                        "matched_via": "model_thesis_judge",
                        "relationship": "checked_not_triggered",
                        "reason": "The filing says no material impact to operations.",
                        "evidence": {
                            "quote_excerpt": "does not anticipate a material impact to mining operations",
                            "source_url": "https://announcements.asx.com.au/asxpdf/example.pdf",
                        },
                    }
                ],
                "trajectory_score": {
                    "direction": "negative",
                    "event_delta": 0.0,
                    "unvalidated_event_delta": -2.0,
                    "validation_type": "related_unmapped",
                },
            },
            "action_decision": {
                "action": "run_delta_only",
                "confidence": 0.8,
                "reason": "A watchlist item was touched, but the model verdict scored no thesis movement.",
            },
        }

        row = ScenarioRouterObservability._summarize_event_payload(
            payload,
            path=Path("20260610_000000__evt-incident.json"),
        )

        self.assertEqual(row["trajectory_state"], "risk_increased")
        self.assertEqual(row["display"]["trajectory_label"], "Risk increased")
        self.assertEqual(row["display"]["queue_bucket"], "open_review")
        self.assertEqual(row["display_adjustment"], "")
        self.assertEqual(row["triggered_watchlist_count"], 0)
        self.assertEqual(row["checked_watchlist_count"], 1)
        self.assertEqual(row["triggered_watchlist"], [])
        self.assertEqual(row["display"]["evidence_label"], "Risk event outside thesis map")
        self.assertEqual(row["watchlist_condition_checks"][0]["group"], "red_flag")
        self.assertEqual(row["watchlist_condition_checks"][0]["status"], "checked_not_triggered")
        self.assertIn("does not anticipate", row["watchlist_condition_checks"][0]["evidence_quote"])
        self.assertEqual(row["trajectory_score"]["direction"], "negative")
        self.assertEqual(row["trajectory_score"]["event_delta"], 0.0)
        self.assertLess(row["trajectory_score"]["unvalidated_event_delta"], 0.0)


if __name__ == "__main__":
    unittest.main()
