from pathlib import Path
import unittest

from backend.scenario_router.inbox_sentinel import InboxSentinel
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

        self.assertEqual(review["escalation_reason_label"], "Add missing thesis condition")
        self.assertEqual(review["next_action"], "update_thesis_map")
        self.assertEqual(updated["display"]["queue_bucket"], "cleared")
        self.assertEqual(updated["display"]["review_status"], "escalated")
        self.assertEqual(updated["display"]["review_label"], "Queued task")
        self.assertEqual(updated["display"]["queue_label"], "Cleared")
        self.assertEqual(updated["display"]["review_queue_label"], "Thesis-map queue")
        self.assertEqual(updated["display"]["review_reason_label"], "Add missing thesis condition")
        self.assertEqual(updated["display"]["next_action_label"], "Update thesis map")
        self.assertFalse(updated["display"]["is_user_action_required"])
        self.assertEqual(updated["display"]["tone"], "neutral")

    def test_display_contract_separates_trajectory_review_and_system_action(self):
        display = build_router_display_contract(
            {"trajectory_state": "material_unmapped", "impact_level": "medium"},
            {"action": "annotate_run", "reason": "Material filing without a mapped thesis condition."},
        )

        self.assertEqual(display["trajectory_label"], "Material filing outside thesis map")
        self.assertEqual(display["queue_bucket"], "open_review")
        self.assertEqual(display["queue_label"], "Needs thesis decision")
        self.assertEqual(display["review_status"], "open")
        self.assertEqual(display["review_label"], "Needs thesis decision")
        self.assertEqual(display["system_action_label"], "Attach to thesis log")
        self.assertTrue(display["is_user_action_required"])

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
        self.assertEqual(row["display"]["trajectory_label"], "Market backdrop only")
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
                    "direction": "neutral",
                    "event_delta": 0.0,
                    "validation_type": "mapped_condition",
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


if __name__ == "__main__":
    unittest.main()
