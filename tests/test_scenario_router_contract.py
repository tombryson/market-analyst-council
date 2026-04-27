from pathlib import Path
import unittest

from backend.scenario_router.inbox_sentinel import InboxSentinel
from backend.scenario_router.observability import ScenarioRouterObservability
from backend.scenario_router.display_contract import MARKET_ONLY_WATCH_REASON


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
            "baseline_run": {"run_id": "run-1"},
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
        self.assertEqual(row["impact_level"], "low")
        self.assertEqual(row["current_path"], "bull")
        self.assertEqual(row["path_transition"], "")
        self.assertEqual(row["action_reason"], MARKET_ONLY_WATCH_REASON)
        self.assertEqual(row["invalidated_sections"], [])
        self.assertEqual(row["affected_domains"], [])
        self.assertNotIn("full rerun", " ".join(row["follow_up_steps"]).lower())


if __name__ == "__main__":
    unittest.main()
