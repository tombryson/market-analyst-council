import unittest

from backend.council import (
    _build_stage2_reconciliation_prompt,
    _normalize_stage2_reconciliation_payload,
)


class Stage2ReconciliationTests(unittest.TestCase):
    def test_normalizer_preserves_topic_override(self) -> None:
        payload = _normalize_stage2_reconciliation_payload(
            {
                "status": "no_material_issues",
                "topic_overrides": [
                    {
                        "topic": "production baseline",
                        "issue": "Top-ranked response used stale production threshold.",
                        "source_resolved_position": "Primary context says current output already exceeds that threshold.",
                        "prefer_models": ["model-low-ranked"],
                        "downweight_models": ["model-top-ranked"],
                        "stage3_instruction": "Use the current source-supported production baseline.",
                        "confidence": 87,
                    }
                ],
                "stage3_constraints": [
                    "Do not describe a surpassed production level as a future bull trigger."
                ],
            }
        )

        self.assertEqual(payload["status"], "issues_found")
        self.assertEqual(payload["topic_overrides"][0]["prefer_models"], ["model-low-ranked"])
        self.assertEqual(payload["topic_overrides"][0]["downweight_models"], ["model-top-ranked"])
        self.assertEqual(payload["topic_overrides"][0]["confidence"], 0.87)
        self.assertIn("surpassed production", payload["stage3_constraints"][0])

    def test_prompt_instructs_topic_specific_ranking_override(self) -> None:
        prompt = _build_stage2_reconciliation_prompt(
            source_context="Primary filing says the company is unhedged.",
            responses_text="model-a says hedging status is a data gap.",
            rankings_text="1. model-a\n2. model-b",
        )

        self.assertIn("lower-ranked response", prompt)
        self.assertIn("Prefer primary/prepass context over peer ranking", prompt)
        self.assertIn("Return JSON only", prompt)


if __name__ == "__main__":
    unittest.main()
