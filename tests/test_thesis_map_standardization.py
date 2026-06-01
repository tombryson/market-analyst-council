import unittest

from backend.investment_synthesis import (
    _ensure_structured_fields_for_template,
    _extract_embedded_thesis_fields,
    _normalize_condition_entries,
)


PACKED_BULL_THESIS = (
    "Permits arrive and drilling restarts on schedule. "
    "Target 12m: A$0.18 | Target 24m: A$0.25 | Probability 24m: 20% "
    "Required Conditions: Serbian ministry approval within 3 months; "
    "Shanac inferred ounces convert to indicated; gold remains above US$3,500/oz. "
    "Failure Conditions: approval slips beyond the field season; assays miss target grade. "
    "Current Positioning: Bear-leaning in near-term due to permit delays. "
    "Why Current Positioning: Value exists, but actual momentum is frozen by regulator timing."
)


class ThesisMapStandardizationTests(unittest.TestCase):
    def test_extracts_embedded_thesis_fields_from_packed_scenario_text(self):
        fields = _extract_embedded_thesis_fields(PACKED_BULL_THESIS)

        self.assertEqual(fields["summary"], "Permits arrive and drilling restarts on schedule")
        self.assertEqual(fields["target_12m"], 0.18)
        self.assertEqual(fields["target_24m"], 0.25)
        self.assertEqual(fields["probability_24m_pct"], 20)
        self.assertEqual(
            fields["required_conditions"],
            [
                "Serbian ministry approval within 3 months",
                "Shanac inferred ounces convert to indicated",
                "gold remains above US$3,500/oz",
            ],
        )
        self.assertEqual(
            fields["failure_conditions"],
            ["approval slips beyond the field season", "assays miss target grade"],
        )
        self.assertEqual(fields["current_positioning"], "bear-leaning")
        self.assertIn("momentum is frozen", fields["why_current_positioning"])

    def test_packed_scenario_text_is_not_accepted_as_a_single_condition(self):
        normalized = _normalize_condition_entries(
            [{"condition": PACKED_BULL_THESIS}],
            scenario="bull",
            prefix="required",
        )

        self.assertEqual(normalized, [])

    def test_structured_field_pass_splits_packed_thesis_map(self):
        payload = {
            "investment_verdict": {
                "rating": "HOLD",
                "conviction": "MEDIUM",
                "top_reasons": ["large gold inventory"],
                "failure_conditions": ["permit delay extends"],
            },
            "price_targets": {
                "scenario_targets": {"24m": {"bull": 0.25, "base": 0.15, "bear": 0.07}},
                "scenario_probabilities": {"24m": {"bull": 0.2, "base": 0.55, "bear": 0.25}},
                "scenario_drivers": {"24m": {"base": ["permits clear but drilling is slow"]}},
            },
            "thesis_map": {
                "bull": {
                    "summary": PACKED_BULL_THESIS,
                    "required_conditions": [{"condition": PACKED_BULL_THESIS}],
                    "failure_conditions": [],
                    "current_positioning": "mixed",
                    "why_current_positioning": "mixed toward positioning.",
                }
            },
        }

        _ensure_structured_fields_for_template(
            payload,
            template_id="resources_gold_monometallic",
            chairman_text="",
        )
        bull = payload["thesis_map"]["bull"]

        self.assertEqual(bull["summary"], "Permits arrive and drilling restarts on schedule")
        self.assertEqual(bull["target_12m"], 0.18)
        self.assertEqual(bull["target_24m"], 0.25)
        self.assertEqual(bull["probability_24m_pct"], 20)
        self.assertEqual(
            [item["condition"] for item in bull["required_conditions"]],
            [
                "Serbian ministry approval within 3 months",
                "Shanac inferred ounces convert to indicated",
                "gold remains above US$3,500/oz",
            ],
        )
        self.assertEqual(
            [item["condition"] for item in bull["failure_conditions"]],
            ["approval slips beyond the field season", "assays miss target grade"],
        )
        self.assertEqual(bull["current_positioning"], "bear-leaning")
        self.assertIn("momentum is frozen", bull["why_current_positioning"])


if __name__ == "__main__":
    unittest.main()
