import unittest

from backend.synthesis.synthesis import _ensure_structured_fields_for_template
from backend.timeline_normalization import normalize_timeline_rows, standardize_timeline_row


class TimelineNormalizationTests(unittest.TestCase):
    def test_strips_status_annotation_into_structured_fields(self):
        row = standardize_timeline_row(
            {
                "milestone": "Shanac MRE Update & Internal Scoping - AT RISK (Delayed by Serbian MoE approvals)",
                "target_period": "Q2 2026",
                "status": "planned",
            }
        )

        self.assertEqual(row["milestone"], "Shanac MRE Update and Internal Scoping")
        self.assertEqual(row["target_period"], "Q2 2026")
        self.assertEqual(row["status"], "at_risk")
        self.assertIn("Delayed by Serbian MoE approvals", row["primary_risk"])

    def test_normalizes_completed_and_period_labels(self):
        row = standardize_timeline_row(
            "Q1 2026: A$55M equity placement completed. Maiden Shanac Indicated MRE delivered. (COMPLETED)"
        )

        self.assertEqual(
            row["milestone"],
            "A$55M Equity Placement Completed. Maiden Shanac Indicated MRE Delivered",
        )
        self.assertEqual(row["target_period"], "Q1 2026")
        self.assertEqual(row["status"], "achieved")
        self.assertNotIn("COMPLETED", row["milestone"])

    def test_normalizes_mixed_range_and_keeps_risk_note_out_of_title(self):
        row = standardize_timeline_row(
            {
                "milestone": "Pre-Feasibility Study (PFS) Finalized - SPECULATIVE (Dependent on drill permit timeline).",
                "target_period": "Mid-2027",
            }
        )

        self.assertEqual(row["milestone"], "Pre-Feasibility Study (PFS) Finalized")
        self.assertEqual(row["target_period"], "H1 2027")
        self.assertEqual(row["status"], "at_risk")
        self.assertIn("Dependent on drill permit timeline", row["primary_risk"])

    def test_normalizes_run_examples_as_a_consistent_list(self):
        rows = normalize_timeline_rows(
            [
                {"milestone": "70,000m drill program and MT geophysics commence (On-track)", "target_period": "Q2 2026", "status": "planned"},
                {"milestone": "70,000m Drill Program Ramp Up - AT RISK", "target_period": "Q2-H2 2026", "status": "at_risk"},
                {"milestone": "Execution of 70,000m drill program across satellite tags (Obradov Potok) and Shanac upon approval. (PARTIALLY ON-TRACK)", "target_period": "Q2-Q4 2026", "status": "planned"},
            ]
        )

        self.assertEqual(rows[0]["milestone"], "70,000m Drill Program and MT Geophysics Commence")
        self.assertEqual(rows[0]["status"], "planned")
        self.assertEqual(rows[1]["milestone"], "70,000m Drill Program Ramp Up")
        self.assertEqual(rows[1]["target_period"], "Q2-H2 2026")
        self.assertEqual(rows[1]["status"], "at_risk")
        self.assertEqual(
            rows[2]["milestone"],
            "Execution of 70,000m Drill Program Across Satellite Tags (Obradov Potok) and Shanac Upon Approval",
        )
        self.assertEqual(rows[2]["status"], "planned")

    def test_synthesis_timeline_recovery_is_not_template_gated(self):
        structured_data = {
            "development_timeline": [],
            "current_development_stage": "",
        }
        chairman_text = """
        <development_timeline>
        - Q2 2026: Placement completed
        - H2 2026: FDA decision expected
        </development_timeline>
        """

        _ensure_structured_fields_for_template(
            structured_data,
            "consumer_retail",
            chairman_text=chairman_text,
        )

        self.assertEqual(len(structured_data["development_timeline"]), 2)
        self.assertEqual(
            structured_data["development_timeline"][0]["milestone"],
            "Placement",
        )
        self.assertEqual(structured_data["development_timeline"][0]["target_period"], "Q2 2026")
        self.assertEqual(structured_data["development_timeline"][0]["status"], "achieved")


if __name__ == "__main__":
    unittest.main()
