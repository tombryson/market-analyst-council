import unittest

from backend.scenario_router.mock_harness import run_mock_router_case, run_mock_router_cases


class ScenarioRouterMockHarnessTests(unittest.TestCase):
    def test_custom_thesis_map_can_be_tested_thesis_by_thesis(self):
        result = run_mock_router_case(
            {
                "case_id": "permit_approval_bull_hit",
                "ticker": "ASX:MOCK",
                "template_id": "rare_earths_critical_minerals",
                "baseline_path": "base",
                "use_legacy_interpreter": True,
                "title": "Mining Permit Approval Received",
                "summary": "The regulator granted the mining permit approval ahead of schedule.",
                "extracted_facts": [
                    "Mining permit approval was granted ahead of schedule.",
                    "The company said approvals unlock the next phase of drilling and development.",
                ],
                "thesis_map": {
                    "bull": {
                        "required_conditions": [
                            {
                                "condition_id": "bull_permit_approval",
                                "condition": "Mining permit approval received ahead of schedule",
                                "evidence_hooks": ["permit approval was granted ahead of schedule"],
                            }
                        ]
                    },
                    "base": {
                        "required_conditions": [
                            {
                                "condition_id": "base_waiting_for_permits",
                                "condition": "Permits remain pending",
                                "evidence_hooks": ["permits remain pending"],
                            }
                        ]
                    },
                    "bear": {
                        "failure_conditions": [
                            {
                                "condition_id": "bear_permit_rejected",
                                "condition": "Permit rejected or withdrawn",
                                "evidence_hooks": ["permit was rejected"],
                            }
                        ]
                    },
                },
                "expected": {
                    "current_path": "bull",
                    "action": "rerun_stage1",
                    "impact_level": "medium",
                    "scenario_hits": {
                        "bull": {"required": ["bull_permit_approval"]},
                        "bear": {"failure": []},
                    },
                    "condition_statuses": {
                        "bull_permit_approval": "matched",
                        "base_waiting_for_permits": "not_matched",
                        "bear_permit_rejected": "not_matched",
                    },
                    "confidence_min": {"classification_confidence": 0.5},
                },
            }
        )

        self.assertTrue(result["passed"], result["assertions"])
        self.assertEqual(result["scenario_results"]["bull"]["matched_required"], ["bull_permit_approval"])
        self.assertEqual(result["actual"]["current_path"], "bull")
        self.assertEqual(result["actual"]["matched_condition_ids"], ["bull_permit_approval"])

    def test_batch_mock_cases_report_pass_rate(self):
        suite = run_mock_router_cases(
            [
                {
                    "case_id": "buyback_no_change",
                    "ticker": "ASX:BRK",
                    "template_id": "energy_oil_gas",
                    "baseline_path": "base",
                    "use_legacy_interpreter": True,
                    "title": "Update - Notification of buy-back - BRK",
                    "summary": "The company lodged an update to its on-market share buy-back notification.",
                    "expected": {
                        "announcement_class": "capital_management",
                        "trajectory_state": "no_thesis_change",
                        "action": "ignore",
                    },
                },
                {
                    "case_id": "unknown_needs_classification",
                    "ticker": "ASX:ABC",
                    "template_id": "general",
                    "baseline_path": "base",
                    "use_legacy_interpreter": True,
                    "title": "Corporate Update",
                    "summary": "The company provides an update for shareholders. " * 20,
                    "thesis_map": {
                        "bull": {"required_conditions": ["Major new contract signed"]},
                        "base": {"required_conditions": ["Revenue remains stable"]},
                        "bear": {"required_conditions": ["Customer churn accelerates"]},
                    },
                    "expected": {
                        "announcement_class": "needs_classification",
                        "trajectory_state": "needs_classification",
                    },
                },
            ]
        )

        self.assertEqual(suite["total_cases"], 2)
        self.assertEqual(suite["passed_cases"], 2)
        self.assertEqual(suite["failed_cases"], 0)


if __name__ == "__main__":
    unittest.main()
