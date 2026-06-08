import unittest

from backend.scenario_router.fixture_suite import load_router_fixture_suite, run_router_fixture_suite


class ScenarioRouterFixtureSuiteTests(unittest.TestCase):
    def test_backwards_designed_fixture_compiles_company_into_cases(self):
        suite = load_router_fixture_suite("atlas_operating_co_suite")

        self.assertEqual(suite["suite_id"], "atlas_operating_co_suite")
        self.assertEqual(suite["company_id"], "atlas_operating_co")
        self.assertEqual(len(suite["cases"]), 10)

        first_case = suite["cases"][0]
        self.assertEqual(first_case["ticker"], "TEST:ATLAS")
        self.assertEqual(first_case["baseline_path"], "base")
        self.assertEqual(first_case["development_timeline"][0]["target_period"], "Q3 2026")
        self.assertEqual(first_case["catalyst_rows"][0]["title"], "Zone A regulatory approval")
        self.assertIn("bull_zone_a_approval_ahead", _condition_ids(first_case, "bull", "required_conditions"))
        self.assertIn("bear_zone_a_approval_delayed", _condition_ids(first_case, "bear", "required_conditions"))

    def test_backwards_designed_fixture_suite_routes_expected_paths(self):
        result = run_router_fixture_suite("atlas_operating_co_suite")

        self.assertEqual(result["total_cases"], 10)
        self.assertEqual(result["failed_cases"], 0, _failed_assertions(result))
        self.assertEqual(result["pass_rate_pct"], 100.0)

        by_id = {item["case_id"].split("__", 1)[1]: item for item in result["results"]}
        self.assertEqual(by_id["bull_regulatory_approval_ahead"]["actual"]["current_path"], "bull")
        self.assertEqual(by_id["bear_regulatory_delay"]["actual"]["current_path"], "bear")
        self.assertEqual(by_id["base_regulatory_pending"]["actual"]["current_path"], "base")
        self.assertEqual(by_id["administrative_appendix"]["actual"]["user_bucket"], "administrative")
        self.assertEqual(by_id["administrative_appendix"]["actual"]["display"]["queue_bucket"], "administrative")
        self.assertEqual(by_id["material_unmapped_acquisition"]["actual"]["trajectory_state"], "material_unmapped")
        self.assertEqual(by_id["material_unmapped_acquisition"]["actual"]["display"]["queue_bucket"], "open_review")
        self.assertEqual(by_id["bull_regulatory_approval_ahead"]["actual"]["display"]["trajectory_label"], "Timeline accelerated")
        self.assertEqual(by_id["bull_regulatory_approval_ahead"]["actual"]["display"]["queue_bucket"], "open_review")
        self.assertEqual(by_id["bull_regulatory_approval_ahead"]["actual"]["display"]["review_reason"], "verification_hit")
        self.assertEqual(by_id["bear_regulatory_delay"]["actual"]["display"]["queue_bucket"], "open_review")
        self.assertEqual(by_id["base_regulatory_pending"]["actual"]["display"]["queue_bucket"], "cleared")
        self.assertEqual(by_id["false_positive_approval_not_granted"]["actual"]["matched_condition_ids"], ["base_zone_a_approval_pending"])
        self.assertEqual(by_id["false_positive_facility_not_expanded"]["actual"]["matched_condition_ids"], ["base_facility_available"])
        self.assertEqual(by_id["false_positive_customer_expansion_not_signed"]["actual"]["matched_condition_ids"], [])
        self.assertEqual(by_id["mixed_approval_granted_with_cost_pressure"]["actual"]["current_path"], "bull")


def _condition_ids(case, scenario, group):
    return [
        item["condition_id"]
        for item in case["thesis_map"][scenario][group]
    ]


def _failed_assertions(result):
    rows = []
    for case in result.get("results") or []:
        failed = [item for item in case.get("assertions") or [] if not item.get("passed")]
        if failed:
            rows.append({"case_id": case.get("case_id"), "failed": failed, "actual": case.get("actual")})
    return rows


if __name__ == "__main__":
    unittest.main()
