import unittest

from backend.scenario_router.document_reader import DocumentReader


class ScenarioRouterTopicTests(unittest.TestCase):
    def test_detects_commercial_customer_product_topics(self):
        facts = [
            "The company signed a three-year customer contract with a major enterprise client.",
            "The software platform launch is expected to expand subscriber adoption.",
        ]

        topics = DocumentReader._infer_material_topics("", facts)

        self.assertIn("commercial", topics)
        self.assertIn("customer", topics)
        self.assertIn("product", topics)

    def test_resource_topics_still_work_for_mining_filings(self):
        facts = [
            "The company reported a JORC mineral resource update and higher reserve confidence.",
            "Drilling results confirmed additional mineralisation near the existing resource.",
        ]

        topics = DocumentReader._infer_material_topics("", facts)

        self.assertIn("resource", topics)


if __name__ == "__main__":
    unittest.main()
