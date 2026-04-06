import unittest

from backend.research.pharma_biotech_supplementary import (
    PIPELINE_ID as PHARMA_PIPELINE_ID,
    CHECKLIST_ITEMS as PHARMA_CHECKLIST_ITEMS,
    build_discovery_prompt as build_pharma_discovery_prompt,
    get_pipeline_spec as get_pharma_pipeline_spec,
    resolve_pharma_biotech_enricher_context,
)
from backend.research.supplementary_registry import (
    get_pipeline_spec,
    resolve_pipeline_id_for_template,
    resolve_pipeline_spec_for_template,
)


class SupplementaryRegistryTests(unittest.TestCase):
    def test_resources_pipeline_routes_from_gold_template(self):
        self.assertEqual(resolve_pipeline_id_for_template("gold_miner"), "resources_supplementary")
        spec = resolve_pipeline_spec_for_template("gold_miner")
        self.assertIsNotNone(spec)
        self.assertEqual(spec.pipeline_id, "resources_supplementary")

    def test_pharma_pipeline_routes_from_template_contract(self):
        self.assertEqual(resolve_pipeline_id_for_template("pharma_biotech"), PHARMA_PIPELINE_ID)
        spec = get_pipeline_spec(PHARMA_PIPELINE_ID)
        self.assertIsNotNone(spec)
        self.assertEqual(spec.pipeline_id, PHARMA_PIPELINE_ID)
        self.assertIn("pharma_biotech", spec.template_ids)

    def test_pharma_pipeline_scaffold_has_expected_shape(self):
        spec = get_pharma_pipeline_spec()
        self.assertEqual(spec.pipeline_id, PHARMA_PIPELINE_ID)
        self.assertEqual(len(spec.segment_definitions), 3)
        self.assertGreaterEqual(len(PHARMA_CHECKLIST_ITEMS), 30)
        prompt = build_pharma_discovery_prompt(company="Clarity Pharma", ticker="CU6", exchange="ASX")
        self.assertIn("ASSET_CLASS: pharma_biotech", prompt)
        self.assertNotIn("COMMODITY:", prompt)

    def test_pharma_context_resolution_uses_canonical_company_name(self):
        resolved = resolve_pharma_biotech_enricher_context(
            user_query="Run full analysis on Neuren",
            ticker="ASX:NEU",
        )
        self.assertEqual(resolved["exchange"], "ASX")
        self.assertEqual(resolved["ticker_symbol"], "NEU")
        self.assertEqual(resolved["display_ticker"], "ASX:NEU")
        self.assertTrue(resolved["company"])


if __name__ == "__main__":
    unittest.main()
