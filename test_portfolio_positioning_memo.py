import unittest

from portfolio_positioning_memo import (
    _build_asset_class_vocabulary,
    _build_research_query,
    _compact_snapshot,
    _merge_positioning_with_snapshot,
    _normalize_evidence_asset_coverage,
    _render_markdown,
)


class PortfolioPositioningMemoTests(unittest.TestCase):
    def _context(self):
        return {
            "portfolio": {
                "total_value": 100000,
                "cash_pct": 3.27,
                "cash_value": 3270,
                "holdings_count": 5,
            },
            "available_asset_classes": [
                {"asset_class": "cash", "display_name": "Cash"},
                {"asset_class": "physical_gold", "display_name": "Physical Gold"},
                {"asset_class": "gold_miners", "display_name": "Gold Miners"},
                {"asset_class": "physical_silver", "display_name": "Physical Silver"},
                {"asset_class": "silver_miners", "display_name": "Silver Miners"},
                {"asset_class": "copper_miners", "display_name": "Copper Miners"},
                {"asset_class": "lithium_miners", "display_name": "Lithium Miners"},
                {
                    "asset_class": "rare_earths_critical_minerals",
                    "display_name": "Rare Earths & Critical Minerals",
                },
                {"asset_class": "consumer_staples", "display_name": "Consumer Staples"},
            ],
            "asset_classes": [
                {
                    "asset_class": "gold_miners",
                    "display_name": "Gold Miners",
                    "portfolio_pct": 22.88,
                    "value": 22880,
                },
                {
                    "asset_class": "silver_miners",
                    "display_name": "Silver Miners",
                    "portfolio_pct": 9.29,
                    "value": 9290,
                },
                {
                    "asset_class": "copper_miners",
                    "display_name": "Copper Miners",
                    "portfolio_pct": 2.39,
                    "value": 2390,
                },
                {
                    "asset_class": "consumer_staples",
                    "display_name": "Consumer Staples",
                    "portfolio_pct": 1.54,
                    "value": 1540,
                },
            ],
        }

    def test_research_query_no_longer_globally_blind(self):
        query = _build_research_query("Position my portfolio")
        self.assertNotIn("Do not anchor to any existing portfolio structure", query)
        self.assertIn("portfolio-aware allocator lanes", query)

    def test_coverage_is_seeded_from_implications_and_commodities(self):
        snapshot = _compact_snapshot(self._context())
        vocabulary = _build_asset_class_vocabulary(snapshot)
        evidence = _normalize_evidence_asset_coverage(
            {
                "asset_class_implications": [
                    {
                        "asset_class": "physical_gold",
                        "stance": "OVERWEIGHT",
                        "reason": "Gold hedge evidence is explicit.",
                    }
                ],
                "commodity_prices": [
                    {
                        "commodity": "Copper",
                        "price_context": "Copper is elevated.",
                        "portfolio_implication": "Supports copper-linked exposure.",
                    }
                ],
            },
            asset_class_vocabulary=vocabulary,
        )
        rows = {row["asset_class"]: row for row in evidence["asset_class_coverage"]}
        self.assertEqual(rows["physical_gold"]["evidence_strength"], "MEDIUM")
        self.assertNotEqual(rows["copper_miners"]["evidence_strength"], "NONE")
        self.assertEqual(evidence["asset_class_coverage_quality"]["status"], "ok")

    def test_merge_keeps_cash_and_current_sleeve_decisions(self):
        snapshot = _compact_snapshot(self._context())
        evidence = {
            "asset_class_coverage": [],
            "asset_class_coverage_quality": {"status": "failed_all_none"},
        }
        macro_positioning = {
            "analysis_date": "2026-05-17T00:00:00Z",
            "executive_summary": "Test memo.",
            "strategic_view": {"cash_target_pct": 10, "cash_role": "Dry powder."},
            "asset_class_targets": [
                {
                    "asset_class": "physical_gold",
                    "display_name": "Physical Gold",
                    "min_pct": 8,
                    "target_pct": 12,
                    "max_pct": 16,
                    "thesis_role": "hedge",
                    "rationale": "Clean monetary hedge.",
                },
                {
                    "asset_class": "copper_miners",
                    "display_name": "Copper Miners",
                    "min_pct": 3,
                    "target_pct": 5,
                    "max_pct": 8,
                    "thesis_role": "tactical",
                    "rationale": "Critical-materials exposure.",
                },
            ],
            "current_sleeve_actions": [
                {
                    "asset_class": "gold_miners",
                    "display_name": "Gold Miners",
                    "current_pct": 22.88,
                    "action": "HOLD",
                    "related_or_substitute_exposure": "physical_gold",
                    "rationale": "Already expresses part of the precious-metals thesis.",
                }
            ],
        }
        structured = _merge_positioning_with_snapshot(
            snapshot=snapshot,
            macro_positioning=macro_positioning,
            evidence_brief=evidence,
            query="test",
            mode="deep",
        )
        targets = {row["asset_class"]: row for row in structured["asset_class_targets"]}
        self.assertEqual(targets["cash"]["current_pct"], 3.27)
        decisions = {row["asset_class"]: row for row in structured["current_sleeve_decisions"]}
        self.assertIn("gold_miners", decisions)
        self.assertIn("silver_miners", decisions)
        self.assertEqual(decisions["gold_miners"]["action"], "HOLD")
        self.assertTrue(
            any("Evidence coverage failed" in item for item in structured["risk_flags"])
        )

    def test_render_markdown_uppercases_human_asset_labels(self):
        snapshot = _compact_snapshot(self._context())
        structured = {
            "analysis_date": "2026-05-17T00:00:00Z",
            "mode": "deep",
            "portfolio_diagnosis": {},
            "strategic_view": {},
            "asset_class_targets": [
                {
                    "asset_class": "fixed_income",
                    "display_name": "fixed_income",
                    "current_pct": 0,
                    "min_pct": 10,
                    "target_pct": 15,
                    "max_pct": 20,
                    "action": "ADD",
                    "conviction": "MEDIUM",
                    "rationale": "Duration ballast.",
                }
            ],
            "current_sleeve_decisions": [
                {
                    "asset_class": "gold_miners",
                    "display_name": "gold_miners",
                    "current_pct": 5,
                    "action": "HOLD",
                    "rationale": "Precious-metals beta.",
                }
            ],
        }
        markdown = _render_markdown(
            snapshot=snapshot,
            structured=structured,
            evidence_brief={},
            citations=[],
        )
        self.assertIn("| FIXED INCOME |", markdown)
        self.assertIn("| GOLD MINERS |", markdown)
        self.assertNotIn("| fixed_income |", markdown)


if __name__ == "__main__":
    unittest.main()
