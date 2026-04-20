import unittest

from backend.investment_synthesis import (
    _apply_energy_source_fact_guardrails,
    _build_stage3_source_fact_guardrails,
)


class Stage3EnergyGuardrailsTest(unittest.TestCase):
    def test_energy_guardrails_extract_hedges_and_production_baseline(self):
        context = """
        User Question: Run an investment analysis on Brookside Energy Ltd.

        Open hedge positions at quarter end: oil ~30,000 bbl (Jan-Jun 2026) at US$60.15/bbl; gas 60,000 MMBTU (Jan-Feb 2026) at US$3.82/MMBTU.
        Post-quarter additional gas hedges: 207,000 MMBTU (Apr-Dec 2026) at avg US$3.915/MMBTU.
        FY2025 net production: 1,790 BOEPD (presented as FY2025 Net Production).
        Production track record: nine horizontal wells drilled/completed and turned to sales, scaled from zero to ~2,000 BOEPD since 2020.
        Bruins well IP24 ~1,040 BOE/d, IP30 ~750 BOE/d.
        """

        guardrails = _build_stage3_source_fact_guardrails(
            context,
            template_id="energy_oil_gas",
        )

        self.assertIn("Hedging facts present", guardrails)
        self.assertIn("Open hedge positions", guardrails)
        self.assertIn("Production baseline facts present", guardrails)
        self.assertIn("1,790 BOEPD", guardrails)
        self.assertIn("do not call the company unhedged", guardrails)
        self.assertIn("do not use a production trigger below", guardrails)

    def test_hedging_gap_is_rewritten_when_sources_have_hedges(self):
        structured = {
            "verification_queue": [
                {
                    "field": "Hedging book standing",
                    "reason": "If hedged, downside protected; if unhedged, full commodity price exposure.",
                    "priority": "high",
                    "required_source": "Quarterly report notes",
                }
            ]
        }
        guardrails = """
        Hedging facts present in source packet:
        - Open hedge positions at quarter end: oil ~30,000 bbl (Jan-Jun 2026) at US$60.15/bbl.
        Instruction: do not call the company unhedged or hedging unknown.
        """

        _apply_energy_source_fact_guardrails(structured, guardrails)

        item = structured["verification_queue"][0]
        self.assertEqual(item["field"], "Residual hedge coverage / commodity exposure")
        self.assertIn("coverage ratio", item["reason"])
        self.assertIn("source_fact_guardrails", structured["council_metadata"])


if __name__ == "__main__":
    unittest.main()
