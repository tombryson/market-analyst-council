import unittest

from backend.template_loader import get_template_loader


class TemplatePromptFallbackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loader = get_template_loader()

    def test_software_prompt_fallback_renders_full_prompt(self):
        prompt = self.loader.render_template_rubric(
            "software_saas",
            company_name="Life360 Inc",
            exchange="asx",
        )
        self.assertIn("Life360 Inc", prompt)
        self.assertIn("Quality Score out of 100", prompt)
        self.assertIn("Bull / base / bear scenario framework", prompt)

    def test_bank_prompt_fallback_renders_full_prompt(self):
        prompt = self.loader.render_template_rubric(
            "bank_financials",
            company_name="Commonwealth Bank of Australia",
            exchange="asx",
        )
        self.assertIn("Commonwealth Bank of Australia", prompt)
        self.assertIn("using this bank and lender rubric", prompt)
        self.assertIn("Step 1: Franchise, Funding, and Balance-Sheet Workup", prompt)
        self.assertIn("Investment recommendation (BUY/HOLD/SELL)", prompt)


if __name__ == "__main__":
    unittest.main()
