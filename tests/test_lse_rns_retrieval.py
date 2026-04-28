import asyncio
import unittest

import httpx

from backend.template_loader import get_template_loader
from test_perplexity_pdf_dump import (
    _classify_lse_rns,
    _discover_investegate_rns_sources,
    _parse_investegate_company_rows,
    _resolve_exchange_profile,
)


INVESTEGATE_SAMPLE = """
<html>
  <body>
    <table class="table-investegate">
      <tr><th>Date</th><th>Time</th><th>Source</th><th>Announcement</th></tr>
      <tr>
        <td>24 Apr 2026</td><td>09:08 AM</td><td>RNS</td>
        <td><a href="/announcement/rns/filtronic--ftc/director-pdmr-shareholding/9537100">Director/PDMR Shareholding</a></td>
      </tr>
      <tr>
        <td>01 Apr 2026</td><td>07:00 AM</td><td>RNS</td>
        <td><a href="/announcement/rns/filtronic--ftc/new-contract-with-major-european-defence-prime/9501949">New contract with major European defence prime</a></td>
      </tr>
      <tr>
        <td>10 Feb 2026</td><td>07:00 AM</td><td>RNS</td>
        <td><a href="/announcement/rns/filtronic--ftc/interim-results/9403379">Interim Results</a></td>
      </tr>
    </table>
  </body>
</html>
"""


class LseRnsRetrievalTests(unittest.TestCase):
    def test_lse_profile_does_not_use_asx_defaults(self):
        params = get_template_loader().get_exchange_retrieval_params("lse")
        self.assertEqual(params["price_sensitive_strategy"], "lse_rns_deterministic_latest")
        self.assertIn("investegate.co.uk", params["allowed_domain_suffixes"])
        self.assertNotIn("asx.com.au", params["allowed_domain_suffixes"])
        self.assertGreater(params["target_price_sensitive_default"], 0)

    def test_resolved_lse_profile_stays_lse_specific(self):
        params = _resolve_exchange_profile(
            query="Latest material filings for Filtronic plc",
            ticker="LSE:FTC",
            explicit_exchange="lse",
        )
        self.assertEqual(params["exchange"], "lse")
        self.assertIn("investegate.co.uk", params["allowed_domain_suffixes"])
        self.assertNotIn("marketindex.com.au", params["allowed_domain_suffixes"])

    def test_investegate_rows_parse_dates_and_materiality(self):
        rows = _parse_investegate_company_rows(
            INVESTEGATE_SAMPLE,
            "https://www.investegate.co.uk/company/FTC",
        )
        self.assertEqual(len(rows), 3)
        by_title = {row["title"]: row for row in rows}
        self.assertEqual(by_title["Interim Results"]["published_at"], "2026-02-10")
        self.assertTrue(by_title["New contract with major European defence prime"]["price_sensitive"])
        self.assertFalse(by_title["Director/PDMR Shareholding"]["price_sensitive"])
        self.assertEqual(by_title["Director/PDMR Shareholding"]["category"], "low_signal_admin")

    def test_investegate_discovery_builds_deterministic_sources(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if str(request.url).startswith("https://www.investegate.co.uk/company/FTC"):
                return httpx.Response(200, text=INVESTEGATE_SAMPLE)
            return httpx.Response(404, text="")

        async def run():
            transport = httpx.MockTransport(handler)
            async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
                return await _discover_investegate_rns_sources(
                    client,
                    symbol="FTC",
                    lookback_days=365,
                    max_rows=10,
                    max_pages=1,
                )

        rows = asyncio.run(run())
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["deterministic_source_kind"] == "lse_investegate_rns" for row in rows))
        self.assertTrue(all(row["issuer_validation"]["status"] == "match" for row in rows))
        self.assertTrue(any(row["price_sensitive_seed"] for row in rows))
        self.assertTrue(any("interim-results" in row["url"] for row in rows))

    def test_lse_title_classifier_downranks_low_signal_notices(self):
        self.assertEqual(_classify_lse_rns("Holding(s) in Company", "RNS"), (False, 4, "low_signal_admin"))
        self.assertEqual(_classify_lse_rns("Final Results", "RNS"), (True, 1, "material_rns"))
        self.assertEqual(
            _classify_lse_rns("Major European defence prime selects Filtronic", "RNS"),
            (True, 1, "material_rns"),
        )


if __name__ == "__main__":
    unittest.main()
