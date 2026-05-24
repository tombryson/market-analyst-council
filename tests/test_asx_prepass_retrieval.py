import unittest

from backend.search import classify_asx_announcement
from test_perplexity_pdf_dump import (
    _build_price_sensitivity_assessment,
    _parse_asx_announcement_rows,
)


ASX_SAMPLE = """
<table>
  <tr>
    <td>04/05/2026<br><span>8:23 AM</span></td>
    <td>
      <span class="icon-price-sensitive" title="Price sensitive"></span>
      <a href="/asx/v2/statistics/displayAnnouncement.do?display=pdf&idsId=02949123">
        Exploration Program Update<br>
      </a>
    </td>
  </tr>
</table>
"""


class AsxPrepassRetrievalTests(unittest.TestCase):
    def test_exploration_program_update_is_material_asx_title(self):
        category, priority = classify_asx_announcement(
            "Exploration Program Update",
            "https://announcements.asx.com.au/asxpdf/20260504/pdf/06a1323802.pdf",
        )
        self.assertEqual(category, "important")
        self.assertEqual(priority, 2)

    def test_official_asx_parser_carries_price_sensitive_icon(self):
        rows = _parse_asx_announcement_rows(ASX_SAMPLE)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["published_at"], "2026-05-04")
        self.assertTrue(rows[0]["price_sensitive"])

    def test_asx_price_sensitivity_uses_material_title_tokens(self):
        ps = _build_price_sensitivity_assessment(
            exchange_id="asx",
            title="Exploration Program Update",
            source_title="Exploration Program Update",
            source_snippet="Deterministic ASX direct announcement search lane. price-sensitive asx announcement.",
            source_url="https://www.asx.com.au/asx/v2/statistics/displayAnnouncement.do?idsId=02949123",
            pdf_url="https://announcements.asx.com.au/asxpdf/20260504/pdf/06a1323802.pdf",
            ii_price_sensitive_marker=False,
            token_marker=True,
        )
        self.assertTrue(ps["is_price_sensitive"])
        self.assertIn("asx_material_title", ps["reason_codes"])
        self.assertGreaterEqual(ps["confidence"], 0.76)


if __name__ == "__main__":
    unittest.main()
