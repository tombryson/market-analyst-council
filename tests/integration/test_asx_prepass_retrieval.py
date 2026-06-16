import unittest

from backend.search import classify_asx_announcement
from test_perplexity_pdf_dump import (
    _build_price_sensitivity_assessment,
    _parse_asx_announcement_rows,
)
from test_pdf_dump_worker_summaries import _normalize_summary_object


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

    def test_worker_keeps_retrieval_price_sensitive_before_model_importance(self):
        doc = {
            "file_name": "03_2026-05-04_exploration_program_update.md",
            "title": "Exploration Program Update",
            "source_url": "https://announcements.asx.com.au/asxpdf/20260504/pdf/06z2lqtps2by3j.pdf",
            "full_text": "Strickland Metals ASX STK Exploration Program Update Shanac licence approval delay.",
            "document_ref": {
                "retrieval_meta": {
                    "price_sensitive_marker": True,
                    "price_sensitive_confidence": 0.86,
                }
            },
        }
        summary = {
            "price_sensitive": {"is_price_sensitive": False, "confidence": 0.2},
            "importance": {
                "is_important": False,
                "importance_score": 10,
                "keep_for_injection": False,
                "reason": "model_underweighted",
            },
            "summary": {
                "one_line": "Exploration program update notes a Shanac licence approval delay.",
                "key_points": ["Shanac licence approval delay affects drilling timing."],
            },
        }

        normalized = _normalize_summary_object(
            summary,
            doc,
            max_key_points=10,
            text_truncated=False,
        )

        self.assertTrue(normalized["price_sensitive"]["is_price_sensitive"])
        self.assertGreaterEqual(normalized["price_sensitive"]["confidence"], 0.86)
        self.assertTrue(normalized["importance"]["is_important"])
        self.assertTrue(normalized["importance"]["keep_for_injection"])
        self.assertGreaterEqual(normalized["importance"]["importance_score"], 82)


if __name__ == "__main__":
    unittest.main()
