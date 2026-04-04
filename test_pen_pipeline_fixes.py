import unittest
import sys
import types
from unittest.mock import AsyncMock, patch

if "liteparse" not in sys.modules:
    liteparse_module = types.ModuleType("liteparse")

    class _LiteParse:  # pragma: no cover - import stub only
        def parse(self, *args, **kwargs):
            raise RuntimeError("liteparse stub should not be used in unit tests")

    liteparse_module.LiteParse = _LiteParse
    sys.modules["liteparse"] = liteparse_module
    liteparse_types_module = types.ModuleType("liteparse.types")

    class _ParseError(Exception):
        pass

    liteparse_types_module.ParseError = _ParseError
    sys.modules["liteparse.types"] = liteparse_types_module

from backend.company_type_detector import (
    _score_text_against_company_types,
    detect_company_type_via_api,
)
from backend.council import (
    _assess_stage1_truncation,
    _stage1_response_looks_truncated,
    apply_stage2_revision_deltas,
    stage2_collect_rankings,
    stage2_collect_revision_deltas,
)
from backend.investment_synthesis import (
    _derive_current_stage_from_timeline_rows,
    _extract_development_timeline_from_text,
    _extract_headwinds_tailwinds_from_text,
    _inject_stage3_audit_context,
)
from backend.market_facts import gather_market_facts_prepass
from backend.prepass_utils import normalize_retrieval_query_seed, tail_text
from backend.template_loader import TemplateLoader
from test_pdf_dump_worker_summaries import (
    _detect_issuer_alignment,
    _heuristic_summary_from_doc,
)
from test_perplexity_pdf_dump import _run_pre_stage1_contamination_gate


class TemplateSelectionFixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.loader = TemplateLoader()

    def test_resolve_template_selection_strips_analysis_prefix_from_company_name(self):
        selection = self.loader.resolve_template_selection(
            user_query="Run full analysis on Peninsula Energy Ltd",
            ticker="ASX:PEN",
            exchange="ASX",
        )
        self.assertEqual(selection.get("company_name"), "Peninsula Energy Ltd")

    def test_primary_injection_query_seed_prefers_clean_company_name(self):
        seed = normalize_retrieval_query_seed(
            company_name="Polymetals Resources Limited",
            query_hint="Run full analysis on Polymetals Resources Limited (ASX:POL)",
            ticker="ASX:POL",
        )
        self.assertEqual(seed, "Polymetals Resources Limited")

    def test_primary_injection_query_seed_strips_analysis_prefix_when_company_name_missing(self):
        seed = normalize_retrieval_query_seed(
            company_name="",
            query_hint="Run full analysis on Peninsula Energy Ltd (ASX:PEN)",
            ticker="ASX:PEN",
        )
        self.assertEqual(seed, "Peninsula Energy Ltd")

    def test_pol_assignment_routes_to_silver_miner(self):
        selection = self.loader.resolve_template_selection(
            user_query="Run full analysis on Polymetals Resources Limited (ASX:POL)",
            ticker="ASX:POL",
            exchange="ASX",
        )
        self.assertEqual(selection.get("company_type"), "silver_miner")
        self.assertEqual(selection.get("template_id"), "silver_miner")

    def test_prepass_error_tail_keeps_useful_end_of_output(self):
        noisy = "Loaded template: x\n" * 200 + "REAL FAILURE AT END"
        tailed = tail_text(noisy, max_chars=60)
        self.assertIn("REAL FAILURE AT END", tailed)
        self.assertNotIn("Loaded template: x\nLoaded template: x\nLoaded template", tailed)


class MarketFactsFixTests(unittest.IsolatedAsyncioTestCase):
    @patch("backend.market_facts._tavily_search", new_callable=AsyncMock)
    @patch("backend.market_facts._gather_yfinance_facts", new_callable=AsyncMock)
    async def test_uranium_market_facts_use_commodity_fallback_when_price_fields_missing(
        self,
        mock_yfinance: AsyncMock,
        mock_tavily: AsyncMock,
    ) -> None:
        mock_yfinance.return_value = {
            "normalized_facts": {
                "current_price": 0.54,
                "market_cap": 230_846_597.99,
                "market_cap_m": 230.84659799,
                "shares_outstanding": 427_493_683.0,
                "shares_outstanding_m": 427.493683,
                "enterprise_value": None,
                "enterprise_value_m": None,
                "currency": "AUD",
                "commodity_profile": "uranium",
                "uranium_price_usd_lb": None,
                "uranium_price_aud_lb": None,
            },
            "source_urls": ["https://finance.yahoo.com/quote/PEN.AX"],
            "notes": [],
            "error": "",
        }
        mock_tavily.side_effect = [
            {
                "answer": "Uranium spot price is US$78.50 per lb.",
                "results": [
                    {
                        "url": "https://example.com/uranium",
                        "title": "Uranium Spot",
                        "content": "Uranium spot price US$78.50/lb.",
                    }
                ],
            },
            {
                "answer": "AUD/USD exchange rate is 0.64.",
                "results": [
                    {
                        "url": "https://example.com/fx",
                        "title": "AUD/USD",
                        "content": "AUD/USD 0.64",
                    }
                ],
            },
        ]

        market_facts = await gather_market_facts_prepass(
            ticker="ASX:PEN",
            company_name="Peninsula Energy Ltd",
            exchange="ASX",
            template_id="uranium_miner",
            company_type="uranium_miner",
        )

        normalized = market_facts.get("normalized_facts") or {}
        self.assertEqual(normalized.get("commodity_profile"), "uranium")
        self.assertAlmostEqual(normalized.get("uranium_price_usd_lb"), 78.5)
        self.assertAlmostEqual(normalized.get("uranium_price_aud_lb"), 78.5 / 0.64, places=4)
        self.assertIn("Applied uranium commodity fallback.", market_facts.get("reason") or "")


class CompanyTypeDetectionFixTests(unittest.IsolatedAsyncioTestCase):
    def test_scoring_prefers_silver_for_polymetallic_silver_text(self):
        scored = _score_text_against_company_types(
            (
                "Polymetals is a silver-zinc-lead producer with polymetallic resources, "
                "a restarted mine, and AgEq-focused investor materials."
            )
        )
        self.assertEqual(scored.get("selected_company_type"), "silver_miner")
        self.assertGreater(
            float((scored.get("scores") or {}).get("silver_miner") or 0.0),
            float((scored.get("scores") or {}).get("gold_miner") or 0.0),
        )

    @patch("backend.company_type_detector._detect_via_tavily", new_callable=AsyncMock)
    async def test_api_detection_short_circuits_to_assignment_for_pol(self, mock_tavily: AsyncMock):
        payload = await detect_company_type_via_api(
            user_query="Run full analysis on Polymetals Resources Limited (ASX:POL)",
            ticker="ASX:POL",
            company_name="Polymetals Resources Limited",
            exchange="ASX",
        )
        self.assertEqual(payload.get("provider"), "assignment")
        self.assertEqual(payload.get("selected_company_type"), "silver_miner")
        mock_tavily.assert_not_awaited()


class WorkerIssuerGuardFixTests(unittest.TestCase):
    def test_detect_issuer_alignment_handles_empty_ticker_mentions(self):
        doc = {
            "title": "Funding Package Completed - Fully Funded for Production",
            "full_text": "Brightstar Resources Limited\nFunding Package Completed - Fully Funded for Production",
            "document_ref": {
                "issuer_hint": "Brightstar Resources Limited",
                "ticker_hint": "",
            },
            "issuer_signals": {
                "ticker_mentions": [],
            },
        }

        alignment = _detect_issuer_alignment(doc)
        self.assertEqual(alignment.get("status"), "match")

    def test_detect_issuer_alignment_marks_foreign_ticker_as_mismatch(self):
        doc = {
            "title": "Half Year Financial Report",
            "full_text": "Genesis Minerals Limited (ASX:GMD)\nHalf Year Financial Report\nMagnetic Resources acquisition...",
            "document_ref": {
                "issuer_hint": "Brightstar Resources Limited",
                "ticker_hint": "ASX:BTR",
            },
            "issuer_signals": {},
        }

        alignment = _detect_issuer_alignment(doc)
        self.assertEqual(alignment.get("status"), "mismatch")
        self.assertEqual(alignment.get("expected_symbol"), "BTR")
        self.assertIn("GMD", alignment.get("observed_symbols") or [])

    def test_heuristic_summary_forces_foreign_issuer_drop(self):
        doc = {
            "file_name": "doc_011.md",
            "title": "Half Year Financial Report",
            "source_url": "https://announcements.asx.com.au/example.pdf",
            "full_text": "Genesis Minerals Limited (ASX:GMD)\nHalf Year Financial Report\nMagnetic Resources acquisition...",
            "document_ref": {
                "issuer_hint": "Brightstar Resources Limited",
                "ticker_hint": "ASX:BTR",
            },
            "issuer_signals": {},
        }

        payload = _heuristic_summary_from_doc(
            doc=doc,
            full_text=str(doc["full_text"]),
            max_key_points=4,
            text_truncated=False,
        )

        importance = payload.get("importance") or {}
        issuer_validation = ((payload.get("source_meta") or {}).get("issuer_validation") or {})
        self.assertFalse(bool(importance.get("keep_for_injection")))
        self.assertFalse(bool(importance.get("is_important")))
        self.assertEqual(issuer_validation.get("status"), "mismatch")

    def test_heuristic_summary_keeps_related_party_docs_out_of_primary_injection(self):
        doc = {
            "file_name": "doc_012.md",
            "title": "Scheme Booklet",
            "source_url": "https://announcements.asx.com.au/example2.pdf",
            "full_text": (
                "Counterparty Scheme Booklet\n"
                "This booklet describes a transaction with Brightstar Resources Limited (ASX:BTR).\n"
                "Issued by Genesis Minerals Limited (ASX:GMD)."
            ),
            "document_ref": {
                "issuer_hint": "Brightstar Resources Limited",
                "ticker_hint": "ASX:BTR",
            },
            "issuer_signals": {},
        }

        payload = _heuristic_summary_from_doc(
            doc=doc,
            full_text=str(doc["full_text"]),
            max_key_points=4,
            text_truncated=False,
        )

        importance = payload.get("importance") or {}
        issuer_validation = ((payload.get("source_meta") or {}).get("issuer_validation") or {})
        self.assertEqual(issuer_validation.get("status"), "related_party")
        self.assertFalse(bool(importance.get("keep_for_injection")))


class PreStage1ContaminationGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_contamination_gate_drops_mismatch_and_related_party_docs(self):
        docs = [
            {
                "doc_id": "doc_a",
                "title": "Brightstar Quarterly",
                "importance_score": 82,
                "one_line": "Brightstar update",
                "key_points": ["Brightstar Resources Limited (ASX:BTR) update"],
                "source_meta": {"issuer_validation": {"status": "match"}},
            },
            {
                "doc_id": "doc_b",
                "title": "Genesis Half Year Report",
                "importance_score": 86,
                "one_line": "Genesis Minerals Limited (ASX:GMD)",
                "key_points": ["Magnetic acquisition"],
                "source_meta": {"issuer_validation": {"status": "mismatch"}},
            },
            {
                "doc_id": "doc_c",
                "title": "Scheme Booklet",
                "importance_score": 72,
                "one_line": "Counterparty document",
                "key_points": ["Brightstar mentioned as counterparty"],
                "source_meta": {"issuer_validation": {"status": "related_party"}},
            },
        ]

        result = await _run_pre_stage1_contamination_gate(
            compact_docs=docs,
            target_ticker="ASX:BTR",
            target_company="Brightstar Resources Limited",
            model="",
        )
        kept = list(result.get("docs", []) or [])
        report = dict(result.get("report", {}) or {})
        self.assertEqual([row.get("doc_id") for row in kept], ["doc_a"])
        self.assertEqual(report.get("status"), "hard_fail")
        self.assertIn("too_few_docs_after_contamination_clipping", report.get("reason", ""))

    async def test_contamination_gate_keeps_clean_packet_without_model(self):
        docs = [
            {
                "doc_id": f"doc_{i}",
                "title": f"Brightstar Doc {i}",
                "importance_score": 70 + i,
                "one_line": "Brightstar document",
                "key_points": ["Brightstar Resources Limited (ASX:BTR)"],
                "source_meta": {"issuer_validation": {"status": "match"}},
            }
            for i in range(1, 7)
        ]
        result = await _run_pre_stage1_contamination_gate(
            compact_docs=docs,
            target_ticker="ASX:BTR",
            target_company="Brightstar Resources Limited",
            model="",
        )
        report = dict(result.get("report", {}) or {})
        self.assertEqual(report.get("status"), "applied")
        self.assertFalse(bool(report.get("hard_fail")))
        self.assertEqual(len(list(result.get("docs", []) or [])), 6)

    async def test_contamination_gate_fails_open_for_ambiguous_docs_without_model(self):
        docs = [
            {
                "doc_id": f"doc_{i}",
                "title": f"Ambiguous Doc {i}",
                "importance_score": 70,
                "one_line": "Ambiguous issuer alignment",
                "key_points": ["No clear mismatch"],
                "source_meta": {"issuer_validation": {"status": "unclear"}},
            }
            for i in range(1, 7)
        ]
        result = await _run_pre_stage1_contamination_gate(
            compact_docs=docs,
            target_ticker="ASX:BTR",
            target_company="Brightstar Resources Limited",
            model="",
        )
        report = dict(result.get("report", {}) or {})
        self.assertEqual(report.get("status"), "applied")
        self.assertEqual(report.get("reason"), "no_model_fail_open")
        self.assertEqual(len(list(result.get("docs", []) or [])), 6)


class CouncilPipelineFixTests(unittest.IsolatedAsyncioTestCase):
    async def test_truncation_assessment_flags_empty_response_immediately(self):
        result = await _assess_stage1_truncation(
            model="anthropic/claude-sonnet-4-6",
            response_text="",
            output_tokens_used=0,
            finish_reason="",
        )
        self.assertTrue(bool(result.get("truncated")))
        self.assertEqual(result.get("reason"), "empty_response")
        self.assertEqual(float(result.get("confidence_pct", 0.0)), 100.0)

    async def test_truncation_assessment_does_not_flag_long_complete_response_by_token_count(self):
        long_complete = "Executive summary.\n" + ("Detailed analysis. " * 400) + "Final recommendation: BUY."
        with patch("backend.council.query_model", new_callable=AsyncMock) as mock_query_model:
            mock_query_model.return_value = {
                "content": "{\"truncated\": false, \"confidence_pct\": 12, \"reason\": \"tail_complete\", \"tail_looks_complete\": true}"
            }
            result = await _assess_stage1_truncation(
                model="anthropic/claude-sonnet-4-6",
                response_text=long_complete,
                output_tokens_used=19865,
                finish_reason="",
            )
        self.assertFalse(bool(result.get("truncated")))
        self.assertTrue(bool(result.get("used")))
        self.assertEqual(result.get("reason"), "tail_complete")

    def test_stage1_response_looks_truncated_only_on_strong_tail_signals(self):
        self.assertFalse(_stage1_response_looks_truncated("Complete response.\nFinal recommendation: BUY."))
        truncated = ("Detailed section. " * 20) + "\n{\"score\": 5,"
        self.assertTrue(_stage1_response_looks_truncated(truncated))

    @patch("backend.council.query_models_parallel", new_callable=AsyncMock)
    async def test_stage2_collect_rankings_adds_clean_ranking_entries(
        self,
        mock_parallel: AsyncMock,
    ) -> None:
        mock_parallel.return_value = {
            "judge-model": {
                "content": (
                    "Response A is weaker.\n\nFINAL RANKING:\n"
                    "1. Response B\n"
                    "2. Response A\n"
                )
            }
        }

        stage1_results = [
            {"model": "model-a", "response": "A"},
            {"model": "model-b", "response": "B"},
        ]
        stage2_results, _ = await stage2_collect_rankings(
            "Question",
            stage1_results,
            ranking_models=["judge-model"],
        )

        self.assertEqual(len(stage2_results), 1)
        row = stage2_results[0]
        self.assertEqual(row.get("parsed_ranking"), ["Response B", "Response A"])
        self.assertEqual(row.get("parsed_ranking_models"), ["model-b", "model-a"])
        self.assertEqual(
            row.get("ranking_entries"),
            [
                {"rank": 1, "label": "Response B", "model": "model-b"},
                {"rank": 2, "label": "Response A", "model": "model-a"},
            ],
        )
        self.assertEqual(row.get("top_choice_model"), "model-b")

    @patch("backend.council.query_model", new_callable=AsyncMock)
    async def test_stage2_revision_retries_empty_response_with_compact_prompt(
        self,
        mock_query_model: AsyncMock,
    ) -> None:
        mock_query_model.side_effect = [
            {"content": ""},
            {"content": "CHANGED: NO\nREVISION_NOTES:\n- No material change after peer review."},
        ]

        stage1_results = [{"model": "model-a", "response": "Stage 1 analysis"}]
        stage2_results = [
            {
                "model": "judge-model",
                "ranking": "FINAL RANKING:\n1. Response A",
                "parsed_ranking": ["Response A"],
                "ranking_entries": [{"rank": 1, "label": "Response A", "model": "model-a"}],
            }
        ]
        label_to_model = {"Response A": "model-a"}

        revision_results, summary = await stage2_collect_revision_deltas(
            "Question",
            stage1_results,
            stage2_results,
            label_to_model,
            revision_models=["model-a"],
        )

        self.assertEqual(len(revision_results), 1)
        row = revision_results[0]
        self.assertTrue(row.get("accepted"))
        self.assertFalse(row.get("changed"))
        self.assertEqual(row.get("attempts"), 2)
        self.assertTrue(row.get("compact_retry_used"))
        self.assertEqual(summary.get("accepted_count"), 1)
        self.assertEqual(summary.get("no_amendment_count"), 1)
        self.assertEqual(summary.get("empty_response_count"), 0)
        self.assertEqual(summary.get("parse_failed_count"), 0)

    def test_apply_stage2_revision_summary_separates_empty_responses_from_parse_failures(self):
        stage1_results = [
            {"model": "model-a", "response": "A"},
            {"model": "model-b", "response": "B"},
        ]
        revision_results = [
            {"model": "model-a", "accepted": False, "parse_error": "empty_response"},
            {"model": "model-b", "accepted": False, "parse_error": "no_json_object_found"},
        ]

        _, summary = apply_stage2_revision_deltas(stage1_results, revision_results)
        self.assertEqual(summary.get("models_unchanged_due_to_empty_response"), ["model-a"])
        self.assertEqual(summary.get("models_unchanged_due_to_parse_or_validation"), ["model-b"])


class Stage3PreservationFixTests(unittest.TestCase):
    def test_extract_development_timeline_maps_complete_to_achieved(self):
        chairman = """
<development_timeline>
- Q3 2025: First dried yellowcake produced (complete).
</development_timeline>
"""
        rows, stage, certainty = _extract_development_timeline_from_text(chairman)
        self.assertEqual(stage, "")
        self.assertIsNone(certainty)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("status"), "achieved")
        self.assertEqual(rows[0].get("target_period"), "Q3 2025")
        self.assertEqual(
            _derive_current_stage_from_timeline_rows(rows),
            "achieved",
        )

    def test_extract_headwinds_tailwinds_from_chairman_text(self):
        chairman = """
<headwinds_tailwinds>
Quantitative Headwinds:
- Cash burn compresses runway.
Quantitative Tailwinds:
- Higher uranium spot lifts cash flow.
Qualitative Headwinds:
- Operational history still needs proof.
Qualitative Tailwinds:
- US domestic supply premium remains supportive.
</headwinds_tailwinds>
"""
        extracted = _extract_headwinds_tailwinds_from_text(chairman)
        self.assertEqual(
            extracted.get("quantitative"),
            [
                "Headwind: Cash burn compresses runway.",
                "Tailwind: Higher uranium spot lifts cash flow.",
            ],
        )
        self.assertEqual(
            extracted.get("qualitative"),
            [
                "Headwind: Operational history still needs proof.",
                "Tailwind: US domestic supply premium remains supportive.",
            ],
        )

    def test_extract_headwinds_tailwinds_strips_bullet_prefixes_from_section_headings(self):
        chairman = """
<headwinds_tailwinds>
* Quantitative Headwinds:
  - Cash runway is limited.
* Qualitative Tailwinds:
  - Brownfield plant lowers restart complexity.
</headwinds_tailwinds>
"""
        extracted = _extract_headwinds_tailwinds_from_text(chairman)
        self.assertEqual(extracted.get("quantitative"), ["Headwind: Cash runway is limited."])
        self.assertEqual(
            extracted.get("qualitative"),
            ["Tailwind: Brownfield plant lowers restart complexity."],
        )

    def test_extract_headwinds_tailwinds_keeps_inline_content_on_heading_line(self):
        chairman = """
<headwinds_tailwinds>
- Quantitative Tailwinds: Spot leverage improves revenue.
- Qualitative Headwinds: Management still needs to prove the reset.
</headwinds_tailwinds>
"""
        extracted = _extract_headwinds_tailwinds_from_text(chairman)
        self.assertEqual(extracted.get("quantitative"), ["Tailwind: Spot leverage improves revenue."])
        self.assertEqual(
            extracted.get("qualitative"),
            ["Headwind: Management still needs to prove the reset."],
        )

    def test_derive_current_stage_uses_milestone_text_when_statuses_are_generic(self):
        rows = [
            {
                "milestone": "Achieved First Production (dried yellowcake from commissioned CPP)",
                "target_period": "Q3 2025",
                "status": "planned",
            },
            {
                "milestone": "Header House 16 acidification commenced",
                "target_period": "Late Jan 2026",
                "status": "planned",
            },
        ]
        self.assertEqual(_derive_current_stage_from_timeline_rows(rows), "current")

    def test_extract_development_timeline_removes_inline_status_stub_and_preserves_meaning(self):
        chairman = """
<development_timeline>
- Sep 2025: First dried yellowcake produced (Status: Achieved).
- Late Jan 2026: Header House 16 acidification commenced (Status: On-track).
</development_timeline>
"""
        rows, _, _ = _extract_development_timeline_from_text(chairman)
        self.assertEqual(rows[0].get("milestone"), "First dried yellowcake produced")
        self.assertEqual(rows[0].get("status"), "achieved")
        self.assertEqual(rows[1].get("milestone"), "Header House 16 acidification commenced")
        self.assertEqual(rows[1].get("status"), "current")

    def test_inject_stage3_audit_context_sets_top_level_market_facts_and_template_contract(self):
        structured: dict = {}
        market_facts = {
            "normalized_facts": {
                "current_price": 0.54,
                "market_cap_m": 230.8,
                "shares_outstanding_m": 427.4,
                "currency": "AUD",
                "commodity_profile": "uranium",
                "uranium_price_usd_lb": 84.0,
                "uranium_price_aud_lb": 121.7,
            }
        }
        template_contract = {
            "id": "uranium_miner",
            "family": "resources",
            "industry_label": "uranium mining",
        }

        _inject_stage3_audit_context(structured, market_facts, template_contract)

        self.assertEqual(structured.get("template_contract"), template_contract)
        self.assertEqual(
            structured.get("market_facts"),
            {
                "normalized_facts": {
                    "current_price": 0.54,
                    "market_cap": None,
                    "market_cap_m": 230.8,
                    "shares_outstanding": None,
                    "shares_outstanding_m": 427.4,
                    "enterprise_value": None,
                    "enterprise_value_m": None,
                    "currency": "AUD",
                    "commodity_profile": "uranium",
                    "uranium_price_usd_lb": 84.0,
                    "uranium_price_aud_lb": 121.7,
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
