import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from backend.scenario_router import (
    ActionJudge,
    AnnouncementAttachment,
    AnnouncementEvent,
    AnnouncementFacts,
    AnnouncementPacket,
    BaselineRunPacket,
    ComparisonFinding,
    ComparisonReport,
    DocumentReader,
    EvidenceRef,
    ScenarioRouterDependencies,
    ScenarioRouterDecision,
    ScenarioRouterService,
    ScenarioMarketFactsResolver,
    StageTrace,
    InboxSentinel,
    LabScribe,
    LatestRunSelector,
    OfficialSourceFinder,
    SourceResolver,
    ThesisComparator,
)


class ActionJudgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.judge = ActionJudge()

    def test_critical_report_triggers_urgent_review(self):
        report = ComparisonReport(
            ticker="ASX:BTR",
            baseline_run_id="run-1",
            impact_level="critical",
            thesis_effect="invalidates",
        )
        decision = self.judge.judge(report)
        self.assertEqual(decision.action, "urgent_human_review")
        self.assertTrue(decision.should_trigger_workflow)

    def test_conflict_triggers_full_rerun(self):
        report = ComparisonReport(
            ticker="ASX:BTR",
            baseline_run_id="run-1",
            impact_level="medium",
            thesis_effect="undermines",
            affected_domains=["financing"],
            material_change_types=["financing"],
            conflicts_with_run=[
                ComparisonFinding(type="conflict", summary="Funding assumptions no longer hold.")
            ],
        )
        decision = self.judge.judge(report)
        self.assertEqual(decision.action, "full_rerun")
        self.assertFalse(decision.run_reuse_ok)
        self.assertIn("financing", decision.invalidated_sections)

    def test_timeline_delay_triggers_stage1_rerun(self):
        report = ComparisonReport(
            ticker="ASX:BTR",
            baseline_run_id="run-1",
            impact_level="low",
            timeline_effect="delayed",
            affected_domains=["timeline"],
        )
        decision = self.judge.judge(report)
        self.assertEqual(decision.action, "rerun_stage1")
        self.assertIn("Refresh Stage 1 evidence", " ".join(decision.follow_up_steps))

    def test_low_impact_findings_annotate_run(self):
        report = ComparisonReport(
            ticker="ASX:BTR",
            baseline_run_id="run-1",
            impact_level="low",
            key_findings=[ComparisonFinding(type="milestone_confirmation", summary="Mill access unchanged.")],
        )
        decision = self.judge.judge(report)
        self.assertEqual(decision.action, "annotate_run")

    def test_empty_report_ignores(self):
        report = ComparisonReport(
            ticker="ASX:BTR",
            baseline_run_id="run-1",
            impact_level="none",
        )
        decision = self.judge.judge(report)
        self.assertEqual(decision.action, "ignore")
        self.assertTrue(decision.run_reuse_ok)

    def test_material_permitting_change_forces_full_rerun_even_without_conflict(self):
        report = ComparisonReport(
            ticker="ASX:WWI",
            baseline_run_id="run-2",
            impact_level="medium",
            thesis_effect="partially_confirms",
            affected_domains=["permitting"],
            material_change_types=["permitting"],
            key_findings=[ComparisonFinding(type="permit_change", summary="Permit milestone moved materially.")],
        )
        decision = self.judge.judge(report)
        self.assertEqual(decision.action, "full_rerun")
        self.assertFalse(decision.run_reuse_ok)

    def test_scenario_break_from_base_to_bear_forces_full_rerun(self):
        report = ComparisonReport(
            ticker="ASX:BTR",
            baseline_run_id="run-3",
            baseline_path="base",
            current_path="bear",
            path_transition="base->bear",
            path_confidence=0.86,
            run_validity="partial_invalidation",
            impact_level="medium",
            thesis_effect="delays",
            key_findings=[ComparisonFinding(type="funding_break", summary="Funding pathway is now at risk.")],
        )
        decision = self.judge.judge(report)
        self.assertEqual(decision.action, "full_rerun")
        self.assertFalse(decision.run_reuse_ok)
        self.assertIn("base->bear", " ".join(decision.follow_up_steps))

    def test_scenario_drift_from_base_to_bull_triggers_stage1_refresh(self):
        report = ComparisonReport(
            ticker="ASX:BTR",
            baseline_run_id="run-4",
            baseline_path="base",
            current_path="bull",
            path_transition="base->bull",
            path_confidence=0.74,
            run_validity="watch",
            impact_level="low",
            thesis_effect="accelerates",
            key_findings=[ComparisonFinding(type="permit_acceleration", summary="Permitting moved ahead of plan.")],
        )
        decision = self.judge.judge(report)
        self.assertEqual(decision.action, "rerun_stage1")
        self.assertFalse(decision.run_reuse_ok)
        self.assertIn("current path to bull", " ".join(decision.follow_up_steps).lower())


class OfficialSourceFinderTests(unittest.IsolatedAsyncioTestCase):
    async def test_finder_prefers_title_matched_primary_source(self):
        finder = OfficialSourceFinder()
        event = AnnouncementEvent(
            event_id="evt-hc-lookup-1",
            ticker="ASX:TOR",
            exchange="ASX",
            subject="TOR (ASX) announcement on HotCopper",
            body_text="TOR: Significant New Thick High-Grade Intercepts at Paris Gold",
            received_at_utc="2026-04-08T09:16:00Z",
        )

        with (
            patch(
                "backend.scenario_router.official_source_finder.scrape_marketindex_announcements",
                return_value=[
                    {
                        "title": "Appendix 3B",
                        "url": "https://announcements.asx.com.au/asxpdf/20260408/pdf/wrong.pdf",
                        "published_at": "2026-04-08",
                        "category": "ignore",
                        "priority": 4,
                    },
                    {
                        "title": "Significant New Thick High-Grade Intercepts at Paris Gold",
                        "url": "https://announcements.asx.com.au/asxpdf/20260408/pdf/right.pdf",
                        "published_at": "2026-04-08",
                        "category": "important",
                        "priority": 2,
                    },
                ],
            ),
            patch(
                "backend.scenario_router.official_source_finder.search_asx_announcements",
                return_value=[],
            ),
        ):
            candidate = await finder.find_best_source(
                event,
                title_hint="Significant New Thick High-Grade Intercepts at Paris Gold",
            )

        self.assertEqual(
            candidate.get("url"),
            "https://announcements.asx.com.au/asxpdf/20260408/pdf/right.pdf",
        )
        self.assertEqual(
            candidate.get("title"),
            "Significant New Thick High-Grade Intercepts at Paris Gold",
        )


class SourceResolverTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolver_prefers_official_asx_source_and_normalizes_subject(self):
        with TemporaryDirectory() as tmpdir:
            attachment_path = Path(tmpdir) / "btr-quarterly.txt"
            attachment_path.write_text("Quarterly update", encoding="utf-8")

            class StubFinder:
                async def find_best_source(self, event, *, title_hint: str = ""):
                    return {
                        "title": "Quarterly Activities Report",
                        "url": "https://announcements.asx.com.au/asxpdf/20260406/pdf/example.pdf",
                        "published_at": "2026-04-06",
                    }

            resolver = SourceResolver(official_source_finder=StubFinder())
            packet = await resolver.resolve(
                AnnouncementEvent(
                    event_id="evt-asx-1",
                    ticker="ASX:BTR",
                    subject="ASX:BTR - Quarterly Activities Report",
                    sender="asxonline@asx.com.au",
                    urls=[
                        "https://example.com/generic-release",
                        "https://announcements.asx.com.au/asxpdf/20260406/pdf/example.pdf",
                    ],
                    attachments=[
                        AnnouncementAttachment(
                            filename="btr-quarterly.txt",
                            local_path=str(attachment_path),
                        )
                    ],
                    body_text="Quarterly Activities Report body.",
                )
            )

        self.assertEqual(packet.exchange, "ASX")
        self.assertEqual(packet.title, "Quarterly Activities Report")
        self.assertEqual(packet.published_at_utc, "2026-04-06")
        self.assertEqual(
            packet.source_url,
            "https://announcements.asx.com.au/asxpdf/20260406/pdf/example.pdf",
        )
        self.assertEqual(packet.source_type, "exchange_filing")
        self.assertEqual(packet.document_path, str(attachment_path))
        self.assertTrue(packet.document_sha256)
        self.assertEqual(packet.body_text, "Quarterly Activities Report body.")


class InboxSentinelTests(unittest.TestCase):
    def test_inbox_sentinel_extracts_asx_ticker_from_subject(self):
        sentinel = InboxSentinel()
        event = sentinel.ingest_email_payload(
            {
                "gmail_message_id": "gmail-1",
                "subject": "ASX:WWI - Capital Raise Update",
                "sender": "asxonline@asx.com.au",
                "body_text": "Please see attached announcement.",
                "urls": ["https://announcements.asx.com.au/asxpdf/20260406/pdf/wwi.pdf"],
            }
        )

        self.assertEqual(event.event_id, "gmail-1")
        self.assertEqual(event.ticker, "ASX:WWI")
        self.assertEqual(event.exchange, "ASX")
        self.assertEqual(event.subject, "ASX:WWI - Capital Raise Update")

    def test_inbox_sentinel_extracts_hotcopper_subject_and_company_hint(self):
        sentinel = InboxSentinel()
        event = sentinel.ingest_email_payload(
            {
                "gmail_message_id": "gmail-hc-1",
                "subject": "TOR (ASX) announcement on HotCopper",
                "sender": "no-reply@hotcopper.com.au",
                "body_text": "\n".join(
                    [
                        "8 April 2026 09:14am (AEST)",
                        "",
                        "TOR: Significant New Thick High-Grade Intercepts at Paris Gold",
                        "Torque Metals Limited.. released an announcement at 09:13am on 8 April 2026.",
                    ]
                ),
            }
        )

        self.assertEqual(event.event_id, "gmail-hc-1")
        self.assertEqual(event.ticker, "ASX:TOR")
        self.assertEqual(event.exchange, "ASX")
        self.assertEqual(event.company_hint, "Torque Metals Limited")


class HotCopperSourceResolverTests(unittest.IsolatedAsyncioTestCase):
    async def test_source_resolver_uses_hotcopper_body_headline_as_title_when_official_lookup_missing(self):
        resolver = SourceResolver()
        packet = await resolver.resolve(
            AnnouncementEvent(
                event_id="evt-hc-1",
                ticker="ASX:TOR",
                subject="TOR (ASX) announcement on HotCopper",
                sender="no-reply@hotcopper.com.au",
                body_text="\n".join(
                    [
                        "8 April 2026 09:14am (AEST)",
                        "",
                        "TOR: Significant New Thick High-Grade Intercepts at Paris Gold",
                        "Torque Metals Limited.. released an announcement at 09:13am on 8 April 2026.",
                    ]
                ),
                company_hint="Torque Metals Limited",
            )
        )

        self.assertEqual(packet.title, "Significant New Thick High-Grade Intercepts at Paris Gold")
        self.assertEqual(packet.company_name, "Torque Metals Limited")
        self.assertEqual(packet.source_url, "")


class DocumentReaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_reader_prefers_remote_exchange_filing_over_local_or_body(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "announcement.txt"
            path.write_text("This local attachment should not win.", encoding="utf-8")

            reader = DocumentReader()

            async def fake_remote(packet):
                return (
                    "Official ASX filing text confirming project funding and permit progress.",
                    ["Official ASX filing text confirming project funding and permit progress."],
                )

            reader._read_remote = fake_remote  # type: ignore[method-assign]
            facts = await reader.read(
                AnnouncementPacket(
                    event_id="evt-doc-remote-1",
                    ticker="ASX:BTR",
                    exchange="ASX",
                    title="Quarterly Activities Report",
                    source_url="https://announcements.asx.com.au/asxpdf/20260406/pdf/example.pdf",
                    source_type="exchange_filing",
                    document_path=str(path),
                    company_name="Brightstar Resources Limited",
                    body_text="Email summary fallback only.",
                )
            )

        self.assertIn("official asx filing", facts.raw_text_excerpt.lower())
        self.assertIn("financing", facts.material_topics)

    async def test_reader_uses_body_only_when_no_primary_source_exists(self):
        reader = DocumentReader()
        facts = await reader.read(
            AnnouncementPacket(
                event_id="evt-doc-body-1",
                ticker="ASX:TOR",
                exchange="ASX",
                title="Significant New Thick High-Grade Intercepts at Paris Gold",
                company_name="Torque Metals Limited",
                body_text="The company announced new drill intercepts and exploration progress.",
            )
        )

        self.assertIn("drill intercepts", facts.raw_text_excerpt.lower())

    async def test_reader_extracts_topics_and_facts_from_local_text_attachment(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "announcement.txt"
            path.write_text(
                "\n".join(
                    [
                        "Brightstar secured a new debt facility for project funding.",
                        "The environmental permit approval remains on track for the June quarter.",
                        "Management stated the processing plant timeline is ahead of schedule.",
                    ]
                ),
                encoding="utf-8",
            )

            reader = DocumentReader()
            facts = await reader.read(
                AnnouncementPacket(
                    event_id="evt-doc-1",
                    ticker="ASX:BTR",
                    exchange="ASX",
                    title="Quarterly Activities Report",
                    source_url="https://announcements.asx.com.au/example.pdf",
                    document_path=str(path),
                    company_name="Brightstar Resources Limited",
                )
            )

        self.assertEqual(facts.title, "Quarterly Activities Report")
        self.assertTrue(facts.summary)
        self.assertGreaterEqual(len(facts.extracted_facts), 2)
        self.assertIn("financing", facts.material_topics)
        self.assertIn("permitting", facts.material_topics)
        self.assertIn("timeline", facts.material_topics)
        self.assertTrue(facts.evidence)
        self.assertIn("debt facility", facts.raw_text_excerpt.lower())

    async def test_reader_skips_header_boilerplate_before_fact_lines(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "announcement.txt"
            path.write_text(
                "\n".join(
                    [
                        "Level 2, 123 Example Street Perth WA 6000",
                        "Phone: +61 8 1111 2222",
                        "Email: info@example.com",
                        "www.example.com.au",
                        "MINERAL RESOURCE INCREASED TO 2.07 MILLION OUNCES",
                        "Mandilla now contains 54Mt at 1.0g/t Au for 1.74Moz under JORC.",
                    ]
                ),
                encoding="utf-8",
            )

            reader = DocumentReader()
            facts = await reader.read(
                AnnouncementPacket(
                    event_id="evt-doc-boilerplate-1",
                    ticker="ASX:AAR",
                    exchange="ASX",
                    title="Mineral Resource Increased to 2.07 Million Ounces",
                    document_path=str(path),
                    company_name="Astral Resources NL",
                )
            )

        self.assertTrue(facts.extracted_facts)
        self.assertNotIn("phone", facts.summary.lower())
        self.assertIn("mineral resource", facts.summary.lower())
        self.assertEqual(facts.material_topics, ["resource"])


class ThesisComparatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.comparator = ThesisComparator()
        self.baseline_run = BaselineRunPacket(
            run_id="run-scenario-1",
            ticker="ASX:BTR",
            exchange="ASX",
            company_name="Brightstar Resources Limited",
            lab_payload={
                "structured_data": {
                    "extended_analysis": {
                        "current_thesis_state": {
                            "leaning": "base",
                            "status": "on-track",
                            "basis": "Funding and permitting remain on plan.",
                        }
                    },
                    "thesis_map": {
                        "bull": {
                            "required_conditions": [
                                {
                                    "condition_id": "bull_permit_fast",
                                    "condition": "Permitting approvals arrive ahead of plan",
                                    "evidence_hooks": ["permit approval ahead of schedule"],
                                    "linked_milestones": ["permit approval"],
                                }
                            ],
                            "failure_conditions": [],
                        },
                        "base": {
                            "required_conditions": [
                                {
                                    "condition_id": "base_financing_secure",
                                    "condition": "Funding remains sufficient for planned milestones",
                                    "evidence_hooks": ["funding remains sufficient"],
                                    "linked_milestones": ["funding"],
                                }
                            ],
                            "failure_conditions": [
                                {
                                    "condition_id": "base_funding_break",
                                    "condition": "Funding pathway breaks before key milestones",
                                    "evidence_hooks": ["funding shortfall", "capital raise under pressure"],
                                    "linked_milestones": ["funding"],
                                }
                            ],
                        },
                        "bear": {
                            "required_conditions": [
                                {
                                    "condition_id": "bear_delay_and_shortfall",
                                    "condition": "Project delays and funding shortfall emerge",
                                    "evidence_hooks": ["delay", "funding shortfall"],
                                    "linked_milestones": ["project timeline"],
                                }
                            ],
                            "failure_conditions": [],
                        },
                    },
                }
            },
        )

    def test_comparator_routes_base_to_bear_on_failure_condition_match(self):
        facts = AnnouncementFacts(
            event_id="evt-compare-1",
            ticker="ASX:BTR",
            company_name="Brightstar Resources Limited",
            title="Funding Update",
            summary="The company disclosed a funding shortfall and project delay.",
            extracted_facts=[
                "Funding shortfall emerged before planned milestones.",
                "Project delay was confirmed for the next quarter.",
            ],
            material_topics=["financing", "timeline"],
            evidence=[EvidenceRef(source_url="https://announcements.asx.com.au/example.pdf")],
            raw_text_excerpt="Funding shortfall and delay were disclosed.",
        )

        report = self.comparator.compare(facts, self.baseline_run)

        self.assertEqual(report.baseline_path, "base")
        self.assertEqual(report.current_path, "bear")
        self.assertEqual(report.path_transition, "base->bear")
        self.assertEqual(report.run_validity, "partial_invalidation")
        self.assertEqual(report.impact_level, "high")
        self.assertTrue(report.conflicts_with_run)

    def test_comparator_routes_base_to_bull_on_bull_condition_match(self):
        facts = AnnouncementFacts(
            event_id="evt-compare-2",
            ticker="ASX:BTR",
            company_name="Brightstar Resources Limited",
            title="Permitting Update",
            summary="Permit approval arrived ahead of schedule.",
            extracted_facts=[
                "Permit approval ahead of schedule was granted.",
            ],
            material_topics=["permitting", "timeline"],
            evidence=[EvidenceRef(source_url="https://announcements.asx.com.au/example2.pdf")],
            raw_text_excerpt="Permit approval ahead of schedule was granted.",
        )

        report = self.comparator.compare(facts, self.baseline_run)

        self.assertEqual(report.baseline_path, "base")
        self.assertEqual(report.current_path, "bull")
        self.assertEqual(report.path_transition, "base->bull")
        self.assertGreater(report.path_confidence, 0.0)
        self.assertEqual(report.thesis_effect, "accelerates")
        self.assertTrue(report.key_findings)
        self.assertIn("bull_permit_fast", report.matched_condition_ids)

    def test_comparator_reports_market_facts_without_rerouting_announcement(self):
        baseline_run = BaselineRunPacket(
            run_id="run-market-1",
            ticker="ASX:WWI",
            exchange="ASX",
            company_name="West Wits Mining Limited",
            lab_payload={
                "structured_data": {
                    "extended_analysis": {
                        "current_thesis_state": {
                            "leaning": "base",
                        }
                    },
                    "thesis_map": {
                        "bull": {
                            "required_conditions": [
                                {
                                    "condition_id": "bull_required_gold_us_5000",
                                    "condition": "Gold >US$5,000/oz",
                                }
                            ],
                            "failure_conditions": [],
                        },
                        "base": {"required_conditions": [], "failure_conditions": []},
                        "bear": {"required_conditions": [], "failure_conditions": []},
                    },
                }
            },
        )
        facts = AnnouncementFacts(
            event_id="evt-market-1",
            ticker="ASX:WWI",
            title="Quarterly Activities Report",
            summary="Operational update only.",
            extracted_facts=["Operational update only."],
            market_facts={
                "normalized_facts": {
                    "commodity_profile": "gold",
                    "gold_price_usd_oz": 5100.0,
                }
            },
        )

        report = self.comparator.compare(facts, baseline_run)

        self.assertEqual(report.current_path, "base")
        self.assertEqual(report.path_transition, "")
        self.assertEqual(report.market_facts_used.get("gold_price_usd_oz"), 5100.0)
        self.assertNotIn("bull_required_gold_us_5000", report.matched_condition_ids)
        evals = [item for item in report.condition_evaluations if item.condition_id == "bull_required_gold_us_5000"]
        self.assertEqual(len(evals), 1)
        self.assertEqual(evals[0].status, "matched")
        self.assertEqual(evals[0].matched_via, "market_facts")

    def test_market_price_failure_condition_is_not_text_matched(self):
        baseline_run = BaselineRunPacket(
            run_id="run-market-natural-1",
            ticker="ASX:AAR",
            exchange="ASX",
            company_name="Astral Resources NL",
            lab_payload={
                "structured_data": {
                    "extended_analysis": {"current_thesis_state": {"leaning": "base"}},
                    "thesis_map": {
                        "bull": {"required_conditions": [], "failure_conditions": []},
                        "base": {
                            "required_conditions": [],
                            "failure_conditions": [
                                {
                                    "condition_id": "base_failure_gold_below_a_5000_oz",
                                    "condition": "Gold below A$5,000/oz",
                                }
                            ],
                        },
                        "bear": {"required_conditions": [], "failure_conditions": []},
                    },
                }
            },
        )
        facts = AnnouncementFacts(
            event_id="evt-market-natural-1",
            ticker="ASX:AAR",
            title="Quarterly Activities & Cashflow Report",
            summary="Quarterly report discusses gold exploration activities.",
            extracted_facts=["Gold drilling continued during the March quarter."],
            raw_text_excerpt="Gold exploration continued below historical workings during 2026.",
            material_topics=["resource", "timeline", "production", "financing", "permitting"],
            market_facts={
                "normalized_facts": {
                    "commodity_profile": "gold",
                    "gold_price_aud_oz": 6730.81,
                }
            },
        )

        report = self.comparator.compare(facts, baseline_run)

        self.assertEqual(report.current_path, "base")
        self.assertEqual(report.path_transition, "")
        self.assertNotIn("base_failure_gold_below_a_5000_oz", report.matched_condition_ids)
        evals = [item for item in report.condition_evaluations if item.condition_id == "base_failure_gold_below_a_5000_oz"]
        self.assertEqual(len(evals), 1)
        self.assertEqual(evals[0].status, "contradicted")
        self.assertEqual(evals[0].matched_via, "market_facts")
        self.assertEqual(report.market_facts_used.get("gold_price_aud_oz"), 6730.81)

    def test_condition_id_is_not_used_as_evidence_phrase(self):
        baseline_run = BaselineRunPacket(
            run_id="run-condition-id-1",
            ticker="ASX:AAR",
            exchange="ASX",
            company_name="Astral Resources NL",
            lab_payload={
                "structured_data": {
                    "extended_analysis": {"current_thesis_state": {"leaning": "base"}},
                    "thesis_map": {
                        "bull": {"required_conditions": [], "failure_conditions": []},
                        "base": {
                            "required_conditions": [
                                {
                                    "condition_id": "base_required_fid_by_dec_2026",
                                    "condition": "FID by Dec 2026",
                                }
                            ],
                            "failure_conditions": [],
                        },
                        "bear": {"required_conditions": [], "failure_conditions": []},
                    },
                }
            },
        )
        facts = AnnouncementFacts(
            event_id="evt-resource-1",
            ticker="ASX:AAR",
            title="Mineral Resource Increased to 2.07 Million Ounces",
            summary="Mandilla now contains 1.74 million ounces gold.",
            extracted_facts=["Updated JORC mineral resource is 54Mt at 1.0g/t Au for 1.74Moz."],
            raw_text_excerpt="Updated mineral resource statement released in April 2026.",
            material_topics=["resource"],
        )

        report = self.comparator.compare(facts, baseline_run)

        self.assertEqual(report.current_path, "base")
        self.assertEqual(report.path_transition, "")
        self.assertNotIn("base_required_fid_by_dec_2026", report.matched_condition_ids)
        self.assertEqual(report.affected_domains, ["resource"])


class ScenarioRouterServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_announcement_event_runs_pipeline_and_persists(self):
        event = AnnouncementEvent(
            event_id="evt-1",
            ticker="ASX:BTR",
            exchange="ASX",
            subject="ASX:BTR - Quarterly Activities Report",
            sender="asxonline@asx.com.au",
            attachments=[AnnouncementAttachment(filename="BTR-qtr.pdf", local_path="/tmp/BTR-qtr.pdf")],
        )

        async def source_resolver(in_event: AnnouncementEvent) -> AnnouncementPacket:
            self.assertEqual(in_event.ticker, "ASX:BTR")
            return AnnouncementPacket(
                event_id=in_event.event_id,
                ticker=in_event.ticker,
                exchange=in_event.exchange,
                title="Quarterly Activities Report",
                source_url="https://announcements.asx.com.au/example.pdf",
                source_type="exchange_filing",
                document_path="/tmp/BTR-qtr.pdf",
                company_name="Brightstar Resources Limited",
            )

        async def document_reader(packet: AnnouncementPacket) -> AnnouncementFacts:
            self.assertEqual(packet.title, "Quarterly Activities Report")
            return AnnouncementFacts(
                event_id=packet.event_id,
                ticker=packet.ticker,
                company_name=packet.company_name,
                title=packet.title,
                summary="Laverton processing pathway remains active and financing unchanged.",
                extracted_facts=[
                    "Genesis arrangement remains active.",
                    "No new funding facility announced.",
                ],
                material_topics=["operations", "timeline"],
                evidence=[
                    EvidenceRef(
                        source_url=packet.source_url,
                        quote_excerpt="The company continues to advance processing arrangements.",
                        source_title=packet.title,
                    )
                ],
            )

        async def run_selector(ticker: str, exchange: str) -> BaselineRunPacket:
            self.assertEqual(ticker, "ASX:BTR")
            self.assertEqual(exchange, "ASX")
            return BaselineRunPacket(
                run_id="run-123",
                ticker=ticker,
                exchange=exchange,
                company_name="Brightstar Resources Limited",
                template_id="resources_gold_monometallic",
                freshness_status="watch",
                freshness_age_days=12,
                summary_fields={"rating": "Buy"},
            )

        async def thesis_comparator(facts: AnnouncementFacts, baseline_run: BaselineRunPacket) -> ComparisonReport:
            self.assertEqual(baseline_run.run_id, "run-123")
            self.assertEqual(
                (facts.market_facts or {}).get("normalized_facts", {}).get("gold_price_usd_oz"),
                4850.0,
            )
            return ComparisonReport(
                ticker=facts.ticker,
                baseline_run_id=baseline_run.run_id,
                announcement_title=facts.title,
                impact_level="low",
                thesis_effect="confirms",
                timeline_effect="on_track",
                capital_effect="no_change",
                key_findings=[
                    ComparisonFinding(
                        type="milestone_confirmation",
                        summary="Announcement confirms existing operating path.",
                        severity="low",
                        evidence=facts.evidence[0],
                    )
                ],
            )

        async def market_facts_resolver(facts: AnnouncementFacts, baseline_run: BaselineRunPacket):
            self.assertEqual(baseline_run.template_id, "resources_gold_monometallic")
            return {
                "normalized_facts": {
                    "commodity_profile": "gold",
                    "gold_price_usd_oz": 4850.0,
                }
            }

        async def lab_scribe(decision):
            return {
                "event_artifact": f"scenario_router_events/{decision.event.event_id}.json",
                "baseline_run_id": decision.baseline_run.run_id,
            }

        service = ScenarioRouterService(
            ScenarioRouterDependencies(
                source_resolver=source_resolver,
                document_reader=document_reader,
                run_selector=run_selector,
                market_facts_resolver=market_facts_resolver,
                thesis_comparator=thesis_comparator,
                lab_scribe=lab_scribe,
            )
        )

        result = await service.process_announcement_event(event)
        self.assertEqual(result.action_decision.action, "annotate_run")
        self.assertEqual(result.baseline_run.run_id, "run-123")
        self.assertEqual(result.persisted_artifacts["baseline_run_id"], "run-123")
        self.assertEqual(result.announcement_packet.title, "Quarterly Activities Report")
        self.assertGreaterEqual(result.processing_duration_ms, 0)
        self.assertEqual(
            [stage.stage for stage in result.processing_trace],
            ["source_resolver", "document_reader", "run_selector", "market_facts_resolver", "thesis_comparator", "action_judge", "lab_scribe"],
        )


class LatestRunSelectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_selector_coerces_report_packet(self):
        selector = LatestRunSelector(limit=5)

        async def fake_list_gantt_runs(limit: int = 20, ticker: str = None):
            self.assertEqual(limit, 5)
            self.assertEqual(ticker, "ASX:BTR")
            return {
                "runs": [
                    {
                        "id": "run-123.json",
                        "ticker": "ASX:BTR",
                    }
                ]
            }

        async def fake_get_gantt_run_report_packet(run_id: str):
            self.assertEqual(run_id, "run-123.json")
            return {
                "run_id": "run-123.json",
                "summary_fields": {
                    "ticker": "ASX:BTR",
                    "company_name": "Brightstar Resources Limited",
                    "template_id": "resources_gold_monometallic",
                    "freshness_status": "watch",
                    "freshness_age_days": 12,
                },
                "lab_payload": {"freshness": {"status": "watch"}},
                "timeline_rows": [{"stage": "Construction"}],
                "memos": {"analyst_memo_markdown": "memo"},
            }

        from backend.scenario_router import run_selector as run_selector_module

        original_list = run_selector_module.main_api.list_gantt_runs
        original_packet = run_selector_module.main_api.get_gantt_run_report_packet
        run_selector_module.main_api.list_gantt_runs = fake_list_gantt_runs
        run_selector_module.main_api.get_gantt_run_report_packet = fake_get_gantt_run_report_packet
        try:
            packet = await selector.select_latest("ASX:BTR", "ASX")
        finally:
            run_selector_module.main_api.list_gantt_runs = original_list
            run_selector_module.main_api.get_gantt_run_report_packet = original_packet

        self.assertEqual(packet.run_id, "run-123.json")
        self.assertEqual(packet.template_id, "resources_gold_monometallic")
        self.assertEqual(packet.freshness_status, "watch")


class ScenarioMarketFactsResolverTests(unittest.IsolatedAsyncioTestCase):
    async def test_market_facts_resolver_uses_prepass_pipeline(self):
        resolver = ScenarioMarketFactsResolver()
        facts = AnnouncementFacts(
            event_id="evt-mf-1",
            ticker="ASX:WWI",
            company_name="West Wits Mining Limited",
        )
        baseline_run = BaselineRunPacket(
            run_id="run-mf-1",
            ticker="ASX:WWI",
            exchange="ASX",
            company_name="West Wits Mining Limited",
            template_id="resources_gold_monometallic",
            summary_fields={"company_type": "gold_miner"},
        )

        with patch(
            "backend.scenario_router.market_facts_resolver.gather_market_facts_prepass",
            return_value={
                "normalized_facts": {
                    "current_price": 0.19,
                    "commodity_profile": "gold",
                    "gold_price_usd_oz": 5001.0,
                }
            },
        ) as mock_gather:
            payload = await resolver.resolve(facts, baseline_run)

        mock_gather.assert_awaited_once()
        self.assertEqual(payload.get("normalized_facts", {}).get("gold_price_usd_oz"), 5001.0)


class LabScribeTests(unittest.IsolatedAsyncioTestCase):
    async def test_lab_scribe_persists_primary_and_by_run_artifacts(self):
        with TemporaryDirectory() as tmpdir:
            scribe = LabScribe(base_dir=Path(tmpdir))
            decision = ScenarioRouterDecision(
                event=AnnouncementEvent(event_id="evt-99", ticker="ASX:BTR", exchange="ASX"),
                announcement_packet=AnnouncementPacket(event_id="evt-99", ticker="ASX:BTR", title="Update"),
                announcement_facts=AnnouncementFacts(event_id="evt-99", ticker="ASX:BTR"),
                baseline_run=BaselineRunPacket(run_id="run-123", ticker="ASX:BTR"),
                comparison_report=ComparisonReport(ticker="ASX:BTR", baseline_run_id="run-123"),
                action_decision=self._decision(),
            )

            persisted = await scribe.persist(decision)
            self.assertTrue(Path(persisted["event_artifact"]).exists())
            self.assertTrue(Path(persisted["by_run_artifact"]).exists())
            self.assertTrue(Path(persisted["latest_by_run_artifact"]).exists())
            self.assertEqual(persisted["run_id"], "run-123")
            latest = LabScribe.load_latest_for_run("run-123", base_dir=Path(tmpdir))
            self.assertEqual(latest.get("comparison_report", {}).get("ticker"), "ASX:BTR")

    async def test_lab_scribe_persists_processing_error_event(self):
        with TemporaryDirectory() as tmpdir:
            scribe = LabScribe(base_dir=Path(tmpdir))
            event = AnnouncementEvent(
                event_id="evt-missing-run",
                ticker="ASX:TOR",
                exchange="ASX",
                subject="TOR (ASX) announcement on HotCopper",
                company_hint="Torque Metals Limited",
            )

            persisted = await scribe.persist_status(
                event=event,
                status="processing_error",
                reason="Document reader failed.",
                action="watch",
            )
            payload = json.loads(Path(persisted["event_artifact"]).read_text(encoding="utf-8"))

        self.assertEqual(payload.get("status"), "processing_error")
        self.assertEqual(payload.get("action_decision", {}).get("action"), "watch")
        self.assertIn("Document reader failed", payload.get("error", {}).get("reason", ""))

    def _decision(self):
        from backend.scenario_router import ActionDecision

        return ActionDecision(action="annotate_run", confidence=0.8, reason="ok")


class ScenarioRouterWebhookHelperTests(unittest.TestCase):
    def test_choose_scenario_router_event_key_prefers_gmail_message_id(self):
        from backend import main as main_module

        key = main_module._choose_scenario_router_event_key(
            {
                "gmail_message_id": "gmail-evt-1",
                "event_id": "evt-1",
                "subject": "TOR (ASX) announcement on HotCopper",
            }
        )
        self.assertEqual(key, "gmail-evt-1")

    def test_persist_and_load_scenario_router_dedupe_marker(self):
        from backend import main as main_module

        with TemporaryDirectory() as tmpdir:
            original_dir = main_module.SCENARIO_ROUTER_DEDUPE_DIR
            main_module.SCENARIO_ROUTER_DEDUPE_DIR = Path(tmpdir)
            try:
                payload = {
                    "event_key": "gmail-evt-2",
                    "ticker": "ASX:TOR",
                    "baseline_run_id": "run-99",
                    "action": "annotate_run",
                }
                main_module._persist_scenario_router_dedupe("gmail-evt-2", payload)
                loaded = main_module._load_scenario_router_dedupe("gmail-evt-2")
            finally:
                main_module.SCENARIO_ROUTER_DEDUPE_DIR = original_dir

        self.assertEqual(loaded.get("ticker"), "ASX:TOR")
        self.assertEqual(loaded.get("baseline_run_id"), "run-99")


class ScenarioRouterObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_observability_summarizes_persisted_events(self):
        from backend.scenario_router.observability import ScenarioRouterObservability

        with TemporaryDirectory() as tmpdir:
            scribe = LabScribe(base_dir=Path(tmpdir))
            decision = ScenarioRouterDecision(
                event=AnnouncementEvent(event_id="evt-obs-1", ticker="ASX:TOR", exchange="ASX"),
                announcement_packet=AnnouncementPacket(
                    event_id="evt-obs-1",
                    ticker="ASX:TOR",
                    title="Operational Update",
                    source_type="exchange_filing",
                    source_url="https://announcements.asx.com.au/asxpdf/example.pdf",
                ),
                announcement_facts=AnnouncementFacts(event_id="evt-obs-1", ticker="ASX:TOR"),
                baseline_run=BaselineRunPacket(run_id="run-obs-1", ticker="ASX:TOR"),
                comparison_report=ComparisonReport(
                    ticker="ASX:TOR",
                    baseline_run_id="run-obs-1",
                    current_path="bull",
                    baseline_path="base",
                    path_transition="base->bull",
                    impact_level="medium",
                ),
                action_decision=ActionJudge().judge(
                    ComparisonReport(
                        ticker="ASX:TOR",
                        baseline_run_id="run-obs-1",
                        current_path="base",
                        baseline_path="base",
                        impact_level="low",
                        key_findings=[ComparisonFinding(type="note", summary="ok")],
                    )
                ),
                processing_started_at_utc="2026-04-08T00:00:00Z",
                processing_completed_at_utc="2026-04-08T00:00:01Z",
                processing_duration_ms=1000,
                processing_trace=[StageTrace(stage="source_resolver", duration_ms=100)],
            )
            await scribe.persist(decision)

            observer = ScenarioRouterObservability(base_dir=Path(tmpdir))
            overview = observer.build_overview()
            events = observer.list_recent_events()

        self.assertEqual(overview.get("total_events"), 1)
        self.assertEqual(overview.get("unique_tickers"), 1)
        self.assertEqual(overview.get("status_counts", {}).get("ok"), 1)
        self.assertEqual(events[0].get("path_transition"), "base->bull")
        self.assertEqual(events[0].get("source_type"), "exchange_filing")
        self.assertEqual(events[0].get("processing_duration_ms"), 1000)

    async def test_observability_includes_processing_errors(self):
        from backend.scenario_router.observability import ScenarioRouterObservability

        with TemporaryDirectory() as tmpdir:
            scribe = LabScribe(base_dir=Path(tmpdir))
            await scribe.persist_status(
                event=AnnouncementEvent(event_id="evt-obs-2", ticker="ASX:TOR", exchange="ASX"),
                status="processing_error",
                reason="Document reader failed.",
                action="watch",
            )

            observer = ScenarioRouterObservability(base_dir=Path(tmpdir))
            overview = observer.build_overview()
            events = observer.list_recent_events()

        self.assertEqual(overview.get("status_counts", {}).get("processing_error"), 1)
        self.assertEqual(events[0].get("status"), "processing_error")
        self.assertIn("Document reader failed", events[0].get("error_reason", ""))

    async def test_observability_excludes_no_baseline_run_records(self):
        from backend.scenario_router.observability import ScenarioRouterObservability

        with TemporaryDirectory() as tmpdir:
            scribe = LabScribe(base_dir=Path(tmpdir))
            await scribe.persist_status(
                event=AnnouncementEvent(event_id="evt-obs-3", ticker="ASX:TLX", exchange="ASX"),
                status="no_baseline_run",
                reason="No saved lab runs found for ASX:TLX.",
            )

            observer = ScenarioRouterObservability(base_dir=Path(tmpdir))
            overview = observer.build_overview()
            events = observer.list_recent_events()

        self.assertEqual(overview.get("total_events"), 0)
        self.assertEqual(events, [])

    async def test_evaluation_suite_cases_pass(self):
        from backend.scenario_router.observability import ScenarioRouterObservability

        observer = ScenarioRouterObservability()
        summary = observer.run_evaluation_suite()
        self.assertGreaterEqual(summary.get("total_cases", 0), 6)
        self.assertEqual(summary.get("failed_cases"), 0)
        self.assertEqual(summary.get("pass_rate_pct"), 100.0)


if __name__ == "__main__":
    unittest.main()
