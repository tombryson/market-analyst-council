import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from backend.freshness_agents import (
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
    FreshnessAgentDependencies,
    FreshnessDecision,
    FreshnessAgentService,
    InboxSentinel,
    LabScribe,
    LatestRunSelector,
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


class SourceResolverTests(unittest.TestCase):
    def test_resolver_prefers_asx_url_and_normalizes_subject(self):
        with TemporaryDirectory() as tmpdir:
            attachment_path = Path(tmpdir) / "btr-quarterly.txt"
            attachment_path.write_text("Quarterly update", encoding="utf-8")

            resolver = SourceResolver()
            packet = resolver.resolve(
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


class DocumentReaderTests(unittest.IsolatedAsyncioTestCase):
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
            summary="Permit approval arrived ahead of schedule and management said the project remains funded.",
            extracted_facts=[
                "Permit approval ahead of schedule was granted.",
                "Funding remains sufficient for planned milestones.",
            ],
            material_topics=["permitting", "timeline", "financing"],
            evidence=[EvidenceRef(source_url="https://announcements.asx.com.au/example2.pdf")],
            raw_text_excerpt="Permit approval ahead of schedule was granted and funding remains sufficient.",
        )

        report = self.comparator.compare(facts, self.baseline_run)

        self.assertEqual(report.baseline_path, "base")
        self.assertEqual(report.current_path, "bull")
        self.assertEqual(report.path_transition, "base->bull")
        self.assertGreater(report.path_confidence, 0.0)
        self.assertEqual(report.thesis_effect, "accelerates")
        self.assertTrue(report.key_findings)


class FreshnessAgentServiceTests(unittest.IsolatedAsyncioTestCase):
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

        async def lab_scribe(decision):
            return {
                "event_artifact": f"freshness_events/{decision.event.event_id}.json",
                "baseline_run_id": decision.baseline_run.run_id,
            }

        service = FreshnessAgentService(
            FreshnessAgentDependencies(
                source_resolver=source_resolver,
                document_reader=document_reader,
                run_selector=run_selector,
                thesis_comparator=thesis_comparator,
                lab_scribe=lab_scribe,
            )
        )

        result = await service.process_announcement_event(event)
        self.assertEqual(result.action_decision.action, "annotate_run")
        self.assertEqual(result.baseline_run.run_id, "run-123")
        self.assertEqual(result.persisted_artifacts["baseline_run_id"], "run-123")
        self.assertEqual(result.announcement_packet.title, "Quarterly Activities Report")


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

        from backend.freshness_agents import run_selector as run_selector_module

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


class LabScribeTests(unittest.IsolatedAsyncioTestCase):
    async def test_lab_scribe_persists_primary_and_by_run_artifacts(self):
        with TemporaryDirectory() as tmpdir:
            scribe = LabScribe(base_dir=Path(tmpdir))
            decision = FreshnessDecision(
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

    def _decision(self):
        from backend.freshness_agents import ActionDecision

        return ActionDecision(action="annotate_run", confidence=0.8, reason="ok")


if __name__ == "__main__":
    unittest.main()
