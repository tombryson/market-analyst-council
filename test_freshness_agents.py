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
    EvidenceRef,
    FreshnessAgentDependencies,
    FreshnessDecision,
    FreshnessAgentService,
    LabScribe,
    LatestRunSelector,
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
            self.assertEqual(persisted["run_id"], "run-123")

    def _decision(self):
        from backend.freshness_agents import ActionDecision

        return ActionDecision(action="annotate_run", confidence=0.8, reason="ok")


if __name__ == "__main__":
    unittest.main()
