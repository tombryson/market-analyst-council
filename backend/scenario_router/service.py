from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Awaitable, Callable, Dict, Optional, Union

from .action_judge import ActionJudge
from .models import (
    ActionDecision,
    AnnouncementEvent,
    AnnouncementFacts,
    AnnouncementPacket,
    BaselineRunPacket,
    ComparisonReport,
    ScenarioRouterDecision,
    StageTrace,
)

ResolverFn = Callable[[AnnouncementEvent], Union[AnnouncementPacket, Awaitable[AnnouncementPacket]]]
ReaderFn = Callable[[AnnouncementPacket], Union[AnnouncementFacts, Awaitable[AnnouncementFacts]]]
RunSelectorFn = Callable[[str, str], Union[BaselineRunPacket, Awaitable[BaselineRunPacket]]]
ComparatorFn = Callable[[AnnouncementFacts, BaselineRunPacket], Union[ComparisonReport, Awaitable[ComparisonReport]]]
ScribeFn = Callable[[ScenarioRouterDecision], Union[Dict[str, Any], Awaitable[Dict[str, Any]]]]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@dataclass
class ScenarioRouterDependencies:
    source_resolver: ResolverFn
    document_reader: ReaderFn
    run_selector: RunSelectorFn
    thesis_comparator: ComparatorFn
    lab_scribe: Optional[ScribeFn] = None
    action_judge: Optional[ActionJudge] = None


class ScenarioRouterService:
    def __init__(self, deps: ScenarioRouterDependencies):
        self._deps = deps
        self._judge = deps.action_judge or ActionJudge()

    async def process_announcement_event(self, event: AnnouncementEvent) -> ScenarioRouterDecision:
        started_at = _utc_now_iso()
        started_perf = perf_counter()
        trace: list[StageTrace] = []

        async def run_stage(stage_name: str, fn, *args):
            stage_started_at = _utc_now_iso()
            stage_started_perf = perf_counter()
            outcome = "ok"
            meta: Dict[str, Any] = {}
            try:
                result = await _maybe_await(fn(*args))
                if stage_name == "source_resolver":
                    meta = {
                        "source_type": str(getattr(result, "source_type", "") or ""),
                        "source_url": str(getattr(result, "source_url", "") or ""),
                    }
                elif stage_name == "document_reader":
                    meta = {
                        "material_topics": list(getattr(result, "material_topics", []) or [])[:8],
                        "fact_count": len(getattr(result, "extracted_facts", []) or []),
                    }
                elif stage_name == "run_selector":
                    meta = {
                        "run_id": str(getattr(result, "run_id", "") or ""),
                        "template_id": str(getattr(result, "template_id", "") or ""),
                    }
                elif stage_name == "thesis_comparator":
                    meta = {
                        "current_path": str(getattr(result, "current_path", "") or ""),
                        "impact_level": str(getattr(result, "impact_level", "") or ""),
                    }
                elif stage_name == "action_judge":
                    meta = {
                        "action": str(getattr(result, "action", "") or ""),
                        "confidence": float(getattr(result, "confidence", 0.0) or 0.0),
                    }
                return result
            except Exception:
                outcome = "error"
                raise
            finally:
                trace.append(
                    StageTrace(
                        stage=stage_name,
                        started_at_utc=stage_started_at,
                        completed_at_utc=_utc_now_iso(),
                        duration_ms=max(0, int(round((perf_counter() - stage_started_perf) * 1000))),
                        outcome=outcome,
                        meta=meta,
                    )
                )

        packet = await run_stage("source_resolver", self._deps.source_resolver, event)
        facts = await run_stage("document_reader", self._deps.document_reader, packet)
        baseline_run = await run_stage("run_selector", self._deps.run_selector, event.ticker, event.exchange)
        report = await run_stage("thesis_comparator", self._deps.thesis_comparator, facts, baseline_run)
        action = await run_stage("action_judge", self._judge.judge, report)

        decision = ScenarioRouterDecision(
            event=event,
            announcement_packet=packet,
            announcement_facts=facts,
            baseline_run=baseline_run,
            comparison_report=report,
            action_decision=action,
            processing_started_at_utc=started_at,
            processing_completed_at_utc=_utc_now_iso(),
            processing_duration_ms=max(0, int(round((perf_counter() - started_perf) * 1000))),
            processing_trace=trace,
            persisted_artifacts={},
        )

        if self._deps.lab_scribe is not None:
            persisted = await run_stage("lab_scribe", self._deps.lab_scribe, decision)
            if isinstance(persisted, dict):
                decision.persisted_artifacts = dict(persisted)

        decision.processing_completed_at_utc = _utc_now_iso()
        decision.processing_duration_ms = max(0, int(round((perf_counter() - started_perf) * 1000)))
        return decision


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
