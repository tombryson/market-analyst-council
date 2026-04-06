from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Union

from .action_judge import ActionJudge
from .models import (
    ActionDecision,
    AnnouncementEvent,
    AnnouncementFacts,
    AnnouncementPacket,
    BaselineRunPacket,
    ComparisonReport,
    FreshnessDecision,
)

ResolverFn = Callable[[AnnouncementEvent], Union[AnnouncementPacket, Awaitable[AnnouncementPacket]]]
ReaderFn = Callable[[AnnouncementPacket], Union[AnnouncementFacts, Awaitable[AnnouncementFacts]]]
RunSelectorFn = Callable[[str, str], Union[BaselineRunPacket, Awaitable[BaselineRunPacket]]]
ComparatorFn = Callable[[AnnouncementFacts, BaselineRunPacket], Union[ComparisonReport, Awaitable[ComparisonReport]]]
ScribeFn = Callable[[FreshnessDecision], Union[Dict[str, Any], Awaitable[Dict[str, Any]]]]


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@dataclass
class FreshnessAgentDependencies:
    source_resolver: ResolverFn
    document_reader: ReaderFn
    run_selector: RunSelectorFn
    thesis_comparator: ComparatorFn
    lab_scribe: Optional[ScribeFn] = None
    action_judge: Optional[ActionJudge] = None


class FreshnessAgentService:
    def __init__(self, deps: FreshnessAgentDependencies):
        self._deps = deps
        self._judge = deps.action_judge or ActionJudge()

    async def process_announcement_event(self, event: AnnouncementEvent) -> FreshnessDecision:
        packet = await _maybe_await(self._deps.source_resolver(event))
        facts = await _maybe_await(self._deps.document_reader(packet))
        baseline_run = await _maybe_await(self._deps.run_selector(event.ticker, event.exchange))
        report = await _maybe_await(self._deps.thesis_comparator(facts, baseline_run))
        action = self._judge.judge(report)

        decision = FreshnessDecision(
            event=event,
            announcement_packet=packet,
            announcement_facts=facts,
            baseline_run=baseline_run,
            comparison_report=report,
            action_decision=action,
            persisted_artifacts={},
        )

        if self._deps.lab_scribe is not None:
            persisted = await _maybe_await(self._deps.lab_scribe(decision))
            if isinstance(persisted, dict):
                decision.persisted_artifacts = dict(persisted)

        return decision
