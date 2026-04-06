from __future__ import annotations

from dataclasses import dataclass

from .models import ActionDecision, ComparisonReport


@dataclass
class ActionJudge:
    """Rule-first decision layer for announcement freshness actions."""

    def judge(self, report: ComparisonReport) -> ActionDecision:
        impact = str(report.impact_level or "none").lower()
        thesis = str(report.thesis_effect or "unknown").lower()
        timeline = str(report.timeline_effect or "unknown").lower()
        capital = str(report.capital_effect or "unknown").lower()
        conflict_count = len(report.conflicts_with_run or [])
        finding_count = len(report.key_findings or [])

        if impact == "critical" or thesis == "invalidates":
            return ActionDecision(
                action="urgent_human_review",
                confidence=0.98,
                reason="Critical or thesis-invalidating announcement detected.",
                should_trigger_workflow=True,
                tags=["critical", "freshness"],
            )

        if impact == "high" or thesis == "undermines" or conflict_count > 0:
            return ActionDecision(
                action="full_rerun",
                confidence=0.93,
                reason="High-impact or conflicting announcement likely invalidates parts of the current run.",
                should_trigger_workflow=True,
                tags=["rerun", "conflict"],
            )

        if capital in {"material_change", "worsens"} or timeline == "delayed":
            return ActionDecision(
                action="rerun_stage1",
                confidence=0.87,
                reason="Material capital or timeline change should refresh core evidence before trusting the current view.",
                should_trigger_workflow=True,
                tags=["stage1", "update"],
            )

        if impact == "medium" or thesis in {"partially_confirms", "accelerates", "delays"}:
            return ActionDecision(
                action="run_delta_only",
                confidence=0.8,
                reason="Meaningful update detected, but not enough to justify a full rerun yet.",
                should_trigger_workflow=True,
                tags=["delta"],
            )

        if impact == "low" and finding_count > 0:
            return ActionDecision(
                action="annotate_run",
                confidence=0.78,
                reason="Low-impact announcement adds context but does not materially change the thesis.",
                should_trigger_workflow=False,
                tags=["annotation"],
            )

        if finding_count == 0:
            return ActionDecision(
                action="ignore",
                confidence=0.92,
                reason="No thesis-relevant findings were identified from the announcement.",
                should_trigger_workflow=False,
                tags=["noise"],
            )

        return ActionDecision(
            action="watch",
            confidence=0.65,
            reason="Announcement may matter later, but current evidence does not justify an automated rerun.",
            should_trigger_workflow=False,
            tags=["watch"],
        )
