from __future__ import annotations

from dataclasses import dataclass

from .models import ActionDecision, ComparisonReport


@dataclass
class ActionJudge:
    """Rule-first decision layer for scenario-router actions."""

    def judge(self, report: ComparisonReport) -> ActionDecision:
        impact = str(report.impact_level or "none").lower()
        thesis = str(report.thesis_effect or "unknown").lower()
        timeline = str(report.timeline_effect or "unknown").lower()
        capital = str(report.capital_effect or "unknown").lower()
        baseline_path = str(report.baseline_path or "unknown").lower()
        current_path = str(report.current_path or "unknown").lower()
        path_transition = str(report.path_transition or "").strip().lower()
        run_validity = str(report.run_validity or "watch").lower()
        conflict_count = len(report.conflicts_with_run or [])
        finding_count = len(report.key_findings or [])
        affected_domains = [str(item or "").strip().lower() for item in (report.affected_domains or []) if str(item or "").strip()]
        material_change_types = {
            str(item or "").strip().lower() for item in (report.material_change_types or []) if str(item or "").strip()
        }

        full_rerun_domains = {"financing", "permitting", "resource", "production", "guidance", "capital_structure", "m_and_a"}
        stage1_rerun_domains = {"timeline", "operations", "management"}
        scenario_break = (
            baseline_path in {"bull", "base"}
            and current_path == "bear"
            and current_path != baseline_path
        )
        scenario_drift = (
            baseline_path in {"bull", "base", "bear"}
            and current_path in {"bull", "base", "bear"}
            and current_path != baseline_path
        )

        if impact == "critical" or thesis == "invalidates" or run_validity == "invalidated":
            return ActionDecision(
                action="urgent_human_review",
                confidence=0.98,
                reason="Critical, thesis-invalidating, or run-invalidating announcement detected.",
                should_trigger_workflow=True,
                run_reuse_ok=False,
                requires_human_ack=True,
                invalidated_sections=list(sorted(set(affected_domains) | material_change_types)),
                follow_up_steps=[
                    "Pause reuse of the current lab run.",
                    "Escalate to human review with the announcement packet and latest run side by side.",
                ],
                tags=["critical", "scenario_router"],
            )

        if (
            impact == "high"
            or thesis == "undermines"
            or conflict_count > 0
            or scenario_break
            or bool(material_change_types & full_rerun_domains)
            or bool(set(affected_domains) & full_rerun_domains)
            or run_validity == "partial_invalidation"
        ):
            return ActionDecision(
                action="full_rerun",
                confidence=0.93,
                reason=(
                    "High-impact, conflicting, or scenario-breaking announcement likely invalidates parts of the current run."
                ),
                should_trigger_workflow=True,
                run_reuse_ok=False,
                invalidated_sections=list(sorted(set(affected_domains) | material_change_types)),
                follow_up_steps=[
                    "Mark the latest run as superseded by a material announcement.",
                    "Queue a full rerun using the announcement as fresh evidence context.",
                    f"Record scenario transition: {path_transition or f'{baseline_path}->{current_path}'}",
                ],
                tags=["rerun", "conflict"],
            )

        if (
            capital in {"material_change", "worsens"}
            or timeline == "delayed"
            or scenario_drift
            or bool(material_change_types & stage1_rerun_domains)
            or bool(set(affected_domains) & stage1_rerun_domains)
        ):
            return ActionDecision(
                action="rerun_stage1",
                confidence=0.87,
                reason="Scenario drift or material capital/timeline change should refresh core evidence before trusting the current view.",
                should_trigger_workflow=True,
                run_reuse_ok=False,
                invalidated_sections=list(sorted(set(affected_domains) | material_change_types)),
                follow_up_steps=[
                    "Refresh Stage 1 evidence and scenario framing.",
                    "Reuse later-stage structure only after the new evidence has been checked.",
                    f"Update visible current path to {current_path or 'unknown'}.",
                ],
                tags=["stage1", "update"],
            )

        if impact == "medium" or thesis in {"partially_confirms", "accelerates", "delays"}:
            return ActionDecision(
                action="run_delta_only",
                confidence=0.8,
                reason="Meaningful update detected, but not enough to justify a full rerun yet.",
                should_trigger_workflow=True,
                run_reuse_ok=True,
                follow_up_steps=[
                    "Run a delta-only comparison against the latest saved run.",
                    "Surface the result in the lab before deciding on a rerun.",
                ],
                tags=["delta"],
            )

        if impact == "low" and finding_count > 0:
            return ActionDecision(
                action="annotate_run",
                confidence=0.78,
                reason="Low-impact announcement adds context but does not materially change the thesis.",
                should_trigger_workflow=False,
                run_reuse_ok=True,
                follow_up_steps=["Attach the announcement note to the run and keep the current thesis active."],
                tags=["annotation"],
            )

        if finding_count == 0:
            return ActionDecision(
                action="ignore",
                confidence=0.92,
                reason="No thesis-relevant findings were identified from the announcement.",
                should_trigger_workflow=False,
                run_reuse_ok=True,
                follow_up_steps=["Record the event as reviewed with no action required."],
                tags=["noise"],
            )

        return ActionDecision(
            action="watch",
            confidence=0.65,
            reason="Announcement may matter later, but current evidence does not justify an automated rerun.",
            should_trigger_workflow=False,
            run_reuse_ok=True,
            follow_up_steps=["Keep the run active and monitor for follow-up disclosures."],
            tags=["watch"],
        )
