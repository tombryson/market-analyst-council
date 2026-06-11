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
        trajectory_state = str(getattr(report, "trajectory_state", "") or "").strip().lower()
        conflict_count = len(report.conflicts_with_run or [])
        finding_count = len(report.key_findings or [])
        affected_domains = [str(item or "").strip().lower() for item in (report.affected_domains or []) if str(item or "").strip()]
        material_change_types = {
            str(item or "").strip().lower() for item in (report.material_change_types or []) if str(item or "").strip()
        }
        relationship_kind = str(getattr(report, "relationship_kind", "") or "").strip().lower()
        if relationship_kind:
            return self._judge_relationship(
                report=report,
                impact=impact,
                run_validity=run_validity,
                affected_domains=affected_domains,
                material_change_types=material_change_types,
            )

        full_rerun_domains = {
            "financing",
            "permitting",
            "regulatory",
            "resource",
            "production",
            "guidance",
            "capital_structure",
            "m_and_a",
            "legal",
            "asset_project",
            "clinical_regulatory",
            "credit_risk",
        }
        stage1_rerun_domains = {
            "timeline",
            "operations",
            "management",
            "governance",
            "commercial",
            "customer",
            "commercial_customer",
            "product",
            "technology",
            "product_technology",
            "drilling_exploration",
            "real_estate_portfolio",
        }
        critical_domain_hit = bool(material_change_types & full_rerun_domains) or bool(set(affected_domains) & full_rerun_domains)
        stage1_domain_hit = bool(material_change_types & stage1_rerun_domains) or bool(set(affected_domains) & stage1_rerun_domains)
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
        positive_scenario_shift = (
            scenario_drift
            and current_path in {"bull", "base"}
            and thesis in {"accelerates", "confirms", "partially_confirms"}
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

        if trajectory_state == "material_unmapped":
            return ActionDecision(
                action="annotate_run",
                confidence=0.84,
                reason=(
                    "Material filing was identified, but it did not match a saved thesis-map or watchlist condition. "
                    "Treat this as a thesis-map coverage gap, not as an immaterial filing."
                ),
                should_trigger_workflow=False,
                run_reuse_ok=True,
                follow_up_steps=[
                    "Attach the filing to the thesis trajectory log.",
                    "Review whether the saved thesis map needs a new driver or condition.",
                    "Only refresh council evidence after the thesis-map gap is resolved.",
                ],
                tags=["material_unmapped", "thesis_map_gap"],
            )

        if trajectory_state == "needs_classification":
            return ActionDecision(
                action="annotate_run",
                confidence=0.72,
                reason="Filing could not be confidently classified against the saved company context.",
                should_trigger_workflow=False,
                run_reuse_ok=True,
                follow_up_steps=["Review the filing classification before deciding whether the thesis trajectory changed."],
                tags=["needs_classification"],
            )

        if (
            impact == "high"
            or thesis == "undermines"
            or conflict_count > 0
            or scenario_break
            or run_validity == "partial_invalidation"
            or (critical_domain_hit and current_path == "bear")
            or (
                critical_domain_hit
                and trajectory_state != "no_thesis_change"
                and not positive_scenario_shift
                and impact in {"medium", "high"}
                and thesis not in {"confirms", "accelerates"}
            )
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
            trajectory_state != "no_thesis_change"
            and (
                capital in {"material_change", "worsens"}
                or timeline == "delayed"
                or scenario_drift
                or (stage1_domain_hit and impact in {"medium", "high"})
            )
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

        if trajectory_state == "no_thesis_change" and (
            "capital_management" in material_change_types or "capital_management" in affected_domains
        ):
            return ActionDecision(
                action="annotate_run",
                confidence=0.76,
                reason="Capital-management filing was classified and recorded, but no saved thesis condition changed.",
                should_trigger_workflow=False,
                run_reuse_ok=True,
                follow_up_steps=["Attach the capital-management update to the trajectory log without changing the saved thesis path."],
                tags=["capital_management", "no_thesis_change"],
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

    @staticmethod
    def _judge_relationship(
        *,
        report: ComparisonReport,
        impact: str,
        run_validity: str,
        affected_domains: list[str],
        material_change_types: set[str],
    ) -> ActionDecision:
        priority = int(getattr(report, "relationship_priority", 0) or 0)
        kind = str(getattr(report, "relationship_kind", "") or "").strip().lower()
        direction = str(getattr(report, "relationship_direction", "") or "").strip().lower()
        summary = str(getattr(report, "relationship_summary", "") or "").strip()
        sections = list(sorted(set(affected_domains) | material_change_types))
        trajectory_state = str(getattr(report, "trajectory_state", "") or "").strip().lower()
        path_transition = str(getattr(report, "path_transition", "") or "").strip()
        filing_type = str(getattr(report, "filing_type", "") or "").strip().lower()
        score = getattr(report, "trajectory_score", {}) or {}
        event_delta = 0.0
        if isinstance(score, dict):
            try:
                event_delta = float(score.get("event_delta") or 0.0)
            except (TypeError, ValueError):
                event_delta = 0.0

        if filing_type == "administrative" or trajectory_state == "administrative_filing":
            return ActionDecision(
                action="ignore",
                confidence=0.92,
                reason=summary or "Administrative filing recorded without changing the thesis path.",
                should_trigger_workflow=False,
                run_reuse_ok=True,
                invalidated_sections=[],
                follow_up_steps=["Record the filing without changing the saved thesis path."],
                tags=[f"priority_{priority}", "administrative"],
            )

        if (
            trajectory_state == "no_thesis_change"
            and kind != "material_unmapped"
            and direction == "neutral"
            and abs(event_delta) <= 0.0001
        ):
            return ActionDecision(
                action="ignore",
                confidence=0.9,
                reason=summary or "Saved thesis relationship was checked without changing the thesis path.",
                should_trigger_workflow=False,
                run_reuse_ok=True,
                invalidated_sections=[],
                follow_up_steps=["Record the filing without changing the saved thesis path."],
                tags=[f"priority_{priority}", kind or "saved_relationship", "no_thesis_change"],
            )

        if priority >= 7:
            if impact == "critical" or run_validity == "invalidated":
                return ActionDecision(
                    action="urgent_human_review",
                    confidence=0.96,
                    reason=summary or "Thesis-breaking filing requires immediate review.",
                    should_trigger_workflow=True,
                    run_reuse_ok=False,
                    requires_human_ack=True,
                    invalidated_sections=sections,
                    follow_up_steps=[
                        "Pause reuse of the current saved run.",
                        "Review the filing against the saved thesis before using the old council view.",
                    ],
                    tags=["priority_7", kind or "thesis_break"],
                )
            return ActionDecision(
                action="full_rerun",
                confidence=0.92,
                reason=summary or "High-priority saved thesis relationship may invalidate the current run.",
                should_trigger_workflow=True,
                run_reuse_ok=False,
                invalidated_sections=sections,
                follow_up_steps=[
                    "Attach the filing to the thesis log.",
                    "Rebuild the council view if the saved thesis remains broken after review.",
                ],
                tags=["priority_7", kind or "thesis_break"],
            )

        if priority == 6:
            if (
                kind == "saved_thesis_condition"
                and direction == "neutral"
                and trajectory_state == "no_thesis_change"
                and not path_transition
            ):
                return ActionDecision(
                    action="ignore",
                    confidence=0.9,
                    reason=summary or "Saved base-case condition was confirmed without changing the thesis path.",
                    should_trigger_workflow=False,
                    run_reuse_ok=True,
                    invalidated_sections=[],
                    follow_up_steps=["Record the filing without changing the saved thesis path."],
                    tags=["priority_6", kind, "base_case_confirmed"],
                )
            if direction == "negative":
                return ActionDecision(
                    action="full_rerun",
                    confidence=0.9,
                    reason=summary or "Direct negative thesis condition matched.",
                    should_trigger_workflow=True,
                    run_reuse_ok=False,
                    invalidated_sections=sections,
                    follow_up_steps=["Attach the filing to the thesis log.", "Rebuild affected council sections."],
                    tags=["priority_6", kind or "direct_thesis"],
                )
            return ActionDecision(
                action="rerun_stage1",
                confidence=0.86,
                reason=summary or "Direct saved thesis condition matched.",
                should_trigger_workflow=True,
                run_reuse_ok=False,
                invalidated_sections=sections,
                follow_up_steps=[
                    "Attach the filing to the thesis log.",
                    "Refresh the evidence pack before relying on the saved council view.",
                ],
                tags=["priority_6", kind or "direct_thesis"],
            )

        if priority in {4, 5}:
            return ActionDecision(
                action="run_delta_only",
                confidence=0.82,
                reason=summary or "Saved thesis evidence relationship found.",
                should_trigger_workflow=True,
                run_reuse_ok=True,
                invalidated_sections=[],
                follow_up_steps=[
                    "Attach the filing to the thesis log.",
                    "Update the saved thesis note with the relationship strength.",
                ],
                tags=[f"priority_{priority}", kind or "saved_relationship"],
            )

        if priority == 3 and trajectory_state in {"risk_increased", "thesis_weakened", "timeline_delayed"}:
            return ActionDecision(
                action="annotate_run",
                confidence=0.82,
                reason=summary or "Unmapped negative risk event needs thesis review before the saved run is trusted.",
                should_trigger_workflow=False,
                run_reuse_ok=True,
                requires_human_ack=True,
                follow_up_steps=[
                    "Attach the filing to the thesis log.",
                    "Review whether safety, regulatory, timeline, or governance risk needs thesis-map coverage.",
                    "Refresh the evidence pack if the saved analysis does not cover this risk.",
                ],
                tags=["priority_3", kind or "coverage_gap", "negative_risk"],
            )

        if priority == 3:
            return ActionDecision(
                action="annotate_run",
                confidence=0.78,
                reason=summary or "Filing needs thesis classification or thesis-map coverage.",
                should_trigger_workflow=False,
                run_reuse_ok=True,
                follow_up_steps=[
                    "Attach the filing to the thesis log.",
                    "Review whether the saved thesis evidence set needs a new condition.",
                ],
                tags=["priority_3", kind or "coverage_gap"],
            )

        return ActionDecision(
            action="ignore",
            confidence=0.88,
            reason=summary or "No maintenance needed for this filing.",
            should_trigger_workflow=False,
            run_reuse_ok=True,
            follow_up_steps=["Record the filing without changing the saved thesis path."],
            tags=[f"priority_{priority}", kind or "no_relation"],
        )
