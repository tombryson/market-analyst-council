from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

ImpactLevel = Literal["none", "low", "medium", "high", "critical"]
ThesisEffect = Literal[
    "unknown",
    "no_change",
    "confirms",
    "partially_confirms",
    "accelerates",
    "delays",
    "undermines",
    "invalidates",
]
TimelineEffect = Literal["unknown", "no_change", "on_track", "accelerated", "delayed", "achieved"]
CapitalEffect = Literal[
    "unknown",
    "no_change",
    "improves",
    "worsens",
    "material_change",
]
ActionType = Literal[
    "ignore",
    "watch",
    "annotate_run",
    "run_delta_only",
    "rerun_stage1",
    "full_rerun",
    "urgent_human_review",
]
ScenarioPath = Literal["unknown", "bull", "base", "bear", "mixed"]
RunValidity = Literal["intact", "watch", "partial_invalidation", "invalidated"]
ConditionStatus = Literal["matched", "not_matched", "contradicted", "unclear"]
MaterialChangeType = Literal[
    "financing",
    "permitting",
    "timeline",
    "resource",
    "production",
    "guidance",
    "capital_structure",
    "m_and_a",
    "management",
    "operations",
]



def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class AnnouncementAttachment:
    filename: str
    content_type: str = ""
    local_path: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AnnouncementEvent:
    event_id: str
    ticker: str
    exchange: str = ""
    subject: str = ""
    sender: str = ""
    body_text: str = ""
    company_hint: str = ""
    source_channel: str = "email"
    received_at_utc: str = field(default_factory=_utc_now_iso)
    urls: List[str] = field(default_factory=list)
    attachments: List[AnnouncementAttachment] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["attachments"] = [item.to_dict() for item in self.attachments]
        return data


@dataclass
class AnnouncementPacket:
    event_id: str
    ticker: str
    exchange: str = ""
    title: str = ""
    published_at_utc: str = ""
    source_url: str = ""
    source_type: str = ""
    document_path: str = ""
    document_sha256: str = ""
    company_name: str = ""
    body_text: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class EvidenceRef:
    source_url: str = ""
    quote_excerpt: str = ""
    source_title: str = ""
    source_date_utc: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AnnouncementFacts:
    event_id: str
    ticker: str
    company_name: str = ""
    title: str = ""
    summary: str = ""
    extracted_facts: List[str] = field(default_factory=list)
    material_topics: List[str] = field(default_factory=list)
    market_facts: Dict[str, Any] = field(default_factory=dict)
    evidence: List[EvidenceRef] = field(default_factory=list)
    raw_text_excerpt: str = ""

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["evidence"] = [item.to_dict() for item in self.evidence]
        return data


@dataclass
class BaselineRunPacket:
    run_id: str
    ticker: str
    exchange: str = ""
    company_name: str = ""
    template_id: str = ""
    freshness_status: str = ""
    freshness_age_days: Optional[int] = None
    summary_fields: Dict[str, Any] = field(default_factory=dict)
    lab_payload: Dict[str, Any] = field(default_factory=dict)
    timeline_rows: List[Dict[str, Any]] = field(default_factory=list)
    memos: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ComparisonFinding:
    type: str
    summary: str
    severity: ImpactLevel = "low"
    evidence: EvidenceRef = field(default_factory=EvidenceRef)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["evidence"] = self.evidence.to_dict()
        return data


@dataclass
class ConditionEvaluation:
    condition_id: str
    scenario: str = ""
    group: str = ""
    label: str = ""
    status: ConditionStatus = "unclear"
    reason: str = ""
    confidence: float = 0.0
    matched_via: str = ""
    market_field: str = ""
    observed_value: Any = None
    comparator: str = ""
    threshold_value: Any = None
    severity: str = ""
    linked_milestones: List[str] = field(default_factory=list)
    evidence: EvidenceRef = field(default_factory=EvidenceRef)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["evidence"] = self.evidence.to_dict()
        return data


@dataclass
class ComparisonReport:
    ticker: str
    baseline_run_id: str
    announcement_title: str = ""
    baseline_path: ScenarioPath = "unknown"
    current_path: ScenarioPath = "unknown"
    path_transition: str = ""
    path_confidence: float = 0.0
    run_validity: RunValidity = "watch"
    impact_level: ImpactLevel = "none"
    thesis_effect: ThesisEffect = "unknown"
    timeline_effect: TimelineEffect = "unknown"
    capital_effect: CapitalEffect = "unknown"
    affected_domains: List[str] = field(default_factory=list)
    material_change_types: List[MaterialChangeType] = field(default_factory=list)
    condition_evaluations: List[ConditionEvaluation] = field(default_factory=list)
    matched_condition_ids: List[str] = field(default_factory=list)
    triggered_watchlist_ids: List[str] = field(default_factory=list)
    market_facts_used: Dict[str, Any] = field(default_factory=dict)
    key_findings: List[ComparisonFinding] = field(default_factory=list)
    conflicts_with_run: List[ComparisonFinding] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["condition_evaluations"] = [item.to_dict() for item in self.condition_evaluations]
        data["key_findings"] = [item.to_dict() for item in self.key_findings]
        data["conflicts_with_run"] = [item.to_dict() for item in self.conflicts_with_run]
        return data


@dataclass
class ActionDecision:
    action: ActionType
    confidence: float
    reason: str
    should_trigger_workflow: bool = False
    run_reuse_ok: bool = True
    requires_human_ack: bool = False
    invalidated_sections: List[str] = field(default_factory=list)
    follow_up_steps: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StageTrace:
    stage: str
    started_at_utc: str = ""
    completed_at_utc: str = ""
    duration_ms: int = 0
    outcome: str = "ok"
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ScenarioRouterDecision:
    event: AnnouncementEvent
    announcement_packet: AnnouncementPacket
    announcement_facts: AnnouncementFacts
    baseline_run: BaselineRunPacket
    comparison_report: ComparisonReport
    action_decision: ActionDecision
    processing_started_at_utc: str = ""
    processing_completed_at_utc: str = ""
    processing_duration_ms: int = 0
    processing_trace: List[StageTrace] = field(default_factory=list)
    persisted_artifacts: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event": self.event.to_dict(),
            "announcement_packet": self.announcement_packet.to_dict(),
            "announcement_facts": self.announcement_facts.to_dict(),
            "baseline_run": self.baseline_run.to_dict(),
            "comparison_report": self.comparison_report.to_dict(),
            "action_decision": self.action_decision.to_dict(),
            "processing_started_at_utc": self.processing_started_at_utc,
            "processing_completed_at_utc": self.processing_completed_at_utc,
            "processing_duration_ms": self.processing_duration_ms,
            "processing_trace": [item.to_dict() for item in self.processing_trace],
            "persisted_artifacts": dict(self.persisted_artifacts or {}),
        }
