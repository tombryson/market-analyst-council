from __future__ import annotations

import inspect
import re
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from .models import (
    AnnouncementFacts,
    BaselineRunPacket,
    ComparisonFinding,
    ComparisonReport,
    ConditionEvaluation,
    EvidenceRef,
)
from .trajectory_scoring import TERMINAL_NEUTRAL_STATES, build_trajectory_score

POSITIVE_TOKENS = {"approved", "secured", "completed", "achieved", "on track", "ahead", "accelerated", "funded", "signed"}
POSITIVE_ACTION_TOKENS = {
    "approved",
    "approval",
    "secured",
    "completed",
    "achieved",
    "accelerated",
    "funded",
    "signed",
    "granted",
    "renewed",
    "expanded",
    "launched",
}
PARTIAL_MATCH_STATUSES = {"matched", "partial_match"}
CHECKED_NOT_TRIGGERED_STATUS = "checked_not_triggered"
NON_FINAL_AGREEMENT_TERMS = {"letter of intent", "loi", "mou", "memorandum of understanding", "non-binding", "non binding"}
BINDING_AGREEMENT_TERMS = {"binding", "definitive", "take-or-pay", "take or pay", "executed agreement"}
MODEL_ADJUDICATION_STOP_WORDS = {
    "about",
    "against",
    "case",
    "announcement",
    "condition",
    "company",
    "current",
    "evidence",
    "filing",
    "latest",
    "material",
    "milestone",
    "monitor",
    "monitoring",
    "signal",
    "signals",
    "source",
    "support",
    "thesis",
    "update",
    "updates",
}
MODEL_THESIS_PRECURSOR_TERMS = {
    "critical path",
    "de-risk",
    "derisk",
    "de risking",
    "enabling",
    "enables",
    "leading into",
    "path to",
    "precursor",
    "prerequisite",
    "progress toward",
    "progress towards",
    "schedule support",
    "supports the path",
    "targeted pathway",
    "toward",
    "towards",
}
MODEL_THESIS_TARGET_ONLY_TERMS = {
    "aim",
    "aimed",
    "aims",
    "expected",
    "goal",
    "intended",
    "pathway",
    "planned",
    "schedule",
    "target",
    "targeted",
}
MODEL_THESIS_SATISFACTION_TERMS = {
    "achieved",
    "completed",
    "commenced",
    "delivered",
    "established",
    "fulfilled",
    "produced",
    "received",
    "satisfied",
}
NEGATIVE_TOKENS = {
    "delay",
    "delayed",
    "risk",
    "at risk",
    "suspend",
    "halt",
    "withdrawn",
    "default",
    "cost overrun",
    "dilution",
    "shortfall",
    "funding gap",
    "cancelled",
    "breached",
}
DOMAIN_KEYWORDS = {
    "financing": {"funding", "facility", "debt", "loan", "placement", "capital raise", "equity raise", "liquidity", "covenant"},
    "permitting": {"permit", "approval", "license", "licence", "heritage", "environmental", "regulator"},
    "regulatory": {"regulator", "regulatory", "approval", "compliance", "investigation", "licence", "license"},
    "timeline": {"timeline", "milestone", "delay", "delayed", "ahead of schedule", "on track", "mid-2027", "2028"},
    "resource": {"resource", "reserve", "jorc", "ore reserve", "mineral resource"},
    "production": {"production", "throughput", "first gold", "ramp-up", "ramp up", "processing", "run-rate", "run rate", "kozpa"},
    "guidance": {"guidance", "forecast", "outlook", "aisc", "cost guidance", "cash margin", "revenue guidance", "earnings guidance"},
    "capital_structure": {"shares", "dilution", "placement", "capital structure", "escrow", "equity raise"},
    "capital_management": {"buyback", "buy-back", "buy back", "share repurchase", "return of capital", "dividend", "capital management"},
    "m_and_a": {"acquisition", "scheme", "takeover", "merger", "joint venture", "farm-in", "farm in"},
    "management": {"director", "ceo", "cfo", "chair", "executive", "resignation", "appointment"},
    "governance": {"board", "director", "chair", "resignation", "appointment", "governance", "audit"},
    "operations": {"operations", "plant", "mill", "mine", "contractor", "site", "power", "grid", "load shedding", "facility", "service", "platform"},
    "commercial": {"contract", "agreement", "customer", "client", "partner", "distribution", "order", "purchase order"},
    "customer": {"customer", "client", "subscriber", "user", "account", "churn", "retention"},
    "product": {"product", "launch", "release", "trial", "platform", "software", "service", "device"},
    "technology": {"technology", "software", "platform", "patent", "clinical", "data", "cyber", "ai"},
    "legal": {"litigation", "claim", "proceeding", "settlement", "court", "dispute", "breach"},
}
MARKET_RULE_RE = re.compile(
    r"\b(?P<asset>gold|silver|copper|lithium|uranium|brent|wti|henry hub|henry_hub|natural gas)\b"
    r"[^<>]{0,60}?(?P<op>>=|<=|>|<)\s*(?P<currency>US\$|USD|A\$|AU\$|AUD)\s*(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)",
    flags=re.IGNORECASE,
)
MARKET_NATURAL_RULE_RE = re.compile(
    r"\b(?P<asset>gold|silver|copper|lithium|uranium|brent|wti|henry hub|henry_hub|natural gas)\b"
    r"[^0-9]{0,60}?\b(?P<word>above|over|greater than|exceeds|exceeding|below|under|less than)\b"
    r"[^0-9]{0,30}?(?P<currency>US\$|USD|A\$|AU\$|AUD)\s*(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)",
    flags=re.IGNORECASE,
)
MARKET_FIELD_MAP = {
    ("gold", "usd"): "gold_price_usd_oz",
    ("gold", "aud"): "gold_price_aud_oz",
    ("silver", "usd"): "silver_price_usd_oz",
    ("silver", "aud"): "silver_price_aud_oz",
    ("copper", "usd"): "copper_price_usd_lb",
    ("copper", "aud"): "copper_price_aud_lb",
    ("lithium", "usd"): "lithium_price_usd_kg",
    ("lithium", "aud"): "lithium_price_aud_kg",
    ("uranium", "usd"): "uranium_price_usd_lb",
    ("uranium", "aud"): "uranium_price_aud_lb",
    ("brent", "usd"): "brent_price_usd_bbl",
    ("brent", "aud"): "brent_price_aud_bbl",
    ("wti", "usd"): "wti_price_usd_bbl",
    ("wti", "aud"): "wti_price_aud_bbl",
    ("henry hub", "usd"): "henry_hub_price_usd_mmbtu",
    ("henry hub", "aud"): "henry_hub_price_aud_mmbtu",
    ("henry_hub", "usd"): "henry_hub_price_usd_mmbtu",
    ("henry_hub", "aud"): "henry_hub_price_aud_mmbtu",
    ("natural gas", "usd"): "henry_hub_price_usd_mmbtu",
    ("natural gas", "aud"): "henry_hub_price_aud_mmbtu",
}

SemanticAdjudicatorFn = Callable[[Dict[str, Any]], Any]


@dataclass
class ThesisRelationship:
    priority: int = 1
    kind: str = "no_relation"
    strength: str = "none"
    direction: str = "neutral"
    label: str = ""
    condition_id: str = ""
    scenario: str = ""
    group: str = ""
    confidence: float = 0.0
    summary: str = "No saved thesis relationship found."
    evaluation: Optional[ConditionEvaluation] = None


@dataclass
class ThesisComparator:
    """Compare announcement evidence to explicit thesis-map and watchlist conditions."""

    semantic_adjudicator: Optional[SemanticAdjudicatorFn] = None
    max_semantic_adjudications: int = 3

    def compare(self, facts: AnnouncementFacts, baseline_run: BaselineRunPacket) -> ComparisonReport:
        ctx = self._comparison_context(facts, baseline_run)
        evaluations = self._collect_evaluations(facts, ctx)
        return self._build_report(facts, baseline_run, ctx, evaluations)

    async def compare_async(self, facts: AnnouncementFacts, baseline_run: BaselineRunPacket) -> ComparisonReport:
        ctx = self._comparison_context(facts, baseline_run)
        evaluations = await self._collect_evaluations_async(facts, baseline_run, ctx)
        return self._build_report(facts, baseline_run, ctx, evaluations)

    def _comparison_context(self, facts: AnnouncementFacts, baseline_run: BaselineRunPacket) -> Dict[str, Any]:
        structured = self._structured(baseline_run)
        thesis_map = structured.get("thesis_map") if isinstance(structured.get("thesis_map"), dict) else {}
        current_state = (structured.get("extended_analysis") or {}).get("current_thesis_state") if isinstance(structured.get("extended_analysis"), dict) else {}
        watchlist = structured.get("monitoring_watchlist") if isinstance(structured.get("monitoring_watchlist"), dict) else {}
        verification_queue = structured.get("verification_queue") if isinstance(structured.get("verification_queue"), list) else []
        baseline_path = self._normalize_path((current_state or {}).get("leaning"))

        context_haystack = self._build_haystack(facts)
        evidence_haystack = self._build_evidence_haystack(facts) or context_haystack
        evidence = facts.evidence[0] if facts.evidence else EvidenceRef(source_title=facts.title)
        market_facts = self._normalized_market_facts(facts.market_facts)
        return {
            "structured": structured,
            "thesis_map": thesis_map,
            "watchlist": watchlist,
            "verification_queue": verification_queue,
            "baseline_path": baseline_path,
            "context_haystack": context_haystack,
            "evidence_haystack": evidence_haystack,
            "evidence": evidence,
            "market_facts": market_facts,
        }

    def _collect_evaluations(self, facts: AnnouncementFacts, ctx: Dict[str, Any]) -> List[ConditionEvaluation]:
        if self._has_valid_model_judgement(facts):
            return self._evaluate_model_judgement(facts, ctx["evidence"], ctx.get("watchlist"))

        thesis_map = ctx["thesis_map"]
        watchlist = ctx["watchlist"]
        verification_queue = ctx["verification_queue"]
        evidence_haystack = ctx["evidence_haystack"]
        evidence = ctx["evidence"]
        market_facts = ctx["market_facts"]
        evaluations: List[ConditionEvaluation] = []
        for scenario in ("bull", "base", "bear"):
            block = thesis_map.get(scenario) if isinstance(thesis_map, dict) else {}
            evaluations.extend(self._evaluate_items(block.get("required_conditions") or [], scenario, "required", evidence_haystack, market_facts, evidence))
            evaluations.extend(self._evaluate_items(block.get("failure_conditions") or [], scenario, "failure", evidence_haystack, market_facts, evidence))

        evaluations.extend(self._evaluate_watchlist(watchlist.get("red_flags") or [], "red_flag", evidence_haystack, market_facts, evidence))
        evaluations.extend(self._evaluate_watchlist(watchlist.get("confirmatory_signals") or [], "confirmatory", evidence_haystack, market_facts, evidence))
        evaluations.extend(self._evaluate_verification_queue(verification_queue, evidence_haystack, market_facts, evidence))
        return evaluations

    async def _collect_evaluations_async(
        self,
        facts: AnnouncementFacts,
        baseline_run: BaselineRunPacket,
        ctx: Dict[str, Any],
    ) -> List[ConditionEvaluation]:
        if self._has_valid_model_judgement(facts):
            return self._evaluate_model_judgement(facts, ctx["evidence"], ctx.get("watchlist"))

        thesis_map = ctx["thesis_map"]
        watchlist = ctx["watchlist"]
        verification_queue = ctx["verification_queue"]
        evidence_haystack = ctx["evidence_haystack"]
        evidence = ctx["evidence"]
        market_facts = ctx["market_facts"]
        evaluations: List[ConditionEvaluation] = []
        for scenario in ("bull", "base", "bear"):
            block = thesis_map.get(scenario) if isinstance(thesis_map, dict) else {}
            evaluations.extend(self._evaluate_items(block.get("required_conditions") or [], scenario, "required", evidence_haystack, market_facts, evidence))
            evaluations.extend(self._evaluate_items(block.get("failure_conditions") or [], scenario, "failure", evidence_haystack, market_facts, evidence))

        evaluations.extend(
            await self._evaluate_watchlist_async(
                watchlist.get("red_flags") or [],
                "red_flag",
                evidence_haystack,
                market_facts,
                evidence,
                facts,
                baseline_run,
            )
        )
        evaluations.extend(
            await self._evaluate_watchlist_async(
                watchlist.get("confirmatory_signals") or [],
                "confirmatory",
                evidence_haystack,
                market_facts,
                evidence,
                facts,
                baseline_run,
            )
        )
        evaluations.extend(self._evaluate_verification_queue(verification_queue, evidence_haystack, market_facts, evidence))
        return evaluations

    def _build_report(
        self,
        facts: AnnouncementFacts,
        baseline_run: BaselineRunPacket,
        ctx: Dict[str, Any],
        evaluations: List[ConditionEvaluation],
    ) -> ComparisonReport:
        structured = ctx["structured"]
        baseline_path = ctx["baseline_path"]
        evidence_haystack = ctx["evidence_haystack"]
        evidence = ctx["evidence"]
        market_facts = ctx["market_facts"]
        matched_evals = [item for item in evaluations if item.status == "matched"]
        announcement_matched_evals = [
            item for item in matched_evals if str(item.matched_via or "").strip() != "market_facts"
        ]
        announcement_engaged_evals = [
            item
            for item in evaluations
            if item.status in PARTIAL_MATCH_STATUSES and str(item.matched_via or "").strip() != "market_facts"
        ]
        matched_condition_ids = [
            item.condition_id
            for item in announcement_matched_evals
            if item.group in {"required", "failure"} and item.condition_id
        ]
        triggered_watchlist_ids = [
            item.condition_id
            for item in announcement_engaged_evals
            if item.group in {"red_flag", "confirmatory"} and item.condition_id
        ]
        triggered_verification_ids = [
            item.condition_id
            for item in announcement_engaged_evals
            if item.group == "verification" and item.condition_id
        ]
        thesis_match_confidence = self._thesis_match_confidence(evaluations, announcement_engaged_evals, facts)
        direct_thesis_match_count = sum(
            1
            for item in announcement_matched_evals
            if item.group in {"required", "failure"}
        )

        bull_required = self._matched_count(announcement_matched_evals, scenario="bull", group="required")
        base_required = self._matched_count(announcement_matched_evals, scenario="base", group="required")
        bear_required = self._matched_count(announcement_matched_evals, scenario="bear", group="required")
        bull_failure = self._matched_count(announcement_matched_evals, scenario="bull", group="failure")
        base_failure = self._matched_count(announcement_matched_evals, scenario="base", group="failure")
        thesis_required_hits = self._matched_count(announcement_matched_evals, group="required")
        thesis_failure_hits = self._matched_count(announcement_matched_evals, group="failure")
        red_flag_hits = self._matched_count(announcement_matched_evals, group="red_flag")
        confirmatory_hits = self._matched_count(announcement_matched_evals, group="confirmatory")
        verification_hits = self._matched_count(announcement_matched_evals, group="verification")
        red_flag_partial_hits = sum(
            1
            for item in announcement_engaged_evals
            if item.group == "red_flag" and item.status == "partial_match"
        )
        confirmatory_partial_hits = sum(
            1
            for item in announcement_engaged_evals
            if item.group == "confirmatory" and item.status == "partial_match"
        )
        verification_partial_hits = sum(
            1
            for item in announcement_engaged_evals
            if item.group == "verification" and item.status == "partial_match"
        )

        positive = self._contains_any(evidence_haystack, POSITIVE_TOKENS)
        negative = self._contains_any(evidence_haystack, NEGATIVE_TOKENS)
        affected_domains = self._infer_domains(
            facts=facts,
            matched_evaluations=announcement_engaged_evals,
        )

        semantic_materiality = str(facts.materiality or "").strip().lower()
        announcement_class = str(facts.announcement_class or "").strip().lower()
        trajectory_effect = str(facts.trajectory_effect or "").strip().lower()
        market_match_count = self._matched_count([item for item in matched_evals if str(item.matched_via or '').strip() == "market_facts"])
        relationship = self._resolve_dominant_relationship(
            facts=facts,
            announcement_engaged_evals=announcement_engaged_evals,
            market_match_count=market_match_count,
            semantic_materiality=semantic_materiality,
            announcement_class=announcement_class,
            trajectory_effect=trajectory_effect,
            positive=positive,
            negative=negative,
        )
        model_verdict = self._model_trajectory_verdict(facts)
        same_case_low_maintenance = self._model_verdict_is_low_same_case_maintenance(
            facts=facts,
            baseline_path=baseline_path,
            verdict=model_verdict,
            relationship=relationship,
        )
        neutral_saved_relationship = self._saved_relationship_is_low_materiality_maintenance(
            facts=facts,
            relationship=relationship,
        )
        insider_buying_signal = str(relationship.kind or "").strip().lower() == "insider_buying"
        neutral_path = (
            not insider_buying_signal
            and (
                self._model_verdict_is_neutral_terminal(model_verdict)
                or same_case_low_maintenance
                or neutral_saved_relationship
            )
        )
        if neutral_path:
            relationship = replace(relationship, direction="neutral")
        current_path = (
            baseline_path
            if neutral_path
            else self._current_path_from_relationship(baseline_path, relationship)
        )
        path_transition = f"{baseline_path}->{current_path}" if baseline_path and current_path and baseline_path != current_path else ""

        key_findings, conflicts = self._build_findings(announcement_engaged_evals)
        material_change_types = list(sorted(affected_domains))
        impact_level = self._impact_level(
            affected_domains,
            current_path,
            baseline_path,
            conflicts,
            red_flag_hits,
            confirmatory_hits,
            verification_hits,
            semantic_materiality=semantic_materiality,
        )
        if relationship.kind == "material_unmapped":
            key_findings.insert(
                0,
                ComparisonFinding(
                    type="unmapped_material_filing",
                    summary=(
                        "Material announcement did not match a saved thesis-map or watchlist condition; "
                        "review thesis-map coverage before treating it as immaterial."
                    ),
                    severity=impact_level if impact_level in {"low", "medium", "high", "critical"} else "medium",
                    evidence=evidence,
                ),
            )
        timeline_effect = self._timeline_effect(affected_domains, positive, negative, evaluations)
        thesis_effect = self._thesis_effect_from_relationship(
            relationship,
            trajectory_effect=trajectory_effect,
        )
        capital_effect = self._capital_effect(affected_domains, positive, negative, evaluations)
        filing_type = self._filing_type(announcement_class=announcement_class, relationship=relationship)
        evidence_scope = self._evidence_scope(
            relationship=relationship,
            direct_match_count=len(announcement_engaged_evals),
            market_match_count=market_match_count,
        )
        thesis_relationship = self._thesis_relationship(relationship)
        if filing_type == "administrative":
            relationship = replace(relationship, direction="neutral")
            current_path = baseline_path
            path_transition = ""
            evidence_scope = "administrative_record"
            thesis_relationship = "unrelated"
        impact_verdict = self._impact_verdict_from_model(
            verdict=model_verdict,
            relationship=relationship,
        )
        if filing_type == "administrative":
            impact_verdict = "neutral"
        if not insider_buying_signal and (same_case_low_maintenance or neutral_saved_relationship):
            impact_verdict = "neutral"
        impact_dimension = self._impact_dimension(
            verdict=model_verdict,
            domains=affected_domains,
            timeline_effect=timeline_effect,
            capital_effect=capital_effect,
        )
        trajectory_state = self._trajectory_state_from_axes(
            filing_type=filing_type,
            impact_verdict=impact_verdict,
            impact_dimension=impact_dimension,
        )
        if impact_verdict in {"neutral", "uncertain"}:
            thesis_effect = "no_change"
        run_validity = self._run_validity(impact_level, current_path, baseline_path, conflicts, red_flag_hits)

        used_market_fields = {
            item.market_field: market_facts.get(item.market_field)
            for item in evaluations
            if item.market_field and market_facts.get(item.market_field) is not None
        }
        notes = [
            f"announcement_bull_required_matches={bull_required}",
            f"announcement_base_required_matches={base_required}",
            f"announcement_bear_required_matches={bear_required}",
            f"announcement_red_flag_hits={red_flag_hits}",
            f"announcement_confirmatory_hits={confirmatory_hits}",
            f"announcement_verification_hits={verification_hits}",
            f"announcement_verification_partial_hits={verification_partial_hits}",
            f"market_condition_matches={market_match_count}",
            f"announcement_class={announcement_class or 'unknown'}",
            f"filing_type={filing_type or 'unknown'}",
            f"materiality={semantic_materiality or 'unknown'}",
            f"evidence_scope={evidence_scope or 'unknown'}",
            f"thesis_relationship={thesis_relationship or 'unknown'}",
            f"impact_verdict={impact_verdict or 'unknown'}",
            f"impact_dimension={impact_dimension or 'unknown'}",
            f"relationship_priority={relationship.priority}",
            f"relationship_kind={relationship.kind or 'unknown'}",
            f"relationship_strength={relationship.strength or 'unknown'}",
            f"relationship_direction={relationship.direction or 'unknown'}",
            f"trajectory_state={trajectory_state or 'unknown'}",
        ]
        trajectory_projection = self._trajectory_projection(
            structured=structured,
            baseline_run=baseline_run,
            facts=facts,
            baseline_path=baseline_path,
            current_path=current_path,
            impact_level=impact_level,
            trajectory_state=trajectory_state,
            direct_match_count=direct_thesis_match_count,
            verification_match_count=verification_hits + verification_partial_hits,
        )
        score_trajectory_effect = trajectory_effect
        score_timeline_effect = timeline_effect
        score_positive = positive
        score_negative = negative
        if model_verdict:
            direction = str(model_verdict.get("direction") or "").strip().lower()
            score_positive = direction == "positive"
            score_negative = direction == "negative"
        if insider_buying_signal:
            score_positive = True
            score_negative = False
        if not insider_buying_signal and (same_case_low_maintenance or neutral_saved_relationship):
            score_positive = False
            score_negative = False
        if thesis_relationship == "related_unmapped":
            score_trajectory_effect = trajectory_effect or "material_update"
        trajectory_score = build_trajectory_score(
            baseline_path=baseline_path,
            current_path=current_path,
            trajectory_state=trajectory_state,
            trajectory_effect=score_trajectory_effect,
            thesis_effect=thesis_effect,
            timeline_effect=score_timeline_effect,
            impact_level=impact_level,
            materiality=semantic_materiality,
            classification_confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
            thesis_match_confidence=thesis_match_confidence,
            direct_match_count=direct_thesis_match_count,
            thesis_required_hits=thesis_required_hits,
            thesis_failure_hits=thesis_failure_hits,
            red_flag_hits=red_flag_hits,
            confirmatory_hits=confirmatory_hits,
            red_flag_partial_hits=red_flag_partial_hits,
            confirmatory_partial_hits=confirmatory_partial_hits,
            verification_hits=verification_hits,
            verification_partial_hits=verification_partial_hits,
            positive=score_positive,
            negative=score_negative,
            impact_verdict=impact_verdict,
            thesis_relationship=thesis_relationship,
            price_time_effect=str(facts.price_time_effect or "").strip(),
        )
        trajectory_score.update(
            {
                "relationship_priority": relationship.priority,
                "relationship_kind": relationship.kind,
                "relationship_strength": relationship.strength,
                "relationship_direction": relationship.direction,
                "relationship_summary": relationship.summary,
            }
        )

        return ComparisonReport(
            ticker=facts.ticker,
            baseline_run_id=baseline_run.run_id,
            announcement_title=facts.title,
            baseline_path=baseline_path,
            current_path=current_path,
            path_transition=path_transition,
            path_confidence=self._path_confidence(bull_required, base_required, bear_required, red_flag_hits, confirmatory_hits),
            run_validity=run_validity,
            impact_level=impact_level,
            thesis_effect=thesis_effect,
            timeline_effect=timeline_effect,
            capital_effect=capital_effect,
            announcement_class=announcement_class,
            filing_type=filing_type,
            materiality=semantic_materiality,
            evidence_scope=evidence_scope,
            thesis_relationship=thesis_relationship,
            impact_verdict=impact_verdict,
            impact_dimension=impact_dimension,
            relationship_priority=relationship.priority,
            relationship_kind=relationship.kind,
            relationship_strength=relationship.strength,
            relationship_direction=relationship.direction,
            relationship_summary=relationship.summary,
            trajectory_state=trajectory_state,
            trajectory_effect=trajectory_effect,
            price_time_effect=str(facts.price_time_effect or "").strip(),
            semantic_summary=str(facts.semantic_summary or "").strip(),
            filing_summary=str(facts.filing_summary or "").strip(),
            parser_confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
            source_confidence=float(facts.source_confidence or 0.0),
            extraction_confidence=float(facts.extraction_confidence or 0.0),
            classification_confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
            thesis_match_confidence=thesis_match_confidence,
            classification_reason=str(facts.classification_reason or "").strip(),
            confidence_breakdown=self._confidence_breakdown(facts, thesis_match_confidence, evaluations, announcement_engaged_evals),
            affected_domains=material_change_types,
            material_change_types=material_change_types,
            condition_evaluations=evaluations,
            matched_condition_ids=matched_condition_ids,
            triggered_watchlist_ids=triggered_watchlist_ids,
            triggered_verification_ids=triggered_verification_ids,
            market_facts_used=used_market_fields,
            trajectory_score=trajectory_score,
            trajectory_projection=trajectory_projection,
            key_findings=key_findings,
            conflicts_with_run=conflicts,
            notes=notes,
        )

    @staticmethod
    def _structured(baseline_run: BaselineRunPacket) -> Dict[str, Any]:
        lab_payload = baseline_run.lab_payload if isinstance(baseline_run.lab_payload, dict) else {}
        structured = lab_payload.get("structured_data") if isinstance(lab_payload.get("structured_data"), dict) else {}
        return structured if isinstance(structured, dict) else {}

    @staticmethod
    def _normalize_path(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text.startswith("bull"):
            return "bull"
        if text.startswith("base"):
            return "base"
        if text.startswith("bear"):
            return "bear"
        if "mixed" in text:
            return "mixed"
        return "unknown"

    @staticmethod
    def _build_haystack(facts: AnnouncementFacts) -> str:
        parts = [facts.title, facts.summary, facts.raw_text_excerpt] + list(facts.extracted_facts or [])
        return "\n".join(str(part or "") for part in parts).lower()

    @staticmethod
    def _build_evidence_haystack(facts: AnnouncementFacts) -> str:
        """Primary filing text only; excludes derived summaries and router labels."""
        quote_excerpts = [item.quote_excerpt for item in (facts.evidence or []) if str(item.quote_excerpt or "").strip()]
        parts = [facts.title, facts.raw_text_excerpt] + quote_excerpts
        return "\n".join(str(part or "") for part in parts if str(part or "").strip()).lower()

    @staticmethod
    def _normalized_market_facts(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        normalized = payload.get("normalized_facts") if isinstance(payload.get("normalized_facts"), dict) else {}
        return {str(key): value for key, value in normalized.items()} if isinstance(normalized, dict) else {}

    def _evaluate_model_judgement(
        self,
        facts: AnnouncementFacts,
        fallback_evidence: EvidenceRef,
        watchlist: Any = None,
    ) -> List[ConditionEvaluation]:
        payload = facts.model_judgement if isinstance(facts.model_judgement, dict) else {}
        if str(payload.get("status") or "").strip().lower() != "valid":
            return []
        rows: List[ConditionEvaluation] = []
        relationships = payload.get("thesis_relationships") if isinstance(payload.get("thesis_relationships"), list) else []
        watchlist_groups = self._watchlist_group_lookup(watchlist)
        for idx, item in enumerate(relationships):
            evaluation = self._model_relationship_evaluation(
                item,
                idx,
                fallback_evidence,
                watchlist_groups=watchlist_groups,
            )
            if evaluation is not None:
                rows.append(evaluation)
        return rows

    @staticmethod
    def _watchlist_group_lookup(watchlist: Any) -> Dict[str, str]:
        if not isinstance(watchlist, dict):
            return {}
        out: Dict[str, str] = {}
        for group, key in (("red_flag", "red_flags"), ("confirmatory", "confirmatory_signals")):
            rows = watchlist.get(key) if isinstance(watchlist.get(key), list) else []
            for idx, item in enumerate(rows or []):
                payload = ThesisComparator._watchlist_payload(item, group, idx)
                if not payload:
                    continue
                condition_id = str(payload.get("condition_id") or "").strip()
                label = str(payload.get("condition") or "").strip()
                for value in {condition_id, label, ThesisComparator._slug_id(label)}:
                    if value:
                        out[value.strip().lower()] = group
        return out

    @staticmethod
    def _slug_id(value: Any) -> str:
        text = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip()).strip("_").lower()
        return text

    @staticmethod
    def _has_valid_model_judgement(facts: AnnouncementFacts) -> bool:
        payload = facts.model_judgement if isinstance(facts.model_judgement, dict) else {}
        return str(payload.get("status") or "").strip().lower() == "valid"

    @staticmethod
    def _model_trajectory_verdict(facts: AnnouncementFacts) -> Dict[str, str]:
        payload = facts.model_judgement if isinstance(facts.model_judgement, dict) else {}
        if str(payload.get("status") or "").strip().lower() != "valid":
            return {}
        verdict = payload.get("trajectory_verdict") if isinstance(payload.get("trajectory_verdict"), dict) else {}
        state = str(verdict.get("state") or "").strip().lower()
        direction = str(verdict.get("direction") or "").strip().lower()
        if not state:
            return {}
        return {
            "state": state,
            "direction": direction,
            "recommended_case": str(verdict.get("recommended_case") or "").strip().lower(),
        }

    @staticmethod
    def _model_verdict_is_neutral_terminal(verdict: Dict[str, str]) -> bool:
        state = str((verdict or {}).get("state") or "").strip().lower()
        return state in TERMINAL_NEUTRAL_STATES

    @staticmethod
    def _filing_type(*, announcement_class: str, relationship: ThesisRelationship) -> str:
        announcement = str(announcement_class or "").strip().lower()
        kind = str(getattr(relationship, "kind", "") or "").strip().lower()
        if kind == "insider_buying":
            return "company_event"
        if announcement == "administrative" or kind == "administrative":
            return "administrative"
        if announcement == "market_backdrop" or kind == "market_backdrop_only":
            return "market_context"
        if announcement in {"", "unknown", "needs_classification"}:
            return "unclassified"
        return "company_event"

    @staticmethod
    def _evidence_scope(
        *,
        relationship: ThesisRelationship,
        direct_match_count: int,
        market_match_count: int,
    ) -> str:
        kind = str(getattr(relationship, "kind", "") or "").strip().lower()
        if kind == "administrative":
            return "administrative_record"
        if kind == "market_backdrop_only":
            return "market_backdrop_only"
        if kind in {"saved_thesis_condition", "saved_thesis_failure"} or int(direct_match_count or 0) > 0:
            return "saved_thesis_evidence"
        if kind in {"watchlist_red_flag", "watchlist_confirmatory"}:
            return "saved_watchlist_evidence"
        if kind == "verification_queue":
            return "saved_verification_evidence"
        if kind == "material_unmapped":
            return "unmapped_filing_evidence"
        if int(market_match_count or 0) > 0:
            return "market_backdrop_only"
        return "filing_evidence"

    @staticmethod
    def _thesis_relationship(relationship: ThesisRelationship) -> str:
        kind = str(getattr(relationship, "kind", "") or "").strip().lower()
        if kind in {"saved_thesis_condition", "saved_thesis_failure"}:
            return "direct_match"
        if kind in {"watchlist_red_flag", "watchlist_confirmatory"}:
            return "watchlist_match"
        if kind == "verification_queue":
            return "verification_match"
        if kind == "material_unmapped":
            return "related_unmapped"
        if kind == "needs_classification":
            return "unresolved"
        return "unrelated"

    @staticmethod
    def _impact_verdict_from_model(*, verdict: Dict[str, str], relationship: ThesisRelationship) -> str:
        direction = str((verdict or {}).get("direction") or "").strip().lower()
        relationship_kind = str(getattr(relationship, "kind", "") or "").strip().lower()
        relationship_direction = str(getattr(relationship, "direction", "") or "").strip().lower()
        if direction == "neutral" and relationship_kind == "insider_buying" and relationship_direction == "positive":
            return "positive"
        if direction in {"positive", "negative", "neutral", "mixed"}:
            return direction
        if direction in {"unclear", "uncertain"}:
            return "uncertain"
        state = str((verdict or {}).get("state") or "").strip().lower()
        if state in {"thesis_strengthened", "timeline_accelerated", "risk_reduced"}:
            return "positive"
        if state in {"thesis_weakened", "timeline_delayed", "risk_increased"}:
            return "negative"
        if state == "needs_classification":
            return "uncertain"
        if relationship_direction in {"positive", "negative", "mixed"}:
            return relationship_direction
        if str(getattr(relationship, "kind", "") or "").strip().lower() == "needs_classification":
            return "uncertain"
        return "neutral"

    @staticmethod
    def _impact_dimension(
        *,
        verdict: Dict[str, str],
        domains: Set[str],
        timeline_effect: str,
        capital_effect: str,
    ) -> str:
        state = str((verdict or {}).get("state") or "").strip().lower()
        timeline = str(timeline_effect or "").strip().lower()
        capital = str(capital_effect or "").strip().lower()
        if state in {"timeline_accelerated", "timeline_delayed"} or timeline in {"accelerated", "delayed"}:
            return "timeline"
        if state in {"risk_reduced", "risk_increased"}:
            return "risk"
        if capital in {"improves", "worsens", "material_change"}:
            return "funding"
        ordered = [
            ("financing", "funding"),
            ("capital_structure", "funding"),
            ("balance_sheet", "funding"),
            ("commercial_customer", "commercial"),
            ("commercial", "commercial"),
            ("customer", "commercial"),
            ("operations", "operations"),
            ("production", "operations"),
            ("regulatory", "regulatory"),
            ("regulatory_legal", "regulatory"),
            ("legal", "regulatory"),
            ("permitting", "regulatory"),
            ("governance", "governance"),
            ("management", "governance"),
            ("asset_project", "operations"),
            ("drilling_exploration", "operations"),
            ("resource", "operations"),
        ]
        clean_domains = {str(item or "").strip().lower() for item in domains}
        for source, dimension in ordered:
            if source in clean_domains:
                return dimension
        return "general"

    @staticmethod
    def _trajectory_state_from_axes(*, filing_type: str, impact_verdict: str, impact_dimension: str) -> str:
        filing = str(filing_type or "").strip().lower()
        verdict = str(impact_verdict or "").strip().lower()
        dimension = str(impact_dimension or "").strip().lower()
        if filing == "administrative":
            return "administrative_filing"
        if verdict == "positive":
            if dimension == "timeline":
                return "timeline_accelerated"
            if dimension == "risk":
                return "risk_reduced"
            return "thesis_strengthened"
        if verdict == "negative":
            if dimension == "timeline":
                return "timeline_delayed"
            if dimension == "risk":
                return "risk_increased"
            return "thesis_weakened"
        if verdict in {"mixed", "uncertain", "unclear"}:
            return "needs_classification"
        return "no_thesis_change"

    @staticmethod
    def _model_verdict_is_low_same_case_maintenance(
        *,
        facts: AnnouncementFacts,
        baseline_path: str,
        verdict: Dict[str, str],
        relationship: ThesisRelationship,
    ) -> bool:
        state = str((verdict or {}).get("state") or "").strip().lower()
        direction = str((verdict or {}).get("direction") or "").strip().lower()
        recommended = str((verdict or {}).get("recommended_case") or "").strip().lower()
        baseline = str(baseline_path or "").strip().lower()
        materiality = str(facts.materiality or "").strip().lower()
        relationship_direction = str(getattr(relationship, "direction", "") or "").strip().lower()
        relationship_kind = str(getattr(relationship, "kind", "") or "").strip().lower()
        relationship_scenario = str(getattr(relationship, "scenario", "") or "").strip().lower()
        same_case = recommended in {"unchanged", baseline} or (
            recommended == "base" and baseline in {"", "base", "unknown"}
        )
        same_saved_condition = (
            relationship_kind == "saved_thesis_condition"
            and relationship_scenario in {"", baseline}
        )
        if state not in {"thesis_strengthened", "risk_reduced", "timeline_accelerated"}:
            return False
        if direction != "positive":
            return False
        if same_saved_condition and materiality in {"", "none", "low"}:
            return True
        return same_case and materiality in {"", "none", "low"}

    @staticmethod
    def _saved_relationship_is_low_materiality_maintenance(
        *,
        facts: AnnouncementFacts,
        relationship: ThesisRelationship,
    ) -> bool:
        kind = str(getattr(relationship, "kind", "") or "").strip().lower()
        direction = str(getattr(relationship, "direction", "") or "").strip().lower()
        if direction != "neutral":
            return False
        materiality = str(facts.materiality or "").strip().lower()
        return materiality in {"", "none", "low"} and kind in {
            "saved_thesis_condition",
            "saved_thesis_failure",
            "watchlist_red_flag",
            "watchlist_confirmatory",
            "verification_queue",
        }

    @staticmethod
    def _non_duplicate_model_evaluations(
        existing: List[ConditionEvaluation],
        model_evaluations: List[ConditionEvaluation],
    ) -> List[ConditionEvaluation]:
        engaged_keys = {
            (str(item.condition_id or "").strip(), str(item.group or "").strip())
            for item in existing
            if item.status in PARTIAL_MATCH_STATUSES
        }
        out: List[ConditionEvaluation] = []
        for item in model_evaluations:
            key = (str(item.condition_id or "").strip(), str(item.group or "").strip())
            if key in engaged_keys:
                continue
            engaged_keys.add(key)
            out.append(item)
        return out

    @staticmethod
    def _model_relationship_evaluation(
        item: Any,
        idx: int,
        fallback_evidence: EvidenceRef,
        *,
        watchlist_groups: Optional[Dict[str, str]] = None,
    ) -> Optional[ConditionEvaluation]:
        if not isinstance(item, dict):
            return None
        reference_type = str(item.get("reference_type") or "").strip().lower()
        relationship = str(item.get("relationship") or "").strip().lower()
        direction = str(item.get("direction") or "").strip().lower()
        evidence_quote = str(item.get("evidence_quote") or "").strip()
        if reference_type in {"", "none"}:
            return None
        if relationship in {"", "none", "unmapped"}:
            return None
        if not evidence_quote:
            return None
        try:
            confidence = float(item.get("confidence") or 0.0)
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        if confidence < 0.5:
            return None

        label = str(item.get("reference_label") or item.get("reference_id") or f"model reference {idx + 1}").strip()
        condition_id = str(item.get("reference_id") or f"model_reference_{idx + 1}").strip()
        original_watchlist_group = ""
        if reference_type == "watchlist":
            lookup = watchlist_groups if isinstance(watchlist_groups, dict) else {}
            original_watchlist_group = (
                lookup.get(condition_id.strip().lower())
                or lookup.get(label.strip().lower())
                or lookup.get(ThesisComparator._slug_id(label).strip().lower())
                or ""
            )

        group, scenario = ThesisComparator._model_relationship_group(
            reference_type=reference_type,
            relationship=relationship,
            direction=direction,
            scenario=str(item.get("scenario") or "").strip().lower(),
            original_watchlist_group=original_watchlist_group,
        )
        if not group:
            return None
        status = "partial_match" if relationship in {"partially_confirms", "updates"} else "matched"
        if relationship == CHECKED_NOT_TRIGGERED_STATUS or (
            reference_type == "watchlist"
            and direction == "neutral"
            and relationship in {"partially_confirms", "updates"}
        ):
            status = CHECKED_NOT_TRIGGERED_STATUS
            relationship = CHECKED_NOT_TRIGGERED_STATUS
        if relationship == "contradicts":
            status = "matched"
        missing_for_full_match = [
            str(value or "").strip()
            for value in (item.get("missing_for_full_match") or [])
            if str(value or "").strip()
        ][:5]
        if (
            status == "matched"
            and relationship == "confirms"
            and reference_type in {"thesis_map", "timeline"}
            and not ThesisComparator._model_full_match_is_supported(item, evidence_quote)
        ):
            status = "partial_match"
            missing_for_full_match = missing_for_full_match or [
                "filing is related to the saved condition but does not announce that the condition itself occurred"
            ]
        evidence = EvidenceRef(
            source_url=fallback_evidence.source_url,
            quote_excerpt=evidence_quote,
            source_title=fallback_evidence.source_title,
            source_date_utc=fallback_evidence.source_date_utc,
        )
        return ConditionEvaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            status=status,
            reason=str(item.get("reason") or "Model thesis judge matched this saved reference.").strip(),
            confidence=confidence,
            matched_via="model_thesis_judge",
            relationship=ThesisComparator._model_condition_relationship(relationship, status),
            satisfies_condition=status == "matched" and relationship != "contradicts",
            missing_for_full_match=[] if status == "matched" else (
                missing_for_full_match
                or (
                    ["watchlist condition checked but not triggered"]
                    if status == CHECKED_NOT_TRIGGERED_STATUS
                    else ["full saved-condition evidence not established"]
                )
            ),
            severity="high" if direction == "negative" or group in {"failure", "red_flag"} else "medium",
            evidence=evidence,
        )

    @staticmethod
    def _model_full_match_is_supported(item: Dict[str, Any], evidence_quote: str) -> bool:
        label = str(item.get("reference_label") or item.get("reference_id") or "").strip()
        if not label:
            return False
        reason = str(item.get("reason") or "").strip()
        evidence_text = ThesisComparator._normalize_model_match_text(evidence_quote)
        combined = f"{evidence_quote}\n{reason}"
        label_text = ThesisComparator._normalize_model_match_text(label)
        combined_text = ThesisComparator._normalize_model_match_text(combined)
        if label_text and label_text in evidence_text:
            return True

        label_terms = ThesisComparator._model_match_terms(label)
        evidence_terms = ThesisComparator._model_match_terms(evidence_quote)
        if not label_terms:
            return False
        overlap = label_terms & evidence_terms
        precursor_language = any(term in combined_text for term in MODEL_THESIS_PRECURSOR_TERMS)
        if precursor_language:
            return ThesisComparator._evidence_explicitly_satisfies_label(
                label_terms=label_terms,
                evidence_terms=evidence_terms,
                evidence_text=evidence_text,
            )
        if len(label_terms) <= 3:
            return len(overlap) >= 1
        return len(overlap) >= max(2, int(len(label_terms) * 0.5))

    @staticmethod
    def _evidence_explicitly_satisfies_label(*, label_terms: Set[str], evidence_terms: Set[str], evidence_text: str) -> bool:
        if any(term in evidence_terms for term in MODEL_THESIS_TARGET_ONLY_TERMS):
            return False
        if not any(term in evidence_terms for term in MODEL_THESIS_SATISFACTION_TERMS):
            return False
        overlap = label_terms & evidence_terms
        return len(overlap) >= max(3, int(len(label_terms) * 0.75))

    @staticmethod
    def _model_match_terms(value: str) -> Set[str]:
        text = ThesisComparator._normalize_model_match_text(value)
        return {
            term
            for term in re.findall(r"[a-z0-9]+", text)
            if len(term) > 2 and term not in MODEL_ADJUDICATION_STOP_WORDS
        }

    @staticmethod
    def _normalize_model_match_text(value: str) -> str:
        text = str(value or "").lower()
        text = re.sub(r"(?<=\d),(?=\d)", "", text)
        text = re.sub(r"\bbought\s+back\b", "buyback", text)
        text = re.sub(r"\bbuy[-\s]?back\b", "buyback", text)
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _model_relationship_group(
        *,
        reference_type: str,
        relationship: str,
        direction: str,
        scenario: str,
        original_watchlist_group: str = "",
    ) -> Tuple[str, str]:
        scenario_norm = scenario if scenario in {"bull", "base", "bear"} else ""
        is_negative = direction == "negative" or relationship == "contradicts"
        if reference_type == "watchlist":
            original = str(original_watchlist_group or "").strip().lower()
            if original in {"red_flag", "confirmatory"}:
                return original, ""
            if direction == "neutral" or relationship == CHECKED_NOT_TRIGGERED_STATUS:
                return "", ""
            return ("red_flag" if is_negative else "confirmatory"), ""
        if reference_type == "verification":
            return "verification", ""
        if reference_type in {"thesis_map", "timeline"}:
            if is_negative:
                return "failure", scenario_norm or "bear"
            return "required", scenario_norm or "bull"
        return "", scenario_norm

    @staticmethod
    def _model_condition_relationship(relationship: str, status: str) -> str:
        if relationship == "contradicts":
            return "contradicts"
        if relationship == CHECKED_NOT_TRIGGERED_STATUS or status == CHECKED_NOT_TRIGGERED_STATUS:
            return CHECKED_NOT_TRIGGERED_STATUS
        if status == "partial_match":
            return "related_partial_match"
        return "full_match"

    def _evaluate_items(
        self,
        items: Iterable[Any],
        scenario: str,
        group: str,
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> List[ConditionEvaluation]:
        evaluations: List[ConditionEvaluation] = []
        for item in items or []:
            if isinstance(item, dict):
                evaluations.append(self._evaluate_item(item, scenario, group, haystack, market_facts, evidence))
        return evaluations

    def _evaluate_watchlist(
        self,
        items: Iterable[Any],
        group: str,
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> List[ConditionEvaluation]:
        evaluations: List[ConditionEvaluation] = []
        for idx, item in enumerate(items or []):
            if isinstance(item, str):
                item = {
                    "watch_id": f"{group}_{idx}",
                    "condition": str(item or "").strip(),
                }
            if not isinstance(item, dict):
                continue
            condition_id = str(item.get("watch_id") or item.get("condition_id") or f"{group}_{idx}").strip()
            payload = {
                "condition_id": condition_id,
                "condition": str(item.get("condition") or item.get("title") or "").strip(),
                "evidence_hooks": item.get("evidence_hooks") or item.get("source_terms") or [],
                "description": str(item.get("description") or item.get("reason") or item.get("source_to_monitor") or "").strip(),
                "severity": str(item.get("severity") or "").strip().lower(),
                "linked_milestones": item.get("linked_milestones") or [],
            }
            evaluations.append(self._evaluate_item(payload, "", group, haystack, market_facts, evidence))
        return evaluations

    async def _evaluate_watchlist_async(
        self,
        items: Iterable[Any],
        group: str,
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
        facts: AnnouncementFacts,
        baseline_run: BaselineRunPacket,
    ) -> List[ConditionEvaluation]:
        evaluations: List[ConditionEvaluation] = []
        adjudications_used = 0
        for idx, item in enumerate(items or []):
            payload = self._watchlist_payload(item, group, idx)
            if not payload:
                continue
            evaluation = self._evaluate_item(payload, "", group, haystack, market_facts, evidence)
            if (
                evaluation.status in {"not_matched", "partial_match"}
                and self.semantic_adjudicator is not None
                and adjudications_used < max(0, int(self.max_semantic_adjudications or 0))
                and self._should_model_adjudicate_watchlist_item(payload, facts, haystack)
            ):
                adjudications_used += 1
                semantic_eval = await self._model_adjudicate_watchlist_item(
                    payload=payload,
                    group=group,
                    fallback=evaluation,
                    facts=facts,
                    baseline_run=baseline_run,
                    haystack=haystack,
                    evidence=evidence,
                )
                if semantic_eval is not None:
                    evaluation = semantic_eval
            evaluations.append(evaluation)
        return evaluations

    @staticmethod
    def _watchlist_payload(item: Any, group: str, idx: int) -> Dict[str, Any]:
        if isinstance(item, str):
            item = {
                "watch_id": f"{group}_{idx}",
                "condition": str(item or "").strip(),
            }
        if not isinstance(item, dict):
            return {}
        condition_id = str(item.get("watch_id") or item.get("condition_id") or f"{group}_{idx}").strip()
        return {
            "condition_id": condition_id,
            "condition": str(item.get("condition") or item.get("title") or "").strip(),
            "evidence_hooks": item.get("evidence_hooks") or item.get("source_terms") or [],
            "description": str(item.get("description") or item.get("reason") or item.get("source_to_monitor") or "").strip(),
            "severity": str(item.get("severity") or "").strip().lower(),
            "linked_milestones": item.get("linked_milestones") or [],
        }

    def _evaluate_verification_queue(
        self,
        items: Iterable[Any],
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> List[ConditionEvaluation]:
        evaluations: List[ConditionEvaluation] = []
        for idx, item in enumerate(items or []):
            if isinstance(item, str):
                item = {
                    "verification_id": f"verification_{idx}",
                    "field": str(item or "").strip(),
                    "reason": str(item or "").strip(),
                }
            if not isinstance(item, dict):
                continue
            field = str(item.get("field") or item.get("field_path") or item.get("title") or "").strip()
            reason = str(item.get("reason") or item.get("condition") or "").strip()
            required_source = str(item.get("required_source") or item.get("source_to_monitor") or "").strip()
            label = " | ".join(part for part in [field, reason, required_source] if part)
            if not label:
                continue
            condition_id = str(
                item.get("verification_id")
                or item.get("condition_id")
                or item.get("field_path")
                or f"verification_{idx}"
            ).strip()
            hooks = list(item.get("evidence_hooks") or item.get("source_terms") or [])
            hooks.extend(part for part in [field, reason, required_source] if part)
            payload = {
                "condition_id": condition_id,
                "condition": label,
                "evidence_hooks": hooks,
                "severity": str(item.get("priority") or item.get("severity") or "medium").strip().lower(),
                "linked_milestones": item.get("linked_milestones") or [],
            }
            evaluations.append(self._evaluate_item(payload, "", "verification", haystack, market_facts, evidence))
        return evaluations

    def _evaluate_item(
        self,
        item: Dict[str, Any],
        scenario: str,
        group: str,
        haystack: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> ConditionEvaluation:
        condition_id = str(item.get("condition_id") or item.get("watch_id") or "").strip()
        label = self._condition_label(item)
        linked_milestones = [str(value or "").strip() for value in (item.get("linked_milestones") or []) if str(value or "").strip()]
        severity = str(item.get("severity") or ("high" if group in {"failure", "red_flag"} else "medium")).strip().lower()

        market_eval = self._try_market_evaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            linked_milestones=linked_milestones,
            severity=severity,
            market_facts=market_facts,
            evidence=evidence,
        )
        if market_eval is not None:
            return market_eval

        if self._is_market_condition(label):
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="unclear",
                reason="Market-price condition was not text-matched; no parseable market rule was available.",
                confidence=0.35,
                matched_via="market_facts",
                relationship="unclear_market_rule",
                satisfies_condition=False,
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )

        phrases = self._condition_phrases(item)
        matched = next(
            (
                (phrase, source)
                for phrase, source in phrases
                if self._phrase_matches(
                    phrase,
                    haystack,
                    allow_token_fallback=(source == "evidence_hook"),
                )
            ),
            ("", ""),
        )
        matched_phrase, matched_source = matched
        if matched_phrase:
            source_label = "evidence hook" if matched_source == "evidence_hook" else "condition phrase"
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="matched",
                reason=f"Matched primary filing text via {source_label}: {matched_phrase}",
                confidence=0.78 if group in {"required", "confirmatory"} else 0.84,
                matched_via="text",
                relationship="full_match",
                satisfies_condition=True,
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )

        semantic_eval = self._try_semantic_evaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            haystack=haystack,
            linked_milestones=linked_milestones,
            severity=severity,
            evidence=evidence,
        )
        if semantic_eval is not None:
            return semantic_eval

        return ConditionEvaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            status="not_matched",
            reason="No explicit support found in the announcement text or market context.",
            confidence=0.5,
            matched_via="",
            relationship="not_related",
            satisfies_condition=False,
            severity=severity,
            linked_milestones=linked_milestones,
            evidence=evidence,
        )

    def _try_semantic_evaluation(
        self,
        *,
        condition_id: str,
        scenario: str,
        group: str,
        label: str,
        haystack: str,
        linked_milestones: List[str],
        severity: str,
        evidence: EvidenceRef,
    ) -> Optional[ConditionEvaluation]:
        """Strict semantic bridge for saved watchlist items.

        Exact thesis-map conditions stay literal. Watchlist rows are looser
        monitoring intents, so this layer can say "related, but not satisfied"
        without pretending a non-binding disclosure completed a binding catalyst.
        """
        if group not in {"red_flag", "confirmatory"}:
            return None

        label_norm = self._normalize_semantic_text(label)
        haystack_norm = self._normalize_semantic_text(haystack)
        if not label_norm or not haystack_norm:
            return None

        offtake_eval = self._semantic_offtake_evaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            label_norm=label_norm,
            haystack_norm=haystack_norm,
            linked_milestones=linked_milestones,
            severity=severity,
            evidence=evidence,
        )
        if offtake_eval is not None:
            return offtake_eval

        return None

    def _should_model_adjudicate_watchlist_item(
        self,
        payload: Dict[str, Any],
        facts: AnnouncementFacts,
        haystack: str,
    ) -> bool:
        label = self._condition_label(payload)
        if not label or not str(haystack or "").strip():
            return False
        materiality = str(facts.materiality or "").strip().lower()
        effect = str(facts.trajectory_effect or "").strip().lower()
        if materiality not in {"medium", "high", "critical"} and effect not in {
            "material_update",
            "strengthens",
            "weakens",
            "delays",
            "risk_reduced",
        }:
            return False

        candidate_text = " ".join(
            [
                label,
                str(payload.get("description") or ""),
                " ".join(str(item or "") for item in payload.get("evidence_hooks") or []),
            ]
        )
        candidate_terms = self._model_candidate_terms(candidate_text)
        if not candidate_terms:
            return False
        haystack_norm = self._normalize_semantic_text(haystack)
        driver_terms = {
            term
            for value in list(facts.affected_drivers or []) + list(facts.material_topics or [])
            for term in self._model_candidate_terms(str(value or "").replace("_", " "))
        }
        if candidate_terms & driver_terms:
            return True
        return any(term in haystack_norm for term in candidate_terms)

    async def _model_adjudicate_watchlist_item(
        self,
        *,
        payload: Dict[str, Any],
        group: str,
        fallback: ConditionEvaluation,
        facts: AnnouncementFacts,
        baseline_run: BaselineRunPacket,
        haystack: str,
        evidence: EvidenceRef,
    ) -> Optional[ConditionEvaluation]:
        if self.semantic_adjudicator is None:
            return None
        request = {
            "kind": "watchlist_condition_adjudication",
            "ticker": facts.ticker,
            "company_name": facts.company_name or baseline_run.company_name,
            "template_id": baseline_run.template_id,
            "announcement": {
                "title": facts.title,
                "filing_summary": facts.filing_summary,
                "semantic_summary": facts.semantic_summary,
                "announcement_class": facts.announcement_class,
                "materiality": facts.materiality,
                "trajectory_effect": facts.trajectory_effect,
                "affected_drivers": list(facts.affected_drivers or [])[:8],
                "evidence_text": str(haystack or "")[:6000],
            },
            "watchlist_item": {
                "condition_id": fallback.condition_id,
                "group": group,
                "label": fallback.label,
                "description": str(payload.get("description") or "").strip(),
                "evidence_hooks": [str(item or "").strip() for item in (payload.get("evidence_hooks") or []) if str(item or "").strip()][:8],
                "severity": fallback.severity,
            },
            "baseline": {
                "run_id": baseline_run.run_id,
                "current_path": self._normalize_path(
                    ((self._structured(baseline_run).get("extended_analysis") or {}).get("current_thesis_state") or {}).get("leaning")
                    if isinstance((self._structured(baseline_run).get("extended_analysis") or {}), dict)
                    else ""
                ),
            },
        }
        try:
            response = self.semantic_adjudicator(request)
            if inspect.isawaitable(response):
                response = await response
        except Exception:
            return None
        return self._coerce_model_condition_evaluation(
            response,
            fallback=fallback,
            group=group,
            evidence=evidence,
        )

    @staticmethod
    def _coerce_model_condition_evaluation(
        response: Any,
        *,
        fallback: ConditionEvaluation,
        group: str,
        evidence: EvidenceRef,
    ) -> Optional[ConditionEvaluation]:
        if not isinstance(response, dict):
            return None
        status = str(response.get("status") or "").strip().lower()
        relationship = str(response.get("relationship") or "").strip().lower()
        if relationship == "contradiction":
            relationship = "contradicts"
        allowed_statuses = {"matched", "partial_match", CHECKED_NOT_TRIGGERED_STATUS, "contradicted", "not_matched", "unclear"}
        if status not in allowed_statuses:
            if relationship == "full_match":
                status = "matched"
            elif relationship in {"partial_match", "precursor_partial_match", "related_partial_match"}:
                status = "partial_match"
            elif relationship == CHECKED_NOT_TRIGGERED_STATUS:
                status = CHECKED_NOT_TRIGGERED_STATUS
            elif relationship == "contradicts":
                status = "contradicted"
            elif relationship == "not_related":
                status = "not_matched"
            else:
                return None
        try:
            confidence = float(response.get("confidence"))
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        if status == "matched" and confidence < 0.65:
            return None
        if status == "partial_match" and confidence < 0.55:
            return None
        if status in {"not_matched", "unclear"}:
            return None
        satisfies_condition = bool(response.get("satisfies_condition"))
        if status == "matched" and not satisfies_condition:
            return None
        if status == "partial_match":
            satisfies_condition = False
        if status == CHECKED_NOT_TRIGGERED_STATUS:
            satisfies_condition = False
        reason = str(response.get("reason") or "").strip()
        if not reason:
            reason = "Model compared the announcement against this saved watchlist item."
        missing = [
            str(item or "").strip()
            for item in (response.get("missing_for_full_match") or [])
            if str(item or "").strip()
        ][:8]
        return ConditionEvaluation(
            condition_id=fallback.condition_id,
            scenario=fallback.scenario,
            group=group,
            label=fallback.label,
            status=status,
            reason=reason,
            confidence=confidence,
            matched_via="model_semantic_adjudication",
            relationship=relationship or ("full_match" if status == "matched" else status),
            satisfies_condition=satisfies_condition,
            missing_for_full_match=missing,
            severity=fallback.severity,
            linked_milestones=list(fallback.linked_milestones or []),
            evidence=evidence,
        )

    @staticmethod
    def _model_candidate_terms(text: str) -> Set[str]:
        return {
            term
            for term in re.split(r"[^a-z0-9]+", str(text or "").lower())
            if len(term) >= 5 and term not in MODEL_ADJUDICATION_STOP_WORDS
        }

    def _semantic_offtake_evaluation(
        self,
        *,
        condition_id: str,
        scenario: str,
        group: str,
        label: str,
        label_norm: str,
        haystack_norm: str,
        linked_milestones: List[str],
        severity: str,
        evidence: EvidenceRef,
    ) -> Optional[ConditionEvaluation]:
        if "offtake" not in label_norm and "off take" not in label_norm:
            return None
        if "offtake" not in haystack_norm and "off take" not in haystack_norm:
            return None

        condition_requires_binding = "binding" in label_norm or "definitive" in label_norm
        filing_is_non_final = any(term in haystack_norm for term in NON_FINAL_AGREEMENT_TERMS)
        filing_has_binding_terms = any(term in haystack_norm for term in BINDING_AGREEMENT_TERMS)
        filing_has_agreement = any(term in haystack_norm for term in ("agreement", "contract", "customer", "supply"))

        if condition_requires_binding and not filing_has_binding_terms:
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="partial_match",
                reason=(
                    "Announcement is related to the saved offtake watchlist item, but it appears to be an "
                    "LoI, MoU, partnership, or otherwise non-final step rather than a binding offtake agreement."
                ),
                confidence=0.72,
                matched_via="semantic_adjudication",
                relationship="precursor_partial_match",
                satisfies_condition=False,
                missing_for_full_match=[
                    "binding or definitive offtake terms",
                    "counterparty commitment scope",
                    "volume, duration, pricing, or take-or-pay economics where disclosed",
                ],
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )

        if condition_requires_binding and filing_has_binding_terms and not filing_is_non_final:
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="matched",
                reason="Announcement appears to satisfy the saved binding offtake watchlist item.",
                confidence=0.86,
                matched_via="semantic_adjudication",
                relationship="full_match",
                satisfies_condition=True,
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )

        if filing_has_agreement or filing_is_non_final:
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="matched" if not filing_is_non_final else "partial_match",
                reason=(
                    "Announcement engages the saved offtake watchlist item."
                    if not filing_is_non_final
                    else "Announcement is an offtake-related precursor, but not yet a final agreement."
                ),
                confidence=0.78 if not filing_is_non_final else 0.68,
                matched_via="semantic_adjudication",
                relationship="full_match" if not filing_is_non_final else "precursor_partial_match",
                satisfies_condition=not filing_is_non_final,
                missing_for_full_match=[] if not filing_is_non_final else ["final executed offtake agreement"],
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )
        return None

    def _try_market_evaluation(
        self,
        *,
        condition_id: str,
        scenario: str,
        group: str,
        label: str,
        linked_milestones: List[str],
        severity: str,
        market_facts: Dict[str, Any],
        evidence: EvidenceRef,
    ) -> Optional[ConditionEvaluation]:
        match = MARKET_RULE_RE.search(label or "") or MARKET_NATURAL_RULE_RE.search(label or "")
        if not match:
            return None

        asset = str(match.group("asset") or "").strip().lower()
        op = str(match.groupdict().get("op") or "").strip()
        if not op:
            op = self._natural_market_comparator(str(match.groupdict().get("word") or ""))
        currency = str(match.group("currency") or "").strip().lower()
        currency_key = "aud" if currency in {"a$", "au$", "aud"} else "usd"
        raw_value = str(match.group("value") or "").replace(",", "")
        try:
            threshold_value = float(raw_value)
        except ValueError:
            threshold_value = None
        market_field = MARKET_FIELD_MAP.get((asset, currency_key), "")
        observed_value = market_facts.get(market_field) if market_field else None

        if threshold_value is None or not market_field:
            return None

        if observed_value is None:
            return ConditionEvaluation(
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                label=label,
                status="unclear",
                reason=f"Condition depends on {market_field}, but no fresh market fact was available.",
                confidence=0.35,
                matched_via="market_facts",
                relationship="unclear_market_fact",
                satisfies_condition=False,
                market_field=market_field,
                observed_value=None,
                comparator=op,
                threshold_value=threshold_value,
                severity=severity,
                linked_milestones=linked_milestones,
                evidence=evidence,
            )

        comparison_ok = self._compare_numeric(float(observed_value), op, threshold_value)
        return ConditionEvaluation(
            condition_id=condition_id,
            scenario=scenario,
            group=group,
            label=label,
            status="matched" if comparison_ok else "contradicted",
            reason=(
                f"Resolved via {market_field}: observed {float(observed_value):.2f} {op} {threshold_value:.2f}."
                if comparison_ok
                else f"Resolved via {market_field}: observed {float(observed_value):.2f}, which does not satisfy {op} {threshold_value:.2f}."
            ),
            confidence=0.92,
            matched_via="market_facts",
            relationship="full_match" if comparison_ok else "contradicts",
            satisfies_condition=bool(comparison_ok),
            market_field=market_field,
            observed_value=float(observed_value),
            comparator=op,
            threshold_value=threshold_value,
            severity=severity,
            linked_milestones=linked_milestones,
            evidence=evidence,
        )

    @staticmethod
    def _compare_numeric(observed_value: float, comparator: str, threshold_value: float) -> bool:
        if comparator == ">":
            return observed_value > threshold_value
        if comparator == "<":
            return observed_value < threshold_value
        if comparator == ">=":
            return observed_value >= threshold_value
        if comparator == "<=":
            return observed_value <= threshold_value
        return False

    @staticmethod
    def _natural_market_comparator(word: str) -> str:
        normalized = str(word or "").strip().lower()
        if normalized in {"above", "over", "greater than", "exceeds", "exceeding"}:
            return ">"
        if normalized in {"below", "under", "less than"}:
            return "<"
        return ""

    @staticmethod
    def _is_market_condition(label: str) -> bool:
        text = str(label or "").strip().lower()
        if not text:
            return False
        if re.search(r"\b(gold|silver|copper|lithium|uranium|brent|wti|henry hub|natural gas)\b", text) and re.search(
            r"(us\$|usd|a\$|au\$|aud|\$)\s*[0-9]|[<>]|above|below|under|over|greater than|less than",
            text,
        ):
            return True
        return False

    @staticmethod
    def _condition_phrases(item: Dict[str, Any]) -> List[Tuple[str, str]]:
        phrases: List[Tuple[str, str]] = []
        value = str(item.get("condition") or "").strip()
        if value and ThesisComparator._is_meaningful_support_phrase(value):
            phrases.append((value, "condition"))
        for value in item.get("evidence_hooks") or []:
            text = str(value or "").strip()
            if ThesisComparator._is_meaningful_support_phrase(text):
                phrases.append((text, "evidence_hook"))
        return phrases[:6]

    @staticmethod
    def _is_meaningful_support_phrase(text: str) -> bool:
        phrase = str(text or "").strip().lower()
        if not phrase:
            return False
        if " " in phrase:
            return True
        return len(phrase) >= 12

    @staticmethod
    def _phrase_matches(phrase: str, haystack: str, *, allow_token_fallback: bool = False) -> bool:
        low = str(phrase or "").strip().lower()
        if not low:
            return False
        sentences = ThesisComparator._sentences_for_matching(haystack)
        phrase_terms = ThesisComparator._phrase_terms(low)
        for sentence in sentences:
            if low in sentence and not ThesisComparator._negates_phrase(sentence, phrase_terms):
                return True
        if not allow_token_fallback:
            return False
        terms = [term for term in phrase_terms if len(term) >= 5]
        if len(terms) < 3:
            return False
        for sentence in sentences:
            # Loose token matching is only a fallback for explicit evidence hooks.
            # Requiring all meaningful terms avoids matching conditions on generic
            # words like "gold", "quarter", "resource", or dates.
            if all(term in sentence for term in terms) and not ThesisComparator._negates_phrase(sentence, terms):
                return True
        return False

    @staticmethod
    def _sentences_for_matching(text: str) -> List[str]:
        return [
            sentence.strip().lower()
            for sentence in re.split(r"(?<=[.!?])\s+|\n+", str(text or ""))
            if sentence.strip()
        ]

    @staticmethod
    def _phrase_terms(text: str) -> List[str]:
        return [term for term in re.split(r"[^a-z0-9]+", str(text or "").lower()) if len(term) >= 3]

    @staticmethod
    def _normalize_semantic_text(text: str) -> str:
        normalized = re.sub(r"[^a-z0-9$./&+\-\s]", " ", str(text or "").lower())
        normalized = normalized.replace("off-take", "offtake").replace("buy-back", "buyback")
        return re.sub(r"\s+", " ", normalized).strip()

    @staticmethod
    def _negates_phrase(sentence: str, phrase_terms: List[str]) -> bool:
        terms = {term for term in phrase_terms if term}
        action_terms = terms & POSITIVE_ACTION_TOKENS
        if not action_terms:
            return False
        for term in action_terms:
            if ThesisComparator._negates_term(sentence, term):
                return True
        return False

    @staticmethod
    def _negates_term(sentence: str, term: str) -> bool:
        escaped = re.escape(str(term or "").strip().lower())
        if not escaped:
            return False
        return bool(
            re.search(
                rf"\b(?:has|have|had|is|was|were|does|did|do|management)?\s*not\s+(?:yet\s+)?(?:been\s+)?(?:[a-z0-9]+\s+){{0,4}}{escaped}\b",
                sentence,
            )
            or re.search(
                rf"\bno\s+(?:binding\s+)?(?:[a-z0-9]+\s+){{0,4}}{escaped}\b",
                sentence,
            )
        )

    @staticmethod
    def _condition_label(item: Dict[str, Any]) -> str:
        return str(item.get("condition") or item.get("title") or item.get("condition_id") or item.get("watch_id") or "").strip()

    @staticmethod
    def _contains_any(haystack: str, tokens: Set[str]) -> bool:
        positive_set = tokens == POSITIVE_TOKENS
        for sentence in ThesisComparator._sentences_for_matching(haystack):
            if positive_set and ThesisComparator._negates_positive_action(sentence):
                continue
            if any(token in sentence for token in tokens):
                return True
        return False

    @staticmethod
    def _negates_positive_action(sentence: str) -> bool:
        return any(ThesisComparator._negates_term(sentence, term) for term in POSITIVE_ACTION_TOKENS)

    @staticmethod
    def _infer_domains(facts: AnnouncementFacts, matched_evaluations: List[ConditionEvaluation]) -> Set[str]:
        labels = "\n".join(item.label for item in matched_evaluations if str(item.label or "").strip()).lower()
        fact_text = "\n".join(
            [facts.title or "", facts.summary or ""] + [str(item or "") for item in (facts.extracted_facts or [])]
        ).lower()
        haystack = f"{fact_text}\n{labels}"
        domains: Set[str] = set()
        semantic_drivers = [
            str(item or "").strip().lower()
            for item in list(facts.affected_drivers or []) + list(facts.material_topics or [])
            if str(item or "").strip()
        ]
        if ThesisComparator._is_insider_buying_signal(facts):
            return {"governance", "insider_transaction", "management"}
        if str(facts.announcement_class or "").strip().lower() == "administrative" and str(facts.materiality or "").strip().lower() in {"", "none"}:
            return {"administrative"}
        domains.update(driver for driver in semantic_drivers if driver not in {"administrative", "needs_classification"})
        for domain, keywords in DOMAIN_KEYWORDS.items():
            if any(ThesisComparator._keyword_in_text(keyword, haystack) for keyword in keywords):
                domains.add(domain)
        if not domains:
            topics = [str(item or "").strip().lower() for item in (facts.material_topics or []) if str(item or "").strip()]
            if len(topics) <= 3:
                domains.update(topics)
        return domains

    @staticmethod
    def _keyword_in_text(keyword: str, haystack: str) -> bool:
        term = str(keyword or "").strip().lower()
        if not term:
            return False
        if " " in term or "-" in term:
            return term in haystack
        return re.search(rf"\b{re.escape(term)}\b", haystack) is not None

    @staticmethod
    def _is_insider_buying_signal(facts: AnnouncementFacts) -> bool:
        domains = {
            str(item or "").strip().lower()
            for item in list(facts.affected_drivers or []) + list(facts.material_topics or [])
            if str(item or "").strip()
        }
        if "insider_transaction" not in domains:
            return False
        haystack = "\n".join(
            [
                facts.title or "",
                facts.summary or "",
                facts.semantic_summary or "",
                facts.filing_summary or "",
                facts.price_time_effect or "",
            ]
            + [str(item or "") for item in (facts.extracted_facts or [])]
        ).lower()
        if not haystack.strip():
            return False
        non_buying_terms = {
            "consolidation",
            "share consolidation",
            "sale",
            "sold",
            "dispose",
            "disposed",
            "disposal",
            "reduced",
            "decreased",
            "ceased",
            "ceasing",
            "no change",
        }
        if any(ThesisComparator._keyword_in_text(term, haystack) for term in non_buying_terms):
            return False
        buying_terms = {
            "on-market purchase",
            "on market purchase",
            "purchased",
            "purchase of",
            "bought",
            "acquired",
            "increased his stake",
            "increased her stake",
            "increased their stake",
            "increased its stake",
            "increased holding",
            "increased holdings",
        }
        return any(ThesisComparator._keyword_in_text(term, haystack) for term in buying_terms)

    @staticmethod
    def _matched_count(evaluations: List[ConditionEvaluation], *, scenario: str = "", group: str = "") -> int:
        return sum(
            1
            for item in evaluations
            if item.status == "matched"
            and (not scenario or item.scenario == scenario)
            and (not group or item.group == group)
        )

    @staticmethod
    def _choose_current_path(
        *,
        baseline_path: str,
        bull_required: int,
        base_required: int,
        bear_required: int,
        bull_failure: int,
        base_failure: int,
        red_flag_hits: int,
        confirmatory_hits: int,
        positive: bool,
        negative: bool,
    ) -> str:
        if bear_required > 0 or bull_failure > 0 or base_failure > 0:
            return "bear"
        if red_flag_hits > 0 and negative and baseline_path in {"bull", "base"}:
            return "bear"
        if bull_required > 0 and (
            bull_required > base_required
            or (bull_required > 0 and base_required == 0 and (confirmatory_hits > 0 or positive))
        ):
            return "bull"
        if base_required > 0 or confirmatory_hits > 0:
            return "base"
        if positive and baseline_path in {"base", "bear"}:
            return baseline_path
        if negative and baseline_path in {"bull", "base"}:
            return baseline_path
        if baseline_path in {"bull", "base", "bear"}:
            return baseline_path
        return "mixed" if positive and negative else "unknown"

    def _resolve_dominant_relationship(
        self,
        *,
        facts: AnnouncementFacts,
        announcement_engaged_evals: List[ConditionEvaluation],
        market_match_count: int,
        semantic_materiality: str,
        announcement_class: str,
        trajectory_effect: str,
        positive: bool,
        negative: bool,
    ) -> ThesisRelationship:
        candidates = [
            relationship
            for relationship in (
                self._relationship_from_evaluation(item)
                for item in announcement_engaged_evals
            )
            if relationship is not None
        ]
        if candidates:
            return sorted(
                candidates,
                key=lambda item: (
                    int(item.priority or 0),
                    1 if item.strength == "full" else 0,
                    float(item.confidence or 0.0),
                ),
                reverse=True,
            )[0]

        materiality = str(semantic_materiality or "").strip().lower()
        announcement = str(announcement_class or "").strip().lower()
        if self._is_insider_buying_signal(facts):
            return ThesisRelationship(
                priority=2,
                kind="insider_buying",
                strength="partial",
                direction="positive",
                confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
                summary="Director on-market buying is a weak positive governance and alignment signal.",
            )
        if announcement == "administrative" and materiality in {"", "none", "low"}:
            return ThesisRelationship(
                priority=0,
                kind="administrative",
                direction="neutral",
                confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
                summary="Administrative filing with no investment-thesis relationship.",
            )
        if announcement == "needs_classification":
            return ThesisRelationship(
                priority=3,
                kind="needs_classification",
                direction="neutral",
                confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
                summary="Filing could not be classified confidently enough to judge the thesis relationship.",
            )
        if self._is_unmapped_material_filing(
            semantic_materiality=materiality,
            announcement_class=announcement,
            direct_match_count=0,
        ):
            return ThesisRelationship(
                priority=3,
                kind="material_unmapped",
                strength="none",
                direction="neutral",
                confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
                summary=(
                    "Material filing did not match the saved thesis evidence set; no validated scenario "
                    "movement was scored."
                ),
            )
        if int(market_match_count or 0) > 0:
            return ThesisRelationship(
                priority=2,
                kind="market_backdrop_only",
                direction="neutral",
                confidence=0.4,
                summary="Only market backdrop conditions matched; no announcement-led thesis relationship was found.",
            )
        return ThesisRelationship(
            priority=1,
            kind="no_relation",
            direction="neutral",
            confidence=float(facts.classification_confidence or facts.semantic_confidence or 0.0),
            summary="No thesis-relevant relationship was found.",
        )

    @staticmethod
    def _relationship_from_evaluation(item: ConditionEvaluation) -> Optional[ThesisRelationship]:
        if str(item.matched_via or "").strip() == "market_facts":
            return None
        if item.status not in PARTIAL_MATCH_STATUSES:
            return None
        group = str(item.group or "").strip().lower()
        scenario = str(item.scenario or "").strip().lower()
        strength = "full" if item.status == "matched" else "partial"
        label = str(item.label or item.condition_id or "").strip()
        condition_id = str(item.condition_id or "").strip()
        confidence = float(item.confidence or 0.0)

        if group == "failure":
            return ThesisRelationship(
                priority=7,
                kind="saved_thesis_failure",
                strength=strength,
                direction="negative",
                label=label,
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                confidence=confidence,
                summary=f"{strength.title()} saved failure condition: {label}",
                evaluation=item,
            )
        if group == "required":
            direction = "negative" if scenario == "bear" else "positive" if scenario == "bull" else "neutral"
            priority = 7 if direction == "negative" else 6
            return ThesisRelationship(
                priority=priority,
                kind="saved_thesis_condition",
                strength=strength,
                direction=direction,
                label=label,
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                confidence=confidence,
                summary=f"{strength.title()} saved {scenario or 'thesis'} condition: {label}",
                evaluation=item,
            )
        if group == "red_flag":
            return ThesisRelationship(
                priority=7 if strength == "full" else 4,
                kind="watchlist_red_flag",
                strength=strength,
                direction="negative",
                label=label,
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                confidence=confidence,
                summary=f"{strength.title()} red-flag watchlist relationship: {label}",
                evaluation=item,
            )
        if group == "confirmatory":
            return ThesisRelationship(
                priority=5 if strength == "full" else 4,
                kind="watchlist_confirmatory",
                strength=strength,
                direction="positive",
                label=label,
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                confidence=confidence,
                summary=f"{strength.title()} confirmatory watchlist relationship: {label}",
                evaluation=item,
            )
        if group == "verification":
            return ThesisRelationship(
                priority=5 if strength == "full" else 4,
                kind="verification_queue",
                strength=strength,
                direction="positive",
                label=label,
                condition_id=condition_id,
                scenario=scenario,
                group=group,
                confidence=confidence,
                summary=f"{strength.title()} verification relationship: {label}",
                evaluation=item,
            )
        return None

    @staticmethod
    def _relationship_direction_from_facts(*, trajectory_effect: str, positive: bool, negative: bool) -> str:
        effect = str(trajectory_effect or "").strip().lower()
        if effect in {"weakens", "delays", "risk_increased"}:
            return "negative"
        if effect in {"strengthens", "risk_reduced", "accelerates"}:
            return "positive"
        if positive and not negative:
            return "positive"
        if negative and not positive:
            return "negative"
        if positive and negative:
            return "mixed"
        return "neutral"

    @staticmethod
    def _current_path_from_relationship(baseline_path: str, relationship: ThesisRelationship) -> str:
        baseline = str(baseline_path or "").strip().lower()
        if relationship.priority >= 7 and relationship.direction == "negative":
            return "bear"
        if relationship.kind == "saved_thesis_condition":
            if relationship.scenario in {"bull", "base", "bear"}:
                return relationship.scenario
        if baseline in {"bull", "base", "bear"}:
            return baseline
        return "mixed" if relationship.direction == "mixed" else "unknown"

    @staticmethod
    def _thesis_effect_from_relationship(relationship: ThesisRelationship, *, trajectory_effect: str = "") -> str:
        effect = str(trajectory_effect or "").strip().lower()
        if relationship.priority >= 7 and relationship.direction == "negative":
            return "undermines"
        if relationship.direction == "negative":
            return "undermines" if effect == "weakens" else "delays" if effect == "delays" else "undermines"
        if relationship.direction == "positive":
            return "partially_confirms" if relationship.strength == "partial" else "confirms"
        return "no_change"

    @staticmethod
    def _trajectory_state_from_relationship(relationship: ThesisRelationship, *, timeline_effect: str = "") -> str:
        timeline = str(timeline_effect or "").strip().lower()
        if relationship.kind == "administrative":
            return "administrative_filing"
        if relationship.kind == "needs_classification":
            return "needs_classification"
        if relationship.kind == "material_unmapped":
            return "material_unmapped"
        if relationship.kind == "market_backdrop_only":
            return "market_backdrop_only"
        if relationship.kind == "no_relation":
            return "no_thesis_change"
        if relationship.direction == "negative":
            if timeline == "delayed":
                return "timeline_delayed"
            return "thesis_weakened" if relationship.priority >= 7 else "risk_increased"
        if relationship.direction == "positive":
            if timeline == "accelerated":
                return "timeline_accelerated"
            return "thesis_strengthened"
        return "no_thesis_change"

    def _build_findings(self, evaluations: List[ConditionEvaluation]) -> Tuple[List[ComparisonFinding], List[ComparisonFinding]]:
        findings: List[ComparisonFinding] = []
        conflicts: List[ComparisonFinding] = []
        for item in evaluations:
            if item.status not in PARTIAL_MATCH_STATUSES:
                continue
            prefix = "Partially engaged" if item.status == "partial_match" else "Matched"
            finding = ComparisonFinding(
                type=f"{item.group}_{item.status}" if item.group else f"condition_{item.status}",
                summary=f"{prefix} {item.group or 'condition'}: {item.label}",
                severity="high" if item.group in {"failure", "red_flag"} else "low",
                evidence=item.evidence,
            )
            if item.group in {"failure", "red_flag"} or item.scenario == "bear":
                conflicts.append(finding)
            else:
                findings.append(finding)
        return findings[:8], conflicts[:8]

    @staticmethod
    def _impact_level(
        affected_domains: Set[str],
        current_path: str,
        baseline_path: str,
        conflicts: List[ComparisonFinding],
        red_flag_hits: int,
        confirmatory_hits: int,
        verification_hits: int = 0,
        *,
        semantic_materiality: str = "",
    ) -> str:
        materiality = str(semantic_materiality or "").strip().lower()
        if materiality in {"", "none"} and affected_domains <= {"administrative"}:
            return "none"
        if materiality == "critical":
            return "critical"
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "high"
        if conflicts or red_flag_hits > 0:
            return "high"
        if materiality == "high":
            return "high"
        if "insider_transaction" in affected_domains and affected_domains <= {"governance", "insider_transaction", "management"}:
            return "low"
        if current_path == "bull" and baseline_path in {"base", "bear"}:
            return "medium"
        if confirmatory_hits > 0:
            return "medium"
        if verification_hits > 0 and materiality in {"medium", "high", "critical"}:
            return "medium"
        if materiality == "medium":
            return "medium"
        if affected_domains & {"timeline", "operations", "management", "asset_project", "commercial_customer", "drilling_exploration", "clinical_regulatory"}:
            return "medium"
        if affected_domains:
            return "low"
        return "none"

    @staticmethod
    def _thesis_effect(
        baseline_path: str,
        current_path: str,
        conflicts: List[ComparisonFinding],
        confirmatory_hits: int,
        red_flag_hits: int,
        positive: bool,
        negative: bool,
        *,
        trajectory_effect: str = "",
    ) -> str:
        effect = str(trajectory_effect or "").strip().lower()
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "undermines"
        if conflicts or red_flag_hits > 0:
            return "undermines"
        if effect in {"weakens", "delays"}:
            return "undermines" if effect == "weakens" else "delays"
        if current_path == "bull" and baseline_path in {"base", "bear"}:
            return "accelerates"
        if effect in {"strengthens", "risk_reduced"}:
            return "confirms"
        if confirmatory_hits > 0 or positive:
            return "confirms"
        if negative:
            return "partially_confirms"
        return "no_change"

    @staticmethod
    def _is_unmapped_material_filing(
        *,
        semantic_materiality: str,
        announcement_class: str,
        direct_match_count: int,
    ) -> bool:
        if int(direct_match_count or 0) > 0:
            return False
        if str(announcement_class or "").strip().lower() in {"administrative", "market_backdrop"}:
            return False
        return str(semantic_materiality or "").strip().lower() in {"medium", "high", "critical"}

    @staticmethod
    def _trajectory_state(
        *,
        announcement_class: str,
        semantic_materiality: str,
        trajectory_effect: str,
        thesis_effect: str,
        timeline_effect: str,
        direct_match_count: int,
        market_match_count: int,
        conflicts: List[ComparisonFinding],
        path_transition: str,
        verification_match_count: int = 0,
    ) -> str:
        announcement = str(announcement_class or "").strip().lower()
        materiality = str(semantic_materiality or "").strip().lower()
        effect = str(trajectory_effect or "").strip().lower()
        thesis = str(thesis_effect or "").strip().lower()
        timeline = str(timeline_effect or "").strip().lower()
        if announcement == "administrative" and materiality in {"", "none", "low"} and direct_match_count <= 0:
            return "administrative_filing"
        if direct_match_count <= 0 and announcement == "needs_classification":
            return "needs_classification"
        if ThesisComparator._is_unmapped_material_filing(
            semantic_materiality=materiality,
            announcement_class=announcement,
            direct_match_count=direct_match_count,
        ):
            return "material_unmapped"
        if direct_match_count <= 0 and materiality in {"", "none", "low"} and announcement not in {"needs_classification"}:
            return "no_thesis_change"
        if direct_match_count <= 0 and market_match_count > 0:
            return "market_backdrop_only"
        if conflicts or thesis in {"undermines", "invalidates"}:
            return "thesis_weakened"
        if timeline == "delayed" or effect == "delays":
            return "timeline_delayed"
        if timeline == "accelerated" or effect == "accelerates":
            return "timeline_accelerated"
        if thesis in {"confirms", "accelerates"} or effect in {"strengthens", "risk_reduced"} or path_transition:
            return "thesis_strengthened"
        if verification_match_count > 0:
            return "no_thesis_change"
        return "no_thesis_change"

    @staticmethod
    def _timeline_effect(domains: Set[str], positive: bool, negative: bool, evaluations: List[ConditionEvaluation]) -> str:
        if "timeline" not in domains:
            return "unknown"
        if any(item.group in {"failure", "red_flag"} and item.status == "matched" for item in evaluations):
            return "delayed"
        if positive:
            return "accelerated"
        if negative:
            return "delayed"
        return "on_track"

    @staticmethod
    def _capital_effect(domains: Set[str], positive: bool, negative: bool, evaluations: List[ConditionEvaluation]) -> str:
        if "financing" not in domains and "capital_structure" not in domains and "capital_management" not in domains:
            return "unknown"
        if any(item.group in {"failure", "red_flag"} and item.status == "matched" for item in evaluations):
            return "worsens"
        if positive:
            return "improves"
        if negative:
            return "worsens"
        if domains <= {"capital_management"}:
            return "no_change"
        return "material_change"

    @staticmethod
    def _run_validity(
        impact_level: str,
        current_path: str,
        baseline_path: str,
        conflicts: List[ComparisonFinding],
        red_flag_hits: int,
    ) -> str:
        if impact_level == "critical":
            return "invalidated"
        if current_path == "bear" and baseline_path in {"bull", "base"}:
            return "partial_invalidation"
        if conflicts or red_flag_hits > 0:
            return "partial_invalidation"
        if impact_level in {"high", "medium"}:
            return "watch"
        return "intact"

    @staticmethod
    def _path_confidence(
        bull_required: int,
        base_required: int,
        bear_required: int,
        red_flag_hits: int,
        confirmatory_hits: int,
    ) -> float:
        total = bull_required + base_required + bear_required + red_flag_hits + confirmatory_hits
        if total <= 0:
            return 0.0
        strongest = max(bull_required, base_required, bear_required + red_flag_hits)
        return round(float(strongest / total), 3)

    @staticmethod
    def _thesis_match_confidence(
        evaluations: List[ConditionEvaluation],
        announcement_matched_evals: List[ConditionEvaluation],
        facts: AnnouncementFacts,
    ) -> float:
        relevant = [
            item
            for item in evaluations
            if item.group in {"required", "failure", "red_flag", "confirmatory", "verification"}
            and str(item.matched_via or "").strip() != "market_facts"
        ]
        if announcement_matched_evals:
            strongest = max(float(item.confidence or 0.0) for item in announcement_matched_evals)
            return round(min(0.96, strongest + min(0.12, 0.03 * (len(announcement_matched_evals) - 1))), 3)
        if not relevant:
            return 0.0
        if str(facts.announcement_class or "").strip().lower() == "needs_classification":
            return 0.22
        return 0.4

    @staticmethod
    def _confidence_breakdown(
        facts: AnnouncementFacts,
        thesis_match_confidence: float,
        evaluations: List[ConditionEvaluation],
        announcement_matched_evals: List[ConditionEvaluation],
    ) -> Dict[str, Any]:
        payload = dict(facts.confidence_breakdown or {}) if isinstance(facts.confidence_breakdown, dict) else {}
        payload["source_confidence"] = round(float(facts.source_confidence or payload.get("source_confidence") or 0.0), 3)
        payload["extraction_confidence"] = round(float(facts.extraction_confidence or payload.get("extraction_confidence") or 0.0), 3)
        payload["classification_confidence"] = round(
            float(facts.classification_confidence or facts.semantic_confidence or payload.get("classification_confidence") or 0.0),
            3,
        )
        payload["thesis_match_confidence"] = round(float(thesis_match_confidence or 0.0), 3)
        payload["thesis_match"] = {
            "direct_matches": len(announcement_matched_evals),
            "announcement_conditions_checked": len(
                [
                    item
                    for item in evaluations
                    if item.group in {"required", "failure"}
                    and str(item.matched_via or "").strip() != "market_facts"
                ]
            ),
            "watchlist_conditions_checked": len(
                [
                    item
                    for item in evaluations
                    if item.group in {"red_flag", "confirmatory"}
                    and str(item.matched_via or "").strip() != "market_facts"
                ]
            ),
            "verification_conditions_checked": len(
                [
                    item
                    for item in evaluations
                    if item.group == "verification"
                    and str(item.matched_via or "").strip() != "market_facts"
                ]
            ),
        }
        return payload

    def _trajectory_projection(
        self,
        *,
        structured: Dict[str, Any],
        baseline_run: BaselineRunPacket,
        facts: AnnouncementFacts,
        baseline_path: str,
        current_path: str,
        impact_level: str,
        trajectory_state: str,
        direct_match_count: int,
        verification_match_count: int,
    ) -> Dict[str, Any]:
        price_targets = structured.get("price_targets") if isinstance(structured.get("price_targets"), dict) else {}
        market_data = structured.get("market_data") if isinstance(structured.get("market_data"), dict) else {}
        market_data_provenance = (
            structured.get("market_data_provenance")
            if isinstance(structured.get("market_data_provenance"), dict)
            else {}
        )
        market_facts = structured.get("market_facts") if isinstance(structured.get("market_facts"), dict) else {}
        normalized_market = market_facts.get("normalized_facts") if isinstance(market_facts.get("normalized_facts"), dict) else {}
        scenario_targets = price_targets.get("scenario_targets") if isinstance(price_targets.get("scenario_targets"), dict) else {}
        scenario_probabilities = (
            price_targets.get("scenario_probabilities")
            if isinstance(price_targets.get("scenario_probabilities"), dict)
            else {}
        )
        targets_24m_raw = scenario_targets.get("24m") if isinstance(scenario_targets.get("24m"), dict) else {}
        probs_24m_raw = scenario_probabilities.get("24m") if isinstance(scenario_probabilities.get("24m"), dict) else {}
        base_24m = self._to_float(targets_24m_raw.get("base"))
        if base_24m is None:
            base_24m = self._to_float(price_targets.get("target_24m"))
        targets_24m = {
            "bear": self._to_float(targets_24m_raw.get("bear")),
            "base": base_24m,
            "bull": self._to_float(targets_24m_raw.get("bull")),
        }
        targets_12m_raw = scenario_targets.get("12m") if isinstance(scenario_targets.get("12m"), dict) else {}
        base_12m = self._to_float(targets_12m_raw.get("base"))
        if base_12m is None:
            base_12m = self._to_float(price_targets.get("target_12m"))
        targets_12m = {
            "bear": self._to_float(targets_12m_raw.get("bear")),
            "base": base_12m,
            "bull": self._to_float(targets_12m_raw.get("bull")),
        }
        probabilities_24m = {
            "bear": self._to_float(probs_24m_raw.get("bear")),
            "base": self._to_float(probs_24m_raw.get("base")),
            "bull": self._to_float(probs_24m_raw.get("bull")),
        }
        current_price = self._to_float(market_data.get("current_price"))
        if current_price is None:
            current_price = self._to_float(price_targets.get("current_price"))
        prob_weighted_24m = self._to_float(price_targets.get("prob_weighted_target_24m"))
        if prob_weighted_24m is None:
            prob_weighted_24m = self._weighted_target(targets_24m, probabilities_24m)

        baseline_started_at = self._baseline_started_at(baseline_run)
        as_of = self._as_of_datetime(facts) or datetime.now(timezone.utc)
        elapsed_days = None
        elapsed_pct = None
        if baseline_started_at is not None:
            elapsed_days = max(0, (as_of.date() - baseline_started_at.date()).days)
            elapsed_pct = round(min(100.0, (elapsed_days / 730.0) * 100.0), 1)

        market_path = self._market_implied_path(current_price, targets_24m)
        rerun_signal, rerun_reason = self._projection_rerun_signal(
            impact_level=impact_level,
            trajectory_state=trajectory_state,
            baseline_path=baseline_path,
            current_path=current_path,
            direct_match_count=direct_match_count,
            verification_match_count=verification_match_count,
        )

        available = bool(current_price is not None and any(value is not None for value in targets_24m.values()))
        return {
            "available": available,
            "currency": str(
                market_data_provenance.get("prepass_currency")
                or market_data.get("currency")
                or normalized_market.get("currency")
                or "AUD"
            ).strip().upper(),
            "as_of_utc": self._iso(as_of),
            "baseline_started_at_utc": self._iso(baseline_started_at),
            "elapsed_days": elapsed_days,
            "elapsed_pct_24m": elapsed_pct,
            "horizon_months": 24,
            "current_price": current_price,
            "target_12m": targets_12m,
            "target_24m": targets_24m,
            "probability_24m": probabilities_24m,
            "prob_weighted_target_24m": prob_weighted_24m,
            "market_implied_path_24m": market_path,
            "saved_path": baseline_path,
            "router_path": current_path,
            "path_transition": f"{baseline_path}->{current_path}" if baseline_path and current_path and baseline_path != current_path else "",
            "timeline_rows": self._projection_timeline_rows(structured, baseline_run),
            "rerun_signal": rerun_signal,
            "rerun_reason": rerun_reason,
        }

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text or text.lower() in {"n/a", "na", "none", "null"}:
            return None
        text = re.sub(r"[^0-9.\-]", "", text)
        try:
            return float(text)
        except Exception:
            return None

    @classmethod
    def _weighted_target(cls, targets: Dict[str, Optional[float]], probabilities: Dict[str, Optional[float]]) -> Optional[float]:
        rows = [
            (targets.get(name), probabilities.get(name))
            for name in ("bear", "base", "bull")
            if targets.get(name) is not None and probabilities.get(name) is not None
        ]
        total_prob = sum(float(prob or 0.0) for _, prob in rows)
        if not rows or total_prob <= 0:
            return None
        scale = 100.0 if total_prob > 1.5 else 1.0
        return round(sum(float(target) * (float(prob) / scale) for target, prob in rows), 4)

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        if re.fullmatch(r"\d{8}_\d{6}", text):
            text = f"{text[:4]}-{text[4:6]}-{text[6:8]}T{text[9:11]}:{text[11:13]}:{text[13:15]}Z"
        match = re.search(r"(\d{8}_\d{6})", text)
        if match:
            return ThesisComparator._parse_datetime(match.group(1))
        try:
            normalized = text.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    @classmethod
    def _baseline_started_at(cls, baseline_run: BaselineRunPacket) -> Optional[datetime]:
        summary = baseline_run.summary_fields if isinstance(baseline_run.summary_fields, dict) else {}
        lab = baseline_run.lab_payload if isinstance(baseline_run.lab_payload, dict) else {}
        for value in (
            lab.get("created_at"),
            lab.get("updated_at"),
            summary.get("analysis_date"),
            summary.get("created_at"),
            summary.get("updated_at"),
            baseline_run.run_id,
        ):
            parsed = cls._parse_datetime(value)
            if parsed is not None:
                return parsed
        return None

    @classmethod
    def _as_of_datetime(cls, facts: AnnouncementFacts) -> Optional[datetime]:
        for item in facts.evidence or []:
            parsed = cls._parse_datetime(getattr(item, "source_date_utc", ""))
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _iso(value: Optional[datetime]) -> str:
        if value is None:
            return ""
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    @classmethod
    def _market_implied_path(cls, current_price: Optional[float], targets: Dict[str, Optional[float]]) -> str:
        if current_price is None:
            return "unknown"
        rows = [
            (name, float(value))
            for name, value in targets.items()
            if name in {"bear", "base", "bull"} and value is not None
        ]
        if not rows:
            return "unknown"
        rows.sort(key=lambda item: item[1])
        if len(rows) >= 2:
            low_name, low_value = rows[0]
            high_name, high_value = rows[-1]
            if current_price < low_value:
                return f"below_{low_name}"
            if current_price > high_value:
                return f"above_{high_name}"
        closest = min(rows, key=lambda item: abs(item[1] - float(current_price)))
        return closest[0]

    @staticmethod
    def _projection_rerun_signal(
        *,
        impact_level: str,
        trajectory_state: str,
        baseline_path: str,
        current_path: str,
        direct_match_count: int,
        verification_match_count: int,
    ) -> Tuple[str, str]:
        impact = str(impact_level or "").strip().lower()
        state = str(trajectory_state or "").strip().lower()
        if impact in {"critical", "high"} or state in {"thesis_weakened", "timeline_delayed", "risk_increased"}:
            return "rebuild_analysis", "The filing changes or threatens the saved thesis path."
        if baseline_path and current_path and baseline_path != current_path:
            return "refresh_evidence", "The router path differs from the saved run path."
        if state == "material_unmapped":
            return "review_thesis_map", "The filing looks material but no saved thesis condition covers it."
        if verification_match_count > 0:
            return "annotate_evidence", "A verification queue item was touched and should be surfaced in the run evidence."
        if direct_match_count > 0:
            return "annotate_evidence", "The filing matched saved evidence checks without forcing a rebuild."
        return "none", "No thesis-path update was detected."

    @staticmethod
    def _projection_timeline_rows(structured: Dict[str, Any], baseline_run: BaselineRunPacket) -> List[Dict[str, Any]]:
        rows = structured.get("development_timeline")
        if not isinstance(rows, list):
            rows = baseline_run.timeline_rows if isinstance(baseline_run.timeline_rows, list) else []
        catalyst_rows = baseline_run.catalyst_rows if isinstance(baseline_run.catalyst_rows, list) else []
        if not catalyst_rows:
            extended = structured.get("extended_analysis") if isinstance(structured.get("extended_analysis"), dict) else {}
            raw_catalysts = extended.get("next_major_catalysts") if isinstance(extended, dict) else []
            catalyst_rows = raw_catalysts if isinstance(raw_catalysts, list) else []

        out: List[Dict[str, Any]] = []
        seen: Set[Tuple[str, str]] = set()
        for source, source_rows in (("development_timeline", rows), ("next_major_catalysts", catalyst_rows)):
            for item in source_rows:
                row = ThesisComparator._projection_row_from_item(item, source=source)
                if not row:
                    continue
                key = (row["title"].lower(), str(row.get("target_period") or "").lower())
                if key in seen:
                    continue
                seen.add(key)
                out.append(row)
                if len(out) >= 8:
                    return out
        return out

    @staticmethod
    def _projection_row_from_item(item: Any, *, source: str) -> Optional[Dict[str, Any]]:
        if isinstance(item, dict):
            title = str(
                item.get("title")
                or item.get("milestone")
                or item.get("event")
                or item.get("label")
                or item.get("name")
                or item.get("catalyst")
                or ""
            ).strip()
            timing = str(
                item.get("target_period")
                or item.get("targetPeriod")
                or item.get("timing")
                or item.get("date")
                or item.get("period")
                or item.get("when")
                or ""
            ).strip()
            status = str(item.get("status") or item.get("current_status") or item.get("state") or "").strip()
            primary_risk = str(item.get("primary_risk") or item.get("risk") or "").strip()
        elif isinstance(item, str):
            title = item.strip()
            timing = ThesisComparator._period_from_text(title)
            status = ""
            primary_risk = ""
        else:
            return None

        if not title:
            return None
        timing = str(timing or "").strip()
        title = ThesisComparator._strip_leading_period(title, timing)
        row: Dict[str, Any] = {
            "title": title,
            "timing": timing,
            "target_period": timing,
            "status": status,
            "source": source,
        }
        if primary_risk:
            row["primary_risk"] = primary_risk
        return row

    @staticmethod
    def _period_from_text(text: str) -> str:
        match = re.search(
            r"\b(Q[1-4](?:\s*[-/]\s*Q[1-4])?\s*20\d{2}|(?:H[12]|[12]H)\s*20\d{2}|[A-Z][a-z]{2,8}\s+20\d{2}|20\d{2})\b",
            str(text or ""),
            flags=re.IGNORECASE,
        )
        return str(match.group(1) or "").strip() if match else ""

    @staticmethod
    def _strip_leading_period(text: str, period: str) -> str:
        if not period:
            return str(text or "").strip()
        cleaned = re.sub(
            rf"^\s*{re.escape(period)}\s*[:\-–—]\s*",
            "",
            str(text or "").strip(),
            flags=re.IGNORECASE,
        ).strip()
        return cleaned or str(text or "").strip()
