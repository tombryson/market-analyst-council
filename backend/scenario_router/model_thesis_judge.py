from __future__ import annotations

import inspect
import json
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional

from ..config import (
    SCENARIO_ROUTER_THESIS_JUDGE_MAX_OUTPUT_TOKENS,
    SCENARIO_ROUTER_THESIS_JUDGE_MODEL,
    SCENARIO_ROUTER_THESIS_JUDGE_REASONING_EFFORT,
    SCENARIO_ROUTER_THESIS_JUDGE_TIMEOUT_SECONDS,
)
from ..openrouter import query_model
from .models import AnnouncementFacts, BaselineRunPacket

QueryFn = Callable[[str, List[Dict[str, str]], float, int, str], Awaitable[Dict[str, Any]]]

MATERIALITY_VALUES = {"none", "low", "medium", "high", "critical"}
DOCUMENT_TYPES = {
    "administrative",
    "agm_presentation",
    "annual_report",
    "capital_management",
    "earnings_update",
    "financing_update",
    "incident_report",
    "investor_presentation",
    "operational_update",
    "permitting_regulatory",
    "production_update",
    "project_development",
    "resource_update",
    "trading_update",
    "unknown",
}
TRAJECTORY_STATES = {
    "thesis_strengthened",
    "thesis_weakened",
    "timeline_accelerated",
    "timeline_delayed",
    "risk_reduced",
    "risk_increased",
    "material_unmapped",
    "market_backdrop_only",
    "administrative_filing",
    "no_thesis_change",
    "needs_classification",
}
TRAJECTORY_DIRECTIONS = {"positive", "negative", "neutral", "mixed", "unclear"}
REFERENCE_TYPES = {"thesis_map", "watchlist", "verification", "timeline", "none"}
RELATIONSHIPS = {
    "confirms",
    "partially_confirms",
    "checked_not_triggered",
    "contradicts",
    "updates",
    "unmapped",
    "none",
}
SERIOUS_INCIDENT_TERMS = {
    "fatality",
    "fatal",
    "death",
    "died",
    "killed",
    "serious injury",
    "formally notified",
    "formal investigation",
    "regulator",
    "department",
    "suspension",
    "shutdown",
}


@dataclass
class ModelAnnouncementThesisJudge:
    """Model-led announcement interpreter for thesis trajectory routing.

    Deterministic code gathers source text and saved council context. This
    component asks a model for the semantic thesis judgement, then validates
    that the answer is JSON, evidence-bound, and safe to pass downstream.
    """

    model: str = SCENARIO_ROUTER_THESIS_JUDGE_MODEL
    timeout_seconds: float = SCENARIO_ROUTER_THESIS_JUDGE_TIMEOUT_SECONDS
    max_output_tokens: int = SCENARIO_ROUTER_THESIS_JUDGE_MAX_OUTPUT_TOKENS
    reasoning_effort: str = SCENARIO_ROUTER_THESIS_JUDGE_REASONING_EFFORT
    query_fn: Optional[QueryFn] = None

    async def interpret(self, facts: AnnouncementFacts, baseline_run: BaselineRunPacket) -> AnnouncementFacts:
        if not str(self.model or "").strip():
            return self._abstain(facts, "model_unavailable")
        request = {
            "filing": self._filing_packet(facts),
            "saved_council_thesis": self._thesis_packet(baseline_run),
            "rubric": self._rubric(),
        }
        messages = [
            {
                "role": "system",
                "content": (
                    "You are an evidence-bound investment thesis router. Read the new filing against the saved "
                    "LLM council thesis packet. Return JSON only. Do not classify from keyword counts. Do not let "
                    "forward-looking disclaimers, risk boilerplate, exchange release footers, or authorisation text "
                    "drive thesis direction. A directional verdict requires a core filing claim with a short evidence "
                    "quote. Treat thesis-map coverage separately from thesis impact: if the filing is material but no "
                    "saved reference covers it, mark the relationship as unmapped, then still judge whether the filing "
                    "is positive, negative, neutral, mixed, or unclear. For thesis_map and timeline references, "
                    "relationship=confirms means the filing directly announces that the saved condition or milestone "
                    "itself has occurred or has been near-explicitly satisfied. Targeted pathways, schedule support, "
                    "enabling, de-risking, prerequisite, critical-path, progress-toward, or related support are "
                    "partially_confirms or updates, not confirms."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Judge this announcement against the saved council thesis.\n\n"
                    "Return exactly this JSON shape:\n"
                    "{\n"
                    '  "one_sentence_summary": "plain-English summary of what happened",\n'
                    '  "document_type": "administrative|agm_presentation|annual_report|capital_management|earnings_update|financing_update|incident_report|investor_presentation|operational_update|permitting_regulatory|production_update|project_development|resource_update|trading_update|unknown",\n'
                    '  "core_claims": [{"claim": "", "evidence_quote": "", "claim_type": "", "is_new_information": true}],\n'
                    '  "ignored_text": [{"type": "boilerplate|footer|disclaimer", "reason": ""}],\n'
                    '  "thesis_relationships": [{"reference_type": "thesis_map|watchlist|verification|timeline|none", "reference_id": "", "reference_label": "", "scenario": "bull|base|bear|", "relationship": "confirms|partially_confirms|checked_not_triggered|contradicts|updates|unmapped|none", "direction": "positive|negative|neutral|mixed|unclear", "evidence_quote": "", "reason": "", "missing_for_full_match": [], "confidence": 0.0}],\n'
                    '  "trajectory_verdict": {"state": "thesis_strengthened|thesis_weakened|timeline_accelerated|timeline_delayed|risk_reduced|risk_increased|administrative_filing|no_thesis_change|needs_classification", "direction": "positive|negative|neutral|mixed|unclear", "materiality": "none|low|medium|high|critical", "intensity": "none|low|medium|high|critical", "recommended_case": "bull|base|bear|unchanged|unclear", "confidence": 0.0, "reason": ""},\n'
                    '  "maintenance_action": {"action": "none|add_thesis_condition|refresh_evidence|rerun_council|human_review", "reason": ""}\n'
                    "}\n\n"
                    f"INPUT_JSON:\n{json.dumps(request, ensure_ascii=True)}"
                ),
            },
        ]
        payload, error = await self._call_model(messages)
        if error:
            return self._abstain(facts, error)
        normalized, validation_error = self._validate_payload(payload)
        if validation_error:
            return self._abstain(facts, validation_error, raw_payload=payload)
        return self._apply_payload(facts, normalized)

    async def _call_model(self, messages: List[Dict[str, str]]) -> tuple[Dict[str, Any], str]:
        last_error = "invalid_or_empty_json"
        attempt_messages = list(messages)
        for attempt in range(2):
            try:
                response = await self._query(attempt_messages)
            except Exception as exc:
                return {}, f"model_error:{type(exc).__name__}"
            content = str((response or {}).get("content") or "").strip()
            payload = parse_thesis_judge_json(content)
            if payload:
                return payload, ""
            last_error = "invalid_or_empty_json"
            if attempt == 0:
                attempt_messages = [
                    *messages,
                    {
                        "role": "assistant",
                        "content": content[:1200] if content else "[empty response]",
                    },
                    {
                        "role": "user",
                        "content": (
                            "Your previous response was not valid JSON matching the requested schema. "
                            "Retry once. Return JSON only, with no markdown fences or explanatory prose."
                        ),
                    },
                ]
        return {}, last_error

    async def _query(self, messages: List[Dict[str, str]]) -> Dict[str, Any]:
        if self.query_fn is not None:
            response = self.query_fn(
                self.model,
                messages,
                float(self.timeout_seconds or 45.0),
                int(self.max_output_tokens or 1500),
                self.reasoning_effort,
            )
            if inspect.isawaitable(response):
                response = await response
            return response or {}
        return await query_model(
            self.model,
            messages,
            timeout=float(self.timeout_seconds or 45.0),
            max_tokens=int(self.max_output_tokens or 1500),
            reasoning_effort=self.reasoning_effort,
        )

    @staticmethod
    def _filing_packet(facts: AnnouncementFacts) -> Dict[str, Any]:
        evidence_quotes = [
            str(item.quote_excerpt or "").strip()
            for item in (facts.evidence or [])
            if str(item.quote_excerpt or "").strip()
        ][:6]
        sections = facts.document_sections if isinstance(facts.document_sections, dict) else {}
        body = str(sections.get("body") or facts.raw_text_excerpt or "").strip()
        text = _compact_text(
            "\n\n".join(
                part
                for part in [
                    str(facts.title or "").strip(),
                    str(facts.summary or "").strip(),
                    body,
                    "\n".join(str(item or "").strip() for item in (facts.extracted_facts or []) if str(item or "").strip()),
                    "\n".join(evidence_quotes),
                ]
                if part
            ),
            limit=18000,
        )
        return {
            "event_id": facts.event_id,
            "ticker": facts.ticker,
            "company_name": facts.company_name,
            "title": facts.title,
            "source_confidence": facts.source_confidence,
            "extraction_confidence": facts.extraction_confidence,
            "parse_quality": facts.parse_quality,
            "evidence_quotes": evidence_quotes,
            "document_sections": _compact_value(sections),
            "filing_text": text,
        }

    @staticmethod
    def _thesis_packet(baseline_run: BaselineRunPacket) -> Dict[str, Any]:
        structured = {}
        if isinstance(baseline_run.lab_payload, dict):
            structured = baseline_run.lab_payload.get("structured_data") if isinstance(baseline_run.lab_payload.get("structured_data"), dict) else {}
        extended = structured.get("extended_analysis") if isinstance(structured.get("extended_analysis"), dict) else {}
        return _compact_payload(
            {
                "run_id": baseline_run.run_id,
                "ticker": baseline_run.ticker,
                "company_name": baseline_run.company_name,
                "template_id": baseline_run.template_id,
                "summary_fields": baseline_run.summary_fields,
                "current_thesis_state": extended.get("current_thesis_state") if isinstance(extended, dict) else {},
                "investment_recommendation": structured.get("investment_recommendation") or structured.get("investment_verdict"),
                "memo_conclusion": _pick_memo_conclusion(baseline_run),
                "thesis_map": structured.get("thesis_map"),
                "monitoring_watchlist": structured.get("monitoring_watchlist"),
                "verification_queue": structured.get("verification_queue"),
                "development_timeline": structured.get("development_timeline") or baseline_run.timeline_rows,
                "catalysts": baseline_run.catalyst_rows,
                "scenario_targets": structured.get("scenario_targets"),
                "price_targets": structured.get("price_targets"),
            },
            limit=26000,
        )

    @staticmethod
    def _rubric() -> Dict[str, Any]:
        return {
            "decision_rules": [
                "Use core announcement claims, not disclaimers or release footers.",
                "A thesis movement requires a core filing claim and an evidence quote.",
                "Do not use thesis-map coverage as the verdict. If material but not covered by the saved thesis packet, mark the relationship as unmapped and judge impact direction separately.",
                "Use partial relationships for precursors such as LoI/MoU where the saved condition requires binding terms.",
                "Return needs_classification if the filing text is too thin or the thesis impact cannot be safely judged.",
                "For watchlist red flags, if the filing discusses the risk area but says the risk did not occur or no material impact is expected, return relationship=checked_not_triggered and direction=neutral. Do not convert a non-triggered red flag into a confirmatory signal.",
                "Fatalities, serious safety incidents, formal regulatory investigations, or safety-related operational shutdowns are risk events. If the company says no immediate production impact is expected, that may avoid a production-delay watchlist trigger, but it does not make the filing no_thesis_change. Classify the trajectory as risk_increased unless the source clearly shows the incident is immaterial and non-operational.",
            ]
        }

    @staticmethod
    def _validate_payload(payload: Any) -> tuple[Dict[str, Any], str]:
        return normalize_model_thesis_payload(payload)

    @staticmethod
    def _apply_payload(facts: AnnouncementFacts, payload: Dict[str, Any]) -> AnnouncementFacts:
        verdict = payload.get("trajectory_verdict") if isinstance(payload.get("trajectory_verdict"), dict) else {}
        relationships = payload.get("thesis_relationships") if isinstance(payload.get("thesis_relationships"), list) else []
        has_valid_relationship = any(_is_valid_directional_relationship(item) for item in relationships)
        state = str(verdict.get("state") or "").strip().lower()
        direction = str(verdict.get("direction") or "").strip().lower()
        materiality = str(verdict.get("materiality") or "low").strip().lower()
        facts.announcement_class = str(payload.get("document_type") or "unknown").strip().lower()
        facts.materiality = materiality
        facts.affected_drivers = _affected_drivers(payload)
        facts.material_topics = list(facts.affected_drivers or [])[:8]
        facts.trajectory_effect = _trajectory_effect_from_model_state(state, direction, has_valid_relationship)
        facts.price_time_effect = _price_time_effect_from_model(payload, has_valid_relationship)
        summary = str(payload.get("one_sentence_summary") or "").strip()
        facts.filing_summary = summary
        facts.semantic_summary = summary or str(verdict.get("reason") or "").strip()
        confidence = max(0.0, min(1.0, _safe_float(verdict.get("confidence"), default=0.0)))
        facts.semantic_confidence = confidence
        facts.classification_confidence = confidence
        facts.domain_profile = str(facts.domain_profile or "")
        facts.classification_basis = ["model_thesis_judge:valid"]
        facts.parser_warnings = []
        if not has_valid_relationship and state == "material_unmapped":
            facts.parser_warnings.append("material_unmapped_no_validated_relationship")
        facts.classification_reason = str(verdict.get("reason") or "Model returned an evidence-bound thesis verdict.").strip()
        facts.model_judgement = payload
        facts.confidence_breakdown = {
            **(facts.confidence_breakdown if isinstance(facts.confidence_breakdown, dict) else {}),
            "classification_reason": facts.classification_reason,
            "classification_confidence": confidence,
            "model_thesis_judge": {
                "status": "valid",
                "model": SCENARIO_ROUTER_THESIS_JUDGE_MODEL,
                "has_valid_relationship": has_valid_relationship,
                "relationship_count": len(relationships),
            },
        }
        return facts

    @staticmethod
    def _abstain(facts: AnnouncementFacts, reason: str, *, raw_payload: Any = None) -> AnnouncementFacts:
        facts.announcement_class = "needs_classification"
        facts.materiality = "low"
        facts.affected_drivers = []
        facts.material_topics = []
        facts.trajectory_effect = "no_clear_change"
        facts.price_time_effect = "No model-validated thesis trajectory change identified."
        facts.filing_summary = "The filing was captured, but the model thesis judge did not return a valid verdict."
        facts.semantic_summary = facts.filing_summary
        facts.semantic_confidence = 0.0
        facts.classification_confidence = 0.0
        facts.classification_basis = ["model_thesis_judge:abstained"]
        facts.parser_warnings = [str(reason or "model_abstained")]
        facts.classification_reason = "Model thesis judge abstained; deterministic keyword routing was not used for direction."
        facts.model_judgement = {
            "status": "invalid",
            "reason": reason,
            "raw_payload": raw_payload if isinstance(raw_payload, dict) else {},
        }
        facts.confidence_breakdown = {
            **(facts.confidence_breakdown if isinstance(facts.confidence_breakdown, dict) else {}),
            "classification_confidence": 0.0,
            "classification_reason": facts.classification_reason,
            "model_thesis_judge": {"status": "abstained", "reason": reason},
        }
        return facts


def parse_thesis_judge_json(content: str) -> Dict[str, Any]:
    text = str(content or "").strip()
    if not text:
        return {}
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {}
        try:
            payload = json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}
    return payload if isinstance(payload, dict) else {}


def normalize_model_thesis_payload(payload: Any) -> tuple[Dict[str, Any], str]:
    """Validate and normalize the model thesis judge contract.

    This is intentionally shared by live model calls and stored-artifact replay.
    A replayed artifact should not be able to bypass invariants that protect the
    router from stale or unsafe model judgements.
    """
    if not isinstance(payload, dict):
        return {}, "payload_not_object"
    summary = _clean_sentence(payload.get("one_sentence_summary"))
    verdict = payload.get("trajectory_verdict") if isinstance(payload.get("trajectory_verdict"), dict) else {}
    document_type = _norm_enum(payload.get("document_type"), DOCUMENT_TYPES, "unknown")
    state = _norm_enum(verdict.get("state"), TRAJECTORY_STATES, "needs_classification")
    direction = _norm_enum(verdict.get("direction"), TRAJECTORY_DIRECTIONS, "unclear")
    materiality = _norm_enum(verdict.get("materiality"), MATERIALITY_VALUES, "low")
    intensity = _norm_enum(verdict.get("intensity"), MATERIALITY_VALUES, materiality)
    confidence = _safe_float(verdict.get("confidence"), default=0.0)
    relationships = _normalize_relationships(payload.get("thesis_relationships") or [])
    core_claims = _normalize_claims(payload.get("core_claims") or [])
    ignored_text = _normalize_ignored_text(payload.get("ignored_text") or [])
    action = payload.get("maintenance_action") if isinstance(payload.get("maintenance_action"), dict) else {}
    if _is_serious_incident(document_type, core_claims) and state in {"no_thesis_change", "material_unmapped"}:
        state = "risk_increased"
        direction = "negative"
        materiality = _max_materiality(materiality, "medium")
        intensity = _max_materiality(intensity, "medium")
        verdict = {
            **verdict,
            "state": state,
            "direction": direction,
            "materiality": materiality,
            "intensity": intensity,
            "recommended_case": "bear",
            "reason": (
                "Fatal or serious safety incident increases regulatory, governance, and operational-risk "
                "uncertainty even if no immediate production impact is expected."
            ),
        }
        if _norm_action(action.get("action")) == "none":
            action = {
                **action,
                "action": "human_review",
                "reason": "Review safety/regulatory risk and whether the saved thesis map needs coverage.",
            }
    normalized = {
        "status": "valid",
        "one_sentence_summary": summary,
        "document_type": document_type,
        "core_claims": core_claims,
        "ignored_text": ignored_text,
        "thesis_relationships": relationships,
        "trajectory_verdict": {
            "state": state,
            "direction": direction,
            "materiality": materiality,
            "intensity": intensity,
            "recommended_case": _norm_recommended_case(verdict.get("recommended_case")),
            "confidence": max(0.0, min(1.0, confidence)),
            "reason": _clean_sentence(verdict.get("reason")),
        },
        "maintenance_action": {
            "action": _norm_action(action.get("action")),
            "reason": _clean_sentence(action.get("reason")),
        },
    }
    if not summary and not core_claims:
        return {}, "missing_summary_and_claims"
    return normalized, ""


def _normalize_relationships(items: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, item in enumerate(items if isinstance(items, list) else []):
        if not isinstance(item, dict):
            continue
        reference_type = _norm_enum(item.get("reference_type"), REFERENCE_TYPES, "none")
        relationship = _norm_enum(item.get("relationship"), RELATIONSHIPS, "none")
        direction = _norm_enum(item.get("direction"), TRAJECTORY_DIRECTIONS, "unclear")
        evidence_quote = _clean_sentence(item.get("evidence_quote"))
        out.append(
            {
                "reference_type": reference_type,
                "reference_id": str(item.get("reference_id") or f"model_reference_{idx}").strip(),
                "reference_label": _clean_sentence(item.get("reference_label")),
                "scenario": _norm_recommended_case(item.get("scenario"), allow_empty=True),
                "relationship": relationship,
                "direction": direction,
                "evidence_quote": evidence_quote,
                "reason": _clean_sentence(item.get("reason")),
                "missing_for_full_match": [
                    _clean_sentence(value)
                    for value in (item.get("missing_for_full_match") or [])
                    if _clean_sentence(value)
                ][:5],
                "confidence": max(0.0, min(1.0, _safe_float(item.get("confidence"), default=0.0))),
            }
        )
    return out[:12]


def _normalize_claims(items: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        claim = _clean_sentence(item.get("claim"))
        quote = _clean_sentence(item.get("evidence_quote"))
        if not claim and not quote:
            continue
        out.append(
            {
                "claim": claim,
                "evidence_quote": quote,
                "claim_type": _clean_key(item.get("claim_type")),
                "is_new_information": bool(item.get("is_new_information")),
            }
        )
    return out[:12]


def _normalize_ignored_text(items: Any) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        text_type = str(item.get("type") or "").strip().lower()
        if text_type not in {"boilerplate", "footer", "disclaimer"}:
            text_type = "boilerplate"
        reason = _clean_sentence(item.get("reason"))
        if reason:
            out.append({"type": text_type, "reason": reason})
    return out[:8]


def _is_serious_incident(document_type: str, core_claims: List[Dict[str, Any]]) -> bool:
    document_key = str(document_type or "").strip().lower()
    claim_text = " ".join(
        " ".join(
            str(claim.get(key) or "").strip().lower()
            for key in ("claim", "evidence_quote", "claim_type")
        )
        for claim in core_claims
        if isinstance(claim, dict)
    )
    if not claim_text:
        return False
    context_terms = {
        "safety",
        "incident",
        "accident",
        "fatality",
        "fatal",
        "death",
        "injury",
        "regulator",
        "regulatory",
        "investigation",
        "shutdown",
        "suspension",
        "operation",
        "mine",
        "plant",
        "workplace",
    }
    incident_document_types = {
        "incident_report",
        "operational_update",
        "production_update",
        "permitting_regulatory",
        "project_development",
        "unknown",
    }
    if document_key not in incident_document_types and not any(term in claim_text for term in context_terms):
        return False
    return any(term in claim_text for term in SERIOUS_INCIDENT_TERMS)


def _max_materiality(left: str, right: str) -> str:
    order = {"none": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
    left_key = str(left or "").strip().lower()
    right_key = str(right or "").strip().lower()
    return left_key if order.get(left_key, 0) >= order.get(right_key, 0) else right_key


def _is_valid_directional_relationship(item: Dict[str, Any]) -> bool:
    if not isinstance(item, dict):
        return False
    if str(item.get("reference_type") or "").strip().lower() == "none":
        return False
    if str(item.get("relationship") or "").strip().lower() in {"", "none", "unmapped"}:
        return False
    if str(item.get("direction") or "").strip().lower() not in {"positive", "negative", "mixed", "neutral"}:
        return False
    return bool(str(item.get("evidence_quote") or "").strip())


def _trajectory_effect_from_model_state(state: str, direction: str, has_valid_relationship: bool) -> str:
    if state == "administrative_filing":
        return "administrative"
    if state == "no_thesis_change":
        return "no_clear_change"
    if state == "needs_classification":
        return "no_clear_change"
    if state == "market_backdrop_only":
        return "no_clear_change"
    if not has_valid_relationship:
        return "material_update" if state == "material_unmapped" else "no_clear_change"
    if state == "timeline_delayed":
        return "delays"
    if state == "timeline_accelerated":
        return "accelerates"
    if state in {"thesis_weakened", "risk_increased"} or direction == "negative":
        return "weakens"
    if state in {"thesis_strengthened", "risk_reduced"} or direction == "positive":
        return "risk_reduced" if state == "risk_reduced" else "strengthens"
    return "material_update"


def _price_time_effect_from_model(payload: Dict[str, Any], has_valid_relationship: bool) -> str:
    verdict = payload.get("trajectory_verdict") if isinstance(payload.get("trajectory_verdict"), dict) else {}
    reason = _clean_sentence(verdict.get("reason"))
    if not has_valid_relationship and str(verdict.get("state") or "").strip().lower() == "material_unmapped":
        return "Material filing outside the saved thesis map; no validated bull/base/bear movement was scored."
    if reason:
        return reason
    return "Model-validated announcement relationship recorded."


def _affected_drivers(payload: Dict[str, Any]) -> List[str]:
    drivers: List[str] = []
    for claim in payload.get("core_claims") or []:
        if isinstance(claim, dict):
            drivers.append(_clean_key(claim.get("claim_type")))
    for relationship in payload.get("thesis_relationships") or []:
        if isinstance(relationship, dict):
            drivers.append(_clean_key(relationship.get("reference_type")))
    return [item for item in _dedupe(drivers) if item and item != "none"][:8]


def _compact_payload(payload: Dict[str, Any], *, limit: int) -> Dict[str, Any]:
    encoded = json.dumps(payload, ensure_ascii=True, default=str)
    if len(encoded) <= limit:
        return payload
    compact = {}
    for key, value in payload.items():
        compact[key] = _compact_value(value)
    encoded = json.dumps(compact, ensure_ascii=True, default=str)
    if len(encoded) <= limit:
        return compact
    return {"compact_thesis_packet": encoded[:limit]}


def _compact_value(value: Any) -> Any:
    if isinstance(value, str):
        return _compact_text(value, limit=1500)
    if isinstance(value, list):
        return [_compact_value(item) for item in value[:12]]
    if isinstance(value, dict):
        return {str(key): _compact_value(item) for key, item in list(value.items())[:40]}
    return value


def _compact_text(text: str, *, limit: int) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    head = int(limit * 0.75)
    tail = max(0, limit - head)
    return value[:head].rstrip() + "\n\n[... truncated ...]\n\n" + value[-tail:].lstrip()


def _pick_memo_conclusion(baseline_run: BaselineRunPacket) -> str:
    memos = baseline_run.memos if isinstance(baseline_run.memos, dict) else {}
    for key in ("memo", "final_memo", "chairman_memo", "conclusion"):
        value = memos.get(key)
        if isinstance(value, dict):
            for nested_key in ("conclusion", "summary", "investment_thesis"):
                nested = str(value.get(nested_key) or "").strip()
                if nested:
                    return _compact_text(nested, limit=1600)
        text = str(value or "").strip()
        if text:
            return _compact_text(text, limit=1600)
    return ""


def _norm_enum(value: Any, allowed: set[str], default: str) -> str:
    text = _clean_key(value)
    return text if text in allowed else default


def _norm_recommended_case(value: Any, *, allow_empty: bool = False) -> str:
    text = _clean_key(value)
    if allow_empty and not text:
        return ""
    if text in {"bull", "base", "bear", "unchanged", "unclear"}:
        return text
    return "" if allow_empty else "unclear"


def _norm_action(value: Any) -> str:
    text = _clean_key(value)
    return text if text in {"none", "add_thesis_condition", "refresh_evidence", "rerun_council", "human_review"} else "none"


def _clean_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _clean_sentence(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _safe_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _dedupe(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out
