from __future__ import annotations

from typing import Any, Dict, Tuple

from .action_judge import ActionJudge
from .model_thesis_judge import ModelAnnouncementThesisJudge, normalize_model_thesis_payload
from .models import AnnouncementFacts, BaselineRunPacket, EvidenceRef
from .thesis_comparator import ThesisComparator
from .trajectory_scoring import baseline_path_score, position_label, score_band

LEGACY_UNVALIDATED_REASON = (
    "Legacy router artifact lacks a valid model thesis judgement; stored directional signal was suppressed."
)


def replay_comparison_from_artifact(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Recompute display decisions from stored source facts and baseline packets.

    Old event artifacts can contain stale comparator decisions. The source facts
    and baseline packet are enough to replay deterministic routing without
    fetching external data again, so monitor views stay aligned with current
    router logic while preserving the raw artifact on disk.
    """
    if not isinstance(payload, dict):
        return {}, {}

    original_report = payload.get("comparison_report") if isinstance(payload.get("comparison_report"), dict) else {}
    original_action = payload.get("action_decision") if isinstance(payload.get("action_decision"), dict) else {}
    facts_payload = payload.get("announcement_facts") if isinstance(payload.get("announcement_facts"), dict) else {}
    baseline_payload = payload.get("baseline_run") if isinstance(payload.get("baseline_run"), dict) else {}
    if not facts_payload or not baseline_payload:
        return original_report, original_action

    try:
        facts = _coerce_facts(facts_payload)
        baseline = _coerce_baseline(baseline_payload)
        has_model_judgement = (
            isinstance(facts.model_judgement, dict)
            and str(facts.model_judgement.get("status") or "").strip().lower() == "valid"
        )
        if not has_model_judgement:
            if _is_legacy_unvalidated_material(original_report, facts_payload):
                return _neutralized_legacy_material_report(
                    original_report,
                    original_action,
                    facts_payload=facts_payload,
                    baseline_payload=baseline_payload,
                )
            if str(original_report.get("relationship_kind") or "").strip().lower() != "material_unmapped":
                return original_report, original_action
            facts = ModelAnnouncementThesisJudge._abstain(facts, "legacy_artifact_without_model_judgement")
        report = ThesisComparator().compare(facts, baseline)
        action = ActionJudge().judge(report)
        return report.to_dict(), action.to_dict()
    except Exception:
        return original_report, original_action


def _is_legacy_unvalidated_material(report: Dict[str, Any], facts: Dict[str, Any]) -> bool:
    if not isinstance(report, dict):
        return False
    state = str(report.get("trajectory_state") or "").strip().lower()
    kind = str(report.get("relationship_kind") or "").strip().lower()
    score = report.get("trajectory_score") if isinstance(report.get("trajectory_score"), dict) else {}
    validation_type = str(score.get("validation_type") or "").strip().lower()
    judgement = facts.get("model_judgement") if isinstance(facts.get("model_judgement"), dict) else {}
    has_valid_model = str(judgement.get("status") or "").strip().lower() == "valid"
    if has_valid_model:
        return False
    return "material_unmapped" in {state, kind, validation_type}


def _neutralized_legacy_material_report(
    report: Dict[str, Any],
    action: Dict[str, Any],
    *,
    facts_payload: Dict[str, Any],
    baseline_payload: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    baseline_path = str(report.get("baseline_path") or "").strip().lower()
    if baseline_path not in {"bull", "base", "bear", "mixed", "unknown"}:
        structured = baseline_payload.get("lab_payload") if isinstance(baseline_payload.get("lab_payload"), dict) else {}
        structured = structured.get("structured_data") if isinstance(structured.get("structured_data"), dict) else {}
        extended = structured.get("extended_analysis") if isinstance(structured.get("extended_analysis"), dict) else {}
        current_state = extended.get("current_thesis_state") if isinstance(extended.get("current_thesis_state"), dict) else {}
        baseline_path = str(current_state.get("leaning") or report.get("current_path") or "unknown").strip().lower()
    if baseline_path not in {"bull", "base", "bear", "mixed", "unknown"}:
        baseline_path = "unknown"
    baseline_score = baseline_path_score(baseline_path)
    neutral_score = {
        "direction": "neutral",
        "intensity": str(report.get("impact_level") or facts_payload.get("materiality") or "medium").strip().lower() or "medium",
        "event_delta": 0.0,
        "unvalidated_event_delta": 0.0,
        "baseline_score": baseline_score,
        "score_after_event": baseline_score,
        "position_band": score_band(baseline_score),
        "position_label": position_label(baseline_path, baseline_score, validated_delta=0.0),
        "confidence": 0.0,
        "mapped_condition": False,
        "validation_type": "related_unmapped",
        "validation_weight": 0.0,
        "reason": LEGACY_UNVALIDATED_REASON,
    }
    neutral_report = {
        **report,
        "current_path": baseline_path,
        "path_transition": "",
        "path_confidence": 0.0,
        "thesis_effect": "no_change",
        "timeline_effect": "no_change",
        "capital_effect": "no_change",
        "relationship_priority": 3,
        "filing_type": "company_event",
        "evidence_scope": "unmapped_filing_evidence",
        "thesis_relationship": "related_unmapped",
        "impact_verdict": "uncertain",
        "impact_dimension": "general",
        "relationship_kind": "material_unmapped",
        "relationship_strength": "none",
        "relationship_direction": "neutral",
        "relationship_summary": LEGACY_UNVALIDATED_REASON,
        "trajectory_state": "needs_classification",
        "trajectory_effect": "material_update",
        "price_time_effect": "Stored legacy router output was not model-validated; no price/time movement is scored.",
        "semantic_summary": "This stored event predates model-led thesis judgement. Reprocess it before relying on any directional interpretation.",
        "thesis_match_confidence": 0.0,
        "classification_reason": LEGACY_UNVALIDATED_REASON,
        "condition_evaluations": [],
        "matched_condition_ids": [],
        "triggered_watchlist_ids": [],
        "triggered_verification_ids": [],
        "trajectory_score": neutral_score,
        "trajectory_projection": {},
        "key_findings": [],
        "conflicts_with_run": [],
        "notes": [LEGACY_UNVALIDATED_REASON],
    }
    neutral_action = {
        **(action if isinstance(action, dict) else {}),
        "action": str((action or {}).get("action") or "annotate_run").strip() or "annotate_run",
        "reason": LEGACY_UNVALIDATED_REASON,
        "should_trigger_workflow": False,
        "run_reuse_ok": True,
        "requires_human_ack": True,
        "invalidated_sections": [],
        "follow_up_steps": ["Reprocess this announcement with the model thesis judge before trusting direction."],
        "tags": ["legacy_artifact", "model_rejudge_required"],
    }
    return neutral_report, neutral_action


def _coerce_facts(payload: Dict[str, Any]) -> AnnouncementFacts:
    evidence = []
    for item in payload.get("evidence") or []:
        if isinstance(item, dict):
            evidence.append(
                EvidenceRef(
                    source_url=str(item.get("source_url") or ""),
                    quote_excerpt=str(item.get("quote_excerpt") or ""),
                    source_title=str(item.get("source_title") or ""),
                    source_date_utc=str(item.get("source_date_utc") or ""),
                )
            )
    model_judgement = payload.get("model_judgement") if isinstance(payload.get("model_judgement"), dict) else {}
    if str(model_judgement.get("status") or "").strip().lower() == "valid":
        normalized_judgement, _error = normalize_model_thesis_payload(model_judgement)
        if normalized_judgement:
            model_judgement = normalized_judgement
    facts = AnnouncementFacts(
        event_id=str(payload.get("event_id") or ""),
        ticker=str(payload.get("ticker") or ""),
        company_name=str(payload.get("company_name") or ""),
        title=str(payload.get("title") or ""),
        summary=str(payload.get("summary") or ""),
        extracted_facts=[str(item or "") for item in (payload.get("extracted_facts") or [])],
        material_topics=[str(item or "") for item in (payload.get("material_topics") or [])],
        market_facts=payload.get("market_facts") if isinstance(payload.get("market_facts"), dict) else {},
        evidence=evidence,
        raw_text_excerpt=str(payload.get("raw_text_excerpt") or ""),
        document_sections=payload.get("document_sections") if isinstance(payload.get("document_sections"), dict) else {},
        parse_quality=payload.get("parse_quality") if isinstance(payload.get("parse_quality"), dict) else {},
        announcement_class=str(payload.get("announcement_class") or ""),
        materiality=str(payload.get("materiality") or ""),
        affected_drivers=[str(item or "") for item in (payload.get("affected_drivers") or [])],
        trajectory_effect=str(payload.get("trajectory_effect") or ""),
        price_time_effect=str(payload.get("price_time_effect") or ""),
        filing_summary=str(payload.get("filing_summary") or ""),
        semantic_summary=str(payload.get("semantic_summary") or ""),
        semantic_confidence=float(payload.get("semantic_confidence") or 0.0),
        source_confidence=float(payload.get("source_confidence") or 0.0),
        extraction_confidence=float(payload.get("extraction_confidence") or 0.0),
        classification_confidence=float(payload.get("classification_confidence") or payload.get("semantic_confidence") or 0.0),
        thesis_match_confidence=float(payload.get("thesis_match_confidence") or 0.0),
        domain_profile=str(payload.get("domain_profile") or ""),
        classification_basis=[str(item or "") for item in (payload.get("classification_basis") or [])],
        parser_warnings=[str(item or "") for item in (payload.get("parser_warnings") or [])],
        classification_reason=str(payload.get("classification_reason") or ""),
        confidence_breakdown=payload.get("confidence_breakdown") if isinstance(payload.get("confidence_breakdown"), dict) else {},
        model_judgement=model_judgement,
    )
    if str(model_judgement.get("status") or "").strip().lower() == "valid":
        facts = ModelAnnouncementThesisJudge._apply_payload(facts, model_judgement)
    return facts


def _coerce_baseline(payload: Dict[str, Any]) -> BaselineRunPacket:
    return BaselineRunPacket(
        run_id=str(payload.get("run_id") or ""),
        ticker=str(payload.get("ticker") or ""),
        exchange=str(payload.get("exchange") or ""),
        company_name=str(payload.get("company_name") or ""),
        template_id=str(payload.get("template_id") or ""),
        freshness_status=str(payload.get("freshness_status") or ""),
        freshness_age_days=payload.get("freshness_age_days"),
        summary_fields=payload.get("summary_fields") if isinstance(payload.get("summary_fields"), dict) else {},
        lab_payload=payload.get("lab_payload") if isinstance(payload.get("lab_payload"), dict) else {},
        timeline_rows=payload.get("timeline_rows") if isinstance(payload.get("timeline_rows"), list) else [],
        catalyst_rows=payload.get("catalyst_rows") if isinstance(payload.get("catalyst_rows"), list) else [],
        memos=payload.get("memos") if isinstance(payload.get("memos"), dict) else {},
    )
