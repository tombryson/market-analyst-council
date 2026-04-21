from __future__ import annotations

from typing import Any, Dict, Tuple

from .action_judge import ActionJudge
from .models import AnnouncementFacts, BaselineRunPacket, EvidenceRef
from .thesis_comparator import ThesisComparator


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
        report = ThesisComparator().compare(facts, baseline)
        action = ActionJudge().judge(report)
        return report.to_dict(), action.to_dict()
    except Exception:
        return original_report, original_action


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
    return AnnouncementFacts(
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
    )


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
        memos=payload.get("memos") if isinstance(payload.get("memos"), dict) else {},
    )
