"""Multi-wave Stage-1 retrieval orchestration.

Implements the research planner that evaluates section coverage after each
Perplexity wave, builds targeted gap queries, and merges wave results.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from ..config import (
    MAX_SOURCES,
    STAGE1_CASHFLOW_DETECTION_MAX_SOURCES,
)
from .source_analysis import (
    _is_low_signal_legal_boilerplate,
    _is_low_signal_notice_source_item,
    _source_authority_rank,
)

logger = logging.getLogger(__name__)

def _normalize_terms_list(raw_terms: Any) -> List[str]:
    out: List[str] = []
    for item in (raw_terms or []):
        value = str(item or "").strip().lower()
        if value and value not in out:
            out.append(value)
    return out


def _markers_for_field_name(field_name: str) -> List[str]:
    key = str(field_name or "").strip().lower()
    if not key:
        return []
    variants = [
        key,
        key.replace("_", " "),
        key.replace("_", "-"),
    ]
    if key.endswith("_pct"):
        base = key[: -len("_pct")]
        variants.extend(
            [
                base,
                f"{base} %",
                f"{base.replace('_', ' ')} %",
            ]
        )
    if key.endswith("_score"):
        base = key[: -len("_score")]
        variants.extend(
            [
                f"{base} score",
                f"{base.replace('_', ' ')} score",
            ]
        )
    deduped: List[str] = []
    for item in variants:
        cleaned = re.sub(r"\s+", " ", item).strip()
        if cleaned and cleaned not in deduped:
            deduped.append(cleaned)
    return deduped


def _default_stage1_verification_profile() -> Dict[str, Any]:
    return {
        "template_id": "",
        "fact_digest_keywords": copy.deepcopy(_FACT_DIGEST_V2_KEYWORDS),
        "fact_digest_narrative_order": list(_FACT_DIGEST_V2_NARRATIVE_ORDER),
        "timeline_terms": list(_STAGE1_DEFAULT_TIMELINE_TERMS),
        "timeline_focus_terms": list(_STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS),
        "timeline_conflict_field": "timeline_window",
        "timeline_conflict_resolution_rule": "prefer newest dated primary-source timeline evidence",
        "timeline_conflict_max_shift_quarters": 3,
        "compliance_section_markers": list(_STAGE1_RUBRIC_SECTION_MARKERS),
        "compliance_critical_sections": set(_STAGE1_RUBRIC_CRITICAL_SECTIONS),
        "cashflow_schema_mode": "auto",
        "cashflow_schema_min_reporting_periods": 3,
        "cashflow_schema_require_operating_cashflow": True,
        "cashflow_schema_detection_max_sources": int(STAGE1_CASHFLOW_DETECTION_MAX_SOURCES),
    }


def _build_stage1_verification_profile(template_id: Optional[str]) -> Dict[str, Any]:
    profile = _default_stage1_verification_profile()
    if not template_id:
        return profile

    from .template_loader import get_template_loader

    loader = get_template_loader()
    template_data = loader.get_template(template_id) or {}
    verification = loader.get_verification_schema(template_id)
    profile["template_id"] = str(template_id)
    template_behavior = loader.get_template_behavior(template_id) or {}

    fact_digest_cfg = verification.get("fact_digest", {}) if isinstance(verification, dict) else {}
    sections_cfg = fact_digest_cfg.get("sections", {})
    normalized_sections: Dict[str, List[str]] = {}
    if isinstance(sections_cfg, dict):
        for section_name, section_payload in sections_cfg.items():
            sid = str(section_name or "").strip().lower()
            if not sid:
                continue
            keywords: List[str] = []
            if isinstance(section_payload, dict):
                keywords = _normalize_terms_list(section_payload.get("keywords", []))
            elif isinstance(section_payload, list):
                keywords = _normalize_terms_list(section_payload)
            if keywords:
                normalized_sections[sid] = keywords
    if normalized_sections:
        profile["fact_digest_keywords"] = normalized_sections

    narrative_order = [
        str(item or "").strip().lower()
        for item in (fact_digest_cfg.get("narrative_order", []) or [])
        if str(item or "").strip()
    ]
    if narrative_order:
        profile["fact_digest_narrative_order"] = narrative_order

    timeline_terms = _normalize_terms_list(fact_digest_cfg.get("timeline_terms", []))
    if timeline_terms:
        profile["timeline_terms"] = timeline_terms
    timeline_focus_terms = _normalize_terms_list(fact_digest_cfg.get("timeline_focus_terms", []))
    if timeline_focus_terms:
        profile["timeline_focus_terms"] = timeline_focus_terms

    conflict_cfg = fact_digest_cfg.get("conflict", {})
    if isinstance(conflict_cfg, dict):
        field_name = str(conflict_cfg.get("field", "")).strip()
        if field_name:
            profile["timeline_conflict_field"] = field_name
        max_shift = conflict_cfg.get("max_shift_quarters")
        if isinstance(max_shift, (int, float)):
            profile["timeline_conflict_max_shift_quarters"] = max(1, int(max_shift))
        resolution = str(conflict_cfg.get("resolution_rule", "")).strip()
        if resolution:
            profile["timeline_conflict_resolution_rule"] = resolution
        conflict_terms = _normalize_terms_list(conflict_cfg.get("terms", []))
        if conflict_terms:
            profile["timeline_focus_terms"] = conflict_terms

    compliance_cfg = verification.get("compliance", {}) if isinstance(verification, dict) else {}
    markers_cfg = compliance_cfg.get("section_markers", {})
    normalized_markers: List[Tuple[str, List[str]]] = []
    normalized_critical: set[str] = set()

    if isinstance(markers_cfg, dict):
        for section_id, marker_payload in markers_cfg.items():
            sid = str(section_id or "").strip().lower()
            if not sid:
                continue
            markers: List[str] = []
            critical = False
            if isinstance(marker_payload, dict):
                markers = _normalize_terms_list(marker_payload.get("markers", []))
                critical = bool(marker_payload.get("critical", False))
            elif isinstance(marker_payload, list):
                markers = _normalize_terms_list(marker_payload)
            if not markers:
                markers = _markers_for_field_name(sid)
            if not markers:
                continue
            normalized_markers.append((sid, markers))
            if critical:
                normalized_critical.add(sid)

    required_sections = _normalize_terms_list(compliance_cfg.get("required_sections", []))
    normalized_critical.update(required_sections)
    normalized_critical.update(
        _normalize_terms_list(compliance_cfg.get("critical_sections", []))
    )

    if not normalized_markers:
        required_fields = (
            ((template_data.get("output_schema") or {}).get("required_fields") or [])
            if isinstance(template_data, dict)
            else []
        )
        for field in required_fields:
            sid = str(field or "").strip().lower()
            if not sid:
                continue
            markers = _markers_for_field_name(sid)
            if not markers:
                continue
            normalized_markers.append((sid, markers))
        # Keep the scoring/timeline-related fields as critical by default.
        for sid in ("quality_score", "value_score", "price_targets", "development_timeline"):
            if any(item[0] == sid for item in normalized_markers):
                normalized_critical.add(sid)

    if normalized_markers:
        profile["compliance_section_markers"] = normalized_markers
    if normalized_critical:
        profile["compliance_critical_sections"] = normalized_critical

    cashflow_cfg = (
        template_behavior.get("cashflow_schema", {})
        if isinstance(template_behavior, dict)
        else {}
    )
    if isinstance(cashflow_cfg, dict):
        mode = str(cashflow_cfg.get("mode", "")).strip().lower()
        if mode in {"disabled", "auto", "required"}:
            profile["cashflow_schema_mode"] = mode
        min_periods = cashflow_cfg.get("min_reporting_periods")
        if isinstance(min_periods, (int, float)):
            profile["cashflow_schema_min_reporting_periods"] = max(1, int(min_periods))
        require_ocf = cashflow_cfg.get("require_operating_cashflow")
        if isinstance(require_ocf, bool):
            profile["cashflow_schema_require_operating_cashflow"] = bool(require_ocf)
        detection_max_sources = cashflow_cfg.get("detection_max_sources")
        if isinstance(detection_max_sources, (int, float)):
            profile["cashflow_schema_detection_max_sources"] = max(
                6,
                int(detection_max_sources),
            )

    return profile


def _keywords_for_gap_section(section_id: str, verification_profile: Dict[str, Any]) -> List[str]:
    """Resolve keyword hints for a compliance section id."""
    sid = str(section_id or "").strip().lower()
    digest_keywords = verification_profile.get("fact_digest_keywords", {}) or {}
    if isinstance(digest_keywords, dict):
        direct = _normalize_terms_list(digest_keywords.get(sid, []))
        if direct:
            return direct

    fallback: Dict[str, List[str]] = {
        "quality_score": ["quality score", "jurisdiction", "management", "funding", "esg"],
        "value_score": ["value score", "npv", "market cap", "ev/resource", "aisc"],
        "price_targets": ["12-month target", "24-month target", "upside scenario"],
        "development_timeline": ["timeline", "milestone", "first gold", "production target"],
        "certainty": ["certainty", "probability", "risk to milestones"],
        "headwinds_tailwinds": ["headwind", "tailwind", "sensitivity", "threshold"],
        "npv_assessment": ["npv", "irr", "capex", "aisc", "mine life"],
        "management_competition_assessment": [
            "management",
            "board",
            "executive",
            "ceo",
            "cfo",
            "insider ownership",
            "governance",
            "leadership changes",
            "track record",
            "competition",
            "peer positioning",
        ],
    }
    return fallback.get(sid, _markers_for_field_name(sid))


def _build_stage1_research_planner(
    *,
    user_query: str,
    research_brief: str,
    ticker: Optional[str],
    verification_profile: Dict[str, Any],
    max_waves: int,
    gap_query_limit: int,
) -> Dict[str, Any]:
    """Create deterministic planner payload for multi-wave retrieval."""
    section_markers = verification_profile.get("compliance_section_markers", []) or []
    critical_sections = set(verification_profile.get("compliance_critical_sections", set()) or set())
    objectives: List[Dict[str, Any]] = []
    ordered_sections: List[str] = []

    for section_id, markers in section_markers:
        sid = str(section_id or "").strip().lower()
        if not sid:
            continue
        if sid in ordered_sections:
            continue
        ordered_sections.append(sid)
        objectives.append(
            {
                "section": sid,
                "critical": sid in critical_sections,
                "markers": list(markers or [])[:6],
                "keywords": _keywords_for_gap_section(sid, verification_profile)[:8],
            }
        )

    if not objectives:
        for sid, keywords in (verification_profile.get("fact_digest_keywords", {}) or {}).items():
            sid_norm = str(sid or "").strip().lower()
            if not sid_norm:
                continue
            objectives.append(
                {
                    "section": sid_norm,
                    "critical": False,
                    "markers": _markers_for_field_name(sid_norm)[:4],
                    "keywords": _normalize_terms_list(keywords)[:8],
                }
            )
            ordered_sections.append(sid_norm)

    wave_plan: List[Dict[str, Any]] = [
        {
            "wave": 1,
            "type": "broad_primary",
            "focus_sections": ordered_sections[: max(2, min(5, len(ordered_sections)))],
            "query_intent": "broad primary-source coverage",
        }
    ]

    safe_gap_limit = max(1, int(gap_query_limit))
    safe_max_waves = max(1, int(max_waves))
    for wave in range(2, safe_max_waves + 1):
        wave_plan.append(
            {
                "wave": wave,
                "type": "gap_fill",
                "focus_sections": [],
                "query_intent": f"target unresolved sections (top {safe_gap_limit})",
            }
        )

    return {
        "planner_version": "stage1_multi_wave_v1",
        "ticker": str(ticker or "").strip(),
        "query_preview": (user_query or "").strip()[:260],
        "brief_preview": (research_brief or "").strip()[:260],
        "max_waves": safe_max_waves,
        "gap_query_limit": safe_gap_limit,
        "objectives": objectives,
        "wave_plan": wave_plan,
    }


def _evaluate_stage1_section_coverage(
    run: Dict[str, Any],
    verification_profile: Dict[str, Any],
) -> Dict[str, Any]:
    """Estimate which rubric sections are currently evidenced by retrieved output."""
    summary_text = str(run.get("research_summary", ""))
    filtered_summary_lines: List[str] = []
    for line in summary_text.splitlines():
        clean_line = re.sub(r"\s+", " ", str(line or "")).strip()
        if not clean_line:
            continue
        if _is_low_signal_legal_boilerplate(clean_line):
            continue
        filtered_summary_lines.append(clean_line)
    text_parts: List[str] = ["\n".join(filtered_summary_lines)]
    for update in (run.get("latest_updates", []) or [])[:8]:
        text_parts.append(str(update.get("update", "")))
        text_parts.append(str(update.get("why_it_matters", "")))
    raw_results = list((run.get("results", []) or [])[:12])
    non_low_results = [row for row in raw_results if not _is_low_signal_notice_source_item(row)]
    coverage_results = non_low_results if non_low_results else raw_results[:4]

    for result in coverage_results[:10]:
        text_parts.append(str(result.get("title", "")))
        text_parts.append(str(result.get("content", ""))[:900])
    corpus = "\n".join(text_parts).lower()

    section_markers = verification_profile.get("compliance_section_markers", []) or []
    critical_sections = set(verification_profile.get("compliance_critical_sections", set()) or set())
    coverage: Dict[str, bool] = {}
    marker_hits: Dict[str, int] = {}
    for section_id, markers in section_markers:
        sid = str(section_id or "").strip().lower()
        if not sid:
            continue
        hit_count = sum(1 for marker in (markers or []) if str(marker).lower() in corpus)
        coverage[sid] = hit_count > 0
        marker_hits[sid] = int(hit_count)

    missing_sections = [sid for sid, covered in coverage.items() if not covered]
    missing_critical = [sid for sid in missing_sections if sid in critical_sections]
    return {
        "coverage": coverage,
        "marker_hits": marker_hits,
        "missing_sections": missing_sections,
        "missing_critical_sections": missing_critical,
        "covered_sections": [sid for sid, covered in coverage.items() if covered],
        "critical_sections_total": len(critical_sections),
        "critical_sections_covered": sum(
            1 for sid in critical_sections if coverage.get(sid, False)
        ),
    }


def _build_stage1_gap_query_block(
    *,
    missing_sections: List[str],
    verification_profile: Dict[str, Any],
    ticker: Optional[str],
    gap_query_limit: int,
) -> str:
    """Create targeted gap-fill query hints for follow-up retrieval waves."""
    if not missing_sections:
        return ""
    lines = ["GAP-FILL OBJECTIVES FOR THIS WAVE:"]
    safe_limit = max(1, int(gap_query_limit))
    ticker_prefix = str(ticker or "").strip()
    for section_id in missing_sections[:safe_limit]:
        keywords = _keywords_for_gap_section(section_id, verification_profile)
        focus = ", ".join(keywords[:5]) if keywords else section_id.replace("_", " ")
        if ticker_prefix:
            query_hint = f"{ticker_prefix} {focus}"
        else:
            query_hint = focus
        lines.append(f"- {section_id}: {query_hint}")
    lines.append("Use primary filings/official investor materials first; then fill with secondary sources if needed.")
    return "\n".join(lines)


def _count_new_primary_sources(run: Dict[str, Any], seen_primary_urls: set[str]) -> int:
    """Count newly discovered primary/high-authority sources in a wave run."""
    new_primary = 0
    for result in (run.get("results", []) or []):
        url = str(result.get("url", "")).strip()
        if not url:
            continue
        authority = _source_authority_rank(url)
        if authority < 2:
            continue
        if url in seen_primary_urls:
            continue
        seen_primary_urls.add(url)
        new_primary += 1
    return new_primary


def _merge_stage1_wave_runs(
    *,
    wave_runs: List[Dict[str, Any]],
    original_query: str,
    max_sources: int,
    planner: Dict[str, Any],
    wave_reports: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge multi-wave retrieval outputs into a single Stage 1 run payload."""
    if not wave_runs:
        return {
            "query": original_query,
            "results": [],
            "result_count": 0,
            "provider": "perplexity",
            "error": "No successful wave runs",
        }

    merged_by_url: Dict[str, Dict[str, Any]] = {}
    for run in wave_runs:
        for item in (run.get("results", []) or []):
            url = str(item.get("url", "")).strip()
            if not url:
                continue
            existing = merged_by_url.get(url)
            if existing is None:
                merged_by_url[url] = dict(item)
                continue
            if float(item.get("score", 0.0)) > float(existing.get("score", 0.0)):
                existing["score"] = float(item.get("score", 0.0))
            if len(str(item.get("content", ""))) > len(str(existing.get("content", ""))):
                existing["content"] = item.get("content", "")
            if not str(existing.get("published_at", "")).strip() and str(
                item.get("published_at", "")
            ).strip():
                existing["published_at"] = item.get("published_at")
            existing_title = str(existing.get("title", "")).strip()
            if existing_title.lower().startswith("asx announcement pdf") and str(
                item.get("title", "")
            ).strip():
                existing["title"] = item.get("title", "")

    merged_results = list(merged_by_url.values())
    merged_results.sort(
        key=lambda row: (
            float(row.get("score", 0.0)),
            str(row.get("published_at", "")),
            _source_authority_rank(str(row.get("url", ""))),
        ),
        reverse=True,
    )

    low_signal_total = sum(1 for row in merged_results if _is_low_signal_notice_source_item(row))
    limit = max(1, int(max_sources))

    filtered_results: List[Dict[str, Any]] = []
    low_signal_used = 0
    for row in merged_results:
        if len(filtered_results) >= limit:
            break
        is_low_signal = _is_low_signal_notice_source_item(row)
        if is_low_signal:
            continue
        filtered_results.append(row)

    merged_results = filtered_results[:limit]

    updates_by_key: Dict[str, Dict[str, Any]] = {}
    for run in wave_runs:
        for update in (run.get("latest_updates", []) or []):
            key = (
                str(update.get("date", "")).strip(),
                str(update.get("update", "")).strip(),
                str(update.get("source_url", "")).strip(),
            )
            if key in updates_by_key:
                continue
            updates_by_key[key] = dict(update)
    merged_updates = list(updates_by_key.values())[:8]

    summary_parts: List[str] = []
    for idx, run in enumerate(wave_runs, start=1):
        summary = str(run.get("research_summary", "")).strip()
        if not summary:
            continue
        first_line = summary.splitlines()[0].strip()
        if first_line:
            summary_parts.append(f"Wave {idx}: {first_line}")
    merged_summary = str(wave_runs[-1].get("research_summary", "")).strip()
    if summary_parts:
        merged_summary = (
            f"{merged_summary}\n\n### Retrieval Waves\n" + "\n".join(f"- {part}" for part in summary_parts)
        ).strip()

    decode_attempted = 0
    decode_decoded = 0
    decode_failed = 0
    decode_sources: List[Dict[str, Any]] = []
    for run in wave_runs:
        decode_meta = (run.get("provider_metadata", {}) or {}).get("source_decoding", {}) or {}
        decode_attempted += int(decode_meta.get("attempted", 0))
        decode_decoded += int(decode_meta.get("decoded", 0))
        decode_failed += int(decode_meta.get("failed", 0))
        for row in (decode_meta.get("sources", []) or [])[:12]:
            decode_sources.append(dict(row))

    merged = copy.deepcopy(wave_runs[-1])
    merged["query"] = original_query
    merged["results"] = merged_results
    merged["result_count"] = len(merged_results)
    merged["latest_updates"] = merged_updates
    merged["research_summary"] = merged_summary

    provider_meta = merged.setdefault("provider_metadata", {})
    if not isinstance(provider_meta, dict):
        provider_meta = {}
        merged["provider_metadata"] = provider_meta
    provider_meta["source_decoding"] = {
        "enabled": True,
        "attempted": decode_attempted,
        "decoded": decode_decoded,
        "failed": decode_failed,
        "sources": decode_sources[:60],
    }
    provider_meta["stage1_multi_wave"] = {
        "enabled": True,
        "planner": planner,
        "waves_requested": int(planner.get("max_waves", len(wave_runs))),
        "waves_completed": len(wave_runs),
        "low_signal_sources_total": int(low_signal_total),
        "low_signal_sources_kept": int(low_signal_used),
        "low_signal_sources_dropped": int(max(0, low_signal_total - low_signal_used)),
        "wave_reports": wave_reports,
    }
    return merged


