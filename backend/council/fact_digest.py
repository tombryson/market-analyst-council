"""Stage-1 fact-digest v2: sentence scoring and digest assembly.

Builds a compact, deduplicated fact digest from Perplexity source rows,
grouping facts by section and scoring by recency and authority.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from ..config import (
    PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_FACTS_PER_SECTION,
    PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_NARRATIVE_WORDS,
    PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_SUMMARY_BULLETS,
)
from .source_analysis import (
    _extract_source_sentences,
    _extract_timeline_windows,
    _source_authority_rank,
    _window_to_quarter_index,
)
from .stage1_multi_wave import _normalize_terms_list
from .perplexity_client import _FACT_DIGEST_V2_KEYWORDS, _FACT_DIGEST_V2_NARRATIVE_ORDER, _FACT_DIGEST_V2_SECTIONS, _STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS

logger = logging.getLogger(__name__)

def _normalize_fact_key(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _truncate_to_word_limit(text: str, max_words: int) -> str:
    words = (text or "").split()
    safe_limit = max(40, int(max_words))
    if len(words) <= safe_limit:
        return (text or "").strip()
    return " ".join(words[:safe_limit]).strip() + " ..."


def _classify_fact_digest_v2_section(
    sentence: str,
    section_keywords: Dict[str, List[str]],
) -> str:
    text = (sentence or "").lower()
    best_section = "other_material_facts"
    best_score = 0
    for section, keywords in (section_keywords or {}).items():
        score = sum(1 for token in keywords if token in text)
        if score > best_score:
            best_score = score
            best_section = section
    return best_section


def _extract_fact_digest_number_tokens(sentence: str, max_tokens: int = 4) -> List[str]:
    tokens: List[str] = []
    for match in re.findall(
        r"(?:A\$|AU\$|US\$|\$)?\s*\d[\d,]*(?:\.\d+)?\s*(?:%|moz|koz|oz|g/t|Mt|M|B|bn|million|billion)?",
        sentence or "",
        flags=re.IGNORECASE,
    ):
        token = re.sub(r"\s+", " ", match).strip()
        if token and token not in tokens:
            tokens.append(token)
        if len(tokens) >= max(1, int(max_tokens)):
            break
    return tokens


def _score_fact_digest_sentence(sentence: str, published_at: str, authority_rank: int) -> int:
    low = (sentence or "").lower()
    score = max(0, int(authority_rank)) * 3
    if re.search(r"\d", low):
        score += 2
    if any(token in low for token in ("first gold", "gold pour", "launch", "approval", "first production")):
        score += 6
    if any(token in low for token in ("funded", "facility", "placement", "capital raise", "loan")):
        score += 4
    if any(token in low for token in ("npv", "irr", "aisc", "capex", "free cash flow", "payback")):
        score += 4
    if published_at.startswith("2026-"):
        score += 3
    elif published_at.startswith("2025-"):
        score += 2
    elif published_at.startswith("2024-"):
        score += 1
    return score


def _build_stage1_fact_digest_v2(
    source_rows: List[Dict[str, Any]],
    timeline_rows: List[Dict[str, Any]],
    *,
    section_keywords: Optional[Dict[str, List[str]]] = None,
    narrative_order: Optional[List[str]] = None,
    conflict_terms: Optional[List[str]] = None,
    conflict_field: str = "timeline_window",
    conflict_resolution_rule: str = "prefer newest dated primary-source timeline evidence",
) -> Dict[str, Any]:
    """
    Build de-noised, rubric-adjacent fact digest to accompany source injection.

    This is a deterministic extraction pass: compact, source-referenced, and conflict-aware.
    """
    safe_max_facts_per_section = max(2, int(PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_FACTS_PER_SECTION))
    safe_max_summary_bullets = max(4, int(PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_SUMMARY_BULLETS))
    safe_max_narrative_words = max(120, int(PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_NARRATIVE_WORDS))
    effective_keywords = section_keywords or _FACT_DIGEST_V2_KEYWORDS
    section_names = list(effective_keywords.keys()) or list(_FACT_DIGEST_V2_SECTIONS)
    if "other_material_facts" not in section_names:
        section_names.append("other_material_facts")
    effective_narrative_order = [
        str(item or "").strip().lower()
        for item in (narrative_order or _FACT_DIGEST_V2_NARRATIVE_ORDER)
        if str(item or "").strip()
    ]
    effective_conflict_terms = _normalize_terms_list(conflict_terms or _STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS)

    sections: Dict[str, List[Dict[str, Any]]] = {
        key: []
        for key in section_names
    }
    seen = set()
    scored_facts: List[Tuple[int, str, Dict[str, Any]]] = []
    material_tokens = (
        "first gold",
        "gold pour",
        "funded",
        "facility",
        "loan",
        "placement",
        "capital",
        "npv",
        "irr",
        "aisc",
        "capex",
        "resource",
        "reserve",
        "grade",
        "production",
        "market cap",
        "shares",
        "enterprise value",
        "cash",
        "debt",
        "timeline",
        "milestone",
        "target",
        "on track",
        "risk",
        "delay",
        "catalyst",
        "tailwind",
        "headwind",
    )

    for source in source_rows:
        source_id = str(source.get("source_id", "S?")).strip() or "S?"
        published = str(source.get("published_at", "")).strip()
        authority_rank = _source_authority_rank(str(source.get("url", "")))
        excerpt = str(source.get("excerpt", ""))
        for sentence in _extract_source_sentences(excerpt):
            low = sentence.lower()
            if not re.search(r"\d", low) and not any(token in low for token in material_tokens):
                continue
            normalized = _normalize_fact_key(sentence)
            if not normalized or normalized in seen:
                continue

            section = _classify_fact_digest_v2_section(sentence, effective_keywords)
            bucket = sections.get(section, [])
            if len(bucket) >= safe_max_facts_per_section:
                continue

            item = {
                "source_id": source_id,
                "published_at": published,
                "fact": sentence,
                "windows": _extract_timeline_windows(sentence),
                "number_tokens": _extract_fact_digest_number_tokens(sentence),
            }
            bucket.append(item)
            seen.add(normalized)
            scored_facts.append(
                (
                    _score_fact_digest_sentence(sentence, published, authority_rank),
                    section,
                    item,
                )
            )

    # Ensure critical timeline facts from dedicated timeline extractor are included.
    for row in timeline_rows:
        sentence = str(row.get("fact", "")).strip()
        if not sentence:
            continue
        normalized = _normalize_fact_key(sentence)
        if normalized in seen:
            continue
        bucket = sections.setdefault("timelines_deadlines", [])
        if len(bucket) >= safe_max_facts_per_section:
            break
        item = {
            "source_id": str(row.get("source_id", "S?")).strip() or "S?",
            "published_at": str(row.get("published_at", "")).strip(),
            "fact": sentence,
            "windows": list(row.get("windows", []) or []),
            "number_tokens": _extract_fact_digest_number_tokens(sentence),
        }
        bucket.append(item)
        seen.add(normalized)
        scored_facts.append(
            (
                _score_fact_digest_sentence(
                    sentence,
                    str(row.get("published_at", "")).strip(),
                    int(row.get("authority_rank", 0)),
                ),
                "timelines_deadlines",
                item,
            )
        )

    compact_sections = {name: rows for name, rows in sections.items() if rows}
    total_facts = sum(len(rows) for rows in compact_sections.values())
    sections_with_facts = list(compact_sections.keys())

    # Minimal conflict table: timeline disagreements across extracted milestone facts.
    timeline_candidates: List[Dict[str, Any]] = []
    for row in compact_sections.get("timelines_deadlines", []):
        low = str(row.get("fact", "")).lower()
        if effective_conflict_terms and not any(token in low for token in effective_conflict_terms):
            continue
        windows = list(row.get("windows", []) or [])
        if not windows:
            windows = _extract_timeline_windows(str(row.get("fact", "")))
        for window in windows:
            timeline_candidates.append(
                {
                    "window": str(window),
                    "source_id": str(row.get("source_id", "S?")),
                    "published_at": str(row.get("published_at", "")),
                }
            )

    conflicts: List[Dict[str, Any]] = []
    unique_windows = []
    for item in timeline_candidates:
        window = item.get("window", "")
        if window and window not in unique_windows:
            unique_windows.append(window)
    if len(unique_windows) > 1:
        ranked = sorted(
            timeline_candidates,
            key=lambda item: (
                str(item.get("published_at", "")),
                int(_window_to_quarter_index(str(item.get("window", ""))) or -1),
            ),
            reverse=True,
        )
        canonical = ranked[0] if ranked else {}
        conflicts.append(
            {
                "field": conflict_field,
                "values": timeline_candidates[:8],
                "canonical": canonical,
                "resolution_rule": conflict_resolution_rule,
            }
        )

    # High-signal bullets used as a de-noised digest for downstream reasoning.
    scored_facts.sort(key=lambda item: item[0], reverse=True)
    summary_bullets: List[str] = []
    for _, _, item in scored_facts:
        source_id = str(item.get("source_id", "S?"))
        published = str(item.get("published_at", "")).strip()
        fact = str(item.get("fact", "")).strip()
        if not fact:
            continue
        line = f"[{source_id}] {published}: {fact}" if published else f"[{source_id}] {fact}"
        if line in summary_bullets:
            continue
        summary_bullets.append(line)
        if len(summary_bullets) >= safe_max_summary_bullets:
            break

    narrative_parts: List[str] = []
    for section in effective_narrative_order:
        rows = compact_sections.get(section, [])
        if not rows:
            continue
        top_facts = "; ".join(str(row.get("fact", "")).strip() for row in rows[:2] if row.get("fact"))
        if not top_facts:
            continue
        narrative_parts.append(f"{section.replace('_', ' ').title()}: {top_facts}")
    narrative_summary = _truncate_to_word_limit(
        " ".join(narrative_parts).strip(),
        safe_max_narrative_words,
    )

    source_index = [
        {
            "source_id": row.get("source_id", ""),
            "title": row.get("title", ""),
            "url": row.get("url", ""),
            "published_at": row.get("published_at", ""),
            "decoded": bool(row.get("decoded")),
        }
        for row in source_rows
    ]

    return {
        "schema": "fact_digest_v2",
        "source_index": source_index,
        "sections": compact_sections,
        "summary_bullets": summary_bullets,
        "narrative_summary": narrative_summary,
        "conflicts": conflicts,
        "counts": {
            "source_count": len(source_rows),
            "decoded_source_count": sum(1 for row in source_rows if row.get("decoded")),
            "total_facts": total_facts,
            "sections_with_facts": len(sections_with_facts),
            "summary_bullets": len(summary_bullets),
            "conflicts": len(conflicts),
        },
    }


_ASX_ANNOUNCEMENT_SEARCH_URL = "https://www.asx.com.au/asx/v2/statistics/announcements.do"
_ASX_DETERMINISTIC_CACHE: Dict[str, Dict[str, Any]] = {}


