"""Investment verdict and rating normalisation."""

import re
from typing import Any, Dict, List, Optional
import logging

from .text_utils import (
    _dedupe_text_list,
    _derive_positioning_basis,
    _extract_tagged_section,
    _normalize_current_positioning_value,
    _split_inline_items,
    _strip_list_prefix,
    _strip_markdown_formatting,
)

logger = logging.getLogger(__name__)

def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_rating_value(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"BUY", "HOLD", "SELL"}:
        return raw
    if raw in {"OUTPERFORM", "OVERWEIGHT", "ACCUMULATE"}:
        return "BUY"
    if raw in {"NEUTRAL", "MARKET PERFORM", "EQUAL WEIGHT"}:
        return "HOLD"
    if raw in {"UNDERPERFORM", "UNDERWEIGHT", "REDUCE"}:
        return "SELL"
    return ""


def _normalize_conviction_value(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"HIGH", "MEDIUM", "LOW"}:
        return raw
    if raw in {"STRONG"}:
        return "HIGH"
    if raw in {"MODERATE", "MID"}:
        return "MEDIUM"
    if raw in {"WEAK"}:
        return "LOW"
    return ""


def _extract_investment_verdict_from_text(chairman_text: str) -> Dict[str, str]:
    import re

    section = _extract_tagged_section(chairman_text, "investment_verdict") or str(
        chairman_text or ""
    )
    normalized_section = _strip_markdown_formatting(section)
    rating_match = re.search(
        r"(?i)\b(?:rating|recommendation)\b\s*[:\-]\s*(buy|hold|sell|outperform|overweight|accumulate|neutral|market perform|equal weight|underperform|underweight|reduce)",
        normalized_section,
    )
    conviction_match = re.search(
        r"(?i)\bconviction\b\s*[:\-]\s*(high|medium|low|strong|moderate|mid|weak)",
        normalized_section,
    )
    top_reasons: List[str] = []
    failure_conditions: List[str] = []
    rationale_lines: List[str] = []
    current_positioning = ""
    why_current_positioning = ""

    def _heading_tail(line: str) -> str:
        parts = re.split(r"\s*:\s*", line, maxsplit=1)
        return parts[1].strip() if len(parts) == 2 else ""

    current_block: Optional[str] = None
    for raw_line in section.splitlines():
        original = str(raw_line or "").strip()
        if not original:
            continue
        line = _strip_list_prefix(original)
        lower = line.lower()

        if re.search(
            r"top\s*3\s+(?:reasons?.*(?:success|bull)|success\s+indicators?|bull\s+indicators?)",
            lower,
        ):
            current_block = "top_reasons"
            tail = _heading_tail(line)
            if tail:
                top_reasons.extend(_split_inline_items(tail))
            continue
        if re.search(
            r"top\s*3\s+(?:failure\s+(?:conditions|indicators?)|bear\s+indicators?|risk\s+indicators?)",
            lower,
        ) or re.search(
            r"failure\s+conditions?.*(bear|thesis|case)", lower
        ):
            current_block = "failure_conditions"
            tail = _heading_tail(line)
            if tail:
                failure_conditions.extend(_split_inline_items(tail))
            continue
        if (
            "where the evidence leans" in lower
            or lower.startswith("current evidence lean")
            or lower.startswith("the evidence leans")
            or lower.startswith("evidence leans")
            or lower.startswith("current lean")
            or lower.startswith("current_positioning")
            or lower.startswith("current positioning")
        ):
            current_block = "current_positioning"
            tail = _heading_tail(line)
            if tail:
                current_positioning = (
                    _normalize_current_positioning_value(tail) or current_positioning
                )
                why_current_positioning = _derive_positioning_basis(tail)
            else:
                current_positioning = (
                    _normalize_current_positioning_value(line) or current_positioning
                )
                why_current_positioning = _derive_positioning_basis(line)
            continue
        if re.match(r"(?i)^(rating|conviction)\b", line):
            current_block = None
            continue
        if re.match(
            r"(?i)^(top\s*3\b|decisive\b|sizing\b|key risks?\b|key opportunities?\b)",
            line,
        ):
            current_block = None

        if current_block == "top_reasons":
            top_reasons.extend(_split_inline_items(line))
            continue
        if current_block == "failure_conditions":
            failure_conditions.extend(_split_inline_items(line))
            continue
        if current_block == "current_positioning":
            if not current_positioning:
                current_positioning = _normalize_current_positioning_value(line)
            if not why_current_positioning:
                why_current_positioning = _derive_positioning_basis(line)
            elif line and line != why_current_positioning:
                why_current_positioning = f"{why_current_positioning} {line}".strip()
            continue

        if re.match(
            r"(?i)^(sizing|the single decisive reason|single decisive reason|decisive reason|key risks?|key opportunities?)\b",
            line,
        ):
            current_block = None
            if ":" in line:
                rationale_piece = _heading_tail(line)
                if rationale_piece:
                    rationale_lines.append(rationale_piece)
            continue

        rationale_lines.append(line)

    top_reasons = _dedupe_text_list(top_reasons, limit=5)
    failure_conditions = _dedupe_text_list(failure_conditions, limit=5)
    if not current_positioning or not why_current_positioning:
        lean_match = re.search(
            r"(?i)(?:current\s+evidence|the\s+evidence)\s+leans?\s*:?\s*(.+?)(?=\btop\s*3\b|$)",
            normalized_section,
        )
        if lean_match:
            lean_tail = str(lean_match.group(1) or "").strip()
            if not current_positioning:
                current_positioning = _normalize_current_positioning_value(lean_tail)
            if not why_current_positioning:
                why_current_positioning = _derive_positioning_basis(lean_tail)
    if not current_positioning or not why_current_positioning:
        sentence_match = re.search(
            r"(?im)\bcurrently\b[^.\n]*\b(base|bull|bear)(?:\s*(?:-|/|to)\s*(?:base|bull|bear))*[^.\n]*",
            normalized_section,
        )
        if sentence_match:
            sentence = str(sentence_match.group(0) or "").strip()
            if not current_positioning:
                current_positioning = _normalize_current_positioning_value(sentence)
            if not why_current_positioning:
                why_current_positioning = _derive_positioning_basis(sentence)
    if not current_positioning:
        current_positioning = _normalize_current_positioning_value(normalized_section)
    if not why_current_positioning:
        why_current_positioning = _derive_positioning_basis(normalized_section)
    return {
        "rating": _normalize_rating_value(rating_match.group(1) if rating_match else ""),
        "conviction": _normalize_conviction_value(conviction_match.group(1) if conviction_match else ""),
        "rationale": " ".join(rationale_lines).strip(),
        "top_reasons": top_reasons,
        "failure_conditions": failure_conditions,
        "current_positioning": current_positioning,
        "why_current_positioning": why_current_positioning,
    }


