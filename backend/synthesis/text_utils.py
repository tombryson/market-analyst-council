"""Shared text-manipulation utilities used across synthesis sub-modules."""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)

def _extract_tagged_section(chairman_text: str, tag: str) -> str:
    import re

    text = str(chairman_text or "")
    safe_tag = re.escape(str(tag or "").strip())
    if not text or not safe_tag:
        return ""
    match = re.search(
        rf"<{safe_tag}>\s*(.*?)\s*</{safe_tag}>",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    return str(match.group(1) if match else "").strip()


def _infer_timeline_status_from_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if re.search(r"\b(at[- ]?risk|delayed|delay|slipped|missed)\b", text):
        return "at_risk"
    if re.search(r"\b(achieved|complete|completed|delivered|produced|commissioned|first production|first dried yellowcake)\b", text):
        return "achieved"
    if re.search(r"\b(commenced|started|in progress|ongoing|ramping|ramp-up|ramp up)\b", text):
        return "current"
    if re.search(r"\b(expected|planned|target|pending|speculative|proposed)\b", text):
        return "planned"
    return ""


def _strip_markdown_formatting(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    prev = None
    while text != prev:
        prev = text
        text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
        text = re.sub(r"__(.*?)__", r"\1", text)
        text = re.sub(r"(?<!\*)\*([^*]+?)\*(?!\*)", r"\1", text)
        text = re.sub(r"(?<!_)_([^_]+?)_(?!_)", r"\1", text)
        text = re.sub(r"`([^`]+)`", r"\1", text)
    text = text.replace("**", "").replace("__", "").replace("`", "")
    text = re.sub(r"(^|\s)\*(?=\S)", r"\1", text)
    text = re.sub(r"(?<=\S)\*(?=\s*[:.,;!?)]|\s*$)", "", text)
    return text


def _strip_list_prefix(value: Any) -> str:
    text = _strip_markdown_formatting(value).strip()
    text = re.sub(r"^(?:(?:[-*•]+|\d+[.)])\s*)+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _split_inline_items(value: Any) -> List[str]:
    text = _strip_list_prefix(value)
    if not text:
        return []
    text = re.sub(r"\s+\d+[.)]\s+", " | ", text)
    parts = re.split(r"\s*[;|]\s*|\s{2,}", text)
    cleaned = [_strip_list_prefix(part) for part in parts if _strip_list_prefix(part)]
    return cleaned or [text]


def _dedupe_text_list(values: Any, limit: int = 6) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in values or []:
        text = _strip_list_prefix(raw)
        if not text:
            continue
        key = re.sub(r"\W+", "", text).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return out


_THESIS_EMBEDDED_LABEL_PATTERNS: List[Tuple[str, str]] = [
    ("target_12m", r"(?:target[_\s-]*12m|12m\s+target)"),
    ("target_24m", r"(?:target[_\s-]*24m|24m\s+target)"),
    ("probability_24m_pct", r"(?:probability[_\s-]*24m|24m\s+probability|probability)"),
    ("required_conditions", r"required\s+conditions?"),
    ("failure_conditions", r"(?:failure|break)\s+conditions?"),
    ("current_positioning", r"current\s+positioning"),
    ("why_current_positioning", r"why\s+current\s+positioning"),
]


def _normalize_current_positioning_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if re.fullmatch(
        r"(?:to\s+)?(?:bull|base|bear)(?:-leaning)?(?:\s+to\s+(?:bull|base|bear)(?:-leaning)?)*\.?",
        text,
    ):
        if len(set(re.findall(r"\b(bull|base|bear)\b", text))) > 1:
            return "mixed"
    flags = {
        "bull": "bull" in text,
        "base": "base" in text,
        "bear": "bear" in text,
    }
    if sum(1 for present in flags.values() if present) > 1:
        return "mixed"
    if "mixed" in text:
        return "mixed"
    if "bull" in text:
        return "bull-leaning"
    if "base" in text:
        return "base-leaning"
    if "bear" in text:
        return "bear-leaning"
    return ""


def _derive_positioning_basis(value: Any) -> str:
    text = _strip_list_prefix(value)
    if not text:
        return ""
    text = re.sub(r"(?i)^currently\b\s*[:,\-]?\s*", "", text).strip()
    text = re.sub(
        r"(?i)\b(where\s+the\s+evidence\s+leans(?:\s+today)?|the\s+evidence\s+leans(?:\s+today)?|evidence\s+leans(?:\s+today)?|current\s+evidence\s+lean|current\s+lean|current_positioning|current positioning)\b\s*[:\-]?\s*",
        "",
        text,
    ).strip()
    text = re.sub(r"(?i)\b(base|bull|bear)\s*/\s*(base|bull|bear)\b", "", text).strip()
    text = re.sub(
        r'(?i)^["\']?(?:base|bull|bear)(?:(?:\s*[-/]\s*|\s+to\s+|-to-)(?:base|bull|bear))+["\']?\b',
        "",
        text,
    ).strip()
    text = re.sub(
        r"(?i)\b(base|bull|bear)(?:-leaning)?\s+to\s+(base|bull|bear)(?:-leaning)?\b",
        "",
        text,
    ).strip()
    text = re.sub(r"(?i)^(bull|base|bear)(?:-leaning)?\b\s*[:,\-]?\s*", "", text).strip()
    text = re.sub(r"(?i)^(bull|base|bear)\b\s*[.]\s*", "", text).strip()
    text = re.sub(r"(?i)^mixed\b\s*[:,\-]?\s*", "", text).strip()
    text = re.sub(r'(?i)^["\']?(?:bull|base|bear)(?:-leaning)?["\']?\b\s*[:,\-]?\s*', "", text).strip()
    text = re.sub(r"(?i)^as\b\s+", "", text).strip()
    text = text.lstrip("/").strip()
    text = re.sub(r"^[\s.,:;\-]+", "", text).strip()
    if re.fullmatch(
        r"(?:to\s+)?(?:bull|base|bear)(?:-leaning)?(?:\s+to\s+(?:bull|base|bear)(?:-leaning)?)*\.?",
        text,
        flags=re.IGNORECASE,
    ):
        return ""
    return text


