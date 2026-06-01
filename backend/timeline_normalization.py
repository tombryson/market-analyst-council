"""Timeline normalization helpers shared by Stage 3 and API consumers."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


_STATUS_TOKENS = (
    "at risk",
    "at-risk",
    "at_risk",
    "delayed",
    "delay",
    "pending",
    "speculative",
    "contingent",
    "dependent",
    "completed",
    "complete",
    "achieved",
    "delivered",
    "closed",
    "on track",
    "on-track",
    "partially on-track",
    "partially on track",
    "planned",
)


_KNOWN_ACRONYMS = {
    "asx": "ASX",
    "aud": "AUD",
    "dfs": "DFS",
    "esg": "ESG",
    "fid": "FID",
    "jorc": "JORC",
    "mre": "MRE",
    "mt": "MT",
    "moe": "MoE",
    "pfs": "PFS",
    "mres": "MREs",
    "q1": "Q1",
    "q2": "Q2",
    "q3": "Q3",
    "q4": "Q4",
}


def normalize_target_period_label(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return ""

    upper = text.upper()
    upper = upper.replace("CALENDAR YEAR", "CY")
    upper = re.sub(r"\s+", " ", upper).strip()

    month_to_quarter = {
        "JAN": "Q1",
        "FEB": "Q1",
        "MAR": "Q1",
        "APR": "Q2",
        "MAY": "Q2",
        "JUN": "Q2",
        "JUL": "Q3",
        "AUG": "Q3",
        "SEP": "Q3",
        "SEPT": "Q3",
        "OCT": "Q4",
        "NOV": "Q4",
        "DEC": "Q4",
    }

    q_range = re.match(r"^Q([1-4])\s*[-/]\s*Q([1-4])\s*(?:CY\s*)?(20\d{2})$", upper)
    if q_range:
        return f"Q{q_range.group(1)}-Q{q_range.group(2)} {q_range.group(3)}"

    q_half_range = re.match(r"^Q([1-4])\s*[-/]\s*H([12])\s*(?:CY\s*)?(20\d{2})$", upper)
    if q_half_range:
        return f"Q{q_half_range.group(1)}-H{q_half_range.group(2)} {q_half_range.group(3)}"

    h_range = re.match(r"^H([12])\s*[-/]\s*H([12])\s*(?:CY\s*)?(20\d{2})$", upper)
    if h_range:
        return f"H{h_range.group(1)}-H{h_range.group(2)} {h_range.group(3)}"

    q_single = re.match(r"^Q([1-4])\s*(?:CY\s*)?(20\d{2})$", upper)
    if q_single:
        return f"Q{q_single.group(1)} {q_single.group(2)}"

    h_single = re.match(r"^H([12])\s*(?:CY\s*)?(20\d{2})$", upper)
    if h_single:
        return f"H{h_single.group(1)} {h_single.group(2)}"

    month_period = re.match(
        r"^(?:END\s+)?(JAN(?:UARY)?|FEB(?:RUARY)?|MAR(?:CH)?|APR(?:IL)?|MAY|JUN(?:E)?|JUL(?:Y)?|AUG(?:UST)?|SEP(?:T(?:EMBER)?)?|OCT(?:OBER)?|NOV(?:EMBER)?|DEC(?:EMBER)?)(?:\s+Q(?:UARTER)?)?\s*(?:CY\s*)?(20\d{2})$",
        upper,
    )
    if month_period:
        month_token = month_period.group(1)[:4]
        month_token = "SEPT" if month_token.startswith("SEPT") else month_token[:3]
        quarter = month_to_quarter.get(month_token)
        if quarter:
            return f"{quarter} {month_period.group(2)}"

    rel_year = re.match(r"^(EARLY|START|BEGINNING|MID|LATE|END)[\s-]+(?:CY\s*)?(20\d{2})$", upper)
    if rel_year:
        qualifier = rel_year.group(1)
        year = rel_year.group(2)
        if qualifier in {"EARLY", "START", "BEGINNING", "MID"}:
            return f"H1 {year}"
        return f"H2 {year}" if qualifier == "LATE" else f"Q4 {year}"

    return text


def infer_timeline_status_from_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if re.search(r"\b(at[- _]?risk|delayed|delay|slipped|missed|pending approval|permit|approval pending|contingent|dependent)\b", text):
        return "at_risk"
    if re.search(r"\b(achieved|complete|completed|delivered|closed|produced|commissioned|first production|first dried yellowcake)\b", text):
        return "achieved"
    if re.search(r"\b(commenced|started|in progress|ongoing|ramping|ramp-up|ramp up)\b", text):
        return "current"
    if re.search(r"\b(expected|planned|target|speculative|proposed|on[- ]track|partially on[- ]track)\b", text):
        return "planned"
    return ""


def normalize_timeline_status(raw: Any, *context: Any) -> str:
    text = str(raw or "").strip().lower().replace("-", "_")
    aliases = {
        "at risk": "at_risk",
        "at_risk": "at_risk",
        "delayed": "at_risk",
        "pending": "at_risk",
        "speculative": "planned",
        "on track": "planned",
        "on_track": "planned",
        "partially on_track": "planned",
        "achieved": "achieved",
        "complete": "achieved",
        "completed": "achieved",
        "closed": "achieved",
        "delivered": "achieved",
        "commenced": "current",
        "started": "current",
        "in progress": "current",
        "in_progress": "current",
        "ongoing": "current",
        "current": "current",
        "planned": "planned",
        "unspecified": "",
    }
    status = aliases.get(text, text if text in {"current", "planned", "at_risk", "achieved"} else "")
    for value in context:
        inferred = infer_timeline_status_from_text(value)
        if inferred == "at_risk":
            return "at_risk"
        if inferred and status in {"", "planned"}:
            status = inferred
    return status or "planned"


def _title_word(word: str, *, is_first: bool) -> str:
    lower = word.lower()
    if lower in _KNOWN_ACRONYMS:
        return _KNOWN_ACRONYMS[lower]
    if "-" in word:
        return "-".join(
            _title_word(part, is_first=is_first and idx == 0)
            for idx, part in enumerate(word.split("-"))
        )
    prefix_match = re.match(r"^([^A-Za-z0-9$]*)(.+?)([^A-Za-z0-9)]*)$", word)
    if prefix_match:
        prefix, core, suffix = prefix_match.groups()
        if prefix or suffix:
            return f"{prefix}{_title_word(core, is_first=is_first)}{suffix}"
    if not is_first and lower in {"and", "or", "of", "to", "from", "for", "by", "with"}:
        return lower
    if any(ch.isdigit() for ch in word) or word.isupper():
        return word
    return word[:1].upper() + word[1:].lower()


def _standardize_title(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = re.sub(r"\s+", " ", text).strip(" .;-")
    words = text.split(" ")
    return " ".join(_title_word(word, is_first=idx == 0) for idx, word in enumerate(words))


def _extract_inline_period(text: Any) -> Tuple[str, str]:
    raw = str(text or "").strip()
    if not raw:
        return "", ""
    period_pattern = re.compile(
        r"\b(Q[1-4](?:\s*[-/]\s*(?:Q[1-4]|H[12]))?\s*20\d{2}|"
        r"H[12](?:\s*[-/]\s*H[12])?\s*20\d{2}|"
        r"(?:early|mid|late|end)\s+(?:[A-Za-z]{3,9}\s+)?20\d{2}|"
        r"[A-Za-z]{3,9}\s+20\d{2}|20\d{2})\b",
        re.IGNORECASE,
    )
    colon_split = re.match(r"^([^:]{2,45}):\s*(.+)$", raw)
    if colon_split:
        lhs = str(colon_split.group(1) or "").strip()
        rhs = str(colon_split.group(2) or "").strip()
        if period_pattern.search(lhs):
            return lhs, rhs or raw
    match = period_pattern.search(raw)
    if not match:
        return "", raw
    period = str(match.group(1) or "").strip()
    stripped = re.sub(r"^[:\-\s]+", "", raw.replace(match.group(0), "")).strip()
    return period, stripped or raw


def _split_status_annotation(text: str) -> Tuple[str, str, str]:
    title = str(text or "").strip()
    notes: List[str] = []
    status = ""

    def absorb_note(note: str) -> None:
        cleaned = re.sub(r"\s+", " ", note or "").strip(" .;-")
        status_only = re.sub(r"[^a-z_ -]+", "", cleaned.lower()).strip()
        status_only = status_only.replace("_", " ")
        if cleaned and cleaned.lower() not in {"reference"} and status_only not in {
            "at risk",
            "at-risk",
            "speculative",
            "completed",
            "complete",
            "achieved",
            "on track",
            "on-track",
            "partially on-track",
            "partially on track",
            "planned",
        }:
            notes.append(cleaned)

    # Remove bracketed reference/status fragments anywhere in the title.
    for match in list(re.finditer(r"\[([^\]]+)\]", title)):
        content = match.group(1)
        inferred = infer_timeline_status_from_text(content)
        if inferred:
            status = inferred
            absorb_note(content)
        title = title.replace(match.group(0), " ")

    # Strip terminal parenthetical status/commentary.
    changed = True
    while changed:
        changed = False
        match = re.search(r"\(([^()]*)\)\s*\.?$", title)
        if match:
            content = str(match.group(1) or "").strip()
            if any(token in content.lower() for token in _STATUS_TOKENS):
                inferred = infer_timeline_status_from_text(content)
                if inferred:
                    status = inferred
                absorb_note(content)
                title = title[: match.start()].strip()
                changed = True

    # Strip suffix forms such as " - AT RISK (Delayed by approvals)".
    suffix_match = re.search(
        r"(?i)\s[-–—]\s*(AT[- _]?RISK|SPECULATIVE|COMPLETED?|ACHIEVED|PARTIALLY\s+ON[- ]TRACK|ON[- ]TRACK|PLANNED)\b(.*)$",
        title,
    )
    if suffix_match:
        suffix = f"{suffix_match.group(1)} {suffix_match.group(2) or ''}".strip()
        inferred = infer_timeline_status_from_text(suffix)
        if inferred:
            status = inferred
        absorb_note(suffix)
        title = title[: suffix_match.start()].strip()

    # Remove standalone trailing status words without losing real title content.
    title = re.sub(
        r"(?i)\b(?:AT[- _]?RISK|SPECULATIVE|COMPLETED?|ACHIEVED|PARTIALLY\s+ON[- ]TRACK|ON[- ]TRACK|PLANNED)\b\s*$",
        "",
        title,
    ).strip(" .;-")

    note = "; ".join(dict.fromkeys(notes))
    return title, status, note


def standardize_timeline_row(item: Any, idx: int = 0) -> Optional[Dict[str, Any]]:
    if isinstance(item, dict):
        milestone = str(
            item.get("milestone")
            or item.get("event")
            or item.get("name")
            or item.get("goal")
            or item.get("title")
            or ""
        ).strip()
        target_period = str(
            item.get("target_period")
            or item.get("targetPeriod")
            or item.get("period")
            or item.get("when")
            or item.get("date")
            or ""
        ).strip()
        status_raw = item.get("status") or item.get("current_status") or item.get("state") or ""
        confidence = item.get("confidence_pct")
        if confidence is None:
            confidence = item.get("certainty_pct")
        primary_risk = str(item.get("primary_risk") or item.get("risk") or item.get("status_note") or "").strip()
    elif isinstance(item, str):
        milestone = item.strip()
        target_period = ""
        status_raw = ""
        confidence = None
        primary_risk = ""
    else:
        return None

    if milestone and not target_period:
        inline_period, cleaned_milestone = _extract_inline_period(milestone)
        if inline_period:
            target_period = inline_period
            milestone = cleaned_milestone or milestone

    cleaned_milestone, annotation_status, annotation_note = _split_status_annotation(milestone)
    if annotation_note:
        primary_risk = "; ".join(part for part in [primary_risk, annotation_note] if part)
    milestone = _standardize_title(cleaned_milestone or milestone or f"Milestone {idx + 1}")
    normalized_period = normalize_target_period_label(target_period)
    status = normalize_timeline_status(status_raw or annotation_status, milestone, primary_risk)

    row: Dict[str, Any] = {
        "milestone": milestone,
        "target_period": normalized_period or target_period,
        "status": status,
        "confidence_pct": confidence,
        "primary_risk": primary_risk,
    }
    if normalized_period and target_period and normalized_period != target_period:
        row["raw_target_period"] = target_period
    return row


def normalize_timeline_rows(raw_timeline: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_timeline, list):
        return []
    rows: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw_timeline):
        row = standardize_timeline_row(item, idx=idx)
        if row and (row.get("milestone") or row.get("target_period")):
            rows.append(row)
    return rows
