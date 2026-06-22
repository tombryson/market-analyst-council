"""Development timeline extraction and normalisation."""

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging

from .text_utils import _infer_timeline_status_from_text
from .verdict import _to_float

logger = logging.getLogger(__name__)

def _extract_development_timeline_from_text(
    chairman_text: str,
) -> Tuple[List[Dict[str, Any]], str, Optional[float]]:
    """Best-effort extraction of development timeline rows from chairman XML text."""
    import re

    text = str(chairman_text or "")
    section_match = re.search(
        r"<development_timeline>\s*(.*?)\s*</development_timeline>",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    section = section_match.group(1) if section_match else text

    current_stage = ""
    stage_match = re.search(r"(?i)\bcurrent\s+stage\b\s*:\s*(.+)", section)
    if stage_match:
        current_stage = re.sub(r"\*\*", "", stage_match.group(1)).strip().strip("-").strip()

    certainty_pct_24m: Optional[float] = None
    certainty_match = re.search(
        r"(?i)\bcertainty\s*24m\s*goals?\b\s*:\s*([0-9]{1,3}(?:\.[0-9]+)?)\s*%?",
        section,
    )
    if certainty_match:
        certainty_pct_24m = _to_float(certainty_match.group(1))

    def _clean_line(raw: str) -> str:
        line = str(raw or "").strip()
        if line.startswith(("-", "*")):
            line = line[1:].strip()
        line = re.sub(r"^\d+\.\s*", "", line)
        line = re.sub(r"\*\*", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        return line

    def _looks_like_period_prefix(value: str) -> bool:
        low = str(value or "").strip().lower()
        if not low:
            return False
        return bool(
            re.search(
                r"\b("
                r"q[1-4]"
                r"|h[12]"
                r"|20\d{2}"
                r"|cy\s*20\d{2}"
                r"|fy\s*20\d{2}"
                r"|jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?"
                r"|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?"
                r"|end\s+(?:cy|fy)?\s*20\d{2}"
                r"|mid\s+(?:cy|fy)?\s*20\d{2}"
                r"|late\s+(?:cy|fy)?\s*20\d{2}"
                r"|early\s+(?:cy|fy)?\s*20\d{2}"
                r"|within\s+\d+\s+(?:days?|weeks?|months?|years?)"
                r")\b",
                low,
                flags=re.IGNORECASE,
            )
        )

    def _normalize_status(raw: str) -> str:
        s = str(raw or "").strip().lower().replace("-", "_")
        aliases = {
            "at risk": "at_risk",
            "on track": "planned",
            "on_track": "planned",
            "achieved": "achieved",
            "complete": "achieved",
            "completed": "achieved",
            "commenced": "current",
            "started": "current",
            "in progress": "current",
            "ongoing": "current",
            "current": "current",
            "planned": "planned",
            "at_risk": "at_risk",
            "speculative": "planned",
        }
        return aliases.get(s, s if s in {"current", "planned", "at_risk", "achieved"} else "planned")

    rows: List[Dict[str, Any]] = []
    for raw_line in section.splitlines():
        line = _clean_line(raw_line)
        if not line:
            continue
        lower = line.lower()
        if "milestone | target period" in lower:
            continue
        if lower.startswith("current stage"):
            stage_parts = line.split(":", 1)
            if len(stage_parts) == 2 and not current_stage:
                current_stage = stage_parts[1].strip()
            continue
        if certainty_pct_24m is None:
            certainty_inline = re.search(
                r"(?i)\bcertainty\s*24m\s*goals?\b\s*:\s*([0-9]{1,3}(?:\.[0-9]+)?)\s*%?",
                line,
            )
            if certainty_inline:
                certainty_pct_24m = _to_float(certainty_inline.group(1))
                continue
        if "|" not in line:
            bullet_match = re.match(r"^([^:]{2,50}):\s*(.+)$", line)
            if not bullet_match:
                continue
            target_period = str(bullet_match.group(1) or "").strip()
            if not _looks_like_period_prefix(target_period):
                continue
            body = str(bullet_match.group(2) or "").strip()
            status = "planned"
            status_match = re.search(r"\[\s*status\s*:\s*([^\]]+)\]", body, flags=re.IGNORECASE)
            if status_match:
                status = _normalize_status(status_match.group(1))
                body = body.replace(status_match.group(0), " ").strip()
            else:
                inline_status_match = re.search(r"(?i)\bstatus\s*:\s*([a-z _-]+)", body)
                if inline_status_match:
                    status = _normalize_status(inline_status_match.group(1))
                    body = re.sub(r"[\(\[\-–—:\s]+$", "", body[: inline_status_match.start()]).strip()
            if status == "planned":
                paren_status_match = re.search(r"\(([^()]+)\)\s*\.?$", body)
                if paren_status_match:
                    paren_bits = [bit.strip() for bit in re.split(r"[\/,;]", paren_status_match.group(1)) if bit.strip()]
                    normalized_bits = [_normalize_status(bit) for bit in paren_bits]
                    if "achieved" in normalized_bits:
                        status = "achieved"
                    elif "at_risk" in normalized_bits:
                        status = "at_risk"
                    elif "current" in normalized_bits:
                        status = "current"
                    elif "planned" in normalized_bits:
                        status = "planned"
            inferred_status = _infer_timeline_status_from_text(body)
            if inferred_status and status == "planned":
                status = inferred_status

            impact = ""
            impact_match = re.search(r"(?i)\bimpact\s*:\s*(.+)$", body)
            if impact_match:
                impact = str(impact_match.group(1) or "").strip().rstrip(".")
                body = body[: impact_match.start()].strip()

            body = re.sub(r"\s+", " ", body).strip(" .;-")
            if not body:
                continue

            row: Dict[str, Any] = {
                "milestone": body,
                "target_period": _normalize_target_period_label(target_period) or target_period,
                "status": status,
                "confidence_pct": None,
            }
            normalized_target = _normalize_target_period_label(target_period)
            if normalized_target and normalized_target != target_period:
                row["raw_target_period"] = target_period
            if impact:
                row["impact_on_24m_pw"] = impact
            rows.append(row)
            continue
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 3:
            continue

        milestone = parts[0]
        target_period = parts[1]
        status = _normalize_status(parts[2])
        confidence = _to_float(parts[3]) if len(parts) > 3 else None
        impact = str(parts[4]).strip().lower() if len(parts) > 4 else ""

        row: Dict[str, Any] = {
            "milestone": milestone,
            "target_period": _normalize_target_period_label(target_period) or target_period,
            "status": status,
            "confidence_pct": confidence,
        }
        normalized_target = _normalize_target_period_label(target_period)
        if normalized_target and normalized_target != target_period:
            row["raw_target_period"] = target_period
        if impact:
            row["impact_on_24m_pw"] = impact
        rows.append(row)

    return rows, current_stage, certainty_pct_24m


def _extract_inline_timeline_period(text: Any) -> Tuple[str, str]:
    raw = str(text or "").strip()
    if not raw:
        return "", ""

    period_pattern = re.compile(
        r"\b(Q[1-4](?:\s*[-/]\s*Q[1-4])?\s*20\d{2}|H[12]\s*20\d{2}|20\d{2}|"
        r"(?:late|mid|early)\s+[A-Za-z]{3,9}\s+20\d{2}|"
        r"[A-Za-z]{3,9}\s+20\d{2})\b",
        re.IGNORECASE,
    )

    colon_split = re.match(r"^([^:]{2,40}):\s*(.+)$", raw)
    if colon_split:
        lhs = str(colon_split.group(1) or "").strip()
        rhs = str(colon_split.group(2) or "").strip()
        if lhs and period_pattern.search(lhs):
            return lhs, rhs or raw

    period_match = period_pattern.search(raw)
    if period_match:
        period = str(period_match.group(1) or "").strip()
        stripped = re.sub(r"^[:\-\s]+", "", raw.replace(period_match.group(0), "")).strip()
        return period, stripped or raw

    return "", raw


def _derive_current_stage_from_timeline_rows(rows: Any) -> str:
    if not isinstance(rows, list):
        return ""
    statuses: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or row.get("current_status") or "").strip().lower()
        inferred_status = _infer_timeline_status_from_text(row.get("milestone") or row.get("condition"))
        if inferred_status and status in {"", "planned", "unspecified"}:
            status = inferred_status
        elif status not in {"achieved", "current", "at_risk", "planned"}:
            status = inferred_status
        if status in {"achieved", "current", "at_risk", "planned"}:
            statuses.append(status)
    for preferred in ("current", "achieved", "at_risk", "planned"):
        if preferred in statuses:
            return preferred
    return ""


def _normalize_target_period_label(value: Any) -> str:
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

    q_single = re.match(r"^Q([1-4])\s*(?:CY\s*)?(20\d{2})$", upper)
    if q_single:
        return f"Q{q_single.group(1)} {q_single.group(2)}"

    h_single = re.match(r"^H([12])\s*(?:CY\s*)?(20\d{2})$", upper)
    if h_single:
        return f"H{h_single.group(1)} {h_single.group(2)}"

    month_period = re.match(
        r"^(JAN(?:UARY)?|FEB(?:RUARY)?|MAR(?:CH)?|APR(?:IL)?|MAY|JUN(?:E)?|JUL(?:Y)?|AUG(?:UST)?|SEP(?:T(?:EMBER)?)?|OCT(?:OBER)?|NOV(?:EMBER)?|DEC(?:EMBER)?)(?:\s+Q(?:UARTER)?)?\s*(?:CY\s*)?(20\d{2})$",
        upper,
    )
    if month_period:
        month_token = month_period.group(1)[:4].replace("UARY", "").replace("RUAR", "")
        if month_token.startswith("SEPT"):
            month_token = "SEPT"
        else:
            month_token = month_token[:3]
        quarter = month_to_quarter.get(month_token)
        if quarter:
            return f"{quarter} {month_period.group(2)}"

    if re.match(r"^(EARLY|START|BEGINNING)\s+(?:CY\s*)?(20\d{2})$", upper):
        year = re.match(r"^(EARLY|START|BEGINNING)\s+(?:CY\s*)?(20\d{2})$", upper).group(2)
        return f"H1 {year}"
    if re.match(r"^MID\s+(?:CY\s*)?(20\d{2})$", upper):
        year = re.match(r"^MID\s+(?:CY\s*)?(20\d{2})$", upper).group(1)
        return f"H1 {year}"
    if re.match(r"^LATE\s+(?:CY\s*)?(20\d{2})$", upper):
        year = re.match(r"^LATE\s+(?:CY\s*)?(20\d{2})$", upper).group(1)
        return f"H2 {year}"
    if re.match(r"^END\s+(?:CY\s*)?(20\d{2})$", upper):
        year = re.match(r"^END\s+(?:CY\s*)?(20\d{2})$", upper).group(1)
        return f"Q4 {year}"

    return text


def _timeline_period_to_quarter_index(period: Any) -> Optional[int]:
    text = _normalize_target_period_label(period).strip().upper()
    if not text:
        return None

    q_range = re.search(r"\bQ([1-4])\s*[-/]\s*Q([1-4])\s*(20\d{2})\b", text)
    if q_range:
        q1 = int(q_range.group(1))
        q2 = int(q_range.group(2))
        year = int(q_range.group(3))
        return (year * 4) + max(q1, q2)

    q_single = re.search(r"\bQ([1-4])\s*(20\d{2})\b", text)
    if q_single:
        quarter = int(q_single.group(1))
        year = int(q_single.group(2))
        return (year * 4) + quarter

    half = re.search(r"\bH([12])\s*(20\d{2})\b", text)
    if half:
        h = int(half.group(1))
        year = int(half.group(2))
        quarter = 2 if h == 1 else 4
        return (year * 4) + quarter

    year_only = re.search(r"\b(20\d{2})\b", text)
    if year_only:
        year = int(year_only.group(1))
        return (year * 4) + 4

    return None


def _status_indicates_past(status: Any) -> bool:
    low = str(status or "").strip().lower()
    if not low:
        return False
    return any(
        token in low
        for token in (
            "achieved",
            "completed",
            "done",
            "delivered",
            "closed",
            "finished",
            "met",
            "launched",
            "commissioned",
            "first gold",
        )
    )


def _status_indicates_future(status: Any) -> bool:
    low = str(status or "").strip().lower()
    if not low:
        return False
    return any(
        token in low
        for token in (
            "planned",
            "at_risk",
            "at risk",
            "pending",
            "upcoming",
            "target",
            "on track",
            "on_track",
            "current",
            "in progress",
            "in_progress",
            "speculative",
            "proposed",
        )
    )


def _timeline_row_is_previous(row: Dict[str, Any], now_utc: Optional[datetime] = None) -> bool:
    status = row.get("status")
    if _status_indicates_past(status):
        return True
    if _status_indicates_future(status):
        return False

    quarter_idx = _timeline_period_to_quarter_index(
        row.get("target_period")
        or row.get("targetPeriod")
        or row.get("period")
        or row.get("date")
    )
    if quarter_idx is None:
        return False

    now = now_utc or datetime.utcnow()
    current_quarter = (now.year * 4) + (((now.month - 1) // 3) + 1)
    return quarter_idx < current_quarter


def _cap_previous_timeline_rows(
    rows: List[Dict[str, Any]],
    *,
    max_previous: int = 1,
    now_utc: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(rows, list) or max_previous < 0:
        return []
    if not rows:
        return []

    indexed: List[Tuple[int, Dict[str, Any], bool, Optional[int]]] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        is_previous = _timeline_row_is_previous(row, now_utc=now_utc)
        quarter_idx = _timeline_period_to_quarter_index(
            row.get("target_period")
            or row.get("targetPeriod")
            or row.get("period")
            or row.get("date")
        )
        indexed.append((idx, row, is_previous, quarter_idx))

    previous_rows = [item for item in indexed if item[2]]
    if len(previous_rows) <= max_previous:
        return [item[1] for item in indexed]

    previous_rows_sorted = sorted(
        previous_rows,
        key=lambda item: (
            item[3] is not None,
            item[3] if item[3] is not None else -1,
            item[0],
        ),
        reverse=True,
    )
    keep_previous_idx = {item[0] for item in previous_rows_sorted[:max_previous]}

    filtered: List[Dict[str, Any]] = []
    for idx, row, is_previous, _ in indexed:
        if not is_previous or idx in keep_previous_idx:
            filtered.append(row)
    return filtered


