"""Thesis map, watchlist, condition entries, and Stage-1 reference rows."""

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import logging

from ..openrouter import query_model
from .json_extract import _parse_json_from_text
from .text_utils import (
    _dedupe_text_list,
    _derive_positioning_basis,
    _extract_tagged_section,
    _normalize_current_positioning_value,
    _split_inline_items,
    _strip_list_prefix,
    _strip_markdown_formatting,
)
from .verdict import _to_float

logger = logging.getLogger(__name__)

def _extract_thesis_map_from_text(chairman_text: str) -> Dict[str, str]:
    """Best-effort extraction of bull/base/bear thesis summaries from chairman XML text."""
    import re

    section = _extract_tagged_section(chairman_text, "thesis_map") or str(
        chairman_text or ""
    )
    if not section.strip():
        return {}

    parsed: Dict[str, List[str]] = {"bull": [], "base": [], "bear": []}
    current: Optional[str] = None
    for raw_line in section.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line.startswith(("-", "*")):
            line = line[1:].strip()
        m = re.match(r"(?i)^(bull|base|bear)\s*:\s*(.+)$", line)
        if m:
            current = str(m.group(1)).lower().strip()
            body = str(m.group(2)).strip()
            if body:
                parsed[current].append(body)
            continue
        if current in parsed:
            parsed[current].append(line)

    out: Dict[str, str] = {}
    for key in ("bull", "base", "bear"):
        summary = " ".join(parsed.get(key) or []).strip()
        if summary:
            out[key] = summary
    return out


def _extract_headwinds_tailwinds_from_text(chairman_text: str) -> Dict[str, List[str]]:
    section = _extract_tagged_section(chairman_text, "headwinds_tailwinds")
    if not section:
        return {"quantitative": [], "qualitative": []}

    quantitative: List[str] = []
    qualitative: List[str] = []
    bucket = ""
    prefix = ""
    header_map = {
        "quantitative headwinds": ("quantitative", "Headwind"),
        "quantitative tailwinds": ("quantitative", "Tailwind"),
        "qualitative headwinds": ("qualitative", "Headwind"),
        "qualitative tailwinds": ("qualitative", "Tailwind"),
    }

    for raw_line in str(section or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        heading_candidate = re.sub(r"^[\-\*\u2022]+\s*", "", line).strip()
        lower = heading_candidate.lower().rstrip(":")
        matched_header = False
        for header, (next_bucket, next_prefix) in header_map.items():
            if lower.startswith(header):
                bucket = next_bucket
                prefix = next_prefix
                remainder = heading_candidate[len(header) :].lstrip(" :-")
                if remainder:
                    line = remainder
                else:
                    matched_header = True
                break
        if matched_header:
            continue
        line = re.sub(r"^[\-\*\u2022]+\s*", "", line).strip()
        if not line:
            continue
        if prefix:
            line = f"{prefix}: {line}"
        if bucket == "quantitative":
            quantitative.append(line)
        elif bucket == "qualitative":
            qualitative.append(line)

    return {
        "quantitative": _dedupe_text_list(quantitative, limit=6),
        "qualitative": _dedupe_text_list(qualitative, limit=6),
    }


def _thesis_embedded_label_matches(text: Any) -> List[Tuple[int, int, str]]:
    source = str(text or "")
    matches: List[Tuple[int, int, str]] = []
    for key, pattern in _THESIS_EMBEDDED_LABEL_PATTERNS:
        for match in re.finditer(rf"(?i)\b{pattern}\b\s*[:=]\s*", source):
            matches.append((match.start(), match.end(), key))
    matches.sort(key=lambda row: row[0])
    deduped: List[Tuple[int, int, str]] = []
    for row in matches:
        if deduped and row[0] < deduped[-1][1]:
            continue
        deduped.append(row)
    return deduped


def _thesis_text_looks_packed(value: Any) -> bool:
    text = _strip_list_prefix(value)
    if not text:
        return False
    labels = _thesis_embedded_label_matches(text)
    if len(labels) >= 2:
        return True
    if len(text) > 260 and labels:
        return True
    return False


def _to_float_from_text(value: Any) -> Optional[float]:
    direct = _to_float(value)
    if direct is not None:
        return direct
    text = str(value or "")
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    return _to_float(match.group(0))


def _extract_embedded_thesis_fields(value: Any) -> Dict[str, Any]:
    text = _strip_list_prefix(value)
    if not text:
        return {}
    labels = _thesis_embedded_label_matches(text)
    if not labels:
        return {}

    out: Dict[str, Any] = {}
    summary = text[: labels[0][0]].strip(" .;:-")
    if summary:
        out["summary"] = summary

    for idx, (start, end, key) in enumerate(labels):
        next_start = labels[idx + 1][0] if idx + 1 < len(labels) else len(text)
        segment = text[end:next_start].strip(" .;:-")
        if not segment:
            continue
        if key in {"target_12m", "target_24m", "probability_24m_pct"}:
            parsed = _to_float_from_text(segment)
            if parsed is not None:
                out[key] = parsed
            continue
        if key in {"required_conditions", "failure_conditions"}:
            out.setdefault(key, []).extend(_split_inline_items(segment))
            continue
        if key == "current_positioning":
            normalized = _normalize_current_positioning_value(segment)
            out[key] = normalized or segment
            basis = _derive_positioning_basis(segment)
            if basis and not out.get("why_current_positioning"):
                out["why_current_positioning"] = basis
            continue
        out[key] = segment
    return out


def _merge_embedded_thesis_fields(
    *values: Any,
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for value in values:
        extracted = _extract_embedded_thesis_fields(value)
        if not extracted:
            continue
        for key, extracted_value in extracted.items():
            if key in {"required_conditions", "failure_conditions"}:
                existing = merged.setdefault(key, [])
                if isinstance(existing, list):
                    existing.extend(extracted_value if isinstance(extracted_value, list) else [extracted_value])
                continue
            if merged.get(key) in (None, "", [], {}):
                merged[key] = extracted_value
    for key in ("required_conditions", "failure_conditions"):
        if isinstance(merged.get(key), list):
            merged[key] = _dedupe_text_list(merged.get(key), limit=6)
    return merged


def _positioning_basis_looks_polluted(value: Any) -> bool:
    text = _strip_list_prefix(value)
    if not text:
        return True
    if len(text) < 12:
        return True
    if re.search(
        r"(?i)\b(recommendation|rating|conviction|sizing|top\s*3|failure conditions?|success indicators?|decisive market mispricing|decisive failure risk)\b",
        text,
    ):
        return True
    if text.lower() in {
        "mixed toward positioning.",
        "toward positioning.",
        "to bull-leaning.",
        "to base-leaning.",
    }:
        return True
    return False


def _make_condition_item(
    text: Any,
    *,
    scenario: str,
    prefix: str,
    idx: int,
) -> Dict[str, Any]:
    condition = _strip_list_prefix(text)
    slug = re.sub(r"[^a-z0-9]+", "_", condition.lower()).strip("_")[:40]
    if not slug:
        slug = f"{prefix}_{idx}"
    return {
        "condition_id": f"{scenario}_{prefix}_{slug}",
        "condition": condition,
        "by": "",
        "trigger_window": "",
        "duration": "",
        "linked_milestones": [],
        "evidence_hooks": [],
        "current_status": "monitor",
    }


def _coerce_condition_list(
    values: Any,
    *,
    scenario: str,
    prefix: str,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, text in enumerate(_dedupe_text_list(values, limit=limit), start=1):
        out.append(_make_condition_item(text, scenario=scenario, prefix=prefix, idx=idx))
    return out


def _normalize_condition_entries(
    values: Any,
    *,
    scenario: str,
    prefix: str,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    if not isinstance(values, list):
        return []
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for idx, raw in enumerate(values, start=1):
        if isinstance(raw, dict):
            condition = _strip_list_prefix(
                raw.get("condition") or raw.get("text") or raw.get("condition_id") or ""
            )
            if not condition or _thesis_text_looks_packed(condition):
                continue
            item = dict(raw)
            item["condition"] = condition
            item.setdefault(
                "condition_id",
                _make_condition_item(condition, scenario=scenario, prefix=prefix, idx=idx)[
                    "condition_id"
                ],
            )
            item.setdefault("by", "")
            item.setdefault("trigger_window", "")
            item.setdefault("duration", "")
            item.setdefault("linked_milestones", [])
            item.setdefault("evidence_hooks", [])
            item.setdefault("current_status", "monitor")
        else:
            condition = _strip_list_prefix(raw)
            if not condition or _thesis_text_looks_packed(condition):
                continue
            item = _make_condition_item(condition, scenario=scenario, prefix=prefix, idx=idx)
        key = re.sub(r"\W+", "", str(item.get("condition") or "").lower())
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
        if len(out) >= limit:
            break
    return out


def _extract_structured_thesis_map_from_text(
    chairman_text: str,
) -> Dict[str, Dict[str, Any]]:
    section = _extract_tagged_section(chairman_text, "thesis_map")
    if not section:
        return {}

    parsed: Dict[str, Dict[str, Any]] = {"bull": {}, "base": {}, "bear": {}}
    current: Optional[str] = None
    current_list: Optional[str] = None

    def _ensure_legacy_condition(scenario: str) -> Dict[str, Any]:
        entries = parsed[scenario].setdefault("required_conditions", [])
        if entries and isinstance(entries[-1], dict):
            return entries[-1]
        item: Dict[str, Any] = {
            "condition": "",
            "by": "",
            "trigger_window": "",
            "duration": "",
            "linked_milestones": [],
            "evidence_hooks": [],
            "current_status": "monitor",
        }
        entries.append(item)
        return item

    for raw_line in section.splitlines():
        line = _strip_list_prefix(raw_line)
        if not line:
            continue

        block_match = re.match(r"(?i)^(bull|base|bear)\b\s*:?\s*(.*)$", line)
        if block_match:
            current = str(block_match.group(1)).lower().strip()
            current_list = None
            tail = str(block_match.group(2) or "").strip()
            if tail:
                parsed[current].setdefault("summary", tail)
            continue

        if current not in parsed:
            continue

        field_match = re.match(
            r"(?i)^(summary|target_12m|target_24m|probability_24m_pct|required_conditions|failure_conditions|current_positioning|why_current_positioning)\s*:\s*(.*)$",
            line,
        )
        if field_match:
            field = str(field_match.group(1)).lower().strip()
            body = str(field_match.group(2) or "").strip()
            if field in {"required_conditions", "failure_conditions"}:
                current_list = field
                if body:
                    parsed[current].setdefault(field, []).extend(_split_inline_items(body))
            else:
                current_list = None
                if field in {"target_12m", "target_24m", "probability_24m_pct"}:
                    parsed[current][field] = _to_float(body)
                else:
                    parsed[current][field] = body
            continue

        legacy_field_match = re.match(
            r"(?i)^(condition|deadline|target windows?|evidence hooks?|status|current status)\s*:\s*(.*)$",
            line,
        )
        if legacy_field_match:
            field = str(legacy_field_match.group(1) or "").strip().lower()
            body = str(legacy_field_match.group(2) or "").strip()
            item = _ensure_legacy_condition(current)
            current_list = None
            if field == "condition":
                item["condition"] = body
            elif field == "deadline":
                item["by"] = body
            elif field.startswith("target window"):
                item["trigger_window"] = body
            elif field.startswith("evidence hook"):
                item["evidence_hooks"] = _split_inline_items(body) or ([body] if body else [])
            else:
                item["current_status"] = body or "monitor"
            continue

        if current_list in {"required_conditions", "failure_conditions"}:
            parsed[current].setdefault(current_list, []).extend(_split_inline_items(line))
            continue

        existing_summary = str(parsed[current].get("summary") or "").strip()
        parsed[current]["summary"] = f"{existing_summary} {line}".strip()

    return parsed


def _guess_field_name_from_text(value: Any) -> str:
    text = _strip_list_prefix(value)
    if not text:
        return ""
    if ":" in text:
        left, right = text.split(":", 1)
        left = left.strip()
        if left and len(left.split()) <= 6 and len(left) <= 48 and right.strip():
            return left
    heuristics = [
        (r"\b(?:global|consolidated|total)\s+.*?\b(?:mre|mineral resource estimate|resource)\b", "Global Mineral Resource Estimate"),
        (r"\b(?:standalone\s+)?aisc\b", "Standalone AISC Guidance"),
        (r"\blom\b|\blife[- ]of[- ]mine\b", "Life-of-Mine Schedule"),
        (r"\bshare(?:s| count| capital)?\b", "Shares Outstanding"),
        (r"\bcapex\b", "Capex"),
        (r"\bcash\s+flow\b", "Cash Flow"),
        (r"\brevenue\b", "Revenue"),
        (r"\bdebt\b", "Debt"),
        (r"\bcash\b", "Cash"),
        (r"\bapproval\b|\bpermit\b", "Permitting / Approval Status"),
    ]
    low = text.lower()
    for pattern, label in heuristics:
        if re.search(pattern, low, re.IGNORECASE):
            return label
    return ""


def _extract_verification_queue_from_text(chairman_text: str) -> List[Dict[str, str]]:
    section = _extract_tagged_section(chairman_text, "verification_queue")
    if not section:
        return []

    items: List[Dict[str, str]] = []
    current: Dict[str, str] = {}

    def _commit_current() -> None:
        if not current:
            return
        field = str(current.get("field") or "").strip() or _guess_field_name_from_text(
            current.get("reason") or ""
        )
        reason = str(current.get("reason") or "").strip()
        if not field and not reason:
            current.clear()
            return
        priority = str(current.get("priority") or "medium").strip().lower()
        if priority not in {"high", "medium", "low"}:
            priority = "medium"
        items.append(
            {
                "field": field or "Unresolved item",
                "reason": reason or "High-impact unresolved item from chairman synthesis.",
                "priority": priority,
                "required_source": str(
                    current.get("required_source")
                    or "Primary filing / latest company update"
                ).strip(),
            }
        )
        current.clear()

    for raw_line in section.splitlines():
        stripped = str(raw_line or "").strip()
        if not stripped:
            _commit_current()
            continue
        line = _strip_list_prefix(stripped)
        field_match = re.match(
            r"(?i)^(field|reason|priority|required_source)\s*:\s*(.+)$",
            line,
        )
        if field_match:
            key = str(field_match.group(1)).lower().strip()
            value = str(field_match.group(2) or "").strip()
            if key == "field" and current.get("field"):
                _commit_current()
            current[key] = value
            continue
        if current.get("reason"):
            current["reason"] = f"{current['reason']} {line}".strip()
        else:
            current["reason"] = line

    _commit_current()
    return items


def _extract_data_gap_verification_items(chairman_text: str) -> List[Dict[str, str]]:
    section = _extract_tagged_section(chairman_text, "data_gaps_and_assumptions")
    if not section:
        return []

    items: List[Dict[str, str]] = []
    for raw_line in section.splitlines():
        stripped = str(raw_line or "").strip()
        if not stripped:
            continue
        line = _strip_list_prefix(stripped)
        low = line.lower()
        if not any(
            token in low
            for token in (
                "not disclosed",
                "estimate",
                "estimated",
                "unverified",
                "unclear",
                "missing",
                "not provided",
                "assumption",
                "verify",
                "verification",
            )
        ):
            continue
        field_name = _guess_field_name_from_text(line)
        if not field_name:
            continue
        priority = (
            "high"
            if any(
                token in low
                for token in (
                    "cash",
                    "debt",
                    "shares",
                    "share count",
                    "capex",
                    "revenue",
                    "ebitda",
                    "cash flow",
                    "financing",
                    "dilution",
                    "resource",
                    "guidance",
                    "approval",
                    "trial",
                )
            )
            else "medium"
        )
        items.append(
            {
                "field": field_name,
                "reason": line,
                "priority": priority,
                "required_source": "Primary filing / latest company update",
            }
        )
    return items[:6]


def _normalize_verification_queue_entries(values: Any) -> List[Dict[str, str]]:
    if not isinstance(values, list):
        return []
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for item in values:
        if isinstance(item, dict):
            field = str(item.get("field") or item.get("field_path") or "").strip()
            reason = str(item.get("reason") or "").strip()
            required_source = str(
                item.get("required_source") or "Primary filing / latest company update"
            ).strip()
            priority = str(item.get("priority") or "medium").strip().lower()
        else:
            field = str(item or "").strip()
            reason = ""
            required_source = "Primary filing / latest company update"
            priority = "medium"

        if not field or field.lower().startswith("missing data includes"):
            field = _guess_field_name_from_text(reason)
        if not field:
            continue
        if priority not in {"high", "medium", "low"}:
            priority = "medium"
        key = re.sub(r"[^a-z0-9]+", "", field.lower())
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "field": field,
                "reason": reason or "High-impact unresolved item from chairman synthesis.",
                "priority": priority,
                "required_source": required_source,
            }
        )
    return out


def _slugify_identifier(value: Any, *, fallback: str) -> str:
    text = _strip_list_prefix(value).lower()
    slug = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return slug[:64] or fallback


def _extract_trigger_window_from_text(value: Any) -> str:
    text = _strip_list_prefix(value)
    if not text:
        return ""
    patterns = [
        r"\b(?:Q[1-4](?:\s*[-/]\s*Q[1-4])?|H[12]|CY)\s*20\d{2}\b",
        r"\b(?:early|mid|late)[-\s]20\d{2}\b",
        r"\b(?:early|mid|late)\s+20\d{2}\b",
        r"\b(?:mid|late|early)[-\s]?\d{4}\b",
        r"\b(?:Q[1-4]|H[12])\s+\d{4}\b",
        r"\b(?:within|over|under)\s+\d{1,2}\s*(?:m|months?)\b",
        r"\b\d{1,2}\s*(?:m|months?)\b",
        r"\b20\d{2}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return str(match.group(0) or "").strip(" .,:;")
    return ""


def _infer_watch_source_to_monitor(title: str, why_it_matters: str = "") -> str:
    text = f"{title} {why_it_matters}".lower()
    if any(token in text for token in ("debt", "financing", "facility", "syndicate", "dilution", "credit committee", "bank")):
        return "Company financing announcements and lender updates"
    if any(token in text for token in ("warden", "court", "injunction", "lease")):
        return "WA Mining Warden / court records and company ASX disclosures"
    if any(token in text for token in ("epa", "permit", "approval", "ministerial", "flora")):
        return "WA EPA / Ministerial approvals and company ASX disclosures"
    if any(token in text for token in ("mre", "resource", "reserve", "drill", "grade")):
        return "Company drill results, resource updates, and technical reports"
    if "gold price" in text or "spot" in text:
        return "Company guidance and commodity price monitoring"
    return "Company filings and milestone updates"


def _infer_watch_why_it_matters(
    title: str,
    *,
    kind: str,
    source_to_monitor: str = "",
) -> str:
    text = _strip_list_prefix(title)
    low = text.lower()
    paren_match = re.search(r"\(([^()]{3,120})\)\s*\.?$", text)
    if paren_match:
        return str(paren_match.group(1) or "").strip().rstrip(".") + "."
    if "," in text:
        tail = text.split(",", 1)[1].strip()
        if tail:
            return tail.rstrip(".") + "."
    if "debt" in low or "credit committee" in low or "facility" in low or "mandate" in low:
        return "Confirms project bankability and reduces the risk of highly dilutive equity funding."
    if "warden" in low or "court" in low or "plaint" in low:
        if "dismiss" in low or "settled" in low:
            return "Removes the legal land-access overhang from the mining leases."
        return "Would block land access and threaten the FID timeline."
    if "epa" in low or "ministerial" in low or "permit" in low or "approval" in low:
        if kind == "confirmatory_signals":
            return "Clears a key regulatory hurdle required for construction and financing."
        return "Would delay approvals and push FID further out."
    if "mre" in low or "resource" in low or "reserve" in low:
        return "Would improve mine life, project flexibility, and valuation support."
    if "capex" in low or "epc" in low or "inflation" in low:
        return "Would erode project NPV and widen the external funding requirement."
    if "gold" in low or "spot" in low:
        return "Would materially change project cash-flow sensitivity and valuation support."
    if "fid" in low:
        return "Would signal slippage on the core de-risking milestone for the project."
    if "equity raise" in low or "dilution" in low:
        return "Would indicate debt markets are not carrying enough of the build and would dilute per-share upside."
    if source_to_monitor.lower().startswith("company financing"):
        return "Directly affects the funding mix, dilution risk, and probability of hitting FID on time."
    if kind == "red_flags":
        return "Would weaken the base-case de-risking path and increase downside probability."
    return "Would support or challenge the current base-case de-risking path."


def _infer_watch_priority(kind: str, title: str, why_it_matters: str = "") -> str:
    text = f"{title} {why_it_matters}".lower()
    if kind == "red_flags":
        return "high"
    if any(
        token in text
        for token in ("fid", "debt", "financing", "approval", "permit", "warden", "mre")
    ):
        return "high"
    return "medium"


def _normalize_watchlist_object(
    item: Any,
    *,
    kind: str,
    fallback: Optional[Dict[str, Any]] = None,
    idx: int = 1,
) -> Optional[Dict[str, Any]]:
    fallback = fallback or {}
    if isinstance(item, dict):
        title = _strip_list_prefix(
            item.get("item")
            or item.get("condition")
            or item.get("label")
            or item.get("title")
            or fallback.get("item")
            or ""
        )
        why_it_matters = _strip_list_prefix(
            item.get("why_it_matters")
            or item.get("reason")
            or item.get("evidence_hook")
            or fallback.get("why_it_matters")
            or ""
        )
        watch_id = str(
            item.get("watch_id") or fallback.get("watch_id") or ""
        ).strip()
        trigger_window = _strip_list_prefix(
            item.get("trigger_window") or fallback.get("trigger_window") or ""
        )
        duration = _strip_list_prefix(item.get("duration") or fallback.get("duration") or "")
        priority = str(item.get("priority") or fallback.get("priority") or "").strip().lower()
        source_to_monitor = _strip_list_prefix(
            item.get("source_to_monitor") or fallback.get("source_to_monitor") or ""
        )
        severity = _strip_list_prefix(item.get("severity") or fallback.get("severity") or "")
    else:
        text = _strip_list_prefix(item)
        title, sep, why = text.partition(":")
        title = _strip_list_prefix(title)
        why_it_matters = (
            _strip_list_prefix(why)
            if sep
            else str(fallback.get("why_it_matters") or "").strip()
        )
        watch_id = str(fallback.get("watch_id") or "").strip()
        trigger_window = str(fallback.get("trigger_window") or "").strip()
        duration = str(fallback.get("duration") or "").strip()
        priority = str(fallback.get("priority") or "").strip().lower()
        source_to_monitor = str(fallback.get("source_to_monitor") or "").strip()
        severity = str(fallback.get("severity") or "").strip()
    if not title:
        return None
    if not trigger_window:
        trigger_window = _extract_trigger_window_from_text(f"{title} {why_it_matters}")
    if not source_to_monitor:
        source_to_monitor = _infer_watch_source_to_monitor(title, why_it_matters)
    if not why_it_matters:
        why_it_matters = _infer_watch_why_it_matters(
            title,
            kind=kind,
            source_to_monitor=source_to_monitor,
        )
    if priority not in {"high", "medium", "low"}:
        priority = _infer_watch_priority(kind, title, why_it_matters)
    if not severity:
        severity = "high" if kind == "red_flags" else "medium"
    if not watch_id:
        watch_id = _slugify_identifier(title, fallback=f"{kind}_{idx}")
    return {
        "watch_id": watch_id,
        "item": title,
        "condition": title,
        "why_it_matters": why_it_matters,
        "evidence_hook": why_it_matters,
        "source_to_monitor": source_to_monitor,
        "trigger_window": trigger_window,
        "duration": duration,
        "priority": priority,
        "severity": severity,
    }


def _extract_monitoring_watchlist_from_text(chairman_text: str) -> Dict[str, List[Dict[str, Any]]]:
    section = _extract_tagged_section(chairman_text, "monitoring_watchlist")
    out: Dict[str, List[Dict[str, Any]]] = {
        "confirmatory_signals": [],
        "red_flags": [],
    }
    if not section:
        return out

    current_bucket: Optional[str] = None
    for raw_line in section.splitlines():
        line = _strip_list_prefix(raw_line)
        if not line:
            continue
        lower = line.lower()
        if re.match(r"(?i)^confirmatory\s+signals?\s*:?\s*$", line):
            current_bucket = "confirmatory_signals"
            continue
        if re.match(r"(?i)^red\s+flags?\s*:?\s*$", line):
            current_bucket = "red_flags"
            continue
        if current_bucket not in out:
            continue
        item = _normalize_watchlist_object(
            line,
            kind=current_bucket,
            idx=len(out[current_bucket]) + 1,
        )
        if item:
            out[current_bucket].append(item)
    return out


def _watchlist_lookup(entries: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for entry in entries or []:
        key = _slugify_identifier(entry.get("item") or entry.get("condition") or "", fallback="")
        if key:
            lookup[key] = entry
    return lookup


_WATCH_MATCH_STOPWORDS = {
    "with",
    "from",
    "that",
    "this",
    "into",
    "than",
    "over",
    "under",
    "without",
    "minor",
    "major",
    "cleanly",
    "typical",
    "development",
    "project",
    "market",
    "support",
    "supporting",
    "current",
    "evidence",
}


def _keyword_set(value: Any) -> set[str]:
    tokens = {
        token.lower()
        for token in re.findall(r"[A-Za-z][A-Za-z0-9+/-]*", _strip_list_prefix(value))
    }
    return {
        token
        for token in tokens
        if len(token) >= 3 and token not in _WATCH_MATCH_STOPWORDS
    }


def _best_watchlist_matches(
    condition_text: str,
    entries: List[Dict[str, Any]],
    *,
    limit: int = 2,
) -> List[Dict[str, Any]]:
    condition_keywords = _keyword_set(condition_text)
    if not condition_keywords:
        return []
    scored: List[Tuple[int, Dict[str, Any]]] = []
    for entry in entries or []:
        combined = " ".join(
            [
                str(entry.get("item") or ""),
                str(entry.get("condition") or ""),
                str(entry.get("why_it_matters") or ""),
            ]
        )
        overlap = condition_keywords & _keyword_set(combined)
        score = len(overlap)
        if score <= 0:
            continue
        scored.append((score, entry))
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return [entry for _, entry in scored[:limit]]


def _enrich_condition_item(
    item: Dict[str, Any],
    *,
    scenario: str,
    condition_kind: str,
    confirmatory_signals: List[Dict[str, Any]],
    red_flags: List[Dict[str, Any]],
) -> Dict[str, Any]:
    condition_text = _strip_list_prefix(item.get("condition") or "")
    if not condition_text:
        return item

    if not str(item.get("by") or "").strip():
        by_match = re.search(
            r"(?i)\bby\s+((?:Q[1-4]|H[12]|CY)\s*20\d{2}|(?:early|mid|late)[-\s]20\d{2}|20\d{2})\b",
            condition_text,
        )
        if by_match:
            item["by"] = str(by_match.group(1) or "").strip()

    if not str(item.get("trigger_window") or "").strip():
        item["trigger_window"] = _extract_trigger_window_from_text(condition_text)

    combined_watchlist = list(confirmatory_signals or []) + list(red_flags or [])
    matches = _best_watchlist_matches(condition_text, combined_watchlist)

    evidence_hooks = item.get("evidence_hooks")
    if not isinstance(evidence_hooks, list):
        evidence_hooks = []
    if not evidence_hooks and matches:
        evidence_hooks = [
            f"{match.get('item')}: {match.get('why_it_matters')}".rstrip(": ").strip()
            for match in matches
            if str(match.get("item") or "").strip()
        ]
    item["evidence_hooks"] = _dedupe_text_list(evidence_hooks, limit=3)

    current_status = _strip_list_prefix(item.get("current_status") or "")
    if not current_status or current_status.lower() == "monitor":
        matched_red = any(match in (red_flags or []) for match in matches)
        matched_confirm = any(match in (confirmatory_signals or []) for match in matches)
        lower = condition_text.lower()
        if matched_red or condition_kind == "failure" or scenario == "bear":
            current_status = "at-risk"
        elif matched_confirm or any(
            token in lower
            for token in ("approval", "mre", "resource", "debt", "financing", "fid", "reserve")
        ):
            current_status = "developing"
        else:
            current_status = "monitor"
    item["current_status"] = current_status

    priority = str(item.get("priority") or "").strip().lower()
    if priority not in {"high", "medium", "low"}:
        if condition_kind == "failure" or scenario == "bear":
            priority = "high"
        elif item.get("trigger_window"):
            priority = "high"
        else:
            priority = "medium"
        item["priority"] = priority

    return item


def _extract_score_total(value: Any) -> Optional[float]:
    """Extract score total from scalar or nested score dict."""
    if isinstance(value, dict):
        for key in ("total", "score", "value"):
            parsed = _to_float(value.get(key))
            if parsed is not None:
                return parsed
        return None
    return _to_float(value)


def _extract_score_from_text(text: str, label: str) -> Optional[float]:
    """Best-effort extraction of quality/value score from free-form Stage 1 text."""
    raw = str(text or "")
    candidates: List[float] = []

    for match in re.finditer(
        rf'(?i)"{label}[_\s-]*score"\s*:\s*([0-9]{{1,3}}(?:\.[0-9]+)?)',
        raw,
    ):
        value = _to_float(match.group(1))
        if value is not None and 0 <= value <= 100:
            candidates.append(value)

    for match in re.finditer(
        rf"(?i)\b{label}\s*score\b[^\n]{{0,240}}?([0-9]{{1,3}}(?:\.[0-9]+)?)\s*/\s*100\b",
        raw,
    ):
        value = _to_float(match.group(1))
        if value is not None and 0 <= value <= 100:
            candidates.append(value)

    return candidates[-1] if candidates else None


def _extract_numeric_target_from_text(text: str, key: str) -> Optional[float]:
    """Extract direct numeric target key from text/JSON-ish output."""
    raw = str(text or "")
    match = re.search(
        rf'(?i)"{key}"\s*:\s*(?:A\$|\$)?\s*([0-9]+(?:,[0-9]{{3}})*(?:\.[0-9]+)?)',
        raw,
    )
    if not match:
        match = re.search(
            rf"(?i)\b{key}\b\s*[:=]\s*(?:A\$|\$)?\s*([0-9]+(?:,[0-9]{{3}})*(?:\.[0-9]+)?)",
            raw,
        )
    if not match:
        return None
    return _to_float(match.group(1).replace(",", ""))


def _extract_stage1_scenario_targets_from_text(
    text: str,
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Conservative text parser for Stage 1 scenario targets.

    Intentionally ignores commodity-price driver lines (e.g., gold > A$4,800/oz).
    """
    out: Dict[str, Dict[str, Optional[float]]] = {
        "12m": {"base": None, "bull": None, "bear": None},
        "24m": {"base": None, "bull": None, "bear": None},
    }
    current_horizon: Optional[str] = None

    for raw_line in str(text or "").splitlines():
        line = re.sub(r"[*_`]+", "", str(raw_line or "")).strip()
        if not line:
            continue
        lower = line.lower()

        if re.search(r"\b12[\s-]*month\b|\b12m\b", lower):
            current_horizon = "12m"
            continue
        if re.search(r"\b24[\s-]*month\b|\b24m\b", lower):
            current_horizon = "24m"
            continue
        if not current_horizon:
            continue

        scenario_match = re.search(r"\b(base|bull|bear)\b", lower)
        if not scenario_match:
            continue
        scenario = scenario_match.group(1).lower()

        # Enforce target-like lines, not driver bullets.
        if not ("target" in lower or "case" in lower or "scenario" in lower):
            continue

        non_price_tokens = (
            "capex",
            "aisc",
            "npv",
            "irr",
            "market cap",
            "enterprise value",
            "ev/",
            "ev per",
            "resource",
            "reserve",
            "cash flow",
            "free cash flow",
        )
        if any(token in lower for token in non_price_tokens) and not (
            "price target" in lower or "share price" in lower
        ):
            continue

        value_match = re.search(
            r"(?:A\$|\$|USD\s*)\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)",
            line,
            flags=re.IGNORECASE,
        )
        if not value_match:
            continue

        right_tail = lower[value_match.end(): value_match.end() + 10]
        if "oz" in right_tail:
            # Commodity prices are not equity price targets.
            continue

        value = _to_float(value_match.group(1).replace(",", ""))
        if value is None:
            continue
        if value > 1000:
            continue

        out[current_horizon][scenario] = value

    return out


def _extract_stage1_reference_rows_heuristic(stage1_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build compact per-model score/target rows for reference table."""
    rows: List[Dict[str, Any]] = []
    for item in stage1_results or []:
        model = str(item.get("model") or "").strip() or "unknown"
        response_text = str(item.get("response") or "")
        parsed_obj, _ = _parse_json_from_text(response_text)

        quality: Optional[float] = None
        value: Optional[float] = None
        target_12m: Optional[float] = None
        target_24m: Optional[float] = None
        horizon_targets: Dict[str, Dict[str, Optional[float]]] = {
            "12m": {"base": None, "bull": None, "bear": None},
            "24m": {"base": None, "bull": None, "bear": None},
        }

        if isinstance(parsed_obj, dict):
            quality = _extract_score_total(parsed_obj.get("quality_score"))
            value = _extract_score_total(parsed_obj.get("value_score"))
            price_targets = parsed_obj.get("price_targets") or {}
            if isinstance(price_targets, dict):
                target_12m = _to_float(price_targets.get("target_12m"))
                target_24m = _to_float(price_targets.get("target_24m"))
                scenario_targets = price_targets.get("scenario_targets") or {}
                if isinstance(scenario_targets, dict):
                    for horizon in ("12m", "24m"):
                        map_obj = scenario_targets.get(horizon) or {}
                        if isinstance(map_obj, dict):
                            for scenario in ("base", "bull", "bear"):
                                horizon_targets[horizon][scenario] = _to_float(
                                    map_obj.get(scenario)
                                )
                scenarios_12m = price_targets.get("scenarios") or {}
                if isinstance(scenarios_12m, dict):
                    for scenario in ("base", "bull", "bear"):
                        if horizon_targets["12m"][scenario] is None:
                            horizon_targets["12m"][scenario] = _to_float(
                                scenarios_12m.get(scenario)
                            )

        if quality is None:
            quality = _extract_score_from_text(response_text, "quality")
        if value is None:
            value = _extract_score_from_text(response_text, "value")

        extracted_targets = _extract_stage1_scenario_targets_from_text(response_text)
        for horizon in ("12m", "24m"):
            for scenario in ("base", "bull", "bear"):
                if horizon_targets[horizon][scenario] is None:
                    horizon_targets[horizon][scenario] = _to_float(
                        extracted_targets.get(horizon, {}).get(scenario)
                    )

        if target_12m is None:
            target_12m = horizon_targets["12m"]["base"]
        if target_24m is None:
            target_24m = horizon_targets["24m"]["base"]
        if target_12m is None:
            target_12m = _extract_numeric_target_from_text(response_text, "target_12m")
        if target_24m is None:
            target_24m = _extract_numeric_target_from_text(response_text, "target_24m")

        # If a model provides only a single 12M/24M target, treat it as Base.
        if horizon_targets["12m"]["base"] is None and target_12m is not None:
            horizon_targets["12m"]["base"] = target_12m
        if horizon_targets["24m"]["base"] is None and target_24m is not None:
            horizon_targets["24m"]["base"] = target_24m

        rows.append(
            {
                "model": model,
                "quality_score": quality,
                "value_score": value,
                "target_12m": target_12m,
                "target_24m": target_24m,
                "targets_12m": horizon_targets["12m"],
                "targets_24m": horizon_targets["24m"],
            }
        )
    return rows


def _coerce_stage1_reference_row(row: Dict[str, Any], model: str) -> Dict[str, Any]:
    """Normalize stage1 reference row shape and numeric coercion."""
    out = {
        "model": str(model or row.get("model") or "unknown"),
        "quality_score": _to_float(row.get("quality_score")),
        "value_score": _to_float(row.get("value_score")),
        "target_12m": _to_float(row.get("target_12m")),
        "target_24m": _to_float(row.get("target_24m")),
        "targets_12m": {"base": None, "bull": None, "bear": None},
        "targets_24m": {"base": None, "bull": None, "bear": None},
    }

    for horizon in ("12m", "24m"):
        key = f"targets_{horizon}"
        source = row.get(key) if isinstance(row.get(key), dict) else {}
        target_map = out[key]
        for scenario in ("base", "bull", "bear"):
            target_map[scenario] = _to_float(source.get(scenario))

    if out["target_12m"] is None:
        out["target_12m"] = out["targets_12m"]["base"]
    if out["target_24m"] is None:
        out["target_24m"] = out["targets_24m"]["base"]
    if out["targets_12m"]["base"] is None and out["target_12m"] is not None:
        out["targets_12m"]["base"] = out["target_12m"]
    if out["targets_24m"]["base"] is None and out["target_24m"] is not None:
        out["targets_24m"]["base"] = out["target_24m"]

    return out


def _merge_stage1_reference_rows(
    *,
    base_row: Dict[str, Any],
    parsed_row: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge parser output onto heuristic baseline, preferring parser when explicit."""
    if not parsed_row:
        return base_row
    merged = dict(base_row)

    for field in ("quality_score", "value_score", "target_12m", "target_24m"):
        parsed_value = _to_float(parsed_row.get(field))
        if parsed_value is not None:
            merged[field] = parsed_value

    for horizon in ("12m", "24m"):
        key = f"targets_{horizon}"
        base_map = merged.get(key) if isinstance(merged.get(key), dict) else {}
        parsed_map = parsed_row.get(key) if isinstance(parsed_row.get(key), dict) else {}
        out_map = {"base": _to_float(base_map.get("base")), "bull": _to_float(base_map.get("bull")), "bear": _to_float(base_map.get("bear"))}
        for scenario in ("base", "bull", "bear"):
            parsed_val = _to_float(parsed_map.get(scenario))
            if parsed_val is not None:
                out_map[scenario] = parsed_val
        merged[key] = out_map

    # Keep single-target compatibility.
    if _to_float(merged.get("target_12m")) is None:
        merged["target_12m"] = _to_float((merged.get("targets_12m") or {}).get("base"))
    if _to_float(merged.get("target_24m")) is None:
        merged["target_24m"] = _to_float((merged.get("targets_24m") or {}).get("base"))
    if _to_float((merged.get("targets_12m") or {}).get("base")) is None and _to_float(merged.get("target_12m")) is not None:
        merged["targets_12m"]["base"] = _to_float(merged.get("target_12m"))
    if _to_float((merged.get("targets_24m") or {}).get("base")) is None and _to_float(merged.get("target_24m")) is not None:
        merged["targets_24m"]["base"] = _to_float(merged.get("target_24m"))

    return merged


async def _parse_stage1_reference_row_with_model(
    *,
    parser_model: str,
    timeout_seconds: float,
    max_output_tokens: int,
    stage1_model_name: str,
    stage1_response_text: str,
) -> Optional[Dict[str, Any]]:
    """Use a small model to extract stage1 score/target fields robustly."""
    from ..openrouter import query_model

    prompt = f"""You are a strict extraction engine.
Extract structured score/target fields from one model response.
Use ONLY explicit values in the input. Do not infer missing numbers.

INPUT MODEL:
{stage1_model_name}

OUTPUT JSON SCHEMA:
{{
  "quality_score": number|null,
  "value_score": number|null,
  "target_12m": number|null,
  "target_24m": number|null,
  "targets_12m": {{"base": number|null, "bull": number|null, "bear": number|null}},
  "targets_24m": {{"base": number|null, "bull": number|null, "bear": number|null}}
}}

Rules:
- Treat only EQUITY price targets as targets (ignore commodity prices like A$/oz, AISC, NPV, capex).
- Handle prose, markdown tables, and bullets.
- If only one 12m target is present with no bull/bear split, set it as 12m base.
- If only one 24m target is present with no bull/bear split, set it as 24m base.
- Return null where absent.
- Output ONLY JSON.

MODEL RESPONSE TO PARSE:
{stage1_response_text}
"""

    response = await query_model(
        parser_model,
        [{"role": "user", "content": prompt}],
        timeout=float(timeout_seconds),
        max_tokens=(int(max_output_tokens) if int(max_output_tokens) > 0 else None),
    )
    if not response:
        return None
    parsed_obj, _ = _parse_json_from_text(str(response.get("content") or ""))
    if not isinstance(parsed_obj, dict):
        return None
    return _coerce_stage1_reference_row(parsed_obj, stage1_model_name)


async def _extract_stage1_reference_rows(stage1_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build compact per-model score/target rows using model parser first, regex fallback."""
    from ..config import (
        STAGE1_REFERENCE_PARSER_ENABLED,
        STAGE1_REFERENCE_PARSER_MODEL,
        STAGE1_REFERENCE_PARSER_TIMEOUT_SECONDS,
        STAGE1_REFERENCE_PARSER_MAX_OUTPUT_TOKENS,
        STAGE1_REFERENCE_PARSER_CONCURRENCY,
    )

    baseline_rows = _extract_stage1_reference_rows_heuristic(stage1_results)
    baseline_by_model = {
        str(row.get("model") or "unknown"): _coerce_stage1_reference_row(
            row, str(row.get("model") or "unknown")
        )
        for row in baseline_rows
    }

    if not STAGE1_REFERENCE_PARSER_ENABLED:
        return list(baseline_by_model.values())

    semaphore = asyncio.Semaphore(max(1, int(STAGE1_REFERENCE_PARSER_CONCURRENCY)))
    parsed_rows: Dict[str, Dict[str, Any]] = {}

    async def _parse_one(item: Dict[str, Any]) -> None:
        model = str(item.get("model") or "").strip() or "unknown"
        response_text = str(item.get("response") or "")
        if not response_text.strip():
            return
        async with semaphore:
            parsed = await _parse_stage1_reference_row_with_model(
                parser_model=STAGE1_REFERENCE_PARSER_MODEL,
                timeout_seconds=float(STAGE1_REFERENCE_PARSER_TIMEOUT_SECONDS),
                max_output_tokens=int(STAGE1_REFERENCE_PARSER_MAX_OUTPUT_TOKENS),
                stage1_model_name=model,
                stage1_response_text=response_text,
            )
        if parsed:
            parsed_rows[model] = parsed

    await asyncio.gather(*[_parse_one(item) for item in (stage1_results or [])])

    merged: List[Dict[str, Any]] = []
    for item in (stage1_results or []):
        model = str(item.get("model") or "").strip() or "unknown"
        base_row = baseline_by_model.get(model) or _coerce_stage1_reference_row({}, model)
        parsed_row = parsed_rows.get(model)
        merged.append(_merge_stage1_reference_rows(base_row=base_row, parsed_row=parsed_row))

    return merged

