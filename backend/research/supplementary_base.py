"""Shared helpers and metadata structures for sector supplementary pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class SupplementaryPipelineSpec:
    pipeline_id: str
    asset_class: str
    industry_label: str
    template_ids: List[str]
    family_ids: List[str]
    checklist_items: List[str]
    segment_definitions: List[Dict[str, Any]]
    category_order: List[str]
    category_schema_blocks: Dict[str, str]
    discovery_prompt_template: str
    segment_extraction_prompt_template: str
    targeted_repair_prompt_template: str
    contamination_review_prompt_template: str


def normalize_text(value: str) -> str:
    text = re.sub(r"\[[^\]]+\]", "", str(value or ""))
    text = text.replace("—", "-").replace("–", "-")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def normalize_exchange_code(value: Optional[str]) -> str:
    text = str(value or "").strip().upper()
    return re.sub(r"[^A-Z0-9]+", "", text)


def normalize_ticker_symbol(value: Optional[str]) -> str:
    raw = str(value or "").strip().upper()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    raw = re.sub(r"\.[A-Z]{1,6}$", "", raw)
    return raw.strip().upper()


def resolve_company_context(
    *,
    user_query: str = "",
    ticker: str = "",
    company: str = "",
    exchange: str = "",
    template_id: str = "",
    company_type: str = "",
) -> Dict[str, str]:
    """Resolve canonical company/exchange/ticker context from pipeline inputs."""
    from ..template_loader import get_template_loader

    loader = get_template_loader()
    ticker_raw = str(ticker or "").strip().upper()
    query_seed = str(user_query or "").strip() or str(company or "").strip() or ticker_raw
    selection = loader.resolve_template_selection(
        query_seed,
        ticker=ticker_raw,
        explicit_template_id=str(template_id or "").strip() or None,
        company_type=str(company_type or "").strip() or None,
        exchange=str(exchange or "").strip() or None,
    )

    selected_template_id = str(template_id or selection.get("template_id") or "").strip()
    selected_company_type = str(company_type or selection.get("company_type") or "").strip()
    exchange_code = normalize_exchange_code(
        exchange
        or selection.get("exchange")
        or (ticker_raw.split(":", 1)[0] if ":" in ticker_raw else "")
    )
    ticker_symbol = normalize_ticker_symbol(ticker_raw)

    selected_company_name = str(company or "").strip()
    if not selected_company_name:
        selected_company_name = str(selection.get("company_name") or "").strip()
    if not selected_company_name:
        selected_company_name = loader.infer_company_name(query_seed, ticker=ticker_raw).strip()
    if not selected_company_name:
        selected_company_name = ticker_symbol or "the company"

    contract = loader.get_template_contract(selected_template_id) if selected_template_id else {}
    family_id = str((contract or {}).get("family") or "").strip()
    pipeline_id = str((contract or {}).get("supplementary_pipeline_id") or "").strip()

    return {
        "exchange": exchange_code,
        "ticker_symbol": ticker_symbol,
        "display_ticker": f"{exchange_code}:{ticker_symbol}" if exchange_code and ticker_symbol else ticker_symbol,
        "company": selected_company_name,
        "template_id": selected_template_id,
        "company_type": selected_company_type,
        "family": family_id,
        "supplementary_pipeline_id": pipeline_id,
    }


def repair_jsonish(text: str) -> str:
    out: List[str] = []
    in_str = False
    escape = False
    for ch in str(text or ""):
        if in_str:
            if escape:
                out.append(ch)
                escape = False
                continue
            if ch == "\\":
                out.append(ch)
                escape = True
                continue
            if ch == '"':
                out.append(ch)
                in_str = False
                continue
            if ch in "\r\n\t":
                out.append(" ")
            else:
                out.append(ch)
        else:
            out.append(ch)
            if ch == '"':
                in_str = True
    return re.sub(r"\s+", " ", "".join(out)).strip()


def extract_json_payload(text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    raw = str(text or "").strip()
    if not raw:
        return None, "empty_response"
    if raw.startswith("```"):
        first_nl = raw.find("\n")
        last_fence = raw.rfind("```")
        if first_nl != -1 and last_fence != -1 and last_fence > first_nl:
            raw = raw[first_nl + 1 : last_fence].strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end < start:
        return None, "no_json_object_found"
    raw = raw[start : end + 1]
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None, None if isinstance(parsed, dict) else "json_not_object"
    except Exception:
        repaired = repair_jsonish(raw)
        try:
            parsed = json.loads(repaired)
            return parsed if isinstance(parsed, dict) else None, None if isinstance(parsed, dict) else "json_not_object"
        except Exception as exc:
            return None, f"json_parse_error:{type(exc).__name__}:{exc}"


def canonicalize_checklist_name(
    value: str,
    *,
    checklist_items: List[str],
    alias_overrides: Optional[Dict[str, str]] = None,
) -> str:
    text = normalize_text(value)
    if alias_overrides:
        text = alias_overrides.get(text, text)
    target_map = {normalize_text(item): item for item in checklist_items}
    return target_map.get(normalize_text(text), text)


def checklist_map(
    obj: Dict[str, Any],
    *,
    checklist_items: List[str],
    alias_overrides: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in ((obj.get("checklist_results") or {}).get("items") or []):
        name = canonicalize_checklist_name(
            str(item.get("checklist_item") or item.get("item") or "").strip(),
            checklist_items=checklist_items,
            alias_overrides=alias_overrides,
        )
        status = str(item.get("status", "")).strip().lower()
        if name:
            out[name] = status
    return out


def missing_or_not_found_items(
    obj: Dict[str, Any],
    *,
    checklist_items: List[str],
    alias_overrides: Optional[Dict[str, str]] = None,
) -> List[str]:
    ck = checklist_map(obj, checklist_items=checklist_items, alias_overrides=alias_overrides)
    return [item for item in checklist_items if ck.get(item) != "found"]


def build_repair_context_slice(
    current_json: Dict[str, Any],
    *,
    categories: List[str],
    checklist_items: List[str],
    alias_overrides: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    checklist_lookup = {
        canonicalize_checklist_name(
            str(item.get("checklist_item") or item.get("item") or "").strip(),
            checklist_items=checklist_items,
            alias_overrides=alias_overrides,
        ): item
        for item in ((current_json.get("checklist_results") or {}).get("items") or [])
        if isinstance(item, dict)
    }
    out: Dict[str, Any] = {
        "checklist_results": {
            "items": [
                checklist_lookup.get(
                    item,
                    {
                        "checklist_item": item,
                        "status": "not_found",
                        "category_populated": None,
                        "not_found_reason": "Not yet supported by current extraction.",
                    },
                )
                for item in checklist_items
            ]
        }
    }
    for category in categories:
        values = current_json.get(category)
        if isinstance(values, list) and values:
            out[category] = values
    return out


def segment_repairs_for_missing_items(
    items: List[str],
    *,
    segment_definitions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    repairs: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Tuple[str, ...]]] = set()
    missing_set = set(items)
    for segment in segment_definitions:
        repair_items = [item for item in segment.get("checklist_items") or [] if item in missing_set]
        if not repair_items:
            continue
        key = (str(segment.get("name") or ""), tuple(repair_items))
        if key in seen:
            continue
        seen.add(key)
        repairs.append(
            {
                "name": str(segment.get("name") or "segment"),
                "checklist_items": repair_items,
                "categories": list(segment.get("categories") or []),
            }
        )
    return repairs


def flatten_packet_rows(obj: Dict[str, Any], *, category_order: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    counter = 1
    for category in category_order:
        if category == "quarantine":
            continue
        values = obj.get(category)
        if not isinstance(values, list):
            continue
        for entry in values:
            if not isinstance(entry, dict):
                continue
            fact = str(entry.get("fact") or "").strip()
            if not fact:
                continue
            rows.append(
                {
                    "row_id": f"R{counter}",
                    "category": category,
                    "fact": fact,
                    "source": str(entry.get("source") or "").strip(),
                    "filing_ref": str(entry.get("filing_ref") or "").strip(),
                    "confidence": str(entry.get("confidence") or "").strip(),
                    "_entry_ref": entry,
                }
            )
            counter += 1
    return rows


def _checklist_items_list(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    checklist_results = obj.setdefault("checklist_results", {})
    items = checklist_results.setdefault("items", [])
    if not isinstance(items, list):
        items = []
        checklist_results["items"] = items
    return items


def _set_checklist_status(
    obj: Dict[str, Any],
    item_name: str,
    *,
    checklist_items: List[str],
    alias_overrides: Optional[Dict[str, str]] = None,
    status: str,
    category_populated: Optional[str] = None,
    not_found_reason: Optional[str] = None,
) -> None:
    items = _checklist_items_list(obj)
    canonical = canonicalize_checklist_name(
        item_name,
        checklist_items=checklist_items,
        alias_overrides=alias_overrides,
    )
    for entry in items:
        current_name = canonicalize_checklist_name(
            str(entry.get("checklist_item") or entry.get("item") or "").strip(),
            checklist_items=checklist_items,
            alias_overrides=alias_overrides,
        )
        if current_name != canonical:
            continue
        entry["checklist_item"] = canonical
        entry["status"] = status
        entry["category_populated"] = category_populated if status == "found" else None
        entry["not_found_reason"] = None if status == "found" else (not_found_reason or "Not supported by sourced evidence.")
        return
    items.append(
        {
            "checklist_item": canonical,
            "status": status,
            "category_populated": category_populated if status == "found" else None,
            "not_found_reason": None if status == "found" else (not_found_reason or "Not supported by sourced evidence."),
        }
    )


def _dedupe_fact_list(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        key = normalize_text(item.get("fact") or json.dumps(item, sort_keys=True, ensure_ascii=False))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _merge_checklist_items(
    parts: List[Dict[str, Any]],
    *,
    checklist_items: List[str],
    alias_overrides: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {
        item: {
            "checklist_item": item,
            "status": "not_found",
            "category_populated": None,
            "not_found_reason": "Not assessed in current run.",
        }
        for item in checklist_items
    }
    for part in parts:
        for item in ((part.get("checklist_results") or {}).get("items") or []):
            canonical = canonicalize_checklist_name(
                str(item.get("checklist_item") or item.get("item") or "").strip(),
                checklist_items=checklist_items,
                alias_overrides=alias_overrides,
            )
            if canonical not in merged:
                continue
            candidate = {
                "checklist_item": canonical,
                "status": str(item.get("status") or merged[canonical]["status"]).strip().lower(),
                "category_populated": item.get("category_populated"),
                "not_found_reason": item.get("not_found_reason"),
            }
            current = merged[canonical]
            if candidate["status"] == "found" or current["status"] != "found":
                merged[canonical] = candidate
    return [merged[item] for item in checklist_items]


def merge_segment_outputs(
    *,
    asset_class: str,
    company: str,
    ticker: str,
    exchange: str,
    discovery_json: Dict[str, Any],
    segment_outputs: List[Dict[str, Any]],
    checklist_items: List[str],
    category_order: List[str],
    alias_overrides: Optional[Dict[str, str]] = None,
    extra_top_level: Optional[Dict[str, Any]] = None,
    warning: str = "Supplementary facts only. Verify SECONDARY items against primary filings before use.",
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {
        "asset_class": asset_class,
        "exchange": exchange,
        "ticker": f"{exchange}:{ticker}",
        "company": company,
        "extraction_date": date.today().isoformat(),
        "source_report_count": len((discovery_json.get("priority_sources") or [])),
        "warning": warning,
        "checklist_results": {
            "items": _merge_checklist_items(
                segment_outputs,
                checklist_items=checklist_items,
                alias_overrides=alias_overrides,
            )
        },
    }
    if isinstance(extra_top_level, dict):
        for key, value in extra_top_level.items():
            merged[key] = value
    for name in category_order:
        values: List[Dict[str, Any]] = []
        for part in segment_outputs:
            part_values = part.get(name) or []
            if isinstance(part_values, list):
                values.extend([item for item in part_values if isinstance(item, dict)])
        if values:
            merged[name] = _dedupe_fact_list(values)
    return merged


def apply_contamination_review(
    obj: Dict[str, Any],
    review: Dict[str, Any],
    *,
    category_order: List[str],
    checklist_items: List[str],
    asset_class: str,
    alias_overrides: Optional[Dict[str, str]] = None,
    min_confidence_pct: float = 95.0,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    adjusted = json.loads(json.dumps(obj))
    packet_rows = flatten_packet_rows(adjusted, category_order=category_order)
    row_lookup = {row["row_id"]: row for row in packet_rows}
    review_rows = review.get("row_decisions") or []
    dropped_rows: List[Dict[str, Any]] = []
    drop_row_ids: set[str] = set()
    for item in review_rows:
        if not isinstance(item, dict):
            continue
        row_id = str(item.get("row_id") or "").strip()
        if row_id not in row_lookup:
            continue
        decision = str(item.get("decision") or "keep").strip().lower()
        confidence = float(item.get("confidence_pct") or 0.0)
        if decision == "drop_row" and confidence >= float(min_confidence_pct):
            drop_row_ids.add(row_id)
            dropped_rows.append(
                {
                    "row_id": row_id,
                    "category": row_lookup[row_id]["category"],
                    "fact": row_lookup[row_id]["fact"],
                    "confidence_pct": confidence,
                    "reason": str(item.get("reason") or "").strip(),
                    "wrong_entity_detected": str(item.get("wrong_entity_detected") or "").strip() or None,
                }
            )
    if drop_row_ids:
        rebuilt_by_category: Dict[str, List[Dict[str, Any]]] = {}
        for row in packet_rows:
            row_id = str(row.get("row_id") or "").strip()
            if row_id in drop_row_ids:
                continue
            category = str(row.get("category") or "").strip()
            entry = row.get("_entry_ref")
            if not category or not isinstance(entry, dict):
                continue
            rebuilt_by_category.setdefault(category, []).append(entry)
        for category in category_order:
            if category == "quarantine":
                continue
            rebuilt = rebuilt_by_category.get(category) or []
            if rebuilt:
                adjusted[category] = rebuilt
            else:
                adjusted.pop(category, None)
        for item in _checklist_items_list(adjusted):
            if str(item.get("status") or "").strip().lower() != "found":
                continue
            category = str(item.get("category_populated") or "").strip()
            if not category:
                continue
            remaining = adjusted.get(category)
            if isinstance(remaining, list) and remaining:
                continue
            _set_checklist_status(
                adjusted,
                str(item.get("checklist_item") or ""),
                checklist_items=checklist_items,
                alias_overrides=alias_overrides,
                status="not_found",
                not_found_reason="Only supporting row(s) were dropped by the contamination guard.",
            )

    packet_decision = str(review.get("packet_decision") or "keep").strip().lower()
    packet_confidence = float(review.get("packet_confidence_pct") or 0.0)
    wrong_entity_detected = str(review.get("wrong_entity_detected") or "").strip() or None
    drop_packet = bool(
        packet_decision == "drop_packet"
        and packet_confidence >= float(min_confidence_pct)
        and wrong_entity_detected
    )
    if drop_packet:
        minimal = {
            "asset_class": str(adjusted.get("asset_class") or asset_class),
            "exchange": str(adjusted.get("exchange") or ""),
            "ticker": str(adjusted.get("ticker") or ""),
            "company": str(adjusted.get("company") or ""),
            "extraction_date": str(adjusted.get("extraction_date") or date.today().isoformat()),
            "source_report_count": 0,
            "warning": str(adjusted.get("warning") or "").strip(),
            "checklist_results": {"items": []},
        }
        if "commodity" in adjusted:
            minimal["commodity"] = str(adjusted.get("commodity") or "")
        reason = str(review.get("packet_reason") or "High-confidence wrong-company contamination.").strip()
        if minimal["warning"]:
            minimal["warning"] = f"{minimal['warning']} Contamination guard dropped the packet: {reason}".strip()
        else:
            minimal["warning"] = f"Contamination guard dropped the packet: {reason}"
        for checklist_item in checklist_items:
            minimal["checklist_results"]["items"].append(
                {
                    "checklist_item": checklist_item,
                    "status": "not_found",
                    "category_populated": None,
                    "not_found_reason": "Packet dropped by high-confidence wrong-company contamination guard.",
                }
            )
        adjusted = minimal

    return adjusted, {
        "packet_decision": packet_decision or "keep",
        "packet_confidence_pct": packet_confidence,
        "packet_reason": str(review.get("packet_reason") or "").strip(),
        "wrong_entity_detected": wrong_entity_detected,
        "drop_packet_applied": drop_packet,
        "dropped_row_count": len(dropped_rows),
        "dropped_rows": dropped_rows,
        "reviewed_row_count": len(packet_rows),
        "min_confidence_pct": float(min_confidence_pct),
    }
