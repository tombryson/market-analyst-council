"""Reusable segmented mining supplementary-facts workflow helpers."""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

CHECKLIST_ITEMS: List[str] = [
    "Carried tax losses or deferred tax assets (quantum and origin)",
    "Deferred payments due at a named trigger event (FID, production, date) with quantum stated",
    "Net smelter return royalty or third-party royalty structures beyond standard state royalty",
    "Streaming or silver/gold streaming agreements with named counterparty and terms",
    "Project finance debt sizing assumptions from named advisors or lenders (not analyst estimates)",
    "Power supply arrangement: named contract type (BOO/BOT/PPA), named counterparty, and whether it eliminates a capex line",
    "Water supply arrangement: named source, permit reference, named contractor if any",
    "Processing infrastructure: any toll milling, shared plant, or existing facility arrangement with named counterparty",
    "Accommodation: named counterparty, lease terms, site location relative to mine, and capex impact",
    "Port, rail, or road access: named infrastructure owner or operator and any access agreement",
    "Named indigenous land use agreement (ILUA) or equivalent: counterparty name, date signed, jurisdiction",
    "Named heritage agreement or cultural heritage management plan: counterparty name and date",
    "Native title determination outcome: court or tribunal name, date, and result",
    "Remaining land access disputes or compensation processes: named party and current status",
    "Competent person(s) for mineral resource estimate: name, firm, and reporting standard",
    "Competent person(s) for ore reserve estimate: name and firm",
    "Independent technical report author(s) for DFS/PFS: firm name",
    "Named EPCM or EPC firm engaged or shortlisted",
    "Named debt advisor or financial advisor for project financing",
    "All substantial holders above exchange disclosure threshold: name, percentage, and disclosure date",
    "Director and executive shareholdings from named disclosure notices: individual name and share count",
    "Any recent substantial holder cessation notices",
    "Any escrow or voluntary restriction on named shareholdings",
    "Named M&A transactions in the same commodity and jurisdiction cited as comparables: acquirer, target, EV/oz or EV/resource metric, and transaction date",
    "Named transactions used to benchmark EV/oz or premium to NAV",
    "Named potential acquirers identified by any source (not analyst speculation — only where a named source makes the attribution)",
    "All named brokers with stated price targets and ratings, including initiation and all subsequent revisions with dates",
    "For any named party in the placement manager or advisor sections: check whether that same firm also published research coverage and extract any stated target if found",
    "Index inclusions or exclusions: named index, effective date, named index provider",
    "Government programme selections or endorsements not already in a regulatory filing: named programme and granting body",
    "Awards, certifications, or rankings from named bodies",
    "Named government grants: granting body, quantum, purpose",
]

SEGMENT_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "core_finance_ops",
        "checklist_items": CHECKLIST_ITEMS[0:12],
        "categories": [
            "named_advisors_and_counterparties",
            "infrastructure_and_project_structure",
            "tax_and_financial_structure",
            "quarantine",
        ],
    },
    {
        "name": "regulatory_ownership_people",
        "checklist_items": CHECKLIST_ITEMS[10:23],
        "categories": [
            "indigenous_and_land_agreements",
            "ownership_and_capital_structure",
            "people_and_appointments",
            "permitting_and_regulatory",
            "quarantine",
        ],
    },
    {
        "name": "technical_broker_misc",
        "checklist_items": CHECKLIST_ITEMS[14:32],
        "categories": [
            "named_advisors_and_counterparties",
            "peer_and_ma_comparables",
            "broker_and_analyst_references",
            "exploration_and_geology",
            "corporate_actions_not_in_filings",
            "quarantine",
        ],
    },
]

CHECKLIST_ALIAS_OVERRIDES: Dict[str, str] = {
    "for placement manager or advisor: check whether same firm also published research coverage and extract stated target": "For any named party in the placement manager or advisor sections: check whether that same firm also published research coverage and extract any stated target if found",
    "named potential acquirers identified by any source (not analyst speculation)": "Named potential acquirers identified by any source (not analyst speculation — only where a named source makes the attribution)",
    "power supply arrangement: named contract type (boo/ bot/ppa), named counterparty, and whether it eliminates a capex line": "Power supply arrangement: named contract type (BOO/BOT/PPA), named counterparty, and whether it eliminates a capex line",
    "power supply arrangement: named contract type (boo/bot/ppa), named counterparty, and whether it eliminates a capex line": "Power supply arrangement: named contract type (BOO/BOT/PPA), named counterparty, and whether it eliminates a capex line",
    "named m&a transactions in the same commodity and jurisdiction cited as comparables: acquirer, target, ev/oz or ev/resource metric, and transaction date": "Named M&A transactions in the same commodity and jurisdiction cited as comparables: acquirer, target, EV/oz or EV/resource metric, and transaction date",
}

CATEGORY_ORDER = [
    "named_advisors_and_counterparties",
    "permitting_and_regulatory",
    "indigenous_and_land_agreements",
    "infrastructure_and_project_structure",
    "tax_and_financial_structure",
    "ownership_and_capital_structure",
    "people_and_appointments",
    "peer_and_ma_comparables",
    "broker_and_analyst_references",
    "exploration_and_geology",
    "corporate_actions_not_in_filings",
    "quarantine",
]

CATEGORY_SCHEMA_BLOCKS: Dict[str, str] = {
    "named_advisors_and_counterparties": '"named_advisors_and_counterparties": [{"fact": "string", "role": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "permitting_and_regulatory": '"permitting_and_regulatory": [{"fact": "string", "body": "string", "jurisdiction": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "indigenous_and_land_agreements": '"indigenous_and_land_agreements": [{"fact": "string", "counterparty": "string", "jurisdiction": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "infrastructure_and_project_structure": '"infrastructure_and_project_structure": [{"fact": "string", "capex_impact": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "tax_and_financial_structure": '"tax_and_financial_structure": [{"fact": "string", "tax_regime": "string", "quantum": "string", "trigger": "string|null", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "ownership_and_capital_structure": '"ownership_and_capital_structure": [{"fact": "string", "holder": "string", "percentage": "string", "disclosure_mechanism": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "people_and_appointments": '"people_and_appointments": [{"fact": "string", "person": "string", "role_change": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "peer_and_ma_comparables": '"peer_and_ma_comparables": [{"fact": "string", "acquirer": "string", "target": "string", "exchange_jurisdiction": "string", "metric": "string", "date": "string", "source": "string", "confidence": "SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "broker_and_analyst_references": '"broker_and_analyst_references": [{"fact": "string", "broker": "string", "target": "string", "basis": "string", "date": "string", "source": "string", "confidence": "SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "exploration_and_geology": '"exploration_and_geology": [{"fact": "string", "reporting_standard": "string", "source_type": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "corporate_actions_not_in_filings": '"corporate_actions_not_in_filings": [{"fact": "string", "granting_body": "string|null", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
}

DISCOVERY_PROMPT_TEMPLATE = """\
ASSET_CLASS: mining
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are sourcing documents for a mining-company enrichment pipeline.
If documents are not attached, use web search and URL fetch to discover them.

Find the most useful sources for extracting supplementary non-redundant facts on {company} ({exchange}:{ticker}).
Prioritize:
- primary filings
- broker research
- technical / DFS / plant / contractor documents
- counterparty documents
- company investor materials

Return one JSON object only:
{{
  "asset_class": "mining",
  "exchange": "{exchange}",
  "ticker": "{exchange}:{ticker}",
  "company": "{company}",
  "priority_sources": [
    {{
      "title": "string",
      "url": "string",
      "source_class": "primary_filing|broker_research|technical_report|counterparty|company_material|other",
      "why_relevant": "string"
    }}
  ],
  "coverage_notes": ["string"],
  "known_gaps": ["string"]
}}

Rules:
- Return at least 12 sources if available.
- Include direct URLs, not homepages, wherever possible.
- Prefer the highest-value sources over long noisy lists.
- Include broker notes only when clearly attributable to a named firm.
"""

SEGMENT_EXTRACTION_PROMPT_TEMPLATE = """\
ASSET_CLASS: mining
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are a mining-company fact extraction engine.

Use web search and URL fetch, but prioritize the discovered sources below first.
Treat these as the starting source packet:
{source_packet}

Focus only on this checklist segment:
{checklist_block}

Rules:
- Return only facts relevant to these checklist items and these categories: {category_names}
- Keep checklist_results complete for this segment only
- If evidence is weak or missing, prefer not_found over guesswork
- Return a single JSON object only

Output schema:
{schema}
"""

TARGETED_REPAIR_PROMPT_TEMPLATE = """\
ASSET_CLASS: mining
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are repairing a mining-company supplementary-facts extraction.

Use web search and URL fetch, but prioritize the discovered sources below first.
Treat these as the starting source packet:
{source_packet}

Current relevant extraction slice:
{current_slice}

Repair only these checklist items:
{missing_items_block}

Allowed categories: {category_names}

Rules:
- Return only facts relevant to these repair items and allowed categories
- Keep checklist_results complete for these repair items only
- If evidence is weak or absent, keep status=not_found with a specific reason
- Keep facts short, declarative, and source-attributed
- Return a single JSON object only

Output schema:
{schema}
"""

CONTAMINATION_REVIEW_PROMPT_TEMPLATE = """\
Target entity:
- company: {company}
- ticker: {display_ticker}
- exchange: {exchange}

You are a high-precision contamination checker for a mining supplementary-facts packet.

Your job is only to identify clear wrong-company contamination.
Be conservative.

Decisions:
- keep: evidence is about the target company or plausibly about the target company
- drop_row: the row is clearly about a different company/entity
- drop_packet: the packet is overwhelmingly about the wrong company and should be discarded

Rules:
- Do not drop for ambiguity alone.
- Do not drop because a counterparty, peer, acquirer, broker, advisor, regulator, indigenous group, contractor, or comparable company is mentioned.
- Only drop if the row itself is clearly misattributed to the wrong issuer.
- Use high precision, not high recall.
- Prefer keep unless the wrong entity is explicit.

Packet rows:
{packet_rows_block}

Return one JSON object only:
{{
  "packet_decision": "keep|drop_packet",
  "packet_confidence_pct": 0,
  "packet_reason": "string",
  "wrong_entity_detected": "string|null",
  "row_decisions": [
    {{
      "row_id": "R1",
      "decision": "keep|drop_row",
      "confidence_pct": 0,
      "reason": "string",
      "wrong_entity_detected": "string|null"
    }}
  ]
}}
"""

SEGMENT_SCHEMA_TEMPLATE = """\
{{
  "asset_class": "mining",
  "exchange": "{exchange}",
  "ticker": "{exchange}:{ticker}",
  "company": "{company}",
  "extraction_date": "{today}",
  "source_report_count": 0,
  "warning": "Supplementary facts only. Verify SECONDARY items against primary filings before use.",
  "checklist_results": {{
    "items": [
      {{
        "checklist_item": "string",
        "status": "found|not_found",
        "category_populated": "string|null",
        "not_found_reason": "string|null"
      }}
    ]
  }},
  {category_blocks}
  "quarantine": [{{"fact": "string", "asserted_by": "string", "confidence": "QUARANTINE", "note": "Do not inject. Human verification required."}}]
}}
"""


def normalize_text(value: str) -> str:
    text = re.sub(r"\[[^\]]+\]", "", str(value or ""))
    text = text.replace("—", "-").replace("–", "-")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


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
        return json.loads(raw), None
    except Exception:
        repaired = repair_jsonish(raw)
        try:
            return json.loads(repaired), None
        except Exception as exc:
            return None, f"json_parse_error:{type(exc).__name__}:{exc}"


def repair_jsonish(text: str) -> str:
    out: List[str] = []
    in_str = False
    escape = False
    for ch in text:
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


def canonicalize_checklist_name(value: str) -> str:
    text = normalize_text(value)
    text = CHECKLIST_ALIAS_OVERRIDES.get(text, text)
    target_map = {normalize_text(item): item for item in CHECKLIST_ITEMS}
    return target_map.get(normalize_text(text), text)


def checklist_map(obj: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in ((obj.get("checklist_results") or {}).get("items") or []):
        name = canonicalize_checklist_name(
            str(item.get("checklist_item") or item.get("item") or "").strip()
        )
        status = str(item.get("status", "")).strip().lower()
        if name:
            out[name] = status
    return out


def missing_or_not_found_items(obj: Dict[str, Any], checklist_items: Optional[List[str]] = None) -> List[str]:
    active_items = checklist_items or CHECKLIST_ITEMS
    ck = checklist_map(obj)
    return [item for item in active_items if ck.get(item) != "found"]


def build_discovery_prompt(*, company: str, ticker: str, exchange: str, commodity: str) -> str:
    return DISCOVERY_PROMPT_TEMPLATE.format(
        company=company,
        ticker=ticker,
        exchange=exchange,
        commodity=commodity,
    )


def _normalize_exchange_code(value: Optional[str]) -> str:
    text = str(value or "").strip().upper()
    return re.sub(r"[^A-Z0-9]+", "", text)


def _normalize_ticker_symbol(value: Optional[str]) -> str:
    raw = str(value or "").strip().upper()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    raw = re.sub(r"\.[A-Z]{1,6}$", "", raw)
    return raw.strip().upper()


def _normalize_primary_commodity(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    aliases = {
        "au": "gold",
        "gold": "gold",
        "cu": "copper",
        "copper": "copper",
        "li": "lithium",
        "lithium": "lithium",
        "ag": "silver",
        "silver": "silver",
        "u": "uranium",
        "u3o8": "uranium",
        "uranium": "uranium",
        "oil": "oil_gas",
        "gas": "oil_gas",
        "oil_gas": "oil_gas",
        "brent": "oil_gas",
        "wti": "oil_gas",
        "diversified": "diversified",
    }
    return aliases.get(raw, raw)


def resolve_mining_enricher_context(
    *,
    user_query: str = "",
    ticker: str = "",
    company: str = "",
    exchange: str = "",
    commodity: str = "",
    template_id: str = "",
    company_type: str = "",
) -> Dict[str, str]:
    """Resolve canonical enricher header fields from pipeline context."""
    from ..template_loader import get_template_loader
    from ..market_facts import _resolve_commodity_profile

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

    selected_template_id = str(
        template_id
        or selection.get("template_id")
        or ""
    ).strip()
    selected_company_type = str(
        company_type
        or selection.get("company_type")
        or ""
    ).strip()
    exchange_code = _normalize_exchange_code(
        exchange
        or selection.get("exchange")
        or (ticker_raw.split(":", 1)[0] if ":" in ticker_raw else "")
    )
    ticker_symbol = _normalize_ticker_symbol(ticker_raw)

    selected_company_name = str(company or "").strip()
    if not selected_company_name:
        selected_company_name = str(selection.get("company_name") or "").strip()
    if not selected_company_name:
        selected_company_name = loader.infer_company_name(query_seed, ticker=ticker_raw).strip()
    if not selected_company_name:
        selected_company_name = ticker_symbol or "the company"

    selected_commodity = _normalize_primary_commodity(commodity)
    if not selected_commodity:
        selected_commodity = _normalize_primary_commodity(
            _resolve_commodity_profile(selected_template_id, selected_company_type)
        )

    return {
        "exchange": exchange_code,
        "ticker_symbol": ticker_symbol,
        "display_ticker": (
            f"{exchange_code}:{ticker_symbol}" if exchange_code and ticker_symbol else ticker_symbol
        ),
        "company": selected_company_name,
        "commodity": selected_commodity,
        "template_id": selected_template_id,
        "company_type": selected_company_type,
    }


def build_segment_schema(*, company: str, ticker: str, exchange: str, commodity: str, categories: List[str]) -> str:
    blocks = [
        CATEGORY_SCHEMA_BLOCKS[name]
        for name in categories
        if name in CATEGORY_SCHEMA_BLOCKS and name != "quarantine"
    ]
    return SEGMENT_SCHEMA_TEMPLATE.format(
        company=company,
        ticker=ticker,
        exchange=exchange,
        commodity=commodity,
        today=date.today().isoformat(),
        category_blocks="\n  ".join(blocks),
    )


def build_segment_extraction_prompt(
    *,
    company: str,
    ticker: str,
    exchange: str,
    commodity: str,
    source_packet: Dict[str, Any],
    checklist_items: List[str],
    categories: List[str],
) -> str:
    return SEGMENT_EXTRACTION_PROMPT_TEMPLATE.format(
        company=company,
        ticker=ticker,
        exchange=exchange,
        commodity=commodity,
        source_packet=json.dumps(source_packet, indent=2, ensure_ascii=False),
        checklist_block="\n".join([f"- {item}" for item in checklist_items]),
        category_names=", ".join(categories),
        schema=build_segment_schema(
            company=company,
            ticker=ticker,
            exchange=exchange,
            commodity=commodity,
            categories=categories,
        ),
    )


def build_repair_context_slice(
    current_json: Dict[str, Any],
    *,
    categories: List[str],
    checklist_items: List[str],
) -> Dict[str, Any]:
    checklist_lookup = {
        canonicalize_checklist_name(str(item.get("checklist_item") or item.get("item") or "").strip()): item
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


def build_targeted_repair_prompt(
    *,
    company: str,
    ticker: str,
    exchange: str,
    commodity: str,
    source_packet: Dict[str, Any],
    current_json: Dict[str, Any],
    checklist_items: List[str],
    categories: List[str],
) -> str:
    current_slice = build_repair_context_slice(
        current_json,
        categories=categories,
        checklist_items=checklist_items,
    )
    return TARGETED_REPAIR_PROMPT_TEMPLATE.format(
        company=company,
        ticker=ticker,
        exchange=exchange,
        commodity=commodity,
        source_packet=json.dumps(source_packet, indent=2, ensure_ascii=False),
        current_slice=json.dumps(current_slice, indent=2, ensure_ascii=False),
        missing_items_block="\n".join([f"- {item}" for item in checklist_items]),
        category_names=", ".join(categories),
        schema=build_segment_schema(
            company=company,
            ticker=ticker,
            exchange=exchange,
            commodity=commodity,
            categories=categories,
        ),
    )


def segment_repairs_for_missing_items(items: List[str]) -> List[Dict[str, Any]]:
    repairs: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Tuple[str, ...]]] = set()
    missing_set = set(items)
    for segment in SEGMENT_DEFINITIONS:
        repair_items = [item for item in segment["checklist_items"] if item in missing_set]
        if not repair_items:
            continue
        key = (str(segment["name"]), tuple(repair_items))
        if key in seen:
            continue
        seen.add(key)
        repairs.append(
            {
                "name": segment["name"],
                "checklist_items": repair_items,
                "categories": list(segment["categories"]),
            }
        )
    return repairs


def build_contamination_review_prompt(
    *,
    company: str,
    ticker: str,
    exchange: str,
    packet_rows: List[Dict[str, Any]],
) -> str:
    packet_lines: List[str] = []
    for row in packet_rows:
        row_id = str(row.get("row_id") or "").strip()
        category = str(row.get("category") or "").strip()
        fact = str(row.get("fact") or "").strip()
        source = str(row.get("source") or "").strip()
        filing_ref = str(row.get("filing_ref") or "").strip()
        confidence = str(row.get("confidence") or "").strip()
        packet_lines.append(
            "\n".join(
                [
                    f"{row_id}",
                    f"category: {category}",
                    f"fact: {fact}",
                    f"source: {source}",
                    f"filing_ref: {filing_ref}",
                    f"confidence: {confidence}",
                ]
            )
        )
    return CONTAMINATION_REVIEW_PROMPT_TEMPLATE.format(
        company=company,
        display_ticker=ticker,
        exchange=exchange,
        packet_rows_block="\n\n".join(packet_lines),
    )


def flatten_packet_rows(obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    counter = 1
    for category in CATEGORY_ORDER:
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


def _minimal_packet_from_context(obj: Dict[str, Any], reason: str) -> Dict[str, Any]:
    base = {
        "asset_class": str(obj.get("asset_class") or "mining"),
        "exchange": str(obj.get("exchange") or ""),
        "ticker": str(obj.get("ticker") or ""),
        "company": str(obj.get("company") or ""),
        "commodity": str(obj.get("commodity") or ""),
        "extraction_date": str(obj.get("extraction_date") or date.today().isoformat()),
        "source_report_count": 0,
        "warning": str(obj.get("warning") or "").strip(),
        "checklist_results": {"items": []},
    }
    if base["warning"]:
        base["warning"] = f"{base['warning']} Contamination guard dropped the packet: {reason}".strip()
    else:
        base["warning"] = f"Contamination guard dropped the packet: {reason}"
    for item in CHECKLIST_ITEMS:
        base["checklist_results"]["items"].append(
            {
                "checklist_item": item,
                "status": "not_found",
                "category_populated": None,
                "not_found_reason": "Packet dropped by high-confidence wrong-company contamination guard.",
            }
        )
    return base


def apply_contamination_review(
    obj: Dict[str, Any],
    review: Dict[str, Any],
    *,
    min_confidence_pct: float = 95.0,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    adjusted = json.loads(json.dumps(obj))
    packet_rows = flatten_packet_rows(adjusted)
    row_lookup = {row["row_id"]: row for row in packet_rows}
    review_rows = review.get("row_decisions") or []
    dropped_rows: List[Dict[str, Any]] = []
    kept_row_ids: set[str] = set()
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
        else:
            kept_row_ids.add(row_id)

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
        for category in CATEGORY_ORDER:
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
            item["status"] = "not_found"
            item["category_populated"] = None
            item["not_found_reason"] = "Only supporting row(s) were dropped by the contamination guard."

    packet_decision = str(review.get("packet_decision") or "keep").strip().lower()
    packet_confidence = float(review.get("packet_confidence_pct") or 0.0)
    wrong_entity_detected = str(review.get("wrong_entity_detected") or "").strip() or None
    drop_packet = bool(
        packet_decision == "drop_packet"
        and packet_confidence >= float(min_confidence_pct)
        and wrong_entity_detected
    )
    if drop_packet:
        adjusted = _minimal_packet_from_context(
            adjusted,
            str(review.get("packet_reason") or "High-confidence wrong-company contamination.").strip(),
        )

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


def _merge_checklist_items(parts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {
        item: {
            "checklist_item": item,
            "status": "not_found",
            "category_populated": None,
            "not_found_reason": "Not assessed in current run.",
        }
        for item in CHECKLIST_ITEMS
    }
    for part in parts:
        for item in ((part.get("checklist_results") or {}).get("items") or []):
            canonical = canonicalize_checklist_name(
                str(item.get("checklist_item") or item.get("item") or "").strip()
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
    return [merged[item] for item in CHECKLIST_ITEMS]


def merge_segment_outputs(
    *,
    company: str,
    ticker: str,
    exchange: str,
    commodity: str,
    discovery_json: Dict[str, Any],
    segment_outputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {
        "asset_class": "mining",
        "exchange": exchange,
        "ticker": f"{exchange}:{ticker}",
        "company": company,
        "commodity": commodity,
        "extraction_date": date.today().isoformat(),
        "source_report_count": len((discovery_json.get("priority_sources") or [])),
        "warning": "Supplementary facts only. Verify SECONDARY items against primary filings before use.",
        "checklist_results": {"items": _merge_checklist_items(segment_outputs)},
    }
    for name in CATEGORY_ORDER:
        values: List[Dict[str, Any]] = []
        for part in segment_outputs:
            part_values = part.get(name) or []
            if isinstance(part_values, list):
                values.extend([item for item in part_values if isinstance(item, dict)])
        if values:
            merged[name] = _dedupe_fact_list(values)
    return merged


def _category_entries(obj: Dict[str, Any], category: str) -> List[Dict[str, Any]]:
    values = obj.get(category) or []
    if not isinstance(values, list):
        return []
    return [item for item in values if isinstance(item, dict)]


def _entry_haystack(entry: Dict[str, Any]) -> str:
    parts = [str(value) for value in entry.values() if isinstance(value, (str, int, float))]
    return normalize_text(" ".join(parts))


def _entries_match_terms(entries: Iterable[Dict[str, Any]], terms: Iterable[str]) -> bool:
    term_list = [normalize_text(term) for term in terms if str(term or "").strip()]
    if not term_list:
        return False
    for entry in entries:
        hay = _entry_haystack(entry)
        if any(term in hay for term in term_list):
            return True
    return False


def _advisor_broker_name_overlap(obj: Dict[str, Any]) -> bool:
    advisors = []
    for entry in _category_entries(obj, "named_advisors_and_counterparties"):
        role = normalize_text(entry.get("role", ""))
        if any(term in role for term in ("placement", "lead manager", "bookrunner", "advisor")):
            advisors.append(normalize_text(entry.get("fact", "")))
            advisors.append(normalize_text(entry.get("source", "")))
    broker_names = {
        normalize_text(entry.get("broker", ""))
        for entry in _category_entries(obj, "broker_and_analyst_references")
        if normalize_text(entry.get("broker", ""))
    }
    broker_names = {name for name in broker_names if len(name) >= 4}
    if not advisors or not broker_names:
        return False
    for advisor_text in advisors:
        for broker_name in broker_names:
            if broker_name and broker_name in advisor_text:
                return True
    return False


def _company_tokens(company: str) -> List[str]:
    stop = {"limited", "ltd", "resources", "mining", "corp", "corporation", "plc", "inc", "nl"}
    tokens = [token for token in re.split(r"[^a-z0-9]+", normalize_text(company)) if token and token not in stop]
    return tokens


def _valid_transaction_entries(obj: Dict[str, Any], company: str) -> List[Dict[str, Any]]:
    tokens = _company_tokens(company)
    valid: List[Dict[str, Any]] = []
    for entry in _category_entries(obj, "peer_and_ma_comparables"):
        acquirer = normalize_text(entry.get("acquirer", ""))
        target = normalize_text(entry.get("target", ""))
        fact = normalize_text(entry.get("fact", ""))
        if not acquirer or not target:
            continue
        if acquirer == target:
            continue
        if tokens and any(token in acquirer and token in target for token in tokens):
            continue
        if not any(term in fact for term in ("acquir", "takeover", "merger", "bought", "purchase", "bid")):
            continue
        valid.append(entry)
    return valid


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
    status: str,
    category_populated: Optional[str] = None,
    not_found_reason: Optional[str] = None,
) -> None:
    items = _checklist_items_list(obj)
    canonical = canonicalize_checklist_name(item_name)
    for entry in items:
        current_name = canonicalize_checklist_name(str(entry.get("checklist_item") or entry.get("item") or "").strip())
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


def apply_deterministic_adjudication(obj: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    adjusted = json.loads(json.dumps(obj))
    audit: List[Dict[str, Any]] = []

    def mark_found(item_name: str, category: str, rule: str) -> None:
        current = checklist_map(adjusted).get(item_name)
        if current == "found":
            return
        _set_checklist_status(adjusted, item_name, status="found", category_populated=category)
        audit.append({"item": item_name, "action": "mark_found", "category": category, "rule": rule})

    def mark_not_found(item_name: str, reason: str, rule: str) -> None:
        current = checklist_map(adjusted).get(item_name)
        if current == "not_found":
            return
        _set_checklist_status(adjusted, item_name, status="not_found", not_found_reason=reason)
        audit.append({"item": item_name, "action": "mark_not_found", "rule": rule, "reason": reason})

    infra_entries = _category_entries(adjusted, "infrastructure_and_project_structure")
    advisor_entries = _category_entries(adjusted, "named_advisors_and_counterparties")
    broker_entries = _category_entries(adjusted, "broker_and_analyst_references")
    geology_entries = _category_entries(adjusted, "exploration_and_geology")

    if _entries_match_terms(infra_entries, ["water", "pipeline", "potable", "raw water", "utility water"]):
        mark_found(
            "Water supply arrangement: named source, permit reference, named contractor if any",
            "infrastructure_and_project_structure",
            "infra_contains_water_terms",
        )
    if _entries_match_terms(infra_entries, ["power", "electricity", "grid", "substation", "ppa", "boo", "bot", "generator"]):
        mark_found(
            "Power supply arrangement: named contract type (BOO/BOT/PPA), named counterparty, and whether it eliminates a capex line",
            "infrastructure_and_project_structure",
            "infra_contains_power_terms",
        )
    if _entries_match_terms(infra_entries, ["processing", "toll", "mill", "plant", "cil", "cip", "existing facility", "shared plant"]):
        mark_found(
            "Processing infrastructure: any toll milling, shared plant, or existing facility arrangement with named counterparty",
            "infrastructure_and_project_structure",
            "infra_contains_processing_terms",
        )
    if _entries_match_terms(infra_entries, ["accommodation", "camp", "village", "lodge"]):
        mark_found(
            "Accommodation: named counterparty, lease terms, site location relative to mine, and capex impact",
            "infrastructure_and_project_structure",
            "infra_contains_accommodation_terms",
        )
    if _entries_match_terms(infra_entries, ["road", "rail", "port", "haul road", "access road", "haulage"]):
        mark_found(
            "Port, rail, or road access: named infrastructure owner or operator and any access agreement",
            "infrastructure_and_project_structure",
            "infra_contains_transport_terms",
        )
    if _entries_match_terms(advisor_entries + infra_entries, ["epcm", "epc", "engineering procurement"]):
        mark_found(
            "Named EPCM or EPC firm engaged or shortlisted",
            "named_advisors_and_counterparties",
            "advisor_or_infra_contains_epc_terms",
        )
    if _entries_match_terms(advisor_entries, ["debt advisor", "financial advisor", "arranger", "mandated lead arranger", "project finance advisor"]):
        mark_found(
            "Named debt advisor or financial advisor for project financing",
            "named_advisors_and_counterparties",
            "advisor_contains_finance_terms",
        )
    if broker_entries:
        mark_found(
            "All named brokers with stated price targets and ratings, including initiation and all subsequent revisions with dates",
            "broker_and_analyst_references",
            "broker_category_non_empty",
        )
    if _advisor_broker_name_overlap(adjusted):
        mark_found(
            "For any named party in the placement manager or advisor sections: check whether that same firm also published research coverage and extract any stated target if found",
            "broker_and_analyst_references",
            "advisor_broker_name_overlap",
        )
    valid_transactions = _valid_transaction_entries(adjusted, str(adjusted.get("company", "")))
    if valid_transactions:
        mark_found(
            "Named M&A transactions in the same commodity and jurisdiction cited as comparables: acquirer, target, EV/oz or EV/resource metric, and transaction date",
            "peer_and_ma_comparables",
            "valid_transaction_entries_present",
        )
        mark_found(
            "Named transactions used to benchmark EV/oz or premium to NAV",
            "peer_and_ma_comparables",
            "valid_transaction_entries_present",
        )
    elif _category_entries(adjusted, "peer_and_ma_comparables"):
        mark_not_found(
            "Named transactions used to benchmark EV/oz or premium to NAV",
            "Comparable entries were self-referential or lacked named acquirer/target transaction detail.",
            "peer_entries_invalid_for_transaction_benchmark",
        )

    if _entries_match_terms(geology_entries, ["competent person", "qualified person", "jorc", "ni 43-101", "samrec"]):
        geology_texts = " ".join(_entry_haystack(entry) for entry in geology_entries)
        if any(term in geology_texts for term in ["resource", "mre", "mineral resource"]):
            mark_found(
                "Competent person(s) for mineral resource estimate: name, firm, and reporting standard",
                "exploration_and_geology",
                "geology_contains_competent_resource_terms",
            )
        if any(term in geology_texts for term in ["reserve", "ore reserve"]):
            mark_found(
                "Competent person(s) for ore reserve estimate: name and firm",
                "exploration_and_geology",
                "geology_contains_competent_reserve_terms",
            )
        if "independent" in geology_texts and any(term in geology_texts for term in ["dfs", "pfs", "feasibility"]):
            mark_found(
                "Independent technical report author(s) for DFS/PFS: firm name",
                "exploration_and_geology",
                "geology_contains_independent_report_terms",
            )

    return adjusted, {"rules_applied": audit, "rule_count": len(audit)}


__all__ = [
    "CATEGORY_ORDER",
    "CHECKLIST_ITEMS",
    "SEGMENT_DEFINITIONS",
    "apply_contamination_review",
    "apply_deterministic_adjudication",
    "build_contamination_review_prompt",
    "build_discovery_prompt",
    "build_segment_extraction_prompt",
    "build_targeted_repair_prompt",
    "canonicalize_checklist_name",
    "checklist_map",
    "extract_json_payload",
    "flatten_packet_rows",
    "merge_segment_outputs",
    "missing_or_not_found_items",
    "resolve_mining_enricher_context",
    "segment_repairs_for_missing_items",
]
