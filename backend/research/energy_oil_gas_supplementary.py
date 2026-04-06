"""Scaffolded segmented supplementary-facts workflow for energy and oil & gas."""

from __future__ import annotations

from datetime import date
import json
from typing import Any, Dict, List, Tuple

from .supplementary_base import (
    SupplementaryPipelineSpec,
    apply_contamination_review as apply_generic_contamination_review,
    build_repair_context_slice,
    extract_json_payload,
    merge_segment_outputs as merge_generic_segment_outputs,
    missing_or_not_found_items as generic_missing_or_not_found_items,
    resolve_company_context,
    segment_repairs_for_missing_items as generic_segment_repairs_for_missing_items,
)

PIPELINE_ID = "energy_oil_gas_supplementary"
SUPPORTED_TEMPLATE_IDS = ["energy_oil_gas"]
SUPPORTED_FAMILY_IDS = ["energy"]
ASSET_CLASS = "energy_oil_gas"
INDUSTRY_LABEL = "energy and oil & gas"

CHECKLIST_ITEMS: List[str] = [
    "Named reserve or resource report: auditor or author, effective date, and reserve class",
    "Named field, basin, block, or permit with operator and working interest",
    "Named development plan, FID, sanction, or phased project milestone",
    "Named drilling programme, rig contract, or completion schedule",
    "Named production, restart, or maintenance outage event from a named source",
    "Named processing, transport, gathering, pipeline, or terminal arrangement",
    "Named gas sales, LNG, crude offtake, or marketing agreement with counterparty",
    "Named hedge, collar, swap, floor, or fixed-price protection structure",
    "Reserve-based lending, redetermination, borrowing-base, or project debt fact",
    "Named JV, farm-in, farm-out, or operatorship change",
    "Named PSC, royalty, tax, tariff, or fiscal-term fact",
    "Named abandonment, decommissioning, plugging, or restoration liability fact",
    "Named regulator, permit, licence, or environmental approval milestone",
    "Named litigation, arbitration, or licence dispute affecting an asset",
    "Named host-government negotiation, tariff reset, or concession amendment",
    "Named service provider, drilling contractor, FPSO, EPC, or field services counterparty",
    "Named processing plant, refinery, fractionation, storage, or midstream counterparty",
    "All substantial holders above exchange disclosure threshold: name, percentage, and date",
    "Director and executive holdings from named disclosures",
    "Named CEO, CFO, COO, exploration, or operations leadership appointment/departure",
    "All named brokers with stated price targets and ratings",
    "Named broker estimate revision or commodity-price sensitivity commentary",
    "Named government grant, strategic programme selection, or acreage award not already in filings",
    "Named M&A transaction, asset sale, or portfolio divestment comparable used by sources",
    "Named customer concentration, counterparty dependency, or single-buyer exposure fact",
    "Named insurance event, force majeure, or material operational interruption",
    "Named ESG, flaring, methane, or emissions compliance milestone",
    "Named shipping, export, or storage bottleneck with attributable source",
]

SEGMENT_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "assets_reserves_operations",
        "checklist_items": CHECKLIST_ITEMS[0:12],
        "categories": [
            "reserves_and_assets",
            "infrastructure_offtake_and_services",
            "hedging_and_financial_structure",
            "quarantine",
        ],
    },
    {
        "name": "regulatory_operations_counterparties",
        "checklist_items": CHECKLIST_ITEMS[12:17] + CHECKLIST_ITEMS[24:28],
        "categories": [
            "permitting_and_regulatory",
            "infrastructure_offtake_and_services",
            "corporate_actions_not_in_filings",
            "quarantine",
        ],
    },
    {
        "name": "capital_people_brokers_manda",
        "checklist_items": CHECKLIST_ITEMS[17:24],
        "categories": [
            "ownership_and_capital_structure",
            "people_and_appointments",
            "broker_and_analyst_references",
            "corporate_actions_not_in_filings",
            "quarantine",
        ],
    },
]

CATEGORY_ORDER = [
    "reserves_and_assets",
    "infrastructure_offtake_and_services",
    "hedging_and_financial_structure",
    "permitting_and_regulatory",
    "ownership_and_capital_structure",
    "people_and_appointments",
    "broker_and_analyst_references",
    "corporate_actions_not_in_filings",
    "quarantine",
]

CHECKLIST_ALIAS_OVERRIDES: Dict[str, str] = {}

CATEGORY_SCHEMA_BLOCKS: Dict[str, str] = {
    "reserves_and_assets": '"reserves_and_assets": [{"fact": "string", "asset": "string", "operator_or_author": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "infrastructure_offtake_and_services": '"infrastructure_offtake_and_services": [{"fact": "string", "counterparty": "string", "arrangement_type": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "hedging_and_financial_structure": '"hedging_and_financial_structure": [{"fact": "string", "instrument_or_facility": "string", "quantum": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "permitting_and_regulatory": '"permitting_and_regulatory": [{"fact": "string", "body": "string", "jurisdiction": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "ownership_and_capital_structure": '"ownership_and_capital_structure": [{"fact": "string", "holder": "string", "percentage": "string", "disclosure_mechanism": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "people_and_appointments": '"people_and_appointments": [{"fact": "string", "person": "string", "role_change": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "broker_and_analyst_references": '"broker_and_analyst_references": [{"fact": "string", "broker": "string", "target": "string", "basis": "string", "date": "string", "source": "string", "confidence": "SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "corporate_actions_not_in_filings": '"corporate_actions_not_in_filings": [{"fact": "string", "granting_body": "string|null", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
}

DISCOVERY_PROMPT_TEMPLATE = """\
ASSET_CLASS: energy_oil_gas
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are sourcing documents for an energy-and-oil-gas enrichment pipeline.
If documents are not attached, use web search and URL fetch to discover them.

Find the most useful sources for extracting supplementary non-redundant facts on {company} ({exchange}:{ticker}).
Prioritize:
- primary filings
- reserve or technical reports
- regulator and permit notices
- operator, partner, or offtake counterparty documents
- company investor materials
- attributable named broker research

Return one JSON object only:
{{
  "asset_class": "energy_oil_gas",
  "exchange": "{exchange}",
  "ticker": "{exchange}:{ticker}",
  "company": "{company}",
  "priority_sources": [
    {{
      "title": "string",
      "url": "string",
      "source_class": "primary_filing|reserve_report|regulator|counterparty|company_material|broker_research|other",
      "why_relevant": "string"
    }}
  ],
  "coverage_notes": ["string"],
  "known_gaps": ["string"]
}}

Rules:
- Return at least 12 sources if available.
- Include direct URLs, not homepages, wherever possible.
- Prefer high-value sources over long noisy lists.
- Include broker notes only when clearly attributable to a named firm.
"""

SEGMENT_EXTRACTION_PROMPT_TEMPLATE = """\
ASSET_CLASS: energy_oil_gas
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are an energy-and-oil-gas fact extraction engine.

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
ASSET_CLASS: energy_oil_gas
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are repairing an energy-and-oil-gas supplementary-facts extraction.

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

You are a high-precision contamination checker for an energy-and-oil-gas supplementary-facts packet.

Your job is only to identify clear wrong-company contamination.
Be conservative.

Decisions:
- keep: evidence is about the target company or plausibly about the target company
- drop_row: the row is clearly about a different company/entity
- drop_packet: the packet is overwhelmingly about the wrong company and should be discarded

Rules:
- Do not drop for ambiguity alone.
- Do not drop because an operator, JV partner, regulator, offtaker, service provider, analyst, or comparable company is mentioned.
- Only drop if the row itself is clearly misattributed to the wrong issuer or asset owner.
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
  "asset_class": "energy_oil_gas",
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


def resolve_energy_oil_gas_enricher_context(
    *,
    user_query: str = "",
    ticker: str = "",
    company: str = "",
    exchange: str = "",
    template_id: str = "",
    company_type: str = "",
) -> Dict[str, str]:
    return resolve_company_context(
        user_query=user_query,
        ticker=ticker,
        company=company,
        exchange=exchange,
        template_id=template_id,
        company_type=company_type,
    )


def build_segment_schema(*, company: str, ticker: str, exchange: str, categories: List[str]) -> str:
    blocks = [
        CATEGORY_SCHEMA_BLOCKS[name]
        for name in categories
        if name in CATEGORY_SCHEMA_BLOCKS and name != "quarantine"
    ]
    return SEGMENT_SCHEMA_TEMPLATE.format(
        company=company,
        ticker=ticker,
        exchange=exchange,
        today=date.today().isoformat(),
        category_blocks="\n  ".join(blocks),
    )


def build_discovery_prompt(*, company: str, ticker: str, exchange: str) -> str:
    return DISCOVERY_PROMPT_TEMPLATE.format(company=company, ticker=ticker, exchange=exchange)


def build_segment_extraction_prompt(
    *,
    company: str,
    ticker: str,
    exchange: str,
    source_packet: Dict[str, Any],
    checklist_items: List[str],
    categories: List[str],
) -> str:
    return SEGMENT_EXTRACTION_PROMPT_TEMPLATE.format(
        company=company,
        ticker=ticker,
        exchange=exchange,
        source_packet=json.dumps(source_packet, indent=2, ensure_ascii=False),
        checklist_block="\n".join([f"- {item}" for item in checklist_items]),
        category_names=", ".join(categories),
        schema=build_segment_schema(company=company, ticker=ticker, exchange=exchange, categories=categories),
    )


def build_targeted_repair_prompt(
    *,
    company: str,
    ticker: str,
    exchange: str,
    source_packet: Dict[str, Any],
    current_slice: Dict[str, Any],
    checklist_items: List[str],
    categories: List[str],
) -> str:
    return TARGETED_REPAIR_PROMPT_TEMPLATE.format(
        company=company,
        ticker=ticker,
        exchange=exchange,
        source_packet=json.dumps(source_packet, indent=2, ensure_ascii=False),
        current_slice=json.dumps(current_slice, indent=2, ensure_ascii=False),
        missing_items_block="\n".join([f"- {item}" for item in checklist_items]),
        category_names=", ".join(categories),
        schema=build_segment_schema(company=company, ticker=ticker, exchange=exchange, categories=categories),
    )


def missing_or_not_found_items(obj: Dict[str, Any], checklist_items: List[str] | None = None) -> List[str]:
    return generic_missing_or_not_found_items(
        obj,
        checklist_items=checklist_items or CHECKLIST_ITEMS,
        alias_overrides=CHECKLIST_ALIAS_OVERRIDES,
    )


def build_repair_context(
    current_json: Dict[str, Any],
    *,
    categories: List[str],
    checklist_items: List[str],
) -> Dict[str, Any]:
    return build_repair_context_slice(
        current_json,
        categories=categories,
        checklist_items=checklist_items,
        alias_overrides=CHECKLIST_ALIAS_OVERRIDES,
    )


def segment_repairs_for_missing_items(items: List[str]) -> List[Dict[str, Any]]:
    return generic_segment_repairs_for_missing_items(items, segment_definitions=SEGMENT_DEFINITIONS)


def build_contamination_review_prompt(
    *,
    company: str,
    ticker: str,
    exchange: str,
    packet_rows: List[Dict[str, Any]],
) -> str:
    packet_lines: List[str] = []
    for row in packet_rows:
        packet_lines.append(
            "\n".join(
                [
                    str(row.get("row_id") or "").strip(),
                    f"category: {str(row.get('category') or '').strip()}",
                    f"fact: {str(row.get('fact') or '').strip()}",
                    f"source: {str(row.get('source') or '').strip()}",
                    f"filing_ref: {str(row.get('filing_ref') or '').strip()}",
                    f"confidence: {str(row.get('confidence') or '').strip()}",
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
    from .supplementary_base import flatten_packet_rows as flatten_generic_packet_rows

    return flatten_generic_packet_rows(obj, category_order=CATEGORY_ORDER)


def merge_segment_outputs(
    *,
    company: str,
    ticker: str,
    exchange: str,
    discovery_json: Dict[str, Any],
    segment_outputs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return merge_generic_segment_outputs(
        asset_class=ASSET_CLASS,
        company=company,
        ticker=ticker,
        exchange=exchange,
        discovery_json=discovery_json,
        segment_outputs=segment_outputs,
        checklist_items=CHECKLIST_ITEMS,
        category_order=CATEGORY_ORDER,
        alias_overrides=CHECKLIST_ALIAS_OVERRIDES,
    )


def apply_deterministic_adjudication(obj: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return json.loads(json.dumps(obj)), {"rules_applied": [], "rule_count": 0}


def apply_contamination_review(
    obj: Dict[str, Any],
    review: Dict[str, Any],
    *,
    min_confidence_pct: float = 95.0,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return apply_generic_contamination_review(
        obj,
        review,
        category_order=CATEGORY_ORDER,
        checklist_items=CHECKLIST_ITEMS,
        asset_class=ASSET_CLASS,
        alias_overrides=CHECKLIST_ALIAS_OVERRIDES,
        min_confidence_pct=min_confidence_pct,
    )


def get_pipeline_spec() -> SupplementaryPipelineSpec:
    return SupplementaryPipelineSpec(
        pipeline_id=PIPELINE_ID,
        asset_class=ASSET_CLASS,
        industry_label=INDUSTRY_LABEL,
        template_ids=list(SUPPORTED_TEMPLATE_IDS),
        family_ids=list(SUPPORTED_FAMILY_IDS),
        checklist_items=list(CHECKLIST_ITEMS),
        segment_definitions=[dict(segment) for segment in SEGMENT_DEFINITIONS],
        category_order=list(CATEGORY_ORDER),
        category_schema_blocks=dict(CATEGORY_SCHEMA_BLOCKS),
        discovery_prompt_template=DISCOVERY_PROMPT_TEMPLATE,
        segment_extraction_prompt_template=SEGMENT_EXTRACTION_PROMPT_TEMPLATE,
        targeted_repair_prompt_template=TARGETED_REPAIR_PROMPT_TEMPLATE,
        contamination_review_prompt_template=CONTAMINATION_REVIEW_PROMPT_TEMPLATE,
    )
