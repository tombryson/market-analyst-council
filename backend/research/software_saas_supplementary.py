"""Scaffolded segmented supplementary-facts workflow for software and SaaS."""

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

PIPELINE_ID = "software_saas_supplementary"
SUPPORTED_TEMPLATE_IDS = ["software_saas"]
SUPPORTED_FAMILY_IDS = ["software"]
ASSET_CLASS = "software_saas"
INDUSTRY_LABEL = "software and SaaS"

CHECKLIST_ITEMS: List[str] = [
    "Named enterprise customers or contract wins with date and product scope",
    "Named customer churn, downsizing, non-renewal, or delayed rollout event",
    "Named reseller, channel, hyperscaler, or implementation partner",
    "Named OEM, marketplace, or embedded distribution arrangement",
    "Pricing, packaging, seat, or SKU changes from a named source",
    "Named product launch, module release, or platform expansion",
    "Usage, seat-growth, or deployment metric from a named source",
    "Named implementation timeline, rollout milestone, or go-live date",
    "Named services partner, systems integrator, or MSP involved in deployment",
    "Named customer concentration or major account dependency fact",
    "Named security incident, outage, or service reliability event",
    "Named compliance, certification, or audit milestone (SOC 2, ISO 27001, FedRAMP, HIPAA, etc.)",
    "Named cloud infrastructure, hosting, or data-residency arrangement",
    "Named data, AI, or model partner and scope of relationship",
    "Named product or platform dependency on a major vendor",
    "All substantial holders above exchange disclosure threshold: name, percentage, and date",
    "Director and executive holdings from named disclosures",
    "Named founder, CEO, CFO, CTO, CRO, or product leadership appointment/departure",
    "Recent financing, convert, term loan, or facility led by a named bank or counterparty",
    "Named capital management action not already clear from primary filings",
    "All named brokers with stated price targets and ratings",
    "Named broker estimate revision, initiation, or thesis change",
    "Named awards, rankings, or vendor-inclusion events not already in filings",
    "Named conference appearance, keynote, or ecosystem selection not already in filings",
    "Named litigation, IP dispute, or material governance action from a named source",
    "Named regulatory or privacy investigation affecting the company or product",
    "Named integration, acquisition, or divestment milestone not already in filings",
    "Named government contract, panel inclusion, or procurement framework selection",
]

SEGMENT_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "customers_products_channels",
        "checklist_items": CHECKLIST_ITEMS[0:10],
        "categories": [
            "customer_and_partner_references",
            "product_and_pricing",
            "implementation_and_channel",
            "quarantine",
        ],
    },
    {
        "name": "security_compliance_platform",
        "checklist_items": CHECKLIST_ITEMS[10:15],
        "categories": [
            "security_and_compliance",
            "customer_and_partner_references",
            "quarantine",
        ],
    },
    {
        "name": "capital_people_brokers_misc",
        "checklist_items": CHECKLIST_ITEMS[15:28],
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
    "customer_and_partner_references",
    "product_and_pricing",
    "implementation_and_channel",
    "security_and_compliance",
    "ownership_and_capital_structure",
    "people_and_appointments",
    "broker_and_analyst_references",
    "corporate_actions_not_in_filings",
    "quarantine",
]

CHECKLIST_ALIAS_OVERRIDES: Dict[str, str] = {}

CATEGORY_SCHEMA_BLOCKS: Dict[str, str] = {
    "customer_and_partner_references": '"customer_and_partner_references": [{"fact": "string", "counterparty": "string", "relationship": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "product_and_pricing": '"product_and_pricing": [{"fact": "string", "product": "string", "change_type": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "implementation_and_channel": '"implementation_and_channel": [{"fact": "string", "counterparty": "string", "channel_type": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "security_and_compliance": '"security_and_compliance": [{"fact": "string", "programme": "string", "status": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "ownership_and_capital_structure": '"ownership_and_capital_structure": [{"fact": "string", "holder": "string", "percentage": "string", "disclosure_mechanism": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "people_and_appointments": '"people_and_appointments": [{"fact": "string", "person": "string", "role_change": "string", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "broker_and_analyst_references": '"broker_and_analyst_references": [{"fact": "string", "broker": "string", "target": "string", "basis": "string", "date": "string", "source": "string", "confidence": "SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
    "corporate_actions_not_in_filings": '"corporate_actions_not_in_filings": [{"fact": "string", "granting_body": "string|null", "date": "string", "source": "string", "filing_ref": "string", "confidence": "PRIMARY|SECONDARY", "corroborated_by": 1, "conflict": false, "conflict_detail": null}],',
}

DISCOVERY_PROMPT_TEMPLATE = """\
ASSET_CLASS: software_saas
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are sourcing documents for a software-and-SaaS enrichment pipeline.
If documents are not attached, use web search and URL fetch to discover them.

Find the most useful sources for extracting supplementary non-redundant facts on {company} ({exchange}:{ticker}).
Prioritize:
- primary filings
- named customer or partner releases
- product or pricing documentation
- compliance or certification notices
- company investor materials
- attributable named broker research

Return one JSON object only:
{{
  "asset_class": "software_saas",
  "exchange": "{exchange}",
  "ticker": "{exchange}:{ticker}",
  "company": "{company}",
  "priority_sources": [
    {{
      "title": "string",
      "url": "string",
      "source_class": "primary_filing|customer_release|partner_document|product_documentation|compliance_notice|company_material|broker_research|other",
      "why_relevant": "string"
    }}
  ],
  "coverage_notes": ["string"],
  "known_gaps": ["string"]
}}

Rules:
- Return at least 12 sources if available.
- Include direct URLs, not homepages, wherever possible.
- Prefer high-signal sources over long noisy lists.
- Include broker notes only when clearly attributable to a named firm.
"""

SEGMENT_EXTRACTION_PROMPT_TEMPLATE = """\
ASSET_CLASS: software_saas
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are a software-and-SaaS fact extraction engine.

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
ASSET_CLASS: software_saas
EXCHANGE: {exchange}
TICKER: {exchange}:{ticker}
COMPANY: {company}

You are repairing a software-and-SaaS supplementary-facts extraction.

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

You are a high-precision contamination checker for a software-and-SaaS supplementary-facts packet.

Your job is only to identify clear wrong-company contamination.
Be conservative.

Decisions:
- keep: evidence is about the target company or plausibly about the target company
- drop_row: the row is clearly about a different company/entity
- drop_packet: the packet is overwhelmingly about the wrong company and should be discarded

Rules:
- Do not drop for ambiguity alone.
- Do not drop because a customer, partner, platform vendor, regulator, analyst, or competitor is mentioned.
- Only drop if the row itself is clearly misattributed to the wrong issuer or product owner.
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
  "asset_class": "software_saas",
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


def resolve_software_saas_enricher_context(
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
