# Consumer Staples Enrichment Prompt

This is a manual copy/paste prompt for Perplexity Deep Research or equivalent web UI use. Replace bracketed placeholders before running. Do not paste YAML wrappers around this prompt.

## Copy/Paste Prompt

```text
ASSET_CLASS: consumer_staples
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
STAPLES_TYPE: [food | beverage | household | personal care | agriculture | mixed | other]
PRIMARY_MARKET: [PRIMARY_MARKET]

You are a fact extraction engine operating within a consumer staples and food research pipeline. Extract traceable facts on volumes, pricing, brands, manufacturing, distribution, food safety, input costs, retailers, and regulation. Return one JSON object only.

This output will be tagged asset_class: consumer_staples and must only be injected into a consumer staples analysis workflow. Do not include facts that belong to unrelated asset classes.

PRIMARY FILING SOURCES FOR THIS EXCHANGE
Non-redundancy is assessed against the primary filing sources for [EXCHANGE_CODE]. Standard primary filing sources by exchange:

ASX: ASX announcements, annual reports, investor presentations lodged on ASX, Appendix filings, quarterly reports where applicable.
TSX/TSXV: SEDAR+ filings, annual information forms, MD&A, material change reports, technical reports where applicable.
LSE/AIM: RNS announcements, annual/interim reports, admission documents, circulars, competent person reports where applicable.
JSE: SENS announcements, JSE filings, annual/interim reports, competent person or sector reports where applicable.
HKEX: HKEX announcements, annual/interim reports, circulars, prospectuses.
NYSE/NASDAQ/AMEX: SEC EDGAR filings, 8-K, 10-K, 10-Q, 20-F, 6-K, DEF 14A, registered offering documents.
OTHER: use the primary regulatory filing platform for that exchange as the redundancy baseline.

A fact is non-redundant only if it does not already appear in the primary filing sources for [EXCHANGE_CODE] listed above. If the fact is already in those filings, do not extract it unless the third-party source adds a named counterparty, date, document, regulatory decision, or factual detail not present in the filing.

SECTOR-SPECIFIC SOURCE PRIORITY
Prioritize retailer/supplier releases, product recall databases, food safety regulators, manufacturing permits, brand documents, distribution agreements, commodity/input supply contracts, sustainability certifications, and exchange filings.

STEP 1 - MANDATORY PRE-EXTRACTION CHECKLIST
Before writing output, search the source reports explicitly for every checklist item. For every item, either extract it into the correct category or record it in checklist_results as not_found with a one-line reason.

CHECKLIST

BRANDS AND DEMAND
[ ] Volume, price/mix, market share, ranging win/loss, retailer distribution, or shelf-space fact
[ ] Named brand launch, reformulation, packaging change, private-label exposure, or category expansion
[ ] Retailer concentration, customer contract, distributor, route-to-market, or export market fact
[ ] Consumer complaint, product quality, recall, contamination, labelling, or food safety event

MANUFACTURING AND INPUTS
[ ] Manufacturing site, capacity, utilization, shutdown, capex, automation, or bottleneck
[ ] Input commodity, packaging, freight, energy, water, labour, or supplier cost fact
[ ] Supplier, co-manufacturer, logistics provider, farm, processor, or procurement agreement
[ ] Certification, sustainability, animal welfare, organic, halal/kosher, or standards-body fact

REGULATORY AND CORPORATE
[ ] Regulator notice, product registration, import/export restriction, tariff, licence, or health claim decision
[ ] Acquisition, brand sale, divestment, JV, licensing, or distribution agreement
[ ] Named broker factual reference, peer transaction, or category data source

STEP 2 - CROSS-CATEGORY LINKAGE CHECK
After completing the checklist, perform these linkage checks before writing output:

1. ADVISORS AND MANAGERS -> RESEARCH COVERAGE
For every named placement manager, broker, financial advisor, arranger, underwriter, or corporate advisor, check whether that firm also published coverage or a named factual note. Extract only stated facts, not valuation opinions.

2. NAMED PEOPLE -> OWNERSHIP / ROLE CHANGES
For every named executive, director, founder, principal investigator, technical lead, or responsible manager, check whether their appointment, resignation, holding, subscription, or responsibility is disclosed by a named source.

3. REGULATORY DECISIONS -> NEXT REQUIRED STEP
For every named regulatory decision, approval, rejection, hold, notice, licence, permit, waiver, or sanction, check whether the source states the next formal step and expected timing. Extract only if a named source states it.

4. COUNTERPARTIES -> ECONOMIC TERMS
For every named customer, supplier, lender, partner, landlord, offtaker, government agency, or contractor, check whether any source states term, duration, quantum, exclusivity, milestone, obligation, or termination right.

STEP 3 - EXTRACTION RULES
INCLUDE A FACT ONLY IF ALL FOUR CONDITIONS ARE MET:
1. SPECIFIC: names a party, date, regulatory body, document, instrument, metric, asset, programme, contract, measurement, or discrete number.
2. TRACEABLE: attributed to a named non-analyst source such as a regulatory filing, regulator, registry, government body, company filing, counterparty release, court record, technical report, customer release, certification body, or named firm.
3. NON-REDUNDANT: not already present in the primary filing sources for [EXCHANGE_CODE], unless the source adds extra named factual detail.
4. FACTUAL: describes something that has happened or currently exists. Not predicted, implied, inferred, modelled, or recommended by the analyst.

NEVER INCLUDE:
- Analyst scores, recommendations, ratings, price targets, NAV, NPV, rNPV, DCF, sum-of-parts, fair value, risked value, scenario output, or probability estimate.
- Any figure the analyst calculated, estimated, risk-weighted, normalized, or derived.
- Interpretive, evaluative, promotional, or forward-looking language.
- Share price, market cap, enterprise value, trading multiples, FX rates, or spot market prices unless the sector-specific schema explicitly asks for a quoted contract, hedge, tariff, or reference price from a named non-analyst source.
- Any claim without a named non-analyst source.
- Any fact about another company unless it is a named peer transaction, named counterparty, or directly connected contractual relationship.

CONFIDENCE TAGS
PRIMARY: sourced directly from a named regulatory filing, regulator, court, exchange filing, government body, official registry, or formal certification body. Treat as equivalent to filing data.
SECONDARY: sourced from a named third party outside the primary filing system, such as a customer, supplier, partner, broker note, trade publication, conference abstract, or technical counterparty. Flag for verification before use.
QUARANTINE: stated as fact but lacking a traceable named non-analyst source, ambiguous entity match, wrong-company risk, or unsupported analytical assertion. Separate block only. Never inject without human review.

CONFLICT AND CORROBORATION
- If two sources give materially different values for the same fact, extract both and set conflict=true with the discrepancy in conflict_detail. Do not resolve it.
- If the same fact appears in multiple independent sources, include it once and set corroborated_by to the count.
- If a source appears to refer to a similarly named company or ticker, move it to quarantine with the contamination concern.

OUTPUT SCHEMA
Omit data categories that have zero qualifying facts. Do not omit checklist_results. One fact per array entry. Fact strings must be declarative, present or past tense, and under 200 characters. Return valid JSON only.

{
  "asset_class": "consumer_staples",
  "exchange": "[EXCHANGE_CODE]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "company": "[COMPANY_NAME]",
  "extraction_date": "YYYY-MM-DD",
  "source_report_count": integer,
  "warning": "Supplementary facts only. No analytical conclusions included. Verify SECONDARY items against primary filings before injection. Never inject QUARANTINE without human review.",

  "checklist_results": {
    "description": "Mandatory pre-extraction checklist results. Every item must appear here as found or not_found.",
    "items": [
      {
        "checklist_item": "string - checklist item text",
        "status": "found | not_found",
        "category_populated": "string - schema category name | null if not_found",
        "not_found_reason": "string - one line | null if found"
      }
    ]
  },

  "brands_volumes_and_distribution": [
    {
      "fact": "string",
      "brand_or_channel": "string | not stated",
      "metric_or_event": "string",
      "quantum": "string | not stated",
      "date": "string",
      "source": "string - named source only",
      "filing_ref": "string | not yet cross-checked",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "manufacturing_inputs_and_supply": [
    {
      "fact": "string",
      "site_or_supplier": "string | not stated",
      "issue_or_arrangement": "string",
      "term_or_quantum": "string | not stated",
      "date": "string",
      "source": "string - named source only",
      "filing_ref": "string | not yet cross-checked",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "regulatory_quality_and_corporate": [
    {
      "fact": "string",
      "body_or_counterparty": "string | not stated",
      "issue": "string",
      "status": "string | not stated",
      "date": "string",
      "source": "string - named source only",
      "filing_ref": "string | not yet cross-checked",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "ownership_and_capital_structure": [
    {
      "fact": "string",
      "holder": "string - named party",
      "percentage": "string - stated figure | not stated",
      "disclosure_mechanism": "string - named mechanism",
      "date": "string",
      "source": "string - named source only",
      "filing_ref": "string | not yet cross-checked",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "people_and_governance": [
    {
      "fact": "string",
      "person_or_body": "string - named person, committee, regulator, board, or body",
      "role_or_issue": "string",
      "date": "string",
      "source": "string - named source only",
      "filing_ref": "string | not yet cross-checked",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "broker_and_third_party_references": [
    {
      "fact": "string - named firm and stated factual reference only",
      "firm": "string - named firm",
      "reference_type": "string - coverage | initiation | factual note | transaction reference | other",
      "date": "string",
      "source": "string",
      "confidence": "SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null",
      "note": "Raw factual reference only. No analytical weight assigned."
    }
  ],

  "corporate_events_not_in_filings": [
    {
      "fact": "string",
      "event_type": "string",
      "named_body_or_counterparty": "string | null",
      "date": "string",
      "source": "string",
      "filing_ref": "string | not yet cross-checked",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "quarantine": [
    {
      "fact": "string - stated claim with no traceable source or wrong-company risk",
      "asserted_by": "string - which report(s) made this claim",
      "confidence": "QUARANTINE",
      "note": "Do not inject. Human verification required."
    }
  ]
}

HARD OUTPUT CONSTRAINTS
- Return a single JSON object and nothing else.
- checklist_results block must always be present and complete.
- Every checklist item must have status found or not_found.
- Every fact string must be declarative and under 200 characters.
- Every fact must have a named non-analyst source.
- Sources listed only as "the report", "this analysis", "calculated", "estimated", "modelled", or "analyst view" are not valid. Move these to quarantine.
- Do not create categories not in the schema.
- Do not merge the quarantine block with any other block.
- Do not include analytical language anywhere in the output.
- Omit data categories with zero qualifying facts. Never omit checklist_results.
- Total output must be under 4,000 tokens.
- asset_class must always read exactly "consumer_staples".
- exchange must always match [EXCHANGE_CODE] exactly.

```
