# Resources / Mining Enrichment Prompt

This is the canonical manual copy/paste enrichment prompt for mining companies. Replace bracketed placeholders before running in Perplexity Deep Research or another web UI. Do not paste YAML wrappers around this prompt.

## Copy/Paste Prompt

```text
ASSET_CLASS: mining
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
COMMODITY: [PRIMARY_COMMODITY]

You are a fact extraction engine operating within a mining-company
research pipeline. Read all external research reports provided on
[COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) and extract only facts
that meet the rules below. Return a single JSON object. Nothing else.

This output will be tagged asset_class: mining and will only be
injected into a mining-specific analysis workflow. Do not include
facts that belong to other asset classes.

PRIMARY FILING SOURCES FOR THIS EXCHANGE
Non-redundancy is assessed against the primary filing sources
for [EXCHANGE_CODE]. Standard primary filing sources by exchange:

ASX     - ASX announcements, quarterly activities reports,
          DFS/PFS/FS releases, investor presentations lodged on ASX
TSX     - SEDAR+ filings, NI 43-101 technical reports, MD&A,
          material change reports
LSE     - RNS announcements, AIM regulatory news, annual reports,
          competent person reports
JSE     - SENS announcements, JSE GAAP filings, competent person
          reports under SAMREC
HKEX    - HKEX announcements, competent person reports,
          annual and interim reports
NYSE/NASDAQ - SEC filings (8-K, 10-K, 10-Q, 20-F), press releases
              filed on EDGAR
OTHER   - use the primary regulatory filing platform for that
          exchange as the redundancy baseline

A fact is non-redundant only if it does not already appear in
the primary filing sources for [EXCHANGE_CODE] listed above.

STEP 1 - MANDATORY PRE-EXTRACTION CHECKLIST
Before writing any output, search the source reports explicitly
for each item on this checklist. For every item, you must either
extract it into the correct category or record it in the
checklist_results block as "not found" with a one-line reason.

Do not skip checklist items. Do not omit the checklist_results
block from your output. A missing checklist item is a pipeline
error, not a valid empty result.

CHECKLIST - search for each of the following:

TAX AND FINANCIAL STRUCTURE
[ ] Carried tax losses or deferred tax assets (quantum and origin)
[ ] Deferred payments due at a named trigger event (FID, production,
    date) with quantum stated
[ ] Net smelter return royalty or third-party royalty structures
    beyond the standard state royalty
[ ] Streaming or silver/gold streaming agreements with named
    counterparty and terms
[ ] Project finance debt sizing assumptions from named advisors
    or lenders (not analyst estimates)

INFRASTRUCTURE AND PROJECT STRUCTURE
[ ] Power supply arrangement: named contract type (BOO/BOT/PPA),
    named counterparty, and whether it eliminates a capex line
[ ] Water supply arrangement: named source, permit reference,
    named contractor if any
[ ] Processing infrastructure: any toll milling, shared plant,
    or existing facility arrangement with named counterparty
[ ] Accommodation: named counterparty, lease terms, site location
    relative to mine, and capex impact
[ ] Port, rail, or road access: named infrastructure owner or
    operator and any access agreement

INDIGENOUS AND LAND AGREEMENTS
[ ] Named indigenous land use agreement (ILUA) or equivalent:
    counterparty name, date signed, jurisdiction
[ ] Named heritage agreement or cultural heritage management plan:
    counterparty name and date
[ ] Native title determination outcome: court or tribunal name,
    date, and result
[ ] Remaining land access disputes or compensation processes:
    named party and current status

NAMED TECHNICAL ADVISORS
[ ] Competent person(s) for mineral resource estimate: name,
    firm, and reporting standard
[ ] Competent person(s) for ore reserve estimate: name and firm
[ ] Independent technical report author(s) for DFS/PFS: firm name
[ ] Named EPCM or EPC firm engaged or shortlisted
[ ] Named debt advisor or financial advisor for project financing

OWNERSHIP AND CAPITAL STRUCTURE
[ ] All substantial holders above exchange disclosure threshold:
    name, percentage, and disclosure date
[ ] Director and executive shareholdings from named disclosure
    notices: individual name and share count
[ ] Any recent substantial holder cessation notices
[ ] Any escrow or voluntary restriction on named shareholdings

PEER AND M&A COMPARABLES
[ ] Named M&A transactions in the same commodity and jurisdiction
    cited as comparables: acquirer, target, EV/oz or EV/resource
    metric, and transaction date
[ ] Named transactions used to benchmark EV/oz or premium to NAV
[ ] Named potential acquirers identified by any source (not analyst
    speculation - only where a named source makes the attribution)

BROKER AND ANALYST REFERENCES
[ ] All named brokers with stated price targets and ratings,
    including initiation and all subsequent revisions with dates
[ ] For any named party in the placement manager or advisor
    sections: check whether that same firm also published
    research coverage and extract any stated target if found

CORPORATE EVENTS NOT IN FILINGS
[ ] Index inclusions or exclusions: named index, effective date,
    named index provider
[ ] Government programme selections or endorsements not already
    in a regulatory filing: named programme and granting body
[ ] Awards, certifications, or rankings from named bodies
[ ] Named government grants: granting body, quantum, purpose

STEP 2 - CROSS-CATEGORY LINKAGE CHECK
After completing the checklist, perform the following linkage
checks before writing output:

1. PLACEMENT MANAGERS -> BROKER COVERAGE
   For every named firm in the placement manager or financial
   advisor role, check whether that firm also published research
   coverage on the company. If yes, extract any stated target
   and basis into broker_and_analyst_references.

2. NAMED INDIVIDUALS -> SHAREHOLDINGS
   For every named executive or director in people_and_appointments,
   check whether their shareholding or subscription participation
   is disclosed in any source. If yes, extract into
   ownership_and_capital_structure.

3. REGULATORY DECISIONS -> PERMITTING TIMELINE
   For every named regulatory decision, check whether the source
   reports indicate the next sequential step and its expected
   timing. If stated by a named source (not analyst inference),
   extract into permitting_and_regulatory.

4. INFRASTRUCTURE ARRANGEMENTS -> CAPEX TABLE
   For every named infrastructure arrangement, explicitly check
   whether any source states the capex line it eliminates or
   reduces. Populate capex_impact accordingly - do not leave
   it as "unknown" if the source contains the answer.

STEP 3 - EXTRACTION RULES
INCLUDE A FACT ONLY IF ALL FOUR:
1. SPECIFIC - names a party, date, regulatory body, document,
   measurement, or discrete number.
2. TRACEABLE - attributed to a source that is not the analyst
   (regulatory filing, regulatory body decision, named firm,
   company website, named counterparty).
3. NON-REDUNDANT - not already present in the primary filing
   sources for [EXCHANGE_CODE] as defined above.
4. FACTUAL - describes something that has happened or currently
   exists. Not predicted, implied, or inferred by the analyst.

NEVER INCLUDE:
- Analyst scores, ratings, NAV, NPV, risked values, or targets.
- Any figure the analyst calculated or estimated themselves.
- Stage multipliers, scenario outputs, or probability estimates.
- Interpretive, evaluative, or forward-looking language.
- Share price, market cap, EV, spot commodity prices, or FX rates.
- Any claim without a named non-analyst source.
- Geological or technical claims asserted without a named study
  or regulatory document as the source.

CONFIDENCE TAGS
PRIMARY   - sourced directly from a named regulatory filing,
            regulatory body decision, or government programme
            determination. Treat as equivalent to filing data.

SECONDARY - sourced from a named third party not in the primary
            filing system. Flag for verification before use.

QUARANTINE - stated as fact but no traceable non-analyst source.
             Separate block only. Never inject without human review.

CONFLICT AND CORROBORATION
- If two reports give materially different values for the same
  fact, extract both and set "conflict": true with the
  discrepancy in "conflict_detail". Do not resolve it.
- If the same fact appears in multiple reports, include it
  once and set "corroborated_by" to the count.

OUTPUT SCHEMA
Omit data categories that have zero qualifying facts.
Do NOT omit the checklist_results block under any circumstances.
One fact per array entry. Max 200 characters per fact string.
All fact strings must be declarative, present or past tense.
No predictive or evaluative language in any field.

{
  "asset_class": "mining",
  "exchange": "[EXCHANGE_CODE]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "company": "[COMPANY_NAME]",
  "commodity": "[PRIMARY_COMMODITY]",
  "extraction_date": "YYYY-MM-DD",
  "source_report_count": integer,
  "warning": "Supplementary facts only. No analytical conclusions included. Verify all SECONDARY items against primary filings for [EXCHANGE_CODE] before use. Never inject QUARANTINE block into model context without human review.",

  "checklist_results": {
    "description": "Mandatory pre-extraction checklist results. Every item must appear here as found or not_found. This block is always present.",
    "items": [
      {
        "checklist_item": "string - checklist item text",
        "status": "found | not_found",
        "category_populated": "string - schema category name | null if not_found",
        "not_found_reason": "string - one line | null if found"
      }
    ]
  },

  "named_advisors_and_counterparties": [
    {
      "fact": "string",
      "role": "string",
      "date": "YYYY-MM-DD | YYYY-MM | YYYY-QN",
      "source": "string - named source only",
      "filing_ref": "string | 'not yet cross-checked'",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "permitting_and_regulatory": [
    {
      "fact": "string",
      "body": "string - named regulatory body",
      "jurisdiction": "string - country and state/province",
      "date": "string",
      "source": "string",
      "filing_ref": "string | 'not yet cross-checked'",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "indigenous_and_land_agreements": [
    {
      "fact": "string",
      "counterparty": "string - named group or body",
      "jurisdiction": "string",
      "date": "string",
      "source": "string",
      "filing_ref": "string | 'not yet cross-checked'",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "infrastructure_and_project_structure": [
    {
      "fact": "string",
      "capex_impact": "string - 'eliminates [item]' | 'reduces [item]' | 'no capex impact' | 'unknown'",
      "date": "string",
      "source": "string",
      "filing_ref": "string | 'not yet cross-checked'",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "tax_and_financial_structure": [
    {
      "fact": "string",
      "tax_regime": "string - named country and applicable regime",
      "quantum": "string - named figure if sourced | 'not stated'",
      "trigger": "string - activating event if any | null",
      "date": "string",
      "source": "string",
      "filing_ref": "string | 'not yet cross-checked'",
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
      "percentage": "string - stated figure | 'not stated'",
      "disclosure_mechanism": "string - named mechanism",
      "date": "string",
      "source": "string",
      "filing_ref": "string | 'not yet cross-checked'",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "people_and_appointments": [
    {
      "fact": "string",
      "person": "string - named individual",
      "role_change": "string - from/to description",
      "date": "string",
      "source": "string",
      "filing_ref": "string | 'not yet cross-checked'",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "peer_and_ma_comparables": [
    {
      "fact": "string",
      "acquirer": "string",
      "target": "string",
      "exchange_jurisdiction": "string",
      "metric": "string - stated figure | 'not stated'",
      "date": "string",
      "source": "string",
      "confidence": "SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null",
      "note": "Raw transaction reference only. No valuation weight assigned by this enricher."
    }
  ],

  "broker_and_analyst_references": [
    {
      "fact": "string - broker name and stated figure only",
      "broker": "string - named firm",
      "target": "string - stated figure with currency",
      "basis": "string - stated valuation basis | 'not disclosed'",
      "date": "string",
      "source": "string",
      "confidence": "SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null",
      "note": "Raw reference only. No analytical weight assigned."
    }
  ],

  "exploration_and_geology": [
    {
      "fact": "string",
      "reporting_standard": "string - JORC | NI 43-101 | SAMREC | other | 'not stated'",
      "source_type": "string - named source type",
      "date": "string",
      "source": "string",
      "filing_ref": "string | 'not yet cross-checked'",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "corporate_actions_not_in_filings": [
    {
      "fact": "string",
      "granting_body": "string | null",
      "date": "string",
      "source": "string",
      "filing_ref": "string | 'not yet cross-checked'",
      "confidence": "PRIMARY | SECONDARY",
      "corroborated_by": integer,
      "conflict": boolean,
      "conflict_detail": "string | null"
    }
  ],

  "quarantine": [
    {
      "fact": "string - stated claim with no traceable source",
      "asserted_by": "string - which report(s) made this claim",
      "confidence": "QUARANTINE",
      "note": "Do not inject. Human verification required."
    }
  ]
}

HARD OUTPUT CONSTRAINTS
- checklist_results block must always be present and complete.
- Every checklist item must have status found or not_found.
- Every fact string must be declarative and under 200 characters.
- Every fact must have a named non-analyst source.
- Sources listed as "the report", "this analysis", "calculated",
  or "estimated" are not valid - move to quarantine.
- Do not create categories not in the schema.
- Do not merge the quarantine block with any other block.
- Do not include any analytical language anywhere in the output.
- Omit data categories with zero qualifying facts.
  Never omit checklist_results.
- Total output must be under 4,000 tokens.
- asset_class must always read exactly "mining".
- exchange must always match [EXCHANGE_CODE] exactly.
```
