# Defence / Aerospace Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/defence-aerospace.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: defence
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
DEFENCE_SUBSECTOR: [aerospace | naval | land systems | cyber | space | munitions | services | other]
PRIMARY_CUSTOMER_REGION: [PRIMARY_CUSTOMER_REGION]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream defence / aerospace enrichment pipeline.

You are performing source retrieval for a downstream enrichment pipeline.

Your job is to find and organise source material. Do not perform investment analysis.
Do not provide buy/sell/hold recommendations. Do not calculate valuation.
Do not provide price targets, NAV, NPV, rNPV, DCF, fair value, or scenario probabilities.
Do not infer facts not present in named sources.

Replace every bracketed placeholder before running this prompt. If any required placeholder is still unresolved, stop and ask for the missing value.

Retrieval requirements:
- Prefer primary filings and regulator records before company marketing pages.
- Include direct URLs where available, not homepages.
- Include source dates and named source bodies.
- Capture factual details useful for a downstream extractor.
- Preserve uncertainty and gaps. Do not guess.
- Broker or analyst material is allowed only for factual references; exclude recommendations, targets, valuation outputs, and opinions.
- If you find likely wrong-company contamination, list it under rejected_sources with the reason.

Return a source packet, not an investment memo.

Source priorities for this asset class:
Prioritize government contract award databases, defence department releases, export-control notices, programme documents, certification bodies, customer releases, budget documents, security clearance records where public, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Named contract award, extension, option, framework, programme, platform, customer, value, or duration
- Backlog, order intake, production lot, delivery milestone, acceptance, sustainment, or through-life support fact
- Named prime, subcontractor, OEM, government agency, alliance, or programme office
- Export approval, ITAR/EAR, security clearance, facility accreditation, or sovereign capability designation
- Manufacturing ramp, facility, capacity, supplier, bottleneck, quality issue, or delivery delay
- Certification, qualification, test event, flight test, safety approval, or technical milestone
- Cost overrun, schedule slip, termination, protest, audit finding, or remediation
- Cyber, classified systems, data handling, incident, or operational availability fact
- Grant, government programme, R&D contract, acquisition, JV, technology licence, or IP fact
- Budget allocation, procurement plan, tender shortlisting, or programme downselect
- Peer defence transaction or broker factual reference where source is named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "defence",
  "retrieval_date": "YYYY-MM-DD",
  "source_count": integer,
  "sources": [
    {
      "title": "string",
      "source_type": "primary_filing | regulator_record | technical_report | company_material | counterparty_document | government_record | registry_record | broker_factual_note | industry_dataset | other",
      "url": "string | null",
      "date": "string",
      "named_source": "string",
      "factual_summary": ["string"],
      "relevance": "string"
    }
  ],
  "rejected_sources": [
    {
      "title": "string",
      "url": "string | null",
      "reason": "wrong company | unsupported claim | valuation opinion | duplicate | low relevance | inaccessible"
    }
  ],
  "known_gaps": ["string"]
}
```
