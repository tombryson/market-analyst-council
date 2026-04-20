# Datacentres Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/datacentres.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: datacentres
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
INFRA_TYPE: [datacentre | colocation | hyperscale | digital infrastructure | fibre | mixed]
PRIMARY_REGION: [PRIMARY_REGION]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream datacentres enrichment pipeline.

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
Prioritize lease announcements, utility/grid filings, planning approvals, power purchase agreements, customer/counterparty releases, property filings, construction contracts, debt documents, sustainability reports, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- MW capacity, IT load, shell capacity, contracted capacity, available capacity, utilization, PUE, or occupancy
- Named hyperscaler, enterprise tenant, customer lease, pre-lease, reservation, renewal, expansion, or churn
- Lease term, commencement, escalation, option, take-or-pay, fit-out, or SLA fact
- Interconnection, fibre, carrier density, cloud on-ramp, or network ecosystem fact
- Grid connection, substation, power allocation, PPA, renewable certificate, generator, battery, or cooling arrangement
- Planning approval, land acquisition, zoning, construction milestone, contractor, cost/MW, or development pipeline fact
- Water, environmental approval, energy efficiency, emissions, or sustainability certification
- Named contractor, utility, landowner, government body, financier, or JV partner
- Debt facility, project finance, sale-leaseback, JV, asset sale, capex commitment, or covenant fact
- Portfolio acquisition, disposal, valuation, cap rate, yield-on-cost, or independent valuation fact
- Regulatory, cyber, outage, incident, or service interruption event

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "datacentres",
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
