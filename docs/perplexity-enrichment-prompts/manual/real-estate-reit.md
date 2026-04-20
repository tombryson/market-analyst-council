# Real Estate / REIT Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/real-estate-reit.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: real_estate_reit
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
PROPERTY_TYPE: [office | retail | industrial | logistics | residential | healthcare | mixed | other]
PRIMARY_REGION: [PRIMARY_REGION]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream real estate / reit enrichment pipeline.

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
Prioritize property valuation reports, lease announcements, registry/property documents, debt documents, rating agency actions, planning approvals, tenant releases, distribution statements, sustainability certifications, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Asset acquisition/disposal with property name, counterparty, price, cap rate, yield, or settlement date
- Occupancy, WALE, rent reversion, lease expiry, tenant concentration, arrears, or incentive fact
- Named tenant lease, renewal, vacancy, administration, insolvency, rent relief, or expansion
- Independent valuation, valuer, cap rate, discount rate, NTA/NAV, impairment, or fair value movement
- LTV, gearing, ICR, covenant, debt maturity, hedge, facility, lender, rating, or refinancing fact
- Development approval, project cost, pre-lease, contractor, completion date, yield-on-cost, or landbank fact
- Distribution, payout ratio, DRP, buyback, equity raise, asset sale programme, or capital recycling fact
- Environmental certification, NABERS/Green Star/LEED, remediation, cladding, or building defect fact
- Planning, zoning, tax, stamp duty, rent control, court, tribunal, or regulator decision
- Management internalization, external manager fee, related-party transaction, or governance fact
- Peer property transaction or broker factual reference where source is named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "real_estate_reit",
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
