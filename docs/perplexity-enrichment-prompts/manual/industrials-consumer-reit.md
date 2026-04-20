# Industrials / Consumer / REIT Umbrella Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/industrials-consumer-reit.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: industrials_consumer_reit
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
COMPANY_TYPE: [industrial | consumer | property | infrastructure | mixed]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream industrials / consumer / reit umbrella enrichment pipeline.

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
Prioritize exchange filings, contract/customer releases, property/lease documents, regulator notices, safety/quality records, certification bodies, government programme documents, and named counterparty materials.

Target source material that can answer these downstream enrichment questions:
- Named contract, customer, tenant, supplier, project, site, property, store, plant, or facility fact
- Backlog, capacity, occupancy, utilization, same-store sales, volume, margin driver, or delivery metric
- Lease, property valuation, asset acquisition/disposal, capex, development, or maintenance milestone
- Safety, quality, recall, environmental, consumer, labour, planning, licence, or regulator issue
- Debt, covenant, financing, capital raise, dividend, buyback, grant, subsidy, or government programme
- Supply chain, inventory, working capital, customer concentration, tenant concentration, or supplier dependency
- Acquisition, divestment, JV, restructuring, closure, market entry/exit, or management change
- Named peer transaction, broker factual reference, certification, award, or third-party dataset

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "industrials_consumer_reit",
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
