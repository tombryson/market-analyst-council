# Consumer Staples Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/consumer-staples.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: consumer_staples
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
STAPLES_TYPE: [food | beverage | household | personal care | agriculture | mixed | other]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream consumer staples enrichment pipeline.

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
Prioritize retailer/supplier releases, product recall databases, food safety regulators, manufacturing permits, brand documents, distribution agreements, commodity/input supply contracts, sustainability certifications, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Volume, price/mix, market share, ranging win/loss, retailer distribution, or shelf-space fact
- Named brand launch, reformulation, packaging change, private-label exposure, or category expansion
- Retailer concentration, customer contract, distributor, route-to-market, or export market fact
- Consumer complaint, product quality, recall, contamination, labelling, or food safety event
- Manufacturing site, capacity, utilization, shutdown, capex, automation, or bottleneck
- Input commodity, packaging, freight, energy, water, labour, or supplier cost fact
- Supplier, co-manufacturer, logistics provider, farm, processor, or procurement agreement
- Certification, sustainability, animal welfare, organic, halal/kosher, or standards-body fact
- Regulator notice, product registration, import/export restriction, tariff, licence, or health claim decision
- Acquisition, brand sale, divestment, JV, licensing, or distribution agreement
- Named broker factual reference, peer transaction, or category data source

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "consumer_staples",
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
