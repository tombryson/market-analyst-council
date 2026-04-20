# Consumer Retail Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/consumer-retail.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: consumer_retail
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
RETAIL_TYPE: [apparel | grocery | specialty | ecommerce | marketplace | restaurant | mixed | other]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream consumer retail enrichment pipeline.

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
Prioritize trading updates, store network disclosures, landlord/lease documents, supplier/brand releases, regulator/consumer notices, product recall databases, franchise documents, payment/platform data, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Same-store sales, comparable sales, transaction count, basket size, conversion, footfall, or online penetration
- Store opening, closure, refurbishment, format change, franchise change, or geography expansion
- Named brand, product launch, exclusive range, private label, supplier, or distribution agreement
- Customer loyalty, membership, active customer, cohort, app usage, or marketplace seller metric
- Gross margin, markdowns, shrink, inventory, stock turn, supplier rebates, freight, rent, or labour cost fact
- Lease term, rent review, landlord dispute, concession, store impairment, or onerous lease fact
- Recall, product safety, food safety, regulator notice, wage underpayment, or consumer law issue
- Franchisee dispute, licence change, supply chain disruption, cyber/outage, or payment incident
- Acquisition, divestment, brand sale, restructuring, loyalty programme change, or channel partnership
- Named competitor store/network transaction or retail M&A comparable
- Broker factual reference or third-party channel data where source is named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "consumer_retail",
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
