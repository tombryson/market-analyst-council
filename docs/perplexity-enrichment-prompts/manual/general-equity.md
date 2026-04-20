# General Equity Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/general-equity.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: general_equity
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
COMPANY_TYPE: [COMPANY_TYPE]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream general equity enrichment pipeline.

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
Prioritize exchange filings, annual/interim reports, regulator/court records, customer/supplier releases, government programme documents, certification bodies, rating agency actions, and named company/counterparty materials.

Target source material that can answer these downstream enrichment questions:
- Named customer, supplier, partner, contract, tender, renewal, loss, concentration, or dependency
- Operational metric, site, capacity, utilization, production, volume, backlog, or delivery fact
- Product/service launch, market entry/exit, certification, licence, accreditation, or standard
- Supply chain, logistics, inventory, working capital, quality, recall, cyber, or outage event
- Debt facility, covenant, maturity, security, lender, capital raise, buyback, dividend, or escrow fact
- Tax asset, grant, subsidy, government programme, deferred payment, royalty, earnout, or contingent liability
- Acquisition, divestment, JV, restructuring, impairment, litigation, settlement, or warranty fact
- Substantial holders, director holdings, related-party transactions, or governance change
- Regulator, court, tribunal, licence, environmental, consumer, labour, privacy, or competition decision
- Index inclusion/exclusion, rating agency action, award, certification, or government endorsement
- Named broker factual reference, peer transaction, or industry dataset where source is named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "general_equity",
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
