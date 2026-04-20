# Insurance Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/insurance.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: insurance
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
INSURANCE_TYPE: [general | life | health | reinsurer | broker | diversified]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream insurance enrichment pipeline.

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
Prioritize statutory filings, prudential regulator disclosures, solvency reports, actuarial reports, rating agency actions, catastrophe updates, reinsurance placement documents, product disclosure statements, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Combined ratio, loss ratio, expense ratio, claims inflation, reserve release/strengthening, or prior-year development
- Catastrophe event exposure, named peril, geography, gross/net loss estimate, or reinsurance recovery
- Premium rate change, policy count, lapse, retention, new business, or product mix fact
- Named claims issue, class action, remediation, or adverse court/regulator decision
- Regulatory capital, solvency ratio, PCA/APRA/Lloyds/RBC metric, or capital buffer
- Reinsurance tower, quota share, excess-of-loss, aggregate cover, retention, limit, or named reinsurer
- Debt, hybrid, dividend, buyback, capital raise, or rating agency action
- Asset portfolio, duration, credit quality, ALM, or investment yield fact from named source
- Broker, agency, bancassurance, affinity, aggregator, or distribution agreement
- Licence, regulator action, underwriting exit/entry, product withdrawal, or market restriction
- Named executive, chief actuary, auditor, regulator, or risk committee change

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "insurance",
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
