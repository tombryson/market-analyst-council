# Gambling / Wagering Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/gambling-wagering.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: gambling
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
GAMBLING_TYPE: [sportsbook | casino | lottery | gaming machines | online wagering | mixed]
PRIMARY_JURISDICTION: [PRIMARY_JURISDICTION]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream gambling / wagering enrichment pipeline.

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
Prioritize gambling regulator databases, licence documents, tax authority notices, responsible-gambling enforcement records, market share datasets with named providers, sports league agreements, payment provider notices, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Handle, turnover, net gaming revenue, gross gaming revenue, hold margin, active customers, staking, or yield metric
- Sportsbook/casino/lottery/product mix, online/retail split, VIP exposure, market share, or cohort fact
- Named sports league, racing body, casino, platform provider, payment provider, affiliate, or distribution partner
- Promotional intensity, bonus, loyalty, retention, churn, or customer acquisition metric from named source
- Licence grant, renewal, suspension, condition, market entry/exit, or jurisdiction restriction
- Point-of-consumption tax, gaming duty, levy, AML/KYC, sanctions, responsible-gambling, or affordability-check change
- Fine, remediation, enforceable undertaking, investigation, complaint, or regulator finding
- Payment processing, chargeback, fraud, cyber, data privacy, or platform outage event
- Acquisition, divestment, JV, white-label, technology platform, odds/feed, or media partnership
- Government tender, lottery concession, casino concession, venue agreement, or machine entitlement
- Named peer transaction, regulator market statistics, or broker factual reference

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "gambling",
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
