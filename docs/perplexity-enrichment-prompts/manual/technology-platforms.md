# Technology Platforms Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/technology-platforms.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: technology_platforms
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
PLATFORM_TYPE: [marketplace | payments | adtech | fintech | social | data | infrastructure | other]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream technology platforms enrichment pipeline.

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
Prioritize platform policy pages, partner/customer releases, regulator/court records, app marketplace data, payment network documents, API/developer documentation, trust/safety reports, privacy notices, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- GMV, TPV, active users, MAU/DAU, merchants, creators, advertisers, listings, or transaction volume
- Take rate, fee schedule, pricing change, seller fee, interchange, subscription, or commission fact
- Named merchant, partner, payment processor, data provider, network, API user, or distribution partner
- Cohort, retention, engagement, conversion, fraud, chargeback, dispute, or cancellation metric
- Product launch, API, developer tool, AI/data/model partnership, integration, or platform policy change
- App store, marketplace, certification, compliance, privacy, data residency, cybersecurity, or outage event
- Trust/safety action, content policy, consumer protection, AML/KYC, sanctions, or regulator issue
- Customer concentration, geographic market entry/exit, licence, or operating restriction
- Acquisition, divestment, strategic investment, JV, partnership, or ecosystem incentive programme
- Patent, standards-body, open-source dependency, data licence, or exclusivity fact
- Named broker factual reference or third-party platform dataset where source is named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "technology_platforms",
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
