# Software / SaaS Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/software-saas.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: software_saas
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
SOFTWARE_CATEGORY: [SaaS | marketplace | payments | cybersecurity | data platform | vertical software | other]
PRIMARY_PRODUCT: [PRIMARY_PRODUCT]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream software / saas enrichment pipeline.

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
Prioritize customer/partner releases, product documentation, pricing pages, trust/compliance pages, security notices, regulator/court records, marketplace listings, hyperscaler partner directories, company filings, and named broker research only for factual references.

Target source material that can answer these downstream enrichment questions:
- Named customer win, renewal, expansion, churn, downsell, delayed rollout, or non-renewal
- ARR, NRR, GRR, churn, ACV, seat count, usage, deployment, cohort, or customer concentration fact
- Named reseller, channel, marketplace, hyperscaler, implementation partner, OEM, or embedded distribution arrangement
- Pricing, packaging, SKU, contract term, minimum commitment, or usage-based pricing change
- Product launch, module release, AI/data/model partnership, integration, API, or platform expansion
- Named go-live, rollout milestone, migration, implementation timeline, or services partner
- Cloud hosting, data residency, infrastructure, vendor dependency, or architecture fact
- Security incident, outage, reliability event, SOC 2, ISO 27001, FedRAMP, HIPAA, PCI, or compliance milestone
- Acquisition, divestment, product sunset, founder/executive change, or restructuring
- App store, marketplace, developer ecosystem, certification, patent, or standards-body fact
- Named broker factual reference or third-party customer survey where source is named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "software_saas",
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
