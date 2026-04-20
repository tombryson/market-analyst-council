# Gaming Interactive Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/gaming-interactive.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: gaming_interactive
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
GAMING_TYPE: [mobile | console | PC | live service | publisher | developer | platform | mixed]
LEAD_TITLE_OR_FRANCHISE: [LEAD_TITLE_OR_FRANCHISE]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream gaming interactive enrichment pipeline.

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
Prioritize platform store pages, publisher/developer releases, licence/IP documents, app analytics where named, regulator notices, age-rating boards, esports/league documents, studio acquisition filings, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Named title launch, delay, update, season, DLC, platform release, rating, or geographic release
- MAU, DAU, downloads, bookings, ARPDAU, retention, conversion, payer rate, or engagement metric
- Platform agreement with Apple, Google, Steam, Sony, Microsoft, Nintendo, Epic, Roblox, or other named platform
- IP licence, publishing agreement, co-development, engine, middleware, or distribution arrangement
- Live-service event, battle pass, loot box, subscription, advertising, in-app purchase, or pricing change
- User acquisition, marketing partner, influencer/esports agreement, community event, or churn issue
- Studio acquisition, closure, redundancy, leadership change, production milestone, or outsourcing partner
- Security, cheating, outage, content moderation, age rating, gambling/loot-box regulation, or platform policy issue
- Franchise licence, film/TV tie-in, merchandising, regional publishing, or government grant
- Named peer transaction, title comparable, or broker factual reference where source is named
- App store ranking or third-party usage dataset only if provider and date are named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "gaming_interactive",
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
