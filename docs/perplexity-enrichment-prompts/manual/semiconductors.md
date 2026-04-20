# Semiconductors Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/semiconductors.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: semiconductors
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
SEMICONDUCTOR_TYPE: [fabless | foundry | equipment | materials | memory | analog | mixed | other]
PRIMARY_NODE_OR_PRODUCT: [PRIMARY_NODE_OR_PRODUCT]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream semiconductors enrichment pipeline.

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
Prioritize customer/design-win releases, foundry documents, export-control notices, equipment shipment data, standards/certification documents, supply agreements, capacity announcements, government grant programmes, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Named design win, socket, platform, customer, OEM, hyperscaler, automotive programme, or qualification
- Product launch, node, process, package, substrate, memory type, wafer size, or technical standard
- Backlog, book-to-bill, inventory, lead time, cancellation, allocation, or customer concentration fact
- Pricing, long-term supply agreement, take-or-pay, minimum purchase, or capacity reservation
- Fab, foundry, OSAT, substrate supplier, equipment vendor, materials supplier, or manufacturing partner
- Capacity utilization, wafer starts, capex, tool installation, ramp, yield, bottleneck, or shutdown
- Export control, licence, CHIPS Act/grant, national security, sanctions, or jurisdiction restriction
- Quality issue, recall, reliability, qualification delay, or certification fact
- Acquisition, JV, divestment, technology licence, IP dispute, patent, or standards-body event
- Government programme, customer prepayment, financing, facility, or incentive
- Named broker factual reference, industry dataset, or peer transaction where source is named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "semiconductors",
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
