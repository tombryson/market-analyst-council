# Energy / Oil and Gas Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/energy-oil-gas.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: energy_oil_gas
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
HYDROCARBON_TYPE: [oil | gas | LNG | coal seam gas | mixed | other]
PRIMARY_BASIN_OR_ASSET: [PRIMARY_ASSET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream energy / oil and gas enrichment pipeline.

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
Prioritize reserve reports, PRMS disclosures, regulator filings, permit databases, operator/partner releases, gas sales agreements, offtake documents, pipeline/terminal documents, environmental approvals, RBL documents, hedge disclosures, and named technical auditor reports.

Target source material that can answer these downstream enrichment questions:
- Reserve or resource report with auditor, effective date, and reserve class
- Named field, basin, block, licence, permit, operator, and working interest
- Development plan, FID, sanction, restart, workover, or phased project milestone
- Production, decline, outage, maintenance, or restart event from a named source
- Gas sales, LNG, crude offtake, marketing, transport, processing, gathering, pipeline, or terminal arrangement
- Hedge, collar, swap, floor, fixed-price sale, or contract pricing structure
- Reserve-based lending, borrowing base, redetermination, project debt, or covenant fact
- JV, farm-in, farm-out, operatorship change, royalty, PSC, tariff, or fiscal-term fact
- Permit, licence, environmental approval, drilling approval, or regulator notice
- Litigation, arbitration, licence dispute, spill, remediation, abandonment, plugging, restoration, or decommissioning liability
- Named technical auditor, reserves engineer, operator, drilling contractor, rig contractor, or facilities contractor
- Substantial holders, strategic investors, government entities, partners, or counterparties
- Peer transaction or asset sale in same basin or hydrocarbon type with named buyer/seller and metric
- Broker factual references, coverage notes, or named third-party reserve comparisons

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "energy_oil_gas",
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
