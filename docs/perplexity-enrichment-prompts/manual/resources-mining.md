# Resources / Mining Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/resources-mining.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: mining
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
COMMODITY: [PRIMARY_COMMODITY]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream resources / mining enrichment pipeline.

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
ASX/SEDAR/RNS/JSE/HKEX/SEC filings; quarterly activities reports; DFS/PFS/FS releases; JORC/NI 43-101/SAMREC reports; competent person reports; environmental approvals; mining leases; land access and heritage agreements; offtake, royalty, streaming, EPCM/EPC, drilling, processing, water, power, port, rail, and road documents; substantial holder notices; government grants; named counterparty documents.

Target source material that can answer these downstream enrichment questions:
- Carried tax losses or deferred tax assets (quantum and origin)
- Deferred payments due at a named trigger event (FID, production,
- Net smelter return royalty or third-party royalty structures
- Streaming or silver/gold streaming agreements with named
- Project finance debt sizing assumptions from named advisors
- Power supply arrangement: named contract type (BOO/BOT/PPA),
- Water supply arrangement: named source, permit reference,
- Processing infrastructure: any toll milling, shared plant,
- Accommodation: named counterparty, lease terms, site location
- Port, rail, or road access: named infrastructure owner or
- Named indigenous land use agreement (ILUA) or equivalent:
- Named heritage agreement or cultural heritage management plan:
- Native title determination outcome: court or tribunal name,
- Remaining land access disputes or compensation processes:
- Competent person(s) for mineral resource estimate: name,
- Competent person(s) for ore reserve estimate: name and firm
- Independent technical report author(s) for DFS/PFS: firm name
- Named EPCM or EPC firm engaged or shortlisted

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "mining",
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
