# Medtech Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/medtech.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: medtech
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
DEVICE_CATEGORY: [DEVICE_CATEGORY]
LEAD_PRODUCT: [LEAD_PRODUCT]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream medtech enrichment pipeline.

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
Prioritize FDA 510(k)/PMA/de novo records, CE mark/MDR notices, TGA/UKCA approvals, clinical registries, reimbursement schedules, hospital/customer releases, distributor agreements, recall/adverse-event databases, manufacturing certifications, and company filings.

Target source material that can answer these downstream enrichment questions:
- FDA, CE, TGA, UKCA, PMDA, or other device approval/clearance with device name and indication
- Clinical validation study, registry, endpoint, investigator, site, or publication
- Recall, adverse event, field safety notice, warning letter, or quality-system issue
- Reimbursement code, payer coverage, DRG, CPT, HCPCS, MBS, NICE, CMS, or procurement decision
- Named hospital, health system, distributor, GPO, surgeon group, or clinical customer
- Installed base, procedure volume, utilization, consumables pull-through, or reorder metric
- Distribution, reseller, OEM, integration, or channel agreement
- Training, credentialing, KOL, reference site, or centre-of-excellence arrangement
- Named contract manufacturer, sterilization provider, component supplier, or quality certification
- Manufacturing scale-up, capacity expansion, transfer, bottleneck, or remediation item
- IP, patent, licence, exclusivity, or freedom-to-operate fact
- Grant, procurement tender, government programme, or clinical guideline inclusion

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "medtech",
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
