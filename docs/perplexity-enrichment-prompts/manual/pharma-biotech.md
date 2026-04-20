# Pharma / Biotech Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/pharma-biotech.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: pharma_biotech
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
PRIMARY_MODALITY: [small molecule | biologic | cell therapy | gene therapy | vaccine | diagnostic | platform | other]
LEAD_PROGRAM: [LEAD_PROGRAM]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream pharma / biotech enrichment pipeline.

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
Prioritize clinical trial registries, FDA/EMA/TGA/MHRA notices, peer-reviewed abstracts, conference abstracts, company filings, partner/licensor releases, patent office records, GMP or inspection documents, reimbursement decisions, and named CRO/CDMO documents.

Target source material that can answer these downstream enrichment questions:
- Named active trial with identifier, phase, indication, endpoint, and sponsor
- Enrollment status, site count, geography, cohort, dose, or randomization fact
- Topline, interim, final, or safety readout date from a named source
- Named principal investigator, lead site, CRO, DSMB, or trial operator
- FDA/EMA/TGA/MHRA/PMDA designation, meeting, clearance, acceptance, hold, protocol amendment, or guidance
- IND, CTA, NDA, BLA, MAA, 510(k), PMA, or equivalent submission/acceptance
- Adverse event, serious adverse event, black box, REMS, warning letter, recall, or clinical hold
- Companion diagnostic, biomarker, assay, or named diagnostic partner
- Named CDMO, CMO, API supplier, fill-finish provider, tech transfer, scale-up, or GMP milestone
- Patent family, expiry, exclusivity, licence field, territory, royalty, milestone, or composition-of-matter fact
- Named university, institute, licensor, collaborator, or platform partner
- Partnering, option, licence, co-development, distribution, or commercialization agreement
- Grant, government programme, reimbursement decision, payer coverage, or named health technology assessment
- Cash runway, facility, placement, milestone payment, royalty financing, or debt instrument from a named filing/source

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "pharma_biotech",
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
