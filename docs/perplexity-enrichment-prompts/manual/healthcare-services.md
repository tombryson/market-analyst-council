# Healthcare Services Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/healthcare-services.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: healthcare_services
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
SERVICE_TYPE: [clinics | hospitals | diagnostics | aged care | dental | allied health | mental health | other]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream healthcare services enrichment pipeline.

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
Prioritize regulator/accreditation records, payer/reimbursement schedules, health department documents, hospital/clinic releases, acquisition documents, licensing databases, quality/safety notices, labour agreements, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Site count, beds, clinics, practitioners, patient volumes, occupancy, utilization, procedure volumes, or waitlist metric
- Payer mix, reimbursement rate, Medicare/MBS/CMS/private payer decision, contract, or tariff fact
- Named hospital, clinic, payer, insurer, government body, referral partner, or customer
- Staffing, wage award, agency labour, clinician recruitment, retention, or industrial action fact
- Accreditation, licence, inspection, quality rating, sanctions, remediation, or regulator notice
- Clinical incident, patient safety issue, class action, coronial finding, privacy/cyber incident, or complaint finding
- Service expansion, new specialty, equipment, diagnostic capability, or centre-of-excellence milestone
- Procurement tender, government programme, grant, public-private partnership, or outsourcing arrangement
- Acquisition, divestment, greenfield site, closure, integration, earnout, or vendor terms
- Lease, landlord, property, equipment finance, or capex commitment
- Named broker factual reference, peer transaction, or sector dataset where source is named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "healthcare_services",
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
