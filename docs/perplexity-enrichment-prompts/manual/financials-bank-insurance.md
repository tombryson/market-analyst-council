# Financials / Bank and Insurance Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/financials-bank-insurance.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: financials_bank_insurance
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
FINANCIAL_TYPE: [bank | insurer | broker | asset finance | diversified financials | other]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream financials / bank and insurance enrichment pipeline.

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
Prioritize prudential/regulatory filings, solvency/capital reports, credit rating actions, central bank or insurance regulator notices, debt documents, reinsurance disclosures, distribution agreements, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- CET1, solvency, capital adequacy, leverage, liquidity, LCR, regulatory capital, or capital buffer
- Debt, hybrid, securitization, reinsurance, facility, covenant, maturity, or rating agency action
- Dividend, buyback, capital raise, regulatory restriction, or remediation programme
- Loan book, premium book, claims, NPL, arrears, provisioning, loss ratio, combined ratio, or reserve fact
- Customer, broker, distribution, product, geography, sector, or concentration fact
- Hedge, funding cost, deposit mix, claims inflation, catastrophe, or reserve-development fact
- Regulator investigation, licence condition, enforceable undertaking, court case, remediation, or compliance breach
- Executive, board, chief risk officer, actuary, auditor, or regulator change
- Acquisition, portfolio sale, book purchase, reinsurance transaction, or market exit

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "financials_bank_insurance",
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
