# Bank Financials Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/bank-financials.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: bank_financials
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
BANK_TYPE: [retail bank | regional bank | lender | neobank | diversified bank | other]
PRIMARY_MARKET: [PRIMARY_MARKET]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream bank financials enrichment pipeline.

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
Prioritize prudential regulator filings, Basel/Pillar 3 reports, annual reports, credit rating agency actions, central bank notices, APRA/PRA/Fed/ECB/OCC/ASIC actions, covered bond or securitization documents, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- CET1, Tier 1, total capital, leverage, LCR, NSFR, liquidity, or management buffer from a named source
- Dividend restriction, buyback approval, capital raise, AT1/Tier 2 issuance, or regulatory capital action
- Wholesale funding, deposit mix, maturity ladder, securitization, covered bond, or refinancing fact
- Credit rating agency action with agency, instrument, date, and rationale facts
- Loan book segmentation by product, geography, sector, LVR, FICO/score band, or collateral type
- Arrears, NPL, impaired loans, provisioning, write-off, forbearance, or watchlist metric
- NIM, deposit beta, cost of funds, hedge book, or repricing fact from named source
- Concentration to named borrower, sector, geography, broker channel, or funding source
- Regulator investigation, remediation programme, enforceable undertaking, consent order, or licence condition
- Acquisition, branch sale, book purchase, platform migration, outage, cyber event, or systems remediation
- Named executive, board, risk committee, auditor, or regulator change affecting bank governance

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "bank_financials",
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
