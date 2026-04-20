# Fixed Income / Credit Stage 1 Retrieval Brief

This is the prompt to paste into Perplexity Deep Research or another active research UI. It is intentionally retrieval-focused. Use the returned source packet as the attachment/input for the normal enrichment or council flow.

The detailed Stage 2 extractor version is preserved in `extraction/fixed-income-credit.md` for advanced/manual extraction use.

## Copy/Paste Prompt

```text
ASSET_CLASS: fixed_income
EXCHANGE: [EXCHANGE_CODE]
TICKER: [EXCHANGE_CODE]:[TICKER]
COMPANY: [COMPANY_NAME]
INSTRUMENT_TYPE: [bond | note | hybrid | loan | listed credit | fund | preferred | other]
ISSUER_OR_BORROWER: [ISSUER_OR_BORROWER]

Research [COMPANY_NAME] ([EXCHANGE_CODE]:[TICKER]) for a downstream fixed income / credit enrichment pipeline.

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
Prioritize offering memoranda, trust deeds, indentures, ASX/SEC/SEDAR filings, rating agency reports/actions, lender notices, covenant waiver documents, administrator/court records, distribution statements, and exchange filings.

Target source material that can answer these downstream enrichment questions:
- Instrument name, ISIN, coupon, maturity, reset, call date, ranking, security, guarantor, or issuer
- Covenant, covenant test, waiver, amendment, event of default, acceleration, or consent solicitation
- Collateral, asset coverage, borrowing base, LTV, ICR, DSCR, reserve account, or guarantee fact
- Refinancing, tender offer, buyback, redemption, new issue, maturity extension, or facility amendment
- Credit rating action with agency, instrument, date, and stated rationale facts
- Liquidity, cash, undrawn facility, restricted cash, working capital, or funding gap fact
- Distribution, interest payment, deferral, suspension, arrears, PIK, or waterfall fact
- Duration, hedging, floating/fixed exposure, inflation linkage, or interest-rate sensitivity fact from named source
- Court, administrator, receiver, trustee, security agent, lender group, or restructuring advisor appointment
- Asset sale, covenant cure, standstill, scheme, restructuring plan, or enforcement action
- Named broker factual reference, comparable credit event, or market quotation only if source and date are named

Output exactly this JSON-compatible structure. The factual_summary arrays may contain detailed bullet strings, but keep them source-grounded.

{
  "company": "[COMPANY_NAME]",
  "ticker": "[EXCHANGE_CODE]:[TICKER]",
  "exchange": "[EXCHANGE_CODE]",
  "asset_class": "fixed_income",
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
