# General Equity

- Template ID: `general_equity`
- Category: `general`
- Default exchange context: `ASX`
- Placeholder: `[company_name]`

## Full Deep Research Prompt

```md
Financial analysis framing:
- Template: general_equity
- Company name: [company_name].
- Company type: general_equity.
- Exchange: ASX.
- Gather evidence needed for scoring and investment judgment, not generic summaries.
- Prioritize recent primary documents with quantitative data.
- Run a dedicated management/governance evidence lane: board and executive bios, prior operating track record, insider ownership/alignment, leadership changes, and governance red flags.

- Run a compact sector-context lane (2-4 sources) on the company’s immediate industry backdrop: demand, pricing power, cost pressure, and funding conditions; tie each point to scenario assumptions.
Exchange assumptions:
Exchange profile: ASX (Australia). Prefer ASX announcements, quarterly/annual reports, Appendix 4D/4E/5B/5C, and investor presentations. Market data in AUD by default unless the company reports otherwise.

Template rubric:
Run an investment analysis on [company_name] following this rubric exactly. Keep the output investment-grade, explicit, and scenario-based. Use ASX-appropriate primary filings first and only use secondary sources where they add real value.

Template focus:
* Template ID: general_equity
* Template Name: General Equity
* Template Description: Cross-sector investment analysis template for listed equities when no specialist sector template is selected
* Analysis Type: general_equity

Required outputs:
* 12-month and 24-month price targets.
* Bull / base / bear scenario framework with probabilities that sum to 100% for each horizon.
* Quality Score out of 100 with explicit sub-factor scoring.
* Value Score out of 100 with explicit sub-factor scoring.
* Development / operating timeline to the next key milestones.
* Thesis Map with explicit conditions for bull, base, and bear paths.
* Monitoring Watchlist with red flags and confirmatory signals.
* Verification Queue listing unresolved facts that still matter to the thesis.
* Investment Recommendation: BUY / HOLD / SELL with conviction and a short justification.

Data sourcing rules:
* Use official exchange filings, official company investor materials, statutory reports, and primary operating disclosures first.
* Use market-data providers only for current price / market cap / share count / enterprise value inputs.
* For every key numeric input, state the value used, the source, the filing date, and whether it is estimated.
* If data is missing, estimate conservatively, say that it is estimated, and explain the basis in one line.

Sector research lanes:
  * Run a compact sector-context lane (2-4 sources) on the company’s immediate industry backdrop: demand, pricing power, cost pressure, and funding conditions; tie each point to scenario assumptions.

Step 1: Core operating and valuation workup
* Build the operating picture from current filings and primary disclosures.
* Identify the key revenue / margin / balance-sheet / capital-allocation drivers.
* Build a defendable valuation anchor appropriate to the sector.
* State the main assumptions that drive the 12-month and 24-month targets.

Step 2: Quality Score (0-100)
Score the company explicitly across these quality factors and show the weighted contribution from each:
  * Market Position
  * Operating Quality
  * Management
  * Execution
  * Balance Sheet
  * Governance
Quality score rules:
* Give a raw score for each factor.
* Explain what evidence supports the score.
* Penalize weak disclosure, funding risk, governance risk, or execution uncertainty.
* Do not hide missing evidence; call it out.

Step 3: Value Score (0-100)
Score the company explicitly across these value factors and show the weighted contribution from each:
  * Intrinsic Value vs Market Cap
  * Cash-Flow Yield
  * Relative Multiple
  * Growth Quality
  * Strategic Value
Value score rules:
* Tie the score back to concrete valuation anchors, not vague sentiment.
* Compare valuation to quality, timing, and balance-sheet risk.
* State where the market is overpricing or underpricing the setup.

Step 4: Scenario framework
For bull, base, and bear cases, provide:
* 12M target and 24M target.
* Probability.
* Short scenario summary.
* Current positioning (e.g. base-leaning, mixed, bear-leaning).
* Explicit conditions that must hold for each path.
* Explicit failure conditions that would invalidate that path.

Step 5: Monitoring and verification
Produce:
* Monitoring Watchlist:
  * Red Flags
  * Confirmatory Signals
* Verification Queue:
  * unresolved facts that should be checked in later filings or primary-source follow-up

Final instruction:
* This is not a generic company summary.
* Produce a decision-grade investment analysis aligned to the rubric above.
* Keep it specific, numeric where possible, and explicit about assumptions and unknowns.
```

## Core Rubric

```md
Run an investment analysis on [company_name] following this rubric exactly. Keep the output investment-grade, explicit, and scenario-based. Use ASX-appropriate primary filings first and only use secondary sources where they add real value.

Template focus:
* Template ID: general_equity
* Template Name: General Equity
* Template Description: Cross-sector investment analysis template for listed equities when no specialist sector template is selected
* Analysis Type: general_equity

Required outputs:
* 12-month and 24-month price targets.
* Bull / base / bear scenario framework with probabilities that sum to 100% for each horizon.
* Quality Score out of 100 with explicit sub-factor scoring.
* Value Score out of 100 with explicit sub-factor scoring.
* Development / operating timeline to the next key milestones.
* Thesis Map with explicit conditions for bull, base, and bear paths.
* Monitoring Watchlist with red flags and confirmatory signals.
* Verification Queue listing unresolved facts that still matter to the thesis.
* Investment Recommendation: BUY / HOLD / SELL with conviction and a short justification.

Data sourcing rules:
* Use official exchange filings, official company investor materials, statutory reports, and primary operating disclosures first.
* Use market-data providers only for current price / market cap / share count / enterprise value inputs.
* For every key numeric input, state the value used, the source, the filing date, and whether it is estimated.
* If data is missing, estimate conservatively, say that it is estimated, and explain the basis in one line.

Sector research lanes:
  * Run a compact sector-context lane (2-4 sources) on the company’s immediate industry backdrop: demand, pricing power, cost pressure, and funding conditions; tie each point to scenario assumptions.

Step 1: Core operating and valuation workup
* Build the operating picture from current filings and primary disclosures.
* Identify the key revenue / margin / balance-sheet / capital-allocation drivers.
* Build a defendable valuation anchor appropriate to the sector.
* State the main assumptions that drive the 12-month and 24-month targets.

Step 2: Quality Score (0-100)
Score the company explicitly across these quality factors and show the weighted contribution from each:
  * Market Position
  * Operating Quality
  * Management
  * Execution
  * Balance Sheet
  * Governance
Quality score rules:
* Give a raw score for each factor.
* Explain what evidence supports the score.
* Penalize weak disclosure, funding risk, governance risk, or execution uncertainty.
* Do not hide missing evidence; call it out.

Step 3: Value Score (0-100)
Score the company explicitly across these value factors and show the weighted contribution from each:
  * Intrinsic Value vs Market Cap
  * Cash-Flow Yield
  * Relative Multiple
  * Growth Quality
  * Strategic Value
Value score rules:
* Tie the score back to concrete valuation anchors, not vague sentiment.
* Compare valuation to quality, timing, and balance-sheet risk.
* State where the market is overpricing or underpricing the setup.

Step 4: Scenario framework
For bull, base, and bear cases, provide:
* 12M target and 24M target.
* Probability.
* Short scenario summary.
* Current positioning (e.g. base-leaning, mixed, bear-leaning).
* Explicit conditions that must hold for each path.
* Explicit failure conditions that would invalidate that path.

Step 5: Monitoring and verification
Produce:
* Monitoring Watchlist:
  * Red Flags
  * Confirmatory Signals
* Verification Queue:
  * unresolved facts that should be checked in later filings or primary-source follow-up

Final instruction:
* This is not a generic company summary.
* Produce a decision-grade investment analysis aligned to the rubric above.
* Keep it specific, numeric where possible, and explicit about assumptions and unknowns.
```

## Stage 1 Query Prompt

```md
Run an investment analysis on [company_name] following this rubric exactly. Keep the output investment-grade, explicit, and scenario-based. Use ASX-appropriate primary filings first and only use secondary sources where they add real value.

Template focus:
* Template ID: general_equity
* Template Name: General Equity
* Template Description: Cross-sector investment analysis template for listed equities when no specialist sector template is selected
* Analysis Type: general_equity

Required outputs:
* 12-month and 24-month price targets.
* Bull / base / bear scenario framework with probabilities that sum to 100% for each horizon.
* Quality Score out of 100 with explicit sub-factor scoring.
* Value Score out of 100 with explicit sub-factor scoring.
* Development / operating timeline to the next key milestones.
* Thesis Map with explicit conditions for bull, base, and bear paths.
* Monitoring Watchlist with red flags and confirmatory signals.
* Verification Queue listing unresolved facts that still matter to the thesis.
* Investment Recommendation: BUY / HOLD / SELL with conviction and a short justification.

Data sourcing rules:
* Use official exchange filings, official company investor materials, statutory reports, and primary operating disclosures first.
* Use market-data providers only for current price / market cap / share count / enterprise value inputs.
* For every key numeric input, state the value used, the source, the filing date, and whether it is estimated.
* If data is missing, estimate conservatively, say that it is estimated, and explain the basis in one line.

Sector research lanes:
  * Run a compact sector-context lane (2-4 sources) on the company’s immediate industry backdrop: demand, pricing power, cost pressure, and funding conditions; tie each point to scenario assumptions.

Step 1: Core operating and valuation workup
* Build the operating picture from current filings and primary disclosures.
* Identify the key revenue / margin / balance-sheet / capital-allocation drivers.
* Build a defendable valuation anchor appropriate to the sector.
* State the main assumptions that drive the 12-month and 24-month targets.

Step 2: Quality Score (0-100)
Score the company explicitly across these quality factors and show the weighted contribution from each:
  * Market Position
  * Operating Quality
  * Management
  * Execution
  * Balance Sheet
  * Governance
Quality score rules:
* Give a raw score for each factor.
* Explain what evidence supports the score.
* Penalize weak disclosure, funding risk, governance risk, or execution uncertainty.
* Do not hide missing evidence; call it out.

Step 3: Value Score (0-100)
Score the company explicitly across these value factors and show the weighted contribution from each:
  * Intrinsic Value vs Market Cap
  * Cash-Flow Yield
  * Relative Multiple
  * Growth Quality
  * Strategic Value
Value score rules:
* Tie the score back to concrete valuation anchors, not vague sentiment.
* Compare valuation to quality, timing, and balance-sheet risk.
* State where the market is overpricing or underpricing the setup.

Step 4: Scenario framework
For bull, base, and bear cases, provide:
* 12M target and 24M target.
* Probability.
* Short scenario summary.
* Current positioning (e.g. base-leaning, mixed, bear-leaning).
* Explicit conditions that must hold for each path.
* Explicit failure conditions that would invalidate that path.

Step 5: Monitoring and verification
Produce:
* Monitoring Watchlist:
  * Red Flags
  * Confirmatory Signals
* Verification Queue:
  * unresolved facts that should be checked in later filings or primary-source follow-up

Final instruction:
* This is not a generic company summary.
* Produce a decision-grade investment analysis aligned to the rubric above.
* Keep it specific, numeric where possible, and explicit about assumptions and unknowns.
```
