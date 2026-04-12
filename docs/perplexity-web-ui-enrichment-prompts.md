# Perplexity Web UI Enrichment Prompts

These are copy-paste prompt bodies for manual use in the Perplexity web UI.

Placeholders:
- `[company_name]`
- `[exchange]`
- `[ticker]`

## Resources / Mining

```md
Run an investment analysis on [company_name] following this rubric exactly. Do not deviate unless data is unavailable; if data is missing, state assumptions explicitly and continue. Adjust for polymetallic resource equivalents where relevant.

Include:
- 12-month and 24-month price targets with bull/base/bear scenarios.
- Quality and Value scores out of 100 using the formulas and scoring tables below.
- Current development stage and timeline to key milestones (specific date or quarter).
- Quantitative and qualitative headwinds/tailwinds with explicit thresholds and directional valuation impact.
- Investment recommendation (BUY/HOLD/SELL) + conviction (HIGH/MEDIUM/LOW) + one-paragraph justification.
- Key risks and key opportunities.

Data sourcing rules:
- Source market data from official exchange sources and reliable market-data providers for [exchange].
- Source project data from primary filings, company presentations, PFS/DFS/FS materials, and technical reports.
- For every key numeric input used in NPV, Quality, or Value scoring, provide:
  1. value used
  2. source URL + document date
  3. ESTIMATE tag with one-line justification if inferred.
- Use current spot commodity pricing and relevant FX conversion where needed; state source and timestamp.

Document review:
For each major document (investor presentation, quarterly, DFS/PFS/FS update), provide:
- document title/date/announcement reference
- 4-6 extracted key points
- relevance to valuation inputs
- implications for investment outlook
- one paragraph on whether market pricing reflects the document’s information

Step 1: Project-Level NPV Calculation
For each major project (up to 3):
- Populate inputs: Resource Tonnes, Grade, Recovery, Mine Life, Annual Production, AISC, Initial Capex, Sustaining Capex, Discount Rate (5%), spot commodity price, study/reference price, Royalty, Tax, Working Capital (5%), Ramp-up, Ownership (%).
- If missing, estimate conservatively and justify.
- Show formulas explicitly.
- Output:
  - post-tax NPV at study/reference price
  - post-tax NPV at current spot price
  - attributable NPV
  - probability-weighted NPV
  - risked NPV
  - equity value/share
  - discount/premium to current share price

Stage multiplier:
Development Base:
- Scoping no MRE: 0.10
- Scoping has MRE: 0.15
- PFS: 0.25
- DFS complete: 0.42
- FEED underway: 0.47
- FEED complete: 0.54
- FID declared / construction commencing: 0.62
- Construction >50%: 0.72
- First production / commissioning: 0.82
- Ramp-up: 0.91
- Peak production: 1.00

Funding Adjustment:
- No funding, gap >50% of capex: ×0.70
- Partial funding, gap 15–50%: ×0.85
- Mostly funded, gap <15%: ×0.95
- Fully funded: ×1.05
- Fully funded with strong buffer: ×1.10

Final Multiplier = Development Base × Funding Adjustment

Step 2: Quality Score (0-100)
Formula:
- 20% Jurisdiction
- 10% Infrastructure
- 20% Management
- 10% Development Stage
- 30% Funding
- 10% ESG

Score each factor with:
- raw score
- weight
- weighted contribution
- evidence

Step 3: Value Score (0-100)
Formula:
- 30% NPV / Market Cap
- 20% EV / Resource
- 20% Exploration Upside
- 15% Cost Competitiveness
- 15% M&A / Strategic Value

Step 4: Required outputs
Provide:
- 12M and 24M bull/base/bear targets
- scenario probabilities
- probability-weighted targets
- target reconciliation
- development timeline
- headwinds/tailwinds
- thesis map
- monitoring watchlist
- verification queue

Final instruction:
This is not a generic company summary. Produce a decision-grade investment analysis with explicit assumptions, scenario logic, and valuation anchors.
```

## Pharma / Biotech

```md
Run an investment analysis on [company_name] following this rubric exactly.

Include:
- 12-month and 24-month price targets
- Quality and Value scores out of 100
- summary of current drug pipeline
- timeline to key milestones
- certainty percentage for achieving stated goals within 24 months
- quantitative and qualitative headwinds/tailwinds
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Data sourcing rules:
- Source market data from official exchange websites and reliable financial data providers.
- Source scientific and financial data from the latest investor presentations, annual reports, SEC/[exchange] filings, and clinical trial registry data.
- For every key numeric input, provide source and note estimates clearly.

Step 1: Drug Candidate rNPV Calculation
For each major drug candidate in the pipeline (up to 3), populate:
- Target Patient Population
- Peak Market Share
- Gross Annual Price
- Net Price after Rebates/Discounts
- Effective Patent Life
- COGS + SG&A (% of revenue)
- Remaining R&D Costs to Launch
- Discount Rate: 10%
- Royalty Rate Payable
- Tax Rate: 30%
- Post-Launch R&D / Lifecycle Management
- Ramp-up Years to Peak Sales

Probability of Success multiplier:
- Pre-Clinical: 0.08
- Phase 1: 0.15
- Phase 2: 0.35
- Phase 3: 0.65
- Submitted for Approval: 0.90
- Approved/Marketed: 1.00

Step 2: Quality Score (0-100)
- 20% Regulatory Environment
- 10% Scientific & Manufacturing Capability
- 15% Management Quality
- 15% Pipeline Maturity
- 20% Cash Runway & Funding
- 10% Certainty of 12M goals
- 10% Clinical & Ethical Standards

Step 3: Value Score (0-100)
- 30% rNPV vs Market Cap
- 20% EV / Risk-Adjusted Peak Sales
- 20% Pipeline & Platform Potential
- 15% Market Positioning & Moat
- 15% M&A / Strategic Value

Step 4: Required outputs
Provide:
- 12M / 24M bull/base/bear targets
- scenario probabilities
- probability-weighted targets
- development timeline
- key readouts / filings / approvals
- headwinds/tailwinds
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Final instruction:
Do not produce a generic biotech summary. Build a scenario-based, catalyst-aware investment analysis anchored in rNPV, funding runway, readout timing, and regulatory probability.
```

## Software / SaaS

```md
Run an investment analysis on [company_name] following this rubric exactly. Keep the output investment-grade, explicit, and scenario-based.

Required outputs:
- 12-month and 24-month price targets
- bull / base / bear scenarios with probabilities
- Quality Score out of 100
- Value Score out of 100
- development / operating timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Data sourcing rules:
- Use official exchange filings, company presentations, statutory reports, and primary operating disclosures first.
- Use market-data providers only for current price / market cap / share count / enterprise value inputs.
- For every key numeric input, state the value used, source, filing date, and whether it is estimated.

Sector research lane:
- Capture recent sources on demand environment, enterprise IT budget trends, competitive pricing pressure, and valuation regime for growth software; tie each point to scenario assumptions.

Step 1: Core operating and valuation workup
- Build the operating picture from current filings and primary disclosures.
- Focus on ARR, revenue growth, NRR, churn, gross margin, EBITDA/FCF trajectory, CAC efficiency, and rule of 40.
- Build a defendable valuation anchor using EV/Sales, margin path, FCF inflection, and retention economics.

Step 2: Quality Score (0-100)
Score explicitly across:
- Market & Demand Quality
- Product Moat
- Management
- Growth Quality
- Balance Sheet
- Governance

Step 3: Value Score (0-100)
Score explicitly across:
- Intrinsic Value vs Market Cap
- EV/Sales vs Growth
- Rule of 40 & Margin Path
- Retention Economics
- Strategic Value

Step 4: Scenario framework
For bull/base/bear provide:
- 12M and 24M target
- probability
- summary
- current positioning
- explicit conditions
- explicit failure conditions

Step 5: Monitoring and verification
Produce:
- Monitoring Watchlist:
  - Red Flags
  - Confirmatory Signals
- Verification Queue

Final instruction:
This is not a generic software company summary. Build the analysis around growth durability, unit economics, retention, valuation regime, and timing to profitability.
```

## Energy / Oil & Gas

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- development / operating timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Sector research lane:
- Capture recent sources on Brent/WTI/Henry Hub trend, OPEC+ policy, geopolitical supply risks, inventories/demand, and tie each point to scenario assumptions.

Step 1: Core operating and valuation workup
- Build the asset-level operating picture from filings.
- Focus on production, reserves, decline rates, lifting cost, breakeven, capex, netbacks, hedge book, and balance sheet.
- Build valuation using NAV, EV/2P reserves, EV/production, and FCF yield at strip.

Step 2: Quality Score (0-100)
Score:
- Jurisdiction
- Asset Quality
- Cost Position
- Management
- Funding / Balance Sheet

Step 3: Value Score (0-100)
Score:
- NAV vs Market Cap
- EV / 2P Reserves
- EV / Production
- FCF Yield at Strip
- Strategic Value

Step 4: Scenario framework
For each scenario provide explicit oil/gas price assumptions, operating assumptions, and balance-sheet implications.

Final instruction:
Do not write a generic energy summary. Build a price-sensitive, reserve-aware, capital-discipline-aware investment analysis.
```

## Banks

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- operating timeline / key milestones
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Sector research lane:
- Capture recent sources on yield curve, funding/deposit competition, credit cycle, and regulatory capital backdrop; tie each point to scenario assumptions.

Step 1: Core operating and valuation workup
- Focus on NIM, loan growth, deposit mix, CET1/capital, arrears, provisions, ROE, and capital return.
- Build valuation using P/B, earnings power, sustainable ROE, and capital return capacity.

Step 2: Quality Score (0-100)
Score:
- Regulatory Environment
- Franchise Quality
- Management
- Asset / Credit Quality
- Capital & Liquidity
- Governance

Step 3: Value Score (0-100)
Score:
- Intrinsic Value vs Market Cap
- P/B vs ROE
- Earnings Power vs Price
- Capital Return Capacity
- Strategic Value

Final instruction:
This is not a generic bank summary. Build it around balance-sheet strength, earnings durability, credit quality, and valuation versus sustainable returns.
```

## Insurance

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- operating timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Sector research lane:
- Capture recent sources on pricing cycle, claims inflation, catastrophe exposure, reserve trends, and investment income backdrop.

Step 1: Core operating and valuation workup
- Focus on gross written premium growth, combined ratio, loss ratio, expense ratio, reserve adequacy, reinsurance, catastrophe exposure, and capital return.
- Build valuation using P/B, ROE, earnings power, and capital generation.

Step 2: Quality Score (0-100)
Score:
- Regulatory Environment
- Underwriting Quality
- Management
- Reserve Adequacy
- Capital & Liquidity
- Governance

Step 3: Value Score (0-100)
Score:
- Intrinsic Value vs Market Cap
- P/B vs ROE
- Earnings Power vs Price
- Capital Return Capacity
- Strategic Value

Final instruction:
Do not produce a generic insurer summary. Build the analysis around underwriting quality, reserve strength, catastrophe risk, and valuation versus sustainable returns.
```

## Real Estate / REIT

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- development / operating timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Sector research lane:
- Capture recent sources on cap-rate regime, leasing demand, vacancy/occupancy trends, financing costs, and transaction comps.

Step 1: Core operating and valuation workup
- Focus on occupancy, WALE, leasing spreads, rent growth, same-property NOI, cap rates, asset values, LTV, refinancing risk, and AFFO/FFO.
- Build valuation using NAV vs market cap, FFO/AFFO yield, and peer multiples.

Step 2: Quality Score (0-100)
Score:
- Asset Quality
- Occupancy & Lease Quality
- Management
- Execution
- Balance Sheet
- Governance

Step 3: Value Score (0-100)
Score:
- NAV vs Market Cap
- FFO / AFFO Yield
- Multiple vs Peers
- Growth / Reinvestment Quality
- Strategic Value

Final instruction:
This is not a generic property summary. Build the analysis around asset quality, leasing durability, financing risk, and discount/premium to NAV.
```

## Industrials

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- development / operating timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Sector research lane:
- Capture recent sources on capex cycle, order intake/backlog, cost inflation, end-market demand, and execution risk.

Step 1: Core operating and valuation workup
- Focus on revenue growth, backlog/orderbook, margin path, working capital, capex, free cash flow, and customer concentration.
- Build valuation using EV/EBITDA, FCF yield, and cycle-adjusted earnings power.

Step 2: Quality Score (0-100)
Score:
- Market / End-Market Quality
- Asset / Plant Quality
- Management
- Execution
- Balance Sheet
- Governance

Step 3: Value Score (0-100)
Score:
- Intrinsic Value vs Market Cap
- EV/EBITDA vs Growth
- Margin Recovery / Earnings Power
- Cash Conversion
- Strategic Value
```

## Consumer / Retail

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- operating timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Sector research lane:
- Capture recent sources on consumer demand, pricing power/promotions, inventory conditions, category mix, and margin pressure.

Step 1: Core operating and valuation workup
- Focus on same-store sales, traffic, basket, gross margin, inventory, markdown risk, store rollout/closures, and free cash flow.
- Build valuation using EV/EBITDA, earnings power, and FCF yield.

Step 2: Quality Score (0-100)
Score:
- Demand Quality
- Brand / Category Strength
- Management
- Execution
- Balance Sheet
- Governance

Step 3: Value Score (0-100)
Score:
- Intrinsic Value vs Market Cap
- Multiple vs Growth
- Margin / Earnings Power
- Cash Conversion
- Strategic Value
```

## Datacentres

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- development / commissioning timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Sector research lane:
- Capture recent sources on hyperscaler demand, power/grid constraints, financing environment, pre-commitments, and construction cost trends.

Step 1: Core operating and valuation workup
- Focus on MW capacity, pre-commitments, utilization, power access, build cost, yield on cost, funding, and customer concentration.
- Build valuation using NAV vs market cap, EV/EBITDA where mature, and development value where not yet stabilized.

Step 2: Quality Score (0-100)
Score:
- Asset / Location Quality
- Customer & Contract Quality
- Management
- Delivery / Execution
- Balance Sheet
- Governance

Step 3: Value Score (0-100)
Score:
- NAV vs Market Cap
- Stabilized Yield / Development Value
- Multiple vs Peers
- Reinvestment / Pipeline Quality
- Strategic Value
```

## Medtech

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- development / commercialization timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Sector research lane:
- Capture recent sources on regulatory pathways, reimbursement, clinical adoption, manufacturing scale-up, and competitive positioning.

Step 1: Core operating and valuation workup
- Focus on regulatory clearance, reimbursement status, installed base, utilization, procedure economics, manufacturing readiness, and sales ramp.
- Build valuation using EV/Sales, margin path, adoption curve, and reimbursement value.

Step 2: Quality Score (0-100)
Score:
- Regulatory Environment
- Scientific / Manufacturing Capability
- Management
- Adoption / Commercial Readiness
- Balance Sheet
- Governance

Step 3: Value Score (0-100)
Score:
- Intrinsic Value vs Market Cap
- EV/Sales vs Growth
- Adoption Curve
- Reimbursement Value
- Strategic Value
```

## General Equity

```md
Run an investment analysis on [company_name] following this rubric exactly.

Required outputs:
- 12M and 24M price targets
- bull / base / bear scenarios
- Quality Score
- Value Score
- operating timeline
- thesis map
- monitoring watchlist
- verification queue
- investment recommendation

Step 1: Core operating and valuation workup
- Build the operating picture from current filings and primary disclosures.
- Identify the key revenue, margin, balance-sheet, and capital-allocation drivers.
- Build a defendable valuation anchor appropriate to the company.

Step 2: Quality Score (0-100)
Score:
- Market Quality
- Business Quality
- Management
- Execution
- Balance Sheet
- Governance

Step 3: Value Score (0-100)
Score:
- Intrinsic Value vs Market Cap
- Multiple vs Growth / Quality
- Earnings or Cash Flow Power
- Reinvestment Quality
- Strategic Value

Final instruction:
Do not produce a generic summary. Produce a decision-grade investment analysis with explicit assumptions, scenario conditions, and monitoring triggers.
```
