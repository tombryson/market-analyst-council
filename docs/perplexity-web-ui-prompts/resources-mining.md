# Resources / Mining

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
