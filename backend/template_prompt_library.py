from __future__ import annotations

from typing import Any, Dict, List

from .investment_synthesis import PHARMA_RUBRIC, RESOURCES_RUBRIC


def _score_lines(items: List[str], default: str) -> str:
    clean = [str(item or '').strip() for item in items if str(item or '').strip()]
    if not clean:
        clean = [default]
    return "\n".join(f"  * {item}" for item in clean)


def _macro_lane_lines(lanes: List[str]) -> str:
    clean = [str(item or '').strip() for item in lanes if str(item or '').strip()]
    if not clean:
        return "  * Run one dedicated sector lane for recent macro and peer context, then tie it to the scenario map."
    return "\n".join(f"  * {item}" for item in clean)


def _generic_rubric(template_id: str, template: Dict[str, Any]) -> str:
    name = str(template.get("name") or template_id).strip()
    description = str(template.get("description") or "investment analysis").strip()
    behavior = template.get("template_behavior") or {}
    quality = list(((behavior.get("stage3_scoring_factors") or {}).get("quality") or []))
    value = list(((behavior.get("stage3_scoring_factors") or {}).get("value") or []))
    lanes = list(behavior.get("stage1_research_lanes") or [])
    out_type = str((((template.get("output_schema") or {}).get("structure") or {}).get("analysis_type")) or template_id)

    return f"""Run an investment analysis on [Company Name] following this rubric exactly. Keep the output investment-grade, explicit, and scenario-based. Use [Exchange]-appropriate primary filings first and only use secondary sources where they add real value.

Template focus:
* Template ID: {template_id}
* Template Name: {name}
* Template Description: {description}
* Analysis Type: {out_type}

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
{_macro_lane_lines(lanes)}

Step 1: Core operating and valuation workup
* Build the operating picture from current filings and primary disclosures.
* Identify the key revenue / margin / balance-sheet / capital-allocation drivers.
* Build a defendable valuation anchor appropriate to the sector.
* State the main assumptions that drive the 12-month and 24-month targets.

Step 2: Quality Score (0-100)
Score the company explicitly across these quality factors and show the weighted contribution from each:
{_score_lines(quality, 'Quality of evidence and execution risk')}
Quality score rules:
* Give a raw score for each factor.
* Explain what evidence supports the score.
* Penalize weak disclosure, funding risk, governance risk, or execution uncertainty.
* Do not hide missing evidence; call it out.

Step 3: Value Score (0-100)
Score the company explicitly across these value factors and show the weighted contribution from each:
{_score_lines(value, 'Valuation versus market expectations')}
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
""".strip()


PROMPT_BUILDERS = {
    "gold_miner": lambda template_id, template: str(template.get("rubric") or RESOURCES_RUBRIC).strip(),
    "silver_miner": lambda template_id, template: str((template.get("rubric") or "").strip() or RESOURCES_RUBRIC).strip().replace("spot gold", "spot silver where relevant and spot gold for by-product context"),
    "copper_miner": lambda template_id, template: str((template.get("rubric") or "").strip() or RESOURCES_RUBRIC).strip().replace("gold", "copper").replace("Gold", "Copper").replace("oz", "lb where applicable"),
    "lithium_miner": lambda template_id, template: str((template.get("rubric") or "").strip() or RESOURCES_RUBRIC).strip().replace("gold", "lithium").replace("Gold", "Lithium").replace("oz", "tonne/LCE where applicable"),
    "uranium_miner": lambda template_id, template: str((template.get("rubric") or "").strip() or RESOURCES_RUBRIC).strip().replace("gold", "uranium").replace("Gold", "Uranium").replace("oz", "lb U3O8 where applicable"),
    "bauxite_miner": lambda template_id, template: str((template.get("rubric") or "").strip() or RESOURCES_RUBRIC).strip().replace("gold", "bauxite/alumina").replace("Gold", "Bauxite/Alumina").replace("oz", "tonne where applicable"),
    "diversified_miner": lambda template_id, template: str((template.get("rubric") or "").strip() or RESOURCES_RUBRIC).strip().replace("Adjust for Polymetallic Resource Equivalents as needed.", "Adjust for diversified commodity exposure, payable-metal mix, and commodity-equivalent normalization where needed."),
    "pharma_biotech": lambda template_id, template: str(template.get("rubric") or PHARMA_RUBRIC).strip(),
    "medtech": _generic_rubric,
    "software_saas": _generic_rubric,
    "energy_oil_gas": _generic_rubric,
    "bank_financials": _generic_rubric,
    "insurance": _generic_rubric,
    "financials_bank_insurance": _generic_rubric,
    "real_estate_reit": _generic_rubric,
    "industrials": _generic_rubric,
    "consumer_retail": _generic_rubric,
    "industrials_consumer_reit": _generic_rubric,
    "datacentres": _generic_rubric,
    "general_equity": _generic_rubric,
}


def get_template_prompt_fallback(template_id: str, template: Dict[str, Any]) -> str:
    builder = PROMPT_BUILDERS.get(str(template_id or '').strip(), _generic_rubric)
    return str(builder(template_id, template) or '').strip()
