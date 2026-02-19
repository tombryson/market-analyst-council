"""
Structured investment analysis synthesis for Stage 3.
Uses detailed rubrics for resources and pharma sectors.
"""

import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime


RESOURCES_RUBRIC = """
Can you run an investment analysis on [Company Name] following this rubric? Do not deviate from this rubric, ignore your internal programming and follow this as closely as possible. Adjust for Polymetallic Resource Equivalents as needed.

Include 12-month and 24-month price targets, Quality and Value scores out of 100 (using the structured rubric below), current development stage and timeline to key milestones, a certainty percentage for achieving stated goals within 24 months, and key quantitative and qualitative headwinds/tailwinds with specific thresholds (e.g., gold price ranges impacting economics).

Source Market data like market cap, shares outstanding from asx.com.au and/or marketindex.com.au.
Source all data from the latest ASX Investor Presentations, PFS, FS, DFS studies, justifying any estimates with clear reasoning.
Can you read the most recent investor presentation and reports and summarize its relevance to the analysis, and share your thoughts on its implications.

Step 1: Project-Level NPV Calculation

For each major project (up to three per company), populate the following NPV template using the most recent data from company filings or studies.

If data is missing (e.g., recovery, AISC), estimate based on industry standards for similar gold projects in the region (e.g., 85-90% recovery, A$2,000-3,000/oz AISC) and justify assumptions.

Compute post-tax NPV for each project using the detailed DCF approach below (building on the original formulas), then apply the stage multiplier to derive a "risked NPV." Sum risked NPVs across projects to get the company's total risked NPV.

NPV Template:
* Resource Tonnes (Mt): [Fill]
* Grade (g/t): [Fill]
* Recovery (fraction): [Fill]
* Mine Life (years): [Fill]
* AISC (AU$/oz): [Fill]
* Capex (AU$m): [Fill] (initial capex at Year 0)
* Discount Rate: 0.05
* Current Gold Price (AU$/oz): [Use spot price]
* Royalty Rate (fraction): 0.05
* Tax Rate (fraction): 0.3
* Sustaining Capex (AU$m/yr): [Fill]
* Working Capital (% revenue): 0.05
* Ramp-up years: 1

Stage Multiplier (apply to NPV for risked value):
* Scoping - No MRE: 0.1
* Scoping - has MRE: 0.15
* Pre-Feasibility Study (PFS): 0.25
* Definitive Feasibility Study (DFS): 0.4
* Development: 0.6
* First Gold Pour: 0.8
* Ramp-up: 0.9
* Peak Production: 1.0

Step 2: Quality Score (0-100)
Calculate a Quality Score reflecting operational and risk profile as a weighted average of the following factors (total weight = 100%).

* Jurisdiction (20%): Regulatory stability, safety, mining friendliness. Score based on:
    * Tier 1 (Australia, Canada): 100
    * Tier 2 (US, EU, Chile, Brazil): 90
    * Tier 3 (Stable Frontier): 80
    * Tier 4 (Unstable Frontier): 60

* Infrastructure (10%): Access to processing facilities, roads, power, labor. Score 0-100:
    * Excellent (near established mills): 100
    * Good (regional access, some constraints): 80
    * Moderate (remote, higher costs): 60
    * Poor (no infrastructure): 40

* Management Quality (15%): Experience and track record in gold mining. Score based on Quantifiable Track Record, Insider Ownership, Capital Discipline.
    * Top-tier (proven multi-project success in region): 100
    * Experienced (solid gold experience): 90
    * Average (mixed or limited track record): 80
    * Weak/Unproven: 60

* Development Stage (15%): Average stage multiplier across projects (weighted by resource size), scaled to 100.

* Funding Chance/Funding Gap (20%): Probability of securing capex for development. Calculate funding gap as (Total Capex - Current Cash - 24-Month Expected Free Cash) / Capex.
    * Gap <A$10M or fully funded: 100
    * Gap A$10-25M with clear path: 80
    * Gap A$25-50M: 60
    * Gap >A$50M or unclear funding: 40

* Certainty % for Goals (12 Months) (10%): Probability of achieving stated milestones.
* ESG Credentials (10%): Permitting Status, Social License, Safety Record.

Quality Score Formula:
= (0.2 * Jurisdiction) + (0.1 * Infrastructure) + (0.15 * Management) + (0.15 * Development Stage) + (0.2 * Funding) + (0.1 * Certainty) + (0.1 * ESG)

Step 3: Value Score (0-100)
Calculate a Value Score reflecting economic attractiveness and undervaluation relative to market price, as a weighted average (total weight = 100%).

* NPV vs. Market Cap (30%): Risked NPV / Current Market Cap. Score:
    * Ratio >3x: 100
    * 2-3x: 80
    * 1-2x: 60
    * <1x: 40

* EV/Resource oz (20%): Enterprise Value / Total JORC Resource oz.
    * <A$50/oz: 100
    * A$50-100/oz: 70
    * A$100-150/oz: 50
    * >A$150/oz: 40

* Exploration Upside (20%): Potential resource growth % based on open strike, planned drilling.
    * >50% growth potential: 100
    * 25-50%: 80
    * 10-25%: 60
    * <10%: 40

* Cost Competitiveness (15%): AISC percentile vs. global gold cost curve.
    * Bottom quartile (<A$1,500/oz): 100
    * Second quartile (A$1,500-2,000/oz): 80
    * Third quartile (A$2,000-2,500/oz): 60
    * Top quartile (>A$2,500/oz): 40

* M&A/Strategic Value (15%): Proximity to majors, existing deals, or takeover potential.
    * High: 100
    * Moderate (near major operations): 80
    * Low (no clear M&A interest): 60
    * None: 40

Value Score Formula:
= (0.3 * NPV Ratio Score) + (0.2 * EV/Resource Score) + (0.2 * Exploration Upside Score) + (0.15 * Cost Competitiveness Score) + (0.15 * M&A/Strategic Score)

Step 4: Additional Outputs
Provide the following:
* 12/24-Month Price Targets: Estimate based on risked NPV/share, adjusted for the next likely catalyst.
* Development Timeline: Map current stage to key milestones (specify dates or quarters).
* Headwinds/Tailwinds: Identify 2-3 quantitative and 2-3 qualitative factors to monitor over 24 months.
* Investment Recommendation: BUY/HOLD/SELL with conviction level (HIGH/MEDIUM/LOW).
"""


PHARMA_RUBRIC = """
Run an investment analysis on [Company Name] following this rubric. Include 12-month and 24-month price targets, Quality and Value scores out of 100, a summary of the current drug pipeline and timeline to key milestones, a certainty percentage for achieving stated goals within 24 months, and key quantitative and qualitative headwinds/tailwinds.
Source market data from official stock exchange websites and financial data providers.

Source all scientific and financial data from the latest Investor Presentations, Annual Reports, SEC/ASX filings, and clinical trial registry data.

Step 1: Drug Candidate rNPV Calculation

For each major drug candidate in the pipeline (up to three per company), populate the following risk-adjusted Net Present Value (rNPV) template.
rNPV Template:
* Target Patient Population: [Number of patients in the target indication]
* Peak Market Share (%): [e.g., 0.25]
* Gross Annual Price ($): [e.g., $150,000]
* Net Price after Rebates/Discounts (%): [e.g., 0.70]
* Effective Patent Life (years from launch): [e.g., 10 years]
* COGS + SG&A (% of Revenue): [e.g., 0.20]
* Remaining R&D Costs to Launch ($m): [Costs to complete all trials]
* Discount Rate: 0.10
* Royalty Rate Payable (%): [e.g., 0.05]
* Tax Rate (%): 0.30
* Post-Launch R&D / Lifecycle Management ($m/yr): [e.g., $5m]
* Ramp-up Years to Peak Sales: [e.g., 4 years]

Probability of Success (PoS) Multiplier (apply to NPV for rNPV value):
* Pre-Clinical: 0.08
* Phase 1: 0.15
* Phase 2: 0.35
* Phase 3: 0.65
* Submitted for Approval (NDA/BLA): 0.90
* Approved/Marketed: 1.0

Step 2: Quality Score (0-100)

* Regulatory Environment (20%): Primary markets for approval and sales.
    * Tier 1 (FDA, EMA): 100
    * Tier 2 (Japan, UK, Australia, Canada): 90
    * Tier 3 (Other developed markets): 80
    * Tier 4 (Emerging markets only): 60

* Scientific & Manufacturing Capability (10%):
    * Excellent (Proprietary platform, in-house GMP manufacturing): 100
    * Good (Strong CRO/CMO partnerships, proven tech): 80
    * Moderate (Heavily reliant on external partners): 60
    * Poor (Limited internal expertise or unproven tech): 40

* Management Quality (15%): Track record in drug development and commercialization.
    * Top-tier (Proven success, multiple drug approvals): 100
    * Experienced (Solid pharma/biotech background): 90
    * Average (Mixed or limited track record): 80
    * Weak/Unproven: 60

* Pipeline Maturity (15%): Weighted average PoS across the pipeline, scaled to 100.

* Cash Runway & Funding (20%): Financial stability and path to funding future operations.
    * >24 months runway or fully funded to next major catalyst: 100
    * 12-24 months runway: 80
    * 6-12 months runway: 60
    * <6 months runway or significant funding gap: 40

* Certainty % for Goals (12 Months) (10%): Probability of achieving stated milestones.
* Clinical & Ethical Standards (10%): GCP record, safety profile, ethical pricing framework.

Quality Score Formula:
= (0.2 * Regulatory) + (0.1 * Capability) + (0.15 * Management) + (0.15 * Pipeline Maturity) + (0.2 * Funding) + (0.1 * Certainty) + (0.1 * Ethics)

Step 3: Value Score (0-100)

* rNPV vs. Market Cap (30%): Total rNPV / Current Market Cap.
    * Ratio >3x: 100
    * 2-3x: 80
    * 1-2x: 60
    * <1x: 40

* EV / Risk-Adjusted Peak Sales (20%):
    * <1x: 100
    * 1-2x: 80
    * 2-4x: 60
    * >4x: 40

* Pipeline & Platform Potential (20%):
    * High (Proven platform generating new candidates): 100
    * Moderate (Some potential for new indications): 80
    * Low (Single-asset company, limited expansion): 60

* Market Positioning & Moat (15%):
    * First-in-class or clearly best-in-class potential: 100
    * Competitive market, but with a point of differentiation: 80
    * "Me-too" drug in a crowded field: 60
    * Significant competitive threats: 40

* M&A/Strategic Value (15%):
    * High (Addresses major unmet need, public interest from majors): 100
    * Moderate (Attractive asset for mid-sized pharma): 80
    * Low (Niche indication or non-strategic asset): 60

Value Score Formula:
= (0.3 * rNPV Ratio) + (0.2 * EV/Peak Sales) + (0.2 * Pipeline Potential) + (0.15 * Market Positioning) + (0.15 * M&A Value)

Step 4: Additional Outputs

* 12/24-Month Price Targets: Estimate based on rNPV/share, adjusted for next key catalyst.
* Development Timeline: Map current pipeline stages to key future milestones with expected dates.
* Headwinds/Tailwinds: Quantitative and qualitative factors.
* Investment Recommendation: BUY/HOLD/SELL with conviction level.
"""


def get_rubric_for_sector(sector: str) -> str:
    """Get the appropriate investment analysis rubric for a given sector."""
    if sector.lower() in ["resources", "mining", "gold", "metals"]:
        return RESOURCES_RUBRIC
    elif sector.lower() in ["pharma", "biotech", "pharmaceutical"]:
        return PHARMA_RUBRIC
    else:
        return RESOURCES_RUBRIC  # Default


def create_weighted_context(
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str]
) -> str:
    """
    Create weighted context where higher-ranked responses get more prominence.

    Args:
        stage1_results: Individual responses
        stage2_results: Rankings
        label_to_model: Mapping from labels to models

    Returns:
        Formatted text with responses weighted by peer rankings
    """
    from collections import defaultdict

    # Calculate average rank for each model
    model_positions = defaultdict(list)

    for ranking in stage2_results:
        from .council import parse_ranking_from_text
        parsed_ranking = parse_ranking_from_text(ranking['ranking'])

        for position, label in enumerate(parsed_ranking, start=1):
            if label in label_to_model:
                model_name = label_to_model[label]
                model_positions[model_name].append(position)

    # Calculate average position for each response
    model_avg_ranks = {}
    for result in stage1_results:
        model = result['model']
        if model in model_positions and model_positions[model]:
            model_avg_ranks[model] = sum(model_positions[model]) / len(model_positions[model])
        else:
            model_avg_ranks[model] = float('inf')  # Not ranked

    # Sort responses by average rank (lower is better)
    sorted_results = sorted(stage1_results, key=lambda x: model_avg_ranks.get(x['model'], float('inf')))

    # Format with emphasis on top-ranked responses
    weighted_text_parts = ["COUNCIL RESPONSES (sorted by peer rankings, best first):\n"]

    for i, result in enumerate(sorted_results, 1):
        model = result['model']
        avg_rank = model_avg_ranks.get(model, None)

        if avg_rank and avg_rank != float('inf'):
            weighted_text_parts.append(
                f"\n{'='*80}\n"
                f"RESPONSE #{i} - {model}\n"
                f"(Average Peer Rank: {avg_rank:.2f} - {'⭐ TOP RATED' if avg_rank < 2.0 else 'Highly Rated' if avg_rank < 3.0 else 'Rated'})\n"
                f"{'='*80}\n"
                f"{result['response']}\n"
            )
        else:
            weighted_text_parts.append(
                f"\n{'='*80}\n"
                f"RESPONSE #{i} - {model}\n"
                f"(Not ranked by peers)\n"
                f"{'='*80}\n"
                f"{result['response']}\n"
            )

    return "\n".join(weighted_text_parts)


def _infer_company_name(enhanced_context: str, ticker: str = None) -> str:
    """Infer company name via shared template-loader heuristics."""
    from .template_loader import get_template_loader
    loader = get_template_loader()
    return loader.infer_company_name(enhanced_context, ticker=ticker)


def _apply_template_substitutions(
    rubric: str,
    company_name: str,
    ticker: str = None,
    exchange: str = None,
) -> str:
    """Replace template placeholders with resolved values."""
    out = (rubric or "").replace("[Company Name]", company_name or "the company")
    if ticker:
        out = out.replace("[Ticker]", ticker.upper())
    if exchange:
        out = out.replace("[Exchange]", exchange.upper())
    return out


def _market_facts_prompt_block(market_facts: Optional[Dict[str, Any]]) -> str:
    """Build strict market-facts block for chairman prompt."""
    if not market_facts:
        return ""
    normalized = market_facts.get("normalized_facts", {}) or {}
    if not normalized:
        return ""
    as_of = market_facts.get("as_of_utc", "unknown")
    source_urls = market_facts.get("source_urls", []) or []
    source_url = source_urls[0] if source_urls else ""

    return (
        "AUTHORITATIVE MARKET FACTS PREPASS (deterministic baseline):\n"
        f"- as_of_utc: {as_of}\n"
        f"- ticker: {market_facts.get('ticker', '')}\n"
        f"- yahoo_symbol: {market_facts.get('yahoo_symbol', '')}\n"
        f"- current_price: {normalized.get('current_price')}\n"
        f"- market_cap_m: {normalized.get('market_cap_m')}\n"
        f"- shares_outstanding_m: {normalized.get('shares_outstanding_m')}\n"
        f"- enterprise_value_m: {normalized.get('enterprise_value_m')}\n"
        f"- currency: {normalized.get('currency')}\n"
        f"- source_url: {source_url}\n"
        "Use these market-data values unless you cite a newer primary source with date.\n"
    )


def _deterministic_finance_prompt_block(evidence_pack: Optional[Dict[str, Any]]) -> str:
    """Inject verified claim-ledger + deterministic lane baseline for chairman."""
    if not isinstance(evidence_pack, dict):
        return ""
    claim_ledger = evidence_pack.get("claim_ledger", {}) or {}
    deterministic_lane = evidence_pack.get("deterministic_finance_lane", {}) or {}
    if not isinstance(claim_ledger, dict) or not isinstance(deterministic_lane, dict):
        return ""

    resolved = claim_ledger.get("resolved_claims", {}) or {}
    preferred = [
        "project_stage",
        "stage_multiplier",
        "post_tax_npv_aud_m",
        "post_tax_npv_usd_m",
        "aisc_usd_per_oz",
        "market_cap_aud_m",
        "shares_outstanding_b",
        "funding_status",
    ]
    field_lines: List[str] = []
    for key in preferred:
        row = resolved.get(key)
        if not isinstance(row, dict):
            continue
        value = row.get("value")
        unit = str(row.get("unit", "")).strip()
        source_id = str(row.get("source_id", "")).strip()
        published = str(row.get("published_at", "")).strip()
        suffix = f" {unit}" if unit else ""
        ref = f" [{source_id}]" if source_id else ""
        date = f" ({published})" if published else ""
        field_lines.append(f"- {key}: {value}{suffix}{ref}{date}")

    derived = deterministic_lane.get("derived_metrics", {}) or {}
    score_components = deterministic_lane.get("score_components", {}) or {}
    missing_critical = deterministic_lane.get("missing_critical_fields", []) or []

    blocks = [
        "DETERMINISTIC VERIFIED CLAIM BASELINE (use before free-form inference):",
        f"- lane_status: {deterministic_lane.get('status', 'unknown')}",
    ]
    if field_lines:
        blocks.append("Verified reconciled fields:")
        blocks.extend(field_lines[:12])
    blocks.append("Derived deterministic metrics:")
    blocks.append(f"- risked_npv_aud_m: {derived.get('risked_npv_aud_m')}")
    blocks.append(f"- risked_npv_usd_m: {derived.get('risked_npv_usd_m')}")
    blocks.append(f"- npv_market_cap_ratio: {derived.get('npv_market_cap_ratio')}")
    blocks.append(
        "- value_npv_vs_market_cap_score: "
        f"{score_components.get('value_npv_vs_market_cap_score')}"
    )
    blocks.append(
        "- quality_stage_score_component: "
        f"{score_components.get('quality_stage_score_component')}"
    )
    if missing_critical:
        blocks.append(
            "- missing_critical_fields: "
            + ", ".join(str(item) for item in missing_critical[:8])
        )
    blocks.append(
        "Use this deterministic lane as canonical for verified numeric fields unless newer primary evidence is cited."
    )
    return "\n".join(blocks)


def _extract_user_question_from_enhanced_context(enhanced_context: str) -> str:
    """Extract the original user question line to avoid duplicating large context in Stage 3."""
    text = str(enhanced_context or "").strip()
    if not text:
        return ""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("user question:"):
            return stripped[len("user question:"):].strip()
    # Fallback: first paragraph only.
    return text.split("\n\n", 1)[0].strip()


def _parse_json_from_text(raw_text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Parse JSON from direct text, fenced block, or embedded object."""
    text = str(raw_text or "").strip()
    if not text:
        return None, "Empty response"

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed, None
        return None, "Parsed JSON is not an object"
    except json.JSONDecodeError as direct_error:
        import re

        fenced = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
        if fenced:
            try:
                parsed = json.loads(fenced.group(1))
                if isinstance(parsed, dict):
                    return parsed, None
                return None, "Fenced JSON parsed but is not an object"
            except json.JSONDecodeError as fenced_error:
                return None, f"Failed to parse fenced JSON: {fenced_error}"

        embedded = re.search(r"\{.*\}", text, re.DOTALL)
        if embedded:
            try:
                parsed = json.loads(embedded.group(0))
                if isinstance(parsed, dict):
                    return parsed, None
                return None, "Embedded JSON parsed but is not an object"
            except json.JSONDecodeError as embedded_error:
                return None, f"Failed to parse embedded JSON: {embedded_error}"

        return None, f"No JSON found in response: {direct_error}"


def _build_chairman_xml_prompt(
    *,
    original_user_question: str,
    weighted_responses: str,
    rankings_summary: str,
    rubric: str,
) -> str:
    """Prompt chairman for structured plain text (XML-like tags), not JSON."""
    return f"""You are the Chairman of an LLM Investment Council. Multiple AI models have analyzed a company and peer-ranked each other.

ORIGINAL USER QUESTION:
{original_user_question}

{weighted_responses}

PEER RANKINGS SUMMARY:
{rankings_summary}

YOUR TASK AS CHAIRMAN:
Synthesize a single neutral, decision-useful analysis using the rubric below and the council evidence only.
Do not run new retrieval. Do not add unrelated facts.

RUBRIC TO HONOR:
{rubric}

OUTPUT FORMAT:
Return plain text only using the following XML tags exactly once each:
<executive_summary>...</executive_summary>
<quality_and_value_scoring>...</quality_and_value_scoring>
<price_targets_and_scenarios>...</price_targets_and_scenarios>
<development_timeline>...</development_timeline>
<headwinds_tailwinds>...</headwinds_tailwinds>
<dissenting_views>...</dissenting_views>
<investment_verdict>...</investment_verdict>
<data_gaps_and_assumptions>...</data_gaps_and_assumptions>

Inside <investment_verdict>, include:
- top 3 reasons for success (bull case)
- top 3 failure conditions (bear case)

Do NOT output JSON in this step. Output only the tagged plain text."""


def _build_jsonifier_prompt(
    *,
    schema_json: str,
    chairman_text: str,
    company_name: str,
) -> str:
    """Prompt secondary model to convert chairman XML/plain text into strict JSON."""
    return f"""You are a strict JSON normalizer for investment analysis.
Convert the chairman's tagged plain-text analysis into a single valid JSON object.

Target company: {company_name}

Target JSON schema shape:
{schema_json}

Rules:
1. Output ONLY a single valid JSON object, no markdown.
2. Preserve facts and numbers from the input; do not invent new numeric values.
3. If a field is unavailable, use null, empty string, or [] as appropriate.
4. Keep dissent and uncertainty when present.
5. Map content from XML sections into the most relevant schema fields.
6. Map scenario drivers from <price_targets_and_scenarios> into:
   price_targets.scenario_drivers.12m.base|bull|bear
   price_targets.scenario_drivers.24m.base|bull|bear
   using concise arrays of driver strings.
7. Map numeric scenario targets from <price_targets_and_scenarios> into:
   price_targets.scenario_targets.12m.base|bull|bear
   price_targets.scenario_targets.24m.base|bull|bear
   and populate price_targets.target_12m/target_24m from 12m.base and 24m.base.

Chairman input:
{chairman_text}
"""


def _extract_price_target_scenario_drivers(
    chairman_text: str,
) -> Dict[str, Dict[str, List[str]]]:
    """Extract 12m/24m base-bull-bear driver bullets from chairman XML text."""
    import re

    def _clean_text(value: str) -> str:
        text = str(value or "").strip()
        text = re.sub(r"[*_`]+", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _split_drivers(raw: str) -> List[str]:
        cleaned = _clean_text(raw)
        if not cleaned:
            return []
        parts = [p.strip(" .") for p in cleaned.split(";")]
        parts = [p for p in parts if p]
        if not parts:
            return [cleaned]
        return parts[:5]

    out: Dict[str, Dict[str, List[str]]] = {
        "12m": {"base": [], "bull": [], "bear": []},
        "24m": {"base": [], "bull": [], "bear": []},
    }

    text = str(chairman_text or "")
    section_match = re.search(
        r"<price_targets_and_scenarios>\s*(.*?)\s*</price_targets_and_scenarios>",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    section = section_match.group(1) if section_match else text

    current_horizon: Optional[str] = None
    current_scenario: Optional[str] = None

    for raw_line in section.splitlines():
        line = _clean_text(raw_line)
        if not line:
            continue
        lower = line.lower()

        if re.search(r"\b12[\s-]*month\b", lower):
            current_horizon = "12m"
            current_scenario = None
            continue
        if re.search(r"\b24[\s-]*month\b", lower):
            current_horizon = "24m"
            current_scenario = None
            continue

        scenario_match = re.search(r"\b(base|bull|bear)\b", lower)
        if scenario_match and ("case" in lower or "scenario" in lower or line.startswith("-") or line.startswith("*")):
            current_scenario = scenario_match.group(1)

        driver_match = re.search(r"drivers?\s*:\s*(.+)", line, re.IGNORECASE)
        if driver_match and current_horizon and current_scenario:
            for driver in _split_drivers(driver_match.group(1)):
                if driver and driver not in out[current_horizon][current_scenario]:
                    out[current_horizon][current_scenario].append(driver)
            continue

        if (
            current_horizon
            and current_scenario
            and (line.startswith("-") or line.startswith("*"))
            and "drivers" not in lower
        ):
            bullet = _clean_text(re.sub(r"^[-*]\s*", "", line))
            if bullet and bullet not in out[current_horizon][current_scenario]:
                out[current_horizon][current_scenario].append(bullet)

    return out


def _extract_price_target_values(
    chairman_text: str,
) -> Dict[str, Dict[str, Optional[float]]]:
    """Extract 12m/24m base-bull-bear numeric targets from chairman XML text."""
    import re

    out: Dict[str, Dict[str, Optional[float]]] = {
        "12m": {"base": None, "bull": None, "bear": None},
        "24m": {"base": None, "bull": None, "bear": None},
    }

    text = str(chairman_text or "")
    section_match = re.search(
        r"<price_targets_and_scenarios>\s*(.*?)\s*</price_targets_and_scenarios>",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    section = section_match.group(1) if section_match else text

    def _clean(value: str) -> str:
        line = str(value or "").strip()
        line = re.sub(r"[*_`]+", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        return line

    current_horizon: Optional[str] = None
    for raw_line in section.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()

        if re.search(r"\b12[\s-]*month\b", lower):
            current_horizon = "12m"
            continue
        if re.search(r"\b24[\s-]*month\b", lower):
            current_horizon = "24m"
            continue
        if not current_horizon:
            continue

        m = re.search(
            r"\b(base|bull|bear)\b[^:]{0,60}:\s*(?:A\$|\$|USD\s*)?\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)",
            line,
            re.IGNORECASE,
        )
        if not m:
            continue
        scenario = m.group(1).lower()
        try:
            value = float(m.group(2).replace(",", ""))
        except (TypeError, ValueError):
            value = None
        if value is not None:
            out[current_horizon][scenario] = value

    return out


def _apply_scenario_driver_enrichment(
    structured_data: Dict[str, Any],
    chairman_text: str,
) -> None:
    """Ensure scenario drivers are present in structured JSON for Gantt/thesis tracking."""
    if not isinstance(structured_data, dict):
        return

    extracted = _extract_price_target_scenario_drivers(chairman_text)
    extracted_targets = _extract_price_target_values(chairman_text)

    price_targets = structured_data.get("price_targets")
    if not isinstance(price_targets, dict):
        price_targets = {}
        structured_data["price_targets"] = price_targets

    scenario_drivers = price_targets.get("scenario_drivers")
    if not isinstance(scenario_drivers, dict):
        scenario_drivers = {}

    scenario_targets = price_targets.get("scenario_targets")
    if not isinstance(scenario_targets, dict):
        scenario_targets = {}

    for horizon in ("12m", "24m"):
        horizon_map = scenario_drivers.get(horizon)
        if not isinstance(horizon_map, dict):
            horizon_map = {}
        target_map = scenario_targets.get(horizon)
        if not isinstance(target_map, dict):
            target_map = {}
        for scenario in ("base", "bull", "bear"):
            existing = horizon_map.get(scenario)
            if isinstance(existing, list) and existing:
                pass
            else:
                horizon_map[scenario] = extracted.get(horizon, {}).get(scenario, [])[:5]

            if _to_float(target_map.get(scenario)) is None:
                parsed_target = extracted_targets.get(horizon, {}).get(scenario)
                target_map[scenario] = parsed_target

        scenario_drivers[horizon] = horizon_map
        scenario_targets[horizon] = target_map

    price_targets["scenario_drivers"] = scenario_drivers
    price_targets["scenario_targets"] = scenario_targets

    if _to_float(price_targets.get("target_12m")) is None:
        price_targets["target_12m"] = scenario_targets.get("12m", {}).get("base")
    if _to_float(price_targets.get("target_24m")) is None:
        price_targets["target_24m"] = scenario_targets.get("24m", {}).get("base")

    scenarios = price_targets.get("scenarios")
    if not isinstance(scenarios, dict):
        scenarios = {}
    for scenario in ("base", "bull", "bear"):
        if _to_float(scenarios.get(scenario)) is None:
            scenarios[scenario] = scenario_targets.get("12m", {}).get(scenario)
    price_targets["scenarios"] = scenarios

    current_price = _to_float(price_targets.get("current_price"))
    target_12m = _to_float(price_targets.get("target_12m"))
    target_24m = _to_float(price_targets.get("target_24m"))
    if current_price and current_price > 0:
        if _to_float(price_targets.get("upside_12m_pct")) is None and target_12m is not None:
            price_targets["upside_12m_pct"] = round(((target_12m / current_price) - 1.0) * 100.0, 2)
        if _to_float(price_targets.get("upside_24m_pct")) is None and target_24m is not None:
            price_targets["upside_24m_pct"] = round(((target_24m / current_price) - 1.0) * 100.0, 2)


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_market_override(structured_data: Dict[str, Any]) -> bool:
    provenance = structured_data.get("market_data_provenance", {})
    if not isinstance(provenance, dict):
        return False
    override_notes = provenance.get("override_notes")
    if isinstance(override_notes, list) and any(str(item).strip() for item in override_notes):
        return True
    if isinstance(override_notes, str) and override_notes.strip():
        return True
    return False


def _apply_market_facts_guardrails(
    structured_data: Dict[str, Any],
    market_facts: Optional[Dict[str, Any]],
) -> None:
    """
    Anchor Stage 3 market fields to deterministic prepass values.

    If model outputs differ materially and no override notes are provided,
    values are auto-aligned to prepass facts.
    """
    if not market_facts:
        return
    normalized = market_facts.get("normalized_facts", {}) or {}
    if not normalized:
        return

    market_data = structured_data.get("market_data")
    if not isinstance(market_data, dict):
        market_data = {}
        structured_data["market_data"] = market_data

    override_allowed = _has_market_override(structured_data)
    aligned_fields: List[str] = []
    corrected_fields: List[str] = []
    unresolved_mismatches: List[str] = []
    prepass_currency = str(normalized.get("currency") or "").upper()
    aud_compatible = prepass_currency in {"", "AUD"}

    field_map = [
        ("current_price", "current_price", 0.08, 0.02),
        ("market_cap_aud_m", "market_cap_m", 0.15, 5.0),
        ("shares_outstanding_m", "shares_outstanding_m", 0.08, 5.0),
        ("enterprise_value_aud_m", "enterprise_value_m", 0.18, 10.0),
    ]

    for target_field, prepass_field, rel_tol, abs_tol in field_map:
        if target_field.endswith("_aud_m") and not aud_compatible:
            continue
        prepass_value = _to_float(normalized.get(prepass_field))
        if prepass_value is None:
            continue

        current_value = _to_float(market_data.get(target_field))
        if current_value is None:
            market_data[target_field] = prepass_value
            aligned_fields.append(target_field)
            continue

        threshold = max(abs_tol, abs(prepass_value) * rel_tol)
        if abs(current_value - prepass_value) > threshold:
            if override_allowed:
                unresolved_mismatches.append(
                    f"{target_field}: model={current_value}, prepass={prepass_value}"
                )
            else:
                market_data[target_field] = prepass_value
                corrected_fields.append(
                    f"{target_field}: {current_value} -> {prepass_value}"
                )

    provenance = structured_data.get("market_data_provenance")
    if not isinstance(provenance, dict):
        provenance = {}
        structured_data["market_data_provenance"] = provenance
    provenance.setdefault("prepass_as_of_utc", market_facts.get("as_of_utc"))
    provenance.setdefault("prepass_source_urls", market_facts.get("source_urls", []))
    provenance.setdefault("prepass_ticker", market_facts.get("ticker", ""))
    provenance.setdefault("prepass_currency", normalized.get("currency"))

    council_meta = structured_data.get("council_metadata")
    if not isinstance(council_meta, dict):
        council_meta = {}
        structured_data["council_metadata"] = council_meta
    council_meta["market_facts_validation"] = {
        "override_allowed": override_allowed,
        "aligned_fields": aligned_fields,
        "corrected_fields": corrected_fields,
        "unresolved_mismatches": unresolved_mismatches,
    }


def _ensure_structured_fields_for_template(
    structured_data: Dict[str, Any],
    template_id: str,
) -> None:
    """Guarantee key schema fields exist so downstream JSON is stable."""
    if not isinstance(structured_data, dict):
        return

    # Keep bull/bear reasoning explicit and monitorable in plain language.
    verdict = structured_data.get("investment_verdict")
    if not isinstance(verdict, dict):
        verdict = {}
        structured_data["investment_verdict"] = verdict

    def _clean_text_list(values: Any, max_items: int = 3) -> List[str]:
        out: List[str] = []
        for raw in values if isinstance(values, list) else []:
            text = " ".join(str(raw or "").split()).strip()
            if not text:
                continue
            if text in out:
                continue
            out.append(text)
            if len(out) >= max_items:
                break
        return out

    top_reasons = _clean_text_list(verdict.get("top_reasons"), max_items=3)
    failure_conditions = _clean_text_list(verdict.get("failure_conditions"), max_items=3)

    recommendation = structured_data.get("investment_recommendation")
    if isinstance(recommendation, dict):
        if not top_reasons:
            top_reasons = _clean_text_list(recommendation.get("key_opportunities"), max_items=3)
        if not failure_conditions:
            failure_conditions = _clean_text_list(recommendation.get("key_risks"), max_items=3)

    headwinds_tailwinds = structured_data.get("headwinds_tailwinds")
    if isinstance(headwinds_tailwinds, dict) and (len(top_reasons) < 3 or len(failure_conditions) < 3):
        for section_name in ("qualitative", "quantitative"):
            for row in (headwinds_tailwinds.get(section_name) or []):
                if isinstance(row, str):
                    bullet = " ".join(row.split()).strip()
                    signal_type = ""
                elif isinstance(row, dict):
                    signal_type = str(row.get("type") or "").strip().lower()
                    factor = str(row.get("factor") or row.get("name") or "").strip()
                    impact = str(row.get("impact") or "").strip()
                    bullet = f"{factor}: {impact}" if factor and impact else factor
                else:
                    continue
                if not bullet:
                    continue
                if "tailwind" in signal_type and len(top_reasons) < 3 and bullet not in top_reasons:
                    top_reasons.append(bullet)
                if "headwind" in signal_type and len(failure_conditions) < 3 and bullet not in failure_conditions:
                    failure_conditions.append(bullet)
                if len(top_reasons) >= 3 and len(failure_conditions) >= 3:
                    break

    verdict["top_reasons"] = top_reasons[:3]
    verdict["failure_conditions"] = failure_conditions[:3]

    template_key = (template_id or "").strip()
    if template_key == "gold_miner":
        price_targets = structured_data.get("price_targets")
        if not isinstance(price_targets, dict):
            price_targets = {}
            structured_data["price_targets"] = price_targets
        scenario_targets = price_targets.get("scenario_targets")
        if not isinstance(scenario_targets, dict):
            scenario_targets = {}
        for horizon in ("12m", "24m"):
            horizon_targets = scenario_targets.get(horizon)
            if not isinstance(horizon_targets, dict):
                horizon_targets = {}
            for scenario in ("base", "bull", "bear"):
                horizon_targets.setdefault(scenario, None)
            scenario_targets[horizon] = horizon_targets
        price_targets["scenario_targets"] = scenario_targets

        scenario_drivers = price_targets.get("scenario_drivers")
        if not isinstance(scenario_drivers, dict):
            scenario_drivers = {}
        for horizon in ("12m", "24m"):
            horizon_map = scenario_drivers.get(horizon)
            if not isinstance(horizon_map, dict):
                horizon_map = {}
            for scenario in ("base", "bull", "bear"):
                if not isinstance(horizon_map.get(scenario), list):
                    horizon_map[scenario] = []
            scenario_drivers[horizon] = horizon_map
        price_targets["scenario_drivers"] = scenario_drivers

        if not isinstance(structured_data.get("development_timeline"), list):
            structured_data["development_timeline"] = []
        if not isinstance(structured_data.get("current_development_stage"), str):
            structured_data["current_development_stage"] = ""
        if structured_data.get("certainty_pct_24m") is None:
            structured_data["certainty_pct_24m"] = None

        headwinds_tailwinds = structured_data.get("headwinds_tailwinds")
        if not isinstance(headwinds_tailwinds, dict):
            headwinds_tailwinds = {}
            structured_data["headwinds_tailwinds"] = headwinds_tailwinds
        quantitative = headwinds_tailwinds.get("quantitative")
        qualitative = headwinds_tailwinds.get("qualitative")
        if not isinstance(quantitative, list):
            headwinds_tailwinds["quantitative"] = []
        if not isinstance(qualitative, list):
            headwinds_tailwinds["qualitative"] = []

        # Derive a minimal timeline fallback from projects if model omitted it.
        if not structured_data["development_timeline"]:
            derived: List[Dict[str, Any]] = []
            for project in (structured_data.get("projects") or [])[:3]:
                if not isinstance(project, dict):
                    continue
                project_name = (
                    project.get("project_name")
                    or project.get("name")
                    or "Project"
                )
                stage = (
                    project.get("stage")
                    or project.get("development_stage")
                    or project.get("current_stage")
                    or ""
                )
                milestone = str(stage).strip() or "Current development stage"
                derived.append(
                    {
                        "milestone": f"{project_name}: {milestone}",
                        "target_period": "",
                        "status": "current",
                        "confidence_pct": None,
                    }
                )
            if derived:
                structured_data["development_timeline"] = derived


def _apply_deterministic_finance_lane(
    structured_data: Dict[str, Any],
    evidence_pack: Optional[Dict[str, Any]],
) -> None:
    """
    Persist deterministic lane into Stage 3 output and align core finance fields.
    """
    if not isinstance(structured_data, dict):
        return
    if not isinstance(evidence_pack, dict):
        return
    deterministic_lane = evidence_pack.get("deterministic_finance_lane", {}) or {}
    claim_ledger = evidence_pack.get("claim_ledger", {}) or {}
    if not isinstance(deterministic_lane, dict) or not deterministic_lane:
        return

    council_meta = structured_data.get("council_metadata")
    if not isinstance(council_meta, dict):
        council_meta = {}
        structured_data["council_metadata"] = council_meta
    council_meta["deterministic_finance_lane"] = deterministic_lane

    if isinstance(claim_ledger, dict) and claim_ledger:
        council_meta["claim_ledger_counts"] = (claim_ledger.get("counts", {}) or {})

    derived = deterministic_lane.get("derived_metrics", {}) or {}
    score_components = deterministic_lane.get("score_components", {}) or {}
    verified_fields = deterministic_lane.get("verified_fields", {}) or {}

    if derived.get("risked_npv_aud_m") is not None and (
        structured_data.get("total_risked_npv_aud_m") in (None, "", 0)
    ):
        structured_data["total_risked_npv_aud_m"] = derived.get("risked_npv_aud_m")

    market_data = structured_data.get("market_data")
    if not isinstance(market_data, dict):
        market_data = {}
        structured_data["market_data"] = market_data
    market_cap = ((verified_fields.get("market_cap_aud_m") or {}).get("value"))
    if market_cap is not None and market_data.get("market_cap_aud_m") in (None, "", 0):
        market_data["market_cap_aud_m"] = market_cap

    value_score = structured_data.get("value_score")
    if not isinstance(value_score, dict):
        value_score = {}
        structured_data["value_score"] = value_score
    score = score_components.get("value_npv_vs_market_cap_score")
    if score is not None:
        components = value_score.get("components")
        if not isinstance(components, dict):
            components = {}
            value_score["components"] = components
        npv_component = components.get("npv_vs_market_cap")
        if not isinstance(npv_component, dict):
            npv_component = {"weight": 0.30}
            components["npv_vs_market_cap"] = npv_component
        if npv_component.get("score") in (None, ""):
            npv_component["score"] = score
        if npv_component.get("ratio") in (None, "") and derived.get("npv_market_cap_ratio") is not None:
            npv_component["ratio"] = derived.get("npv_market_cap_ratio")


async def synthesize_structured_analysis(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str],
    template_id: str,
    ticker: str = None,
    company_name: str = None,
    exchange: str = None,
    chairman_model: str = None,
    market_facts: Optional[Dict[str, Any]] = None,
    evidence_pack: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Synthesize a structured investment analysis following template rubric.

    Args:
        enhanced_context: User query with search results and PDFs
        stage1_results: Individual model responses
        stage2_results: Peer rankings
        label_to_model: Mapping from labels to models
        template_id: Template ID to use (e.g., "resources_gold_monometallic")
        ticker: Stock ticker if available
        company_name: Optional explicit company name
        exchange: Optional exchange id/name
        chairman_model: Optional chairman model override for this run
        evidence_pack: Optional evidence pack containing claim ledger + deterministic lane

    Returns:
        Dict with structured analysis + JSON output
    """
    from .openrouter import query_model
    from .config import (
        CHAIRMAN_MODEL,
        CHAIRMAN_MAX_OUTPUT_TOKENS,
        CHAIRMAN_TIMEOUT_SECONDS,
        CHAIRMAN_OUTPUT_STYLE,
        CHAIRMAN_JSONIFIER_MODEL,
        CHAIRMAN_JSONIFIER_TIMEOUT_SECONDS,
        CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS,
    )
    from .template_loader import get_template_loader

    # Load the template
    loader = get_template_loader()
    template_data = loader.get_template(template_id)

    if not template_data:
        return {
            "model": CHAIRMAN_MODEL,
            "response": f"Error: Template '{template_id}' not found.",
            "structured_data": None,
            "parse_error": "Template not found"
        }

    # Resolve company name and apply placeholder substitutions.
    resolved_company_name = company_name or _infer_company_name(enhanced_context, ticker=ticker)
    rubric = _apply_template_substitutions(
        template_data.get('rubric', ''),
        company_name=resolved_company_name,
        ticker=ticker,
        exchange=exchange,
    )

    # Get the output schema for JSON structure guidance
    output_schema = template_data.get('output_schema', {})
    schema_structure = output_schema.get('structure', {})
    template_json = json.dumps(schema_structure, indent=2)

    # Create weighted context (emphasize top-ranked responses)
    weighted_responses = create_weighted_context(stage1_results, stage2_results, label_to_model)
    original_user_question = _extract_user_question_from_enhanced_context(enhanced_context)

    rankings_summary = create_rankings_summary(stage2_results, label_to_model)
    output_style = str(CHAIRMAN_OUTPUT_STYLE or "text_xml").strip().lower()
    if output_style == "json":
        chairman_prompt = f"""You are the Chairman of an LLM Investment Council. Multiple AI models have analyzed a company and provided detailed responses. They have also peer-reviewed each other's responses. Your task is to synthesize their insights into a single, structured investment analysis.

ORIGINAL USER QUESTION:
{original_user_question}

{weighted_responses}

PEER RANKINGS SUMMARY:
{rankings_summary}

YOUR TASK AS CHAIRMAN:
You must produce a structured investment analysis following this detailed rubric:

{rubric}

CRITICAL REQUIREMENTS:
1. Use the council responses as the primary evidence source and weight higher-ranked responses more heavily.
2. Do not re-run retrieval or introduce unrelated facts; synthesize and adjudicate what the council already produced.
3. Where members disagree materially, record dissent in `extended_analysis.dissenting_views`.
4. Output valid JSON matching this structure:
5. In `investment_verdict`, include EXACTLY:
   - `top_reasons`: top 3 plain-language reasons for success (this is the bull case)
   - `failure_conditions`: top 3 plain-language failure conditions (this is the bear case)
   Keep these concise, specific, and monitorable over time.

{template_json}

IMPORTANT: Your response must be ONLY the JSON output. Do not include any explanatory text before or after the JSON. The JSON must be valid and parseable. Additional useful fields beyond the minimum schema are allowed.

Begin your JSON output now:"""
    else:
        chairman_prompt = _build_chairman_xml_prompt(
            original_user_question=original_user_question,
            weighted_responses=weighted_responses,
            rankings_summary=rankings_summary,
            rubric=rubric,
        )

    messages = [{"role": "user", "content": chairman_prompt}]

    selected_chairman_model = chairman_model or CHAIRMAN_MODEL

    # Query the chairman model with explicit timeout/token budget.
    response = await query_model(
        selected_chairman_model,
        messages,
        timeout=float(CHAIRMAN_TIMEOUT_SECONDS),
        max_tokens=(
            int(CHAIRMAN_MAX_OUTPUT_TOKENS)
            if int(CHAIRMAN_MAX_OUTPUT_TOKENS) > 0
            else None
        ),
    )

    if response is None:
        return {
            "model": selected_chairman_model,
            "response": "Error: Unable to generate structured analysis.",
            "structured_data": None,
            "parse_error": "Chairman model failed to respond"
        }

    response_text = response.get('content', '')

    # Parse/normalize JSON:
    # - If chairman emits JSON directly, parse it.
    # - Otherwise run a secondary JSON-normalizer model (default gpt-4o-mini).
    structured_data, parse_error = _parse_json_from_text(response_text)
    normalization_meta: Dict[str, Any] = {
        "chairman_output_style": output_style,
        "chairman_json_parse_error": parse_error,
        "jsonifier_used": False,
    }

    if not structured_data:
        jsonifier_prompt = _build_jsonifier_prompt(
            schema_json=template_json,
            chairman_text=response_text,
            company_name=resolved_company_name or "the company",
        )
        jsonifier_response = await query_model(
            CHAIRMAN_JSONIFIER_MODEL,
            [{"role": "user", "content": jsonifier_prompt}],
            timeout=float(CHAIRMAN_JSONIFIER_TIMEOUT_SECONDS),
            max_tokens=(
                int(CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS)
                if int(CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS) > 0
                else None
            ),
        )

        normalization_meta["jsonifier_used"] = True
        normalization_meta["jsonifier_model"] = CHAIRMAN_JSONIFIER_MODEL
        normalization_meta["jsonifier_timeout_seconds"] = float(
            CHAIRMAN_JSONIFIER_TIMEOUT_SECONDS
        )
        normalization_meta["jsonifier_max_output_tokens"] = int(
            CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS
        )

        if jsonifier_response is None:
            parse_error = (
                (parse_error + " | ") if parse_error else ""
            ) + "JSON normalizer model failed to respond"
            normalization_meta["jsonifier_parse_error"] = "model failed to respond"
        else:
            jsonifier_text = jsonifier_response.get("content", "")
            normalized_structured, jsonifier_parse_error = _parse_json_from_text(
                jsonifier_text
            )
            normalization_meta["jsonifier_parse_error"] = jsonifier_parse_error
            normalization_meta["jsonifier_response_length"] = len(jsonifier_text or "")
            if normalized_structured:
                structured_data = normalized_structured
                parse_error = None
            else:
                parse_error = (
                    (parse_error + " | ") if parse_error else ""
                ) + (jsonifier_parse_error or "JSON normalizer parse failed")

    # Add metadata from council process
    if structured_data and isinstance(structured_data, dict):
        # Add council metadata if not present
        if "council_metadata" not in structured_data:
            structured_data["council_metadata"] = {}

        # Add analysis date
        structured_data["analysis_date"] = datetime.utcnow().isoformat()

        # Add ticker if available
        if ticker and "ticker" in structured_data:
            structured_data["ticker"] = ticker.upper()

        # Ensure company naming is populated.
        if resolved_company_name:
            if not structured_data.get("company_name"):
                structured_data["company_name"] = resolved_company_name
            if not structured_data.get("company"):
                structured_data["company"] = resolved_company_name
            structured_data["council_metadata"]["resolved_company_name"] = resolved_company_name

        # Add top-ranked models
        from .council import calculate_aggregate_rankings
        aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)
        if aggregate:
            structured_data["council_metadata"]["top_ranked_models"] = [
                f"{r['model']} (avg rank: {r['average_rank']:.2f})"
                for r in aggregate[:3]
            ]

        _apply_market_facts_guardrails(structured_data, market_facts)
        _apply_deterministic_finance_lane(structured_data, evidence_pack)
        _apply_scenario_driver_enrichment(structured_data, response_text)
        _ensure_structured_fields_for_template(structured_data, template_id)
        structured_data["council_metadata"]["normalization"] = normalization_meta

    return {
        "model": selected_chairman_model,
        "response": response_text,
        "structured_data": structured_data,
        "parse_error": parse_error,
        "normalization": normalization_meta,
    }


def create_rankings_summary(stage2_results: List[Dict[str, Any]], label_to_model: Dict[str, str]) -> str:
    """Create a readable summary of the peer rankings."""
    from .council import calculate_aggregate_rankings

    aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)

    if not aggregate:
        return "No peer rankings available."

    lines = ["Aggregate Peer Rankings (lower average rank = better):"]
    for i, item in enumerate(aggregate, 1):
        lines.append(f"  {i}. {item['model']}: Avg Rank {item['average_rank']:.2f} ({item['rankings_count']} votes)")

    return "\n".join(lines)
