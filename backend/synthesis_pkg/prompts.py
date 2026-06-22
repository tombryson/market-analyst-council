"""Chairman XML prompt, jsonifier prompt, context-block helpers, and sector rubrics."""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import logging

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

* Management Quality (20%): Experience and track record in gold mining. Score based on Quantifiable Track Record, Insider Ownership, Capital Discipline.
    * Top-tier (proven multi-project success in region): 100
    * Experienced (solid gold experience): 90
    * Average (mixed or limited track record): 80
    * Weak/Unproven: 60

* Development Stage (10%): Average stage multiplier across projects (weighted by resource size), scaled to 100.

* Funding Chance/Funding Gap (20%): Probability of securing capex for development. Calculate funding gap as (Total Capex - Current Cash - 24-Month Expected Free Cash) / Capex.
    * Gap <A$10M or fully funded: 100
    * Gap A$10-25M with clear path: 80
    * Gap A$25-50M: 60
    * Gap >A$50M or unclear funding: 40

* Certainty % for Goals (12 Months) (10%): Probability of achieving stated milestones.
* ESG Credentials (10%): Permitting Status, Social License, Safety Record.

Quality Score Formula:
= (0.2 * Jurisdiction) + (0.1 * Infrastructure) + (0.2 * Management) + (0.1 * Development Stage) + (0.2 * Funding) + (0.1 * Certainty) + (0.1 * ESG)

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
        from .council import _ranking_labels_from_result
        parsed_ranking = _ranking_labels_from_result(ranking)

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
    from ..template_loader import get_template_loader
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


def _stage2_reconciliation_prompt_block(
    stage2_reconciliation: Optional[Dict[str, Any]],
) -> str:
    """Render the compact discrepancy review for chairman synthesis."""
    if not isinstance(stage2_reconciliation, dict):
        return ""
    if not stage2_reconciliation.get("accepted"):
        return ""

    payload = {
        "status": stage2_reconciliation.get("status"),
        "summary": stage2_reconciliation.get("summary"),
        "blocking": stage2_reconciliation.get("blocking") or [],
        "material": stage2_reconciliation.get("material") or [],
        "unresolved": stage2_reconciliation.get("unresolved") or [],
        "topic_overrides": stage2_reconciliation.get("topic_overrides") or [],
        "stage3_constraints": stage2_reconciliation.get("stage3_constraints") or [],
    }
    rendered = json.dumps(payload, indent=2, ensure_ascii=False)
    if len(rendered) > 9000:
        rendered = rendered[:9000].rstrip() + "\n...[TRUNCATED STAGE 2.5 REVIEW]"
    return (
        "STAGE 2.5 DISCREPANCY REVIEW (single-pass evidence check):\n"
        f"{rendered}\n\n"
        "Evidence precedence for Stage 3:\n"
        "- Peer rankings are a quality signal, not a fact source.\n"
        "- If this review identifies a source-evidence contradiction, resolve or qualify it before synthesis.\n"
        "- If this review identifies a topic override, prefer the evidence-aligned model on that topic even if it ranked lower overall.\n"
        "- Preserve unresolved disputes rather than smoothing them into false certainty.\n"
    )


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


def _build_chairman_xml_prompt(
    *,
    original_user_question: str,
    weighted_responses: str,
    rankings_summary: str,
    rubric: str,
    consensus_nudge: str = "",
    template_contract_guidance: str = "",
    source_fact_guardrails: str = "",
    reconciliation_context: str = "",
) -> str:
    """Prompt chairman for structured plain text (XML-like tags), not JSON."""
    contract_block = ""
    consensus_block = ""
    source_guardrail_block = ""
    if str(template_contract_guidance or "").strip():
        contract_block = f"""
TEMPLATE-SPECIFIC COVERAGE CONTRACT:
{template_contract_guidance}

You must preserve the analytical intent of the selected Stage 1 template through this contract. Do not silently compress or omit industry-specific valuation, monitoring, or verification detail that the contract requires.
"""
    if str(consensus_nudge or "").strip():
        consensus_block = f"""
TOP-RANKED PANEL NUMERIC ANCHOR:
{consensus_nudge}
"""
    if str(source_fact_guardrails or "").strip():
        source_guardrail_block = f"""
PRIMARY-SOURCE FACT GUARDRAILS:
{source_fact_guardrails}

These guardrails are deterministic extracts from the injected primary-source packet. Treat them as higher priority than council paraphrases. If a council response conflicts with these guardrails, identify it as a council error and do not carry the conflicting claim into the final memo.
"""
    return f"""You are the Chairman of an LLM Investment Council. Multiple AI models have analyzed a company and peer-ranked each other.

ORIGINAL USER QUESTION:
{original_user_question}

{weighted_responses}

PEER RANKINGS SUMMARY:
{rankings_summary}
{consensus_block}
{source_guardrail_block}

{reconciliation_context}

YOUR TASK AS CHAIRMAN:
Synthesize a single neutral, decision-useful analysis using the rubric below and the council evidence only.
Do not run new retrieval. Do not add unrelated facts.
Give precedence to higher-ranked responses while still acknowledging the breadth of views across the council.
Use peer rankings as a weighting signal, except where the Stage 2.5 discrepancy review identifies a source-evidence contradiction or topic-specific override.
Make a professional, authoritative investment conclusion from the outputs, mediate between disagreements, and state clearly the council's investment position on the company based on the evidence.

RUBRIC TO HONOR:
{rubric}
{contract_block}

CRITICAL REQUIREMENTS:
1. Use the council evidence and rankings as inputs, but do not merely restate the highest-ranked response.
2. Reconcile disagreements explicitly and state which evidence is strongest, weakest, or unresolved.
3. Use concrete numbers wherever they are available in the council evidence.
4. If a key number is missing, say "Not disclosed" or clearly label it "ESTIMATE" with a short basis.
5. Do not default mechanically to evenly split scenario probabilities. Probabilities must reflect the actual evidence and causal drivers.
6. For bull, base, and bear cases, explain what must happen, what could break, and where the company appears to sit today relative to those paths.
7. Include current financial position where available: cash, debt, current or last reported operating cash flow, free cash flow, revenue, EBITDA, funding runway, or financing need.
8. Include basic valuation framing using the most relevant metrics for the company type, such as EV, market cap, EV/resource, EV/sales, EV/EBITDA, P/NAV, risked NPV, FCF yield, or peer multiple framing.
9. If the company is early-stage and full valuation inputs are unavailable, still provide a compact valuation bridge using the best available numbers and state what is missing.
10. Preserve dissent and uncertainty. Do not smooth over real disagreements.
11. Keep the output analytical, concise, and non-promotional.
12. Use the top-ranked numeric cluster as the default starting point for base-case targets. If you land materially away from that cluster, explain why briefly in <dissenting_views> and <investment_verdict>.
13. Do not list a field as a data gap when the primary-source fact guardrails already disclose it. This applies across sectors: resources/reserves, permits, financing, production, clinical milestones, contracts, ownership, guidance, and other template-specific facts.
14. Do not set a bull/base trigger below the latest disclosed source baseline. If the latest source packet already satisfies a candidate threshold, restate the trigger as an incremental uplift or a higher threshold.

OUTPUT FORMAT:
Return plain text only using the following XML tags exactly once each:
<executive_summary>...</executive_summary>
<quality_and_value_scoring>...</quality_and_value_scoring>
<cash_flow_and_valuation>...</cash_flow_and_valuation>
<price_targets_and_scenarios>...</price_targets_and_scenarios>
<thesis_map>...</thesis_map>
<development_timeline>...</development_timeline>
<monitoring_watchlist>...</monitoring_watchlist>
<verification_queue>...</verification_queue>
<headwinds_tailwinds>...</headwinds_tailwinds>
<dissenting_views>...</dissenting_views>
<investment_verdict>...</investment_verdict>
<data_gaps_and_assumptions>...</data_gaps_and_assumptions>

SECTION REQUIREMENTS:

Inside <cash_flow_and_valuation>, include:
- current cash
- current debt or net cash or net debt
- current or last reported operating cash flow, free cash flow, revenue, or EBITDA, whichever is most decision-relevant and actually available
- projected or expected 12-24 month cash flow direction
- funding gap or funding sufficiency
- 3-6 core valuation metrics with numbers
- short statement of valuation method used for the 12m and 24m targets

Inside <price_targets_and_scenarios>, include:
- current price
- 12m bear, base, bull targets
- 24m bear, base, bull targets
- 12m and 24m scenario probabilities
- 12m and 24m probability-weighted targets
- short causal explanation for each scenario
- short explanation of why the probabilities are weighted as stated

Inside <thesis_map>, provide three blocks: BULL, BASE, BEAR.
For each block include:
- summary
- target_12m
- target_24m
- probability_24m_pct
- required_conditions: 3-5 concise monitorable conditions
- failure_conditions: 2-4 concise break conditions
- current_positioning: bull-leaning, base-leaning, bear-leaning, or mixed
- why_current_positioning: one short explanation

Inside <development_timeline>, include the major milestones in chronological order.
Focus on forward milestones and include at most one prior milestone as reference.

Inside <monitoring_watchlist>, include:
- confirmatory_signals: 3-5 things that would support the thesis
- red_flags: 3-5 things that would weaken or break the thesis
For each item, include what to monitor and why it matters.

Inside <verification_queue>, include only the highest-impact unresolved items.
For each item include:
- field
- reason
- priority
- required_source

Inside <investment_verdict>, include:
- rating
- conviction
- sizing if appropriate
- the single decisive reason the market may be mispricing the company
- the single decisive reason the thesis could fail
- where the evidence leans today: bull, base, bear, or mixed
- top 3 reasons for success (bull case)
- top 3 failure conditions (bear case)

Do NOT output JSON in this step. Output only the tagged plain text."""


def _build_jsonifier_prompt(
    *,
    schema_json: str,
    chairman_text: str,
    company_name: str,
    template_contract_guidance: str = "",
) -> str:
    """Prompt secondary model to convert chairman XML/plain text into strict JSON."""
    contract_block = ""
    if str(template_contract_guidance or "").strip():
        contract_block = f"""
Template-specific normalization contract:
{template_contract_guidance}

When the chairman text is ambiguous, prefer the structure implied by this contract over shallow or lossy normalization.
"""
    return f"""You are a strict JSON normalizer for investment analysis.
Convert the chairman's tagged plain-text analysis into a single valid JSON object.

Target company: {company_name}

Target JSON schema shape:
{schema_json}
{contract_block}

Rules:
1. Output ONLY a single valid JSON object, no markdown.
2. Preserve facts and numbers from the input; do not invent new numeric values by default.
3. Controlled inference rule: if a needed numeric field has no direct value, you may infer it ONLY when your own extraction confidence from the provided input evidence is >=80%.
   Exception: NEVER infer market_data.current_price or price_targets.current_price.
4. For every inferred value, add an entry under data_gaps_and_assumptions.inferred_values[] with:
   - field_path
   - inferred_value
   - confidence_pct
   - basis_text (short quote/paraphrase from chairman input)
   Also append that field_path to verification_required_fields unless already present.
5. Never invent confidence or external evidence. If your confidence is <80% for that metric, leave the field null.
6. If a field is unavailable, use null, empty string, or [] as appropriate.
7. Keep dissent and uncertainty when present.
8. Map content from XML sections into the most relevant schema fields.
9. Map scenario drivers from <price_targets_and_scenarios> into:
   price_targets.scenario_drivers.12m.base|bull|bear
   price_targets.scenario_drivers.24m.base|bull|bear
   using concise arrays of driver strings.
10. Map numeric scenario targets from <price_targets_and_scenarios> into:
   price_targets.scenario_targets.12m.base|bull|bear
   price_targets.scenario_targets.24m.base|bull|bear
   and populate price_targets.target_12m/target_24m from 12m.base and 24m.base.
11. Map scenario probabilities as normalized decimals in [0,1]:
   price_targets.scenario_probabilities.12m.base|bull|bear
   price_targets.scenario_probabilities.24m.base|bull|bear
   If chairman text gives percentages, convert (e.g., 55% => 0.55).
12. Map both weighted fields when available:
   - price_targets.prob_weighted_target_12m
   - price_targets.prob_weighted_target_24m
13. Map current share price into BOTH:
   - market_data.current_price
   - price_targets.current_price
   Use only explicitly stated current/spot/last-traded price from the chairman input.
   If not explicit, leave null (do not infer).
14. Parse <investment_verdict> explicitly:
   - investment_verdict.rating must be one of BUY/HOLD/SELL
   - investment_verdict.conviction must be one of HIGH/MEDIUM/LOW
   - keep investment_verdict.rationale when present
   - map top 3 reasons for success into investment_verdict.top_reasons[]
   - map top 3 failure conditions into investment_verdict.failure_conditions[]
   - map the current lean into extended_analysis.current_thesis_state with:
     - leaning
     - status
     - basis
   - map key risks/opportunities into:
     investment_verdict.key_risks[] and investment_verdict.key_opportunities[]
   If missing in that section, fallback to investment_recommendation values.
15. Parse <cash_flow_and_valuation> into:
   - extended_analysis.cash_flow_and_valuation_summary
   - and map any explicit numeric fields into existing structured fields where an exact schema field already exists.
   Do not invent missing numeric metrics.
16. Map <thesis_map> into thesis_map.bull/base/bear with:
   - summary
   - target_12m
   - target_24m
   - probability_24m_pct
   - required_conditions[]
   - failure_conditions[]
   - current_positioning
   - why_current_positioning
17. If <thesis_map> is absent or incomplete, backfill it from:
   - <investment_verdict>
   - <price_targets_and_scenarios>
   using only explicit chairman content.
   Do not invent new scenarios.
18. Map <monitoring_watchlist> into top-level monitoring_watchlist with:
   - red_flags[]
   - confirmatory_signals[]
   If missing, derive concise watchlist items from thesis_map conditions.
19. Map <verification_queue> into top-level verification_queue[] with:
   - field
   - reason
   - priority
   - required_source
   If missing, derive only the highest-impact unresolved items from:
   - <data_gaps_and_assumptions>
   - verification_required_fields
   - inferred_values
20. Keep investment_verdict concise but informative: rating, conviction, rationale, top_reasons, failure_conditions, key risks, key opportunities.
21. Map <dissenting_views> into top-level `dissenting_views` as either a string or array of strings.
22. Map <management_competition_assessment> into top-level `management_competition_assessment` with:
    - management_quality
    - competition_positioning
    - decision_relevance
23. Map <verification_required_fields> into top-level `verification_required_fields` as an array of field-path strings.

Chairman input:
{chairman_text}
"""


def _render_stage3_template_contract_guidance(
    template_contract: Optional[Dict[str, Any]],
    *,
    include_sections: Optional[List[str]] = None,
    max_chars: int = 4000,
) -> str:
    """Render a compact prompt-friendly summary of the template-specific Stage 3 contract."""
    contract = template_contract if isinstance(template_contract, dict) else {}
    if not contract:
        return ""

    sections = include_sections or [
        "analysis_contract",
        "chairman_contract",
        "jsonifier_contract",
        "monitoring_contract",
    ]

    lines: List[str] = []
    template_id = str(contract.get("id") or "").strip()
    family = str(contract.get("family") or "").strip()
    industry_label = str(contract.get("industry_label") or "").strip()
    if template_id:
        lines.append(f"- template_id: {template_id}")
    if family:
        lines.append(f"- family: {family}")
    if industry_label:
        lines.append(f"- industry_label: {industry_label}")

    for section_name in sections:
        section = contract.get(section_name, {})
        if not isinstance(section, dict) or not section:
            continue
        lines.append(f"- {section_name}:")
        for key, value in section.items():
            if value in (None, "", [], {}):
                continue
            serialized = json.dumps(value, ensure_ascii=True, separators=(", ", ": "))
            lines.append(f"  - {key}: {serialized}")

    rendered = "\n".join(lines).strip()
    if max_chars > 0 and len(rendered) > max_chars:
        rendered = rendered[: max_chars - 3].rstrip() + "..."
    return rendered

