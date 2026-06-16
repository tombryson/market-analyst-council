"""Analyst memo generation, market-facts guardrails, and synthesize_structured_analysis."""

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
import logging

from ..timeline_normalization import normalize_timeline_rows as _standardize_timeline_rows
from .guardrails import (
    _apply_source_fact_guardrails,
    _build_stage3_source_fact_guardrails,
    _inject_stage3_audit_context,
)
from .json_extract import _parse_json_from_text
from .price_targets import _apply_scenario_driver_enrichment, _build_top_rank_consensus_nudge
from .prompts import (
    _apply_template_substitutions,
    _build_chairman_xml_prompt,
    _build_jsonifier_prompt,
    _extract_user_question_from_enhanced_context,
    _infer_company_name,
    _render_stage3_template_contract_guidance,
    _stage2_reconciliation_prompt_block,
    create_weighted_context,
)
from .text_utils import (
    _dedupe_text_list,
    _derive_positioning_basis,
    _extract_tagged_section,
    _infer_timeline_status_from_text,
    _normalize_current_positioning_value,
)
from .thesis import (
    _coerce_condition_list,
    _enrich_condition_item,
    _extract_data_gap_verification_items,
    _extract_headwinds_tailwinds_from_text,
    _extract_monitoring_watchlist_from_text,
    _extract_stage1_reference_rows,
    _extract_structured_thesis_map_from_text,
    _extract_thesis_map_from_text,
    _extract_verification_queue_from_text,
    _merge_embedded_thesis_fields,
    _normalize_condition_entries,
    _normalize_verification_queue_entries,
    _normalize_watchlist_object,
    _positioning_basis_looks_polluted,
    _slugify_identifier,
    _thesis_text_looks_packed,
    _watchlist_lookup,
)
from .timeline import (
    _cap_previous_timeline_rows,
    _derive_current_stage_from_timeline_rows,
    _extract_development_timeline_from_text,
    _extract_inline_timeline_period,
    _normalize_target_period_label,
)
from .verdict import (
    _extract_investment_verdict_from_text,
    _normalize_conviction_value,
    _normalize_rating_value,
    _to_float,
)

logger = logging.getLogger(__name__)

def _format_stage1_reference_table(stage1_rows: List[Dict[str, Any]]) -> str:
    """Render a compact, table-free Stage 1 model snapshot block."""
    def _fmt(value: Any) -> str:
        parsed = _to_float(value)
        return f"{parsed:.2f}" if parsed is not None else "n/a"

    lines: List[str] = []
    for row in stage1_rows or []:
        t12 = row.get("targets_12m") or {}
        t24 = row.get("targets_24m") or {}
        lines.append(
            "- "
            + f"{str(row.get('model') or 'unknown')}: "
            + f"Q={_fmt(row.get('quality_score'))}, "
            + f"V={_fmt(row.get('value_score'))}, "
            + f"12M B/B/B={_fmt(t12.get('base'))}/{_fmt(t12.get('bull'))}/{_fmt(t12.get('bear'))}, "
            + f"24M B/B/B={_fmt(t24.get('base'))}/{_fmt(t24.get('bull'))}/{_fmt(t24.get('bear'))}"
        )
    return "\n".join(lines)


def _build_analyst_memo_prompt(
    *,
    company_name: str,
    ticker: str,
    stage1_reference_table: str,
    chairman_text: str,
    structured_json: str,
) -> str:
    """Prompt for human-readable market-analyst memo."""
    return f"""You are a senior financial journalist and equity analyst writing a neutral market article for informed investors.

Write a human-readable analyst note using ONLY the inputs below.
Do not invent new numbers or facts. If a value is uncertain, say so explicitly.
Do not repeat XML tags in your output.
Do not use source section labels like "Input A/B/C" in your output.

Company: {company_name}
Ticker: {ticker or "N/A"}

Input A: Stage 1 council model snapshot
{stage1_reference_table}

Input B: Stage 3 chairman synthesis (XML-like text)
{chairman_text}

Input C: Stage 3 structured JSON (normalized)
{structured_json}

Output format (strict Markdown; use EXACT headings and order):
- Start with one short opening paragraph: investment call (rating + conviction + why now).
- Then use these exact H3 headings:
### Valuation and Quality Metrics
### Core Investment Thesis
### Scenario Analysis and Price Targets
### Management and Competitive Landscape
### Risk and Uncertainty
### 90-Day Monitoring Checklist

Section requirements:
- Valuation and Quality Metrics: explicitly state Quality Score total and Value Score total; explain main drivers.
- Core Investment Thesis: explain base/bull/bear logic and what must happen for each path.
- Scenario Analysis and Price Targets: include 12m/24m bull/base/bear targets, probabilities, and probability-weighted targets in prose.
- Management and Competitive Landscape: decision-relevant assessment of leadership quality and competitive position.
- Risk and Uncertainty: key failure modes, thesis invalidation triggers, and material council disagreements/data gaps.
- 90-Day Monitoring Checklist: concise, monitorable checkpoints with practical interpretation.

Hard extraction constraints (must follow):
- Treat Input C (normalized JSON) as the primary numeric truth source.
- If Input C is missing a key numeric field, use Input B (chairman text) and label it as "from chairman narrative".
- If Input B and Input C conflict, prefer Input C and explicitly note the conflict in disagreement/uncertainty.
- You must explicitly include these fields if available:
  - quality_score.total and value_score.total
  - 12m base/bull/bear, 24m base/bull/bear, and both probability-weighted targets
  - current development stage
  - management_competition_assessment (or explicit note that it was not provided)
  - dissenting_views summary
  - verification_required_fields summary (top 3 highest-impact items)
- Do NOT replace specific numeric fields with vague language.

Style:
- Read like a high-quality financial newspaper analysis piece (AFR/FT style): fluid, coherent, and decision-useful.
- Neutral, evidence-led, and non-promotional.
- Prioritize natural paragraph flow over checklist formatting.
- Use bullets only where they materially improve readability.
- Do NOT use markdown tables anywhere in the output.
- Avoid template-like phrasing, XML-like language, and repetitive boilerplate.
- Keep transitions explicit so the thesis, valuation, timeline, and risks read as one continuous argument.
- Avoid hype or promotional wording (e.g., "massive", "tsunami", "explosive upside").
- Do not use bold-only pseudo-headings (e.g., **Heading**). Use only the required H3 headings above.
"""


def _build_analyst_memo_fallback(
    *,
    company_name: str,
    ticker: str,
    structured_data: Dict[str, Any],
    stage1_reference_table: str,
) -> str:
    """Fallback analyst memo when LLM summarizer fails."""
    rec = (structured_data.get("investment_recommendation") or {}) if isinstance(structured_data, dict) else {}
    rating = rec.get("rating") or "UNKNOWN"
    conviction = rec.get("conviction") or "UNKNOWN"
    summary = rec.get("summary") or ""
    price_targets = structured_data.get("price_targets") or {}
    thesis = structured_data.get("thesis_map") or {}
    timeline = structured_data.get("development_timeline") or []

    lines = [
        f"# Investment Analysis: {company_name}",
        "",
        (
            summary
            if summary
            else f"{company_name} ({ticker or 'N/A'}) fallback memo: rating {rating}, conviction {conviction}."
        ),
        "",
        "### Valuation and Quality Metrics",
        f"Quality score: {((structured_data.get('quality_score') or {}).get('total') if isinstance(structured_data, dict) else 'n/a')}",
        f"Value score: {((structured_data.get('value_score') or {}).get('total') if isinstance(structured_data, dict) else 'n/a')}",
        "",
        "### Core Investment Thesis",
        "Base/bull/bear thesis conditions were inferred from structured outputs and may be incomplete in fallback mode.",
        "",
        "### Scenario Analysis and Price Targets",
        f"- 12M target (base): {price_targets.get('target_12m', 'n/a')}",
        f"- 24M target (base): {price_targets.get('target_24m', 'n/a')}",
        "",
    ]

    lines.append("### Management and Competitive Landscape")
    lines.append("Management/competition assessment unavailable in fallback mode unless present in structured output.")
    lines.append("")
    lines.append("### Risk and Uncertainty")
    lines.append("Fallback path triggered; uncertainty is elevated and key fields may require manual verification.")
    lines.append("")
    lines.append("### 90-Day Monitoring Checklist")
    lines.append("- Confirm next milestone timing and status updates in latest filings.")
    lines.append("- Validate market-cap and share-count fields against primary exchange sources.")
    lines.append("- Re-run full synthesis if major financing/operational updates are released.")
    lines.append("")

    for key in ("bull", "base", "bear"):
        block = thesis.get(key) if isinstance(thesis, dict) else None
        if not isinstance(block, dict):
            continue
        lines.append(f"- {key.upper()}: prob={block.get('probability_pct', 'n/a')}%, 12m={block.get('target_12m', 'n/a')}, 24m={block.get('target_24m', 'n/a')}")

    if isinstance(timeline, list) and timeline:
        lines.extend(["", "Timeline checkpoints:"])
        for item in timeline[:8]:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {item.get('target_period', 'n/a')}: {item.get('milestone', 'milestone')} "
                f"(status={item.get('status', 'n/a')}, confidence={item.get('confidence_pct', 'n/a')})"
            )

    return "\n".join(lines)


async def _generate_human_readable_analyst_document(
    *,
    stage1_results: List[Dict[str, Any]],
    structured_data: Dict[str, Any],
    chairman_text: str,
    company_name: str,
    ticker: str,
) -> Dict[str, Any]:
    """Generate a market-analyst style human-readable memo from Stage 3 outputs."""
    from .openrouter import query_model
    from .config import (
        STAGE3_ANALYST_MEMO_ENABLED,
        STAGE3_ANALYST_MEMO_MODEL,
        STAGE3_ANALYST_MEMO_TIMEOUT_SECONDS,
        STAGE3_ANALYST_MEMO_MAX_OUTPUT_TOKENS,
    )

    rows = await _extract_stage1_reference_rows(stage1_results)
    table_md = _format_stage1_reference_table(rows)
    structured_json = json.dumps(structured_data or {}, indent=2)

    if not STAGE3_ANALYST_MEMO_ENABLED:
        return {
            "enabled": False,
            "model": "",
            "content_markdown": _build_analyst_memo_fallback(
                company_name=company_name,
                ticker=ticker,
                structured_data=structured_data or {},
                stage1_reference_table=table_md,
            ),
            "stage1_reference_table_markdown": table_md,
            "stage1_reference_rows": rows,
            "generated_utc": f"{datetime.utcnow().isoformat()}Z",
            "parse_error": "analyst_memo_disabled",
        }

    prompt = _build_analyst_memo_prompt(
        company_name=company_name or "the company",
        ticker=ticker or "",
        stage1_reference_table=table_md,
        chairman_text=chairman_text or "",
        structured_json=structured_json,
    )
    response = await query_model(
        STAGE3_ANALYST_MEMO_MODEL,
        [{"role": "user", "content": prompt}],
        timeout=float(STAGE3_ANALYST_MEMO_TIMEOUT_SECONDS),
        max_tokens=(
            int(STAGE3_ANALYST_MEMO_MAX_OUTPUT_TOKENS)
            if int(STAGE3_ANALYST_MEMO_MAX_OUTPUT_TOKENS) > 0
            else None
        ),
    )

    memo_text = ""
    parse_error: Optional[str] = None
    if response and str(response.get("content") or "").strip():
        memo_text = str(response.get("content") or "").strip()
    else:
        parse_error = "analyst_memo_model_failed"
        memo_text = _build_analyst_memo_fallback(
            company_name=company_name,
            ticker=ticker,
            structured_data=structured_data or {},
            stage1_reference_table=table_md,
        )

    return {
        "enabled": True,
        "model": STAGE3_ANALYST_MEMO_MODEL,
        "timeout_seconds": float(STAGE3_ANALYST_MEMO_TIMEOUT_SECONDS),
        "max_output_tokens": int(STAGE3_ANALYST_MEMO_MAX_OUTPUT_TOKENS),
        "content_markdown": memo_text,
        "stage1_reference_table_markdown": table_md,
        "stage1_reference_rows": rows,
        "generated_utc": f"{datetime.utcnow().isoformat()}Z",
        "parse_error": parse_error,
    }


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
        market_data = structured_data.get("market_data")
        if not isinstance(market_data, dict):
            market_data = {}
            structured_data["market_data"] = market_data

        price_targets = structured_data.get("price_targets")
        if not isinstance(price_targets, dict):
            price_targets = {}
            structured_data["price_targets"] = price_targets

        verification_required = structured_data.get("verification_required_fields")
        if not isinstance(verification_required, list):
            verification_required = []
            structured_data["verification_required_fields"] = verification_required

        existing_fields: Set[str] = set()
        for item in verification_required:
            if isinstance(item, dict):
                field = str(item.get("field") or item.get("field_path") or "").strip()
            else:
                field = str(item or "").strip()
            if field:
                existing_fields.add(field)

        cleared_fields: List[str] = []
        if _to_float(market_data.get("current_price")) is not None:
            market_data["current_price"] = None
            cleared_fields.append("market_data.current_price")
        if _to_float(price_targets.get("current_price")) is not None:
            price_targets["current_price"] = None
            cleared_fields.append("price_targets.current_price")

        for field in ("market_data.current_price", "price_targets.current_price"):
            if field not in existing_fields:
                verification_required.append(field)

        council_meta = structured_data.get("council_metadata")
        if not isinstance(council_meta, dict):
            council_meta = {}
            structured_data["council_metadata"] = council_meta
        council_meta["market_facts_validation"] = {
            "override_allowed": False,
            "aligned_fields": [],
            "corrected_fields": [],
            "unresolved_mismatches": [],
            "prepass_market_facts_present": False,
            "cleared_unverified_fields": cleared_fields,
        }
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

    # Keep price_targets.current_price aligned to market_data.current_price so
    # front-end "Now" anchor never drifts to scenario targets.
    price_targets = structured_data.get("price_targets")
    if not isinstance(price_targets, dict):
        price_targets = {}
        structured_data["price_targets"] = price_targets

    market_current = _to_float(market_data.get("current_price"))
    if market_current is not None:
        pt_current = _to_float(price_targets.get("current_price"))
        threshold = max(0.02, abs(market_current) * 0.08)
        if pt_current is None:
            price_targets["current_price"] = market_current
            aligned_fields.append("price_targets.current_price")
        elif abs(pt_current - market_current) > threshold:
            if override_allowed:
                unresolved_mismatches.append(
                    f"price_targets.current_price: model={pt_current}, market={market_current}"
                )
            else:
                price_targets["current_price"] = market_current
                corrected_fields.append(
                    f"price_targets.current_price: {pt_current} -> {market_current}"
                )

    provenance = structured_data.get("market_data_provenance")
    if not isinstance(provenance, dict):
        provenance = {}
        structured_data["market_data_provenance"] = provenance
    provenance.setdefault("prepass_as_of_utc", market_facts.get("as_of_utc"))
    provenance.setdefault("prepass_source_urls", market_facts.get("source_urls", []))
    provenance.setdefault("prepass_ticker", market_facts.get("ticker", ""))
    provenance.setdefault("prepass_currency", normalized.get("currency"))
    provenance.setdefault("prepass_current_price", normalized.get("current_price"))

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
    chairman_text: str = "",
) -> None:
    """Guarantee key schema fields exist so downstream JSON is stable."""
    if not isinstance(structured_data, dict):
        return

    # Keep investment verdict concise by default.
    verdict = structured_data.get("investment_verdict")
    if not isinstance(verdict, dict):
        verdict = {}
        structured_data["investment_verdict"] = verdict
    verdict["rating"] = _normalize_rating_value(verdict.get("rating"))
    verdict["conviction"] = _normalize_conviction_value(verdict.get("conviction"))

    # Fallback from investment_recommendation when verdict is empty.
    recommendation = structured_data.get("investment_recommendation")
    if not isinstance(recommendation, dict):
        recommendation = {}
        structured_data["investment_recommendation"] = recommendation

    rec_rating = _normalize_rating_value(recommendation.get("rating"))
    rec_conviction = _normalize_conviction_value(recommendation.get("conviction"))
    if not verdict["rating"] and rec_rating:
        verdict["rating"] = rec_rating
    if not verdict["conviction"] and rec_conviction:
        verdict["conviction"] = rec_conviction

    # Final fallback from chairman text extraction.
    parsed_verdict = _extract_investment_verdict_from_text(chairman_text)
    if not verdict["rating"] and parsed_verdict.get("rating"):
        verdict["rating"] = parsed_verdict["rating"]
    if not verdict["conviction"] and parsed_verdict.get("conviction"):
        verdict["conviction"] = parsed_verdict["conviction"]

    # Keep recommendation synchronized.
    recommendation["rating"] = verdict["rating"] or rec_rating or recommendation.get("rating", "")
    recommendation["conviction"] = (
        verdict["conviction"] or rec_conviction or recommendation.get("conviction", "")
    )
    # Keep optional legacy fields present but empty by default for compatibility.
    if not isinstance(verdict.get("top_reasons"), list):
        verdict["top_reasons"] = []
    if not isinstance(verdict.get("failure_conditions"), list):
        verdict["failure_conditions"] = []
    if not isinstance(verdict.get("rationale"), str):
        verdict["rationale"] = ""
    if not isinstance(verdict.get("current_positioning"), str):
        verdict["current_positioning"] = ""
    if not isinstance(verdict.get("why_current_positioning"), str):
        verdict["why_current_positioning"] = ""
    if not isinstance(verdict.get("key_risks"), list):
        verdict["key_risks"] = []
    if not isinstance(verdict.get("key_opportunities"), list):
        verdict["key_opportunities"] = []
    if parsed_verdict.get("top_reasons"):
        verdict["top_reasons"] = list(parsed_verdict.get("top_reasons") or [])
    if parsed_verdict.get("failure_conditions"):
        verdict["failure_conditions"] = list(
            parsed_verdict.get("failure_conditions") or []
        )
    if parsed_verdict.get("rationale") and not verdict["rationale"]:
        verdict["rationale"] = str(parsed_verdict.get("rationale") or "").strip()
    if parsed_verdict.get("current_positioning"):
        verdict["current_positioning"] = str(
            parsed_verdict.get("current_positioning") or ""
        ).strip()
    if parsed_verdict.get("why_current_positioning"):
        verdict["why_current_positioning"] = str(
            parsed_verdict.get("why_current_positioning") or ""
        ).strip()
    normalized_verdict_positioning = _normalize_current_positioning_value(
        verdict.get("current_positioning")
    )
    if normalized_verdict_positioning:
        verdict["current_positioning"] = normalized_verdict_positioning
    dissenting = structured_data.get("dissenting_views")
    if not isinstance(dissenting, (str, list)):
        structured_data["dissenting_views"] = []
    mgmt_comp = structured_data.get("management_competition_assessment")
    if not isinstance(mgmt_comp, dict):
        mgmt_comp = {}
        structured_data["management_competition_assessment"] = mgmt_comp
    if not isinstance(mgmt_comp.get("management_quality"), str):
        mgmt_comp["management_quality"] = ""
    if not isinstance(mgmt_comp.get("competition_positioning"), str):
        mgmt_comp["competition_positioning"] = ""
    if not isinstance(mgmt_comp.get("decision_relevance"), str):
        mgmt_comp["decision_relevance"] = ""
    if not isinstance(structured_data.get("verification_required_fields"), list):
        structured_data["verification_required_fields"] = []
    verification_queue = structured_data.get("verification_queue")
    verification_queue = _normalize_verification_queue_entries(verification_queue)
    if not verification_queue:
        verification_queue = _extract_verification_queue_from_text(chairman_text)
        verification_queue = _normalize_verification_queue_entries(verification_queue)
    if not verification_queue:
        for item in (structured_data.get("verification_required_fields") or [])[:50]:
            if isinstance(item, dict):
                field = str(item.get("field") or item.get("field_path") or "").strip()
                reason = str(item.get("reason") or "").strip()
                required_source = str(item.get("required_source") or "").strip()
                priority = str(item.get("priority") or "medium").strip().lower()
            else:
                field = str(item or "").strip()
                reason = ""
                required_source = ""
                priority = "medium"
            if not field:
                continue
            if priority not in {"high", "medium", "low"}:
                priority = "medium"
            verification_queue.append(
                {
                    "field": field,
                    "priority": priority,
                    "reason": reason or "High-impact uncertain field from chairman synthesis.",
                    "required_source": required_source or "Primary filing / latest company update",
                }
            )
        verification_queue = _normalize_verification_queue_entries(verification_queue)
    if len(verification_queue) < 2:
        existing_keys = {
            str(item.get("field") or item.get("reason") or "").strip().lower()
            for item in verification_queue
            if isinstance(item, dict)
        }
        for extra in _extract_data_gap_verification_items(chairman_text):
            key = str(extra.get("field") or extra.get("reason") or "").strip().lower()
            if not key or key in existing_keys:
                continue
            verification_queue.append(extra)
            existing_keys.add(key)
            if len(verification_queue) >= 6:
                break
    verification_queue = _normalize_verification_queue_entries(verification_queue)
    structured_data["verification_queue"] = verification_queue

    template_key = (template_id or "").strip()
    resource_template_keys = {
        "gold_miner",
        "resources_gold_monometallic",
        "copper_miner",
        "resources_copper_monometallic",
        "lithium_miner",
        "resources_lithium_monometallic",
        "silver_miner",
        "resources_silver_monometallic",
        "uranium_miner",
        "resources_uranium_monometallic",
        "energy_oil_gas",
    }
    gantt_normalized_template_keys = set(resource_template_keys) | {
        "pharma_biotech",
        "financials_bank_insurance",
        "software_saas",
        "industrials_consumer_reit",
        "general_equity",
    }

    def _normalize_key(raw: Any) -> str:
        text = str(raw or "").strip().lower()
        text = re.sub(r"[^a-z0-9]+", "_", text)
        return text.strip("_")

    def _remap_breakdown(
        breakdown: Any,
        canonical_order: List[str],
        aliases: Dict[str, List[str]],
    ) -> Dict[str, Any]:
        if not isinstance(breakdown, dict):
            return {}
        alias_to_canonical: Dict[str, str] = {}
        for canonical, variants in aliases.items():
            for variant in variants:
                alias_to_canonical[_normalize_key(variant)] = canonical
        remapped: Dict[str, Any] = {}
        leftovers: Dict[str, Any] = {}
        for raw_key, raw_value in breakdown.items():
            normalized = _normalize_key(raw_key)
            canonical = alias_to_canonical.get(normalized)
            if canonical:
                remapped[canonical] = raw_value
            else:
                leftovers[str(raw_key)] = raw_value
        ordered: Dict[str, Any] = {}
        for key_name in canonical_order:
            if key_name in remapped:
                ordered[key_name] = remapped[key_name]
        # Preserve unmatched keys for transparency instead of silently dropping.
        ordered.update(leftovers)
        return ordered

    if template_key in gantt_normalized_template_keys:
        quality_score = structured_data.get("quality_score")
        if not isinstance(quality_score, dict):
            quality_score = {}
            structured_data["quality_score"] = quality_score
        value_score = structured_data.get("value_score")
        if not isinstance(value_score, dict):
            value_score = {}
            structured_data["value_score"] = value_score

        quality_breakdown = quality_score.get("breakdown")
        value_breakdown = value_score.get("breakdown")

        if template_key == "pharma_biotech":
            quality_score["breakdown"] = _remap_breakdown(
                quality_breakdown,
                canonical_order=[
                    "regulatory_environment",
                    "scientific_manufacturing",
                    "management",
                    "pipeline_maturity",
                    "cash_runway_funding",
                    "certainty_12m",
                    "clinical_ethical_standards",
                ],
                aliases={
                    "regulatory_environment": [
                        "regulatory_environment",
                        "regulatory path",
                        "regulatory",
                        "jurisdiction",
                    ],
                    "scientific_manufacturing": [
                        "scientific_manufacturing",
                        "scientific & manufacturing capability",
                        "scientific and manufacturing capability",
                        "infrastructure",
                        "cmc_readiness",
                        "cmc",
                        "capability",
                    ],
                    "management": ["management", "management_quality", "management_execution"],
                    "pipeline_maturity": [
                        "pipeline_maturity",
                        "development_stage",
                        "development stage",
                        "stage",
                    ],
                    "cash_runway_funding": [
                        "cash_runway_funding",
                        "cash runway/funding",
                        "funding",
                        "cash_runway",
                        "runway",
                    ],
                    "certainty_12m": [
                        "certainty_12m",
                        "certainty",
                        "certainty_pct",
                        "execution_certainty",
                    ],
                    "clinical_ethical_standards": [
                        "clinical_ethical_standards",
                        "clinical & ethical standards",
                        "clinical_ethics",
                        "clinical_ethical",
                        "clinical_and_ethical_standards",
                        "esg",
                    ],
                },
            )
            value_score["breakdown"] = _remap_breakdown(
                value_breakdown,
                canonical_order=[
                    "rnpv_vs_market_cap",
                    "ev_per_risk_adj_peak_sales",
                    "pipeline_platform_potential",
                    "market_positioning_moat",
                    "ma_strategic_value",
                ],
                aliases={
                    "rnpv_vs_market_cap": [
                        "rnpv_vs_market_cap",
                        "npv_vs_market_cap",
                        "value_npv_vs_market_cap",
                    ],
                    "ev_per_risk_adj_peak_sales": [
                        "ev_per_risk_adj_peak_sales",
                        "ev_risk_adjusted_sales",
                        "ev_risk_adjusted_peak_sales",
                        "ev_per_risk_adjusted_peak_sales",
                        "ev_vs_peak_sales",
                        "ev_peak_sales",
                        "ev_vs_sales_potential",
                        "ev_sales",
                        "ev_resource",
                        "ev_per_resource_oz",
                    ],
                    "pipeline_platform_potential": [
                        "pipeline_platform_potential",
                        "pipeline_optionality",
                        "pipeline optionality",
                        "pipeline_quality",
                        "exploration_upside",
                    ],
                    "market_positioning_moat": [
                        "market_positioning_moat",
                        "competitive_position",
                        "competition",
                        "cost_competitiveness",
                    ],
                    "ma_strategic_value": [
                        "ma_strategic_value",
                        "ma_strategic",
                        "strategic_value",
                    ],
                },
            )
        elif template_key in resource_template_keys:
            quality_score["breakdown"] = _remap_breakdown(
                quality_breakdown,
                canonical_order=[
                    "jurisdiction",
                    "infrastructure",
                    "management",
                    "development_stage",
                    "funding",
                    "certainty",
                    "esg",
                ],
                aliases={
                    "jurisdiction": ["jurisdiction", "regulatory_environment", "regulatory path"],
                    "infrastructure": ["infrastructure", "scientific_manufacturing", "cmc"],
                    "management": ["management", "management_quality", "management_execution"],
                    "development_stage": ["development_stage", "pipeline_maturity", "stage"],
                    "funding": ["funding", "cash_runway_funding", "cash runway/funding"],
                    "certainty": ["certainty", "certainty_12m"],
                    "esg": ["esg", "clinical_ethical_standards"],
                },
            )
            value_score["breakdown"] = _remap_breakdown(
                value_breakdown,
                canonical_order=[
                    "npv_vs_market_cap",
                    "ev_resource",
                    "exploration_upside",
                    "cost_competitiveness",
                    "ma_strategic",
                ],
                aliases={
                    "npv_vs_market_cap": ["npv_vs_market_cap", "rnpv_vs_market_cap"],
                    "ev_resource": ["ev_resource", "ev_per_resource_oz", "ev_per_risk_adj_peak_sales"],
                    "exploration_upside": ["exploration_upside", "pipeline_platform_potential"],
                    "cost_competitiveness": ["cost_competitiveness", "market_positioning_moat"],
                    "ma_strategic": ["ma_strategic", "ma_strategic_value", "strategic_value"],
                },
            )

        # Explicitly remove deprecated fields from final Stage 3 output for resource templates.
        if template_key in resource_template_keys:
            structured_data.pop("all_goals_met_certainty_pct", None)

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

        scenario_probabilities = price_targets.get("scenario_probabilities")
        if not isinstance(scenario_probabilities, dict):
            scenario_probabilities = {}
        for horizon in ("12m", "24m"):
            horizon_prob = scenario_probabilities.get(horizon)
            if not isinstance(horizon_prob, dict):
                horizon_prob = {}
            for scenario in ("base", "bull", "bear"):
                if _to_float(horizon_prob.get(scenario)) is None:
                    horizon_prob[scenario] = None
            scenario_probabilities[horizon] = horizon_prob
        price_targets["scenario_probabilities"] = scenario_probabilities

        if _to_float(price_targets.get("prob_weighted_target_12m")) is None:
            price_targets["prob_weighted_target_12m"] = None
        if _to_float(price_targets.get("prob_weighted_target_24m")) is None:
            price_targets["prob_weighted_target_24m"] = None

        timeline_raw = structured_data.get("development_timeline")
        if isinstance(timeline_raw, list):
            normalized_timeline: List[Dict[str, Any]] = []
            period_pattern = re.compile(
                r"\b(Q[1-4](?:\s*[-/]\s*Q[1-4])?\s*20\d{2}|H[12]\s*20\d{2}|20\d{2})\b",
                re.IGNORECASE,
            )
            for idx, item in enumerate(timeline_raw):
                if isinstance(item, dict):
                    milestone = str(
                        item.get("milestone")
                        or item.get("event")
                        or item.get("name")
                        or item.get("goal")
                        or item.get("title")
                        or ""
                    ).strip()
                    target_period = str(
                        item.get("target_period")
                        or item.get("targetPeriod")
                        or item.get("period")
                        or item.get("when")
                        or item.get("date")
                        or ""
                    ).strip()
                    if milestone and not target_period:
                        inline_period, cleaned_milestone = _extract_inline_timeline_period(milestone)
                        if inline_period:
                            target_period = inline_period
                            milestone = cleaned_milestone or milestone
                    status = str(
                        item.get("status")
                        or item.get("current_status")
                        or item.get("state")
                        or "unspecified"
                    ).strip()
                    inferred_status = _infer_timeline_status_from_text(milestone)
                    if inferred_status and status.lower() in {"", "unspecified", "planned"}:
                        status = inferred_status
                    confidence = _to_float(
                        item.get("confidence_pct")
                        if item.get("confidence_pct") is not None
                        else item.get("certainty_pct")
                    )
                    if milestone or target_period:
                        normalized_target_period = _normalize_target_period_label(target_period)
                        normalized_timeline.append(
                            {
                                "milestone": milestone or f"Milestone {idx + 1}",
                                "target_period": normalized_target_period or target_period,
                                "status": status or "unspecified",
                                "confidence_pct": confidence,
                                "primary_risk": str(
                                    item.get("primary_risk")
                                    or item.get("risk")
                                    or ""
                                ).strip(),
                                **(
                                    {"raw_target_period": target_period}
                                    if normalized_target_period and normalized_target_period != target_period
                                    else {}
                                ),
                            }
                        )
                    continue

                if isinstance(item, str):
                    text = item.strip()
                    if not text:
                        continue
                    milestone = text
                    target_period = ""

                    # Common chairman line: "Q1-Q2 2026: Milestone"
                    colon_split = re.match(r"^([^:]{2,40}):\s*(.+)$", text)
                    if colon_split:
                        lhs = str(colon_split.group(1) or "").strip()
                        rhs = str(colon_split.group(2) or "").strip()
                        if lhs and period_pattern.search(lhs):
                            target_period = lhs
                            milestone = rhs or text

                    if not target_period:
                        period_match = period_pattern.search(text)
                        if period_match:
                            target_period = str(period_match.group(1) or "").strip()
                            stripped = re.sub(r"^[:\-\s]+", "", text.replace(period_match.group(0), "")).strip()
                            milestone = stripped or text

                    normalized_timeline.append(
                        {
                            "milestone": milestone or f"Milestone {idx + 1}",
                            "target_period": _normalize_target_period_label(target_period) or target_period,
                            "status": "unspecified",
                            "confidence_pct": None,
                            "primary_risk": "",
                            **(
                                {"raw_target_period": target_period}
                                if (_normalize_target_period_label(target_period) and _normalize_target_period_label(target_period) != target_period)
                                else {}
                            ),
                        }
                    )
            structured_data["development_timeline"] = _standardize_timeline_rows(normalized_timeline)
        else:
            structured_data["development_timeline"] = []
        if not isinstance(structured_data.get("current_development_stage"), str):
            structured_data["current_development_stage"] = ""

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
        if not headwinds_tailwinds["quantitative"] and not headwinds_tailwinds["qualitative"]:
            extracted_headwinds = _extract_headwinds_tailwinds_from_text(chairman_text)
            if extracted_headwinds.get("quantitative"):
                headwinds_tailwinds["quantitative"] = extracted_headwinds.get("quantitative") or []
            if extracted_headwinds.get("qualitative"):
                headwinds_tailwinds["qualitative"] = extracted_headwinds.get("qualitative") or []

        # First fallback: parse timeline directly from chairman XML tag text.
        if not structured_data["development_timeline"]:
            extracted_rows, extracted_stage, _ = _extract_development_timeline_from_text(
                chairman_text
            )
            if extracted_rows:
                structured_data["development_timeline"] = extracted_rows
            if not structured_data["current_development_stage"] and extracted_stage:
                structured_data["current_development_stage"] = extracted_stage

        # Second fallback: derive a minimal timeline from projects if still empty.
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

        # Third fallback: derive a minimal timeline from pipeline (common in pharma template).
        if not structured_data["development_timeline"]:
            derived_pipeline: List[Dict[str, Any]] = []
            for item in (structured_data.get("pipeline") or [])[:3]:
                if isinstance(item, dict):
                    name = (
                        item.get("candidate")
                        or item.get("name")
                        or item.get("asset")
                        or "Pipeline asset"
                    )
                    stage = (
                        item.get("stage")
                        or item.get("phase")
                        or item.get("status")
                        or "Pipeline milestone"
                    )
                else:
                    name = str(item or "").strip() or "Pipeline asset"
                    stage = "Pipeline milestone"
                derived_pipeline.append(
                    {
                        "milestone": f"{name}: {stage}",
                        "target_period": "",
                        "status": "planned",
                        "confidence_pct": None,
                    }
                )
            if derived_pipeline:
                structured_data["development_timeline"] = derived_pipeline

        # Limit retrospective milestones to one reference item; keep focus on forward timeline.
        if isinstance(structured_data.get("development_timeline"), list):
            structured_data["development_timeline"] = _standardize_timeline_rows(
                structured_data.get("development_timeline") or []
            )
            structured_data["development_timeline"] = _cap_previous_timeline_rows(
                structured_data.get("development_timeline") or [],
                max_previous=1,
            )
        if not structured_data["current_development_stage"]:
            derived_stage = _derive_current_stage_from_timeline_rows(
                structured_data.get("development_timeline") or []
            )
            if derived_stage:
                structured_data["current_development_stage"] = derived_stage
        structured_data.pop("certainty_pct_24m", None)
        if isinstance(structured_data.get("investment_verdict"), dict):
            structured_data["investment_verdict"].pop("certainty_pct_24m", None)

        extended_analysis = structured_data.get("extended_analysis")
        if not isinstance(extended_analysis, dict):
            extended_analysis = {}
        if not isinstance(extended_analysis.get("cash_flow_and_valuation_summary"), str):
            extended_analysis["cash_flow_and_valuation_summary"] = ""
        cash_flow_summary = _extract_tagged_section(
            chairman_text, "cash_flow_and_valuation"
        )
        if (
            not str(extended_analysis.get("cash_flow_and_valuation_summary") or "").strip()
            and cash_flow_summary
        ):
            extended_analysis["cash_flow_and_valuation_summary"] = cash_flow_summary
        current_thesis_state = extended_analysis.get("current_thesis_state")
        if not isinstance(current_thesis_state, dict):
            current_thesis_state = {}
        if not isinstance(current_thesis_state.get("leaning"), str):
            current_thesis_state["leaning"] = ""
        if not isinstance(current_thesis_state.get("status"), str):
            current_thesis_state["status"] = ""
        if not isinstance(current_thesis_state.get("basis"), str):
            current_thesis_state["basis"] = ""
        normalized_state_leaning = _normalize_current_positioning_value(
            current_thesis_state.get("leaning")
        )
        if normalized_state_leaning:
            current_thesis_state["leaning"] = normalized_state_leaning
        if not current_thesis_state["leaning"] and verdict.get("current_positioning"):
            current_thesis_state["leaning"] = str(
                verdict.get("current_positioning") or ""
            ).strip()
        if not current_thesis_state["basis"] and verdict.get("why_current_positioning"):
            current_thesis_state["basis"] = str(
                verdict.get("why_current_positioning") or ""
            ).strip()
        if not current_thesis_state["status"]:
            leaning = str(current_thesis_state.get("leaning") or "").lower()
            if leaning == "mixed":
                current_thesis_state["status"] = "mixed"
            elif leaning:
                current_thesis_state["status"] = "on-track"
        extended_analysis["current_thesis_state"] = current_thesis_state
        next_major_catalysts = extended_analysis.get("next_major_catalysts")
        if not isinstance(next_major_catalysts, list):
            next_major_catalysts = []
        if not next_major_catalysts:
            derived_catalysts: List[str] = []
            for row in (structured_data.get("development_timeline") or [])[:6]:
                if not isinstance(row, dict):
                    continue
                milestone = str(row.get("milestone") or "").strip()
                target_period = str(row.get("target_period") or "").strip()
                if not milestone:
                    continue
                label = f"{target_period}: {milestone}" if target_period else milestone
                derived_catalysts.append(label)
            next_major_catalysts = derived_catalysts
        extended_analysis["next_major_catalysts"] = next_major_catalysts
        structured_data["extended_analysis"] = extended_analysis

        # Ensure thesis_map is consistently structured for gantt/lab consumers.
        thesis_map = structured_data.get("thesis_map")
        if not isinstance(thesis_map, dict):
            thesis_map = {}
            structured_data["thesis_map"] = thesis_map

        extracted_thesis = _extract_thesis_map_from_text(chairman_text)
        extracted_thesis_blocks = _extract_structured_thesis_map_from_text(chairman_text)
        extracted_watchlist = _extract_monitoring_watchlist_from_text(chairman_text)
        extracted_confirmatory_signals = list(
            (extracted_watchlist.get("confirmatory_signals") or [])
            if isinstance(extracted_watchlist, dict)
            else []
        )
        extracted_red_flags = list(
            (extracted_watchlist.get("red_flags") or [])
            if isinstance(extracted_watchlist, dict)
            else []
        )
        scenario_targets = price_targets.get("scenario_targets") or {}
        scenario_probabilities = price_targets.get("scenario_probabilities") or {}
        scenario_drivers = price_targets.get("scenario_drivers") or {}
        for scenario in ("bull", "base", "bear"):
            raw_block = thesis_map.get(scenario)
            if isinstance(raw_block, dict):
                block = raw_block
            elif isinstance(raw_block, str):
                block = {"summary": raw_block.strip()}
            else:
                block = {}
            extracted_block = (
                extracted_thesis_blocks.get(scenario)
                if isinstance(extracted_thesis_blocks, dict)
                else {}
            )
            embedded_sources: List[Any] = [
                block.get("summary") if isinstance(block, dict) else "",
                (extracted_block or {}).get("summary") if isinstance(extracted_block, dict) else "",
                extracted_thesis.get(scenario) if isinstance(extracted_thesis, dict) else "",
            ]
            for source_key in ("required_conditions", "failure_conditions"):
                for source_list in (
                    block.get(source_key) if isinstance(block, dict) else [],
                    (extracted_block or {}).get(source_key) if isinstance(extracted_block, dict) else [],
                ):
                    if not isinstance(source_list, list):
                        continue
                    for raw_item in source_list:
                        if isinstance(raw_item, dict):
                            embedded_sources.append(raw_item.get("condition") or raw_item.get("text") or "")
                        else:
                            embedded_sources.append(raw_item)
            embedded_fields = _merge_embedded_thesis_fields(*embedded_sources)
            if isinstance(extracted_block, dict):
                for key in (
                    "summary",
                    "target_12m",
                    "target_24m",
                    "probability_24m_pct",
                    "current_positioning",
                    "why_current_positioning",
                ):
                    if not block.get(key) and extracted_block.get(key) not in (None, ""):
                        block[key] = extracted_block.get(key)

            summary = str(block.get("summary") or "").strip()
            if _thesis_text_looks_packed(summary) and embedded_fields.get("summary"):
                summary = str(embedded_fields.get("summary") or "").strip()
            if not summary:
                summary = str((extracted_block or {}).get("summary") or "").strip()
            if not summary:
                summary = str(extracted_thesis.get(scenario) or "").strip()
            if _thesis_text_looks_packed(summary) and embedded_fields.get("summary"):
                summary = str(embedded_fields.get("summary") or "").strip()
            if not summary:
                driver_fallback = []
                for horizon in ("24m", "12m"):
                    driver_fallback = (scenario_drivers.get(horizon) or {}).get(scenario) or []
                    if driver_fallback:
                        break
                if isinstance(driver_fallback, list):
                    summary = "; ".join([str(x).strip() for x in driver_fallback if str(x).strip()][:3])
            block["summary"] = summary

            if _to_float(block.get("target_12m")) is None:
                block["target_12m"] = (
                    _to_float(embedded_fields.get("target_12m"))
                    if _to_float(embedded_fields.get("target_12m")) is not None
                    else _to_float((scenario_targets.get("12m") or {}).get(scenario))
                )
            if _to_float(block.get("target_24m")) is None:
                block["target_24m"] = (
                    _to_float(embedded_fields.get("target_24m"))
                    if _to_float(embedded_fields.get("target_24m")) is not None
                    else _to_float((scenario_targets.get("24m") or {}).get(scenario))
                )

            prob_24m = _to_float((scenario_probabilities.get("24m") or {}).get(scenario))
            existing_prob_24m_pct = _to_float(block.get("probability_24m_pct"))
            existing_prob_pct = _to_float(block.get("probability_pct"))
            embedded_prob_pct = _to_float(embedded_fields.get("probability_24m_pct"))
            if existing_prob_24m_pct is not None and existing_prob_24m_pct <= 1.0:
                existing_prob_24m_pct = round(existing_prob_24m_pct * 100.0, 2)
                block["probability_24m_pct"] = existing_prob_24m_pct
            if existing_prob_pct is not None and existing_prob_pct <= 1.0:
                existing_prob_pct = round(existing_prob_pct * 100.0, 2)
                block["probability_pct"] = existing_prob_pct
            if existing_prob_24m_pct is None and embedded_prob_pct is not None:
                existing_prob_24m_pct = embedded_prob_pct * 100.0 if embedded_prob_pct <= 1.0 else embedded_prob_pct
                existing_prob_24m_pct = round(existing_prob_24m_pct, 2)
                block["probability_24m_pct"] = existing_prob_24m_pct
            if existing_prob_pct is None and embedded_prob_pct is not None:
                existing_prob_pct = embedded_prob_pct * 100.0 if embedded_prob_pct <= 1.0 else embedded_prob_pct
                existing_prob_pct = round(existing_prob_pct, 2)
                block["probability_pct"] = existing_prob_pct
            if existing_prob_24m_pct is None and prob_24m is not None:
                existing_prob_24m_pct = round(prob_24m * 100.0, 2)
                block["probability_24m_pct"] = existing_prob_24m_pct
            if existing_prob_pct is None and prob_24m is not None:
                existing_prob_pct = round(prob_24m * 100.0, 2)
                block["probability_pct"] = existing_prob_pct

            condition_logic = block.get("condition_logic")
            if not isinstance(condition_logic, dict):
                condition_logic = {}
            if not isinstance(condition_logic.get("required_conditions"), str):
                condition_logic["required_conditions"] = "all_of"
            if not isinstance(condition_logic.get("failure_conditions"), str):
                condition_logic["failure_conditions"] = "any_of"
            block["condition_logic"] = condition_logic

            required_conditions = _normalize_condition_entries(
                block.get("required_conditions"),
                scenario=scenario,
                prefix="required",
                limit=5,
            )
            if not required_conditions and embedded_fields.get("required_conditions"):
                required_conditions = _coerce_condition_list(
                    embedded_fields.get("required_conditions") or [],
                    scenario=scenario,
                    prefix="required",
                    limit=5,
                )
            if not required_conditions and isinstance(extracted_block, dict):
                required_conditions = _coerce_condition_list(
                    extracted_block.get("required_conditions") or [],
                    scenario=scenario,
                    prefix="required",
                    limit=5,
                )
            if not required_conditions and scenario == "bull":
                required_conditions = _coerce_condition_list(
                    verdict.get("top_reasons") or [],
                    scenario=scenario,
                    prefix="required",
                    limit=5,
                )
            if not required_conditions and scenario == "base":
                base_drivers: List[str] = []
                for horizon in ("24m", "12m"):
                    drivers = (scenario_drivers.get(horizon) or {}).get("base")
                    if isinstance(drivers, list) and drivers:
                        base_drivers = [str(item).strip() for item in drivers if str(item).strip()]
                        break
                required_conditions = _coerce_condition_list(
                    base_drivers,
                    scenario=scenario,
                    prefix="required",
                    limit=5,
                )
            if not required_conditions and summary:
                required_conditions = [
                    {
                        "condition_id": f"{scenario}_thesis_core",
                        "condition": summary,
                        "by": "",
                        "trigger_window": "",
                        "duration": "",
                        "linked_milestones": [],
                        "evidence_hooks": [],
                        "current_status": "monitor",
                    }
                ]
            required_conditions = [
                _enrich_condition_item(
                    dict(item),
                    scenario=scenario,
                    condition_kind="required",
                    confirmatory_signals=extracted_confirmatory_signals,
                    red_flags=extracted_red_flags,
                )
                for item in required_conditions
                if isinstance(item, dict)
            ]
            block["required_conditions"] = required_conditions

            failure_conditions = _normalize_condition_entries(
                block.get("failure_conditions"),
                scenario=scenario,
                prefix="failure",
                limit=4,
            )
            if not failure_conditions and embedded_fields.get("failure_conditions"):
                failure_conditions = _coerce_condition_list(
                    embedded_fields.get("failure_conditions") or [],
                    scenario=scenario,
                    prefix="failure",
                    limit=4,
                )
            if not failure_conditions and isinstance(extracted_block, dict):
                failure_conditions = _coerce_condition_list(
                    extracted_block.get("failure_conditions") or [],
                    scenario=scenario,
                    prefix="failure",
                    limit=4,
                )
            if not failure_conditions and extracted_red_flags:
                failure_conditions = _coerce_condition_list(
                    extracted_red_flags,
                    scenario=scenario,
                    prefix="failure",
                    limit=4,
                )
            if not failure_conditions and verdict.get("failure_conditions"):
                failure_conditions = _coerce_condition_list(
                    verdict.get("failure_conditions") or [],
                    scenario=scenario,
                    prefix="failure",
                    limit=4,
                )
            failure_conditions = [
                _enrich_condition_item(
                    dict(item),
                    scenario=scenario,
                    condition_kind="failure",
                    confirmatory_signals=extracted_confirmatory_signals,
                    red_flags=extracted_red_flags,
                )
                for item in failure_conditions
                if isinstance(item, dict)
            ]
            block["failure_conditions"] = failure_conditions

            if not isinstance(block.get("current_positioning"), str):
                block["current_positioning"] = ""
            if not isinstance(block.get("why_current_positioning"), str):
                block["why_current_positioning"] = ""
            normalized_block_positioning = _normalize_current_positioning_value(
                block.get("current_positioning")
            )
            if normalized_block_positioning:
                block["current_positioning"] = normalized_block_positioning
            if embedded_fields.get("current_positioning") and (
                not block["current_positioning"] or block["current_positioning"] == "mixed"
            ):
                embedded_positioning = _normalize_current_positioning_value(
                    embedded_fields.get("current_positioning")
                )
                if embedded_positioning:
                    block["current_positioning"] = embedded_positioning
            if not block["current_positioning"] and (extracted_block or {}).get(
                "current_positioning"
            ):
                block["current_positioning"] = str(
                    (extracted_block or {}).get("current_positioning") or ""
                ).strip()
            if not block["current_positioning"] and verdict.get("current_positioning"):
                block["current_positioning"] = str(
                    verdict.get("current_positioning") or ""
                ).strip()
            if not block["why_current_positioning"] and (extracted_block or {}).get(
                "why_current_positioning"
            ):
                block["why_current_positioning"] = str(
                    (extracted_block or {}).get("why_current_positioning") or ""
                ).strip()
            if embedded_fields.get("why_current_positioning") and (
                not block["why_current_positioning"]
                or _positioning_basis_looks_polluted(block.get("why_current_positioning"))
            ):
                block["why_current_positioning"] = str(
                    embedded_fields.get("why_current_positioning") or ""
                ).strip()
            if not block["why_current_positioning"] and verdict.get(
                "why_current_positioning"
            ):
                block["why_current_positioning"] = str(
                    verdict.get("why_current_positioning") or ""
                ).strip()

            thesis_map[scenario] = block

        if _positioning_basis_looks_polluted(verdict.get("why_current_positioning")):
            candidate_bases: List[str] = []
            current_thesis_state = verdict.get("current_thesis_state")
            if isinstance(current_thesis_state, dict):
                candidate_bases.append(str(current_thesis_state.get("basis") or "").strip())
            for scenario_name in ("base", "bull", "bear"):
                block = thesis_map.get(scenario_name) if isinstance(thesis_map, dict) else {}
                if isinstance(block, dict):
                    candidate_bases.append(
                        str(block.get("why_current_positioning") or "").strip()
                    )
            candidate_bases.append(str(parsed_verdict.get("why_current_positioning") or "").strip())
            candidate_bases.append(str(parsed_verdict.get("rationale") or "").strip())
            for candidate in candidate_bases:
                if not _positioning_basis_looks_polluted(candidate):
                    verdict["why_current_positioning"] = candidate
                    break

        # Derive monitoring watchlist from thesis conditions when missing.
        monitoring_watchlist = structured_data.get("monitoring_watchlist")
        if not isinstance(monitoring_watchlist, dict):
            monitoring_watchlist = {}
        red_flags_raw = monitoring_watchlist.get("red_flags")
        if not isinstance(red_flags_raw, list):
            red_flags_raw = []
        confirmatory_raw = monitoring_watchlist.get("confirmatory_signals")
        if not isinstance(confirmatory_raw, list):
            confirmatory_raw = []

        def _as_condition_text(value: Any) -> str:
            if isinstance(value, dict):
                return str(value.get("condition") or value.get("condition_id") or "").strip()
            return str(value or "").strip()

        extracted_red_lookup = _watchlist_lookup(extracted_red_flags)
        extracted_confirm_lookup = _watchlist_lookup(extracted_confirmatory_signals)

        red_flags: List[Dict[str, Any]] = []
        for idx, item in enumerate(red_flags_raw, start=1):
            title = (
                item.get("item")
                if isinstance(item, dict)
                else str(item or "").split(":", 1)[0]
            )
            fallback = extracted_red_lookup.get(
                _slugify_identifier(title, fallback=f"red_flags_{idx}")
            )
            normalized = _normalize_watchlist_object(
                item,
                kind="red_flags",
                fallback=fallback,
                idx=idx,
            )
            if normalized:
                red_flags.append(normalized)

        confirmatory_signals: List[Dict[str, Any]] = []
        for idx, item in enumerate(confirmatory_raw, start=1):
            title = (
                item.get("item")
                if isinstance(item, dict)
                else str(item or "").split(":", 1)[0]
            )
            fallback = extracted_confirm_lookup.get(
                _slugify_identifier(title, fallback=f"confirmatory_signals_{idx}")
            )
            normalized = _normalize_watchlist_object(
                item,
                kind="confirmatory_signals",
                fallback=fallback,
                idx=idx,
            )
            if normalized:
                confirmatory_signals.append(normalized)

        if not red_flags and extracted_red_flags:
            red_flags = list(extracted_red_flags)
        if not confirmatory_signals and extracted_confirmatory_signals:
            confirmatory_signals = list(extracted_confirmatory_signals)

        if not red_flags:
            bear_block = thesis_map.get("bear") if isinstance(thesis_map, dict) else {}
            for cond in ((bear_block.get("required_conditions") or []) + (bear_block.get("failure_conditions") or []))[:6]:
                txt = _as_condition_text(cond)
                if not txt:
                    continue
                cid = str((cond.get("condition_id") if isinstance(cond, dict) else "") or "bear_watch").strip()
                trigger_window = str((cond.get("trigger_window") if isinstance(cond, dict) else "") or "").strip()
                duration = str((cond.get("duration") if isinstance(cond, dict) else "") or "").strip()
                red_flags.append(
                    {
                        "watch_id": cid or "bear_watch",
                        "item": txt,
                        "condition": txt,
                        "why_it_matters": "Bear-case trigger from thesis map.",
                        "evidence_hook": "Bear-case trigger from thesis map.",
                        "source_to_monitor": "Company filings and milestone updates",
                        "trigger_window": trigger_window,
                        "duration": duration,
                        "priority": "high",
                        "severity": "high",
                    }
                )

        if not confirmatory_signals:
            for scenario_name in ("base", "bull"):
                block = thesis_map.get(scenario_name) if isinstance(thesis_map, dict) else {}
                for cond in (block.get("required_conditions") or [])[:3]:
                    txt = _as_condition_text(cond)
                    if not txt:
                        continue
                    cid = str((cond.get("condition_id") if isinstance(cond, dict) else "") or f"{scenario_name}_watch").strip()
                    confirmatory_signals.append(
                        {
                            "watch_id": cid or f"{scenario_name}_watch",
                            "item": txt,
                            "condition": txt,
                            "why_it_matters": "Confirmatory thesis condition from scenario map.",
                            "evidence_hook": "Confirmatory thesis condition from scenario map.",
                            "source_to_monitor": "Company filings and milestone updates",
                            "trigger_window": str((cond.get("trigger_window") if isinstance(cond, dict) else "") or "").strip(),
                            "priority": "medium",
                            "severity": "medium",
                        }
                    )

        if not verdict.get("top_reasons"):
            top_reasons = []
            for scenario_name in ("base", "bull"):
                block = thesis_map.get(scenario_name) if isinstance(thesis_map, dict) else {}
                for cond in block.get("required_conditions") or []:
                    text = _as_condition_text(cond)
                    if text:
                        top_reasons.append(text)
            verdict["top_reasons"] = _dedupe_text_list(top_reasons, limit=3)

        if not verdict.get("failure_conditions"):
            failure_reasons = []
            for scenario_name in ("bear", "base"):
                block = thesis_map.get(scenario_name) if isinstance(thesis_map, dict) else {}
                for cond in (block.get("failure_conditions") or []) + (
                    block.get("required_conditions") or []
                ):
                    text = _as_condition_text(cond)
                    if text:
                        failure_reasons.append(text)
            verdict["failure_conditions"] = _dedupe_text_list(failure_reasons, limit=3)

        polluted_positioning = str(verdict.get("why_current_positioning") or "").strip()
        if re.search(
            r"(?i)\b(rating|conviction|top\s*3|decisive market mispricing|decisive failure risk)\b",
            polluted_positioning,
        ):
            cleaner_positioning = _derive_positioning_basis(
                parsed_verdict.get("why_current_positioning") or ""
            )
            if cleaner_positioning:
                verdict["why_current_positioning"] = cleaner_positioning

        monitoring_watchlist["red_flags"] = red_flags
        monitoring_watchlist["confirmatory_signals"] = confirmatory_signals
        structured_data["monitoring_watchlist"] = monitoring_watchlist


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
    stage2_reconciliation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Synthesize a structured investment analysis following template rubric.

    Args:
        enhanced_context: User query with search results and PDFs
        stage1_results: Individual model responses
        stage2_results: Peer rankings
        label_to_model: Mapping from labels to models
        template_id: Template ID to use (e.g., "gold_miner")
        ticker: Stock ticker if available
        company_name: Optional explicit company name
        exchange: Optional exchange id/name
        chairman_model: Optional chairman model override for this run
        evidence_pack: Optional evidence pack containing claim ledger + deterministic lane
        stage2_reconciliation: Optional single-pass discrepancy review from Stage 2.5

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
        CHAIRMAN_JSONIFY_ALWAYS,
        CHAIRMAN_JSONIFIER_TIMEOUT_SECONDS,
        CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS,
    )
    from .template_loader import get_template_loader

    # Load the template
    loader = get_template_loader()
    template_data = loader.get_template(template_id)
    template_contract = loader.get_template_contract(template_id)

    if not template_data:
        return {
            "model": CHAIRMAN_MODEL,
            "response": f"Error: Template '{template_id}' not found.",
            "structured_data": None,
            "parse_error": "Template not found",
            "stage2_reconciliation": stage2_reconciliation,
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
    chairman_contract_guidance = _render_stage3_template_contract_guidance(
        template_contract,
        include_sections=["analysis_contract", "chairman_contract", "monitoring_contract"],
        max_chars=4500,
    )
    jsonifier_contract_guidance = _render_stage3_template_contract_guidance(
        template_contract,
        include_sections=["jsonifier_contract", "monitoring_contract", "chairman_contract"],
        max_chars=3500,
    )

    # Create weighted context (emphasize top-ranked responses)
    weighted_responses = create_weighted_context(stage1_results, stage2_results, label_to_model)
    original_user_question = _extract_user_question_from_enhanced_context(enhanced_context)

    rankings_summary = create_rankings_summary(stage2_results, label_to_model)
    consensus_nudge = _build_top_rank_consensus_nudge(
        stage1_results,
        stage2_results,
        label_to_model,
        top_n=3,
    )
    source_fact_guardrails = _build_stage3_source_fact_guardrails(
        enhanced_context,
        template_id=template_id,
        evidence_pack=evidence_pack,
    )
    reconciliation_context = _stage2_reconciliation_prompt_block(stage2_reconciliation)
    output_style = str(CHAIRMAN_OUTPUT_STYLE or "text_xml").strip().lower()
    if output_style == "json":
        source_guardrail_block = ""
        if source_fact_guardrails:
            source_guardrail_block = f"""
PRIMARY-SOURCE FACT GUARDRAILS:
{source_fact_guardrails}

These guardrails are deterministic extracts from the injected primary-source packet. Treat them as higher priority than council paraphrases. If a council response conflicts with these guardrails, identify it as a council error and do not carry the conflicting claim into the final memo.
"""
        chairman_prompt = f"""You are the Chairman of an LLM Investment Council. Multiple AI models have analyzed a company and provided detailed responses. They have also peer-reviewed each other's responses. Your task is to synthesize their insights into a single, structured investment analysis.

ORIGINAL USER QUESTION:
{original_user_question}

{weighted_responses}

PEER RANKINGS SUMMARY:
{rankings_summary}

TOP-RANKED PANEL NUMERIC ANCHOR:
{consensus_nudge}
{source_guardrail_block}

{reconciliation_context}

YOUR TASK AS CHAIRMAN:
You must produce a structured investment analysis following this detailed rubric:

CHAIRMAN OPERATING RULES:
1. Use ONLY council evidence already provided above.
2. Do NOT run retrieval.
3. Do NOT introduce new facts, assumptions, or external claims.
4. Resolve disagreements by weighting higher-ranked responses and source-grounded arguments.
5. Your job is adjudication and consolidation, not first-principles re-analysis.
6. If data is missing, continue with explicit "Unavailable"/null values and record verification gaps.
7. Use the top-ranked numeric cluster as the default starting point for base-case targets. If you land materially away from it, explain why briefly in dissent-oriented fields.
8. Do not list a field as a data gap when the primary-source fact guardrails already disclose it.
9. For hedging, distinguish partial hedge coverage with residual commodity exposure from "unhedged" or "hedging unknown".
10. Do not set a bull/base trigger below the latest disclosed source baseline. If the latest source packet already satisfies a candidate threshold, restate the trigger as an incremental uplift or a higher threshold.

CRITICAL REQUIREMENTS:
1. Use the council responses as the primary evidence source and weight higher-ranked responses more heavily, except where the Stage 2.5 discrepancy review identifies a source-evidence contradiction or topic-specific override.
2. Do not re-run retrieval or introduce unrelated facts; synthesize and adjudicate what the council already produced.
3. Where members disagree materially, record dissent in `extended_analysis.dissenting_views`.
4. Explicitly cover rubric-priority outputs in this order:
   - `quality_score` and `value_score` (with defensible rationale)
   - `price_targets` including BOTH 12m and 24m, each with base/bull/bear and scenario drivers
   - `current_development_stage` and `development_timeline`
   - `headwinds_tailwinds` with residual items only (not duplicating thesis-map conditions)
   - `thesis_map` for bull/base/bear with monitorable required/failure conditions
   - `management_competition_assessment` (or equivalent field in extended analysis) with decision relevance
   - `current_thesis_state` with bull/base/bear leaning, on-track/at-risk status, and evidence basis
5. `investment_verdict` must include only `rating` and `conviction` (concise).
6. Output valid JSON matching this structure:

{template_json}

TEMPLATE-SPECIFIC COVERAGE CONTRACT:
{chairman_contract_guidance}

PROBABILITY DISCIPLINE:
- Do not default to symmetric scenario probabilities unless explicitly justified by evidence.

AUDITABILITY DISCIPLINE (LIGHT):
- For high-impact numeric claims, include compact source/date attribution where possible.
- If unavailable, mark as unverified and include in verification-required outputs.

NUMERIC COVERAGE DISCIPLINE:
- Ensure numeric conclusions are explicit, not implied:
  - scenario targets and probabilities
  - score totals and weighted logic
  - capital path (liquidity/funding/dilution risk)
  - cashflow/earnings state with latest available numeric markers where present

IMPORTANT: Your response must be ONLY the JSON output. Do not include any explanatory text before or after the JSON. The JSON must be valid and parseable. Additional useful fields beyond the minimum schema are allowed.

Begin your JSON output now:"""
    else:
        chairman_prompt = _build_chairman_xml_prompt(
            original_user_question=original_user_question,
            weighted_responses=weighted_responses,
            rankings_summary=rankings_summary,
            consensus_nudge=consensus_nudge,
            rubric=rubric,
            template_contract_guidance=chairman_contract_guidance,
            source_fact_guardrails=source_fact_guardrails,
            reconciliation_context=reconciliation_context,
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
            "parse_error": "Chairman model failed to respond",
            "stage2_reconciliation": stage2_reconciliation,
        }

    response_text = response.get('content', '')

    # Parse/normalize JSON:
    # - Keep any direct JSON parse only as fallback.
    # - Prefer JSONifier output so Stage 3 consistently derives structured data from
    #   the chairman plain-text memo.
    direct_structured, direct_parse_error = _parse_json_from_text(response_text)
    structured_data = direct_structured
    parse_error = direct_parse_error
    normalization_meta: Dict[str, Any] = {
        "chairman_output_style": output_style,
        "chairman_json_parse_error": direct_parse_error,
        "jsonifier_used": False,
        "jsonifier_forced": bool(CHAIRMAN_JSONIFY_ALWAYS),
    }

    should_use_jsonifier = bool(CHAIRMAN_JSONIFY_ALWAYS) or not bool(direct_structured)
    if should_use_jsonifier:
        jsonifier_prompt = _build_jsonifier_prompt(
            schema_json=template_json,
            chairman_text=response_text,
            company_name=resolved_company_name or "the company",
            template_contract_guidance=jsonifier_contract_guidance,
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
                (direct_parse_error + " | ") if direct_parse_error else ""
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
                if direct_structured:
                    structured_data = direct_structured
                parse_error = (
                    (direct_parse_error + " | ") if direct_parse_error else ""
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

        # Add full Stage 2 ranking telemetry.
        from .council import calculate_aggregate_rankings, compact_stage2_rankings_for_telemetry
        aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)
        if aggregate:
            structured_data["council_metadata"]["stage2_aggregate_rankings"] = aggregate
            structured_data["council_metadata"]["stage2_judge_rankings"] = (
                compact_stage2_rankings_for_telemetry(stage2_results, label_to_model)
            )
            structured_data["council_metadata"]["top_ranked_models"] = [
                f"{r['model']} (avg rank: {r['average_rank']:.2f})"
                for r in aggregate[:3]
            ]
        if isinstance(stage2_reconciliation, dict):
            structured_data["council_metadata"]["stage2_reconciliation"] = {
                "enabled": bool(stage2_reconciliation.get("enabled")),
                "accepted": bool(stage2_reconciliation.get("accepted")),
                "status": stage2_reconciliation.get("status"),
                "issue_count": stage2_reconciliation.get("issue_count"),
                "model": stage2_reconciliation.get("model"),
                "summary": stage2_reconciliation.get("summary"),
                "blocking": stage2_reconciliation.get("blocking") or [],
                "material": stage2_reconciliation.get("material") or [],
                "unresolved": stage2_reconciliation.get("unresolved") or [],
                "topic_overrides": stage2_reconciliation.get("topic_overrides") or [],
                "stage3_constraints": stage2_reconciliation.get("stage3_constraints") or [],
            }

        _apply_market_facts_guardrails(structured_data, market_facts)
        _apply_deterministic_finance_lane(structured_data, evidence_pack)
        _apply_scenario_driver_enrichment(structured_data, response_text)
        _ensure_structured_fields_for_template(
            structured_data,
            template_id,
            chairman_text=response_text,
        )
        _apply_source_fact_guardrails(structured_data, source_fact_guardrails)
        _inject_stage3_audit_context(structured_data, market_facts, template_contract)
        structured_data["council_metadata"]["normalization"] = normalization_meta
        structured_data["template_id"] = str(template_contract.get("id", "") or template_id or "")
        structured_data["council_metadata"]["template_contract"] = {
            "id": str(template_contract.get("id", "") or ""),
            "family": str(template_contract.get("family", "") or ""),
            "industry_label": str(template_contract.get("industry_label", "") or ""),
        }

    analyst_document = await _generate_human_readable_analyst_document(
        stage1_results=stage1_results,
        structured_data=structured_data if isinstance(structured_data, dict) else {},
        chairman_text=response_text,
        company_name=resolved_company_name or "the company",
        ticker=(ticker or "").upper() if ticker else "",
    )

    return {
        "model": selected_chairman_model,
        "response": response_text,
        "chairman_document": {
            "format": "xml_text",
            "content": response_text,
        },
        "analyst_document": analyst_document,
        "structured_data": structured_data,
        "parse_error": parse_error,
        "normalization": normalization_meta,
        "stage2_reconciliation": stage2_reconciliation,
    }


def create_rankings_summary(stage2_results: List[Dict[str, Any]], label_to_model: Dict[str, str]) -> str:
    """Create a readable summary of the peer rankings."""
    from .council import calculate_aggregate_rankings

    aggregate = calculate_aggregate_rankings(stage2_results, label_to_model)

    if not aggregate:
        return "No peer rankings available."

    lines = ["Aggregate Peer Rankings (lower average rank = better):"]
    for i, item in enumerate(aggregate, 1):
        lines.append(
            f"  {i}. {item['model']}: Avg Rank {item['average_rank']:.2f} "
            f"({item['rankings_count']} votes, firsts={item.get('first_place_votes', 0)}, "
            f"borda={item.get('borda_score', 0)})"
        )

    return "\n".join(lines)
