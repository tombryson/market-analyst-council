"""Investment synthesis package.

Re-exports all public symbols from backend.investment_synthesis so callers
need no changes when investment_synthesis.py is replaced by this package.
"""

from .prompts import (
    RESOURCES_RUBRIC,
    PHARMA_RUBRIC,
    create_weighted_context,
    _apply_template_substitutions,
    _build_chairman_xml_prompt,
    _build_jsonifier_prompt,
    _extract_user_question_from_enhanced_context,
    _infer_company_name,
    _render_stage3_template_contract_guidance,
    _stage2_reconciliation_prompt_block,
    _market_facts_prompt_block,
    _deterministic_finance_prompt_block,
)
from .guardrails import (
    _apply_source_fact_guardrails,
    _build_stage3_source_fact_guardrails,
    _inject_stage3_audit_context,
    _split_source_fact_candidates,
    _dedupe_source_fact_lines,
    _rank_energy_guardrail_line,
)
from .json_extract import _parse_json_from_text
from .text_utils import (
    _extract_tagged_section,
    _infer_timeline_status_from_text,
    _strip_markdown_formatting,
    _strip_list_prefix,
    _split_inline_items,
    _dedupe_text_list,
    _normalize_current_positioning_value,
    _derive_positioning_basis,
)
from .verdict import (
    _to_float,
    _normalize_rating_value,
    _normalize_conviction_value,
    _extract_investment_verdict_from_text,
)
from .price_targets import (
    _extract_stage1_price_targets_from_response,
    _build_top_rank_consensus_nudge,
    _extract_price_target_scenario_drivers,
    _extract_price_target_values,
    _extract_price_target_probabilities,
    _apply_scenario_driver_enrichment,
)
from .timeline import (
    _extract_development_timeline_from_text,
    _extract_inline_timeline_period,
    _derive_current_stage_from_timeline_rows,
    _normalize_target_period_label,
    _cap_previous_timeline_rows,
    _timeline_period_to_quarter_index,
    _status_indicates_past,
    _status_indicates_future,
    _timeline_row_is_previous,
)
from .thesis import (
    _extract_thesis_map_from_text,
    _extract_headwinds_tailwinds_from_text,
    _thesis_embedded_label_matches,
    _thesis_text_looks_packed,
    _to_float_from_text,
    _extract_embedded_thesis_fields,
    _merge_embedded_thesis_fields,
    _positioning_basis_looks_polluted,
    _make_condition_item,
    _coerce_condition_list,
    _normalize_condition_entries,
    _extract_structured_thesis_map_from_text,
    _extract_verification_queue_from_text,
    _extract_data_gap_verification_items,
    _normalize_verification_queue_entries,
    _slugify_identifier,
    _normalize_watchlist_object,
    _extract_monitoring_watchlist_from_text,
    _watchlist_lookup,
    _enrich_condition_item,
    _extract_stage1_reference_rows,
)
from .synthesis import (
    synthesize_structured_analysis,
    create_rankings_summary,
    _ensure_structured_fields_for_template,
)

__all__ = [
    "RESOURCES_RUBRIC",
    "PHARMA_RUBRIC",
    "synthesize_structured_analysis",
    "create_rankings_summary",
]
