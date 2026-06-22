"""
Shim — this module has been decomposed into backend/synthesis/.
All public symbols are re-exported here for any surviving legacy imports.
"""
from .synthesis import *  # noqa: F401, F403
from .synthesis import (
    RESOURCES_RUBRIC,
    PHARMA_RUBRIC,
    synthesize_structured_analysis,
    create_rankings_summary,
    # private symbols imported by tests
    _ensure_structured_fields_for_template,
)
from .synthesis.guardrails import _build_stage3_source_fact_guardrails
from .synthesis.thesis import _extract_embedded_thesis_fields, _normalize_condition_entries
