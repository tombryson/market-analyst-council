"""LLM Council orchestration package.

All public symbols that external code imports from ``backend.council`` are
re-exported here so callers require no changes when council.py is replaced
by this package.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from ..config import (
    STAGE2_RECONCILIATION_ENABLED,
    STAGE2_REVISION_PASS_ENABLED,
)
from ..openrouter import query_model
from .stage1 import (
    stage1_collect_responses,
    stage1_collect_perplexity_research_responses,
    _assess_stage1_truncation,
    _build_stage1_mandatory_fact_ledger,
    _build_stage1_second_pass_prompt,
    _stage1_response_looks_truncated,
    _validate_stage1_prompt_mandatory_fact_coverage,
)
from .stage1_attempt import _ensure_system_enabled, _progress_log
from .stage2 import (
    apply_stage2_revision_deltas,
    compact_stage2_rankings_for_telemetry,
    stage2_collect_rankings,
    stage2_collect_reconciliation,
    stage2_collect_revision_deltas,
    _build_stage2_reconciliation_prompt,
    _normalize_stage2_reconciliation_payload,
    _ranking_labels_from_result,
)
from .stage3 import stage3_synthesize_final
from .perplexity_client import _is_openrouter_compatible_model

logger = logging.getLogger(__name__)

from .stage2 import parse_ranking_from_text, calculate_aggregate_rankings


async def generate_conversation_title(user_query: str) -> str:
    """
    Generate a short title for a conversation based on the first user message.

    Args:
        user_query: The first user message

    Returns:
        A short title (3-5 words)
    """
    title_prompt = f"""Generate a very short title (3-5 words maximum) that summarizes the following question.
The title should be concise and descriptive. Do not use quotes or punctuation in the title.

Question: {user_query}

Title:"""

    messages = [{"role": "user", "content": title_prompt}]

    # Use a lightweight Gemini model for title generation.
    response = await query_model("google/gemini-3-flash-preview", messages, timeout=30.0)

    if response is None:
        # Fallback to a generic title
        return "New Conversation"

    title = response.get('content', 'New Conversation').strip()

    # Clean up the title - remove quotes, limit length
    title = title.strip('"\'')

    # Truncate if too long
    if len(title) > 50:
        title = title[:47] + "..."

    return title


async def run_full_council(
    enhanced_context: str,
    use_structured_analysis: bool = False,
    template_id: str = None,
    ticker: str = None,
    company_name: str = None,
    exchange: str = None,
) -> Tuple[List, List, Dict, Dict]:
    """
    Run the complete 3-stage council process.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content
        use_structured_analysis: If True, use analysis template with structured output
        template_id: Template ID to use for synthesis
        ticker: Stock ticker for structured analysis
        company_name: Optional explicit company name
        exchange: Optional exchange id/name

    Returns:
        Tuple of (stage1_results, stage2_results, stage3_result, metadata)
    """
    _ensure_system_enabled(diagnostic_mode=False)
    # Stage 1: Collect individual responses
    stage1_results = await stage1_collect_responses(enhanced_context)

    # If no models responded successfully, return error
    if not stage1_results:
        return [], [], {
            "model": "error",
            "response": "All models failed to respond. Please try again."
        }, {}

    # Stage 2: Collect rankings
    stage2_results, label_to_model = await stage2_collect_rankings(enhanced_context, stage1_results)

    # Calculate aggregate rankings
    aggregate_rankings = calculate_aggregate_rankings(stage2_results, label_to_model)

    stage1_results_for_stage3 = stage1_results
    stage2_revision_results: List[Dict[str, Any]] = []
    stage2_revision_summary: Dict[str, Any] = {"enabled": False}
    if STAGE2_REVISION_PASS_ENABLED:
        stage2_revision_results, stage2_revision_summary = await stage2_collect_revision_deltas(
            enhanced_context,
            stage1_results,
            stage2_results,
            label_to_model,
            revision_models=[item.get("model") for item in stage1_results if item.get("model")],
        )
        stage1_results_for_stage3, apply_summary = apply_stage2_revision_deltas(
            stage1_results,
            stage2_revision_results,
        )
        stage2_revision_summary["apply"] = apply_summary

    stage2_reconciliation = await stage2_collect_reconciliation(
        enhanced_context,
        stage1_results_for_stage3,
        stage2_results,
        label_to_model,
    )

    # Stage 3: Synthesize final answer (with optional structured analysis)
    stage3_result = await stage3_synthesize_final(
        enhanced_context,
        stage1_results_for_stage3,
        stage2_results,
        label_to_model=label_to_model,
        use_structured_analysis=use_structured_analysis,
        template_id=template_id,
        ticker=ticker,
        company_name=company_name,
        exchange=exchange,
        evidence_pack=None,
        stage2_reconciliation=stage2_reconciliation,
    )

    # Prepare metadata
    metadata = {
        "label_to_model": label_to_model,
        "aggregate_rankings": aggregate_rankings,
        "stage2_revision_pass_enabled": bool(STAGE2_REVISION_PASS_ENABLED),
        "stage2_revision_summary": stage2_revision_summary,
        "stage2_revision_results": stage2_revision_results,
        "stage2_reconciliation": stage2_reconciliation,
    }

    return stage1_results_for_stage3, stage2_results, stage3_result, metadata
