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

def parse_ranking_from_text(ranking_text: str) -> List[str]:
    """
    Parse the FINAL RANKING section from the model's response.

    Args:
        ranking_text: The full text response from the model

    Returns:
        List of response labels in ranked order
    """
    import re

    # Look for "FINAL RANKING:" section
    if "FINAL RANKING:" in ranking_text:
        # Extract everything after "FINAL RANKING:"
        parts = ranking_text.split("FINAL RANKING:")
        if len(parts) >= 2:
            ranking_section = parts[1]
            # Try to extract numbered list format (e.g., "1. Response A")
            # This pattern looks for: number, period, optional space, "Response X"
            numbered_matches = re.findall(r'\d+\.\s*Response [A-Z]', ranking_section)
            if numbered_matches:
                # Extract just the "Response X" part
                return [re.search(r'Response [A-Z]', m).group() for m in numbered_matches]

            # Fallback: Extract all "Response X" patterns in order
            matches = re.findall(r'Response [A-Z]', ranking_section)
            return matches

    # Fallback: try to find any "Response X" patterns in order
    matches = re.findall(r'Response [A-Z]', ranking_text)
    return matches


def calculate_aggregate_rankings(
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Calculate aggregate rankings across all models.

    Args:
        stage2_results: Rankings from each model
        label_to_model: Mapping from anonymous labels to model names

    Returns:
        List of dicts with model name and average rank, sorted best to worst
    """
    from collections import defaultdict

    # Track positions for each model
    model_positions = defaultdict(list)
    first_place_votes = defaultdict(int)
    borda_scores = defaultdict(int)

    for ranking in stage2_results:
        parsed_ranking = _ranking_labels_from_result(ranking)
        total = len(parsed_ranking)

        for position, label in enumerate(parsed_ranking, start=1):
            if label in label_to_model:
                model_name = label_to_model[label]
                model_positions[model_name].append(position)
                borda_scores[model_name] += (total - position + 1)
                if position == 1:
                    first_place_votes[model_name] += 1

    # Calculate average position for each model
    aggregate = []
    for model, positions in model_positions.items():
        if positions:
            avg_rank = sum(positions) / len(positions)
            aggregate.append({
                "model": model,
                "average_rank": round(avg_rank, 2),
                "rankings_count": len(positions),
                "first_place_votes": int(first_place_votes.get(model, 0)),
                "borda_score": int(borda_scores.get(model, 0)),
            })

    # Sort by average rank (lower is better)
    aggregate.sort(key=lambda x: (x['average_rank'], -x.get('first_place_votes', 0), -x.get('borda_score', 0)))

    return aggregate


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
