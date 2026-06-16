"""Stage-3 chairman synthesis.

Feeds all Stage-1 responses, Stage-2 rankings, and the reconciliation
report to the chairman model for final answer synthesis.
"""

import logging
from typing import Any, Dict, List, Optional

from ..config import CHAIRMAN_MODEL
from ..openrouter import query_model
from .stage1_attempt import _progress_log
from .stage2 import _source_evidence_pack_from_stage1_results

logger = logging.getLogger(__name__)

async def stage3_synthesize_final(
    enhanced_context: str,
    stage1_results: List[Dict[str, Any]],
    stage2_results: List[Dict[str, Any]],
    label_to_model: Dict[str, str] = None,
    use_structured_analysis: bool = False,
    template_id: str = None,
    ticker: str = None,
    company_name: str = None,
    exchange: str = None,
    chairman_model: Optional[str] = None,
    market_facts: Optional[Dict[str, Any]] = None,
    evidence_pack: Optional[Dict[str, Any]] = None,
    stage2_reconciliation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Stage 3: Chairman synthesizes final response.

    Args:
        enhanced_context: The enhanced user query including search results and PDF content
        stage1_results: Individual model responses from Stage 1
        stage2_results: Rankings from Stage 2
        label_to_model: Mapping from labels to models (for weighted synthesis)
        use_structured_analysis: If True, use investment analysis rubric
        template_id: Template ID to use for synthesis
        ticker: Stock ticker for structured analysis
        company_name: Optional explicit company name
        exchange: Optional exchange id/name
        chairman_model: Optional chairman model override for this run
        evidence_pack: Optional normalized evidence pack (claim ledger + deterministic lane)

    Returns:
        Dict with 'model' and 'response' keys (and 'structured_data' if applicable)
    """
    # Check if we should use structured investment analysis
    selected_chairman_model = chairman_model or CHAIRMAN_MODEL
    stage_source_evidence_pack = _source_evidence_pack_from_stage1_results(
        stage1_results,
        evidence_pack,
    )

    if use_structured_analysis and template_id and label_to_model:
        from ..synthesis import synthesize_structured_analysis
        return await synthesize_structured_analysis(
            enhanced_context,
            stage1_results,
            stage2_results,
            label_to_model,
            template_id,
            ticker,
            company_name=company_name,
            exchange=exchange,
            chairman_model=selected_chairman_model,
            market_facts=market_facts,
            evidence_pack=stage_source_evidence_pack,
            stage2_reconciliation=stage2_reconciliation,
        )

    # Otherwise use standard synthesis
    # Build comprehensive context for chairman
    stage1_text = "\n\n".join([
        f"Model: {result['model']}\nResponse: {result['response']}"
        for result in stage1_results
    ])

    stage2_text = "\n\n".join([
        f"Model: {result['model']}\nRanking: {result['ranking']}"
        for result in stage2_results
    ])
    reconciliation_text = ""
    if isinstance(stage2_reconciliation, dict) and stage2_reconciliation.get("accepted"):
        reconciliation_text = (
            "\n\nSTAGE 2.5 - Discrepancy Review:\n"
            f"{json.dumps(stage2_reconciliation, indent=2)[:8000]}\n\n"
            "Instruction: peer rankings are useful, but source-evidence contradictions "
            "and topic-specific overrides in this review must take precedence."
        )

    chairman_prompt = f"""You are the Chairman of an LLM Council. Multiple AI models have provided responses to a user's question, and then ranked each other's responses.

Original Question (with context):
{enhanced_context}

STAGE 1 - Individual Responses:
{stage1_text}

STAGE 2 - Peer Rankings:
{stage2_text}
{reconciliation_text}

Your task as Chairman is to synthesize all of this information into a single, comprehensive, accurate answer to the user's original question. Consider:
- The individual responses and their insights
- The peer rankings and what they reveal about response quality
- Any Stage 2.5 discrepancy review constraints, which can override peer ranking on specific evidence conflicts
- Any patterns of agreement or disagreement

Provide a clear, well-reasoned final answer that represents the council's collective wisdom:"""

    messages = [{"role": "user", "content": chairman_prompt}]

    # Query the chairman model
    response = await query_model(selected_chairman_model, messages)

    if response is None:
        # Fallback if chairman fails
        return {
            "model": selected_chairman_model,
            "response": "Error: Unable to generate final synthesis.",
            "stage2_reconciliation": stage2_reconciliation,
        }

    return {
        "model": selected_chairman_model,
        "response": response.get('content', ''),
        "stage2_reconciliation": stage2_reconciliation,
    }


