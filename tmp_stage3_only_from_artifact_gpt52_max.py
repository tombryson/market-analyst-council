import asyncio
import json
from datetime import datetime
from pathlib import Path

import backend.openrouter as openrouter
from backend.main import build_enhanced_context
from backend.council import stage3_synthesize_final

ARTIFACT = Path('/Users/Toms_Macbook/Projects/llm-council/outputs/quality_ausgold_dual_chairman_pplx3_20260218_152501.json')
OUT = Path('/Users/Toms_Macbook/Projects/llm-council/outputs') / f"stage3_only_ausgold_gpt52_max_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

orig_query_model = openrouter.query_model

async def query_model_with_cap(model, messages, timeout=120.0, max_tokens=None, reasoning_effort=""):
    return await orig_query_model(
        model,
        messages,
        timeout=timeout,
        max_tokens=16000,
        reasoning_effort=reasoning_effort,
    )

openrouter.query_model = query_model_with_cap

async def main() -> None:
    payload = json.loads(ARTIFACT.read_text(encoding='utf-8'))
    stage1_results = payload.get('stage1_results_for_stage3') or payload.get('stage1_results') or []
    stage2_results = payload.get('stage2_results') or []
    label_to_model = {f"Response {chr(65 + i)}": row.get('model') for i, row in enumerate(stage1_results)}

    effective_query = payload.get('effective_query') or ''
    search_results = (payload.get('metadata') or {}).get('aggregated_search_results') or {}
    market_facts = (payload.get('input_audit') or {}).get('market_facts') or {}

    enhanced_context = build_enhanced_context(effective_query, search_results, [], market_facts=market_facts)

    selection = payload.get('selection') or {}
    template_id = selection.get('template_id') or 'gold_miner'
    company_name = selection.get('company_name')
    exchange = selection.get('exchange')

    ticker = (((payload.get('stage3_result_primary') or {}).get('structured_data') or {}).get('ticker'))

    result = await stage3_synthesize_final(
        enhanced_context,
        stage1_results,
        stage2_results,
        label_to_model=label_to_model,
        use_structured_analysis=True,
        template_id=template_id,
        ticker=ticker,
        company_name=company_name,
        exchange=exchange,
        chairman_model='openai/gpt-5.2',
        market_facts=market_facts,
        evidence_pack=(search_results.get('evidence_pack') if isinstance(search_results, dict) else None),
    )

    output = {
        'source_artifact': str(ARTIFACT),
        'chairman_model': 'openai/gpt-5.2',
        'query_model_override': {'max_tokens': 16000},
        'label_to_model': label_to_model,
        'stage3_result': result,
    }
    OUT.write_text(json.dumps(output, indent=2), encoding='utf-8')
    print(str(OUT))

if __name__ == '__main__':
    asyncio.run(main())
