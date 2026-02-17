# Hybrid Stage 1 Failure Audit (WWI)

Date: 2026-02-16
Scope: `stage1_collect_perplexity_research_responses` mixed-provider path, second-pass analysis routing, and WWI run failures.
Artifact audited: `/tmp/wwi_mixed_stage1_validation.json`

## Executive Summary
The failures were caused by the hybrid split introducing a new second-pass routing behavior, plus retry/error-handling issues in the Perplexity second-pass client.

Primary root cause:
- With mixed mode ON, Perplexity-pool models are routed to Perplexity for second-pass analysis (`backend/council.py:4713-4725`).
- With mixed mode OFF, second-pass defaults to OpenRouter for all models (`backend/council.py:4725`).

This means historical "Perplexity model success" runs were mostly succeeding through OpenRouter second-pass, not Perplexity second-pass.

## What changed in hybrid mode

### 1) Provider routing changed materially
Code path:
- `backend/council.py:4713-4725` (`_analysis_provider_for_model`)

Behavior:
- `mixed_mode=False` -> all models `openrouter`
- `mixed_mode=True` -> Perplexity-pool models `perplexity`, OpenRouter-pool models `openrouter`

Impact:
- Perplexity-pool models began using `_query_model_via_perplexity` for second-pass instead of `query_model` (OpenRouter).

### 2) Shared retrieval forced in mixed mode
Code path:
- `backend/council.py:5223-5228`, `5233-5333`

Behavior:
- Mixed mode forces shared retrieval fanout when multiple models are present.
- Retrieval seed selected via `_select_shared_retrieval_model` with first non-openai preference (`backend/council.py:491-517`).

Impact:
- One seed retrieval/decode context is reused across all fanout models; this is not the direct failure trigger here but increases coupling/risk.

## Evidence from WWI failing run

### Per-model second-pass outcomes
From `/tmp/wwi_mixed_stage1_validation.json`:
- `openai/gpt-5.1`: second-pass success true, compliance amber, citation gate fail.
- `google/gemini-3-pro-preview`: second-pass error `empty_response`, compliance red.
- `anthropic/claude-sonnet-4.5`: second-pass error `empty_response`, compliance red.
- OpenRouter pool (`minimax`, `glm-5`, `kimi`) produced usable analysis; all amber due citation/compliance thresholds.

### Direct API probe findings (same account/config)

1) `anthropic/claude-sonnet-4.5` via Perplexity Responses API
- Returns 400 unsupported model.
- This guarantees Perplexity-lane second-pass failure for Claude in current account/API context.

2) `google/gemini-3-pro-preview` via Perplexity Responses API
- `reasoning.effort=medium` returns 400 invalid_request (reproducible).
- `high`/`low` accepted, but high with large prompt often yields tiny/truncated usable text and can fail conformance gate.

### Retry-policy interaction causing hard fail
Code path:
- `backend/council.py:3953-3958` (high -> medium -> low retry degradation)
- `backend/council.py:4048-4062` (gate fail -> retry)
- `backend/council.py:4183-4216` (final empty_response failure)

Impact:
- Gemini attempt 1 (high) can produce weak output that fails gate.
- Attempt 2 downgrades to medium, which is invalid_request for Gemini on Perplexity in this setup.
- Final status becomes `empty_response` and prior attempt content is discarded.

## Secondary implementation gaps found

### A) HTTP errors are collapsed to None in Perplexity second-pass helper
Code path:
- `backend/council.py:454-465`

Impact:
- Unsupported/invalid_request errors become generic `empty_response` later.
- Observability is degraded; root cause hidden in metadata.

### B) No "best-attempt" fallback in second-pass loop
Code path:
- `backend/council.py:3933-4216`

Impact:
- If early attempt had partial content but later retry errors, final result can be full failure instead of best-available response.

## Why this looked like "hybrid broke everything"
This diagnosis is correct in practice:
- Hybrid switched Perplexity-pool models to Perplexity second-pass path.
- That path currently has model-compatibility/retry issues (Claude unsupported; Gemini medium invalid; weak fallback handling).
- Therefore previously stable behavior regressed immediately when hybrid routing was enabled.

## Immediate recovery options

### Option A (fastest stability)
- Disable mixed mode now:
  - `PERPLEXITY_STAGE1_MIXED_MODE_ENABLED=false`
- This reverts to prior behavior where second-pass uses OpenRouter lane for all models.

### Option B (keep hybrid, fix correctness)
Implement these patches:
1. Model-aware retry ladder for Perplexity second-pass:
   - Gemini: `high -> low -> no reasoning` (skip `medium`).
2. Preserve best successful attempt content if later attempts fail.
3. Propagate Perplexity HTTP error details into second-pass error metadata (no `empty_response` masking).
4. Capability fallback:
   - On Perplexity 400 unsupported model, auto-fallback second-pass to OpenRouter for that model.

## Recommendation
- Short term: use Option A immediately for reliability.
- Then implement Option B and re-enable mixed mode after one controlled WWI validation pass.
