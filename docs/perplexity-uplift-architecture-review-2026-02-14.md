# Perplexity Uplift: Architecture Review and Improvement Plan (2026-02-14)

## Scope

This document covers:
- current repository architecture and observed runtime behavior
- verified Perplexity platform constraints and capabilities
- prioritized, low-risk improvements to improve retrieval quality, model reliability, and analysis fidelity

Inspected code paths:
- `/Users/Toms_Macbook/Projects/llm-council/backend/main.py`
- `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`
- `/Users/Toms_Macbook/Projects/llm-council/backend/research/providers/perplexity.py`
- `/Users/Toms_Macbook/Projects/llm-council/backend/openrouter.py`
- `/Users/Toms_Macbook/Projects/llm-council/backend/market_facts.py`
- `/Users/Toms_Macbook/Projects/llm-council/backend/investment_synthesis.py`
- latest run artifact `/private/tmp/quality_real_wwi_all3_v2.json`

## External Constraints (Verified)

From current Perplexity docs:
- Responses API supports `stream`, `max_steps`, `max_output_tokens`, and `reasoning.effort`.
- Sonar Pro Search requires streaming for full multi-step behavior; non-streaming falls back to standard behavior.
- Tool filtering is documented under `tools[].filters` (domain/date/recency style filters).
- Rate limits are tier-dependent; deep research and pro workflows have lower RPM than standard search tiers.
- Agent API presets include `deep-research` and `advanced-deep-research`; presets can be customized by passing override fields in the request.
- Preset customization behavior is "inherit defaults, override only provided fields", which allows controlled tuning without rebuilding full preset objects.
- Responses API `max_steps` is bounded (current docs indicate max 10), so broad research must use staged passes rather than a single oversized call.

Implication:
- We should not assume that non-streaming runs are getting full Pro Search behavior.
- We should shape Stage 1 orchestration to expected tier limits and add stagger + backoff by default.
- We should not try to force website-level depth in one request. We need orchestrated multi-wave retrieval and verification loops on top of API primitives.

## Current vs Needed Snapshot

| Capability | Current status | Needed next |
|---|---|---|
| Presets (`deep-research`, `advanced-deep-research`) | Dual-preset strategy is live (`single|adaptive|deep_only|advanced_only`) and defaults to adaptive | Run A/B tests by template/profile and lock strategy per template complexity |
| Preset customization | Request-level overrides supported (model/tokens/reasoning/tools) | Add per-template preset policy (e.g., complex resource templates default to advanced mode) |
| Sonar Pro multi-step | Stream path + Sonar `search_type=pro` path + telemetry pass/fail gate/retry now active | Expand telemetry assertions to include tool-step counters once available in stream payloads |
| Multi-wave retrieval | Planner-driven broad wave + gap-fill wave 2/3 is active in Stage 1 | Tune stop criteria thresholds with production telemetry |
| Verification before council debate | Claim-ledger reconciliation loop is active before Stage 2 and injected into evidence pack | Expand field coverage per template verification schema |
| Deterministic compute | Deterministic finance lane from reconciled claims is active and injected into Stage 2/3 + Stage 3 guardrails | Extend to full deterministic DCF table when all required project inputs are verified |

## Implemented This Iteration

- Stage 1 planner + multi-wave retrieval is now active with gap-filling wave 2/3 and stop conditions.
- Claim ledger reconciliation now runs before Stage 2 and is attached to `aggregated_search_results.evidence_pack`.
- Deterministic finance lane now derives risked NPV/ratio components from reconciled fields and is injected into Stage 3 prompts and output metadata.
- Sonar telemetry gate now evaluates stream/pro-search evidence per run and retries failed Sonar runs before acceptance.
- Default preset strategy is now adaptive (`deep-research` first, `advanced-deep-research` on retries).
- Default Perplexity council model set now includes `sonar-pro`.

## Website-Style Deep Research Emulation (Target Behavior)

The Perplexity website flow you shared is best described as:
- planning -> targeted search wave -> read/fetch -> extract insights
- identify gaps -> targeted search wave 2/3
- extract structured facts -> compute metrics -> verify contradictions
- finalize narrative with citations

To emulate this in our stack, use a controller pattern rather than a single retrieval pass.

### Control Loop Design

1. Planner pass (cheap model / low token budget)
- Input: user template + company/exchange context + mandatory fields from verification schema.
- Output:
  - `research_objectives[]` (what must be proven)
  - `query_plan[]` grouped by objective
  - `must_verify_claim_types[]` (timeline, funding, cap structure, project economics, management changes).

2. Retrieval wave 1 (broad primary-source pull)
- Use Perplexity with source filters favoring exchange filings/company IR domains.
- Keep `max_steps<=10` and bounded source window.
- Persist raw run telemetry: searches, tool invocations, citations, per-source metadata.

3. Decode + denoise + fact ledger build
- Decode fetched pages/PDFs.
- Build structured claim ledger rows:
  - `field`, `value`, `unit`, `effective_date`, `source_url`, `source_authority`, `source_id`, `confidence`, `is_estimate`.
- Build gap map: required fields still unknown or weakly sourced.

4. Retrieval wave 2/3 (gap-driven targeted verification)
- Generate additional queries only for missing/conflicting fields.
- Re-query with narrow search filters and recency/domain constraints.
- Reconcile conflicts by newest high-authority source.

5. Deterministic compute lane
- Run DCF/score/sensitivity calculations in local code from the reconciled claim ledger.
- Keep model role as analyst/interpreter, not arithmetic source of truth.

6. Stage 1 model analysis fanout
- Feed compact fact bundle + verified claim ledger + supporting excerpt appendix.
- Require explicit citation markers for quantitative statements.

7. Stage 2/3 council flow
- Ranking + chairman synthesis operate on corrected Stage 1 outputs.
- Chairman should report unresolved conflicts explicitly, not silently average them.

## Sonar Pro Streaming + Multi-Step Orchestration Plan

### What docs imply for implementation

- Sonar Pro multi-step behavior requires `stream=true`.
- `search_type` should be `pro` or `auto` for Sonar Pro mode.
- Streaming responses expose intermediate reasoning/tool progress events and final content.

### Recommended integration strategy

1. Keep Responses API path for generic models/presets.
- Continue using preset-driven retrieval (`deep-research` / `advanced-deep-research`) where model is not Sonar-specific.

2. Add explicit Sonar Pro execution path.
- When model family is Sonar Pro and `PERPLEXITY_SEARCH_MODE in {pro, auto}`:
  - force `stream=true`
  - send Sonar-compatible search mode parameter (`search_type`)
  - collect reasoning/tool stream events for observability.

3. Add hard guardrails to prevent silent downgrade.
- If Sonar Pro mode requested without streaming, fail fast or auto-upgrade to streaming with warning metadata.
- Log explicit flag `sonar_pro_multistep_enabled`.

4. Add observability payload in run metadata.
- `stream_event_count`
- `reasoning_steps_count`
- `tool_calls_count`
- `queries_executed_count`
- `citations_count`
- `multistep_completed` boolean.

## Preset Strategy (Agent API-Aligned)

Use presets as operating profiles, not one-off prompt hacks:

- `deep-research`:
  - default baseline for routine analyses.
  - lower cost/latency and stable throughput.

- `advanced-deep-research`:
  - triggered when template demands full valuation model + multi-document reconciliation.
  - use for "decision-grade" runs, not every portfolio refresh.

Preset customization guidance:
- Override only what is necessary per run:
  - `model`
  - `max_output_tokens`
  - `reasoning.effort`
  - tool/filter params
- Avoid cloning full preset configuration into app config; that increases drift risk when provider defaults improve.

## High-Level Implementation Plan (Concrete Changes)

### Phase A: Retrieval Orchestrator (small-to-medium patch)

Files:
- `/Users/Toms_Macbook/Projects/llm-council/backend/research/providers/perplexity.py`
- `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`

Changes:
- Add planner output object (`query_plan`, `required_fields`, `verification_targets`).
- Add multi-wave retrieval loop (`wave_index`, `wave_reason`, `queries[]`, `new_claims_count`).
- Add stopping conditions:
  - required-field coverage threshold reached
  - no new high-authority claims in last wave
  - max waves reached.

### Phase B: Claim Ledger + Verification

Files:
- `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`
- `/Users/Toms_Macbook/Projects/llm-council/backend/templates/*.yaml`

Changes:
- Build normalized claim ledger schema.
- Extend template `verification_schema` to declare required fields and reconciliation rules.
- Add strict conflict resolution policy per field (newest primary source wins unless overridden).

### Phase C: Sonar Pro stream-mode path

Files:
- `/Users/Toms_Macbook/Projects/llm-council/backend/research/providers/perplexity.py`
- `/Users/Toms_Macbook/Projects/llm-council/backend/config.py`
- `/Users/Toms_Macbook/Projects/llm-council/.env.example`

Changes:
- Add config:
  - `PERPLEXITY_SONAR_MULTISTEP_ENABLED=true`
  - `PERPLEXITY_SEARCH_MODE=standard|pro|auto`
- Route Sonar models through stream-enforced multistep mode.
- Persist stream telemetry counters in provider metadata.

### Phase D: Deterministic compute lane

Files:
- `/Users/Toms_Macbook/Projects/llm-council/backend/investment_synthesis.py`
- new helper module for DCF/scoring reconciliation

Changes:
- Consume claim ledger for all numeric scoring inputs.
- Emit explicit assumptions table + calculation trace.
- Keep model outputs as interpretation layer over deterministic numbers.

## Best-Practice Defaults (Next Test Cycle)

- `MAX_SOURCES=8` (expand to 10 only after claim-coverage metrics stabilize)
- `PERPLEXITY_STREAM_ENABLED=true`
- `PERPLEXITY_SEARCH_MODE=pro` for Sonar tests, `standard` otherwise
- `PERPLEXITY_MAX_STEPS=10`
- `PERPLEXITY_STAGE1_EXECUTION_MODE=staggered`
- `PERPLEXITY_STAGE1_MAX_ATTEMPTS=3`
- `PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS=3`
- `PERPLEXITY_STAGE1_SECOND_PASS_REASONING_EFFORT=medium`

## Acceptance Criteria For "Website-Like" Depth

Per run, require:
- >=2 retrieval waves when required fields are initially missing
- required-field coverage >=85%
- contradiction resolution log present for timeline/funding/economics fields
- primary-source share in final top-N >=60%
- Stage 1 outputs include citation-backed values for all critical numeric claims
- deterministic compute trace emitted for scoring sections

## Current Architecture (As Implemented)

1. Stage 1 retrieval:
- Perplexity Responses call per configured model (or shared retrieval once + fanout).
- Source ranking + optional local decode (PDF/HTML) in `perplexity.py`.

2. Stage 1 analysis pass:
- Second-pass analysis over decoded evidence through OpenRouter in `council.py`.
- Citation/compliance gate + timeline guard.

3. Stage 2:
- Peer ranking of Stage 1 responses.

4. Stage 3:
- Chairman structured synthesis in `investment_synthesis.py`.

5. Deterministic market prepass:
- `yfinance` + fallback in `market_facts.py`, injected before template query.

## Findings From Latest Real Run

### 1) Source quality drift still occurs in top N
Evidence set included several weak/secondary pages while missing stronger recent primary filings in top slots.

Impact:
- timeline and catalyst extraction become noisy
- Stage 1 downstream reasoning degrades despite successful decode

### 2) Decode is successful but extraction is low-signal
Decoded excerpts are clipped from the leading text window. For many HTML pages this is navigation boilerplate, not financial signal.

Impact:
- timeline extractor gets "Q1/Q2 labels" from page scaffolding, not authoritative milestones
- fact-pack density is low even with 8 decoded sources

### 3) Timeline guard starves when evidence windows are weak
Timeline evidence in the inspected run was sparse and low-authority; guard often becomes non-comparable or passes low-confidence windows.

Impact:
- wrong milestone windows can pass Stage 1
- Stage 2/3 cannot reliably recover from Stage 1 factual drift

### 4) OpenAI second-pass remains the main reliability risk
In latest run OpenAI second-pass failed with `empty_response` after retries while other models returned content.

Impact:
- inconsistent council composition
- ranking/synthesis bias to remaining models

### 5) Stage 2/3 are too late for fact correction
Current flow is synthesis-heavy but correction-light. If Stage 1 evidence is wrong/weak, later stages mostly debate the same weak inputs.

## Improvement Opportunities (Prioritized)

## P0: Immediate, Low-Risk Uplift (1-2 days)

1. Add streaming path for Perplexity retrieval calls.
- Keep existing non-stream fallback.
- Emit step-level SSE/log events: request start, tool step count, citations count, elapsed.
- Gain: observability and early timeout diagnostics.

2. Add explicit Pro Search mode flag.
- `PERPLEXITY_SEARCH_MODE=standard|pro`.
- If `pro`, force streaming and send documented mode fields.
- Gain: predictable use of multi-step search behavior.

3. Align tool filter payload to documented structure.
- Move domain/date filters under `tools[].filters`.
- Keep backward-compat fallback path.
- Gain: filters actually applied, fewer irrelevant sources.

4. Tighten source quality policy for expanded windows.
- Enforce minimum primary-source quota in top-N.
- Penalize stale secondary pages harder when `max_sources >= 8`.
- Gain: better evidence baseline before model analysis.

## P1: Evidence Quality Upgrade (2-4 days)

1. Replace "first N chars" excerpt strategy with query-aware chunking.
- Strip boilerplate nav/footer.
- Split into chunks and score by:
  - rubric keyword overlap
  - numeric density
  - date/milestone terms
  - source authority
- Inject top-K chunks per source, not page prefix.

2. Add compact denoise prepass (accompanying current injection).
- Generate a strict fact bundle from decoded chunks:
  - timeline claims
  - financing claims
  - project economics claims
  - market/share-structure claims
- Keep each claim with source id/date.

3. Upgrade timeline evidence extraction to source-aware windows.
- Prefer explicit milestone phrases from primary filings over generic quarter labels.
- Require at least one high-authority window before "pass".

## P2: OpenAI Robustness and Cost Control (1-2 days)

1. Adaptive second-pass profile by model class.
- OpenAI:
  - attempt 1: medium effort, bounded output tokens
  - attempt 2: low effort, smaller appendix
  - attempt 3: low effort + reduced digest size
- Non-OpenAI: keep current broader defaults.

2. Preserve successful partial output with explicit status.
- Distinguish:
  - `hard_error` (no usable response)
  - `soft_conformance_warning` (usable but incomplete)
- Keep usable output in council with metadata tags.

3. Add model fallback chain for Stage 1 second-pass only.
- Example: `openai/gpt-5.2 -> openai/gpt-5.1`.
- Use only on transport/timeout/empty-response failures.

## P3: Field-Level Verification Layer (2-3 days)

Add template-driven verification queries after Stage 1:
- derive a small set of critical fields from template verification schema
- issue targeted search/fetch checks for each field
- reconcile conflicting windows/values before Stage 2

Important:
- do not hardcode "first_gold_target_window" globally
- derive critical fields from template-specific verification schema

## Architecture Delta (Target)

Current:
- Retrieval -> Decode -> Second-pass analysis -> Ranking -> Chairman

Target:
- Retrieval (streamed, filter-correct) -> Decode (query-aware chunks) -> Fact bundle + field checks -> Second-pass analysis -> Ranking -> Chairman

Main gain:
- factual correction happens before council debate, not after

## Recommended Config Baseline (Production-Safe)

Use this as starting profile:
- `MAX_SOURCES=8`
- `PERPLEXITY_STAGE1_EXECUTION_MODE=staggered`
- `PERPLEXITY_STAGE1_STAGGER_SECONDS=2.0`
- `PERPLEXITY_STAGE1_MAX_ATTEMPTS=3`
- `PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS=5`
- `PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS=3`
- `PERPLEXITY_STAGE1_SECOND_PASS_REASONING_EFFORT=medium`
- `PERPLEXITY_STAGE1_SECOND_PASS_MAX_OUTPUT_TOKENS=6144`
- `PERPLEXITY_STAGE1_SECOND_PASS_APPENDIX_MAX_SOURCES=2`
- `SOURCE_DECODING_MAX_PER_MODEL=8`
- `SOURCE_DECODING_MAX_CHARS=3500`

Then raise depth gradually after verifying completion rate.

## Success Metrics

Track per run:
- Stage 1 completion rate by model
- empty-response rate by model
- % primary sources in top-N
- timeline evidence count and authority mix
- conformance score and warning/hard-error split
- Stage 3 factual correction count vs Stage 1 claims

Target thresholds:
- OpenAI second-pass completion >= 85%
- primary-source share in top-8 >= 60%
- timeline evidence: >= 4 rows with >= 2 high-authority rows
- conformance warnings accepted, hard errors < 10%

## Implementation Order

1. P0 (streaming + filter schema + source quota)
2. P1 (query-aware decode chunking + denoise fact bundle)
3. P2 (OpenAI adaptive profile + hard/soft split + fallback chain)
4. P3 (template-driven field verification)

## Notes on Repository Ownership

Committing this local clone does not modify the original upstream repository unless you explicitly push to its remote. Local commits only affect this clone.

## References

- Perplexity Best Practices: https://docs.perplexity.ai/docs/search/best-practices
- Perplexity Agent API Presets: https://docs.perplexity.ai/docs/agent-api/presets
- Perplexity Advanced Deep Research Preset: https://docs.perplexity.ai/docs/agent-api/presets#advanced-deep-research
- Perplexity Preset Customization: https://docs.perplexity.ai/docs/agent-api/presets#customizing-presets
- Perplexity Agent API Filters: https://docs.perplexity.ai/docs/agent-api/filters
- Perplexity Sonar Features: https://docs.perplexity.ai/docs/sonar/features
- Perplexity Sonar Stream Mode: https://docs.perplexity.ai/docs/sonar/pro-search/stream-mode
- Perplexity Sonar Pro Search Tools: https://docs.perplexity.ai/docs/sonar/pro-search/tools
- Perplexity Pro Search Quickstart: https://docs.perplexity.ai/docs/sonar/pro-search/quickstart
- Perplexity Responses API: https://docs.perplexity.ai/api-reference/post_responses
- Perplexity Usage Tiers / Limits: https://docs.perplexity.ai/getting-started/usage-tiers
