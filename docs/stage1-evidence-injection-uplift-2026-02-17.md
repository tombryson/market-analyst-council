# Stage 1 Evidence Injection Uplift (Hard Reset)
Date: 2026-02-17  
Scope: Stage 1 retrieval, decode, denoise, evidence injection, and downstream Stage 2/3 reliability.

## 1) Executive Summary
The current Stage 1 payload path is not fit for financial analysis-grade outputs. The core failure is not model intelligence; it is evidence quality and evidence packaging. The system is currently over-compressing, over-truncating, and mixing low-signal retrieval summaries with partially useful decoded excerpts.

This uplift resets Stage 1 around one rule:

`Models must receive self-contained, source-grounded evidence text they can reason over without opening links.`

## 2) Critical Failures (Observed)
Primary artifact audited:
- `/Users/Toms_Macbook/Projects/llm-council/outputs/tgm_stage1_second_pass_prompt_openai_gpt_5_1.txt`

Observed failures:
- Corrupted synthesis text injected into prompt (`# Investmentheta...`, broken sentence fragments).
- "Latest updates" includes stale 2022/2023 rows in a current analysis context.
- Link-first summary table is injected even though Stage 1 analysis models do not browse from this prompt.
- Two update blocks are injected (table plus bullet list), causing conflicting context.
- Facts are aggressively truncated across multiple pipeline stages, often to short fragments.
- Fact classification quality is poor for several sections (market/share fields contaminated by financing snippets, admin text).
- Appendix coverage is partial compared to source index (not all sources represented equally).
- Timeline logic previously had directional constraints; this has been relaxed, but prompt and extraction bias still needed cleanup.

## 3) Root Cause Analysis (Code-Level)
### 3.1 Multi-stage truncation destroys evidence
Current code applies truncation repeatedly:
- Source excerpt cap: `_prepare_stage1_source_rows` truncates at `PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE`  
  File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py:2668`
- Sentence cap at extraction: `len(sentence) > 220` then truncate  
  File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py:2795`
- Compact bundle fact cap: truncate to 260  
  File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py:3110`
- Prompt row cap: `_compact_prompt_fact_row` uses `PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS` (currently 180)  
  Files: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py:3279`, `/Users/Toms_Macbook/Projects/llm-council/backend/config.py:348`

Impact: facts become short fragments lacking numeric context, qualifiers, and sentence boundaries.

### 3.2 Link-centric summary block pollutes Stage 1
Prompt builder injects retrieval summary and update links directly:
- `RESEARCH SUMMARY FROM RETRIEVAL PASS`
- `LATEST UPDATES (from retrieval)`
  File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py:4605`

Provider summary path builds markdown tables with links and generic "why it matters":
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/research/providers/perplexity.py:2753`

Impact: models consume low-signal summaries and URL-heavy noise before factual evidence.

### 3.3 Prompt compression can drop core evidence sections
When prompt budget hits target, system can slim or omit major evidence blocks:
- Slim path and omission path:
  - `compact_fact_bundle` replaced by omitted note
  - `fact_digest` replaced by omitted note
  - `fact_pack` replaced by omitted note
  File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py:5275`

Impact: fallback behavior can silently remove the strongest data.

### 3.4 Source list and appendix inconsistencies
- Builder includes top sources for index, but appendix size is independently capped:
  File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py:5147`
- Summary path has separate update limits:
  File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py:4588`

Impact: evidence representation is inconsistent and can bias what models see.

### 3.5 Configuration complexity amplifies instability
There are 47 `PERPLEXITY_STAGE1_*` environment toggles:
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/config.py`

Impact: behavior is hard to predict, hard to tune, and hard to debug reliably.

## 4) Target State Architecture
## 4.1 Single canonical Evidence Pack (Stage1EvidencePack v2)
Replace mixed summary blocks with one deterministic payload:
- `normalized_facts`
- `source_index` with quality metadata
- `claim_ledger` rows for numeric fields
- `timeline_ledger` rows for milestone facts
- `narrative_ledger` rows for non-numeric but material evidence
- `critical_gaps`

Every ledger row must include:
- `field` (or `theme`)
- `value` (if numeric), `unit`, `as_of_date`
- `source_id`
- `quote` (minimum context window)
- `source_url`
- `confidence`
- `estimate` boolean and reason if inferred

## 4.2 Prompt contract simplification
Stage 1 prompt should contain:
- User template query (full rubric)
- Normalized market facts
- Canonical evidence pack only
- Output requirements

Stage 1 prompt should not contain:
- Markdown update tables
- Generic "why it matters" boilerplate
- Duplicate "latest updates" sections
- Link-only lists without supporting quote text

## 4.3 Self-contained evidence rule
No evidence row is accepted if it is only title + URL.
Each selected source must contribute at least one quote-level evidence row or it is excluded from Stage 1 prompt injection.

## 5) Implementation Plan
## P0: Immediate Hardening (now)
1. Remove retrieval-summary table injection from Stage 1 prompt.
- Keep summary in metadata only, not in model prompt.
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`

2. Remove duplicate update blocks from Stage 1 prompt.
- Keep one canonical source list inside evidence pack.
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`

3. Raise evidence row character budgets.
- `PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS`: 180 -> 420
- Sentence truncation in `_extract_source_sentences`: 220 -> 420
- Compact bundle fact truncation: 260 -> 420
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/config.py`

4. Always build source key points, not only when prompt compression is triggered.
- Source key points become standard evidence channel.
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`

5. Enforce "no link-only evidence row" gate.
- Any source lacking quote-bearing rows is excluded.
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`

6. Keep timeline logic observational only (already partially done) and remove special-case directional rules from prompts.
- File: `/Users/Toms_Macbook/Projects/llm-council/backend/council.py`

## P1: Evidence Pack v2 (short sprint)
1. Introduce `Stage1EvidencePack v2` schema and serializer.
- Add explicit ledger arrays with stable field names.
- Add per-row quote context and source provenance.

2. Convert fact extraction from sentence fragments to claim rows.
- Parse key numeric fields with regex + section-aware extraction.
- Keep neighboring context span around extracted values.

3. Add recency and authority weighting at source row level.
- Recency window defaults:
  - Primary filings target: last 12 months
  - Allow older documents only when they carry core baseline economics.
- Authority ranking order:
  - Exchange filings / company reports
  - Official investor docs
  - High-quality secondary coverage
  - General commentary

4. Add stale-source and low-signal guards before prompt assembly.
- Reject source packs where stale/noisy rows dominate.

## P2: Deterministic Rubric Field Coverage
1. Add deterministic required-field coverage checks before model call.
- Required core fields by template section.
- If missing: trigger targeted retrieval gap-fill wave.

2. Add hard evidence completeness gate.
- Do not run Stage 1 model if evidence pack fails minimum thresholds (see Section 7).

3. Separate hard extraction failures from soft conformance warnings.
- Hard fail: missing core evidence pack quality.
- Soft warning: model omitted a rubric subsection.

## P3: Stage 2/3 Reliability Uplift
1. Stage 2 ranking prompt to include evidence quality diagnostics per response.
- Penalize responses that invent values outside evidence pack.

2. Stage 3 chairman prompt update:
- Preserve full template structure.
- Force explicit treatment of `development_timeline`, `next_major_catalysts`, `sensitivity_analysis`, `market_context`, `commentary`.
- Reduce overweighting of headline scalar scores in chairman narrative.

3. Carry deterministic claim ledger into Stage 3 with explicit "cannot override without newer primary source" rule.

## 6) Configuration Uplift
Current Stage 1 config is over-fragmented (47 env toggles).  
Reduce to profile-based config:
- `STAGE1_PROFILE=balanced|deep|max`
- `STAGE1_EVIDENCE_PROFILE=v2_strict`
- `STAGE1_MODEL_PROFILE=openai_heavy|mixed|sonar_only`

Keep advanced toggles but mark as expert-only. Use profile defaults for day-to-day runs.

## 7) Acceptance Criteria (Definition of Done)
Evidence quality gates per model run:
1. `source_count >= 8` and `decoded_source_count >= 8`
2. `claim_ledger.numeric_rows >= 30`
3. `median_quote_chars >= 220`
4. `link_only_rows == 0`
5. `stale_rows_ratio (older than 18 months) <= 0.20` unless explicitly justified
6. `critical_gaps` must not include core template-critical sections
7. Stage 1 prompt must not include markdown update table from retrieval summary
8. Stage 1 prompt must include canonical evidence pack and timeline ledger

Model-output quality gates:
1. Numeric citation coverage >= configured threshold
2. No catastrophic template non-conformance
3. Explicit `development_timeline` and `next_major_catalysts` populated

## 8) Observability and Auditability
Persist these artifacts per run:
- Raw retrieval results
- Decoded source excerpts pre- and post-sanitization
- Stage1EvidencePack v2 JSON
- Actual model prompt per model
- Gate metrics and reasons

Add a first-class injection quality report:
- `injection_quality_score` (0-100)
- `source_quality_breakdown`
- `field_coverage_breakdown`
- `staleness_breakdown`
- `noise_rejection_breakdown`

## 9) Rollout Strategy
1. Build in shadow mode.
- Produce both current pack and v2 pack.
- Do not change Stage 1 prompts yet.

2. Compare on fixed benchmark set.
- ASX: WWI, SMI, BTR, TGM
- Score evidence-pack quality and downstream rubric adherence.

3. Switch default to v2 only after acceptance criteria are met on benchmark set.

4. Remove legacy summary-table injection path.

## 10) Immediate Next Actions
1. Implement P0 changes in code.
2. Run benchmark on WWI/SMI/BTR/TGM with full artifact capture.
3. Publish delta report: before vs after evidence quality metrics and Stage 1 rubric conformance.

---

This document is intentionally strict. The current failure mode is an evidence-engineering problem, not a model-selection problem. Fixing Stage 1 evidence quality and prompt contract discipline is the highest-leverage path to recover output quality.
