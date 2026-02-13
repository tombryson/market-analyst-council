# LLM Council Research Upgrade: Sprint Checklist

This checklist turns the migration plan into execution-ready tasks with rough effort estimates.

Current status:
- Sprint 0 mostly implemented (provider abstraction, flags, stream routing behind toggle).
- Sprint 1 partially implemented (Perplexity responses integration and normalized evidence extraction).
- Emulated Perplexity deep-research Stage 1 mode implemented with per-model runs.

Assumptions:
- 1 engineer focused on backend + light frontend support.
- Estimates are working time, not calendar time.
- We prioritize "quality gain quickly" over perfect architecture purity.

## Sprint 0: Foundations and Flags (2-3 days)

Goal:
- Introduce research-provider abstraction without breaking existing behavior.

Tasks:
1. Add config flags and provider env vars (2-3h)
2. Scaffold `backend/research/` package and interfaces (3-5h)
3. Add `ResearchService` with provider selection + fallback path (4-6h)
4. Wire stream endpoint behind feature flag (4-6h)
5. Add minimal event payload contract for evidence metadata (2-3h)

Acceptance criteria:
- App still runs with current behavior when feature flag is disabled.
- With flag enabled, retrieval flows through research service.
- Errors in provider path do not block council execution.

Risks:
- Misconfigured env keys causing silent fallback confusion.

## Sprint 1: Perplexity Retrieval MVP (3-5 days)

Goal:
- Replace shallow search with Perplexity-backed deep retrieval.

Tasks:
1. Implement Perplexity provider request/response parser (6-10h)
2. Normalize to evidence pack schema (4-6h)
3. Add source dedupe and max-source clipping (3-4h)
4. Improve enhanced prompt formatting for evidence packs (3-5h)
5. Add test script for provider smoke checks (2-3h)

Acceptance criteria:
- Most company queries work without manual PDF upload.
- Search payload includes sources + summary + provider metadata.
- Council outputs show improved breadth of referenced material.

Risks:
- API response shape variability across models/tools.
- Latency and cost spikes with deep mode.

## Sprint 2: Evidence Quality Controls (3-4 days)

Goal:
- Improve trustworthiness and relevance of gathered evidence.

Tasks:
1. Source scoring (recency + domain class + document type) (6-8h)
2. Required-doc coverage checks (filings/presentation/quarterly) (4-6h)
3. Missing-data detection in evidence pack (3-4h)
4. URL canonicalization and duplicate collapse (3-4h)
5. Add quality diagnostics to SSE payload (2-3h)

Acceptance criteria:
- Evidence panel clearly shows why sources were selected.
- Critical doc classes are present or explicitly flagged missing.

Risks:
- Over-filtering and dropping useful niche sources.

## Sprint 3: Frontend Research Transparency (2-3 days)

Goal:
- Make evidence visible and actionable in the UI.

Tasks:
1. Add evidence panel component (4-6h)
2. Render source list with type/date/provider labels (3-5h)
3. Show missing-data and confidence hints (2-3h)
4. Add controls for depth and max sources (3-4h)
5. Improve streaming state UX for evidence collection (2-3h)

Acceptance criteria:
- User can inspect what the council used before stage outputs.
- Search depth and source limits are user-configurable.

Risks:
- UI clutter reducing readability.

## Sprint 4: Financial Analysis Upgrade (4-6 days)

Goal:
- Improve company analysis depth and consistency.

Tasks:
1. Add company-analysis evidence checklist logic (4-6h)
2. Add structured validation for template-required fields (4-6h)
3. Add dissent/conflict section in final synthesis (3-4h)
4. Add valuation-input extraction table in response metadata (4-6h)
5. Build benchmark query suite and compare old/new outputs (4-8h)

Acceptance criteria:
- Better consistency in financial outputs.
- Clear statement of unresolved disagreements and evidence gaps.

Risks:
- Hallucinated numeric fields if evidence is thin.

## Cross-Sprint Operational Tasks

1. Logging and observability
- provider used
- elapsed times by phase
- source counts
- fallback occurrences

Estimate: 4-6h total

2. Cost controls
- per-request source caps
- depth defaults
- optional "fast mode"

Estimate: 3-5h total

3. Regression checks
- maintain current council behavior if provider path fails

Estimate: 2-4h total

## Suggested Delivery Order

1. Sprint 0
2. Sprint 1
3. Sprint 3 (UI visibility early)
4. Sprint 2 (tighten quality filters once baseline works)
5. Sprint 4

## Initial "Do Now" Checklist (This Week)

1. Implement Sprint 0 fully.
2. Implement Sprint 1 MVP with one Perplexity model profile.
3. Run 10 side-by-side query comparisons (old vs new retrieval).
4. Ship behind feature flag and gather qualitative feedback.
