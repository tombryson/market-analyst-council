# LLM Council: Research Upgrade Plan

## Status Update (Current Implementation)

- Implemented: research-provider abstraction (`backend/research/`), Perplexity provider, and feature-flag routing.
- Implemented: emulated Perplexity deep-research council mode:
  - one Perplexity deep-research run per configured model in Stage 1
  - existing Stage 2/3 ranking and synthesis retained
  - backend + frontend council mode toggle path wired (`local` vs `perplexity_emulated`)
- Next major gaps: stronger source quality scoring, benchmark suite, and UI/source diagnostics hardening.

## 1) Objective

Upgrade the current app from "manual-doc + shallow search council" to a "deep-research council" that automatically gathers high-quality evidence and then runs multi-model deliberation.

Core strategy:
- Keep the existing 3-stage council orchestration.
- Replace/extend retrieval with Perplexity API-powered research.
- Preserve manual PDF upload as an optional override, not the primary path.

## 2) Current State (What We Have Now)

### 2.1 User Experience
- User asks a question.
- Optional ticker input and optional PDF uploads.
- Optional lightweight internet search.
- Council runs:
  - Stage 1: model responses
  - Stage 2: peer ranking
  - Stage 3: chairman synthesis

### 2.2 Technical Flow
- Backend:
  - FastAPI SSE endpoint: `/api/conversations/{id}/message/stream`
  - Search helper functions in `backend/search.py`
  - PDF extract/summarize in `backend/pdf_processor.py`
  - Orchestration in `backend/council.py`
- Frontend:
  - Receives SSE events (`search_start`, `search_complete`, `stage*_complete`, etc.)
  - Renders stage-by-stage panels.

### 2.3 Current Constraints
- Research depth is limited by current query strategy and sources.
- Manual document upload is often required for better answers.
- Evidence pipeline is not strong enough yet for "deep research" quality.
- Council quality is bounded by input quality (garbage in, garbage out).

## 3) Target State (What We Want It To Become)

### 3.1 Product Vision

An automated deep-research council where:
- Research is auto-gathered from the web and key filings/reports.
- Evidence is normalized into a common "evidence pack."
- Multiple models analyze the same evidence pack.
- Council debates and synthesizes with citations and explicit disagreements.

### 3.2 Desired User Experience
- User asks a company question (ticker optional).
- System performs deep evidence gathering automatically.
- UI shows:
  - evidence collection progress,
  - source list + confidence/quality signals,
  - normal stage 1/2/3 council output.
- User can still attach PDFs to force inclusion of critical docs.

### 3.3 Key Quality Bar
- Better than current manual process.
- Closer to "deep research" depth while retaining council transparency.
- Not focused on perfect reproducibility, but robust and inspectable.

## 4) Architecture Direction

## 4.1 New Component: Research Provider Layer

Add a provider abstraction:
- `ResearchProvider` interface
  - `research_query(...)`
  - `fetch_url(...)`
  - `extract_key_documents(...)`
  - `build_evidence_pack(...)`

Implementations:
- `PerplexityResearchProvider` (primary)
- `TavilyResearchProvider` (fallback / legacy)

This avoids hard-coupling `backend/main.py` to one vendor.

## 4.2 Evidence Pack Contract

Standard structure passed into council prompts:
- `question`
- `company/ticker metadata`
- `sources[]` with:
  - URL
  - title
  - publication date (if known)
  - snippet/extracted content
  - source type (filing, presentation, news, transcript, etc.)
  - evidence score / trust hint
- `key_facts[]` synthesized from sources
- `missing_data[]` list

Council stages consume this normalized pack, not ad-hoc search text.

## 4.3 Keep Existing Council Loop

Do not replace:
- Stage 1 parallel responses
- Stage 2 peer review/ranking
- Stage 3 synthesis

Only improve the inputs + structured output quality.

## 5) Implementation Plan (Phased)

## Phase 0: Design + Feature Flag
- Add config flags:
  - `RESEARCH_PROVIDER=perplexity|tavily`
  - `RESEARCH_DEPTH=basic|deep`
  - `MAX_SOURCES`
- Keep current behavior as fallback path.

Deliverable:
- No breaking changes.

## Phase 1: Perplexity Retrieval MVP
- Add `backend/research/` module:
  - `providers/base.py`
  - `providers/perplexity.py`
  - `service.py`
  - `schemas.py`
- Replace current `perform_search(...)` call path with `ResearchService`.
- Output normalized evidence pack and include it in prompt context.

Deliverable:
- Auto evidence gathering works without manual PDFs for most queries.

## Phase 2: Evidence Quality Controls
- Add source filtering and scoring:
  - recency scoring
  - source quality weighting
  - dedupe/canonical URL logic
- Add "must include" source classes for company analysis:
  - official filings
  - latest investor presentation
  - latest quarterly/annual update

Deliverable:
- Better evidence relevance and less noise.

## Phase 3: UI Upgrade for Research Transparency
- Show evidence panel before Stage 1:
  - sources found
  - source types
  - top facts extracted
  - missing info
- Add controls:
  - research depth
  - max sources
  - optional domain/focus constraints

Deliverable:
- Users can trust and inspect what the council is using.

## Phase 4: Financial Deep-Research Enhancements
- Add company-analysis mode:
  - required doc checklist
  - milestone extraction
  - valuation input table
- Improve template auto-selection and output validation.
- Emit explicit "confidence + unresolved disagreements" section in Stage 3.

Deliverable:
- More thorough, decision-grade company analysis workflow.

## 6) Concrete Code Change Map

Backend:
- `backend/main.py`
  - replace direct search calls with `ResearchService`.
  - add SSE events for evidence pack progress.
- `backend/search.py`
  - keep as legacy fallback.
- new `backend/research/*`
  - provider abstraction + Perplexity integration + pack builder.
- `backend/council.py`
  - prompt context format switched to evidence-pack format.
- `backend/config.py`
  - add provider/depth/source config.

Frontend:
- `frontend/src/api.js`
  - handle new SSE event types for evidence.
- `frontend/src/App.jsx`
  - store evidence metadata in assistant message state.
- `frontend/src/components/ChatInterface.jsx`
  - render evidence section above Stage 1.

## 7) Risks and Trade-offs

- Vendor dependence:
  - Better retrieval quality, less custom search engineering.
  - But behavior can change as provider evolves.
- Cost/latency:
  - Deep research adds time and spend.
  - Need tiered depth settings.
- Citation quality:
  - Must enforce source hygiene and dedupe to avoid noisy outputs.

## 8) Success Criteria

Primary:
- Fewer manual PDF uploads needed.
- Higher answer quality on company analysis queries.
- Better evidence breadth (filings + presentations + market context).

Operational:
- Stable completion rate for end-to-end stream.
- Acceptable latency at chosen depth profile.
- Clear source visibility in UI.

## 9) Immediate Next Tasks

1. Create `backend/research/` scaffolding and provider interface.
2. Add Perplexity provider config/env variables.
3. Route existing stream endpoint through new `ResearchService` behind a feature flag.
4. Emit and render evidence SSE events in frontend.
5. Run side-by-side evaluation: current search vs Perplexity retrieval on a fixed query set.
