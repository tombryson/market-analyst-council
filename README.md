# LLM Council

![llmcouncil](header.jpg)

The idea of this repo is that instead of asking a question to your favorite LLM provider (e.g. OpenAI GPT 5.2, Google Gemini 3 Pro Preview, Anthropic Claude Opus 4.6, xAI Grok 4.1 Fast, etc.), you can group them into your "LLM Council". This repo is a simple, local web app that essentially looks like ChatGPT except it uses OpenRouter to send your query to multiple LLMs, it then asks them to review and rank each other's work, and finally a Chairman LLM produces the final response.

In a bit more detail, here is what happens when you submit a query:

1. **Stage 1: First opinions**. The user query is given to all LLMs individually, and the responses are collected. The individual responses are shown in a "tab view", so that the user can inspect them all one by one.
2. **Stage 2: Review**. Each individual LLM is given the responses of the other LLMs. Under the hood, the LLM identities are anonymized so that the LLM can't play favorites when judging their outputs. The LLM is asked to rank them in accuracy and insight.
3. **Stage 3: Final response**. The designated Chairman of the LLM Council takes all of the model's responses and compiles them into a single final answer that is presented to the user.

## Vibe Code Alert

This project was 99% vibe coded as a fun Saturday hack because I wanted to explore and evaluate a number of LLMs side by side in the process of [reading books together with LLMs](https://x.com/karpathy/status/1990577951671509438). It's nice and useful to see multiple responses side by side, and also the cross-opinions of all LLMs on each other's outputs. I'm not going to support it in any way, it's provided here as is for other people's inspiration and I don't intend to improve it. Code is ephemeral now and libraries are over, ask your LLM to change it in whatever way you like.

## Setup

### 1. Install Dependencies

The project uses [uv](https://docs.astral.sh/uv/) for project management.

**Backend:**
```bash
uv sync
```

**Frontend:**
```bash
cd frontend
npm install
cd ..
```

### 2. Configure API Keys

Copy the baseline config and fill secrets:

```bash
cp .env.example .env
```

Then edit `.env` in the project root:

```bash
OPENROUTER_API_KEY=sk-or-v1-...
TAVILY_API_KEY=tvly-...
ENV_PROFILE=

# Research provider routing
ENABLE_RESEARCH_SERVICE=false
PROGRESS_LOGGING=true
SYSTEM_ENABLED=true
SYSTEM_SHUTDOWN_REASON=System temporarily disabled for audit and diagnostics.
SYSTEM_ALLOW_DIAGNOSTICS_WHEN_DISABLED=true
RESEARCH_PROVIDER=tavily
RESEARCH_DEPTH=basic
MAX_SOURCES=15
ENABLE_MARKET_FACTS_PREPASS=true
MARKET_FACTS_TIMEOUT_SECONDS=12
# Options: tavily | perplexity
COUNCIL_MODELS=openai/gpt-5.1,google/gemini-3-pro-preview,anthropic/claude-opus-4.6,x-ai/grok-4.1-fast
CHAIRMAN_MODEL=google/gemini-3-pro-preview

# Council execution mode
# local | perplexity_emulated
COUNCIL_EXECUTION_MODE=local

# Perplexity research (used when ENABLE_RESEARCH_SERVICE=true and RESEARCH_PROVIDER=perplexity)
PERPLEXITY_API_KEY=pplx-...
PERPLEXITY_API_URL=https://api.perplexity.ai/v1/responses
PERPLEXITY_MODEL=openai/gpt-5.1
# single | adaptive | deep_only | advanced_only
PERPLEXITY_PRESET_STRATEGY=adaptive
PERPLEXITY_PRESET=deep-research
PERPLEXITY_PRESET_DEEP=deep-research
PERPLEXITY_PRESET_ADVANCED=advanced-deep-research
PERPLEXITY_STREAM_ENABLED=true
# standard | pro (pro is Sonar-oriented and requires streaming)
PERPLEXITY_SEARCH_MODE=standard
# Retry once with legacy web_search keys if filters payload is rejected
PERPLEXITY_USE_LEGACY_TOOL_FILTER_FALLBACK=true
PERPLEXITY_TIMEOUT_SECONDS=180
PERPLEXITY_MAX_STEPS=10
PERPLEXITY_MAX_OUTPUT_TOKENS=4096
PERPLEXITY_REASONING_EFFORT=low
PERPLEXITY_ENABLE_WEB_SEARCH_TOOL=true
PERPLEXITY_ENABLE_FETCH_URL_TOOL=true
PERPLEXITY_MAX_RESULTS_PER_QUERY=10
PERPLEXITY_MAX_TOKENS_PER_PAGE=1024
PERPLEXITY_STAGE1_EXECUTION_MODE=staggered
PERPLEXITY_STAGE1_STAGGER_SECONDS=2.0
PERPLEXITY_STAGE1_MULTI_WAVE_ENABLED=true
PERPLEXITY_STAGE1_MULTI_WAVE_MAX_WAVES=3
PERPLEXITY_STAGE1_MULTI_WAVE_GAP_QUERY_LIMIT=3
PERPLEXITY_STAGE1_MULTI_WAVE_MIN_NEW_PRIMARY_SOURCES=1
PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED=true
DETERMINISTIC_FINANCE_LANE_ENABLED=true
PERPLEXITY_STAGE1_MAX_RETRIES=3
PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS=5
PERPLEXITY_STAGE1_SECOND_PASS_ENABLED=true
PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS=180
PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS=2
PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS=4
PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES=5
PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE=1200
PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_COMPRESSION_ENABLED=true
PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_TARGET_CHARS=32000
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_PER_SOURCE=8
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_WORDS_PER_SOURCE=500
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS=180
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED=true
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MIN_COUNT=2
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MAX_UNCITED_NUMERIC_LINES=2
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE=0.75
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT=0.60
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT=0.60
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_CATASTROPHIC_SCORE=0.35
# Deterministic ASX lane: scrape latest ASX announcements, resolve direct PDFs,
# decode, then inject as high-priority evidence before Stage 1 analysis.
ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED=true
ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS=10
ASX_DETERMINISTIC_LOOKBACK_YEARS=2
ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY=true
ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL=false
ASX_DETERMINISTIC_MAX_DECODE=10
ASX_DETERMINISTIC_FETCH_TIMEOUT_SECONDS=20
PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED=true
# Optional override
# PERPLEXITY_STAGE1_SHARED_RETRIEVAL_MODEL=google/gemini-3-pro-preview
PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED=true
PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES=6
PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS=3
PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT=medium
ENABLE_SOURCE_DECODING=true
SOURCE_DECODING_MAX_PER_MODEL=10
SOURCE_DECODING_MAX_CHARS=3500
SOURCE_DECODING_TIMEOUT_SECONDS=20.0

# Note: on some Perplexity model IDs, medium/high reasoning effort can return
# a generic 400 invalid_request. Keep this at low unless you have verified
# compatibility for your selected model set.

# Models to run in emulated Perplexity council mode (comma-separated)
# If omitted, falls back to backend-config defaults:
# openai/gpt-5.1,google/gemini-3-pro-preview,sonar-pro
# PERPLEXITY_COUNCIL_MODELS=openai/gpt-5.1,google/gemini-3-pro-preview,sonar-pro
# Optional mixed-provider fanout:
# PERPLEXITY_STAGE1_MIXED_MODE_ENABLED=true
# PERPLEXITY_STAGE1_OPENROUTER_MODELS=openai/gpt-5.2,anthropic/claude-opus-4.6
# Optional comma-separated domain filters:
# PERPLEXITY_ALLOWED_DOMAINS=asx.com.au,sec.gov
# PERPLEXITY_BLOCKED_DOMAINS=reddit.com
# PERPLEXITY_SEARCH_AFTER_DATE_FILTER=2025-01-01
# PERPLEXITY_SEARCH_BEFORE_DATE_FILTER=2026-12-31
```

Environment configuration policy:
- `.env` is the source of truth for runtime/test settings.
- Optional overlays:
  - `.env.dev` (set `ENV_PROFILE=dev` in `.env`)
  - `.env.staging` (set `ENV_PROFILE=staging` in `.env`)
  - `.env.prod` (set `ENV_PROFILE=prod` in `.env`)
- Optional `.env.local` is loaded last for machine-local overrides.
- Core settings are not intended to be overridden by test CLI flags.
- Global shutdown controls:
  - `SYSTEM_ENABLED=false` blocks normal API/runtime council execution
  - `SYSTEM_ALLOW_DIAGNOSTICS_WHEN_DISABLED=true` allows audit scripts only

Profile templates:
- `.env.dev.example`
- `.env.staging.example`
- `.env.prod.example`
- `.env.gpt52_baseline.example`
- `.env.gpt52_medium.example`
- `.env.gpt52_light.example`

Get API keys at:
- [openrouter.ai](https://openrouter.ai/) for council model calls
- [tavily.com](https://tavily.com/) for legacy search mode
- [perplexity.ai](https://www.perplexity.ai/) for Perplexity research mode

To enable Perplexity retrieval:
```bash
ENABLE_RESEARCH_SERVICE=true
RESEARCH_PROVIDER=perplexity
RESEARCH_DEPTH=deep
```

To use both deep presets in Stage 1 retrieval:
```bash
# attempt 1 uses deep-research, retries use advanced-deep-research
PERPLEXITY_PRESET_STRATEGY=adaptive
PERPLEXITY_PRESET_DEEP=deep-research
PERPLEXITY_PRESET_ADVANCED=advanced-deep-research
```
Use `PERPLEXITY_PRESET_STRATEGY=single` to keep fixed behavior via `PERPLEXITY_PRESET`.

To enable streaming retrieval (recommended for long deep runs):
```bash
PERPLEXITY_STREAM_ENABLED=true
```

To use Sonar Pro-style search mode where supported:
```bash
PERPLEXITY_SEARCH_MODE=pro
```
`pro` mode is Sonar-oriented and requires streaming. For non-Sonar models, the provider keeps standard behavior.

To enable full emulated Perplexity deep-research council mode by default:
```bash
COUNCIL_EXECUTION_MODE=perplexity_emulated
ENABLE_RESEARCH_SERVICE=true
RESEARCH_PROVIDER=perplexity
RESEARCH_DEPTH=deep
PERPLEXITY_COUNCIL_MODELS=openai/gpt-5.1,google/gemini-3-pro-preview,sonar-pro
```

To enable mixed-provider Stage 1 fanout (single shared retrieval + dual analysis lanes):
```bash
COUNCIL_EXECUTION_MODE=perplexity_emulated
PERPLEXITY_COUNCIL_MODELS=openai/gpt-5.1,google/gemini-3-pro-preview,anthropic/claude-sonnet-4-5
PERPLEXITY_STAGE1_MIXED_MODE_ENABLED=true
PERPLEXITY_STAGE1_OPENROUTER_MODELS=openai/gpt-5.2,anthropic/claude-opus-4.6,moonshotai/kimi-k2.5
```
- Perplexity pool models are analyzed via Perplexity API.
- OpenRouter pool models are analyzed via OpenRouter API.
- Retrieval/decode still runs once and is reused for all models.

To preflight Perplexity model support before Stage 1 (recommended):
```bash
PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED=true
PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS=30
PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN=true
```
Run a manual preflight check:
```bash
uv run python test_perplexity_model_capabilities.py --timeout 30
```

To reduce burst rate-limit risk, switch Stage 1 to staggered model calls:
```bash
PERPLEXITY_STAGE1_EXECUTION_MODE=staggered
PERPLEXITY_STAGE1_STAGGER_SECONDS=2.0
PERPLEXITY_STAGE1_MAX_RETRIES=3
PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS=5
```

To enable planner-driven multi-wave retrieval and gap-filling:
```bash
PERPLEXITY_STAGE1_MULTI_WAVE_ENABLED=true
PERPLEXITY_STAGE1_MULTI_WAVE_MAX_WAVES=3
PERPLEXITY_STAGE1_MULTI_WAVE_GAP_QUERY_LIMIT=3
PERPLEXITY_STAGE1_MULTI_WAVE_MIN_NEW_PRIMARY_SOURCES=1
```

To enforce Sonar multistep telemetry checks per Stage 1 run:
```bash
PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED=true
```
When a Sonar run is missing required stream/pro-search telemetry, Stage 1 retries (up to max attempts) before accepting the run.

Deterministic finance lane can be toggled with:
```bash
DETERMINISTIC_FINANCE_LANE_ENABLED=true
```

If strict template-retry attempts later fail with transient API errors, Stage 1 now keeps the last successful model result instead of discarding it.

To print detailed progress logs (model start/end, API elapsed time, decode elapsed time):
```bash
PROGRESS_LOGGING=true
```

To enforce two-pass Stage 1 (retrieve/decode first, then model analysis over decoded evidence):
```bash
ENABLE_SOURCE_DECODING=true
SOURCE_DECODING_MAX_PER_MODEL=10
SOURCE_DECODING_MAX_CHARS=3500
SOURCE_DECODING_TIMEOUT_SECONDS=20.0
PERPLEXITY_STAGE1_SECOND_PASS_ENABLED=true
PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES=5
PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE=1200
PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_COMPRESSION_ENABLED=true
PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_TARGET_CHARS=32000
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_PER_SOURCE=8
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_WORDS_PER_SOURCE=500
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS=180
PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS=180
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED=true
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MIN_COUNT=2
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MAX_UNCITED_NUMERIC_LINES=2
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE=0.75
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT=0.60
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT=0.60
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_CATASTROPHIC_SCORE=0.35
PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED=true
PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED=true
PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES=6
PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS=3
PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT=medium
```
Run the backend via `uv run` so project dependencies (including `pymupdf`) are available for PDF decoding. In this mode, Stage 1 retrieves/decodes once (shared mode) and fans the same evidence into each model analysis, with rubric-aligned fact packs and source-ID citation checks.
Decoded-source injection now uses query-aware chunk scoring (instead of first-chars clipping), and Stage 1 second-pass starts from a compact denoised fact bundle before consulting larger supporting excerpts.

To anchor price/market-cap/share-structure fields before synthesis:
```bash
ENABLE_MARKET_FACTS_PREPASS=true
MARKET_FACTS_TIMEOUT_SECONDS=12
```
This runs a deterministic ticker prepass and injects those values into Stage 1 and Stage 3 prompts.

To enable API-assisted company-type detection before template selection:
```bash
ENABLE_COMPANY_TYPE_API_DETECTION=true
COMPANY_TYPE_DETECTION_PROVIDER=tavily
COMPANY_TYPE_DETECTION_TIMEOUT_SECONDS=20
COMPANY_TYPE_DETECTION_MAX_RESULTS=5
COMPANY_TYPE_DETECTION_MIN_CONFIDENCE=0.55
# Optional when provider=perplexity
COMPANY_TYPE_DETECTION_PERPLEXITY_MODEL=sonar-pro
COMPANY_TYPE_DETECTION_MAX_OUTPUT_TOKENS=320
```

You can also toggle council mode per run in the UI using the **Council Mode** selector.
When `perplexity_emulated` mode is selected, internet search is treated as required for Stage 1.
Use **Topic / Analysis Template** in the UI to switch analysis topic. For an MVP scorecard flow, choose `Financial Quality Score (MVP)` or ask naturally (e.g., "rate this company out of 100") and keep auto-detect enabled.
You can now also pass a predefined `company_type` (manual) or rely on auto-detection, and the backend will route to the mapped template before Stage 1 retrieval.
You can also pass an `exchange` (manual) or rely on auto-detection (from ticker/query) so exchange-specific assumptions are injected into Stage 1 research prompts.
The first dedicated sector template is `gold_miner`.
`[Company Name]` placeholders in templates are now automatically substituted from the query context (or ticker fallback).
Templates can now define an optional `verification_schema` block to drive Stage 1 de-noised fact digest sectioning, timeline conflict checks, and conformance markers. This removes gold-specific hardcoding from the verification path and lets each template define what "material evidence" means.

Predefined company types are exposed via:
```bash
GET /api/company-types
```

API-assisted company-type detection endpoint:
```bash
POST /api/company-types/detect
```

Predefined exchanges are exposed via:
```bash
GET /api/exchanges
```

### 3. Configure Models (Optional)

Prefer setting model lists in `.env`:

```bash
COUNCIL_MODELS=openai/gpt-5.1,google/gemini-3-pro-preview,anthropic/claude-opus-4.6,x-ai/grok-4.1-fast
CHAIRMAN_MODEL=google/gemini-3-pro-preview
PERPLEXITY_COUNCIL_MODELS=openai/gpt-5.1,google/gemini-3-pro-preview,sonar-pro
```

If you prefer code defaults, edit `backend/config.py`:

```python
COUNCIL_MODELS = [
    "openai/gpt-5.1",
    "google/gemini-3-pro-preview",
    "anthropic/claude-opus-4.6",
    "x-ai/grok-4.1-fast",
]

CHAIRMAN_MODEL = "google/gemini-3-pro-preview"
```

## Running the Application

**Option 1: Use the start script**
```bash
./start.sh
```

**Option 2: Run manually**

Terminal 1 (Backend):
```bash
uv run python -m backend.main
```

Terminal 2 (Frontend):
```bash
cd frontend
npm run dev
```

Then open http://localhost:5173 in your browser.

## Smoke Test: Emulated Council

Run a one-shot emulated Perplexity council Stage 1 test and print per-model success/failure:

```bash
uv run python test_emulated_council.py --query "Analyze ASX:WWI over 24 months" --ticker WWI
```

Run with explicit company-type routing:

```bash
uv run python test_emulated_council.py \
  --query "Rate the quality of ASX:WWI out of 100" \
  --ticker WWI \
  --company-type gold_miner \
  --exchange asx
```

Optional: dump full JSON output for inspection:

```bash
uv run python test_emulated_council.py --query "Analyze BHP valuation" --ticker BHP --dump-json /tmp/emulated_council.json
```

## Smoke Test: Financial Quality MVP

Configuration note: core run settings come from `.env` (timeouts, models, max sources, reasoning, decode budgets).
Set those there before running tests.

Run a full Stage 1 -> Stage 2 -> Stage 3 test using the `financial_quality_mvp` topic template:

```bash
uv run python test_quality_mvp.py --query "Can you rate the quality of ASX:WWI out of 100?" --ticker WWI
```

Run with explicit company-type routing (template auto-selected from type):

```bash
uv run python test_quality_mvp.py \
  --query "Rate the quality of ASX:WWI out of 100" \
  --ticker WWI \
  --company-type gold_miner \
  --exchange asx
```

Optional: dump full JSON output:

```bash
uv run python test_quality_mvp.py --query "Can you rate the quality of ASX:WWI out of 100?" --ticker WWI --dump-json /tmp/quality_mvp.json
```

For a deeper run with 10-source retrieval + 10-source decode per model:

```bash
# .env:
# MAX_SOURCES=10
# PERPLEXITY_MAX_RESULTS_PER_QUERY=10
# SOURCE_DECODING_MAX_PER_MODEL=10
# PERPLEXITY_COUNCIL_MODELS=openai/gpt-5.1,google/gemini-3-pro-preview,anthropic/claude-opus-4.6
uv run python test_quality_mvp.py --query "Can you rate the quality of ASX:WWI out of 100?" --ticker WWI --dump-json /tmp/quality_mvp_wwi_10sources.json
```

## GPT-5.2 Grid Search (Diagnostics)

When `SYSTEM_ENABLED=false`, use diagnostics mode to audit GPT behavior safely.

Create profile overlays:

```bash
cp .env.gpt52_baseline.example .env.gpt52_baseline
cp .env.gpt52_medium.example .env.gpt52_medium
cp .env.gpt52_light.example .env.gpt52_light
```

Run profile matrix (Stage 1 only, diagnostic mode):

```bash
uv run python test_gpt52_diagnostics.py \
  --profiles gpt52_baseline,gpt52_medium,gpt52_light \
  --ticker WWI \
  --template-id gold_miner \
  --exchange asx \
  --out-dir /tmp/gpt52_diag
```

Summary output:
- `/tmp/gpt52_diag/summary.json`
- Per-profile logs and JSON artifacts in the same folder.

## Tech Stack

- **Backend:** FastAPI (Python 3.10+), async httpx, OpenRouter API
- **Frontend:** React + Vite, react-markdown for rendering
- **Storage:** JSON files in `data/conversations/`
- **Package Management:** uv for Python, npm for JavaScript
