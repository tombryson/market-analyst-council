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

Create a `.env` file in the project root:

```bash
OPENROUTER_API_KEY=sk-or-v1-...
TAVILY_API_KEY=tvly-...

# Research provider routing
ENABLE_RESEARCH_SERVICE=false
PROGRESS_LOGGING=true
RESEARCH_PROVIDER=tavily
RESEARCH_DEPTH=basic
MAX_SOURCES=15
ENABLE_MARKET_FACTS_PREPASS=true
MARKET_FACTS_TIMEOUT_SECONDS=12
COUNCIL_MODELS=openai/gpt-5.2,google/gemini-3-pro-preview,anthropic/claude-opus-4.6,x-ai/grok-4.1-fast
CHAIRMAN_MODEL=google/gemini-3-pro-preview

# Council execution mode
# local | perplexity_emulated
COUNCIL_EXECUTION_MODE=local

# Perplexity research (used when ENABLE_RESEARCH_SERVICE=true and RESEARCH_PROVIDER=perplexity)
PERPLEXITY_API_KEY=pplx-...
PERPLEXITY_API_URL=https://api.perplexity.ai/v1/responses
PERPLEXITY_MODEL=openai/gpt-5.2
PERPLEXITY_PRESET=deep-research
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
PERPLEXITY_STAGE1_MAX_RETRIES=3
PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS=5
PERPLEXITY_STAGE1_SECOND_PASS_ENABLED=true
PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS=180
PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS=2
PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS=4
PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES=5
PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE=1200
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
# openai/gpt-5.2,google/gemini-3-pro-preview,anthropic/claude-opus-4-6,xai/grok-4-fast-reasoning
# PERPLEXITY_COUNCIL_MODELS=openai/gpt-5.2,google/gemini-3-pro-preview,anthropic/claude-opus-4-6,xai/grok-4-fast-reasoning
# Optional comma-separated domain filters:
# PERPLEXITY_ALLOWED_DOMAINS=asx.com.au,sec.gov
# PERPLEXITY_BLOCKED_DOMAINS=reddit.com
```

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

To enable full emulated Perplexity deep-research council mode by default:
```bash
COUNCIL_EXECUTION_MODE=perplexity_emulated
ENABLE_RESEARCH_SERVICE=true
RESEARCH_PROVIDER=perplexity
RESEARCH_DEPTH=deep
PERPLEXITY_COUNCIL_MODELS=openai/gpt-5.2,google/gemini-3-pro-preview
```

To reduce burst rate-limit risk, switch Stage 1 to staggered model calls:
```bash
PERPLEXITY_STAGE1_EXECUTION_MODE=staggered
PERPLEXITY_STAGE1_STAGGER_SECONDS=2.0
PERPLEXITY_STAGE1_MAX_RETRIES=3
PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS=5
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
PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS=180
PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED=true
PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES=6
PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS=3
PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT=medium
```
Run the backend via `uv run` so project dependencies (including `pymupdf`) are available for PDF decoding. In this mode, Stage 1 answers are generated from a rubric-aligned fact pack built from decoded excerpts, with source IDs preserved for citation.

To anchor price/market-cap/share-structure fields before synthesis:
```bash
ENABLE_MARKET_FACTS_PREPASS=true
MARKET_FACTS_TIMEOUT_SECONDS=12
```
This runs a deterministic ticker prepass and injects those values into Stage 1 and Stage 3 prompts.

You can also toggle council mode per run in the UI using the **Council Mode** selector.
When `perplexity_emulated` mode is selected, internet search is treated as required for Stage 1.
Use **Topic / Analysis Template** in the UI to switch analysis topic. For an MVP scorecard flow, choose `Financial Quality Score (MVP)` or ask naturally (e.g., "rate this company out of 100") and keep auto-detect enabled.
You can now also pass a predefined `company_type` (manual) or rely on auto-detection, and the backend will route to the mapped template before Stage 1 retrieval.
You can also pass an `exchange` (manual) or rely on auto-detection (from ticker/query) so exchange-specific assumptions are injected into Stage 1 research prompts.
The first dedicated sector template is `gold_miner`.
`[Company Name]` placeholders in templates are now automatically substituted from the query context (or ticker fallback).

Predefined company types are exposed via:
```bash
GET /api/company-types
```

Predefined exchanges are exposed via:
```bash
GET /api/exchanges
```

### 3. Configure Models (Optional)

Prefer setting model lists in `.env`:

```bash
COUNCIL_MODELS=openai/gpt-5.2,google/gemini-3-pro-preview,anthropic/claude-opus-4.6,x-ai/grok-4.1-fast
CHAIRMAN_MODEL=google/gemini-3-pro-preview
PERPLEXITY_COUNCIL_MODELS=openai/gpt-5.2,google/gemini-3-pro-preview,anthropic/claude-opus-4-6,xai/grok-4-fast-reasoning
```

If you prefer code defaults, edit `backend/config.py`:

```python
COUNCIL_MODELS = [
    "openai/gpt-5.2",
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
uv run python test_quality_mvp.py \
  --query "Can you rate the quality of ASX:WWI out of 100?" \
  --ticker WWI \
  --max-sources 10 \
  --decode-max-per-model 10 \
  --council-models openai/gpt-5.2,google/gemini-3-pro-preview,anthropic/claude-opus-4-6,xai/grok-4-fast-reasoning \
  --dump-json /tmp/quality_mvp_wwi_10sources.json
```

## Tech Stack

- **Backend:** FastAPI (Python 3.10+), async httpx, OpenRouter API
- **Frontend:** React + Vite, react-markdown for rendering
- **Storage:** JSON files in `data/conversations/`
- **Package Management:** uv for Python, npm for JavaScript
