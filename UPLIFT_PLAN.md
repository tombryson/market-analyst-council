# LLM Council — Uplift Plan

Generated from full code audit: June 2026.

This plan addresses the most pressing issues in order of risk and prerequisite dependency.
Each phase can be worked as a separate branch and merged independently.

---

## Summary of issues (from audit)

| Area | Grade | Primary problem |
|---|---|---|
| Module size | F | council.py 435 KB, main.py 222 KB, investment_synthesis.py 228 KB |
| Repo hygiene | D | tmp_ scripts, .DS_Store, dist/, .env.archive/ at risk |
| Dead code | D | root main.py stub, legacy storage function, temp replay scripts |
| Config management | C+ | 100+ flat globals, no subsystem namespacing |
| Logging | C+ | print() throughout — no levels, no correlation IDs |
| Type safety | C | Dict[str,Any] overused, no Pydantic in storage layer |
| Test organisation | C- | Two test hierarchies, 268 KB test files, no fixtures/ dir |
| API design | B | Deprecated on_event, aliased route pairs, otherwise clean |
| Frontend | B+ | Lean and correct for its scope |

---

## Phase 0 — Repo hygiene (est. 2 hrs, zero code risk)

These are pure deletions and gitignore additions. No logic changes.

### 0.1 Delete committed artefacts that must never live in git

```
# Temp debug scripts
rm tmp_stage3_only_from_artifact.py
rm tmp_stage3_only_from_artifact_gpt52_max.py
rm tmp_stage3_replay_jsonify.py

# Root stub that does nothing
rm main.py          # prints "Hello from llm-council!" — not the entry point

# Built frontend output
rm -rf frontend/dist/
```

### 0.2 Audit .env.archive/ for live secrets

The directory `.env.archive/2026-02-15/` contains 16 named experiment env files.
If any contain real API keys (OPENROUTER_API_KEY, TAVILY_API_KEY, etc.) they must be:

1. Rotated immediately
2. Removed from git history via `git filter-repo --path .env.archive --invert-paths`

If they contain only placeholder values, simply delete the directory and commit.

### 0.3 Expand .gitignore

Add the following lines:

```gitignore
# macOS metadata
**/.DS_Store

# Built frontend
frontend/dist/
frontend/.vite/

# Root-level scratch scripts (never commit tmp_ files)
tmp_*.py

# Outputs and job artefacts
outputs/
data/
```

Remove `.DS_Store` from the repo index:

```bash
git rm --cached .DS_Store backend/.DS_Store backend/.venv/.DS_Store
```

### 0.4 Move portfolio_positioning_memo.py

`portfolio_positioning_memo.py` at root is 198 KB, has execute permissions, and is a
research script that grew into a de-facto analysis runner. Move it to `scripts/` so the
root directory is navigable:

```bash
mkdir -p scripts
git mv portfolio_positioning_memo.py scripts/portfolio_positioning_memo.py
```

---

## Phase 1 — Immediate bugs and deprecated APIs (est. 3 hrs)

These are isolated, low-risk fixes with no architectural impact.

### 1.1 Fix openrouter.py — parameter forwarding bug

In `query_models_parallel`, the inner closure ignores `timeout`, `max_tokens`, and
`reasoning_effort`. Any caller that sets these values for parallel queries gets silent
defaults instead:

```python
# Current — drops timeout, max_tokens, reasoning_effort
async def _run_model(model: str) -> tuple[str, Optional[Dict[str, Any]]]:
    return model, await query_model(model, messages)

# Fix — add per-call overrides as optional kwargs to query_models_parallel
async def query_models_parallel(
    models: List[str],
    messages: List[Dict[str, str]],
    timeout: float = 120.0,
    max_tokens: Optional[int] = None,
    reasoning_effort: str = "",
    on_model_complete: Optional[Callable[...]] = None,
) -> Dict[str, Optional[Dict[str, Any]]]:

    async def _run_model(model: str) -> tuple[str, Optional[Dict[str, Any]]]:
        return model, await query_model(
            model, messages,
            timeout=timeout,
            max_tokens=max_tokens,
            reasoning_effort=reasoning_effort,
        )
```

### 1.2 Fix deprecated datetime.utcnow() in storage.py

`datetime.utcnow()` is deprecated since Python 3.12 and will be removed.

```python
# Replace in storage.py
from datetime import datetime, timezone
# Old
"created_at": datetime.utcnow().isoformat()
# New
"created_at": datetime.now(timezone.utc).isoformat()
```

### 1.3 Replace @app.on_event("startup") with lifespan

`@app.on_event` is deprecated in FastAPI 0.95+. Convert to the lifespan context manager:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    await _hydrate_analysis_jobs_from_disk()
    yield

app = FastAPI(title="LLM Council API", lifespan=lifespan)
```

### 1.4 Remove legacy storage.add_assistant_message

`add_assistant_message` (lines 155-181 in storage.py) is a simplified version of
`add_assistant_message_with_metadata` that silently drops search results, evidence pack,
and loading state. It should either be removed or replaced with a call to the full version.
Search the codebase for callers before deleting:

```bash
grep -rn "add_assistant_message\b" backend/ --include="*.py"
```

If no callers remain after checking, delete the function. If callers exist, migrate them
to `add_assistant_message_with_metadata`.

---

## Phase 2 — Structured logging (est. 4 hrs)

Currently all error and progress output uses `print()`. This produces no log levels,
no timestamps, and no correlation IDs in production.

### 2.1 Create backend/logging_config.py

```python
"""Shared logging configuration for the LLM Council backend."""

import logging
import os

def configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        level=getattr(logging, level, logging.INFO),
    )
```

Call `configure_logging()` once at startup in main.py.

### 2.2 Replace print() in each module

Mechanical change — each module gets its own logger:

```python
import logging
logger = logging.getLogger(__name__)

# Replace print(f"Error querying model {model}: ...")
# With:
logger.error("Error querying model %s: HTTP %s body=%s", model, status, body)
logger.exception("Unexpected error querying model %s", model)
```

Prioritise `openrouter.py`, `council.py`, and `main.py` first — they have the highest
error surface area. The `PROGRESS_LOGGING` flag in config should gate `logger.debug()`
calls rather than `print()`.

### 2.3 Add request correlation ID middleware

A correlation ID in every log line makes production debugging tractable:

```python
import uuid
from contextvars import ContextVar

_request_id: ContextVar[str] = ContextVar("request_id", default="")

@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    rid = request.headers.get("x-request-id") or str(uuid.uuid4())[:8]
    _request_id.set(rid)
    response = await call_next(request)
    response.headers["x-request-id"] = rid
    return response
```

Extend the log formatter to include `%(request_id)s` via a custom filter.

---

## Phase 3 — Config refactoring (est. 4 hrs)

`config.py` is 790 lines of flat global variables. The problem is not the length — it's
that 8 distinct subsystems share a single flat namespace, making it impossible to scan
for what a given subsystem needs.

### 3.1 Group into dataclasses by subsystem

Introduce a `backend/settings/` package. Each file owns one subsystem's settings and
exposes a singleton instance:

```
backend/settings/
    __init__.py          # re-exports all settings objects
    base.py              # OPENROUTER_API_KEY, DATA_DIR, system flags
    council.py           # COUNCIL_MODELS, CHAIRMAN_*, STAGE2_*, STAGE3_*
    perplexity.py        # All PERPLEXITY_* vars
    asx.py               # ASX_DETERMINISTIC_* vars
    llamaparse.py        # LLAMAPARSE_*, LITEPARSE_* vars
    scenario_router.py   # SCENARIO_ROUTER_* vars
    research.py          # TAVILY_*, RESEARCH_*, XAI_* vars
```

Each file uses `pydantic-settings` (already available via FastAPI's dependency tree) or
plain dataclasses with `_get_bool`/`_get_int` helpers moved to `settings/helpers.py`.

```python
# backend/settings/council.py
from dataclasses import dataclass, field
from .helpers import get_bool, get_int, get_float, get_csv

@dataclass
class CouncilSettings:
    models: list[str] = field(default_factory=lambda: get_csv("COUNCIL_MODELS") or [...])
    chairman_model: str = ...
    chairman_timeout_seconds: float = field(default_factory=lambda: get_float("CHAIRMAN_TIMEOUT_SECONDS", 300.0))
    # etc.

council_settings = CouncilSettings()
```

Existing `from .config import CHAIRMAN_MODEL` imports become
`from .settings import council_settings` — a mechanical find-and-replace across files.

### 3.2 Deprecate the two hardcoded non-env constants

```python
# These in config.py are not env-driven and not documented:
ENABLE_SEARCH_BY_DEFAULT = True   # hardcoded True — should be env-driven or removed
MAX_SEARCH_RESULTS = 5            # shadowed by MAX_SOURCES in the same file
```

---

## Phase 4 — Storage type safety (est. 3 hrs)

Storage reads and writes raw `Dict[str, Any]` everywhere. A malformed write is
undetected until the frontend tries to render it.

### 4.1 Add Pydantic models to storage.py

```python
from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime

class LoadingState(BaseModel):
    search: bool = False
    evidence: bool = False
    stage1: bool = False
    stage2: bool = False
    stage3: bool = False
    stage1Progress: int = 0
    stage1Completed: int = 0
    stage1Total: int = 0
    stage1Model: str = ""
    stage1Message: str = ""

class UserMessage(BaseModel):
    role: str = "user"
    content: str
    enable_search: Optional[bool] = None
    attachments: Optional[list] = None
    template_id: Optional[str] = None
    company_name: Optional[str] = None
    exchange: Optional[str] = None

class AssistantMessage(BaseModel):
    role: str = "assistant"
    status: str = "complete"
    stage1: Optional[Any] = None
    stage2: Optional[Any] = None
    stage3: Optional[Any] = None
    loading: LoadingState = LoadingState()
    metadata: dict = {}

class Conversation(BaseModel):
    id: str
    created_at: str
    title: str = "New Conversation"
    messages: list = []
```

Storage functions then validate on write:
`AssistantMessage(**message_dict).model_dump()` catches missing required fields at
insertion time rather than at render time.

---

## Phase 5 — Test organisation (est. 2 hrs)

### 5.1 Consolidate test files under tests/

Current state: test files are split between the project root (~20 files) and
`backend/tests/` (~15 files), with no clear rule for which goes where.

Target layout:

```
tests/
    conftest.py
    unit/
        test_storage.py
        test_openrouter.py
        test_reasoning.py
        test_prepass_utils.py
        test_timeline_normalization.py
        ...
    integration/
        test_stage2_reconciliation.py
        test_template_smoke.py
        test_supplementary_registry.py
        ...
    scenario_router/
        (move backend/tests/test_scenario_router_*.py here)
    fixtures/
        ausgold_stage3.json        # extracted from test_perplexity_pdf_dump.py
        javelin_stage3.json        # extracted from root data files
        ...
```

Add `pyproject.toml` (or `pytest.ini`) so `pytest` discovers `tests/` automatically:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
```

### 5.2 Extract fixture data from large test files

`test_perplexity_pdf_dump.py` (268 KB) and `test_quality_mvp.py` (114 KB) are large
because they contain large inline JSON fixtures. Extract these to `tests/fixtures/*.json`
and load them with:

```python
import json, pathlib
FIXTURE_DIR = pathlib.Path(__file__).parent.parent / "fixtures"

def load_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text())
```

This reduces the test files to logic-only and makes fixtures reusable across test files.

---

## Phase 6 — Decompose main.py (est. 1 day)

`main.py` at 5,589 lines / 222 KB mixes route handlers with job management, file path
resolution, Gantt logic, scenario router wiring, and progress infrastructure. Use FastAPI
`APIRouter` to split by domain.

### Target structure

```
backend/
    main.py               # ~150 lines: app creation, middleware, lifespan, router inclusion
    middleware.py         # auth_middleware, correlation_id_middleware
    routers/
        __init__.py
        conversations.py  # /api/conversations/**
        analysis_jobs.py  # /api/analysis-jobs/**
        gantt_runs.py     # /api/gantt-runs/**
        portfolio.py      # /api/portfolio-positioning-runs/**
        scenario_router.py # /api/scenario-router/** + /api/announcement-router/**
        memos.py          # /api/memos/**
        company_types.py  # /api/company-types/**
        misc.py           # /, /api/templates, /api/exchanges
    jobs/
        executor.py       # _build_analysis_job_command, subprocess launch logic
        manifest.py       # _persist_job_record, _load_analysis_jobs_from_disk, ANALYSIS_JOBS dict
        progress.py       # _scale_stage_progress, _ANALYSIS_PROGRESS_MARKERS, _ANALYSIS_STAGE_ORDER
        paths.py          # _resolve_run_artifact_path, _resolve_portfolio_positioning_artifact_path
```

### Migration approach

1. Create `backend/routers/` package.
2. For each domain, create a router file with `router = APIRouter(prefix="/api/...")`.
3. Move the relevant handler functions and their private helpers into that file.
4. In `main.py`, replace the inline definitions with `app.include_router(conversations.router)`.
5. Run tests after each router extraction to catch any missed imports.

The private helpers in main.py (146 functions total) should each follow their closest
route handler into the relevant router or jobs/ sub-module. Functions used across multiple
routers go into the `jobs/` shared layer.

---

## Phase 7 — Decompose council.py (est. 2–3 days)

`council.py` at 11,411 lines / 435 KB is the most critical decomposition. The file
contains at least 7 distinct concerns that share no state except function calls.

### Identified sub-modules by line range

| Lines | Concern | Target module |
|---|---|---|
| 1–130 | Imports, module-level constants | (split across sub-modules) |
| 131–610 | Stage 1 attempt/retry profiles, compliance checks | `backend/council/stage1_attempt.py` |
| 611–904 | xAI supplementary macro news lane | `backend/council/stage1_xai_lane.py` |
| 905–1636 | Perplexity API client, model probing, streaming | `backend/council/perplexity_client.py` |
| 1637–2185 | Multi-wave retrieval orchestration, gap analysis | `backend/council/stage1_multi_wave.py` |
| 2186–2491 | Fact digest v2 builder, sentence scoring | `backend/council/fact_digest.py` |
| 2492–3165 | ASX deterministic source ingestion, HTML parsing | `backend/council/asx_ingestion.py` |
| 3166–8434 | Stage 1 collect (OpenRouter + Perplexity orchestration) | `backend/council/stage1.py` |
| 8435–10102 | Stage 1 public entrypoints | `backend/council/stage1.py` (public API) |
| 10103–11080 | Stage 2: rankings, revision deltas, reconciliation | `backend/council/stage2.py` |
| 11081–11284 | Stage 3: chairman synthesis | `backend/council/stage3.py` |
| 11285–11411 | Utilities, title generation, run_full_council | `backend/council/__init__.py` |

### Target structure

```
backend/council/
    __init__.py           # run_full_council, generate_conversation_title, public re-exports
    stage1.py             # stage1_collect_responses, stage1_collect_perplexity_research_responses
    stage1_attempt.py     # attempt profiles, retry logic, compliance checks
    stage1_xai_lane.py    # xAI supplementary macro news
    stage1_multi_wave.py  # planner, gap queries, wave merging
    fact_digest.py        # _build_stage1_fact_digest_v2 and sentence scoring helpers
    perplexity_client.py  # _query_model_via_perplexity, model probing, streaming
    asx_ingestion.py      # ASX HTML parser, deterministic source collector
    stage2.py             # stage2_collect_rankings, revision deltas, reconciliation
    stage3.py             # stage3_synthesize_final
    utils.py              # parse_ranking_from_text, calculate_aggregate_rankings, helpers
```

All public symbols currently imported from `council` in `main.py` remain re-exported
from `backend/council/__init__.py`, so no callers outside the package need to change.

### Migration approach

Work bottom-up: extract the most self-contained modules first (fact_digest,
asx_ingestion, perplexity_client) since they have no cross-dependencies within
council.py. Extract stage2 and stage3 next. Extract stage1 last since it calls
everything else.

After each extraction: run `python -m py_compile backend/council/__init__.py` and the
existing test suite to verify no circular imports or missing references.

---

## Phase 8 — Decompose investment_synthesis.py (est. 1 day)

`investment_synthesis.py` at 5,651 lines / 228 KB contains:

- Stage 3 prompt builders (`_build_chairman_xml_prompt`, `_build_jsonifier_prompt`)
- JSON extraction and normalisation (`_parse_json_from_text`, `_normalize_rating_value`)
- Investment verdict extraction (`_extract_investment_verdict_from_text`)
- Price target extraction and scenario driver logic
- Development timeline extraction and normalisation
- Thesis map and headwinds/tailwinds extraction
- Source fact guardrails and energy guardrail logic

### Target structure

```
backend/synthesis/
    __init__.py           # re-exports used by council/stage3.py
    prompts.py            # _build_chairman_xml_prompt, _build_jsonifier_prompt
    json_extract.py       # _parse_json_from_text, _extract_synthesis_block
    verdict.py            # _extract_investment_verdict_from_text, rating normalisation
    price_targets.py      # price target extraction, scenario drivers
    timeline.py           # development timeline extraction and normalisation
    thesis.py             # thesis map, headwinds/tailwinds extraction
    guardrails.py         # source fact guardrails, energy guardrail logic
```

---

## Phase 9 — CI and quality gates (est. 3 hrs)

The llm-council repo has no continuous integration. Add a GitHub Actions workflow:

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e ".[dev]"
      - run: python -m pytest tests/ -x -q
      - run: python -m py_compile backend/**/*.py
```

Add a `pyproject.toml` with dev dependencies:

```toml
[project.optional-dependencies]
dev = ["pytest", "pytest-asyncio", "httpx", "ruff"]

[tool.ruff]
line-length = 100
select = ["E", "F", "I"]   # pycodestyle errors, pyflakes, isort
```

Run `ruff check .` as a lint step in CI to catch unused imports and style drift.

---

## Execution order and risk

| Phase | Risk | Effort | Prerequisite |
|---|---|---|---|
| 0 — Hygiene | None | 2 hrs | — |
| 1 — Bug fixes | Low | 3 hrs | — |
| 2 — Logging | Low | 4 hrs | 1 |
| 3 — Config | Low-medium | 4 hrs | — |
| 4 — Storage types | Low | 3 hrs | — |
| 5 — Test layout | Low | 2 hrs | — |
| 9 — CI | Low | 3 hrs | 5 |
| 6 — main.py split | Medium | 1 day | 2, 3 |
| 7 — council.py split | Medium | 2–3 days | 2, 3 |
| 8 — investment_synthesis split | Medium | 1 day | 7 |

Phases 0–5 and 9 can all be executed in any order and carry essentially no regression
risk since they touch only imports, deletions, test paths, and additive type annotations.
Phases 6–8 are refactors of live orchestration code and should each have the test suite
green before merging.

Total estimated effort: **8–10 working days** to complete all phases.
