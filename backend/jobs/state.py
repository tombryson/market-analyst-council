"""
Shared mutable state for the analysis job system.

All routers that read or write ANALYSIS_JOBS must import from here so they
all reference the same dict object (Python module cache guarantees this).
"""

import asyncio
import os
import socket
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Output / artifact directories
# ---------------------------------------------------------------------------

OUTPUTS_DIR = Path(
    os.getenv("ANALYSIS_OUTPUTS_DIR", str(Path(__file__).resolve().parents[2] / "outputs"))
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOBS_OUTPUTS_DIR = Path(os.getenv("ANALYSIS_JOBS_DIR", str(OUTPUTS_DIR / "jobs")))
JOBS_META_DIR = JOBS_OUTPUTS_DIR / "meta"
PORTFOLIO_POSITIONING_OUTPUTS_DIR = Path(
    os.getenv("PORTFOLIO_POSITIONING_OUTPUTS_DIR", str(JOBS_OUTPUTS_DIR / "portfolio_positioning"))
)
PREPASS_OUTPUTS_DIR = Path(
    os.getenv("ANALYSIS_PREPASS_DIR", str(JOBS_OUTPUTS_DIR / "prepass"))
)

# ---------------------------------------------------------------------------
# In-memory job registry (populated at startup from disk, mutated at runtime)
# ---------------------------------------------------------------------------

ANALYSIS_JOB_LOG_TAIL_CHARS = 24000
ANALYSIS_JOBS: Dict[str, Dict[str, Any]] = {}
ANALYSIS_JOBS_LOCK = asyncio.Lock()
SYNTHETIC_RUN_JOB_PREFIX = "run::"

# ---------------------------------------------------------------------------
# Gantt / portfolio run list caches
# ---------------------------------------------------------------------------

GANTT_RUN_LIST_CACHE_TTL_SEC = max(
    1,
    int(os.getenv("GANTT_RUN_LIST_CACHE_TTL_SEC", "15")),
)
_GANTT_RUN_LIST_CACHE: Dict[str, Any] = {
    "expires_at": 0.0,
    "key": None,
    "runs": None,
}
_PORTFOLIO_POSITIONING_RUN_LIST_CACHE: Dict[str, Any] = {
    "expires_at": 0.0,
    "key": None,
    "runs": None,
}

# ---------------------------------------------------------------------------
# Instance identity
# ---------------------------------------------------------------------------

INSTANCE_ID = (
    str(os.getenv("FLY_MACHINE_ID") or "").strip()
    or str(os.getenv("HOSTNAME") or "").strip()
    or socket.gethostname()
)

# ---------------------------------------------------------------------------
# Supplementary document upload limits
# ---------------------------------------------------------------------------

SUPPLEMENTARY_DOC_MAX_CHARS = 12000
SUPPLEMENTARY_DOC_ALLOWED_EXTENSIONS = {".pdf", ".md", ".txt", ".json"}

# ---------------------------------------------------------------------------
# Scenario router event directories
# ---------------------------------------------------------------------------

SCENARIO_ROUTER_EVENTS_DIR = OUTPUTS_DIR / "scenario_router_events"
SCENARIO_ROUTER_DEDUPE_DIR = SCENARIO_ROUTER_EVENTS_DIR / "dedupe"
LEGACY_FRESHNESS_EVENTS_DIR = OUTPUTS_DIR / "freshness_events"
LEGACY_FRESHNESS_DEDUPE_DIR = LEGACY_FRESHNESS_EVENTS_DIR / "dedupe"

# ---------------------------------------------------------------------------
# Analysis job progress markers
# ---------------------------------------------------------------------------

_ANALYSIS_PROGRESS_MARKERS: List[Tuple[str, str, int]] = [
    ("market facts prepass start", "prepass", 4),
    ("market facts prepass done", "prepass", 8),
    ("primary injection prepass start", "prepass", 10),
    ("primary injection bundle ready", "prepass", 16),
    ("stage 1 start", "stage1", 18),
    ("stage 1 done", "stage1", 55),
    ("stage 2 start", "stage2", 60),
    ("stage 2 done", "stage2", 72),
    ("stage 2.5 revision pass start", "stage2_5", 76),
    ("stage 2.5 revision pass done", "stage2_5", 84),
    ("stage 3 start", "stage3", 88),
    ("stage 3 primary done", "stage3", 95),
    ("stage 4 start", "stage4", 96),
    ("stage 4 done", "stage4", 98),
    ("stage 3 secondary start", "stage3_secondary", 96),
    ("stage 3 secondary done", "stage3_secondary", 98),
    ("run complete", "complete", 100),
    ("mvp quality test complete", "complete", 100),
]

_ANALYSIS_STAGE_ORDER: Dict[str, int] = {
    "queued": 0,
    "initializing": 1,
    "prepass": 2,
    "stage1": 3,
    "stage2": 4,
    "stage2_5": 5,
    "stage3": 6,
    "stage4": 7,
    "stage3_secondary": 8,
    "complete": 9,
    "failed": 10,
}

_ANALYSIS_STAGE_RANGES: Dict[str, Tuple[int, int]] = {
    "prepass": (4, 16),
    "stage1": (18, 55),
    "stage2": (60, 72),
    "stage2_5": (76, 84),
    "stage3": (88, 95),
    "stage4": (96, 98),
    "stage3_secondary": (96, 98),
}
