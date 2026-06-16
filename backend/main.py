"""FastAPI backend for LLM Council."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import logging

from .logging_config import configure_logging
from .middleware import auth_middleware
from .jobs.state import ANALYSIS_JOBS, ANALYSIS_JOBS_LOCK, PROJECT_ROOT
from .jobs.executor import _load_analysis_jobs_from_disk
from .routers import misc, memos, company_types as company_types_router
from .routers import conversations as conversations_router
from .routers import scenario_router_routes
from .routers import runs as runs_router
from .routers import analysis_jobs as analysis_jobs_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Configure logging and load persisted analysis jobs at startup."""
    configure_logging()
    loaded = _load_analysis_jobs_from_disk()
    async with ANALYSIS_JOBS_LOCK:
        ANALYSIS_JOBS.clear()
        ANALYSIS_JOBS.update(loaded)
    yield


app = FastAPI(title="LLM Council API", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.middleware("http")(auth_middleware)

app.include_router(misc.router)
app.include_router(memos.router)
app.include_router(company_types_router.router)
app.include_router(conversations_router.router)
app.include_router(scenario_router_routes.router)
app.include_router(runs_router.router)
app.include_router(analysis_jobs_router.router)

# ── Static / SPA fallback ────────────────────────────────────────────────────

_STATIC_DIR = PROJECT_ROOT / "frontend" / "dist"
if _STATIC_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=str(_STATIC_DIR / "assets")), name="assets")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa_fallback(full_path: str):
        index = _STATIC_DIR / "index.html"
        if index.is_file():
            return FileResponse(str(index))
        raise HTTPException(status_code=404, detail="Frontend not built")
