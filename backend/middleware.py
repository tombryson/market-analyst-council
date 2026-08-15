"""FastAPI middleware for the LLM Council backend."""

import hmac
import logging
import os

from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Authentication configuration (read once at import time)
# ---------------------------------------------------------------------------

_AUTH_DISABLED: bool = os.getenv("AUTH_DISABLED", "").strip().lower() in {"1", "true", "yes"}
_API_TOKEN: str = os.getenv("API_TOKEN", "").strip()

if not _API_TOKEN and not _AUTH_DISABLED:
    raise RuntimeError(
        "API_TOKEN env var is not set and AUTH_DISABLED is not true. "
        "Set API_TOKEN via fly secrets set or set AUTH_DISABLED=true for local dev."
    )

# Routes that bypass authentication.
_OPEN_PATHS: frozenset[str] = frozenset({"/", "/api/health"})

# These are machine-to-machine ingress aliases, not general public API routes.
# Their handler verifies X-Scenario-Router-Secret before reading or processing an
# announcement. Requiring the broad API_TOKEN here as well forced the Gmail
# automation to carry a second, far more privileged credential.
_SCENARIO_ROUTER_INGEST_PATHS: frozenset[str] = frozenset(
    {
        "/api/announcement-router/process-announcement",
        "/api/scenario-router/process-announcement",
        "/api/freshness/process-announcement",
    }
)


def _is_scenario_router_ingest_request(request: Request) -> bool:
    path = request.url.path.rstrip("/") or "/"
    return (
        request.method.upper() == "POST"
        and path in _SCENARIO_ROUTER_INGEST_PATHS
    )


async def auth_middleware(request: Request, call_next):
    """Bearer-token authentication gate for all /api/* routes."""
    path = request.url.path

    # OPTIONS preflight, health endpoints, and the dedicated scenario-router
    # ingress are authenticated by their own route-level checks.
    if (
        request.method == "OPTIONS"
        or path in _OPEN_PATHS
        or _is_scenario_router_ingest_request(request)
        or not path.startswith("/api/")
    ):
        return await call_next(request)

    # Auth disabled (local dev only).
    if _AUTH_DISABLED:
        return await call_next(request)

    # Blank configured token never matches — fail closed.
    if not _API_TOKEN:
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    # Bearer token check.
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        provided = auth_header[len("Bearer "):]
        if hmac.compare_digest(provided, _API_TOKEN):
            return await call_next(request)

    return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
