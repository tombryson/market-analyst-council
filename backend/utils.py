"""Shared utilities for FastAPI route handlers."""

import logging
from fastapi import HTTPException
from .config import SYSTEM_ENABLED, SYSTEM_SHUTDOWN_REASON

logger = logging.getLogger(__name__)


def _ensure_system_enabled() -> None:
    """Block runtime execution while global shutdown is active."""
    if SYSTEM_ENABLED:
        return
    raise HTTPException(
        status_code=503,
        detail=(
            f"System disabled: {SYSTEM_SHUTDOWN_REASON or 'maintenance mode active'}"
        ),
    )
