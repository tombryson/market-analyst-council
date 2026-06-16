"""Miscellaneous top-level routes: health, templates, exchanges."""

import logging
from fastapi import APIRouter
from ..config import SYSTEM_ENABLED, SYSTEM_SHUTDOWN_REASON

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "LLM Council API",
        "system_enabled": bool(SYSTEM_ENABLED),
        "shutdown_reason": SYSTEM_SHUTDOWN_REASON if not SYSTEM_ENABLED else "",
    }


@router.get("/api/templates")
async def list_templates():
    """List all available analysis templates."""
    from ..template_loader import list_available_templates
    return list_available_templates()


@router.get("/api/company-types")
async def list_company_types():
    """List predefined company types and mapped templates."""
    from ..template_loader import list_company_types as list_available_company_types
    return list_available_company_types()


@router.get("/api/exchanges")
async def list_exchanges():
    """List predefined exchange profiles used for assumption substitution."""
    from ..template_loader import list_exchanges as list_available_exchanges
    return list_available_exchanges()
