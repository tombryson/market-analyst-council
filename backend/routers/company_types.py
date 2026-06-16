"""Company-type detection endpoint."""

import logging
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/company-types")


class CompanyTypeDetectRequest(BaseModel):
    """Request payload for company-type detection prepass."""
    content: str = ""
    ticker: Optional[str] = None
    company_name: Optional[str] = None
    exchange: Optional[str] = None


@router.post("/detect")
async def detect_company_type(request: CompanyTypeDetectRequest):
    """Deterministic company-type detection for template routing."""
    from ..template_loader import get_template_loader

    loader = get_template_loader()
    selected = loader.detect_company_type(
        user_query=request.content,
        ticker=request.ticker,
    )
    return {
        "status": "ok" if selected else "unresolved",
        "provider": "deterministic_resolver",
        "selected_company_type": selected,
        "candidate_company_type": selected,
        "applied": bool(selected),
        "confidence": 1.0 if selected else 0.0,
        "company_name": loader.infer_company_name(request.content, ticker=request.ticker),
        "exchange": loader.normalize_exchange(request.exchange) or "unknown",
    }
