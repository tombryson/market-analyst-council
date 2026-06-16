"""Memo artifact endpoints."""

import logging
from pathlib import Path
from fastapi import APIRouter, HTTPException
from ..jobs.state import OUTPUTS_DIR

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/memos")


@router.get("/{memo_name}")
async def get_memo_file(memo_name: str):
    """
    Load a markdown memo artifact from outputs/ by filename.
    Example: /api/memos/analyst_memo_regen_20260306_115115
    """
    safe_name = Path(memo_name).name
    if not safe_name.endswith(".md"):
        safe_name = f"{safe_name}.md"

    path = OUTPUTS_DIR / safe_name
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Memo artifact not found")

    try:
        markdown = path.read_text(encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read memo artifact: {exc}") from exc

    return {
        "id": safe_name,
        "markdown": markdown,
    }
