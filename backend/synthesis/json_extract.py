"""JSON extraction from raw LLM text output."""

import json
import re
from typing import Any, Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

def _parse_json_from_text(raw_text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Parse JSON from direct text, fenced block, or embedded object."""
    text = str(raw_text or "").strip()
    if not text:
        return None, "Empty response"

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed, None
        return None, "Parsed JSON is not an object"
    except json.JSONDecodeError as direct_error:
        import re

        fenced = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
        if fenced:
            try:
                parsed = json.loads(fenced.group(1))
                if isinstance(parsed, dict):
                    return parsed, None
                return None, "Fenced JSON parsed but is not an object"
            except json.JSONDecodeError as fenced_error:
                return None, f"Failed to parse fenced JSON: {fenced_error}"

        embedded = re.search(r"\{.*\}", text, re.DOTALL)
        if embedded:
            try:
                parsed = json.loads(embedded.group(0))
                if isinstance(parsed, dict):
                    return parsed, None
                return None, "Embedded JSON parsed but is not an object"
            except json.JSONDecodeError as embedded_error:
                return None, f"Failed to parse embedded JSON: {embedded_error}"

        return None, f"No JSON found in response: {direct_error}"


