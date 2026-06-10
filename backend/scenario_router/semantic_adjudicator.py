from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict

from ..config import (
    SCENARIO_ROUTER_MODEL_ADJUDICATION_MAX_OUTPUT_TOKENS,
    SCENARIO_ROUTER_MODEL_ADJUDICATION_MODEL,
    SCENARIO_ROUTER_MODEL_ADJUDICATION_REASONING_EFFORT,
    SCENARIO_ROUTER_MODEL_ADJUDICATION_TIMEOUT_SECONDS,
)
from ..openrouter import query_model


@dataclass
class ModelSemanticAdjudicator:
    """Schema-bound model comparator for ambiguous router watchlist items."""

    model: str = SCENARIO_ROUTER_MODEL_ADJUDICATION_MODEL
    timeout_seconds: float = SCENARIO_ROUTER_MODEL_ADJUDICATION_TIMEOUT_SECONDS
    max_output_tokens: int = SCENARIO_ROUTER_MODEL_ADJUDICATION_MAX_OUTPUT_TOKENS
    reasoning_effort: str = SCENARIO_ROUTER_MODEL_ADJUDICATION_REASONING_EFFORT

    async def adjudicate(self, request: Dict[str, Any]) -> Dict[str, Any]:
        if not self.model:
            return {}
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a strict evidence-bound thesis-router adjudicator. Compare one new filing "
                    "against one saved monitoring watchlist item. Use only the provided filing text. "
                    "Return JSON only. Do not infer missing binding terms, economics, dates, volumes, "
                    "pricing, approvals, or customer commitments. If a saved item requires a binding or "
                    "definitive agreement, an LoI, MoU, framework, non-binding term sheet, negotiation, "
                    "or partnership announcement can be a precursor/partial match, but not a full match."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Adjudicate this router watchlist candidate.\n\n"
                    "Return exactly this JSON shape:\n"
                    "{\n"
                    '  "status": "matched|partial_match|contradicted|not_matched|unclear",\n'
                    '  "relationship": "full_match|precursor_partial_match|related_partial_match|contradicts|not_related|unclear",\n'
                    '  "satisfies_condition": true,\n'
                    '  "confidence": 0.0,\n'
                    '  "reason": "one sentence, evidence-bound",\n'
                    '  "evidence_quote": "short quote from the filing text, if available",\n'
                    '  "missing_for_full_match": ["specific missing evidence if partial"]\n'
                    "}\n\n"
                    f"INPUT_JSON:\n{json.dumps(_compact_request(request), ensure_ascii=False)}"
                ),
            },
        ]
        response = await query_model(
            self.model,
            messages,
            timeout=float(self.timeout_seconds or 45.0),
            max_tokens=int(self.max_output_tokens or 900),
            reasoning_effort=self.reasoning_effort,
        )
        content = str((response or {}).get("content") or "").strip()
        return parse_adjudicator_json(content)


def parse_adjudicator_json(content: str) -> Dict[str, Any]:
    text = str(content or "").strip()
    if not text:
        return {}
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {}
        try:
            payload = json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}
    return payload if isinstance(payload, dict) else {}


def _compact_request(request: Dict[str, Any]) -> Dict[str, Any]:
    payload = request if isinstance(request, dict) else {}
    announcement = payload.get("announcement") if isinstance(payload.get("announcement"), dict) else {}
    watchlist = payload.get("watchlist_item") if isinstance(payload.get("watchlist_item"), dict) else {}
    return {
        "ticker": str(payload.get("ticker") or "").strip(),
        "template_id": str(payload.get("template_id") or "").strip(),
        "announcement": {
            "title": str(announcement.get("title") or "").strip(),
            "filing_summary": str(announcement.get("filing_summary") or "").strip(),
            "semantic_summary": str(announcement.get("semantic_summary") or "").strip(),
            "announcement_class": str(announcement.get("announcement_class") or "").strip(),
            "materiality": str(announcement.get("materiality") or "").strip(),
            "trajectory_effect": str(announcement.get("trajectory_effect") or "").strip(),
            "affected_drivers": announcement.get("affected_drivers") if isinstance(announcement.get("affected_drivers"), list) else [],
            "evidence_text": str(announcement.get("evidence_text") or "").strip()[:5000],
        },
        "watchlist_item": {
            "condition_id": str(watchlist.get("condition_id") or "").strip(),
            "group": str(watchlist.get("group") or "").strip(),
            "label": str(watchlist.get("label") or "").strip(),
            "description": str(watchlist.get("description") or "").strip(),
            "evidence_hooks": watchlist.get("evidence_hooks") if isinstance(watchlist.get("evidence_hooks"), list) else [],
            "severity": str(watchlist.get("severity") or "").strip(),
        },
    }
