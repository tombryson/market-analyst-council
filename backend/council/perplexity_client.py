"""Perplexity API client: model probing, streaming, and single-model query.

Also contains `_is_openrouter_compatible_model` and `_is_sonar_model` which
are used by the Stage-1 orchestrator to route queries to the right backend.
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

from ..config import (
    OPENROUTER_API_KEY,
    PERPLEXITY_API_KEY,
    PERPLEXITY_API_URL,
    PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED,
    PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN,
    PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS,
    PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED,
    PERPLEXITY_STAGE1_SHARED_RETRIEVAL_MODEL,
    PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED,
    PERPLEXITY_STREAM_ENABLED,
)
from ..reasoning import build_reasoning_payload, normalize_reasoning_effort
from .stage1_attempt import _progress_log

logger = logging.getLogger(__name__)

def _dedupe_model_ids(models: List[str]) -> List[str]:
    """Preserve-order dedupe for model-id lists."""
    deduped: List[str] = []
    for item in models or []:
        cleaned = str(item or "").strip()
        if cleaned and cleaned not in deduped:
            deduped.append(cleaned)
    return deduped


_PERPLEXITY_MODEL_ID_ALIASES: Dict[str, str] = {
    # Perplexity model IDs use hyphenated semantic versions.
    "anthropic/claude-sonnet-4.5": "anthropic/claude-sonnet-4-5",
    "anthropic/claude-opus-4.5": "anthropic/claude-opus-4-5",
}


def _normalize_perplexity_model_id(model: str) -> str:
    """Normalize known Perplexity model-id aliases to canonical IDs."""
    raw = str(model or "").strip()
    if not raw:
        return ""
    normalized = _PERPLEXITY_MODEL_ID_ALIASES.get(raw, raw)
    if normalized != raw:
        _progress_log(f"Perplexity model alias normalized: {raw} -> {normalized}")
    return normalized


def _is_perplexity_model_unsupported_error(status_code: int, body: str) -> bool:
    text = str(body or "").lower()
    if status_code != 400:
        return False
    return (
        "not supported" in text
        or "unsupported" in text
        or "model" in text and "validation failed" in text
    )


async def _probe_perplexity_model_support(
    *,
    model: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    """Probe whether Perplexity accepts a model ID with a minimal request."""
    raw_model = str(model or "").strip()
    resolved_model = _normalize_perplexity_model_id(raw_model)
    if not PERPLEXITY_API_KEY:
        return {
            "requested_model": raw_model,
            "resolved_model": resolved_model,
            "supported": False,
            "status_code": 0,
            "reason": "missing_api_key",
            "error_type": "config",
        }

    payload: Dict[str, Any] = {
        "model": resolved_model,
        "input": "Reply with exactly OK.",
        "max_output_tokens": 24,
    }
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=max(5.0, float(timeout_seconds))) as client:
            response = await client.post(
                PERPLEXITY_API_URL,
                headers=headers,
                json=payload,
            )
        status_code = int(response.status_code)
        body = (response.text or "")[:400]
        if status_code == 200:
            return {
                "requested_model": raw_model,
                "resolved_model": resolved_model,
                "supported": True,
                "status_code": status_code,
                "reason": "ok",
                "error_type": "",
            }
        if _is_perplexity_model_unsupported_error(status_code, body):
            return {
                "requested_model": raw_model,
                "resolved_model": resolved_model,
                "supported": False,
                "status_code": status_code,
                "reason": "unsupported_model",
                "error_type": "unsupported",
                "body_preview": body,
            }
        return {
            "requested_model": raw_model,
            "resolved_model": resolved_model,
            "supported": False,
            "status_code": status_code,
            "reason": "probe_request_failed",
            "error_type": "transient",
            "body_preview": body,
        }
    except Exception as exc:
        return {
            "requested_model": raw_model,
            "resolved_model": resolved_model,
            "supported": False,
            "status_code": 0,
            "reason": f"{type(exc).__name__}: {exc}",
            "error_type": "transient",
        }


def _extract_perplexity_finish_reason(data: Dict[str, Any]) -> str:
    """Best-effort finish/status extraction from Responses API payload."""
    output = data.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            finish = item.get("finish_reason")
            if finish:
                return str(finish)
            status = item.get("status")
            if status:
                return str(status)
    for key in ("finish_reason", "status"):
        value = data.get(key)
        if value:
            return str(value)
    return ""


def _supports_perplexity_reasoning_payload(model: str) -> bool:
    """Reasoning payload is enabled for all routed models."""
    _ = model
    return True


async def _query_model_via_perplexity(
    *,
    model: str,
    prompt: str,
    timeout: float,
    max_tokens: Optional[int],
    reasoning_effort: str,
) -> Optional[Dict[str, Any]]:
    """
    Query one model via Perplexity Responses API for Stage 1 second-pass analysis.

    This call intentionally disables web-search tools and uses only injected prompt context.
    """
    if not PERPLEXITY_API_KEY:
        _progress_log(f"Perplexity second-pass skipped model={model}: missing_api_key")
        return None

    resolved_model = _normalize_perplexity_model_id(model)
    headers = {
        "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    payload: Dict[str, Any] = {
        "model": resolved_model or model,
        "input": prompt,
    }
    stream_requested = bool(PERPLEXITY_STREAM_ENABLED)
    if stream_requested:
        payload["stream"] = True
    if isinstance(max_tokens, int) and max_tokens > 0:
        payload["max_output_tokens"] = int(max_tokens)
    effort = normalize_reasoning_effort(reasoning_effort)
    reasoning_payload_sent = False
    reasoning_effort_effective = effort
    if _supports_perplexity_reasoning_payload(resolved_model or model):
        payload["reasoning"] = build_reasoning_payload(
            resolved_model or model,
            effort,
            provider="perplexity",
        )
        reasoning_payload_sent = True

    def _is_invalid_request_400(exc: httpx.HTTPStatusError) -> bool:
        if exc.response is None or exc.response.status_code != 400:
            return False
        body = (exc.response.text or "").strip().lower()
        return "invalid request" in body

    async def _post_once(
        client: httpx.AsyncClient,
        req_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        response = await client.post(
            PERPLEXITY_API_URL,
            headers=headers,
            json=req_payload,
        )
        response.raise_for_status()
        return response.json()

    async def _post_stream(
        client: httpx.AsyncClient,
        req_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        text_deltas: List[str] = []
        final_response: Dict[str, Any] = {}
        async with client.stream(
            "POST",
            PERPLEXITY_API_URL,
            headers=headers,
            json=req_payload,
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line:
                    continue
                chunk = line.strip()
                if not chunk or chunk.startswith(":"):
                    continue
                if chunk.startswith("event:"):
                    continue
                if chunk.startswith("data:"):
                    chunk = chunk[5:].strip()
                if not chunk or chunk == "[DONE]":
                    continue
                try:
                    event = json.loads(chunk)
                except Exception:
                    continue
                event_type = str(event.get("type", "")).strip().lower()
                if event_type in {"response.output_text.delta", "output_text.delta"}:
                    delta = event.get("delta")
                    if isinstance(delta, str) and delta:
                        text_deltas.append(delta)
                if event_type in {"response.completed", "completed"}:
                    response_obj = event.get("response")
                    if isinstance(response_obj, dict):
                        final_response = response_obj
                elif event_type in {"response", "output"} and isinstance(event, dict):
                    final_response = event

        merged_text = "".join(text_deltas).strip()
        if final_response:
            if merged_text:
                final_response["output_text"] = merged_text
            return final_response
        if merged_text:
            return {
                "output_text": merged_text,
                "output": [{"type": "output_text", "text": merged_text}],
            }
        raise RuntimeError("perplexity_stream_empty_payload")

    async def _perform_request(
        client: httpx.AsyncClient,
        req_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        use_stream = bool(req_payload.get("stream"))
        if use_stream:
            try:
                return await _post_stream(client, req_payload)
            except RuntimeError as exc:
                if "perplexity_stream_empty_payload" not in str(exc):
                    raise
                retry_payload = dict(req_payload)
                retry_payload.pop("stream", None)
                _progress_log(
                    f"Perplexity second-pass stream fallback model={model}: using non-stream"
                )
                return await _post_once(client, retry_payload)
        return await _post_once(client, req_payload)

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                data = await _perform_request(client, payload)
            except httpx.HTTPStatusError as exc:
                # Some routed models reject specific reasoning payload shapes.
                # Retry once with a conservative low-effort payload.
                if _is_invalid_request_400(exc) and "reasoning" in payload:
                    retry_payload = dict(payload)
                    retry_payload["reasoning"] = build_reasoning_payload(
                        resolved_model or model,
                        "low",
                        provider="perplexity",
                    )
                    reasoning_payload_sent = True
                    reasoning_effort_effective = "low"
                    _progress_log(
                        f"Perplexity second-pass retry with low reasoning model={model} "
                        f"after 400 invalid_request"
                    )
                    data = await _perform_request(client, retry_payload)
                else:
                    raise
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        body = (exc.response.text or "")[:500] if exc.response is not None else ""
        _progress_log(
            f"Perplexity second-pass HTTP error model={model} status={status} body={body}"
        )
        return None
    except Exception as exc:
        _progress_log(
            f"Perplexity second-pass error model={model}: {type(exc).__name__}: {exc}"
        )
        return None

    content = ""
    try:
        from .research.providers.perplexity import PerplexityResearchProvider
        parser = PerplexityResearchProvider()
        content = parser._extract_content(data).strip()
    except Exception:
        content = ""

    if not content:
        output_text = data.get("output_text")
        if isinstance(output_text, str):
            content = output_text.strip()
        elif isinstance(output_text, list):
            content = "\n".join([str(item) for item in output_text if isinstance(item, str)]).strip()

    return {
        "content": content,
        "finish_reason": _extract_perplexity_finish_reason(data),
        "usage": data.get("usage"),
        "id": data.get("id"),
        "provider": data.get("provider") or "perplexity",
        "reasoning_payload_sent": bool(reasoning_payload_sent),
        "reasoning_effort_effective": str(reasoning_effort_effective or ""),
    }


def _select_shared_retrieval_model(models: List[str]) -> str:
    """
    Pick the model used for shared retrieval/decode.

    Priority:
    1) explicit env override when present in configured model list
    2) first non-openai model (typically more stable for retrieval latency)
    3) first configured model
    """
    if not models:
        return ""

    preferred = str(PERPLEXITY_STAGE1_SHARED_RETRIEVAL_MODEL or "").strip()
    if preferred:
        for model in models:
            if model == preferred:
                return model
        preferred_lower = preferred.lower()
        for model in models:
            if model.lower() == preferred_lower:
                return model

    for model in models:
        if not str(model).strip().lower().startswith("openai/"):
            return model

    return models[0]


def _is_openrouter_compatible_model(model: str) -> bool:
    """
    Return True when model id is expected to route through OpenRouter.

    Perplexity-native families like Sonar should not be sent to OpenRouter
    for Stage 1 second-pass or Stage 2 judging.
    """
    key = str(model or "").strip().lower()
    if not key:
        return False
    if "sonar" in key:
        return False
    if key.startswith("pplx/") or key.startswith("perplexity/"):
        return False
    return True


def _is_sonar_model(model: str) -> bool:
    """Return True when model id appears to be a Perplexity Sonar family model."""
    key = str(model or "").strip().lower()
    if not key:
        return False
    return "sonar" in key or key.startswith("pplx/") or key.startswith("perplexity/")


def _evaluate_stage1_sonar_telemetry(
    *,
    model: str,
    provider_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Validate Sonar multistep telemetry per run.

    We treat this as a run-quality gate for Sonar family models when enabled.
    """
    if not _is_sonar_model(model):
        return {
            "required": False,
            "passed": True,
            "reason": "not_sonar_model",
            "is_sonar_model": False,
        }

    stream_requested = bool(provider_meta.get("stream_requested", False))
    stream_used = bool(provider_meta.get("stream_used", False))
    stream_event_count = int(provider_meta.get("stream_event_count", 0) or 0)
    stream_completed = bool(provider_meta.get("stream_completed_event_seen", False))
    search_mode = str(provider_meta.get("search_mode", "") or "").strip().lower()
    search_type = str(provider_meta.get("search_type", "") or "").strip().lower()

    required = bool(PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED)
    if not required:
        return {
            "required": False,
            "passed": True,
            "reason": "sonar_multistep_not_required",
            "is_sonar_model": True,
            "stream_requested": stream_requested,
            "stream_used": stream_used,
            "stream_event_count": stream_event_count,
            "stream_completed_event_seen": stream_completed,
            "search_mode": search_mode,
            "search_type": search_type,
        }

    stream_ok = bool(stream_used and stream_event_count >= 3 and stream_completed)
    pro_mode_ok = (search_mode != "pro") or (search_type == "pro")
    passed = bool(stream_ok and pro_mode_ok)

    reasons: List[str] = []
    if not stream_requested:
        reasons.append("stream_not_requested")
    if not stream_used:
        reasons.append("stream_not_used")
    if stream_event_count < 3:
        reasons.append("insufficient_stream_events")
    if not stream_completed:
        reasons.append("stream_completion_event_missing")
    if search_mode == "pro" and search_type != "pro":
        reasons.append("search_type_not_pro_for_pro_mode")

    return {
        "required": True,
        "passed": passed,
        "reason": ",".join(reasons) if reasons else "ok",
        "is_sonar_model": True,
        "stream_requested": stream_requested,
        "stream_used": stream_used,
        "stream_event_count": stream_event_count,
        "stream_completed_event_seen": stream_completed,
        "search_mode": search_mode,
        "search_type": search_type,
    }


_FACT_PACK_SECTIONS = [
    "market_data",
    "project_economics_npv_inputs",
    "resource_and_reserve",
    "funding_and_balance_sheet",
    "development_timeline_and_milestones",
    "headwinds_and_risks",
    "tailwinds_and_catalysts",
    "management_and_governance",
    "valuation_and_peer_signals",
    "other_material_facts",
]

_FACT_PACK_KEYWORDS = {
    "market_data": [
        "market cap",
        "shares outstanding",
        "share price",
        "enterprise value",
        "ev ",
        "cash",
        "debt",
    ],
    "project_economics_npv_inputs": [
        "npv",
        "irr",
        "aisc",
        "capex",
        "opex",
        "mine life",
        "recovery",
        "royalty",
        "tax",
        "production",
        "oz",
    ],
    "resource_and_reserve": [
        "resource",
        "reserve",
        "jorc",
        "grade",
        "g/t",
        "moz",
        "ore",
    ],
    "funding_and_balance_sheet": [
        "facility",
        "loan",
        "placement",
        "capital raising",
        "financing",
        "liquidity",
        "runway",
        "cash",
    ],
    "development_timeline_and_milestones": [
        "first gold",
        "commission",
        "ramp-up",
        "production",
        "milestone",
        "q1",
        "q2",
        "q3",
        "q4",
        "202",
        "dfs",
        "pfs",
        "feasibility",
    ],
    "headwinds_and_risks": [
        "risk",
        "headwind",
        "delay",
        "dilution",
        "permit",
        "regulatory",
        "inflation",
        "power",
        "labor",
        "execution",
        "geopolitical",
    ],
    "tailwinds_and_catalysts": [
        "tailwind",
        "catalyst",
        "upside",
        "expansion",
        "drilling",
        "resource growth",
        "gold price",
        "strategic",
        "offtake",
    ],
    "management_and_governance": [
        "management",
        "board",
        "director",
        "ceo",
        "governance",
        "track record",
        "insider",
    ],
    "valuation_and_peer_signals": [
        "valuation",
        "ev/oz",
        "peer",
        "undervalued",
        "multiple",
        "discount",
        "premium",
    ],
}

_FACT_DIGEST_V2_SECTIONS = [
    "timelines_deadlines",
    "financing_deals",
    "project_economics",
    "market_share_structure",
    "management_governance",
    "operational_objectives",
    "risks_constraints",
    "catalysts_tailwinds",
    "other_material_facts",
]

_FACT_DIGEST_V2_KEYWORDS = {
    "timelines_deadlines": [
        "first gold",
        "gold pour",
        "milestone",
        "q1",
        "q2",
        "q3",
        "q4",
        "march",
        "april",
        "deadline",
        "target",
        "on track",
    ],
    "financing_deals": [
        "facility",
        "loan",
        "debt",
        "capital raise",
        "placement",
        "financing",
        "offtake",
        "agreement",
        "deal",
        "funded",
        "cash",
    ],
    "project_economics": [
        "npv",
        "irr",
        "aisc",
        "capex",
        "opex",
        "free cash flow",
        "payback",
        "mine life",
        "production",
        "gold price",
        "resource",
        "reserve",
        "grade",
    ],
    "market_share_structure": [
        "market cap",
        "enterprise value",
        "shares",
        "price",
        "valuation",
        "ev/oz",
        "multiple",
    ],
    "management_governance": [
        "management",
        "board",
        "director",
        "ceo",
        "cfo",
        "executive",
        "insider ownership",
        "governance",
        "track record",
        "appointment",
        "resignation",
    ],
    "operational_objectives": [
        "objective",
        "guidance",
        "commissioning",
        "ramp-up",
        "development",
        "stockpiling",
        "processing",
        "production",
    ],
    "risks_constraints": [
        "risk",
        "headwind",
        "delay",
        "dilution",
        "permit",
        "regulatory",
        "inflation",
        "power",
        "labor",
        "geopolitical",
        "uncertain",
    ],
    "catalysts_tailwinds": [
        "catalyst",
        "tailwind",
        "upside",
        "drilling",
        "resource growth",
        "expansion",
        "strategic",
        "improved",
    ],
}

_FACT_DIGEST_V2_NARRATIVE_ORDER = [
    "timelines_deadlines",
    "financing_deals",
    "project_economics",
    "market_share_structure",
    "management_governance",
    "operational_objectives",
    "risks_constraints",
    "catalysts_tailwinds",
]

_STAGE1_DEFAULT_TIMELINE_TERMS = [
    "first gold",
    "gold pour",
    "first ore",
    "stockpile",
    "processing",
    "on track",
    "targeting",
    "milestone",
    "timeline",
    "guidance",
    "launch",
    "approval",
    "commissioning",
    "ramp-up",
    "production",
    "q1",
    "q2",
    "q3",
    "q4",
    "march",
    "april",
    "may",
    "june",
]

_STAGE1_DEFAULT_TIMELINE_FOCUS_TERMS = [
    "milestone",
    "timeline",
    "commissioning",
    "production",
    "ramp-up",
    "target",
]
_STAGE1_SECOND_PASS_MIN_RESPONSE_CHARS = 300


