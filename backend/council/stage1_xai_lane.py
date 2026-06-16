"""xAI supplementary macro news lane for Stage 1.

Fetches macroeconomic / sector context from xAI's API and formats it as
a supplementary source block that enriches Stage-1 model queries.
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from ..config import (
    MAX_SOURCES,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_ENABLED,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_RECENCY_DAYS,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_SOURCES,
    PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_RETRIEVAL_MAX_SOURCES,
    STAGE1_SUPPLEMENTARY_XAI_MAX_TOKENS,
    STAGE1_SUPPLEMENTARY_XAI_MAX_TOOL_ITERATIONS,
    STAGE1_SUPPLEMENTARY_XAI_MODEL,
    STAGE1_SUPPLEMENTARY_XAI_TEMPERATURE,
    STAGE1_SUPPLEMENTARY_XAI_TIMEOUT_SECONDS,
    XAI_API_KEY,
    XAI_API_URL,
)
from .stage1_attempt import _progress_log

logger = logging.getLogger(__name__)

def _resolve_template_commodity_profile(template_id: str) -> str:
    """Resolve commodity profile from template behavior."""
    key = str(template_id or "").strip()
    if not key:
        return ""
    try:
        from .template_loader import get_template_loader

        loader = get_template_loader()
        behavior = loader.get_template_behavior(key) or {}
        profile = str(behavior.get("commodity_profile", "")).strip().lower()
        return profile if profile in _SUPPLEMENTARY_MACRO_PROFILE_CONFIG else ""
    except Exception:
        return ""


def _build_supplementary_macro_summary_prompt(*, sector_label: str) -> str:
    """Build xAI prompt for supplementary macro context (single dense paragraph)."""
    sector = str(sector_label or "").strip() or "sector"
    return (
        f"Provide one single-paragraph macro news brief for the [{sector}]. "
        "Minimum 200 words (target 220-320 words). "
        "Cover: the last week, the last month, the last year, and the 12-24 month forward outlook. "
        "Include concrete levels where relevant (e.g., commodity prices, inventories, policy moves, supply disruptions) "
        "and make reference to the broader macro environment including oil prices, inflation, interest rates, "
        "and the four quadrant global macro framework. "
        "Keep the paragraph decision-useful for scenario assumptions. "
        "Output plain text only. Do NOT include URLs, citation markers, footnotes, source lists, markdown, or bullet points."
    ).strip()


def _sanitize_supplementary_macro_summary_text(text: str) -> str:
    """Normalize xAI macro brief to plain-text paragraph with no citation/link artifacts."""
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned = raw
    # Drop markdown links -> keep anchor text only.
    cleaned = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", cleaned, flags=re.IGNORECASE)
    # Drop explicit citation markers like [[1]] / [1].
    cleaned = re.sub(r"\[\[\d+\]\]|\[\d+\]", "", cleaned)
    # Drop raw URLs.
    cleaned = re.sub(r"https?://\S+", "", cleaned, flags=re.IGNORECASE)
    # Single paragraph.
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


async def _fetch_xai_supplementary_macro_summary(
    *,
    sector_label: str,
    user_query: str,
) -> Dict[str, Any]:
    """Generate a single supplementary sector-macro paragraph via xAI."""
    if not XAI_API_KEY:
        return {
            "attempted": False,
            "success": False,
            "error": "xai_api_key_missing",
            "summary": "",
            "prompt": "",
            "http_status": 0,
            "request_count": 0,
            "tool_calls_count": 0,
            "finish_reason": "",
        }

    prompt = _build_supplementary_macro_summary_prompt(sector_label=sector_label)
    # Per requested behavior, send the exact sector prompt only.
    input_text = prompt
    tools: List[Dict[str, Any]] = [
        {"type": "web_search"},
        {"type": "x_search"},
    ]

    request_count = 0
    tool_calls_count = 0
    http_status = 0
    finish_reason = ""
    final_content = ""
    timeout_seconds = max(20.0, float(STAGE1_SUPPLEMENTARY_XAI_TIMEOUT_SECONDS))
    max_iterations = max(1, int(STAGE1_SUPPLEMENTARY_XAI_MAX_TOOL_ITERATIONS))
    max_tokens = max(128, int(STAGE1_SUPPLEMENTARY_XAI_MAX_TOKENS))
    temperature = max(0.0, min(1.5, float(STAGE1_SUPPLEMENTARY_XAI_TEMPERATURE)))
    endpoint = str(XAI_API_URL or "https://api.x.ai/v1/responses").strip()

    def _extract_responses_output_text(data: Dict[str, Any]) -> str:
        direct = data.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        output = data.get("output")
        texts: List[str] = []
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict):
                    continue
                item_text = item.get("text")
                if isinstance(item_text, str) and item_text.strip():
                    texts.append(item_text.strip())
                content = item.get("content")
                if isinstance(content, list):
                    for part in content:
                        if not isinstance(part, dict):
                            continue
                        txt = part.get("text")
                        if isinstance(txt, str) and txt.strip():
                            texts.append(txt.strip())
        if not texts:
            return ""
        return texts[-1]

    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            for _ in range(max_iterations):
                payload = {
                    "model": str(STAGE1_SUPPLEMENTARY_XAI_MODEL or "grok-4-1-fast-reasoning").strip(),
                    "input": input_text,
                    "tools": tools,
                    "max_output_tokens": max_tokens,
                    "temperature": temperature,
                }
                request_count += 1
                response = await client.post(
                    endpoint,
                    headers={
                        "Authorization": f"Bearer {XAI_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                http_status = int(response.status_code)
                if response.status_code >= 400:
                    err_text = (response.text or "").strip()
                    return {
                        "attempted": True,
                        "success": False,
                        "error": f"xai_http_{response.status_code}:{err_text[:800]}",
                        "summary": "",
                        "prompt": prompt,
                        "http_status": http_status,
                        "request_count": request_count,
                        "tool_calls_count": tool_calls_count,
                        "finish_reason": finish_reason,
                    }

                data = response.json() if response.content else {}
                finish_reason = str(
                    (data.get("status") if isinstance(data, dict) else "") or ""
                ).strip().lower()
                output = (data.get("output") if isinstance(data, dict) else None) or []
                if isinstance(output, list):
                    tool_calls_count += len(
                        [
                            item
                            for item in output
                            if isinstance(item, dict)
                            and str(item.get("type", "")).strip().lower().endswith("_call")
                        ]
                    )

                content = _extract_responses_output_text(data if isinstance(data, dict) else {})
                if content:
                    final_content = content
                    break
    except Exception as exc:
        return {
            "attempted": True,
            "success": False,
            "error": str(exc).strip() or "xai_request_failed",
            "summary": "",
            "prompt": prompt,
            "http_status": http_status,
            "request_count": request_count,
            "tool_calls_count": tool_calls_count,
            "finish_reason": finish_reason,
        }

    normalized = _sanitize_supplementary_macro_summary_text(str(final_content or ""))
    return {
        "attempted": True,
        "success": bool(normalized),
        "error": "" if normalized else "xai_empty_response",
        "summary": normalized,
        "prompt": prompt,
        "http_status": http_status,
        "request_count": request_count,
        "tool_calls_count": tool_calls_count,
        "finish_reason": finish_reason,
    }


async def _collect_stage1_supplementary_macro_news(
    *,
    model: str,
    user_query: str,
    run: Dict[str, Any],
    template_id: str,
    existing_source_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build one xAI-generated supplementary macro paragraph without mutating core rows.

    This lane is additive only. It injects summary text (not extra source rows).
    """
    if not PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_ENABLED:
        return {
            "enabled": False,
            "used": False,
            "commodity_profile": "",
            "sector_label": "",
            "summary_paragraph": "",
            "sources": [],
            "reason": "supplementary_news_disabled",
        }

    max_sources = max(0, int(PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_SOURCES))
    if max_sources <= 0:
        return {
            "enabled": True,
            "used": False,
            "commodity_profile": "",
            "sector_label": "",
            "summary_paragraph": "",
            "sources": [],
            "reason": "max_sources_zero",
        }

    commodity_profile = _resolve_template_commodity_profile(template_id)
    lane_cfg = _SUPPLEMENTARY_MACRO_PROFILE_CONFIG.get(commodity_profile, {})
    sector_label = ""
    query_focus = ""
    try:
        from .template_loader import get_template_loader

        loader = get_template_loader()
        behavior = loader.get_template_behavior(str(template_id or "").strip()) or {}
        sector_label = str(behavior.get("supplementary_sector_label", "")).strip()
        query_focus = str(behavior.get("supplementary_query_focus", "")).strip()
    except Exception:
        sector_label = ""
        query_focus = ""
    if not sector_label:
        sector_label = str(lane_cfg.get("sector_label", "")).strip()
    if not query_focus:
        query_focus = str(lane_cfg.get("query_focus", "")).strip()
    if not sector_label:
        return {
            "enabled": True,
            "used": False,
            "commodity_profile": commodity_profile,
            "sector_label": sector_label,
            "summary_paragraph": "",
            "sources": [],
            "reason": "no_sector_label",
        }

    summary_result = await _fetch_xai_supplementary_macro_summary(
        sector_label=sector_label,
        user_query=user_query,
    )
    summary_paragraph = str(summary_result.get("summary", "")).strip()
    retrieval_error = str(summary_result.get("error", "")).strip()
    retrieval_attempted = bool(summary_result.get("attempted", False))
    retrieval_result_count = int(1 if summary_paragraph else 0)
    reason = "ok" if summary_paragraph else "xai_summary_empty"
    if retrieval_error and not summary_paragraph:
        reason = f"xai_error:{retrieval_error}"

    return {
        "enabled": True,
        "used": bool(summary_paragraph),
        "commodity_profile": commodity_profile,
        "sector_label": sector_label,
        "query_focus": query_focus,
        "summary_paragraph": summary_paragraph,
        "summary_prompt": str(summary_result.get("prompt", "")),
        "summary_model": str(STAGE1_SUPPLEMENTARY_XAI_MODEL or "grok-4-1-fast-reasoning"),
        "summary_provider": "xai",
        "summary_http_status": int(summary_result.get("http_status", 0) or 0),
        "summary_request_count": int(summary_result.get("request_count", 0) or 0),
        "summary_tool_calls_count": int(summary_result.get("tool_calls_count", 0) or 0),
        "summary_finish_reason": str(summary_result.get("finish_reason", "")),
        "sources": [],
        "count": int(retrieval_result_count),
        "reason": reason,
        "retrieval_attempted": retrieval_attempted,
        "retrieval_query": str(summary_result.get("prompt", "")),
        "retrieval_result_count": int(retrieval_result_count),
        "retrieval_error": retrieval_error,
        "max_sources": int(max_sources),
        "max_recency_days": int(max(7, int(PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_RECENCY_DAYS))),
    }


