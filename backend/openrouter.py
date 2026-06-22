"""OpenRouter API client for making LLM requests."""

import asyncio
import logging
import httpx
from typing import List, Dict, Any, Optional, Callable
from .config import OPENROUTER_API_KEY, OPENROUTER_API_URL
from .reasoning import build_reasoning_payload, normalize_reasoning_effort

logger = logging.getLogger(__name__)
_TRANSIENT_HTTP_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}


def _build_error_response(
    *,
    model: str,
    status: Optional[int] = None,
    body: str = "",
    error_type: str,
    error_message: str,
    retryable: bool,
) -> Dict[str, Any]:
    return {
        "content": "",
        "provider": "openrouter",
        "model": model,
        "error": error_message,
        "error_type": error_type,
        "error_status": status,
        "error_body": body,
        "error_retryable": bool(retryable),
    }


async def query_model(
    model: str,
    messages: List[Dict[str, str]],
    timeout: float = 120.0,
    max_tokens: Optional[int] = None,
    reasoning_effort: str = "",
    include_error_details: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Query a single model via OpenRouter API.

    Args:
        model: OpenRouter model identifier (e.g., "openai/gpt-4o")
        messages: List of message dicts with 'role' and 'content'
        timeout: Request timeout in seconds
        max_tokens: Optional completion token cap for the model response
        reasoning_effort: Optional reasoning effort override
        include_error_details: When true, return structured failure metadata
            instead of None so callers can distinguish permanent from retryable
            errors.

    Returns:
        Response dict with 'content' and metadata, or None if failed.
    """
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": messages,
    }
    if isinstance(max_tokens, int) and max_tokens > 0:
        payload["max_tokens"] = int(max_tokens)
    effort = normalize_reasoning_effort(reasoning_effort)
    payload["reasoning"] = build_reasoning_payload(
        model=model,
        effort=effort,
        provider="openrouter",
    )

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                OPENROUTER_API_URL,
                headers=headers,
                json=payload
            )
            response.raise_for_status()

            data = response.json()
            choices = data.get("choices") or []
            choice = choices[0] if choices else {}
            message = choice.get("message") or {}
            raw_content = message.get("content")
            content: str = ""
            if isinstance(raw_content, str):
                content = raw_content
            elif isinstance(raw_content, list):
                parts: List[str] = []
                for item in raw_content:
                    if not isinstance(item, dict):
                        continue
                    text_part = item.get("text")
                    if isinstance(text_part, str) and text_part:
                        parts.append(text_part)
                content = "\n".join(parts).strip()
            elif raw_content is not None:
                content = str(raw_content)

            return {
                "content": content,
                "reasoning_details": message.get("reasoning_details"),
                "finish_reason": choice.get("finish_reason"),
                "usage": data.get("usage"),
                "id": data.get("id"),
                "provider": data.get("provider"),
            }

    except httpx.HTTPStatusError as e:
        status = e.response.status_code if e.response is not None else "unknown"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        logger.error("Error querying model %s: HTTP %s body=%s", model, status, body)
        if include_error_details:
            status_int = status if isinstance(status, int) else None
            body_lower = body.lower()
            if status_int == 402 and (
                "more credits" in body_lower
                or "can only afford" in body_lower
                or "\"code\":402" in body_lower
            ):
                return _build_error_response(
                    model=model,
                    status=status_int,
                    body=body,
                    error_type="insufficient_credits",
                    error_message=f"openrouter_insufficient_credits_http_{status_int}",
                    retryable=False,
                )
            return _build_error_response(
                model=model,
                status=status_int,
                body=body,
                error_type="http_error",
                error_message=f"openrouter_http_{status}",
                retryable=status_int in _TRANSIENT_HTTP_STATUS_CODES,
            )
        return None
    except Exception as e:
        logger.error("Error querying model %s: %s: %s", model, type(e).__name__, e)
        if include_error_details:
            error_type = "timeout" if isinstance(e, (httpx.TimeoutException, asyncio.TimeoutError)) else "transport_error"
            return _build_error_response(
                model=model,
                error_type=error_type,
                error_message=f"openrouter_{type(e).__name__.lower()}",
                retryable=error_type == "timeout",
            )
        return None


async def query_models_parallel(
    models: List[str],
    messages: List[Dict[str, str]],
    timeout: float = 120.0,
    max_tokens: Optional[int] = None,
    reasoning_effort: str = "",
    on_model_complete: Optional[Callable[[str, Optional[Dict[str, Any]], int, int], None]] = None,
) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    Query multiple models in parallel.

    Args:
        models: List of OpenRouter model identifiers
        messages: List of message dicts to send to each model
        timeout: Per-model request timeout in seconds
        max_tokens: Optional completion token cap applied to every model
        reasoning_effort: Optional reasoning effort override applied to every model
        on_model_complete: Optional progress callback (model, response, completed, total)

    Returns:
        Dict mapping model identifier to response dict (or None if failed)
    """
    async def _run_model(model: str) -> tuple[str, Optional[Dict[str, Any]]]:
        return model, await query_model(
            model,
            messages,
            timeout=timeout,
            max_tokens=max_tokens,
            reasoning_effort=reasoning_effort,
        )

    tasks = [asyncio.create_task(_run_model(model)) for model in models]
    responses: Dict[str, Optional[Dict[str, Any]]] = {}
    total = len(tasks)
    completed = 0

    for task in asyncio.as_completed(tasks):
        model, response = await task
        responses[model] = response
        completed += 1
        if on_model_complete is not None:
            try:
                on_model_complete(model, response, completed, total)
            except Exception:
                # Progress callbacks are advisory only; never fail the model run.
                pass

    return responses
