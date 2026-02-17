"""OpenRouter API client for making LLM requests."""

import httpx
from typing import List, Dict, Any, Optional
from .config import OPENROUTER_API_KEY, OPENROUTER_API_URL


async def query_model(
    model: str,
    messages: List[Dict[str, str]],
    timeout: float = 120.0,
    max_tokens: Optional[int] = None,
    reasoning_effort: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Query a single model via OpenRouter API.

    Args:
        model: OpenRouter model identifier (e.g., "openai/gpt-4o")
        messages: List of message dicts with 'role' and 'content'
        timeout: Request timeout in seconds
        max_tokens: Optional completion token cap for the model response
        reasoning_effort: Optional reasoning effort override (low|medium|high)

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
    effort = str(reasoning_effort or "").strip().lower()
    if effort in {"low", "medium", "high"}:
        payload["reasoning"] = {"effort": effort}

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
        print(f"Error querying model {model}: HTTP {status} body={body}")
        return None
    except Exception as e:
        print(f"Error querying model {model}: {type(e).__name__}: {e}")
        return None


async def query_models_parallel(
    models: List[str],
    messages: List[Dict[str, str]]
) -> Dict[str, Optional[Dict[str, Any]]]:
    """
    Query multiple models in parallel.

    Args:
        models: List of OpenRouter model identifiers
        messages: List of message dicts to send to each model

    Returns:
        Dict mapping model identifier to response dict (or None if failed)
    """
    import asyncio

    # Create tasks for all models
    tasks = [query_model(model, messages) for model in models]

    # Wait for all to complete
    responses = await asyncio.gather(*tasks)

    # Map models to their responses
    return {model: response for model, response in zip(models, responses)}
