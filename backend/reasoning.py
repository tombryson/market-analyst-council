"""Reasoning payload helpers shared across providers."""

from typing import Any, Dict


_VALID_EFFORTS = {"xhigh", "high", "medium", "low", "minimal"}


def normalize_reasoning_effort(effort: str, *, default: str = "low") -> str:
    """Normalize user/provider reasoning effort to a non-disabled value."""
    fallback = default if default in _VALID_EFFORTS else "low"
    value = str(effort or "").strip().lower()
    if value in _VALID_EFFORTS:
        return value
    if value in {"none", "off", "disabled", "false", "0", ""}:
        return fallback
    if value in {"max", "maximum", "very_high"}:
        return "xhigh"
    if value in {"min", "minimum"}:
        return "minimal"
    return fallback


def build_reasoning_payload(
    model: str,
    effort: str,
    *,
    provider: str,
) -> Dict[str, Any]:
    """
    Build a reasoning payload that keeps reasoning enabled.

    OpenRouter: For Grok-family models include `enabled: true` explicitly.
    Perplexity: Use conservative `effort` payload shape for routed models.
    """
    normalized_effort = normalize_reasoning_effort(effort)
    model_key = str(model or "").strip().lower()
    provider_key = str(provider or "").strip().lower()

    if provider_key == "openrouter":
        if model_key.startswith("x-ai/") or "grok" in model_key:
            return {"enabled": True, "effort": normalized_effort}
        return {"effort": normalized_effort}

    # Perplexity routed calls are safest with effort-only shape.
    return {"effort": normalized_effort}

