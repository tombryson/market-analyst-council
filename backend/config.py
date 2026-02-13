"""Configuration for the LLM Council."""

import os
from dotenv import load_dotenv, find_dotenv

# Load .env - find_dotenv() searches up from current directory
load_dotenv(find_dotenv())

# OpenRouter API key
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# OpenRouter API endpoint
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# Data directory for conversation storage
DATA_DIR = "data/conversations"

# Research service flags
def _get_bool(name: str, default: bool = False) -> bool:
    """Read boolean env var with simple true/false coercion."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    """Read integer env var with fallback."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_float(name: str, default: float) -> float:
    """Read float env var with fallback."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_csv(name: str) -> list[str]:
    """Read comma-separated env var into list."""
    value = os.getenv(name, "").strip()
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


# Council members - list of OpenRouter model identifiers
_COUNCIL_MODELS = _get_csv("COUNCIL_MODELS")
COUNCIL_MODELS = _COUNCIL_MODELS or [
    "openai/gpt-5.2",
    "google/gemini-3-pro-preview",
    "anthropic/claude-opus-4.6",
    "x-ai/grok-4.1-fast",
]

# Chairman model - synthesizes final response
CHAIRMAN_MODEL = os.getenv("CHAIRMAN_MODEL", "google/gemini-3-pro-preview").strip()


# Feature flag to route retrieval through backend/research service
ENABLE_RESEARCH_SERVICE = _get_bool("ENABLE_RESEARCH_SERVICE", default=False)
PROGRESS_LOGGING = _get_bool("PROGRESS_LOGGING", default=True)

# Retrieval configuration
RESEARCH_PROVIDER = os.getenv("RESEARCH_PROVIDER", "tavily").strip().lower()
RESEARCH_DEPTH = os.getenv("RESEARCH_DEPTH", "basic").strip().lower()
MAX_SOURCES = _get_int("MAX_SOURCES", 7)
ENABLE_MARKET_FACTS_PREPASS = _get_bool("ENABLE_MARKET_FACTS_PREPASS", default=True)
MARKET_FACTS_TIMEOUT_SECONDS = _get_float("MARKET_FACTS_TIMEOUT_SECONDS", 12.0)

# Council execution mode
# - local: existing Stage1 responses via OpenRouter
# - perplexity_emulated: Stage1 responses are per-model Perplexity deep-research runs
COUNCIL_EXECUTION_MODE = os.getenv("COUNCIL_EXECUTION_MODE", "local").strip().lower()

# Model list for Perplexity emulated council stage 1.
# Falls back to COUNCIL_MODELS if not explicitly set.
# Note: Perplexity model IDs can differ from OpenRouter IDs (e.g. xai/... vs x-ai/...).
_PERPLEXITY_COUNCIL_MODELS = _get_csv("PERPLEXITY_COUNCIL_MODELS")
PERPLEXITY_COUNCIL_MODELS = _PERPLEXITY_COUNCIL_MODELS or [
    "openai/gpt-5.2",
    "google/gemini-3-pro-preview",
]

# Tavily Search API
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
ENABLE_SEARCH_BY_DEFAULT = True
MAX_SEARCH_RESULTS = 5

# Perplexity API
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
PERPLEXITY_API_URL = os.getenv(
    "PERPLEXITY_API_URL",
    "https://api.perplexity.ai/v1/responses",
)
PERPLEXITY_MODEL = os.getenv("PERPLEXITY_MODEL", "openai/gpt-5.2")
PERPLEXITY_PRESET = os.getenv("PERPLEXITY_PRESET", "deep-research")
PERPLEXITY_TIMEOUT_SECONDS = _get_float("PERPLEXITY_TIMEOUT_SECONDS", 180.0)
PERPLEXITY_MAX_STEPS = _get_int("PERPLEXITY_MAX_STEPS", 10)
PERPLEXITY_MAX_OUTPUT_TOKENS = _get_int("PERPLEXITY_MAX_OUTPUT_TOKENS", 4096)
PERPLEXITY_REASONING_EFFORT = os.getenv(
    "PERPLEXITY_REASONING_EFFORT",
    "low",
).strip().lower()
PERPLEXITY_ENABLE_WEB_SEARCH_TOOL = _get_bool(
    "PERPLEXITY_ENABLE_WEB_SEARCH_TOOL",
    default=True,
)
PERPLEXITY_ENABLE_FETCH_URL_TOOL = _get_bool(
    "PERPLEXITY_ENABLE_FETCH_URL_TOOL",
    default=True,
)
PERPLEXITY_MAX_RESULTS_PER_QUERY = _get_int("PERPLEXITY_MAX_RESULTS_PER_QUERY", 7)
PERPLEXITY_MAX_TOKENS_PER_PAGE = _get_int("PERPLEXITY_MAX_TOKENS_PER_PAGE", 1024)
PERPLEXITY_ALLOWED_DOMAINS = _get_csv("PERPLEXITY_ALLOWED_DOMAINS")
PERPLEXITY_BLOCKED_DOMAINS = _get_csv("PERPLEXITY_BLOCKED_DOMAINS")
PERPLEXITY_SEARCH_AFTER_DATE_FILTER = os.getenv(
    "PERPLEXITY_SEARCH_AFTER_DATE_FILTER",
    "",
).strip()
PERPLEXITY_SEARCH_BEFORE_DATE_FILTER = os.getenv(
    "PERPLEXITY_SEARCH_BEFORE_DATE_FILTER",
    "",
).strip()
PERPLEXITY_STAGE1_EXECUTION_MODE = os.getenv(
    "PERPLEXITY_STAGE1_EXECUTION_MODE",
    "staggered",
).strip().lower()
PERPLEXITY_STAGE1_STAGGER_SECONDS = _get_float("PERPLEXITY_STAGE1_STAGGER_SECONDS", 2.0)
# Preferred setting: total Stage 1 attempts per model (including the first attempt).
PERPLEXITY_STAGE1_MAX_ATTEMPTS = _get_int("PERPLEXITY_STAGE1_MAX_ATTEMPTS", 3)
# Legacy setting retained for compatibility; interpreted as retry count.
PERPLEXITY_STAGE1_MAX_RETRIES = _get_int("PERPLEXITY_STAGE1_MAX_RETRIES", 3)
PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS = _get_float("PERPLEXITY_STAGE1_RETRY_BACKOFF_SECONDS", 5.0)
PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_TEMPLATE_RETRY_ENABLED",
    default=False,
)
PERPLEXITY_STAGE1_SECOND_PASS_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_SECOND_PASS_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS = _get_float(
    "PERPLEXITY_STAGE1_SECOND_PASS_TIMEOUT_SECONDS",
    180.0,
)
PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS",
    2,
)
PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS = _get_float(
    "PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS",
    4.0,
)
PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES",
    5,
)
PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE",
    1200,
)
PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_OPENAI_BASE_GUARDRAILS_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES = _get_int(
    "PERPLEXITY_STAGE1_OPENAI_BASE_MAX_SOURCES",
    6,
)
PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS = _get_int(
    "PERPLEXITY_STAGE1_OPENAI_BASE_MAX_STEPS",
    3,
)
PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT = os.getenv(
    "PERPLEXITY_STAGE1_OPENAI_BASE_REASONING_EFFORT",
    "medium",
).strip().lower()
ENABLE_SOURCE_DECODING = _get_bool("ENABLE_SOURCE_DECODING", default=True)
SOURCE_DECODING_MAX_PER_MODEL = _get_int("SOURCE_DECODING_MAX_PER_MODEL", 10)
SOURCE_DECODING_MAX_CHARS = _get_int("SOURCE_DECODING_MAX_CHARS", 3500)
SOURCE_DECODING_TIMEOUT_SECONDS = _get_float("SOURCE_DECODING_TIMEOUT_SECONDS", 20.0)

# PDF Processing
ATTACHMENTS_DIR = "data/attachments"
MAX_PDF_SIZE_MB = 50
MAX_PDF_PAGES_FULL_TEXT = 20
