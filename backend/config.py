"""Configuration for the LLM Council."""

import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Load env files deterministically:
# 1) .env (base)
# 2) .env.<ENV_PROFILE> (optional profile overlay)
# 3) .env.local (optional machine-local overlay)
#
# Precedence is controlled by ENV_PRECEDENCE:
# - env  (default): parent-process env vars override file values.
# - file: .env/.env.<profile>/.env.local override parent-process env vars.
_preloaded_env_profile = os.getenv("ENV_PROFILE", "").strip().lower()
_base_env_path = find_dotenv(".env", usecwd=True)
if _base_env_path:
    # Bootstrap pass so ENV_PRECEDENCE/ENV_PROFILE can come from .env when unset.
    load_dotenv(_base_env_path, override=False)
_base_env_dir = (
    Path(_base_env_path).resolve().parent
    if _base_env_path
    else Path.cwd().resolve()
)
_env_precedence = os.getenv("ENV_PRECEDENCE", "env").strip().lower()
if _env_precedence not in {"env", "file"}:
    _env_precedence = "env"
_files_override_parent = _env_precedence == "file"

if _base_env_path and _files_override_parent:
    # Re-apply base values with file precedence semantics.
    load_dotenv(_base_env_path, override=True)

ENV_PRECEDENCE = _env_precedence
ENV_PROFILE = _preloaded_env_profile or os.getenv("ENV_PROFILE", "").strip().lower()
if ENV_PROFILE:
    _profile_path = _base_env_dir / f".env.{ENV_PROFILE}"
    if _profile_path.exists():
        load_dotenv(_profile_path, override=_files_override_parent)

_local_env_path = _base_env_dir / ".env.local"
if _local_env_path.exists():
    load_dotenv(_local_env_path, override=_files_override_parent)

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


# LlamaParse API settings
LLAMAPARSE_API_KEY = os.getenv("LLAMAPARSE_API_KEY", "").strip()
LLAMAPARSE_API_URL = os.getenv(
    "LLAMAPARSE_API_URL",
    "https://api.cloud.llamaindex.ai/api/v2/parse",
).strip()
LLAMAPARSE_UPLOAD_URL = os.getenv(
    "LLAMAPARSE_UPLOAD_URL",
    "https://api.cloud.llamaindex.ai/api/v2/parse/upload",
).strip()
LLAMAPARSE_TIER = os.getenv("LLAMAPARSE_TIER", "agentic").strip().lower()
LLAMAPARSE_VERSION = os.getenv("LLAMAPARSE_VERSION", "latest").strip()
LLAMAPARSE_COST_OPTIMIZER_ENABLED = _get_bool(
    "LLAMAPARSE_COST_OPTIMIZER_ENABLED",
    default=True,
)
LLAMAPARSE_TIMEOUT_SECONDS = _get_float("LLAMAPARSE_TIMEOUT_SECONDS", 240.0)
LLAMAPARSE_POLL_INTERVAL_SECONDS = _get_float(
    "LLAMAPARSE_POLL_INTERVAL_SECONDS",
    2.0,
)

# LiteParse settings
LITEPARSE_OCR_ENABLED = _get_bool("LITEPARSE_OCR_ENABLED", default=False)
LITEPARSE_TIMEOUT_SECONDS = _get_float("LITEPARSE_TIMEOUT_SECONDS", 240.0)


# Council members - list of OpenRouter model identifiers
_COUNCIL_MODELS = _get_csv("COUNCIL_MODELS")
COUNCIL_MODELS = _COUNCIL_MODELS or [
    "minimax/minimax-m2.7",
    "x-ai/grok-4.20",
    "qwen/qwen3.6-plus",
    "z-ai/glm-5.1",
    "google/gemma-4-26b-a4b-it",
    "moonshotai/kimi-k2.5",
]

# Chairman model - synthesizes final response
CHAIRMAN_MODEL = os.getenv("CHAIRMAN_MODEL", "google/gemini-3.1-pro-preview").strip()
CHAIRMAN_TIMEOUT_SECONDS = _get_float("CHAIRMAN_TIMEOUT_SECONDS", 300.0)
# Explicit Stage 3 completion budget for chairman calls.
# Set to 0 only if you intentionally want provider-default limits.
CHAIRMAN_MAX_OUTPUT_TOKENS = _get_int("CHAIRMAN_MAX_OUTPUT_TOKENS", 16000)
# Chairman output style:
# - text_xml (default): chairman writes structured plain text with XML-style tags
# - json: chairman writes JSON directly
CHAIRMAN_OUTPUT_STYLE = os.getenv("CHAIRMAN_OUTPUT_STYLE", "text_xml").strip().lower()
# Secondary JSON normalizer model (used when chairman output is non-JSON or malformed).
CHAIRMAN_JSONIFIER_MODEL = os.getenv(
    "CHAIRMAN_JSONIFIER_MODEL",
    "google/gemini-3.1-flash-lite-preview",
).strip()
CHAIRMAN_JSONIFY_ALWAYS = _get_bool("CHAIRMAN_JSONIFY_ALWAYS", default=True)
CHAIRMAN_JSONIFIER_TIMEOUT_SECONDS = _get_float(
    "CHAIRMAN_JSONIFIER_TIMEOUT_SECONDS",
    180.0,
)
CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS = _get_int(
    "CHAIRMAN_JSONIFIER_MAX_OUTPUT_TOKENS",
    12000,
)
# Human-readable Stage 3 memo (market-analyst style) derived from chairman output.
STAGE3_ANALYST_MEMO_ENABLED = _get_bool("STAGE3_ANALYST_MEMO_ENABLED", default=True)
STAGE3_ANALYST_MEMO_MODEL = os.getenv(
    "STAGE3_ANALYST_MEMO_MODEL",
    "google/gemini-3.1-flash-lite-preview",
).strip()
STAGE3_ANALYST_MEMO_TIMEOUT_SECONDS = _get_float(
    "STAGE3_ANALYST_MEMO_TIMEOUT_SECONDS",
    180.0,
)
STAGE3_ANALYST_MEMO_MAX_OUTPUT_TOKENS = _get_int(
    "STAGE3_ANALYST_MEMO_MAX_OUTPUT_TOKENS",
    5000,
)

# Stage 1 reference-table parser (for analyst memo): use model-to-model extraction
# instead of regex heuristics when enabled.
STAGE1_REFERENCE_PARSER_ENABLED = _get_bool(
    "STAGE1_REFERENCE_PARSER_ENABLED",
    default=True,
)
STAGE1_REFERENCE_PARSER_MODEL = os.getenv(
    "STAGE1_REFERENCE_PARSER_MODEL",
    "google/gemini-3.1-flash-lite-preview",
).strip()
STAGE1_REFERENCE_PARSER_TIMEOUT_SECONDS = _get_float(
    "STAGE1_REFERENCE_PARSER_TIMEOUT_SECONDS",
    90.0,
)
STAGE1_REFERENCE_PARSER_MAX_OUTPUT_TOKENS = _get_int(
    "STAGE1_REFERENCE_PARSER_MAX_OUTPUT_TOKENS",
    900,
)
STAGE1_REFERENCE_PARSER_CONCURRENCY = _get_int(
    "STAGE1_REFERENCE_PARSER_CONCURRENCY",
    3,
)

# Stage 1 truncation checker: lightweight post-response adjudicator used to
# decide whether a second-pass answer was genuinely cut off.
STAGE1_TRUNCATION_CHECKER_ENABLED = _get_bool(
    "STAGE1_TRUNCATION_CHECKER_ENABLED",
    default=True,
)
STAGE1_TRUNCATION_CHECKER_MODEL = os.getenv(
    "STAGE1_TRUNCATION_CHECKER_MODEL",
    "google/gemini-3.1-flash-lite-preview",
).strip()
STAGE1_TRUNCATION_CHECKER_TIMEOUT_SECONDS = _get_float(
    "STAGE1_TRUNCATION_CHECKER_TIMEOUT_SECONDS",
    35.0,
)
STAGE1_TRUNCATION_CHECKER_MAX_OUTPUT_TOKENS = _get_int(
    "STAGE1_TRUNCATION_CHECKER_MAX_OUTPUT_TOKENS",
    220,
)
STAGE1_TRUNCATION_CHECKER_REASONING_EFFORT = os.getenv(
    "STAGE1_TRUNCATION_CHECKER_REASONING_EFFORT",
    "low",
).strip().lower()
STAGE1_TRUNCATION_CHECKER_MIN_CONFIDENCE_PCT = _get_float(
    "STAGE1_TRUNCATION_CHECKER_MIN_CONFIDENCE_PCT",
    90.0,
)

# Scenario router inbox/webhook settings
SCENARIO_ROUTER_WEBHOOK_SECRET = os.getenv(
    "SCENARIO_ROUTER_WEBHOOK_SECRET",
    os.getenv("FRESHNESS_WEBHOOK_SECRET", ""),
).strip()
SCENARIO_ROUTER_WEBHOOK_REQUIRE_SECRET = _get_bool(
    "SCENARIO_ROUTER_WEBHOOK_REQUIRE_SECRET",
    default=_get_bool("FRESHNESS_WEBHOOK_REQUIRE_SECRET", default=True),
)


# Feature flag to route retrieval through backend/research service
ENABLE_RESEARCH_SERVICE = _get_bool("ENABLE_RESEARCH_SERVICE", default=False)
PROGRESS_LOGGING = _get_bool("PROGRESS_LOGGING", default=True)
SYSTEM_ENABLED = _get_bool("SYSTEM_ENABLED", default=True)
SYSTEM_SHUTDOWN_REASON = os.getenv(
    "SYSTEM_SHUTDOWN_REASON",
    "System temporarily disabled for audit and diagnostics.",
).strip()
SYSTEM_ALLOW_DIAGNOSTICS_WHEN_DISABLED = _get_bool(
    "SYSTEM_ALLOW_DIAGNOSTICS_WHEN_DISABLED",
    default=True,
)

# Retrieval configuration
RESEARCH_PROVIDER = os.getenv("RESEARCH_PROVIDER", "tavily").strip().lower()
RESEARCH_DEPTH = os.getenv("RESEARCH_DEPTH", "basic").strip().lower()
MAX_SOURCES = _get_int("MAX_SOURCES", 7)
ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED = _get_bool(
    "ASX_DETERMINISTIC_ANNOUNCEMENTS_ENABLED",
    default=True,
)
ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS = _get_int(
    "ASX_DETERMINISTIC_TARGET_ANNOUNCEMENTS",
    10,
)
ASX_DETERMINISTIC_LOOKBACK_YEARS = _get_int(
    "ASX_DETERMINISTIC_LOOKBACK_YEARS",
    2,
)
ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY = _get_bool(
    "ASX_DETERMINISTIC_PRICE_SENSITIVE_ONLY",
    default=True,
)
ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL = _get_bool(
    "ASX_DETERMINISTIC_INCLUDE_NON_SENSITIVE_FILL",
    default=False,
)
ASX_DETERMINISTIC_MAX_DECODE = _get_int(
    "ASX_DETERMINISTIC_MAX_DECODE",
    10,
)
ASX_DETERMINISTIC_FETCH_TIMEOUT_SECONDS = _get_float(
    "ASX_DETERMINISTIC_FETCH_TIMEOUT_SECONDS",
    20.0,
)
ENABLE_MARKET_FACTS_PREPASS = _get_bool("ENABLE_MARKET_FACTS_PREPASS", default=True)
MARKET_FACTS_TIMEOUT_SECONDS = _get_float("MARKET_FACTS_TIMEOUT_SECONDS", 12.0)
ENABLE_COMPANY_TYPE_API_DETECTION = _get_bool(
    "ENABLE_COMPANY_TYPE_API_DETECTION",
    default=True,
)
COMPANY_TYPE_DETECTION_PROVIDER = os.getenv(
    "COMPANY_TYPE_DETECTION_PROVIDER",
    "tavily",
).strip().lower()
COMPANY_TYPE_DETECTION_TIMEOUT_SECONDS = _get_float(
    "COMPANY_TYPE_DETECTION_TIMEOUT_SECONDS",
    20.0,
)
COMPANY_TYPE_DETECTION_MAX_RESULTS = _get_int(
    "COMPANY_TYPE_DETECTION_MAX_RESULTS",
    5,
)
COMPANY_TYPE_DETECTION_MIN_CONFIDENCE = _get_float(
    "COMPANY_TYPE_DETECTION_MIN_CONFIDENCE",
    0.55,
)
COMPANY_TYPE_DETECTION_PERPLEXITY_MODEL = os.getenv(
    "COMPANY_TYPE_DETECTION_PERPLEXITY_MODEL",
    "sonar-pro",
).strip()
COMPANY_TYPE_DETECTION_MAX_OUTPUT_TOKENS = _get_int(
    "COMPANY_TYPE_DETECTION_MAX_OUTPUT_TOKENS",
    320,
)

# Council execution mode
# - local: existing Stage1 responses via OpenRouter
# - perplexity_emulated: Stage1 responses are per-model Perplexity deep-research runs
COUNCIL_EXECUTION_MODE = os.getenv("COUNCIL_EXECUTION_MODE", "local").strip().lower()

# Model list for Perplexity emulated council stage 1.
# Falls back to COUNCIL_MODELS if not explicitly set.
# Note: Perplexity model IDs can differ from OpenRouter IDs (e.g. xai/... vs x-ai/...).
_PERPLEXITY_COUNCIL_MODELS = _get_csv("PERPLEXITY_COUNCIL_MODELS")
PERPLEXITY_COUNCIL_MODELS = _PERPLEXITY_COUNCIL_MODELS or [
    "openai/gpt-5.4",
    "google/gemini-3.1-pro-preview",
    "anthropic/claude-sonnet-4-6",
]
PERPLEXITY_STAGE1_MIXED_MODE_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_MIXED_MODE_ENABLED",
    default=False,
)
PERPLEXITY_STAGE1_OPENROUTER_MODELS = _get_csv("PERPLEXITY_STAGE1_OPENROUTER_MODELS")
PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_MODEL_PREFLIGHT_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS = _get_float(
    "PERPLEXITY_STAGE1_MODEL_PREFLIGHT_TIMEOUT_SECONDS",
    30.0,
)
PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN = _get_bool(
    "PERPLEXITY_STAGE1_MODEL_PREFLIGHT_FAIL_OPEN",
    default=True,
)

# Tavily Search API
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
ENABLE_SEARCH_BY_DEFAULT = True
MAX_SEARCH_RESULTS = 5

# xAI API (supplementary sector news brief lane)
XAI_API_KEY = os.getenv("XAI_API_KEY")
XAI_API_URL = os.getenv(
    "XAI_API_URL",
    "https://api.x.ai/v1/responses",
).strip()
STAGE1_SUPPLEMENTARY_XAI_MODEL = os.getenv(
    "STAGE1_SUPPLEMENTARY_XAI_MODEL",
    "grok-4-1-fast-reasoning",
).strip()
STAGE1_SUPPLEMENTARY_XAI_TIMEOUT_SECONDS = _get_float(
    "STAGE1_SUPPLEMENTARY_XAI_TIMEOUT_SECONDS",
    90.0,
)
STAGE1_SUPPLEMENTARY_XAI_MAX_TOKENS = _get_int(
    "STAGE1_SUPPLEMENTARY_XAI_MAX_TOKENS",
    700,
)
STAGE1_SUPPLEMENTARY_XAI_TEMPERATURE = _get_float(
    "STAGE1_SUPPLEMENTARY_XAI_TEMPERATURE",
    0.2,
)
STAGE1_SUPPLEMENTARY_XAI_MAX_TOOL_ITERATIONS = _get_int(
    "STAGE1_SUPPLEMENTARY_XAI_MAX_TOOL_ITERATIONS",
    6,
)

# Perplexity API
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
PERPLEXITY_API_URL = os.getenv(
    "PERPLEXITY_API_URL",
    "https://api.perplexity.ai/v1/responses",
)
PERPLEXITY_MODEL = os.getenv("PERPLEXITY_MODEL", "openai/gpt-5.4")
PERPLEXITY_PRESET = os.getenv("PERPLEXITY_PRESET", "deep-research")
PERPLEXITY_PRESET_STRATEGY = os.getenv(
    "PERPLEXITY_PRESET_STRATEGY",
    "adaptive",
).strip().lower()
PERPLEXITY_PRESET_DEEP = os.getenv("PERPLEXITY_PRESET_DEEP", "deep-research").strip()
PERPLEXITY_PRESET_ADVANCED = os.getenv(
    "PERPLEXITY_PRESET_ADVANCED",
    "advanced-deep-research",
).strip()
PERPLEXITY_MINING_ENRICHER_PRESET = os.getenv(
    "PERPLEXITY_MINING_ENRICHER_PRESET",
    PERPLEXITY_PRESET_DEEP or "deep-research",
).strip()
PERPLEXITY_MINING_ENRICHER_REPAIR_PRESET = os.getenv(
    "PERPLEXITY_MINING_ENRICHER_REPAIR_PRESET",
    PERPLEXITY_PRESET_ADVANCED or "advanced-deep-research",
).strip()
PERPLEXITY_MINING_ENRICHER_MAX_PRIORITY_SOURCES = _get_int(
    "PERPLEXITY_MINING_ENRICHER_MAX_PRIORITY_SOURCES",
    18,
)
PERPLEXITY_MINING_ENRICHER_ENABLE_TARGETED_REPAIRS = _get_bool(
    "PERPLEXITY_MINING_ENRICHER_ENABLE_TARGETED_REPAIRS",
    default=True,
)
PERPLEXITY_MINING_ENRICHER_REASONING_EFFORT = os.getenv(
    "PERPLEXITY_MINING_ENRICHER_REASONING_EFFORT",
    "low",
).strip().lower()
PERPLEXITY_MINING_CONTAMINATION_CHECKER_ENABLED = _get_bool(
    "PERPLEXITY_MINING_CONTAMINATION_CHECKER_ENABLED",
    default=True,
)
PERPLEXITY_MINING_CONTAMINATION_CHECKER_MODEL = os.getenv(
    "PERPLEXITY_MINING_CONTAMINATION_CHECKER_MODEL",
    "google/gemini-3.1-flash-lite-preview",
).strip()
PERPLEXITY_MINING_CONTAMINATION_CHECKER_TIMEOUT_SECONDS = _get_float(
    "PERPLEXITY_MINING_CONTAMINATION_CHECKER_TIMEOUT_SECONDS",
    45.0,
)
PERPLEXITY_MINING_CONTAMINATION_CHECKER_MAX_OUTPUT_TOKENS = _get_int(
    "PERPLEXITY_MINING_CONTAMINATION_CHECKER_MAX_OUTPUT_TOKENS",
    2200,
)
PERPLEXITY_MINING_CONTAMINATION_CHECKER_REASONING_EFFORT = os.getenv(
    "PERPLEXITY_MINING_CONTAMINATION_CHECKER_REASONING_EFFORT",
    "low",
).strip().lower()
PERPLEXITY_MINING_CONTAMINATION_MIN_CONFIDENCE_PCT = _get_float(
    "PERPLEXITY_MINING_CONTAMINATION_MIN_CONFIDENCE_PCT",
    95.0,
)
SUPPLEMENTARY_API_PIPELINES_ENABLED = _get_bool(
    "SUPPLEMENTARY_API_PIPELINES_ENABLED",
    default=False,
)
PERPLEXITY_STREAM_ENABLED = _get_bool("PERPLEXITY_STREAM_ENABLED", default=True)
PERPLEXITY_SEARCH_MODE = os.getenv(
    "PERPLEXITY_SEARCH_MODE",
    "standard",
).strip().lower()
PERPLEXITY_USE_LEGACY_TOOL_FILTER_FALLBACK = _get_bool(
    "PERPLEXITY_USE_LEGACY_TOOL_FILTER_FALLBACK",
    default=True,
)
PERPLEXITY_TIMEOUT_SECONDS = _get_float("PERPLEXITY_TIMEOUT_SECONDS", 180.0)
PERPLEXITY_MAX_STEPS = _get_int("PERPLEXITY_MAX_STEPS", 10)
PERPLEXITY_MAX_OUTPUT_TOKENS = _get_int("PERPLEXITY_MAX_OUTPUT_TOKENS", 0)
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
# Attachment-context cap for Stage 1.
# 0 = no truncation (provider/model limits still apply server-side).
PERPLEXITY_STAGE1_ATTACHMENT_CONTEXT_MAX_CHARS = _get_int(
    "PERPLEXITY_STAGE1_ATTACHMENT_CONTEXT_MAX_CHARS",
    0,
)
# Multi-wave retrieval orchestration (planner + gap-filling waves)
PERPLEXITY_STAGE1_MULTI_WAVE_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_MULTI_WAVE_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_MULTI_WAVE_MAX_WAVES = _get_int(
    "PERPLEXITY_STAGE1_MULTI_WAVE_MAX_WAVES",
    3,
)
PERPLEXITY_STAGE1_MULTI_WAVE_GAP_QUERY_LIMIT = _get_int(
    "PERPLEXITY_STAGE1_MULTI_WAVE_GAP_QUERY_LIMIT",
    3,
)
PERPLEXITY_STAGE1_MULTI_WAVE_MIN_NEW_PRIMARY_SOURCES = _get_int(
    "PERPLEXITY_STAGE1_MULTI_WAVE_MIN_NEW_PRIMARY_SOURCES",
    1,
)
PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED = _get_bool(
    "PERPLEXITY_STAGE1_SONAR_MULTISTEP_REQUIRED",
    default=True,
)
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
    300.0,
)
PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_MAX_ATTEMPTS",
    3,
)
PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS = _get_float(
    "PERPLEXITY_STAGE1_SECOND_PASS_RETRY_BACKOFF_SECONDS",
    4.0,
)
PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_MAX_SOURCES",
    7,
)
PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_MAX_CHARS_PER_SOURCE",
    1200,
)
PERPLEXITY_STAGE1_SECOND_PASS_MAX_OUTPUT_TOKENS = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_MAX_OUTPUT_TOKENS",
    0,
)
PERPLEXITY_STAGE1_SECOND_PASS_REASONING_EFFORT = os.getenv(
    "PERPLEXITY_STAGE1_SECOND_PASS_REASONING_EFFORT",
    "high",
).strip().lower()
PERPLEXITY_STAGE1_SECOND_PASS_APPENDIX_MAX_SOURCES = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_APPENDIX_MAX_SOURCES",
    3,
)
PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_COMPRESSION_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_COMPRESSION_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_TARGET_CHARS = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_PROMPT_TARGET_CHARS",
    32000,
)
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_PER_SOURCE = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_PER_SOURCE",
    8,
)
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_WORDS_PER_SOURCE = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_WORDS_PER_SOURCE",
    500,
)
PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_DOC_KEYPOINTS_MAX_FACT_CHARS",
    420,
)
STAGE1_CASHFLOW_DETECTION_MAX_SOURCES = _get_int(
    "STAGE1_CASHFLOW_DETECTION_MAX_SOURCES",
    24,
)
STAGE1_CASHFLOW_CLASSIFIER_ENABLED = _get_bool(
    "STAGE1_CASHFLOW_CLASSIFIER_ENABLED",
    default=True,
)
STAGE1_CASHFLOW_CLASSIFIER_MODEL = os.getenv(
    "STAGE1_CASHFLOW_CLASSIFIER_MODEL",
    "google/gemini-3-flash-preview",
).strip()
STAGE1_CASHFLOW_CLASSIFIER_TIMEOUT_SECONDS = _get_float(
    "STAGE1_CASHFLOW_CLASSIFIER_TIMEOUT_SECONDS",
    35.0,
)
STAGE1_CASHFLOW_CLASSIFIER_MAX_OUTPUT_TOKENS = _get_int(
    "STAGE1_CASHFLOW_CLASSIFIER_MAX_OUTPUT_TOKENS",
    260,
)
STAGE1_CASHFLOW_CLASSIFIER_REASONING_EFFORT = os.getenv(
    "STAGE1_CASHFLOW_CLASSIFIER_REASONING_EFFORT",
    "low",
).strip().lower()
STAGE1_CASHFLOW_CLASSIFIER_MIN_CONFIDENCE_PCT = _get_float(
    "STAGE1_CASHFLOW_CLASSIFIER_MIN_CONFIDENCE_PCT",
    70.0,
)
PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_SOURCES = _get_int(
    "PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_SOURCES",
    3,
)
PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_RETRIEVAL_MAX_SOURCES = _get_int(
    "PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_RETRIEVAL_MAX_SOURCES",
    8,
)
PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_RECENCY_DAYS = _get_int(
    "PERPLEXITY_STAGE1_SUPPLEMENTARY_NEWS_MAX_RECENCY_DAYS",
    180,
)
PERPLEXITY_STAGE1_TIMELINE_GUARD_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_TIMELINE_GUARD_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_TIMELINE_GUARD_HARD_FAIL = _get_bool(
    "PERPLEXITY_STAGE1_TIMELINE_GUARD_HARD_FAIL",
    default=True,
)
PERPLEXITY_STAGE1_TIMELINE_DIGEST_MAX_ITEMS = _get_int(
    "PERPLEXITY_STAGE1_TIMELINE_DIGEST_MAX_ITEMS",
    8,
)
PERPLEXITY_STAGE1_FACT_DIGEST_V2_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_FACT_DIGEST_V2_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_FACTS_PER_SECTION = _get_int(
    "PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_FACTS_PER_SECTION",
    6,
)
PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_SUMMARY_BULLETS = _get_int(
    "PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_SUMMARY_BULLETS",
    12,
)
PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_NARRATIVE_WORDS = _get_int(
    "PERPLEXITY_STAGE1_FACT_DIGEST_V2_MAX_NARRATIVE_WORDS",
    500,
)
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_SECOND_PASS_CITATION_GATE_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MIN_COUNT = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MIN_COUNT",
    2,
)
PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MAX_UNCITED_NUMERIC_LINES = _get_int(
    "PERPLEXITY_STAGE1_SECOND_PASS_CITATION_MAX_UNCITED_NUMERIC_LINES",
    2,
)
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE = _get_float(
    "PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_SCORE",
    0.75,
)
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT = _get_float(
    "PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_RUBRIC_COVERAGE_PCT",
    0.60,
)
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT = _get_float(
    "PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_MIN_NUMERIC_CITATION_PCT",
    0.60,
)
PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_CATASTROPHIC_SCORE = _get_float(
    "PERPLEXITY_STAGE1_SECOND_PASS_COMPLIANCE_CATASTROPHIC_SCORE",
    0.35,
)
PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED = _get_bool(
    "PERPLEXITY_STAGE1_SHARED_RETRIEVAL_ENABLED",
    default=True,
)
PERPLEXITY_STAGE1_SHARED_RETRIEVAL_MODEL = os.getenv(
    "PERPLEXITY_STAGE1_SHARED_RETRIEVAL_MODEL",
    "",
).strip()
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
    "",
).strip().lower()
PERPLEXITY_STAGE1_OPENAI_BASE_DOWNGRADE_HIGH_REASONING = _get_bool(
    "PERPLEXITY_STAGE1_OPENAI_BASE_DOWNGRADE_HIGH_REASONING",
    default=False,
)
STAGE2_REVISION_PASS_ENABLED = _get_bool(
    "STAGE2_REVISION_PASS_ENABLED",
    default=True,
)
STAGE2_REVISION_PASS_TIMEOUT_SECONDS = _get_float(
    "STAGE2_REVISION_PASS_TIMEOUT_SECONDS",
    120.0,
)
STAGE2_REVISION_PASS_MAX_OUTPUT_TOKENS = _get_int(
    "STAGE2_REVISION_PASS_MAX_OUTPUT_TOKENS",
    1200,
)
STAGE2_RECONCILIATION_ENABLED = _get_bool(
    "STAGE2_RECONCILIATION_ENABLED",
    default=True,
)
STAGE2_RECONCILIATION_MODEL = os.getenv(
    "STAGE2_RECONCILIATION_MODEL",
    CHAIRMAN_JSONIFIER_MODEL,
).strip()
STAGE2_RECONCILIATION_TIMEOUT_SECONDS = _get_float(
    "STAGE2_RECONCILIATION_TIMEOUT_SECONDS",
    180.0,
)
STAGE2_RECONCILIATION_MAX_OUTPUT_TOKENS = _get_int(
    "STAGE2_RECONCILIATION_MAX_OUTPUT_TOKENS",
    4000,
)
STAGE2_RECONCILIATION_MAX_SOURCE_CHARS = _get_int(
    "STAGE2_RECONCILIATION_MAX_SOURCE_CHARS",
    24000,
)
STAGE2_RECONCILIATION_MAX_RESPONSE_CHARS = _get_int(
    "STAGE2_RECONCILIATION_MAX_RESPONSE_CHARS",
    6000,
)
STAGE2_RECONCILIATION_TOP_N = _get_int(
    "STAGE2_RECONCILIATION_TOP_N",
    0,
)
ENABLE_SOURCE_DECODING = _get_bool("ENABLE_SOURCE_DECODING", default=True)
SOURCE_DECODING_MAX_PER_MODEL = _get_int("SOURCE_DECODING_MAX_PER_MODEL", 10)
SOURCE_DECODING_MAX_CHARS = _get_int("SOURCE_DECODING_MAX_CHARS", 3500)
SOURCE_DECODING_TIMEOUT_SECONDS = _get_float("SOURCE_DECODING_TIMEOUT_SECONDS", 20.0)
DETERMINISTIC_FINANCE_LANE_ENABLED = _get_bool("DETERMINISTIC_FINANCE_LANE_ENABLED", default=True)

# PDF Processing
ATTACHMENTS_DIR = "data/attachments"
MAX_PDF_SIZE_MB = 50
MAX_PDF_PAGES_FULL_TEXT = 20
