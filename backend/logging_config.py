"""Shared logging configuration for the LLM Council backend.

Call configure_logging() once at process startup (in main.py lifespan).
All other modules obtain a logger via:

    import logging
    logger = logging.getLogger(__name__)

The LOG_LEVEL env var controls verbosity (default INFO).
The PROGRESS_LOGGING env var (bool) enables DEBUG-level stage progress messages.
"""

import logging
import os


def configure_logging() -> None:
    """Configure root logger with a timestamped format and env-driven level."""
    raw_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, raw_level, logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=level,
        force=True,   # override any earlier basicConfig calls from dependencies
    )
