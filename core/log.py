"""
core/log.py
───────────
Structured JSON logging via structlog.

Usage:
    from core.log import get_logger
    log = get_logger(__name__)
    log.info("job_picked_up", ticket_id="abc123", retry_count=1)
    log.error("grade_failed", ticket_id="abc123", exc_info=True)

Environment variables:
    LOG_LEVEL   (default: INFO)   DEBUG | INFO | WARNING | ERROR
"""

import logging
import os
import sys

import structlog


def configure_logging() -> None:
    """Configure structlog for JSON output.  Safe to call multiple times."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level      = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        format = "%(message)s",
        stream = sys.stdout,
        level  = level,
    )

    structlog.configure(
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class  = structlog.stdlib.BoundLogger,
        context_class  = dict,
        logger_factory = structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use = True,
    )


_configured = False


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger, configuring logging on first call."""
    global _configured
    if not _configured:
        configure_logging()
        _configured = True
    return structlog.get_logger(name)
