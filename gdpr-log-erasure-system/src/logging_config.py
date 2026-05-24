"""Structured-logging bootstrap.

Configures stdlib ``logging`` to emit through structlog's JSON renderer
so every log line is a single JSON object (timestamps in ISO-8601 UTC,
log level, message, plus any context bound via
``structlog.contextvars``). This is the only place log formatting is
configured; everything else just calls ``get_logger`` and binds context.
"""
import logging
import sys
import structlog


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=getattr(logging, level.upper(), logging.INFO))
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper(), logging.INFO)),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None):
    return structlog.get_logger(name)
