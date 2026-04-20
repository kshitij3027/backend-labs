"""Structured JSON logging for the real-time log indexing engine.

All modules import ``logging`` normally and rely on this module,
called once from ``main.py`` at startup, to install a JSON formatter
on the root logger. Each record becomes a single JSON object on
stdout so downstream aggregators can parse it without regex.

Kept intentionally lightweight: standard-library ``logging`` plus
``json.dumps`` — no new dependencies.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


# Attributes that ``logging.LogRecord`` sets by default. Anything NOT
# in this set was passed in via ``extra={...}`` and should be promoted
# into the JSON payload as a structured field.
_DEFAULT_LOG_RECORD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "taskName",
    "message",
    "asctime",
}


class _JsonFormatter(logging.Formatter):
    """Render a ``LogRecord`` as a single JSON line.

    Core fields (timestamp, level, logger, message) are always
    emitted. Any ``extra={...}`` kwargs the caller passed in become
    top-level keys under ``extra`` so we don't collide with logging's
    reserved names.
    """

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        payload: dict[str, object] = {
            "timestamp": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Promote caller-supplied extras into an "extra" sub-object so
        # the line stays readable and reserved fields are not shadowed.
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _DEFAULT_LOG_RECORD_ATTRS and not k.startswith("_")
        }
        if extras:
            payload["extra"] = extras

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


def setup_logging(level: str) -> None:
    """Configure the root logger for the whole process.

    Safe to call multiple times — existing handlers on the root logger
    are dropped and replaced so we don't double-emit every record.
    The level is matched case-insensitively; unknown strings fall
    back to INFO.
    """

    numeric_level = logging.getLevelName(level.upper())
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root.addHandler(handler)
    root.setLevel(numeric_level)

    # Tame very chatty third-party loggers by default so the JSON
    # feed stays useful. Callers can override later with getLogger().
    logging.getLogger("uvicorn.access").setLevel(max(numeric_level, logging.INFO))
    logging.getLogger("httpx").setLevel(max(numeric_level, logging.WARNING))

    # Anchor logger name used across the project so tests can
    # introspect via ``logging.getLogger("real_time_log_indexing")``.
    logging.getLogger("real_time_log_indexing").setLevel(numeric_level)
