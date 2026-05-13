"""Structlog setup + request-id middleware.

Wires a JSON renderer + ISO timestamp + merged contextvars so every log
line emitted through structlog carries the active request_id (stamped by
:func:`request_id_middleware`) plus any other contextvars the caller has
bound on the current async task.

Two public functions:

- :func:`configure_logging` — call once at app startup (before any
  ``structlog.get_logger`` use). Idempotent enough that calling twice
  with different levels just re-binds the wrapper class.
- :func:`request_id_middleware` — FastAPI HTTP middleware that mints a
  UUID per request (or honors the ``X-Request-Id`` header if a caller
  supplies one), binds ``request_id`` + ``path`` to the structlog
  contextvars for the lifetime of the request, and echoes the id back
  on the response so traces line up across hops.
"""

from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import Awaitable, Callable

import structlog
from fastapi import Request, Response


def configure_logging(level: str = "INFO") -> None:
    """Wire structlog with a JSON renderer + ISO timestamp + log level.

    Processors (in order):
        1. ``merge_contextvars`` — pull anything bound on the current
           task (e.g. ``request_id``, ``path``) into the event dict.
        2. ``add_log_level`` — surface the level as a JSON field.
        3. ``TimeStamper(iso, utc=True)`` — wall-clock timestamp.
        4. ``StackInfoRenderer`` + ``format_exc_info`` — surface tracebacks.
        5. ``JSONRenderer`` — emit a single-line JSON object on stdout.

    The filtering bound logger is rebuilt every call so a later
    invocation with a different level actually takes effect.
    """
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        timestamper,
    ]
    structlog.configure(
        processors=shared_processors
        + [
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


async def request_id_middleware(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Stamp every request with a UUID and surface it on the contextvars.

    Honors an inbound ``X-Request-Id`` header so the caller can correlate
    requests across services; otherwise mints a fresh uuid4 hex. The id
    is echoed on the response as ``X-Request-Id`` and cleared from the
    contextvars on the way out so it cannot leak between requests
    sharing the same asyncio task pool.
    """
    request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
    structlog.contextvars.bind_contextvars(
        request_id=request_id, path=str(request.url.path)
    )
    try:
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        return response
    finally:
        structlog.contextvars.clear_contextvars()
