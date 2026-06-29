"""Observability: structured logging (+ Prometheus metrics later).

C1 keeps this deliberately minimal but importable: the single job here is
:func:`configure_logging`, which ``create_app`` calls once at startup. The full
Prometheus metric singletons + request middleware arrive in C14; until then this
module intentionally exposes only the logging surface so ``src.api`` can import it
without dragging in the metrics machinery.

Both optional dependencies (``structlog`` / ``prometheus_client``) are imported
under a guard: if either cannot be imported the app still runs — observability
degrades to a no-op rather than crashing the service. This keeps the contract
"observability must never crash the app" true from the very first commit.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

# --------------------------------------------------------------------------- #
# Guarded optional dependencies
# --------------------------------------------------------------------------- #
try:  # structlog is pinned; guard so a broken install never hard-fails import.
    import structlog as _structlog
except Exception:  # pragma: no cover - structlog is a hard dependency
    _structlog = None  # type: ignore[assignment]

try:  # prometheus_client is pinned; full metrics wiring lands in C14.
    import prometheus_client as _prometheus_client  # noqa: F401

    _PROM_OK = True
except Exception:  # pragma: no cover - prometheus_client is a hard dependency
    _PROM_OK = False

_stdlib_logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# structlog configuration
# --------------------------------------------------------------------------- #
def configure_logging(level: str | None = None) -> None:
    """Configure structlog (and the stdlib root) once at startup.

    ``level`` defaults to ``settings.log_level``. A JSON renderer is used unless
    the level resolves to ``DEBUG`` (then a human-friendly console renderer is
    used for local development). Never raises — a misconfigured logger must not
    stop the app from booting.
    """
    if level is None:
        try:
            from src.config import get_settings

            level = get_settings().log_level
        except Exception:  # noqa: BLE001
            level = "INFO"
    level_name = str(level or "INFO").upper()
    numeric = getattr(logging, level_name, logging.INFO)

    # Always set the stdlib root level (uvicorn/SQLAlchemy/etc. flow through it).
    logging.basicConfig(level=numeric, stream=sys.stdout, format="%(message)s")

    if _structlog is None:  # pragma: no cover - structlog pinned
        _stdlib_logger.warning("structlog unavailable; using stdlib logging only")
        return

    try:
        renderer = (
            _structlog.dev.ConsoleRenderer()
            if numeric <= logging.DEBUG
            else _structlog.processors.JSONRenderer()
        )
        _structlog.configure(
            processors=[
                _structlog.contextvars.merge_contextvars,
                _structlog.processors.add_log_level,
                _structlog.processors.TimeStamper(fmt="iso", utc=True),
                _structlog.processors.StackInfoRenderer(),
                _structlog.processors.format_exc_info,
                renderer,
            ],
            wrapper_class=_structlog.make_filtering_bound_logger(numeric),
            logger_factory=_structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
    except Exception as exc:  # noqa: BLE001 - logging setup must never crash boot
        _stdlib_logger.warning("structlog configuration failed: %s", exc)


def get_logger(name: str | None = None) -> Any:
    """Return a bound structlog logger (or a stdlib logger if structlog is absent)."""
    if _structlog is None:  # pragma: no cover
        return logging.getLogger(name or __name__)
    try:
        return _structlog.get_logger(name or __name__)
    except Exception:  # noqa: BLE001
        return logging.getLogger(name or __name__)


__all__ = ["configure_logging", "get_logger"]
