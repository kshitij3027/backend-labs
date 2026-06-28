"""Observability: structured logging + Prometheus metrics (C11).

This module is the single place that wires the two cross-cutting observability
concerns named in the build plan:

* **structlog** — :func:`configure_logging` installs a JSON (production) or
  console (dev) renderer keyed off ``settings.log_level``; :func:`get_logger`
  returns a bound logger. ``create_app`` calls ``configure_logging()`` once at
  startup.
* **prometheus-client** — module-level metric singletons (request counter +
  latency histogram, predictions-generated counter, computation-time histogram,
  deployed-model gauge, last-confidence gauge) plus a
  :class:`PrometheusMiddleware` that times every request and records it, and a
  :func:`metrics_endpoint` returning the text exposition.

Both dependencies are guarded: if ``structlog`` / ``prometheus_client`` cannot be
imported (or a metric op fails) the app still runs — observability degrades to a
no-op rather than crashing the service. This keeps the contract "observability
must never crash the app".

The ``/metrics`` path split (resolved in :mod:`src.api`)
-------------------------------------------------------
``project_requirements.md`` defines ``GET /metrics`` as the *application* metrics
endpoint (prediction accuracy, processing times, resource usage) in JSON — which
collides with the conventional Prometheus *text* exposition and with the existing
data route ``GET /metrics/{metric_name}``. The resolution:

* ``GET /metrics``            -> application metrics **JSON** (the requirement).
* ``GET /metrics/prometheus`` -> Prometheus **text** exposition (this module's
  :func:`metrics_endpoint`).
* ``GET /metrics/{metric_name}`` -> unchanged data read (the existing router).

These do not structurally collide in FastAPI (``/metrics`` is a bare path;
``/metrics/prometheus`` is a literal that is declared before the
``/metrics/{metric_name}`` parameter route so it always wins).
"""

from __future__ import annotations

import logging
import os
import resource
import sys
import time
from typing import Any

# --------------------------------------------------------------------------- #
# Guarded optional dependencies
# --------------------------------------------------------------------------- #
try:  # structlog is pinned; guard so a broken install never hard-fails import.
    import structlog as _structlog
except Exception:  # pragma: no cover - structlog is a hard dependency
    _structlog = None  # type: ignore[assignment]

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )

    _PROM_OK = True
except Exception:  # pragma: no cover - prometheus_client is a hard dependency
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    _PROM_OK = False

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

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


# --------------------------------------------------------------------------- #
# Prometheus metric singletons
# --------------------------------------------------------------------------- #
# A dedicated registry keeps repeated imports / test reloads from raising
# "Duplicated timeseries" against the global default registry.
if _PROM_OK:
    REGISTRY = CollectorRegistry(auto_describe=True)

    REQUEST_COUNT = Counter(
        "lfe_api_requests_total",
        "Total API requests, labelled by method, path template and status.",
        ["method", "path", "status"],
        registry=REGISTRY,
    )
    REQUEST_LATENCY = Histogram(
        "lfe_api_request_latency_seconds",
        "API request latency in seconds, labelled by method and path template.",
        ["method", "path"],
        registry=REGISTRY,
    )
    PREDICTIONS_TOTAL = Counter(
        "lfe_predictions_generated_total",
        "Forecasts generated on demand, labelled by metric and alert level.",
        ["metric", "alert_level"],
        registry=REGISTRY,
    )
    PREDICTION_COMPUTE_SECONDS = Histogram(
        "lfe_prediction_compute_seconds",
        "Wall-clock time to compute a forecast on demand.",
        ["metric"],
        registry=REGISTRY,
    )
    DEPLOYED_MODELS = Gauge(
        "lfe_deployed_models",
        "Number of currently deployed ensemble member models.",
        registry=REGISTRY,
    )
    LAST_CONFIDENCE = Gauge(
        "lfe_last_prediction_confidence",
        "Aggregate confidence of the most recently generated forecast.",
        ["metric"],
        registry=REGISTRY,
    )
else:  # pragma: no cover - exercised only if prometheus_client is missing
    REGISTRY = None
    REQUEST_COUNT = REQUEST_LATENCY = None
    PREDICTIONS_TOTAL = PREDICTION_COMPUTE_SECONDS = None
    DEPLOYED_MODELS = LAST_CONFIDENCE = None


# In-memory ring of recent compute durations (seconds) so the application-metrics
# JSON can report processing times without scraping the histogram internals.
_RECENT_COMPUTE_MS: list[float] = []
_RECENT_COMPUTE_CAP = 200


def record_prediction(metric: str, alert_level: str, confidence: float) -> None:
    """Record that a forecast was generated (counter + last-confidence gauge)."""
    if not _PROM_OK:
        return
    try:
        PREDICTIONS_TOTAL.labels(metric=metric, alert_level=str(alert_level)).inc()
        LAST_CONFIDENCE.labels(metric=metric).set(float(confidence))
    except Exception as exc:  # noqa: BLE001 - metrics must never break a request
        _stdlib_logger.debug("record_prediction failed: %s", exc)


def observe_compute_seconds(metric: str, seconds: float) -> None:
    """Observe an on-demand forecast compute duration (histogram + recent ring)."""
    try:
        seconds = float(seconds)
    except (TypeError, ValueError):
        return
    if _PROM_OK:
        try:
            PREDICTION_COMPUTE_SECONDS.labels(metric=metric).observe(seconds)
        except Exception as exc:  # noqa: BLE001
            _stdlib_logger.debug("observe_compute_seconds failed: %s", exc)
    _RECENT_COMPUTE_MS.append(seconds * 1000.0)
    if len(_RECENT_COMPUTE_MS) > _RECENT_COMPUTE_CAP:
        del _RECENT_COMPUTE_MS[: len(_RECENT_COMPUTE_MS) - _RECENT_COMPUTE_CAP]


def set_deployed_models(count: int) -> None:
    """Set the deployed-model-count gauge."""
    if not _PROM_OK:
        return
    try:
        DEPLOYED_MODELS.set(int(count))
    except Exception as exc:  # noqa: BLE001
        _stdlib_logger.debug("set_deployed_models failed: %s", exc)


def recent_compute_ms() -> list[float]:
    """Return a copy of the recent on-demand compute durations (milliseconds)."""
    return list(_RECENT_COMPUTE_MS)


# --------------------------------------------------------------------------- #
# Request middleware
# --------------------------------------------------------------------------- #
class PrometheusMiddleware(BaseHTTPMiddleware):
    """Time each request and record count + latency against the path *template*.

    Using the matched route's ``path_format`` (e.g. ``/metrics/{metric_name}``)
    instead of the raw URL keeps cardinality bounded — one time series per route,
    not per distinct metric name. Falls back to the raw path when no route
    matched (404s). Never raises: a metrics failure is swallowed so it cannot
    turn a good response into a 500.
    """

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        start = time.perf_counter()
        response: Response | None = None
        try:
            response = await call_next(request)
            return response
        finally:
            try:
                elapsed = time.perf_counter() - start
                path = _route_template(request)
                method = request.method
                status = str(response.status_code if response is not None else 500)
                if _PROM_OK:
                    REQUEST_LATENCY.labels(method=method, path=path).observe(elapsed)
                    REQUEST_COUNT.labels(
                        method=method, path=path, status=status
                    ).inc()
            except Exception as exc:  # noqa: BLE001 - never break the response
                _stdlib_logger.debug("PrometheusMiddleware record failed: %s", exc)


def _route_template(request: Request) -> str:
    """Return the matched route's path template, or the raw path on no match."""
    route = request.scope.get("route")
    template = getattr(route, "path_format", None) or getattr(route, "path", None)
    if isinstance(template, str) and template:
        return template
    return request.url.path


# --------------------------------------------------------------------------- #
# Exposition + resource usage
# --------------------------------------------------------------------------- #
def metrics_endpoint() -> tuple[bytes, str]:
    """Return ``(body, content_type)`` for the Prometheus text exposition.

    Body is empty (with the standard content type) when prometheus_client is
    unavailable, so the endpoint still answers 200.
    """
    if not _PROM_OK:
        return b"", CONTENT_TYPE_LATEST
    try:
        return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
    except Exception as exc:  # noqa: BLE001
        _stdlib_logger.debug("generate_latest failed: %s", exc)
        return b"", CONTENT_TYPE_LATEST


def resource_usage() -> dict[str, Any]:
    """Best-effort, cross-platform process resource snapshot.

    Uses the stdlib ``resource`` module (no psutil dependency). ``ru_maxrss`` is
    in **kilobytes on Linux** but **bytes on macOS**, so we normalise to MB with a
    platform check. CPU times come from the same struct. Every field is guarded;
    on any failure the snapshot degrades to ``None`` values rather than raising.
    """
    snap: dict[str, Any] = {
        "rss_mb": None,
        "cpu_user_seconds": None,
        "cpu_system_seconds": None,
        "pid": None,
    }
    try:
        snap["pid"] = os.getpid()
    except Exception:  # noqa: BLE001
        pass
    try:
        ru = resource.getrusage(resource.RUSAGE_SELF)
        maxrss = float(ru.ru_maxrss)
        # macOS reports bytes; Linux reports kilobytes.
        divisor = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
        snap["rss_mb"] = round(maxrss / divisor, 2)
        snap["cpu_user_seconds"] = round(float(ru.ru_utime), 4)
        snap["cpu_system_seconds"] = round(float(ru.ru_stime), 4)
    except Exception as exc:  # noqa: BLE001 - resource is best-effort
        _stdlib_logger.debug("resource_usage failed: %s", exc)
    return snap


__all__ = [
    "configure_logging",
    "get_logger",
    "PrometheusMiddleware",
    "metrics_endpoint",
    "record_prediction",
    "observe_compute_seconds",
    "set_deployed_models",
    "recent_compute_ms",
    "resource_usage",
    "CONTENT_TYPE_LATEST",
]
