"""Observability: structured logging + Prometheus metrics (C14).

This module is the single place that wires the two cross-cutting observability
concerns for the recommendation engine:

* **structlog** — :func:`configure_logging` installs a JSON (production) or
  console (dev) renderer keyed off ``settings.log_level``; :func:`get_logger`
  returns a bound logger. ``create_app`` calls ``configure_logging()`` once at
  startup.
* **prometheus-client** — **module-level** metric singletons (HTTP request
  counter + latency histogram, recommend counter + latency histogram, cache
  hit/miss counters, feedback counter, corpus-size gauge) plus a
  :class:`PrometheusMiddleware` that times every request and records it, and a
  :func:`metrics_endpoint` returning the text exposition.

Why module-level singletons?
----------------------------
The metrics are defined **once at import time**, never inside ``create_app`` or a
request handler. FastAPI's test-suite builds the app repeatedly (each test may
call ``create_app()``), and Prometheus raises ``Duplicated timeseries in
CollectorRegistry`` if the *same* metric name is registered twice. Defining them
at module scope means a single registration for the process lifetime, so any
number of ``create_app()`` calls reuse the same collectors. A **dedicated**
:class:`CollectorRegistry` (not the global default) further isolates this service's
series and keeps repeated imports / reloads clean.

Both dependencies are guarded: if ``structlog`` / ``prometheus_client`` cannot be
imported (or a metric op fails) the app still runs — observability degrades to a
no-op rather than crashing the service. This keeps the contract "observability
must never crash the app" true. The helper functions other modules call
(:func:`record_recommend`, :func:`record_cache`, :func:`record_feedback`,
:func:`observe_http`, :func:`set_corpus_size`) are all no-ops when
prometheus_client is absent and swallow any internal error.

The ``/metrics`` split (wired in :mod:`src.routers.system`)
-----------------------------------------------------------
* ``GET /metrics``      -> Prometheus **text** exposition (this module's
  :func:`metrics_endpoint`).
* ``GET /metrics/json`` -> a small JSON snapshot of the key counters/gauges for a
  dashboard that does not speak the Prometheus text format.
"""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager
from typing import Any, Iterator

# --------------------------------------------------------------------------- #
# Guarded optional dependencies
# --------------------------------------------------------------------------- #
try:  # structlog is pinned; guard so a broken install never hard-fails import.
    import structlog as _structlog
except Exception:  # pragma: no cover - structlog is a hard dependency
    _structlog = None  # type: ignore[assignment]

try:
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )

    _PROM_OK = True
except Exception:  # pragma: no cover - prometheus_client is a hard dependency
    _PROM_OK = False

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

_stdlib_logger = logging.getLogger(__name__)

#: Prometheus text-exposition content type (kept local so the endpoint answers a
#: stable ``text/plain; version=0.0.4`` even when prometheus_client is missing).
CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"


# --------------------------------------------------------------------------- #
# structlog configuration
# --------------------------------------------------------------------------- #
def configure_logging(level: str | None = None) -> None:
    """Configure structlog (and the stdlib root) once at startup.

    ``level`` defaults to ``settings.log_level``. A JSON renderer (ISO timestamp,
    level, event) is used unless the level resolves to ``DEBUG`` (then a
    human-friendly console renderer is used for local development). Never raises —
    a misconfigured logger must not stop the app from booting.
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
# Prometheus metric singletons (MODULE LEVEL — registered exactly once)
# --------------------------------------------------------------------------- #
# A dedicated registry keeps repeated imports / test reloads / repeated
# create_app() calls from raising "Duplicated timeseries" against the global
# default registry. Defining every metric here (not inside a factory/handler)
# means a single registration for the process lifetime.
if _PROM_OK:
    REGISTRY = CollectorRegistry(auto_describe=True)

    HTTP_REQUESTS_TOTAL = Counter(
        "http_requests_total",
        "Total HTTP requests, labelled by method, path template and status.",
        ["method", "path", "status"],
        registry=REGISTRY,
    )
    HTTP_REQUEST_DURATION = Histogram(
        "http_request_duration_seconds",
        "HTTP request duration in seconds, labelled by method and path template.",
        ["method", "path"],
        registry=REGISTRY,
    )
    RECOMMEND_REQUESTS_TOTAL = Counter(
        "recommend_requests_total",
        "Total /recommend pipeline invocations (cache hits + fresh computes).",
        registry=REGISTRY,
    )
    RECOMMEND_LATENCY = Histogram(
        "recommend_latency_seconds",
        "Wall-clock latency of the /recommend pipeline in seconds.",
        registry=REGISTRY,
    )
    CACHE_HITS_TOTAL = Counter(
        "cache_hits_total",
        "Cache hits, labelled by cache (embedding | recommendation).",
        ["cache"],
        registry=REGISTRY,
    )
    CACHE_MISSES_TOTAL = Counter(
        "cache_misses_total",
        "Cache misses, labelled by cache (embedding | recommendation).",
        ["cache"],
        registry=REGISTRY,
    )
    FEEDBACK_TOTAL = Counter(
        "feedback_total",
        "Feedback votes recorded, labelled by helpful (true | false).",
        ["helpful"],
        registry=REGISTRY,
    )
    CORPUS_SIZE = Gauge(
        "corpus_size",
        "Number of incidents in the corpus (best-effort; set on scrape/ingest).",
        registry=REGISTRY,
    )
else:  # pragma: no cover - exercised only if prometheus_client is missing
    REGISTRY = None
    HTTP_REQUESTS_TOTAL = HTTP_REQUEST_DURATION = None
    RECOMMEND_REQUESTS_TOTAL = RECOMMEND_LATENCY = None
    CACHE_HITS_TOTAL = CACHE_MISSES_TOTAL = None
    FEEDBACK_TOTAL = None
    CORPUS_SIZE = None


# --------------------------------------------------------------------------- #
# Recording helpers (all no-ops if prometheus_client is absent; never raise)
# --------------------------------------------------------------------------- #
def observe_http(method: str, path: str, status: str, duration: float) -> None:
    """Record one HTTP request: increment the counter and observe the duration.

    ``path`` should already be a bounded **template** (e.g. ``/incidents/{id}``),
    not a concrete path, so label cardinality stays bounded. Never raises.
    """
    if not _PROM_OK:
        return
    try:
        HTTP_REQUEST_DURATION.labels(method=method, path=path).observe(
            float(duration)
        )
        HTTP_REQUESTS_TOTAL.labels(
            method=method, path=path, status=str(status)
        ).inc()
    except Exception as exc:  # noqa: BLE001 - metrics must never break a request
        _stdlib_logger.debug("observe_http failed: %s", exc)


def record_recommend(latency_seconds: float) -> None:
    """Record one /recommend invocation: bump the counter + observe its latency."""
    if not _PROM_OK:
        return
    try:
        RECOMMEND_REQUESTS_TOTAL.inc()
        RECOMMEND_LATENCY.observe(float(latency_seconds))
    except Exception as exc:  # noqa: BLE001
        _stdlib_logger.debug("record_recommend failed: %s", exc)


def record_cache(cache: str, hit: bool) -> None:
    """Record a cache lookup outcome for ``cache`` (``"embedding"``/``"recommendation"``).

    ``hit=True`` increments ``cache_hits_total{cache=...}``; ``hit=False`` increments
    ``cache_misses_total{cache=...}``. Never raises — a metrics error must not turn a
    cache lookup into a request failure.
    """
    if not _PROM_OK:
        return
    try:
        if hit:
            CACHE_HITS_TOTAL.labels(cache=cache).inc()
        else:
            CACHE_MISSES_TOTAL.labels(cache=cache).inc()
    except Exception as exc:  # noqa: BLE001
        _stdlib_logger.debug("record_cache failed: %s", exc)


def record_feedback(helpful: bool) -> None:
    """Record one feedback vote (``feedback_total{helpful="true"|"false"}``)."""
    if not _PROM_OK:
        return
    try:
        FEEDBACK_TOTAL.labels(helpful="true" if helpful else "false").inc()
    except Exception as exc:  # noqa: BLE001
        _stdlib_logger.debug("record_feedback failed: %s", exc)


def set_corpus_size(size: int) -> None:
    """Set the ``corpus_size`` gauge (best-effort; ignores bad values / errors)."""
    if not _PROM_OK:
        return
    try:
        CORPUS_SIZE.set(int(size))
    except Exception as exc:  # noqa: BLE001
        _stdlib_logger.debug("set_corpus_size failed: %s", exc)


@contextmanager
def timer() -> Iterator[Any]:
    """Context manager yielding a zero-arg callable returning elapsed seconds.

    Usage::

        with observability.timer() as elapsed:
            ... work ...
        observability.record_recommend(elapsed())

    ``elapsed()`` may be called inside or after the block; after exit it returns the
    total wall-clock duration of the block. Uses ``time.perf_counter`` for a
    monotonic, high-resolution measurement.
    """
    start = time.perf_counter()
    end: dict[str, float] = {}

    def _elapsed() -> float:
        return (end.get("t") or time.perf_counter()) - start

    try:
        yield _elapsed
    finally:
        end["t"] = time.perf_counter()


# --------------------------------------------------------------------------- #
# Request middleware
# --------------------------------------------------------------------------- #
class PrometheusMiddleware(BaseHTTPMiddleware):
    """Time each request and record count + latency against the path *template*.

    Using the matched route's ``path_format`` (e.g. ``/incidents/{incident_id}``)
    instead of the raw URL keeps cardinality bounded — one time series per route,
    not one per distinct id. Falls back to the raw path when no route matched
    (404s). Never raises: a metrics failure is swallowed so it cannot turn a good
    response into a 500.
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
                observe_http(method, path, status, elapsed)
            except Exception as exc:  # noqa: BLE001 - never break the response
                _stdlib_logger.debug("PrometheusMiddleware record failed: %s", exc)


def _route_template(request: Request) -> str:
    """Return the matched route's path template, or the raw path on no match.

    Prefers ``route.path_format`` (Starlette's templated form,
    e.g. ``/incidents/{incident_id}``); falls back to ``route.path`` and finally the
    concrete request path. Templating here is what prevents unbounded label
    cardinality from path params like ``/incidents/123``.
    """
    route = request.scope.get("route")
    template = getattr(route, "path_format", None) or getattr(route, "path", None)
    if isinstance(template, str) and template:
        return template
    return request.url.path


# --------------------------------------------------------------------------- #
# Exposition
# --------------------------------------------------------------------------- #
def metrics_endpoint() -> Response:
    """Return a Starlette ``Response`` with the Prometheus text exposition.

    The body is the ``generate_latest`` dump of this service's dedicated registry
    with content type ``text/plain; version=0.0.4``. When prometheus_client is
    unavailable (or a scrape fails) the body is empty but the endpoint still answers
    200 with the same content type, so a scraper never sees a 5xx from ``/metrics``.
    """
    if not _PROM_OK:
        return Response(content=b"", media_type=CONTENT_TYPE_LATEST)
    try:
        body = generate_latest(REGISTRY)
    except Exception as exc:  # noqa: BLE001 - exposition must never 500
        _stdlib_logger.debug("generate_latest failed: %s", exc)
        body = b""
    return Response(content=body, media_type=CONTENT_TYPE_LATEST)


def _counter_value(metric: Any, **labels: str) -> float:
    """Best-effort read of a (possibly labelled) counter/gauge child value.

    Reads the internal ``_value`` of the child sample so the JSON snapshot can be
    built without re-parsing the text exposition. Returns ``0.0`` on any error or
    when prometheus_client is unavailable, so the snapshot degrades gracefully.
    """
    if not _PROM_OK or metric is None:
        return 0.0
    try:
        child = metric.labels(**labels) if labels else metric
        return float(child._value.get())  # noqa: SLF001 - documented internal read
    except Exception:  # noqa: BLE001 - snapshot is best-effort
        return 0.0


def metrics_snapshot() -> dict[str, Any]:
    """Return a small JSON-serialisable snapshot of the key counters/gauges.

    Shape::

        {
          "recommend_requests_total": <float>,
          "cache": {
            "embedding":      {"hits": <float>, "misses": <float>},
            "recommendation": {"hits": <float>, "misses": <float>},
          },
          "feedback": {"helpful": <float>, "unhelpful": <float>},
          "corpus_size": <float>,
          "prometheus": <bool>,     # False when prometheus_client is unavailable
        }

    Every figure is read best-effort (``0.0`` on any error), so this never raises
    and is safe to serve from ``GET /metrics/json`` regardless of dependency state.
    """
    return {
        "recommend_requests_total": _counter_value(RECOMMEND_REQUESTS_TOTAL),
        "cache": {
            "embedding": {
                "hits": _counter_value(CACHE_HITS_TOTAL, cache="embedding"),
                "misses": _counter_value(CACHE_MISSES_TOTAL, cache="embedding"),
            },
            "recommendation": {
                "hits": _counter_value(CACHE_HITS_TOTAL, cache="recommendation"),
                "misses": _counter_value(
                    CACHE_MISSES_TOTAL, cache="recommendation"
                ),
            },
        },
        "feedback": {
            "helpful": _counter_value(FEEDBACK_TOTAL, helpful="true"),
            "unhelpful": _counter_value(FEEDBACK_TOTAL, helpful="false"),
        },
        "corpus_size": _counter_value(CORPUS_SIZE),
        "prometheus": bool(_PROM_OK),
    }


__all__ = [
    "configure_logging",
    "get_logger",
    "PrometheusMiddleware",
    "metrics_endpoint",
    "metrics_snapshot",
    "observe_http",
    "record_recommend",
    "record_cache",
    "record_feedback",
    "set_corpus_size",
    "timer",
    "CONTENT_TYPE_LATEST",
]
