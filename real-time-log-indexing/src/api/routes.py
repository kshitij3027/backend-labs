"""HTTP routes for health and stats.

The router here is mounted onto the FastAPI app in :mod:`src.main`.
Only ``/health`` and ``/api/stats`` ship in Commit 7. ``/api/search``,
``/api/generate-sample``, and the WebSocket endpoint land in Commits
8, 9, and 11 respectively and will be added to this module without
breaking the existing surface.

Design notes
------------

* Endpoints read their dependencies off ``request.app.state`` so the
  :mod:`src.main` lifespan fully owns construction / teardown — handlers
  never build Redis clients or indexes directly. That keeps them
  cheap, thread-safe under the single-worker uvicorn, and trivially
  swappable in tests via ``app.dependency_overrides`` or direct state
  mutation.
* ``/health`` ping latency is capped: a slow Redis ping should not
  stall the health probe. We use a short ``asyncio.wait_for`` so a
  stuck server is reported as degraded rather than never answering.
* ``/api/stats`` combines three sources — the index (write-side
  counters), the consumer (ingest-side counters), and the app state
  (uptime) — into the single :class:`StatsResponse` shape the
  dashboard plots. Throughput is a best-effort running average for
  now; a real 1-minute rolling counter lands in a later commit.
"""

from __future__ import annotations

import asyncio
import logging
import time

from fastapi import APIRouter, Request

from src.models import HealthResponse, StatsResponse


logger = logging.getLogger(__name__)


router = APIRouter()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

# Upper bound on how long we'll wait for Redis's PING to reply before
# declaring the connection degraded. Keeps ``/health`` responsive even
# when the broker is wedged — a health probe that blocks forever is
# worse than one that returns "degraded" with accurate uptime.
_REDIS_PING_TIMEOUT_S: float = 1.0


@router.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    """Liveness / readiness probe.

    Reports ``status="ok"`` only when Redis is reachable; otherwise
    ``status="degraded"`` so compose / orchestrators see a warn-but-
    live signal. ``segments_ready`` is always ``True`` once the
    lifespan has populated ``app.state.index`` — the index is usable
    from the moment :meth:`InvertedIndex.load_from_disk` returns.
    """
    redis_client = getattr(request.app.state, "redis_client", None)

    redis_ok = False
    if redis_client is not None:
        try:
            # ``ping`` is a single-RTT Redis command; cap it so a
            # stalled broker can't stall the probe.
            await asyncio.wait_for(
                redis_client.ping(), timeout=_REDIS_PING_TIMEOUT_S
            )
            redis_ok = True
        except Exception:
            # Any failure (connection refused, timeout, auth error)
            # drops us into degraded mode. We deliberately swallow
            # the exception because ``/health`` is a read-only probe
            # and must always answer with a valid response shape.
            redis_ok = False

    started_at = getattr(request.app.state, "started_at", None)
    uptime = (time.time() - started_at) if started_at is not None else 0.0
    segments_ready = getattr(request.app.state, "index", None) is not None

    status = "ok" if redis_ok and segments_ready else "degraded"

    return HealthResponse(
        status=status,
        redis_connected=redis_ok,
        segments_ready=segments_ready,
        uptime_s=uptime,
    )


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@router.get("/api/stats", response_model=StatsResponse)
async def api_stats(request: Request) -> StatsResponse:
    """Return the full :class:`StatsResponse` payload for the dashboard.

    Combines counters from three sources:

    * :class:`InvertedIndex.stats` — per-tier document and vocabulary
      counts, approximate memory footprint.
    * :class:`RedisStreamConsumer` (if started) — error count; later
      commits will also surface ``ingest_lag`` from XLEN.
    * ``app.state.started_at`` — lifespan start timestamp used for
      uptime and the rough throughput average.

    Throughput and ``query_p95_ms`` are placeholders in this commit:
    the dashboard still renders them, but a proper rolling window
    lands later. ``ingest_lag`` is wired as 0 until we start calling
    ``XLEN`` from the consumer.
    """
    index = request.app.state.index
    consumer = getattr(request.app.state, "consumer", None)
    started_at = getattr(request.app.state, "started_at", time.time())
    uptime = max(time.time() - started_at, 0.0)

    raw = index.stats()

    # ``throughput_1m`` is a best-effort running average until the
    # rolling 1-minute counter lands. Dividing by at least 1 s avoids
    # a ZeroDivisionError on the very first request.
    denom = max(uptime, 1.0)
    throughput = raw["docs_indexed"] / denom

    consumer_errors = getattr(consumer, "errors", 0) if consumer else 0
    errors = consumer_errors + raw.get("errors", 0)

    return StatsResponse(
        docs_indexed=raw["docs_indexed"],
        current_segment_docs=raw["current_segment_docs"],
        flushed_memory_segments=raw["flushed_memory_segments"],
        disk_segments=raw["disk_segments"],
        vocab_size=raw["vocab_size"],
        memory_bytes=raw["memory_bytes"],
        throughput_1m=throughput,
        # Populated in a later commit via ``XLEN logs`` against Redis.
        ingest_lag=0,
        # Real query-time p95 lands when we wire a per-request timer.
        query_p95_ms=0.0,
        errors=errors,
        uptime_s=uptime,
    )
