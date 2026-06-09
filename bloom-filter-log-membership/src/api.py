"""FastAPI application for the Bloom Filter Log Membership service.

C8 wires the core membership API around the per-log-type
:class:`~src.manager.FilterManager`:

* ``POST /logs/add`` / ``POST /logs/query`` — the spec's hot endpoints, with
  ``log_type`` constrained to the three configured filters (FastAPI 422s
  anything else before it reaches the manager) and the exact spec response
  shapes (``"probably_exists"`` / ``"definitely_not_exist"``).
* ``GET /stats`` — per-filter gauges (elements, memory, fill, estimated vs
  target FP rate, slices, rotations) merged with operation metrics (counts,
  avg/p50/p99 latencies) plus cross-filter totals and process uptime.
* ``GET /health`` — unchanged liveness probe for Docker's HEALTHCHECK.

The :func:`lifespan` context manager owns the whole state graph: it resolves
settings, builds the metrics registry + manager, reloads persisted snapshots
from ``data_dir`` (warm start), runs the periodic snapshot and rotation
background tasks while the app serves, and saves one final snapshot on the
way out. Later commits grow this module in place: C9 adds the ``/demo``
endpoints, C10 the ``/pipeline`` two-tier endpoints, and C11 the
``/sessions`` endpoints.

Why the hot handlers are ``async def`` with no await
----------------------------------------------------
A bloom add/query is a few µs of C-extension work under a never-contended
lock. Declaring the handler ``sync def`` would make Starlette bounce every
request through the AnyIO threadpool — pure dispatch overhead that costs
more than the operation itself. So the hot handlers are ``async def`` and
call the manager's sync methods inline on the event loop; the filter locks
are never held across an await because there is no await to hold them
across.

The service always runs a SINGLE uvicorn worker: all filter state lives
in-process, so multiple workers would each hold a divergent copy of every
filter and answer queries inconsistently.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import uvicorn
from fastapi import FastAPI, Request
from pydantic import BaseModel, Field

from src.manager import FilterManager
from src.metrics import MetricsRegistry
from src.settings import Settings, get_settings

logger = logging.getLogger("bloom_filter_log_membership")

#: Bytes per mebibyte — /stats reports both raw bytes and a human MB figure.
_MB = 1024 * 1024


# --------------------------------------------------------------------- #
# request / response models (exact spec shapes)                         #
# --------------------------------------------------------------------- #

#: The only log types the API accepts. Mirrors
#: :meth:`Settings.filter_configs` — FastAPI turns any other value into an
#: automatic 422 with a list of permitted values, so the manager's
#: ``KeyError`` branch is unreachable from HTTP.
LogType = Literal["error_logs", "access_logs", "security_logs"]


class LogEntryRequest(BaseModel):
    """Body of both ``/logs/add`` and ``/logs/query``.

    ``log_key`` is whatever uniquely identifies a log entry to the caller
    (request id, hashed log line, session token, ...) — the service never
    parses it, only hashes it. Empty keys are rejected: an empty string is
    always a caller bug, never a real log identity.
    """

    log_type: LogType
    log_key: str = Field(min_length=1)


class AddResponse(BaseModel):
    """Spec shape for ``/logs/add``: ``{"status": "added", "processing_time_ms": 0.123}``.

    ``status`` is ``"added"`` even when the key was already present — the
    spec's response carries no dedup flag, and an add of a duplicate is
    still a successful add from the caller's point of view. The dedup
    detail is visible in ``/stats`` instead (``adds_total`` keeps counting,
    ``elements_added`` does not).
    """

    status: Literal["added"]
    processing_time_ms: float


class QueryResponse(BaseModel):
    """Spec shape for ``/logs/query``.

    ``confidence`` carries the asymmetry of a bloom answer in the spec's
    exact wording: a positive is only ``"probably_exists"`` (bounded false
    positives) while a negative is ``"definitely_not_exist"`` (zero false
    negatives within the two live generations).
    """

    might_exist: bool
    confidence: Literal["probably_exists", "definitely_not_exist"]
    processing_time_ms: float


# --------------------------------------------------------------------- #
# background tasks                                                      #
# --------------------------------------------------------------------- #

#: Serializes every ``save_all`` in this process: the periodic snapshot
#: loop (threadpool via ``asyncio.to_thread``) and the final shutdown save
#: (lifespan thread). Needed because ``write_atomic`` stages each file at a
#: FIXED ``<name>.bloom.tmp`` path — if shutdown cancelled the loop while a
#: save was mid-flight in its worker thread and then ran the final save
#: concurrently, two writers could interleave on the same tmp file and
#: rename a corrupt snapshot into place. One tiny lock makes that race
#: unrepresentable; hold time is a single save (ms).
_save_lock = threading.Lock()


def _locked_save(manager: FilterManager, data_dir: str | Path) -> None:
    """Run ``manager.save_all`` while holding the process-wide save lock."""
    with _save_lock:
        manager.save_all(data_dir)


async def _snapshot_loop(manager: FilterManager, settings: Settings) -> None:
    """Persist every filter each ``snapshot_interval_seconds`` until cancelled.

    The save runs in a worker thread (``asyncio.to_thread``) so the file
    fsyncs never stall the event loop the hot handlers run on. Failures are
    logged and the loop keeps going — a transient full disk must not
    silently kill snapshotting for the rest of the process's life.
    Cancellation (``CancelledError`` is a ``BaseException``) is deliberately
    NOT caught: it is the loop's only exit.
    """
    while True:
        await asyncio.sleep(settings.snapshot_interval_seconds)
        try:
            await asyncio.to_thread(_locked_save, manager, settings.data_dir)
            logger.debug("periodic snapshot saved to %s", settings.data_dir)
        except Exception:
            logger.exception(
                "periodic snapshot to %s failed; retrying next interval",
                settings.data_dir,
            )


async def _rotation_loop(manager: FilterManager, settings: Settings) -> None:
    """Call ``rotate_if_due`` each ``rotation_check_interval_seconds``.

    Runs inline on the event loop: the not-due pre-check inside
    ``rotate_if_due`` is pure µs-level Python for the common case, and even
    a due rotation is one in-memory allocation + pointer swap — cheaper
    than a threadpool round trip. When rotation is disabled
    (``rotation_max_age_seconds == 0``) the call just returns ``[]``; the
    loop still ticks, which keeps the task graph identical in every
    configuration. Same failure stance as the snapshot loop: log and keep
    going, exit only via cancellation.
    """
    while True:
        await asyncio.sleep(settings.rotation_check_interval_seconds)
        try:
            rotated = manager.rotate_if_due()
            if rotated:
                logger.info("rotation task rotated filters: %s", rotated)
        except Exception:
            logger.exception("rotation check failed; retrying next interval")


# --------------------------------------------------------------------- #
# lifespan                                                              #
# --------------------------------------------------------------------- #


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build, warm-start, run, and tear down the filter state graph.

    Startup: settings → logging → metrics + manager → ``load_all`` (adopt
    any valid on-disk snapshots — corrupt/missing/config-mismatched files
    just mean a fresh filter, never a crash) → publish everything on
    ``app.state`` → start the snapshot + rotation tasks.

    Shutdown: cancel both tasks and wait for them to finish unwinding
    (``gather`` with ``return_exceptions`` swallows their
    ``CancelledError``), then take one final snapshot so everything added
    since the last periodic save survives the restart.
    """
    settings = get_settings()
    # basicConfig accepts level names ("INFO") as well as numeric levels.
    logging.basicConfig(level=settings.log_level)

    data_dir = Path(settings.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    metrics = MetricsRegistry()
    manager = FilterManager(settings, metrics)
    loaded = manager.load_all(data_dir)
    logger.info(
        "startup reload from %s: %s",
        data_dir,
        {name: "restored" if ok else "fresh" for name, ok in loaded.items()},
    )

    app.state.settings = settings
    app.state.manager = manager
    app.state.metrics = metrics
    app.state.started_at = time.time()

    snapshot_task = asyncio.create_task(
        _snapshot_loop(manager, settings), name="bloom-snapshot-loop"
    )
    rotation_task = asyncio.create_task(
        _rotation_loop(manager, settings), name="bloom-rotation-loop"
    )

    logger.info(
        "bloom-filter-log-membership starting on %s:%s (data_dir=%s, "
        "filters=%s, snapshot every %.0fs, rotation check every %.0fs)",
        settings.api_host,
        settings.api_port,
        settings.data_dir,
        list(manager.names),
        settings.snapshot_interval_seconds,
        settings.rotation_check_interval_seconds,
    )
    try:
        yield
    finally:
        for task in (snapshot_task, rotation_task):
            task.cancel()
        await asyncio.gather(snapshot_task, rotation_task, return_exceptions=True)
        _locked_save(manager, data_dir)
        logger.info("final snapshot saved to %s", data_dir)
        logger.info("bloom-filter-log-membership shutdown")


app = FastAPI(title="Bloom Filter Log Membership API", lifespan=lifespan)


# --------------------------------------------------------------------- #
# routes                                                                #
# --------------------------------------------------------------------- #


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe used by Docker's HEALTHCHECK and the compose wait loops."""
    return {"status": "healthy"}


@app.post("/logs/add", response_model=AddResponse)
async def add_log(entry: LogEntryRequest, request: Request) -> AddResponse:
    """Record that this log entry has now been seen.

    Routes to the filter named by ``log_type`` and reports how long the
    add took (lock wait + filter operation), rounded to 3 decimals — at
    µs-scale operations, more digits would just be timer noise.
    """
    manager: FilterManager = request.app.state.manager
    _, duration_ms = manager.add(entry.log_type, entry.log_key)
    return AddResponse(status="added", processing_time_ms=round(duration_ms, 3))


@app.post("/logs/query", response_model=QueryResponse)
async def query_log(entry: LogEntryRequest, request: Request) -> QueryResponse:
    """Answer "have we seen this log entry before?" — possibly-yes or surely-no.

    Checks the current generation and falls back to the previous one, so
    keys added before the last rotation remain answerable (manager module
    docstring has the full generation story).
    """
    manager: FilterManager = request.app.state.manager
    might_exist, confidence, duration_ms = manager.query(
        entry.log_type, entry.log_key
    )
    return QueryResponse(
        might_exist=might_exist,
        confidence=confidence,
        processing_time_ms=round(duration_ms, 3),
    )


def _shape_filter_stats(merged: dict) -> dict:
    """Flatten one manager stats entry into the public /stats filter shape.

    ``merged`` is :meth:`FilterManager.stats` output for one filter: the
    current generation's SBF gauges + generation bookkeeping + an ``"ops"``
    metrics snapshot. The public shape is deliberately flat (dashboards and
    curl users read it) and reports:

    * ``memory_bytes`` as the filter's TOTAL footprint — current plus the
      still-queryable previous generation — because that is what the
      process actually pays for answering this filter's queries;
    * ``fill_ratio`` aggregated across the current generation's slices
      (total bits set / total bits), the simple saturation gauge;
    * ``estimated_fp_rate`` as the compound fill-based estimate, the number
      the SBF budget guarantees stays at or below ``target_fp_rate``.
    """
    ops = merged["ops"]
    memory_bytes = merged["memory_bytes_total"]
    total_bits = sum(s["m_bits"] for s in merged["slices"])
    bits_set = sum(s["bits_set"] for s in merged["slices"])
    return {
        "elements_added": merged["count"],
        "capacity": merged["capacity"],
        "slice_count": merged["slice_count"],
        "rotations": merged["rotations"],
        "previous_count": merged["previous_count"],
        "memory_bytes": memory_bytes,
        "memory_mb": round(memory_bytes / _MB, 3),
        "fill_ratio": round(bits_set / total_bits, 6) if total_bits else 0.0,
        "estimated_fp_rate": merged["compound_estimated_fp"],
        "target_fp_rate": merged["target_fp_rate"],
        "adds_total": ops["adds_total"],
        "queries_total": ops["queries_total"],
        "positives": ops["positives"],
        "negatives": ops["negatives"],
        "observed_false_positives": ops["observed_false_positives"],
        "observed_fp_rate": ops["observed_fp_rate"],
        "avg_add_ms": ops["avg_add_ms"],
        "p99_add_ms": ops["p99_add_ms"],
        "avg_query_ms": ops["avg_query_ms"],
        "p50_query_ms": ops["p50_query_ms"],
        "p99_query_ms": ops["p99_query_ms"],
        "created_at": merged["created_at"],
        "generation_age_seconds": round(merged["generation_age_seconds"], 3),
    }


@app.get("/stats")
async def get_stats(request: Request) -> dict:
    """Service-wide statistics: per-filter detail, cross-filter totals, uptime.

    No ``response_model``: the per-filter shape is an open dict that later
    commits extend (C10 pipeline counters, C11 sessions filter) and the
    dashboard consumes whatever is present. Cheap enough for the event loop:
    three filters' worth of dict copies plus sorting bounded 1000-sample
    latency windows — well under a millisecond.
    """
    manager: FilterManager = request.app.state.manager
    filters = {
        name: _shape_filter_stats(merged)
        for name, merged in manager.stats().items()
    }
    total_memory = sum(f["memory_bytes"] for f in filters.values())
    return {
        "service": "bloom-filter-log-membership",
        "uptime_seconds": round(time.time() - request.app.state.started_at, 3),
        "filters": filters,
        "totals": {
            "elements_added": sum(f["elements_added"] for f in filters.values()),
            "adds_total": sum(f["adds_total"] for f in filters.values()),
            "queries_total": sum(f["queries_total"] for f in filters.values()),
            "memory_bytes": total_memory,
            "memory_mb": round(total_memory / _MB, 3),
        },
    }


if __name__ == "__main__":
    # Convenience entrypoint for `python -m src.api`; Docker runs uvicorn directly.
    settings = get_settings()
    uvicorn.run(
        "src.api:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
