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

C9 adds the demo endpoints:

* ``POST /demo/populate`` — bulk-seed N demo entries round-robined across
  the three log-type filters through the same ``manager.add`` path real
  traffic uses (so /stats counters and latency metrics move like real load).
* ``POST /demo/performance-test`` — a self-contained bloom-vs-linear-search
  micro-benchmark that quantifies the speed and memory trade WITHOUT
  touching the live filters or their metrics.

The :func:`lifespan` context manager owns the whole state graph: it resolves
settings, builds the metrics registry + manager, reloads persisted snapshots
from ``data_dir`` (warm start), runs the periodic snapshot and rotation
background tasks while the app serves, and saves one final snapshot on the
way out. Later commits keep growing this module in place: C10 adds the
``/pipeline`` two-tier endpoints and C11 the ``/sessions`` endpoints.

Why the hot handlers are ``async def`` with no await
----------------------------------------------------
A bloom add/query is a few µs of C-extension work under a never-contended
lock. Declaring the handler ``sync def`` would make Starlette bounce every
request through the AnyIO threadpool — pure dispatch overhead that costs
more than the operation itself. So the hot handlers are ``async def`` and
call the manager's sync methods inline on the event loop; the filter locks
are never held across an await because there is no await to hold them
across.

The ``/demo`` handlers are the mirror image: bulk/CPU-bound work (tens of
thousands of adds, seconds-long linear-scan benchmarks) declared ``sync
def``, so Starlette dispatches them to the AnyIO threadpool and the event
loop stays free to serve concurrent hot ``/logs`` traffic. Their bulk loops
go through ``manager.add`` one key at a time, so a filter lock is only ever
held for a single µs-scale operation — a long populate can interleave with,
but never starve, live adds and queries.

The service always runs a SINGLE uvicorn worker: all filter state lives
in-process, so multiple workers would each hold a divergent copy of every
filter and answer queries inconsistently.
"""
from __future__ import annotations

import asyncio
import logging
import random
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, get_args
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, Query, Request
from pydantic import BaseModel, Field

from src.bloom import BloomFilter
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


class PopulateResponse(BaseModel):
    """Spec shape for ``/demo/populate``: ``{"status": "completed", "records_added": N}``.

    Exactly these two keys and nothing else — the spec's sample output shows
    only ``status`` and ``records_added``, and restraint here IS fidelity.
    Timing detail for demo seeding belongs in ``/stats`` (the adds run
    through the metered ``manager.add`` path), not in extra response fields.
    """

    status: Literal["completed"]
    records_added: int


class PerformanceTestResponse(BaseModel):
    """Result of one ``/demo/performance-test`` bloom-vs-linear benchmark run.

    All times come from ``time.perf_counter`` around identical probe loops.
    ``speedup_vs_linear`` is the headline number (avg linear lookup / avg
    bloom lookup); ``memory_ratio`` is bloom bitset bytes over the summed
    UTF-8 byte length of the dataset keys — a conservative ratio, see the
    handler. ``false_positives_observed`` counts absent-key probes the bloom
    filter answered True (the bounded error the memory win is paid for
    with); ``processing_time_ms`` is whole-handler wall time including
    dataset construction.
    """

    dataset_size: int
    lookups: int
    bloom_total_ms: float
    bloom_avg_ms: float
    linear_total_ms: float
    linear_avg_ms: float
    speedup_vs_linear: float
    bloom_memory_bytes: int
    keys_memory_bytes_estimate: int
    memory_ratio: float
    false_positives_observed: int
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


# --------------------------------------------------------------------- #
# demo endpoints (C9)                                                   #
# --------------------------------------------------------------------- #

#: Round-robin targets for ``/demo/populate`` — the three spec log types in
#: ``LogType`` declaration order. Deliberately derived from the ``Literal``
#: (not ``manager.names``): C11 registers a fourth ``sessions`` filter with
#: the manager, and demo seeding must keep targeting only the log-type
#: filters the public ``/logs`` API exposes.
_DEMO_LOG_TYPES: tuple[str, ...] = get_args(LogType)

#: Every benchmark key is padded to this many bytes. The benchmark's memory
#: comparison is "bloom bitset vs storing the full keys", so the keys must
#: be shaped like REAL log keys — request ids, session tokens, sha256-hashed
#: log lines — which run ~64 bytes, not like a 15-byte loop counter (the
#: sizing discussion in ``src/bloom.py`` uses the same 64-byte reference
#: key). At p=0.01 the filter pays ~1.2 bytes per key against 64 bytes of
#: key text → ratio ≈ 0.019, comfortably inside the spec's <5% criterion.
_PERF_KEY_LENGTH = 64

#: FP target for the throwaway benchmark filter — matches the spec's
#: ``error_logs`` configuration (the "default" filter config).
_PERF_FP_RATE = 0.01

#: Fixed seed for the probe-position RNG: probe positions are deterministic
#: across calls (reproducible benchmark), while key CONTENT still varies via
#: the per-call nonce. Same constant as the filters' hash seed, reused
#: purely for recognizability.
_PERF_PROBE_SEED = 0x5EEDB10C


def _perf_key(namespace: str, nonce: str, i: int) -> str:
    """Build one realistic-length benchmark key: ``<namespace>-<nonce>-<i>`` padded.

    The namespace keeps dataset keys (``perf``) and guaranteed-absent probe
    keys (``absent``) in disjoint families; the zero-padded index plus
    right-padding to ``_PERF_KEY_LENGTH`` makes every key the same realistic
    size — constant per-comparison cost for the linear scan and an honest
    byte count for the memory comparison.
    """
    return f"{namespace}-{nonce}-{i:010d}".ljust(_PERF_KEY_LENGTH, "x")


def _time_membership(
    contains: Callable[[str], bool], probes: list[str]
) -> tuple[list[bool], float]:
    """Run ``contains`` over ``probes``; return ``(answers, total_ms)``.

    One shared harness so both data structures pay the *identical* loop and
    result-append overhead — the measured difference is then the structures',
    not the harness's. Answers are collected (not discarded) because the
    bloom run needs them to count observed false positives.
    """
    answers: list[bool] = []
    append = answers.append
    start = time.perf_counter()
    for probe in probes:
        append(contains(probe))
    total_ms = (time.perf_counter() - start) * 1000.0
    return answers, total_ms


@app.post("/demo/populate", response_model=PopulateResponse)
def demo_populate(
    request: Request,
    count: int = Query(default=10_000, ge=1, le=1_000_000),
) -> PopulateResponse:
    """Bulk-insert ``count`` demo entries round-robined across the log types.

    ``sync def`` on purpose: this is bulk work (default 10k adds, up to 1M),
    so FastAPI runs it on the AnyIO threadpool and the event loop stays free
    for concurrent hot ``/logs`` traffic; each iteration takes a filter lock
    for one µs-scale add only (module docstring). Worst case at the 1M cap
    is a few seconds of threadpool time — fine for a demo endpoint, and the
    ``le`` bound exists precisely to cap it.

    Keys are ``demo-<nonce>-<i>`` with a fresh uuid4-derived 8-hex-char
    nonce per call, so REPEATED populates add new records instead of
    re-adding the previous batch (``elements_added`` keeps growing, which is
    what a demo wants to show). Inserts go through :meth:`FilterManager.add`
    exactly like ``/logs/add`` traffic, so per-filter counters and latency
    metrics in ``/stats`` move like real load — that is the point of a seed.

    Distribution: key ``i`` goes to ``_DEMO_LOG_TYPES[i % 3]``, so a count
    divisible by 3 lands exactly count/3 in each filter.
    """
    manager: FilterManager = request.app.state.manager
    nonce = uuid4().hex[:8]
    n_types = len(_DEMO_LOG_TYPES)
    for i in range(count):
        manager.add(_DEMO_LOG_TYPES[i % n_types], f"demo-{nonce}-{i}")
    return PopulateResponse(status="completed", records_added=count)


@app.post("/demo/performance-test", response_model=PerformanceTestResponse)
def demo_performance_test(
    lookups: int = Query(default=2_000, ge=1, le=50_000),
    dataset_size: int = Query(default=20_000, ge=100, le=200_000),
) -> PerformanceTestResponse:
    """Micro-benchmark: bloom filter vs traditional (linear list) lookup.

    Entirely self-contained — builds a throwaway key list and a throwaway
    :class:`BloomFilter` and never touches the live filters or their
    metrics, because a benchmark must not pollute the ``/stats`` numbers
    operators (and the dashboard) are watching. "Traditional lookup" is a
    plain Python list with linear search, per the spec's success criteria
    ("100x+ speed improvement over linear search").

    Method:

    * ``dataset_size`` keys, padded to 64 bytes each (``_PERF_KEY_LENGTH``
      rationale above), inserted into a fresh filter sized for exactly
      ``dataset_size`` at p=0.01.
    * ``lookups`` membership probes, 50/50 present/absent. Present probes
      are drawn uniformly (seeded RNG → deterministic positions) from the
      dataset; absent probes come from the disjoint ``absent-*`` family.
    * BOTH structures consume the *same* probe list through the same timing
      harness (:func:`_time_membership`); bloom runs first, so any cache
      warming of the probe strings benefits the linear baseline — the
      reported speedup is, if anything, understated.
    * ``keys_memory_bytes_estimate`` is the summed UTF-8 length of the raw
      keys only. Deliberately conservative: it ignores CPython str object
      overhead (~49 bytes each) and set/dict bucket overhead, so the real
      ratio against an in-memory key set is even better than reported.

    Runtime: ``sync def`` → threadpool. The defaults cost ~2000 × 20000 ≈
    40M worst-case string compares for the linear baseline — a couple of
    seconds of CPython; that slowness is the exhibit, not a bug. Maxed-out
    params (50k × 200k) run for minutes — an operator's deliberate choice
    on a demo endpoint, bounded by the ``le`` caps.
    """
    handler_start = time.perf_counter()
    nonce = uuid4().hex[:8]

    dataset = [_perf_key("perf", nonce, i) for i in range(dataset_size)]

    bloom = BloomFilter(expected_items=dataset_size, fp_rate=_PERF_FP_RATE)
    for key in dataset:
        bloom.add(key)

    rng = random.Random(_PERF_PROBE_SEED)
    probes: list[str] = []
    present: list[bool] = []
    for i in range(lookups):
        if i % 2 == 0:
            probes.append(dataset[rng.randrange(dataset_size)])
            present.append(True)
        else:
            probes.append(_perf_key("absent", nonce, i // 2))
            present.append(False)

    bloom_answers, bloom_total_ms = _time_membership(bloom.might_contain, probes)
    _, linear_total_ms = _time_membership(dataset.__contains__, probes)

    false_positives = sum(
        1
        for answer, is_present in zip(bloom_answers, present)
        if answer and not is_present
    )

    bloom_avg_ms = bloom_total_ms / lookups
    linear_avg_ms = linear_total_ms / lookups
    # max() guards the (theoretical) zero-duration bloom run on a coarse timer.
    speedup = linear_avg_ms / max(bloom_avg_ms, 1e-9)

    keys_memory_bytes = sum(len(key.encode("utf-8")) for key in dataset)
    memory_ratio = bloom.memory_bytes / keys_memory_bytes

    processing_time_ms = (time.perf_counter() - handler_start) * 1000.0
    return PerformanceTestResponse(
        dataset_size=dataset_size,
        lookups=lookups,
        bloom_total_ms=round(bloom_total_ms, 3),
        bloom_avg_ms=round(bloom_avg_ms, 6),
        linear_total_ms=round(linear_total_ms, 3),
        linear_avg_ms=round(linear_avg_ms, 6),
        speedup_vs_linear=round(speedup, 2),
        bloom_memory_bytes=bloom.memory_bytes,
        keys_memory_bytes_estimate=keys_memory_bytes,
        memory_ratio=round(memory_ratio, 6),
        false_positives_observed=false_positives,
        processing_time_ms=round(processing_time_ms, 3),
    )


if __name__ == "__main__":
    # Convenience entrypoint for `python -m src.api`; Docker runs uvicorn directly.
    settings = get_settings()
    uvicorn.run(
        "src.api:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
