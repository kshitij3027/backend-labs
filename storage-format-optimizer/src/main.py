"""FastAPI application for the adaptive storage-format optimizer.

This is the C19 wiring commit: the :func:`lifespan` context manager now builds
the **full object graph** — the on-disk manifest store, the access-pattern
tracker, the bounded metrics aggregator, the compression chooser, the index /
tier managers, the format selector, the three per-format storage backends, and
the ingest / query / migration engines — and attaches every piece to
``app.state`` so the REST routers (via :mod:`src.api.dependencies`) and the
dashboard/WS layers in later commits can reach them.

Startup ordering and teardown mirror the sibling ``multi-tier-caching-layer``
service: the data/log directories are ensured to exist, the whole graph is wired,
a best-effort orphan sweep reclaims any leftover files from a previous run, the
graph is published on ``app.state``, and only then is the background migration
loop started. On shutdown we reverse: signal the loop to stop, cancel it, and
await it while suppressing :class:`asyncio.CancelledError`.

The migration loop is the one long-lived background task here. It rewrites live
partitions copy-on-write and is engineered to be unkillable by data errors (see
:meth:`~src.migration_engine.MigrationEngine.run`), so the lifespan only needs to
own its lifecycle, not its error handling.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from src.api.routes_ingest import router as ingest_router
from src.api.routes_query import router as query_router
from src.api.routes_stats import router as stats_router
from src.compression import CompressionChooser
from src.format_selector import FormatSelector
from src.index_manager import IndexManager
from src.ingest_engine import IngestEngine
from src.manifest import ManifestStore
from src.metrics import Metrics
from src.migration_engine import MigrationEngine
from src.models import Format
from src.paths import ensure_dir
from src.pattern_tracker import PatternTracker
from src.query_engine import QueryEngine
from src.settings import get_settings
from src.storage.columnar_backend import ColumnarBackend
from src.storage.hybrid_backend import HybridBackend
from src.storage.row_backend import RowBackend
from src.tier_manager import TierManager
from src.websocket import ConnectionManager

logger = logging.getLogger("storage_format_optimizer")


def _build_tick(app: FastAPI) -> dict:
    """Build the live dashboard payload from current ``app.state`` snapshots.

    Cheap by design: it folds the manifest once, reading each partition's
    **already-stored** ``format`` / ``tier`` / ``index`` off the cached
    :class:`~src.manifest.PartitionMeta` — it never re-runs the format selector
    or touches a data file. The base document comes from
    :meth:`~src.metrics.Metrics.snapshot`; this function fills in the
    manifest-derived format distribution, per-tenant breakdown, per-tier counts,
    indexed-column total, and the storage byte/compression rollup that the
    metrics aggregator deliberately leaves to the caller.

    The returned dict is fully JSON-serialisable (plain dicts/lists of
    numbers/strings) so :meth:`ConnectionManager.broadcast` can send it as-is::

        {"type": "tick", "stats": <enriched snapshot>, "series": {...},
         "tenants": {tenant: {format: count}}, "migrations": [...],
         "indexes": {"columns_indexed": int}, "tiers": {tier: count}}
    """
    metrics = app.state.metrics
    manifest = app.state.manifest
    snap = metrics.snapshot()

    dist = {"row": 0, "columnar": 0, "hybrid": 0}
    tiers = {"hot": 0, "warm": 0, "cold": 0}
    by_format_bytes = {"row": 0, "columnar": 0, "hybrid": 0}
    total = 0
    uncompressed = 0
    per_tenant: dict[str, dict[str, int]] = {}  # tenant -> {format: count}
    indexed_columns_total = 0

    for tenant in manifest.all_tenants():
        tenant_counts = {"row": 0, "columnar": 0, "hybrid": 0}
        for meta in manifest.list_partitions(tenant):
            fmt = meta.format.value
            dist[fmt] = dist.get(fmt, 0) + 1
            tenant_counts[fmt] = tenant_counts.get(fmt, 0) + 1
            # Guard None tier (defaults to "hot") so the payload never carries
            # a non-serialisable / missing key.
            tier = meta.tier.value if meta.tier else "hot"
            tiers[tier] = tiers.get(tier, 0) + 1
            by_format_bytes[fmt] = by_format_bytes.get(fmt, 0) + meta.size_bytes
            total += meta.size_bytes
            uncompressed += meta.uncompressed_estimate_bytes
            indexed_columns_total += (
                len(meta.index.get("columns", [])) if meta.index else 0
            )
        per_tenant[tenant] = tenant_counts

    # Overlay the manifest-derived format + storage rollups onto the snapshot.
    snap["formats"]["distribution"] = dist
    snap["formats"]["partitions_total"] = sum(dist.values())
    snap["storage"]["by_format"] = by_format_bytes
    snap["storage"]["total_bytes"] = total
    snap["storage"]["uncompressed_estimate_bytes"] = uncompressed
    snap["storage"]["compression_ratio"] = (uncompressed / total) if total > 0 else 1.0

    return {
        "type": "tick",
        "stats": snap,
        "series": metrics.series(),
        "tenants": per_tenant,
        "migrations": snap["migrations"]["recent"],
        "indexes": {"columns_indexed": indexed_columns_total},
        "tiers": tiers,
    }


async def _broadcast_loop(app: FastAPI, stop_event: asyncio.Event) -> None:
    """Advance the time-series and fan a tick out to every dashboard client.

    Runs alongside the migration loop and shares the **same** ``stop_event``, so
    a single ``stop_event.set()`` in the lifespan teardown wakes both promptly.
    Each iteration appends one point to the metrics time-series (so the live
    charts scroll even while idle) and broadcasts the freshly built tick, then
    sleeps until either ``ws_push_interval_seconds`` elapses or the stop event
    fires — never busy-spinning. Any per-tick exception is logged and swallowed
    so one bad tick can never kill the loop.
    """
    while not stop_event.is_set():
        try:
            app.state.metrics.append_series_point()  # advance the series each tick
            await app.state.ws_manager.broadcast(_build_tick(app))
        except Exception:  # noqa: BLE001 — never let one tick kill the loop
            logger.exception("ws broadcast tick failed")
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=app.state.settings.ws_push_interval_seconds,
            )
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the object graph, start the migration loop, and tear it all down.

    Build order is load-bearing: the data/log directories must exist before the
    manifest store touches them, and the whole graph (plus a startup orphan
    sweep) must be in place and published on ``app.state`` before the background
    migration loop is allowed to start — the loop immediately scans the manifest
    and may rewrite partitions. On shutdown we reverse: set the stop event, cancel
    the migration task, and await it while suppressing
    :class:`asyncio.CancelledError`.
    """
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)

    # --- filesystem: ensure the data + log roots exist before anything reads ---
    ensure_dir(Path(settings.data_dir))
    ensure_dir(Path(settings.log_dir))

    # --- durable source of truth + observational state -----------------------
    manifest = ManifestStore(settings.data_dir)
    pattern_tracker = PatternTracker()
    metrics = Metrics(history_points=settings.metrics_history_points)

    # --- adaptive policy engines --------------------------------------------
    compression = CompressionChooser(
        enabled=settings.compression_learn_enabled,
        sample_rows=settings.compression_learn_sample_rows,
        size_weight=settings.compression_learn_size_weight,
        latency_weight=settings.compression_learn_latency_weight,
    )
    index_manager = IndexManager(
        min_filter_hits=settings.index_min_filter_hits,
        min_selectivity=settings.index_min_selectivity,
        drop_benefit_window=settings.index_drop_benefit_window,
        drop_min_benefit=settings.index_drop_min_benefit,
    )
    tier_manager = TierManager(
        hot_max_age_seconds=settings.tier_hot_max_age_seconds,
        cold_min_age_seconds=settings.tier_cold_min_age_seconds,
        hot_min_reads_per_min=settings.tier_hot_min_reads_per_min,
    )
    selector = FormatSelector(
        write_ratio_row=settings.select_write_ratio_row,
        point_lookup_row=settings.select_point_lookup_row,
        scan_ratio_columnar=settings.select_scan_ratio_columnar,
        few_columns_fraction=settings.select_few_columns_fraction,
        min_confidence=settings.select_min_confidence,
        min_rows=settings.select_min_rows,
    )

    # --- per-format storage backends -----------------------------------------
    # COLUMNAR and HYBRID share the compression chooser so learned codecs are
    # consistent across the sealed Parquet they both write.
    backends = {
        Format.ROW: RowBackend(),
        Format.COLUMNAR: ColumnarBackend(compression=compression),
        Format.HYBRID: HybridBackend(compression=compression),
    }

    # --- request-path engines ------------------------------------------------
    ingest_engine = IngestEngine(
        manifest=manifest,
        backends=backends,
        pattern_tracker=pattern_tracker,
        metrics=metrics,
        settings=settings,
    )
    query_engine = QueryEngine(
        manifest=manifest,
        backends=backends,
        pattern_tracker=pattern_tracker,
        index_manager=index_manager,
        metrics=metrics,
        settings=settings,
    )

    # --- background migration engine -----------------------------------------
    migration_engine = MigrationEngine(
        manifest=manifest,
        backends=backends,
        selector=selector,
        tier_manager=tier_manager,
        pattern_tracker=pattern_tracker,
        compression=compression,
        index_manager=index_manager,
        metrics=metrics,
        settings=settings,
    )

    # --- startup cleanup: reclaim orphaned files from a previous run ---------
    # Best-effort and fully isolated: a sweep failure must never block startup
    # (e.g. an unreadable directory). The app stays serviceable either way.
    try:
        migration_engine.sweep_orphans()
    except Exception:  # noqa: BLE001 - startup cleanup is best-effort, never fatal.
        logger.exception("orphan sweep failed on startup; continuing")

    # --- dashboard WebSocket fan-out -----------------------------------------
    ws_manager = ConnectionManager()

    # --- publish the whole graph on app.state --------------------------------
    # Dependencies / routers reach these via src.api.dependencies; later commits
    # (stats route, WS broadcast) read the policy engines + backends directly.
    app.state.settings = settings
    app.state.manifest = manifest
    app.state.pattern_tracker = pattern_tracker
    app.state.metrics = metrics
    app.state.compression = compression
    app.state.index_manager = index_manager
    app.state.tier_manager = tier_manager
    app.state.selector = selector
    app.state.backends = backends
    app.state.ingest_engine = ingest_engine
    app.state.query_engine = query_engine
    app.state.migration_engine = migration_engine
    app.state.ws_manager = ws_manager

    # --- start the background loops AFTER the graph is published -------------
    # Both the migration loop and the WS broadcast loop share this one stop
    # event, so a single ``stop_event.set()`` on teardown signals both at once.
    stop_event = asyncio.Event()
    app.state.stop_event = stop_event
    migration_task = asyncio.create_task(migration_engine.run(stop_event))
    app.state.migration_task = migration_task
    # Metrics broadcast loop: pushes a tick to every connected dashboard every
    # ``ws_push_interval_seconds`` and sleeps on the shared stop_event so
    # shutdown wakes it immediately.
    ws_broadcast_task = asyncio.create_task(_broadcast_loop(app, stop_event))
    app.state.ws_broadcast_task = ws_broadcast_task

    logger.info(
        "storage-format-optimizer starting on %s:%s (data_dir=%s)",
        settings.api_host,
        settings.api_port,
        settings.data_dir,
    )

    try:
        yield
    finally:
        # Reverse-order teardown: the single stop_event signals both loops; then
        # cancel + await each (broadcast first, since it started last) while
        # suppressing the expected CancelledError.
        stop_event.set()
        ws_broadcast_task.cancel()
        migration_task.cancel()
        with suppress(asyncio.CancelledError):
            await ws_broadcast_task
        with suppress(asyncio.CancelledError):
            await migration_task
        logger.info("storage-format-optimizer shutdown")


app = FastAPI(title="Adaptive Storage Format Optimizer", lifespan=lifespan)

app.include_router(ingest_router)
app.include_router(query_router)
app.include_router(stats_router)


@app.websocket("/ws")
async def ws_metrics(websocket: WebSocket) -> None:
    """Stream live optimizer metrics to a dashboard client.

    On connect the client receives an immediate :func:`_build_tick` snapshot (so
    the dashboard paints without waiting for the next tick); thereafter the
    background broadcast loop in :func:`lifespan` pushes a tick every
    ``ws_push_interval_seconds``. We loop on ``receive_text`` purely to detect the
    client going away, unregistering it on :class:`WebSocketDisconnect` (and on
    any other receive error) so a dead socket is always pruned.
    """
    mgr = websocket.app.state.ws_manager
    await mgr.connect(websocket)
    try:
        # Immediate push so a freshly connected dashboard renders at once.
        await mgr.send_personal(websocket, _build_tick(websocket.app))
        while True:
            await websocket.receive_text()  # keep-alive; client messages ignored
    except WebSocketDisconnect:
        mgr.disconnect(websocket)
    except Exception:  # noqa: BLE001 — prune on any receive failure, never raise
        mgr.disconnect(websocket)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe used by Docker's HEALTHCHECK and the e2e wait loop."""
    return {"status": "healthy"}


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "src.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
