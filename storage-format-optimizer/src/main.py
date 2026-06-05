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
from fastapi import FastAPI

from src.api.routes_ingest import router as ingest_router
from src.api.routes_query import router as query_router
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

logger = logging.getLogger("storage_format_optimizer")


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

    # --- start the background migration loop AFTER the graph is published ----
    stop_event = asyncio.Event()
    app.state.stop_event = stop_event
    migration_task = asyncio.create_task(migration_engine.run(stop_event))
    app.state.migration_task = migration_task

    logger.info(
        "storage-format-optimizer starting on %s:%s (data_dir=%s)",
        settings.api_host,
        settings.api_port,
        settings.data_dir,
    )

    try:
        yield
    finally:
        # Reverse-order teardown: stop + cancel + await the migration loop.
        stop_event.set()
        migration_task.cancel()
        with suppress(asyncio.CancelledError):
            await migration_task
        logger.info("storage-format-optimizer shutdown")


app = FastAPI(title="Adaptive Storage Format Optimizer", lifespan=lifespan)

app.include_router(ingest_router)
app.include_router(query_router)


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
