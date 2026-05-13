"""FastAPI app factory + lifespan wiring."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from .api.routes_admin import router as admin_router
from .api.routes_experiments import router as experiments_router
from .api.routes_health import router as health_router
from .api.routes_metrics import router as metrics_router
from .api.routes_runs import router as runs_router
from .api.routes_targets import router as targets_router
from .api.ws import (
    ConnectionManager,
    broadcaster_task,
    record_snapshot,
    router as ws_router,
)
from .config.settings import get_settings
from .docker_client.client import DockerClient
from .engine.experiment_engine import (
    ExperimentEngine,
    default_probes_for_latency,
)
from .engine.run_manager import RunManager
from .engine.supervisor import SafetySupervisor
from .injection.injector import FailureInjector
from .monitoring.system_monitor import SystemMonitor, set_monitor
from .observability import configure_logging, request_id_middleware
from .persistence.repo import create_all_tables, make_engine, make_sessionmaker

logger = logging.getLogger("chaos.framework")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    # --- persistence ---
    db_engine = make_engine(settings.database_url)
    await create_all_tables(db_engine)
    db_sessionmaker = make_sessionmaker(db_engine)

    # --- docker + monitor ---
    docker_client = DockerClient(
        allowlist=settings.target_allowlist,
        socket_path=settings.docker_socket_path,
    )
    monitor = SystemMonitor(
        docker_client=docker_client,
        interval_seconds=settings.metrics_collection_interval_seconds,
        history_size=settings.metrics_history_size,
        target_health_paths={
            name: "/health"
            for name in settings.target_allowlist
            if name != "redis"
        },
    )
    set_monitor(monitor)
    monitor.add_listener(record_snapshot)
    await monitor.start()

    # --- ws broadcaster scaffolding ---
    event_queue: asyncio.Queue = asyncio.Queue(maxsize=1024)
    ws_manager = ConnectionManager()
    ws_stop_event = asyncio.Event()

    # --- injector + engine + run manager ---
    injector = FailureInjector(
        docker_client=docker_client,
        allowlist=settings.target_allowlist,
        max_concurrent=settings.max_concurrent_scenarios,
        cpu_emergency_threshold_pct=settings.cpu_emergency_threshold_pct,
        mem_emergency_threshold_pct=settings.mem_emergency_threshold_pct,
        metrics_snapshot=monitor.snapshot,
    )
    engine = ExperimentEngine(
        injector=injector,
        monitor=monitor,
        probes_factory=default_probes_for_latency,
        event_queue=event_queue,
    )
    run_manager = RunManager(
        engine=engine,
        injector=injector,
        sessionmaker=db_sessionmaker,
    )

    # --- safety supervisor (kill switch + circuit breaker) ---
    def _emit_supervisor_event(event_dict: dict) -> None:
        # Push the emergency_stop event into the engine event queue so
        # the WS broadcaster relays it to dashboards. Non-blocking enqueue
        # — under sustained overload we drop rather than block the monitor
        # listener fanout.
        try:
            event_queue.put_nowait(event_dict)
        except asyncio.QueueFull:
            logger.warning("event queue full; dropping supervisor event")

    supervisor = SafetySupervisor(
        injector=injector,
        cpu_emergency_threshold_pct=settings.cpu_emergency_threshold_pct,
        mem_emergency_threshold_pct=settings.mem_emergency_threshold_pct,
        max_concurrent_scenarios=settings.max_concurrent_scenarios,
        abort_callback=run_manager.abort_all,
        event_callback=_emit_supervisor_event,
    )
    monitor.add_listener(supervisor.on_snapshot)

    bcast_task = asyncio.create_task(
        broadcaster_task(ws_manager, event_queue, stop_event=ws_stop_event),
        name="chaos.ws-broadcaster",
    )

    # --- expose on app.state ---
    app.state.settings = settings
    app.state.docker_client = docker_client
    app.state.monitor = monitor
    app.state.injector = injector
    app.state.engine = engine
    app.state.run_manager = run_manager
    app.state.supervisor = supervisor
    app.state.db_engine = db_engine
    app.state.db_sessionmaker = db_sessionmaker
    app.state.ws_manager = ws_manager
    app.state.event_queue = event_queue

    logger.info(
        "framework lifespan started (interval=%ss history=%s)",
        settings.metrics_collection_interval_seconds,
        settings.metrics_history_size,
    )
    try:
        yield
    finally:
        ws_stop_event.set()
        bcast_task.cancel()
        with suppress(asyncio.CancelledError):
            await bcast_task
        try:
            await run_manager.abort_all()
        except Exception:  # noqa: BLE001
            logger.exception("abort_all on shutdown failed")
        await monitor.stop()
        docker_client.close()
        await db_engine.dispose()
        set_monitor(None)
        logger.info("framework lifespan stopped")


def create_app() -> FastAPI:
    # Configure structlog with the log level from settings BEFORE the app
    # is instantiated so any import-time logger acquisition picks up the
    # JSON renderer / wrapper class we want.
    settings = get_settings()
    configure_logging(settings.log_level)

    app = FastAPI(
        title="Chaos Testing Framework",
        version="0.1.0",
        lifespan=lifespan,
    )
    # Stamp every request with a UUID + structlog-bound contextvars so
    # log lines emitted during the request carry ``request_id`` for
    # cross-line correlation.
    app.middleware("http")(request_id_middleware)
    app.include_router(health_router)
    app.include_router(experiments_router)
    app.include_router(runs_router)
    app.include_router(targets_router)
    app.include_router(admin_router)
    app.include_router(ws_router)
    app.include_router(metrics_router)
    return app


app = create_app()
