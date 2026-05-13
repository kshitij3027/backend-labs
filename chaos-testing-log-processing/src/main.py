"""FastAPI app factory + lifespan wiring."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api.routes_admin import router as admin_router
from .api.routes_experiments import router as experiments_router
from .api.routes_health import router as health_router
from .api.routes_runs import router as runs_router
from .api.routes_targets import router as targets_router
from .config.settings import get_settings
from .docker_client.client import DockerClient
from .engine.experiment_engine import (
    ExperimentEngine,
    default_probes_for_latency,
)
from .engine.run_manager import RunManager
from .injection.injector import FailureInjector
from .monitoring.system_monitor import SystemMonitor, set_monitor
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
    await monitor.start()

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
    )
    run_manager = RunManager(
        engine=engine,
        injector=injector,
        sessionmaker=db_sessionmaker,
    )

    # --- expose on app.state ---
    app.state.settings = settings
    app.state.docker_client = docker_client
    app.state.monitor = monitor
    app.state.injector = injector
    app.state.engine = engine
    app.state.run_manager = run_manager
    app.state.db_engine = db_engine
    app.state.db_sessionmaker = db_sessionmaker

    logger.info(
        "framework lifespan started (interval=%ss history=%s)",
        settings.metrics_collection_interval_seconds,
        settings.metrics_history_size,
    )
    try:
        yield
    finally:
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
    app = FastAPI(
        title="Chaos Testing Framework",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(health_router)
    app.include_router(experiments_router)
    app.include_router(runs_router)
    app.include_router(targets_router)
    app.include_router(admin_router)
    return app


app = create_app()
