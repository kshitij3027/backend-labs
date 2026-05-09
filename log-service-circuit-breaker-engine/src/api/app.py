"""FastAPI app factory and lifespan."""
from __future__ import annotations
import asyncio
import os
import time
from collections import deque
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Deque, Optional

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.alerts import StateChangeAlerter
from src.breaker import CircuitBreaker
from src.config import (
    CircuitBreakerConfig,
    critical_service_config,
    load_config_from_env,
    standard_service_config,
)
from src.failure_injection import FailureInjector
from src.logging_setup import configure_logging
from src.registry import CircuitBreakerRegistry
from src.services.database import DatabaseService
from src.services.external_api import ExternalAPIService
from src.services.log_processor import LogProcessorService
from src.services.queue import MessageQueueService
from src.api.prometheus import PrometheusMetrics, state_change_metric_listener
from src.api.routes import router
from src.api.websocket import ConnectionManager, get_broadcast_interval, metrics_broadcaster

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_DIR = PROJECT_ROOT / "templates"
STATIC_DIR = PROJECT_ROOT / "web" / "static"

# History ring buffer fed by the WebSocket broadcaster (Commit 12).
HISTORY_MAX = 300


class MetricsHistory:
    def __init__(self, maxlen: int = HISTORY_MAX) -> None:
        self._buf: Deque[dict] = deque(maxlen=maxlen)

    def append(self, snapshot: dict) -> None:
        self._buf.append(snapshot)

    def list(self) -> list[dict]:
        return list(self._buf)


def _build_registry_and_processor():
    registry = CircuitBreakerRegistry()

    primary_cfg = critical_service_config("database_primary")
    backup_cfg = standard_service_config("database_backup")
    queue_cfg = standard_service_config("queue_main")
    api_cfg = standard_service_config("external_api")

    # Apply env-var overrides on top of presets, if env vars are set.
    env_cfg = load_config_from_env("__env_template__")
    for cfg in (primary_cfg, backup_cfg, queue_cfg, api_cfg):
        # mutate values that env actually wants overridden:
        if os.getenv("CB_DEFAULT_RECOVERY_TIMEOUT"):
            cfg.recovery_timeout = env_cfg.recovery_timeout
        if os.getenv("CB_DEFAULT_TIMEOUT_DURATION"):
            cfg.timeout_duration = env_cfg.timeout_duration

    primary_br = registry.register(primary_cfg)
    backup_br = registry.register(backup_cfg)
    queue_br = registry.register(queue_cfg)
    api_br = registry.register(api_cfg)

    primary_db = DatabaseService("database_primary", primary_br, FailureInjector())
    backup_db = DatabaseService("database_backup", backup_br, FailureInjector())
    queue_svc = MessageQueueService("queue_main", queue_br, FailureInjector())
    api_svc = ExternalAPIService("external_api", api_br, FailureInjector())

    processor = LogProcessorService(primary_db, backup_db, queue_svc, api_svc)
    return registry, processor, {
        "database_primary": primary_db,
        "database_backup": backup_db,
        "queue_main": queue_svc,
        "external_api": api_svc,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    registry, processor, services = _build_registry_and_processor()
    app.state.registry = registry
    app.state.processor = processor
    app.state.services = services
    app.state.history = MetricsHistory()
    app.state.start_time = time.time()
    app.state.manager = ConnectionManager()
    app.state.alerter = StateChangeAlerter()
    app.state.prometheus = PrometheusMetrics()
    registry.add_global_listener(app.state.alerter)
    registry.add_global_listener(state_change_metric_listener(app.state.prometheus))
    interval = get_broadcast_interval()
    task = asyncio.create_task(metrics_broadcaster(app, interval=interval))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def create_app() -> FastAPI:
    app = FastAPI(title="Log Service Circuit Breaker Engine", lifespan=lifespan)
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    if TEMPLATES_DIR.exists():
        app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    else:
        app.state.templates = None
    app.include_router(router)
    return app
