import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

from src.admission import Admission
from src.aimd import AIMDLimiter
from src.breaker import CircuitBreaker
from src.config import Settings, get_settings
from src.load_tester import InternalLoadTester
from src.logging_setup import configure_logging, get_logger
from src.metrics_core import PressureFuser
from src.pressure_sensor import PressureSensor
from src.processor import WorkerPool
from src.queues import PriorityQueues
from src.state_machine import BackpressureManager
from src.upstream_breaker import UpstreamBreaker


class AppState:
    """Holds every long-lived component for the app's lifespan."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.queues = PriorityQueues(settings)
        self.fuser = PressureFuser(alpha=settings.ewma_alpha, history_size=settings.pressure_history_size)
        self.manager = BackpressureManager(settings)
        self.aimd = AIMDLimiter(
            initial_limit=settings.max_queue_size,
            beta=settings.aimd_beta,
            additive=1,
            ai_period_ticks=settings.ai_period_ticks,
            jitter=settings.retry_after_jitter,
        )
        self.upstream = UpstreamBreaker(settings)
        self.admission = Admission(settings, self.aimd, self.upstream)
        self.breaker = CircuitBreaker(name="downstream-sim", failure_threshold=5, recovery_timeout=30.0)
        self.sensor = PressureSensor(self.queues, self.fuser, self.manager, self.aimd, self.upstream, settings)
        self.workers = WorkerPool(self.queues, self.breaker, settings)
        self.load_tester = InternalLoadTester(self.admission, self.queues, self.manager, settings)
        self._bumper_task: asyncio.Task | None = None

    def start(self) -> None:
        import time
        self.sensor.start()
        self.workers.start()
        self._bumper_task = asyncio.create_task(
            self.queues.bump_aged_items(now_fn=time.monotonic),
            name="anti-starvation-bumper",
        )

    async def stop(self) -> None:
        try:
            await self.load_tester.stop()
        except Exception:
            pass
        if self._bumper_task is not None:
            self._bumper_task.cancel()
            try:
                await self._bumper_task
            except (asyncio.CancelledError, Exception):
                pass
            self._bumper_task = None
        await self.sensor.stop()
        await self.workers.stop()


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)
    state = AppState(settings)
    app.state.components = state
    state.start()
    log = get_logger("app")
    log.info("startup_complete", workers=settings.worker_count, sampling_interval=settings.sampling_interval)
    try:
        yield
    finally:
        await state.stop()
        log.info("shutdown_complete")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Adaptive Backpressure Manager",
        version="0.1.0",
        lifespan=_lifespan,
    )

    @app.get("/system/health", tags=["health"])
    def health() -> dict:
        return {"status": "ok"}

    from src.api.routes import router as v1_router, _metrics
    app.include_router(v1_router, prefix="/api/v1")

    @app.get("/metrics")
    async def root_metrics(request: Request) -> PlainTextResponse:
        c = request.app.state.components
        _metrics.pressure_score.set(c.fuser.last_score)
        _metrics.throttle_rate.set(c.aimd.throttle_rate)
        _metrics.aimd_limit.set(c.aimd.limit)
        from src.state import Priority
        for p in Priority:
            _metrics.queue_size.labels(priority=p.value).set(c.queues.qsize(p))
        return PlainTextResponse(_metrics.text(), media_type="text/plain; version=0.0.4")

    return app
