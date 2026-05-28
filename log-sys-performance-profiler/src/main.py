import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import src.optimizations.batch_writer  # noqa: F401 — registers
import src.optimizations.object_pool  # noqa: F401 — registers
import src.optimizations.fsm_parser  # noqa: F401 — registers
import src.optimizations.precompiled_validator  # noqa: F401 — registers
import src.optimizations.async_io_variant  # noqa: F401 — registers
import src.optimizations.mmap_reader  # noqa: F401 — registers
from src.api.routes_compare import router as compare_router
from src.api.routes_dashboard import router as dashboard_router
from src.api.routes_metrics import router as metrics_router
from src.api.routes_optimizations import router as optimizations_router
from src.api.routes_runs import router as runs_router
from src.benchmark.harness import BeforeAfterHarness
from src.instrumentation.decorator import set_collector
from src.loadgen.runner import LoadRunner
from src.logging_config import configure_logging, get_logger
from src.metrics.collector import MetricsCollector
from src.metrics.ring_buffer import RingBuffer
from src.resource_sampler.sampler import ResourceSampler
from src.settings import get_settings
from src.store.run_store import RunStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("main")

    buffer = RingBuffer(maxlen=settings.metrics_buffer_size)
    collector = MetricsCollector(buffer=buffer, batch_size=settings.metrics_batch_size)
    stages = settings.instrumented_stages_list
    sampler = ResourceSampler(
        stages=stages,
        interval_sec=settings.resource_sampler_interval_sec,
    )
    collector.set_resource_lookup(sampler.latest_for)
    run_store = RunStore()
    runner = LoadRunner(buffer=buffer, sampler=sampler, settings=settings)
    harness = BeforeAfterHarness(runner=runner, store=run_store, settings=settings)

    set_collector(collector)

    app.state.settings = settings
    app.state.buffer = buffer
    app.state.collector = collector
    app.state.sampler = sampler
    app.state.runner = runner
    app.state.run_store = run_store
    app.state.harness = harness
    app.state.templates = Jinja2Templates(directory="dashboard/templates")

    drain_task = asyncio.create_task(collector.drain_loop())
    sample_task = asyncio.create_task(sampler.sample_loop())
    logger.info("profiler_starting", port=settings.profiler_port)

    try:
        yield
    finally:
        for t in (drain_task, sample_task):
            t.cancel()
        for t in (drain_task, sample_task):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        set_collector(None)
        logger.info("profiler_shutdown")


app = FastAPI(title="Log Pipeline Performance Profiler", lifespan=lifespan)
app.include_router(runs_router)
app.include_router(metrics_router)
app.include_router(compare_router)
app.include_router(optimizations_router)
app.include_router(dashboard_router)
app.mount("/static", StaticFiles(directory="dashboard/static"), name="static")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
