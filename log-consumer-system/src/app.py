"""FastAPI application with lifespan context manager."""

from __future__ import annotations

from contextlib import asynccontextmanager

import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI

from src.config import Config
from src.consumer import ConsumerManager
from src.health import HealthMonitor
from src.metrics import MetricsAggregator
from src.processor import LogProcessor

# Module-level globals (set during lifespan)
config: Config = None  # type: ignore[assignment]
redis_client: aioredis.Redis = None  # type: ignore[assignment]
metrics: MetricsAggregator = None  # type: ignore[assignment]
processor: LogProcessor = None  # type: ignore[assignment]
consumer_manager: ConsumerManager = None  # type: ignore[assignment]
health_monitor: HealthMonitor = None  # type: ignore[assignment]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown."""
    global config, redis_client, metrics, processor, consumer_manager, health_monitor

    config = Config.load()
    redis_client = aioredis.from_url(config.redis_url, decode_responses=True)
    metrics = MetricsAggregator(window_sec=config.metrics_window_sec)
    processor = LogProcessor()
    consumer_manager = ConsumerManager(config, processor, metrics)
    health_monitor = HealthMonitor(consumer_manager.get_consumer_stats)

    await consumer_manager.start(redis_client)

    yield

    await consumer_manager.stop()
    await redis_client.aclose()


app = FastAPI(title="Log Consumer System", lifespan=lifespan)


@app.get("/health")
async def health():
    return health_monitor.status()


@app.get("/api/stats")
async def get_stats():
    snapshot = await metrics.snapshot()
    # Add consumer stats
    snapshot.consumers = consumer_manager.get_consumer_stats()
    return snapshot.model_dump()


@app.get("/api/stats/requests")
async def get_requests():
    snapshot = await metrics.snapshot()
    return {
        "total_processed": snapshot.total_processed,
        "requests_per_second": snapshot.requests_per_second,
    }


@app.get("/api/stats/status-codes")
async def get_status_codes():
    snapshot = await metrics.snapshot()
    return {"status_codes": snapshot.status_code_distribution}


@app.get("/api/stats/top-paths")
async def get_top_paths():
    snapshot = await metrics.snapshot()
    return {"top_paths": snapshot.top_paths}


@app.get("/api/stats/top-ips")
async def get_top_ips():
    snapshot = await metrics.snapshot()
    return {"top_ips": snapshot.top_ips}


@app.get("/api/stats/latency")
async def get_latency():
    snapshot = await metrics.snapshot()
    return {"latency_percentiles": snapshot.latency_percentiles}


@app.get("/api/stats/errors")
async def get_errors():
    snapshot = await metrics.snapshot()
    return {
        "total_errors": snapshot.total_errors,
        "error_rate": snapshot.total_errors / max(snapshot.total_processed, 1),
    }


if __name__ == "__main__":
    _startup_config = Config.load()
    uvicorn.run("src.app:app", host="0.0.0.0", port=_startup_config.dashboard_port, reload=False)
