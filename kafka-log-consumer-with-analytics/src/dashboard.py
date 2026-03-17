"""FastAPI dashboard with REST API, WebSocket, and web UI."""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from src.analytics import AnalyticsEngine
from src.batch_processor import BatchProcessor
from src.config import Settings
from src.consumer import LogConsumer
from src.redis_store import RedisStore
from src.websocket_manager import ConnectionManager

logger = logging.getLogger(__name__)

# Module-level state (populated during lifespan)
_analytics: AnalyticsEngine | None = None
_consumer: LogConsumer | None = None
_processor: BatchProcessor | None = None
_redis_store: RedisStore | None = None
_manager: ConnectionManager | None = None
_settings: Settings | None = None

templates = Jinja2Templates(directory="templates")


async def _ws_broadcast_loop() -> None:
    """Periodically broadcast analytics stats to all WebSocket clients."""
    while True:
        await asyncio.sleep(_settings.ws_broadcast_interval)
        if _analytics is not None and _manager is not None:
            data = {
                "stats": _analytics.get_stats(),
                "consumer": _consumer.stats if _consumer else {},
                "processor": _processor.stats if _processor else {},
            }
            await _manager.broadcast(data)


async def _redis_snapshot_loop() -> None:
    """Periodically save analytics snapshot to Redis."""
    while True:
        await asyncio.sleep(_settings.snapshot_interval_s)
        if _redis_store and _redis_store.is_connected and _analytics:
            snapshot_data = {
                "stats": _analytics.get_stats(),
                "analytics": _analytics.get_analytics(),
                "consumer": _consumer.stats if _consumer else {},
                "processor": _processor.stats if _processor else {},
            }
            _redis_store.save_snapshot(snapshot_data)


def create_app(
    settings: Settings,
    analytics: AnalyticsEngine,
    consumer: LogConsumer,
    processor: BatchProcessor,
    redis_store: RedisStore,
) -> FastAPI:
    """Build and return the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _analytics, _consumer, _processor, _redis_store, _manager, _settings

        _settings = settings
        _analytics = analytics
        _consumer = consumer
        _processor = processor
        _redis_store = redis_store
        _manager = ConnectionManager()

        # Start consumer thread
        consumer.start()
        logger.info("Consumer thread started via dashboard lifespan")

        # Background tasks
        broadcast_task = asyncio.create_task(_ws_broadcast_loop())
        snapshot_task = asyncio.create_task(_redis_snapshot_loop())

        yield

        # Shutdown
        broadcast_task.cancel()
        snapshot_task.cancel()
        try:
            await broadcast_task
        except asyncio.CancelledError:
            pass
        try:
            await snapshot_task
        except asyncio.CancelledError:
            pass
        consumer.stop()

    app = FastAPI(title="Kafka Log Consumer Analytics", lifespan=lifespan)

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_page(request: Request):
        return templates.TemplateResponse("index.html", {"request": request})

    @app.get("/api/stats")
    async def get_stats():
        """Consumer status, message counts, throughput."""
        consumer_stats = _consumer.stats if _consumer else {}
        processor_stats = _processor.stats if _processor else {}
        analytics_stats = _analytics.get_stats() if _analytics else {}
        return {
            "consumer": consumer_stats,
            "processor": processor_stats,
            "analytics": analytics_stats,
        }

    @app.get("/api/analytics")
    async def get_analytics():
        """Per-endpoint breakdown, percentiles, geo distribution."""
        if _analytics is None:
            return {}
        return _analytics.get_analytics()

    @app.get("/api/metrics")
    async def get_metrics():
        """Throughput history, processing latency, error rates."""
        if _analytics is None:
            return {}
        return _analytics.get_metrics()

    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        consumer_running = _consumer.is_running if _consumer else False
        partitions = len(_consumer.assigned_partitions) if _consumer else 0
        redis_ok = _redis_store.ping() if _redis_store else False
        return {
            "status": "ok" if consumer_running else "degraded",
            "consumer_running": consumer_running,
            "assigned_partitions": partitions,
            "redis_connected": redis_ok,
        }

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await _manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            _manager.disconnect(websocket)

    return app
