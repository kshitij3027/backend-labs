"""FastAPI web dashboard with REST API and WebSocket for real-time monitoring."""
import asyncio
import logging
import threading
import time

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse

from src.config import Settings, load_config
from src.consumer.consumer_group import ConsumerGroupCoordinator
from src.monitoring.metrics import MetricsCollector
from src.monitoring.websocket_manager import ConnectionManager
from src.producer.log_generator import LogGenerator
from src.producer.smart_producer import SmartProducer

logger = logging.getLogger(__name__)

# Module-level state
_metrics: MetricsCollector | None = None
_coordinator: ConsumerGroupCoordinator | None = None
_producer: SmartProducer | None = None
_manager: ConnectionManager | None = None
_settings: Settings | None = None
_shutdown: threading.Event | None = None
_auto_scaler = None


def _producer_loop(settings: Settings, producer: SmartProducer, generator: LogGenerator, shutdown: threading.Event) -> None:
    """Run the producer in a background thread."""
    interval = 1.0 / settings.producer_rate if settings.producer_rate > 0 else 1.0
    start_time = time.time()
    while not shutdown.is_set():
        if settings.duration > 0 and (time.time() - start_time) >= settings.duration:
            break
        entry = generator.generate_one()
        producer.produce(entry)
        shutdown.wait(interval)
    producer.flush()
    logger.info("Producer loop ended. Stats: %s", producer.stats)


def create_app(settings: Settings | None = None, num_consumers: int | None = None,
               rate: int | None = None, duration: int | None = None,
               auto_scale: bool = False) -> FastAPI:
    """Build and return the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _metrics, _coordinator, _producer, _manager, _settings, _shutdown, _auto_scaler

        _settings = settings or load_config()
        if num_consumers is not None:
            _settings.num_consumers = num_consumers
        if rate is not None:
            _settings.producer_rate = rate
        if duration is not None:
            _settings.duration = duration

        _metrics = MetricsCollector()
        _manager = ConnectionManager()
        _shutdown = threading.Event()

        # Start consumer group
        _coordinator = ConsumerGroupCoordinator(_settings, _metrics)
        _coordinator.start()
        logger.info("Consumer group started with %d consumers", _settings.num_consumers)

        # Start auto-scaler if enabled
        if auto_scale:
            from src.consumer.auto_scaler import AutoScaler
            _auto_scaler = AutoScaler(
                _settings, _metrics,
                add_fn=_coordinator.add_consumer,
                remove_fn=_coordinator.remove_consumer,
                count_fn=lambda: _coordinator.active_consumers,
            )
            _auto_scaler.start()
            logger.info("Auto-scaler enabled")

        # Start producer thread
        _producer = SmartProducer(_settings)
        generator = LogGenerator(_settings)
        producer_thread = threading.Thread(
            target=_producer_loop,
            args=(_settings, _producer, generator, _shutdown),
            daemon=True,
        )
        producer_thread.start()
        logger.info("Producer started at %d msg/s", _settings.producer_rate)

        # Start WebSocket broadcast loop
        broadcast_task = asyncio.create_task(_ws_broadcast_loop())

        yield

        # Shutdown
        broadcast_task.cancel()
        try:
            await broadcast_task
        except asyncio.CancelledError:
            pass
        if _auto_scaler is not None:
            _auto_scaler.stop()
        _shutdown.set()
        producer_thread.join(timeout=5)
        _coordinator.stop()
        logger.info("Dashboard shutdown complete")

    app = FastAPI(title="Kafka Partitioning & Consumer Group", lifespan=lifespan)

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_page():
        html_path = Path("static/dashboard.html")
        return HTMLResponse(content=html_path.read_text())

    @app.get("/health")
    async def health():
        return {
            "status": "ok",
            "consumers": _coordinator.active_consumers if _coordinator else 0,
            "uptime": round(time.time() - _metrics._start_time, 1) if _metrics else 0,
        }

    @app.get("/api/stats")
    async def stats():
        if _metrics is None:
            return {}
        snap = _metrics.snapshot()
        snap["producer"] = _producer.stats if _producer else {}
        return snap

    @app.get("/api/partitions")
    async def partitions():
        if _metrics is None:
            return {}
        snap = _metrics.snapshot()
        return {
            "per_partition": snap["per_partition"],
            "per_consumer": snap["per_consumer"],
            "num_partitions": _settings.num_partitions if _settings else 6,
        }

    @app.get("/api/scaling-history")
    async def scaling_history():
        if _auto_scaler is None:
            return {"history": [], "enabled": False}
        return {"history": _auto_scaler.scaling_history, "enabled": True}

    @app.get("/api/lag")
    async def lag():
        if _metrics is None:
            return {}
        snap = _metrics.snapshot()
        return {"lag": snap.get("lag", {}), "total_lag": sum(snap.get("lag", {}).values())}

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await _manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            _manager.disconnect(websocket)

    return app


async def _ws_broadcast_loop() -> None:
    """Broadcast metrics to all WebSocket clients every second."""
    while True:
        await asyncio.sleep(1.0)
        if _metrics is not None and _manager is not None:
            snap = _metrics.snapshot()
            snap["producer"] = _producer.stats if _producer else {}
            if _auto_scaler is not None:
                snap["scaling_history"] = _auto_scaler.scaling_history
            await _manager.broadcast(snap)
