"""FastAPI application with REST API, WebSocket, and web dashboard."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from prometheus_client import start_http_server

from src.config import Config
from src.producer import KafkaLogProducer
from src.log_generator import LogGenerator
from src.websocket_manager import ConnectionManager

logger = logging.getLogger(__name__)

# Module-level state (populated during lifespan)
_producer: KafkaLogProducer | None = None
_generator: LogGenerator | None = None
_manager: ConnectionManager | None = None
_config: Config | None = None

templates = Jinja2Templates(directory="templates")


async def _ws_broadcast_loop() -> None:
    """Periodically broadcast producer stats to all WebSocket clients."""
    while True:
        await asyncio.sleep(_config.ws_interval)
        if _producer is not None:
            stats = _producer.stats
        else:
            stats = {
                "total_sent": 0,
                "total_failed": 0,
                "topic_counts": {},
                "partition_counts": {},
                "success_rate": 0.0,
                "metrics": {
                    "total_sent": 0,
                    "total_failed": 0,
                    "topic_counts": {},
                    "throughput": 0.0,
                    "error_counts": {},
                    "error_rate": 0.0,
                },
            }
        await _manager.broadcast(stats)


def create_app(config: Config | None = None) -> FastAPI:
    """Build and return the FastAPI application."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _producer, _generator, _manager, _config

        _config = config if config is not None else Config()
        _generator = LogGenerator()
        _manager = ConnectionManager()

        # Attempt Kafka connection — dashboard remains usable without it
        try:
            _producer = KafkaLogProducer(_config)
            logger.info("Kafka producer connected")
        except Exception as exc:
            logger.warning("Kafka producer unavailable: %s", exc)
            _producer = None

        # Start Prometheus metrics server
        try:
            start_http_server(_config.prometheus_port)
            logger.info("Prometheus metrics on port %d", _config.prometheus_port)
        except OSError:
            logger.warning("Prometheus port %d already in use", _config.prometheus_port)

        # Background WebSocket broadcaster
        broadcast_task = asyncio.create_task(_ws_broadcast_loop())

        yield

        # Shutdown
        broadcast_task.cancel()
        try:
            await broadcast_task
        except asyncio.CancelledError:
            pass
        if _producer is not None:
            _producer.close()

    app = FastAPI(title="Kafka Log Producer Dashboard", lifespan=lifespan)

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_page(request: Request):
        """Serve the web dashboard."""
        return templates.TemplateResponse("index.html", {"request": request})

    @app.post("/api/send-sample")
    async def send_sample():
        """Generate and send 10 sample log entries."""
        entries = _generator.generate_batch(10)
        if _producer is not None:
            result = _producer.send_logs_batch(entries)
            return {"logs_sent": 10, "logs_failed": 0, **result}
        return {"logs_sent": 0, "logs_failed": 10, "error": "Producer not available"}

    @app.post("/api/send-error-burst")
    async def send_error_burst():
        """Generate and send 5 error/critical log entries."""
        entries = _generator.generate_error_burst(5)
        if _producer is not None:
            result = _producer.send_logs_batch(entries)
            return {"logs_sent": 5, "logs_failed": 0, **result}
        return {"logs_sent": 0, "logs_failed": 5, "error": "Producer not available"}

    @app.get("/api/stats")
    async def get_stats():
        """Return current producer statistics."""
        if _producer is not None:
            return _producer.stats
        return {
            "total_sent": 0,
            "total_failed": 0,
            "topic_counts": {},
            "partition_counts": {},
            "success_rate": 0.0,
            "metrics": {
                "total_sent": 0,
                "total_failed": 0,
                "topic_counts": {},
                "throughput": 0.0,
                "error_counts": {},
                "error_rate": 0.0,
            },
        }

    @app.get("/health")
    async def health_check():
        """Liveness/readiness probe."""
        connected = _producer is not None and not _producer._closed
        return {"status": "ok", "producer_connected": connected}

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        """Stream real-time stats to connected clients."""
        await _manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()  # keep-alive
        except WebSocketDisconnect:
            _manager.disconnect(websocket)

    return app
