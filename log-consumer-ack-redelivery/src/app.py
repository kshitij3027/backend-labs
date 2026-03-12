"""FastAPI dashboard with consumer lifecycle management."""

import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.ack_tracker import AckTracker
from src.config import Settings, get_settings
from src.log_processor import LogProcessor
from src.logging_config import get_logger
from src.redelivery_handler import RedeliveryHandler
from src.reliable_consumer import ReliableConsumer

logger = get_logger("app")
settings = get_settings()

# Global references for the consumer components
_ack_tracker: AckTracker | None = None
_consumer: ReliableConsumer | None = None
_consumer_thread: threading.Thread | None = None

templates = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the consumer in a background thread on startup."""
    global _ack_tracker, _consumer, _consumer_thread

    config = get_settings()
    _ack_tracker = AckTracker()
    redelivery = RedeliveryHandler(config)
    processor = LogProcessor(failure_rate=config.FAILURE_RATE, timeout_rate=config.TIMEOUT_RATE)
    _consumer = ReliableConsumer(config, _ack_tracker, redelivery, processor)

    _consumer_thread = threading.Thread(target=_consumer.start, daemon=True)
    _consumer_thread.start()

    logger.info("app_started", rabbitmq_host=config.RABBITMQ_HOST)
    yield

    if _consumer:
        _consumer.shutdown()
    logger.info("app_shutting_down")


app = FastAPI(
    title="Log Consumer Ack Redelivery",
    description="RabbitMQ consumer with ack tracking, retry, and DLQ",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Render the monitoring dashboard."""
    stats = _ack_tracker.get_stats() if _ack_tracker else None
    is_connected = _consumer.is_connected if _consumer else False
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "stats": stats, "is_connected": is_connected},
    )


@app.get("/api/stats")
async def api_stats():
    """Return current stats as JSON."""
    if _ack_tracker is None:
        return {"error": "not_initialized"}
    stats = _ack_tracker.get_stats()
    return {
        "total_received": stats.total_received,
        "total_acked": stats.total_acked,
        "total_failed": stats.total_failed,
        "total_retried": stats.total_retried,
        "total_dead_lettered": stats.total_dead_lettered,
        "pending_count": stats.pending_count,
        "processing_count": stats.processing_count,
        "success_rate": round(stats.success_rate, 2),
        "is_connected": _consumer.is_connected if _consumer else False,
    }


@app.get("/health")
async def health():
    """Return health status."""
    return {
        "status": "ok",
        "rabbitmq": {
            "host": settings.RABBITMQ_HOST,
            "port": settings.RABBITMQ_PORT,
            "main_queue": settings.MAIN_QUEUE,
        },
        "consumer_connected": _consumer.is_connected if _consumer else False,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.app:app", host="0.0.0.0", port=settings.DASHBOARD_PORT, reload=False)
