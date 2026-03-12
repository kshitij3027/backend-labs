"""Minimal FastAPI application with health endpoint and lifespan."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import get_settings
from src.logging_config import get_logger

logger = get_logger("app")
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Log startup and shutdown events."""
    logger.info(
        "app_starting",
        rabbitmq_host=settings.RABBITMQ_HOST,
        rabbitmq_port=settings.RABBITMQ_PORT,
        dashboard_port=settings.DASHBOARD_PORT,
    )
    yield
    logger.info("app_shutting_down")


app = FastAPI(
    title="Log Consumer Ack Redelivery",
    description="RabbitMQ consumer with ack tracking, retry, and DLQ",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    """Return basic health status and RabbitMQ connection info."""
    return {
        "status": "ok",
        "rabbitmq": {
            "host": settings.RABBITMQ_HOST,
            "port": settings.RABBITMQ_PORT,
            "main_queue": settings.MAIN_QUEUE,
        },
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.app:app",
        host="0.0.0.0",
        port=settings.DASHBOARD_PORT,
        reload=False,
    )
