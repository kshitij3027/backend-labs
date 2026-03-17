"""Entry point for the Kafka log consumer with analytics dashboard."""
import logging
import signal
import sys

import uvicorn

from src.analytics import AnalyticsEngine
from src.batch_processor import BatchProcessor
from src.config import load_config
from src.consumer import LogConsumer
from src.dashboard import create_app
from src.redis_store import RedisStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Start the consumer and dashboard."""
    settings = load_config()
    logger.info(
        "Configuration loaded — bootstrap=%s, topics=%s, port=%d",
        settings.bootstrap_servers,
        settings.topics,
        settings.dashboard_port,
    )

    # Core components
    analytics = AnalyticsEngine(window_seconds=settings.sliding_window_seconds)
    processor = BatchProcessor(analytics=analytics)
    consumer = LogConsumer(settings, on_batch=processor.process_batch)

    # Redis persistence
    redis_store = RedisStore(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
    )
    snapshot = redis_store.load_snapshot()
    if snapshot:
        logger.info("Loaded analytics snapshot from Redis: %s", list(snapshot.keys()))

    # Build FastAPI app
    app = create_app(settings, analytics, consumer, processor, redis_store)

    # Run uvicorn (consumer starts in dashboard lifespan)
    uvicorn.run(
        app,
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
