"""Entry point for the Kafka log consumer application."""
import logging
import signal
import sys
import time

from src.analytics import AnalyticsEngine
from src.config import load_config
from src.consumer import LogConsumer
from src.batch_processor import BatchProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Start the consumer and block until interrupted."""
    settings = load_config()
    logger.info("Configuration loaded — bootstrap=%s, topics=%s",
                settings.bootstrap_servers, settings.topics)

    analytics = AnalyticsEngine(window_seconds=settings.sliding_window_seconds)
    processor = BatchProcessor(analytics=analytics)
    consumer = LogConsumer(settings, on_batch=processor.process_batch)

    # Graceful shutdown handler
    def shutdown(signum, frame):
        logger.info("Received signal %d, shutting down...", signum)
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer.start()
    logger.info("Consumer running. Press Ctrl+C to stop.")

    # Keep main thread alive
    try:
        while consumer.is_running:
            time.sleep(5)
            stats = consumer.stats
            proc_stats = processor.stats
            logger.info(
                "Status — consumed=%d, committed=%d, errors=%d, "
                "batches=%d, throughput=%.1f msg/s, "
                "web=%d, app=%d, error=%d, success_rate=%.1f%%",
                stats["total_consumed"],
                stats["total_committed"],
                stats["total_errors"],
                stats["batches_processed"],
                stats["throughput"],
                proc_stats["web_count"],
                proc_stats["app_count"],
                proc_stats["error_count"],
                proc_stats["success_rate"],
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.stop()


if __name__ == "__main__":
    main()
