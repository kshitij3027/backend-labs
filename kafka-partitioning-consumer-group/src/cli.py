"""CLI entry point for the Kafka partitioning consumer group system."""
import logging
import signal
import sys
import threading
import time

import click

from src.config import Settings, load_config
from src.consumer.consumer_group import ConsumerGroupCoordinator
from src.monitoring.metrics import MetricsCollector
from src.producer.log_generator import LogGenerator
from src.producer.smart_producer import SmartProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Kafka Partitioning & Consumer Group Log Processor."""
    pass


@cli.command()
@click.option("--consumers", type=int, default=None, help="Number of consumers")
@click.option("--rate", type=int, default=None, help="Messages per second")
@click.option("--duration", type=int, default=None, help="Duration in seconds (0=infinite)")
@click.option("--mode", type=click.Choice(["cli", "web"]), default="cli", help="Dashboard mode")
def run(consumers, rate, duration, mode):
    """Run the producer and consumer group."""
    settings = load_config()
    if consumers is not None:
        settings.num_consumers = consumers
    if rate is not None:
        settings.producer_rate = rate
    if duration is not None:
        settings.duration = duration

    logger.info(
        "Starting system: mode=%s, consumers=%d, rate=%d msg/s, duration=%s",
        mode, settings.num_consumers, settings.producer_rate,
        f"{settings.duration}s" if settings.duration > 0 else "infinite",
    )

    if mode == "web":
        from src.monitoring.web_dashboard import create_app
        import uvicorn
        app = create_app(settings, num_consumers=consumers, rate=rate, duration=duration)
        uvicorn.run(app, host=settings.dashboard_host, port=settings.dashboard_port, log_level="info")
        return

    # CLI mode
    metrics = MetricsCollector()
    shutdown = threading.Event()

    def signal_handler(sig, frame):
        logger.info("Received signal %s, shutting down...", sig)
        shutdown.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start consumers
    coordinator = ConsumerGroupCoordinator(settings, metrics)
    coordinator.start()
    logger.info("Consumer group started with %d consumers", settings.num_consumers)

    # Start producer
    producer = SmartProducer(settings)
    generator = LogGenerator(settings)
    logger.info("Producer started at %d msg/s", settings.producer_rate)

    interval = 1.0 / settings.producer_rate if settings.producer_rate > 0 else 1.0
    start_time = time.time()
    produced_count = 0

    try:
        while not shutdown.is_set():
            # Check duration
            if settings.duration > 0 and (time.time() - start_time) >= settings.duration:
                logger.info("Duration reached (%ds), stopping...", settings.duration)
                break

            # Produce a message
            entry = generator.generate_one()
            producer.produce(entry)
            produced_count += 1

            if produced_count % 100 == 0:
                snap = metrics.snapshot()
                logger.info(
                    "Progress: produced=%d, consumed=%d, errors=%d",
                    produced_count, snap["total_consumed"], snap["total_errors"],
                )

            # Rate limiting
            shutdown.wait(interval)

    except Exception as e:
        logger.error("Error in main loop: %s", e)
    finally:
        logger.info("Shutting down...")
        producer.flush()
        logger.info("Producer flushed. Stats: %s", producer.stats)
        coordinator.stop()
        snap = metrics.snapshot()
        logger.info(
            "Final: consumed=%d, errors=%d, partitions=%s",
            snap["total_consumed"], snap["total_errors"], snap["per_partition"],
        )
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    cli()
