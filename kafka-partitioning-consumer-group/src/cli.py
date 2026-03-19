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
    import time as _time
    from src.monitoring.cli_dashboard import CLIDashboard

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

    # Start producer in background thread
    producer = SmartProducer(settings)
    generator = LogGenerator(settings)

    def producer_loop():
        interval = 1.0 / settings.producer_rate if settings.producer_rate > 0 else 1.0
        start_time = _time.time()
        while not shutdown.is_set():
            if settings.duration > 0 and (_time.time() - start_time) >= settings.duration:
                logger.info("Duration reached (%ds)", settings.duration)
                shutdown.set()
                break
            entry = generator.generate_one()
            producer.produce(entry)
            shutdown.wait(interval)
        producer.flush()

    producer_thread = threading.Thread(target=producer_loop, daemon=True)
    producer_thread.start()
    logger.info("Producer started at %d msg/s", settings.producer_rate)

    # Run Rich dashboard in main thread
    dashboard = CLIDashboard(metrics, producer_stats_fn=lambda: producer.stats)
    try:
        dashboard.run(shutdown)
    except KeyboardInterrupt:
        shutdown.set()
    finally:
        logger.info("Shutting down...")
        shutdown.set()
        producer_thread.join(timeout=5)
        coordinator.stop()
        snap = metrics.snapshot()
        logger.info("Final: consumed=%d, errors=%d", snap["total_consumed"], snap["total_errors"])
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    cli()
