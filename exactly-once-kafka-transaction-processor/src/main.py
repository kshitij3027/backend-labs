"""Multiprocessing entrypoint for the exactly-once transaction processor."""

import multiprocessing
import signal
import sys
from dataclasses import asdict

import structlog

from src.config import Settings, load_config
from src.consumer import ExactlyOnceConsumer
from src.dashboard.app import create_app, register_process
from src.monitor import TransactionMonitor
from src.producer import TransactionalProducer

logger = structlog.get_logger(__name__)


def run_producer(config_dict: dict, shutdown_event) -> None:
    """Child process target for the transactional producer."""
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    config = Settings(**config_dict)
    producer = TransactionalProducer(config)
    producer.run(shutdown_event)


def run_consumer(config_dict: dict, shutdown_event) -> None:
    """Child process target for the exactly-once consumer."""
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    config = Settings(**config_dict)
    consumer = ExactlyOnceConsumer(config)
    consumer.run(shutdown_event)


def run_monitor(config_dict: dict, shutdown_event) -> None:
    """Child process target for the transaction monitor."""
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    config = Settings(**config_dict)
    monitor = TransactionMonitor(config)
    monitor.run(shutdown_event)


def main() -> None:
    """Start producer, consumer, monitor as child processes and run the dashboard."""
    config = load_config()
    config_dict = asdict(config)
    shutdown_event = multiprocessing.Event()

    # Spawn child processes
    producer_process = multiprocessing.Process(
        target=run_producer,
        args=(config_dict, shutdown_event),
        name="producer",
        daemon=True,
    )

    # Consumer gets its own shutdown event so crash injection can restart it
    # without affecting other processes
    consumer_shutdown = multiprocessing.Event()
    consumer_process = multiprocessing.Process(
        target=run_consumer,
        args=(config_dict, consumer_shutdown),
        name="consumer",
        daemon=True,
    )

    monitor_process = multiprocessing.Process(
        target=run_monitor,
        args=(config_dict, shutdown_event),
        name="monitor",
        daemon=True,
    )

    processes = [producer_process, consumer_process, monitor_process]

    def shutdown_handler(signum, frame):
        """Handle SIGTERM/SIGINT: signal shutdown, terminate children, wait."""
        sig_name = signal.Signals(signum).name
        logger.info("shutdown_signal_received", signal=sig_name)
        shutdown_event.set()
        consumer_shutdown.set()

        for proc in processes:
            try:
                if proc.is_alive():
                    logger.info("terminating_process", name=proc.name)
                    proc.terminate()
            except Exception:
                pass

        for proc in processes:
            try:
                proc.join(timeout=10)
                if proc.is_alive():
                    logger.warning("force_killing_process", name=proc.name)
                    proc.kill()
            except Exception:
                pass

        logger.info("all_processes_stopped")
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    # Start child processes
    for proc in processes:
        proc.start()
        logger.info("process_started", name=proc.name, pid=proc.pid)

    # Register consumer process for failure injection
    register_process(
        "consumer", consumer_process, run_consumer, config_dict
    )

    # Run Flask dashboard in the main process
    app = create_app(config)
    logger.info("starting_dashboard", port=config.dashboard_port)
    app.run(host="0.0.0.0", port=config.dashboard_port, use_reloader=False)


if __name__ == "__main__":
    main()
