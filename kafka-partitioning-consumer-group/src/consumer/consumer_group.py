"""Consumer group coordinator managing multiple consumer threads."""
import logging
import threading
import time
from confluent_kafka import Consumer, KafkaError, KafkaException
from src.config import Settings
from src.consumer.log_processor import LogProcessor
from src.consumer.rebalance_handler import RebalanceHandler
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class ConsumerGroupCoordinator:
    """Manages a group of consumer threads consuming from Kafka."""

    def __init__(self, settings: Settings, metrics: MetricsCollector) -> None:
        self._settings = settings
        self._metrics = metrics
        self._shutdown = threading.Event()
        self._threads: list[threading.Thread] = []
        self._consumers: list[Consumer] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start all consumer threads."""
        for i in range(self._settings.num_consumers):
            consumer_id = f"consumer-{i}"
            t = threading.Thread(
                target=self._consumer_loop,
                args=(consumer_id,),
                name=consumer_id,
                daemon=True,
            )
            self._threads.append(t)
            t.start()
            logger.info("Started %s", consumer_id)

    def stop(self) -> None:
        """Signal all consumers to stop and wait for threads to finish."""
        logger.info("Stopping consumer group...")
        self._shutdown.set()
        for t in self._threads:
            t.join(timeout=10)
        with self._lock:
            for c in self._consumers:
                try:
                    c.close()
                except Exception:
                    pass
            self._consumers.clear()
        self._threads.clear()
        logger.info("Consumer group stopped.")

    @property
    def is_running(self) -> bool:
        return not self._shutdown.is_set()

    @property
    def active_consumers(self) -> int:
        return len([t for t in self._threads if t.is_alive()])

    def _consumer_loop(self, consumer_id: str) -> None:
        """Main loop for a single consumer thread."""
        consumer = Consumer({
            "bootstrap.servers": self._settings.bootstrap_servers,
            "group.id": self._settings.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": self._settings.session_timeout_ms,
            "heartbeat.interval.ms": self._settings.heartbeat_interval_ms,
            "partition.assignment.strategy": "cooperative-sticky",
        })

        with self._lock:
            self._consumers.append(consumer)

        processor = LogProcessor(consumer_id, self._metrics)
        handler = RebalanceHandler(consumer, consumer_id, self._metrics)

        consumer.subscribe(
            [self._settings.topic],
            on_assign=handler.on_assign,
            on_revoke=handler.on_revoke,
        )

        batch_count = 0
        commit_interval = 50  # Commit every N messages

        try:
            while not self._shutdown.is_set():
                try:
                    msg = consumer.poll(timeout=1.0)
                except KafkaException as e:
                    logger.error("Consumer %s poll error: %s", consumer_id, e)
                    self._metrics.record_error(consumer_id)
                    time.sleep(0.5)
                    continue

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer %s error: %s", consumer_id, msg.error())
                    self._metrics.record_error(consumer_id)
                    continue

                entry = processor.process(msg)
                if entry:
                    batch_count += 1
                    if batch_count >= commit_interval:
                        try:
                            consumer.commit(asynchronous=False)
                        except KafkaException as e:
                            logger.warning("Consumer %s commit failed, will retry: %s", consumer_id, e)
                        batch_count = 0

            # Final commit before exiting
            try:
                consumer.commit(asynchronous=False)
            except Exception:
                pass
        except Exception as e:
            logger.error("Consumer %s crashed: %s", consumer_id, e)
        finally:
            logger.info("Consumer %s shutting down", consumer_id)
