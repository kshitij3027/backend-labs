"""Reliable RabbitMQ consumer with ack tracking, retry, and DLQ support."""

import json
import signal
import threading
import time
import uuid
from typing import Any

import pika
import pika.exceptions

from src.ack_tracker import AckTracker
from src.config import Settings
from src.log_processor import FatalProcessingError, LogProcessor, ProcessingError
from src.logging_config import get_logger
from src.redelivery_handler import RedeliveryHandler

logger = get_logger(__name__)


class ReliableConsumer:
    """Consumes messages from RabbitMQ with explicit ack, retry, and DLQ.

    Key design decisions
    --------------------
    * **Ack-then-republish** for retries -- we *never* use
      ``basic_nack(requeue=True)`` because requeued messages lose ordering
      guarantees and can cause infinite loops.
    * **Thread safety** -- pika's ``BlockingConnection`` is *not* thread-safe.
      The timeout monitor thread uses
      ``connection.add_callback_threadsafe()`` to schedule nacks on pika's
      IO thread.
    * **Graceful shutdown** -- SIGINT / SIGTERM set a threading Event which
      is checked each iteration of the consume loop.
    """

    def __init__(
        self,
        config: Settings,
        ack_tracker: AckTracker,
        redelivery_handler: RedeliveryHandler,
        processor: LogProcessor,
    ) -> None:
        self.config = config
        self.ack_tracker = ack_tracker
        self.redelivery = redelivery_handler
        self.processor = processor

        self._connection: Any = None
        self._channel: Any = None
        self._shutdown_event = threading.Event()
        self._is_connected = False
        self._reconnect_attempts = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Main entry point.  Outer reconnection loop with signal handlers."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info("consumer_starting")

        while not self._shutdown_event.is_set():
            try:
                self._connect()
                self._setup_channel()
                self._start_timeout_monitor()
                logger.info("consumer_ready", queue=self.config.MAIN_QUEUE)
                self._consume_loop()
            except pika.exceptions.AMQPConnectionError as exc:
                logger.error("connection_lost", error=str(exc))
                self._is_connected = False
                if not self._shutdown_event.is_set():
                    self._reconnect_backoff()
            except Exception as exc:  # noqa: BLE001
                logger.error("unexpected_error", error=str(exc))
                self._is_connected = False
                if not self._shutdown_event.is_set():
                    self._reconnect_backoff()

        logger.info("consumer_stopped")

    def shutdown(self) -> None:
        """Trigger a graceful shutdown of the consumer."""
        logger.info("shutdown_requested")
        self._shutdown_event.set()
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception:  # noqa: BLE001
            pass
        self._is_connected = False

    @property
    def is_connected(self) -> bool:  # noqa: D401
        """Whether the consumer currently has an open connection."""
        return self._is_connected

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """Create a new ``pika.BlockingConnection``."""
        credentials = pika.PlainCredentials(
            self.config.RABBITMQ_USER,
            self.config.RABBITMQ_PASS,
        )
        params = pika.ConnectionParameters(
            host=self.config.RABBITMQ_HOST,
            port=self.config.RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        self._connection = pika.BlockingConnection(params)
        self._is_connected = True
        self._reconnect_attempts = 0
        logger.info(
            "connected",
            host=self.config.RABBITMQ_HOST,
            port=self.config.RABBITMQ_PORT,
        )

    def _setup_channel(self) -> None:
        """Open channel, set prefetch, declare infrastructure, start consuming."""
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=self.config.PREFETCH_COUNT)
        self.redelivery.declare_infrastructure(self._channel)
        self._channel.basic_consume(
            queue=self.config.MAIN_QUEUE,
            on_message_callback=self._on_message,
        )
        logger.info("channel_ready", prefetch=self.config.PREFETCH_COUNT)

    def _consume_loop(self) -> None:
        """Block on ``process_data_events`` with a 1-second poll, checking for shutdown."""
        while not self._shutdown_event.is_set():
            self._connection.process_data_events(time_limit=1)

    def _reconnect_backoff(self) -> None:
        """Sleep with exponential back-off: 1 s, 2 s, 4 s, ... up to 30 s."""
        delay = min(2 ** self._reconnect_attempts, 30)
        logger.info(
            "reconnecting",
            delay_sec=delay,
            attempt=self._reconnect_attempts + 1,
        )
        # Use the shutdown event as the sleep so we can bail early
        self._shutdown_event.wait(timeout=delay)
        self._reconnect_attempts += 1

    # ------------------------------------------------------------------
    # Message handling (core ack / retry / DLQ logic)
    # ------------------------------------------------------------------

    def _on_message(
        self,
        channel: Any,
        method: Any,
        properties: Any,
        body: bytes,
    ) -> None:
        """Callback invoked for every message delivered by RabbitMQ.

        Flow
        ----
        1. Parse JSON body; extract or generate a ``msg_id``.
        2. Read ``x-retry-count`` from headers.
        3. Track & mark processing in :class:`AckTracker`.
        4. Attempt processing via :class:`LogProcessor`.
        5. On success  -> ack + mark acknowledged.
        6. On retryable error -> ack-then-republish (retry or DLQ).
        7. On fatal error -> ack + DLQ immediately.
        8. On JSON parse failure -> ack + DLQ.
        """
        delivery_tag = method.delivery_tag

        # --- 1. Parse body ---------------------------------------------------
        try:
            message = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("json_parse_error", error=str(exc), delivery_tag=delivery_tag)
            channel.basic_ack(delivery_tag=delivery_tag)
            self.redelivery.publish_to_dlq(
                channel, body, properties, f"json_parse_error: {exc}"
            )
            return

        msg_id = message.get("id") or str(uuid.uuid4())

        # --- 2. Retry count ---------------------------------------------------
        retry_count = self.redelivery.get_retry_count(properties)

        # --- 3. Track ---------------------------------------------------------
        self.ack_tracker.start_tracking(msg_id, delivery_tag)
        self.ack_tracker.mark_processing(msg_id)

        logger.info(
            "message_received",
            msg_id=msg_id,
            delivery_tag=delivery_tag,
            retry_count=retry_count,
        )

        # --- 4-8. Process -----------------------------------------------------
        try:
            self.processor.process(message)

            # Success
            channel.basic_ack(delivery_tag=delivery_tag)
            self.ack_tracker.mark_acknowledged(msg_id)
            logger.info("message_acked", msg_id=msg_id)

        except FatalProcessingError as exc:
            # Non-retryable -- ack then DLQ
            channel.basic_ack(delivery_tag=delivery_tag)
            self.ack_tracker.mark_dead_lettered(msg_id)
            self.redelivery.publish_to_dlq(
                channel, body, properties, str(exc)
            )
            logger.warning("message_fatal_dlq", msg_id=msg_id, error=str(exc))

        except ProcessingError as exc:
            # Retryable -- ack first (ack-then-republish pattern)
            channel.basic_ack(delivery_tag=delivery_tag)

            if self.redelivery.should_retry(retry_count):
                self.ack_tracker.mark_retrying(msg_id)
                self.redelivery.publish_to_retry(
                    channel, body, properties, retry_count
                )
                logger.info(
                    "message_retrying",
                    msg_id=msg_id,
                    retry_count=retry_count + 1,
                    error=str(exc),
                )
            else:
                self.ack_tracker.mark_dead_lettered(msg_id)
                self.redelivery.publish_to_dlq(
                    channel, body, properties, f"max_retries_exceeded: {exc}"
                )
                logger.warning(
                    "message_max_retries_dlq",
                    msg_id=msg_id,
                    retry_count=retry_count,
                    error=str(exc),
                )

        except Exception as exc:  # noqa: BLE001
            # Unexpected error -- treat as retryable
            channel.basic_ack(delivery_tag=delivery_tag)
            if self.redelivery.should_retry(retry_count):
                self.ack_tracker.mark_retrying(msg_id)
                self.redelivery.publish_to_retry(
                    channel, body, properties, retry_count
                )
            else:
                self.ack_tracker.mark_dead_lettered(msg_id)
                self.redelivery.publish_to_dlq(
                    channel, body, properties, f"unexpected_error: {exc}"
                )
            logger.error("message_unexpected_error", msg_id=msg_id, error=str(exc))

    # ------------------------------------------------------------------
    # Timeout monitor (runs on a daemon thread)
    # ------------------------------------------------------------------

    def _start_timeout_monitor(self) -> None:
        """Spawn a daemon thread that periodically checks for timed-out messages."""
        thread = threading.Thread(target=self._timeout_monitor, daemon=True)
        thread.start()
        logger.info("timeout_monitor_started", interval_sec=5)

    def _timeout_monitor(self) -> None:
        """Check every 5 s for messages stuck in PROCESSING beyond the ack timeout.

        For each timed-out record we:
        1. Mark it as FAILED in the tracker.
        2. Schedule a ``basic_nack`` on pika's IO thread via
           ``connection.add_callback_threadsafe``.
        """
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(5)
            if self._shutdown_event.is_set():
                break

            timed_out = self.ack_tracker.get_timed_out(self.config.ACK_TIMEOUT_SEC)
            for record in timed_out:
                logger.warning(
                    "ack_timeout",
                    msg_id=record.msg_id,
                    delivery_tag=record.delivery_tag,
                )
                self.ack_tracker.mark_failed(record.msg_id, "ack_timeout")
                if self._connection and self._connection.is_open:
                    self._connection.add_callback_threadsafe(
                        lambda tag=record.delivery_tag: self._channel.basic_nack(
                            delivery_tag=tag, requeue=False
                        )
                    )

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _signal_handler(self, signum: int, frame: Any) -> None:
        logger.info("signal_received", signal=signum)
        self.shutdown()
