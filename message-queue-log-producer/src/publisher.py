"""Dedicated publisher thread — sole owner of the pika connection."""

import json
import threading
import logging
import queue
import time

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from src.connection import RabbitMQConnection
from src.setup import setup_topology

logger = logging.getLogger(__name__)


class PublisherThread:
    """Daemon thread that reads batches from an internal queue and publishes to RabbitMQ.

    This thread is the SOLE user of the pika connection — no other thread touches it.
    """

    def __init__(self, config, internal_queue, circuit_breaker, fallback, metrics):
        """
        Args:
            config: Config instance
            internal_queue: queue.Queue of batches to publish
            circuit_breaker: CircuitBreaker instance
            fallback: FallbackStorage instance
            metrics: MetricsCollector instance
        """
        self._config = config
        self._queue = internal_queue
        self._cb = circuit_breaker
        self._fallback = fallback
        self._metrics = metrics
        self._conn = None
        self._channel = None
        self._stop_event = threading.Event()
        self._connected = False

        self._thread = threading.Thread(target=self._run, daemon=True, name="publisher")
        self._thread.start()

    def _connect(self):
        """Establish pika connection and set up topology."""
        try:
            rmq_conn = RabbitMQConnection(self._config)
            rmq_conn.connect()
            self._conn = rmq_conn
            self._channel = rmq_conn.get_channel()
            self._channel.confirm_delivery()

            # Set up topology
            try:
                setup_topology(self._channel, self._config)
            except Exception:
                logger.exception("Failed to setup topology (may already exist)")

            self._connected = True
            logger.info("Publisher connected to RabbitMQ")
        except Exception:
            self._connected = False
            logger.exception("Publisher failed to connect to RabbitMQ")

    def _publish_batch(self, batch):
        """Publish a batch of log entries to RabbitMQ."""
        if not self._cb.allow_request():
            # Circuit is open, write to fallback
            self._fallback.write(batch)
            self._metrics.record_fallback_write(len(batch))
            self._metrics.record_batch_flushed()
            return

        if not self._connected:
            self._connect()
            if not self._connected:
                self._cb.record_failure()
                self._fallback.write(batch)
                self._metrics.record_fallback_write(len(batch))
                self._metrics.record_batch_flushed()
                return

        exchange_name = self._config.exchange["name"]
        try:
            for entry in batch:
                routing_key = "log.{}.{}".format(
                    entry.get("level", "unknown"),
                    entry.get("source", "unknown"),
                )
                body = json.dumps(entry)
                self._channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # persistent
                        content_type="application/json",
                    ),
                )
            self._cb.record_success()
            self._metrics.record_published(len(batch))
            self._metrics.record_batch_flushed()
        except (AMQPConnectionError, AMQPChannelError, Exception) as e:
            logger.error("Publish failed: %s", e)
            self._cb.record_failure()
            self._connected = False
            self._fallback.write(batch)
            self._metrics.record_fallback_write(len(batch))
            self._metrics.record_publish_error()
            self._metrics.record_batch_flushed()

    def _drain_fallback(self):
        """If circuit is closed and fallback has data, drain it back to RabbitMQ."""
        if not self._fallback.has_data():
            return
        if not self._cb.allow_request():
            return
        if not self._connected:
            return

        def publish_chunk(chunk):
            exchange_name = self._config.exchange["name"]
            for entry in chunk:
                routing_key = "log.{}.{}".format(
                    entry.get("level", "unknown"),
                    entry.get("source", "unknown"),
                )
                body = json.dumps(entry)
                self._channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                    ),
                )
            self._metrics.record_published(len(chunk))

        try:
            drained = self._fallback.drain(publish_chunk)
            if drained > 0:
                self._metrics.record_fallback_drained(drained)
                self._cb.record_success()
                logger.info("Drained %d entries from fallback", drained)
        except Exception:
            logger.exception("Failed to drain fallback")
            self._cb.record_failure()
            self._connected = False

    def _run(self):
        """Main publisher loop."""
        self._connect()

        while not self._stop_event.is_set():
            try:
                batch = self._queue.get(timeout=0.1)
                self._publish_batch(batch)
            except queue.Empty:
                # No batch available — try draining fallback
                self._drain_fallback()

            # Send heartbeat to keep connection alive
            if self._connected and self._conn:
                try:
                    conn = self._conn._connection
                    if conn and not conn.is_closed:
                        conn.process_data_events(time_limit=0)
                except Exception:
                    self._connected = False

        # Drain remaining items from queue on stop
        while not self._queue.empty():
            try:
                batch = self._queue.get_nowait()
                self._publish_batch(batch)
            except queue.Empty:
                break

    def stop(self):
        """Stop the publisher thread gracefully."""
        self._stop_event.set()
        self._thread.join(timeout=5)
        if self._conn:
            self._conn.close()
        logger.info("Publisher thread stopped")

    @property
    def is_connected(self):
        return self._connected
