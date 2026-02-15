"""Resilient log shipper with buffering, retry, and reconnect."""

import logging
import queue
import threading
import time

from src.compressor import compress_payload
from src.config import Config
from src.file_reader import read_batch, FileTailer
from src.formatter import parse_log_line, format_ndjson
from src.health import HealthMonitor
from src.metrics import Metrics
from src.tcp_client import TCPClient

logger = logging.getLogger(__name__)


class ResilientLogShipper:
    """Producer-consumer shipper that decouples file reading from network I/O.

    Producer thread reads file -> parses -> enqueues formatted NDJSON bytes.
    Consumer thread dequeues -> sends with retry -> auto-reconnects.
    """

    def __init__(self, config: Config, shutdown_event: threading.Event):
        self._config = config
        self._shutdown = shutdown_event
        self._client = TCPClient(config.server_host, config.server_port, shutdown_event)
        self._queue: queue.Queue = queue.Queue(maxsize=config.buffer_size)
        self._health = HealthMonitor(
            config.server_host, config.server_port, shutdown_event, interval=5.0,
        )
        self._metrics = Metrics()
        self._sent = 0
        self._failed = 0
        self._lock = threading.Lock()

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    @property
    def sent(self) -> int:
        with self._lock:
            return self._sent

    @property
    def failed(self) -> int:
        with self._lock:
            return self._failed

    def run(self):
        """Start health monitor and consumer thread, then produce messages."""
        self._health.start()
        consumer = threading.Thread(target=self._consumer_loop, daemon=True)
        consumer.start()

        try:
            if self._config.batch_mode:
                self._produce_batch()
            else:
                self._produce_continuous()
        finally:
            # Send poison pill to signal consumer to stop
            self._queue.put(None)
            consumer.join(timeout=10)
            self._client.close()
            self._health.stop()
            logger.info(
                "Resilient shipper finished: sent=%d, failed=%d",
                self._sent, self._failed,
            )

    def _produce_batch(self):
        """Read all lines and enqueue them."""
        lines = read_batch(self._config.log_file)
        for line in lines:
            if self._shutdown.is_set():
                break
            self._enqueue_line(line)

    def _produce_continuous(self):
        """Tail the file and enqueue new lines."""
        tailer = FileTailer(
            self._config.log_file,
            self._shutdown,
            callback=self._enqueue_line,
            poll_interval=self._config.poll_interval,
        )
        tailer.run()

    def _enqueue_line(self, raw: str):
        """Parse and enqueue a single log line (uncompressed)."""
        entry = parse_log_line(raw)
        if entry is None:
            return

        payload = format_ndjson(entry)
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            with self._lock:
                self._failed += 1
            logger.warning("Buffer full, dropping log line")

    def _consumer_loop(self):
        """Dequeue messages in batches and send them with retry."""
        while not self._shutdown.is_set():
            try:
                first = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if first is None:
                self._drain_queue()
                return

            batch = [first]
            while len(batch) < self._config.batch_size:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is None:
                    self._send_batch_with_retry(batch)
                    self._drain_queue()
                    return
                batch.append(item)

            self._send_batch_with_retry(batch)

    def _drain_queue(self):
        """Send any remaining queued messages before shutdown."""
        batch: list[bytes] = []
        while True:
            try:
                payload = self._queue.get_nowait()
            except queue.Empty:
                break
            if payload is None:
                break
            batch.append(payload)
            if len(batch) >= self._config.batch_size:
                self._send_batch_with_retry(batch, max_retries=1)
                batch = []
        if batch:
            self._send_batch_with_retry(batch, max_retries=1)

    def _send_batch_with_retry(self, batch: list[bytes], max_retries: int = 3):
        """Send a batch of payloads, retrying with reconnect on failure."""
        payload = b"".join(batch)
        if self._config.compress:
            payload = compress_payload(payload)

        for attempt in range(max_retries):
            if self._shutdown.is_set():
                with self._lock:
                    self._failed += len(batch)
                return

            if not self._client.connected:
                if not self._health.is_healthy:
                    logger.debug("Server unhealthy, waiting before reconnect")
                    self._health.wait_for_healthy(timeout=5.0)
                if not self._client.connect_with_backoff(max_attempts=3):
                    continue

            self._metrics.record_buffer_usage(self._queue.qsize())

            t0 = time.monotonic()
            if not self._client.send(payload):
                continue

            acked = 0
            for _ in range(len(batch)):
                result = self._client.recv_line()
                if result and result.get("status") == "ok":
                    acked += 1
                else:
                    break

            latency_ms = (time.monotonic() - t0) * 1000
            for _ in range(acked):
                self._metrics.record_sent(latency_ms / max(acked, 1))
            for _ in range(len(batch) - acked):
                self._metrics.record_failed()

            with self._lock:
                self._sent += acked
                self._failed += len(batch) - acked
            return

        for _ in range(len(batch)):
            self._metrics.record_failed()
        with self._lock:
            self._failed += len(batch)
        logger.warning("Failed to send batch of %d after %d retries", len(batch), max_retries)
