"""Resilient log shipper with buffering, retry, and reconnect."""

import logging
import queue
import threading

from src.config import Config
from src.file_reader import read_batch, FileTailer
from src.formatter import parse_log_line, format_ndjson
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
        self._sent = 0
        self._failed = 0
        self._lock = threading.Lock()

    @property
    def sent(self) -> int:
        with self._lock:
            return self._sent

    @property
    def failed(self) -> int:
        with self._lock:
            return self._failed

    def run(self):
        """Start consumer thread, then produce messages from the file."""
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
        """Parse and enqueue a single log line."""
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
        """Dequeue messages and send them with retry."""
        while not self._shutdown.is_set():
            try:
                payload = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            # Poison pill — clean shutdown
            if payload is None:
                # Drain remaining items
                self._drain_queue()
                return

            self._send_with_retry(payload, max_retries=3)

    def _drain_queue(self):
        """Send any remaining queued messages before shutdown."""
        while True:
            try:
                payload = self._queue.get_nowait()
            except queue.Empty:
                return
            if payload is None:
                return
            self._send_with_retry(payload, max_retries=1)

    def _send_with_retry(self, payload: bytes, max_retries: int = 3):
        """Send a payload, retrying with reconnect on failure."""
        for attempt in range(max_retries):
            if self._shutdown.is_set():
                with self._lock:
                    self._failed += 1
                return

            if not self._client.connected:
                if not self._client.connect_with_backoff(max_attempts=3):
                    continue

            result = self._client.send_and_recv(payload)
            if result and result.get("status") == "ok":
                with self._lock:
                    self._sent += 1
                return

        with self._lock:
            self._failed += 1
        logger.warning("Failed to send after %d retries", max_retries)
