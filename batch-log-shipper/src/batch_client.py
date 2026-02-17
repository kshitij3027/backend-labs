"""Batch log client — orchestrates buffer, serializer, splitter, sender, and metrics."""

import random
import threading
import time
import logging

from src.config import ClientConfig
from src.models import create_log_entry, entry_to_dict
from src.batch_buffer import BatchBuffer
from src.splitter import split_batch
from src.sender import UDPSender
from src.metrics import MetricsCollector

logger = logging.getLogger(__name__)

SAMPLE_LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARNING", "ERROR"]
SAMPLE_MESSAGES = [
    "User logged in",
    "Request processed successfully",
    "Database query completed",
    "Cache miss for key",
    "Configuration reloaded",
    "Health check passed",
    "Connection timeout to upstream",
    "Disk usage above threshold",
    "Authentication failed for user",
    "Service restarted",
]


class BatchLogClient:
    """High-level client that wires together the batch buffer, splitter,
    UDP sender, and metrics collector to ship log entries in batches."""

    def __init__(self, config: ClientConfig, shutdown_event: threading.Event):
        self._config = config
        self._shutdown = shutdown_event
        self._metrics = MetricsCollector()
        self._sender = UDPSender(
            config.target_host, config.target_port, config.max_retries
        )
        self._buffer = BatchBuffer(
            batch_size=config.batch_size,
            flush_interval=config.flush_interval,
            on_flush=self._handle_flush,
            shutdown_event=shutdown_event,
        )
        self._sequence = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Flush callback (called by BatchBuffer)
    # ------------------------------------------------------------------

    def _handle_flush(self, batch: list[dict]):
        """Serialize, split, and send a batch of log entries over UDP."""
        trigger = "size" if len(batch) >= self._buffer.batch_size else "timer"

        with self._lock:
            self._sequence += 1
            seq = self._sequence

        chunks = split_batch(batch, compress=self._config.compress)
        total_bytes = 0

        for chunk in chunks:
            start = time.monotonic()
            success = self._sender.send(chunk)
            elapsed_ms = (time.monotonic() - start) * 1000

            if success:
                total_bytes += len(chunk)
                self._metrics.record_batch(
                    batch_size=len(batch),
                    bytes_sent=len(chunk),
                    send_time_ms=elapsed_ms,
                    trigger=trigger,
                )

        logger.info(
            "Sent batch #%d of %d logs (%d bytes, %d chunk(s))",
            seq,
            len(batch),
            total_bytes,
            len(chunks),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_log(
        self,
        level: str,
        message: str,
        service: str = "batch-log-shipper",
        metadata: dict | None = None,
    ):
        """Create a log entry and add it to the batch buffer."""
        entry = entry_to_dict(create_log_entry(level, message, service, metadata))
        self._buffer.add(entry)

    def generate_sample_logs(self, logs_per_second: int, run_time: int):
        """Generate random sample logs at the specified rate for *run_time* seconds."""
        for _ in range(run_time):
            if self._shutdown.is_set():
                break

            second_start = time.monotonic()

            for _ in range(logs_per_second):
                if self._shutdown.is_set():
                    break
                level = random.choice(SAMPLE_LEVELS)
                message = random.choice(SAMPLE_MESSAGES)
                self.add_log(level, message)

            # Sleep until the next second boundary
            elapsed = time.monotonic() - second_start
            remaining = 1.0 - elapsed
            if remaining > 0 and not self._shutdown.is_set():
                self._shutdown.wait(timeout=remaining)

        logger.info("Client metrics: %s", self._metrics.snapshot())

    def stop(self):
        """Flush remaining entries, close the sender, and log final metrics."""
        self._buffer.stop()
        self._sender.close()
        logger.info("Client metrics: %s", self._metrics.snapshot())

    # ------------------------------------------------------------------
    # Dynamic-config properties
    # ------------------------------------------------------------------

    @property
    def batch_size(self) -> int:
        return self._buffer.batch_size

    @batch_size.setter
    def batch_size(self, value: int):
        self._buffer.batch_size = value

    @property
    def flush_interval(self) -> float:
        return self._buffer.flush_interval

    @flush_interval.setter
    def flush_interval(self, value: float):
        self._buffer.flush_interval = value

    @property
    def metrics(self) -> MetricsCollector:
        return self._metrics
