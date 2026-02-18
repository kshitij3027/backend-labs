"""Log shipper — orchestrates batching, compression, and TCP transmission."""

import json
import logging
import threading

from src.config import ClientConfig
from src.compression import CompressionHandler
from src.protocol import Algorithm
from src.batch_buffer import BatchBuffer
from src.tcp_client import TCPClient
from src.metrics import ShipperMetrics

logger = logging.getLogger(__name__)

ALGO_MAP = {
    "gzip": Algorithm.GZIP,
    "zlib": Algorithm.ZLIB,
    "none": Algorithm.NONE,
}


class LogShipper:
    def __init__(self, config: ClientConfig, shutdown_event: threading.Event):
        self._config = config
        self._shutdown = shutdown_event
        self._stats_interval = 1000

        self._compressor = CompressionHandler(
            algorithm=config.compression_algorithm,
            level=config.compression_level,
            enabled=config.compression_enabled,
            bypass_threshold=config.bypass_threshold,
        )
        self._client = TCPClient(config.server_host, config.server_port, shutdown_event)
        self._metrics = ShipperMetrics()
        self._buffer = None

    @property
    def compressor(self) -> CompressionHandler:
        """Expose compressor for adaptive compression integration."""
        return self._compressor

    @property
    def metrics(self) -> ShipperMetrics:
        return self._metrics

    def start(self) -> bool:
        """Connect to server and start batch buffer. Returns True on success."""
        if not self._client.connect_with_backoff(max_attempts=5):
            return False

        self._buffer = BatchBuffer(
            batch_size=self._config.batch_size,
            flush_interval=self._config.batch_interval,
            on_flush=self._handle_flush,
            shutdown_event=self._shutdown,
        )
        return True

    def stop(self):
        """Stop buffer, close connection, print final stats."""
        if self._buffer:
            self._buffer.stop()
        self._client.close()

        stats = self._metrics.snapshot()
        logger.info("Final stats: %s", stats)
        print(f"\n--- Final Statistics ---")
        for key, value in stats.items():
            print(f"  {key}: {value}")

    def ship(self, entry_dict: dict):
        """Add a log entry dict to the batch buffer."""
        if self._buffer:
            self._buffer.add(entry_dict)

    def _handle_flush(self, batch: list[dict]):
        """Callback: compress and send a batch of log entries."""
        payload = json.dumps(batch).encode("utf-8")
        result = self._compressor.compress(payload)

        algo_enum = ALGO_MAP.get(result.algorithm, Algorithm.NONE)
        compressed = result.algorithm != "none"

        success = self._client.send_frame(result.data, compressed, algo_enum)

        if success:
            self._metrics.record_send(
                logs_count=len(batch),
                compression_ratio=result.ratio,
                compression_time_ms=result.time_ms,
            )
            stats = self._metrics.snapshot()
            if stats["logs_sent"] % self._stats_interval < len(batch):
                print(
                    f"[Shipper] Sent {stats['logs_sent']} logs | "
                    f"Batches: {stats['batches_sent']} | "
                    f"Avg ratio: {stats['avg_compression_ratio']}x | "
                    f"Throughput: {stats['throughput_logs_per_sec']} logs/s"
                )
        else:
            self._metrics.record_failure()
            logger.warning("Failed to send batch of %d entries", len(batch))
