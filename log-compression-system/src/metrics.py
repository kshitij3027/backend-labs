"""Thread-safe metrics for shipper and receiver."""

import threading
import time


class ShipperMetrics:
    """Tracks client-side shipping statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._logs_sent = 0
        self._batches_sent = 0
        self._failed_sends = 0
        self._total_compression_ratio = 0.0
        self._total_compression_time_ms = 0.0
        self._start_time = time.monotonic()

    def record_send(self, logs_count: int, compression_ratio: float, compression_time_ms: float):
        with self._lock:
            self._logs_sent += logs_count
            self._batches_sent += 1
            self._total_compression_ratio += compression_ratio
            self._total_compression_time_ms += compression_time_ms

    def record_failure(self):
        with self._lock:
            self._failed_sends += 1

    def snapshot(self) -> dict:
        with self._lock:
            elapsed = time.monotonic() - self._start_time
            avg_ratio = (
                self._total_compression_ratio / self._batches_sent
                if self._batches_sent > 0
                else 0.0
            )
            return {
                "logs_sent": self._logs_sent,
                "batches_sent": self._batches_sent,
                "failed_sends": self._failed_sends,
                "avg_compression_ratio": round(avg_ratio, 2),
                "total_compression_time_ms": round(self._total_compression_time_ms, 2),
                "elapsed_seconds": round(elapsed, 1),
                "throughput_logs_per_sec": round(self._logs_sent / elapsed, 1) if elapsed > 0 else 0,
            }


class ReceiverMetrics:
    """Tracks server-side receive statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._bytes_compressed = 0
        self._bytes_decompressed = 0
        self._batches_received = 0
        self._logs_received = 0
        self._start_time = time.monotonic()

    def record_batch(self, logs_count: int, compressed_size: int, decompressed_size: int):
        with self._lock:
            self._logs_received += logs_count
            self._batches_received += 1
            self._bytes_compressed += compressed_size
            self._bytes_decompressed += decompressed_size

    def snapshot(self) -> dict:
        with self._lock:
            elapsed = time.monotonic() - self._start_time
            ratio = (
                self._bytes_decompressed / self._bytes_compressed
                if self._bytes_compressed > 0
                else 0.0
            )
            return {
                "logs_received": self._logs_received,
                "batches_received": self._batches_received,
                "bytes_compressed": self._bytes_compressed,
                "bytes_decompressed": self._bytes_decompressed,
                "compression_ratio": round(ratio, 2),
                "elapsed_seconds": round(elapsed, 1),
                "throughput_logs_per_sec": round(self._logs_received / elapsed, 1) if elapsed > 0 else 0,
            }
