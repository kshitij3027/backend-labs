"""Thread-safe transmission metrics for the TLS log server."""

import threading
import time


class TransmissionMetrics:
    """Tracks server-side transmission statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._logs_received = 0
        self._bytes_compressed = 0
        self._bytes_decompressed = 0
        self._connections = 0
        self._active_connections = 0
        self._start_time = time.monotonic()

    def record_log(self, compressed_size: int, decompressed_size: int):
        with self._lock:
            self._logs_received += 1
            self._bytes_compressed += compressed_size
            self._bytes_decompressed += decompressed_size

    def record_connection(self):
        with self._lock:
            self._connections += 1
            self._active_connections += 1

    def record_disconnection(self):
        with self._lock:
            self._active_connections -= 1

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
                "bytes_compressed": self._bytes_compressed,
                "bytes_decompressed": self._bytes_decompressed,
                "compression_ratio": round(ratio, 2),
                "total_connections": self._connections,
                "active_connections": self._active_connections,
                "elapsed_seconds": round(elapsed, 1),
                "throughput_logs_per_sec": round(
                    self._logs_received / elapsed, 1
                ) if elapsed > 0 else 0,
            }
