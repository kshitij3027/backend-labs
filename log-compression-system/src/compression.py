"""Compression handler with gzip/zlib support, bypass threshold, and thread-safe level."""

import gzip
import zlib
import time
import threading
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CompressionResult:
    data: bytes
    original_size: int
    compressed_size: int
    ratio: float
    time_ms: float
    algorithm: str
    level: int


class CompressionHandler:
    """Compresses/decompresses data using gzip or zlib.

    Thread-safe level property allows adaptive compression to update
    the level at runtime.
    """

    def __init__(
        self,
        algorithm: str = "gzip",
        level: int = 6,
        enabled: bool = True,
        bypass_threshold: int = 256,
    ):
        self._algorithm = algorithm.lower()
        self._level = level
        self._enabled = enabled
        self._bypass_threshold = bypass_threshold
        self._lock = threading.Lock()

        if self._algorithm not in ("gzip", "zlib", "none"):
            raise ValueError(f"Unsupported algorithm: {self._algorithm}")

    @property
    def level(self) -> int:
        with self._lock:
            return self._level

    @level.setter
    def level(self, value: int):
        with self._lock:
            self._level = max(1, min(9, value))

    @property
    def algorithm(self) -> str:
        return self._algorithm

    @property
    def enabled(self) -> bool:
        return self._enabled

    def should_compress(self, data_size: int) -> bool:
        """Return True if data should be compressed (enabled and above threshold)."""
        return self._enabled and data_size >= self._bypass_threshold

    def compress(self, data: bytes) -> CompressionResult:
        """Compress data. Returns raw data with algorithm='none' if disabled,
        below threshold, or on error."""
        original_size = len(data)

        if not self.should_compress(original_size):
            return CompressionResult(
                data=data,
                original_size=original_size,
                compressed_size=original_size,
                ratio=1.0,
                time_ms=0.0,
                algorithm="none",
                level=0,
            )

        current_level = self.level
        start = time.monotonic()

        try:
            if self._algorithm == "gzip":
                compressed = gzip.compress(data, compresslevel=current_level)
            elif self._algorithm == "zlib":
                compressed = zlib.compress(data, level=current_level)
            else:
                # algorithm == "none"
                return CompressionResult(
                    data=data,
                    original_size=original_size,
                    compressed_size=original_size,
                    ratio=1.0,
                    time_ms=0.0,
                    algorithm="none",
                    level=0,
                )
        except Exception:
            logger.exception("Compression failed, returning raw data")
            return CompressionResult(
                data=data,
                original_size=original_size,
                compressed_size=original_size,
                ratio=1.0,
                time_ms=0.0,
                algorithm="none",
                level=0,
            )

        elapsed_ms = (time.monotonic() - start) * 1000
        compressed_size = len(compressed)
        ratio = original_size / compressed_size if compressed_size > 0 else 1.0

        return CompressionResult(
            data=compressed,
            original_size=original_size,
            compressed_size=compressed_size,
            ratio=ratio,
            time_ms=elapsed_ms,
            algorithm=self._algorithm,
            level=current_level,
        )

    def decompress(self, data: bytes, algorithm: str) -> bytes:
        """Decompress data using the specified algorithm."""
        algo = algorithm.lower()
        if algo == "none":
            return data
        elif algo == "gzip":
            return gzip.decompress(data)
        elif algo == "zlib":
            return zlib.decompress(data)
        else:
            raise ValueError(f"Unsupported algorithm: {algo}")
