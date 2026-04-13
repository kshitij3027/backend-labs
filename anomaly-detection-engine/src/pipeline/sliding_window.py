"""Thread-safe sliding window buffer for feature vectors."""
from __future__ import annotations

import collections
import threading

import numpy as np


class SlidingWindow:
    """Fixed-capacity sliding window that stores feature vectors in a deque.

    All public methods are thread-safe via a threading.Lock.
    """

    def __init__(self, maxlen: int = 100) -> None:
        self._buffer: collections.deque[np.ndarray] = collections.deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def add(self, features: np.ndarray) -> None:
        """Append a feature vector to the window (thread-safe)."""
        with self._lock:
            self._buffer.append(features)

    def get_all(self) -> np.ndarray:
        """Return all stored vectors as a 2-D array of shape (N, num_features).

        Returns an empty array with shape (0,) when the buffer is empty.
        """
        with self._lock:
            if len(self._buffer) == 0:
                return np.array([])
            return np.array(list(self._buffer))

    def get_stats(self) -> tuple[np.ndarray, np.ndarray]:
        """Compute per-feature mean and std across the window.

        Returns (mean, std) each of shape (num_features,).
        Standard deviation uses ddof=0 with an epsilon of 1e-10 added
        to avoid division-by-zero issues downstream.
        """
        with self._lock:
            data = np.array(list(self._buffer))
            mean = np.mean(data, axis=0)
            std = np.std(data, axis=0, ddof=0) + 1e-10
            return mean, std

    def __len__(self) -> int:
        """Return the number of items currently in the buffer."""
        with self._lock:
            return len(self._buffer)

    def is_ready(self, min_size: int = 10) -> bool:
        """Return True if the buffer contains at least *min_size* items."""
        with self._lock:
            return len(self._buffer) >= min_size

    def clear(self) -> None:
        """Remove all items from the buffer."""
        with self._lock:
            self._buffer.clear()
