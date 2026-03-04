"""Phi accrual failure detection for cluster nodes."""

import time
from collections import deque

from src.config import ClusterConfig


class PhiAccrualFailureDetector:
    """Detects node failures using phi accrual failure detection.

    Phi is computed as: time_since_last_heartbeat / mean_inter_arrival_interval
    This gives an adaptive threshold that adjusts to network conditions.
    """

    def __init__(self, config: ClusterConfig) -> None:
        self._config = config
        self._window_size = config.heartbeat_window_size
        # Per-node: deque of inter-arrival times
        self._windows: dict[str, deque[float]] = {}
        # Per-node: timestamp of last heartbeat
        self._last_heartbeat: dict[str, float] = {}

    def record_heartbeat(self, node_id: str) -> None:
        """Record a heartbeat arrival from a node."""
        now = time.time()
        if node_id in self._last_heartbeat:
            interval = now - self._last_heartbeat[node_id]
            if node_id not in self._windows:
                self._windows[node_id] = deque(maxlen=self._window_size)
            self._windows[node_id].append(interval)
        self._last_heartbeat[node_id] = now

    def compute_phi(self, node_id: str) -> float:
        """Compute the phi value for a node.

        phi = time_since_last_heartbeat / mean_interval
        Returns 0.0 if no heartbeat data is available.
        """
        if node_id not in self._last_heartbeat:
            return 0.0

        window = self._windows.get(node_id)
        if not window:
            return 0.0

        mean_interval = sum(window) / len(window)
        if mean_interval <= 0:
            return 0.0

        time_since_last = time.time() - self._last_heartbeat[node_id]
        return time_since_last / mean_interval

    def interpret_phi(self, phi: float) -> str:
        """Interpret a phi value into a human-readable category."""
        if phi < 1.0:
            return "normal"
        elif phi < 3.0:
            return "minor_delay"
        elif phi < 8.0:
            return "significant_delay"
        else:
            return "probable_failure"

    def remove_node(self, node_id: str) -> None:
        """Remove all tracking data for a node."""
        self._windows.pop(node_id, None)
        self._last_heartbeat.pop(node_id, None)

    def reset_node(self, node_id: str) -> None:
        """Reset tracking data for a node (keeps it tracked but clears history)."""
        self._windows.pop(node_id, None)
        self._last_heartbeat.pop(node_id, None)
