"""In-memory log storage for a single node in the consistent hashing cluster."""

import threading
from datetime import datetime, timezone


class StorageNode:
    """Thread-safe in-memory log storage for a single node."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._logs: list[dict] = []
        self._lock = threading.RLock()

    def store(self, log_entry: dict) -> dict:
        """Store a log entry and add metadata.

        Adds stored_at timestamp and node_id to the entry.
        Returns the stored entry with metadata.
        """
        with self._lock:
            entry = dict(log_entry)  # copy to avoid mutation
            entry["stored_at"] = datetime.now(timezone.utc).isoformat()
            entry["node_id"] = self.node_id
            self._logs.append(entry)
            return entry

    def get_logs(self) -> list[dict]:
        """Return all stored logs."""
        with self._lock:
            return list(self._logs)

    def get_log_count(self) -> int:
        """Return number of stored logs."""
        with self._lock:
            return len(self._logs)

    def remove_logs(self, predicate) -> list[dict]:
        """Remove and return logs matching the predicate function.

        Used during rebalancing to extract logs that should move to another node.
        predicate: callable that takes a log_entry dict and returns True to remove.
        """
        with self._lock:
            removed = []
            remaining = []
            for log in self._logs:
                if predicate(log):
                    removed.append(log)
                else:
                    remaining.append(log)
            self._logs = remaining
            return removed

    def add_logs(self, logs: list[dict]) -> int:
        """Bulk add logs (for receiving migrated logs during rebalancing).

        Returns the number of logs added.
        """
        with self._lock:
            self._logs.extend(logs)
            return len(logs)

    def get_stats(self) -> dict:
        """Return node statistics."""
        with self._lock:
            sources = {}
            levels = {}
            for log in self._logs:
                src = log.get("source", "unknown")
                sources[src] = sources.get(src, 0) + 1
                lvl = log.get("level", "unknown")
                levels[lvl] = levels.get(lvl, 0) + 1

            return {
                "node_id": self.node_id,
                "log_count": len(self._logs),
                "sources": sources,
                "levels": levels,
            }
