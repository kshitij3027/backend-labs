from __future__ import annotations

import threading
from typing import Optional

from src.loadgen.runner import RunSummary


class RunStore:
    """Thread-safe in-memory dict of RunSummary objects keyed by run_id."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._runs: dict[str, RunSummary] = {}

    def put(self, summary: RunSummary) -> None:
        with self._lock:
            self._runs[summary.run_id] = summary

    def get(self, run_id: str) -> Optional[RunSummary]:
        with self._lock:
            return self._runs.get(run_id)

    def list(self, limit: int = 50) -> list[RunSummary]:
        with self._lock:
            items = list(self._runs.values())
        items.sort(key=lambda r: r.started_at, reverse=True)
        return items[:limit]

    def clear(self) -> None:
        with self._lock:
            self._runs.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._runs)
