"""Shared metrics class for tracking operational counters."""

import json
import os
import tempfile
import time
from datetime import datetime, timezone


class Metrics:
    def __init__(self, path: str):
        self._counters: dict[str, int] = {}
        self._path = path
        self._start_time = time.time()

    def increment(self, name: str, amount: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + amount

    def get_all(self) -> dict:
        return {
            "counters": dict(self._counters),
            "uptime_seconds": round(time.time() - self._start_time, 1),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    def save(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        data = self.get_all()
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self._path))
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, self._path)
        except Exception:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise
