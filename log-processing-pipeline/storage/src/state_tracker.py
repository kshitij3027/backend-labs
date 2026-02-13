"""Tracks which parsed JSON files have been ingested into storage."""

import json
import os
import tempfile


class StateTracker:
    def __init__(self, state_file: str):
        self._state_file = state_file
        self.processed: set[str] = set()
        self._load()

    def _load(self) -> None:
        if os.path.exists(self._state_file):
            with open(self._state_file, "r") as f:
                data = json.load(f)
            self.processed = set(data.get("processed", []))

    def save(self) -> None:
        os.makedirs(os.path.dirname(self._state_file), exist_ok=True)
        data = {"processed": sorted(self.processed)}
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self._state_file))
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f)
            os.replace(tmp, self._state_file)
        except Exception:
            os.unlink(tmp)
            raise

    def is_processed(self, filename: str) -> bool:
        return filename in self.processed

    def mark_processed(self, filename: str) -> None:
        self.processed.add(filename)
