"""Handles rotation of the active NDJSON data file."""

import os
import time
from datetime import datetime, timezone


class Rotator:
    def __init__(self, active_dir: str, archive_dir: str,
                 size_threshold_bytes: int, age_threshold_seconds: int):
        self._active_dir = active_dir
        self._archive_dir = archive_dir
        self._size_threshold = size_threshold_bytes
        self._age_threshold = age_threshold_seconds
        self._created_at: float = time.time()
        os.makedirs(self._active_dir, exist_ok=True)
        os.makedirs(self._archive_dir, exist_ok=True)

    def needs_rotation(self, active_path: str) -> bool:
        if not os.path.exists(active_path):
            return False
        size = os.path.getsize(active_path)
        age = time.time() - self._created_at
        return size >= self._size_threshold or age >= self._age_threshold

    def rotate(self, active_path: str) -> str | None:
        """Move active file to archive. Returns the archive path or None."""
        if not os.path.exists(active_path):
            return None
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        archive_name = f"store_{now}.ndjson"
        archive_path = os.path.join(self._archive_dir, archive_name)
        os.replace(active_path, archive_path)
        self._created_at = time.time()
        return archive_path
