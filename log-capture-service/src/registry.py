"""Offset registry: persists file read positions to survive restarts."""

import json
import os
import logging

logger = logging.getLogger(__name__)


class OffsetRegistry:
    def __init__(self, registry_file: str):
        self._path = registry_file
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self):
        if not os.path.exists(self._path):
            return
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                self._data = json.load(f)
            logger.info("Loaded offset registry from %s (%d entries)", self._path, len(self._data))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load registry %s: %s", self._path, e)
            self._data = {}

    def save(self):
        """Atomic write: write to tmp file then replace."""
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        tmp_path = self._path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        os.replace(tmp_path, self._path)

    def get_offset(self, path: str) -> int:
        return self._data.get(path, {}).get("offset", 0)

    def get_inode(self, path: str) -> int | None:
        return self._data.get(path, {}).get("inode")

    def update(self, path: str, offset: int, inode: int):
        self._data[path] = {"offset": offset, "inode": inode}
