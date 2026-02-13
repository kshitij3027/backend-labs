"""Tracks the byte offset into the source log file.

State is persisted as a JSON file with atomic writes (tmp + os.replace).
Handles file truncation by resetting offset when the file shrinks.
"""

import json
import os
import tempfile


class OffsetTracker:
    def __init__(self, state_file: str):
        self._state_file = state_file
        self.offset: int = 0
        self.inode: int = 0
        self._load()

    def _load(self) -> None:
        if os.path.exists(self._state_file):
            with open(self._state_file, "r") as f:
                data = json.load(f)
            self.offset = data.get("offset", 0)
            self.inode = data.get("inode", 0)

    def save(self) -> None:
        os.makedirs(os.path.dirname(self._state_file), exist_ok=True)
        data = {"offset": self.offset, "inode": self.inode}
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(self._state_file))
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f)
            os.replace(tmp, self._state_file)
        except Exception:
            os.unlink(tmp)
            raise

    def check_truncation(self, current_size: int, current_inode: int) -> None:
        """Reset offset if the file was truncated or replaced."""
        if current_inode != self.inode or current_size < self.offset:
            self.offset = 0
            self.inode = current_inode
