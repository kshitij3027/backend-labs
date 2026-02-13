"""Core storage engine — ingests parsed JSON, writes NDJSON, indexes, rotates."""

import json
import os

from storage.src.state_tracker import StateTracker
from storage.src.indexer import Indexer
from storage.src.rotator import Rotator


class StorageEngine:
    def __init__(self, input_dir: str, storage_dir: str,
                 tracker: StateTracker, indexer: Indexer, rotator: Rotator,
                 compression_enabled: bool = True):
        self._input_dir = input_dir
        self._storage_dir = storage_dir
        self._tracker = tracker
        self._indexer = indexer
        self._rotator = rotator
        self._compression_enabled = compression_enabled
        self._active_dir = os.path.join(storage_dir, "active")
        os.makedirs(self._active_dir, exist_ok=True)

    @property
    def _active_path(self) -> str:
        return os.path.join(self._active_dir, "store_current.ndjson")

    def poll_once(self) -> int:
        """Ingest unprocessed parsed JSON files. Return entries stored."""
        if not os.path.isdir(self._input_dir):
            return 0

        files = sorted(
            f for f in os.listdir(self._input_dir)
            if f.endswith(".json") and not f.startswith(".")
            and not self._tracker.is_processed(f)
        )

        total = 0
        for filename in files:
            src = os.path.join(self._input_dir, filename)
            n = self._ingest_file(src)
            total += n
            self._tracker.mark_processed(filename)

        if files:
            self._tracker.save()
            self._indexer.save()

        # Check rotation
        if self._rotator.needs_rotation(self._active_path):
            archive = self._rotator.rotate(self._active_path)
            if archive:
                if self._compression_enabled:
                    archive = self._rotator.compress(archive)
                print(f"Storage: rotated to {archive}", flush=True)

        return total

    def _ingest_file(self, path: str) -> int:
        with open(path, "r") as f:
            entries = json.load(f)

        # Count existing lines to know starting line number
        start_line = 0
        if os.path.exists(self._active_path):
            with open(self._active_path, "r") as f:
                start_line = sum(1 for _ in f)

        data_file = "store_current.ndjson"
        with open(self._active_path, "a") as f:
            for i, entry in enumerate(entries):
                f.write(json.dumps(entry, separators=(",", ":")) + "\n")
                self._indexer.add_entry(data_file, start_line + i, entry)

        return len(entries)
