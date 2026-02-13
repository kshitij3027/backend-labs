"""Manifest-based indexer for NDJSON storage.

Maintains index/level/<LEVEL>/manifest.json and
index/date/<YYYY-MM-DD>/manifest.json.

Each manifest is a list of {"file": "store_current.ndjson", "line_numbers": [0, 3, 7]}.
"""

import json
import os
import tempfile


class Indexer:
    def __init__(self, index_dir: str):
        self._index_dir = index_dir
        # In-memory cache: {index_type: {index_value: {file: set(line_numbers)}}}
        self._data: dict[str, dict[str, dict[str, set[int]]]] = {}

    def add_entry(self, data_file: str, line_number: int, entry: dict) -> None:
        """Index an entry by level and date."""
        level = entry.get("level", "UNKNOWN")
        self._add("level", level, data_file, line_number)

        timestamp = entry.get("timestamp", "")
        if timestamp:
            date = timestamp[:10]  # ISO8601: YYYY-MM-DD
            self._add("date", date, data_file, line_number)

    def _add(self, index_type: str, value: str, data_file: str, line_no: int) -> None:
        bucket = self._data.setdefault(index_type, {}).setdefault(value, {})
        bucket.setdefault(data_file, set()).add(line_no)

    def save(self) -> None:
        """Persist all in-memory indexes to disk as manifest files."""
        for index_type, values in self._data.items():
            for value, file_map in values.items():
                manifest_dir = os.path.join(self._index_dir, index_type, value)
                os.makedirs(manifest_dir, exist_ok=True)
                manifest_path = os.path.join(manifest_dir, "manifest.json")

                # Load existing manifest
                existing: list[dict] = []
                if os.path.exists(manifest_path):
                    with open(manifest_path, "r") as f:
                        existing = json.load(f)

                # Merge
                existing_map: dict[str, set[int]] = {}
                for item in existing:
                    existing_map[item["file"]] = set(item["line_numbers"])

                for fname, lines in file_map.items():
                    existing_map.setdefault(fname, set()).update(lines)

                manifest = [
                    {"file": fname, "line_numbers": sorted(lns)}
                    for fname, lns in sorted(existing_map.items())
                ]

                fd, tmp = tempfile.mkstemp(dir=manifest_dir)
                try:
                    with os.fdopen(fd, "w") as f:
                        json.dump(manifest, f, indent=2)
                    os.replace(tmp, manifest_path)
                except Exception:
                    os.unlink(tmp)
                    raise

        # Clear in-memory cache after flush
        self._data.clear()
