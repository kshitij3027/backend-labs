"""Search NDJSON storage by pattern or by index lookup."""

import json
import os
import re
from typing import Iterator


def _ndjson_files(storage_dir: str) -> list[str]:
    """Return paths to all NDJSON files (active first, then archives sorted)."""
    files = []
    active = os.path.join(storage_dir, "active", "store_current.ndjson")
    if os.path.exists(active):
        files.append(active)
    archive_dir = os.path.join(storage_dir, "archive")
    if os.path.isdir(archive_dir):
        for f in sorted(os.listdir(archive_dir)):
            if f.endswith(".ndjson"):
                files.append(os.path.join(archive_dir, f))
    return files


def search_by_pattern(storage_dir: str, pattern: str, limit: int = 50) -> Iterator[dict]:
    """Iterate NDJSON files and yield entries matching the regex pattern."""
    regex = re.compile(pattern, re.IGNORECASE)
    count = 0
    for path in _ndjson_files(storage_dir):
        with open(path, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                if regex.search(line):
                    try:
                        yield json.loads(line)
                        count += 1
                        if count >= limit:
                            return
                    except json.JSONDecodeError:
                        continue


def search_by_index(storage_dir: str, index_type: str, index_value: str,
                    limit: int = 50) -> Iterator[dict]:
    """Use a manifest to look up entries by index type/value."""
    manifest_path = os.path.join(
        storage_dir, "index", index_type, index_value, "manifest.json"
    )
    if not os.path.exists(manifest_path):
        return

    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    count = 0
    for item in manifest:
        filename = item["file"]
        line_numbers = set(item["line_numbers"])

        # Resolve file path
        path = os.path.join(storage_dir, "active", filename)
        if not os.path.exists(path):
            path = os.path.join(storage_dir, "archive", filename)
        if not os.path.exists(path):
            continue

        with open(path, "r") as f:
            for i, line in enumerate(f):
                if i in line_numbers:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                        count += 1
                        if count >= limit:
                            return
                    except json.JSONDecodeError:
                        continue
