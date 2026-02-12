"""Generator-based file reading, glob expansion, and tail."""

import glob
import os
import time
from typing import Generator


def read_lines(filepath: str) -> Generator[tuple[str, str], None, None]:
    """Yield (line, filepath) for each line in a single file."""
    with open(filepath, "r") as f:
        for line in f:
            yield line, filepath


def read_multiple(paths: list[str]) -> Generator[tuple[str, str], None, None]:
    """Yield (line, filepath) from multiple files, sequentially."""
    for path in paths:
        yield from read_lines(path)


def expand_paths(raw_paths: list[str]) -> list[str]:
    """Expand globs, deduplicate, and validate that files exist.

    Raises FileNotFoundError if a non-glob path doesn't exist.
    Raises FileNotFoundError if expansion produces zero files.
    """
    expanded = []
    seen = set()

    for raw in raw_paths:
        if any(c in raw for c in ("*", "?", "[")):
            matches = sorted(glob.glob(raw))
            for m in matches:
                if m not in seen:
                    seen.add(m)
                    expanded.append(m)
        else:
            if not os.path.isfile(raw):
                raise FileNotFoundError(f"File not found: {raw}")
            if raw not in seen:
                seen.add(raw)
                expanded.append(raw)

    if not expanded:
        raise FileNotFoundError("No log files found matching the given paths")

    return expanded


def tail_file(filepath: str, poll_interval: float = 0.1) -> Generator[tuple[str, str], None, None]:
    """Seek to end of file and yield new lines as they appear.

    Polls with time.sleep(poll_interval). Runs until interrupted.
    """
    with open(filepath, "r") as f:
        f.seek(0, os.SEEK_END)
        buffer = ""
        while True:
            chunk = f.read()
            if chunk:
                buffer += chunk
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    yield line + "\n", filepath
            else:
                time.sleep(poll_interval)
