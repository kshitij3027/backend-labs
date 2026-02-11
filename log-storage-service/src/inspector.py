"""Inspector logic: list, read, and search log files."""

import gzip
import os


def list_log_files(log_dir: str) -> list[str]:
    """Return all log-related files (active .log, rotated, and .gz) sorted by name."""
    files = []
    for name in os.listdir(log_dir):
        if name.endswith(".log") or name.endswith(".gz") or ".log." in name:
            files.append(name)
    files.sort()
    return files


def read_file(log_dir: str, filename: str) -> str:
    """Read a log file, transparently decompressing .gz files."""
    path = os.path.join(log_dir, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    if filename.endswith(".gz"):
        with gzip.open(path, "rt") as f:
            return f.read()
    else:
        with open(path, "r") as f:
            return f.read()


def search_files(log_dir: str, text: str) -> list[tuple[str, int, str]]:
    """Search for text across all log files. Returns (filename, line_num, line) tuples."""
    results = []
    for filename in list_log_files(log_dir):
        path = os.path.join(log_dir, filename)
        try:
            if filename.endswith(".gz"):
                f = gzip.open(path, "rt")
            else:
                f = open(path, "r")
            with f:
                for line_num, line in enumerate(f, 1):
                    if text in line:
                        results.append((filename, line_num, line.rstrip("\n")))
        except (OSError, gzip.BadGzipFile):
            continue
    return results
