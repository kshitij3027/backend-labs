"""Output formatters — text, JSON (NDJSON), colorized (ANSI)."""

from typing import Callable

from src.parser import LogEntry

# ANSI color codes
COLORS = {
    "DEBUG": "\033[36m",   # cyan
    "INFO": "\033[32m",    # green
    "WARN": "\033[33m",    # yellow
    "WARNING": "\033[33m", # yellow
    "ERROR": "\033[31m",   # red
}
RESET = "\033[0m"


def format_text(entry: LogEntry) -> str:
    """Return the raw log line."""
    return entry.raw


def format_json(entry: LogEntry) -> str:
    """Return NDJSON — one JSON object per line, compatible with jq."""
    import json
    return json.dumps({
        "timestamp": entry.timestamp.isoformat(),
        "level": entry.level,
        "message": entry.message,
        "source_file": entry.source_file,
    })


def format_color(entry: LogEntry) -> str:
    """Return the log line with ANSI-colored level."""
    color = COLORS.get(entry.level, "")
    ts = entry.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return f"[{ts}] [{color}{entry.level}{RESET}] {entry.message}"


def get_formatter(output_format: str = "text", color: bool = False) -> Callable[[LogEntry], str]:
    """Factory that returns the right formatter based on args."""
    if output_format == "json":
        return format_json
    if color:
        return format_color
    return format_text
