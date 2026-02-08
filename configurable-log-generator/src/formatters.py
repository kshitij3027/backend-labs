"""Log entry formatters: text, JSON, CSV."""

from src.models import LogEntry


def format_text(entry: LogEntry) -> str:
    ts = entry.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{ts} | {entry.level:<7} | {entry.id} | {entry.service} | "
        f"{entry.user_id} | {entry.request_id} | {entry.duration_ms}ms | "
        f"{entry.message}"
    )


def get_formatter(fmt: str):
    """Return the formatter function for the given format string."""
    formatters = {
        "text": format_text,
    }
    return formatters[fmt]
