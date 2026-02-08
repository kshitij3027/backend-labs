"""Log entry formatters: text, JSON, CSV."""

import csv
import io
import json

from src.models import LogEntry

CSV_HEADER = "timestamp,level,id,service,user_id,request_id,duration_ms,message"


def format_text(entry: LogEntry) -> str:
    ts = entry.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"{ts} | {entry.level:<7} | {entry.id} | {entry.service} | "
        f"{entry.user_id} | {entry.request_id} | {entry.duration_ms}ms | "
        f"{entry.message}"
    )


def format_json(entry: LogEntry) -> str:
    return json.dumps({
        "timestamp": entry.timestamp.isoformat(),
        "level": entry.level,
        "id": entry.id,
        "service": entry.service,
        "user_id": entry.user_id,
        "request_id": entry.request_id,
        "duration_ms": entry.duration_ms,
        "message": entry.message,
    })


def format_csv(entry: LogEntry) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        entry.timestamp.isoformat(),
        entry.level,
        entry.id,
        entry.service,
        entry.user_id,
        entry.request_id,
        entry.duration_ms,
        entry.message,
    ])
    return buf.getvalue().rstrip("\r\n")


def get_formatter(fmt: str):
    """Return the formatter function for the given format string."""
    formatters = {
        "text": format_text,
        "json": format_json,
        "csv": format_csv,
    }
    return formatters[fmt]
