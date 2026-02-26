"""Output formatters for parsed log entries."""
import json
from typing import Optional
from src.models import ParsedLog


def format_json(log: ParsedLog) -> str:
    """Format a parsed log as a JSON string."""
    return json.dumps(log.to_dict(), default=str)


def format_structured(log: ParsedLog) -> str:
    """Format a parsed log as structured text."""
    parts = []
    if log.timestamp:
        parts.append(f"timestamp={log.timestamp.isoformat()}")
    if log.level:
        parts.append(f"level={log.level.name}")
    if log.hostname:
        parts.append(f"hostname={log.hostname}")
    if log.app_name:
        parts.append(f"app={log.app_name}")
    if log.pid:
        parts.append(f"pid={log.pid}")
    if log.facility:
        parts.append(f"facility={log.facility}")
    parts.append(f"format={log.source_format}")
    parts.append(f"msg={log.message}")
    return " | ".join(parts)


def format_plain(log: ParsedLog) -> str:
    """Format a parsed log as plain text."""
    parts = []
    if log.timestamp:
        parts.append(log.timestamp.strftime("%Y-%m-%d %H:%M:%S"))
    if log.level:
        parts.append(f"[{log.level.name}]")
    if log.hostname:
        parts.append(log.hostname)
    if log.app_name:
        app = log.app_name
        if log.pid:
            app += f"[{log.pid}]"
        parts.append(f"{app}:")
    parts.append(log.message)
    return " ".join(parts)


def get_formatter(format_name: str):
    """Get a formatter function by name."""
    formatters = {
        "json": format_json,
        "structured": format_structured,
        "plain": format_plain,
    }
    return formatters.get(format_name, format_json)
