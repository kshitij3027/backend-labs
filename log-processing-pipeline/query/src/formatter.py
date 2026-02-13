"""Format query results as text or JSON."""

import json


def format_text(entries: list[dict]) -> str:
    lines = []
    for e in entries:
        ts = e.get("timestamp", "?")
        level = e.get("level", "?")
        method = e.get("method", "")
        path = e.get("path", "")
        status = e.get("status_code", "")
        host = e.get("remote_host", "")
        lines.append(f"[{ts}] {level:7s} {host:15s} {method:6s} {path} -> {status}")
    return "\n".join(lines)


def format_json(entries: list[dict]) -> str:
    return json.dumps(entries, indent=2)


def get_formatter(fmt: str):
    if fmt == "json":
        return format_json
    return format_text
