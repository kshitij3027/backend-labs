"""Parses JSON log lines into structured dicts."""

import json


def parse_json_line(line: str) -> dict | None:
    try:
        data = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(data, dict) or "timestamp" not in data:
        return None

    # Normalize level field
    if "level" not in data:
        data["level"] = "UNKNOWN"
    data["format"] = "json"
    data["raw"] = line
    return data
