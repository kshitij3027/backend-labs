"""Validate log entry dicts before serialization."""

from __future__ import annotations

from datetime import datetime

VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
REQUIRED_FIELDS = ("timestamp", "service_name", "level", "message")


class ValidationError(Exception):
    """Raised when a log entry dict fails validation."""


def validate_log_entry(entry: dict) -> dict:
    """Validate a log entry dict and return it unchanged.

    Checks:
        - ``entry`` is a dict.
        - Required fields are present: timestamp, service_name, level, message.
        - ``timestamp`` is a :class:`datetime` instance.
        - ``service_name`` is a non-empty string.
        - ``level`` is one of DEBUG, INFO, WARNING, ERROR, CRITICAL.
        - ``message`` is a non-empty string.
        - If ``metadata`` is present, it must be a dict with string keys and
          string values.

    Args:
        entry: The log entry dict to validate.

    Returns:
        The validated entry dict (unmodified).

    Raises:
        ValidationError: If any check fails.
    """
    if not isinstance(entry, dict):
        raise ValidationError(
            f"Expected a dict, got {type(entry).__name__}"
        )

    # --- required fields ---
    for field in REQUIRED_FIELDS:
        if field not in entry:
            raise ValidationError(f"Missing required field: '{field}'")

    # --- timestamp ---
    if not isinstance(entry["timestamp"], datetime):
        raise ValidationError(
            f"'timestamp' must be a datetime object, "
            f"got {type(entry['timestamp']).__name__}"
        )

    # --- service_name ---
    if not isinstance(entry["service_name"], str) or not entry["service_name"]:
        raise ValidationError(
            "'service_name' must be a non-empty string"
        )

    # --- level ---
    if entry["level"] not in VALID_LOG_LEVELS:
        raise ValidationError(
            f"'level' must be one of {sorted(VALID_LOG_LEVELS)}, "
            f"got '{entry['level']}'"
        )

    # --- message ---
    if not isinstance(entry["message"], str) or not entry["message"]:
        raise ValidationError("'message' must be a non-empty string")

    # --- metadata (optional) ---
    if "metadata" in entry:
        meta = entry["metadata"]
        if not isinstance(meta, dict):
            raise ValidationError(
                f"'metadata' must be a dict, got {type(meta).__name__}"
            )
        for key, value in meta.items():
            if not isinstance(key, str):
                raise ValidationError(
                    f"metadata key must be a string, got {type(key).__name__}"
                )
            if not isinstance(value, str):
                raise ValidationError(
                    f"metadata value for key '{key}' must be a string, "
                    f"got {type(value).__name__}"
                )

    return entry
