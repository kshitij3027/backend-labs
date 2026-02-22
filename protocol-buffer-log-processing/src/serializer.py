"""Serialize and deserialize log entries as JSON and Protocol Buffers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from google.protobuf.timestamp_pb2 import Timestamp

from src.generated.log_entry_pb2 import LogBatch, LogEntry, LogLevel

# ---------------------------------------------------------------------------
# Level mapping helpers
# ---------------------------------------------------------------------------

_LEVEL_STR_TO_PROTO: dict[str, int] = {
    "DEBUG": LogLevel.LOG_LEVEL_DEBUG,
    "INFO": LogLevel.LOG_LEVEL_INFO,
    "WARNING": LogLevel.LOG_LEVEL_WARNING,
    "ERROR": LogLevel.LOG_LEVEL_ERROR,
    "CRITICAL": LogLevel.LOG_LEVEL_CRITICAL,
}

_LEVEL_PROTO_TO_STR: dict[int, str] = {v: k for k, v in _LEVEL_STR_TO_PROTO.items()}


class SerializationError(Exception):
    """Raised when serialization or deserialization fails."""


# ---------------------------------------------------------------------------
# Internal proto <-> dict converters
# ---------------------------------------------------------------------------


def _dict_to_proto(entry: dict) -> LogEntry:
    """Convert a log entry dict to a protobuf :class:`LogEntry`.

    Args:
        entry: A validated log entry dict.

    Returns:
        A populated ``LogEntry`` protobuf message.
    """
    proto_entry = LogEntry()

    # timestamp
    ts = Timestamp()
    ts.FromDatetime(entry["timestamp"])
    proto_entry.timestamp.CopyFrom(ts)

    # scalar fields
    proto_entry.service_name = entry["service_name"]
    proto_entry.level = _LEVEL_STR_TO_PROTO[entry["level"]]
    proto_entry.message = entry["message"]

    # metadata map
    if entry.get("metadata"):
        for key, value in entry["metadata"].items():
            proto_entry.metadata[key] = value

    return proto_entry


def _proto_to_dict(proto_entry: LogEntry) -> dict:
    """Convert a protobuf :class:`LogEntry` back to a plain dict.

    Args:
        proto_entry: A ``LogEntry`` protobuf message.

    Returns:
        A dict with keys: timestamp, service_name, level, message, metadata.
    """
    return {
        "timestamp": proto_entry.timestamp.ToDatetime(tzinfo=timezone.utc),
        "service_name": proto_entry.service_name,
        "level": _LEVEL_PROTO_TO_STR.get(proto_entry.level, "INFO"),
        "message": proto_entry.message,
        "metadata": dict(proto_entry.metadata),
    }


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------


def _json_default(obj: Any) -> str:
    """Default handler for json.dumps — converts datetime to ISO format."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def serialize_json(entries: list[dict]) -> bytes:
    """Serialize a list of log entry dicts to JSON bytes (UTF-8).

    Args:
        entries: List of log entry dicts.

    Returns:
        UTF-8 encoded JSON bytes.

    Raises:
        SerializationError: If serialization fails.
    """
    try:
        return json.dumps(entries, default=_json_default).encode("utf-8")
    except Exception as exc:
        raise SerializationError(f"JSON serialization failed: {exc}") from exc


def deserialize_json(data: bytes) -> list[dict]:
    """Deserialize JSON bytes back to a list of log entry dicts.

    Timestamp strings in ISO format are converted back to
    :class:`datetime` objects.

    Args:
        data: UTF-8 encoded JSON bytes.

    Returns:
        List of log entry dicts.

    Raises:
        SerializationError: If deserialization fails.
    """
    try:
        entries = json.loads(data.decode("utf-8"))
        for entry in entries:
            if "timestamp" in entry and isinstance(entry["timestamp"], str):
                entry["timestamp"] = datetime.fromisoformat(entry["timestamp"])
        return entries
    except Exception as exc:
        raise SerializationError(f"JSON deserialization failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Protobuf serialization
# ---------------------------------------------------------------------------


def serialize_protobuf(entries: list[dict]) -> bytes:
    """Serialize a list of log entry dicts to protobuf bytes.

    Creates a :class:`LogBatch` message, converts each dict via
    :func:`_dict_to_proto`, and returns the binary wire format.

    Args:
        entries: List of log entry dicts.

    Returns:
        Protobuf binary bytes.

    Raises:
        SerializationError: If serialization fails.
    """
    try:
        batch = LogBatch()
        for entry in entries:
            proto_entry = _dict_to_proto(entry)
            batch.entries.append(proto_entry)
        return batch.SerializeToString()
    except Exception as exc:
        raise SerializationError(f"Protobuf serialization failed: {exc}") from exc


def deserialize_protobuf(data: bytes) -> list[dict]:
    """Deserialize protobuf bytes back to a list of log entry dicts.

    Parses a :class:`LogBatch` message and converts each entry via
    :func:`_proto_to_dict`.

    Args:
        data: Protobuf binary bytes.

    Returns:
        List of log entry dicts.

    Raises:
        SerializationError: If deserialization fails.
    """
    try:
        batch = LogBatch()
        batch.ParseFromString(data)
        return [_proto_to_dict(entry) for entry in batch.entries]
    except Exception as exc:
        raise SerializationError(f"Protobuf deserialization failed: {exc}") from exc
