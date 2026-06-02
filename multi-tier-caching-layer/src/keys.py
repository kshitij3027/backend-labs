"""Semantic cache-key generation.

Two queries that are *semantically* the same — same text modulo whitespace,
same params modulo ordering, timestamps that fall in the same time bucket —
must map to the **same** cache key so they share a cached result. This module
turns a ``(query, params)`` pair into a deterministic canonical string, a
SHA-256-based cache key, and a set of invalidation tags.

Pure stdlib only (``hashlib``, ``json``, ``re``, ``datetime``); no settings
import — ``bucket_seconds`` is always passed in by the caller.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any

# Param keys whose values are timestamps and should be bucketed so that
# near-identical times collapse onto one cache entry. Compared case-insensitively.
TIMESTAMP_KEYS: set[str] = {
    "ts",
    "time",
    "timestamp",
    "start",
    "end",
    "start_time",
    "end_time",
    "from",
    "to",
    "since",
    "until",
    "at",
}

# Param keys that scope an invalidation tag (e.g. invalidate everything for one source).
TAG_KEYS: set[str] = {"source", "service", "metric", "level", "host"}

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_query(query: str) -> str:
    """Collapse internal whitespace runs to a single space and strip ends.

    Case is preserved — two queries differing only in case are treated as
    distinct, matching the spec.
    """
    return _WHITESPACE_RE.sub(" ", query).strip()


def _as_number(value: Any) -> float | int | None:
    """Return ``value`` as a number if it is one (or a numeric string), else None.

    Bools are rejected (``isinstance(True, int)`` is True in Python, but a bool
    is not a timestamp).
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            return float(value) if ("." in value or "e" in value.lower()) else int(value)
        except ValueError:
            return None
    return None


def _bucket_value(value: Any, bucket_seconds: int) -> Any:
    """Floor a timestamp value to the start of its ``bucket_seconds`` window.

    Accepts an int/float epoch, a numeric string, or an ISO-8601 datetime
    string. Returns the bucketed epoch as an int. If the value cannot be
    interpreted as a timestamp it is returned unchanged so the key stays
    deterministic rather than raising.
    """
    if bucket_seconds <= 0:
        return value

    number = _as_number(value)
    if number is not None:
        epoch = int(number)
        return (epoch // bucket_seconds) * bucket_seconds

    if isinstance(value, str):
        try:
            epoch = int(datetime.fromisoformat(value).timestamp())
        except ValueError:
            return value
        return (epoch // bucket_seconds) * bucket_seconds

    return value


def _bucket_params(params: dict | None, bucket_seconds: int) -> dict:
    """Return params with timestamp-keyed values bucketed; non-ts values pass through."""
    if not params:
        return {}
    bucketed: dict[str, Any] = {}
    for key, value in params.items():
        if str(key).lower() in TIMESTAMP_KEYS:
            bucketed[key] = _bucket_value(value, bucket_seconds)
        else:
            bucketed[key] = value
    return bucketed


def canonicalize(
    query: str,
    params: dict | None = None,
    *,
    bucket_seconds: int = 300,
) -> str:
    """Return a deterministic canonical string for ``(query, params)``.

    Rules:
      * the query has internal whitespace collapsed and ends stripped (case kept);
      * params are sorted by key (via ``json.dumps(sort_keys=True)``);
      * any param whose lowercased key is in :data:`TIMESTAMP_KEYS` has its value
        floored to the start of its ``bucket_seconds`` window.

    The output is compact JSON: ``{"p": <params>, "q": <query>}`` with sorted
    keys, so it is stable across runs and process restarts.
    """
    normalized = _normalize_query(query)
    bucketed = _bucket_params(params, bucket_seconds)
    return json.dumps(
        {"q": normalized, "p": bucketed},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def cache_key(
    query: str,
    params: dict | None = None,
    *,
    bucket_seconds: int = 300,
    prefix: str = "q",
) -> str:
    """Return a prefixed SHA-256 hex cache key for ``(query, params)``."""
    canonical = canonicalize(query, params, bucket_seconds=bucket_seconds)
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    return f"{prefix}:{digest}"


def tags_for(query: str, params: dict | None = None) -> set[str]:
    """Return invalidation tags for ``(query, params)``.

    For each :data:`TAG_KEYS` member present in ``params`` a ``"<key>:<value>"``
    tag is added. A stable per-query-scope tag ``"query:<12-hex>"`` (derived from
    the normalized query text, ignoring params) is always added so a query's
    results can be invalidated regardless of params.
    """
    tags: set[str] = set()
    if params:
        for key in TAG_KEYS:
            if key in params:
                tags.add(f"{key}:{params[key]}")

    normalized = _normalize_query(query)
    query_hash = hashlib.sha256(normalized.encode()).hexdigest()[:12]
    tags.add(f"query:{query_hash}")
    return tags
