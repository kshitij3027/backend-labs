"""L3 read/write over the ``precomputed_aggregates`` table.

L3 is the *materialized* tier: aggregation results computed by the slow backend
are serialized (via :mod:`src.compression`) and persisted here so they survive
process restarts and back the higher tiers (L1/L2) on a cold start. Payloads are
stored as ``BYTEA`` using the flag-prefixed codec, so they may be plain JSON or
zstd-compressed transparently.

All SQL is fully parameterized (``$1, $2, ...``); the ``params`` dict is bound as
a single ``jsonb`` value via ``json.dumps(...)::jsonb`` rather than interpolated.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg

from src.compression import decode_value, encode_value


async def get(pool: asyncpg.Pool, key: str) -> Any | None:
    """Return the decoded result stored under ``key``, or ``None`` if absent."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload FROM precomputed_aggregates WHERE key = $1", key
        )
    if row is None:
        return None
    return decode_value(row["payload"])


async def get_query_params(
    pool: asyncpg.Pool, key: str
) -> tuple[str, dict[str, Any]] | None:
    """Return the ``(query, params)`` that produced the row under ``key``.

    Used by the proactive warmer (C14): recommendations carry only the cache
    *key* (plus query name and source), not the full ``params`` (start/end
    window). To re-warm a recommended key the warmer recovers its original
    ``(query, params)`` from here and replays it through
    :meth:`CacheManager.get`, which pulls the value up from L3 into L1/L2
    without recomputing when L3 already holds it.

    ``params`` is stored as ``jsonb``. asyncpg may decode it either as a native
    ``dict`` or as a JSON ``str`` (depending on codec registration), so we
    ``json.loads`` when it arrives as text and coerce a missing/odd value to an
    empty dict. Returns ``None`` if no row exists for ``key``.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT query, params FROM precomputed_aggregates WHERE key = $1", key
        )
    if row is None:
        return None

    params = row["params"]
    if isinstance(params, str):
        params = json.loads(params)
    if not isinstance(params, dict):
        params = {}
    return row["query"], params


async def get_with_meta(pool: asyncpg.Pool, key: str) -> dict[str, Any] | None:
    """Return ``{"result", "computed_at", "tags"}`` for ``key``, or ``None``.

    ``result`` is the decoded payload, ``computed_at`` the storage timestamp
    (:class:`datetime`), and ``tags`` the stored invalidation tags (``list[str]``).
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload, computed_at, tags "
            "FROM precomputed_aggregates WHERE key = $1",
            key,
        )
    if row is None:
        return None
    return {
        "result": decode_value(row["payload"]),
        "computed_at": row["computed_at"],
        "tags": list(row["tags"]) if row["tags"] is not None else [],
    }


async def upsert(
    pool: asyncpg.Pool,
    key: str,
    query: str,
    params: dict[str, Any],
    result: Any,
    *,
    tags: list[str] | None = None,
    compress: bool = False,
) -> None:
    """Insert-or-update the materialized result for ``key``.

    The result is encoded with the compression codec (optionally zstd) and
    written to ``payload``. On a primary-key conflict the row is overwritten and
    ``computed_at`` is refreshed to ``now()`` so freshness reflects the latest
    materialization.

    ``params`` is serialized to JSON text and cast to ``jsonb`` ($3::jsonb) so no
    user value is ever interpolated into the SQL string.
    """
    payload = encode_value(result, compress=compress)
    params_json = json.dumps(params or {}, default=str)
    tag_list = list(tags) if tags else []

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO precomputed_aggregates
                (key, query, params, payload, computed_at, tags)
            VALUES ($1, $2, $3::jsonb, $4, now(), $5)
            ON CONFLICT (key) DO UPDATE SET
                query = EXCLUDED.query,
                params = EXCLUDED.params,
                payload = EXCLUDED.payload,
                computed_at = now(),
                tags = EXCLUDED.tags
            """,
            key,
            query,
            params_json,
            payload,
            tag_list,
        )


async def delete(pool: asyncpg.Pool, key: str) -> int:
    """Delete the row for ``key``; return the number of rows removed (0 or 1)."""
    async with pool.acquire() as conn:
        status = await conn.execute(
            "DELETE FROM precomputed_aggregates WHERE key = $1", key
        )
    return _rowcount(status)


async def invalidate_pattern(pool: asyncpg.Pool, like_pattern: str) -> int:
    """Delete every row whose ``key`` matches the SQL ``LIKE`` ``like_pattern``.

    Returns the number of rows removed. The pattern is bound as ``$1`` (never
    interpolated), so callers may pass ``"q:%"`` etc. safely.
    """
    async with pool.acquire() as conn:
        status = await conn.execute(
            "DELETE FROM precomputed_aggregates WHERE key LIKE $1", like_pattern
        )
    return _rowcount(status)


def _rowcount(command_status: str) -> int:
    """Parse the affected-row count out of an asyncpg command-status string.

    asyncpg returns e.g. ``"DELETE 3"``; the trailing integer is the row count.
    Returns 0 if it can't be parsed.
    """
    try:
        return int(command_status.split()[-1])
    except (ValueError, IndexError, AttributeError):
        return 0
