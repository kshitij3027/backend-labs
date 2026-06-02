"""The slow log-query backend — the *source of truth* the cache fronts.

Every supported aggregation is a real ``GROUP BY`` (or aggregate) scan over the
seeded ``raw_logs`` table. This is deliberately the expensive path: a tunable
``delay_ms`` simulates the cost of a wide table scan so the demo can show
"seconds -> milliseconds" once results are cached / materialized into L3.

Security: user-supplied values are **never** string-interpolated into SQL. All
predicate values are bound as ``$1, $2, ...`` parameters, and the only token
that must appear inline — the ``date_trunc`` bucket — is validated against a
fixed whitelist (:data:`_ALLOWED_BUCKETS`) before use, so an attacker cannot
inject SQL through the ``bucket`` param.

Results are returned as plain JSON-serializable structures: ``date_trunc``
timestamps are converted to ISO-8601 strings, counts to ``int``, and averages
to ``float``.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import asyncpg

# The set of aggregation query names this backend knows how to run. Anything
# outside this set raises ``ValueError`` (callers treat that as a 4xx).
SUPPORTED_QUERIES: set[str] = {
    "requests_over_time",
    "error_rate",
    "avg_latency",
    "top_sources",
}

# Whitelist of acceptable ``date_trunc`` field tokens. The bucket name is the
# one value that must be interpolated into the SQL text (Postgres does not let
# ``date_trunc``'s field be a bind parameter), so it is strictly validated here
# to keep the query injection-proof.
_ALLOWED_BUCKETS: set[str] = {"minute", "hour", "day"}

# Default time bucket for the time-series style queries.
_DEFAULT_BUCKET = "hour"


def _to_datetime(v: Any) -> datetime | None:
    """Coerce ``v`` into a timezone-aware UTC datetime, or ``None``.

    Accepts:
      * ``None`` -> ``None`` (lets optional filters pass straight through);
      * an ``int``/``float`` epoch (seconds) -> UTC datetime;
      * an ISO-8601 string -> UTC datetime (naive inputs are assumed UTC);
      * a ``datetime`` -> normalized to UTC.

    Raises :class:`ValueError`/:class:`TypeError` on an uninterpretable value.
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, bool):
        # Guard: bools are ints in Python but are never valid timestamps.
        raise TypeError(f"cannot interpret bool as datetime: {v!r}")
    elif isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    elif isinstance(v, str):
        dt = datetime.fromisoformat(v)
    else:
        raise TypeError(f"cannot interpret {type(v).__name__} as datetime: {v!r}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _validate_bucket(params: dict[str, Any]) -> str:
    """Return a whitelisted ``date_trunc`` bucket token from ``params``.

    Falls back to :data:`_DEFAULT_BUCKET` when unset. Raises :class:`ValueError`
    for any value not in :data:`_ALLOWED_BUCKETS` so an injected string can never
    reach the SQL text.
    """
    bucket = params.get("bucket", _DEFAULT_BUCKET)
    if bucket is None:
        bucket = _DEFAULT_BUCKET
    bucket = str(bucket).lower()
    if bucket not in _ALLOWED_BUCKETS:
        raise ValueError(
            f"unsupported bucket {bucket!r}; expected one of "
            f"{sorted(_ALLOWED_BUCKETS)}"
        )
    return bucket


def _time_filters(
    params: dict[str, Any],
    args: list[Any],
    *,
    include_source: bool = True,
) -> str:
    """Build a parameterized WHERE clause from optional source/start/end filters.

    Appends bound values to ``args`` (so positional ``$N`` placeholders line up
    with the running list length) and returns either ``""`` or a
    ``" WHERE ..."`` fragment. ``start`` is inclusive (``ts >= start``); ``end``
    is exclusive (``ts < end``) — a half-open window so adjacent ranges don't
    double-count a boundary row.
    """
    clauses: list[str] = []

    if include_source:
        source = params.get("source")
        if source is not None:
            args.append(str(source))
            clauses.append(f"source = ${len(args)}")

    start = _to_datetime(params.get("start"))
    if start is not None:
        args.append(start)
        clauses.append(f"ts >= ${len(args)}")

    end = _to_datetime(params.get("end"))
    if end is not None:
        args.append(end)
        clauses.append(f"ts < ${len(args)}")

    if not clauses:
        return ""
    return " WHERE " + " AND ".join(clauses)


def _iso(dt: datetime) -> str:
    """Render a datetime row value as an ISO-8601 string (JSON-friendly)."""
    return dt.isoformat()


async def _requests_over_time(
    conn: asyncpg.Connection, params: dict[str, Any]
) -> list[dict[str, Any]]:
    bucket = _validate_bucket(params)
    args: list[Any] = []
    where = _time_filters(params, args)
    # ``bucket`` is whitelisted above; everything else is bound as $N.
    sql = (
        f"SELECT date_trunc('{bucket}', ts) AS bucket, count(*) AS count "
        f"FROM raw_logs{where} GROUP BY bucket ORDER BY bucket"
    )
    rows = await conn.fetch(sql, *args)
    return [{"bucket": _iso(r["bucket"]), "count": int(r["count"])} for r in rows]


async def _error_rate(
    conn: asyncpg.Connection, params: dict[str, Any]
) -> dict[str, Any]:
    args: list[Any] = []
    where = _time_filters(params, args)
    sql = (
        "SELECT count(*) AS total, "
        "count(*) FILTER (WHERE level = 'ERROR') AS errors "
        f"FROM raw_logs{where}"
    )
    row = await conn.fetchrow(sql, *args)
    total = int(row["total"]) if row and row["total"] is not None else 0
    errors = int(row["errors"]) if row and row["errors"] is not None else 0
    rate = (errors / total) if total else 0.0
    return {"total": total, "errors": errors, "error_rate": rate}


async def _avg_latency(
    conn: asyncpg.Connection, params: dict[str, Any]
) -> list[dict[str, Any]]:
    bucket = _validate_bucket(params)
    args: list[Any] = []
    where = _time_filters(params, args)
    sql = (
        f"SELECT date_trunc('{bucket}', ts) AS bucket, "
        "avg(latency_ms) AS avg_latency_ms "
        f"FROM raw_logs{where} GROUP BY bucket ORDER BY bucket"
    )
    rows = await conn.fetch(sql, *args)
    return [
        {
            "bucket": _iso(r["bucket"]),
            "avg_latency_ms": float(r["avg_latency_ms"]),
        }
        for r in rows
    ]


async def _top_sources(
    conn: asyncpg.Connection, params: dict[str, Any]
) -> list[dict[str, Any]]:
    args: list[Any] = []
    # No source filter for top_sources (we're ranking across all sources), but
    # start/end windowing still applies.
    where = _time_filters(params, args, include_source=False)

    limit = params.get("limit", 5)
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        raise ValueError(f"invalid limit: {limit!r}")
    if limit <= 0:
        raise ValueError(f"limit must be positive, got {limit}")

    args.append(limit)
    sql = (
        "SELECT source, count(*) AS count "
        f"FROM raw_logs{where} GROUP BY source ORDER BY count DESC, source "
        f"LIMIT ${len(args)}"
    )
    rows = await conn.fetch(sql, *args)
    return [{"source": r["source"], "count": int(r["count"])} for r in rows]


# Dispatch table: query name -> coroutine implementing it.
_DISPATCH = {
    "requests_over_time": _requests_over_time,
    "error_rate": _error_rate,
    "avg_latency": _avg_latency,
    "top_sources": _top_sources,
}


async def run_aggregation(
    pool: asyncpg.Pool,
    query: str,
    params: dict[str, Any] | None = None,
    *,
    delay_ms: int = 0,
) -> Any:
    """Run a supported aggregation against ``raw_logs`` and return JSON-able data.

    Args:
        pool: an :class:`asyncpg.Pool` connected to the logs Postgres.
        query: one of :data:`SUPPORTED_QUERIES`.
        params: optional per-query filter dict (``source``, ``start``, ``end``,
            ``bucket``, ``limit`` — see each query's contract).
        delay_ms: when ``> 0``, sleep this many milliseconds first to simulate
            the cost of a real wide table scan (the "slow backend" knob).

    Returns:
        * ``requests_over_time`` -> ``list[{"bucket": iso, "count": int}]``
        * ``error_rate``         -> ``{"total", "errors", "error_rate"}``
        * ``avg_latency``        -> ``list[{"bucket": iso, "avg_latency_ms": float}]``
        * ``top_sources``        -> ``list[{"source": str, "count": int}]``

    Raises:
        ValueError: if ``query`` is not supported (or a param is invalid).
    """
    if query not in _DISPATCH:
        raise ValueError(
            f"unsupported query {query!r}; expected one of {sorted(SUPPORTED_QUERIES)}"
        )

    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000.0)

    params = params or {}
    handler = _DISPATCH[query]
    async with pool.acquire() as conn:
        return await handler(conn, params)
