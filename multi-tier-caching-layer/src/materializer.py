"""Materialize slow-backend aggregations into the L3 store.

The materializer is the bridge between the expensive source of truth
(:func:`src.backend.run_aggregation`) and the persistent L3 tier
(:mod:`src.l3_store`): it runs an aggregation once, then writes the result under
its semantic cache key with the matching invalidation tags. The background
warmer and the cache manager call this on an L3 miss so subsequent reads are
cheap.
"""
from __future__ import annotations

from typing import Any

import asyncpg

from src import l3_store
from src.backend import run_aggregation
from src.keys import cache_key, tags_for


async def materialize(
    pool: asyncpg.Pool,
    query: str,
    params: dict[str, Any] | None = None,
    *,
    delay_ms: int = 0,
    bucket_seconds: int = 300,
    compress: bool = False,
) -> tuple[str, Any]:
    """Compute ``(query, params)`` via the backend and upsert it into L3.

    Args:
        pool: asyncpg pool for both the backend scan and the L3 write.
        query: a :data:`src.backend.SUPPORTED_QUERIES` name.
        params: optional filter dict passed through to the backend and used to
            derive the cache key + tags.
        delay_ms: slow-backend delay forwarded to :func:`run_aggregation`.
        bucket_seconds: timestamp-bucket width for the semantic cache key.
        compress: when ``True``, the L3 payload is zstd-compressed.

    Returns:
        ``(key, result)`` — the semantic cache key the result was stored under
        and the freshly computed result.
    """
    result = await run_aggregation(pool, query, params, delay_ms=delay_ms)
    key = cache_key(query, params, bucket_seconds=bucket_seconds)
    tags = list(tags_for(query, params))
    await l3_store.upsert(
        pool, key, query, params or {}, result, tags=tags, compress=compress
    )
    return key, result
