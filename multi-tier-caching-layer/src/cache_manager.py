"""Read-through, multi-tier cache manager (the orchestration keystone).

This is the heart of the project: it wires the four tiers together into a
single ``get(query, params)`` entry point and implements the **read-through
hierarchy with upward backfill** the whole system is built around.

Lookup order (fastest -> slowest), with backfill of every faster tier on the
way back up:

1. **L1** — in-process LRU+TTL (:class:`src.l1_cache.L1Cache`). Sub-millisecond.
2. **L2** — Redis (:class:`src.l2_redis.L2Redis`). Network-local; fail-soft.
3. **L3** — materialized Postgres aggregates (:mod:`src.l3_store`). Survives
   restarts; backs the upper tiers on a cold start.
4. **backend** — the slow source of truth, computed via
   :func:`src.materializer.materialize` (which runs the real ``GROUP BY`` scan
   and upserts the result into L3). The compute is wrapped in
   :class:`src.singleflight.SingleFlight` so a herd of concurrent misses for the
   same key collapses onto a single backend call.

Each ``get`` returns a :class:`CacheResult` stamping which ``tier`` served the
value, the wall ``elapsed_ms``, the semantic ``key``, and whether L2 is
currently ``degraded``. Every served request is also fed to the metrics
aggregator and the heuristic pattern engine.

Design notes
------------
* **Fail-soft w.r.t. L2.** :class:`L2Redis` never raises (it degrades to a
  miss and flips ``degraded``), so an L2 outage simply falls through to L3 /
  the backend — the system keeps serving. A :class:`ValueError` from the
  backend (an *unknown query*), however, **does** propagate out of ``get`` so
  the API layer can map it to a 4xx.
* **Deterministic ``get``.** ``get`` does not spawn background early-refresh
  tasks; proactive near-expiry refresh is the warmer's job (C14). This keeps
  request handling synchronous and the integration tests deterministic.
* **Module-level imports for monkeypatchability.** ``materialize`` and
  ``l3_store`` are imported at module scope so tests can monkeypatch
  ``src.cache_manager.materialize`` to count backend computations.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from src import l3_store
from src.keys import cache_key, tags_for
from src.materializer import materialize


@dataclass
class CacheResult:
    """The outcome of a single :meth:`CacheManager.get` call.

    Attributes
    ----------
    result:
        The query result (a JSON-serializable structure — dict or list).
    tier:
        Which tier served the value: exactly one of ``"l1"``, ``"l2"``,
        ``"l3"``, or ``"backend"``.
    elapsed_ms:
        Wall-clock time spent serving this request, in milliseconds.
    key:
        The semantic cache key the result is stored under.
    degraded:
        Whether the L2 (Redis) tier is currently degraded at the moment this
        request finished.
    """

    result: Any
    tier: str
    elapsed_ms: float
    key: str
    degraded: bool


class CacheManager:
    """Read-through cache over L1 -> L2 -> L3 -> slow backend with backfill.

    The manager owns no tier state itself; it is composed of the already-built
    tier objects and observability components, which keeps it trivially testable
    (each collaborator can be a real instance or a fake).

    Args:
        l1: in-process :class:`src.l1_cache.L1Cache`.
        l2: :class:`src.l2_redis.L2Redis` distributed tier (fail-soft).
        pg_pool: :class:`asyncpg.Pool` backing L3 + the slow backend.
        metrics: :class:`src.metrics.Metrics` aggregator.
        patterns: :class:`src.patterns.PatternEngine` heuristic learner.
        singleflight: :class:`src.singleflight.SingleFlight` request coalescer.
        time_bucket_seconds: timestamp-bucket width for semantic cache keys.
        backend_delay_ms: artificial slow-backend delay forwarded to materialize.
        l2_ttl_seconds: TTL applied when writing to L2.
        l2_compress: whether L2/L3 payloads are zstd-compressed.
    """

    def __init__(
        self,
        *,
        l1,
        l2,
        pg_pool,
        metrics,
        patterns,
        singleflight,
        time_bucket_seconds: int = 300,
        backend_delay_ms: int = 0,
        l2_ttl_seconds: int = 600,
        l2_compress: bool = False,
    ) -> None:
        self.l1 = l1
        self.l2 = l2
        self.pg_pool = pg_pool
        self.metrics = metrics
        self.patterns = patterns
        self.singleflight = singleflight
        self.time_bucket_seconds = time_bucket_seconds
        self.backend_delay_ms = backend_delay_ms
        self.l2_ttl_seconds = l2_ttl_seconds
        self.l2_compress = l2_compress

    # ------------------------------------------------------------------ #
    # Public read-through entry point
    # ------------------------------------------------------------------ #
    async def get(self, query: str, params: dict | None = None) -> CacheResult:
        """Resolve ``(query, params)`` through the tier hierarchy.

        Walks L1 -> L2 -> L3 -> backend, returning as soon as a tier has the
        value and backfilling every faster tier on the way up. On a full miss
        the backend compute is coalesced through single-flight so concurrent
        identical misses run the backend exactly once.

        Raises:
            ValueError: propagated from the backend when ``query`` is not a
                supported aggregation (the API maps this to a 400).
        """
        params = params or {}
        key = cache_key(query, params, bucket_seconds=self.time_bucket_seconds)
        source = params.get("source")
        t0 = time.monotonic()

        def finalize(tier: str, value: Any) -> CacheResult:
            """Stamp timing, record metrics + patterns, and build the result."""
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            # Reflect the current L2 health into the metrics surface.
            self.metrics.mark_l2_degraded(self.l2.degraded)
            self.metrics.record_request(tier, elapsed_ms)
            self.patterns.record_query(key, query, source, elapsed_ms)
            return CacheResult(
                result=value,
                tier=tier,
                elapsed_ms=elapsed_ms,
                key=key,
                degraded=self.l2.degraded,
            )

        # 1) L1 — in-process, fastest.
        v = self.l1.get(key)
        if v is not None:
            return finalize("l1", v)

        # 2) L2 — Redis (fail-soft: a miss/failure simply returns None).
        v = await self.l2.get(key)
        if v is not None:
            # Backfill the faster tier so the next read is L1.
            self.l1.set(key, v)
            return finalize("l2", v)

        # 3) L3 — materialized Postgres aggregate.
        v = await l3_store.get(self.pg_pool, key)
        if v is not None:
            # Backfill both faster tiers.
            self.l1.set(key, v)
            await self.l2.set(
                key,
                v,
                ttl=self.l2_ttl_seconds,
                tags=list(tags_for(query, params)),
                compress=self.l2_compress,
            )
            return finalize("l3", v)

        # 4) Full miss — compute via the slow backend, coalescing the herd.
        result = await self.singleflight.do(
            key, lambda: self._compute(query, params, key)
        )
        return finalize("backend", result)

    # ------------------------------------------------------------------ #
    # Backend compute + upward population
    # ------------------------------------------------------------------ #
    async def _compute(self, query: str, params: dict, key: str) -> Any:
        """Compute via the slow backend and populate the upper tiers.

        :func:`src.materializer.materialize` runs the real aggregation and
        upserts the result into L3, so this method only needs to populate L1 and
        L2 afterwards. A :class:`ValueError` from an unknown query propagates
        (single-flight re-raises it to every waiter).
        """
        _key, result = await materialize(
            self.pg_pool,
            query,
            params,
            delay_ms=self.backend_delay_ms,
            bucket_seconds=self.time_bucket_seconds,
            compress=self.l2_compress,
        )
        # Populate the faster tiers so the next read short-circuits.
        self.l1.set(key, result)
        await self.l2.set(
            key,
            result,
            ttl=self.l2_ttl_seconds,
            tags=list(tags_for(query, params)),
            compress=self.l2_compress,
        )
        return result

    # ------------------------------------------------------------------ #
    # Tier-walk read without side effects (no backend, no metrics)
    # ------------------------------------------------------------------ #
    async def _peek(self, key: str) -> Any | None:
        """Return the cached value for ``key`` from the fastest tier holding it.

        Walks L1 -> L2 -> L3 and backfills the faster tiers on the way up, but —
        unlike :meth:`get` — it does **not** fall through to the slow backend and
        does **not** record metrics or query patterns. It is the read side of
        :meth:`append_timeseries`, which must inspect (and incrementally update)
        an already-cached series without paying for or skewing a backend compute.

        Returns the value, or ``None`` if no tier currently holds ``key``.
        """
        # 1) L1 — in-process, fastest.
        v = self.l1.get(key)
        if v is not None:
            return v

        # 2) L2 — Redis (fail-soft: a miss/failure simply returns None).
        v = await self.l2.get(key)
        if v is not None:
            self.l1.set(key, v)  # backfill L1
            return v

        # 3) L3 — materialized Postgres aggregate.
        v = await l3_store.get(self.pg_pool, key)
        if v is not None:
            # Backfill both faster tiers (no tags — we don't have query/params here).
            self.l1.set(key, v)
            await self.l2.set(
                key, v, ttl=self.l2_ttl_seconds, compress=self.l2_compress
            )
            return v

        return None

    # ------------------------------------------------------------------ #
    # Selective invalidation
    # ------------------------------------------------------------------ #
    async def invalidate(
        self,
        *,
        pattern: str | None = None,
        tags: list[str] | None = None,
    ) -> dict:
        """Evict cache entries by glob ``pattern`` and/or invalidation ``tags``.

        At least one of ``pattern`` or ``tags`` must be supplied (else a
        :class:`ValueError` is raised). Both may be supplied, in which case their
        per-tier removal counts are summed.

        * **pattern** — a glob (e.g. ``"q:*"``). L1 is purged via
          :meth:`L1Cache.scan_delete`, L2 via :meth:`L2Redis.invalidate_pattern`
          (``SCAN`` based), and L3 via :func:`l3_store.invalidate_pattern` after
          translating the glob to a SQL ``LIKE`` pattern (``*`` -> ``%``,
          ``?`` -> ``_``).
        * **tags** — for each tag, the member keys recorded in its ``tag:<tag>``
          set are read via :meth:`L2Redis.tag_members`, unioned, then deleted
          from **every** tier (L1, L2, L3). The ``tag:<tag>`` set itself is then
          dropped so stale bookkeeping doesn't linger.

        Returns a per-tier removal tally ``{"l1": int, "l2": int, "l3": int}``
        (pattern + tag contributions summed). Robust to a tier reporting 0 or
        degrading (L2 ops are fail-soft and simply contribute 0).
        """
        if not pattern and not tags:
            raise ValueError("invalidate() requires at least one of pattern or tags")

        l1_n = 0
        l2_n = 0
        l3_n = 0

        # --- pattern invalidation across all three tiers ------------------ #
        if pattern:
            l1_n += self.l1.scan_delete(pattern)
            l2_n += await self.l2.invalidate_pattern(pattern)
            like_pattern = pattern.replace("*", "%").replace("?", "_")
            l3_n += await l3_store.invalidate_pattern(self.pg_pool, like_pattern)

        # --- tag invalidation: resolve members, delete from every tier ---- #
        if tags:
            keys: set[str] = set()
            for tag in tags:
                members = await self.l2.tag_members(tag)
                keys.update(members)

            for key in keys:
                if self.l1.delete(key):
                    l1_n += 1
                l2_n += await self.l2.delete(key)
                l3_n += await l3_store.delete(self.pg_pool, key)

            # Drop the tag sets themselves now that their members are gone.
            for tag in tags:
                await self.l2.delete(f"tag:{tag}")

        return {"l1": l1_n, "l2": l2_n, "l3": l3_n}

    # ------------------------------------------------------------------ #
    # Incremental time-series append (no backend recompute)
    # ------------------------------------------------------------------ #
    async def append_timeseries(
        self, query: str, params: dict | None, point: dict
    ) -> bool:
        """Append ``point`` to an already-cached time-series, updating all tiers.

        This is the §3 *incremental update* path: instead of re-running the slow
        backend to refresh a time-series, a single new data ``point`` is appended
        to the existing cached list and the result is re-stored across L1, L2, and
        L3 — **no backend recompute happens**.

        The current value is read via :meth:`_peek` (so the backend is never
        invoked). If nothing is cached for ``(query, params)``, or the cached
        value is not a ``list``, this returns ``False`` (the caller may choose to
        compute the series first via :meth:`get`). Otherwise ``point`` is appended
        to a shallow copy of the list, the new list is written to every tier with
        the query's invalidation tags, and ``True`` is returned.
        """
        params = params or {}
        key = cache_key(query, params, bucket_seconds=self.time_bucket_seconds)

        current = await self._peek(key)
        if current is None or not isinstance(current, list):
            return False

        new_list = list(current)
        new_list.append(point)

        tags = list(tags_for(query, params))
        self.l1.set(key, new_list)
        await self.l2.set(
            key,
            new_list,
            ttl=self.l2_ttl_seconds,
            tags=tags,
            compress=self.l2_compress,
        )
        await l3_store.upsert(
            self.pg_pool,
            key,
            query,
            params,
            new_list,
            tags=tags,
            compress=self.l2_compress,
        )
        return True
