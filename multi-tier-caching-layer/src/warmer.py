"""Proactive background cache warmer (Feature Area B — §3).

The warmer is the *proactive* half of the system: while
:class:`~src.cache_manager.CacheManager` serves reads reactively (and the
pattern engine learns *what* is hot), this task periodically pushes the hottest
queries into the fast tiers **before** they are next requested, so the cold-start
latency is amortized away.

A single sweep (:meth:`Warmer.warm_once`) does two things:

* **Preload** — pulls the top recommendations from the heuristic
  :class:`~src.patterns.PatternEngine`. Recommendations carry the cache *key*
  (plus query name and source) but **not** the full ``params`` (the start/end
  time window). For each recommended key that is *not* already hot in L1, the
  warmer recovers its original ``(query, params)`` from L3 via
  :func:`src.l3_store.get_query_params` and replays it through
  :meth:`CacheManager.get`. That single call walks the tier hierarchy: if L3
  still holds the materialized value it is pulled **up** into L1+L2 with **no
  backend recompute**; only a genuine full miss (value evicted from every tier)
  pays the slow backend.
* **Refresh near-expiry** — asks L1 for keys within the final ``fraction`` of
  their TTL (:meth:`L1Cache.near_expiry_keys`), evicts each, then re-gets it
  so it is re-populated with a *fresh* L1 TTL. This keeps perennially-hot
  keys from ever lapsing to a cold miss.

Design notes
------------
* **Per-item exception isolation.** Every recommendation and every refresh is
  wrapped in its own ``try/except`` — one bad key (e.g. an unknown query, a
  transient L2 blip) logs and is skipped; it never aborts the rest of the sweep.
* **Responsive shutdown.** :meth:`run` sleeps on ``stop_event.wait()`` with a
  timeout instead of a bare ``asyncio.sleep``, so a requested stop is honored
  promptly rather than after a full interval.
* **Module-level ``l3_store`` import** mirrors the cache manager so the lookup
  path is easy to reason about (and monkeypatch) in tests.
"""
from __future__ import annotations

import asyncio
import logging

from src import l3_store

logger = logging.getLogger(__name__)


class Warmer:
    """Periodic preload + near-expiry refresh over a :class:`CacheManager`.

    Args:
        cache_manager: the read-through :class:`~src.cache_manager.CacheManager`;
            exposes ``.l1`` (L1 cache), ``.pg_pool`` (L3 backing pool), and the
            ``get(query, params)`` entry point the warmer replays through.
        patterns: the :class:`~src.patterns.PatternEngine` supplying ranked
            warming recommendations. Accepted explicitly even though it is also
            reachable via ``cache_manager.patterns``, so the warmer can be driven
            with a stand-in engine in isolation.
        interval_seconds: seconds between background sweeps in :meth:`run`.
        top_n: how many recommendations to consider per preload sweep.
        near_expiry_fraction: an L1 entry is refreshed when only this final
            fraction of its TTL remains (``1.0`` => every live key qualifies).
    """

    def __init__(
        self,
        cache_manager,
        patterns,
        *,
        interval_seconds: float = 5.0,
        top_n: int = 20,
        near_expiry_fraction: float = 0.2,
    ) -> None:
        self.cm = cache_manager
        self.patterns = patterns
        self.interval_seconds = interval_seconds
        self.top_n = top_n
        self.near_expiry_fraction = near_expiry_fraction

    # ------------------------------------------------------------------ #
    # One sweep: preload top recommendations + refresh near-expiry L1 keys
    # ------------------------------------------------------------------ #
    async def warm_once(self) -> dict:
        """Run one preload + refresh sweep.

        Returns a tally ``{"warmed": int, "refreshed": int}`` — the number of
        recommended keys preloaded (those that were missing from L1) and the
        number of near-expiry L1 keys refreshed. Per-item failures are logged
        and skipped; the sweep always returns a dict and never raises.
        """
        warmed = 0
        refreshed = 0

        # --- PRELOAD: hottest recommended keys missing from L1 ------------ #
        recs = self.patterns.recommendations(self.top_n)
        for rec in recs:
            key = rec["key"]
            try:
                # Already hot in L1 — nothing to do.
                if self.cm.l1.get(key) is not None:
                    continue
                qp = await l3_store.get_query_params(self.cm.pg_pool, key)
                if qp is None:
                    # No L3 row records this key's (query, params); can't replay.
                    continue
                query, params = qp
                # Pulls L3 -> L1/L2 (no recompute) when L3 holds the value.
                await self.cm.get(query, params)
                warmed += 1
            except Exception:  # noqa: BLE001 — isolate one bad key per the spec
                logger.exception("warmer: preload failed for key %r", key)
                continue

        # --- REFRESH: near-expiry L1 keys get a fresh TTL ----------------- #
        keys = self.cm.l1.near_expiry_keys(self.near_expiry_fraction)
        for key in keys:
            try:
                qp = await l3_store.get_query_params(self.cm.pg_pool, key)
                if qp is None:
                    continue
                query, params = qp
                # Drop then re-get so L1 is re-populated with a fresh TTL.
                self.cm.l1.delete(key)
                await self.cm.get(query, params)
                refreshed += 1
            except Exception:  # noqa: BLE001 — isolate one bad key per the spec
                logger.exception("warmer: refresh failed for key %r", key)
                continue

        return {"warmed": warmed, "refreshed": refreshed}

    # ------------------------------------------------------------------ #
    # Manual warm (backs POST /cache/warm in C15)
    # ------------------------------------------------------------------ #
    async def warm_now(self, items: list[dict] | None = None) -> int:
        """Warm an explicit list of queries, or run one full sweep.

        When ``items`` is given, each ``{"query": ..., "params": ...}`` is
        replayed through :meth:`CacheManager.get` (a missing ``params`` defaults
        to ``{}``); the number of successful warms is returned. Per-item failures
        are logged and skipped.

        When ``items`` is falsy, a single :meth:`warm_once` sweep is run and its
        ``"warmed"`` count is returned. This backs the ``POST /cache/warm``
        endpoint (C15), which may target specific keys or trigger a sweep.
        """
        if not items:
            res = await self.warm_once()
            return res["warmed"]

        count = 0
        for item in items:
            try:
                await self.cm.get(item["query"], item.get("params") or {})
                count += 1
            except Exception:  # noqa: BLE001 — isolate one bad item
                logger.exception("warmer: warm_now failed for item %r", item)
                continue
        return count

    # ------------------------------------------------------------------ #
    # Background loop
    # ------------------------------------------------------------------ #
    async def run(self, stop_event: asyncio.Event) -> None:
        """Run sweeps every ``interval_seconds`` until ``stop_event`` is set.

        Each iteration runs :meth:`warm_once` with its own exception guard (a
        failed sweep is logged and the loop continues), then waits up to
        ``interval_seconds`` on ``stop_event`` — so a requested stop is honored
        promptly instead of after a full sleep. Exits cleanly once the event is
        set.
        """
        while not stop_event.is_set():
            try:
                await self.warm_once()
            except Exception:  # noqa: BLE001 — a bad sweep must not kill the loop
                logger.exception("warmer: sweep failed")
            # Responsive sleep: wake early if stop is requested.
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self.interval_seconds
                )
            except asyncio.TimeoutError:
                pass
