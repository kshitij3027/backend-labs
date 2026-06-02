"""Integration tests for the proactive background warmer (``src.warmer``).

These run against the REAL Redis + Postgres wired by the compose ``test``
service, via the ``cache_manager`` fixture (which seeds a deterministic
``raw_logs`` corpus and builds the manager with a 100ms slow-backend delay and
the in-process ``PatternEngine`` exposed as ``cm.patterns``).

They verify the warmer's contract:

* **preload** — after some traffic builds recommendations *and* materializes
  L3, clearing L1 and running one sweep re-warms the top recommended key(s)
  into L1 (pulled up from L3, no recompute), so a follow-up ``get`` is an
  L1/L2 hit;
* **failure isolation** — a sweep whose per-item ``get`` raises still returns a
  dict rather than propagating;
* **near-expiry refresh** — with ``near_expiry_fraction=1.0`` every live L1 key
  counts as near-expiry and is refreshed in place;
* **warm_now(items)** — an explicit list of queries is warmed and counted;
* **run() honors stop_event** — the background loop exits promptly when the
  event is set.
"""
from __future__ import annotations

import asyncio

from src.keys import cache_key
from src.warmer import Warmer

# A small set of distinct (query, params) the warming tests replay. Each lands
# in both the pattern engine (via record_query) and L3 (via materialize) when
# first served through cm.get(...).
_WINDOW = {"start": 1_779_000_000, "end": 1_781_000_000}
_ERROR_RATE_SOURCES = ("api", "web", "db")


def _error_rate_params(source: str) -> dict:
    return {"source": source, **_WINDOW}


async def _seed_traffic(cm) -> list[str]:
    """Drive a few distinct gets; return their semantic keys (hottest first-ish).

    Builds pattern recommendations AND populates L3 for each (query, params).
    """
    keys: list[str] = []
    for source in _ERROR_RATE_SOURCES:
        params = _error_rate_params(source)
        await cm.get("error_rate", params)
        keys.append(cache_key("error_rate", params, bucket_seconds=300))
    return keys


async def test_warm_once_preloads_top_recommendation_into_l1(cache_manager):
    """A sweep re-warms the top recommended key into L1; next get is L1/L2."""
    cm = cache_manager
    await _seed_traffic(cm)

    # The hottest recommended key drives the assertion below.
    recs = cm.patterns.recommendations(10)
    assert recs, "expected non-empty recommendations after seeding traffic"
    top_key = recs[0]["key"]

    # Cold-start the fast tier: drop everything from L1 so the warmer must work.
    cm.l1.clear()
    assert cm.l1.get(top_key) is None

    w = Warmer(cm, cm.patterns, top_n=10)
    res = await w.warm_once()

    assert res["warmed"] >= 1
    # The top recommended key is now hot in L1 again.
    assert cm.l1.get(top_key) is not None

    # And a follow-up get for that same query/params is served from a fast tier
    # (pulled up from L3 by the warmer, not recomputed by the backend).
    # Recover the original params for the top key from the seeded set.
    served = None
    for source in _ERROR_RATE_SOURCES:
        params = _error_rate_params(source)
        if cache_key("error_rate", params, bucket_seconds=300) == top_key:
            served = await cm.get("error_rate", params)
            break
    assert served is not None
    assert served.tier in ("l1", "l2")


async def test_warm_once_isolates_per_item_failures(cache_manager, monkeypatch):
    """If cm.get raises, warm_once must not propagate — it returns a dict."""
    cm = cache_manager
    await _seed_traffic(cm)
    cm.l1.clear()

    async def boom(*args, **kwargs):
        raise RuntimeError("backend exploded")

    monkeypatch.setattr(cm, "get", boom)

    w = Warmer(cm, cm.patterns, top_n=10)
    # Must not raise despite every per-item get blowing up.
    res = await w.warm_once()
    assert isinstance(res, dict)
    assert res == {"warmed": 0, "refreshed": 0}


async def test_warm_once_refreshes_near_expiry_keys(cache_manager):
    """With fraction=1.0 all live L1 keys are near-expiry and get refreshed."""
    cm = cache_manager
    await _seed_traffic(cm)

    # Ensure the keys are present in L1 (a fresh warm sweep guarantees this).
    w = Warmer(cm, cm.patterns, top_n=10)
    await w.warm_once()
    assert len(cm.l1) >= 1

    # fraction=1.0 => every live key counts as near-expiry.
    w2 = Warmer(cm, cm.patterns, near_expiry_fraction=1.0)
    res = await w2.warm_once()

    assert res["refreshed"] >= 1
    # Keys are still present after the in-place refresh.
    assert len(cm.l1) >= 1


async def test_warm_now_with_explicit_items(cache_manager):
    """warm_now(items) replays each query and counts successes; key lands in L1."""
    cm = cache_manager
    params = {"start": 1_779_000_000, "end": 1_781_000_000}
    key = cache_key("top_sources", params, bucket_seconds=300)

    cm.l1.clear()
    assert cm.l1.get(key) is None

    w = Warmer(cm, cm.patterns)
    n = await w.warm_now([{"query": "top_sources", "params": params}])

    assert n == 1
    assert cm.l1.get(key) is not None


async def test_run_honors_stop_event(cache_manager):
    """run() loops on a tiny interval and exits promptly when stop_event is set."""
    cm = cache_manager
    await _seed_traffic(cm)

    w_fast = Warmer(cm, cm.patterns, interval_seconds=0.05, top_n=10)
    stop = asyncio.Event()
    task = asyncio.create_task(w_fast.run(stop))

    # Let a few sweeps run, then request a stop.
    await asyncio.sleep(0.15)
    stop.set()

    # The loop must observe the event and exit well within the timeout.
    await asyncio.wait_for(task, timeout=2.0)
    assert task.done()
    assert task.exception() is None
