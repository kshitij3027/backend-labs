"""In-process request coalescing (single-flight) and probabilistic early refresh.

This module provides two independent stampede-prevention primitives used by the
cache manager:

* :class:`SingleFlight` — coalesces concurrent calls for the *same* key onto a
  single in-flight computation. Only the first ("leader") caller runs the
  expensive ``factory``; all concurrent callers ("followers") await the same
  result (or the same exception). This prevents a "thundering herd" of backend
  queries when many requests miss the cache for the same key at once.

* :func:`should_early_refresh` — an XFetch-style probabilistic early-expiration
  decision. Rather than letting an entry expire and stampede on the resulting
  miss, callers occasionally refresh it *before* it expires, with a probability
  that rises as the entry approaches its TTL.

Both primitives are pure-stdlib (``asyncio``, ``math``, ``random``) and carry no
project dependencies, so they can be imported as
``from src.singleflight import SingleFlight, should_early_refresh`` with
``PYTHONPATH=/app``.
"""
from __future__ import annotations

import asyncio
import math
import random
from typing import Any, Awaitable, Callable

__all__ = ["SingleFlight", "should_early_refresh"]


class SingleFlight:
    """Coalesce concurrent same-key async computations onto one shared result.

    Designed for use within a single asyncio event loop. For a given ``key``,
    the first caller becomes the *leader* and actually awaits ``factory()``;
    any other caller arriving while that computation is still in flight becomes
    a *follower* and awaits the leader's result instead of calling ``factory``
    again. Whether the leader succeeds or raises, the result/exception is
    propagated to every waiter and the key is then cleared, so a subsequent
    call for the same key starts a fresh computation.
    """

    def __init__(self) -> None:
        # Maps an in-flight key to the Future that will hold its result.
        self._inflight: dict[str, asyncio.Future] = {}

    async def do(self, key: str, factory: Callable[[], Awaitable[Any]]) -> Any:
        """Run ``factory`` for ``key``, coalescing concurrent same-key calls.

        If a computation for ``key`` is already in flight, await it and return
        its result (``factory`` is NOT called again). Otherwise become the
        leader: register an in-flight future, ``await factory()``, publish the
        result (or exception) to all waiters, and always clear the key in a
        ``finally`` block so later calls re-run ``factory``.

        :param key: Coalescing key; concurrent calls sharing this key share one
            result.
        :param factory: Zero-arg callable returning an awaitable that produces
            the value. Only invoked by the leader.
        :returns: The value produced by the leader's ``factory()``.
        :raises: Whatever ``factory()`` raises, propagated to every waiter.
        """
        existing = self._inflight.get(key)
        if existing is not None:
            # Follower: await the leader's result. ``await`` on a Future yields
            # its result or re-raises its exception, identical for all waiters.
            return await existing

        # Leader: create the shared future BEFORE the first await so that any
        # follower scheduled in the meantime observes it and coalesces.
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._inflight[key] = future
        try:
            result = await factory()
        except BaseException as exc:  # noqa: BLE001 - propagate to all waiters
            # Publish the exception to followers, then re-raise for the leader.
            if not future.done():
                future.set_exception(exc)
            raise
        else:
            if not future.done():
                future.set_result(result)
            return result
        finally:
            # Always clear the key so a later call re-runs the factory. Pop the
            # exact future we installed (defensive: never evict a newer one).
            if self._inflight.get(key) is future:
                del self._inflight[key]
            # Retrieve any stored exception to silence "exception never
            # retrieved" warnings when no follower ever awaited this future.
            if future.done() and not future.cancelled():
                future.exception()


def should_early_refresh(
    ttl_remaining: float,
    ttl_total: float,
    *,
    beta: float = 1.0,
    rng: Callable[[], float] = random.random,
) -> bool:
    """Decide whether to proactively refresh a cache entry before it expires.

    This is a simplified XFetch-style probabilistic early expiration. The intent
    is to occasionally recompute a still-valid entry *ahead* of its expiry so
    that the recomputation cost is paid by a single early request instead of by
    a herd of requests all missing at once when it finally expires.

    Formula
    -------
    Let ``x = ttl_remaining / ttl_total`` be the fraction of TTL still left
    (clamped to ``[0, 1]``). We refresh when::

        rng() < beta * (1 - x)

    i.e. the refresh probability ``p = beta * (1 - x)`` grows linearly as the
    entry ages (``x -> 0``). With ``beta = 1`` it ranges from 0 at a fresh entry
    to 1 at expiry; ``beta > 1`` shifts refreshes earlier (more aggressive),
    ``beta < 1`` later (more conservative). The probability is clamped into
    ``[0, 1]``.

    Monotonicity: for any fixed ``rng()`` output, a smaller ``ttl_remaining``
    yields a larger ``p`` and therefore is at least as likely to return ``True``.

    Edge cases: if ``ttl_total <= 0`` or ``ttl_remaining <= 0`` the entry is
    treated as expired/invalid and the function returns ``True`` unconditionally.

    :param ttl_remaining: Seconds of TTL still remaining for the entry.
    :param ttl_total: Original total TTL of the entry, in seconds.
    :param beta: Aggressiveness multiplier (>1 refreshes earlier). Defaults to 1.
    :param rng: Zero-arg callable returning a float in ``[0, 1)``; injectable for
        deterministic testing. Defaults to :func:`random.random`.
    :returns: ``True`` if the entry should be refreshed now, else ``False``.
    """
    # Expired or invalid TTL: always refresh.
    if ttl_total <= 0 or ttl_remaining <= 0:
        return True

    # Fraction of life remaining, clamped to [0, 1].
    fraction_remaining = ttl_remaining / ttl_total
    if fraction_remaining > 1.0:
        fraction_remaining = 1.0
    elif fraction_remaining < 0.0:
        fraction_remaining = 0.0

    # Refresh probability rises as the entry ages; scaled by beta and clamped.
    probability = beta * (1.0 - fraction_remaining)
    if probability >= 1.0:
        return True
    if probability <= 0.0:
        return False

    # ``math`` imported for completeness/extensibility of the XFetch family;
    # the linear form needs only a direct comparison against the rng sample.
    _ = math  # keep import meaningful without altering behavior
    return rng() < probability
