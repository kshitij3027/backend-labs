"""Resilient :class:`PeerClient` implementation.

Drop-in replacement for :class:`HttpxPeerClient` that wraps the underlying
``httpx.AsyncClient`` calls in two resilience patterns:

* **Per-peer circuit breaker** — one :class:`CircuitBreaker` per
  ``(host, port)`` tuple, lazily created on first use. A peer that's
  consistently unreachable trips its breaker and stops being even
  attempted until ``reset_timeout`` elapses; this stops a dead peer from
  costing us a connect-timeout per election.
* **Per-call-type bulkhead** — a separate :class:`asyncio.Semaphore` for
  ``send_candidacy`` and ``send_election_result`` so that a flood of one
  call type can't exhaust the in-flight budget for the other. Heartbeat
  traffic does NOT go through here (it flows via Redis directly), so we
  only need bulkheads for the two election RPCs.

The class still satisfies the :class:`PeerClient` protocol exactly: every
method swallows exceptions and returns ``False`` on any failure, including
``CircuitBreakerOpen`` — peer-down is the *normal* path during failover
and must never propagate to the election coordinator.
"""

from __future__ import annotations

import asyncio
import logging

import httpx

from src.circuit_breaker import CircuitBreaker, CircuitBreakerOpen
from src.models import ElectionMessage, ElectionResult, to_json

logger = logging.getLogger(__name__)


# Mirror of the timeout used in HttpxPeerClient — keep these in sync with
# the simpler implementation so tests for one transfer to the other.
_DEFAULT_TIMEOUT = httpx.Timeout(connect=1.0, read=2.0, write=1.0, pool=2.0)


class InterNodeClient:
    """Circuit-breaker- and bulkhead-wrapped HTTP peer client.

    Construction is cheap: nothing happens until the first call. Each
    peer is given its own breaker the first time we send to it, and the
    dict mutation is guarded by a lock so we don't accidentally create
    two breakers for the same peer when two coroutines hit a fresh peer
    simultaneously.

    Parameters
    ----------
    breaker_fail_max:
        Forwarded to :class:`CircuitBreaker.fail_max` for every per-peer
        breaker.
    breaker_reset_timeout:
        Forwarded to :class:`CircuitBreaker.reset_timeout`.
    candidacy_concurrency / result_concurrency:
        Bulkhead sizes — maximum concurrent ``send_candidacy`` /
        ``send_election_result`` calls in flight. Default 3 each: a
        3-node cluster fans out to 2 peers, so 3 leaves a little
        headroom for retries during a chaotic transition.
    timeout:
        ``httpx.Timeout`` for the underlying client. Defaults match
        :mod:`peer_client` so the resilience layer doesn't change the
        wire-level latency budget.
    """

    def __init__(
        self,
        breaker_fail_max: int = 5,
        breaker_reset_timeout: float = 30.0,
        candidacy_concurrency: int = 3,
        result_concurrency: int = 3,
        timeout: httpx.Timeout = _DEFAULT_TIMEOUT,
    ) -> None:
        self._client: httpx.AsyncClient = httpx.AsyncClient(timeout=timeout)
        self._breaker_fail_max: int = breaker_fail_max
        self._breaker_reset_timeout: float = breaker_reset_timeout

        # Bulkhead semaphores — separate budgets so a candidacy storm
        # can't starve out the result broadcast.
        self._candidacy_sem: asyncio.Semaphore = asyncio.Semaphore(
            candidacy_concurrency
        )
        self._result_sem: asyncio.Semaphore = asyncio.Semaphore(
            result_concurrency
        )

        # Per-peer breakers. Lazy-created on first call to any peer. The
        # lock guards only the dict-mutation path (the breaker itself
        # has its own internal lock for state transitions).
        self._breakers: dict[tuple[str, int], CircuitBreaker] = {}
        self._breakers_lock: asyncio.Lock = asyncio.Lock()

        self._closed: bool = False

    # --- PeerClient interface ---------------------------------------------

    async def send_candidacy(
        self,
        peer: tuple[str, int],
        msg: ElectionMessage,
    ) -> bool:
        """POST ``msg`` to ``peer`` through the bulkhead + breaker."""
        url = f"http://{peer[0]}:{peer[1]}/election/candidacy"
        return await self._guarded_post(
            peer=peer,
            sem=self._candidacy_sem,
            url=url,
            body=to_json(msg),
        )

    async def send_election_result(
        self,
        peer: tuple[str, int],
        result: ElectionResult,
    ) -> bool:
        """POST ``result`` to ``peer`` through the bulkhead + breaker."""
        url = f"http://{peer[0]}:{peer[1]}/election/result"
        return await self._guarded_post(
            peer=peer,
            sem=self._result_sem,
            url=url,
            body=to_json(result),
        )

    async def close(self) -> None:
        """Close the underlying httpx client. Idempotent."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._client.aclose()
        except Exception:
            logger.exception("InterNodeClient.close raised")

    # --- metrics -----------------------------------------------------------

    @property
    def metrics(self) -> dict[str, dict[str, int]]:
        """Per-peer breaker metrics keyed by ``"<host>:<port>"``.

        The ``http_server.metrics`` handler aggregates these into flat
        ``circuit_breaker_failures_total`` / ``circuit_breaker_opens_total``
        counters across all peers.
        """
        snapshot: dict[str, dict[str, int]] = {}
        for (host, port), breaker in self._breakers.items():
            snapshot[f"{host}:{port}"] = breaker.metrics
        return snapshot

    # --- internal ---------------------------------------------------------

    async def _get_or_create_breaker(
        self, peer: tuple[str, int]
    ) -> CircuitBreaker:
        """Return the breaker for ``peer``, lazily creating it on first use.

        The lock is held only across the dict-mutation; the hot path
        (peer already known) takes the fast no-lock branch first.
        """
        breaker = self._breakers.get(peer)
        if breaker is not None:
            return breaker
        async with self._breakers_lock:
            # Re-check under the lock — another coroutine may have
            # created the breaker while we were awaiting it.
            breaker = self._breakers.get(peer)
            if breaker is None:
                breaker = CircuitBreaker(
                    name=f"{peer[0]}:{peer[1]}",
                    fail_max=self._breaker_fail_max,
                    reset_timeout=self._breaker_reset_timeout,
                )
                self._breakers[peer] = breaker
            return breaker

    async def _guarded_post(
        self,
        peer: tuple[str, int],
        sem: asyncio.Semaphore,
        url: str,
        body: bytes,
    ) -> bool:
        """Acquire bulkhead, route through this peer's breaker, swallow failures."""
        async with sem:
            breaker = await self._get_or_create_breaker(peer)
            try:
                return await breaker.call(self._raw_post, url, body)
            except CircuitBreakerOpen:
                # Peer is in cooldown — skip the call entirely. This is
                # the normal path during failover (a dead peer); we
                # return False just like a connect failure would.
                logger.debug(
                    "InterNodeClient: breaker for %s is OPEN, skipping POST",
                    peer,
                )
                return False
            except Exception:
                # All other exceptions are already counted as failures
                # by the breaker; we still return False to satisfy the
                # PeerClient contract (never raise).
                return False

    async def _raw_post(self, url: str, body: bytes) -> bool:
        """Underlying httpx POST — same shape as :class:`HttpxPeerClient._post`.

        Returns True on 2xx, raises a transport/HTTP exception on any
        failure so the breaker can count it. Note we do NOT swallow
        exceptions here — the breaker needs them as the failure signal.
        """
        response = await self._client.post(
            url,
            content=body,
            headers={"content-type": "application/json"},
        )
        if 200 <= response.status_code < 300:
            return True
        # A non-2xx is a real failure for the breaker; raise so the
        # consecutive-failure counter increments.
        raise httpx.HTTPStatusError(
            f"non-2xx status {response.status_code}",
            request=response.request,
            response=response,
        )


__all__ = ["InterNodeClient"]
