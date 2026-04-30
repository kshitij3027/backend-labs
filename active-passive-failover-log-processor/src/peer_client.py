"""HTTP client abstraction for peer-to-peer election traffic.

Defines the :class:`PeerClient` protocol — the contract every concrete
client (plain httpx now, circuit-breaker-wrapped in commit 5) must
satisfy — and ships one concrete implementation, :class:`HttpxPeerClient`.

The election code (``src/election.py``) only ever depends on the
protocol; commit 5 swaps in ``inter_node_client.InterNodeClient`` without
touching a single line of election logic.

**Failure semantics**: peers being unreachable is the *normal* path
during failover (that's why we're voting in the first place). Every
network or HTTP error is therefore caught here and converted to a
``False`` return; an exception NEVER propagates out of these methods.
"""

from __future__ import annotations

import logging
from typing import Protocol

import httpx

from src.models import ElectionMessage, ElectionResult, to_json

logger = logging.getLogger(__name__)


# Tight timeouts: a peer that doesn't respond inside a couple of seconds
# is effectively dead for our 10-second failover budget. We'd rather
# treat it as down than block the whole election waiting for it.
_DEFAULT_TIMEOUT = httpx.Timeout(connect=1.0, read=2.0, write=1.0, pool=2.0)


class PeerClient(Protocol):
    """Contract for sending election RPCs to peer nodes.

    Implementations MUST:

    * Return ``True`` on a 2xx response, ``False`` on any other status,
      timeout, or transport error.
    * Never raise. The caller (election code) treats peers as
      best-effort — a peer being down should not break the election.
    * Be safe to call concurrently from multiple coroutines.
    """

    async def send_candidacy(
        self,
        peer: tuple[str, int],
        msg: ElectionMessage,
    ) -> bool:
        """POST a candidacy announcement to ``peer``. Returns success."""
        ...

    async def send_election_result(
        self,
        peer: tuple[str, int],
        result: ElectionResult,
    ) -> bool:
        """POST the final election result to ``peer``. Returns success."""
        ...

    async def close(self) -> None:
        """Release any underlying transport resources."""
        ...


class HttpxPeerClient:
    """Plain ``httpx.AsyncClient``-backed implementation of :class:`PeerClient`.

    No circuit breaker, no semaphores — those are layered on in commit 5
    by ``inter_node_client.InterNodeClient``. This implementation is
    intentionally minimal so the election protocol can be exercised end
    to end before the resilience layer lands.
    """

    def __init__(
        self,
        timeout: httpx.Timeout = _DEFAULT_TIMEOUT,
    ) -> None:
        self._client: httpx.AsyncClient = httpx.AsyncClient(timeout=timeout)

    async def send_candidacy(
        self,
        peer: tuple[str, int],
        msg: ElectionMessage,
    ) -> bool:
        """POST ``msg`` to ``http://{peer}/election/candidacy``."""
        url = f"http://{peer[0]}:{peer[1]}/election/candidacy"
        return await self._post(url, to_json(msg))

    async def send_election_result(
        self,
        peer: tuple[str, int],
        result: ElectionResult,
    ) -> bool:
        """POST ``result`` to ``http://{peer}/election/result``."""
        url = f"http://{peer[0]}:{peer[1]}/election/result"
        return await self._post(url, to_json(result))

    async def close(self) -> None:
        """Close the underlying HTTP client and release pooled sockets."""
        try:
            await self._client.aclose()
        except Exception:  # pragma: no cover - defensive only
            logger.exception("HttpxPeerClient.close raised")

    # --- internal -----------------------------------------------------------

    async def _post(self, url: str, body: bytes) -> bool:
        """Send a POST and convert every kind of failure into ``False``.

        We catch the broad ``httpx.HTTPError`` family (connect, read,
        write, pool, remote-protocol, etc.) plus ``OSError`` so we cover
        DNS / network-stack issues that can show up before httpx
        materialises one of its own exception types.
        """
        try:
            response = await self._client.post(
                url,
                content=body,
                headers={"content-type": "application/json"},
            )
        except httpx.HTTPError as exc:
            logger.warning("peer POST %s failed: %s", url, exc)
            return False
        except OSError as exc:
            logger.warning("peer POST %s failed (os): %s", url, exc)
            return False

        if 200 <= response.status_code < 300:
            return True
        logger.warning(
            "peer POST %s returned non-2xx: %d", url, response.status_code
        )
        return False
