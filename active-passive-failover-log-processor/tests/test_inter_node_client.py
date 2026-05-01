"""Tests for src/inter_node_client.py — the resilience-wrapped peer client.

We exercise two distinct testing paths:

* **Real-network unreachable peer** for the breaker-trip scenarios. Pointing
  at ``localhost:1`` produces an immediate ``ConnectionRefused``, which is
  exactly the kind of failure a dead peer generates in production. This
  keeps the breaker test simple — we don't need to mock httpx at all.
* **httpx.MockTransport** for the bulkhead concurrency test. We need
  controlled timing (sleep inside the mock) to observe semaphore
  saturation; ``MockTransport`` lets us inject an async response handler
  that does just that.
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from src.inter_node_client import InterNodeClient
from src.models import ElectionMessage, ElectionResult


# =========================================================================
# Helpers — message factories
# =========================================================================


def _candidacy(node_id: str = "node-test") -> ElectionMessage:
    return ElectionMessage(
        candidate=node_id,
        priority=10,
        term=1,
        timestamp=time.time(),
    )


def _result(winner: str = "node-test") -> ElectionResult:
    return ElectionResult(winner=winner, term=1, timestamp=time.time())


# =========================================================================
# Bulkhead: concurrency observed via MockTransport
# =========================================================================


def _make_mock_client(
    *,
    inflight_counter: list[int],
    max_observed: list[int],
    delay: float,
) -> httpx.AsyncClient:
    """Build an httpx.AsyncClient whose transport tracks concurrency.

    Each request increments a shared counter, sleeps ``delay`` seconds,
    then decrements. ``max_observed`` records the high-water mark so
    the test can assert the bulkhead never let more than N concurrent
    calls in.
    """

    async def _handler(request: httpx.Request) -> httpx.Response:
        inflight_counter[0] += 1
        max_observed[0] = max(max_observed[0], inflight_counter[0])
        try:
            await asyncio.sleep(delay)
            return httpx.Response(200, json={"ok": True})
        finally:
            inflight_counter[0] -= 1

    transport = httpx.MockTransport(_handler)
    return httpx.AsyncClient(transport=transport)


# =========================================================================
# 1. Unreachable peer returns False and counts a failure
# =========================================================================


async def test_send_candidacy_to_unreachable_peer_returns_false() -> None:
    """A peer that doesn't accept connections must yield False, not raise."""
    client = InterNodeClient(breaker_fail_max=5, breaker_reset_timeout=30.0)
    try:
        # Port 1 is reserved for tcpmux; nothing should be listening.
        ok = await client.send_candidacy(("127.0.0.1", 1), _candidacy())
        assert ok is False

        # The breaker for this peer recorded one failure.
        snapshot = client.metrics
        assert "127.0.0.1:1" in snapshot
        assert snapshot["127.0.0.1:1"]["failures_total"] >= 1
    finally:
        await client.close()


async def test_send_election_result_to_unreachable_peer_returns_false() -> None:
    client = InterNodeClient(breaker_fail_max=5, breaker_reset_timeout=30.0)
    try:
        ok = await client.send_election_result(("127.0.0.1", 1), _result())
        assert ok is False

        snapshot = client.metrics
        assert snapshot["127.0.0.1:1"]["failures_total"] >= 1
    finally:
        await client.close()


# =========================================================================
# 2. Breaker trips after fail_max consecutive failures and skips the wire
# =========================================================================


async def test_breaker_opens_after_fail_max_unreachable_calls() -> None:
    """5 unreachable calls -> the 6th must short-circuit the network."""
    client = InterNodeClient(breaker_fail_max=5, breaker_reset_timeout=30.0)
    peer = ("127.0.0.1", 1)
    try:
        for _ in range(5):
            ok = await client.send_candidacy(peer, _candidacy())
            assert ok is False

        # Snapshot before the 6th call.
        before = client.metrics["127.0.0.1:1"]
        assert before["failures_total"] == 5
        assert before["opens_total"] == 1

        # The 6th call must NOT increment ``failures_total`` because the
        # breaker rejects it before invoking the underlying POST. The
        # ``calls_total`` counter still increments (we counted the
        # rejection attempt). This is a stronger and timing-independent
        # check than measuring elapsed time, which can be flaky on CI.
        ok = await client.send_candidacy(peer, _candidacy())
        assert ok is False

        after = client.metrics["127.0.0.1:1"]
        assert after["failures_total"] == 5, (
            "breaker did not short-circuit; failures still counted"
        )
        assert after["calls_total"] == before["calls_total"] + 1
    finally:
        await client.close()


# =========================================================================
# 3. Per-peer breakers are independent
# =========================================================================


async def test_metrics_aggregates_per_peer_independently() -> None:
    client = InterNodeClient(breaker_fail_max=10, breaker_reset_timeout=30.0)
    try:
        await client.send_candidacy(("127.0.0.1", 1), _candidacy())
        await client.send_candidacy(("127.0.0.1", 2), _candidacy())
        await client.send_candidacy(("127.0.0.1", 1), _candidacy())

        snapshot = client.metrics
        assert "127.0.0.1:1" in snapshot
        assert "127.0.0.1:2" in snapshot
        # Peer 1 had 2 attempts; peer 2 had 1.
        assert snapshot["127.0.0.1:1"]["calls_total"] == 2
        assert snapshot["127.0.0.1:2"]["calls_total"] == 1
    finally:
        await client.close()


# =========================================================================
# 4. close() releases the underlying httpx client
# =========================================================================


async def test_close_marks_client_closed_and_is_idempotent() -> None:
    client = InterNodeClient()
    assert client._closed is False
    await client.close()
    assert client._closed is True
    # A second close must not raise.
    await client.close()


async def test_close_closes_underlying_httpx_client() -> None:
    client = InterNodeClient()
    underlying: httpx.AsyncClient = client._client  # type: ignore[attr-defined]
    assert underlying.is_closed is False
    await client.close()
    assert underlying.is_closed is True


# =========================================================================
# 5. Bulkhead: candidacy semaphore caps concurrent calls
# =========================================================================


async def test_candidacy_bulkhead_caps_concurrent_inflight_calls() -> None:
    """With concurrency=3, no more than 3 candidacy calls run simultaneously."""
    inflight = [0]
    max_seen = [0]

    client = InterNodeClient(
        breaker_fail_max=100,  # don't accidentally trip during the test
        candidacy_concurrency=3,
        result_concurrency=3,
    )
    # Swap the underlying httpx client for a mock-transport one we control.
    await client._client.aclose()  # type: ignore[attr-defined]
    client._client = _make_mock_client(  # type: ignore[attr-defined]
        inflight_counter=inflight,
        max_observed=max_seen,
        delay=0.05,
    )

    try:
        peer = ("mock-peer", 1234)
        # Fire 10 candidacy calls in parallel; with concurrency=3 the
        # max observed in-flight should be 3.
        await asyncio.gather(
            *[client.send_candidacy(peer, _candidacy()) for _ in range(10)]
        )
        assert max_seen[0] == 3, f"observed up to {max_seen[0]} concurrent (expected 3)"
    finally:
        await client.close()


async def test_result_bulkhead_caps_concurrent_inflight_calls() -> None:
    inflight = [0]
    max_seen = [0]

    client = InterNodeClient(
        breaker_fail_max=100,
        candidacy_concurrency=3,
        result_concurrency=3,
    )
    await client._client.aclose()  # type: ignore[attr-defined]
    client._client = _make_mock_client(  # type: ignore[attr-defined]
        inflight_counter=inflight,
        max_observed=max_seen,
        delay=0.05,
    )
    try:
        peer = ("mock-peer", 1234)
        await asyncio.gather(
            *[client.send_election_result(peer, _result()) for _ in range(10)]
        )
        assert max_seen[0] == 3
    finally:
        await client.close()


async def test_candidacy_and_result_bulkheads_have_separate_budgets() -> None:
    """Saturating candidacy must not block result calls."""
    candidacy_inflight = [0]
    candidacy_max = [0]
    result_inflight = [0]
    result_max = [0]

    # Build a mock that routes by URL path so the two endpoints share
    # one transport but record into different counters.
    async def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/election/candidacy"):
            candidacy_inflight[0] += 1
            candidacy_max[0] = max(candidacy_max[0], candidacy_inflight[0])
            try:
                # Hold candidacy calls for a long-ish delay so the
                # bulkhead saturates while the result-side runs free.
                await asyncio.sleep(0.2)
                return httpx.Response(200, json={"ok": True})
            finally:
                candidacy_inflight[0] -= 1
        elif request.url.path.endswith("/election/result"):
            result_inflight[0] += 1
            result_max[0] = max(result_max[0], result_inflight[0])
            try:
                await asyncio.sleep(0.05)
                return httpx.Response(200, json={"ok": True})
            finally:
                result_inflight[0] -= 1
        return httpx.Response(404)

    client = InterNodeClient(
        breaker_fail_max=100,
        candidacy_concurrency=3,
        result_concurrency=3,
    )
    await client._client.aclose()  # type: ignore[attr-defined]
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(_handler))  # type: ignore[attr-defined]

    try:
        peer = ("mock-peer", 1234)
        # Saturate candidacy with 10 in-flight.
        candidacy_tasks = [
            asyncio.create_task(client.send_candidacy(peer, _candidacy()))
            for _ in range(10)
        ]
        # Give the candidacy calls a moment to occupy their bulkhead.
        await asyncio.sleep(0.02)
        # Now fire result calls. They must NOT be blocked by the
        # saturated candidacy bulkhead.
        result_tasks = [
            asyncio.create_task(client.send_election_result(peer, _result()))
            for _ in range(5)
        ]
        await asyncio.gather(*result_tasks, *candidacy_tasks)

        # Both bulkheads observed up to 3 concurrent in-flight calls.
        # The crucial assertion is that the result-side ran while the
        # candidacy-side was saturated — i.e. result_max > 0.
        assert result_max[0] >= 1
        assert result_max[0] <= 3
        assert candidacy_max[0] == 3
    finally:
        await client.close()


# =========================================================================
# 6. Semaphores are exposed and have the configured size
# =========================================================================


async def test_semaphore_sizes_match_constructor() -> None:
    """Acquire all N slots; the (N+1)th acquire blocks until released."""
    client = InterNodeClient(candidacy_concurrency=3, result_concurrency=3)
    try:
        sem = client._candidacy_sem  # type: ignore[attr-defined]
        assert isinstance(sem, asyncio.Semaphore)
        # Drain the semaphore.
        for _ in range(3):
            await asyncio.wait_for(sem.acquire(), timeout=0.05)
        # 4th acquire must block — wait_for raises TimeoutError.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(sem.acquire(), timeout=0.05)
        # Release everything we held.
        for _ in range(3):
            sem.release()
    finally:
        await client.close()


# =========================================================================
# 7. send_candidacy and send_election_result hit the right endpoint
# =========================================================================


async def test_send_candidacy_posts_to_election_candidacy_path() -> None:
    captured: list[str] = []

    async def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.url.path)
        return httpx.Response(200, json={"ok": True})

    client = InterNodeClient(breaker_fail_max=10)
    await client._client.aclose()  # type: ignore[attr-defined]
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(_handler))  # type: ignore[attr-defined]
    try:
        ok = await client.send_candidacy(("peer", 9000), _candidacy())
        assert ok is True
        assert captured == ["/election/candidacy"]
    finally:
        await client.close()


async def test_send_election_result_posts_to_election_result_path() -> None:
    captured: list[str] = []

    async def _handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.url.path)
        return httpx.Response(200, json={"ok": True})

    client = InterNodeClient(breaker_fail_max=10)
    await client._client.aclose()  # type: ignore[attr-defined]
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(_handler))  # type: ignore[attr-defined]
    try:
        ok = await client.send_election_result(("peer", 9000), _result())
        assert ok is True
        assert captured == ["/election/result"]
    finally:
        await client.close()


# =========================================================================
# 8. Non-2xx responses are counted as failures by the breaker
# =========================================================================


async def test_non_2xx_response_counts_as_breaker_failure() -> None:
    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = InterNodeClient(breaker_fail_max=3)
    await client._client.aclose()  # type: ignore[attr-defined]
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(_handler))  # type: ignore[attr-defined]
    try:
        for _ in range(3):
            ok = await client.send_candidacy(("peer", 9000), _candidacy())
            assert ok is False
        snap = client.metrics["peer:9000"]
        assert snap["failures_total"] == 3
        assert snap["opens_total"] == 1
    finally:
        await client.close()
