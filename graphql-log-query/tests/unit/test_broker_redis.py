"""The Redis pub/sub bridge — spec §4 bonus: subscriptions that survive ``uvicorn --workers N``.

Driven against a **fake** client rather than a real Redis, and that is the point rather than a
convenience. The three behaviours that matter here are all things a real Redis makes hard to
observe:

* **Own-echo suppression.** Redis delivers a published message to every subscriber *including the
  publisher*, so the bridge has to recognise and discard its own traffic. Against a real server you
  can see the end state (one delivery, not two); against the fake you can see the counter that says
  *why*, which is the difference between "it works" and "it works for the reason we think".
* **Cross-worker delivery.** Two brokers with different publisher ids on one bus is exactly the
  ``--workers 2`` topology, in one process, with no ports.
* **Degradation.** "Redis is down" is a state a test has to be able to *cause*. A fake whose
  ``publish`` raises makes it a parameter.

The fake is a broadcast bus, not a stub that records calls: ``publish`` really does deliver to every
registered subscriber of that channel, so a broker publishing and a broker receiving are connected
by the same code path they would be in production.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import pytest

from src.broker import LogBroker, SubscriptionFilter, encode_event
from src.graphql.enums import LogLevel
from tests.unit.test_broker import make_entry, make_settings

#: Bound on every "wait until the reader has done its thing" helper. Generous, because it is a
#: failure deadline rather than a sleep: a healthy run satisfies these conditions in microseconds
#: and never waits, and a broken one fails in five seconds instead of hanging the suite.
CONDITION_TIMEOUT = 5.0


async def wait_until(predicate: Any, *, timeout: float = CONDITION_TIMEOUT, what: str = "") -> None:
    """Poll ``predicate`` until it is true, or fail after ``timeout``.

    A bounded wait rather than ``await asyncio.sleep(0.2)``: the reader task's progress is not
    something the test can await directly, and a fixed sleep is either flaky (too short) or slow
    (long enough not to be). This returns the instant the condition holds.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"condition never became true within {timeout}s: {what}")
        await asyncio.sleep(0.005)


# =================================================================================================
# The fake
# =================================================================================================


class FakePubSub:
    """One pub/sub connection onto a :class:`FakeRedis` bus."""

    def __init__(self, bus: "FakeRedis") -> None:
        self._bus = bus
        self._inbox: asyncio.Queue[Any] = asyncio.Queue()
        self.channels: list[str] = []
        self.closed = False

    async def subscribe(self, channel: str) -> None:
        if self._bus.subscribe_failures > 0:
            self._bus.subscribe_failures -= 1
            raise ConnectionError("simulated: redis is unreachable")
        self.channels.append(channel)
        self._bus.register(channel, self._inbox)

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ) -> Optional[dict[str, Any]]:
        """Mirror redis-py: a message dict, or ``None`` when ``timeout`` elapses with nothing."""
        try:
            data = await asyncio.wait_for(self._inbox.get(), timeout or 0.01)
        except asyncio.TimeoutError:
            return None
        return {"type": "message", "channel": self.channels[0], "data": data}

    async def aclose(self) -> None:
        self.closed = True
        self._bus.unregister(self._inbox)


class FakeRedis:
    """A pub/sub broadcast bus with the two methods :class:`~src.broker.LogBroker` uses.

    Duck-typed rather than a ``redis.asyncio.Redis`` subclass on purpose: the broker's contract with
    its client is exactly ``publish()`` and ``pubsub()``, and writing the fake to that contract is
    what keeps the contract small enough to be worth having.
    """

    def __init__(
        self, *, publish_error: Optional[Exception] = None, subscribe_failures: int = 0
    ) -> None:
        self.published: list[tuple[str, str]] = []
        self.publish_error = publish_error
        self.subscribe_failures = subscribe_failures
        self.closed = False
        self.pubsubs: list[FakePubSub] = []
        self._inboxes: dict[str, list[asyncio.Queue[Any]]] = {}

    def register(self, channel: str, inbox: "asyncio.Queue[Any]") -> None:
        self._inboxes.setdefault(channel, []).append(inbox)

    def unregister(self, inbox: "asyncio.Queue[Any]") -> None:
        for inboxes in self._inboxes.values():
            if inbox in inboxes:
                inboxes.remove(inbox)

    async def publish(self, channel: str, payload: str) -> int:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((channel, payload))
        inboxes = list(self._inboxes.get(channel, ()))
        for inbox in inboxes:
            inbox.put_nowait(payload)
        return len(inboxes)

    def pubsub(self) -> FakePubSub:
        pubsub = FakePubSub(self)
        self.pubsubs.append(pubsub)
        return pubsub

    async def aclose(self) -> None:
        self.closed = True


def build_broker(
    redis_client: Optional[FakeRedis],
    *,
    publisher_id: str,
    channel: str = "test:bridge",
) -> LogBroker:
    """A broker wired to ``redis_client``, with the reader's timers shrunk to test scale.

    The three attributes below are class-level constants on :class:`~src.broker.LogBroker` chosen
    for production (notice a Redis outage quickly, do not connect-storm, shut down promptly).
    Shadowing them per instance keeps the retry *logic* under test while removing the seconds it
    would otherwise cost — the loop still backs off and still doubles, just at a scale a test can
    afford.
    """
    broker = LogBroker(
        make_settings(channel=channel), redis_client=redis_client, publisher_id=publisher_id
    )
    broker._BACKOFF_INITIAL = 0.01  # noqa: SLF001 - deliberately reaching into the timers
    broker._BACKOFF_MAX = 0.05  # noqa: SLF001
    broker._POLL_TIMEOUT = 0.05  # noqa: SLF001
    return broker


# =================================================================================================
# Publishing
# =================================================================================================


async def test_publishing_reaches_both_the_local_queues_and_the_channel() -> None:
    """One publish, two destinations. Neither is allowed to be conditional on the other."""
    redis = FakeRedis()
    broker = build_broker(redis, publisher_id="worker-a")
    subscriber = broker.subscribe(SubscriptionFilter())
    entry = make_entry(entry_id=11, service="api")

    delivered = await broker.publish(entry)
    await broker.drain_pending_publishes()

    assert delivered == 1
    assert subscriber.queue.get_nowait() is entry, "the local subscriber got the object itself"

    assert len(redis.published) == 1
    channel, payload = redis.published[0]
    assert channel == "test:bridge"
    assert '"origin":"worker-a"' in payload
    assert broker.stats.remote_published_total == 1


async def test_a_broker_with_no_redis_client_still_fans_out_in_process() -> None:
    """Single-worker deployments and a misconfigured REDIS_URL land here, and both must work.

    ``logStream`` is a **core** requirement (spec §2 item 25); crossing workers is a §4 bonus. A
    missing bridge therefore degrades the bonus and must not touch the requirement.
    """
    broker = build_broker(None, publisher_id="worker-solo")
    subscriber = broker.subscribe(SubscriptionFilter())

    await broker.start()  # no-op without a client
    delivered = await broker.publish(make_entry())
    await broker.drain_pending_publishes()

    assert delivered == 1
    assert subscriber.queue.qsize() == 1
    assert broker.stats.remote_published_total == 0
    assert broker.redis_healthy is None, "nothing was ever attempted, so there is no state to report"
    await broker.stop()


async def test_a_failing_publish_never_escapes_and_never_costs_the_local_fan_out() -> None:
    """Redis down must degrade, not crash — and specifically must not fail ``createLog``.

    The ordering inside ``publish`` is what this pins: local fan-out happens *before* the remote hop
    is scheduled, so a broken client cannot cost the subscribers on this worker anything at all.
    """
    redis = FakeRedis(publish_error=ConnectionError("simulated: connection refused"))
    broker = build_broker(redis, publisher_id="worker-a")
    subscriber = broker.subscribe(SubscriptionFilter())

    delivered = await broker.publish(make_entry())  # must not raise
    await broker.drain_pending_publishes()

    assert delivered == 1, "the local subscriber was served despite Redis being down"
    assert subscriber.queue.qsize() == 1
    assert broker.stats.remote_published_total == 0
    assert broker.stats.redis_errors_total == 1
    assert broker.redis_healthy is False


async def test_a_redis_outage_logs_once_per_state_change_not_once_per_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Otherwise an outage under load prints one line per ingested log entry.

    That turns a degraded optional feature into an operational problem of its own — a log an
    operator cannot read is a log that hides the next real failure.
    """
    caplog.set_level(logging.INFO, logger="src.broker")
    redis = FakeRedis(publish_error=ConnectionError("simulated: connection refused"))
    broker = build_broker(redis, publisher_id="worker-a")

    for index in range(5):
        await broker.publish(make_entry(entry_id=index))
    await broker.drain_pending_publishes()

    degraded = [record for record in caplog.records if "degraded" in record.getMessage()]
    assert len(degraded) == 1, f"expected exactly one degradation line, got {len(degraded)}"
    assert broker.stats.redis_errors_total == 5, "every failure is still COUNTED, just not logged"

    # Recovery is a state change too, and it must be visible — an operator watching a log needs to
    # see the bridge come back, not infer it from the absence of further warnings.
    caplog.clear()
    redis.publish_error = None
    await broker.publish(make_entry(entry_id=99))
    await broker.drain_pending_publishes()

    recovered = [record for record in caplog.records if "connected" in record.getMessage()]
    assert len(recovered) == 1
    assert broker.redis_healthy is True


# =================================================================================================
# Receiving
# =================================================================================================


async def test_the_reader_injects_a_remote_event_into_local_fan_out() -> None:
    """A subscriber on this worker receives an entry published by another one."""
    redis = FakeRedis()
    broker = build_broker(redis, publisher_id="worker-a")
    await broker.start()
    try:
        subscriber = broker.subscribe(SubscriptionFilter())
        await wait_until(
            lambda: broker.redis_healthy is True, what="the reader subscribed to the channel"
        )

        entry = make_entry(entry_id=77, service="billing", level=LogLevel.ERROR, trace_id="t-1")
        # Straight onto the bus, as if a different process had published it.
        await redis.publish("test:bridge", encode_event(entry, origin="worker-b"))

        received = await asyncio.wait_for(subscriber.queue.get(), CONDITION_TIMEOUT)
    finally:
        await broker.stop()

    assert received is not None
    assert received.id == entry.id
    assert received.service == "billing"
    assert received.level is LogLevel.ERROR
    assert received.trace_id == "t-1"
    assert broker.stats.remote_received_total >= 1
    assert broker.stats.remote_suppressed_total == 0, "it came from another publisher"


async def test_a_remote_event_is_filtered_by_the_subscribers_filter() -> None:
    """Remote and local events go through the *same* fan-out, so filtering cannot differ between them.

    A bridge that injected events past the filter would deliver a firehose to a narrowly-scoped
    subscription the moment a second worker existed — and would look perfect in single-worker
    development.
    """
    redis = FakeRedis()
    broker = build_broker(redis, publisher_id="worker-a")
    await broker.start()
    try:
        api_only = broker.subscribe(SubscriptionFilter(service="api"))
        await wait_until(lambda: broker.redis_healthy is True, what="the reader subscribed")

        await redis.publish(
            "test:bridge", encode_event(make_entry(entry_id=1, service="worker"), origin="w-b")
        )
        await redis.publish(
            "test:bridge", encode_event(make_entry(entry_id=2, service="api"), origin="w-b")
        )

        received = await asyncio.wait_for(api_only.queue.get(), CONDITION_TIMEOUT)
    finally:
        await broker.stop()

    # The "worker" entry was published FIRST, so if it had been enqueued it would be the one read
    # here. Getting the "api" entry proves the non-matching one never entered the queue — an
    # ordered tracer rather than a sleep-and-hope-nothing-arrived.
    assert received is not None
    assert received.id == "2"
    assert api_only.queue.qsize() == 0
    assert broker.stats.remote_received_total == 2, "both were read off the channel"


async def test_our_own_echo_is_suppressed_so_a_local_subscriber_sees_one_copy() -> None:
    """Redis echoes a published message back to its publisher; without the id check, entries double.

    This is the real round trip, not a simulated one: the broker publishes onto the bus, its own
    reader reads it back off, and the origin check is the only thing standing between one delivery
    and two.
    """
    redis = FakeRedis()
    broker = build_broker(redis, publisher_id="worker-a")
    await broker.start()
    try:
        subscriber = broker.subscribe(SubscriptionFilter())
        await wait_until(lambda: broker.redis_healthy is True, what="the reader subscribed")

        await broker.publish(make_entry(entry_id=5))
        await broker.drain_pending_publishes()
        await wait_until(
            lambda: broker.stats.remote_received_total >= 1,
            what="the echo came back off the channel",
        )
    finally:
        await broker.stop()

    assert broker.stats.remote_suppressed_total == 1
    assert subscriber.queue.qsize() == 1, "one entry, delivered by local fan-out and not again"


async def test_two_workers_share_a_channel_and_neither_double_delivers() -> None:
    """The ``--workers 2`` topology, in one process: A publishes, B's subscriber receives.

    This is the requirement the whole bridge exists for. Without it, a dashboard whose socket landed
    on worker B would silently miss every entry created through worker A — and would look perfectly
    healthy in single-worker development.
    """
    bus = FakeRedis()
    worker_a = build_broker(bus, publisher_id="worker-a")
    worker_b = build_broker(bus, publisher_id="worker-b")
    await worker_a.start()
    await worker_b.start()
    try:
        on_a = worker_a.subscribe(SubscriptionFilter())
        on_b = worker_b.subscribe(SubscriptionFilter())
        await wait_until(
            lambda: worker_a.redis_healthy is True and worker_b.redis_healthy is True,
            what="both readers subscribed",
        )

        entry = make_entry(entry_id=1234, service="checkout")
        await worker_a.publish(entry)
        await worker_a.drain_pending_publishes()

        received_on_b = await asyncio.wait_for(on_b.queue.get(), CONDITION_TIMEOUT)
        await wait_until(
            lambda: worker_a.stats.remote_suppressed_total == 1,
            what="worker A recognised and discarded its own echo",
        )
    finally:
        await worker_a.stop()
        await worker_b.stop()

    assert received_on_b is not None
    assert received_on_b.id == "1234"
    assert received_on_b.service == "checkout"

    assert on_a.queue.qsize() == 1, "the publishing worker delivered exactly one copy locally"
    assert on_b.queue.qsize() == 0, "and the receiving worker delivered exactly one, now consumed"
    assert worker_b.stats.remote_suppressed_total == 0


async def test_a_malformed_channel_message_is_counted_and_does_not_kill_the_reader() -> None:
    """The channel is writable by anything holding the Redis credentials.

    A reader that died on the first junk message would take the cross-worker bridge down silently —
    subscriptions would keep working on each worker and quietly stop crossing between them.
    """
    redis = FakeRedis()
    broker = build_broker(redis, publisher_id="worker-a")
    await broker.start()
    try:
        subscriber = broker.subscribe(SubscriptionFilter())
        await wait_until(lambda: broker.redis_healthy is True, what="the reader subscribed")

        await redis.publish("test:bridge", "}{ not json at all")
        await redis.publish("test:bridge", '{"v":99,"kind":"log","origin":"w","entry":{}}')
        await redis.publish(
            "test:bridge", encode_event(make_entry(entry_id=8), origin="worker-b")
        )

        received = await asyncio.wait_for(subscriber.queue.get(), CONDITION_TIMEOUT)
    finally:
        await broker.stop()

    assert received is not None and received.id == "8", "the reader survived both bad messages"
    assert broker.stats.remote_invalid_total == 2


async def test_the_reader_retries_a_failing_subscribe_until_it_connects() -> None:
    """Redis unavailable at boot must be a retry, not a dead bridge for the life of the process.

    ``subscribe_failures=3`` makes the first three connection attempts raise, which is what a Redis
    that is still starting looks like from here. The reader has to back off and come back — and then
    actually deliver, which is the part a "it did not crash" assertion would miss.
    """
    redis = FakeRedis(subscribe_failures=3)
    broker = build_broker(redis, publisher_id="worker-a")
    await broker.start()
    try:
        subscriber = broker.subscribe(SubscriptionFilter())
        await wait_until(
            lambda: broker.redis_healthy is True,
            what="the reader eventually connected after three failures",
        )
        assert redis.subscribe_failures == 0
        assert broker.stats.redis_errors_total == 3

        await redis.publish(
            "test:bridge", encode_event(make_entry(entry_id=3), origin="worker-b")
        )
        received = await asyncio.wait_for(subscriber.queue.get(), CONDITION_TIMEOUT)
    finally:
        await broker.stop()

    assert received is not None and received.id == "3"


# =================================================================================================
# Lifecycle
# =================================================================================================


async def test_start_is_idempotent_and_stop_leaves_no_reader_behind() -> None:
    """A reader task that outlives the lifespan is a container that will not exit."""
    redis = FakeRedis()
    broker = build_broker(redis, publisher_id="worker-a")

    await broker.start()
    first_reader = broker._reader  # noqa: SLF001 - the task is the thing under test
    await broker.start()
    assert broker._reader is first_reader, "start() must not spawn a second reader"  # noqa: SLF001

    await broker.stop()

    assert broker._reader is None  # noqa: SLF001
    assert first_reader is not None and first_reader.done()
    await broker.stop()  # idempotent


async def test_stop_settles_in_flight_publishes_rather_than_cancelling_them() -> None:
    """An event a peer worker's subscriber is entitled to must not be lost to our shutdown."""
    redis = FakeRedis()
    broker = build_broker(redis, publisher_id="worker-a")

    await broker.publish(make_entry(entry_id=1))
    await broker.publish(make_entry(entry_id=2))
    # Deliberately not drained: the PUBLISH tasks are scheduled and have not run yet, which is
    # exactly the state a shutdown arriving one tick after a mutation would find.
    await broker.stop()

    assert len(redis.published) == 2
    assert broker.stats.remote_published_total == 2
