"""Unit tests for the store's SSE fan-out — subscriptions, back-pressure, resume, stream caps.

The subject here is :class:`~src.store.Subscription` and the six store methods around it
(``subscribe`` / ``unsubscribe`` / ``subscriber_count`` / ``stream_count`` /
``close_all_subscribers`` / ``replay_since``) plus the ``_publish`` fan-out that ``append`` runs
inside its critical section. No HTTP, no ASGI, no event-source parsing — that is
``tests/integration/test_stream_api.py``'s job. Testing the fan-out here means the two hardest
guarantees in the project get pinned without a socket in the way:

* **A slow consumer is dropped, never buffered.** Per-subscriber memory is hard-bounded at
  ``queue_size``. This is the README's central back-pressure claim, and the only way to observe
  it honestly is to watch ``qsize()`` across an overflow rather than to trust the branch exists.
* **``unsubscribe`` is idempotent.** The route has six exit paths that all release the same
  subscription, and the per-principal stream counter must decrement **exactly once** no matter
  how many of them run. A counter that drifts down stops enforcing the cap (a security hole);
  one that drifts up locks a principal out forever (a support ticket).

Every test is ``async def`` — ``asyncio_mode = auto`` in ``pytest.ini`` runs them without a
decorator. They are async on purpose rather than by accident: ``asyncio.Queue`` resolves a parked
getter's future against *the running loop*, and ``_publish`` documents its safety argument as
"appends that can reach a subscriber happen on the loop thread". Running these tests on a loop is
running them under the invariant the production code claims.

There is not a single ``sleep()`` in this file. Delivery is observed with ``get_nowait()``,
because ``append`` publishes **synchronously** inside the critical section — by the time
``append`` returns, every matching subscriber's queue already holds the record. If that ever
stops being true, these tests fail loudly instead of hanging.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from src.models import LogEntry, LogQuery, SortOrder
from src.store import (
    Filter,
    LogStore,
    StoredEntry,
    StreamLimitExceeded,
    Subscription,
)

#: Fixed anchor for generated timestamps, mirroring ``tests/unit/test_store.py``. Never
#: ``datetime.now()``: a corpus whose contents depend on when the suite ran cannot pin a bound.
BASE_TS = datetime(2026, 7, 27, 10, 0, 0, tzinfo=UTC)

#: The subject every single-principal test subscribes under.
SUBJECT = "analyst"


def make_entry(
    i: int,
    *,
    level: str = "INFO",
    service: str = "auth-svc",
    host: str = "node-1",
    message: str | None = None,
) -> LogEntry:
    """One deterministic entry. Ids are ordered so a failure prints something placeable."""
    return LogEntry(
        id=f"e{i:06d}",
        ts=BASE_TS + timedelta(seconds=i),
        level=level,
        service=service,
        host=host,
        message=message if message is not None else f"event {i}",
    )


def drain(sub: Subscription) -> list[StoredEntry | None]:
    """Everything currently queued for ``sub``, in order, without awaiting.

    ``get_nowait`` rather than ``await get()`` is the whole point: publication is synchronous, so
    a test that had to await delivery would be a test that could hang when delivery breaks.
    """
    out: list[StoredEntry | None] = []
    while True:
        try:
            out.append(sub.queue.get_nowait())
        except asyncio.QueueEmpty:
            return out


def ids_of(items: list[StoredEntry | None]) -> list[str]:
    """Entry ids of a drained queue. Raises on a sentinel, which is what a caller wants."""
    return [item.entry.id for item in items if item is not None]


def level_filter(*levels: str) -> Filter:
    """A filter built through the same ``LogQuery`` door the stream route uses.

    Deliberately not ``Filter(levels=frozenset(...))``: constructing the internal shape directly
    would let a test keep passing after ``from_query`` stopped producing it, and "the tail and
    the history are the same predicate" is exactly the property worth protecting.
    """
    return Filter.from_query(LogQuery(level=list(levels)))


class BrokenFilter:
    """A filter whose predicate raises. Duck-typed, because that is the realistic failure.

    ``Filter`` and ``CompiledFilter`` both produce total predicates today, so the only way to
    reach ``_publish``'s defensive ``except`` is to stand in something that does not. The
    fan-out never type-checks its subscribers' filters — it calls ``matches`` — so a stub with
    the right shape and the wrong behaviour is a faithful stand-in for a future filter with a bug
    in it.
    """

    is_empty = False

    def matches(self, rec: StoredEntry) -> bool:
        raise RuntimeError("this predicate is broken on purpose")


# ---------------------------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------------------------


async def test_subscribe_receives_appended_entry() -> None:
    """The base case: subscribe, append, the record is already queued when append returns."""
    store = LogStore(capacity=100)
    sub = store.subscribe(Filter(), subject=SUBJECT)

    record = store.append(make_entry(0))

    assert drain(sub) == [record], "an unfiltered subscriber must receive every append"
    assert sub.released is False
    assert sub.dropped is False


async def test_subscriber_only_receives_matching_entries() -> None:
    """The subscriber's filter is applied at fan-out, not left for the client to do."""
    store = LogStore(capacity=100)
    sub = store.subscribe(level_filter("ERROR"), subject=SUBJECT)

    store.append(make_entry(0, level="INFO"))
    store.append(make_entry(1, level="ERROR"))
    store.append(make_entry(2, level="WARN"))
    store.append(make_entry(3, level="ERROR"))

    assert ids_of(drain(sub)) == ["e000001", "e000003"]


async def test_multiple_subscribers_all_receive() -> None:
    """Fan-out is a fan, not a hand-off: every matching subscriber gets its own copy."""
    store = LogStore(capacity=100)
    subs = [store.subscribe(Filter(), subject=f"sub-{i}") for i in range(3)]

    record = store.append(make_entry(0))

    for sub in subs:
        queued = drain(sub)
        assert queued == [record]
        # Shared by reference, not copied. `StoredEntry` is frozen precisely so this is safe,
        # and at STORE_CAPACITY entries * N subscribers, copying would be the memory bug.
        assert queued[0] is record
    assert store.subscriber_count() == 3


async def test_unsubscribe_stops_delivery() -> None:
    """A released subscription receives nothing further — the counter is not the only effect."""
    store = LogStore(capacity=100)
    sub = store.subscribe(Filter(), subject=SUBJECT)

    store.append(make_entry(0))
    assert len(drain(sub)) == 1

    assert store.unsubscribe(sub) is True
    store.append(make_entry(1))

    assert drain(sub) == [], "an unsubscribed queue must not keep filling"
    assert store.subscriber_count() == 0


# ---------------------------------------------------------------------------------------------
# Back-pressure: drop, never buffer
# ---------------------------------------------------------------------------------------------


async def test_slow_consumer_is_dropped_not_buffered() -> None:
    """**The central back-pressure claim.** Per-subscriber memory is hard-bounded at the cap.

    A reader that never reads is simulated by simply not draining. The assertion is not "the
    subscriber was dropped" alone — a buggy implementation could drop it *after* growing the
    queue — but that ``qsize()`` never once exceeded ``queue_size`` across the whole overflow.
    That is the property the README promises and the one an OOM would violate.
    """
    queue_size = 4
    store = LogStore(capacity=100)
    sub = store.subscribe(Filter(), subject=SUBJECT, queue_size=queue_size)

    observed = []
    for i in range(queue_size * 5):
        store.append(make_entry(i))
        observed.append(sub.queue.qsize())

    assert max(observed) <= queue_size, (
        f"queue grew past its {queue_size}-record bound (peak {max(observed)}); a stalled "
        "reader can now grow the server's memory"
    )
    assert sub.dropped is True, "the overflowing subscriber must be marked dropped"
    assert sub.released is True, "the overflowing subscriber must be unregistered"
    assert store.subscriber_count() == 0
    assert store.stream_count(SUBJECT) == 0, "a dropped stream must give its slot back"


async def test_dropped_subscriber_gets_terminal_sentinel() -> None:
    """The drop drains and then enqueues ``None``, so the generator learns it was cut off.

    Without the sentinel the generator stays parked on ``await queue.get()`` against a queue
    nothing will ever write to again — the exact "stalled reader parked forever" outcome the
    drop exists to prevent. The drain is what makes room for it.
    """
    store = LogStore(capacity=100)
    sub = store.subscribe(Filter(), subject=SUBJECT, queue_size=2)

    for i in range(6):
        store.append(make_entry(i))

    queued = drain(sub)
    assert queued == [None], (
        "a drop drains first and leaves exactly the sentinel: the queue being full IS the "
        "problem, so a sentinel that could not be enqueued would leave the generator parked"
    )
    assert sub.dropped is True


async def test_broken_subscriber_does_not_break_append() -> None:
    """A predicate that raises costs *that* subscriber and nothing else. ``POST /logs`` still 201s.

    ``_publish`` runs on the writer's thread inside the store's critical section, over predicates
    that came from a client's query string. If one bad subscriber could propagate an exception,
    every subsequent append would be a ``500`` — an unrelated client's bug taking down ingest for
    everyone.
    """
    store = LogStore(capacity=100)
    broken = store.subscribe(BrokenFilter(), subject="broken")  # type: ignore[arg-type]
    healthy = store.subscribe(Filter(), subject="healthy")

    record = store.append(make_entry(0))  # must not raise

    assert drain(healthy) == [record], "a healthy subscriber must be unaffected by a broken peer"
    assert broken.released is True, "the raising subscriber is the one that pays"
    assert store.stream_count("broken") == 0
    assert store.stream_count("healthy") == 1

    # And ingest keeps working afterwards — the store is not left in a poisoned state.
    second = store.append(make_entry(1))
    assert drain(healthy) == [second]


# ---------------------------------------------------------------------------------------------
# Lifecycle: idempotent release and the per-principal cap
# ---------------------------------------------------------------------------------------------


async def test_unsubscribe_is_idempotent() -> None:
    """Six exit paths, exactly one decrement. The return value names which call did the work."""
    store = LogStore(capacity=100)
    first = store.subscribe(Filter(), subject=SUBJECT)
    second = store.subscribe(Filter(), subject=SUBJECT)
    assert store.stream_count(SUBJECT) == 2

    assert store.unsubscribe(first) is True, "the first call releases"
    assert store.unsubscribe(first) is False, "every later call is a no-op"
    assert store.unsubscribe(first) is False

    # The load-bearing assertion: three calls, one decrement. A per-call decrement would read 0
    # here and the cap would have quietly stopped counting `second`.
    assert store.stream_count(SUBJECT) == 1
    assert store.subscriber_count() == 1

    assert store.unsubscribe(second) is True
    assert store.stream_count(SUBJECT) == 0


async def test_stream_counter_increments_and_decrements() -> None:
    """The cap is measured per principal, not process-wide — and the two counts are independent."""
    store = LogStore(capacity=100)
    alice_a = store.subscribe(Filter(), subject="alice")
    alice_b = store.subscribe(Filter(), subject="alice")
    bob = store.subscribe(Filter(), subject="bob")

    assert store.stream_count("alice") == 2
    assert store.stream_count("bob") == 1
    assert store.stream_count("nobody") == 0
    assert store.subscriber_count() == 3

    store.unsubscribe(alice_a)
    assert store.stream_count("alice") == 1
    assert store.stream_count("bob") == 1, "one principal's release must not touch another's"

    store.unsubscribe(alice_b)
    store.unsubscribe(bob)
    assert store.stream_count("alice") == 0
    assert store.subscriber_count() == 0


async def test_exceeding_stream_cap_raises() -> None:
    """``max_streams`` is enforced at subscribe time, and the exception carries both facts.

    The route maps this to a ``429`` whose detail is deliberately unlike the rate limiter's, so
    ``subject`` and ``limit`` both have to survive to the handler.
    """
    store = LogStore(capacity=100)
    held = [store.subscribe(Filter(), subject=SUBJECT, max_streams=2) for _ in range(2)]

    with pytest.raises(StreamLimitExceeded) as excinfo:
        store.subscribe(Filter(), subject=SUBJECT, max_streams=2)

    assert excinfo.value.subject == SUBJECT
    assert excinfo.value.limit == 2
    assert store.stream_count(SUBJECT) == 2, "a refused subscribe must not consume a slot"

    # Another principal is unaffected, and a freed slot is immediately reusable.
    store.subscribe(Filter(), subject="someone-else", max_streams=2)
    store.unsubscribe(held[0])
    store.subscribe(Filter(), subject=SUBJECT, max_streams=2)
    assert store.stream_count(SUBJECT) == 2


async def test_zero_queue_size_is_rejected() -> None:
    """``asyncio.Queue(maxsize=0)`` is *unbounded*, which inverts the one guarantee here.

    Failing loudly at subscribe time is the only safe reading of a zero: a silently unbounded
    per-subscriber buffer is the memory bug this whole subsystem exists to make unwritable.
    """
    store = LogStore(capacity=100)
    with pytest.raises(ValueError, match="UNBOUNDED"):
        store.subscribe(Filter(), subject=SUBJECT, queue_size=0)
    assert store.stream_count(SUBJECT) == 0


async def test_close_all_subscribers() -> None:
    """Shutdown terminates every subscription and reports how many. Unlike a drop, it does not drain.

    Without this the lifespan teardown leaves every generator parked on a queue nothing will ever
    write to again, and the process waits on them instead of exiting.
    """
    store = LogStore(capacity=100)
    subs = [store.subscribe(Filter(), subject=f"sub-{i}") for i in range(3)]
    record = store.append(make_entry(0))

    closed = store.close_all_subscribers()

    assert closed == 3
    assert store.subscriber_count() == 0
    for sub in subs:
        assert sub.released is True
        assert sub.dropped is False, "a shutdown is not a slow-consumer drop; the flag must differ"
        queued = drain(sub)
        # In-flight records are legitimately the client's, so they are delivered *before* the
        # sentinel rather than drained away.
        assert queued == [record, None]
    assert store.close_all_subscribers() == 0, "closing twice must be harmless"


# ---------------------------------------------------------------------------------------------
# `Last-Event-ID` resume
# ---------------------------------------------------------------------------------------------


async def test_replay_since_returns_newer_entries() -> None:
    """``after_seq`` is exclusive — an SSE ``id`` names an entry the client already received."""
    store = LogStore(capacity=100)
    for i in range(10):
        store.append(make_entry(i))

    items, truncated = store.replay_since(4, Filter(), max_items=100)

    assert [record.seq for record in items] == [5, 6, 7, 8, 9]
    assert truncated is False
    assert ids_of(list(items)) == [f"e{i:06d}" for i in range(5, 10)]


async def test_replay_since_applies_the_filter() -> None:
    """A resume must not deliver rows the same query would not have delivered live."""
    store = LogStore(capacity=100)
    for i in range(6):
        store.append(make_entry(i, level="ERROR" if i % 2 else "INFO"))

    items, truncated = store.replay_since(-1, level_filter("ERROR"), max_items=100)

    assert ids_of(list(items)) == ["e000001", "e000003", "e000005"]
    assert truncated is False


async def test_replay_is_bounded_and_flags_truncated() -> None:
    """A client away for an hour cannot make the server materialise the ring into one response.

    When the bound bites, the **newest** ``max_items`` are returned, not the oldest: the replay
    is about to be spliced onto the live tail, so keeping the newest end makes that join seamless
    and puts the gap at the far, already-flagged end. Returning fewer rows *silently* is the one
    behaviour that must never happen — hence the flag rather than just a shorter list.
    """
    store = LogStore(capacity=100)
    for i in range(20):
        store.append(make_entry(i))

    items, truncated = store.replay_since(-1, Filter(), max_items=5)

    assert truncated is True, "an incomplete resume must say so"
    assert len(items) == 5
    assert [record.seq for record in items] == [15, 16, 17, 18, 19], (
        "the bounded replay must keep the NEWEST window so it joins the live tail without a gap"
    )


async def test_replay_flags_truncated_when_the_anchor_was_evicted() -> None:
    """An anchor the ring has already passed is a real, unrecoverable gap — and is flagged."""
    store = LogStore(capacity=5)
    for i in range(20):
        store.append(make_entry(i))
    assert store.oldest_seq() == 15

    items, truncated = store.replay_since(2, Filter(), max_items=100)

    assert truncated is True, "the records after seq 2 are genuinely gone; say so"
    assert [record.seq for record in items] == [15, 16, 17, 18, 19]


async def test_subscribe_before_replay_duplicates_rather_than_drops() -> None:
    """At-least-once is the right choice for logs. The overlap is real and it is deliberate.

    The route subscribes *before* it reads the replay, so an entry appended in between lands in
    both. This pins that the overlap is a duplicate (which one line of client code dedupes on
    ``id``) and never a hole (which is not recoverable at all).
    """
    store = LogStore(capacity=100)
    for i in range(3):
        store.append(make_entry(i))

    # The route's ordering, reproduced: subscribe, then replay.
    sub = store.subscribe(Filter(), subject=SUBJECT)
    mid_flight = store.append(make_entry(3))
    items, _truncated = store.replay_since(2, Filter(), max_items=100)

    assert mid_flight.seq in {record.seq for record in items}, "the replay must include it"
    assert mid_flight in drain(sub), "and so must the live tail — duplicated, never lost"


async def test_replay_of_an_empty_store_is_empty_not_an_error() -> None:
    """Nothing to resume is a legitimate answer, and it is not a truncation."""
    store = LogStore(capacity=10)
    items, truncated = store.replay_since(-1, Filter(), max_items=100)
    assert items == []
    assert truncated is False


async def test_replay_with_a_nonpositive_bound_is_flagged_empty() -> None:
    """``max_items < 1`` cannot return anything, so it must not pretend the resume was complete."""
    store = LogStore(capacity=10)
    for i in range(3):
        store.append(make_entry(i))

    items, truncated = store.replay_since(-1, Filter(), max_items=0)

    assert items == []
    assert truncated is True


# ---------------------------------------------------------------------------------------------
# The delivery contract the route depends on
# ---------------------------------------------------------------------------------------------


async def test_queued_records_carry_the_seq_used_as_the_event_id() -> None:
    """``id = seq`` is what makes ``Last-Event-ID`` resume work without a second identifier.

    The route stringifies ``record.seq`` into the SSE ``id`` field and feeds it straight back to
    :meth:`~src.store.LogStore.replay_since` on reconnect, so the streamed id and the replay
    anchor have to be the same number by construction rather than by convention.
    """
    store = LogStore(capacity=100)
    sub = store.subscribe(Filter(), subject=SUBJECT)

    appended = [store.append(make_entry(i)) for i in range(3)]
    queued = drain(sub)

    assert [record.seq for record in queued if record is not None] == [0, 1, 2]
    assert [record.seq for record in appended] == [0, 1, 2]

    # And the anchor round-trips: replaying from the last delivered id yields nothing new.
    items, truncated = store.replay_since(queued[-1].seq, Filter(), max_items=100)  # type: ignore[union-attr]
    assert items == []
    assert truncated is False


async def test_a_parked_getter_is_woken_by_an_append() -> None:
    """The generator's ``await queue.get()`` really does wake on the writer's ``put_nowait``.

    Everything else in this file drains synchronously, which proves the record was *enqueued*.
    This proves the other half — that a consumer parked on the queue is scheduled when it lands —
    which is the whole reason heartbeats are sse-starlette's ping task rather than something the
    read loop does. Bounded by ``asyncio.timeout`` so a regression fails instead of hanging.
    """
    store = LogStore(capacity=100)
    sub = store.subscribe(Filter(), subject=SUBJECT)

    async with asyncio.timeout(5):
        getter = asyncio.create_task(sub.queue.get())
        await asyncio.sleep(0)  # let the task reach the await; not a delay, a scheduling yield
        assert not getter.done(), "nothing has been appended yet"

        record = store.append(make_entry(0))
        assert await getter is record


async def test_iter_matching_and_the_tail_agree_on_the_same_filter() -> None:
    """"The tail of this search" and "the history of this search" are one predicate, not two.

    The same ``Filter`` instance drives ``iter_matching`` (what ``GET /logs`` and ``/stats`` walk)
    and the fan-out (what the stream delivers). If they could disagree, a client would see
    different sets from ``?level=ERROR`` depending on which delivery mode it chose.
    """
    store = LogStore(capacity=100)
    flt = level_filter("ERROR", "FATAL")
    sub = store.subscribe(flt, subject=SUBJECT)

    for i, level in enumerate(["INFO", "ERROR", "WARN", "FATAL", "DEBUG", "ERROR"]):
        store.append(make_entry(i, level=level))

    streamed = ids_of(drain(sub))
    scanned = [record.entry.id for record in store.iter_matching(flt, SortOrder.ASC)]

    assert streamed == scanned == ["e000001", "e000003", "e000005"]
