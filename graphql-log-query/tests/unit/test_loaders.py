"""The DataLoader layer as pure logic: alignment, grouping, batching and the batch window.

No database and no GraphQL. Everything here is either a pure function over rows and keys, or a
loader driven by a **recording load function** that counts the batches it was called with — which
is what makes "these N loads became one batch" assertable without a server, a schema or Postgres in
the way. The integration suite then proves the same property end to end by counting the statements
PostgreSQL actually received; neither is sufficient alone, and this one is where the *ordering
contract* can be tested against the batches that only occur in theory.

.. rubric:: Why the alignment tests use shuffled keys, misses and duplicates

A load function must return results **positionally aligned to the keys it was given**. Nothing
enforces that beyond the length — Strawberry raises only for a length mismatch — so a function that
returns the right number of results in the wrong order silently hands every parent somebody else's
rows, and the response looks perfectly healthy. The batches below are therefore deliberately
awkward: keys out of order, keys with no rows, the same key twice. A test that loaded three keys
that each matched, in order, would pass against an implementation that ignored the keys entirely
and returned ``list(grouped.values())``.

.. rubric:: Why the batch-window tests close the window by hand instead of sleeping

A window test has two things to say — *these loads became one batch* and *they came out in arrival
order* — and only the first of them has anything to do with elapsed time. Written the obvious way,
both ride on "sleep less than the window and hope", which makes the scheduler a silent participant
in every assertion. So most of the tests below open a window that will never close on its own
(:data:`NEVER_CLOSES_BY_ITSELF_MS`), step the event loop deterministically with
:func:`_let_the_loop_run`, and then set the window's closing event themselves — which is precisely
what the loader's timer does, one line earlier. No wall-clock time passes, nothing races, and the
"nothing has dispatched yet" assertions that hold the window's feet to the fire become possible at
all.

One test keeps a real timer and a real (generous) wall-clock gap, because something has to prove
the ``loop.call_at`` deadline is wired up and fires. It is the exception, not the pattern.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest

from src.config import Settings
from src.db.models import LogEntryORM
from src.graphql.loaders import (
    LoaderRegistry,
    WindowedDataLoader,
    align_logs_by_id,
    group_logs_by_trace_id,
)

#: A fixed instant, because these rows are compared by value and a wall-clock read would make the
#: expected ordering depend on when the suite ran.
ANCHOR = datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc)


def _row(row_id: int, trace_id: Optional[str], *, age_minutes: int = 0) -> LogEntryORM:
    """One unmapped ``LogEntryORM`` instance — a value object, never added to a session.

    Constructing a declarative class without a session is ordinary Python: the attributes are set,
    nothing is persisted, and :meth:`src.graphql.types.LogEntry.from_orm` reads exactly the same
    attributes it reads off a real row. That is what lets the projection be exercised here without
    a database.
    """
    return LogEntryORM(
        id=row_id,
        timestamp=ANCHOR - timedelta(minutes=age_minutes),
        service="api-gateway",
        level="INFO",
        message=f"entry {row_id}",
        metadata_=None,
        trace_id=trace_id,
    )


def _settings(**overrides: Any) -> Settings:
    """Settings built directly, so no environment or ``.env`` can reach these tests."""
    return Settings(_env_file=None, seed_entries=0, seed_orders=0, **overrides)


# --- group_logs_by_trace_id: the relatedLogs alignment -------------------------------------------


def test_groups_are_returned_in_key_order_not_in_row_order() -> None:
    """The result is indexed by the **keys**, whatever order the database returned rows in.

    The rows below arrive interleaved and newest-first, exactly as
    ``ORDER BY timestamp DESC, id DESC`` delivers them, while the keys are in an unrelated order —
    which is the realistic case, since a batch's key order is the order the parent entries happened
    to resolve in.
    """
    rows = [_row(5, "b", age_minutes=1), _row(4, "a", age_minutes=2), _row(3, "b", age_minutes=3)]

    groups = group_logs_by_trace_id(rows, ["a", "b"])

    assert [[entry.id for entry in group] for group in groups] == [["4"], ["5", "3"]]


def test_a_key_with_no_rows_gets_an_empty_list_at_its_own_position() -> None:
    """A miss is an empty list **in place**, never a dropped entry that shifts everything after it.

    This is the failure that produces confidently wrong data rather than an error: drop the miss
    and every later key is answered with the previous key's group, with no length mismatch for
    Strawberry to catch (there would be one here — which is precisely why the check below asserts
    the position rather than only the length).
    """
    rows = [_row(1, "present")]

    groups = group_logs_by_trace_id(rows, ["missing-1", "present", "missing-2"])

    assert len(groups) == 3
    assert groups[0] == []
    assert [entry.id for entry in groups[1]] == ["1"]
    assert groups[2] == []


def test_a_repeated_key_is_answered_at_both_of_its_positions() -> None:
    """Duplicates in one batch are legal and must not shorten or reorder the result.

    Strawberry's per-key cache normally collapses duplicates before the load function sees them, so
    this is belt and braces — but a load function that assumed unique keys would break silently the
    day caching is turned off, and "the loader has a cache" is not something the ordering contract
    is allowed to depend on.
    """
    rows = [_row(1, "a"), _row(2, "b", age_minutes=1)]

    groups = group_logs_by_trace_id(rows, ["a", "b", "a"])

    assert len(groups) == 3
    assert [entry.id for entry in groups[0]] == ["1"]
    assert [entry.id for entry in groups[1]] == ["2"]
    assert [entry.id for entry in groups[2]] == ["1"]


def test_rows_outside_the_requested_keys_are_ignored() -> None:
    """A row the query could not have returned is dropped rather than crashing the batch.

    ``WHERE trace_id IN (...)`` cannot produce one, so this is defence rather than behaviour — but
    the alternative (a ``KeyError`` deep inside a load function) would fail the whole operation for
    every parent in the batch, not just the odd row.
    """
    rows = [_row(1, "a"), _row(2, "stranger"), _row(3, None)]

    groups = group_logs_by_trace_id(rows, ["a"])

    assert [entry.id for entry in groups[0]] == ["1"]


def test_each_group_is_capped_independently_at_max_per_key() -> None:
    """The cap applies **per key**, to the newest entries, and does not spend one key's budget on
    another.

    A cap applied to the flat result instead would let one busy trace consume the whole allowance
    and leave the next key empty — a parent whose ``relatedLogs`` is then wrongly ``[]``.
    """
    rows = [_row(index, "hot", age_minutes=index) for index in range(1, 6)]
    rows += [_row(index, "quiet", age_minutes=index) for index in range(10, 13)]

    groups = group_logs_by_trace_id(rows, ["hot", "quiet"], max_per_key=2)

    assert [entry.id for entry in groups[0]] == ["1", "2"], "newest two, because rows arrive newest first"
    assert [entry.id for entry in groups[1]] == ["10", "11"]


def test_an_empty_batch_produces_an_empty_result() -> None:
    """No keys, no answers — and specifically not a spurious empty group."""
    assert group_logs_by_trace_id([_row(1, "a")], []) == []


def test_the_projection_is_the_published_type_not_the_row() -> None:
    """Groups carry :class:`~src.graphql.types.LogEntry`, projected through ``from_orm``.

    The ``metadata_`` -> ``metadata`` rename and the ``int`` -> ``ID`` string conversion both live
    in that one classmethod, so a loader that returned ORM rows would publish a different shape
    from every other path — with the same field names, which is why it is worth pinning.
    """
    row = _row(7, "a")
    row.metadata_ = {"host": "node-1"}

    entry = group_logs_by_trace_id([row], ["a"])[0][0]

    assert entry.id == "7"
    assert isinstance(entry.id, str)
    assert entry.metadata == {"host": "node-1"}
    assert entry.trace_id == "a"


# --- align_logs_by_id: the Query.log alignment ---------------------------------------------------


def test_entries_are_aligned_to_the_ids_that_were_asked_for() -> None:
    """Rows come back in the store's ordering; the result is indexed by the caller's key order."""
    rows = [_row(30, None, age_minutes=1), _row(10, None, age_minutes=2)]

    aligned = align_logs_by_id(rows, [10, 30])

    assert [None if entry is None else entry.id for entry in aligned] == ["10", "30"]


def test_an_absent_id_is_none_at_its_own_position() -> None:
    """``None``, not an omission and not an exception — ``Query.log`` answers ``null`` for a miss."""
    aligned = align_logs_by_id([_row(2, None)], [1, 2, 3])

    assert aligned[0] is None
    assert aligned[1] is not None and aligned[1].id == "2"
    assert aligned[2] is None


def test_a_repeated_id_is_answered_at_both_positions() -> None:
    """The same entry can legitimately be asked for twice in one document (two aliases)."""
    aligned = align_logs_by_id([_row(4, None)], [4, 9, 4])

    assert [None if entry is None else entry.id for entry in aligned] == ["4", None, "4"]


# --- The loader itself: batching, and the batch window --------------------------------------------


def _recording_loader(**kwargs: Any) -> tuple[list[list[Any]], WindowedDataLoader]:
    """A loader whose load function records every batch it was handed and echoes the keys back.

    Returns ``(batches, loader)``. ``batches`` is the whole point: it is a list of the key lists the
    load function was actually called with, so "these N loads became one call" is an assertion
    about a list length rather than about a timing.
    """
    batches: list[list[Any]] = []

    async def load_fn(keys: list[Any]) -> list[Any]:
        batches.append(list(keys))
        return [f"value-{key}" for key in keys]

    return batches, WindowedDataLoader(load_fn=load_fn, **kwargs)


#: A window so wide that nothing inside a test run will ever reach its deadline. Tests that use it
#: close it themselves with :func:`_close_the_window`, so the number is not a timeout waiting to be
#: tuned — it is a promise that the timer stays out of the way.
NEVER_CLOSES_BY_ITSELF_MS = 60 * 60 * 1000


async def _let_the_loop_run(passes: int = 8) -> None:
    """Drain the event loop's ready queue ``passes`` times without advancing the wall clock.

    Each ``await asyncio.sleep(0)`` is exactly one pass, so this is how a test says "let everything
    that is ready run" instead of "sleep for a few milliseconds and hope that was enough".
    Strawberry's dispatch needs three passes to reach the load function (``call_soon`` ->
    ``create_task`` -> the task's first step), so eight is several times any assertion below needs
    — which matters most for the assertions that a batch has **not** been dispatched, since those
    are only worth anything if the dispatch had every chance to happen.
    """
    for _ in range(passes):
        await asyncio.sleep(0)


def _close_the_window(loader: WindowedDataLoader) -> None:
    """Close the loader's currently-open window by hand, ahead of its timer.

    The window is one :class:`asyncio.Event` set by one ``loop.call_at`` timer, so setting that
    event *is* what closing the window means; this just does it on the test's schedule rather than
    the scheduler's. The timer stays scheduled and fires harmlessly later — ``Event.set`` on an
    already-set event is a no-op.

    Reaching into the loader for the event is deliberate. It is the mechanism the loader's
    docstring commits to, and coupling to it is what buys these tests determinism; if it is ever
    replaced, these tests should fail loudly and be rewritten against whatever replaced it, not
    quietly keep passing while measuring something else.
    """
    window = loader._window_closed
    assert window is not None, "no window is open — nothing has called load() yet"
    window.set()


async def _finish(*tasks: Any) -> list[Any]:
    """Await ``tasks`` with a ceiling on how long a broken implementation may hang.

    Every await below is on a waiter that should already be runnable, so the timeout is never
    reached in a passing run. It exists so that a regression which strands a waiter — a window
    nothing closes, a waiter parked on an event nobody sets — surfaces as a failed test rather than
    as a suite that never returns.
    """
    return await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)


async def test_loads_issued_in_one_tick_become_exactly_one_batch() -> None:
    """The property the whole feature exists for, at its smallest: ten loads, one call.

    Ten separate calls would also produce the right ten answers — which is why the assertion is on
    ``len(batches)`` and not only on the values. A test that checked the values alone would pass
    against no batching at all.
    """
    batches, loader = _recording_loader(batch_window_ms=0)

    results = await asyncio.gather(*(loader.load(key) for key in range(10)))

    assert results == [f"value-{key}" for key in range(10)]
    assert len(batches) == 1
    assert batches[0] == list(range(10))


async def test_a_repeated_key_is_loaded_once_within_an_operation() -> None:
    """The per-key cache is what makes a loader more than a batcher.

    Safe **because the registry is per-operation** (see :mod:`src.graphql.context`): the memoised
    value cannot outlive the request it was read for. That is a lifetime property, not a loader
    property, which is why it is asserted in the integration suite as well.
    """
    batches, loader = _recording_loader(batch_window_ms=0)

    results = await asyncio.gather(loader.load("k"), loader.load("k"), loader.load("k"))

    assert results == ["value-k"] * 3
    assert batches == [["k"]]


async def test_with_no_window_a_load_a_tick_later_starts_a_new_batch() -> None:
    """The behaviour ``DATALOADER_BATCH_WINDOW_MS=0`` actually has — stated, not assumed.

    Zero means "dispatch on the next event-loop tick", so a load issued after that tick cannot join
    the batch: it gets its own. That is the baseline the window is measured against, and pinning it
    here is what makes the next test a proof rather than a coincidence.
    """
    batches, loader = _recording_loader(batch_window_ms=0)

    async def straggler() -> Any:
        await asyncio.sleep(0.01)
        return await loader.load("late")

    await asyncio.gather(loader.load("early"), straggler())

    assert batches == [["early"], ["late"]], (
        "with no window the two loads are in different ticks and must be in different batches"
    )


async def test_a_positive_window_holds_the_batch_open_across_ticks() -> None:
    """``DATALOADER_BATCH_WINDOW_MS > 0`` does something, and this is what it does.

    Same two loads as the test above and the same real gap between them, but with a real window
    wide enough to span them: they land in **one** batch. This is the test that proves the deadline
    is genuinely wired to ``loop.call_at`` and genuinely fires — the others below close the window
    by hand and so would still pass if the timer were never scheduled at all.

    The window is far wider than the gap (50ms against 5ms) so that the *batching* is measuring the
    mechanism rather than the scheduler's punctuality on a loaded machine. The *ordering*, by
    contrast, no longer depends on timing in any way: both loads park on one shared event, and
    ``Event.set`` wakes its waiters in registration order. That is why this asserts the batch's
    exact contents. It is the assertion that fails if anyone reverts the window to a per-waiter
    ``asyncio.sleep(deadline - now)``, which batches just as well and shuffles the keys, because
    ``sleep`` re-reads the clock and every waiter ends up with its own wake-up instant.
    """
    batches, loader = _recording_loader(batch_window_ms=50)

    # Started and settled first, so "early arrived before late" is a fact about the program rather
    # than a fact about how `gather` happens to order its arguments.
    early = asyncio.create_task(loader.load("early"))
    await _let_the_loop_run()

    async def straggler() -> Any:
        await asyncio.sleep(0.005)
        return await loader.load("late")

    results = await asyncio.gather(early, straggler())

    assert results == ["value-early", "value-late"]
    assert batches == [["early", "late"]], (
        f"the window must hold the batch open for the straggler, and must wake its waiters in "
        f"arrival order; got {batches!r}"
    )


async def test_a_positive_window_produces_exactly_one_batch_containing_both_keys() -> None:
    """The same window, asserted **without** reference to the order inside the batch.

    The test above pins arrival order as well, and that ordering is a real guarantee worth pinning.
    But it is the *cheaper* of the two properties: a maintainer who decides FIFO is not worth
    guaranteeing can relax that assertion in one edit, and if it were the only test of a positive
    window, the thing the setting actually exists for — **one** database round trip instead of two
    — would go with it silently. This is the load-bearing half, stated so it cannot be deleted by
    accident.
    """
    batches, loader = _recording_loader(batch_window_ms=50)

    async def straggler() -> Any:
        await asyncio.sleep(0.005)
        return await loader.load("late")

    results = await asyncio.gather(loader.load("early"), straggler())

    # `gather` returns in argument order whatever order the loads completed in, so this says
    # nothing about the batch — it is here only to prove both loads were actually answered.
    assert results == ["value-early", "value-late"]
    assert len(batches) == 1, f"two loads inside one window must cost one call; got {batches!r}"
    assert sorted(batches[0]) == ["early", "late"], "and both keys must be in it"


async def test_the_window_wakes_its_waiters_in_the_order_they_arrived() -> None:
    """FIFO, stated on its own and proved without the clock in the way.

    Four loads, each settled onto the window before the next is issued, so their arrival order is
    unambiguous — and deliberately in an order that is neither sorted nor reverse-sorted, so an
    implementation that woke waiters in *any* other order would have to reproduce this permutation
    by luck. The window is closed by hand, so nothing here depends on elapsed time.

    The mid-way assertion is the other half of the point: while the window is open, the load
    function must not have been called at all. With ``batch_window_ms=0`` the batch would have
    dispatched several loop passes earlier, so this is what distinguishes "the window held" from
    "the loads happened to be close together".
    """
    batches, loader = _recording_loader(batch_window_ms=NEVER_CLOSES_BY_ITSELF_MS)

    keys = ["mike", "alpha", "zulu", "bravo"]
    loads = []
    for key in keys:
        loads.append(asyncio.create_task(loader.load(key)))
        await _let_the_loop_run()

    assert batches == [], "the window is open; nothing may have been dispatched yet"

    _close_the_window(loader)
    results = await _finish(*loads)

    assert results == [f"value-{key}" for key in keys]
    assert batches == [keys], f"one batch, keys in arrival order; got {batches!r}"


async def test_a_load_arriving_after_its_window_closed_gets_a_fresh_one() -> None:
    """The window is a repeating quantum, and a late arrival is neither dropped nor stuck.

    Two failure modes live at this boundary and both are silent. Fold the late load into the window
    that just closed and it joins a batch that has already been handed to the load function — its
    key never reaches a query and its future is never resolved, so the field hangs rather than
    errors. Have it wait on the closed window's already-fired timer and it hangs for the opposite
    reason. It must get its own window: a second batch, promptly.
    """
    batches, loader = _recording_loader(batch_window_ms=NEVER_CLOSES_BY_ITSELF_MS)

    first = asyncio.create_task(loader.load("first"))
    await _let_the_loop_run()
    _close_the_window(loader)

    assert await _finish(first) == ["value-first"]
    assert batches == [["first"]]

    second = asyncio.create_task(loader.load("second"))
    await _let_the_loop_run()

    assert batches == [["first"]], "the closed window must not be reopened, and its batch is gone"
    assert not second.done(), "the late load waits for a window of its own rather than sailing past"

    _close_the_window(loader)

    assert await _finish(second) == ["value-second"]
    assert batches == [["first"], ["second"]]


async def test_a_cancelled_waiter_does_not_take_its_batch_mates_down_with_it() -> None:
    """Cancelling the load that **opened** the window must not strand the ones that joined it.

    This is the failure an implementation invites the moment the window's timer belongs to a
    waiter rather than to the loader: cancel that waiter, and the timer goes with it, and every
    other load in the batch waits forever on an event nobody will ever set. A GraphQL client
    disconnecting mid-operation cancels resolvers exactly this way, so it is an ordinary Tuesday
    rather than a hypothetical.

    The cancelled key must also leave no trace in the batch — it never reached ``super().load``, so
    the load function must never be asked to answer for it.
    """
    batches, loader = _recording_loader(batch_window_ms=NEVER_CLOSES_BY_ITSELF_MS)

    doomed = asyncio.create_task(loader.load("doomed"))
    await _let_the_loop_run()  # `doomed` is the load that opens the window
    survivor = asyncio.create_task(loader.load("survivor"))
    await _let_the_loop_run()

    doomed.cancel()
    await _let_the_loop_run()

    _close_the_window(loader)

    assert await _finish(survivor) == ["value-survivor"]
    assert doomed.cancelled()
    assert batches == [["survivor"]], (
        f"the cancelled load must leave no key behind and must not close its batch-mate's window; "
        f"got {batches!r}"
    )


async def test_a_zero_window_costs_no_timer_at_all() -> None:
    """The shipped default is Strawberry's own behaviour with **nothing** added to it.

    "Zero means no window" has an obvious wrong implementation — ``call_at(now, ...)`` — which is a
    real timer, a real event and a real extra trip through the event loop on every single load, in
    exchange for a delay of zero. Nothing in the batching assertions would notice: the batches come
    out identical either way. So this asserts the cost instead of the behaviour, by watching the
    loop's own scheduler: across three loads it must be asked for no timer whatsoever, and the
    loader must not so much as build a window to hold one.
    """
    loop = asyncio.get_running_loop()
    scheduled: list[float] = []
    real_call_at = loop.call_at

    def recording_call_at(when: float, callback: Any, *args: Any, **kwargs: Any) -> Any:
        scheduled.append(when)
        return real_call_at(when, callback, *args, **kwargs)

    batches, loader = _recording_loader(batch_window_ms=0)
    loop.call_at = recording_call_at  # type: ignore[method-assign]
    try:
        results = await asyncio.gather(*(loader.load(key) for key in ("a", "b", "c")))
    finally:
        # Deleting the instance attribute restores the bound method, rather than leaving a
        # permanent shadow of it on the loop.
        del loop.call_at  # type: ignore[attr-defined]

    assert results == ["value-a", "value-b", "value-c"]
    assert batches == [["a", "b", "c"]], "and it still batches, which is the whole of zero's job"
    assert scheduled == [], f"a zero window must schedule no timer; got {scheduled!r}"
    assert loader._window_closed is None, "a zero window must not even open a window"


# --- LoaderRegistry -------------------------------------------------------------------------------


class _FakeRepository:
    """Just enough of :class:`~src.db.repository.LogRepository` for the registry's two load functions."""

    def __init__(self, rows: list[LogEntryORM]) -> None:
        self._rows = rows
        self.trace_batches: list[list[str]] = []
        self.id_batches: list[list[int]] = []

    async def list_logs_by_trace_ids(self, trace_ids: Any) -> list[LogEntryORM]:
        self.trace_batches.append(list(trace_ids))
        wanted = set(trace_ids)
        return [row for row in self._rows if row.trace_id in wanted]

    async def get_logs_by_ids(self, log_ids: Any) -> list[LogEntryORM]:
        self.id_batches.append(list(log_ids))
        wanted = set(log_ids)
        return [row for row in self._rows if row.id in wanted]


@asynccontextmanager
async def _provider_for(repository: Any) -> AsyncIterator[Any]:
    """Hand the same fake repository to every batch."""
    yield repository


async def test_the_registry_builds_both_loaders_and_takes_the_window_from_settings() -> None:
    """Construction alone opens nothing — no session, no connection, no query.

    That is what makes it safe for ``on_operation`` to mint a registry for **every** operation,
    including the ones that never touch the database. The provider below fails loudly if it is
    entered, so this test would fail rather than quietly pass if construction started doing work.
    """

    @asynccontextmanager
    async def exploding() -> AsyncIterator[Any]:
        raise AssertionError("constructing a registry must not open a repository")
        yield  # pragma: no cover - unreachable, present so this is an async generator

    registry = LoaderRegistry(exploding, _settings(dataloader_batch_window_ms=7))

    assert registry.batch_window_ms == 7
    assert isinstance(registry.logs_by_trace_id, WindowedDataLoader)
    assert isinstance(registry.log_by_id, WindowedDataLoader)


async def test_an_explicit_window_overrides_the_configured_one() -> None:
    """Tests pin the window; production always takes the configured value."""

    @asynccontextmanager
    async def exploding() -> AsyncIterator[Any]:
        raise AssertionError("not used")
        yield  # pragma: no cover

    registry = LoaderRegistry(
        exploding, _settings(dataloader_batch_window_ms=7), batch_window_ms=0
    )

    assert registry.batch_window_ms == 0


async def test_a_negative_window_is_clamped_rather_than_scheduled_in_the_past() -> None:
    """Settings refuses a negative window; the loader still clamps, because it takes any int.

    Clamping to zero takes the no-window branch, so a negative value never reaches ``call_at`` with
    a deadline behind it — which would fire the window closed on the very next loop iteration and
    give the operator a knob that gets *less* batching the further they turn it the wrong way.
    """
    with pytest.raises(ValueError, match="DATALOADER_BATCH_WINDOW_MS"):
        _settings(dataloader_batch_window_ms=-1)

    _, loader = _recording_loader(batch_window_ms=-100)

    assert await loader.load("k") == "value-k"
    assert loader._window_closed is None, "clamped to zero means no window, not a window of zero"


async def test_the_registry_batches_related_lookups_through_one_repository_call() -> None:
    """Five entries, three distinct traces, **one** call into the store — with aligned results.

    The registry is exercised through its loaders (not by calling the load function directly), so
    what is being asserted is the whole path a resolver takes: ``load()`` per parent, one batch, one
    store call, results back at the right positions.
    """
    rows = [
        _row(1, "t-a", age_minutes=1),
        _row(2, "t-a", age_minutes=2),
        _row(3, "t-b", age_minutes=3),
        _row(4, None, age_minutes=4),
    ]
    repository = _FakeRepository(rows)
    registry = LoaderRegistry(
        lambda: _provider_for(repository), _settings(), batch_window_ms=0
    )

    groups = await asyncio.gather(
        registry.logs_by_trace_id.load("t-b"),
        registry.logs_by_trace_id.load("t-a"),
        registry.logs_by_trace_id.load("t-missing"),
    )

    assert len(repository.trace_batches) == 1, "three parents must cost one store call"
    assert repository.trace_batches[0] == ["t-b", "t-a", "t-missing"]
    assert [[entry.id for entry in group] for group in groups] == [["3"], ["1", "2"], []]


async def test_the_registry_batches_by_id_lookups_and_reports_misses_as_none() -> None:
    """The same for ``Query.log``: several aliases, one store call, ``None`` for the id with no row."""
    repository = _FakeRepository([_row(1, None), _row(2, None, age_minutes=1)])
    registry = LoaderRegistry(
        lambda: _provider_for(repository), _settings(), batch_window_ms=0
    )

    entries = await asyncio.gather(
        registry.log_by_id.load(2), registry.log_by_id.load(99), registry.log_by_id.load(1)
    )

    assert len(repository.id_batches) == 1
    assert repository.id_batches[0] == [2, 99, 1]
    assert [None if entry is None else entry.id for entry in entries] == ["2", None, "1"]


async def test_the_registry_applies_the_configured_per_group_cap() -> None:
    """``MAX_QUERY_LIMIT`` reaches ``relatedLogs`` too — spec §2 item 22 says *every* query path."""
    rows = [_row(index, "t", age_minutes=index) for index in range(1, 6)]
    repository = _FakeRepository(rows)
    registry = LoaderRegistry(
        lambda: _provider_for(repository),
        _settings(default_query_limit=2, max_query_limit=2),
        batch_window_ms=0,
    )

    group = await registry.logs_by_trace_id.load("t")

    assert [entry.id for entry in group] == ["1", "2"]
