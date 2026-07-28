"""The DataLoaders, the registry that owns them, and the batch window — spec §2 item 29.

The requirement in one line: *N field resolutions produce one database round trip, not N.*

``{ logs(limit: 50) { id relatedLogs { id } } }`` resolves ``relatedLogs`` fifty times. Written the
obvious way that is fifty ``SELECT ... WHERE trace_id = :id`` statements — the N+1 problem, and the
single most common way a GraphQL API becomes slower than the REST API it replaced. A DataLoader
turns those fifty calls into one ``SELECT ... WHERE trace_id IN (...)``, by collecting the keys that
arrive before the batch is dispatched and answering all of them from one result.

.. rubric:: THE POSITIONAL-ORDERING CONTRACT — the invariant this whole module is built around

A load function is handed ``keys`` and must return a sequence **of the same length, in the same
order**. ``result[i]`` is the answer to ``keys[i]``. Nothing checks this at run time beyond the
length (Strawberry raises ``WrongNumberOfResultsReturned`` for a length mismatch, and *only* for
that), so a load function that returns the right number of results in the wrong order is accepted
silently and hands every parent somebody else's rows. The response is well-formed, the row counts
are plausible, and a test that checks "``relatedLogs`` returned three entries" passes.

That is why the re-alignment is written as two **pure functions** — :func:`group_logs_by_trace_id`
and :func:`align_logs_by_id` — that take the rows and the keys and return the aligned result. They
have no database in them, so the ordering logic is unit-testable against a shuffled batch with
misses and duplicates in it, which is exactly the case that produces silently wrong data.

Three consequences of the contract, all of them tested:

* A key with **no** matching rows gets an empty list (or ``None`` for the by-id loader), never an
  exception and never a hole. Absence is an ordinary answer.
* A **repeated** key gets the same answer at both positions. (Strawberry's per-key cache normally
  collapses duplicates before the load function ever sees them, so this is belt and braces — but a
  load function that assumed unique keys would fail the day the cache is turned off.)
* The order the *database* returns rows in is irrelevant to the alignment. The SQL orders by
  ``(timestamp DESC, id DESC)`` because that is what a client should see inside each group, not
  because anything downstream depends on it.

.. rubric:: The loaders are PER-OPERATION, and their cache is why that matters

A ``DataLoader`` memoises: once ``load(k)`` has resolved, every later ``load(k)`` on the same loader
returns the same value without asking the database. That is what makes it a loader rather than a
batcher, and it is also what makes its lifetime a correctness question rather than a tuning one.
A loader that outlives an operation serves rows from whenever it happened to load them — and
because Strawberry resolves ``context_getter`` **once per WebSocket connection**, the obvious place
to build one is the one place that is wrong. See :mod:`src.graphql.context`, which owns the
lifecycle, for the full argument. This module deliberately takes no view: it is handed a way to get
a repository and builds loaders over it.

.. rubric:: ``DATALOADER_BATCH_WINDOW_MS`` — what the setting means, and why it is implemented here

**Strawberry 0.324.0's ``DataLoader`` has no batch-window knob.** Its constructor takes
``load_fn``, ``max_batch_size``, ``cache``, ``loop``, ``cache_map`` and ``cache_key_fn``; the batch
is dispatched with ``loop.call_soon(...)``, i.e. on the **next event-loop tick**. Every key loaded
in the current tick joins one batch; a key loaded one tick later starts a new one. There is nothing
time-based to configure.

So the declared setting had two honest resolutions and one dishonest one. The dishonest one is to
pass it nowhere and leave a documented knob that moves nothing — a lie in the config table, and the
worst kind, because an operator tunes it and measures noise. The two honest ones are to delete the
key or to make it real. It is one of the spec's own §7 parameters, so it stays, and
:class:`WindowedDataLoader` makes it real:

* ``0`` (**the default**) — dispatch on the next event-loop tick. Stock Strawberry behaviour; the
  window code is a single comparison that returns immediately. This is the right default for *this*
  schema: Strawberry resolves a selection set's fields concurrently, so all fifty ``relatedLogs``
  calls are issued in one tick and are already maximally batched. A window would add latency to
  every operation and widen nothing.
* ``> 0`` — hold the batch open for that many milliseconds. Implemented as **one timer and one
  shared** :class:`asyncio.Event` per window: the first ``load()`` of a window schedules a single
  ``loop.call_at(deadline, event.set)``, and every ``load()`` that arrives in the meantime parks on
  that same event. When it is set they all resume in the same event-loop iteration — **in the order
  they arrived** — and land in one batch. This is what buys wider batches when loads *straddle
  awaits*: a resolver that awaits something else before loading, which is what C11's cross-entity
  traversals do.

The trade is the usual one and it is worth stating: a window adds up to its own length to the
latency of the fields that use it. At the project's default of 0 it costs nothing at all.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from strawberry.dataloader import DataLoader

from src.config import Settings
from src.db.models import LogEntryORM
from src.db.repository import LogRepository
from src.graphql.types import LogEntry

#: How a registry gets a repository for one batch. Deliberately a *factory of context managers*
#: rather than a repository or a session: the lifetime rule ("one session per operation, except for
#: subscriptions") lives in :meth:`src.graphql.context.Context.repository`, and a loader that held a
#: session directly would be a second place that rule has to be remembered. Every batch simply
#: enters the provider and gets whatever the current operation's policy says it should get.
RepositoryProvider = Callable[[], AbstractAsyncContextManager[LogRepository]]


# =================================================================================================
# The pure half: rows + keys -> results aligned to the keys.
#
# No session, no await, no Strawberry. These two functions ARE the ordering contract, and keeping
# them out of the I/O is what lets a unit test hand them a deliberately shuffled batch containing
# misses and duplicates and assert on the alignment — the failure mode that produces confidently
# wrong data rather than an error.
# =================================================================================================


def group_logs_by_trace_id(
    rows: Sequence[LogEntryORM],
    keys: Sequence[str],
    *,
    max_per_key: Optional[int] = None,
) -> list[list[LogEntry]]:
    """Bucket ``rows`` by ``trace_id`` and return one bucket per key, **in key order**.

    Args:
        rows: What :meth:`~src.db.repository.LogRepository.list_logs_by_trace_ids` returned — a
            flat list covering every key, newest first. Rows whose ``trace_id`` is not among the
            keys are ignored rather than treated as an error; the query cannot produce them, and
            silently dropping one is better than failing a whole operation if it ever does.
        keys: The batch, in the order the loader was called in. The result is aligned to **this**.
        max_per_key: Optional per-bucket ceiling, applied to the newest entries because the rows
            arrive newest first. Passed ``MAX_QUERY_LIMIT`` by :class:`LoaderRegistry`, so the
            "every list a client can select is capped" rule (spec §2 item 22) covers this field
            too. ``None`` means uncapped, which is what the unit tests use.

    Returns:
        ``len(keys)`` lists. A key with no rows gets ``[]`` — the empty list is the answer, not a
        missing entry. A key that appears twice gets the **same list object** at both positions, so
        callers must treat the result as read-only (``LogEntry.related_logs`` builds a new list).
    """
    grouped: dict[str, list[LogEntry]] = {key: [] for key in keys}

    for row in rows:
        trace_id = row.trace_id
        if trace_id is None:
            continue
        bucket = grouped.get(trace_id)
        if bucket is None:
            continue
        if max_per_key is not None and len(bucket) >= max_per_key:
            continue
        bucket.append(LogEntry.from_orm(row))

    return [grouped[key] for key in keys]


def align_logs_by_id(
    rows: Sequence[LogEntryORM], keys: Sequence[int]
) -> list[Optional[LogEntry]]:
    """Index ``rows`` by primary key and return one entry per key, **in key order**.

    ``None`` at any position whose id had no row. That is what makes ``Query.log(id:)`` able to
    answer ``null`` for a miss with no ``errors`` entry, exactly as it did before the loader existed
    — see its resolver for why absence is an ordinary answer rather than a ``NOT_FOUND``.
    """
    by_id = {row.id: LogEntry.from_orm(row) for row in rows}
    return [by_id.get(key) for key in keys]


# =================================================================================================
# The loader itself
# =================================================================================================


class WindowedDataLoader(DataLoader):
    """A Strawberry :class:`~strawberry.dataloader.DataLoader` with a real batch window.

    Subclassed rather than wrapped so the window **cannot be bypassed**: every call site goes
    through :meth:`load` (and through ``load_many``, which is built on it), so there is no path that
    reaches the underlying batcher without passing the window first.

    Everything else is Strawberry's: the per-key cache, the futures, the dispatch, the
    exception-per-key handling. The only override is when ``load`` hands the key over.

    .. rubric:: The two guarantees, because callers and tests are entitled to rely on them

    ``batch_window_ms == 0`` (the shipped default)
        Nothing is scheduled and nothing is awaited. :meth:`_hold_the_batch_open` returns on its
        first comparison, so ``load`` is Strawberry's ``load`` with one branch in front of it — no
        timer, no event, not even an extra event-loop iteration. Zero is free.

    ``batch_window_ms > 0``
        Every ``load`` issued while a window is open lands in **one** batch, and its keys appear in
        that batch **in the order the loads arrived** (FIFO). The ordering is a deliberate property
        of the mechanism rather than an accident of scheduling, and tests may rely on it; see
        :meth:`_hold_the_batch_open` for exactly what guarantees it.

    See this module's docstring for why the window exists at all.
    """

    def __init__(self, *args: Any, batch_window_ms: int = 0, **kwargs: Any) -> None:
        """Build the loader.

        Args:
            *args: Forwarded to :class:`~strawberry.dataloader.DataLoader` (``load_fn`` in
                practice).
            batch_window_ms: How long a batch stays open. ``0`` (or anything negative, which is
                clamped) means "dispatch on the next event-loop tick", which is stock behaviour.
            **kwargs: Forwarded unchanged — ``cache``, ``max_batch_size``, ``cache_key_fn``, ...
        """
        super().__init__(*args, **kwargs)
        # Seconds, because that is what loop.time() and loop.call_at speak. Clamped at zero so a
        # negative configuration degrades to "no window" instead of to a timer in the past.
        self._batch_window_seconds = max(0.0, batch_window_ms / 1000.0)
        #: The event every waiter in the currently-open window is parked on. ``None`` before the
        #: first window; an event that is **set** means the last window has closed and the next
        #: ``load`` opens a fresh one. This one object is the whole mechanism — see
        #: :meth:`_hold_the_batch_open`.
        self._window_closed: Optional[asyncio.Event] = None
        #: The **single** timer that closes the current window. One per window, never one per
        #: waiter — that distinction is the entire fix this class exists to keep.
        self._window_timer: Optional[asyncio.TimerHandle] = None
        #: The instant (on the loop's clock) that timer is set to fire at. Recorded because it
        #: names the shared deadline and is worth having when debugging a window; the timer, not
        #: this number, is what actually closes the window.
        self._window_closes_at: Optional[float] = None

    async def load(self, key: Any) -> Any:  # type: ignore[override]
        """Wait for the batch window (if any), then load ``key`` the way Strawberry does.

        The base method is not a coroutine — it returns the future the batch will resolve — and this
        override is. Both are awaited identically at every call site, and ``load_many`` gathers
        coroutines just as happily as futures, so the substitution is invisible to callers.
        """
        await self._hold_the_batch_open()
        return await super().load(key)

    async def _hold_the_batch_open(self) -> None:
        """Park on the currently-open window; open one if none is open.

        .. rubric:: One timer, one event — and why the batch comes out in arrival order

        Every waiter in a window awaits the **same** :class:`asyncio.Event`, and **one**
        ``loop.call_at`` sets it at the shared deadline. Ten loads arriving over three milliseconds
        therefore resume in a single pass of the event loop and all reach the underlying ``load``
        before the dispatch callback the first of them scheduled gets to run — that is what makes
        them one batch instead of ten.

        The *order* they come out in is a property of that same chain, not luck:

        1. ``Event.wait`` appends one future per waiter to the event's ``_waiters`` deque, in the
           order the waiters arrived.
        2. ``Event.set`` walks that deque **in order** and resolves each future, and each
           resolution appends the waiting task's wake-up to the loop's ready queue — still in
           order.
        3. The loop drains the ready queue in order, and a resumed waiter reaches
           ``super().load(key)`` with **no await in between**, so it adds its key to the batch
           before the next waiter runs at all.

        Hence FIFO, and hence a batch whose contents a test can assert exactly.

        .. rubric:: What this deliberately is not

        The obvious-looking alternative is to compute the shared deadline and then
        ``await asyncio.sleep(deadline - loop.time())`` in each waiter. It batches correctly and it
        **shuffles the batch**, because :func:`asyncio.sleep` re-reads the clock itself: each
        waiter is really scheduled for ``deadline + (its own sub-microsecond gap between the two
        clock reads)``. That is one timer per waiter and N independent wake-up instants, and which
        fires first is a coin flip. It was the implementation here until a test asserting a batch's
        contents in order began failing roughly two runs in three, with the batching itself never
        once at fault. Do not reintroduce it.

        .. rubric:: Windows that have already closed

        A load arriving after its window closed opens a **fresh** one rather than joining a closed
        window whose batch may already have been handed to the load function — so the window is a
        repeating quantum rather than a one-off delay, and a late arrival waits rather than hangs.
        A load arriving in the sliver between the deadline instant and the loop iteration that
        fires the timer joins the closing window and wakes with it: at most one iteration of extra
        generosity, and it errs towards wider batches.
        """
        if self._batch_window_seconds <= 0:
            # Zero means "next event-loop tick". Returning here rather than scheduling a
            # `call_at(now, ...)` is what keeps the shipped default costing nothing at all: no
            # timer, no event, no extra iteration on the way to Strawberry's own behaviour.
            return

        window = self._window_closed
        if window is None or window.is_set():
            window = self._open_a_window()

        # Deliberately the local, never `self._window_closed`: by the time this waiter resumes, a
        # later load may already have replaced that attribute with the *next* window's event, and
        # this waiter belongs to the window it arrived in.
        await window.wait()

    def _open_a_window(self) -> asyncio.Event:
        """Start a window and return the event its waiters park on.

        Cancellation-safe by construction, which is the reason the timer lives on the loader rather
        than on the waiter that happened to open the window: cancelling any waiter — the first one
        included — removes only its own future from the event's queue (``Event.wait`` does that in
        a ``finally``), leaves the one timer scheduled, and leaves its batch-mates to wake on time.
        There is no per-waiter state left behind to dangle and no way for one cancellation to wedge
        a batch.
        """
        loop = asyncio.get_running_loop()
        window = asyncio.Event()
        closes_at = loop.time() + self._batch_window_seconds

        # Scheduled BEFORE it is published: if `call_at` were to raise — a loop closing under a
        # request, say — the loader is left holding its previous, already-closed window rather than
        # a live window that nothing will ever close and that every subsequent load would hang on.
        timer = loop.call_at(closes_at, window.set)

        if self._window_timer is not None:
            # Normally a no-op: the previous window is closed by now, so its handle has already
            # fired. It is here for the case where that is not true — an event closed by something
            # other than its own timer leaves a live handle with nothing left to do.
            self._window_timer.cancel()

        self._window_timer = timer
        self._window_closes_at = closes_at
        self._window_closed = window
        return window


# =================================================================================================
# The registry
# =================================================================================================


class LoaderRegistry:
    """The loaders for **one** GraphQL operation.

    One instance per operation, minted by :class:`src.graphql.context.PerOperationResources` in its
    ``on_operation`` hook and reachable from any resolver as ``info.context.loaders``. Its lifetime
    is the point rather than an implementation detail: every loader in here caches by key, so two
    operations sharing a registry would mean the second one serving whatever the first one happened
    to read. On the WebSocket transport, where one context object serves a socket for hours, that is
    a client watching a database that stopped changing.

    Attributes:
        logs_by_trace_id: ``trace_id -> list[LogEntry]`` — every entry carrying that correlation
            id, newest first, **including** the entry the caller is resolving from.
            ``LogEntry.related_logs`` is what excludes self; the loader deliberately does not,
            because its result is shared by every parent in the group and each of them excludes a
            different row.
        log_by_id: ``id -> LogEntry | None`` — behind ``Query.log(id:)``, so a document selecting
            several entries by id costs one statement rather than one per alias.
    """

    def __init__(
        self,
        repository: RepositoryProvider,
        settings: Settings,
        *,
        batch_window_ms: Optional[int] = None,
    ) -> None:
        """Build the loaders.

        **Call this from inside the running event loop** (which ``on_operation`` is): Strawberry's
        ``DataLoader`` binds the loop it will create futures on when it is constructed, so a
        registry built outside one would hand resolvers futures belonging to a different loop.

        Args:
            repository: How to get a repository for one batch. See :data:`RepositoryProvider`.
            settings: Supplies ``MAX_QUERY_LIMIT`` (the per-group cap) and the default batch
                window. Carried rather than read from :func:`src.config.get_settings` so a test can
                run an operation under deliberately different limits.
            batch_window_ms: Overrides ``settings.dataloader_batch_window_ms``. Only tests pass it;
                production always takes the configured value.
        """
        self._repository = repository
        self._settings = settings
        window = (
            settings.dataloader_batch_window_ms if batch_window_ms is None else batch_window_ms
        )
        self._batch_window_ms = max(0, int(window))

        self.logs_by_trace_id: DataLoader = WindowedDataLoader(
            load_fn=self.load_logs_by_trace_id,
            batch_window_ms=self._batch_window_ms,
        )
        self.log_by_id: DataLoader = WindowedDataLoader(
            load_fn=self.load_log_by_id,
            batch_window_ms=self._batch_window_ms,
        )

    @classmethod
    def from_session(
        cls,
        session: AsyncSession,
        settings: Settings,
        *,
        batch_window_ms: Optional[int] = None,
    ) -> LoaderRegistry:
        """Build a registry whose every batch runs on ``session``.

        The direct form, for callers that already own a session and its lifetime — tests, and any
        future code path that is not a GraphQL operation. The operation path goes through
        :meth:`src.graphql.context.Context.repository` instead, which is what applies the
        "subscriptions get no long-lived session" rule.
        """

        @asynccontextmanager
        async def provider() -> AsyncIterator[LogRepository]:
            yield LogRepository(session, settings)

        return cls(provider, settings, batch_window_ms=batch_window_ms)

    @property
    def batch_window_ms(self) -> int:
        """The window the loaders were built with, in milliseconds. ``0`` means next-tick."""
        return self._batch_window_ms

    async def load_logs_by_trace_id(self, keys: list[str]) -> list[list[LogEntry]]:
        """Load every entry for each trace id in ``keys`` — **one statement for the whole batch**.

        Returns:
            One list per key, positionally aligned to ``keys``; ``[]`` for a trace id with no
            entries. See this module's docstring for why that alignment is the contract.
        """
        async with self._repository() as repository:
            rows = await repository.list_logs_by_trace_ids(keys)
            # Projected INSIDE the block, like every other read path: the objects are built while
            # their rows are still attached to a live session rather than relying on
            # `expire_on_commit=False` to keep detached instances readable.
            return group_logs_by_trace_id(
                rows, keys, max_per_key=self._settings.max_query_limit
            )

    async def load_log_by_id(self, keys: list[int]) -> list[Optional[LogEntry]]:
        """Load each entry named in ``keys`` — **one statement for the whole batch**.

        Returns:
            One entry (or ``None``) per key, positionally aligned to ``keys``.
        """
        async with self._repository() as repository:
            rows = await repository.get_logs_by_ids(keys)
            return align_logs_by_id(rows, keys)


__all__ = [
    "LoaderRegistry",
    "RepositoryProvider",
    "WindowedDataLoader",
    "align_logs_by_id",
    "group_logs_by_trace_id",
]
