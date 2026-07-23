"""Aggregate statistics over the corpus — the computation behind ``GET /api/v1/stats``.

One public function, :func:`compute_stats`, and two ceilings that keep it honest under a
hostile corpus. Everything here is a *read*: nothing in this module mutates the store, and
nothing in the store knows this module exists.

.. rubric:: Why the stats are computed on demand rather than maintained incrementally

The tempting design is a set of counters the store bumps on every append — O(1) reads, no scan.
It is the wrong design here, and the reason is one sentence: **an incremental aggregate is
inherently unfiltered.** ``by_level`` maintained at append time answers "how many ERRORs are in
the store", which is a different question from "how many ERRORs match
``?service=auth-svc&since=…``" — the only question a dashboard actually asks. The moment a
filter appears, the incremental path cannot answer it, so a second code path has to be written
that scans anyway. Two implementations of "count the matching entries" is exactly the situation
where ``/stats`` and ``/logs`` quietly start disagreeing: one rounds a boundary differently, one
forgets that ``until`` is inclusive, and the dashboard shows 1,204 next to a table that
paginates through 1,203 rows. Nobody notices for months.

So: one pass, on demand, over :meth:`~src.store.LogStore.iter_matching` — the *same* iterator,
with the *same* filter object, that :func:`~src.api.v1._paginate` walks for a page. That makes

    ``StatsSnapshot.total == LogPage.page.total``

true **by construction** rather than by two implementations happening to agree, which is the
guarantee the README sells ("stats for this search and results for this search are guaranteed to
describe the same set") and the property C12's E2E verifier asserts over HTTP.

The cost is a full walk of the match set per request. At ``STORE_CAPACITY=100000`` that is a
few milliseconds of tuple comparisons, it is bounded by the ring's capacity rather than by
uptime, and it is metered by the same token bucket as every other read. The alternative buys
microseconds and sells the one property the endpoint exists to have.

.. rubric:: The one deliberately filter-independent facet

:class:`~src.models.IngestStats` describes **the store, not the query**. "How fast are logs
arriving" and "how many entries have ever been appended" are properties of the process; there is
no coherent way to filter them (an entry that was evicted an hour ago cannot be tested against
``?service=auth-svc`` — it is gone). Reporting them unfiltered is the honest option, and
``test_ingest_is_filter_independent`` pins it so a future refactor cannot quietly make ``ingest``
depend on the filter and turn a store-health panel into a query-scoped one.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from src.models import (
    ERROR_LEVELS,
    BucketPoint,
    IngestStats,
    SortOrder,
    StatsSnapshot,
    StatsWindow,
    TopMessage,
)

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from src.store import CompiledFilter, Filter, LogStore

#: Hard ceiling on how many points the histogram may return. A client is free to ask for
#: ``bucket_sec=1`` over a corpus spanning a week; honouring that literally would mean 604,800
#: zero-filled points — several megabytes of JSON that no chart can draw and that would make one
#: cheap-looking request the most expensive thing the process does. Past this ceiling the series
#: is **folded** (see :func:`_fold_factor`) rather than truncated: truncation would silently drop
#: the tail of the window, which is a lie about the data; folding lowers the resolution and
#: *says so* by echoing the effective width back in :attr:`~src.models.StatsWindow.bucket_sec`.
MAX_STATS_BUCKETS = 1000

#: Ceiling on the number of distinct messages the ``top_errors`` counter will ever hold.
#:
#: The counter is keyed by raw message text, which is client-supplied. A corpus in which every
#: ERROR carries a unique message — an id or a timestamp interpolated into the string, which is
#: exactly what a badly-instrumented service does — would make the counter as large as the error
#: subset itself. That is still bounded by the ring, but it is an allocation proportional to the
#: corpus on *every* ``/stats`` request, which is a different and worse thing than a bounded
#: store. Past the cap, messages already being counted keep incrementing (so the ranking of the
#: frequent ones, which is the entire point, stays exact) and new keys are simply not admitted.
MAX_TOP_ERROR_KEYS = 50_000

#: How many error messages a snapshot reports when the caller expresses no preference.
DEFAULT_TOP_N = 10


def _fold_factor(bucket_count: int, *, maximum: int = MAX_STATS_BUCKETS) -> int:
    """Smallest integer ``k`` such that folding ``bucket_count`` buckets ``k``-to-1 fits.

    Integer, not fractional: a fold factor of 2.5 would produce buckets of unequal width, and a
    histogram whose bars cover different spans is not a histogram. ``k = ceil(n / maximum)``
    always suffices — after folding, the point count is at most
    ``floor((n - 1) / k) + 1 <= maximum``, since ``k * maximum >= n``.
    """
    if bucket_count <= maximum or maximum < 1:
        return 1
    return -(-bucket_count // maximum)  # ceil division, without importing math for one line


def compute_stats(
    store: LogStore,
    flt: Filter | CompiledFilter,
    *,
    bucket_sec: int,
    top_n: int = DEFAULT_TOP_N,
    now: float,
) -> StatsSnapshot:
    """Aggregate the entries matching ``flt`` into one snapshot, in a single pass.

    Args:
        store: The log store. Only :meth:`~src.store.LogStore.iter_matching` and the four
            store-level accessors behind :class:`~src.models.IngestStats` are used, so any object
            exposing them works — the same duck-typed seam :meth:`~src.store.LogStore.scan` uses.
        flt: The predicate. A flat :class:`~src.store.Filter` from the query string, or a
            :class:`~src.store.CompiledFilter` from a boolean tree. The store cannot tell them
            apart and neither can this function, which is why ``/stats`` and ``POST
            /logs/search`` describe the same set for equivalent inputs.
        bucket_sec: Requested histogram resolution, in seconds. Values below 1 are clamped to 1
            rather than rejected: this function is also called from tests and scripts, and a
            ``ZeroDivisionError`` deep in an aggregation is a worse answer than the finest
            resolution that means anything.
        top_n: How many error messages to rank. ``0`` (or less) returns an empty list.
        now: Wall-clock POSIX timestamp, **injected**. Never read from the clock inside, so a
            test can assert on ``window.generated_at`` without racing a real second boundary and
            without the freezegun-shaped machinery that would otherwise be needed.

    Returns:
        A fully-populated :class:`~src.models.StatsSnapshot`. An empty match set is not an error
        and never raises: it returns a well-formed zero snapshot (empty maps, empty lists,
        ``None`` bounds), because an over-narrow filter is a normal thing for a dashboard to
        send and a ``500`` for "nothing matched" would be indefensible.
    """
    width = max(1, int(bucket_sec))

    by_level: Counter[str] = Counter()
    by_service: Counter[str] = Counter()
    error_messages: Counter[str] = Counter()
    raw_buckets: Counter[int] = Counter()

    total = 0
    earliest_epoch: float | None = None
    latest_epoch: float | None = None
    earliest: datetime | None = None
    latest: datetime | None = None

    # THE single pass. Ascending order because the histogram is a time series and reading the
    # corpus oldest-first means `raw_buckets` is populated in key order — irrelevant to
    # correctness, mildly kinder to the dict. Nothing else here depends on the direction.
    for record in store.iter_matching(flt, SortOrder.ASC):
        entry = record.entry
        total += 1
        by_level[entry.level.value] += 1
        by_service[entry.service] += 1

        # `ts_epoch` is precomputed by the store precisely so aggregation and scanning do not
        # re-derive a POSIX timestamp per record. The bounds track the float for the comparison
        # and carry the original `datetime` alongside, so the reported window is the entry's
        # exact instant rather than a float round-tripped back through `fromtimestamp`.
        ts = record.ts_epoch
        if earliest_epoch is None or ts < earliest_epoch:
            earliest_epoch, earliest = ts, entry.ts
        if latest_epoch is None or ts > latest_epoch:
            latest_epoch, latest = ts, entry.ts

        # Floor-aligned to a multiple of the bucket width, NOT to the first entry's timestamp.
        # Alignment to the data would make two adjacent windows over the same corpus produce
        # bars that do not line up, so a dashboard comparing "last hour" with "the hour before"
        # would be comparing differently-phased buckets. Floor division handles pre-epoch
        # timestamps correctly; truncation toward zero would not.
        raw_buckets[int(ts // width) * width] += 1

        if entry.level in ERROR_LEVELS:
            message = entry.message
            # The bound, in one line: an existing key always increments, a new key is admitted
            # only while there is room. See MAX_TOP_ERROR_KEYS for why the ceiling exists.
            if message in error_messages or len(error_messages) < MAX_TOP_ERROR_KEYS:
                error_messages[message] += 1

    buckets, effective_width = _build_buckets(raw_buckets, width)

    # Ties are broken lexicographically, matching `src.generators.expected_counts` exactly.
    # `Counter.most_common` breaks ties by insertion order, which depends on corpus order — so
    # the same multiset of messages arriving in a different order would rank differently, and
    # the oracle the unit tests grade this against would disagree with itself across seeds.
    ranked = sorted(error_messages.items(), key=lambda item: (-item[1], item[0]))
    top_errors = [
        TopMessage(message=message, count=count) for message, count in ranked[: max(0, top_n)]
    ]

    return StatsSnapshot(
        total=total,
        # Plain dicts, not Counters: a Counter serialises identically but invites a reader to
        # assume missing keys read as 0, and the response is JSON where they simply are not
        # there. Only observed keys appear — "no such level" and "level with no entries" are the
        # same statement about a filtered set, and inventing zeros would obscure it.
        by_level=dict(by_level),
        by_service=dict(by_service),
        buckets=buckets,
        top_errors=top_errors,
        window=StatsWindow(
            earliest=earliest,
            latest=latest,
            bucket_sec=effective_width,
            requested_bucket_sec=width,
            generated_at=datetime.fromtimestamp(now, tz=UTC),
        ),
        ingest=_ingest_stats(store),
    )


def empty_snapshot(*, bucket_sec: int, now: float) -> StatsSnapshot:
    """The zero snapshot, for a runtime that has no store at all.

    Distinct from "the filter matched nothing", which :func:`compute_stats` already returns in
    exactly this shape — and deliberately *identical* to it on the wire. A read route on a
    half-wired runtime degrades to an honest empty answer rather than a ``500`` (the convention
    every read handler in ``src/api/v1.py`` follows), and "there is nothing to aggregate" is a
    true statement about a store that does not exist. ``/health`` is where the real state is
    reported; a stats panel does not need to be the thing that discovers it.
    """
    width = max(1, int(bucket_sec))
    return StatsSnapshot(
        total=0,
        by_level={},
        by_service={},
        buckets=[],
        top_errors=[],
        window=StatsWindow(
            earliest=None,
            latest=None,
            bucket_sec=width,
            requested_bucket_sec=width,
            generated_at=datetime.fromtimestamp(now, tz=UTC),
        ),
        ingest=IngestStats(
            entries_total=0, resident=0, capacity=0, evicted=0, per_sec=0.0
        ),
    )


def _build_buckets(raw: Counter[int], width: int) -> tuple[list[BucketPoint], int]:
    """Turn per-bucket tallies into a continuous series, folding if it would be too long.

    Returns ``(points, effective_width)``. The second element is what the response echoes: a
    client that asked for 1-second resolution over a week must be told it received 10-minute
    resolution, or it will label its own axis wrongly and never find out.

    Two properties the caller depends on, in order:

    * **Zero-filled.** Every bucket between the first and the last is present, including the ones
      no entry landed in. A gap in the series is indistinguishable, to a charting library, from
      a bucket it should interpolate across — so an outage (the most interesting thing a log
      dashboard can show) would be drawn as a straight line between the points either side of
      it. An explicit ``count: 0`` draws it as the hole it is.
    * **Folded, never truncated.** Folding is done over the bucket dict, which has at most one
      key per *occupied* bucket, so it is O(#buckets) and never a second pass over the corpus.
    """
    if not raw:
        return [], width

    first, last = min(raw), max(raw)
    span_buckets = (last - first) // width + 1
    factor = _fold_factor(span_buckets)
    effective = width * factor

    if factor > 1:
        folded: Counter[int] = Counter()
        for start, count in raw.items():
            folded[start // effective * effective] += count
        raw, first, last = folded, min(folded), max(folded)

    return (
        [
            BucketPoint(
                bucket_start=datetime.fromtimestamp(start, tz=UTC), count=raw.get(start, 0)
            )
            for start in range(first, last + effective, effective)
        ],
        effective,
    )


def _ingest_stats(store: LogStore) -> IngestStats:
    """Snapshot the store's own throughput and occupancy. **Deliberately unfiltered.**

    See the module docstring: these four numbers describe the process, not the query. They are
    read defensively-shaped but not defensively-wrapped — every one of them is a plain attribute
    read or a bounded arithmetic on the store's own state, and a store that cannot report its own
    size has a problem that a ``0`` here would hide rather than solve.
    """
    return IngestStats(
        entries_total=store.total_appended(),
        resident=store.size(),
        capacity=store.capacity(),
        evicted=store.evicted(),
        per_sec=store.ingest_rate(),
    )
