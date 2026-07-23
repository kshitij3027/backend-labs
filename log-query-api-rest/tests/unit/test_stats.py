"""Unit tests for :mod:`src.stats` — the on-demand aggregation behind ``GET /api/v1/stats``.

Two things are being pinned here, and they are different in kind.

The first is **agreement with an independent implementation**. ``src.generators.expected_counts``
is a deliberately naive brute-force tally that exists for exactly this purpose: it shares no code
with :func:`~src.stats.compute_stats`, reads no index, takes no shortcut, and is documented as
something that must stay slow. Grading the optimised aggregation against it catches the class of
bug that an assertion written by the same hand that wrote the aggregation never catches — because
a test that re-derives the expected number the same way the code derives it is only checking that
the code is self-consistent.

The second is **the properties the histogram has to have to be drawable**: fixed-width buckets,
floor-aligned to the clock rather than to the data, zero-filled across gaps, summing to the
total, and folded (never truncated) past the point ceiling. Each one of those is a separate way a
chart can lie, so each gets its own test.

Nothing here sleeps, and nothing reads the wall clock: ``now`` is a parameter of
:func:`~src.stats.compute_stats` precisely so these tests can be exact rather than tolerant.
"""

from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pytest

from src.generators import expected_counts, generate_entries
from src.models import ERROR_LEVELS, LogEntry, LogQuery, SortOrder
from src.stats import (
    DEFAULT_TOP_N,
    MAX_STATS_BUCKETS,
    MAX_TOP_ERROR_KEYS,
    compute_stats,
    empty_snapshot,
)
from src.store import Filter, LogStore

#: Anchor for hand-built corpora. Fixed and — this matters for the alignment test — deliberately
#: **not** on a minute boundary, so a bucket start that merely echoed the first entry's timestamp
#: would be visibly different from one that floor-aligned to the clock.
BASE_TS = datetime(2026, 7, 27, 10, 0, 7, tzinfo=UTC)

#: An arbitrary but fixed "current time" for snapshots whose `generated_at` is not under test.
NOW = datetime(2026, 7, 27, 11, 0, 0, tzinfo=UTC).timestamp()

#: Corpus size for the oracle comparisons. Large enough that every level and service is
#: represented several times over; small enough that a brute-force tally is instant.
CORPUS_SIZE = 500

#: Seed for the oracle corpus. Not `DEFAULT_SEED`: reusing the production seed would make a test
#: that accidentally depends on one specific corpus look like it depends on "the" corpus.
CORPUS_SEED = 20260723


def make_entry(
    i: int,
    *,
    level: str = "INFO",
    service: str = "auth-svc",
    host: str = "node-1",
    message: str | None = None,
    ts: datetime | None = None,
) -> LogEntry:
    """One deterministic entry. Timestamps march one second per index from :data:`BASE_TS`."""
    return LogEntry(
        id=f"e{i:06d}",
        ts=ts if ts is not None else BASE_TS + timedelta(seconds=i),
        level=level,
        service=service,
        host=host,
        message=message if message is not None else f"event {i}",
    )


def filled(entries: list[LogEntry], *, capacity: int = 100_000) -> LogStore:
    """A store holding exactly ``entries``, appended in order."""
    store = LogStore(capacity=capacity)
    store.append_many(entries)
    return store


class StoppableClock:
    """A clock that moves only when told to. Injected via ``LogStore(time_func=…)``.

    :meth:`~src.store.LogStore.ingest_rate` reads its clock **at call time** and divides by the
    elapsed span, so under the real clock two reads microseconds apart legitimately return
    different rates — there is no tolerance-free way to assert "these two calls agree" against a
    running clock. Stopping the clock removes the variable instead of accommodating it, which is
    what lets ``test_ingest_is_filter_independent`` assert the actual invariant (identical inputs
    produce identical outputs) rather than a loosened version of it.

    Distinct from ``tests/unit/test_store.py``'s ``FakeClock``, which advances a fixed step per
    *read*: that shape is right for exercising the estimator, and exactly wrong here, where two
    reads must see the same instant.
    """

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def bucket_starts(snapshot) -> list[datetime]:
    """The histogram's bucket start instants, in order."""
    return [point.bucket_start for point in snapshot.buckets]


@pytest.fixture()
def oracle_corpus() -> list[LogEntry]:
    """A generated corpus, and the thing :func:`~src.generators.expected_counts` is tallied over."""
    return generate_entries(CORPUS_SIZE, seed=CORPUS_SEED)


@pytest.fixture()
def oracle_store(oracle_corpus: list[LogEntry]) -> LogStore:
    """A store holding exactly ``oracle_corpus``, with room to spare so nothing is evicted."""
    return filled(oracle_corpus)


# ---------------------------------------------------------------------------------------------
# Agreement with the generator's brute-force oracle
# ---------------------------------------------------------------------------------------------


def test_by_level_counts_match_generator_oracle(oracle_store, oracle_corpus):
    """The optimised tally equals the naive one, level for level.

    ``expected_counts`` is graded against here rather than against a dict typed into the test,
    which is the whole reason it exists and is documented as "must stay naive". A hardcoded
    expectation would have to be rewritten whenever the seed or size changed, and a rewritten
    expectation is one that gets *adjusted until it passes* — which is precisely the failure mode
    an oracle is supposed to make impossible.
    """
    oracle = expected_counts(oracle_corpus)

    snapshot = compute_stats(oracle_store, Filter(), bucket_sec=60, now=NOW)

    assert snapshot.by_level == oracle.by_level
    assert snapshot.total == oracle.total
    # Observed keys only, on both sides: a level nobody logged is absent, not present with a 0.
    assert set(snapshot.by_level) <= {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}


def test_by_service_counts_match_generator_oracle(oracle_store, oracle_corpus):
    """Same comparison, other dimension — and the window bounds while we are here.

    ``earliest``/``latest`` are checked against the oracle too because they are computed in the
    same pass off ``ts_epoch`` (a float) while the oracle compares ``datetime`` objects directly.
    If the float round-trip ever lost precision, this is where it would surface.
    """
    oracle = expected_counts(oracle_corpus)

    snapshot = compute_stats(oracle_store, Filter(), bucket_sec=60, now=NOW)

    assert snapshot.by_service == oracle.by_service
    assert snapshot.window.earliest == oracle.earliest
    assert snapshot.window.latest == oracle.latest


def test_top_errors_match_generator_oracle(oracle_store, oracle_corpus):
    """The ranking, including its tie-break, agrees with the oracle's.

    Both sides sort by ``(-count, message)``. The tie-break is not cosmetic: ``Counter.
    most_common`` breaks ties by insertion order, so without it the same multiset of messages
    arriving in a different order would rank differently and the two implementations would
    disagree for reasons that have nothing to do with counting.
    """
    oracle = expected_counts(oracle_corpus, top_n=DEFAULT_TOP_N)

    snapshot = compute_stats(oracle_store, Filter(), bucket_sec=60, now=NOW)

    assert [(m.message, m.count) for m in snapshot.top_errors] == oracle.top_error_messages


def test_total_equals_store_count_for_same_filter(oracle_store):
    """``total`` is the number ``store.count`` reports — the equality ``page.total`` rides on.

    This is the unit-level half of the headline guarantee. The route half
    (``/stats`` total == ``/logs`` ``page.total``) is asserted over HTTP in
    ``tests/integration/test_stats_api.py``; here it is asserted against the store primitive that
    both of them ultimately call, for several filters including one that matches nothing.
    """
    filters = [
        Filter(),
        Filter.from_query(LogQuery(level=["ERROR"])),
        Filter.from_query(LogQuery(level=["ERROR", "FATAL"])),
        Filter.from_query(LogQuery(q="the")),
        Filter.from_query(LogQuery(service=["no-such-service"])),
    ]

    for flt in filters:
        snapshot = compute_stats(oracle_store, flt, bucket_sec=60, now=NOW)
        assert snapshot.total == oracle_store.count(flt)


def test_stats_respect_filters(oracle_store, oracle_corpus):
    """A filtered snapshot is a strict, correct narrowing of the unfiltered one.

    "Disagrees in the expected direction" is the assertion, spelled out three ways: fewer
    entries, exactly one surviving level key, and that key's count unchanged from the
    unfiltered tally — because filtering must remove entries, never re-count the ones it keeps.
    """
    unfiltered = compute_stats(oracle_store, Filter(), bucket_sec=60, now=NOW)
    errors_only = Filter.from_query(LogQuery(level=["ERROR"]))

    filtered = compute_stats(oracle_store, errors_only, bucket_sec=60, now=NOW)

    assert filtered.total < unfiltered.total
    assert set(filtered.by_level) == {"ERROR"}
    assert filtered.by_level["ERROR"] == unfiltered.by_level["ERROR"]
    assert sum(filtered.by_service.values()) == filtered.total


# ---------------------------------------------------------------------------------------------
# The histogram
# ---------------------------------------------------------------------------------------------


def test_buckets_are_bucket_sec_wide():
    """Consecutive bucket starts are exactly ``bucket_sec`` apart. Every pair, not just the ends.

    A histogram whose bars cover different spans is not a histogram — the height of a bar is only
    comparable to its neighbour if they measure the same amount of time.
    """
    store = filled([make_entry(i) for i in range(300)])

    snapshot = compute_stats(store, Filter(), bucket_sec=60, now=NOW)

    starts = bucket_starts(snapshot)
    assert len(starts) > 1
    assert {(b - a).total_seconds() for a, b in pairwise(starts)} == {60.0}


def test_buckets_are_floor_aligned():
    """Bucket starts sit on multiples of ``bucket_sec``, not on the first entry's timestamp.

    :data:`BASE_TS` is deliberately at ``10:00:07``, so an implementation that anchored the
    series to the data would produce ``10:00:07, 10:01:07, …`` and fail here. Alignment to the
    clock is what lets two windows over the same corpus be compared bar-for-bar; alignment to the
    data makes every window a different phase.
    """
    store = filled([make_entry(i) for i in range(150)])

    snapshot = compute_stats(store, Filter(), bucket_sec=60, now=NOW)

    starts = bucket_starts(snapshot)
    assert starts[0] == datetime(2026, 7, 27, 10, 0, 0, tzinfo=UTC)
    assert all(int(start.timestamp()) % 60 == 0 for start in starts)
    assert all(start.tzinfo is not None for start in starts)


def test_buckets_are_zero_filled():
    """A gap in the corpus is a bucket with ``count == 0``, not a missing bucket.

    This is the property that makes an outage visible. A charting library handed a series with a
    hole draws a straight line across it, so the most interesting five minutes a log dashboard
    can show — the five where nothing was logged — would render as a smooth interpolation between
    the points either side. An explicit zero draws it as the hole it is.
    """
    early = [make_entry(i, ts=BASE_TS + timedelta(seconds=i)) for i in range(5)]
    # Nothing at all for the three minutes in between.
    late = [make_entry(100 + i, ts=BASE_TS + timedelta(minutes=4, seconds=i)) for i in range(5)]

    snapshot = compute_stats(filled(early + late), Filter(), bucket_sec=60, now=NOW)

    counts = [point.count for point in snapshot.buckets]
    assert len(counts) == 5, bucket_starts(snapshot)
    assert counts == [5, 0, 0, 0, 5]


def test_bucket_counts_sum_to_total():
    """Every matching entry lands in exactly one bucket — no double counting, no drops."""
    entries = [
        make_entry(i, level="ERROR" if i % 7 == 0 else "INFO", ts=BASE_TS + timedelta(seconds=i))
        for i in range(400)
    ]
    store = filled(entries)

    for bucket_sec in (1, 15, 60, 3600):
        snapshot = compute_stats(store, Filter(), bucket_sec=bucket_sec, now=NOW)
        assert sum(point.count for point in snapshot.buckets) == snapshot.total == 400


def test_buckets_fold_past_max_and_report_effective_bucket_sec():
    """Too many points folds the series to a coarser width — and says so in the response.

    Two entries a full day apart at 1-second resolution is 86,400 points: several megabytes of
    JSON no chart can draw, from a request that costs the client nothing to make. Folding is the
    answer rather than truncation because truncation would drop the tail of the window, which is
    a lie about the data; folding lowers the resolution and reports the resolution it lowered to,
    so a client labels its own axis correctly.
    """
    span = [
        make_entry(0, ts=BASE_TS),
        make_entry(1, ts=BASE_TS + timedelta(days=1)),
    ]

    snapshot = compute_stats(filled(span), Filter(), bucket_sec=1, now=NOW)

    assert snapshot.window.requested_bucket_sec == 1
    assert snapshot.window.bucket_sec > 1, "a day at 1s resolution must have been folded"
    assert len(snapshot.buckets) <= MAX_STATS_BUCKETS
    # Folding redistributes, it never discards: the counts still account for every entry, and the
    # window still covers both ends of the match set.
    assert sum(point.count for point in snapshot.buckets) == snapshot.total == 2
    starts = bucket_starts(snapshot)
    assert starts[0] <= snapshot.window.earliest
    assert starts[-1] <= snapshot.window.latest
    # The effective width is an integer multiple of the requested one, so the bars stay equal.
    assert snapshot.window.bucket_sec % snapshot.window.requested_bucket_sec == 0
    gaps = {(b - a).total_seconds() for a, b in pairwise(starts)}
    assert gaps == {float(snapshot.window.bucket_sec)}


def test_unfolded_series_echoes_the_requested_bucket_sec():
    """The common case: nothing folded, so both widths are the value that was asked for."""
    store = filled([make_entry(i) for i in range(120)])

    snapshot = compute_stats(store, Filter(), bucket_sec=30, now=NOW)

    assert snapshot.window.bucket_sec == snapshot.window.requested_bucket_sec == 30


def test_non_positive_bucket_sec_is_clamped_not_a_crash():
    """``bucket_sec=0`` clamps to 1 rather than dividing by zero.

    The route rejects it at the edge with ``ge=1``, so this only ever fires for a direct caller —
    a script or a test. A ``ZeroDivisionError`` surfacing from inside an aggregation is a worse
    answer than the finest resolution that means anything.
    """
    store = filled([make_entry(i) for i in range(10)])

    snapshot = compute_stats(store, Filter(), bucket_sec=0, now=NOW)

    assert snapshot.window.bucket_sec == 1
    assert sum(point.count for point in snapshot.buckets) == 10


# ---------------------------------------------------------------------------------------------
# top_errors
# ---------------------------------------------------------------------------------------------


def test_top_errors_ranked_by_frequency():
    """Most frequent first. The ranking is the feature; the counts are how it is derived."""
    entries = (
        [make_entry(i, level="ERROR", message="disk full") for i in range(5)]
        + [make_entry(100 + i, level="ERROR", message="timeout") for i in range(9)]
        + [make_entry(200 + i, level="FATAL", message="oom") for i in range(2)]
    )

    snapshot = compute_stats(filled(entries), Filter(), bucket_sec=60, now=NOW)

    assert [(m.message, m.count) for m in snapshot.top_errors] == [
        ("timeout", 9),
        ("disk full", 5),
        ("oom", 2),
    ]


def test_top_errors_respects_top_n():
    """``top_n`` bounds the list, and it keeps the *top* n rather than the first n seen."""
    entries = [
        make_entry(i, level="ERROR", message=f"failure {i % 12}") for i in range(120)
    ]
    # Make one message unambiguously the most frequent, so "kept the right ones" is checkable.
    entries += [make_entry(500 + i, level="ERROR", message="failure 11") for i in range(50)]

    snapshot = compute_stats(filled(entries), Filter(), bucket_sec=60, now=NOW, top_n=3)

    assert len(snapshot.top_errors) == 3
    assert snapshot.top_errors[0].message == "failure 11"
    counts = [m.count for m in snapshot.top_errors]
    assert counts == sorted(counts, reverse=True)


def test_top_n_zero_returns_no_errors():
    """``top_n=0`` is an empty list, not the default — a caller asking for nothing gets nothing."""
    entries = [make_entry(i, level="ERROR", message="boom") for i in range(5)]

    snapshot = compute_stats(filled(entries), Filter(), bucket_sec=60, now=NOW, top_n=0)

    assert snapshot.top_errors == []
    assert snapshot.by_level == {"ERROR": 5}


def test_top_errors_only_counts_error_levels():
    """Only ERROR and FATAL contribute — and the definition comes from one place.

    :data:`~src.models.ERROR_LEVELS` is imported and asserted against rather than re-spelled as a
    literal, so "what counts as an error" cannot drift between the stats panel and the search
    results it is meant to summarise.
    """
    noisy = "the same message everywhere"
    entries = [
        make_entry(0, level="DEBUG", message=noisy),
        make_entry(1, level="INFO", message=noisy),
        make_entry(2, level="WARN", message=noisy),
        make_entry(3, level="ERROR", message=noisy),
        make_entry(4, level="FATAL", message=noisy),
    ]

    snapshot = compute_stats(filled(entries), Filter(), bucket_sec=60, now=NOW)

    assert {level.value for level in ERROR_LEVELS} == {"ERROR", "FATAL"}
    assert [(m.message, m.count) for m in snapshot.top_errors] == [(noisy, 2)]
    assert snapshot.total == 5, "the non-error entries are still counted everywhere else"


def test_top_errors_tie_break_is_deterministic():
    """Equal counts rank lexicographically, so the ranking is a total order.

    Appended in reverse alphabetical order precisely so an implementation relying on
    ``Counter.most_common``'s insertion-order tie-break would produce the opposite list.
    """
    entries = [
        make_entry(i, level="ERROR", message=message)
        for i, message in enumerate(["charlie", "bravo", "alpha"])
    ]

    snapshot = compute_stats(filled(entries), Filter(), bucket_sec=60, now=NOW)

    assert [m.message for m in snapshot.top_errors] == ["alpha", "bravo", "charlie"]


def test_top_error_keys_are_bounded(monkeypatch):
    """A corpus of all-unique error messages cannot grow the counter without limit.

    The pathological input is not hypothetical: a service that interpolates a request id into its
    error string produces exactly this, and the resulting counter would be as large as the error
    subset itself — allocated fresh on *every* ``/stats`` request, which is a different and worse
    thing than a bounded store.

    The cap is lowered for the test rather than fed 50,000 entries. That is not a shortcut around
    the assertion: what is under test is the admission rule, and a small cap makes the bound
    directly *observable* (ask for far more rows than the cap and count what comes back) instead
    of inferable. The production constant is asserted separately, below.
    """
    monkeypatch.setattr("src.stats.MAX_TOP_ERROR_KEYS", 5)
    entries = [make_entry(i, level="ERROR", message=f"unique failure {i}") for i in range(60)]
    # A repeat of a message admitted long before the cap: it must keep incrementing, because the
    # ranking exists for the frequent messages and those are exactly the ones already in the map.
    entries += [make_entry(900 + i, level="ERROR", message="unique failure 0") for i in range(4)]

    snapshot = compute_stats(filled(entries), Filter(), bucket_sec=60, now=NOW, top_n=100)

    assert snapshot.total == 64, "every entry is still counted everywhere else"
    assert len(snapshot.top_errors) <= 5, [m.message for m in snapshot.top_errors]
    assert snapshot.top_errors[0].message == "unique failure 0"
    assert snapshot.top_errors[0].count == 5


def test_top_error_key_cap_is_bounded_and_positive():
    """The shipped ceiling is a real bound: finite, positive, and below the store's capacity.

    Pinned as its own assertion because :func:`test_top_error_keys_are_bounded` lowers the
    constant — a cap that was accidentally set to zero (no messages ever ranked) or to a
    billion (no bound at all) would leave that test green.
    """
    assert 0 < MAX_TOP_ERROR_KEYS <= 1_000_000


# ---------------------------------------------------------------------------------------------
# Degenerate sets, ingest, and the injected clock
# ---------------------------------------------------------------------------------------------


def test_empty_result_set_returns_zeroed_snapshot():
    """An over-narrow filter is a normal answer, not an error. Zeros all the way down.

    ``/stats`` must never ``500`` because nothing matched: a dashboard whose filter bar can
    produce an empty set — which is every dashboard — would otherwise show a stack trace as a
    routine consequence of typing.
    """
    store = filled([make_entry(i) for i in range(50)])
    matches_nothing = Filter.from_query(LogQuery(service=["no-such-service"]))

    snapshot = compute_stats(store, matches_nothing, bucket_sec=60, now=NOW)

    assert snapshot.total == 0
    assert snapshot.by_level == {}
    assert snapshot.by_service == {}
    assert snapshot.buckets == []
    assert snapshot.top_errors == []
    assert snapshot.window.earliest is None
    assert snapshot.window.latest is None
    # The store still has entries, and `ingest` still says so — it describes the store.
    assert snapshot.ingest.resident == 50


def test_empty_store_returns_zeroed_snapshot():
    """The same shape for an empty ring, so a cold-start dashboard renders instead of failing."""
    snapshot = compute_stats(LogStore(capacity=10), Filter(), bucket_sec=60, now=NOW)

    assert snapshot.total == 0
    assert snapshot.buckets == []
    assert snapshot.ingest.resident == 0
    assert snapshot.ingest.entries_total == 0
    assert snapshot.ingest.per_sec == 0.0


def test_empty_snapshot_matches_the_no_match_shape():
    """The store-less degradation is the same shape as "nothing matched", aggregate for aggregate.

    ``src/api/v1.py`` returns :func:`~src.stats.empty_snapshot` when the runtime has no store, so
    a read against a half-wired process degrades to an honest empty answer instead of a ``500``.
    A client must not have to parse two different "no data" shapes, so every aggregate is
    compared field for field rather than trusted to match.

    ``ingest`` is excluded from that comparison and asserted separately, because it is the one
    block that legitimately differs: a real store reports its capacity even while empty, and a
    runtime with no store has no capacity to report. Zeros there are the true statement.
    """
    from_store = compute_stats(LogStore(capacity=10), Filter(), bucket_sec=60, now=NOW)
    degraded = empty_snapshot(bucket_sec=60, now=NOW)

    assert degraded.model_dump(exclude={"ingest"}) == from_store.model_dump(exclude={"ingest"})
    assert degraded.ingest.model_dump() == {
        "entries_total": 0,
        "resident": 0,
        "capacity": 0,
        "evicted": 0,
        "per_sec": 0.0,
    }


def test_ingest_is_filter_independent():
    """Two different filters over one store produce byte-identical ``ingest`` blocks.

    This is the deliberate asymmetry in the response, and it is worth a test of its own because
    it looks like an inconsistency. Throughput and occupancy describe the *store*; an entry
    evicted an hour ago cannot be tested against ``?service=auth-svc`` because it is gone, so a
    filtered ingest rate would be a smaller number that answers no question anyone asked.

    The store's clock is stopped for the two reads (see :class:`StoppableClock`) but advanced
    *between appends*, so ``per_sec`` is a real non-zero rate rather than the ``0.0`` a fully
    frozen clock would produce. That matters: against ``0.0 == 0.0`` this test would pass even if
    the filter did leak into the rate, which is the one thing it exists to catch.
    """
    clock = StoppableClock()
    store = LogStore(capacity=100_000, time_func=clock)
    for i in range(80):
        store.append(make_entry(i, level="ERROR" if i % 2 else "INFO"))
        # 100 ms of simulated wall time per append: 80 appends over ~8 s, so the estimator has a
        # real interval to divide by. Nothing sleeps — the clock is arithmetic.
        clock.advance(0.1)

    everything = compute_stats(store, Filter(), bucket_sec=60, now=NOW)
    errors = compute_stats(
        store, Filter.from_query(LogQuery(level=["ERROR"])), bucket_sec=60, now=NOW
    )

    assert errors.total < everything.total, "the filter must actually narrow the set"
    assert errors.ingest.model_dump() == everything.ingest.model_dump()
    assert everything.ingest.per_sec > 0, "a rate of 0.0 would make the equality vacuous"
    assert everything.ingest.entries_total == 80
    assert everything.ingest.resident == 80
    assert everything.ingest.capacity == 100_000


def test_ingest_entries_total_includes_evicted():
    """``entries_total`` is monotone and counts through eviction; ``resident`` does not.

    The pair is what makes a memory gate interpretable: a ring at capacity with a high total is a
    healthy busy process, and the same resident count with a total that never moves is a stalled
    ingest. One number cannot say both.
    """
    store = filled([make_entry(i) for i in range(50)], capacity=20)

    snapshot = compute_stats(store, Filter(), bucket_sec=60, now=NOW)

    assert snapshot.ingest.entries_total == 50
    assert snapshot.ingest.resident == 20
    assert snapshot.ingest.evicted == 30
    assert snapshot.total == 20, "aggregates cover the resident set, which is all there is"


def test_now_is_injected_not_wall_clock():
    """``generated_at`` is exactly the injected instant — the function owns no clock.

    An aggregation that read ``time.time()`` internally would be untestable to the second and
    would race a real minute boundary in CI roughly once every sixty runs. The parameter is the
    whole seam: a value decades away from now is reported back verbatim.
    """
    far_future = datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC)
    store = filled([make_entry(i) for i in range(3)])

    snapshot = compute_stats(store, Filter(), bucket_sec=60, now=far_future.timestamp())

    assert snapshot.window.generated_at == far_future
    # And the window bounds still come from the data, not from `now`.
    assert snapshot.window.latest < far_future


def test_iteration_order_does_not_change_the_answer():
    """The pass is over ``SortOrder.ASC``, and the result is order-independent regardless.

    Asserted by comparing against a tally taken from the descending walk of the same store: an
    aggregation that accidentally depended on arrival order (a running min that assumed
    monotonicity, say) would disagree here.
    """
    entries = [make_entry(i, level="ERROR" if i % 3 == 0 else "WARN") for i in range(60)]
    store = filled(entries)

    snapshot = compute_stats(store, Filter(), bucket_sec=60, now=NOW)
    descending = list(store.iter_matching(Filter(), SortOrder.DESC))

    assert snapshot.total == len(descending)
    assert snapshot.window.earliest == min(rec.entry.ts for rec in descending)
    assert snapshot.window.latest == max(rec.entry.ts for rec in descending)
