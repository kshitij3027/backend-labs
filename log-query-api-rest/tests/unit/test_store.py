"""Unit tests for :mod:`src.store` — the ring, the seq spine, the indexes and the scan.

These are the correctness core of the project. Cursor pagination, the SSE event ids, the stats
totals and the `page.total` on every list response are all consequences of the invariants pinned
here; ``tests/unit/test_cursor.py`` builds directly on top of them.

Corpora are constructed **inline** from :class:`~src.models.LogEntry`, deliberately without
importing ``src.generators``. Two reasons: a store test should fail when the *store* is wrong and
never when the corpus generator changes underneath it, and every corpus here is shaped for the
specific property it pins (a known level mix, a known service/host split, timestamps one second
apart so range bounds land on exact records) rather than for statistical realism.

There is not a single ``sleep()`` in this file. The one time-dependent behaviour — the ingest
rate — is exercised through :class:`FakeClock`, injected via ``LogStore(time_func=...)``.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from src.models import LogEntry, LogLevel, LogQuery, SortOrder
from src.store import (
    INDEX_COMPACT_MIN,
    INDEX_HINT_MIN_SELECTIVITY,
    Filter,
    LogStore,
    StoredEntry,
)

#: Anchor for every generated timestamp. Fixed, never ``datetime.now()``: a corpus whose
#: contents depend on when the suite ran cannot pin an inclusive range boundary.
BASE_TS = datetime(2026, 7, 27, 10, 0, 0, tzinfo=UTC)


def make_entry(
    i: int,
    *,
    level: str = "INFO",
    service: str = "auth-svc",
    host: str = "node-1",
    message: str | None = None,
    ts: datetime | None = None,
    entry_id: str | None = None,
) -> LogEntry:
    """Build one deterministic :class:`~src.models.LogEntry`.

    Ids are ``e000000``-style and ordered, so a failing assertion prints something a human can
    place in the corpus immediately. Timestamps march one second per index, which makes
    ``since``/``until`` bounds land on exact records instead of between them.
    """
    return LogEntry(
        id=entry_id if entry_id is not None else f"e{i:06d}",
        ts=ts if ts is not None else BASE_TS + timedelta(seconds=i),
        level=level,
        service=service,
        host=host,
        message=message if message is not None else f"event {i}",
    )


def make_corpus(count: int, **kwargs: object) -> list[LogEntry]:
    """``count`` entries sharing the same shape — the plain background corpus."""
    return [make_entry(i, **kwargs) for i in range(count)]  # type: ignore[arg-type]


def filled(capacity: int, entries: list[LogEntry]) -> LogStore:
    """A store of ``capacity`` with ``entries`` already appended, in order."""
    store = LogStore(capacity=capacity)
    store.append_many(entries)
    return store


def ids_of(records: list[StoredEntry]) -> list[str]:
    """The entry ids of a page, in page order."""
    return [record.entry.id for record in records]


def seqs_of(records: list[StoredEntry]) -> list[int]:
    """The seqs of a page, in page order."""
    return [record.seq for record in records]


class LinearFilter(Filter):
    """The same predicate with the index hint suppressed, forcing the linear scan path.

    Lets a test run one query down *both* strategies and compare, which is the only way to prove
    the index really is an optimisation and not a second, subtly different implementation.
    """

    def index_hint(self, store: LogStore) -> None:  # noqa: ARG002 - signature must match
        return None


def linear_twin(flt: Filter) -> LinearFilter:
    """The hint-free twin of ``flt`` — same predicate, guaranteed linear scan."""
    return LinearFilter(
        levels=flt.levels,
        services=flt.services,
        hosts=flt.hosts,
        since_epoch=flt.since_epoch,
        until_epoch=flt.until_epoch,
        q_lower=flt.q_lower,
    )


class FakeClock:
    """A monotone clock that advances a fixed step per read. No sleeping, exact arithmetic.

    ``start + ticks * step`` rather than ``now += step``, so a hundred reads do not accumulate
    a hundred float rounding errors and the expected ingest rate stays computable by hand.
    """

    def __init__(self, *, start: float = 0.0, step: float = 0.0) -> None:
        self.start = start
        self.step = step
        self.ticks = 0

    def __call__(self) -> float:
        value = self.start + self.ticks * self.step
        self.ticks += 1
        return value


# ---------------------------------------------------------------------------------------------
# seq: the spine
# ---------------------------------------------------------------------------------------------


def test_append_assigns_monotonic_seq() -> None:
    """Seqs start at 0 and increase by exactly one per append."""
    store = LogStore(capacity=100)

    assigned = [store.append(entry).seq for entry in make_corpus(10)]

    assert assigned == list(range(10))
    assert store.next_seq() == 10
    assert store.oldest_seq() == 0
    assert store.newest_seq() == 9


def test_seq_never_reused_after_eviction() -> None:
    """Eviction frees ring slots, never seq values — the single fact everything else rests on.

    If seqs restarted or were recycled after eviction, a cursor anchored at ``seq=3`` would
    silently address a different entry after one full rotation, and a paginated walk would
    return the same rows forever without any error being reachable.
    """
    store = LogStore(capacity=5)

    assigned = [store.append(entry).seq for entry in make_corpus(20)]
    after_eviction = store.append(make_entry(999, entry_id="later")).seq

    assert assigned == list(range(20)), "seq must not restart when the ring wraps"
    assert after_eviction == 20
    assert len({*assigned, after_eviction}) == 21, "no seq may ever be handed out twice"
    assert store.oldest_seq() == 16, "only the newest `capacity` entries stay resident"
    assert store.newest_seq() == 20
    assert store.next_seq() == 21


def test_total_appended_includes_evicted() -> None:
    """``total_appended`` counts every append ever made; ``evicted`` is the difference."""
    store = filled(5, make_corpus(17))

    assert store.total_appended() == 17
    assert store.size() == 5
    assert store.capacity() == 5
    assert store.evicted() == 12
    assert store.evicted() == store.total_appended() - store.size()


def test_append_many_bulk() -> None:
    """The bulk seed path appends every entry, in order, and reports how many."""
    store = LogStore(capacity=100)
    entries = make_corpus(40)

    appended = store.append_many(entries)

    assert appended == 40
    assert store.size() == 40
    assert seqs_of(store.scan(Filter(), SortOrder.ASC, limit=100).items) == list(range(40))
    assert store.append_many([]) == 0, "an empty batch is a no-op, not an error"
    assert store.next_seq() == 40


def test_len_matches_size() -> None:
    """``len(store)`` is the resident count, and tracks *residency* rather than total appends.

    ``GET /health`` probes the store with ``len(store)`` behind a defensive
    ``except (TypeError, ValueError): return 0``. That ``except`` is right for a liveness route
    — it must never fail — but it means a missing ``__len__`` does not raise where anyone can
    see it: the probe just reports ``store_entries: 0`` forever, which looks exactly like an
    empty store. So the container protocol is pinned here, on both sides of eviction.
    """
    store = LogStore(capacity=10)

    store.append_many(make_corpus(6))
    assert len(store) == store.size() == 6

    store.append_many([make_entry(i, entry_id=f"more{i:04d}") for i in range(40)])
    assert len(store) == store.size() == 10, "len follows the ring, not total_appended"
    assert store.total_appended() == 46
    assert len(store) != store.total_appended()


def test_len_is_zero_on_empty_store() -> None:
    """A fresh store has length 0 — and is therefore **falsy**, which callers must not rely on.

    Defining ``__len__`` makes an empty container false in a boolean context. Any defensive read
    of the runtime has to spell the check ``store is None``; ``if not store`` would take the
    degraded path for a perfectly healthy empty ring.
    """
    store = LogStore(capacity=10)

    assert len(store) == 0
    assert store.size() == 0
    assert not store, "an empty store is falsy — hence the `is None` rule at every read site"
    store.append(make_entry(0))
    assert store, "a non-empty store is truthy"


def test_capacity_must_be_positive() -> None:
    """A zero-capacity ring is a silent data sink, so it fails at construction."""
    with pytest.raises(ValueError, match="capacity"):
        LogStore(capacity=0)


# ---------------------------------------------------------------------------------------------
# id lookup and eviction bookkeeping
# ---------------------------------------------------------------------------------------------


def test_get_by_id_returns_entry() -> None:
    """``GET /logs/{id}`` resolves to the very object that was appended, not a copy."""
    entries = make_corpus(20)
    store = filled(100, entries)

    assert store.get(entries[7].id) is entries[7]
    assert store.get(entries[0].id) is entries[0]


def test_get_unknown_id_returns_none() -> None:
    """An unknown or evicted id is ``None`` — C5's 404 — never a raise."""
    entries = make_corpus(20)
    store = filled(5, entries)

    assert store.get("no-such-id") is None
    assert store.get("") is None
    assert store.get(entries[0].id) is None, "an evicted entry is no longer fetchable"
    assert store.get(entries[-1].id) is entries[-1]


def test_ring_evicts_oldest_past_capacity() -> None:
    """Past capacity the ring drops from the head and keeps the newest ``capacity`` entries."""
    entries = make_corpus(12)
    store = filled(5, entries)

    resident = store.scan(Filter(), SortOrder.ASC, limit=100).items

    assert store.size() == 5
    assert seqs_of(resident) == [7, 8, 9, 10, 11]
    assert ids_of(resident) == [entry.id for entry in entries[7:]]
    assert store.oldest_seq() == 7


def test_eviction_prunes_by_id_map() -> None:
    """The id map shrinks with the ring. This is the memory-leak pin.

    ``_by_id`` is a dict keyed by an unbounded id space sitting behind a bounded ring: forgetting
    to prune it is the easiest bug in the whole module to write and the hardest to notice, since
    every functional test still passes while the process grows without limit.
    """
    entries = make_corpus(500)
    store = filled(10, entries)

    assert store.size() == 10
    assert len(store._by_id) == 10, "the id map must not outgrow the ring"
    assert set(store._by_id) == {entry.id for entry in entries[-10:]}
    assert store.get(entries[0].id) is None
    assert store.get(entries[-1].id) is entries[-1]


def test_eviction_prunes_secondary_indexes() -> None:
    """Index lists stay bounded under sustained ingest, and still cover every resident record.

    Pruning is lazy — a head-garbage counter advanced in O(1) per eviction, with the physical
    ``del`` deferred — so the assertion is a *bound*, not "exactly ``size`` entries". The bound
    is the one :func:`src.store._prune_indexes` documents; what matters is that it does not grow
    with the number of appends.
    """
    levels = ["INFO", "ERROR"]
    services = ["auth-svc", "api-svc", "db-svc"]
    hosts = ["node-1", "node-2"]
    entries = [
        make_entry(i, level=levels[i % 2], service=services[i % 3], host=hosts[i % 2])
        for i in range(1000)
    ]
    store = filled(10, entries)

    for dimension in ("level", "service", "host"):
        index = store.index_for(dimension)
        total = sum(len(seqs) for seqs in index.values())
        bound = 2 * store.size() + INDEX_COMPACT_MIN * len(index)
        assert total <= bound, f"{dimension} index exceeded its documented bound"
        assert total < 200, f"{dimension} index grew with appends, not with residency"

    # Bounded is not enough — the surviving entries must still be the *right* ones.
    resident = store.scan(Filter(), SortOrder.ASC, limit=100).items
    for record in resident:
        assert record.seq in store.index_for("level")[record.entry.level.value]
        assert record.seq in store.index_for("service")[record.entry.service]
        assert record.seq in store.index_for("host")[record.entry.host]

    # And a filtered scan must never surface a seq the ring no longer holds.
    filtered = store.scan(Filter(levels=frozenset({"ERROR"})), SortOrder.DESC, limit=50)
    expected = [r.seq for r in reversed(resident) if r.entry.level is LogLevel.ERROR]
    assert seqs_of(filtered.items) == expected


def test_eviction_drops_index_keys_that_go_fully_stale() -> None:
    """A value whose records have all been evicted stops occupying an index key.

    Without this, a store fed by a high-cardinality dimension — think thousands of short-lived
    container hostnames — accumulates one empty list per value it has ever seen: bounded lists,
    unbounded dict.
    """
    store = LogStore(capacity=5)
    store.append_many([make_entry(i, host=f"ephemeral-{i}") for i in range(200)])

    assert len(store.index_for("host")) <= store.size() + 1


# ---------------------------------------------------------------------------------------------
# scan: order
# ---------------------------------------------------------------------------------------------


def test_scan_returns_newest_first_by_default() -> None:
    """DESC is the API's default order, and it means strictly descending seq."""
    entries = make_corpus(30)
    store = filled(100, entries)

    page = store.scan(Filter(), SortOrder.DESC, limit=5)

    assert seqs_of(page.items) == [29, 28, 27, 26, 25]
    assert ids_of(page.items) == [entry.id for entry in reversed(entries[25:])]
    assert page.has_more is True
    assert page.next_seq == 25
    assert page.truncated is False


def test_scan_asc_returns_oldest_first() -> None:
    """ASC is the mirror image: strictly ascending seq from the oldest resident record."""
    entries = make_corpus(30)
    store = filled(100, entries)

    page = store.scan(Filter(), SortOrder.ASC, limit=5)

    assert seqs_of(page.items) == [0, 1, 2, 3, 4]
    assert ids_of(page.items) == [entry.id for entry in entries[:5]]
    assert page.has_more is True
    assert page.next_seq == 4


def test_scan_on_empty_store_is_an_empty_page() -> None:
    """An empty store answers with an empty page, not a raise and not a truncation flag."""
    page = LogStore(capacity=10).scan(Filter(), SortOrder.DESC, limit=50, start_after_seq=7)

    assert page.items == []
    assert page.has_more is False
    assert page.next_seq is None
    assert page.truncated is False


# ---------------------------------------------------------------------------------------------
# scan: filtering
# ---------------------------------------------------------------------------------------------


def test_scan_filters_by_level() -> None:
    """A level filter returns exactly the matching levels, and honours multi-value OR."""
    entries = [
        make_entry(i, level=["DEBUG", "INFO", "WARN", "ERROR", "FATAL"][i % 5]) for i in range(50)
    ]
    store = filled(100, entries)

    errors = store.scan(Filter(levels=frozenset({"ERROR"})), SortOrder.ASC, limit=100)
    severe = store.scan(Filter(levels=frozenset({"ERROR", "FATAL"})), SortOrder.ASC, limit=100)

    assert {r.entry.level for r in errors.items} == {LogLevel.ERROR}
    assert len(errors.items) == 10
    assert {r.entry.level for r in severe.items} == {LogLevel.ERROR, LogLevel.FATAL}
    assert len(severe.items) == 20
    assert seqs_of(severe.items) == sorted(seqs_of(severe.items)), "merged lists stay ordered"


def test_scan_filters_by_service_and_host() -> None:
    """Service and host filters work the same way, including a value nothing carries."""
    services = ["auth-svc", "api-svc"]
    hosts = ["node-1", "node-2", "node-3"]
    entries = [make_entry(i, service=services[i % 2], host=hosts[i % 3]) for i in range(60)]
    store = filled(100, entries)

    by_service = store.scan(Filter(services=frozenset({"auth-svc"})), SortOrder.ASC, limit=100)
    by_host = store.scan(Filter(hosts=frozenset({"node-3"})), SortOrder.ASC, limit=100)
    missing = store.scan(Filter(hosts=frozenset({"node-9"})), SortOrder.ASC, limit=100)

    assert {r.entry.service for r in by_service.items} == {"auth-svc"}
    assert len(by_service.items) == 30
    assert {r.entry.host for r in by_host.items} == {"node-3"}
    assert len(by_host.items) == 20
    assert missing.items == [], "an unknown value matches nothing rather than everything"
    assert missing.has_more is False


def test_scan_filters_are_anded() -> None:
    """Multiple fields intersect. ANDing is the only thing a flat query string can express."""
    entries = [
        make_entry(i, level="ERROR" if i % 2 == 0 else "INFO", service=f"svc-{i % 3}")
        for i in range(60)
    ]
    store = filled(100, entries)
    flt = Filter(levels=frozenset({"ERROR"}), services=frozenset({"svc-0"}))

    page = store.scan(flt, SortOrder.ASC, limit=100)

    assert len(page.items) == 10
    for record in page.items:
        assert record.entry.level is LogLevel.ERROR
        assert record.entry.service == "svc-0"
    # The intersection is strictly smaller than either side on its own.
    only_level = store.scan(Filter(levels=flt.levels), SortOrder.ASC, limit=100)
    only_service = store.scan(Filter(services=flt.services), SortOrder.ASC, limit=100)
    assert len(page.items) < len(only_level.items)
    assert len(page.items) < len(only_service.items)


def test_scan_time_range_is_inclusive() -> None:
    """``since`` and ``until`` are inclusive on **both** ends, matching the model's docstring."""
    entries = make_corpus(20)
    store = filled(100, entries)
    since = entries[3].ts.timestamp()
    until = entries[7].ts.timestamp()

    page = store.scan(
        Filter(since_epoch=since, until_epoch=until), SortOrder.ASC, limit=100
    )

    assert seqs_of(page.items) == [3, 4, 5, 6, 7], "both boundary records must be included"
    only_since = store.scan(Filter(since_epoch=since), SortOrder.ASC, limit=100)
    only_until = store.scan(Filter(until_epoch=until), SortOrder.ASC, limit=100)
    assert seqs_of(only_since.items) == list(range(3, 20))
    assert seqs_of(only_until.items) == list(range(0, 8))


def test_scan_substring_is_case_insensitive() -> None:
    """``q`` matches regardless of case on either side, and is a substring, not a prefix."""
    entries = [
        make_entry(0, message="Invalid Token for user 7"),
        make_entry(1, message="invalid token"),
        make_entry(2, message="connection reset"),
        make_entry(3, message="request had an INVALID TOKEN header"),
    ]
    store = filled(100, entries)

    page = store.scan(Filter(q_lower="invalid token"), SortOrder.ASC, limit=100)

    assert seqs_of(page.items) == [0, 1, 3]
    from_query = Filter.from_query(LogQuery(q="INVALID TOKEN"))
    assert from_query.q_lower == "invalid token", "lower-casing happens once, at build time"
    assert seqs_of(store.scan(from_query, SortOrder.ASC, limit=100).items) == [0, 1, 3]


def test_empty_q_is_not_a_filter() -> None:
    """An empty ``q`` collapses to "unconstrained" — ``"" in anything`` is always true."""
    flt = Filter.from_query(LogQuery(q=""))

    assert flt.q_lower is None
    assert flt.is_empty is True
    assert flt.fingerprint() == Filter().fingerprint()


def test_from_query_translates_the_whole_bundle() -> None:
    """:meth:`Filter.from_query` is the single translation point from wire to store vocabulary."""
    query = LogQuery(
        level=[LogLevel.ERROR, LogLevel.FATAL],
        service=["auth-svc"],
        host=["node-1", "node-2"],
        since=BASE_TS,
        until=BASE_TS + timedelta(minutes=5),
        q="Boom",
    )

    flt = Filter.from_query(query)

    assert flt.levels == frozenset({"ERROR", "FATAL"}), "levels become plain strings"
    assert all(isinstance(level, str) and not isinstance(level, LogLevel) for level in flt.levels)
    assert flt.services == frozenset({"auth-svc"})
    assert flt.hosts == frozenset({"node-1", "node-2"})
    assert flt.since_epoch == BASE_TS.timestamp()
    assert flt.until_epoch == (BASE_TS + timedelta(minutes=5)).timestamp()
    assert flt.q_lower == "boom"
    assert flt.is_empty is False


def test_empty_value_set_matches_nothing() -> None:
    """An *empty* collection is a real constraint, not an absent one.

    ``frozenset()`` means "the level must be one of: nothing", which matches no record. Treating
    it as "unconstrained" — the mistake a truthiness check makes — would turn a query that should
    return zero rows into one that returns the entire corpus.
    """
    store = filled(100, make_corpus(20))

    page = store.scan(Filter(levels=frozenset()), SortOrder.DESC, limit=50)

    assert page.items == []
    assert Filter(levels=frozenset()).is_empty is False
    assert store.count(Filter(levels=frozenset())) == 0


def test_index_hint_picks_the_most_selective_dimension() -> None:
    """The hint is the smallest candidate set among the constrained indexed dimensions."""
    entries = [make_entry(i, level="INFO", service=f"svc-{i % 20}") for i in range(200)]
    store = filled(500, entries)
    flt = Filter(levels=frozenset({"INFO"}), services=frozenset({"svc-3"}))

    hint = flt.index_hint(store)

    assert hint is not None
    assert sum(len(seqs) for seqs in hint) == 10, "service is 20x more selective than level"
    assert Filter().index_hint(store) is None, "an unindexed filter falls back to a linear pass"
    assert Filter(q_lower="x").index_hint(store) is None


def test_hinted_and_linear_scans_agree() -> None:
    """The index is an optimisation, so both strategies must return identical pages.

    An index that can change an answer is not an index, it is a second implementation of the
    filter with its own bugs. Every case below is selective enough that the planner genuinely
    takes the hinted path (asserted), and is then replayed against a hint-free twin — across both
    sort orders, with and without an anchor, including anchors outside the resident range.
    """
    entries = [
        make_entry(
            i,
            level="FATAL" if i % 50 == 0 else "INFO",
            service=f"svc-{i % 8}",
            host=f"node-{i % 3}",
        )
        for i in range(2000)
    ]
    store = filled(5000, entries)
    cases = [
        Filter(levels=frozenset({"FATAL"})),
        Filter(levels=frozenset({"FATAL"}), hosts=frozenset({"node-0"})),
        Filter(services=frozenset({"svc-7"})),
        Filter(levels=frozenset({"FATAL"}), q_lower="event 1"),
        Filter(levels=frozenset({"nope"})),
    ]

    for flt in cases:
        hint = flt.index_hint(store)
        assert hint is not None
        assert sum(len(seqs) for seqs in hint) * INDEX_HINT_MIN_SELECTIVITY <= store.size(), (
            "case is not selective enough to exercise the hinted path"
        )
        twin = linear_twin(flt)
        assert store.count(flt) == store.count(twin), f"count disagreed for {flt}"
        for order in (SortOrder.DESC, SortOrder.ASC):
            for anchor in (None, 1500, 3, 0, 5000):
                hinted = store.scan(flt, order, limit=37, start_after_seq=anchor)
                linear = store.scan(twin, order, limit=37, start_after_seq=anchor)
                assert seqs_of(hinted.items) == seqs_of(linear.items), f"{flt} {order} {anchor}"
                assert hinted.has_more == linear.has_more
                assert hinted.next_seq == linear.next_seq


def test_non_selective_filter_falls_back_to_a_linear_pass() -> None:
    """A hint covering most of the ring is declined — and the answer is unchanged.

    Following an index costs a random access per candidate; reading the ring costs a pointer
    step. Past :data:`~src.store.INDEX_HINT_MIN_SELECTIVITY` the "shortcut" is the slower route,
    so the planner ignores it. That is what keeps the worst case a single linear pass.
    """
    entries = [make_entry(i, level="INFO" if i % 2 else "ERROR") for i in range(400)]
    store = filled(1000, entries)
    flt = Filter(levels=frozenset({"ERROR"}))

    hint = flt.index_hint(store)
    assert hint is not None
    assert sum(len(seqs) for seqs in hint) * INDEX_HINT_MIN_SELECTIVITY > store.size()

    assert store.count(flt) == 200
    assert seqs_of(store.scan(flt, SortOrder.DESC, limit=3).items) == [398, 396, 394]
    assert store.count(flt) == store.count(linear_twin(flt))


# ---------------------------------------------------------------------------------------------
# scan: paging mechanics
# ---------------------------------------------------------------------------------------------


def test_has_more_false_on_exact_boundary_page() -> None:
    """The classic off-by-one: ``len(items) == limit`` does **not** mean another page exists.

    Deciding ``has_more`` by comparing the page length to the limit advertises a next page
    whenever the match count is an exact multiple of the page size. The store instead looks one
    record past the page, so an exactly-full final page is correctly terminal.
    """
    store = filled(100, make_corpus(20))

    exact = store.scan(Filter(), SortOrder.DESC, limit=20)
    one_short = store.scan(Filter(), SortOrder.DESC, limit=19)
    oversized = store.scan(Filter(), SortOrder.DESC, limit=500)

    assert len(exact.items) == 20
    assert exact.has_more is False, "20 matches read 20 at a time is one page, not two"
    assert exact.next_seq is None

    assert len(one_short.items) == 19
    assert one_short.has_more is True
    assert one_short.next_seq == 1

    assert len(oversized.items) == 20
    assert oversized.has_more is False


def test_skip_offset_matches_full_slice() -> None:
    """``skip=N`` is exactly ``full_result[N:]`` — offset paging over the same ordered set."""
    store = filled(1000, make_corpus(50))
    full = store.scan(Filter(), SortOrder.DESC, limit=1000).items
    assert len(full) == 50

    for skip in (0, 1, 7, 40, 45, 50, 60):
        page = store.scan(Filter(), SortOrder.DESC, limit=10, skip=skip)
        assert seqs_of(page.items) == seqs_of(full[skip : skip + 10]), f"skip={skip}"
        assert page.has_more is (len(full) > skip + 10), f"skip={skip}"


def test_skip_counts_matching_records_not_raw_records() -> None:
    """Offset is measured in *matches*, so ``?offset=N`` means the same thing under any filter."""
    entries = [make_entry(i, level="ERROR" if i % 2 == 0 else "INFO") for i in range(40)]
    store = filled(100, entries)
    flt = Filter(levels=frozenset({"ERROR"}))
    matching = store.scan(flt, SortOrder.DESC, limit=100).items

    page = store.scan(flt, SortOrder.DESC, limit=5, skip=5)

    assert seqs_of(page.items) == seqs_of(matching[5:10])


# ---------------------------------------------------------------------------------------------
# count and iter_matching
# ---------------------------------------------------------------------------------------------


def test_count_matches_scan_length() -> None:
    """``count`` is the size of the set a full scan returns — ``page.total`` cannot disagree."""
    entries = [
        make_entry(i, level=["INFO", "ERROR", "WARN"][i % 3], service=f"svc-{i % 4}")
        for i in range(120)
    ]
    store = filled(200, entries)

    for flt in (
        Filter(),
        Filter(levels=frozenset({"ERROR"})),
        Filter(levels=frozenset({"ERROR", "WARN"})),
        Filter(services=frozenset({"svc-1"})),
        Filter(levels=frozenset({"ERROR"}), services=frozenset({"svc-1"})),
        Filter(q_lower="event 1"),
        Filter(since_epoch=entries[50].ts.timestamp()),
        Filter(levels=frozenset({"nope"})),
    ):
        scanned = store.scan(flt, SortOrder.DESC, limit=1000).items
        assert store.count(flt) == len(scanned), f"count disagreed with scan for {flt}"
        assert len(list(store.iter_matching(flt, SortOrder.ASC))) == len(scanned)


def test_count_empty_filter_equals_size() -> None:
    """The unfiltered count short-circuits to the ring's length rather than walking it."""
    store = filled(10, make_corpus(50))

    assert store.count(Filter()) == store.size() == 10
    assert LogStore(capacity=5).count(Filter()) == 0


def test_iter_matching_is_lazy_and_ordered() -> None:
    """``iter_matching`` yields the whole match set in order without materialising a copy."""
    store = filled(1000, make_corpus(100))

    walker = store.iter_matching(Filter(), SortOrder.DESC)
    first_three = [next(walker).seq for _ in range(3)]

    assert first_three == [99, 98, 97]
    assert seqs_of(list(store.iter_matching(Filter(), SortOrder.ASC))) == list(range(100))


# ---------------------------------------------------------------------------------------------
# ingest rate
# ---------------------------------------------------------------------------------------------


def test_ingest_rate_with_injected_clock() -> None:
    """The rate is derived from an injected clock, so it is exact and the suite never sleeps.

    Note that :meth:`LogStore.ingest_rate` reads the clock too, which is why each store here is
    fresh: an interleaved measurement would consume a tick and shift the window it is measuring.
    """
    clock = FakeClock(start=1000.0, step=0.1)
    store = LogStore(capacity=1000, time_func=clock)

    store.append_many(make_corpus(100))  # samples at t = 1000.0 .. 1009.9
    rate = store.ingest_rate()  # measured at t = 1010.0: 100 appends over 10 seconds

    assert rate == pytest.approx(10.0, rel=1e-6)


def test_ingest_rate_is_zero_below_two_samples() -> None:
    """Zero or one append yields ``0.0`` — one sample gives no interval to divide by.

    Inventing a denominator (say, the full window) would report a confident number derived from
    nothing, which on a freshly-started process is exactly when someone is watching it.
    """
    empty = LogStore(capacity=10, time_func=FakeClock(start=5.0, step=1.0))
    single = LogStore(capacity=10, time_func=FakeClock(start=5.0, step=1.0))
    single.append(make_entry(0))

    assert empty.ingest_rate() == 0.0
    assert single.ingest_rate() == 0.0


def test_ingest_rate_only_counts_the_recent_window() -> None:
    """Samples older than the window are dropped, so a long-idle store reports a low rate."""
    clock = FakeClock(start=0.0, step=10.0)
    store = LogStore(capacity=1000, time_func=clock)

    store.append_many(make_corpus(10))  # samples at t = 0, 10, .. 90
    rate = store.ingest_rate()  # measured at t = 100; window keeps t >= 40

    assert rate == pytest.approx(6 / 60, rel=1e-6)


def test_ingest_rate_uses_the_real_clock_by_default() -> None:
    """The default ``time_func`` is wall-clock, and produces a finite non-negative rate.

    No threshold is asserted: a real clock over ten instant appends can report an enormous
    rate, and pinning a number here would make the suite fail on a fast machine. What must hold
    is that the arithmetic never divides by zero or emits a NaN into a stats response.
    """
    store = LogStore(capacity=100)
    store.append_many(make_corpus(10))

    rate = store.ingest_rate()

    assert rate >= 0.0
    assert math.isfinite(rate)
