"""Unit tests for :mod:`src.generators` — the corpus that every other assertion is graded against.

These are not "does it produce plausible logs" tests. The generator is the project's **oracle**:
integration tests compute the expected result of a filter by running that filter over the objects
this module returned, and C12's E2E verifier grades the live API the same way. An oracle that
drifted between two processes would make every downstream assertion unfalsifiable — it would
"pass" by comparing the server to itself, or fail for reasons unrelated to the code under test.

So what is pinned here is the *contract* that makes it an oracle: purity, the vocabulary, the
distribution, and the two structural properties (some rows have no ``trace_id``, some traces are
shared) that make the C5 branches reachable at all.
"""

from __future__ import annotations

import inspect
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest

from src.db.models import LEVEL_MAX_LENGTH, SERVICE_MAX_LENGTH, TRACE_ID_MAX_LENGTH, LogRecord
from src.generators import (
    DEFAULT_WINDOW,
    LEVEL_WEIGHTS,
    LOG_LEVELS,
    METADATA_RATIO,
    ORDER_TRACE_LOG_RATIO,
    SERVICES,
    TRACE_GROUP_MAX,
    TRACE_GROUP_MIN,
    TRACE_ID_RATIO,
    generate_event_corpus,
    generate_log_records,
    order_traces_with_logs,
)

#: A **fixed** instant, deliberately years away from any plausible test-run wall clock. Every
#: assertion below is expressed relative to it, so a generator that quietly consulted
#: :func:`datetime.now` would not merely fail — it would fail by five years, unmistakably.
ANCHOR = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)

#: Corpus size for the statistical assertions. Large enough that a 1%-weighted level is expected
#: ~40 times (so "every level appears" is a statement about the weights rather than about luck),
#: small enough that the whole module runs in well under a second.
SAMPLE_SIZE = 4000

SEED = 20260725

#: Orders for the correlation section, matching the shipped ``SEED_ORDERS`` so the numbers pinned
#: below are the numbers a running container actually produces.
SAMPLE_ORDERS = 200


# --- Purity: the property the entire test strategy rests on ------------------------------------


def test_same_arguments_produce_equal_corpora() -> None:
    """Same ``(count, seed, end_time)`` in, equal corpus out — the oracle guarantee itself.

    Equality here is *value* equality across every field of every record, which is exactly what
    :class:`~src.db.models.LogRecord` being a frozen dataclass buys. Had the generator emitted ORM
    instances this comparison would be identity-based and would pass trivially for two corpora
    that shared nothing but their length.
    """
    first = generate_log_records(500, seed=SEED, end_time=ANCHOR)
    second = generate_log_records(500, seed=SEED, end_time=ANCHOR)

    assert first == second
    assert first is not second, "two calls must build two lists, not memoise one"


def test_global_random_state_does_not_affect_the_corpus() -> None:
    """Reseeding the module-level :mod:`random` between calls changes nothing.

    This is the executable form of "never the global RNG". Any library imported anywhere in the
    process may call :func:`random.seed`; a generator that drew from the shared module state would
    produce a different corpus depending on import order, and the resulting flake would be
    attributed to whatever test happened to run first.
    """
    random.seed(1)
    first = generate_log_records(300, seed=SEED, end_time=ANCHOR)
    random.seed(999_999)
    second = generate_log_records(300, seed=SEED, end_time=ANCHOR)

    assert first == second


def test_end_time_is_a_required_parameter() -> None:
    """``end_time`` has no default, so no code path can fall back to the wall clock.

    Structural rather than behavioural on purpose: "it happens to produce the same corpus twice
    in a row" is weak evidence (two calls a millisecond apart would too). "There is no default, so
    the caller must supply the instant" is a property a future edit cannot break silently.
    """
    parameter = inspect.signature(generate_log_records).parameters["end_time"]

    assert parameter.default is inspect.Parameter.empty, (
        "end_time must stay required — a default would let the generator read the clock, and the "
        "corpus would stop being reproducible between the API process and the E2E verifier"
    )


def test_a_different_seed_produces_a_different_corpus() -> None:
    """The seed actually seeds. Without this, "deterministic" could mean "constant"."""
    first = generate_log_records(300, seed=SEED, end_time=ANCHOR)
    second = generate_log_records(300, seed=SEED + 1, end_time=ANCHOR)

    assert first != second
    assert len(first) == len(second) == 300


def test_a_different_end_time_translates_the_corpus() -> None:
    """Shifting ``end_time`` shifts every timestamp by the same delta and changes nothing else.

    A stronger statement than "the output differs": it says the anchor is a pure translation of
    the time axis, so a caller can move the corpus into the present without perturbing which
    records exist, what they say, or how they are correlated.
    """
    shift = timedelta(days=30)
    baseline = generate_log_records(200, seed=SEED, end_time=ANCHOR)
    shifted = generate_log_records(200, seed=SEED, end_time=ANCHOR + shift)

    assert [record.timestamp + shift for record in baseline] == [
        record.timestamp for record in shifted
    ]
    assert [record.message for record in baseline] == [record.message for record in shifted]
    assert [record.trace_id for record in baseline] == [record.trace_id for record in shifted]


# --- Timestamps --------------------------------------------------------------------------------


def test_timestamps_lie_strictly_inside_the_window() -> None:
    """Every timestamp is in ``[end_time - window, end_time)`` and every one is UTC-aware."""
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)
    floor = ANCHOR - DEFAULT_WINDOW

    for record in records:
        assert record.timestamp.tzinfo is not None, (
            "a naive timestamp would be compared against the timestamptz column under the "
            "server's TimeZone setting, so the same query would select different rows on a "
            "differently-configured database"
        )
        assert record.timestamp.utcoffset() == timedelta(0)
        assert floor <= record.timestamp < ANCHOR


def test_timestamps_are_strictly_ascending() -> None:
    """Oldest first, with no duplicates.

    Seeding inserts in this order, so ``BIGSERIAL`` ids ascend with time — which is what makes the
    ``(timestamp DESC, id DESC)`` tiebreak deterministic against the seeded corpus. A duplicate
    instant would also leave the *oracle's* ordering ambiguous while the database's stayed
    well-defined, so the two could disagree without either being wrong.
    """
    timestamps = [r.timestamp for r in generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)]

    assert timestamps == sorted(timestamps)
    assert len(set(timestamps)) == len(timestamps)


def test_a_custom_window_is_respected() -> None:
    """A narrow window packs the corpus into it rather than ignoring the argument."""
    window = timedelta(minutes=30)
    records = generate_log_records(100, seed=SEED, end_time=ANCHOR, window=window)

    assert all(ANCHOR - window <= r.timestamp < ANCHOR for r in records)
    # And it really is narrower — the spread must be inside the requested window, which the
    # 24-hour default would not satisfy.
    assert records[-1].timestamp - records[0].timestamp < window


def test_naive_end_time_is_interpreted_as_utc() -> None:
    """A naive anchor means UTC — the same rule the repository applies to filter bounds."""
    aware = generate_log_records(50, seed=SEED, end_time=ANCHOR)
    naive = generate_log_records(50, seed=SEED, end_time=ANCHOR.replace(tzinfo=None))

    assert aware == naive


def test_non_utc_end_time_is_converted_not_dropped() -> None:
    """An anchor in another zone names the same instant, so it yields the same corpus."""
    other_zone = ANCHOR.astimezone(timezone(timedelta(hours=5, minutes=30)))
    assert other_zone.tzinfo is not None and other_zone.utcoffset() != timedelta(0)

    assert generate_log_records(50, seed=SEED, end_time=other_zone) == generate_log_records(
        50, seed=SEED, end_time=ANCHOR
    )


# --- Vocabulary and distribution ---------------------------------------------------------------


def test_error_share_sits_in_the_expected_band() -> None:
    """~10% of the corpus is ERROR, which is what makes ``logStats.errorCount`` meaningful.

    ``logStats { totalLogs errorCount services }`` is one of the spec's own verification commands.
    A corpus with a handful of errors makes that number look like noise and a corpus that is a
    third errors makes it look like an outage; either way the field stops demonstrating anything.
    The band is wide enough that it cannot fail on sampling (the seed is fixed, so the count is a
    constant anyway) and narrow enough to catch a mangled weight table.
    """
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)
    share = Counter(r.level for r in records)["ERROR"] / len(records)

    assert 0.06 <= share <= 0.16, f"ERROR share {share:.3f} is outside the corpus's promise"
    assert 0.08 <= LEVEL_WEIGHTS["ERROR"] <= 0.12, "the declared weight drifted out of the band"


def test_every_declared_level_appears() -> None:
    """All five levels are present at a realistic corpus size, including the 1% CRITICAL tail.

    C3's ``LogLevel`` enum is built from ``LOG_LEVELS``, so a level that never occurs is an enum
    member no filter test can exercise and no dashboard legend will ever show.
    """
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)
    observed = Counter(r.level for r in records)

    assert set(observed) == set(LOG_LEVELS)
    assert all(observed[level] > 0 for level in LOG_LEVELS)
    # INFO is weighted highest and must actually dominate; a weight table applied in the wrong
    # order would still yield "every level appears" while inverting the shape entirely.
    assert observed.most_common(1)[0][0] == "INFO"


def test_services_are_drawn_from_the_roster_and_all_of_it_is_used() -> None:
    """Every service is in :data:`SERVICES`, and every entry in :data:`SERVICES` occurs.

    The second half matters for C12: the verifier probes ``service: "..."`` filters using names
    imported from this roster, and a roster entry the generator never emits would make that probe
    assert on an empty result set and pass for the wrong reason.
    """
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)
    observed = {r.service for r in records}

    assert observed <= set(SERVICES)
    assert observed == set(SERVICES)


def test_generated_values_fit_their_columns() -> None:
    """Nothing the generator emits could be rejected by the schema it is seeded into.

    A row the seeder can write but ``createLog`` could not is a contract inconsistency; a row that
    overflows its column turns startup into a driver error midway through a thousand-row chunk.
    """
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)

    for record in records:
        assert len(record.service) <= SERVICE_MAX_LENGTH
        assert len(record.level) <= LEVEL_MAX_LENGTH
        assert record.message, "an empty message would make every substring filter match it"
        if record.trace_id is not None:
            assert len(record.trace_id) <= TRACE_ID_MAX_LENGTH


def test_messages_contain_literal_like_metacharacters() -> None:
    """The corpus contains literal ``%`` and ``_`` — the material the escaping tests need.

    :func:`src.db.repository.escape_like` exists so that searching for ``"%"`` returns messages
    *containing a percent sign* rather than every row. That claim can only be proven against a
    corpus in which some messages contain one and most do not, so the presence of both characters
    is a property of the generator, not an accident of the templates.
    """
    messages = [r.message for r in generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)]

    with_percent = [m for m in messages if "%" in m]
    with_underscore = [m for m in messages if "_" in m]

    assert with_percent, "no message contains a literal '%'"
    assert with_underscore, "no message contains a literal '_'"
    # And crucially NOT all of them — a filter that matched everything would be indistinguishable
    # from a filter that was never applied.
    assert len(with_percent) < len(messages)
    assert len(with_underscore) < len(messages)


def test_messages_are_varied_but_recur() -> None:
    """Many distinct messages (so the trigram index earns its keep) and none of them unique-per-row.

    A corpus of 4000 identical lines makes ``search_text`` meaningless; a corpus of 4000 unique
    lines (a request id in every message) makes any grouping meaningless. Both ends are pinned.
    """
    messages = [r.message for r in generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)]
    distinct = set(messages)

    assert len(distinct) > 100, "too few distinct messages for a substring filter to discriminate"
    assert len(distinct) < len(messages), "every message unique leaves nothing to aggregate on"


# --- Correlation: the two branches C5 has to handle --------------------------------------------


def test_some_records_have_no_trace_id_and_some_share_one() -> None:
    """Both halves of ``related_logs`` are reachable from the seeded corpus alone.

    Spec §2 item 17 requires ``related_logs`` to return **an empty list** when ``trace_id`` is
    null. A corpus in which every row is correlated would leave that path untested by construction,
    no matter how thorough the C5 tests are.
    """
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)

    untraced = [r for r in records if r.trace_id is None]
    traced = [r for r in records if r.trace_id is not None]

    assert untraced, "no record without a trace_id: the empty-list branch is unreachable"
    assert traced

    groups = Counter(r.trace_id for r in traced)
    assert any(size >= 2 for size in groups.values()), "no trace is shared by two records"


def test_trace_group_sizes_stay_within_the_declared_bounds() -> None:
    """Every trace covers between :data:`TRACE_GROUP_MIN` and :data:`TRACE_GROUP_MAX` records."""
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)
    groups = Counter(r.trace_id for r in records if r.trace_id is not None)

    assert groups, "expected at least one trace group"
    assert all(TRACE_GROUP_MIN <= size <= TRACE_GROUP_MAX for size in groups.values()), (
        f"group sizes {sorted(set(groups.values()))} escape "
        f"[{TRACE_GROUP_MIN}, {TRACE_GROUP_MAX}]"
    )


def test_traced_share_is_close_to_the_declared_ratio() -> None:
    """Roughly :data:`TRACE_ID_RATIO` of records are correlated — not all, not a handful."""
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)
    share = sum(r.trace_id is not None for r in records) / len(records)

    assert abs(share - TRACE_ID_RATIO) < 0.05, f"traced share {share:.3f} drifted from the ratio"


def test_trace_groups_are_not_merely_adjacent_rows() -> None:
    """At least one trace spans non-consecutive records.

    Real requests interleave with unrelated traffic. If every group were a contiguous run, a C5
    batching bug that returned a row's *neighbours* instead of its correlated rows would produce
    the right answer and the N+1 test would still pass.
    """
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)
    positions: dict[str, list[int]] = {}
    for index, record in enumerate(records):
        if record.trace_id is not None:
            positions.setdefault(record.trace_id, []).append(index)

    assert any(
        indices[-1] - indices[0] > len(indices) - 1 for indices in positions.values()
    ), "every trace group is a contiguous run of records"


# --- The deliberate correlation with the order corpus (C10) --------------------------------------
#
# WHAT THESE PIN AND WHY IT MATTERS MORE THAN IT LOOKS: `Query.correlatedEvents(traceId:)` is the
# only interface-typed field in the schema, and its point is returning a MIX of __typenames.
# `OrderEvent`, `PaymentEvent` and `UserEvent` come free — one order's events share one trace by
# construction. `LogEntry` does not, and until `ORDER_TRACE_LOG_RATIO` existed it appeared only
# because both generators run on the same seed and both render `getrandbits(64)`, so aligned stream
# positions produced identical ids (about fifteen of two hundred at the shipped defaults, and ZERO
# at 4000 log rows — the same corpus, one setting away from an `... on LogEntry` fragment that
# matches nothing, with no test failing).
#
# So the correlation is declared, and these tests pin the declaration: a non-zero, deterministic
# number of order traces carry log lines, it is exactly the set `order_traces_with_logs` names, and
# the two independent populations C5 depends on are still there.
# -------------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def order_traces() -> tuple[str, ...]:
    """The e-commerce corpus's trace ids — the input the log corpus correlates against."""
    return tuple(generate_event_corpus(SAMPLE_ORDERS, seed=SEED, end_time=ANCHOR).trace_ids())


@pytest.fixture(scope="module")
def correlated(order_traces: tuple[str, ...]) -> list[LogRecord]:
    """A log corpus generated with the order traces threaded in. Never mutated, so module-scoped."""
    return generate_log_records(
        SAMPLE_SIZE, seed=SEED, end_time=ANCHOR, order_traces=order_traces
    )


def test_a_deterministic_non_zero_set_of_order_traces_carries_log_lines(
    correlated: list[LogRecord], order_traces: tuple[str, ...]
) -> None:
    """The count is a function of the ratio, and the set is **exactly** the declared one.

    Equality rather than ``>= 1`` on purpose, and it is a strictly stronger statement than "some
    log line shares an order's trace":

    * ``>=`` would hold for the accident this replaced — and the accident is a function of the seed,
      so it can be zero;
    * equality also says no *undeclared* correlation crept in, which is what the redraw in
      :func:`~src.generators._assign_trace_ids` is for. Without it an independently drawn log trace
      could collide with an order's, and that log group would silently join a timeline it has
      nothing to do with.
    """
    declared = order_traces_with_logs(order_traces)

    assert len(declared) == int(len(order_traces) * ORDER_TRACE_LOG_RATIO)
    assert len(declared) >= 10, (
        "the ratio dropped so low the correlation is barely demonstrable; correlatedEvents is the "
        "schema's flagship field and it needs real material"
    )

    observed = {record.trace_id for record in correlated if record.trace_id is not None} & set(
        order_traces
    )

    assert observed == set(declared)


def test_the_oldest_order_always_carries_log_lines(order_traces: tuple[str, ...]) -> None:
    """``order_id_for(0)``'s trace is always selected, and one order is enough to select it.

    That is what lets a test, C12's verifier and the C13 dashboard name a trace **by construction**
    rather than scanning the corpus for a lucky one — and it is what makes the zero case
    unreachable: ``int(1 * 0.25)`` is 0, so the floor of one is doing real work here.
    """
    assert order_traces_with_logs(order_traces)[0] == order_traces[0]
    assert order_traces_with_logs(order_traces[:1]) == order_traces[:1]
    assert order_traces_with_logs(()) == ()


def test_the_selection_is_spread_across_the_corpus_rather_than_a_prefix(
    order_traces: tuple[str, ...]
) -> None:
    """A stride, not the first quarter.

    ``trace_ids()`` is in first-appearance order, i.e. oldest order first. Taking a prefix would
    correlate only the oldest quarter of the corpus and leave every recent order — the top of any
    dashboard, which sorts newest first — with no log lines at all.
    """
    declared = order_traces_with_logs(order_traces)
    positions = [order_traces.index(trace) for trace in declared]

    assert positions == sorted(positions)
    assert positions[-1] > len(order_traces) * 0.9, (
        f"the last correlated order sits at {positions[-1]} of {len(order_traces)}, so the newest "
        "orders have no log lines"
    )


def test_correlation_leaves_the_log_only_and_untraced_populations_intact(
    correlated: list[LogRecord], order_traces: tuple[str, ...]
) -> None:
    """Both branches C5 depends on survive, and the traced share is unchanged.

    ``related_logs`` needs traces that only log rows carry, and spec §2 item 17 needs rows whose
    ``trace_id`` is NULL. Correlating *everything* would satisfy this module's other assertions
    while quietly making both branches unreachable — which is why the ratio is a quarter and why
    this test asserts the majority stayed independent rather than merely that something did.
    """
    traced = [record for record in correlated if record.trace_id is not None]
    untraced = [record for record in correlated if record.trace_id is None]
    log_only = {record.trace_id for record in traced} - set(order_traces)

    assert untraced, "no untraced row: related_logs' empty-list branch is unreachable"
    assert len(log_only) > 10 * len(order_traces_with_logs(order_traces)), (
        "the log-only population is no longer the majority of traces"
    )
    # And the correlation moved trace ids around WITHOUT changing how many rows carry one.
    assert abs(len(traced) / len(correlated) - TRACE_ID_RATIO) < 0.05


def test_group_size_bounds_survive_the_adoption(correlated: list[LogRecord]) -> None:
    """No trace grew past :data:`TRACE_GROUP_MAX` when the order ids were adopted.

    The failure this catches is specific: hand two log groups the *same* order trace — by selecting
    a duplicate, or by letting an independently drawn id collide with an adopted one — and the two
    merge into a single trace of up to ten records. Nothing else in the suite would notice, and
    ``relatedLogs`` would start returning a group twice the size the corpus promises.
    """
    groups = Counter(
        record.trace_id for record in correlated if record.trace_id is not None
    )

    assert groups
    assert all(TRACE_GROUP_MIN <= size <= TRACE_GROUP_MAX for size in groups.values()), (
        f"group sizes {sorted(set(groups.values()))} escape "
        f"[{TRACE_GROUP_MIN}, {TRACE_GROUP_MAX}]"
    )


def test_passing_order_traces_is_pure_and_actually_changes_the_corpus(
    order_traces: tuple[str, ...]
) -> None:
    """Same arguments in, equal corpus out — and the argument is not decorative.

    The first half is the oracle guarantee extended to the new parameter: an integration fixture
    regenerates the corpus the seeder wrote and grades the database against it, so the two calls
    have to agree. The second half is what stops that from being vacuous.
    """
    first = generate_log_records(500, seed=SEED, end_time=ANCHOR, order_traces=order_traces)
    second = generate_log_records(500, seed=SEED, end_time=ANCHOR, order_traces=order_traces)

    assert first == second
    assert first != generate_log_records(500, seed=SEED, end_time=ANCHOR)


def test_omitting_order_traces_leaves_the_corpus_uncorrelated(
    order_traces: tuple[str, ...]
) -> None:
    """The default is opt-out, so every pre-C10 caller and oracle is untouched.

    Also the reason none of the assertions above this section moved: they call the generator without
    the argument, and without the argument neither the adoption nor the redraw can fire.
    """
    plain = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)

    assert plain == generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR, order_traces=())
    # It is *not* asserted that the uncorrelated corpus shares no trace with the order corpus: it
    # may, by the seed-alignment accident described at the top of this section. That accident is
    # precisely what `order_traces` replaces with something a test can rely on.


def test_a_log_corpus_with_fewer_groups_than_orders_adopts_what_it_can(
    order_traces: tuple[str, ...]
) -> None:
    """A degenerate configuration truncates the selection instead of raising or indexing past it.

    ``SEED_ENTRIES=20`` with ``SEED_ORDERS=200`` asks for fifty correlated traces from a corpus that
    only has a handful of groups. Every group it does have is correlated, and nothing crashes.
    """
    records = generate_log_records(20, seed=SEED, end_time=ANCHOR, order_traces=order_traces)
    traces = {record.trace_id for record in records if record.trace_id is not None}

    assert traces
    assert traces <= set(order_traces)


@pytest.mark.parametrize(
    "trace",
    [
        pytest.param("", id="empty"),
        pytest.param("   ", id="blank"),
        pytest.param("x" * (TRACE_ID_MAX_LENGTH + 1), id="too-long"),
    ],
)
def test_an_unusable_order_trace_is_rejected_up_front(trace: str) -> None:
    """A trace id no ``trace_id`` column could hold fails here, not on the seeding INSERT.

    The alternative is a driver error partway through a thousand-row chunk at container startup,
    several layers from the caller that supplied the value.
    """
    with pytest.raises(ValueError):
        generate_log_records(100, seed=SEED, end_time=ANCHOR, order_traces=[trace])


# --- Metadata ----------------------------------------------------------------------------------


def test_metadata_is_present_on_some_records_and_null_on_others() -> None:
    """Both the JSONB-value and the SQL-NULL paths exist in the seeded corpus."""
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)

    with_metadata = [r for r in records if r.metadata is not None]
    without = [r for r in records if r.metadata is None]

    assert with_metadata and without
    share = len(with_metadata) / len(records)
    assert abs(share - METADATA_RATIO) < 0.05


def test_metadata_is_json_serialisable() -> None:
    """Everything in a metadata bag survives a JSON round trip.

    It has to: the column is ``JSONB``, so a value the encoder cannot handle is a driver error on
    the seeding INSERT, and the same object is later handed to Strawberry to serialise into a
    GraphQL response.
    """
    records = generate_log_records(SAMPLE_SIZE, seed=SEED, end_time=ANCHOR)

    for record in records:
        if record.metadata is None:
            continue
        assert json.loads(json.dumps(record.metadata)) == record.metadata
        assert all(isinstance(key, str) for key in record.metadata)


# --- Degenerate inputs -------------------------------------------------------------------------


def test_zero_count_returns_an_empty_corpus() -> None:
    """``SEED_ENTRIES=0`` is the compose ``test`` service's normal configuration, not an edge case."""
    assert generate_log_records(0, seed=SEED, end_time=ANCHOR) == []


@pytest.mark.parametrize("count", [-1, -100])
def test_negative_count_is_rejected(count: int) -> None:
    """A negative count is a caller bug and is refused rather than silently coerced to empty."""
    with pytest.raises(ValueError, match="count must be >= 0"):
        generate_log_records(count, seed=SEED, end_time=ANCHOR)


@pytest.mark.parametrize("window", [timedelta(0), timedelta(seconds=-1)])
def test_non_positive_window_is_rejected(window: timedelta) -> None:
    """A window of zero width cannot hold an ordered corpus, so it is refused up front."""
    with pytest.raises(ValueError, match="window must be positive"):
        generate_log_records(10, seed=SEED, end_time=ANCHOR, window=window)


def test_window_too_narrow_for_the_count_is_rejected_with_a_useful_message() -> None:
    """Asking for more records than the window has microseconds fails loudly, not silently.

    The alternative would be duplicate or out-of-range timestamps — a corpus that quietly violates
    the two invariants the rest of the suite is built on.
    """
    with pytest.raises(ValueError, match="strictly-increasing timestamps"):
        generate_log_records(1000, seed=SEED, end_time=ANCHOR, window=timedelta(microseconds=10))


def test_records_are_immutable() -> None:
    """An oracle is compared against, never adjusted until a comparison passes."""
    record = generate_log_records(1, seed=SEED, end_time=ANCHOR)[0]

    assert isinstance(record, LogRecord)
    with pytest.raises((AttributeError, TypeError)):
        record.service = "tampered"  # type: ignore[misc]
