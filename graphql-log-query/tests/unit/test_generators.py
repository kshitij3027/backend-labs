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
    SERVICES,
    TRACE_GROUP_MAX,
    TRACE_GROUP_MIN,
    TRACE_ID_RATIO,
    generate_log_records,
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
