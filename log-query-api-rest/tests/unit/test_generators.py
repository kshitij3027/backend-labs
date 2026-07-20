"""Unit tests for src.generators — the corpus that every other check is graded against.

These are not ordinary "does the function work" tests. ``src/generators.py`` is the **oracle**:
C12's E2E verifier computes its expected search counts, stats totals and top error messages by
importing this module and tallying the corpus itself. That makes reproducibility a correctness
property rather than a nicety — if the generator drifted between the API process and the
verifier process, every E2E assertion would still *run*, and none of them would mean anything.

So the suite is organised around the three properties the module docstring calls
non-negotiable, and each one is pinned as directly as possible rather than by proxy:

* **Determinism** — same seed, byte-identical corpus (full ``model_dump()``, not just ids), and
  provably independent of the global :mod:`random` module and of the wall clock.
* **Ordering** — ascending, UTC, monotonic even under jitter larger than the step, because C4's
  store assigns ``seq`` in append order and assumes append order equals time order.
* **Recurrence** — the modal ERROR/FATAL message repeats often enough for C11's top-errors panel
  to be a histogram rather than a list of singletons.

Every corpus here is built from a fixed seed, so the statistical assertions
(:func:`test_level_distribution_roughly_matches_weights`) are deterministic and cannot flake;
the tolerances are generous anyway, so a template edit that shifts the draw sequence does not
break the build for no reason, while a genuinely broken weight table still fails loudly.
"""

from __future__ import annotations

import json
import random
import time
from collections import Counter
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid5

import pytest

from src.generators import (
    ANCHOR_TS,
    ATTR_KEYS,
    DEFAULT_SEED,
    HOSTS,
    ID_NAMESPACE,
    LEVEL_WEIGHTS,
    MAX_ERROR_TEMPLATES,
    MESSAGE_TEMPLATES,
    SERVICES,
    CorpusCounts,
    expected_counts,
    generate_entries,
    generate_one,
)
from src.models import (
    ERROR_LEVELS,
    MAX_ATTR_KEY_LEN,
    MAX_ATTR_VALUE_LEN,
    MAX_ATTRS_KEYS,
    LogEntry,
    LogLevel,
)

#: Corpus sizes used across the suite. 5,000 is large enough for every level (including the 1%
#: FATAL slice) to appear and for the recurrence check to be meaningful; 10,000 matches the
#: production ``SEED_ENTRIES`` default, so the distribution test measures the real corpus.
SMALL_N = 5_000
FULL_N = 10_000


def dumps(entries: Sequence[LogEntry]) -> list[dict]:
    """Full field-by-field view of a corpus, for equality comparisons.

    Comparing ids alone would pass even if every message, level and timestamp had changed —
    ids are derived from ``(seed, index)`` and are the *least* likely thing to drift.
    """
    return [entry.model_dump() for entry in entries]


@pytest.fixture(scope="module")
def corpus() -> list[LogEntry]:
    """A 5,000-entry default-seed corpus, built once for the whole module.

    Safe to share: :class:`~src.models.LogEntry` is frozen, so no test can mutate it out from
    under another.
    """
    return generate_entries(SMALL_N)


@pytest.fixture(scope="module")
def full_corpus() -> list[LogEntry]:
    """A 10,000-entry corpus — the production ``SEED_ENTRIES`` default."""
    return generate_entries(FULL_N)


@pytest.fixture()
def pristine_global_random() -> Iterator[None]:
    """Save and restore the global :mod:`random` state around a test that reseeds it.

    The generator must not read global random state, but a test that proves that has to *write*
    it — and leaking a reseeded global RNG into unrelated tests is exactly the kind of ambient
    coupling this module exists to avoid.
    """
    state = random.getstate()
    try:
        yield
    finally:
        random.setstate(state)


# ---------------------------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------------------------


def test_same_seed_yields_identical_corpus():
    """Same seed in, byte-identical corpus out — every field, not just the ids."""
    assert dumps(generate_entries(500)) == dumps(generate_entries(500))
    assert dumps(generate_entries(500, seed=99)) == dumps(generate_entries(500, seed=99))


def test_different_seed_yields_different_corpus():
    """A different seed must actually move the corpus, or ``seed`` is decoration."""
    assert dumps(generate_entries(500, seed=1)) != dumps(generate_entries(500, seed=2))


def test_generator_does_not_use_global_random(pristine_global_random: None):
    """The corpus must not shift when the *global* RNG is reseeded and drained.

    This is the real pin against a stray ``random.choice(...)`` (module-level, no ``rng.``)
    slipping into a template draw. Such a call would look harmless and pass every other test in
    this file, but it would make the corpus depend on process-wide state that any import — or
    any earlier test — is free to change, and the E2E verifier's ground truth would silently
    stop matching the server's data.
    """
    random.seed(1)
    first = generate_entries(300)

    random.seed(999_999)
    for _ in range(50):  # drain the global stream so its position differs too, not just its seed
        random.random()
    second = generate_entries(300)

    assert dumps(first) == dumps(second)


def test_generate_one_is_deterministic_for_a_given_rng():
    """Two identically-seeded RNGs must drive :func:`generate_one` to the same entry."""
    left = generate_one(random.Random(5), ts=ANCHOR_TS, index=3)
    right = generate_one(random.Random(5), ts=ANCHOR_TS, index=3)
    assert left == right


# ---------------------------------------------------------------------------------------------
# Ordering and timestamps
# ---------------------------------------------------------------------------------------------


def test_timestamps_are_ascending_and_utc(corpus: list[LogEntry]):
    """Oldest first, never going backwards, always aware UTC.

    Ascending order is what lets the caller append the corpus straight into the store and have
    the store's monotonic ``seq`` agree with time order. UTC-awareness is what makes
    ``since``/``until`` range scans comparable at all — a naive datetime in the corpus would
    raise ``TypeError`` deep inside a scan.
    """
    timestamps = [entry.ts for entry in corpus]
    # Not ``strict=True``: pairing a list with its own tail is intentionally ragged by one.
    assert all(a <= b for a, b in zip(timestamps, timestamps[1:]))
    assert all(ts.tzinfo is not None and ts.utcoffset() == timedelta(0) for ts in timestamps)


def test_extreme_jitter_cannot_reverse_time():
    """Jitter far larger than the step must still not produce a backwards (or zero) gap.

    The clamp is the only thing standing between a caller's plausible-looking arguments and a
    corpus whose append order disagrees with its time order.
    """
    entries = generate_entries(500, step_ms=1, jitter_ms=10_000)
    timestamps = [entry.ts for entry in entries]
    assert all(a < b for a, b in zip(timestamps, timestamps[1:]))


def test_default_anchor_is_fixed_not_now():
    """The default ``end`` is :data:`ANCHOR_TS`, not the wall clock.

    Two calls made microseconds apart producing identical timestamps is necessary but weak
    evidence (a coarse clock would fake it), so the test also asserts the stronger property: the
    default-argument corpus is identical to one generated with ``end=ANCHOR_TS`` explicitly, and
    the newest entry sits exactly on the anchor. A generator reading ``datetime.now()`` cannot
    satisfy that, no matter how fast the loop runs.
    """
    first = generate_entries(200)
    second = generate_entries(200)
    explicit = generate_entries(200, end=ANCHOR_TS)

    assert dumps(first) == dumps(second) == dumps(explicit)
    assert first[-1].ts == ANCHOR_TS
    assert ANCHOR_TS != datetime.now(UTC)


def test_explicit_end_is_respected():
    """The newest entry lands exactly on the supplied ``end`` — not near it."""
    end = datetime(2030, 1, 2, 3, 4, 5, 678_000, tzinfo=UTC)
    entries = generate_entries(50, end=end)
    assert entries[-1].ts == end
    assert entries[0].ts < end


def test_naive_or_offset_end_is_normalised_to_utc():
    """A naive ``end`` means UTC; an offset-aware one is converted — the model's own rule.

    The rule lives in ``src.models`` and is reused here rather than re-implemented, so a corpus
    can never end up holding a mix of timezones.
    """
    naive = generate_entries(5, end=datetime(2026, 7, 18, 12, 0, 0))
    assert naive[-1].ts == ANCHOR_TS

    plus_two = timezone(timedelta(hours=2))
    shifted = generate_entries(5, end=datetime(2026, 7, 18, 14, 0, 0, tzinfo=plus_two))
    assert shifted[-1].ts == ANCHOR_TS


# ---------------------------------------------------------------------------------------------
# Level distribution
# ---------------------------------------------------------------------------------------------


def test_level_weights_sum_to_one():
    """The weight table itself is a contract; a table summing to 0.97 loses 3% of draws."""
    assert set(LEVEL_WEIGHTS) == set(LogLevel)
    assert sum(LEVEL_WEIGHTS.values()) == pytest.approx(1.0, abs=1e-9)


def test_all_levels_present_in_large_corpus(corpus: list[LogEntry]):
    """Every level appears, including the 1% FATAL slice.

    A corpus missing a level would make the ``level=FATAL`` filter, the RBAC demos and the stats
    panel all look broken for reasons that have nothing to do with their own code.
    """
    seen = {entry.level for entry in corpus}
    assert seen == set(LogLevel)


def test_level_distribution_roughly_matches_weights(full_corpus: list[LogEntry]):
    """Observed shares track :data:`LEVEL_WEIGHTS` within a generous absolute tolerance.

    The corpus is deterministic, so this cannot flake; the tolerance is wide (3 percentage
    points against an observed worst case near 0.9) purely so a template edit that shifts the
    RNG consumption sequence does not fail the build. It is still far tighter than any real
    weighting bug — a uniform 20%-per-level regression puts INFO 40 points off.
    """
    counts = Counter(entry.level for entry in full_corpus)
    for level, weight in LEVEL_WEIGHTS.items():
        share = counts[level] / FULL_N
        assert abs(share - weight) <= 0.03, f"{level}: share {share:.4f} vs weight {weight}"


# ---------------------------------------------------------------------------------------------
# Ids
# ---------------------------------------------------------------------------------------------


def test_ids_are_unique(corpus: list[LogEntry]):
    """Ids are the key for ``GET /logs/{id}``; a duplicate makes that route ambiguous."""
    ids = [entry.id for entry in corpus]
    assert len(set(ids)) == len(ids)


def test_ids_are_stable_across_runs():
    """Ids are ``uuid5(ID_NAMESPACE, "<seed>:<index>")`` — a function of the seed, not of luck.

    Pinned against the formula, not merely against a second run, because the E2E verifier is
    entitled to compute an id in advance and fetch that specific entry over HTTP. ``uuid4`` would
    satisfy "unique" and nothing else here.
    """
    entries = generate_entries(200)
    assert [entry.id for entry in entries] == [entry.id for entry in generate_entries(200)]
    assert all(
        entry.id == uuid5(ID_NAMESPACE, f"{DEFAULT_SEED}:{index}").hex
        for index, entry in enumerate(entries)
    )
    # A different seed must move the ids too, or the namespace derivation ignores the seed.
    assert generate_entries(5, seed=4321)[0].id != entries[0].id


# ---------------------------------------------------------------------------------------------
# Model conformance
# ---------------------------------------------------------------------------------------------


def test_every_entry_validates_as_log_entry(corpus: list[LogEntry]):
    """Entries are real :class:`~src.models.LogEntry` instances that survive a JSON round-trip.

    ``LogEntry`` is ``extra="forbid"`` and serialises ``ts`` to RFC-3339 with a ``Z`` suffix, so
    re-parsing the dumped JSON exercises the exact path a client takes — and would fail if the
    generator ever produced a value the model's own wire form cannot represent.
    """
    assert corpus and all(isinstance(entry, LogEntry) for entry in corpus)
    for entry in corpus[:200]:
        restored = LogEntry(**json.loads(entry.model_dump_json()))
        assert restored == entry


def test_services_and_hosts_are_from_the_declared_vocabulary(corpus: list[LogEntry]):
    """Only published names appear — and all of them do.

    Both halves matter: the verifier filters on ``service=auth-svc`` and needs rows back, and a
    stray name outside the tuple would mean the vocabulary constant is no longer the truth.
    """
    assert {entry.service for entry in corpus} == set(SERVICES)
    assert {entry.host for entry in corpus} == set(HOSTS)


def test_attrs_respect_model_caps(corpus: list[LogEntry]):
    """Generated ``attrs`` stay inside the caps ``src.models`` enforces on a write.

    A seeded entry that a client could not legally have POSTed would be an inconsistency between
    the corpus and the contract — and the caps exist precisely because the ring bounds the entry
    *count* but not the per-entry size.
    """
    populated = 0
    for entry in corpus:
        assert len(entry.attrs) <= MAX_ATTRS_KEYS
        assert set(entry.attrs) <= set(ATTR_KEYS)
        for key, value in entry.attrs.items():
            assert len(key) <= MAX_ATTR_KEY_LEN
            assert len(value) <= MAX_ATTR_VALUE_LEN
            assert isinstance(value, str)
        if entry.attrs:
            populated += 1

    # A subset, not all and not none — both branches of every consumer that reads attrs get
    # exercised by the seeded corpus.
    assert 0 < populated < len(corpus)


# ---------------------------------------------------------------------------------------------
# Counts and edge cases
# ---------------------------------------------------------------------------------------------


def test_count_zero_returns_empty_list():
    """``SEED_ENTRIES=0`` is the compose ``test`` service's normal setting, not an edge case."""
    assert generate_entries(0) == []


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"count": -1}, "count"),
        ({"count": 10, "step_ms": -5}, "step_ms"),
        ({"count": 10, "jitter_ms": -5}, "jitter_ms"),
    ],
)
def test_negative_arguments_are_rejected(kwargs: dict, fragment: str):
    """Nonsense arguments fail loudly rather than producing a quietly strange corpus."""
    with pytest.raises(ValueError, match=fragment):
        generate_entries(**kwargs)


# ---------------------------------------------------------------------------------------------
# Error-message recurrence — what makes the top-errors panel meaningful
# ---------------------------------------------------------------------------------------------


def test_error_messages_recur(corpus: list[LogEntry]):
    """The modal ERROR/FATAL message occurs many times, not once.

    C11's ``top_errors`` groups by the exact message string. If error lines carried per-request
    ids, the "top" list would be an arbitrary sample of thousands of 1-count messages, the
    dashboard panel would be noise, and C12's check on it would assert nothing. Twenty is a
    floor, not a target — the observed value for this corpus is comfortably above it.
    """
    counts = Counter(
        entry.message for entry in corpus if entry.level in ERROR_LEVELS
    )
    assert counts, "corpus contains no ERROR/FATAL entries at all"
    assert counts.most_common(1)[0][1] >= 20


def test_error_templates_are_literal_and_few():
    """The structural reason recurrence holds: no interpolation, small pools.

    :func:`test_error_messages_recur` measures the outcome; this pins the cause, so that adding
    a ``{request_id}`` slot to an error template fails here with an explanation rather than
    somewhere downstream with a mysterious histogram.
    """
    for level in ERROR_LEVELS:
        templates = MESSAGE_TEMPLATES[level]
        assert 0 < len(templates) <= MAX_ERROR_TEMPLATES
        assert all("{" not in template for template in templates), (
            f"{level} templates must be literal so identical errors group together"
        )


# ---------------------------------------------------------------------------------------------
# expected_counts — the oracle
# ---------------------------------------------------------------------------------------------


def test_expected_counts_matches_manual_tally():
    """The oracle agrees with a tally written independently, right here in the test.

    Deliberately re-counts with plain :class:`collections.Counter` instead of reusing anything
    from the module under test: an oracle validated by its own machinery validates nothing.
    """
    entries = generate_entries(400, seed=7)
    counts = expected_counts(entries)

    assert counts.total == len(entries)
    assert counts.by_level == Counter(entry.level.value for entry in entries)
    assert counts.by_service == Counter(entry.service for entry in entries)
    assert counts.by_host == Counter(entry.host for entry in entries)
    assert counts.earliest == min(entry.ts for entry in entries)
    assert counts.latest == max(entry.ts for entry in entries)

    errors = Counter(
        entry.message for entry in entries if entry.level in ERROR_LEVELS
    )
    assert counts.top_error_messages == sorted(
        errors.items(), key=lambda item: (-item[1], item[0])
    )[:10]
    # Sums must reconcile: every entry lands in exactly one bucket of each facet.
    assert sum(counts.by_level.values()) == counts.total
    assert sum(counts.by_service.values()) == counts.total
    assert sum(counts.by_host.values()) == counts.total


def test_expected_counts_ranks_by_frequency_then_message():
    """Ranking is a *total* order: count descending, then message ascending.

    ``Counter.most_common`` breaks ties by insertion order, which depends on the order entries
    happen to arrive — so an oracle built on it could rank the same multiset two different ways
    and disagree with itself. The lexicographic tiebreak removes that.
    """
    ts = ANCHOR_TS

    def error(message: str, index: int) -> LogEntry:
        return LogEntry(
            id=f"{index:032x}",
            ts=ts,
            level=LogLevel.ERROR,
            service=SERVICES[0],
            host=HOSTS[0],
            message=message,
        )

    entries = [error("beta", 0), error("beta", 1), error("zulu", 2), error("alpha", 3)]
    counts = expected_counts(entries)
    assert counts.top_error_messages == [("beta", 2), ("alpha", 1), ("zulu", 1)]

    # top_n truncates the ranking rather than changing it.
    assert expected_counts(entries, top_n=1).top_error_messages == [("beta", 2)]
    assert expected_counts(entries, top_n=0).top_error_messages == []


def test_expected_counts_ignores_non_error_levels():
    """Only ERROR and FATAL feed ``top_error_messages`` — the model's own definition of 'error'."""
    entries = generate_entries(600, seed=11)
    counts = expected_counts(entries, top_n=100)
    error_messages = {
        entry.message for entry in entries if entry.level in ERROR_LEVELS
    }
    assert {message for message, _ in counts.top_error_messages} <= error_messages


def test_expected_counts_on_empty_corpus():
    """An empty corpus tallies to zeros and ``None`` bounds — never an exception.

    ``SEED_ENTRIES=0`` plus a filter that matches nothing is an ordinary state, and the stats
    route has to be able to describe it.
    """
    counts = expected_counts([])
    assert isinstance(counts, CorpusCounts)
    assert counts.total == 0
    assert counts.by_level == {}
    assert counts.by_service == {}
    assert counts.by_host == {}
    assert counts.top_error_messages == []
    assert counts.earliest is None
    assert counts.latest is None


# ---------------------------------------------------------------------------------------------
# Startup budget
# ---------------------------------------------------------------------------------------------


def test_seeding_the_full_corpus_is_fast():
    """Generating the production corpus must not eat the container's healthcheck window.

    ``SEED_ENTRIES=10000`` is generated inside the lifespan, before the app can answer
    ``/health``, and the Dockerfile healthcheck allows a 20-second ``start_period``. The bound
    here is deliberately loose — the measured cost is well under a tenth of a second — so this
    fails only on an algorithmic regression (per-entry regex, datetime string parsing, an
    accidental O(n^2)), never on a busy machine.
    """
    started = time.perf_counter()
    entries = generate_entries(FULL_N)
    elapsed = time.perf_counter() - started

    assert len(entries) == FULL_N
    assert elapsed < 5.0, f"generating {FULL_N} entries took {elapsed:.2f}s"
