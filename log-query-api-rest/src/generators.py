"""Deterministic seeded log corpus — the project's **ground-truth data foundation**.

There is no real log corpus in this repo, so the store is filled at startup by this module
(``SEED_ENTRIES`` entries, built in :meth:`src.main.Runtime.build_seeded`). That makes this file
much more than a fixture factory: it is the *oracle*. ``scripts/verify_e2e.py`` (C12) grades the
live API's answers — the advanced-search match count, the ``/stats`` totals, the top recurring
error messages — against numbers computed straight from this module by :func:`expected_counts`.
A check whose expected value is derived from the same data the server serves is only meaningful
if that data is reproducible; a generator that drifted between the API process and the verifier
process would make every E2E assertion unfalsifiable (it would "pass" by comparing the server to
itself, or fail for reasons that have nothing to do with the code under test).

Three properties are therefore non-negotiable, and each one is pinned by a test in
``tests/unit/test_generators.py``:

1. **Determinism.** Every draw comes from a private ``random.Random(seed)`` instance — never the
   global :mod:`random` module, which any other import in the process is free to reseed, and
   never :func:`datetime.now`. Same arguments in, byte-identical corpus out, in any process.
2. **Ascending timestamps.** :func:`generate_entries` returns **oldest first**, so a caller can
   ``for e in entries: store.append(e)`` and have the store's monotonic ``seq`` order agree with
   time order. C4's ring, its cursor codec and its newest-first scan all assume "append order ==
   time order"; seeding out of order would leave the store internally consistent but sorted
   wrongly, which is the kind of bug that only shows up as a confusing pagination result.
3. **Recurring error messages.** ERROR/FATAL messages are drawn from a deliberately *small*,
   fully literal pool (see :data:`MESSAGE_TEMPLATES`), so the same error text repeats hundreds of
   times across a 10,000-entry corpus. C11's "top error messages" panel and the E2E check that
   grades it are meaningless if every error line is unique.

Standard library only (``random`` / ``uuid`` / ``datetime``) plus :mod:`src.models`, so the
E2E scripts and the test suite can import it without dragging in the web stack. It must never
import :mod:`src.store`, :mod:`src.main` or :mod:`src.api` — those import *this*, and a cycle
would break startup.
"""

from __future__ import annotations

import random
from bisect import bisect_right
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import accumulate
from types import MappingProxyType
from uuid import NAMESPACE_URL, UUID, uuid5

from src.models import (
    ERROR_LEVELS,
    MAX_ATTR_KEY_LEN,
    MAX_ATTR_VALUE_LEN,
    MAX_ATTRS_KEYS,
    LogEntry,
    LogLevel,
    _to_utc,
)

# ---------------------------------------------------------------------------------------------
# Reproducibility anchors
#
# These three constants are the whole reason the E2E verifier can assert on specific values.
# They are module-level (not defaults buried in a signature) so tests and scripts can import and
# compare against the exact same objects.
# ---------------------------------------------------------------------------------------------

#: Default RNG seed. Matches the plan's ``seed=1337`` so the startup corpus, the unit tests and
#: the E2E verifier all describe the same 10,000 entries unless a caller says otherwise.
DEFAULT_SEED = 1337

#: The default *newest* timestamp. A **fixed instant**, never ``datetime.now(UTC)``: the corpus
#: must be identical no matter when the container starts, or C12's ground-truth counts would
#: drift between the verifier run and the API run, and every timestamp-sensitive assertion would
#: become a flake. Callers that genuinely want "now" pass ``end=`` explicitly.
ANCHOR_TS: datetime = datetime(2026, 7, 18, 12, 0, 0, tzinfo=UTC)

#: Namespace for :func:`uuid.uuid5` id derivation. ``uuid5`` — not ``uuid4`` — because an id has
#: to be a *function* of ``(seed, index)``: reproducible ids let the verifier fetch a specific
#: entry through ``GET /logs/{id}`` and assert on its contents, and let a test assert that two
#: runs produced the same corpus rather than merely the same shape. uuid4 would be fresh every
#: process and nothing downstream could name an entry in advance.
ID_NAMESPACE: UUID = uuid5(NAMESPACE_URL, "https://backend-labs.local/log-query-api-rest/logs")

#: Floor on the gap between two consecutive entries, in milliseconds. Jitter is applied to a
#: nominal step and could otherwise produce a zero or negative gap, which would make the corpus
#: non-monotonic — and a store whose append order disagrees with its time order breaks the
#: newest-first scan contract described in the module docstring.
MIN_GAP_MS = 1


# ---------------------------------------------------------------------------------------------
# Vocabulary
#
# Module-level and public: ``tests/unit/test_generators.py`` asserts that every generated entry
# draws from these exact pools, and the E2E verifier builds its filter queries out of them
# (``?service=auth-svc`` is only a useful probe if ``auth-svc`` is guaranteed to be in the
# corpus). Both are tuples, i.e. immutable — a shared mutable list that a caller could append to
# would silently change the ground truth for everybody else in the process.
# ---------------------------------------------------------------------------------------------

#: The eight emitting services. ``auth-svc`` is mandatory: it is the service in the README's
#: pagination example and in its ``curl`` snippets, so the documented request must return rows.
SERVICES: tuple[str, ...] = (
    "auth-svc",
    "api-gateway",
    "payments-svc",
    "search-svc",
    "user-svc",
    "notify-svc",
    "ingest-svc",
    "billing-svc",
)

#: The six emitting hosts. ``node-3`` appears in the README's example row.
HOSTS: tuple[str, ...] = ("node-1", "node-2", "node-3", "node-4", "node-5", "node-6")

#: Severity mix of a healthy-ish production service: mostly INFO, a fifth DEBUG, a thin tail of
#: WARN/ERROR and a very rare FATAL. The shape matters for the project's own demos — a corpus of
#: 20% FATAL would make the stats panel and the ``level=ERROR`` filter look impressive and
#: nothing like reality — and the weights are checked to sum to 1.0 at import (see
#: :func:`_validate_vocabulary`), because a table that quietly sums to 0.97 would leave 3% of
#: draws falling off the end of the cumulative distribution.
LEVEL_WEIGHTS: Mapping[LogLevel, float] = MappingProxyType(
    {
        LogLevel.DEBUG: 0.20,
        LogLevel.INFO: 0.60,
        LogLevel.WARN: 0.12,
        LogLevel.ERROR: 0.07,
        LogLevel.FATAL: 0.01,
    }
)

#: Interpolation vocabularies for the DEBUG/INFO/WARN templates. Small, fixed pools rather than
#: random strings: realistic-looking text that is still low-cardinality enough for the ``q=``
#: substring filter to return a meaningful number of rows.
USER_IDS: tuple[str, ...] = (
    "u-1001", "u-1002", "u-1044", "u-2087", "u-3310", "u-4471", "u-5560", "u-7702",
)
ENDPOINTS: tuple[str, ...] = (
    "/api/v1/logs",
    "/api/v1/logs/search",
    "/api/v1/auth/token",
    "/api/v1/stats",
    "/api/v1/logs/stream",
    "/health",
)
TABLES: tuple[str, ...] = ("sessions", "accounts", "payments", "audit_log", "api_tokens")
QUEUES: tuple[str, ...] = ("ingest", "notifications", "billing")
REGIONS: tuple[str, ...] = ("us-east-1", "eu-west-1", "ap-south-1")

#: Message templates per level, filled from the vocabularies above via ``str.format``.
#:
#: **The ERROR and FATAL pools are deliberately small and completely literal** — no ``{slot}``,
#: no ids, no latencies. C11 groups ``top_errors`` by the exact message string, so an error line
#: carrying a per-request id would produce a histogram of thousands of 1-count entries, and both
#: the dashboard panel and C12's "top error message occurs N times" check would be worthless.
#: Per-entry detail for an error belongs in ``attrs`` (a ``request_id``), which is exactly the
#: structured-logging split this API's model already encodes: low-cardinality ``message``,
#: high-cardinality ``attrs``.
#:
#: DEBUG/INFO/WARN interpolate freely — nothing groups on them, and the variety is what makes
#: the ``q=`` substring filter and the SSE tail look like a real log stream.
MESSAGE_TEMPLATES: Mapping[LogLevel, tuple[str, ...]] = MappingProxyType(
    {
        LogLevel.DEBUG: (
            "entering handler for {endpoint}",
            "cache lookup hit for key user:{user}",
            "acquired pooled connection for {table}",
            "serialized {count} rows in {latency}ms",
            "heartbeat tick",
            "resolved region {region} from request context",
        ),
        LogLevel.INFO: (
            "request completed {endpoint} status=200 in {latency}ms",
            "user {user} authenticated successfully",
            "health check passed",
            "published {count} events to queue {queue}",
            "session established for user {user}",
            "cache warm-up finished for {region}",
            "background compaction of {table} finished",
        ),
        LogLevel.WARN: (
            "slow query on {table} took {latency}ms",
            "retrying {endpoint} after transient failure",
            "connection pool nearing capacity",
            "clock drift detected against peer node",
            "rate limit approaching for user {user}",
            "queue {queue} backlog growing",
        ),
        # --- low-cardinality on purpose; see the note above ---
        LogLevel.ERROR: (
            "invalid token",
            "upstream request timed out",
            "database connection refused",
            "payment authorization declined",
            "cache write failed",
        ),
        LogLevel.FATAL: (
            "unrecoverable data corruption detected",
            "out of memory, aborting process",
            "primary replica lost, shutting down",
            "startup configuration validation failed",
        ),
    }
)

#: Ceiling enforced at import on the size of the ERROR/FATAL pools. This is the recurrence
#: guarantee in executable form: with ~8% of a corpus at ERROR/FATAL, six templates keep the
#: modal error message well above the "at least 20 occurrences in 5,000 entries" floor that
#: ``test_error_messages_recur`` pins. Adding a seventh error template is a decision, not a typo,
#: so it should have to change this number too.
MAX_ERROR_TEMPLATES = 6

#: Probability an entry carries a ``request_id`` attribute, and (independently) a ``region`` one.
#: Not every entry, on purpose: a partially-populated ``attrs`` bag is what exercises both
#: branches of every consumer that reads it, and it matches real emitters where only
#: request-scoped lines carry correlation ids.
REQUEST_ID_PROB = 0.5
REGION_ATTR_PROB = 0.25

#: The attribute keys this module can emit. Named here so :func:`_validate_vocabulary` can check
#: them against the model's caps at import rather than trusting that nobody adds a long one.
ATTR_KEYS: tuple[str, ...] = ("request_id", "region")


# ---------------------------------------------------------------------------------------------
# Weighted level selection
#
# Precomputed cumulative distribution + a single ``rng.random()`` and a binary search per entry.
# ``random.choices`` would also work but allocates a list per call, and this is on the startup
# path for 10,000 entries inside the healthcheck's 20-second start_period.
# ---------------------------------------------------------------------------------------------

_LEVEL_SEQUENCE: tuple[LogLevel, ...] = tuple(LEVEL_WEIGHTS)

#: Cumulative weights, with the final entry **forced to exactly 1.0**. Binary floating point
#: cannot represent 0.6/0.12/0.07/0.01 exactly, so the accumulated total lands a few ULPs off;
#: if it landed *below* 1.0, an ``rng.random()`` draw in the gap would bisect past the end of the
#: table and raise IndexError roughly once every few billion entries — the worst kind of bug to
#: debug. Pinning the last bucket's upper edge removes the possibility instead of making it rare.
_LEVEL_CUMULATIVE: tuple[float, ...] = (
    *tuple(accumulate(LEVEL_WEIGHTS.values()))[:-1],
    1.0,
)

#: Every slot name the templates may reference. Drawn *in full* for every entry regardless of
#: which template was picked, so each entry consumes exactly the same number of RNG values. That
#: uniformity is what keeps the corpus stable when a template is added to one level's pool: the
#: draw *shape* per entry does not depend on the template, only the rendered text does.
_SLOT_NAMES: tuple[str, ...] = (
    "user", "endpoint", "table", "queue", "region", "latency", "count",
)


def _draw_slots(rng: random.Random) -> dict[str, object]:
    """Draw one value for every template slot from the seeded RNG.

    Order is fixed and must stay fixed: it defines the RNG consumption sequence, so reordering
    these lines changes every corpus generated from every seed.
    """
    return {
        "user": rng.choice(USER_IDS),
        "endpoint": rng.choice(ENDPOINTS),
        "table": rng.choice(TABLES),
        "queue": rng.choice(QUEUES),
        "region": rng.choice(REGIONS),
        "latency": rng.randint(3, 2500),
        "count": rng.randint(1, 500),
    }


def _validate_vocabulary() -> None:
    """Fail fast **at import** if the tables above violate an invariant the corpus depends on.

    A typo in a template (``{sevrice}``) or a weight table that sums to 0.97 would otherwise
    surface as a ``KeyError`` deep inside container startup or as a silently skewed corpus that
    quietly invalidates the E2E ground truth. Import-time validation turns both into an
    immediate, obvious crash with the offending value in the message.
    """
    if set(LEVEL_WEIGHTS) != set(LogLevel):
        raise ValueError("LEVEL_WEIGHTS must assign a weight to every LogLevel member")
    if any(weight <= 0 for weight in LEVEL_WEIGHTS.values()):
        raise ValueError("LEVEL_WEIGHTS entries must all be positive")
    # Tolerance rather than ``== 1.0``: the literals are decimal, the arithmetic is binary. A
    # 1e-9 window is far tighter than any real weighting mistake and far looser than float noise.
    total_weight = sum(LEVEL_WEIGHTS.values())
    if abs(total_weight - 1.0) > 1e-9:
        raise ValueError(f"LEVEL_WEIGHTS must sum to 1.0, got {total_weight!r}")

    if set(MESSAGE_TEMPLATES) != set(LogLevel):
        raise ValueError("MESSAGE_TEMPLATES must cover every LogLevel member")
    for level, templates in MESSAGE_TEMPLATES.items():
        if not templates:
            raise ValueError(f"no message templates for level {level}")
    for level in ERROR_LEVELS:
        if len(MESSAGE_TEMPLATES[level]) > MAX_ERROR_TEMPLATES:
            raise ValueError(
                f"{level} has {len(MESSAGE_TEMPLATES[level])} templates; at most "
                f"{MAX_ERROR_TEMPLATES} are allowed so error messages recur often enough "
                "for the top-errors aggregation to mean anything"
            )

    # Render every template against a real slot draw. This is stricter than checking a hardcoded
    # name list because it validates the templates against the function that actually fills them
    # — the two cannot drift. The local Random(0) is private and never touches a corpus.
    probe = _draw_slots(random.Random(0))
    if set(probe) != set(_SLOT_NAMES):
        raise ValueError("_draw_slots and _SLOT_NAMES disagree on the slot vocabulary")
    for templates in MESSAGE_TEMPLATES.values():
        for template in templates:
            try:
                template.format(**probe)
            except (KeyError, IndexError, ValueError) as exc:
                raise ValueError(f"unfillable message template {template!r}: {exc}") from exc

    if len(set(SERVICES)) != len(SERVICES) or not SERVICES:
        raise ValueError("SERVICES must be a non-empty tuple of unique names")
    if len(set(HOSTS)) != len(HOSTS) or not HOSTS:
        raise ValueError("HOSTS must be a non-empty tuple of unique names")
    if "auth-svc" not in SERVICES:
        raise ValueError("SERVICES must include 'auth-svc' — the README's examples filter on it")

    # The generated ``attrs`` bag must fit inside the caps ``src.models`` enforces on a write;
    # a seeded entry that a client could not have POSTed would be a contract inconsistency.
    if len(ATTR_KEYS) > MAX_ATTRS_KEYS:
        raise ValueError(f"generator emits more than {MAX_ATTRS_KEYS} attrs keys")
    for key in ATTR_KEYS:
        if len(key) > MAX_ATTR_KEY_LEN:
            raise ValueError(f"attrs key {key!r} exceeds {MAX_ATTR_KEY_LEN} characters")
    for region in REGIONS:
        if len(region) > MAX_ATTR_VALUE_LEN:
            raise ValueError(f"region {region!r} exceeds {MAX_ATTR_VALUE_LEN} characters")


_validate_vocabulary()


def _pick_level(rng: random.Random) -> LogLevel:
    """Draw one level from :data:`LEVEL_WEIGHTS` using one RNG value and a binary search."""
    return _LEVEL_SEQUENCE[bisect_right(_LEVEL_CUMULATIVE, rng.random())]


def _build_timestamps(
    count: int, *, end: datetime, step_ms: int, jitter_ms: int, rng: random.Random
) -> list[datetime]:
    """Build ``count`` **ascending** timestamps ending exactly on ``end``.

    The walk is computed backwards (accumulate jittered gaps *before* ``end``) and then reversed,
    which is what makes ``entries[-1].ts == end`` exact rather than approximately right: the
    newest entry sits on the anchor and the accumulated jitter falls off the old end of the
    corpus, where nothing depends on it.

    Each gap is clamped to :data:`MIN_GAP_MS` so the sequence is strictly increasing even when a
    caller passes ``jitter_ms > step_ms``. See the module docstring for why non-monotonic seeding
    would quietly corrupt the store's ordering contract.
    """
    if count <= 0:
        return []

    offsets_ms: list[int] = [0]  # the newest entry sits exactly on ``end``
    low, high = -jitter_ms, jitter_ms
    for _ in range(count - 1):
        gap = step_ms + rng.randint(low, high)
        offsets_ms.append(offsets_ms[-1] + (gap if gap >= MIN_GAP_MS else MIN_GAP_MS))

    # ``offsets_ms[i]`` is "milliseconds before end", increasing; reversing yields ascending time.
    return [end - timedelta(milliseconds=offset) for offset in reversed(offsets_ms)]


def generate_one(
    rng: random.Random, *, ts: datetime, index: int = 0, seed: int = DEFAULT_SEED
) -> LogEntry:
    """Build a single :class:`~src.models.LogEntry` at ``ts`` from the supplied RNG.

    Split out from :func:`generate_entries` so the live paths — the SSE demo appending a fresh
    entry, the load harness manufacturing write traffic — produce entries drawn from exactly the
    same vocabulary and distribution as the seeded corpus, instead of a second, subtly different
    ad-hoc generator.

    Args:
        rng: A caller-owned ``random.Random``. Passed in rather than created here so a loop can
            keep drawing from one stream; a fresh ``Random(seed)`` per call would emit the same
            entry every time.
        ts: The entry's timestamp. Normalised to UTC by :class:`~src.models.LogEntry` itself.
        index: Combined with ``seed`` to derive the id. **Callers appending live entries must
            pass a distinct index per call** (a running counter) — two calls with the same
            ``(seed, index)`` deliberately produce the same id, which is what makes the seeded
            corpus reproducible but would be a duplicate-id bug in a live append loop.
        seed: The corpus seed this entry belongs to; only used for id derivation.
    """
    level = _pick_level(rng)
    service = rng.choice(SERVICES)
    host = rng.choice(HOSTS)
    slots = _draw_slots(rng)
    message = rng.choice(MESSAGE_TEMPLATES[level]).format(**slots)

    # Attributes are populated on a subset of entries — see REQUEST_ID_PROB. Both keys and both
    # value shapes are bounded by construction, well inside the model's attrs caps.
    attrs: dict[str, str] = {}
    if rng.random() < REQUEST_ID_PROB:
        attrs["request_id"] = f"{rng.getrandbits(48):012x}"
    if rng.random() < REGION_ATTR_PROB:
        attrs["region"] = str(slots["region"])

    return LogEntry(
        id=uuid5(ID_NAMESPACE, f"{seed}:{index}").hex,
        ts=ts,
        level=level,
        service=service,
        host=host,
        message=message,
        attrs=attrs,
    )


def generate_entries(
    count: int,
    *,
    seed: int = DEFAULT_SEED,
    end: datetime | None = None,
    step_ms: int = 250,
    jitter_ms: int = 200,
) -> list[LogEntry]:
    """Generate a reproducible corpus of ``count`` entries, **oldest first**.

    Ascending order is a contract, not a convenience: the caller seeds the store with
    ``for entry in generate_entries(n): store.append(entry)``, and C4's store assigns its
    monotonic ``seq`` in append order. Returning newest-first would give the oldest entry the
    highest ``seq``, so every newest-first scan would walk the corpus backwards in time while
    looking perfectly self-consistent.

    Determinism comes from a private ``random.Random(seed)``: the global :mod:`random` module is
    process-wide state that any library import is free to reseed, so a generator that used it
    would produce a different corpus depending on what else happened to be imported.

    Args:
        count: Number of entries. ``0`` returns ``[]`` — the compose ``test`` service runs with
            ``SEED_ENTRIES=0``, so the empty case is a normal configuration, not an edge case.
        seed: Seeds the private RNG and derives the ids. Same seed, byte-identical corpus.
        end: Timestamp of the **newest** entry. Defaults to :data:`ANCHOR_TS`, a fixed instant —
            never the wall clock, or the corpus (and every ground-truth count derived from it)
            would change between runs.
        step_ms: Nominal gap between consecutive entries, in milliseconds.
        jitter_ms: Symmetric jitter applied to each gap, in milliseconds. Gaps are clamped to
            :data:`MIN_GAP_MS`, so the result is monotonic for any jitter.

    Returns:
        ``count`` entries in ascending timestamp order, the last of which sits exactly on ``end``.

    Raises:
        ValueError: If ``count``, ``step_ms`` or ``jitter_ms`` is negative.
    """
    if count < 0:
        raise ValueError("count must be >= 0")
    if step_ms < 0:
        raise ValueError("step_ms must be >= 0")
    if jitter_ms < 0:
        raise ValueError("jitter_ms must be >= 0")
    if count == 0:
        return []

    # Normalised through the model's own helper rather than re-implementing "naive means UTC":
    # two definitions of that rule in one codebase is exactly how a mixed-timezone corpus gets
    # created, and a mixed-timezone corpus makes ``since``/``until`` scans quietly wrong.
    anchor = ANCHOR_TS if end is None else _to_utc(end)

    rng = random.Random(seed)
    timestamps = _build_timestamps(
        count, end=anchor, step_ms=step_ms, jitter_ms=jitter_ms, rng=rng
    )
    return [
        generate_one(rng, ts=ts, index=index, seed=seed)
        for index, ts in enumerate(timestamps)
    ]


# ---------------------------------------------------------------------------------------------
# Ground truth
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CorpusCounts:
    """Brute-force tallies over a corpus — the expected answers for stats and search checks.

    Frozen because it is an oracle: something that gets compared against, never adjusted to make
    a comparison pass.

    Attributes:
        total: Number of entries tallied.
        by_level: ``level -> count``, keyed by the wire string (``"ERROR"``), not the enum
            member, so it compares directly against a decoded JSON response body.
        by_service: ``service -> count``.
        by_host: ``host -> count``.
        top_error_messages: ``(message, count)`` for ERROR/FATAL entries only, most frequent
            first, ties broken lexicographically. The tiebreak is what makes the ranking a
            *total* order: ``Counter.most_common`` breaks ties by insertion order, which depends
            on corpus order and would make the oracle disagree with itself across seeds.
        earliest: Oldest ``ts``, or ``None`` for an empty corpus.
        latest: Newest ``ts``, or ``None`` for an empty corpus.
    """

    total: int
    by_level: dict[str, int]
    by_service: dict[str, int]
    by_host: dict[str, int]
    top_error_messages: list[tuple[str, int]]
    earliest: datetime | None
    latest: datetime | None


def expected_counts(entries: Sequence[LogEntry], *, top_n: int = 10) -> CorpusCounts:
    """Tally ``entries`` the dumb way — one pass, plain counters, no indexes, no shortcuts.

    **This implementation must stay naive.** It is the oracle that C11's optimized
    ``compute_stats`` (which reads the store's secondary indexes and bucketed aggregates) is
    graded against, and an oracle that shares an optimisation with the thing it grades cannot
    catch a bug in that optimisation — the two would simply be wrong together. Slow and
    obviously-correct is the entire point; it only ever runs over a few thousand entries in a
    test or a verifier.

    Only *observed* keys are reported: a level that never occurred is absent rather than present
    with a zero. Consumers that want zero-filled buckets (the stats snapshot does) fill them in
    themselves — inventing zeros here would hide the difference between "no such level" and
    "level with no entries" in the very place that difference is being checked.

    Args:
        entries: The corpus to tally. Any sequence of entries — a generated corpus, a filtered
            subset, or entries read back from the API.
        top_n: How many error messages to keep in ``top_error_messages``.
    """
    levels: Counter[str] = Counter()
    services: Counter[str] = Counter()
    hosts: Counter[str] = Counter()
    error_messages: Counter[str] = Counter()
    earliest: datetime | None = None
    latest: datetime | None = None

    for entry in entries:
        levels[entry.level.value] += 1
        services[entry.service] += 1
        hosts[entry.host] += 1
        if entry.level in ERROR_LEVELS:
            error_messages[entry.message] += 1
        if earliest is None or entry.ts < earliest:
            earliest = entry.ts
        if latest is None or entry.ts > latest:
            latest = entry.ts

    ranked = sorted(error_messages.items(), key=lambda item: (-item[1], item[0]))
    return CorpusCounts(
        total=len(entries),
        by_level=dict(levels),
        by_service=dict(services),
        by_host=dict(hosts),
        top_error_messages=ranked[: max(0, top_n)],
        earliest=earliest,
        latest=latest,
    )
