"""Deterministic seeded log corpus — the project's **ground truth**.

There is no upstream log source in this project (the spec leaves it unspecified), so the store is
filled at startup from this module: ``SEED_ENTRIES`` rows, written by
:meth:`src.db.session.Database.seed_if_empty`. That makes this file far more than a fixture
factory — it is the *oracle*. An integration test computes the expected result of a filter by
running the same filter over the objects this function returned, and then asserts the database
agrees. C12's E2E verifier does the same thing against the live API over HTTP.

That only works if the function is **pure**, so the contract is stated as an executable one:

1. **Every draw comes from a private** ``random.Random(seed)``. Never the module-level
   :mod:`random`, which is process-wide state any import is free to reseed — a corpus that
   depended on it would change depending on what else happened to be imported first.
2. **The wall clock is never read.** ``end_time`` is a **required parameter with no default**, so
   there is no code path in which this function can consult :func:`datetime.now`. Production
   passes ``datetime.now(timezone.utc)`` from the lifespan; tests and the verifier pass a fixed
   instant. This is the property that lets a test compute the expected answer to
   ``start_time`` / ``search_text`` / ``level`` filtering *without querying the database at all* —
   and therefore lets the assertion fail for the right reason instead of comparing the server to
   itself.
3. **The draw order per record is fixed.** Reordering the draws in :func:`_draw_slots` or moving
   the trace-id assignment relative to the timestamp walk changes every corpus from every seed.
   Both are marked below.

Standard library only, plus the value objects in :mod:`src.db.models`. It must never import
:mod:`src.db.session`, :mod:`src.db.repository` or :mod:`src.main` — those import *this*, and a
cycle would break startup.

.. rubric:: Two corpora, one contract (C10)

:func:`generate_log_records` builds the log corpus; :func:`generate_event_corpus` builds the
correlated e-commerce corpus (spec §3 Feature Area A) that fills ``order_events``,
``payment_events`` and ``user_events``. Both obey the three rules above identically — private
``Random(seed)``, required ``end_time``, fixed draw order — so ``SEED_ENTRIES`` and ``SEED_ORDERS``
can be changed independently without disturbing the corpus the other one produces.

The two corpora meet in exactly one place, and it is a **parameter, not a shared global**:
:func:`generate_log_records` accepts the event corpus's trace ids as ``order_traces`` and files log
lines under :data:`ORDER_TRACE_LOG_RATIO` of them (see :func:`order_traces_with_logs`). That is what
makes a service log line part of an order's story, and what makes ``correlatedEvents(traceId:)``
return all four ``__typename``s rather than three. The dependency runs one way only — the event
corpus never reads the log corpus — and it is opt-in: omit the argument and the log corpus is
byte-identical to what it was before C10, which is why every caller that only wants log rows is
untouched by it.

The second one adds a fourth property the first does not need: **coherence**. An order's status
events, its payment events and the acting user's activity are one interleaved timeline sharing one
``trace_id``, and statuses advance along a declared lifecycle rather than being drawn independently.
That is what makes ``correlatedEvents(traceId:)`` return a story instead of three unrelated rows,
and what lets a test assert on the *shape* of a trace rather than merely on its size.
"""

from __future__ import annotations

import random
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import accumulate
from types import MappingProxyType
from typing import Any

from src.db.models import (
    LEVEL_MAX_LENGTH,
    ORDER_ID_MAX_LENGTH,
    ORDER_STATUS_MAX_LENGTH,
    PAYMENT_METHOD_MAX_LENGTH,
    PAYMENT_OUTCOME_MAX_LENGTH,
    SERVICE_MAX_LENGTH,
    TRACE_ID_MAX_LENGTH,
    USER_ACTIVITY_MAX_LENGTH,
    USER_ID_MAX_LENGTH,
    LogRecord,
    OrderEventRecord,
    PaymentEventRecord,
    UserEventRecord,
)

# ---------------------------------------------------------------------------------------------
# Vocabulary
#
# Module-level and public. The unit tests assert that every generated record draws from these
# exact pools, and C12's verifier builds its GraphQL probe queries out of them — `service:
# "auth-service"` is only a useful filter probe if `auth-service` is guaranteed to be in the
# corpus. All tuples, i.e. immutable: a shared mutable list a caller could append to would
# silently change the ground truth for everyone else in the process.
# ---------------------------------------------------------------------------------------------

#: The ten emitting services. Named after the e-commerce domain C10 extends into (`order-service`,
#: `payment-service`, `user-service`) so the log corpus and the order/user/payment event corpus
#: describe one plausible system rather than two unrelated ones.
SERVICES: tuple[str, ...] = (
    "auth-service",
    "api-gateway",
    "order-service",
    "payment-service",
    "user-service",
    "inventory-service",
    "notification-service",
    "search-service",
    "shipping-service",
    "analytics-service",
)

#: The severity roster, in ascending order of severity. These exact strings become C3's
#: strongly-typed ``LogLevel`` GraphQL enum members, so this tuple — not a hand-written list in
#: the schema module — is the single definition of what a level can be. They are the Python
#: :mod:`logging` names, which is also what ``LOG_LEVEL`` accepts, so the vocabulary an operator
#: reads in a log line is the same one they configure the process with.
LOG_LEVELS: tuple[str, ...] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

#: The severity mix of a service that is mostly working: INFO-heavy, a fifth of the tail at
#: DEBUG, and **10% ERROR**. The ERROR share is a deliberate target, not a leftover: C4's
#: ``logStats.errorCount`` is one of the spec's own verification commands, and a corpus with
#: 0.5% errors makes it look like a rounding artefact while a corpus with 30% makes it look like
#: an outage. The weights are checked to sum to 1.0 at import — see :func:`_validate_vocabulary`.
LEVEL_WEIGHTS: Mapping[str, float] = MappingProxyType(
    {
        "DEBUG": 0.15,
        "INFO": 0.62,
        "WARNING": 0.12,
        "ERROR": 0.10,
        "CRITICAL": 0.01,
    }
)

#: Interpolation vocabularies. Small, fixed pools rather than random strings: the point is text
#: that looks real but stays low-cardinality enough that a ``search_text`` substring filter
#: returns a meaningful number of rows instead of exactly one or all of them.
HOSTS: tuple[str, ...] = ("node-1", "node-2", "node-3", "node-4", "node-5", "node-6")
REGIONS: tuple[str, ...] = ("us-east-1", "eu-west-1", "ap-south-1")
USER_IDS: tuple[str, ...] = (
    "u-1001", "u-1002", "u-1044", "u-2087", "u-3310", "u-4471", "u-5560", "u-7702",
)
ORDER_IDS: tuple[str, ...] = (
    "ord-52001", "ord-52014", "ord-52099", "ord-53120", "ord-53877", "ord-54312",
)
ENDPOINTS: tuple[str, ...] = (
    "/graphql",
    "/api/v1/orders",
    "/api/v1/users",
    "/api/v1/payments",
    "/api/v1/search",
    "/health",
)
TABLES: tuple[str, ...] = ("sessions", "accounts", "payments", "audit_log", "api_tokens")
QUEUES: tuple[str, ...] = ("ingest", "notifications", "billing")
STATUS_CODES: tuple[int, ...] = (200, 201, 204, 400, 401, 404, 429, 500, 503)

#: Message templates per level, filled from the vocabularies above via :meth:`str.format`.
#:
#: TWO CHARACTERS IN HERE ARE LOAD-BEARING AND MUST NOT BE TIDIED AWAY: the literal ``%`` (in the
#: ``{percent}%`` templates) and the literal ``_`` (in ``user_id=`` and in the ``audit_log`` /
#: ``api_tokens`` table names). Both are ``LIKE`` metacharacters, and the repository escapes them
#: so that a client searching for ``%`` finds messages *containing a percent sign* rather than
#: every row in the table. That escaping is only provable against a corpus that actually contains
#: those characters — :func:`_validate_vocabulary` fails the import if a future edit removes them.
MESSAGE_TEMPLATES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "DEBUG": (
            "entering handler for {endpoint}",
            "cache lookup for key user_id={user_id}",
            "acquired pooled connection for table {table}",
            "serialized {items} rows in {latency}ms",
            "resolved region {region} from request context",
            "connection pool utilisation at {percent}% on {endpoint}",
        ),
        "INFO": (
            "request completed {endpoint} status=200 in {latency}ms",
            "user {user_id} authenticated successfully",
            "order {order_id} accepted for fulfilment",
            "published {items} events to queue {queue}",
            "cache hit ratio {percent}% over the last minute",
            "background compaction of {table} finished in {latency}ms",
            "session established for user_id={user_id} in {region}",
        ),
        "WARNING": (
            "slow query on {table} took {latency}ms",
            "retrying {endpoint} after a transient failure",
            "connection pool {percent}% saturated in {region}",
            "rate limit approaching for user {user_id}",
            "queue {queue} backlog grew to {items} messages",
            "clock drift detected against peer in {region}",
        ),
        "ERROR": (
            "upstream request to {endpoint} timed out after {latency}ms",
            "database connection refused for table {table}",
            "payment authorization declined for order {order_id}",
            "cache write failed for key user_id={user_id}",
            "disk usage exceeded {percent}% on the primary volume",
        ),
        "CRITICAL": (
            "primary replica lost, shutting down",
            "out of memory while serving {endpoint}",
            "unrecoverable corruption detected in table {table}",
        ),
    }
)

# ---------------------------------------------------------------------------------------------
# Corpus shape constants
# ---------------------------------------------------------------------------------------------

#: How far back from ``end_time`` the corpus reaches when the caller does not say. A day is wide
#: enough that a "last hour" filter returns a meaningful *partial* set rather than everything or
#: nothing, which is what makes a time-range assertion able to fail.
DEFAULT_WINDOW: timedelta = timedelta(hours=24)

#: Fraction of records that carry a ``trace_id``. Well short of 1.0 on purpose: C5 must return an
#: **empty list** from ``related_logs`` when ``trace_id`` is NULL (spec §2 item 17), and a corpus
#: where every row is correlated would leave that branch permanently unexercised.
TRACE_ID_RATIO = 0.6

#: Records per trace. A group of one would make ``related_logs`` return a single row — technically
#: correct and completely uninformative about whether the batching in C5 works — so groups are
#: always at least a pair.
TRACE_GROUP_MIN = 2
TRACE_GROUP_MAX = 5

#: Fraction of the **order** traces (see :func:`generate_event_corpus`) that a log trace group
#: adopts, so that some log lines genuinely belong to an order's story.
#:
#: .. rubric:: Why this exists rather than being left to chance
#:
#: ``Query.correlatedEvents(traceId:)`` is the only interface-typed field in the schema, and its
#: whole point is returning a **mix** of ``__typename``s. Three of the four come free — an order's
#: statuses, its payments and its user's activity already share one trace by construction. The
#: fourth, ``LogEntry``, only appears if a log row carries an order's trace id, and before this
#: constant existed that happened **by accident**: both generators are seeded from the same
#: ``RANDOM_SEED`` and both render ``getrandbits(64)``, so aligned positions in the two streams
#: produce identical ids — about fifteen of two hundred at the shipped defaults. A different
#: ``RANDOM_SEED`` or ``SEED_ENTRIES`` could have taken that to zero and quietly turned
#: ``... on LogEntry { message }`` into a fragment that never matches, with nothing failing.
#:
#: So the correlation is now *declared*: :func:`order_traces_with_logs` names which order traces get
#: log lines, :func:`_assign_trace_ids` files them there, and an id drawn for an independent log
#: trace that happens to equal an order's is **rejected and redrawn** — so the only correlation in
#: the corpus is the one this constant asks for, and the count is a function of the ratio rather
#: than of the seed.
#:
#: A quarter rather than all of them, because the *un*-correlated populations are load-bearing too:
#: C5's ``related_logs`` needs traces that only log rows carry, spec §2 item 17 needs rows whose
#: ``trace_id`` is NULL, and a corpus where every trace led to an order would be a corpus in which
#: "this log line is part of an order" said nothing. At the shipped 200 orders that is 50 correlated
#: traces against roughly 340 log-only ones and 800 untraced rows.
ORDER_TRACE_LOG_RATIO = 0.25

#: Fraction of records carrying a ``metadata`` object. The rest store SQL ``NULL``, so both
#: branches of every consumer that reads it are exercised by the seeded corpus alone.
#:
#: "SQL ``NULL``" is a claim about the column, not about this constant: it holds only because
#: :class:`~src.db.models.LogEntryORM` declares ``JSONB(none_as_null=True)``. Without that flag a
#: ``None`` here is stored as the JSONB scalar ``'null'`` — still ``None`` when read back into
#: Python, so this comment would go on reading true while every ``WHERE metadata IS NULL`` in the
#: system matched nothing.
METADATA_RATIO = 0.7

# ---------------------------------------------------------------------------------------------
# Weighted level selection
#
# Precomputed cumulative distribution plus one `rng.random()` and a binary search per record.
# `random.choices` would work too but allocates a list per call, and this runs SEED_ENTRIES times
# inside the container's healthcheck start_period.
# ---------------------------------------------------------------------------------------------

_LEVEL_SEQUENCE: tuple[str, ...] = tuple(LEVEL_WEIGHTS)

#: Cumulative weights with the final bucket's upper edge **forced to exactly 1.0**. Binary floating
#: point cannot represent 0.62/0.12/0.10/0.01 exactly, so the accumulated total lands a few ULPs
#: away from 1.0; if it landed *below*, a draw in the gap would bisect past the end of the table
#: and raise IndexError roughly once in a few billion records. Pinning the edge removes the
#: possibility rather than making it rare.
_LEVEL_CUMULATIVE: tuple[float, ...] = (
    *tuple(accumulate(LEVEL_WEIGHTS.values()))[:-1],
    1.0,
)

#: Every slot name a template may reference. Drawn **in full for every record** regardless of
#: which template was chosen, so each record consumes exactly the same number of RNG values. That
#: uniformity is what keeps a corpus stable when a template is added to one level's pool: the
#: draw *shape* per record does not depend on the template, only the rendered text does.
_SLOT_NAMES: tuple[str, ...] = (
    "endpoint", "user_id", "order_id", "table", "queue", "region", "latency", "items", "percent",
)


def _draw_slots(rng: random.Random) -> dict[str, Any]:
    """Draw one value for every template slot from the seeded RNG.

    THE ORDER OF THESE LINES IS THE RNG CONSUMPTION SEQUENCE. Reordering them changes every
    corpus generated from every seed, which would silently invalidate any expected value written
    down elsewhere. Add new slots at the end.
    """
    return {
        "endpoint": rng.choice(ENDPOINTS),
        "user_id": rng.choice(USER_IDS),
        "order_id": rng.choice(ORDER_IDS),
        "table": rng.choice(TABLES),
        "queue": rng.choice(QUEUES),
        "region": rng.choice(REGIONS),
        "latency": rng.randint(3, 2500),
        "items": rng.randint(1, 500),
        "percent": rng.randint(11, 99),
    }


def _validate_vocabulary() -> None:
    """Fail **at import** if the tables above violate an invariant something else depends on.

    A weight table that quietly sums to 0.97, a typo in a template (``{sevrice}``) or a service
    name one character too long for its column would otherwise surface as a skewed corpus, a
    ``KeyError`` deep inside container startup, or a driver error on the first INSERT of a
    thousand-row chunk. Import-time validation turns all three into an immediate crash naming the
    offending value.
    """
    if set(LEVEL_WEIGHTS) != set(LOG_LEVELS):
        raise ValueError("LEVEL_WEIGHTS must assign a weight to every level in LOG_LEVELS")
    if any(weight <= 0 for weight in LEVEL_WEIGHTS.values()):
        raise ValueError("LEVEL_WEIGHTS entries must all be positive")
    # Tolerance rather than `== 1.0`: the literals are decimal, the arithmetic is binary. 1e-9 is
    # far tighter than any real weighting mistake and far looser than float noise.
    total = sum(LEVEL_WEIGHTS.values())
    if abs(total - 1.0) > 1e-9:
        raise ValueError(f"LEVEL_WEIGHTS must sum to 1.0, got {total!r}")
    # The ERROR share is asserted here as well as in the unit suite because C4's `errorCount` is
    # one of the spec's own verification commands and a corpus that made it meaningless would
    # otherwise only be noticed by reading the numbers.
    if not 0.08 <= LEVEL_WEIGHTS["ERROR"] <= 0.12:
        raise ValueError(
            f"ERROR weight {LEVEL_WEIGHTS['ERROR']!r} is outside the 8-12% band the corpus "
            "promises; logStats.errorCount is a spec verification command and needs to be a "
            "meaningful number"
        )

    if set(MESSAGE_TEMPLATES) != set(LOG_LEVELS):
        raise ValueError("MESSAGE_TEMPLATES must cover every level in LOG_LEVELS")

    # Render every template against a real slot draw. Stricter than checking a hardcoded name
    # list, because it validates the templates against the function that actually fills them —
    # the two cannot drift. The local Random(0) is private and never touches a corpus.
    probe = _draw_slots(random.Random(0))
    if set(probe) != set(_SLOT_NAMES):
        raise ValueError("_draw_slots and _SLOT_NAMES disagree on the slot vocabulary")
    rendered: list[str] = []
    for level, templates in MESSAGE_TEMPLATES.items():
        if not templates:
            raise ValueError(f"no message templates for level {level}")
        for template in templates:
            try:
                rendered.append(template.format(**probe))
            except (KeyError, IndexError, ValueError) as exc:
                raise ValueError(f"unfillable message template {template!r}: {exc}") from exc

    # The LIKE-metacharacter guarantee, in executable form. See the note on MESSAGE_TEMPLATES.
    if not any("%" in text for text in rendered):
        raise ValueError(
            "no message template renders a literal '%': the repository's LIKE-escaping test "
            "proves that searching for '%' matches messages CONTAINING a percent sign rather "
            "than every row, and it cannot prove that against a corpus with no percent signs"
        )
    if not any("_" in text for text in rendered):
        raise ValueError(
            "no message template renders a literal '_': same reason as '%' above — '_' is the "
            "LIKE single-character wildcard and the escaping test needs real material"
        )

    if len(set(SERVICES)) != len(SERVICES) or not SERVICES:
        raise ValueError("SERVICES must be a non-empty tuple of unique names")
    # Column caps, checked against the model rather than restated: a generated record that could
    # not have been POSTed through `createLog` would be a contract inconsistency, and the failure
    # would land on the seeding INSERT at container startup.
    for service in SERVICES:
        if len(service) > SERVICE_MAX_LENGTH:
            raise ValueError(f"service {service!r} exceeds {SERVICE_MAX_LENGTH} characters")
    for level in LOG_LEVELS:
        if len(level) > LEVEL_MAX_LENGTH:
            raise ValueError(f"level {level!r} exceeds {LEVEL_MAX_LENGTH} characters")


_validate_vocabulary()


def _pick_level(rng: random.Random) -> str:
    """Draw one level from :data:`LEVEL_WEIGHTS` using one RNG value and a binary search."""
    return _LEVEL_SEQUENCE[bisect_right(_LEVEL_CUMULATIVE, rng.random())]


def _build_timestamps(
    count: int, *, end_time: datetime, window: timedelta, rng: random.Random
) -> list[datetime]:
    """Build ``count`` **strictly ascending** timestamps inside ``[end_time - window, end_time)``.

    Stratified rather than uniform-and-sorted: the window is cut into ``count`` equal slices of
    whole microseconds and one instant is drawn inside each. That buys three properties at once,
    and all three are relied on elsewhere:

    * **In range by construction.** The largest offset is ``count * slice - 1``, which is at most
      ``window - 1`` microsecond, so no draw can escape the window and no clamp is needed.
    * **Strictly increasing**, because the slices are disjoint. Seeding inserts in this order, so
      ``BIGSERIAL`` ids ascend with time and the ``(timestamp, id)`` tiebreak is deterministic.
    * **Evenly spread**, so a "last hour" filter over a 24-hour corpus returns roughly a
      twenty-fourth of it — a meaningful partial set. Sampling uniformly and sorting would give
      the same spread on average but would allow duplicate instants and clumping, and a duplicate
      timestamp makes the *oracle's* ordering ambiguous even though the database's is not.
    """
    if count == 0:
        return []

    window_us = window // timedelta(microseconds=1)
    if window_us < count:
        raise ValueError(
            f"window {window!r} is only {window_us} microseconds wide, which cannot hold "
            f"{count} strictly-increasing timestamps; widen the window or lower the count"
        )

    slice_us = window_us // count
    start = end_time - window
    return [
        start + timedelta(microseconds=index * slice_us + rng.randrange(slice_us))
        for index in range(count)
    ]


def order_traces_with_logs(order_traces: Sequence[str]) -> tuple[str, ...]:
    """Which of ``order_traces`` the log corpus files log lines under. A pure function of its input.

    No RNG and no clock, so this is *declarative*: a test, C12's E2E verifier and the C13 dashboard
    can all name a trace that is **guaranteed** to answer ``correlatedEvents`` with all four
    ``__typename``s, instead of scanning the corpus for one that happens to. Scanning is what the
    seed-alignment accident used to require, and a scan that finds nothing looks exactly like a
    resolver that returns nothing.

    Selected by an even stride rather than from the front, because ``order_traces`` arrives in
    first-appearance order — i.e. oldest order first. A prefix would correlate only the oldest
    quarter of the corpus and leave every recent order in a dashboard view with no log lines at all.
    The stride always includes index 0, so ``order_id_for(0)``'s trace — the one order that is
    nameable in any corpus from any seed — is always one of them.

    Args:
        order_traces: The event corpus's trace ids, from :meth:`EventCorpus.trace_ids`. Duplicates
            are collapsed (first occurrence wins): two log groups sharing one order trace would
            build a "group" of up to ``2 x TRACE_GROUP_MAX`` records and break the bound
            :func:`_assign_trace_ids` promises.

    Returns:
        The selected trace ids, distinct, in the order they appeared — **empty only for an empty
        input**. The floor of one is doing real work: ``int(1 * 0.25)`` and ``int(3 * 0.25)`` are
        both 0, so a small order corpus would otherwise correlate nothing at all and
        ``correlatedEvents`` would quietly go back to three ``__typename``s, which is exactly the
        silent zero :data:`ORDER_TRACE_LOG_RATIO` exists to make impossible.
    """
    unique = tuple(dict.fromkeys(order_traces))
    if not unique:
        return ()
    wanted = max(1, int(len(unique) * ORDER_TRACE_LOG_RATIO))
    return tuple(unique[index * len(unique) // wanted] for index in range(wanted))


def _assign_trace_ids(
    count: int, rng: random.Random, order_traces: Sequence[str] = ()
) -> list[str | None]:
    """Assign shared trace ids to ~:data:`TRACE_ID_RATIO` of the indices, ``None`` to the rest.

    Members of a group are drawn from a **shuffled** index list rather than from a contiguous
    run, so a trace's records are interleaved with unrelated traffic exactly as they are in a real
    system serving concurrent requests. That matters for C5: with contiguous groups, a
    ``related_logs`` lookup could be satisfied by whatever rows happened to be adjacent, and a
    batching bug that returned neighbours instead of correlated rows would still look right.

    Leftovers are left uncorrelated rather than forced into a group: when fewer than
    :data:`TRACE_GROUP_MIN` indices remain the loop stops, because a one-record "group" would make
    ``related_logs`` return a single row and prove nothing.

    .. rubric:: Adopting order traces (C10)

    When ``order_traces`` is supplied, the first ``len(order_traces_with_logs(order_traces))``
    groups take an order's trace id instead of their own freshly drawn one. Group *n* takes selected
    trace *n*, so the ids stay distinct and every group keeps a size inside
    ``[TRACE_GROUP_MIN, TRACE_GROUP_MAX]``. Which *records* those groups hold is unchanged: the pool
    is already shuffled, so "the first groups" is a random set of positions and the adoption
    introduces no positional bias.

    Two details that are easy to get wrong and are therefore stated:

    * **The draw happens either way.** A group that adopts still consumes its ``getrandbits`` draw,
      the same discipline :func:`_draw_slots` follows — the branch must not change how many values
      the next group starts from.
    * **A drawn id that collides with *any* order trace is rejected and redrawn.** Both generators
      run on the same seed and both render ``getrandbits(64)``, so aligned positions produce equal
      ids; without the rejection an independent log trace could land on an order's id and either
      merge two groups into one over-sized trace or manufacture a correlation nobody asked for. The
      loop cannot fire at all when ``order_traces`` is empty, which is what keeps the default corpus
      byte-identical to the pre-C10 one.

    Args:
        count: Corpus size.
        rng: The caller's private RNG. Consumed in a fixed order — see the note above.
        order_traces: Order trace ids to correlate a fraction of the groups with. Empty by default,
            i.e. no correlation and no behaviour change.

    Returns:
        ``count`` entries, each a trace id or ``None``. A corpus with fewer trace groups than the
        selection asks for adopts as many as it has and drops the rest, which only happens in
        degenerate configurations (far more orders than log rows).
    """
    trace_ids: list[str | None] = [None] * count
    pool = list(range(count))
    rng.shuffle(pool)
    del pool[int(count * TRACE_ID_RATIO):]

    adopted = order_traces_with_logs(order_traces)
    reserved = frozenset(order_traces)

    cursor = 0
    group = 0
    while len(pool) - cursor >= TRACE_GROUP_MIN:
        size = min(rng.randint(TRACE_GROUP_MIN, TRACE_GROUP_MAX), len(pool) - cursor)
        # 64 bits rendered as 16 hex characters: fixed width, well inside the column's 64, and a
        # pure function of the RNG state so the same seed names the same traces every run.
        drawn = f"{rng.getrandbits(64):016x}"
        # The rejection. Never runs without `order_traces`; runs a handful of times with it.
        while drawn in reserved:
            drawn = f"{rng.getrandbits(64):016x}"
        trace_id = adopted[group] if group < len(adopted) else drawn
        for index in pool[cursor : cursor + size]:
            trace_ids[index] = trace_id
        cursor += size
        group += 1

    return trace_ids


def generate_log_records(
    count: int,
    seed: int,
    end_time: datetime,
    window: timedelta = DEFAULT_WINDOW,
    *,
    order_traces: Sequence[str] = (),
) -> list[LogRecord]:
    """Generate a reproducible corpus of ``count`` records, **oldest first**.

    Args:
        count: How many records. ``0`` returns ``[]`` — the compose ``test`` service runs with
            ``SEED_ENTRIES=0``, so the empty corpus is a normal configuration rather than an edge
            case.
        seed: Seeds the private :class:`random.Random`. Same ``(count, seed, end_time, window)``
            in, equal corpus out, in any process.
        end_time: Exclusive upper bound on the generated timestamps. **Required, with no
            default**, which is the structural half of "this function never reads the clock":
            there is no code path that could fall back to :func:`datetime.now`. A naive value is
            interpreted as UTC. Production passes ``datetime.now(timezone.utc)`` from the
            lifespan; every test and the E2E verifier pass a fixed instant.
        window: How far back from ``end_time`` the corpus reaches.
        order_traces: The e-commerce corpus's trace ids (:meth:`EventCorpus.trace_ids`).
            :data:`ORDER_TRACE_LOG_RATIO` of them are adopted by log trace groups, so those log
            lines are part of an order's story and ``correlatedEvents(traceId:)`` returns a
            ``LogEntry`` alongside the order, payment and user events.

            **Threaded as an argument rather than read from a module global on purpose.** It is what
            keeps this function pure: the same arguments still produce the same corpus, the caller
            that wants the correlation has to ask for it, and the oracle a test regenerates is the
            corpus the seeder wrote as long as both pass the same list. The default ``()``
            reproduces the uncorrelated corpus byte for byte.

            Correlation is by **id only**. An adopted group keeps its own timestamps, which are
            scattered across the whole window exactly as every other group's are (the pool is
            shuffled — see :func:`_assign_trace_ids`), so a correlated log line is not necessarily
            adjacent in time to the order's events. ``correlatedEvents`` joins on the trace id and
            orders the merged result by time, which is the relationship this models.

    Returns:
        ``count`` records in strictly ascending timestamp order, every timestamp inside
        ``[end_time - window, end_time)``.

    Raises:
        ValueError: If ``count`` is negative, ``window`` is not positive, the window is too narrow
            to hold ``count`` distinct microsecond instants, or an ``order_traces`` entry is blank
            or too long for the ``trace_id`` column.
    """
    if count < 0:
        raise ValueError(f"count must be >= 0, got {count}")
    if window <= timedelta(0):
        raise ValueError(f"window must be positive, got {window!r}")
    # Validated even when `count` is 0, so a caller passing an unusable trace id is told about it
    # rather than finding out from a driver error on the first seeding INSERT of a larger run.
    for trace in order_traces:
        if not trace or not trace.strip():
            raise ValueError("order_traces entries must be non-empty trace ids")
        if len(trace) > TRACE_ID_MAX_LENGTH:
            raise ValueError(
                f"order trace {trace!r} exceeds the trace_id column's {TRACE_ID_MAX_LENGTH} "
                "characters, so a log row carrying it could not be stored"
            )
    if count == 0:
        return []

    # Naive means UTC — the same rule the repository applies to filter bounds, stated here so a
    # caller cannot produce a corpus whose timestamps are compared against a `timestamptz` column
    # under the server's local TimeZone setting.
    anchor = end_time if end_time.tzinfo is not None else end_time.replace(tzinfo=timezone.utc)
    anchor = anchor.astimezone(timezone.utc)

    rng = random.Random(seed)

    # DRAW ORDER, and it is part of the contract: timestamps, then trace assignment, then the
    # per-record loop. Moving either of the first two changes every corpus from every seed.
    timestamps = _build_timestamps(count, end_time=anchor, window=window, rng=rng)
    trace_ids = _assign_trace_ids(count, rng, order_traces)

    records: list[LogRecord] = []
    for index, timestamp in enumerate(timestamps):
        level = _pick_level(rng)
        service = rng.choice(SERVICES)
        host = rng.choice(HOSTS)
        slots = _draw_slots(rng)
        message = rng.choice(MESSAGE_TEMPLATES[level]).format(**slots)

        # Every roll is taken unconditionally, then used conditionally, so that whether a record
        # carries metadata does not change how many RNG values the next record starts from.
        metadata_roll = rng.random()
        latency_roll = rng.random()
        status_roll = rng.random()
        status_code = rng.choice(STATUS_CODES)

        metadata: dict[str, Any] | None = None
        if metadata_roll < METADATA_RATIO:
            metadata = {"host": host, "region": slots["region"]}
            if latency_roll < 0.5:
                metadata["latency_ms"] = slots["latency"]
            if status_roll < 0.5:
                metadata["status_code"] = status_code

        records.append(
            LogRecord(
                timestamp=timestamp,
                service=service,
                level=level,
                message=message,
                metadata=metadata,
                trace_id=trace_ids[index],
            )
        )

    return records


# =================================================================================================
# C10 — the correlated e-commerce corpus (spec §3 Feature Area A)
#
# Everything below is public and every roster is a module constant, because three other places use
# them as GROUND TRUTH and none of them may hold a copy:
#
#   * `src.graphql.enums` builds `OrderStatus` / `PaymentMethod` / `PaymentOutcome` /
#     `UserActivity` from these tuples and fails at IMPORT if they ever drift apart;
#   * C11's tests compute expected filter results from them;
#   * C12's E2E verifier and the C13 dashboard build their probe queries and their filter dropdowns
#     out of them — `status: SHIPPED` is only a useful probe if SHIPPED is guaranteed to exist.
#
# All tuples and mapping proxies, i.e. immutable: a shared mutable list a caller could append to
# would silently change the ground truth for everyone else in the process.
# =================================================================================================

#: Which service emits each stream. Drawn from :data:`SERVICES` (checked at import) so the log
#: corpus and the event corpus describe **one** plausible system: a log line from ``order-service``
#: and an ``OrderEvent`` are two views of the same component.
ORDER_EVENT_SERVICE = "order-service"
PAYMENT_EVENT_SERVICE = "payment-service"
USER_EVENT_SERVICE = "user-service"

#: The order lifecycle vocabulary, in the order a healthy order passes through it (with the two
#: terminal failure states last). These exact strings become ``src.graphql.enums.OrderStatus``.
ORDER_STATUSES: tuple[str, ...] = (
    "CREATED",
    "PAID",
    "PACKED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "REFUNDED",
)

#: **The reason statuses progress instead of being drawn independently.** Each entry is a complete
#: path an order can take, always starting at ``CREATED``; an order picks one and emits its statuses
#: in that order, with strictly increasing timestamps. Drawing a status per event from a flat roster
#: would produce corpora in which an order is DELIVERED before it is PAID — which is not merely
#: untidy, it makes every downstream assertion vacuous: "the newest status for this order" would be
#: meaningless, C12's ``orderStatusStream`` would replay nonsense, and a dashboard funnel chart
#: would be noise.
#:
#: The union of these paths is exactly :data:`ORDER_STATUSES` (checked at import), so every declared
#: status is reachable and the roster is a promise the corpus keeps.
ORDER_LIFECYCLES: tuple[tuple[str, ...], ...] = (
    ("CREATED", "PAID", "PACKED", "SHIPPED", "DELIVERED"),
    ("CREATED", "PAID", "PACKED", "SHIPPED"),
    ("CREATED", "PAID", "PACKED"),
    ("CREATED", "PAID"),
    ("CREATED", "PAID", "REFUNDED"),
    ("CREATED", "CANCELLED"),
    ("CREATED",),
)

#: Payment instruments. Become ``src.graphql.enums.PaymentMethod``.
PAYMENT_METHODS: tuple[str, ...] = (
    "CARD",
    "PAYPAL",
    "APPLE_PAY",
    "BANK_TRANSFER",
    "GIFT_CARD",
)

#: What a payment attempt did. Become ``src.graphql.enums.PaymentOutcome``.
#:
#: These are **derived from the order's lifecycle, not drawn** — see :func:`_payment_plan`. A
#: CANCELLED order has a DECLINED payment; a REFUNDED order was AUTHORIZED and CAPTURED first.
#: Independent draws would produce a captured payment on a cancelled order, which is exactly the
#: incoherence this corpus exists not to have.
PAYMENT_OUTCOMES: tuple[str, ...] = ("AUTHORIZED", "CAPTURED", "DECLINED", "REFUNDED")

#: What a user did. Become ``src.graphql.enums.UserActivity``.
USER_ACTIVITIES: tuple[str, ...] = (
    "SIGNUP",
    "LOGIN",
    "BROWSE",
    "ADD_TO_CART",
    "CHECKOUT",
    "REVIEW",
    "LOGOUT",
)

#: Severity per order status. A **mapping rather than a draw**, because severity is a property of
#: what happened: a cancellation is a WARNING whether or not an RNG says so. It also makes the
#: ``level`` filter on ``orderEvents`` predictable enough to grade against the oracle exactly.
ORDER_STATUS_LEVELS: Mapping[str, str] = MappingProxyType(
    {
        "CREATED": "INFO",
        "PAID": "INFO",
        "PACKED": "INFO",
        "SHIPPED": "INFO",
        "DELIVERED": "INFO",
        "CANCELLED": "WARNING",
        "REFUNDED": "WARNING",
    }
)

#: Severity per payment outcome. A decline is the one genuinely bad thing in this domain, so it is
#: the one ERROR — which is what makes ``paymentEvents(filters: {level: ERROR})`` return the
#: declines and nothing else.
PAYMENT_OUTCOME_LEVELS: Mapping[str, str] = MappingProxyType(
    {
        "AUTHORIZED": "INFO",
        "CAPTURED": "INFO",
        "DECLINED": "ERROR",
        "REFUNDED": "WARNING",
    }
)

#: Severity per user activity. Browsing and logging out are DEBUG noise; the rest are INFO. Two
#: levels rather than one on purpose: a corpus where every user event shared a severity would make
#: a ``level`` filter on ``userEvents`` return either everything or nothing, and a test over it
#: could not fail.
USER_ACTIVITY_LEVELS: Mapping[str, str] = MappingProxyType(
    {
        "SIGNUP": "INFO",
        "LOGIN": "INFO",
        "BROWSE": "DEBUG",
        "ADD_TO_CART": "INFO",
        "CHECKOUT": "INFO",
        "REVIEW": "INFO",
        "LOGOUT": "DEBUG",
    }
)

#: Where the session came from. Lands in every event's ``metadata`` alongside a region, so C11 has
#: a real JSONB dimension to aggregate on and the C13 dashboard has something to break a series by.
ORDER_CHANNELS: tuple[str, ...] = ("web", "ios", "android", "partner-api")

#: Order ids are ``ord-60000`` upward, assigned by **index** rather than drawn — an order id has to
#: be unique per order, and a draw from a fixed pool would collide and merge two orders' histories.
#: Deriving it from the index also makes it predictable: ``order_id_for(0)`` is the oldest order in
#: any corpus from any seed, which is what lets the E2E verifier probe a known id.
ORDER_ID_BASE = 60_000

#: Fraction of orders whose events carry a ``metadata`` object; the rest store SQL ``NULL``. Rolled
#: **once per order**, so a whole session either has context or does not — which is how it behaves
#: in a real system (the SDK is configured or it is not) and which still leaves both branches of
#: every consumer exercised by the seeded corpus alone.
EVENT_METADATA_RATIO = 0.7

#: Fraction of orders whose acting user signs up first (so SIGNUP is reachable).
NEW_USER_RATIO = 0.2
#: Fraction of DELIVERED orders that get a review (so REVIEW is reachable).
REVIEW_RATIO = 0.5
#: Fraction of sessions that end with an explicit logout (so LOGOUT is reachable).
LOGOUT_RATIO = 0.6

#: Seconds between two consecutive events in one order's timeline. Bounded at both ends: a floor
#: above zero keeps an order's own events strictly increasing (so "the latest status" is
#: well-defined), and a ceiling is what makes :data:`ORDER_CLUSTER_MAX_SPAN` a provable bound
#: rather than a hope.
ORDER_STEP_MIN_SECONDS = 20
ORDER_STEP_MAX_SECONDS = 600

#: The longest timeline any order can produce: the fulfilled lifecycle plus a full user trail plus
#: every payment event. Enumerated rather than guessed — see :func:`_build_order_timeline`, which
#: raises if it is ever exceeded, so a new lifecycle path cannot quietly break the span bound.
MAX_EVENTS_PER_ORDER = 14

#: How far an order's cluster of events may extend past its start instant. ``MAX_EVENTS_PER_ORDER``
#: steps of at most ``ORDER_STEP_MAX_SECONDS`` is 2h20m, so three hours is a real bound with
#: headroom rather than a round number. It matters because order *starts* are drawn from a window
#: shortened by exactly this much: that is what guarantees no generated event can land after
#: ``end_time``, by construction rather than by clamping.
ORDER_CLUSTER_MAX_SPAN: timedelta = timedelta(hours=3)


def order_id_for(index: int) -> str:
    """The order id the ``index``-th generated order carries. A pure function of the index."""
    return f"ord-{ORDER_ID_BASE + index}"


@dataclass(frozen=True, slots=True)
class EventCorpus:
    """The three correlated streams :func:`generate_event_corpus` emits, each **oldest first**.

    One object rather than a 3-tuple so a caller cannot transpose two streams that happen to have
    compatible shapes, and so the seeder and every test refer to them by name.

    Each list is sorted by ``timestamp`` (ties broken by generation order, which the stable sort
    preserves), because seeding inserts in list order and ``BIGSERIAL`` therefore assigns ids in
    that order — which is what makes ``ORDER BY timestamp DESC, id DESC`` exactly the reverse of
    the generated list, for the event tables as it already is for ``log_entries``.

    No field carries a default: a mutable default on a dataclass is a ``ValueError`` at import on
    Python 3.11 (the guard is "is unhashable", so a ``list`` **or** a ``MappingProxyType`` trips
    it), and an import-time failure here takes the whole service down rather than one test.
    """

    orders: list[OrderEventRecord]
    payments: list[PaymentEventRecord]
    user_activity: list[UserEventRecord]

    def trace_ids(self) -> list[str]:
        """Every distinct ``trace_id`` in the corpus, in first-appearance order.

        One per order, so this is also the corpus's order count — and it is what a test uses to
        pick a trace whose correlated set spans all three streams.

        It is also the value handed to :func:`generate_log_records` as ``order_traces``: a strided
        :data:`ORDER_TRACE_LOG_RATIO` of these get log lines filed under them, which is what makes
        the fourth ``__typename`` show up in ``correlatedEvents``. First-appearance order is load
        bearing for that — :func:`order_traces_with_logs` strides over this sequence, so it must be
        oldest-order-first and stable, not a set.
        """
        seen: dict[str, None] = {}
        for record in self.orders:
            if record.trace_id is not None:
                seen.setdefault(record.trace_id, None)
        return list(seen)

    def total_events(self) -> int:
        """How many rows the whole corpus writes, across all three tables."""
        return len(self.orders) + len(self.payments) + len(self.user_activity)


def _validate_event_vocabulary() -> None:
    """Fail **at import** if the e-commerce rosters violate something else's assumption.

    Same register as :func:`_validate_vocabulary`: the alternative to failing here is a skewed
    corpus, a ``KeyError`` deep inside container startup, or a driver error on the first INSERT of a
    thousand-row chunk — all of them several layers from the typo that caused them.

    Raises:
        ValueError: If a roster has duplicates, a lifecycle does not start at ``CREATED``, the
            lifecycles do not cover every declared status, a level map is incomplete or names a
            severity outside :data:`LOG_LEVELS`, a service is not in :data:`SERVICES`, or any value
            is too long for the column that will hold it.
    """
    rosters: Mapping[str, tuple[str, ...]] = {
        "ORDER_STATUSES": ORDER_STATUSES,
        "PAYMENT_METHODS": PAYMENT_METHODS,
        "PAYMENT_OUTCOMES": PAYMENT_OUTCOMES,
        "USER_ACTIVITIES": USER_ACTIVITIES,
        "ORDER_CHANNELS": ORDER_CHANNELS,
    }
    for name, roster in rosters.items():
        if not roster:
            raise ValueError(f"{name} must not be empty")
        if len(set(roster)) != len(roster):
            raise ValueError(f"{name} contains duplicates: {roster!r}")

    # Every emitting service must be one the log corpus can also produce, or the two corpora
    # describe two different systems and `logs(service: "order-service")` would return log lines
    # about orders that no OrderEvent corresponds to.
    for service in (ORDER_EVENT_SERVICE, PAYMENT_EVENT_SERVICE, USER_EVENT_SERVICE):
        if service not in SERVICES:
            raise ValueError(
                f"event service {service!r} is not in SERVICES; the log corpus and the event "
                "corpus must describe one system, not two"
            )

    if not ORDER_LIFECYCLES:
        raise ValueError("ORDER_LIFECYCLES must not be empty")
    for lifecycle in ORDER_LIFECYCLES:
        if not lifecycle or lifecycle[0] != "CREATED":
            raise ValueError(f"lifecycle {lifecycle!r} must start at CREATED")
        unknown = set(lifecycle) - set(ORDER_STATUSES)
        if unknown:
            raise ValueError(f"lifecycle {lifecycle!r} uses statuses outside ORDER_STATUSES: {unknown}")
    # The roster is a promise: every declared status must actually be reachable, or a client
    # filtering on it gets an empty list that is indistinguishable from a quiet period.
    covered = {status for lifecycle in ORDER_LIFECYCLES for status in lifecycle}
    if covered != set(ORDER_STATUSES):
        raise ValueError(
            f"ORDER_LIFECYCLES cover {sorted(covered)} but ORDER_STATUSES declares "
            f"{sorted(ORDER_STATUSES)}; a status no lifecycle produces is a filter that can only "
            "ever return nothing"
        )

    level_maps: Mapping[str, tuple[Mapping[str, str], tuple[str, ...]]] = {
        "ORDER_STATUS_LEVELS": (ORDER_STATUS_LEVELS, ORDER_STATUSES),
        "PAYMENT_OUTCOME_LEVELS": (PAYMENT_OUTCOME_LEVELS, PAYMENT_OUTCOMES),
        "USER_ACTIVITY_LEVELS": (USER_ACTIVITY_LEVELS, USER_ACTIVITIES),
    }
    for name, (mapping, roster) in level_maps.items():
        if set(mapping) != set(roster):
            raise ValueError(f"{name} must assign a level to exactly its roster, got {sorted(mapping)}")
        outside = set(mapping.values()) - set(LOG_LEVELS)
        if outside:
            raise ValueError(
                f"{name} names severities outside LOG_LEVELS: {sorted(outside)}; the `level` "
                "column is shared with log_entries and the published LogLevel enum covers both"
            )

    # Column caps, checked against the model rather than restated. A generated value that could not
    # be stored would surface as a driver error on the first seeding INSERT.
    caps: tuple[tuple[str, tuple[str, ...], int], ...] = (
        ("ORDER_STATUSES", ORDER_STATUSES, ORDER_STATUS_MAX_LENGTH),
        ("PAYMENT_METHODS", PAYMENT_METHODS, PAYMENT_METHOD_MAX_LENGTH),
        ("PAYMENT_OUTCOMES", PAYMENT_OUTCOMES, PAYMENT_OUTCOME_MAX_LENGTH),
        ("USER_ACTIVITIES", USER_ACTIVITIES, USER_ACTIVITY_MAX_LENGTH),
    )
    for name, roster, maximum in caps:
        for value in roster:
            if len(value) > maximum:
                raise ValueError(f"{name} entry {value!r} exceeds {maximum} characters")
    for user_id in USER_IDS:
        if len(user_id) > USER_ID_MAX_LENGTH:
            raise ValueError(f"user id {user_id!r} exceeds {USER_ID_MAX_LENGTH} characters")
    # Checked at a realistic upper bound rather than at index 0: the id grows with the corpus.
    widest_order_id = order_id_for(1_000_000)
    if len(widest_order_id) > ORDER_ID_MAX_LENGTH:
        raise ValueError(f"order id {widest_order_id!r} exceeds {ORDER_ID_MAX_LENGTH} characters")

    longest_lifecycle = max(len(lifecycle) for lifecycle in ORDER_LIFECYCLES)
    if longest_lifecycle > MAX_EVENTS_PER_ORDER:
        raise ValueError("MAX_EVENTS_PER_ORDER cannot hold the longest declared lifecycle")
    span = timedelta(seconds=(MAX_EVENTS_PER_ORDER - 1) * ORDER_STEP_MAX_SECONDS)
    if span > ORDER_CLUSTER_MAX_SPAN:
        raise ValueError(
            f"an order's timeline can span {span} but ORDER_CLUSTER_MAX_SPAN is "
            f"{ORDER_CLUSTER_MAX_SPAN}; the shortened start window would stop guaranteeing that "
            "no generated event lands after end_time"
        )


_validate_event_vocabulary()


def _payment_plan(lifecycle: Sequence[str]) -> tuple[str, ...]:
    """The payment outcomes an order following ``lifecycle`` produces, in order.

    Derived rather than drawn — that derivation *is* the coherence requirement. The rules, and the
    incoherence each one prevents:

    * A **cancelled** order (one that never reached PAID) has a single DECLINED payment. Drawing
      independently would produce a captured payment on a cancelled order.
    * A **refunded** order was AUTHORIZED and CAPTURED before it was REFUNDED, because a refund of
      a payment that was never captured is not a thing that happens.
    * Any other order that reached **PAID** was AUTHORIZED then CAPTURED.
    * An order still sitting at **CREATED** has an AUTHORIZED payment and nothing more — the money
      is held, the fulfilment has not started.

    Every order therefore has at least one payment event, which is a deliberate invariant rather
    than a coincidence: ``orderEvents`` and ``paymentEvents`` are joined on ``order_id`` in C11, and
    a corpus where some orders had no payments at all would let a broken join look correct.
    """
    if "CANCELLED" in lifecycle:
        return ("DECLINED",)
    if "REFUNDED" in lifecycle:
        return ("AUTHORIZED", "CAPTURED", "REFUNDED")
    if "PAID" in lifecycle:
        return ("AUTHORIZED", "CAPTURED")
    return ("AUTHORIZED",)


def _user_trail(
    lifecycle: Sequence[str],
    *,
    new_user: bool,
    reviews: bool,
    logs_out: bool,
) -> tuple[str, ...]:
    """The activity trail the acting user leaves around one order, in order.

    Shaped by the same lifecycle, so the story reads: log in, browse, add to cart, and only *then*
    check out — and only check out at all if the order actually got past CREATED. The three
    booleans are what make SIGNUP, REVIEW and LOGOUT reachable; without them three quarters of
    :data:`USER_ACTIVITIES` would never appear in any corpus and a filter on them could not be
    tested against anything.
    """
    trail: list[str] = []
    if new_user:
        trail.append("SIGNUP")
    trail.extend(("LOGIN", "BROWSE", "ADD_TO_CART"))
    if len(lifecycle) > 1:
        # A checkout is what moves an order off CREATED. An order that never moved never had one.
        trail.append("CHECKOUT")
    if "DELIVERED" in lifecycle and reviews:
        trail.append("REVIEW")
    if logs_out:
        trail.append("LOGOUT")
    return tuple(trail)


def generate_event_corpus(
    count: int,
    seed: int,
    end_time: datetime,
    window: timedelta = DEFAULT_WINDOW,
) -> EventCorpus:
    """Generate ``count`` coherent order lifecycles as three correlated streams, **oldest first**.

    Same purity contract as :func:`generate_log_records`, and for the same reason: this is an
    oracle. Equal ``(count, seed, end_time, window)`` in, equal corpus out, in any process, with no
    wall-clock read on any path.

    .. rubric:: What "coherent" buys, concretely

    For each order the function builds ONE interleaved timeline and splits it across three streams:
    the acting user logs in and browses, the order is CREATED, the user checks out, the payment is
    AUTHORIZED, the order is PAID, the payment is CAPTURED, the order is PACKED / SHIPPED /
    DELIVERED. Every event in that timeline carries the **same** ``trace_id`` and strictly
    increasing timestamps. So:

    * ``correlatedEvents(traceId:)`` returns a story, and a test can assert the ``__typename`` mix
      and the ordering rather than merely that rows came back;
    * "the current status of order X" is well-defined (the newest ``OrderEvent`` for that id);
    * C11's order -> user and order -> payments traversals have something true to return, so a
      broken join produces a *wrong* answer rather than a plausible one.

    .. rubric:: THE DRAW ORDER IS PART OF THE CONTRACT

    Per order: ten scalar draws (trace id, user, lifecycle, method, channel, region, and four
    rolls), then :data:`MAX_EVENTS_PER_ORDER` step deltas — **always that many**, whatever the
    lifecycle turns out to need. Drawing a fixed number keeps each order's RNG consumption constant,
    which is what stops a new lifecycle path from shifting every later order in the corpus. Same
    discipline as :func:`_draw_slots` in the log generator, and the same reason.

    Args:
        count: How many orders. ``0`` returns an empty corpus — the compose ``test`` service runs
            with ``SEED_ORDERS=0``, so that is a normal configuration rather than an edge case.
        seed: Seeds the private :class:`random.Random`.
        end_time: Exclusive upper bound on every generated timestamp. **Required, with no default**
            — the structural half of "this function never reads the clock". Naive means UTC.
        window: How far back from ``end_time`` the corpus reaches. Order *starts* are drawn from
            ``window - ORDER_CLUSTER_MAX_SPAN`` so a whole timeline still fits inside the window.

    Returns:
        An :class:`EventCorpus` whose three lists are each sorted oldest-first.

    Raises:
        ValueError: If ``count`` is negative, or ``window`` is not wide enough to hold both an
            order cluster and ``count`` strictly-ascending start instants.
    """
    if count < 0:
        raise ValueError(f"count must be >= 0, got {count}")
    if count == 0:
        return EventCorpus(orders=[], payments=[], user_activity=[])

    start_window = window - ORDER_CLUSTER_MAX_SPAN
    if start_window <= timedelta(0):
        raise ValueError(
            f"window {window!r} is not wider than ORDER_CLUSTER_MAX_SPAN "
            f"({ORDER_CLUSTER_MAX_SPAN!r}), so an order's timeline could not fit inside it; "
            "widen the window"
        )

    anchor = end_time if end_time.tzinfo is not None else end_time.replace(tzinfo=timezone.utc)
    anchor = anchor.astimezone(timezone.utc)

    rng = random.Random(seed)

    # Starts are laid out with the same stratified walk the log corpus uses, over a window
    # shortened by one cluster span — which is what makes "no event lands after end_time" true by
    # construction rather than by a clamp nobody would notice failing.
    starts = _build_timestamps(
        count, end_time=anchor - ORDER_CLUSTER_MAX_SPAN, window=start_window, rng=rng
    )

    orders: list[OrderEventRecord] = []
    payments: list[PaymentEventRecord] = []
    user_activity: list[UserEventRecord] = []

    for index, start in enumerate(starts):
        _build_order_timeline(
            index=index,
            start=start,
            rng=rng,
            orders=orders,
            payments=payments,
            user_activity=user_activity,
        )

    # Each stream is generated order-by-order, so clusters overlap and the lists are not globally
    # sorted. Sorting on `timestamp` alone is enough AND is what the seeder needs: Python's sort is
    # stable, so records sharing an instant keep generation order, ids are assigned in list order,
    # and `(timestamp, id)` is therefore strictly increasing down each list — which makes
    # `ORDER BY timestamp DESC, id DESC` exactly `reversed(list)` even with a tie.
    orders.sort(key=lambda record: record.timestamp)
    payments.sort(key=lambda record: record.timestamp)
    user_activity.sort(key=lambda record: record.timestamp)

    return EventCorpus(orders=orders, payments=payments, user_activity=user_activity)


def _build_order_timeline(
    *,
    index: int,
    start: datetime,
    rng: random.Random,
    orders: list[OrderEventRecord],
    payments: list[PaymentEventRecord],
    user_activity: list[UserEventRecord],
) -> None:
    """Append one order's whole interleaved timeline onto the three streams.

    The draw order in the first block is the contract; see :func:`generate_event_corpus`. Nothing
    here reads the clock, and every timestamp is derived from ``start`` plus the step deltas drawn
    below, so the cluster cannot escape :data:`ORDER_CLUSTER_MAX_SPAN`.
    """
    trace_id = f"{rng.getrandbits(64):016x}"
    user_id = rng.choice(USER_IDS)
    lifecycle = rng.choice(ORDER_LIFECYCLES)
    method = rng.choice(PAYMENT_METHODS)
    channel = rng.choice(ORDER_CHANNELS)
    region = rng.choice(REGIONS)
    metadata_roll = rng.random()
    new_user_roll = rng.random()
    review_roll = rng.random()
    logout_roll = rng.random()
    # ALWAYS this many, whatever the lifecycle needs — see the draw-order note in the public
    # function's docstring. Unused deltas are discarded, which costs nothing and buys a corpus that
    # does not reshuffle when a lifecycle path is added.
    steps = [
        rng.randint(ORDER_STEP_MIN_SECONDS, ORDER_STEP_MAX_SECONDS)
        for _ in range(MAX_EVENTS_PER_ORDER)
    ]

    order_id = order_id_for(index)
    carries_metadata = metadata_roll < EVENT_METADATA_RATIO

    def context() -> dict[str, Any] | None:
        """A fresh metadata object per event, or ``None`` for the whole order.

        Fresh rather than shared: a frozen dataclass holds the dict by reference, and one shared
        instance across an order's events would mean a caller mutating one record's metadata
        silently rewriting its siblings' — in an object whose entire job is to be an immutable
        oracle.
        """
        if not carries_metadata:
            return None
        return {"channel": channel, "region": region}

    trail = _user_trail(
        lifecycle,
        new_user=new_user_roll < NEW_USER_RATIO,
        reviews=review_roll < REVIEW_RATIO,
        logs_out=logout_roll < LOGOUT_RATIO,
    )
    plan = _payment_plan(lifecycle)

    # The timeline, as a flat list of (stream, value) pairs in the order they happen. Building it
    # declaratively — rather than emitting as we branch — is what keeps "user checks out BEFORE the
    # payment is authorized BEFORE the order is PAID" readable as a single sequence.
    timeline: list[tuple[str, str]] = []

    # Everything up to and including adding to the cart happens before the order exists.
    head = [activity for activity in trail if activity in ("SIGNUP", "LOGIN", "BROWSE", "ADD_TO_CART")]
    timeline.extend(("user", activity) for activity in head)
    timeline.append(("order", "CREATED"))

    if "CHECKOUT" in trail:
        timeline.append(("user", "CHECKOUT"))

    # The payment and the status it unlocks, interleaved: authorize, then PAID, then capture.
    remaining_statuses = list(lifecycle[1:])
    remaining_payments = list(plan)

    if remaining_payments and remaining_payments[0] in ("AUTHORIZED", "DECLINED"):
        timeline.append(("payment", remaining_payments.pop(0)))

    if remaining_statuses and remaining_statuses[0] == "PAID":
        timeline.append(("order", remaining_statuses.pop(0)))
        if remaining_payments and remaining_payments[0] == "CAPTURED":
            timeline.append(("payment", remaining_payments.pop(0)))

    for status in remaining_statuses:
        # A refund is authorised by the payment processor before the order reflects it.
        if status == "REFUNDED" and remaining_payments and remaining_payments[0] == "REFUNDED":
            timeline.append(("payment", remaining_payments.pop(0)))
        timeline.append(("order", status))

    # Anything the plan did not place (it cannot happen today, and if a future lifecycle makes it
    # happen the events must still exist rather than be silently dropped).
    timeline.extend(("payment", outcome) for outcome in remaining_payments)

    timeline.extend(("user", activity) for activity in trail if activity in ("REVIEW", "LOGOUT"))

    if len(timeline) > MAX_EVENTS_PER_ORDER:
        raise ValueError(
            f"order {order_id} produced {len(timeline)} events but MAX_EVENTS_PER_ORDER is "
            f"{MAX_EVENTS_PER_ORDER}; raise it (and re-check ORDER_CLUSTER_MAX_SPAN) rather than "
            "letting a cluster overrun the window it was drawn to fit inside"
        )

    cursor = start
    for position, (stream, value) in enumerate(timeline):
        if stream == "order":
            orders.append(
                OrderEventRecord(
                    timestamp=cursor,
                    service=ORDER_EVENT_SERVICE,
                    level=ORDER_STATUS_LEVELS[value],
                    trace_id=trace_id,
                    order_id=order_id,
                    user_id=user_id,
                    status=value,
                    metadata=context(),
                )
            )
        elif stream == "payment":
            payments.append(
                PaymentEventRecord(
                    timestamp=cursor,
                    service=PAYMENT_EVENT_SERVICE,
                    level=PAYMENT_OUTCOME_LEVELS[value],
                    trace_id=trace_id,
                    order_id=order_id,
                    method=method,
                    outcome=value,
                    metadata=context(),
                )
            )
        else:
            user_activity.append(
                UserEventRecord(
                    timestamp=cursor,
                    service=USER_EVENT_SERVICE,
                    level=USER_ACTIVITY_LEVELS[value],
                    trace_id=trace_id,
                    user_id=user_id,
                    activity_type=value,
                    metadata=context(),
                )
            )
        cursor = cursor + timedelta(seconds=steps[position])
