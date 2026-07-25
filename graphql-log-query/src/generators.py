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

Standard library only, plus :class:`src.db.models.LogRecord`. It must never import
:mod:`src.db.session`, :mod:`src.db.repository` or :mod:`src.main` — those import *this*, and a
cycle would break startup.
"""

from __future__ import annotations

import random
from bisect import bisect_right
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from itertools import accumulate
from types import MappingProxyType
from typing import Any

from src.db.models import LEVEL_MAX_LENGTH, SERVICE_MAX_LENGTH, LogRecord

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


def _assign_trace_ids(count: int, rng: random.Random) -> list[str | None]:
    """Assign shared trace ids to ~:data:`TRACE_ID_RATIO` of the indices, ``None`` to the rest.

    Members of a group are drawn from a **shuffled** index list rather than from a contiguous
    run, so a trace's records are interleaved with unrelated traffic exactly as they are in a real
    system serving concurrent requests. That matters for C5: with contiguous groups, a
    ``related_logs`` lookup could be satisfied by whatever rows happened to be adjacent, and a
    batching bug that returned neighbours instead of correlated rows would still look right.

    Leftovers are left uncorrelated rather than forced into a group: when fewer than
    :data:`TRACE_GROUP_MIN` indices remain the loop stops, because a one-record "group" would make
    ``related_logs`` return a single row and prove nothing.
    """
    trace_ids: list[str | None] = [None] * count
    pool = list(range(count))
    rng.shuffle(pool)
    del pool[int(count * TRACE_ID_RATIO):]

    cursor = 0
    while len(pool) - cursor >= TRACE_GROUP_MIN:
        size = min(rng.randint(TRACE_GROUP_MIN, TRACE_GROUP_MAX), len(pool) - cursor)
        # 64 bits rendered as 16 hex characters: fixed width, well inside the column's 64, and a
        # pure function of the RNG state so the same seed names the same traces every run.
        trace_id = f"{rng.getrandbits(64):016x}"
        for index in pool[cursor : cursor + size]:
            trace_ids[index] = trace_id
        cursor += size

    return trace_ids


def generate_log_records(
    count: int,
    seed: int,
    end_time: datetime,
    window: timedelta = DEFAULT_WINDOW,
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

    Returns:
        ``count`` records in strictly ascending timestamp order, every timestamp inside
        ``[end_time - window, end_time)``.

    Raises:
        ValueError: If ``count`` is negative, ``window`` is not positive, or the window is too
            narrow to hold ``count`` distinct microsecond instants.
    """
    if count < 0:
        raise ValueError(f"count must be >= 0, got {count}")
    if window <= timedelta(0):
        raise ValueError(f"window must be positive, got {window!r}")
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
    trace_ids = _assign_trace_ids(count, rng)

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
