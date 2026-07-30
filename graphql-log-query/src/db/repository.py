"""The filter -> SELECT builder every read path goes through.

Spec §2 items 18-22 in one place: filter by ``service``, ``level``, a time range and a substring
of ``message``; ignore whatever the client omitted; AND everything it supplied; cap the row count.

.. rubric:: Why the builders are module-level functions and not methods

:func:`build_log_select` and :func:`build_count_select` take a :class:`LogQuery` and some
:class:`~src.config.Settings` and return a statement. They touch no session and open no
connection, which means the whole of the filtering, clamping and escaping logic — the part that
is actually easy to get wrong — is unit-testable without a database. :class:`LogRepository` is
then a thin thing that executes them.

.. rubric:: Three invariants that live *here* rather than in a resolver

1. **The limit is clamped inside the builder.** Spec §2 item 22 says the cap applies "on every
   query path", and a clamp that a resolver applies is a clamp the next caller can forget — the
   DataLoader in C5, the cache warm path in C7, the E2E script. Putting it in the one function
   that constructs the statement makes "every path is capped" structurally true instead of
   conventionally true.
2. **``search_text`` is escaped for LIKE.** Not for injection — the pattern is a bind parameter,
   so injection was never possible — but for *meaning*: ``%`` and ``_`` are LIKE metacharacters,
   and an unescaped ``%`` turns a substring search into a wildcard that matches every row.
3. **Naive datetimes are normalised to UTC once**, in :func:`as_utc`. A naive value compared
   against a ``timestamptz`` column is interpreted by PostgreSQL in the *server's* TimeZone
   setting, so the same query would return different rows on a differently-configured server.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Select, distinct, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import ColumnElement

from src.config import Settings, get_settings
from src.db.models import LogEntryORM, OrderEventORM, PaymentEventORM, UserEventORM

#: The character that turns the next LIKE metacharacter into a literal. Backslash is PostgreSQL's
#: own default, so this only restates what the server would already do — but it is passed
#: explicitly via ``escape=`` anyway, because "the default happens to be what we want" is not a
#: property a reader can check and not one a future server setting is obliged to preserve.
LIKE_ESCAPE_CHARACTER = "\\"

#: The characters :func:`escape_like` neutralises, **escape character first**. The order is not
#: cosmetic: escaping ``%`` before ``\`` would leave the backslash this function just inserted to
#: be escaped again on the next pass, doubling it and breaking the pattern. Building the tuple
#: from :data:`LIKE_ESCAPE_CHARACTER` makes that ordering structural rather than remembered.
_LIKE_METACHARACTERS: tuple[str, ...] = (LIKE_ESCAPE_CHARACTER, "%", "_")

#: The severity ``logStats.errorCount`` counts, as stored in the ``level`` column.
#:
#: **ERROR only — CRITICAL is deliberately not folded in.** ``errorCount`` is one of the spec's own
#: verification commands (§5) and it names one severity; a client that wants "errors and worse"
#: sums the ``levelBreakdown``, which is why that field exists. Silently including CRITICAL would
#: make the headline number disagree with the breakdown printed beside it in the same response.
#:
#: A module constant rather than a literal inside the statement builder so the string appears once,
#: and so ``tests/unit/test_graphql_schema.py`` can pin it against ``LogLevel.ERROR`` — the
#: published enum member — instead of against a copy of itself. Not imported from
#: :mod:`src.graphql.enums`: the store must not depend on the API layer above it.
ERROR_LEVEL = "ERROR"


def escape_like(value: str) -> str:
    """Neutralise LIKE metacharacters so ``value`` matches itself literally.

    ``%`` (any run of characters), ``_`` (any single character) and the escape character itself
    are the three things a ``LIKE``/``ILIKE`` pattern treats specially. A client searching for
    ``"100%"`` means "messages containing the text 100%" — without this, the trailing ``%`` is a
    wildcard and the pattern ``%100%%`` matches every message containing "100", which is a
    strictly larger and quietly wrong answer.

    This is **not** SQL-injection defence. The pattern is bound as a parameter, so the string
    never reaches the parser as SQL. The bug this prevents is semantic, not a security hole, and
    conflating the two is how the escaping gets deleted by someone who correctly observes that
    bind parameters already handle injection.
    """
    escaped = value
    for character in _LIKE_METACHARACTERS:
        escaped = escaped.replace(character, LIKE_ESCAPE_CHARACTER + character)
    return escaped


def as_utc(value: datetime | None) -> datetime | None:
    """Return ``value`` as a timezone-aware UTC datetime; ``None`` passes through.

    A **naive** input is *assumed to be UTC* rather than rejected. The alternative — refusing it —
    would push the decision onto every caller, and the callers are a GraphQL scalar parser, a
    seeding routine and a test, three places that would each pick their own convention. An aware
    input in another zone is converted, so the value compared against the column is always the
    same instant expressed the same way.

    The failure this closes: asyncpg will happily bind a naive datetime against a ``timestamptz``
    column, and PostgreSQL then interprets it in the **server's** ``TimeZone`` setting. The query
    still succeeds; it just silently selects a different set of rows depending on how the database
    container happens to be configured.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def clamp_limit(limit: int | None, settings: Settings) -> int:
    """Resolve and clamp a requested row limit into ``[1, MAX_QUERY_LIMIT]``.

    ``None`` means the caller did not ask, and becomes ``DEFAULT_QUERY_LIMIT`` — the *configured*
    default rather than a literal 100, so an operator who raises ``DEFAULT_QUERY_LIMIT`` sees it
    take effect on exactly the requests that omitted ``limit``.

    Both ends are clamped rather than validated-and-rejected, deliberately: a client asking for
    ``limit: 100000`` gets ``MAX_QUERY_LIMIT`` rows and a usable answer, which is what a cap is
    for. The lower clamp to 1 exists because ``LIMIT 0`` and ``LIMIT -1`` are both accepted by
    PostgreSQL and both mean something a client did not intend — ``0`` returns nothing at all
    (indistinguishable from "no rows matched"), and a negative value raises.
    """
    requested = settings.default_query_limit if limit is None else int(limit)
    return max(1, min(requested, settings.max_query_limit))


@dataclass(frozen=True, slots=True)
class LogQuery:
    """A read request, expressed in plain Python.

    Deliberately **not** a GraphQL type. C3 defines ``LogFilterInput`` as a Strawberry input and
    maps it onto this; keeping the two apart means the repository can be exercised (and unit
    tested) without importing Strawberry, and means a second caller — the DataLoader in C5, the
    cache warm path in C7, the E2E script — is not obliged to construct a GraphQL input object to
    ask the database a question.

    Every field defaults to ``None``, and ``None`` means **omitted**, which the spec (§2 item 19)
    requires to be ignored rather than treated as "match NULL". The test for that is
    :func:`build_predicates` returning an empty list for ``LogQuery()``.

    Note the ``is not None`` (rather than truthiness) checks in :func:`build_predicates`: an empty
    string is a *supplied* filter. ``search_text=""`` therefore matches every message, which is
    exactly what an empty search box should do; ``service=""`` matches nothing, which is exactly
    what asking for a service with no name should do.

    Attributes:
        limit: ``None`` defers to ``DEFAULT_QUERY_LIMIT``. Any supplied value is clamped to
            ``[1, MAX_QUERY_LIMIT]`` by the builder — see :func:`clamp_limit`.
    """

    service: str | None = None
    level: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    search_text: str | None = None
    limit: int | None = None


def build_predicates(query: LogQuery) -> list[ColumnElement[bool]]:
    """Turn the supplied filters into WHERE conditions; omitted filters contribute nothing.

    .. rubric:: Time-range semantics: **both bounds are inclusive**

    ``start_time <= timestamp <= end_time``. Chosen over a half-open interval because both bounds
    are user-facing here — they are two datetime pickers in the C13 dashboard and two arguments to
    ``logStats`` — and a user who types the timestamp of a row they can see expects that row to be
    included. The cost is the usual one for closed intervals: two adjacent ranges that share an
    endpoint both contain the row sitting exactly on it. That is documented rather than hidden,
    and both boundary rows are pinned by integration tests.

    Returned as a list rather than pre-``and_``-ed so callers can count them — which is how the
    "omitted filters are ignored" test asserts on absence rather than on a compiled SQL string.
    """
    predicates: list[ColumnElement[bool]] = []

    if query.service is not None:
        predicates.append(LogEntryORM.service == query.service)
    if query.level is not None:
        predicates.append(LogEntryORM.level == query.level)

    start_time = as_utc(query.start_time)
    if start_time is not None:
        predicates.append(LogEntryORM.timestamp >= start_time)
    end_time = as_utc(query.end_time)
    if end_time is not None:
        predicates.append(LogEntryORM.timestamp <= end_time)

    if query.search_text is not None:
        # ILIKE (not LIKE): the spec asks for substring search on human-written log text, where
        # case is noise. The leading wildcard is why `ix_log_entries_message_trgm` exists — see
        # the index comment in src/db/models.py.
        pattern = f"%{escape_like(query.search_text)}%"
        predicates.append(
            LogEntryORM.message.ilike(pattern, escape=LIKE_ESCAPE_CHARACTER)
        )

    return predicates


def build_log_select(query: LogQuery, settings: Settings) -> Select[tuple[LogEntryORM]]:
    """Build the row-returning SELECT: filters, newest-first ordering, clamped LIMIT.

    Ordering is ``(timestamp DESC, id DESC)`` and the second column is not decoration. Two rows
    can share a timestamp — the seeded corpus avoids it, but ``createLog`` under load does not —
    and ``ORDER BY timestamp DESC`` alone leaves their relative order undefined, which means
    repeating the same query can return them in a different order and a cursor built on it can
    skip or repeat a row. ``id DESC`` makes the order total. ``ix_log_entries_ts_id`` covers
    exactly this via a backward index scan.
    """
    statement = select(LogEntryORM)
    for predicate in build_predicates(query):
        statement = statement.where(predicate)
    return statement.order_by(
        LogEntryORM.timestamp.desc(), LogEntryORM.id.desc()
    ).limit(clamp_limit(query.limit, settings))


def build_keyset_log_select(
    query: LogQuery,
    settings: Settings,
    *,
    after: tuple[datetime, int] | None = None,
    lookahead: int = 0,
) -> Select[tuple[LogEntryORM]]:
    """Build the SELECT for **one keyset page** of the same filtered, newest-first result.

    Added at C3, alongside :func:`build_log_select` rather than instead of it, because the two
    answer different questions: ``build_log_select`` serves ``Query.logs`` ("the newest N rows
    matching these filters", the spec's core requirement) and this serves
    ``Query.logsConnection`` ("the next N rows after this position", the §4 pagination bonus).
    They share :func:`build_predicates`, the ordering and the clamp, so the two can never disagree
    about what a filter *means* — only about where a page starts.

    .. rubric:: Keyset, not OFFSET

    ``LIMIT n OFFSET k`` numbers rows by their position in the current result, and this result is
    over an **append-heavy** table: ``createLog`` writes rows that sort to the very front of a
    ``timestamp DESC`` ordering. A row inserted between the client's page 1 and page 2 shifts every
    later row one position down, so page 2 re-serves the row that ended page 1 — a duplicate. A
    deletion shifts them the other way and page 2 skips one. Neither raises; the client just sees a
    list with a row twice, or missing one, and no request in the sequence was wrong.

    A keyset cursor names a **row**, not a position: ``WHERE (timestamp, id) < (:ts, :id)`` resumes
    strictly after the last row the client actually saw, so concurrent writes in front of the
    cursor are simply not in the page — which is correct, and is what "stable pagination" means.
    It also costs the same at page 1000 as at page 1 (the index seeks straight to the key), where
    OFFSET has to walk and discard every skipped row.

    The comparison is a **row-value** expression rather than the expanded
    ``ts < :ts OR (ts = :ts AND id < :id)``, matching what ``ix_log_entries_ts_id`` was declared
    for (see its comment in :mod:`src.db.models`): PostgreSQL turns it into a single ordered index
    range scan, where the OR form is a BitmapOr over two.

    Args:
        query: Filters and page size. ``query.limit`` is the page size, clamped exactly as it is
            for every other path.
        settings: Supplies ``DEFAULT_QUERY_LIMIT`` / ``MAX_QUERY_LIMIT``.
        after: The ``(timestamp, id)`` of the last row of the previous page, or ``None`` to start
            at the newest row. Normalised to UTC for the same reason filter bounds are.
        lookahead: Extra rows to fetch **beyond** the page, so the caller can tell whether another
            page exists without issuing a second COUNT. These rows are a probe: they are never
            returned to a client (:meth:`LogRepository.list_logs_page` truncates), so the clamped
            page size remains the cap on what any caller can actually receive.
    """
    statement = select(LogEntryORM)
    for predicate in build_predicates(query):
        statement = statement.where(predicate)

    if after is not None:
        after_timestamp, after_id = after
        statement = statement.where(
            tuple_(LogEntryORM.timestamp, LogEntryORM.id) < (as_utc(after_timestamp), after_id)
        )

    page_size = clamp_limit(query.limit, settings)
    return statement.order_by(LogEntryORM.timestamp.desc(), LogEntryORM.id.desc()).limit(
        page_size + max(0, lookahead)
    )


def build_count_select(query: LogQuery) -> Select[tuple[int]]:
    """Build ``SELECT count(*)`` over the **same predicates**, with no LIMIT and no ORDER BY.

    Read that again, because it is the one asymmetry in this module: the count deliberately
    ignores ``query.limit``. It answers "how many rows match these filters", not "how many rows
    would that query return" — which is what ``LogConnection.totalCount`` and C4's
    ``logStats.totalLogs`` both need. A count that respected the limit could never exceed it and
    would therefore be unable to tell a client there is more data, making the field useless for
    the only two things it is for.

    ``ORDER BY`` is dropped for the same reason it is always dropped from an aggregate: sorting
    rows that are about to be collapsed into one number is work with no observable effect.
    """
    statement = select(func.count()).select_from(LogEntryORM)
    for predicate in build_predicates(query):
        statement = statement.where(predicate)
    return statement


# =================================================================================================
# Batch loads — spec §2 item 29: "N field resolutions produce one database round trip, not N"
#
# The two builders below are the SQL half of C5's DataLoaders. They are the only statements in the
# project that take a *set* of keys, and the shape is the whole point: ONE statement per batch,
# never one per key.
#
# THE ORDERING CONTRACT LIVES ABOVE THIS LAYER, NOT IN IT. Both return a flat list of rows in the
# store's usual `(timestamp DESC, id DESC)` order — NOT one bucket per key, and NOT in key order.
# Re-aligning the rows with the keys the loader was called with is
# :mod:`src.graphql.loaders`'s job, because that is a pure function over the result and is worth
# unit testing without a database. What this module guarantees is only that every row matching any
# of the keys is in the result exactly once.
#
# Keys are DEDUPLICATED, order-preservingly, before they reach the IN clause. A DataLoader batch
# can legitimately contain the same key twice (two entries sharing a trace id ask for the same
# group), and PostgreSQL's extended protocol caps a statement at 32767 bound parameters — sending
# the duplicates would spend that budget on nothing. `dict.fromkeys` rather than `set` because a
# stable parameter order keeps two identical batches compiling to two identical statements, which
# is what makes a statement counter in a test readable.
# =================================================================================================


def build_logs_by_trace_ids_select(
    trace_ids: Sequence[str], settings: Settings
) -> Select[tuple[LogEntryORM]]:
    """Build the ONE SELECT that fetches every entry belonging to any of ``trace_ids``.

    ``ix_log_entries_trace_id`` exists for exactly this statement (see its comment in
    :mod:`src.db.models`): without it, a batch of 25 trace ids is 25 sequential scans folded into
    one query — batched, and still O(corpus) work per operation.

    .. rubric:: The LIMIT is a transfer bound, and it is deliberately NOT a per-group one

    ``max_query_limit * len(keys)`` — the same ceiling every other read path clamps to, multiplied
    by how many groups were asked for. It exists so that a single pathological ``trace_id``
    (a retry storm that tagged a million lines with one correlation id) cannot pull the whole table
    through one field selection.

    Its cost, stated rather than hidden: because the ordering is global, a hot trace *could* in
    principle consume the whole allowance and leave another key in the same batch with no rows —
    a parent whose ``relatedLogs`` is then wrongly empty. That needs one trace with more rows than
    the entire allowance, so it cannot happen at any corpus this project builds (groups are 2-5
    entries). The clean fix, if it ever does, is a
    ``ROW_NUMBER() OVER (PARTITION BY trace_id ORDER BY timestamp DESC, id DESC) <= cap`` filter,
    which caps each group independently in the same single statement. It is not written now because
    an untriggerable optimisation is a maintenance cost with no reader.

    Args:
        trace_ids: The batch's keys. Duplicates are collapsed; an empty sequence still produces a
            valid statement, but :meth:`LogRepository.list_logs_by_trace_ids` short-circuits before
            building one so an empty batch costs no round trip at all.
        settings: Supplies ``MAX_QUERY_LIMIT``.
    """
    keys = list(dict.fromkeys(trace_ids))
    return (
        select(LogEntryORM)
        .where(LogEntryORM.trace_id.in_(keys))
        .order_by(LogEntryORM.timestamp.desc(), LogEntryORM.id.desc())
        .limit(settings.max_query_limit * max(1, len(keys)))
    )


def build_logs_by_ids_select(log_ids: Sequence[int]) -> Select[tuple[LogEntryORM]]:
    """Build the ONE SELECT that fetches every entry named by ``log_ids``.

    No ``LIMIT`` clause, and that is not an oversight: ``id`` is the primary key, so the result can
    never exceed the number of distinct keys asked for — the batch size *is* the cap, and a
    ``LIMIT`` on top of it could only ever silently drop a row a caller explicitly named. Every
    other read path is capped because its predicate is open-ended; this one is not.
    """
    keys = list(dict.fromkeys(log_ids))
    return (
        select(LogEntryORM)
        .where(LogEntryORM.id.in_(keys))
        .order_by(LogEntryORM.timestamp.desc(), LogEntryORM.id.desc())
    )


# =================================================================================================
# Aggregates — spec §2 item 23 (`Query.logStats`)
#
# THE ONE RULE THIS SECTION EXISTS TO ENFORCE: every number below is computed BY POSTGRESQL. There
# is no `SELECT *` here and there must never be one. The tempting implementation —
# `rows = await repository.list_logs(LogQuery(start_time=…, end_time=…))` followed by
# `len(rows)` and a `Counter` — is wrong three times over and only one of the three is visible in
# a test:
#
#   1. It is silently CAPPED. `list_logs` clamps to MAX_QUERY_LIMIT, so `totalLogs` over a
#      million-row window would confidently report 500. The response looks perfectly healthy.
#   2. It transfers every matching row over the wire to count them. A stats call on a dashboard
#      refresh becomes the most expensive query the server serves, and its cost grows with the
#      corpus while the answer stays a handful of numbers long.
#   3. It holds them all in memory at once, in a process that is also serving 100 concurrent
#      requests (the C14 gate).
#
# Only (1) changes an assertion, which is why the tests grade `totalLogs` against the generator
# oracle at a corpus size ABOVE the default limit.
#
# TWO statements, not one, and the split is deliberate:
#
#   * `build_stats_totals_select` is a scalar aggregate — one row, always, whatever the data looks
#     like. It answers the spec's three headline numbers and cannot be affected by how many
#     distinct services exist.
#   * `build_stats_breakdown_select` is the GROUP BY that feeds the dashboard extras. Its result
#     size is bounded by the *vocabulary* (distinct service x level pairs: 50 for the seeded
#     corpus), never by the row count.
#
# Folding them into one `GROUP BY service, level` and summing in Python would work and would be one
# round trip — but then the spec's headline numbers would be a Python sum over a result whose size
# is a property of the data, and `totalLogs` would inherit any cardinality surprise the breakdown
# has. Keeping them apart makes `sum(serviceBreakdown) == totalLogs` an assertion across two
# independent statements, which is a real cross-check rather than a restatement.
#
# Both share `build_predicates`, so a filter means precisely what it means in `Query.logs`.
# Neither applies a LIMIT — for the same reason `build_count_select` ignores one.
# =================================================================================================


@dataclass(frozen=True, slots=True)
class ServiceLevelCount:
    """One ``(service, level)`` bucket and how many entries fell in it."""

    service: str
    level: str
    entries: int


@dataclass(frozen=True, slots=True)
class LogStatsResult:
    """Everything ``logStats`` publishes, in plain Python.

    Not a GraphQL type, for the same reason :class:`LogQuery` is not: the repository stays usable
    (and unit-testable) without importing Strawberry, and C7's aggregate cache will serialise
    *this* rather than a schema object. :meth:`src.graphql.types.LogStats.from_result` is the only
    place it is projected onto the published shape.

    Attributes:
        total_logs: Rows matching the filters. Exact — no limit was applied.
        error_count: How many of them are :data:`ERROR_LEVEL`.
        earliest: Oldest matching ``timestamp``, or ``None`` when nothing matched.
        latest: Newest matching ``timestamp``, or ``None`` when nothing matched.
        breakdown: One entry per non-empty ``(service, level)`` bucket, ordered by service then
            level. Buckets with no rows are absent rather than reported as zero — the database has
            nothing to say about a combination that never occurred.
    """

    total_logs: int
    error_count: int
    earliest: datetime | None
    latest: datetime | None
    breakdown: tuple[ServiceLevelCount, ...]


def build_stats_totals_select(query: LogQuery) -> Select[Any]:
    """Build the one-row scalar aggregate: totals, error count, and the observed time span.

    ``count(*) FILTER (WHERE level = 'ERROR')`` rather than a second query or a
    ``sum(CASE WHEN …)``: the filtered aggregate is evaluated in the same pass over the same rows,
    so the error count costs nothing beyond the total and — the part that matters — is guaranteed
    to be counted over *exactly* the same set. Two separate statements could disagree if a write
    landed between them.

    ``min``/``max`` on ``timestamp`` come along in the same pass and answer "what span does this
    result actually cover", which is a different question from the window that was *requested*: a
    dashboard asking for the last 24 hours wants to know the newest entry is 40 minutes old. Both
    are ``NULL`` when nothing matched, which is why the published fields are nullable.
    """
    statement = select(
        func.count().label("total_logs"),
        func.count().filter(LogEntryORM.level == ERROR_LEVEL).label("error_count"),
        func.min(LogEntryORM.timestamp).label("earliest"),
        func.max(LogEntryORM.timestamp).label("latest"),
    ).select_from(LogEntryORM)
    for predicate in build_predicates(query):
        statement = statement.where(predicate)
    return statement


def build_stats_breakdown_select(query: LogQuery) -> Select[Any]:
    """Build the ``GROUP BY service, level`` breakdown behind the per-service and per-level views.

    Grouped on both columns at once rather than run twice, because the per-service and per-level
    breakdowns are two projections of the same cross-tabulation: one statement, one scan, and the
    two views cannot disagree with each other about which rows they counted.

    ``ORDER BY service, level`` is for determinism, not presentation — the published ordering
    (services by descending volume, levels by ascending severity) is applied where the projection
    happens. Without any ORDER BY, PostgreSQL is free to return groups in whatever order the
    aggregation produced them, which can differ between two identical calls and would make a
    response diff-unstable for no reason.
    """
    entries = func.count().label("entries")
    statement = select(LogEntryORM.service, LogEntryORM.level, entries).select_from(LogEntryORM)
    for predicate in build_predicates(query):
        statement = statement.where(predicate)
    return statement.group_by(LogEntryORM.service, LogEntryORM.level).order_by(
        LogEntryORM.service.asc(), LogEntryORM.level.asc()
    )


# =================================================================================================
# C10 — the e-commerce event tables (spec §3 Feature Area A)
#
# Three more filter -> SELECT builders, in the same shape as the log ones above and reusing the same
# three primitives rather than restating them:
#
#   * `clamp_limit`  — so "the cap applies on every query path" (spec §2 item 22) covers these too,
#                      structurally, in the function that builds the statement;
#   * `escape_like`  — so a `searchText` of `ord-6%` finds ids CONTAINING a percent sign rather than
#                      every order in the table;
#   * `as_utc`       — so a naive bound is never compared against a `timestamptz` under the server's
#                      TimeZone setting.
#
# `build_common_event_predicates` factors out the four columns the three tables share, which is the
# same four `src.graphql.types.LogEvent` publishes as an interface. That is not a coincidence to be
# tidied away: it is the SAME abstraction seen from the storage side, and having one function build
# it means "filter by service" cannot mean one thing for orders and another for payments.
#
# WHAT IS DELIBERATELY NOT HERE: no joins, no aggregates, no batch (`IN (...)`) loads. C11 owns
# cross-entity traversal and the DataLoaders that batch it; these builders answer one flat question
# about one table, which is exactly what C10's three top-level list fields need. The `trace_id`
# filter below is the seam C11 widens into `... WHERE trace_id IN (:keys)` for its loaders.
# =================================================================================================

#: The three event models, as the union the shared predicate builder accepts. Spelled out rather
#: than typed as ``type[Base]`` so a caller cannot hand it ``LogEntryORM`` — which has a ``message``
#: column and no ``trace_id`` index shape in common with these — and get a statement that compiles
#: but means something else.
EventModel = type[OrderEventORM] | type[PaymentEventORM] | type[UserEventORM]


@dataclass(frozen=True, slots=True)
class EventQueryBase:
    """The filters every event stream shares — the storage-side twin of the ``LogEvent`` interface.

    A base class rather than four copies of five fields, because the subclasses genuinely *are* the
    same request with extra dimensions, and because :func:`build_common_event_predicates` can then
    be written once against this shape. Every field defaults to ``None``, and ``None`` means
    **omitted**, which is ignored rather than treated as "match NULL" — the same rule
    :class:`LogQuery` documents.

    Attributes:
        limit: ``None`` defers to ``DEFAULT_QUERY_LIMIT``; any supplied value is clamped to
            ``[1, MAX_QUERY_LIMIT]`` by the builder. Same discipline as every other read path.
    """

    service: str | None = None
    level: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    trace_id: str | None = None
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class OrderEventQuery(EventQueryBase):
    """A read request against ``order_events``.

    ``search_text`` is a substring match on ``order_id`` — the table's business identifier and the
    only free-form thing an order event carries. It is **not** trigram-indexed, unlike
    ``log_entries.message``: a leading-wildcard ILIKE over a few thousand short identifiers is a
    cheap sequential scan, while `message` is long free text over a corpus two orders of magnitude
    larger. If it ever becomes hot the fix is the identical ``gin_trgm_ops`` index, and this comment
    is where to look.
    """

    order_id: str | None = None
    user_id: str | None = None
    status: str | None = None
    search_text: str | None = None


@dataclass(frozen=True, slots=True)
class PaymentEventQuery(EventQueryBase):
    """A read request against ``payment_events``. ``search_text`` matches on ``order_id``."""

    order_id: str | None = None
    method: str | None = None
    outcome: str | None = None
    search_text: str | None = None


@dataclass(frozen=True, slots=True)
class UserEventQuery(EventQueryBase):
    """A read request against ``user_events``. ``search_text`` matches on ``user_id``."""

    user_id: str | None = None
    activity_type: str | None = None
    search_text: str | None = None


def build_common_event_predicates(
    model: EventModel, query: EventQueryBase
) -> list[ColumnElement[bool]]:
    """The WHERE conditions for the four columns every event stream shares, plus the time range.

    Time-range semantics are :func:`build_predicates`'s, unchanged and deliberately so: **both
    bounds inclusive**, normalised through :func:`as_utc`. "The last hour" must mean one instant
    range across ``logs``, ``orderEvents``, ``paymentEvents`` and ``userEvents``, or a dashboard
    whose panels share a time picker is showing four different windows.

    Note ``trace_id`` is matched by equality and only when supplied, so it never contributes a
    ``trace_id IS NULL`` clause — a client that omits it wants every event, correlated or not.
    """
    predicates: list[ColumnElement[bool]] = []

    if query.service is not None:
        predicates.append(model.service == query.service)
    if query.level is not None:
        predicates.append(model.level == query.level)
    if query.trace_id is not None:
        predicates.append(model.trace_id == query.trace_id)

    start_time = as_utc(query.start_time)
    if start_time is not None:
        predicates.append(model.timestamp >= start_time)
    end_time = as_utc(query.end_time)
    if end_time is not None:
        predicates.append(model.timestamp <= end_time)

    return predicates


def _ilike_predicate(column: Any, needle: str) -> ColumnElement[bool]:
    """``column ILIKE '%needle%'`` with LIKE metacharacters neutralised.

    One helper so the escape character is passed the same way on every table. See
    :func:`escape_like` for why this is a *semantic* fix rather than an injection defence.
    """
    return column.ilike(f"%{escape_like(needle)}%", escape=LIKE_ESCAPE_CHARACTER)


def build_order_event_predicates(query: OrderEventQuery) -> list[ColumnElement[bool]]:
    """Turn an :class:`OrderEventQuery` into WHERE conditions; omitted filters contribute nothing."""
    predicates = build_common_event_predicates(OrderEventORM, query)

    if query.order_id is not None:
        predicates.append(OrderEventORM.order_id == query.order_id)
    if query.user_id is not None:
        predicates.append(OrderEventORM.user_id == query.user_id)
    if query.status is not None:
        predicates.append(OrderEventORM.status == query.status)
    if query.search_text is not None:
        predicates.append(_ilike_predicate(OrderEventORM.order_id, query.search_text))

    return predicates


def build_payment_event_predicates(query: PaymentEventQuery) -> list[ColumnElement[bool]]:
    """Turn a :class:`PaymentEventQuery` into WHERE conditions."""
    predicates = build_common_event_predicates(PaymentEventORM, query)

    if query.order_id is not None:
        predicates.append(PaymentEventORM.order_id == query.order_id)
    if query.method is not None:
        predicates.append(PaymentEventORM.method == query.method)
    if query.outcome is not None:
        predicates.append(PaymentEventORM.outcome == query.outcome)
    if query.search_text is not None:
        predicates.append(_ilike_predicate(PaymentEventORM.order_id, query.search_text))

    return predicates


def build_user_event_predicates(query: UserEventQuery) -> list[ColumnElement[bool]]:
    """Turn a :class:`UserEventQuery` into WHERE conditions."""
    predicates = build_common_event_predicates(UserEventORM, query)

    if query.user_id is not None:
        predicates.append(UserEventORM.user_id == query.user_id)
    if query.activity_type is not None:
        predicates.append(UserEventORM.activity_type == query.activity_type)
    if query.search_text is not None:
        predicates.append(_ilike_predicate(UserEventORM.user_id, query.search_text))

    return predicates


def build_order_event_select(
    query: OrderEventQuery, settings: Settings
) -> Select[tuple[OrderEventORM]]:
    """Filters, newest-first ordering with the ``id`` tiebreak, clamped LIMIT."""
    statement = select(OrderEventORM)
    for predicate in build_order_event_predicates(query):
        statement = statement.where(predicate)
    return statement.order_by(
        OrderEventORM.timestamp.desc(), OrderEventORM.id.desc()
    ).limit(clamp_limit(query.limit, settings))


def build_payment_event_select(
    query: PaymentEventQuery, settings: Settings
) -> Select[tuple[PaymentEventORM]]:
    """Filters, newest-first ordering with the ``id`` tiebreak, clamped LIMIT."""
    statement = select(PaymentEventORM)
    for predicate in build_payment_event_predicates(query):
        statement = statement.where(predicate)
    return statement.order_by(
        PaymentEventORM.timestamp.desc(), PaymentEventORM.id.desc()
    ).limit(clamp_limit(query.limit, settings))


def build_user_event_select(
    query: UserEventQuery, settings: Settings
) -> Select[tuple[UserEventORM]]:
    """Filters, newest-first ordering with the ``id`` tiebreak, clamped LIMIT."""
    statement = select(UserEventORM)
    for predicate in build_user_event_predicates(query):
        statement = statement.where(predicate)
    return statement.order_by(
        UserEventORM.timestamp.desc(), UserEventORM.id.desc()
    ).limit(clamp_limit(query.limit, settings))


# =================================================================================================
# C11 — the cross-entity batch loads (spec §3 Feature Area D: "DataLoader batching extended across
# all entity types, not just logs")
#
# Two generic builders rather than ten near-identical ones. They take the model and the column
# because the SHAPE is genuinely identical across every edge this schema publishes — order -> its
# payments, order -> its user's activity, user -> their orders, anything -> its trace — and ten
# copies of `select(X).where(X.c.in_(keys)).order_by(...).limit(...)` would be ten places for the
# `dict.fromkeys` dedup or the `max(1, ...)` on the limit to be forgotten in exactly one of them.
#
# EVERY CONTRACT THE C5 BUILDERS ESTABLISHED HOLDS HERE UNCHANGED, because the loaders above them
# depend on all three:
#
#   * ONE statement per batch, never one per key. That is the whole requirement.
#   * A FLAT list in `(timestamp DESC, id DESC)` order — NOT one bucket per key and NOT in key
#     order. Re-aligning rows onto keys is `src.graphql.loaders`'s job, because it is a pure
#     function worth unit testing against a shuffled batch with misses and duplicates in it.
#   * Keys DEDUPLICATED order-preservingly. A DataLoader batch legitimately contains the same key
#     twice (two payment events for one order ask for the same order), and PostgreSQL's extended
#     protocol caps a statement at 32767 bound parameters.
#
# INDEX COVERAGE. Every batched predicate below is served by an index C10 already declared, and the
# claim is checkable rather than asserted — `tests/integration/test_cross_entity_loaders.py` reads
# `pg_indexes` and pins the pairing:
#
#   order_events.order_id   -> ix_order_events_order_ts     (order_id, timestamp, id)
#   order_events.user_id    -> ix_order_events_user_ts      (user_id, timestamp, id)
#   order_events.trace_id   -> ix_order_events_trace_id     (trace_id)
#   payment_events.order_id -> ix_payment_events_order_ts   (order_id, timestamp, id)
#   payment_events.trace_id -> ix_payment_events_trace_id   (trace_id)
#   user_events.user_id     -> ix_user_events_user_ts       (user_id, timestamp, id)
#   user_events.trace_id    -> ix_user_events_trace_id      (trace_id)
#   every `id`              -> the primary key
#
# So C11 adds no index. That is not luck — C10 declared each of these for the traversal it knew was
# coming, and the trailing-`id` tiebreak on the composite three is what lets the ordered read come
# out of the index instead of out of a Sort node. The one thing to check when adding a new edge is
# that its column leads an index; if it does not, add one in the same shape rather than shipping a
# batched sequential scan (batched, and still O(table) per operation).
# =================================================================================================


def build_events_by_keys_select(
    model: EventModel,
    column: Any,  # noqa: ANN401 - an InstrumentedAttribute on `model`
    keys: Sequence[str],
    settings: Settings,
) -> Select[Any]:
    """The ONE SELECT that fetches every ``model`` row whose ``column`` is any of ``keys``.

    .. rubric:: The LIMIT is a transfer bound and it is deliberately NOT a per-group one

    ``MAX_QUERY_LIMIT * len(keys)`` — exactly the ceiling
    :func:`build_logs_by_trace_ids_select` applies, for exactly the same reason: one pathological
    key (an order retried ten thousand times, a user_id shared by a load-test fixture) must not be
    able to pull a whole table through one field selection.

    Its cost, stated rather than hidden: the ordering is global, so a hot key *could* in principle
    consume the whole allowance and leave another key in the same batch wrongly empty. That needs
    one key with more rows than the entire allowance, which cannot happen at any corpus this
    project builds (an order has at most 14 events, a user a few dozen). The clean fix, if it ever
    does, is the same one named on the log builder — a
    ``ROW_NUMBER() OVER (PARTITION BY <column> ORDER BY timestamp DESC, id DESC) <= cap`` filter,
    which caps each group independently inside the same single statement.

    Args:
        model: One of the three C10 event models.
        column: The mapped column to match on — ``OrderEventORM.user_id`` and friends. Passed in
            rather than named by string so a typo is an ``AttributeError`` at import rather than a
            statement that compiles against the wrong table.
        keys: The batch, in the order the loader was called in. Duplicates are collapsed.
        settings: Supplies ``MAX_QUERY_LIMIT``.
    """
    unique = list(dict.fromkeys(keys))
    return (
        select(model)
        .where(column.in_(unique))
        .order_by(model.timestamp.desc(), model.id.desc())
        .limit(settings.max_query_limit * max(1, len(unique)))
    )


def build_events_by_ids_select(model: EventModel, event_ids: Sequence[int]) -> Select[Any]:
    """The ONE SELECT that fetches every ``model`` row named by ``event_ids``.

    No ``LIMIT``, and that is not an oversight — it is :func:`build_logs_by_ids_select`'s argument
    applied to the event tables: ``id`` is the primary key, so the result can never exceed the
    number of distinct keys asked for. The batch size *is* the cap, and a ``LIMIT`` on top could
    only ever silently drop a row a caller explicitly named.
    """
    unique = list(dict.fromkeys(event_ids))
    return (
        select(model)
        .where(model.id.in_(unique))
        .order_by(model.timestamp.desc(), model.id.desc())
    )


# =================================================================================================
# C11 — the e-commerce aggregates (spec §3 Feature Area D: "Redis caching applied to aggregations")
#
# THE RULE FROM THE `logStats` SECTION ABOVE APPLIES HERE WORD FOR WORD: every number below is
# computed BY POSTGRESQL. There is no `SELECT *` in this section and there must never be one. The
# tempting implementation — pull the order events and count them in Python — is silently CAPPED by
# `clamp_limit`, transfers the whole table to count it, and holds it in memory in a process serving
# 100 concurrent requests. Only the first of those three changes an assertion, which is why the
# tests grade these against a corpus larger than the default limit.
#
# THREE aggregates, and the three are genuinely different questions rather than one question with
# three renderings. That distinction is what makes a per-aggregation TTL policy meaningful instead
# of decorative (see `src.cache.TTL_POLICY`):
#
#   * DISTRIBUTION — "where does every order stand RIGHT NOW". One row per order (its newest event's
#     status), then grouped. VOLATILE: one new event moves an order between buckets.
#   * FUNNEL — "how many orders EVER reached each status". MONOTONIC: a status once reached is never
#     un-reached, so this can only grow and a stale read can only undercount.
#   * PAYMENT BREAKDOWN — the (method x outcome) cross-tabulation. Bounded by the VOCABULARY
#     (5 x 4 = 20 buckets), never by the row count, exactly as `build_stats_breakdown_select` is.
#
# All three share `build_order_event_predicates` / `build_payment_event_predicates` with the flat
# list reads, so a filter means precisely the same thing on an aggregate as on the rows behind it —
# a dashboard's summary can never describe a different set from the table beneath it. None applies a
# LIMIT, for the same reason `build_count_select` ignores one.
#
# ORDERING IS BY NAME HERE AND BY LIFECYCLE IN THE PROJECTION. The SQL orders by the status string
# purely for determinism (an unordered GROUP BY may return groups in a different order between two
# identical calls, which makes a response diff-unstable for no reason). The *published* order —
# CREATED, PAID, PACKED, … — is applied in `src.graphql.ecommerce`, exactly as `LogStats.from_result`
# applies ascending severity to `levelBreakdown`. Keeping it there is what lets the store stay
# ignorant of the API's enum declaration order.
# =================================================================================================


@dataclass(frozen=True, slots=True)
class OrderStatusBucket:
    """How many orders are **currently** sitting at one status."""

    status: str
    orders: int


@dataclass(frozen=True, slots=True)
class FunnelBucket:
    """How many distinct orders have **ever** reached one status."""

    status: str
    orders: int


@dataclass(frozen=True, slots=True)
class PaymentOutcomeBucket:
    """One ``(method, outcome)`` cell: how many events, and how many distinct orders."""

    method: str
    outcome: str
    events: int
    orders: int


def build_order_status_distribution_select(query: OrderEventQuery) -> Select[Any]:
    """``DISTINCT ON (order_id)`` newest event per order, then ``GROUP BY status``.

    .. rubric:: Why the subquery is not optional

    ``order_events`` is an append-only stream: an order that reached DELIVERED has a CREATED row, a
    PAID row and a PACKED row still sitting in the table. A plain ``GROUP BY status`` therefore
    counts *transitions*, not orders — every fulfilled order would be counted once in every bucket
    it passed through, and the "where do orders stand" panel would show a monotonically rising
    count in every state at once. (That number is a real and useful one; it is the **funnel**, and
    it has its own builder below.)

    ``DISTINCT ON`` is PostgreSQL's own primitive for "the first row of each group under this
    ORDER BY", and the ORDER BY has to lead with the distinct column — which is why the sort is
    ``(order_id, timestamp DESC, id DESC)`` rather than the store's usual pair. The ``id`` tiebreak
    is doing real work here rather than decorating: two events of one order can share an instant
    under a bulk write, and without it "the newest status" would be whichever row the executor
    happened to reach first, i.e. a count that changes between two identical calls.

    The alternative shapes were considered and are worse at this size: a correlated
    ``MAX(timestamp)`` subquery re-scans per order, and a ``ROW_NUMBER()`` window sorts every row of
    every group rather than seeking one per group. ``ix_order_events_order_ts`` is
    ``(order_id, timestamp, id)``, which is exactly the ordering this asks for.

    ``query.limit`` is ignored, as it is on every aggregate in this module.
    """
    newest = select(OrderEventORM.order_id, OrderEventORM.status).distinct(
        OrderEventORM.order_id
    )
    for predicate in build_order_event_predicates(query):
        newest = newest.where(predicate)
    newest = newest.order_by(
        OrderEventORM.order_id.asc(),
        OrderEventORM.timestamp.desc(),
        OrderEventORM.id.desc(),
    )

    current = newest.subquery("current_order_status")
    return (
        select(current.c.status, func.count().label("orders"))
        .group_by(current.c.status)
        .order_by(current.c.status.asc())
    )


def build_order_funnel_select(query: OrderEventQuery) -> Select[Any]:
    """``count(DISTINCT order_id)`` per status: how many orders ever reached each stage.

    ``DISTINCT`` rather than ``count(*)`` is the whole difference between a funnel and an event
    histogram, and it is not a distinction the seeded corpus can expose on its own — a generated
    order emits each status at most once, so the two agree there and a test written against this
    corpus alone cannot tell them apart. They diverge the moment anything retries: a payment
    processor that re-notifies PAID writes a second PAID row, and ``count(*)`` would report an
    order that converted twice. Counting orders is what the field claims to do, so it counts
    orders.

    One flat ``GROUP BY`` over an indexed column, no subquery: unlike the distribution above, this
    question genuinely is about every row rather than about the newest one per order.
    """
    statement = select(
        OrderEventORM.status,
        func.count(distinct(OrderEventORM.order_id)).label("orders"),
    ).select_from(OrderEventORM)
    for predicate in build_order_event_predicates(query):
        statement = statement.where(predicate)
    return statement.group_by(OrderEventORM.status).order_by(OrderEventORM.status.asc())


def build_payment_outcome_breakdown_select(query: PaymentEventQuery) -> Select[Any]:
    """The ``(method, outcome)`` cross-tabulation, with both an event count and an order count.

    Grouped on both columns at once rather than run twice, for the reason
    :func:`build_stats_breakdown_select` gives: the per-method and per-outcome views a dashboard
    draws are two marginals of one cross-tabulation, and folding them from a single scan is what
    makes them unable to disagree about which rows they counted.

    **Two counts, because they answer different questions and their difference is the signal.** A
    payment is a stream — AUTHORIZED, then CAPTURED, then possibly REFUNDED — so ``events`` counts
    attempts and ``orders`` counts the orders those attempts belong to. ``events > orders`` in a
    DECLINED bucket means orders are being retried, which is precisely what a payments dashboard
    exists to show; a single count could not express it.

    Result size is bounded by the vocabulary (5 methods x 4 outcomes = 20 cells), never by the row
    count — which is what lets the cost gate price this field at a fixed size rather than at the
    page-size assumption.
    """
    statement = select(
        PaymentEventORM.method,
        PaymentEventORM.outcome,
        func.count().label("events"),
        func.count(distinct(PaymentEventORM.order_id)).label("orders"),
    ).select_from(PaymentEventORM)
    for predicate in build_payment_event_predicates(query):
        statement = statement.where(predicate)
    return statement.group_by(PaymentEventORM.method, PaymentEventORM.outcome).order_by(
        PaymentEventORM.method.asc(), PaymentEventORM.outcome.asc()
    )


class LogRepository:
    """Executes the builders above against one :class:`~sqlalchemy.ext.asyncio.AsyncSession`.

    Holds a session rather than a session factory, because the unit of work is chosen one level
    up: C5's ``PerOperationResources`` extension opens exactly one session per GraphQL operation
    and hands it to every loader and resolver in that operation, so batching works and so a
    subscription's long-lived socket never pins a connection.

    .. rubric:: The repository does not commit

    :meth:`insert_log` flushes — enough to make the generated ``id`` available — and stops there.
    Transaction boundaries belong to the caller: C4's ``createLog`` resolver commits and only then
    publishes to the broker, and C10's order events will want to land in the same transaction as
    the log line that describes them. A repository that committed on every write would make that
    impossible and would silently defeat any test that wanted rollback-based isolation.
    """

    def __init__(self, session: AsyncSession, settings: Settings | None = None) -> None:
        """Bind to a session.

        Args:
            session: The session every statement runs on. Owned by the caller.
            settings: Supplies ``DEFAULT_QUERY_LIMIT`` / ``MAX_QUERY_LIMIT``. Falls back to the
                process-wide cached configuration; tests pass an explicit object so they do not
                depend on the LRU cache another test may have populated.
        """
        self._session = session
        self._settings = settings if settings is not None else get_settings()

    @property
    def settings(self) -> Settings:
        """The resolved configuration, exposed so callers can report the clamp they will get."""
        return self._settings

    async def list_logs(self, query: LogQuery) -> list[LogEntryORM]:
        """Return matching rows, newest first, capped at the clamped limit."""
        result = await self._session.execute(build_log_select(query, self._settings))
        return list(result.scalars().all())

    async def list_logs_page(
        self, query: LogQuery, *, after: tuple[datetime, int] | None = None
    ) -> tuple[list[LogEntryORM], bool]:
        """Return one keyset page and whether another page follows it.

        The "another page follows" half is answered by fetching **one row more than the page** and
        checking whether it arrived, rather than by a second ``COUNT(*)`` over the remainder. One
        extra row is free (the index scan is already positioned there); a count over the remainder
        is a second full scan of everything the client has not read yet, which grows as the client
        pages *backwards* through history — the opposite of what pagination is for.

        The probe row is dropped before returning, so what a caller receives is still capped at
        the clamped page size.

        Returns:
            ``(rows, has_next_page)`` — at most ``clamp_limit(query.limit, settings)`` rows,
            newest first.
        """
        page_size = clamp_limit(query.limit, self._settings)
        statement = build_keyset_log_select(query, self._settings, after=after, lookahead=1)
        result = await self._session.execute(statement)
        rows = list(result.scalars().all())
        return rows[:page_size], len(rows) > page_size

    async def count_logs(self, query: LogQuery) -> int:
        """Return how many rows match ``query``'s filters, **ignoring its limit**.

        See :func:`build_count_select` for why the limit is ignored.
        """
        total = await self._session.scalar(build_count_select(query))
        # `scalar()` returns None only when the statement produced no row at all, which a bare
        # aggregate cannot do; the coalesce is there so a caller never has to consider `None`.
        return int(total or 0)

    async def log_stats(self, query: LogQuery) -> LogStatsResult:
        """Aggregate the rows matching ``query`` **in SQL**, ignoring ``query.limit``.

        Two statements, both explained in the section comment above :class:`LogStatsResult`. The
        limit is ignored for the same reason :meth:`count_logs` ignores it: these numbers describe
        the whole matching set, and an aggregate that silently stopped at ``MAX_QUERY_LIMIT`` would
        report a plausible, wrong total with no indication anything had been truncated.

        An empty result is a normal answer, not an error: the scalar aggregate still returns its
        one row (zeros and two ``NULL`` timestamps), the breakdown returns no rows, and the caller
        gets zeros rather than an exception. A dashboard filtering to a quiet window must render
        "0", not a failure.

        Returns:
            A :class:`LogStatsResult`. Every number in it was computed by PostgreSQL.
        """
        totals = (await self._session.execute(build_stats_totals_select(query))).one()
        groups = (await self._session.execute(build_stats_breakdown_select(query))).all()

        return LogStatsResult(
            total_logs=int(totals.total_logs or 0),
            error_count=int(totals.error_count or 0),
            earliest=totals.earliest,
            latest=totals.latest,
            breakdown=tuple(
                ServiceLevelCount(
                    service=row.service, level=row.level, entries=int(row.entries)
                )
                for row in groups
            ),
        )

    async def get_by_id(self, log_id: int) -> LogEntryORM | None:
        """Fetch one row by primary key, or ``None``.

        ``Session.get`` rather than a hand-written SELECT, because it consults the identity map
        first — within one unit of work the same entry is frequently reached twice, and this makes
        the second reach free rather than a second round trip.

        **``Query.log`` no longer comes through here.** Since C5 it goes through the by-id
        DataLoader (:meth:`get_logs_by_ids`), because a document naming several entries under
        aliases is several resolver calls and this method can only ever answer one of them per
        statement. This remains the right call for a single, known lookup that is not part of a
        GraphQL selection set — the mutation and store tests use it, and C10's writes will.
        """
        return await self._session.get(LogEntryORM, log_id)

    async def list_logs_by_trace_ids(self, trace_ids: Sequence[str]) -> list[LogEntryORM]:
        """Fetch every entry belonging to any of ``trace_ids`` in **one** statement.

        The store half of C5's ``LogEntry.relatedLogs`` batching. Returns a flat list in
        ``(timestamp DESC, id DESC)`` order — grouping the rows back onto the keys is
        :func:`src.graphql.loaders.group_logs_by_trace_id`'s job, for the reason given in the
        section comment above :func:`build_logs_by_trace_ids_select`.

        An empty batch returns ``[]`` **without executing anything**. That is load-bearing rather
        than tidy: ``WHERE trace_id IN ()`` is a statement that cannot match, and issuing it would
        put a round trip on the wire for a question whose answer is already known — which is
        exactly what the statement-counting tests exist to catch.
        """
        if not trace_ids:
            return []
        result = await self._session.execute(
            build_logs_by_trace_ids_select(trace_ids, self._settings)
        )
        return list(result.scalars().all())

    async def get_logs_by_ids(self, log_ids: Sequence[int]) -> list[LogEntryORM]:
        """Fetch every entry named by ``log_ids`` in **one** statement.

        The batched counterpart of :meth:`get_by_id`. An id with no row is simply absent from the
        result: "no such entry" is an ordinary answer, and
        :func:`src.graphql.loaders.align_logs_by_id` turns it into a ``None`` at the right
        position rather than into an error.

        As above, an empty batch costs no round trip.
        """
        if not log_ids:
            return []
        result = await self._session.execute(build_logs_by_ids_select(log_ids))
        return list(result.scalars().all())

    async def insert_log(
        self,
        *,
        service: str,
        level: str,
        message: str,
        timestamp: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> LogEntryORM:
        """Insert one log entry and return it with its generated ``id`` populated.

        Written now rather than at C4 so the mutation lands on a stable interface instead of
        reshaping this module the week after it was reviewed.

        Keyword-only on purpose: ``service``, ``level``, ``message`` and ``trace_id`` are four
        strings in a row, and a positional call site that transposed two of them would be accepted
        by the type checker, accepted by the database, and wrong.

        ``timestamp`` defaults to now — this *is* a wall-clock read, and it is correct here: a log
        line created through the API happens at the moment it is created. (The generator's refusal
        to read the clock is about reproducibility of a synthetic corpus, which is a different
        problem.) Naive values are normalised to UTC by :func:`as_utc`.

        The caller commits. See the class docstring.
        """
        resolved_timestamp = as_utc(timestamp)
        if resolved_timestamp is None:
            resolved_timestamp = datetime.now(timezone.utc)

        entry = LogEntryORM(
            timestamp=resolved_timestamp,
            service=service,
            level=level,
            message=message,
            metadata_=metadata,
            trace_id=trace_id,
        )
        self._session.add(entry)
        # Flush, not commit: this emits the INSERT (so PostgreSQL assigns the BIGSERIAL id and
        # RETURNING brings it back) while leaving the transaction open for the caller to commit
        # or roll back as one unit.
        await self._session.flush()
        return entry

    async def insert_order_event(
        self,
        *,
        order_id: str,
        user_id: str,
        status: str,
        service: str,
        level: str,
        timestamp: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> OrderEventORM:
        """Insert one order status transition and return it with its generated ``id`` populated.

        C12's write path, and the exact shape of :meth:`insert_log` deliberately: keyword-only
        (``order_id``, ``user_id``, ``status``, ``service``, ``level`` and ``trace_id`` are six
        strings in a row, and a positional call site transposing two of them would be accepted by
        the type checker, accepted by PostgreSQL, and wrong), defaulting ``timestamp`` to now, and
        **flushing without committing** so the caller owns the transaction boundary.

        That last point is what makes ``createOrderEvent`` able to publish only after the row is
        durable — see :mod:`src.graphql.mutation`. It is also the seam that would let an order
        event and the log line describing it land in **one** transaction, which is the reason the
        commit was left to the caller in the first place.

        No ordering or uniqueness is enforced against the order's existing history, and that is the
        domain rather than an omission: ``order_events`` is an append-only stream, an out-of-order
        arrival from a partner feed is data, and "the order's current status" is defined as the
        status of its newest event rather than as a state machine this table polices.
        """
        resolved_timestamp = as_utc(timestamp)
        if resolved_timestamp is None:
            resolved_timestamp = datetime.now(timezone.utc)

        event = OrderEventORM(
            timestamp=resolved_timestamp,
            service=service,
            level=level,
            trace_id=trace_id,
            order_id=order_id,
            user_id=user_id,
            status=status,
            metadata_=metadata,
        )
        self._session.add(event)
        # Flush, not commit — see `insert_log`. This emits the INSERT so PostgreSQL assigns the
        # BIGSERIAL id and RETURNING brings it back, while leaving the transaction open.
        await self._session.flush()
        return event

    # ---------------------------------------------------------------------------------------------
    # C10 — the e-commerce event reads.
    #
    # Deliberately methods on THIS repository rather than a second `EventRepository` class. One
    # repository per session is what `Context.repository()` hands out, and it is what every resolver
    # and every DataLoader batch already goes through; a second class would need a second provider,
    # a second lifetime rule and a second place for "which session does this run on" to be answered.
    # The name stays `LogRepository` because everything in here is a log event — an order status
    # transition IS a log line with a schema, which is the whole premise of `LogEvent`.
    #
    # Each is one statement over one table. C11 adds the batched (`IN (:keys)`) forms beside them.
    # ---------------------------------------------------------------------------------------------

    async def list_order_events(self, query: OrderEventQuery) -> list[OrderEventORM]:
        """Matching order events, newest first, capped at the clamped limit."""
        result = await self._session.execute(build_order_event_select(query, self._settings))
        return list(result.scalars().all())

    async def list_payment_events(self, query: PaymentEventQuery) -> list[PaymentEventORM]:
        """Matching payment events, newest first, capped at the clamped limit."""
        result = await self._session.execute(build_payment_event_select(query, self._settings))
        return list(result.scalars().all())

    async def list_user_events(self, query: UserEventQuery) -> list[UserEventORM]:
        """Matching user activity events, newest first, capped at the clamped limit."""
        result = await self._session.execute(build_user_event_select(query, self._settings))
        return list(result.scalars().all())

    # ---------------------------------------------------------------------------------------------
    # C11 — the batched cross-entity reads (spec §3 Feature Area D).
    #
    # Ten methods, three lines each, all of them the same sentence: dedupe, one statement, flat
    # result. They are the store half of `src.graphql.loaders`'s cross-entity DataLoaders, and the
    # ONLY thing each one adds over the two generic builders above is naming the (model, column)
    # pair — which is worth a method rather than a call-site argument, because "which column is the
    # order -> user edge" is a fact about this schema and should be written down once.
    #
    # EVERY ONE SHORT-CIRCUITS AN EMPTY BATCH WITHOUT EXECUTING ANYTHING. That is load-bearing
    # rather than tidy: `WHERE order_id IN ()` is a statement that cannot match, and issuing it
    # would put a round trip on the wire for a question whose answer is already known — which is
    # exactly what the statement-counting tests exist to catch. It is also what makes
    # `OrderEvent.relatedLogs` free (not merely cheap) on a page of untraced events.
    # ---------------------------------------------------------------------------------------------

    async def get_order_events_by_ids(self, event_ids: Sequence[int]) -> list[OrderEventORM]:
        """Fetch every order event named by ``event_ids`` in **one** statement."""
        if not event_ids:
            return []
        result = await self._session.execute(
            build_events_by_ids_select(OrderEventORM, event_ids)
        )
        return list(result.scalars().all())

    async def get_payment_events_by_ids(self, event_ids: Sequence[int]) -> list[PaymentEventORM]:
        """Fetch every payment event named by ``event_ids`` in **one** statement."""
        if not event_ids:
            return []
        result = await self._session.execute(
            build_events_by_ids_select(PaymentEventORM, event_ids)
        )
        return list(result.scalars().all())

    async def get_user_events_by_ids(self, event_ids: Sequence[int]) -> list[UserEventORM]:
        """Fetch every user event named by ``event_ids`` in **one** statement."""
        if not event_ids:
            return []
        result = await self._session.execute(
            build_events_by_ids_select(UserEventORM, event_ids)
        )
        return list(result.scalars().all())

    async def list_order_events_by_order_ids(
        self, order_ids: Sequence[str]
    ) -> list[OrderEventORM]:
        """Every order event belonging to any of ``order_ids`` — one statement, newest first.

        The order's own history, which is also how ``PaymentEvent.order`` answers "what is this
        payment's order doing": the newest row of the group is the current status.
        """
        if not order_ids:
            return []
        result = await self._session.execute(
            build_events_by_keys_select(
                OrderEventORM, OrderEventORM.order_id, order_ids, self._settings
            )
        )
        return list(result.scalars().all())

    async def list_order_events_by_user_ids(self, user_ids: Sequence[str]) -> list[OrderEventORM]:
        """Every order event placed by any of ``user_ids`` — the order -> user edge, read backwards."""
        if not user_ids:
            return []
        result = await self._session.execute(
            build_events_by_keys_select(
                OrderEventORM, OrderEventORM.user_id, user_ids, self._settings
            )
        )
        return list(result.scalars().all())

    async def list_order_events_by_trace_ids(
        self, trace_ids: Sequence[str]
    ) -> list[OrderEventORM]:
        """Every order event carrying any of ``trace_ids`` — the correlation edge."""
        if not trace_ids:
            return []
        result = await self._session.execute(
            build_events_by_keys_select(
                OrderEventORM, OrderEventORM.trace_id, trace_ids, self._settings
            )
        )
        return list(result.scalars().all())

    async def list_payment_events_by_order_ids(
        self, order_ids: Sequence[str]
    ) -> list[PaymentEventORM]:
        """Every payment event filed under any of ``order_ids`` — the order -> payments edge."""
        if not order_ids:
            return []
        result = await self._session.execute(
            build_events_by_keys_select(
                PaymentEventORM, PaymentEventORM.order_id, order_ids, self._settings
            )
        )
        return list(result.scalars().all())

    async def list_payment_events_by_trace_ids(
        self, trace_ids: Sequence[str]
    ) -> list[PaymentEventORM]:
        """Every payment event carrying any of ``trace_ids``."""
        if not trace_ids:
            return []
        result = await self._session.execute(
            build_events_by_keys_select(
                PaymentEventORM, PaymentEventORM.trace_id, trace_ids, self._settings
            )
        )
        return list(result.scalars().all())

    async def list_user_events_by_user_ids(self, user_ids: Sequence[str]) -> list[UserEventORM]:
        """Every activity event belonging to any of ``user_ids`` — the order -> user traversal."""
        if not user_ids:
            return []
        result = await self._session.execute(
            build_events_by_keys_select(
                UserEventORM, UserEventORM.user_id, user_ids, self._settings
            )
        )
        return list(result.scalars().all())

    async def list_user_events_by_trace_ids(self, trace_ids: Sequence[str]) -> list[UserEventORM]:
        """Every activity event carrying any of ``trace_ids``."""
        if not trace_ids:
            return []
        result = await self._session.execute(
            build_events_by_keys_select(
                UserEventORM, UserEventORM.trace_id, trace_ids, self._settings
            )
        )
        return list(result.scalars().all())

    # ---------------------------------------------------------------------------------------------
    # C11 — the e-commerce aggregates. Every number computed by PostgreSQL; see the section comment
    # above `OrderStatusBucket` for the three questions and why they are three.
    #
    # `query.limit` is ignored by all three, exactly as `log_stats` and `count_logs` ignore it: these
    # numbers describe the whole matching set, and an aggregate that silently stopped at
    # MAX_QUERY_LIMIT would report a plausible, wrong total with nothing to say it had truncated.
    #
    # An empty result is a normal answer rather than an error. A GROUP BY over no rows returns no
    # rows, so the caller gets an empty tuple and the dashboard renders zeros — a filter narrowed to
    # a quiet window must not look like a failure.
    # ---------------------------------------------------------------------------------------------

    async def order_status_distribution(
        self, query: OrderEventQuery
    ) -> tuple[OrderStatusBucket, ...]:
        """How many orders currently sit at each status — **one** statement, computed in SQL."""
        rows = (
            await self._session.execute(build_order_status_distribution_select(query))
        ).all()
        return tuple(
            OrderStatusBucket(status=row.status, orders=int(row.orders)) for row in rows
        )

    async def order_funnel(self, query: OrderEventQuery) -> tuple[FunnelBucket, ...]:
        """How many distinct orders ever reached each status — **one** statement, computed in SQL."""
        rows = (await self._session.execute(build_order_funnel_select(query))).all()
        return tuple(FunnelBucket(status=row.status, orders=int(row.orders)) for row in rows)

    async def payment_outcome_breakdown(
        self, query: PaymentEventQuery
    ) -> tuple[PaymentOutcomeBucket, ...]:
        """The ``(method, outcome)`` cross-tabulation — **one** statement, computed in SQL."""
        rows = (
            await self._session.execute(build_payment_outcome_breakdown_select(query))
        ).all()
        return tuple(
            PaymentOutcomeBucket(
                method=row.method,
                outcome=row.outcome,
                events=int(row.events),
                orders=int(row.orders),
            )
            for row in rows
        )
