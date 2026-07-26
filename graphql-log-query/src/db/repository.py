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

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Select, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import ColumnElement

from src.config import Settings, get_settings
from src.db.models import LogEntryORM

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

    async def get_by_id(self, log_id: int) -> LogEntryORM | None:
        """Fetch one row by primary key, or ``None``.

        ``Session.get`` rather than a hand-written SELECT, because it consults the identity map
        first: within one GraphQL operation the same entry is frequently reached twice (once
        through ``logs``, once through another entry's ``related_logs``), and this makes the
        second reach free rather than a second round trip.
        """
        return await self._session.get(LogEntryORM, log_id)

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
