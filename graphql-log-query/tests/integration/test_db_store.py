"""Integration tests for the store, against the **real** ``gqllogs_test`` PostgreSQL database.

Nothing here is faked. ``timestamptz`` round-tripping, ``JSONB``, ``ILIKE`` with an ``ESCAPE``
clause, a GIN trigram index and ``BIGSERIAL`` id assignment are all things SQLite cannot emulate,
and every one of them is load-bearing. The compose ``test`` service points ``DATABASE_URL`` at a
separate database (created by ``docker/postgres-init/10-create-test-db.sql``) precisely so this
suite can create, truncate and drop tables without touching a stack an operator has running.

.. rubric:: What makes these tests worth having

The deterministic corpus. Almost every assertion below computes its expected answer **in Python**,
by running the same filter over the objects :func:`~src.generators.generate_log_records` returned,
and then asserts the database returned exactly that set. That is a real comparison between two
independent computations. The alternative — asserting that a ``service`` filter returns rows whose
service is the one asked for — is a tautology: it would pass against a repository that returned
one arbitrary matching row and dropped the other forty.

.. rubric:: Schema and isolation

A **session-scoped** fixture creates the schema once (through the real
:meth:`~src.db.session.Database.init_db`, so the extension and the trigram index are exercised
too) and drops it at the end. Each test then gets a freshly **truncated** table.

Truncation rather than a rolled-back outer transaction, deliberately: the repository leaves
committing to its caller, so these tests commit — and a write that commits escapes a
rollback-based fixture unless every commit is wrapped in a restarting SAVEPOINT, which is a lot of
machinery whose failure mode is silent cross-test leakage. ``RESTART IDENTITY`` additionally
resets the id sequence, so ids are small and predictable in every test and the ``(timestamp, id)``
tiebreak assertions can be read at a glance.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import datetime, timedelta, timezone
from typing import TypeVar

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Settings
from src.db.base import Base
from src.db.models import LogEntryORM, LogRecord
from src.db.repository import LogQuery, LogRepository
from src.db.session import SEED_INSERT_CHUNK_SIZE, Database
from src.generators import generate_log_records

#: Fixed corpus parameters. ANCHOR is a constant instant, so the oracle a test computes and the
#: rows the database holds describe the same corpus no matter when the suite runs.
#:
#: 1200 rather than a couple of hundred because the level mix has a 1% CRITICAL tail: at 300 rows
#: the expected count is three, and "does the CRITICAL filter return the right rows" would be one
#: unlucky seed away from asserting that an empty set equals an empty set. At 1200 the thinnest
#: bucket is a dozen rows and every filter test grades a real subset.
CORPUS_SIZE = 1200
SEED = 20260725
ANCHOR = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)

#: Every index the model declares, by name. Asserted as a set so an index that is silently dropped
#: from ``__table_args__`` fails here rather than as an unexplained slow query at C14.
EXPECTED_INDEXES = {
    "ix_log_entries_ts_id",
    "ix_log_entries_service_ts",
    "ix_log_entries_level_ts",
    "ix_log_entries_trace_id",
    "ix_log_entries_message_trgm",
}

_T = TypeVar("_T")


def _run_sync(coro: Awaitable[_T]) -> _T:
    """Run ``coro`` on a private event loop, leaving the ambient loop policy untouched.

    Used by the session-scoped schema fixture, which is synchronous. ``asyncio.run`` would also
    work but it *sets* and then clears the current event loop, and pytest-asyncio manages that
    same global around every test — so this creates a loop, uses it, and closes it without ever
    touching :func:`asyncio.set_event_loop`. Anything opened inside the coroutine (the engine, its
    connections) must also be closed inside it, since the loop does not outlive the call.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)  # type: ignore[arg-type]
    finally:
        loop.close()


@pytest.fixture(scope="session")
def db_settings() -> Settings:
    """Configuration for the test database, read from the environment compose supplies.

    ``_env_file=None`` so a stray ``.env`` in the working directory cannot redirect the suite;
    environment variables still apply, which is the point — ``DATABASE_URL`` here is the compose
    ``test`` service's literal, pinned value pointing at ``gqllogs_test``.

    ``max_query_limit`` is raised above :data:`CORPUS_SIZE` so the oracle comparisons can ask for
    the whole corpus in one query. That deliberately takes the production clamp *out* of the way
    of these tests — the clamp itself is proved separately, by
    :func:`test_the_limit_is_clamped_on_every_path`, against a repository built with a small
    ceiling. Leaving the production 500 here would silently truncate every full-corpus comparison
    to 500 rows and turn a set-equality assertion into a prefix check.
    """
    return Settings(
        _env_file=None,
        seed_entries=0,
        seed_orders=0,
        log_level="WARNING",
        max_query_limit=CORPUS_SIZE * 5,
    )


@pytest.fixture(scope="session")
def _schema(db_settings: Settings) -> Iterator[None]:
    """Create the schema once per session through the real :meth:`Database.init_db`; drop it after.

    Going through ``init_db`` rather than a bare ``create_all`` is deliberate: it is what installs
    ``pg_trgm`` and creates the trigram index, and two tests below assert on exactly those. A
    fixture that took a shortcut would leave the production startup path untested by the suite
    that exists to test it.
    """

    async def _create() -> None:
        database = Database.create(db_settings)
        try:
            await database.init_db()
        finally:
            await database.dispose()

    async def _drop() -> None:
        database = Database.create(db_settings)
        try:
            async with database.engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
        finally:
            await database.dispose()

    _run_sync(_create())
    yield
    _run_sync(_drop())


@pytest.fixture()
async def database(_schema: None, db_settings: Settings) -> AsyncIterator[Database]:
    """A :class:`Database` over an empty ``log_entries``, disposed at the end of the test."""
    db = Database.create(db_settings)
    try:
        async with db.engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE log_entries RESTART IDENTITY"))
        yield db
    finally:
        # Every test disposes its own engine. An engine left open holds its pooled connections
        # until the process exits, and a suite that opens one per test would eventually meet
        # PostgreSQL's connection limit — as a failure in whichever test happened to run then.
        await db.dispose()


@pytest.fixture()
async def session(database: Database) -> AsyncIterator[AsyncSession]:
    """One session for the test, from the same factory the application uses."""
    async with database.session_factory() as async_session:
        yield async_session


@pytest.fixture()
def repo(session: AsyncSession, db_settings: Settings) -> LogRepository:
    """A repository bound to the test session, with configuration passed explicitly."""
    return LogRepository(session, db_settings)


@pytest.fixture()
async def seeded(database: Database) -> list[LogRecord]:
    """Seed the fixed corpus and return **the oracle**: the same records, in Python.

    The returned list is what every expectation below is computed from. It is generated a second
    time rather than captured from the seeder, so the two sides of each comparison come from
    independent calls — a generator that was accidentally stateful would be caught here rather
    than agreeing with itself.
    """
    written = await database.seed_if_empty(CORPUS_SIZE, SEED, end_time=ANCHOR)
    assert written == CORPUS_SIZE, "the fixture expected to seed an empty table"
    return generate_log_records(CORPUS_SIZE, seed=SEED, end_time=ANCHOR)


# --- Oracle helpers ------------------------------------------------------------------------------


def _newest_first(records: list[LogRecord]) -> list[LogRecord]:
    """Put a generated corpus into the order the database returns it in.

    The generator emits oldest-first with strictly increasing timestamps, and seeding inserts in
    that order — so ``BIGSERIAL`` ids ascend with time and ``ORDER BY timestamp DESC, id DESC`` is
    exactly the reverse of generation order. Reversing (rather than re-sorting) states that
    relationship instead of re-deriving it, so a test would notice if it ever stopped holding.
    """
    return list(reversed(records))


def _matching(
    records: list[LogRecord], predicate: Callable[[LogRecord], bool]
) -> list[LogRecord]:
    """The subset of the corpus a filter should select, newest first."""
    return _newest_first([record for record in records if predicate(record)])


def _as_records(rows: list[LogEntryORM]) -> list[LogRecord]:
    """Project database rows onto the identity-free value objects the oracle is made of."""
    return [LogRecord.from_orm_row(row) for row in rows]


async def _metadata_storage(session: AsyncSession, log_id: int) -> tuple[bool, str | None]:
    """Ask **PostgreSQL** how one row's ``metadata`` is stored: ``(is_sql_null, json_type)``.

    Python cannot answer this question. A JSONB column can hold the JSON scalar ``null``, which is
    not SQL ``NULL`` — and asyncpg deserialises *both* of them to the Python ``None``. So
    ``row.metadata_ is None`` is true in either case and an assertion built on it cannot fail.

    These two expressions can:

    * ``metadata IS NULL`` is true only for SQL ``NULL``. For the JSONB value ``'null'`` the column
      is not null at all, so it is false.
    * ``jsonb_typeof(metadata)`` returns SQL ``NULL`` (``None`` here) for SQL ``NULL``, and the
      **string** ``'null'`` for the JSON scalar — the one place the two are told apart by name.
    """
    row = (
        await session.execute(
            text(
                "SELECT metadata IS NULL AS is_sql_null, jsonb_typeof(metadata) AS json_type "
                "FROM log_entries WHERE id = :id"
            ),
            {"id": log_id},
        )
    ).one()
    return bool(row.is_sql_null), row.json_type


# --- Round trip ----------------------------------------------------------------------------------


async def test_a_row_round_trips_every_field(
    repo: LogRepository, session: AsyncSession
) -> None:
    """Insert with a metadata object and a trace id, read back byte-for-byte.

    The ``metadata`` assertion is the interesting one: it proves the ``metadata_`` attribute /
    ``metadata`` column split works in both directions, and that JSONB preserved the value types
    (``503`` comes back an ``int``, not the string ``"503"``).

    .. rubric:: ``expunge_all`` is what makes this a round trip at all

    ``expire_on_commit=False`` (deliberately — see :meth:`Database.create`) means the committed
    object stays fully loaded in the session's identity map, and ``Session.get`` answers from that
    map without touching the database. So the obvious version of this test would assert that the
    object we just built in Python still holds the values we put in it — a tautology that would
    pass with the entire read path broken. Detaching first forces a real SELECT.
    """
    payload = {"host": "node-3", "region": "eu-west-1", "latency_ms": 42, "status_code": 503}
    created = await repo.insert_log(
        service="payment-service",
        level="ERROR",
        message="payment authorization declined for order ord-52001",
        timestamp=ANCHOR,
        metadata=payload,
        trace_id="c0ffee0000000001",
    )
    await session.commit()

    assert created.id is not None and created.id > 0, "flush must populate the generated id"

    created_id = created.id
    session.expunge_all()
    fetched = await repo.get_by_id(created_id)

    assert fetched is not None
    assert fetched.service == "payment-service"
    assert fetched.level == "ERROR"
    assert fetched.message == "payment authorization declined for order ord-52001"
    assert fetched.trace_id == "c0ffee0000000001"
    assert fetched.metadata_ == payload
    assert fetched.metadata_["status_code"] == 503
    assert isinstance(fetched.metadata_["status_code"], int)
    assert fetched.timestamp == ANCHOR
    assert fetched.timestamp.tzinfo is not None
    assert fetched.timestamp.utcoffset() == timedelta(0)


async def test_absent_metadata_is_stored_as_sql_null_not_json_null(
    repo: LogRepository, session: AsyncSession
) -> None:
    """Omitted metadata is SQL ``NULL`` in the column; a supplied dict is a JSONB ``object``.

    .. rubric:: Why this is asserted in SQL and not in Python

    A JSONB column can hold the JSON scalar ``null``, which is a *different thing* from SQL
    ``NULL``, and the driver deserialises both of them to the Python ``None``. So the obvious
    version of this test — ``assert fetched.metadata_ is None`` and nothing else — passes in both
    worlds. It did: the column was declared without ``none_as_null=True``, every "null" metadata in
    the corpus was the JSONB scalar, ``SELECT count(*) … WHERE metadata IS NULL`` returned 0, and
    this test was green the whole time. A test that cannot fail for the condition it documents is
    worse than no test, because it is read as a guarantee.

    So the Python-side assertion stays (it is still the contract C3 exposes to a client) but it is
    no longer the *only* one, and both branches are pinned: the wrong storage for either one now
    fails here rather than at C11, when an aggregation over ``WHERE metadata IS NOT NULL`` quietly
    counts the whole table.
    """
    without = await repo.insert_log(
        service="search-service", level="INFO", message="health check passed"
    )
    with_object = await repo.insert_log(
        service="search-service",
        level="INFO",
        message="health check passed with detail",
        metadata={"host": "node-7"},
    )
    await session.commit()

    without_id, with_object_id = without.id, with_object.id
    session.expunge_all()  # force a real SELECT rather than an identity-map hit
    fetched = await repo.get_by_id(without_id)

    # The Python-visible contract: not `{}`, not a string, not a JSON-null lookalike.
    assert fetched is not None
    assert fetched.metadata_ is None
    assert fetched.trace_id is None

    # The assertions that can actually fail. See `_metadata_storage`.
    is_sql_null, json_type = await _metadata_storage(session, without_id)
    assert is_sql_null, (
        "omitted metadata must be SQL NULL; `metadata IS NULL` was false, so the column holds the "
        "JSONB scalar 'null' (SQLAlchemy's JSONB defaults to none_as_null=False)"
    )
    assert json_type is None, (
        f"jsonb_typeof(metadata) must be SQL NULL for an absent value, got {json_type!r} "
        "('null' is the JSON scalar, which is the bug this test exists for)"
    )

    # And the other branch, so "always NULL" is not a passing answer either.
    is_sql_null, json_type = await _metadata_storage(session, with_object_id)
    assert not is_sql_null, "a supplied dict must leave the column NOT NULL"
    assert json_type == "object"


async def test_a_naive_timestamp_is_stored_as_utc(
    repo: LogRepository, session: AsyncSession
) -> None:
    """A naive input keeps its wall time and comes back tagged UTC, never shifted by the server."""
    created = await repo.insert_log(
        service="api-gateway",
        level="INFO",
        message="request completed /graphql status=200 in 12ms",
        timestamp=datetime(2026, 7, 25, 12, 0, 0),
    )
    await session.commit()

    created_id = created.id
    session.expunge_all()  # force a real SELECT rather than an identity-map hit
    fetched = await repo.get_by_id(created_id)

    assert fetched is not None
    assert fetched.timestamp == ANCHOR
    assert fetched.timestamp.utcoffset() == timedelta(0)


async def test_get_by_id_returns_none_for_a_missing_row(repo: LogRepository) -> None:
    """A miss is ``None``, not an exception — C5's ``NOT_FOUND`` taxonomy depends on it."""
    assert await repo.get_by_id(999_999_999) is None


async def test_an_inserted_row_is_visible_to_a_follow_up_query(
    repo: LogRepository, session: AsyncSession
) -> None:
    """The spec's own verification flow: create a record, then find it with a query."""
    await repo.insert_log(
        service="order-service",
        level="WARNING",
        message="queue billing backlog grew to 41 messages",
    )
    await session.commit()
    session.expunge_all()  # the row must come back from PostgreSQL, not from the identity map

    found = await repo.list_logs(LogQuery(service="order-service"))

    assert [row.message for row in found] == ["queue billing backlog grew to 41 messages"]


# --- Seeding -------------------------------------------------------------------------------------


async def test_the_seeded_rows_are_the_generated_corpus(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """Every seeded row equals the corresponding generated record, in the expected order.

    This is the assertion the rest of the file rests on: if seeding and generation ever disagreed,
    every oracle-based expectation below would be comparing the database against a corpus it does
    not hold, and the failures would look like filter bugs.
    """
    rows = await repo.list_logs(LogQuery(limit=CORPUS_SIZE))

    assert len(rows) == CORPUS_SIZE
    assert _as_records(rows) == _newest_first(seeded)


async def test_the_seeded_corpus_holds_both_metadata_branches_as_sql_sees_them(
    seeded: list[LogRecord], session: AsyncSession
) -> None:
    """After seeding, ``metadata IS NULL`` selects exactly the records the oracle says have none.

    :func:`test_the_seeded_rows_are_the_generated_corpus` compares the corpus through the ORM, and
    the ORM is precisely the layer that cannot see this: JSONB ``'null'`` and SQL ``NULL`` both
    arrive as the Python ``None``, so that comparison is satisfied by either storage. This one
    counts in SQL instead, and it is the check the E2E step wanted and could not express — the
    seeder writes through a Core multi-row INSERT rather than the ORM, so it is a genuinely
    separate write path from the one the round-trip test above exercises.

    Both bounds matter. ``> 0`` is what the defect violated (every row was the JSONB scalar, so
    ``IS NULL`` counted zero). ``< CORPUS_SIZE`` is what stops a column that discarded every
    metadata object from passing. And the exact equality against the oracle is what makes it a
    real comparison rather than a smoke test: ``METADATA_RATIO`` is 0.7, so the two counts are
    roughly 360/840 at this corpus size and could not coincide by accident.
    """
    counts = (
        await session.execute(
            text(
                "SELECT count(*) AS total, "
                "count(*) FILTER (WHERE metadata IS NULL) AS sql_nulls, "
                "count(*) FILTER (WHERE jsonb_typeof(metadata) = 'null') AS json_nulls, "
                "count(*) FILTER (WHERE jsonb_typeof(metadata) = 'object') AS objects "
                "FROM log_entries"
            )
        )
    ).one()

    expected_nulls = sum(1 for record in seeded if record.metadata is None)
    assert 0 < expected_nulls < CORPUS_SIZE, "the corpus must exercise both branches to prove this"

    assert counts.total == CORPUS_SIZE
    assert 0 < counts.sql_nulls < counts.total
    assert counts.sql_nulls == expected_nulls
    assert counts.objects == CORPUS_SIZE - expected_nulls
    assert counts.json_nulls == 0, (
        f"{counts.json_nulls} rows hold the JSONB scalar 'null' instead of SQL NULL — "
        "the seeding INSERT path lost none_as_null=True"
    )


async def test_seed_if_empty_is_idempotent(
    seeded: list[LogRecord], database: Database, repo: LogRepository
) -> None:
    """A second call writes nothing and reports it, so a restart cannot double the corpus."""
    written_again = await database.seed_if_empty(CORPUS_SIZE, SEED, end_time=ANCHOR)

    assert written_again == 0
    assert await repo.count_logs(LogQuery()) == CORPUS_SIZE


async def test_seeding_zero_rows_is_a_no_op(database: Database, repo: LogRepository) -> None:
    """``SEED_ENTRIES=0`` — the compose ``test`` service's own configuration — writes nothing."""
    assert await database.seed_if_empty(0, SEED, end_time=ANCHOR) == 0
    assert await repo.count_logs(LogQuery()) == 0


async def test_ids_ascend_with_time(seeded: list[LogRecord], repo: LogRepository) -> None:
    """Insert order is time order, so the generated ids agree with the timestamps.

    Not decoration: it is what makes ``ORDER BY timestamp DESC, id DESC`` equal to "reverse of
    generation order", which is the assumption :func:`_newest_first` encodes.
    """
    rows = await repo.list_logs(LogQuery(limit=CORPUS_SIZE))
    ids = [row.id for row in rows]
    timestamps = [row.timestamp for row in rows]

    assert ids == sorted(ids, reverse=True)
    assert timestamps == sorted(timestamps, reverse=True)


# --- Individual filters, each graded against the oracle -------------------------------------------


@pytest.mark.parametrize("service", ["auth-service", "payment-service", "analytics-service"])
async def test_service_filter_selects_exactly_the_expected_rows(
    service: str, seeded: list[LogRecord], repo: LogRepository
) -> None:
    """The service filter returns the whole expected subset and nothing else."""
    expected = _matching(seeded, lambda r: r.service == service)
    assert expected, "the corpus must contain rows for this service or the test proves nothing"

    rows = await repo.list_logs(LogQuery(service=service, limit=CORPUS_SIZE))

    assert _as_records(rows) == expected


@pytest.mark.parametrize("level", ["ERROR", "INFO", "CRITICAL"])
async def test_level_filter_selects_exactly_the_expected_rows(
    level: str, seeded: list[LogRecord], repo: LogRepository
) -> None:
    """Same for level, including the thin 1% CRITICAL tail."""
    expected = _matching(seeded, lambda r: r.level == level)
    assert expected

    rows = await repo.list_logs(LogQuery(level=level, limit=CORPUS_SIZE))

    assert _as_records(rows) == expected


async def test_a_filter_that_matches_nothing_returns_an_empty_list(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """No rows is a legitimate answer, not an error and not "everything"."""
    assert await repo.list_logs(LogQuery(service="no-such-service")) == []


async def test_time_range_filter_selects_exactly_the_expected_rows(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """A window in the middle of the corpus returns a meaningful *partial* set."""
    start = seeded[100].timestamp
    end = seeded[200].timestamp
    expected = _matching(seeded, lambda r: start <= r.timestamp <= end)

    rows = await repo.list_logs(LogQuery(start_time=start, end_time=end, limit=CORPUS_SIZE))

    assert _as_records(rows) == expected
    assert 0 < len(expected) < CORPUS_SIZE, "the window must be a proper subset to prove anything"


async def test_both_time_bounds_are_inclusive(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """The rows sitting exactly on ``start_time`` and ``end_time`` are both included.

    The documented semantics are a closed interval, and boundary behaviour is precisely the thing
    a reader assumes rather than checks. Asserting on a two-row window makes it unambiguous: a
    half-open interval on either end would return one row, not two.
    """
    low, high = seeded[42], seeded[43]

    rows = await repo.list_logs(
        LogQuery(start_time=low.timestamp, end_time=high.timestamp, limit=CORPUS_SIZE)
    )

    assert _as_records(rows) == [high, low]


async def test_a_start_bound_alone_selects_the_tail(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """One bound supplied, the other omitted — the omitted one imposes nothing."""
    start = seeded[250].timestamp
    expected = _matching(seeded, lambda r: r.timestamp >= start)

    rows = await repo.list_logs(LogQuery(start_time=start, limit=CORPUS_SIZE))

    assert _as_records(rows) == expected
    assert len(expected) == CORPUS_SIZE - 250


async def test_search_text_selects_exactly_the_matching_messages(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """Substring search over ``message``, graded against a Python ``in`` over the same corpus."""
    needle = "timed out"
    expected = _matching(seeded, lambda r: needle in r.message.lower())
    assert expected

    rows = await repo.list_logs(LogQuery(search_text=needle, limit=CORPUS_SIZE))

    assert _as_records(rows) == expected
    assert len(expected) < CORPUS_SIZE, "a needle that matches everything proves nothing"


async def test_search_text_is_case_insensitive(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """``TIMED OUT`` and ``timed out`` select the same rows (spec §2 item 20)."""
    lower = await repo.list_logs(LogQuery(search_text="timed out", limit=CORPUS_SIZE))
    upper = await repo.list_logs(LogQuery(search_text="TIMED OUT", limit=CORPUS_SIZE))

    assert lower and _as_records(lower) == _as_records(upper)


# --- LIKE metacharacters: the whole point of the escaping -----------------------------------------


async def test_a_literal_percent_search_does_not_match_every_row(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """Searching for ``%`` finds messages *containing a percent sign*, not the entire table.

    Without :func:`~src.db.repository.escape_like` the pattern would be ``%%%`` — three wildcards
    — and this query would return the whole corpus while looking completely reasonable. The
    expected set is computed with a plain Python ``in``, so the two computations agree only if the
    escaping is right.
    """
    expected = _matching(seeded, lambda r: "%" in r.message)

    assert expected, "the corpus must contain messages with a literal '%'"
    assert len(expected) < CORPUS_SIZE, "the corpus must also contain messages without one"

    rows = await repo.list_logs(LogQuery(search_text="%", limit=CORPUS_SIZE))

    assert _as_records(rows) == expected


async def test_a_literal_underscore_search_does_not_match_every_row(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """``_`` is the single-character wildcard, and is escaped for the same reason ``%`` is.

    An unescaped ``_`` gives the pattern ``%_%``: "any message with at least one character",
    i.e. all of them.
    """
    expected = _matching(seeded, lambda r: "_" in r.message)

    assert expected
    assert len(expected) < CORPUS_SIZE

    rows = await repo.list_logs(LogQuery(search_text="_", limit=CORPUS_SIZE))

    assert _as_records(rows) == expected


async def test_a_backslash_search_is_matched_literally(
    repo: LogRepository, session: AsyncSession
) -> None:
    """The escape character itself survives a round trip through the pattern.

    The corpus contains no backslashes, so this one plants its own: a message with a literal
    backslash must be found by searching for a backslash, and a message without one must not.
    Double-escaping (or forgetting to escape the escape character) breaks exactly this case.
    """
    await repo.insert_log(service="auth-service", level="INFO", message="path C:\\temp\\cache")
    await repo.insert_log(service="auth-service", level="INFO", message="path /var/tmp/cache")
    await session.commit()

    rows = await repo.list_logs(LogQuery(search_text="\\"))

    assert [row.message for row in rows] == ["path C:\\temp\\cache"]


# --- Composition, omission, ordering, limits ------------------------------------------------------


async def test_all_supplied_filters_compose(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """Four dimensions at once narrow to the intersection, not the union.

    A builder that ORed its predicates, or that let a later filter overwrite an earlier one, would
    still return "rows that look right" for any single-filter test. Only the intersection catches
    it — and it is asserted to be a proper, non-empty subset of what each filter returns alone.

    The filter values are taken from one real record in the middle of the corpus rather than
    hardcoded. That guarantees the intersection is non-empty *by construction* — a four-way
    intersection of hardcoded values could easily select nothing on a given seed, and a test whose
    expected set is empty passes by asserting that ``[] == []``.
    """
    pivot = seeded[CORPUS_SIZE // 2]
    needle = pivot.message.split()[0]
    start, end = seeded[0].timestamp, seeded[-1].timestamp

    query = LogQuery(
        service=pivot.service,
        level=pivot.level,
        start_time=start,
        end_time=end,
        search_text=needle,
        limit=CORPUS_SIZE,
    )
    expected = _matching(
        seeded,
        lambda r: (
            r.service == pivot.service
            and r.level == pivot.level
            and start <= r.timestamp <= end
            and needle.lower() in r.message.lower()
        ),
    )

    rows = await repo.list_logs(query)

    assert pivot in expected, "the pivot record must satisfy the filters built from it"
    assert _as_records(rows) == expected

    # And the composition genuinely narrowed: the intersection is smaller than any single filter.
    service_only = await repo.list_logs(LogQuery(service=pivot.service, limit=CORPUS_SIZE))
    level_only = await repo.list_logs(LogQuery(level=pivot.level, limit=CORPUS_SIZE))
    assert len(rows) < len(service_only)
    assert len(rows) < len(level_only)


async def test_omitted_filters_are_ignored(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """A query with nothing supplied selects the whole corpus, capped by the default limit.

    Spec §2 item 19. The cap is the *configured* ``DEFAULT_QUERY_LIMIT``, and the rows returned
    are the newest that many — not an arbitrary slice.
    """
    default_limit = repo.settings.default_query_limit
    assert default_limit < CORPUS_SIZE, "the corpus must exceed the default limit to prove the cap"

    rows = await repo.list_logs(LogQuery())

    assert len(rows) == default_limit
    assert _as_records(rows) == _newest_first(seeded)[:default_limit]


async def test_results_are_newest_first_and_stable_across_calls(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """Repeating an identical query returns an identical sequence, ids included.

    Stability is what a cursor is built on. An ordering that were merely "sorted by timestamp"
    could legally permute rows sharing an instant between two calls, and a client paging through
    the result would silently skip or repeat one.
    """
    first = await repo.list_logs(LogQuery(limit=50))
    second = await repo.list_logs(LogQuery(limit=50))

    assert [row.id for row in first] == [row.id for row in second]
    assert [row.timestamp for row in first] == sorted(
        (row.timestamp for row in first), reverse=True
    )


async def test_id_breaks_ties_between_rows_sharing_a_timestamp(
    repo: LogRepository, session: AsyncSession
) -> None:
    """Three rows at the same instant come back highest-id first, every time.

    The seeded corpus deliberately has no duplicate timestamps, so the tiebreak would otherwise
    never be exercised — but ``createLog`` under load produces them readily, and this is the case
    where ``ORDER BY timestamp DESC`` alone is non-deterministic.
    """
    for suffix in ("a", "b", "c"):
        await repo.insert_log(
            service="api-gateway", level="INFO", message=f"tie {suffix}", timestamp=ANCHOR
        )
    await session.commit()

    rows = await repo.list_logs(LogQuery())

    assert [row.message for row in rows] == ["tie c", "tie b", "tie a"]
    assert [row.id for row in rows] == sorted((row.id for row in rows), reverse=True)


async def test_the_limit_is_clamped_on_every_path(
    seeded: list[LogRecord], session: AsyncSession
) -> None:
    """An over-large ``limit`` is capped at ``MAX_QUERY_LIMIT`` — spec §2 item 22.

    A repository built with a deliberately small ceiling, so the clamp is observable against a
    300-row corpus. The clamp lives in the statement builder, which is what makes it apply to the
    resolver, the DataLoader and any script that ever calls this — not only to the paths someone
    remembered to guard.
    """
    capped = LogRepository(
        session, Settings(_env_file=None, default_query_limit=10, max_query_limit=25)
    )

    assert len(await capped.list_logs(LogQuery(limit=1_000_000))) == 25
    assert len(await capped.list_logs(LogQuery(limit=7))) == 7
    assert len(await capped.list_logs(LogQuery())) == 10
    # 0 clamps up to 1 rather than returning an empty set that looks like "nothing matched".
    assert len(await capped.list_logs(LogQuery(limit=0))) == 1


async def test_count_matches_the_oracle_and_ignores_the_limit(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """``count_logs`` reports how many rows *match*, not how many a limited query would return."""
    assert await repo.count_logs(LogQuery()) == CORPUS_SIZE
    assert await repo.count_logs(LogQuery(limit=5)) == CORPUS_SIZE

    expected_errors = sum(1 for record in seeded if record.level == "ERROR")
    assert expected_errors > 0
    assert await repo.count_logs(LogQuery(level="ERROR")) == expected_errors


async def test_count_of_an_empty_match_is_zero(
    seeded: list[LogRecord], repo: LogRepository
) -> None:
    """Zero rather than ``None`` — a caller must never have to consider a missing count."""
    assert await repo.count_logs(LogQuery(service="no-such-service")) == 0


# --- Schema-level facts ---------------------------------------------------------------------------


async def test_the_pg_trgm_extension_is_installed(session: AsyncSession) -> None:
    """``init_db`` created the extension, which the trigram index's operator class requires."""
    result = await session.execute(
        text("SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'")
    )

    assert result.scalar_one_or_none() == "pg_trgm"


async def test_every_declared_index_exists_on_the_table(session: AsyncSession) -> None:
    """All five named indexes are present after ``init_db``."""
    result = await session.execute(
        text("SELECT indexname FROM pg_indexes WHERE tablename = 'log_entries'")
    )
    present = set(result.scalars().all())

    assert EXPECTED_INDEXES <= present, f"missing indexes: {sorted(EXPECTED_INDEXES - present)}"


@pytest.mark.parametrize(
    ("index_name", "expected_columns"),
    [
        ("ix_log_entries_ts_id", ["timestamp", "id"]),
        ("ix_log_entries_service_ts", ["service", "timestamp", "id"]),
        ("ix_log_entries_level_ts", ["level", "timestamp", "id"]),
    ],
)
async def test_the_ordering_indexes_end_with_the_id_tiebreak(
    index_name: str, expected_columns: list[str], session: AsyncSession
) -> None:
    """Every index that serves the default ordering carries ``id`` as its last column.

    Asserting the index *exists* is not enough, because an index missing its trailing ``id`` still
    serves the filter — the planner just adds a ``Sort (Sort Key: timestamp DESC, id DESC)`` node
    on top and every result is still correct. Nothing fails; the query is simply sorting rows it
    should have been reading in order, which is invisible until C3's keyset pagination is measured
    against the sub-100ms budget on the busiest filter in the system.

    So the column *list* is what is asserted, in order, and dropping the tiebreak from any of the
    three fails here rather than showing up as an unexplained regression at C14.
    """
    result = await session.execute(
        text(
            "SELECT indexdef FROM pg_indexes "
            "WHERE tablename = 'log_entries' AND indexname = :name"
        ),
        {"name": index_name},
    )
    definition = result.scalar_one()

    # `indexdef` ends in the parenthesised column list, and PostgreSQL quotes reserved words —
    # `timestamp` comes back as `"timestamp"`, so the quotes are stripped before comparing.
    columns = [
        column.strip().strip('"')
        for column in definition.rpartition("(")[2].rstrip(")").split(",")
    ]

    assert columns == expected_columns, f"{index_name} is {columns}, expected {expected_columns}"


async def test_the_message_index_is_a_gin_trigram_index(session: AsyncSession) -> None:
    """It is specifically a GIN index using ``gin_trgm_ops`` — a btree here would be ignored.

    Asserting the index merely *exists* would pass for a plain btree on ``message``, which the
    planner cannot use for a leading-wildcard ``ILIKE`` at all. The operator class is the whole
    point, so it is the thing asserted on.
    """
    result = await session.execute(
        text(
            "SELECT indexdef FROM pg_indexes "
            "WHERE tablename = 'log_entries' AND indexname = 'ix_log_entries_message_trgm'"
        )
    )
    definition = result.scalar_one()

    assert "USING gin" in definition
    assert "gin_trgm_ops" in definition


async def test_the_metadata_column_is_named_metadata_in_the_database(
    session: AsyncSession,
) -> None:
    """The Python attribute is ``metadata_``; the SQL column must be ``metadata``.

    ``metadata`` is reserved on a declarative class (``Base.metadata`` is the table registry), so
    the attribute carries a trailing underscore and ``mapped_column`` renames the column back. If
    someone "fixes" the underscore by dropping the rename, the column becomes ``metadata_``, the
    GraphQL field keeps working, and only a query written against the database by hand — or this
    test — notices.
    """
    result = await session.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'log_entries'"
        )
    )
    columns = set(result.scalars().all())

    assert "metadata" in columns
    assert "metadata_" not in columns
    assert columns == {"id", "timestamp", "service", "level", "message", "metadata", "trace_id"}


async def test_the_timestamp_column_is_timezone_aware(session: AsyncSession) -> None:
    """``timestamptz``, not ``timestamp``.

    Without the timezone, PostgreSQL discards the offset on write and every comparison happens in
    whatever ``TimeZone`` the server is set to — so the same query returns different rows on a
    differently-configured database, and no amount of normalisation in Python can fix it.
    """
    result = await session.execute(
        text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'log_entries' AND column_name = 'timestamp'"
        )
    )

    assert result.scalar_one() == "timestamp with time zone"


async def test_metadata_is_stored_as_jsonb_not_text(
    repo: LogRepository, session: AsyncSession
) -> None:
    """The column really is JSONB, so the database can index and query inside it.

    A ``Text`` column holding a JSON string would satisfy every round-trip assertion above and
    would still be the wrong type: C11's aggregations reach into this object from SQL.
    """
    created = await repo.insert_log(
        service="user-service",
        level="INFO",
        message="session established for user_id=u-1002 in eu-west-1",
        metadata={"host": "node-1", "status_code": 200},
    )
    await session.commit()

    result = await session.execute(
        text("SELECT metadata ->> 'host' FROM log_entries WHERE id = :id"),
        {"id": created.id},
    )

    assert result.scalar_one() == "node-1"


async def test_the_store_survives_a_large_multi_chunk_seed(database: Database) -> None:
    """Seeding more rows than one INSERT chunk holds writes all of them exactly once.

    ``SEED_INSERT_CHUNK_SIZE`` is 1000, so this crosses the boundary twice and leaves a short
    final chunk. An off-by-one in the chunk loop would drop or duplicate rows only at that seam.
    """
    size = SEED_INSERT_CHUNK_SIZE * 2 + 7
    written = await database.seed_if_empty(size, SEED, end_time=ANCHOR)

    assert written == size

    async with database.session_factory() as check_session:
        repository = LogRepository(check_session, Settings(_env_file=None))
        assert await repository.count_logs(LogQuery()) == size


async def test_repeated_lifespans_do_not_double_the_corpus(
    seeded: list[LogRecord], database: Database, db_settings: Settings
) -> None:
    """A second :class:`Database` over the same store also declines to seed.

    The idempotency check has to hold across *processes*, not just across calls on one object —
    that is the restart case, and it is guarded by a row count plus an advisory lock rather than
    by any in-memory flag.
    """
    other = Database.create(db_settings)
    try:
        assert await other.seed_if_empty(CORPUS_SIZE, SEED, end_time=ANCHOR) == 0
    finally:
        await other.dispose()

    async with database.session_factory() as check_session:
        repository = LogRepository(check_session, db_settings)
        assert await repository.count_logs(LogQuery()) == CORPUS_SIZE
