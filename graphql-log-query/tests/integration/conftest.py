"""Integration fixtures: the real PostgreSQL store, the real app, and the real GraphQL context.

Nothing here is faked. ``timestamptz`` round-tripping, ``JSONB``, ``ILIKE`` with an ``ESCAPE``
clause, a GIN trigram index and ``BIGSERIAL`` id assignment are all things SQLite cannot emulate,
and every one of them is load-bearing. The compose ``test`` service points ``DATABASE_URL`` at a
separate database (``gqllogs_test``, created by ``docker/postgres-init/10-create-test-db.sql``)
precisely so this suite can create, truncate and drop tables without touching a stack an operator
has running.

.. rubric:: Schema and isolation

A **session-scoped** fixture creates the schema once — through the real
:meth:`~src.db.session.Database.init_db`, so the ``pg_trgm`` extension and the trigram index are
exercised too — and drops it at the end. Each test then gets a freshly **truncated** table.

Truncation rather than a rolled-back outer transaction, deliberately: the repository leaves
committing to its caller, so these tests commit — and a write that commits escapes a
rollback-based fixture unless every commit is wrapped in a restarting SAVEPOINT, which is a lot of
machinery whose failure mode is silent cross-test leakage. ``RESTART IDENTITY`` additionally resets
the id sequence, so ids are small and predictable in every test and the ``(timestamp, id)``
tiebreak assertions can be read at a glance.

.. rubric:: Two ways to drive the application, and both are here on purpose

* ``gql_context`` executes operations straight against the schema with an injected
  :class:`~src.graphql.context.Context`. Fast, and it is the only way to run the same operation
  under *deliberately different* settings (the ``MAX_QUERY_LIMIT`` clamp is only observable when a
  test can choose the ceiling).
* ``http_client`` drives the assembled ASGI app over HTTP, through the real lifespan. That is what
  proves the router mount, the ``context_getter`` dependency and the GraphQL JSON envelope work —
  none of which ``schema.execute`` touches at all. A suite with only the first would be green with
  the application unmounted.

``real_app`` / ``real_client`` build the app the way production does — :func:`src.main.create_app`
with **no injected settings**, so configuration resolves exactly as it does in the container. The
unit suite's ``client`` skips the lifespan to stay fast and hermetic; the code that runs *in* the
lifespan is precisely the code that can only break in production, so it gets driven here.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Settings
from src.db.base import Base
from src.db.models import LogRecord
from src.db.repository import LogRepository
from src.db.session import Database
from src.generators import EventCorpus, generate_event_corpus, generate_log_records
from src.graphql.context import Context
from src.main import create_app
from tests.integration.corpus import (
    ANCHOR,
    CORPUS_SIZE,
    EVENT_CORPUS_ORDERS,
    SEED,
    CorrelatedCorpus,
    run_sync,
)


# --- The application, as production builds it ----------------------------------------------------


@pytest.fixture()
def real_app() -> FastAPI:
    """The application as production builds it: settings resolved from the environment."""
    return create_app()


@pytest.fixture()
def real_client(real_app: FastAPI) -> Iterator[TestClient]:
    """A TestClient over ``real_app``, entered as a context manager so the lifespan runs.

    Yielded from a generator fixture rather than returned, so the ``__exit__`` — and therefore the
    shutdown half of the lifespan — is guaranteed to run even when the test fails.
    """
    with TestClient(real_app) as client:
        yield client


@pytest.fixture()
async def http_client(real_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """An **async** HTTP client over ``real_app``, with the lifespan entered by hand.

    ``httpx`` + ``ASGITransport`` rather than ``TestClient`` because the tests that use it are
    ``async def``: ``TestClient`` runs the app on its own event loop in another thread, so an async
    test would be blocking its own loop to wait on a second one — workable, but it makes any
    interleaving with the async database fixtures a matter of luck rather than of ordering.

    ``ASGITransport`` does not run lifespan events (it only ever sends ``http.*`` scopes), so the
    lifespan is entered explicitly. That is not optional here: :func:`src.graphql.context.get_context`
    reads ``app.state.db``, which the lifespan creates, so without this every GraphQL request would
    fail on a missing database rather than on anything under test.
    """
    async with real_app.router.lifespan_context(real_app):
        transport = httpx.ASGITransport(app=real_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://graphql-log-query.test"
        ) as client:
            yield client


# --- The real PostgreSQL store -------------------------------------------------------------------


@pytest.fixture(scope="session")
def db_settings() -> Settings:
    """Configuration for the test database, read from the environment compose supplies.

    ``_env_file=None`` so a stray ``.env`` in the working directory cannot redirect the suite;
    environment variables still apply, which is the point — ``DATABASE_URL`` here is the compose
    ``test`` service's literal, pinned value pointing at ``gqllogs_test``.

    ``max_query_limit`` is raised above :data:`~tests.integration.corpus.CORPUS_SIZE` so the oracle
    comparisons can ask for the whole corpus in one query. That deliberately takes the production
    clamp *out* of the way of most tests — the clamp itself is proved separately, by tests that
    build their own ``Settings`` with a small ceiling. Leaving the production 500 here would
    silently truncate every full-corpus comparison to 500 rows and turn a set-equality assertion
    into a prefix check.
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
    ``pg_trgm`` and creates the trigram index, and tests assert on exactly those. A fixture that
    took a shortcut would leave the production startup path untested by the suite that exists to
    test it.
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

    run_sync(_create())
    yield
    run_sync(_drop())


@pytest.fixture()
async def database(_schema: None, db_settings: Settings) -> AsyncIterator[Database]:
    """A :class:`Database` over an **empty store**, disposed at the end of the test.

    All four tables are truncated, not just ``log_entries``. C10's event tables are seeded by the
    same :meth:`~src.db.session.Database.seed_if_empty` call, and that call's idempotency check is
    "does this table already hold rows" — so a test that left order events behind would silently
    turn the *next* test's seeding into a no-op, and the failure would land wherever the empty
    corpus happened to be noticed rather than where it was caused.

    One statement rather than four: ``TRUNCATE a, b, c, d`` takes all four locks at once, which
    cannot deadlock against another session doing the same, and ``RESTART IDENTITY`` resets every
    sequence so ids stay small and readable in every test.
    """
    db = Database.create(db_settings)
    try:
        async with db.engine.begin() as conn:
            await conn.execute(
                text(
                    "TRUNCATE TABLE log_entries, order_events, payment_events, user_events "
                    "RESTART IDENTITY"
                )
            )
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

    The returned list is what expectations are computed from. It is generated a second time rather
    than captured from the seeder, so the two sides of each comparison come from independent calls
    — a generator that was accidentally stateful would be caught here rather than agreeing with
    itself.

    ``orders`` is left at 0, so this corpus is **uncorrelated**: every trace here belongs to log rows
    only. That is what the filter and ``relatedLogs`` tests want, and it is why neither this fixture
    nor any expectation graded against it moved when C10 correlated the two corpora — the correlation
    is an argument the seeder only passes when there are orders to correlate with. Tests that need
    the joined corpus use :func:`seeded_correlated`.
    """
    written = await database.seed_if_empty(CORPUS_SIZE, SEED, end_time=ANCHOR)
    assert written == CORPUS_SIZE, "the fixture expected to seed an empty table"
    return generate_log_records(CORPUS_SIZE, seed=SEED, end_time=ANCHOR)


@pytest.fixture()
async def seeded_events(database: Database) -> EventCorpus:
    """Seed the fixed **e-commerce** corpus and return the oracle for it.

    Deliberately a separate fixture from :func:`seeded` rather than one that seeds both: most
    integration tests grade log queries and would otherwise pay for three extra tables of inserts,
    and — more importantly — a test that asks only for events proves the event seeding works with
    ``SEED_ENTRIES`` effectively 0, which is the compose ``test`` service's own configuration.

    ``count=0`` for the log corpus is what makes that explicit.

    **A test that needs both corpora asks for** :func:`seeded_correlated`, not for this fixture and
    :func:`seeded` together. Requesting both would populate both tables, but the log corpus would be
    generated with no ``order_traces`` — i.e. correlated with nothing — so ``correlatedEvents`` would
    return three ``__typename``s and the fourth would look like a resolver bug.

    Regenerated rather than captured from the seeder, for the same reason :func:`seeded` regenerates
    — the two sides of every comparison must come from independent calls, or a stateful generator
    would agree with itself.
    """
    await database.seed_if_empty(0, SEED, orders=EVENT_CORPUS_ORDERS, end_time=ANCHOR)
    return generate_event_corpus(EVENT_CORPUS_ORDERS, seed=SEED, end_time=ANCHOR)


@pytest.fixture()
async def seeded_correlated(database: Database) -> CorrelatedCorpus:
    """Seed **both** corpora in one call — the only fixture where a log line joins an order.

    :func:`seeded` and :func:`seeded_events` deliberately fill one side each, because most tests
    grade one surface and should not pay for the other's inserts. But the schema's flagship claim —
    ``correlatedEvents(traceId:)`` returning all four ``__typename``s in one round trip, the thing
    that would otherwise be four REST calls — is only observable when both are present, so it gets
    its own fixture rather than being wedged into either of those.

    .. rubric:: The oracle is regenerated the way the seeder generated it, and that is not optional

    :meth:`~src.db.session.Database.seed_if_empty` generates the event corpus first and hands its
    trace ids to the log generator, because a declared fraction of orders also carry log lines (see
    :data:`~src.generators.ORDER_TRACE_LOG_RATIO`). Regenerating the log corpus *without*
    ``order_traces`` here would produce a corpus with different trace ids from the one in the
    database — so every assertion graded against it would fail, and it would fail somewhere far away
    from the mistake. The two calls below mirror the seeder's exactly.
    """
    written = await database.seed_if_empty(
        CORPUS_SIZE, SEED, orders=EVENT_CORPUS_ORDERS, end_time=ANCHOR
    )
    assert written == CORPUS_SIZE, "the fixture expected to seed an empty store"

    events = generate_event_corpus(EVENT_CORPUS_ORDERS, seed=SEED, end_time=ANCHOR)
    logs = generate_log_records(
        CORPUS_SIZE, seed=SEED, end_time=ANCHOR, order_traces=events.trace_ids()
    )
    return CorrelatedCorpus(logs=logs, events=events)


# --- The GraphQL layer over that store -----------------------------------------------------------


@pytest.fixture()
def gql_context(database: Database, db_settings: Settings) -> Context:
    """A GraphQL context wired to the test database, for ``schema.execute(...)``.

    Built directly rather than through :func:`src.graphql.context.get_context`, because that
    function's job is to pull the same three things off ``app.state`` — which is what the
    ``http_client`` fixture exercises. Constructing it here is what lets a test choose the
    settings an operation runs under.

    Note it holds the **session factory**, exactly as the production context does. A fixture that
    handed resolvers a live session would be testing an arrangement the server does not use — and
    specifically the arrangement src/graphql/context.py exists to argue against.
    """
    return Context(
        settings=db_settings,
        session_factory=database.session_factory,
        db=database,
    )
