"""The engine, the session factory, boot-time schema creation, and seeding.

One object — :class:`Database` — owns all four, is built once in the lifespan and hung on
``app.state.db``. Everything that needs a session reaches it through
``request.app.state.db.session_factory`` rather than through a module global, which is what keeps
two applications in one test process from sharing a connection pool.

.. rubric:: The single most important design note in this file

**The GraphQL context holds the session FACTORY, never a session.**

Strawberry's ``context_getter`` resolves **once per WebSocket connection**, not once per
operation (strawberry-graphql#1754). A session created there would therefore live for the entire
socket: minutes or hours, one pooled connection pinned for the duration, and — because a session
caches loaded objects in its identity map and holds a transaction with a fixed snapshot — every
subsequent query on that socket answering from a view of the database frozen at connect time. The
symptom is not an error; it is a subscriber being served stale rows forever, which is close to
the worst failure a real-time API can have.

So :attr:`Database.session_factory` is the thing that gets passed around. C5's
``PerOperationResources`` extension opens a session from it in ``on_operation`` for query and
mutation operations and closes it at operation end; subscription resolvers open a short-lived one
per yielded item. Both are correct because both derive the session's lifetime from the *operation*
rather than from the transport.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.config import Settings
from src.db.base import Base

# Imported for its side effect as much as for the name: defining a mapped class is what registers
# its table in `Base.metadata`, and `create_all` only creates tables that are registered by the
# time it runs. See the note in src/db/base.py — this import is what makes it true.
from src.db.models import LogEntryORM
from src.generators import generate_log_records

logger = logging.getLogger(__name__)

#: Errors that mean "the database is not reachable *yet*" and are worth another attempt.
#:
#: Narrow on purpose. ``DBAPIError`` — the parent of all of these — would also catch
#: ``ProgrammingError`` (a syntax error in the DDL) and ``IntegrityError``, and retrying a broken
#: ``CREATE INDEX`` ten times at two seconds apart converts an instant, obvious failure into a
#: twenty-second one that ends with the same traceback buried under nine identical warnings.
#: ``OperationalError`` and ``InterfaceError`` are what SQLAlchemy wraps a refused connection, a
#: still-starting server and a dropped socket in; ``OSError`` covers the cases that never reach
#: the driver at all (DNS not resolving the compose service name yet, connection refused).
_RETRYABLE_BOOT_ERRORS: tuple[type[BaseException], ...] = (
    OperationalError,
    InterfaceError,
    OSError,
    asyncio.TimeoutError,
)

#: Rows per INSERT statement when seeding.
#:
#: The ceiling is not arbitrary: PostgreSQL's extended query protocol carries the parameter count
#: as a 16-bit integer, so a single statement can bind at most 32767 parameters. At six columns a
#: row that is ~5400 rows, and a chunk size chosen without knowing this is a bug that only appears
#: once ``SEED_ENTRIES`` is raised. 1000 rows (6000 parameters) sits comfortably below it while
#: still turning a 2000-row seed into two statements instead of 2000.
SEED_INSERT_CHUNK_SIZE = 1000

#: Advisory-lock key that serialises **the whole bootstrap** — schema creation and seeding — across
#: processes. ``uvicorn --workers N`` runs the lifespan once per worker, and neither operation is
#: atomic on its own:
#:
#: * two workers can both find the table absent and both issue ``CREATE TABLE``, and the loser
#:   gets ``DuplicateTable`` — ``create_all(checkfirst=True)`` checks, it does not lock;
#: * two workers can both find the table empty and both seed it, giving ``N x SEED_ENTRIES`` rows
#:   and an E2E verifier whose ground-truth counts are all wrong by a factor nobody can explain.
#:
#: ``pg_advisory_xact_lock`` closes both: the lock is held for the length of the transaction that
#: took it, so the losers block until the winner commits and then observe the finished state. One
#: key for both operations on purpose — a worker still creating the schema *should* block a worker
#: about to seed it. The number itself only has to be stable and unlikely to collide with another
#: application's advisory locks on the same server.
BOOTSTRAP_ADVISORY_LOCK_KEY = 7726_0725

#: The lock statement, built once. The key is interpolated rather than bound, which is safe here
#: and only here: it is an ``int`` constant defined three lines above, never anything a client can
#: influence. Binding it would leave asyncpg to negotiate the parameter's type with the server for
#: a function that has both a ``(bigint)`` and an ``(int, int)`` overload — a needless dependency
#: on type inference for a value that is a literal in the source.
_ADVISORY_LOCK_SQL = text(f"SELECT pg_advisory_xact_lock({BOOTSTRAP_ADVISORY_LOCK_KEY:d})")


class Database:
    """Owns the async engine and the session factory for one application instance."""

    def __init__(
        self,
        engine: AsyncEngine,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._engine = engine
        self._session_factory = session_factory
        self._settings = settings

    @classmethod
    def create(cls, settings: Settings) -> Database:
        """Build the engine and session factory. Opens no connection — that is :meth:`init_db`.

        ``pool_pre_ping=True`` issues a cheap liveness check before handing out a pooled
        connection. It costs a round trip per checkout and it is worth it here: this pool sits
        behind a compose network where the ``postgres`` container can restart under the API, and
        the alternative to pre-ping is the first query after such a restart failing with a stale
        socket — once per pooled connection, so the errors arrive spread out and look random.

        ``expire_on_commit=False`` is the other load-bearing argument. SQLAlchemy's default
        expires every attribute of every instance at commit, so the next attribute access emits a
        lazy-load SELECT. In a synchronous application that is merely wasteful; in this one it is
        a crash — a lazy load inside an async resolver raises ``MissingGreenlet``, because the
        implicit I/O has no await point to suspend on. C4's ``createLog`` commits and then returns
        the created object for Strawberry to resolve fields from, and C6 hands that same object to
        the broker to publish, both *after* the commit. Turning expiry off is what makes an object
        remain usable once its transaction is over.
        """
        engine = create_async_engine(
            settings.database_url,
            pool_pre_ping=True,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
        )
        session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        return cls(engine, session_factory, settings)

    @property
    def engine(self) -> AsyncEngine:
        """The engine. Exposed for schema-level work (DDL, catalog queries) and for tests."""
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """The factory to hand to the GraphQL context. **Not** a session — see the module docstring."""
        return self._session_factory

    async def init_db(self) -> None:
        """Create the extension and the schema, retrying while the server is still coming up.

        Compose already gates ``api`` on ``pg_isready``, but "the server accepts connections" and
        "this database is ready for DDL" are not the same instant — the official image's own
        initdb runs a temporary server that answers ``pg_isready`` while the real one is still
        starting, and the ``docker-entrypoint-initdb.d`` script that creates ``gqllogs_test`` runs
        in that window. So the healthcheck reduces the retry window; it does not remove it.

        .. rubric:: The two statements are ordered, and the order is not negotiable

        ``CREATE EXTENSION IF NOT EXISTS pg_trgm`` runs **before** ``create_all``, in the same
        transaction. ``ix_log_entries_message_trgm`` is declared with ``gin_trgm_ops``, and that
        operator class does not exist until the extension does — so the reverse order fails with
        ``operator class "gin_trgm_ops" does not exist``, and it fails at *index* creation, after
        the table has already been created, leaving a half-built schema behind. Both statements
        share one transaction so either the whole schema appears or none of it does.

        (``pg_trgm`` is a *trusted* extension from PostgreSQL 13 onward, so a database owner can
        create it without superuser rights. The compose role is the image's ``POSTGRES_USER``,
        which is a superuser anyway, and it owns ``gqllogs_test`` explicitly — see
        ``docker/postgres-init/10-create-test-db.sql``.)

        Raises:
            Exception: The last error, re-raised once ``DB_INIT_RETRIES`` attempts have failed, or
                immediately for any error outside :data:`_RETRYABLE_BOOT_ERRORS`.
        """
        attempts = max(1, self._settings.db_init_retries)
        for attempt in range(1, attempts + 1):
            try:
                async with self._engine.begin() as conn:
                    # Serialise the DDL across processes before touching the catalog — see
                    # BOOTSTRAP_ADVISORY_LOCK_KEY. `CREATE EXTENSION IF NOT EXISTS` and
                    # `create_all(checkfirst=True)` are both check-then-act, so under `uvicorn
                    # --workers N` the loser of the race gets a duplicate-object error rather
                    # than the no-op the "IF NOT EXISTS" spelling suggests.
                    await conn.execute(_ADVISORY_LOCK_SQL)
                    await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                    await conn.run_sync(Base.metadata.create_all)
            except _RETRYABLE_BOOT_ERRORS as exc:
                logger.warning(
                    "init_db attempt %d/%d failed (%s): %s",
                    attempt,
                    attempts,
                    type(exc).__name__,
                    exc,
                )
                if attempt == attempts:
                    raise
                await asyncio.sleep(self._settings.db_init_retry_delay_seconds)
            else:
                logger.info(
                    "database schema ready after %d attempt(s): tables=%s",
                    attempt,
                    ", ".join(sorted(Base.metadata.tables)),
                )
                return

    async def seed_if_empty(
        self,
        count: int,
        seed: int,
        *,
        end_time: datetime | None = None,
    ) -> int:
        """Fill an empty ``log_entries`` with a deterministic corpus. Idempotent.

        Counts first and returns ``0`` without writing anything if the table already holds rows,
        so a container restart against a persisted volume does not double the corpus, and so
        ``make up`` twice is the same as ``make up`` once.

        Args:
            count: How many rows to write. ``0`` (the compose ``test`` service) is a no-op.
            seed: RNG seed handed to :func:`~src.generators.generate_log_records`.
            end_time: Newest instant in the generated corpus. Defaults to now, which is what
                production wants — a dashboard whose newest log line is from container build time
                looks broken. Tests and the E2E verifier pass a **fixed** instant instead, which
                is the whole point of the parameter: it is what lets them regenerate the identical
                corpus locally and use it as an oracle for what the database should return.

        Returns:
            The number of rows actually written — ``0`` when the table was already populated, so
            the lifespan can log which of the two happened rather than guessing.
        """
        if count <= 0:
            return 0

        anchor = end_time if end_time is not None else datetime.now(timezone.utc)

        async with self._session_factory() as session, session.begin():
            # Taken BEFORE the count and held until this transaction commits, which is what makes
            # "check then insert" atomic across processes. See BOOTSTRAP_ADVISORY_LOCK_KEY.
            await session.execute(_ADVISORY_LOCK_SQL)

            existing = await session.scalar(select(func.count()).select_from(LogEntryORM))
            if existing:
                logger.info("store already holds %d rows; skipping seed", existing)
                return 0

            # Generated only once the table is known to be empty and the lock is held, so a
            # restart against a populated volume does no work at all.
            records = generate_log_records(count, seed=seed, end_time=anchor)

            # Core multi-row INSERT against the Table rather than the ORM: the ORM's unit of work
            # would build `count` mapped instances, put every one of them in an identity map and
            # then flush them, which is a lot of machinery for rows nobody is going to mutate.
            # `as_insert_params()` supplies COLUMN names (so `metadata`, not `metadata_`) and
            # deliberately omits `id`, letting BIGSERIAL assign ids in insert order — which is
            # ascending timestamp order, because the generator emits oldest first.
            table = LogEntryORM.__table__
            for start in range(0, len(records), SEED_INSERT_CHUNK_SIZE):
                chunk = records[start : start + SEED_INSERT_CHUNK_SIZE]
                await session.execute(
                    insert(table).values([record.as_insert_params() for record in chunk])
                )
            written = len(records)

        logger.info("seeded %d log rows (seed=%d, newest=%s)", written, seed, anchor.isoformat())
        return written

    async def dispose(self) -> None:
        """Close every pooled connection. Called from the lifespan's shutdown path.

        Not optional housekeeping: an engine that is never disposed leaves its connections open
        until the process dies, and in a test process that runs many lifespans in sequence the
        pooled connections accumulate until PostgreSQL refuses new ones with "too many clients
        already" — a failure that lands on whichever test happens to be running at the time.
        """
        await self._engine.dispose()
