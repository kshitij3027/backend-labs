"""The per-request GraphQL context, and the ``context_getter`` the router is wired to.

.. rubric:: READ THIS BEFORE PUTTING A SESSION IN THE CONTEXT

**Strawberry resolves ``context_getter`` ONCE PER WEBSOCKET CONNECTION, not once per operation**
(`strawberry-graphql#1754 <https://github.com/strawberry-graphql/strawberry/issues/1754>`_). It is
declared as a FastAPI dependency, and for the WebSocket transport a dependency is solved when the
socket is accepted — after which the same context object serves every ``subscribe`` message on
that socket for as long as it stays open. Minutes. Hours.

So an :class:`~sqlalchemy.ext.asyncio.AsyncSession` created *here* would be a per-socket session,
and that is three bugs stacked on top of each other:

1. **A pooled connection is pinned for the life of the socket.** ``DB_POOL_SIZE`` is 10; ten idle
   dashboards would exhaust the pool while doing nothing at all.
2. **Every read on that socket answers from a frozen snapshot.** A session holds an open
   transaction and an identity map, so rows loaded once are handed back unchanged forever. The
   subscriber sees a database that stopped changing when it connected. Nothing errors.
3. **A failed statement poisons the rest of the socket.** One error puts the session in a state
   where every subsequent statement raises ``PendingRollbackError`` until somebody rolls back —
   and there is nobody, because the operation that failed is long gone.

None of the three raises at the point of the mistake, and the second one is close to the worst
failure a real-time API can have: a live view that is silently stale.

The fix is structural rather than careful: **the context carries the session FACTORY**, and a
session's lifetime is derived from the *operation*, never from the transport. Resolvers open one
with :meth:`Context.repository` (or :meth:`Context.session`) and it is closed before the resolver
returns. See the module docstring of :mod:`src.db.session` for the same argument from the store's
side.

.. rubric:: The seam this leaves for C5

C5 needs one session **shared by every resolver in a single operation**, because that is what makes
DataLoader batching possible — a loader that opened its own session per batch would still be one
round trip per field. That belongs in a ``SchemaExtension``'s ``on_operation`` hook, which fires
once per operation on both transports, and the marked seam below is where it installs itself. When
it lands, :meth:`Context.repository` prefers the operation-scoped session if one is present and
falls back to opening its own — so the resolvers written here do not change, and a subscription
(which deliberately gets no long-lived session) keeps using the short-lived path.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.requests import HTTPConnection
from strawberry.fastapi import BaseContext

from src.config import Settings, get_settings
from src.db.repository import LogRepository
from src.db.session import Database


class Context(BaseContext):
    """What every resolver reaches through ``info.context``.

    Subclasses Strawberry's :class:`~strawberry.fastapi.BaseContext` so the router populates
    ``request`` / ``response`` / ``background_tasks`` on it (that population is why the base class
    exists; returning a plain object instead would silently lose them).

    Attributes:
        settings: The application configuration. Carried on the context rather than read from
            :func:`src.config.get_settings` inside resolvers, so a test can execute an operation
            against deliberately different limits without touching a process-wide LRU cache.
        session_factory: The factory resolvers open sessions from. **Not a session** — see the
            module docstring.
        db: The owning :class:`~src.db.session.Database`, for the rare caller that needs the
            engine itself (schema-level work, the C14 memory probe). Optional so tests can build a
            context from a bare factory.
    """

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        db: Optional[Database] = None,
    ) -> None:
        super().__init__()
        self.settings = settings
        self.session_factory = session_factory
        self.db = db

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Open one session for the duration of the block, and close it on the way out.

        A single helper rather than ``async with ctx.session_factory() as session:`` hand-rolled
        in every resolver — not to save four words, but so that the day the lifetime rule changes
        (C5's per-operation session) there is exactly one place to change it and no resolver that
        quietly kept its own.
        """
        # === C5 ===  if (operation_session := getattr(self, "operation_session", None)):
        #                 yield operation_session          # opened by PerOperationResources
        #                 return
        # The fallback below stays for subscription resolvers, which deliberately get no
        # long-lived session: they open one per yielded item so a socket never pins a connection.
        async with self.session_factory() as session:
            yield session

    @asynccontextmanager
    async def repository(self) -> AsyncIterator[LogRepository]:
        """Open a session and hand back a :class:`~src.db.repository.LogRepository` over it.

        The altitude every read resolver actually wants. Note the repository is constructed with
        **this context's** settings, which is what makes the ``DEFAULT_QUERY_LIMIT`` /
        ``MAX_QUERY_LIMIT`` clamp observable to a test that injected different ones.
        """
        async with self.session() as session:
            yield LogRepository(session, self.settings)


async def get_context(connection: HTTPConnection) -> Context:
    """Build the context for one HTTP request — or, on the WebSocket transport, one connection.

    The parameter is annotated :class:`~starlette.requests.HTTPConnection` rather than ``Request``
    deliberately: FastAPI solves this function as a dependency for **both** the POST route and the
    WebSocket route, and ``Request`` cannot be provided for a WebSocket scope. ``HTTPConnection``
    is the common base of ``Request`` and ``WebSocket``, FastAPI injects it for either, and
    ``connection.app`` reaches the application state on both.

    Everything read here is process-scoped and already built: no session is opened, nothing is
    awaited against the database, nothing is cached. That is the whole design — see the module
    docstring for what happens when this function starts owning resources.

    Raises:
        RuntimeError: If the lifespan has not run. ``app.state.db`` is created in
            :func:`src.main.lifespan`, so this fires for a ``TestClient`` used without its context
            manager. Raised with an explanation rather than letting an ``AttributeError`` on
            ``state.db`` surface, because the fix ("enter the lifespan") is not deducible from the
            attribute error.
    """
    app = connection.app
    db: Optional[Database] = getattr(app.state, "db", None)
    if db is None:
        raise RuntimeError(
            "the GraphQL context needs app.state.db, which src.main.lifespan creates on startup. "
            "Reaching /graphql without it means the lifespan never ran — a TestClient must be "
            "entered as a context manager (`with TestClient(app) as client:`) for that to happen."
        )

    # `create_app` attaches this before the lifespan can run, so the fallback is only reached by
    # an application assembled some other way. It resolves configuration the same way production
    # does rather than inventing a default.
    settings: Settings = getattr(app.state, "settings", None) or get_settings()
    return Context(settings=settings, session_factory=db.session_factory, db=db)
