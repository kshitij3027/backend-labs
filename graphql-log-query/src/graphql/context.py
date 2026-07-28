"""The GraphQL context, the per-operation resources, and the extension that owns their lifecycle.

.. rubric:: READ THIS BEFORE PUTTING A SESSION OR A LOADER IN THE CONTEXT

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

A **DataLoader** created here is the same bug wearing different clothes, and arguably worse: a
loader memoises by key, so a socket-scoped loader answers ``relatedLogs`` for the rest of the
connection from whatever it read the first time. Nothing errors there either. A cache whose
lifetime is a *transport* rather than a *request* is a data leak waiting for a second user.

.. rubric:: What C5 did about it

:class:`PerOperationResources` — a ``SchemaExtension`` whose ``on_operation`` hook fires **once per
operation on both transports**, which is the granularity that is actually correct. For every
operation it mints a fresh :class:`~src.graphql.loaders.LoaderRegistry`, and for query and mutation
operations it lends out **one** ``AsyncSession`` shared by every resolver and every loader batch in
that operation, closed when the operation ends.

**Subscription operations deliberately get no long-lived session.** A subscription's operation
scope is the whole life of the stream — Strawberry wraps the entire ``async for`` yield loop in
``on_operation`` — so a session opened there would be exactly the per-socket session this module
exists to argue against, just reached by a different route. Under a subscription the resources
object hands out a short-lived session per request instead, and C6's resolver opens one per yielded
item.

.. rubric:: Why the resources live in a ContextVar and not on the context object

One WebSocket connection multiplexes **concurrent** operations (``graphql-transport-ws`` allows
many ``subscribe`` messages on one socket; ``MAX_SUBSCRIPTIONS_PER_CONNECTION`` caps it at 10), and
they all share this one context object. An attribute on ``self`` would therefore be a single slot
that concurrent operations overwrite: operation B would hand its loaders to operation A's
resolvers, and whichever finished first would close the session the other was still using. Saving
and restoring the previous value does not help, because concurrent operations do not finish in the
order they started.

A :class:`~contextvars.ContextVar` has exactly the right scope. Strawberry's WebSocket handler runs
each operation in its own :class:`asyncio.Task`, and a task gets a *copy* of the context it was
created in — so a value set inside one operation is invisible to its siblings and inherited by the
child tasks graphql-core creates to resolve fields concurrently. Which is precisely the shape we
need: per-operation, and readable from anywhere underneath it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.requests import HTTPConnection
from strawberry.extensions import SchemaExtension
from strawberry.fastapi import BaseContext
from strawberry.types.graphql import OperationType

from src.config import Settings, get_settings
from src.db.repository import LogRepository
from src.db.session import Database
from src.graphql.loaders import LoaderRegistry

if TYPE_CHECKING:  # pragma: no cover - annotations only; `from __future__` makes them strings
    from strawberry.types import ExecutionContext

logger = logging.getLogger(__name__)

#: How long a caller waits for the operation's shared session before giving up. Not a tuning knob
#: and deliberately not a setting: it is a **deadlock detector**, and the only thing it needs to be
#: is longer than any legitimate wait. The whole operation is budgeted at sub-100ms (spec §5), so
#: thirty seconds means "this is never coming" with a wide margin. See
#: :class:`OperationResources` for the mistake it catches.
SESSION_LOCK_TIMEOUT_SECONDS = 30.0

#: The resources belonging to the operation running in *this* asyncio context. Set by
#: :class:`PerOperationResources` and read by :class:`Context`; see the module docstring for why it
#: is a ContextVar rather than an attribute on the context object.
_OPERATION_RESOURCES: ContextVar[Optional["OperationResources"]] = ContextVar(
    "graphql_operation_resources", default=None
)


def operation_is_subscription(execution_context: ExecutionContext) -> bool:
    """Is the operation being executed a subscription?

    Answered from the **parsed document**, which is the only place the truth lives:

    * ``allowed_operations`` cannot answer it. Strawberry derives that from the HTTP method, and
      ``POST`` allows all three types — every ordinary query would look like a possible
      subscription.
    * ``execution_context.operation_type`` can, but only *after* parsing. Parsing happens **inside**
      ``on_operation``, so this question has no answer at the start of the hook: the property raises
      ``RuntimeError("No GraphQL document available")`` there. That is exactly why
      :class:`OperationResources` opens its session lazily rather than up front — by the time a
      resolver or a loader batch asks for one, the document is parsed and this is answerable.

    Returns:
        ``True`` for a subscription, **and also when the operation type cannot be determined**.
        The conservative answer is the one that declines to hold a connection open: an unknown
        operation gets short-lived sessions, which is never wrong, only slightly less efficient.
    """
    try:
        return execution_context.operation_type is OperationType.SUBSCRIPTION
    except Exception:  # noqa: BLE001 - see the Returns note: any failure means "do not share"
        return True


class OperationResources:
    """The loaders and (for non-subscriptions) the one session belonging to a single operation.

    Built by :class:`PerOperationResources` and reachable through :meth:`Context.loaders` and
    :meth:`Context.session`. Nothing else should construct one.

    .. rubric:: The session is opened LAZILY, and both halves of that matter

    *Lazily* because the operation type is unknowable until the document is parsed, which happens
    after the hook has already started (see :func:`operation_is_subscription`) — and because an
    operation that never touches the database (``{ __typename }``, an introspection query, a cache
    hit once C7 lands) should not check a connection out of a pool sized 10 to do nothing with it.

    .. rubric:: Access to the shared session is serialised, because a session is not concurrency-safe

    graphql-core resolves sibling fields **concurrently**, so two resolvers — or two DataLoader
    batches dispatched in the same tick — can reach for the session at the same instant. An
    ``AsyncSession`` handed two overlapping statements raises
    ``IllegalStateChangeError``/``InvalidRequestError``, or worse interleaves them on one
    connection. The lock below makes concurrent callers queue instead, which costs nothing real:
    one session is one connection, and one connection can only run one statement at a time anyway.

    The lock is **re-entrant within one task**, because a task cannot interleave with itself: a
    resolver that has the session and opens a nested :meth:`Context.repository` block (C10 wants
    exactly that — an order event and the log line describing it inside one transaction) must not
    queue behind itself.

    .. rubric:: THE ONE RULE THIS IMPOSES: do not await a loader while holding the session

    A DataLoader dispatches its batch in **its own asyncio task**. So a resolver that holds the
    session and then awaits ``loader.load(...)`` is waiting on a task that is queueing for the
    session it is holding — a deadlock, and one that no test would report as anything other than a
    suite that never finishes. Load first, then open the session; every resolver in the project
    does. :data:`SESSION_LOCK_TIMEOUT_SECONDS` turns the hang into an error that names this
    paragraph, because a diagnosable failure at thirty seconds beats a hung CI job.
    """

    def __init__(
        self,
        context: "Context",
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        is_subscription: Callable[[], bool],
    ) -> None:
        """Build the resources for one operation.

        Args:
            context: The context these belong to. Kept so :class:`Context` can refuse to serve
                resources minted for a *different* context — see :meth:`Context._resources`.
            session_factory: Where the shared session comes from, if one is ever needed.
            settings: Carried into the loaders (``MAX_QUERY_LIMIT``, the batch window).
            is_subscription: Called the first time a session is asked for. A callable rather than a
                bool because the answer is not knowable when this object is built.
        """
        self.context = context
        self._session_factory = session_factory
        self._is_subscription = is_subscription
        self._shared: Optional[AsyncSession] = None
        self._lock = asyncio.Lock()
        self._owner: Optional[asyncio.Task] = None
        self.loaders = LoaderRegistry(context.repository, settings)

    @property
    def shared_session(self) -> Optional[AsyncSession]:
        """The operation-scoped session, or ``None`` if none has been opened. For tests."""
        return self._shared

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield the session this operation should use for one unit of work.

        For a **subscription**, a fresh short-lived session that is closed on the way out — see the
        module docstring for why a stream must never hold one. For anything else, the operation's
        shared session, opened on first use and closed by :meth:`aclose`.
        """
        if self._is_subscription():
            async with self._session_factory() as session:
                yield session
            return

        task = asyncio.current_task()
        if self._owner is not None and self._owner is task and self._shared is not None:
            # Re-entry from the task that already holds it. Sequential by construction, so there is
            # nothing to serialise — and taking the lock again here is a self-deadlock.
            yield self._shared
            return

        await self._acquire()
        try:
            if self._shared is None:
                # Constructed, not entered: `async with` would close it at the end of this block,
                # and the whole point is that it outlives this block and serves the next resolver.
                # No connection is checked out until the first statement runs.
                self._shared = self._session_factory()
            self._owner = task
            try:
                yield self._shared
            except BaseException:
                # A failed statement leaves the session unusable until somebody rolls back, and the
                # next resolver in this operation would meet `PendingRollbackError` about a
                # failure it had nothing to do with. Suppressed on the way out because the original
                # exception is the interesting one and a rollback on a session that never managed
                # to connect raises again.
                with suppress(Exception):
                    await self._shared.rollback()
                raise
            finally:
                self._owner = None
        finally:
            self._lock.release()

    async def _acquire(self) -> None:
        """Take the session lock, or fail with the reason it could not be taken.

        Raises:
            RuntimeError: If the lock could not be taken within
                :data:`SESSION_LOCK_TIMEOUT_SECONDS`. See the rule in the class docstring: the
                cause is almost always a loader awaited from inside a session block. The
                alternative to raising is waiting forever, which presents as a test suite that
                stops rather than fails.
        """
        if not self._lock.locked():
            # The uncontended path, which is nearly every path: `Lock.acquire` on a free lock has
            # no await point at all, so this reaches the session without a trip through the
            # scheduler. Race-free because there is no suspension between the check and the take.
            await self._lock.acquire()
            return

        try:
            await asyncio.wait_for(self._lock.acquire(), SESSION_LOCK_TIMEOUT_SECONDS)
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                "timed out waiting for this operation's database session after "
                f"{SESSION_LOCK_TIMEOUT_SECONDS:g}s. The usual cause is a DataLoader awaited from "
                "INSIDE a Context.session()/Context.repository() block: the loader dispatches its "
                "batch in its own task, which then queues for the session the awaiting resolver "
                "is still holding, and neither can proceed. Load first, then open the session."
            ) from exc

    async def aclose(self) -> None:
        """Close the shared session, if one was ever opened. Idempotent."""
        session, self._shared = self._shared, None
        if session is not None:
            await session.close()


class Context(BaseContext):
    """What every resolver reaches through ``info.context``.

    Subclasses Strawberry's :class:`~strawberry.fastapi.BaseContext` so the router populates
    ``request`` / ``response`` / ``background_tasks`` on it (that population is why the base class
    exists; returning a plain object instead would silently lose them).

    Attributes:
        settings: The application configuration. Carried on the context rather than read from
            :func:`src.config.get_settings` inside resolvers, so a test can execute an operation
            against deliberately different limits without touching a process-wide LRU cache.
        session_factory: The factory sessions are opened from. **Not a session** — see the module
            docstring.
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

    def _resources(self) -> Optional[OperationResources]:
        """The current operation's resources, or ``None`` outside an operation.

        The ownership check is not paranoia about Strawberry: a ContextVar is process-wide, and one
        process runs several applications in the test suite alone. Resources minted for another
        context carry another session factory and another set of loaders, and serving them here
        would be the cross-request leak this whole design exists to prevent — so they are treated
        as absent rather than borrowed.
        """
        resources = _OPERATION_RESOURCES.get()
        if resources is None or resources.context is not self:
            return None
        return resources

    @property
    def loaders(self) -> LoaderRegistry:
        """The DataLoaders for the operation currently executing.

        Raises:
            RuntimeError: If no operation is in scope, which means
                :class:`PerOperationResources` is not installed on the schema. Raised rather than
                quietly building a registry here, because a registry created on demand would live
                on whatever called it and would reintroduce exactly the connection-scoped cache the
                extension exists to prevent — and it would do so invisibly.
        """
        resources = self._resources()
        if resources is None:
            raise RuntimeError(
                "no per-operation loaders are in scope. src.graphql.context.PerOperationResources "
                "mints them in its `on_operation` hook, so this means the extension is missing "
                "from the schema's `extensions=[...]` list, or a resolver is being called outside "
                "a GraphQL operation."
            )
        return resources.loaders

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Open one session for the duration of the block, and release it on the way out.

        Inside an operation this hands back the **operation-scoped** session (one connection for
        the whole operation, which is what lets a DataLoader batch and a resolver share a
        transaction) — except under a subscription, which deliberately gets a short-lived one.
        Outside an operation it falls back to opening a session from the factory, which is what
        keeps this usable from a script or a test that is not executing a document.

        A single helper rather than ``async with ctx.session_factory() as session:`` hand-rolled in
        every resolver — not to save four words, but so that the lifetime rule lives in exactly one
        place and no resolver quietly keeps its own.
        """
        resources = self._resources()
        if resources is not None:
            async with resources.session() as session:
                yield session
            return

        async with self.session_factory() as session:
            yield session

    @asynccontextmanager
    async def repository(self) -> AsyncIterator[LogRepository]:
        """Open a session and hand back a :class:`~src.db.repository.LogRepository` over it.

        The altitude every read resolver actually wants. Note the repository is constructed with
        **this context's** settings, which is what makes the ``DEFAULT_QUERY_LIMIT`` /
        ``MAX_QUERY_LIMIT`` clamp observable to a test that injected different ones.

        This is also the :data:`~src.graphql.loaders.RepositoryProvider` every loader batch runs
        through, so "which session does a batch use" is answered here, once, for resolvers and
        loaders alike.
        """
        async with self.session() as session:
            yield LogRepository(session, self.settings)


class PerOperationResources(SchemaExtension):
    """Mints the loaders (and, for non-subscriptions, the shared session) for each operation.

    Registered on the schema as a **class**, never an instance: Strawberry constructs one per
    execution, and this extension holds per-operation state that must not be shared between
    concurrent requests. See the ordering and factory notes in :mod:`src.graphql.schema`.

    The hook is an **async generator**, which makes this extension async-only — ``execute_sync``
    on a schema carrying it raises ``RuntimeError: ... failed to complete synchronously``. That is
    a real constraint and it is the right trade: closing a database session requires an await, and
    the alternatives are to leak the session or to fire-and-forget its close (which, in a suite
    that truncates tables between tests, means an open read transaction blocking the next
    ``TRUNCATE``). Nothing in this server was ever synchronous anyway — every resolver is a
    coroutine, so ``execute_sync`` could only ever have served introspection.
    """

    async def on_operation(self) -> AsyncIterator[None]:
        """Install fresh resources for this operation; tear them down when it ends.

        For a query or mutation "when it ends" is when the response is ready. **For a subscription
        it is when the stream ends** — Strawberry wraps the entire ``async for`` yield loop in this
        hook — which is the whole reason the session is not opened here: a subscription would hold
        it for the life of the socket. See :class:`OperationResources`.
        """
        execution_context = self.execution_context
        context = execution_context.context

        if not isinstance(context, Context):
            # No context, or somebody else's. Introspection through `schema.execute()` with no
            # `context_value` lands here, and so would a future non-HTTP caller. Nothing to install
            # and nothing to clean up — but the hook still has to yield, or the operation does not
            # run at all.
            yield
            return

        resources = OperationResources(
            context=context,
            session_factory=context.session_factory,
            settings=context.settings,
            is_subscription=lambda: operation_is_subscription(execution_context),
        )
        token: Token[Optional[OperationResources]] = _OPERATION_RESOURCES.set(resources)
        try:
            yield
        finally:
            # Both halves run whatever happened above, including a cancelled subscription: the
            # session has to be released even when the client vanished mid-stream, and the
            # ContextVar has to be reset even if closing the session raised, or the next operation
            # in this task inherits a closed session.
            try:
                await resources.aclose()
            except Exception:  # noqa: BLE001 - teardown must not replace the real error
                logger.warning("failed to close the operation session", exc_info=True)
            finally:
                # `reset` rejects a token minted in another context, which happens only if this
                # generator is finalised from somewhere other than the task that started it — an
                # abandoned subscription collected by the event loop's async-generator finaliser.
                # In that case the context holding the value is already gone, so there is nothing
                # to leak and nothing to do but not crash the teardown.
                with suppress(ValueError):
                    _OPERATION_RESOURCES.reset(token)


async def get_context(connection: HTTPConnection) -> Context:
    """Build the context for one HTTP request — or, on the WebSocket transport, one connection.

    The parameter is annotated :class:`~starlette.requests.HTTPConnection` rather than ``Request``
    deliberately: FastAPI solves this function as a dependency for **both** the POST route and the
    WebSocket route, and ``Request`` cannot be provided for a WebSocket scope. ``HTTPConnection``
    is the common base of ``Request`` and ``WebSocket``, FastAPI injects it for either, and
    ``connection.app`` reaches the application state on both.

    Everything read here is process-scoped and already built: no session is opened, no loader is
    created, nothing is awaited against the database, nothing is cached. That is the whole design —
    see the module docstring for what happens when this function starts owning resources, and
    :class:`PerOperationResources` for where they are owned instead.

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
