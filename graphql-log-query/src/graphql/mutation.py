"""The ``Mutation`` root type: ``createLog`` — spec §2 item 24.

.. rubric:: The argument is named ``logData``, and that is not negotiable

Spec §5 lists this as a literal acceptance command::

    mutation { createLog(logData: {...}) { id service } }

The Python parameter is therefore ``log_data``, which Strawberry's ``auto_camel_case`` publishes as
``logData``. Renaming it to ``input`` (the Relay convention) or ``entry`` would break the spec's own
verification command while leaving every behavioural test green — a schema shape test pins the
published name for exactly that reason.

.. rubric:: This resolver owns a transaction, so it holds the session

Every read resolver reaches for :meth:`src.graphql.context.Context.repository`, which hands back a
repository and keeps the session to itself. A write cannot use that shape:
:meth:`src.db.repository.LogRepository.insert_log` deliberately **flushes without committing** (see
its class docstring — transaction boundaries belong to the caller), so somebody has to hold the
session and say when the unit of work ends. That somebody is this resolver, which is why it opens
:meth:`~src.graphql.context.Context.session` directly and constructs the repository over it.

The block is also the seam for C10: an order event and the log line describing it must land in one
transaction, and that is possible precisely because the commit is here rather than buried in the
repository.

.. rubric:: Order of operations, which is the part that is easy to get subtly wrong

    validate  ->  insert  ->  commit  ->  project  ->  publish

**Validation first**, so a bad payload never opens a session. **Commit before publish** (C6): the
broker fan-out is what a live dashboard renders from, and publishing an entry that a rolled-back
transaction never stored would show every subscriber a row that does not exist and cannot be
fetched again. The reverse mistake — committing and failing to publish — costs a subscriber one
missed frame on a stream that is explicitly lossy under back-pressure, which is the cheaper of the
two failures by a wide margin.

**Projection inside the session block.** ``expire_on_commit=False`` means the committed instance
stays usable after the block, so building the :class:`~src.graphql.types.LogEntry` outside would
also work today — by relying on a session *setting* rather than on the object still being attached.
That is the kind of dependency that breaks silently when somebody changes the setting for an
unrelated reason.
"""

from __future__ import annotations

import strawberry

from src.db.repository import LogRepository
from src.graphql.context import Context
from src.graphql.inputs import CreateLogInput
from src.graphql.types import LogEntry
from src.graphql.validation import validate_create_log


@strawberry.type
class Mutation:
    """The write surface. C4 gives it ``createLog``; C10/C11 add the e-commerce writes."""

    @strawberry.mutation
    async def create_log(
        self,
        info: strawberry.Info[Context, None],
        log_data: CreateLogInput,
    ) -> LogEntry:
        """Persist one log entry and return it, ids and defaults resolved.

        Returns the **created object** rather than a status or an id (spec §2 item 24), which is
        what lets a client render the new row immediately — and specifically what makes C13's
        optimistic update reconcilable: Apollo replaces the optimistic entry with this response, so
        it has to carry the same fields, including the server-assigned ``id`` and ``timestamp``.

        Args:
            info: Carries the :class:`~src.graphql.context.Context` — settings and the session
                factory.
            log_data: Published as ``logData``. Validated by
                :func:`src.graphql.validation.validate_create_log` before anything else happens.

        Returns:
            The stored entry, projected through :meth:`src.graphql.types.LogEntry.from_orm` — the
            single mapping every other path uses, so a created entry and the same row fetched back
            by ``Query.log`` are guaranteed to serialise identically.

        Raises:
            src.graphql.errors.ValidationError: If the payload breaks a rule — blank or over-long
                ``service``/``message``/``traceId``, a NUL byte, or ``metadata`` that is not a
                bounded JSON object. Carries ``extensions.code = "VALIDATION_ERROR"`` and reaches
                the client as a normal errors envelope, never a 500.
        """
        context = info.context
        params = validate_create_log(log_data)

        async with context.session() as session:
            # Constructed here rather than taken from `context.repository()` because this resolver
            # needs the session itself: the repository flushes and stops, and the commit below is
            # the transaction boundary it is deliberately leaving to us.
            repository = LogRepository(session, context.settings)
            row = await repository.insert_log(
                service=params.service,
                level=params.level,
                message=params.message,
                timestamp=params.timestamp,
                metadata=params.metadata,
                trace_id=params.trace_id,
            )
            await session.commit()
            entry = LogEntry.from_orm(row)

        # PUBLISH THE COMMITTED ENTRY (C6). `createLog` is the event source for
        # `Subscription.logStream`, and this is the only place an entry enters the system at run
        # time.
        #
        # THE PUBLISH IS HERE — OUTSIDE THE SESSION BLOCK, AFTER `await session.commit()` — AND THE
        # ORDER IS NOT INTERCHANGEABLE. Publishing first would announce a row to every live
        # subscriber before the transaction that creates it has succeeded, so a commit that then
        # failed (a constraint, a lost connection, a serialisation failure) would leave every
        # dashboard rendering an entry that does not exist, cannot be fetched again, and will never
        # appear in a query. The reverse mistake — committing and then failing to publish — costs a
        # subscriber one missed frame on a stream that is *explicitly* lossy under back-pressure
        # (see src/broker.py). One of those is a correctness bug visible to users; the other is
        # within the stream's documented guarantees. So: commit, then publish.
        #
        # No try/except, and adding one would be wrong: `LogBroker.publish` has no await points and
        # never raises by construction (bounded queues, `put_nowait`, drop-on-overflow, every
        # per-subscriber step in its own `try`, the Redis hop handed to a background task). A slow
        # subscriber, a dead subscriber or a Redis that is entirely down therefore cannot add
        # latency to — or fail — a write that already committed.
        broker = context.broker
        if broker is not None:
            # `None` only when the app was assembled without a lifespan, which is how the C4
            # mutation tests build their context. The write is what `createLog` promises; fan-out is
            # an additional delivery guarantee, so its absence must not fail the mutation. See the
            # `broker` note on `src.graphql.context.Context` for why `logStream` treats it as fatal
            # and this does not.
            await broker.publish(entry)

        return entry
