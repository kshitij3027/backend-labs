"""The assembled :class:`strawberry.Schema`, and the seams the rest of the project bolts onto it.

One module-level ``schema`` object, built at import. It is what :func:`src.main.create_app` mounts,
what ``scripts/export_sdl.py`` prints, and what the unit suite introspects — one schema, so a test
can never be green against a schema the server does not serve.

.. rubric:: Code-first means the SDL is a build output, so it is committed and diffed

Nothing here declares GraphQL syntax; the schema is derived from Python types. That is pleasant to
write and it costs the thing SDL-first gets for free: **a schema change is invisible in review**.
Renaming a field, making one nullable, or adding an argument shows up as an ordinary-looking edit
to a dataclass, and the reviewer never sees the contract move.

``schema.graphql`` at the repository root is the fix. ``python -m scripts.export_sdl`` regenerates
it, ``tests/unit/test_schema_sdl.py`` fails when the committed copy and the live schema disagree,
and every schema change therefore arrives in a pull request as an explicit SDL diff sitting next to
the Python that caused it. An *intended* change is one regeneration away; an unintended one is a
red test instead of a silently broken client.

.. rubric:: Configuration is deliberately left at its defaults

No ``config=StrawberryConfig(...)`` argument, which means ``auto_camel_case`` stays **on**. That is
load-bearing rather than lazy: the spec's own sample operations are camel-cased
(``{ logStats { totalLogs errorCount services } }``, ``createLog(logData: …)``), so turning it off
would make the acceptance commands fail to validate. See the naming note in
:mod:`src.graphql.types`.

.. rubric:: Why the schema is a subclass at all (C4)

``Schema.process_errors`` is Strawberry's hook for "an operation produced errors", and by default
it logs **every one of them with a full stack trace** at ERROR level, on the
``strawberry.execution`` logger. That is right for a crash and wrong for a client mistake — a bad
enum, a malformed cursor, a non-numeric id are all handled, all already returning a correct 200
``errors`` envelope, and all logging a traceback that names internal files. At the C14 load harness
that is thousands of tracebacks a run.

:class:`LogQuerySchema` splits the stream: expected errors get one INFO line with no ``exc_info``
(see :func:`src.graphql.errors.log_expected_error`), everything else is handed to Strawberry
untouched so a genuine fault still gets its full trace where an operator expects to find it. The
``strawberry.execution`` logger is **not** silenced, filtered or reconfigured — doing that would
have removed the noise by also removing every real crash report.

The classification runs here rather than in the extension because Strawberry calls
``process_errors`` *before* the extensions get a chance to modify the result, which is precisely so
a server can log the truth and return something sanitised. By the time
:class:`~src.graphql.errors.MaskInternalErrors` has run, the ``original_error`` the classification
reads is gone.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import strawberry
from graphql import GraphQLError

from src.graphql.errors import MaskInternalErrors, is_expected_error, log_expected_error
from src.graphql.mutation import Mutation
from src.graphql.query import Query

if TYPE_CHECKING:  # pragma: no cover - annotations only; `from __future__` makes them strings
    from strawberry.types import ExecutionContext

# === C6 ===   from src.graphql.subscription import Subscription
# === C7-C9 == from src.graphql.apq import PersistedQueries
#              from src.graphql.cost import build_cost_validation_rules
#              from src.metrics import MetricsExtension
#              from strawberry.extensions import (
#                  AddValidationRules, MaxAliasesLimiter, MaxTokensLimiter, QueryDepthLimiter,
#              )


class LogQuerySchema(strawberry.Schema):
    """The project schema, with error logging split by whether the error was our fault.

    The only override is :meth:`process_errors`. Everything else — execution, introspection, SDL
    rendering — is Strawberry's, unchanged.
    """

    def process_errors(
        self,
        errors: list[GraphQLError],
        execution_context: Optional[ExecutionContext] = None,
    ) -> None:
        """Log expected errors quietly; hand the rest to Strawberry with their traces intact.

        Args:
            errors: Every error in the result, still carrying ``original_error`` and their
                original ``extensions`` — this runs before
                :class:`~src.graphql.errors.MaskInternalErrors` strips both.
            execution_context: Optional in this signature (Strawberry passes it positionally, and
                a test calling this directly should not have to build one) and forwarded unchanged
                so the base class's log records keep whatever context they normally carry.
        """
        unexpected: list[GraphQLError] = []
        for error in errors:
            if is_expected_error(error):
                # One line, no exc_info, no file paths. The client already received this message.
                log_expected_error(error)
            else:
                unexpected.append(error)

        if unexpected:
            # Strawberry's own handling, unchanged: full stack trace at ERROR on the
            # `strawberry.execution` logger. This is the branch a real crash takes, and it is
            # deliberately the loudest thing this module does.
            super().process_errors(unexpected, execution_context)


#: The schema. Assembled once at import; every consumer imports this object rather than building
#: its own, so the SDL test, the router and the E2E verifier can never be looking at three
#: slightly different schemas.
schema = LogQuerySchema(
    query=Query,
    mutation=Mutation,  # createLog
    # === C6 ===  subscription=Subscription,  # logStream, orderStatusStream
    #
    # ORDER IS SIGNIFICANT and the list is not a set.
    #
    # Strawberry drives these like nested context managers: each extension's `on_operation` code
    # BEFORE its `yield` runs in list order, and the code AFTER its `yield` runs in REVERSE list
    # order. So the FIRST entry is the OUTERMOST wrapper — it sets up first and observes the
    # result last. That is why masking leads the list rather than trailing it: whatever a later
    # extension adds to the result, MaskInternalErrors still sees it.
    #
    # (With a single extension this is moot. It is written down because it is the opposite of what
    # "outermost goes last" intuition suggests, and C7-C9 add four more. Re-verify against the
    # installed Strawberry when the stack grows — the property to check is that an error raised by
    # the cost gate or the metrics extension still comes out masked.)
    #
    # ENTRIES ARE CLASSES OR ZERO-ARGUMENT FACTORIES, NEVER INSTANCES.
    #
    # `Schema.get_extensions()` resolves each entry with `ext if isinstance(ext, SchemaExtension)
    # else ext()`, so a class is constructed fresh for every request while an instance is shared
    # by all of them — extensions hold per-operation state (`execution_context`, and C7's cache
    # keys), so sharing one is a cross-request leak under concurrency. Strawberry deprecated the
    # instance form for that reason (#4369) and warns at Schema construction. A parameterised
    # extension therefore goes in as a factory: `lambda: QueryDepthLimiter(MAX_QUERY_DEPTH)`,
    # not `QueryDepthLimiter(MAX_QUERY_DEPTH)`.
    #
    # The full stack, in the order it will be written as the remaining commits land:
    #
    #   MaskInternalErrors                       (C4) OUTERMOST — nothing below can leak a stack
    #                                                 trace past it.
    #   MetricsExtension                         (C9) timings; wraps everything inside it.
    #   PersistedQueries                         (C9) resolves a hash into a document, so it has
    #                                                 to run before anything reads the document
    #                                                 (its work is pre-yield, so any position
    #                                                 ahead of parsing does; parsing happens
    #                                                 inside every `on_operation`).
    #   lambda: AddValidationRules(cost rules)   (C8) rejects over-budget operations during
    #                                                 VALIDATION, i.e. before a resolver runs.
    #   lambda: QueryDepthLimiter(MAX_QUERY_DEPTH)      (C8)
    #   lambda: MaxTokensLimiter(MAX_QUERY_TOKENS)      (C8)
    #   lambda: MaxAliasesLimiter(MAX_QUERY_ALIASES)    (C8)
    #   PerOperationResources                    (C5) mints the loaders and the per-operation
    #                                                 session in `on_operation` — see
    #                                                 src/graphql/context.py for why they cannot
    #                                                 be created in `context_getter`.
    #   ResultCache                              (C7) cache-aside on Query.logs / Query.logStats.
    extensions=[
        MaskInternalErrors,
    ],
)
