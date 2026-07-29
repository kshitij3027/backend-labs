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
from strawberry.extensions import MaxAliasesLimiter, MaxTokensLimiter, QueryDepthLimiter

from src.config import Settings, get_settings
from src.graphql.context import PerOperationResources
from src.graphql.cost import CostConfig, QueryCostLimiter
from src.graphql.errors import MaskInternalErrors, is_expected_error, log_expected_error
from src.graphql.mutation import Mutation
from src.graphql.query import Query
from src.graphql.subscription import Subscription

if TYPE_CHECKING:  # pragma: no cover - annotations only; `from __future__` makes them strings
    from strawberry.types import ExecutionContext

# === C9 ===== from src.graphql.apq import PersistedQueries
#              from src.metrics import MetricsExtension


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


def build_schema(settings: Settings) -> LogQuerySchema:
    """Assemble the schema, with the four C8 budgets read off ``settings`` **here and once**.

    The types are configuration-free; the *extensions* are not. ``QueryDepthLimiter``,
    ``MaxTokensLimiter`` and ``MaxAliasesLimiter`` take plain integers, and they are constructed
    when the schema is — so this function is where ``MAX_QUERY_DEPTH`` / ``MAX_QUERY_TOKENS`` /
    ``MAX_QUERY_ALIASES`` stop being environment and become behaviour. Passing them in as an
    argument rather than reaching for :func:`~src.config.get_settings` inside the extension list is
    what makes that wiring visible in one place and testable in another: a test can build a schema
    with ``max_query_depth=3`` and watch a four-deep query fail, which is not something a module
    that read the global at import could ever be asked.

    The **cost** budget is the one exception, and deliberately:
    :class:`~src.graphql.cost.QueryCostLimiter` prefers the settings on the operation's ``Context``
    and falls back to what is captured here. That is the same convention every resolver follows
    (``info.context.settings``, never the global — see :class:`src.graphql.context.Context`), and
    it is what lets an integration test drive the real HTTP surface under a deliberately different
    budget by passing ``create_app(settings=…)``. Depth, tokens and aliases cannot join that
    convention without reimplementing three Strawberry extensions, so they stay fixed at build
    time; the asymmetry is written down here rather than discovered later.
    """
    return LogQuerySchema(
        query=Query,
        mutation=Mutation,  # createLog
        # C6: logStream. C12 adds orderStatusStream to the same type.
        #
        # Declaring a subscription root is what makes `GraphQLRouter`'s WebSocket half reachable —
        # the mount and its `subscription_protocols` have been in place since C1 (see
        # src/main.py), so this one keyword is the whole difference between a socket that
        # negotiates and immediately has nothing to offer and a working `graphql-transport-ws`
        # endpoint.
        subscription=Subscription,
        #
        # ORDER IS SIGNIFICANT and the list is not a set.
        #
        # Strawberry drives these like nested context managers: each extension's `on_operation`
        # code BEFORE its `yield` runs in list order, and the code AFTER its `yield` runs in
        # REVERSE list order. So the FIRST entry is the OUTERMOST wrapper — it sets up first and
        # observes the result last. That is why masking leads the list rather than trailing it:
        # whatever a later extension adds to the result, MaskInternalErrors still sees it.
        #
        # (It is written down because it is the opposite of what "outermost goes last" intuition
        # suggests. Re-verify against the installed Strawberry when the stack grows — the property
        # to check is that an error raised by the cost gate or the metrics extension still comes
        # out masked.)
        #
        # ENTRIES ARE CLASSES OR ZERO-ARGUMENT FACTORIES, NEVER INSTANCES.
        #
        # `Schema.get_extensions()` resolves each entry with `ext if isinstance(ext,
        # SchemaExtension) else ext()`, so a class is constructed fresh for every request while an
        # instance is shared by all of them — extensions hold per-operation state
        # (`execution_context`, and C9's resolved persisted-query document), so sharing one is a
        # cross-request leak under concurrency. Strawberry deprecated the instance form for that
        # reason (#4369) and warns at Schema construction. A parameterised extension therefore goes
        # in as a factory: `lambda: QueryDepthLimiter(depth)`, not `QueryDepthLimiter(depth)`.
        #
        # The stack, with the two C9 entries still to land:
        #
        #   MaskInternalErrors                       (C4) OUTERMOST — nothing below can leak a
        #                                                 stack trace past it.
        #   MetricsExtension                         (C9) timings; wraps everything inside it.
        #   PersistedQueries                         (C9) resolves a hash into a document, so it
        #                                                 has to run before anything reads the
        #                                                 document (its work is pre-yield, so any
        #                                                 position ahead of parsing does; parsing
        #                                                 happens inside every `on_operation`).
        #   lambda: QueryCostLimiter(cost_config)    (C8) an AddValidationRules subclass. Rejects
        #                                                 over-budget operations during
        #                                                 VALIDATION, i.e. before a resolver runs.
        #   lambda: QueryDepthLimiter(depth)         (C8) the stack bound, where the cost gate is
        #   lambda: MaxTokensLimiter(tokens)         (C8) the row bound. See src/graphql/cost.py
        #   lambda: MaxAliasesLimiter(aliases)       (C8) for why all four are needed.
        #   PerOperationResources                    (C5) mints the loaders and the per-operation
        #                                                 session in `on_operation` — see
        #                                                 src/graphql/context.py for why they
        #                                                 cannot be created in `context_getter`.
        #
        # C8 NOTE — the three stock limiters are position-INSENSITIVE and the ordering above is for
        # readability rather than correctness: two of them only append a validation rule and the
        # third only sets a parse option, all of it pre-yield, and parsing and validation both
        # happen after every extension's pre-yield code has run. What their placement DOES matter
        # for is that they sit inside `MaskInternalErrors` (so a rejection is still seen by it) and
        # outside `PerOperationResources` (so nothing is allocated for an operation that is about
        # to be refused).
        #
        # C7 NOTE — THE RESULT CACHE IS **NOT** AN EXTENSION, and the plan's sketch of one here was
        # wrong. An extension sees the operation, not the resolved arguments: to key on the filter
        # set it would have to re-derive `LogFilterInput` -> `LogQuery` from the AST and the
        # variables, duplicating `to_log_query` (including its validation and its limit resolution)
        # in a second place that could disagree with the first. Worse, a whole-operation cache keys
        # on the DOCUMENT as well as the filters, so `{ logs { id } }` and `{ logs { id message } }`
        # would be two entries holding the same rows, and any operation selecting a cached field
        # beside an uncached one could not be cached at all. So the cache-aside sits in the two
        # resolvers, around the repository call, keyed on the `LogQuery` they were about to run —
        # one wrapper each. See src/cache.py.
        #
        # C5 NOTE — `PerOperationResources` is INNERMOST, and that is the position it wants: it
        # sets up last, so nothing it allocates is held while an outer extension is still deciding
        # whether to reject the operation, and it tears down FIRST, so the operation's database
        # session is closed before `MaskInternalErrors` inspects the result. It also makes this
        # schema **async-only** — its hook is an async generator, so `schema.execute_sync()` on it
        # raises "failed to complete synchronously". That was already effectively true (every
        # resolver is a coroutine, so `execute_sync` could only ever serve introspection); it is
        # now enforced.
        extensions=[
            MaskInternalErrors,
            lambda: QueryCostLimiter(CostConfig.from_settings(settings)),
            lambda: QueryDepthLimiter(settings.max_query_depth),
            lambda: MaxTokensLimiter(settings.max_query_tokens),
            lambda: MaxAliasesLimiter(settings.max_query_aliases),
            PerOperationResources,
        ],
    )


#: The schema. Assembled once at import; every consumer imports this object rather than building
#: its own, so the SDL test, the router and the E2E verifier can never be looking at three
#: slightly different schemas.
#:
#: Built from the process-wide configuration, which is what `src.main.create_app` mounts. Note that
#: an app built with injected settings (`create_app(settings=…)`, the hermetic test seam) still
#: mounts THIS object: the depth/token/alias budgets it carries are the environment's, while the
#: cost budget follows the injected settings through the request context. See `build_schema`.
schema = build_schema(get_settings())
