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
"""

from __future__ import annotations

import strawberry

from src.graphql.query import Query

# === C4 ===   from src.graphql.mutation import Mutation
# === C6 ===   from src.graphql.subscription import Subscription
# === C7-C9 == from src.graphql.apq import PersistedQueries
#              from src.graphql.cost import build_cost_validation_rules
#              from src.metrics import MetricsExtension
#              from strawberry.extensions import (
#                  AddValidationRules, MaxAliasesLimiter, MaxTokensLimiter, MaskErrors,
#                  QueryDepthLimiter,
#              )

#: The schema. Assembled once at import; every consumer imports this object rather than building
#: its own, so the SDL test, the router and the E2E verifier can never be looking at three
#: slightly different schemas.
schema = strawberry.Schema(
    query=Query,
    # === C4 ===  mutation=Mutation,          # createLog
    # === C6 ===  subscription=Subscription,  # logStream, orderStatusStream
    #
    # === C7-C9 ==  extensions=[...]. ORDER IS SIGNIFICANT and the list is not a set:
    #
    #   PersistedQueries()                       (C9) resolves a hash into a document, so it has
    #                                                 to run before anything reads the document.
    #   AddValidationRules(cost rules)           (C8) rejects over-budget operations during
    #                                                 VALIDATION, i.e. before a resolver runs.
    #   QueryDepthLimiter(MAX_QUERY_DEPTH)       (C8)
    #   MaxTokensLimiter(MAX_QUERY_TOKENS)       (C8)
    #   MaxAliasesLimiter(MAX_QUERY_ALIASES)     (C8)
    #   PerOperationResources()                  (C5) mints the loaders and the per-operation
    #                                                 session in `on_operation` — see
    #                                                 src/graphql/context.py for why they cannot
    #                                                 be created in `context_getter`.
    #   ResultCache()                            (C7) cache-aside on Query.logs / Query.logStats.
    #   MetricsExtension()                       (C9) timings; wraps everything inside it.
    #   MaskErrors()                             (C4) OUTERMOST, so nothing above it can leak a
    #                                                 stack trace past it.
)
