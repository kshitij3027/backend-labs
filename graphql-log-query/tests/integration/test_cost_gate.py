"""The cost gate through the real schema and the real HTTP surface — spec §2 item 33, §5.

The claim this file has to earn is **"before execution"**. A budget checked after the database has
answered protects nothing, and "the resolver did not run" is exactly the sort of statement that
stays in a docstring long after it stopped being true. So the central test carries two independent
instruments — a spy on the first line of the ``logs`` resolver and the same
:func:`~tests.integration.corpus.count_statements` listener C5 and C7 use to count SQL — and both
must read zero for a rejected operation *and* must read non-zero in the same test for an accepted
one. A spy that never fires proves nothing about the gate; it proves the spy is not attached.

.. rubric:: These tests pin the budget explicitly, and that is not incidental

The compose ``test`` service raises ``MAX_QUERY_COMPLEXITY`` for the rest of the suite (see the long
comment there: the oracle comparisons in ``test_dataloader.py`` deliberately ask for the whole
1200-row corpus with correlated entries attached, which an honest multiplicative model prices at
855,610). Everything here therefore constructs its own :class:`~src.config.Settings` with the
**shipped** defaults, read off ``Settings.model_fields`` so they track the declared values rather
than a copy of them. Constructor arguments outrank the environment in pydantic-settings, so no
container variable can reach these numbers.

That is what makes :func:`test_the_flagship_correlated_query_is_admitted_by_the_shipped_budget` and
:func:`test_the_shapes_the_budget_exists_to_refuse_are_still_refused` load-bearing rather than
decorative: they are the two sides of the calibration, asserted against the number a clean clone
boots with, in a container that is deliberately running a different one.

That is possible at all because of how the budget is wired: :class:`src.graphql.cost.
QueryCostLimiter` prices each operation against the settings on its ``Context``, exactly as every
resolver in this project reads ``info.context.settings`` rather than the global. ``DEPTH``,
``TOKENS`` and ``ALIASES`` cannot follow that convention — they are Strawberry's own extensions,
constructed with plain integers when the schema is built — so the three tests that exercise them
read the live :func:`~src.config.get_settings` and build a document that exceeds whatever this
process actually booted with.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any, Optional

import httpx
import pytest
from fastapi import FastAPI
from graphql import GraphQLSchema, build_schema as build_graphql_schema, get_introspection_query, parse

from src.config import Settings, get_settings
from src.db.models import LogRecord
from src.db.session import Database
from src.graphql import query as query_module
from src.graphql.context import Context
from src.graphql.cost import CostConfig, document_cost
from src.graphql.errors import MASKED_ERROR_MESSAGE, ErrorCode
from src.graphql.inputs import LogFilterInput
from src.graphql.schema import schema
from src.main import create_app
from tests.integration.corpus import count_statements


def _shipped(field: str) -> int:
    """The value ``src/config.py`` declares for ``field``, whatever the environment says."""
    default = Settings.model_fields[field].default
    assert isinstance(default, int), field
    return default


#: The shipped budget and the two list bounds the cost model is calibrated against.
SHIPPED_COMPLEXITY = _shipped("max_query_complexity")
SHIPPED_DEFAULT_LIMIT = _shipped("default_query_limit")
SHIPPED_MAX_LIMIT = _shipped("max_query_limit")

# --- The documents, with their costs worked out ---------------------------------------------------

#: Two levels of correlation under an unbounded parent list:
#: ``10 + 10x100 + 10x100x100 + 1x100x100x100``. Four fields deep, so the DEPTH limiter (10) is
#: nowhere near it — this document is over budget for one reason only, which is what makes it
#: usable as evidence that the *cost* gate is what rejected it.
OVER_BUDGET = "{ logs { relatedLogs { relatedLogs { id } } } }"
OVER_BUDGET_COST = 10 + (10 * 100) + (10 * 100 * 100) + (1 * 100 * 100 * 100)

#: A ``MAX_QUERY_LIMIT``-wide page with ONE level of correlation attached: ``10 + 500 x (10 + 100)``.
#: One level deep, so no depth limit would ever see it — the shape a client reaches for when told to
#: ask for a bigger page, and the realistic half of what this budget refuses.
WIDE_PAGE = "{ logs(filters: {limit: 500}) { relatedLogs { id } } }"
WIDE_PAGE_COST = 10 + (500 * 10) + (500 * 100)

#: THE FLAGSHIP: one level of correlation at the DEFAULT page size — ``10 + 1x100 + 10x100 +
#: 1x100x100``. Spec §2 item 17 asked for at item 29's default limit; the query C5's DataLoader
#: exists to make cheap and the one C13's dashboard sends. The shipped budget is calibrated to
#: admit it (see the note on ``max_query_complexity`` in ``src/config.py``), and the test below is
#: what fails if anyone lowers the budget back under it.
FLAGSHIP = "{ logs { id relatedLogs { id } } }"
FLAGSHIP_COST = 10 + (1 * 100) + (10 * 100) + (1 * 100 * 100)

#: ``10 + 5``. Comfortably inside the shipped budget, and it reads real rows.
IN_BUDGET = "{ logs(filters: {limit: 5}) { id } }"

#: ``10 + 10``. Used to pin both sides of the boundary exactly.
BOUNDARY = "{ logs(filters: {limit: 10}) { id } }"
BOUNDARY_COST = 20


# --- Fixtures --------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def gql_schema() -> GraphQLSchema:
    """The published schema as a ``GraphQLSchema``, for pricing a document outside a request."""
    return build_graphql_schema(schema.as_str())


@pytest.fixture()
def cost_settings(db_settings: Settings) -> Settings:
    """The integration settings with every number the cost gate reads pinned to what we ship.

    Derived from ``db_settings`` so the database URL, the empty-seed policy and the log level are
    the suite's; overridden on the four fields that decide what an operation costs. In particular
    ``max_query_limit`` goes back to the shipped 500 — ``db_settings`` raises it to five times the
    corpus so the oracle comparisons can ask for everything, and leaving that here would change
    what an absurd ``limit`` is clamped to and therefore what it is priced at.
    """
    return db_settings.model_copy(
        update={
            "max_query_complexity": SHIPPED_COMPLEXITY,
            "default_query_limit": SHIPPED_DEFAULT_LIMIT,
            "max_query_limit": SHIPPED_MAX_LIMIT,
        }
    )


@pytest.fixture()
def cost_context(database: Database, cost_settings: Settings) -> Context:
    """A GraphQL context wired to the test database and to the shipped budget."""
    return Context(
        settings=cost_settings, session_factory=database.session_factory, db=database
    )


@pytest.fixture()
def cost_app(database: Database, cost_settings: Settings) -> FastAPI:
    """The real application, assembled around the shipped budget rather than the environment.

    ``create_app(settings=…)`` is the project's hermetic seam (see :func:`src.main.create_app`), and
    it reaches the cost gate because the budget travels on the request context. The ``database``
    dependency is what guarantees the table exists and has been truncated before the app's own
    lifespan opens its engine over it.
    """
    return create_app(settings=cost_settings)


@pytest.fixture()
async def cost_client(cost_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """An async HTTP client over ``cost_app``, with the lifespan entered by hand.

    Same construction as the shared ``http_client`` fixture and for the same reason
    (``ASGITransport`` never sends lifespan events, and ``get_context`` needs ``app.state.db``);
    the only difference is the settings the app was built from.
    """
    async with cost_app.router.lifespan_context(cost_app):
        transport = httpx.ASGITransport(app=cost_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://graphql-cost.test"
        ) as client:
            yield client


def _codes(payload_or_errors: Any) -> list[Optional[str]]:  # noqa: ANN401 - errors, either shape
    """Every ``extensions.code`` in a result, whether it arrived over HTTP or from the schema."""
    codes: list[Optional[str]] = []
    for error in payload_or_errors or ():
        extensions = error["extensions"] if isinstance(error, dict) else error.extensions
        codes.append((extensions or {}).get("code"))
    return codes


def _messages(payload_or_errors: Any) -> str:  # noqa: ANN401 - errors, either shape
    """Every error message in a result, joined, lower-cased — for substring assertions."""
    parts = [
        error["message"] if isinstance(error, dict) else error.message
        for error in payload_or_errors or ()
    ]
    return " | ".join(parts).lower()


# --- The whole point: rejected BEFORE anything runs ----------------------------------------------


async def test_an_over_budget_query_is_rejected_before_any_resolver_runs(
    seeded: list[LogRecord],
    cost_context: Context,
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zero resolver calls and zero SQL statements for an over-budget operation.

    Two instruments, because either alone is weak. The spy replaces
    :func:`src.graphql.inputs.to_log_query` as ``Query.logs`` resolves it — the first thing that
    resolver does — so it fires if and only if the resolver body was entered. The statement counter
    listens on the engine, so it sees anything the resolver, a DataLoader batch or a lazy load would
    have sent to PostgreSQL.

    The **control** in the second half is what makes both readings mean something: the identical
    instruments, an in-budget document, and both of them move. Without it a spy that was never
    attached and a database that was never reachable would produce exactly the same green test.
    """
    calls: list[Optional[LogFilterInput]] = []
    original = query_module.to_log_query

    def spy(filters: Optional[LogFilterInput], settings: Settings) -> Any:  # noqa: ANN401
        calls.append(filters)
        return original(filters, settings)

    monkeypatch.setattr(query_module, "to_log_query", spy)

    with count_statements(database.engine) as rejected_statements:
        rejected = await schema.execute(OVER_BUDGET, context_value=cost_context)

    assert rejected.errors, "the over-budget document was accepted"
    assert rejected.data is None
    assert _codes(rejected.errors) == [ErrorCode.COST_LIMIT_EXCEEDED.value]
    assert calls == [], "a resolver ran for an operation that was supposed to be rejected first"
    assert len(rejected_statements) == 0, (
        "an over-budget operation reached the database:\n" + rejected_statements.report()
    )

    with count_statements(database.engine) as accepted_statements:
        accepted = await schema.execute(IN_BUDGET, context_value=cost_context)

    assert accepted.errors is None, accepted.errors
    assert len(accepted.data["logs"]) == 5
    assert len(calls) == 1, "the spy never fired at all — it is not attached to the resolver"
    assert len(accepted_statements) >= 1, "the counter never saw a statement — it is not attached"


async def test_the_rejection_carries_the_computed_cost_and_the_limit(
    cost_context: Context,
) -> None:
    """``computedCost`` and ``maxCost`` in ``extensions``, with the exact numbers.

    Pinned exactly rather than loosely, because these two numbers are the difference between a gate
    and a wall: a client told only "too expensive" has to bisect its own document to find out by how
    much, while a client told 1,101,010 against 25,000 can see immediately that one nested list has
    to go. It also means this test fails if the weight table or the multiplication rule changes
    without the calibration being revisited.
    """
    result = await schema.execute(OVER_BUDGET, context_value=cost_context)

    assert result.errors is not None
    (error,) = result.errors
    assert error.extensions["code"] == ErrorCode.COST_LIMIT_EXCEEDED.value
    assert error.extensions["computedCost"] == OVER_BUDGET_COST == 1_101_010
    assert error.extensions["maxCost"] == SHIPPED_COMPLEXITY == 25_000
    assert error.message != MASKED_ERROR_MESSAGE, "a client mistake is not a server fault"
    assert str(OVER_BUDGET_COST) in error.message and str(SHIPPED_COMPLEXITY) in error.message


async def test_the_rejection_is_a_200_errors_envelope_and_never_a_500(
    seeded: list[LogRecord], cost_client: httpx.AsyncClient
) -> None:
    """Spec §2 item 35 at the HTTP boundary: the failure is inside the body, with a code.

    Asserted over HTTP because the status code does not exist at the schema level, and a rejection
    that arrived as a 500 with a traceback would break every client that reads ``data``/``errors``
    while every schema-level assertion above stayed green.
    """
    response = await cost_client.post("/graphql", json={"query": OVER_BUDGET})

    assert response.status_code == 200
    assert "Traceback" not in response.text
    payload = response.json()

    assert payload.get("data") is None
    assert _codes(payload["errors"]) == [ErrorCode.COST_LIMIT_EXCEEDED.value]
    extensions = payload["errors"][0]["extensions"]
    assert extensions["computedCost"] == OVER_BUDGET_COST
    assert extensions["maxCost"] == SHIPPED_COMPLEXITY
    assert payload["errors"][0]["message"] != MASKED_ERROR_MESSAGE


# --- Both sides of the boundary -------------------------------------------------------------------


async def test_both_sides_of_the_budget_boundary_behave(
    seeded: list[LogRecord], database: Database, cost_settings: Settings
) -> None:
    """A document costing exactly the budget runs; one unit more is refused.

    Pinning only the rejection would leave "the gate rejects everything" indistinguishable from "the
    gate works", and pinning only the acceptance would leave it uninstalled. The boundary itself is
    ``>``, not ``>=``, and this is where that is decided.
    """
    exact = cost_settings.model_copy(update={"max_query_complexity": BOUNDARY_COST})
    tight = cost_settings.model_copy(update={"max_query_complexity": BOUNDARY_COST - 1})

    accepted = await schema.execute(
        BOUNDARY,
        context_value=Context(settings=exact, session_factory=database.session_factory, db=database),
    )
    rejected = await schema.execute(
        BOUNDARY,
        context_value=Context(settings=tight, session_factory=database.session_factory, db=database),
    )

    assert accepted.errors is None, accepted.errors
    assert len(accepted.data["logs"]) == 10, "the accepted operation must really have run"

    assert rejected.errors is not None
    assert rejected.errors[0].extensions["computedCost"] == BOUNDARY_COST
    assert rejected.errors[0].extensions["maxCost"] == BOUNDARY_COST - 1


async def test_the_budget_applied_is_the_operations_own_settings(
    seeded: list[LogRecord], database: Database, cost_settings: Settings
) -> None:
    """The settings-to-extension wiring, asserted rather than assumed.

    The same document, the same schema object, the same process — and opposite outcomes, decided
    entirely by the ``Settings`` carried on the request context. That is what lets
    ``create_app(settings=…)`` configure the gate for a test, and it is why the compose ``test``
    service can raise the budget for the rest of the suite without making this file untestable.
    """
    generous = cost_settings.model_copy(update={"max_query_complexity": OVER_BUDGET_COST})
    strict = cost_settings.model_copy(update={"max_query_complexity": SHIPPED_COMPLEXITY})

    def context(settings: Settings) -> Context:
        return Context(settings=settings, session_factory=database.session_factory, db=database)

    assert (await schema.execute(OVER_BUDGET, context_value=context(generous))).errors is None
    assert (await schema.execute(OVER_BUDGET, context_value=context(strict))).errors is not None


async def test_a_limit_supplied_as_a_variable_reaches_the_walker(
    seeded: list[LogRecord], cost_context: Context
) -> None:
    """The variable plumbing, end to end: same document, two values, opposite outcomes.

    ``logs(filters: {limit: $limit}) { id relatedLogs { id } }`` costs ``10 + 111 x limit``, so the
    shipped budget of 25,000 accepts 150 (16,660) and refuses 300 (33,310). Nothing but the variable
    value differs, which is what makes this a test of the path from ``execution_context.variables``
    into the rule rather than of the arithmetic (the unit suite owns that).

    Neither value is ``DEFAULT_QUERY_LIMIT``, and that is what keeps the failure loud in both
    directions. A walker that could not see the variable would price *both* operations at the
    default assumption of 11,110 — inside the budget — so the rejection below would simply stop
    happening rather than the acceptance turning into a silent over-charge.
    """
    document = (
        "query Sized($limit: Int!) "
        "{ logs(filters: {limit: $limit}) { id relatedLogs { id } } }"
    )

    accepted = await schema.execute(
        document, variable_values={"limit": 150}, context_value=cost_context
    )
    rejected = await schema.execute(
        document, variable_values={"limit": 300}, context_value=cost_context
    )

    assert accepted.errors is None, accepted.errors
    assert len(accepted.data["logs"]) == 150

    assert rejected.errors is not None
    assert rejected.errors[0].extensions["computedCost"] == 10 + 111 * 300 == 33_310


# --- The other three limiters, each demonstrably on its own ---------------------------------------


async def test_the_depth_limit_rejects_a_deep_query_and_the_cost_gate_is_not_what_fired(
    database: Database, cost_settings: Settings
) -> None:
    """``QueryDepthLimiter`` catches the narrow-and-deep document the cost model is bad at.

    The cost gate is deliberately taken out of the picture here, by pricing this operation against
    a budget it cannot possibly exceed. That is not a workaround, it is the only way to attribute
    the rejection: in *this* schema every extra level of depth goes through ``relatedLogs``, and
    every level of ``relatedLogs`` multiplies the cost by ``DEFAULT_QUERY_LIMIT`` — so a document
    deep enough to trip the depth limiter is necessarily also expensive, and a test that left both
    gates armed could not tell which one had done the work.
    """
    max_depth = get_settings().max_query_depth
    document = "{ logs(filters: {limit: 1}) { " + "relatedLogs { " * (max_depth + 2)
    document += "id" + " }" * (max_depth + 2) + " } }"

    no_cost_ceiling = cost_settings.model_copy(update={"max_query_complexity": 10**40})
    result = await schema.execute(
        document,
        context_value=Context(
            settings=no_cost_ceiling, session_factory=database.session_factory, db=database
        ),
    )

    assert result.errors, f"a {max_depth + 3}-deep document was accepted"
    assert "depth" in _messages(result.errors)
    assert ErrorCode.COST_LIMIT_EXCEEDED.value not in _codes(result.errors), (
        "the depth rejection was attributed to the cost gate"
    )


async def test_the_alias_limit_rejects_many_aliases_and_the_cost_gate_is_not_what_fired(
    cost_context: Context, gql_schema: GraphQLSchema
) -> None:
    """``MaxAliasesLimiter`` catches the one-cheap-field-ten-thousand-times document.

    This one needs no help to isolate: aliasing ``id`` on a one-row query is genuinely cheap, so it
    runs under the **shipped** budget and is rejected purely for its alias count.
    """
    count = get_settings().max_query_aliases + 2
    selections = " ".join(f"a{index}: id" for index in range(count))
    document = "{ logs(filters: {limit: 1}) { " + selections + " } }"

    # Cheap by construction, and asserted so rather than hoped: 10 for `logs` plus one per alias.
    priced = document_cost(
        gql_schema, parse(document), CostConfig.from_settings(cost_context.settings)
    )
    assert priced == 10 + count < SHIPPED_COMPLEXITY

    result = await schema.execute(document, context_value=cost_context)

    assert result.errors, f"a document with {count} aliases was accepted"
    assert "alias" in _messages(result.errors)
    assert ErrorCode.COST_LIMIT_EXCEEDED.value not in _codes(result.errors)


async def test_the_token_limit_rejects_an_enormous_document_and_the_cost_gate_is_not_what_fired(
    cost_context: Context,
) -> None:
    """``MaxTokensLimiter`` catches the enormous-but-shallow document, at PARSE time.

    ``__typename`` repeated past the ceiling is depth 0, alias 0 and — because introspection keys
    are exempt from the cost model — cost 0. The only thing wrong with it is its size, which is
    exactly the attack tokens exist to bound, and the rejection therefore cannot be credited to
    anything else. It is also the one limiter that fires before validation rather than during it:
    the document never becomes an AST at all.
    """
    document = "{ " + "__typename " * (get_settings().max_query_tokens + 10) + "}"

    result = await schema.execute(document, context_value=cost_context)

    assert result.errors, "a document past MAX_QUERY_TOKENS was parsed anyway"
    assert "token" in _messages(result.errors)
    assert ErrorCode.COST_LIMIT_EXCEEDED.value not in _codes(result.errors)


# --- The regressions that would make the server unusable ------------------------------------------


async def test_the_flagship_correlated_query_is_admitted_by_the_shipped_budget(
    seeded: list[LogRecord],
    cost_client: httpx.AsyncClient,
    cost_settings: Settings,
    gql_schema: GraphQLSchema,
) -> None:
    """One level of ``relatedLogs`` at the DEFAULT page size runs — over HTTP, under what we ship.

    THE regression this budget exists to prevent, and the reason it is 25,000 rather than 1000.
    ``{ logs { id relatedLogs { id } } }`` prices at 11,110, so under the old default the server
    refused *by default* the one capability C5's DataLoader was built for — spec §2 item 17's
    correlated entries at item 29's default limit — which is also the exact query C13's dashboard
    and C11's multi-dimensional traversals send. A default that rejects legitimate, intended use is
    broken rather than strict, and this one went unnoticed because the suite ran with the gate
    raised out of the way.

    Both numbers are asserted, separately and on purpose: the cost, so a change to the weight table
    cannot move it quietly, and the budget, so lowering ``MAX_QUERY_COMPLEXITY`` back under it fails
    *here* rather than in a dashboard. The document is then really executed over HTTP, because "the
    gate would allow it" and "the server serves it" are two different claims.
    """
    priced = document_cost(gql_schema, parse(FLAGSHIP), CostConfig.from_settings(cost_settings))

    assert priced == FLAGSHIP_COST == 11_110
    assert priced <= SHIPPED_COMPLEXITY, (
        f"the shipped budget of {SHIPPED_COMPLEXITY} rejects `{FLAGSHIP}` at {priced}: one level of "
        "correlation at the default page size is the capability this API exists to provide"
    )

    response = await cost_client.post("/graphql", json={"query": FLAGSHIP})

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload, payload.get("errors")

    rows = payload["data"]["logs"]
    assert len(rows) == SHIPPED_DEFAULT_LIMIT, "the accepted operation must really have run"
    assert any(row["relatedLogs"] for row in rows), (
        "nothing in the page came back with correlated entries, so this would pass without ever "
        "exercising the field the budget was calibrated around"
    )


async def test_the_shapes_the_budget_exists_to_refuse_are_still_refused(
    cost_context: Context, cost_settings: Settings, gql_schema: GraphQLSchema
) -> None:
    """Admitting the flagship must not have admitted the fan-out behind it.

    The other half of the calibration, and the reason the pair is worth more than either test alone:
    the same shipped budget that accepts 11,110 rejects both shapes spec §3 Feature Area D names. A
    second level of correlation is 1,101,010 — forty-four times the budget, so no plausible number
    admits it — and a ``MAX_QUERY_LIMIT``-wide page with one level attached is 55,010. The wide page
    is the one worth having here: it is a single level deep, so the depth limiter cannot see it, and
    it is what a client reaches for after being told to ask for a bigger page.
    """
    config = CostConfig.from_settings(cost_settings)

    assert document_cost(gql_schema, parse(WIDE_PAGE), config) == WIDE_PAGE_COST == 55_010
    assert document_cost(gql_schema, parse(OVER_BUDGET), config) == OVER_BUDGET_COST

    for document in (WIDE_PAGE, OVER_BUDGET):
        result = await schema.execute(document, context_value=cost_context)

        assert result.errors, f"the shipped budget accepted `{document}`"
        assert _codes(result.errors) == [ErrorCode.COST_LIMIT_EXCEEDED.value], document


async def test_the_ide_introspection_query_still_works_under_the_shipped_defaults(
    cost_client: httpx.AsyncClient,
) -> None:
    """GraphiQL sends this on every page load; if the gate rejects it the playground is dead.

    The real ``get_introspection_query()``, over the real transport, against the shipped budgets —
    not an approximation of it. Introspection is deep, wide, and entirely bounded by the schema's
    own size, which is why :mod:`src.graphql.cost` exempts ``__``-prefixed fields outright and
    Strawberry's depth limiter does the same.
    """
    response = await cost_client.post("/graphql", json={"query": get_introspection_query()})

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload, payload.get("errors")

    type_names = {entry["name"] for entry in payload["data"]["__schema"]["types"]}
    assert {"LogEntry", "LogStats", "Query", "Subscription"} <= type_names


@pytest.mark.parametrize(
    ("label", "body"),
    [
        # The spec's §5 acceptance command, verbatim. 10 + 4 x 100 = 410.
        ("spec logs", {"query": "{ logs { id service level message } }"}),
        # The spec's aggregate command. 30 + 3 = 33.
        ("spec logStats", {"query": "{ logStats { totalLogs errorCount services } }"}),
        # The spec's mutation. 10 + 2 = 12.
        (
            "spec createLog",
            {
                "query": (
                    "mutation Create($data: CreateLogInput!) "
                    "{ createLog(logData: $data) { id service } }"
                ),
                "variables": {
                    "data": {
                        "service": "order-service",
                        "level": "INFO",
                        "message": "written by the cost-gate suite",
                    }
                },
            },
        ),
        # C5's correlated selection on a small page: 10 + 8 + 8x10 + 8x100 = 898. The same shape at
        # the DEFAULT page size is 11,110 and has a test of its own above — it is the document the
        # budget is calibrated around, so it is pinned rather than listed.
        ("relatedLogs", {"query": "{ logs(filters: {limit: 8}) { id relatedLogs { id } } }"}),
        # C3's connection paging. 15 (logsConnection) + 5 (totalCount, once) + 1 (pageInfo)
        # + 1 (hasNextPage) + 1 (edges) + 5x(cursor + node + id + message) = 43.
        (
            "logsConnection",
            {
                "query": (
                    "{ logsConnection(first: 5) { totalCount pageInfo { hasNextPage } "
                    "edges { cursor node { id message } } } }"
                )
            },
        ),
        # Every published field on LogEntry at the default limit: 10 + 7 x 100 = 710.
        (
            "full projection",
            {"query": "{ logs { id timestamp service level message metadata traceId } }"},
        ),
    ],
)
async def test_legitimate_queries_still_pass_under_the_shipped_budget(
    seeded: list[LogRecord],
    cost_client: httpx.AsyncClient,
    label: str,
    body: dict[str, Any],
) -> None:
    """A gate that rejects the spec's own acceptance commands is a regression, not a feature.

    Every document here is one this project already promises to serve, priced in its comment. They
    run over HTTP against the shipped budget, so this is the test that would fail first if the
    weights were tightened without the calibration being rechecked.
    """
    response = await cost_client.post("/graphql", json=body)

    assert response.status_code == 200, label
    payload = response.json()
    assert "errors" not in payload, f"{label}: {payload.get('errors')}"
    assert payload["data"], label


# --- The C4 logging discipline holds --------------------------------------------------------------


async def test_a_cost_rejection_logs_one_concise_line_and_no_stack_trace(
    cost_context: Context, caplog: pytest.LogCaptureFixture
) -> None:
    """A rejected query is a **client** error, so it must not print a traceback per request.

    C4's whole argument (see :mod:`src.graphql.errors`): Strawberry logs every error with
    ``exc_info``, which is right for a crash and wrong for a client mistake — and the C14 load
    harness will send thousands of these. Because the rejection is raised as a
    :class:`~src.graphql.errors.DomainError` rather than as a bare ``GraphQLError``, it is
    classified as expected, logged as one INFO line on ``src.graphql.errors``, and never reaches
    ``strawberry.execution``.
    """
    caplog.set_level(logging.INFO, logger="src.graphql.errors")

    result = await schema.execute(OVER_BUDGET, context_value=cost_context)

    assert result.errors
    expected = [record for record in caplog.records if record.name == "src.graphql.errors"]
    assert len(expected) == 1, [record.getMessage() for record in expected]
    assert expected[0].levelno == logging.INFO
    assert expected[0].exc_info is None, "a cost rejection logged a stack trace"
    assert ErrorCode.COST_LIMIT_EXCEEDED.value in expected[0].getMessage()

    unexpected = [record for record in caplog.records if record.name == "strawberry.execution"]
    assert unexpected == [], "the rejection was logged as a server fault"
