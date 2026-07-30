"""The cost gate against the C11 traversals, executed — spec §3 Feature Area D.

*"Complexity analysis tuned so deep nested e-commerce queries are rejected."* ``tests/unit/
test_ecommerce_cost.py`` prices the documents; this module runs them, against the real store and the
**shipped** budget, and asserts the three things pricing alone cannot say:

* the flagship dossier is not merely affordable in theory but **actually served**, with data in
  every nested leg;
* the deep traversal is refused with a ``COST_LIMIT_EXCEEDED`` envelope carrying ``computedCost``
  and ``maxCost``, and it is the **cost** gate that refused it rather than the depth, token or alias
  limiter standing in front of it;
* the refusal costs **zero SQL statements** — which is the entire reason the gate is a validation
  rule rather than a check inside a resolver. A budget enforced after the database has answered
  protects nothing.

.. rubric:: Why these settings are built by hand rather than taken from the environment

The compose ``test`` service raises ``MAX_QUERY_COMPLEXITY`` to 980,000 so the suite's oracle
comparisons can pull whole streams. A gate test that inherited that would be measuring a budget
nobody ships. So the fixtures below pin the four numbers back to the values ``src/config.py``
declares, read off ``Settings.model_fields`` rather than copied — constructor arguments outrank the
environment in pydantic-settings, so the container variable cannot reach them.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Optional

import httpx
import pytest
from fastapi import FastAPI

from src.config import Settings
from src.db.session import Database
from src.generators import EventCorpus
from src.graphql.context import Context
from src.graphql.errors import ErrorCode
from src.graphql.schema import schema
from src.main import create_app
from tests.integration.corpus import CorrelatedCorpus, count_statements


def _shipped(field: str) -> int:
    """The value ``src/config.py`` declares for ``field``, whatever the environment says."""
    default = Settings.model_fields[field].default
    assert isinstance(default, int), field
    return default


SHIPPED_COMPLEXITY = _shipped("max_query_complexity")
SHIPPED_DEFAULT_LIMIT = _shipped("default_query_limit")
SHIPPED_MAX_LIMIT = _shipped("max_query_limit")

#: THE FLAGSHIP, verbatim as ``tests/integration/test_multi_dimensional.py`` runs it. 10,360 under
#: the shipped budget — an order with its payments, its buyer's activity and its correlated log
#: lines, i.e. the four REST calls spec §3 Feature Area B asks to collapse.
FLAGSHIP = """
query OrderDossier($orderId: String!) {
  orderEvents(filters: {orderId: $orderId, limit: 10}) {
    id
    timestamp
    status
    orderId
    userId
    payments { id timestamp method outcome }
    userActivity { id timestamp activityType }
    relatedLogs { id timestamp message }
  }
}
"""
FLAGSHIP_COST = 10_360

#: The same three traversals with the page size left at the default: 10 + 3 x (10x100 + 1x100x100).
#: ONE selection set deeper than nothing — every list is a single level below `orderEvents`, so the
#: depth limiter (10) cannot see it. It is over budget for exactly one reason, which is what makes
#: it usable as evidence about the *cost* gate.
DEEP_PAGE = """
{
  orderEvents {
    payments { id }
    userActivity { id }
    relatedLogs { id }
  }
}
"""
DEEP_PAGE_COST = 33_010

#: Walking back UP an edge after coming down it: list -> single -> list.
#: 10 + 10x100 + 5x(100x100) + 10x(100x100) + 1x(100x100x100). Depth 5, so again the depth limiter
#: is nowhere near. Forty-six times the budget.
DEEP_CYCLE = "{ orderEvents { payments { order { payments { id } } } } }"
DEEP_CYCLE_COST = 1_151_010

#: The C13 dashboard: three aggregate panels in one document, 220 units. Included here rather than
#: only in the unit suite because "the gate would allow it" and "the server serves it" are different
#: claims, and this is the one a dashboard depends on.
DASHBOARD = """
{
  orderStatusDistribution { status orders }
  orderFunnel { status ordersReached share }
  paymentOutcomeBreakdown { method outcome events orders }
}
"""


@pytest.fixture()
def cost_settings(db_settings: Settings) -> Settings:
    """The integration settings with every number the cost gate reads pinned to what we ship.

    ``max_query_limit`` goes back to the shipped 500 as well: ``db_settings`` raises it to five
    times the corpus so oracle comparisons can ask for everything, and leaving that here would
    change what an absurd ``limit`` is clamped to and therefore what it is priced at.
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
    return Context(settings=cost_settings, session_factory=database.session_factory, db=database)


@pytest.fixture()
def cost_app(database: Database, cost_settings: Settings) -> FastAPI:
    """The real application, assembled around the shipped budget rather than the environment."""
    return create_app(settings=cost_settings)


@pytest.fixture()
async def cost_client(cost_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """An async HTTP client over ``cost_app``, with the lifespan entered by hand."""
    async with cost_app.router.lifespan_context(cost_app):
        transport = httpx.ASGITransport(app=cost_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://graphql-ecom-cost.test"
        ) as client:
            yield client


def _codes(errors: Any) -> list[Optional[str]]:  # noqa: ANN401 - errors, either shape
    """Every ``extensions.code`` in a result, whether it arrived over HTTP or from the schema."""
    codes: list[Optional[str]] = []
    for error in errors or ():
        extensions = error["extensions"] if isinstance(error, dict) else error.extensions
        codes.append((extensions or {}).get("code"))
    return codes


def _extensions(errors: Any) -> dict[str, Any]:  # noqa: ANN401 - errors, either shape
    """The first error's ``extensions``, as a plain dict."""
    error = list(errors)[0]
    extensions = error["extensions"] if isinstance(error, dict) else error.extensions
    return dict(extensions or {})


def _anchor_order_id(corpus: CorrelatedCorpus) -> str:
    """An order whose trace is declared to carry log lines, so all four legs return data."""
    traces = set(corpus.shared_traces)
    assert traces
    for record in corpus.events.orders:
        if record.trace_id in traces:
            return record.order_id
    raise AssertionError("no order event carries a declared shared trace")


# =================================================================================================
# ADMITTED — and really served
# =================================================================================================


async def test_the_flagship_dossier_is_admitted_by_the_shipped_budget_and_runs(
    seeded_correlated: CorrelatedCorpus, cost_client: httpx.AsyncClient
) -> None:
    """The four-in-one query runs over HTTP under the number we actually ship.

    This is the regression the whole calibration exists to prevent, in its C11 form: adding the
    traversals and then discovering the default budget refuses the query they were added for. C8 met
    exactly that failure with ``relatedLogs`` and a budget of 1000, and it went unnoticed because
    the suite ran with the gate raised out of the way — which is why this test builds its own
    settings instead of using the suite's.

    Executed rather than priced, and every nested leg asserted non-empty: a document that was
    admitted and returned three empty lists would satisfy a status-code check while proving nothing.
    """
    order_id = _anchor_order_id(seeded_correlated)

    response = await cost_client.post(
        "/graphql", json={"query": FLAGSHIP, "variables": {"orderId": order_id}}
    )

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload, (
        f"the shipped budget of {SHIPPED_COMPLEXITY} rejected the flagship dossier "
        f"({FLAGSHIP_COST}): {payload.get('errors')}"
    )

    rows = payload["data"]["orderEvents"]
    assert rows, "the accepted operation must really have run"
    assert all(row["payments"] for row in rows)
    assert all(row["userActivity"] for row in rows)
    assert all(row["relatedLogs"] for row in rows)


async def test_the_three_panel_dashboard_is_admitted_and_returns_all_three_series(
    seeded_events: EventCorpus, cost_client: httpx.AsyncClient
) -> None:
    """Feature Area E's "single query result" must survive Feature Area D's gate.

    220 units for three aggregate panels, because each declares the size its vocabulary bounds it
    to. Without those declarations the same document would be priced at the 100-row assumption and
    the dashboard would be refused by the mechanism that exists to protect it.
    """
    response = await cost_client.post("/graphql", json={"query": DASHBOARD})

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload, payload.get("errors")

    data = payload["data"]
    assert data["orderStatusDistribution"], "the distribution panel came back empty"
    assert data["orderFunnel"], "the funnel panel came back empty"
    assert data["paymentOutcomeBreakdown"], "the payments panel came back empty"


# =================================================================================================
# REJECTED — before a single statement
# =================================================================================================


@pytest.mark.parametrize(
    ("label", "document", "expected_cost"),
    [
        ("three traversals over a full page", DEEP_PAGE, DEEP_PAGE_COST),
        ("a list -> single -> list cycle", DEEP_CYCLE, DEEP_CYCLE_COST),
    ],
)
async def test_a_deep_nested_traversal_is_rejected_with_the_numbers_a_client_needs(
    label: str,
    document: str,
    expected_cost: int,
    seeded_events: EventCorpus,
    cost_context: Context,
    database: Database,
) -> None:
    """``COST_LIMIT_EXCEEDED``, carrying ``computedCost`` and ``maxCost`` — and **zero** SQL.

    Three assertions in one, and each is load-bearing:

    * **The code.** Not merely "there were errors": the depth, token and alias limiters all reject
      documents too, and a test that accepted any rejection would keep passing if the cost gate were
      removed entirely and one of the others happened to fire.
    * **The numbers.** ``computedCost`` and ``maxCost`` in ``extensions`` are what let a client
      shrink its query deliberately instead of bisecting it. Without them the gate is a wall.
    * **Zero statements.** The rule runs during *validation*, so an over-budget operation opens no
      session, allocates no loaders and asks the database nothing. That is the claim that quietly
      stops being true if the check is ever moved into a resolver, and a statement counter is the
      only thing that would notice.
    """
    with count_statements(database.engine) as counter:
        result = await schema.execute(document, context_value=cost_context)

    assert result.errors, f"{label} was admitted at {expected_cost} against {SHIPPED_COMPLEXITY}"
    assert _codes(result.errors) == [ErrorCode.COST_LIMIT_EXCEEDED.value], (
        f"{label} was rejected by something other than the cost gate: {result.errors}"
    )

    extensions = _extensions(result.errors)
    assert extensions["computedCost"] == expected_cost
    assert extensions["maxCost"] == SHIPPED_COMPLEXITY
    assert extensions["computedCost"] > extensions["maxCost"]

    assert len(counter) == 0, (
        f"{label} reached the database before being rejected — the gate must run during "
        f"validation, not during execution:\n{counter.report()}"
    )


async def test_the_rejection_is_a_200_errors_envelope_over_http_and_never_a_500(
    seeded_events: EventCorpus, cost_client: httpx.AsyncClient
) -> None:
    """A client asking for too much gets a GraphQL error, not a server failure.

    Spec §2 item 35 ("errors return GraphQL-shaped error responses rather than raw stack traces or
    HTTP 500s") applied to the new fields. Worth re-asserting per feature area rather than once,
    because C4's masking is configured on the schema and a resolver that raised a bare exception on
    a new path would surface differently.
    """
    response = await cost_client.post("/graphql", json={"query": DEEP_PAGE})

    assert response.status_code == 200
    payload = response.json()
    assert _codes(payload["errors"]) == [ErrorCode.COST_LIMIT_EXCEEDED.value]
    assert payload["errors"][0]["extensions"]["computedCost"] == DEEP_PAGE_COST
    assert "traceback" not in response.text.lower()


async def test_lowering_the_page_size_is_a_remedy_that_actually_works(
    seeded_events: EventCorpus, cost_context: Context
) -> None:
    """The rejection message tells a client to lower ``limit``; this proves the advice is true.

    ``DEEP_PAGE`` is refused at 33,010. The identical three traversals with ``limit: 10`` price at
    3,310 and are served with real rows. That is the difference between a gate and a wall, and it is
    the property that makes 25,000 a *budget* rather than a capability ceiling.
    """
    refused = await schema.execute(DEEP_PAGE, context_value=cost_context)
    assert _codes(refused.errors) == [ErrorCode.COST_LIMIT_EXCEEDED.value]

    narrowed = await schema.execute(
        """
        {
          orderEvents(filters: {limit: 10}) {
            payments { id }
            userActivity { id }
            relatedLogs { id }
          }
        }
        """,
        context_value=cost_context,
    )

    assert narrowed.errors is None, f"the narrowed query must run: {narrowed.errors}"
    rows = narrowed.data["orderEvents"]
    assert len(rows) == 10
    assert all(row["payments"] for row in rows), "the narrowed query must return real data"


async def test_the_depth_limiter_is_not_what_rejects_the_deep_traversals(
    seeded_events: EventCorpus, cost_context: Context
) -> None:
    """Both refused documents are well inside ``MAX_QUERY_DEPTH``, so the *cost* gate is the reason.

    ``DEEP_PAGE`` is three levels and ``DEEP_CYCLE`` is five, against a depth budget of ten. Stated
    as its own test because the two limiters are stacked on one schema: if the depth limiter were
    silently doing the work, every assertion above would still pass and the cost weights could be
    anything at all.
    """
    assert cost_context.settings.max_query_depth >= 10

    for document in (DEEP_PAGE, DEEP_CYCLE):
        result = await schema.execute(document, context_value=cost_context)
        messages = " ".join(error.message for error in (result.errors or ()))

        assert "depth" not in messages.lower(), (
            f"the depth limiter fired instead of the cost gate on {document!r}: {messages}"
        )
        assert _codes(result.errors) == [ErrorCode.COST_LIMIT_EXCEEDED.value]
