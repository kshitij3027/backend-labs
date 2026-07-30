"""What the cost gate charges for the C11 traversals and aggregates — spec §3 Feature Area D.

*"Complexity analysis tuned so deep nested e-commerce queries are rejected."* The claim has two
halves and a test that only checks one of them is worthless: a gate that rejects everything rejects
deep queries too. So every assertion here comes in a pair — a document that **must be admitted** and
a document that **must be refused**, both priced against the **shipped** ``MAX_QUERY_COMPLEXITY``.

.. rubric:: Every expectation is an exact integer, and the arithmetic is written out

Same discipline as ``tests/unit/test_cost.py``, for the same reason: a range hides the regression
this file exists to catch. A walker that stopped multiplying nested lists would still return "a big
number" for a big query, and ``> 1000`` would stay green through it. The arithmetic is in each
docstring so a failing number can be diagnosed without re-deriving the model.

.. rubric:: The headline finding, stated so it is not buried in the numbers

**C11 did not move ``MAX_QUERY_COMPLEXITY``.** The tuning the requirement asks for turned out to be
*weights*, not budget — every nested traversal is one batched indexed read, so every one is priced
at 10 (exactly what ``LogEntry.relatedLogs`` costs, because it is exactly the same work) and the
multiplication the model already had does the rest. Widening the gate to admit a query one has just
made expensive is the failure C8 met once already; ``test_the_shipped_budget_did_not_move`` is what
would notice it happening again.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from graphql import GraphQLSchema, build_schema as build_graphql_schema, parse, validate

from src.config import Settings
from src.graphql.cost import DEFAULT_WEIGHTS, CostConfig, document_cost
from src.graphql.enums import OrderStatus, PaymentMethod, PaymentOutcome
from src.graphql.schema import schema as strawberry_schema

#: The shipped budget and the shipped list sizes, as literals so the arithmetic below is readable
#: rather than a function of the environment — and tied back to ``src/config.py`` by
#: :func:`test_the_shipped_budget_did_not_move`, so the literals cannot drift.
CONFIG = CostConfig(max_complexity=25_000, default_list_size=100, max_list_size=500)

SHIPPED_COMPLEXITY = 25_000


# =================================================================================================
# THE FLAGSHIP — one query, four REST calls collapsed (spec §3 Feature Area B)
#
# GET /orders/{id}/events        -> orderEvents(filters: {orderId: …})
# GET /orders/{id}/payments      -> ... { payments { … } }
# GET /users/{userId}/activity   -> ... { userActivity { … } }
# GET /traces/{traceId}/logs     -> ... { relatedLogs { … } }
#
# Four round trips, three of which a REST client cannot even ISSUE until the first has come back
# (it does not know the userId or the traceId until it has read the order). That serialisation is
# the part a "3+ REST calls" claim is really about, and it is why this document is the centrepiece
# of the integration suite as well as of this one.
# =================================================================================================

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

#: 10 (orderEvents) + 5 leaves x 10 parents
#: + payments      10 x 10 + 4 leaves x (10 x 100)
#: + userActivity  10 x 10 + 3 leaves x (10 x 100)
#: + relatedLogs   10 x 10 + 3 leaves x (10 x 100)
#: = 10 + 50 + 100 + 4000 + 100 + 3000 + 100 + 3000
FLAGSHIP_COST = 10_360

#: The same three traversals with no ``limit`` at all: 10 + 3 x (10 x 100 + 1 x 100 x 100).
#: The shape a client reaches for after being told to ask for more data, and the one the budget
#: exists to refuse.
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

#: Alternating list -> single -> list, which is how a client walks *back up* an edge it just came
#: down. 10 + 10x100 + 5x10,000 + 10x10,000 + 1x1,000,000. Forty-six times the budget, so no
#: plausible number admits it — which is the point: `PaymentEvent.order` is the cheapest field in
#: the C11 block (weight 5, not a list) and is therefore the one that could have made a cycle look
#: affordable.
DEEP_CYCLE = "{ orderEvents { payments { order { payments { id } } } } }"
DEEP_CYCLE_COST = 1_151_010

#: The C13 dashboard: three aggregate panels, every field, in ONE document.
DASHBOARD = """
{
  orderStatusDistribution { status orders }
  orderFunnel { status ordersReached share }
  paymentOutcomeBreakdown { method outcome events orders }
}
"""
#: 35 + 2x7  +  40 + 3x7  +  30 + 4x20  =  49 + 61 + 110
DASHBOARD_COST = 220


@pytest.fixture(scope="module")
def gql_schema() -> GraphQLSchema:
    """The published schema as a ``GraphQLSchema``, built from the SDL the server renders."""
    return build_graphql_schema(strawberry_schema.as_str())


def cost(
    gql_schema: GraphQLSchema,
    document: str,
    variables: Optional[dict[str, Any]] = None,
) -> int:
    """Price ``document`` against the shipped configuration, as the validation rule does."""
    return document_cost(gql_schema, parse(document), CONFIG, variables)


# --- The table is anchored to what is actually shipped ---------------------------------------------


def test_the_shipped_budget_did_not_move() -> None:
    """``MAX_QUERY_COMPLEXITY`` is still 25,000 — C11 tuned weights, not the gate.

    The one assertion in this file that is about a *decision* rather than about arithmetic. Raising
    the budget is the easy way to make every document below pass, and it is the wrong one: the gate
    exists to bound what a client can ask for, so a commit that adds expensive fields and then
    widens the ceiling to fit them has removed the bound while appearing to test it.
    """
    # Read the DECLARED defaults off the model, not off a constructed Settings.
    # `Settings(_env_file=None)` disables only the dotenv source; pydantic-settings'
    # EnvSettingsSource still reads os.environ, and the compose `test` service sets
    # MAX_QUERY_COMPLEXITY=980000 so the suite's own oversized documents can run. So the one
    # test asserting "the shipped budget did not move" was the only one in the file that
    # could not see the shipped budget — it read 980000 and failed while the default was
    # untouched at 25000. Every sibling assertion in the suite already reads model_fields.
    declared = Settings.model_fields

    assert declared["max_query_complexity"].default == SHIPPED_COMPLEXITY == CONFIG.max_complexity
    assert declared["default_query_limit"].default == CONFIG.default_list_size
    assert declared["max_query_limit"].default == CONFIG.max_list_size


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("OrderEvent.payments", 10),
        ("OrderEvent.userActivity", 10),
        ("OrderEvent.relatedLogs", 10),
        ("PaymentEvent.relatedLogs", 10),
        ("UserEvent.orders", 10),
        ("UserEvent.relatedLogs", 10),
    ],
)
def test_every_batched_traversal_costs_exactly_what_related_logs_costs(
    key: str, expected: int
) -> None:
    """Identical work, identical price — and the reference is ``LogEntry.relatedLogs``.

    Each of these is one ``WHERE <key> IN (…)`` against an indexed column, dispatched once per
    operation by a DataLoader. Pricing them differently would put a number in the weight table that
    no measurement supports, and pricing them *lower* than ``relatedLogs`` would let a client swap
    an expensive traversal for a cheaper-looking one that costs the server the same.
    """
    assert DEFAULT_WEIGHTS[key].weight == expected == DEFAULT_WEIGHTS["LogEntry.relatedLogs"].weight


def test_the_single_valued_traversal_is_priced_like_the_other_by_key_lookups() -> None:
    """``PaymentEvent.order`` is one row from a batched read, so it costs what ``Query.log`` costs.

    It is not a list, so it multiplies nothing on its own — which makes it the cheapest field in
    the C11 block and the one a cycle would be built out of. ``DEEP_CYCLE`` below is what says the
    budget still catches that.
    """
    assert DEFAULT_WEIGHTS["PaymentEvent.order"].weight == DEFAULT_WEIGHTS["Query.log"].weight
    assert DEFAULT_WEIGHTS["PaymentEvent.order"].size is None


@pytest.mark.parametrize(
    ("key", "expected_size"),
    [
        ("Query.orderStatusDistribution", len(OrderStatus)),
        ("Query.orderFunnel", len(OrderStatus)),
        ("Query.paymentOutcomeBreakdown", len(PaymentMethod) * len(PaymentOutcome)),
    ],
)
def test_each_aggregate_declares_the_size_its_vocabulary_bounds_it_to(
    key: str, expected_size: int
) -> None:
    """A ``GROUP BY`` returns one row per bucket, and the buckets are an enum.

    Pinned against ``len(...)`` rather than against a literal, because the number is a *consequence*
    of the enum: a status added to ``OrderStatus`` makes the distribution one row wider, and a
    hardcoded 7 would keep pricing it at seven while the server returned eight. Without any ``size``
    at all these would inherit the ``DEFAULT_QUERY_LIMIT`` assumption and the dashboard query would
    price at roughly fourteen times what it costs — a gate rejecting the dashboard it was built for.
    """
    assert DEFAULT_WEIGHTS[key].size == expected_size


def test_no_traversal_carries_a_declared_size() -> None:
    """The nested lists take THE default assumption, exactly as ``relatedLogs`` does.

    An ``OrderEvent.payments`` sized at 3 because the seeded corpus produces three payment events
    per order would be pricing the *corpus* rather than the *schema* — nothing stops a real order
    from accumulating hundreds of retries, and the client cannot bound this list at all. So it is
    priced at ``DEFAULT_QUERY_LIMIT``, which is what the loader's ``max_per_key`` and the server's
    own cap make the realistic worst case.
    """
    for key in (
        "OrderEvent.payments",
        "OrderEvent.userActivity",
        "OrderEvent.relatedLogs",
        "UserEvent.orders",
    ):
        assert DEFAULT_WEIGHTS[key].size is None, key


# --- The pair that IS the requirement --------------------------------------------------------------


def test_the_flagship_dossier_is_admitted_and_the_document_is_valid(
    gql_schema: GraphQLSchema,
) -> None:
    """The four-REST-calls-in-one query prices at 10,360 and runs under the shipped budget.

    Both numbers asserted separately and on purpose: the cost, so a weight change cannot move it
    quietly, and the budget, so lowering ``MAX_QUERY_COMPLEXITY`` under it fails *here* rather than
    in a dashboard.

    Validity is asserted too, because "the gate would allow it" says nothing about whether the
    document is a document. A typo in a field name would price at the default weight and sail
    through the arithmetic while being un-executable — the integration suite then actually runs it.
    """
    assert validate(gql_schema, parse(FLAGSHIP)) == [], "the flagship must be a valid document"

    priced = cost(gql_schema, FLAGSHIP, {"orderId": "ord-60000"})

    assert priced == FLAGSHIP_COST == 10_360
    assert priced <= SHIPPED_COMPLEXITY, (
        f"the shipped budget of {SHIPPED_COMPLEXITY} rejects the flagship dossier at {priced}: "
        "one order with its payments, its user's activity and its correlated log lines is the "
        "capability spec §3 Feature Area B asks for, and a budget that refuses it is broken "
        "rather than strict"
    )


@pytest.mark.parametrize(
    ("label", "document", "expected"),
    [
        ("three traversals over a full page", DEEP_PAGE, DEEP_PAGE_COST),
        ("a list -> single -> list cycle", DEEP_CYCLE, DEEP_CYCLE_COST),
    ],
)
def test_deep_nested_e_commerce_traversals_are_refused(
    gql_schema: GraphQLSchema, label: str, document: str, expected: int
) -> None:
    """Admitting the flagship must not have admitted the fan-out behind it.

    The other half of the calibration, and the pair is worth more than either test alone: the same
    budget that accepts 10,360 refuses 33,010 and 1,151,010. Both are **one selection set deeper**
    than something legitimate, so neither is caught by ``MAX_QUERY_DEPTH`` — which is precisely why
    a cost gate exists alongside a depth limit.
    """
    priced = cost(gql_schema, document)

    assert priced == expected, label
    assert priced > SHIPPED_COMPLEXITY, f"{label} was admitted at {priced}"


def test_the_gate_is_a_gate_rather_than_a_wall(gql_schema: GraphQLSchema) -> None:
    """The fix the rejection message names — lower ``limit`` — actually works.

    ``DEEP_PAGE`` is refused at 33,010. The *same three traversals* with ``limit: 10`` are served,
    and that is the difference between a bound a client can work with and a capability that is
    simply unavailable. A gate whose only remedy is "ask for something else" would be a wall with a
    number on it.
    """
    refused = cost(gql_schema, DEEP_PAGE)
    narrowed = cost(
        gql_schema,
        """
        {
          orderEvents(filters: {limit: 10}) {
            payments { id }
            userActivity { id }
            relatedLogs { id }
          }
        }
        """,
    )

    assert refused > SHIPPED_COMPLEXITY
    # 10 + 3 x (10 x 10 + 1 x 10 x 100) = 10 + 3 x 1100
    assert narrowed == 3_310
    assert narrowed <= SHIPPED_COMPLEXITY


def test_one_traversal_over_a_full_page_is_admitted_exactly_like_related_logs(
    gql_schema: GraphQLSchema,
) -> None:
    """``{ orderEvents { payments { id } } }`` is 11,010 — the e-commerce twin of C8's flagship.

    C8 calibrated 25,000 so that ONE level of correlation at the default page size runs
    (``{ logs { id relatedLogs { id } } }`` = 11,110). The e-commerce traversals are priced
    identically, so the same sentence holds for them: one level at the default page size is served.
    That is not a coincidence to be admired, it is the property that lets a client move between the
    two halves of this schema without relearning what it can afford.
    """
    assert cost(gql_schema, "{ orderEvents { payments { id } } }") == 11_010
    assert cost(gql_schema, "{ logs { id relatedLogs { id } } }") == 11_110

    # Two traversals still fit; the third is what tips it. Stated as the boundary rather than as
    # two unrelated numbers, because the interesting fact is where the line falls.
    assert cost(gql_schema, "{ orderEvents { payments { id } userActivity { id } } }") == 22_010
    assert cost(gql_schema, DEEP_PAGE) == 33_010


# --- The dashboard the aggregates exist for --------------------------------------------------------


def test_the_three_panel_dashboard_costs_almost_nothing(gql_schema: GraphQLSchema) -> None:
    """220 for every field of all three aggregates — because a GROUP BY returns buckets, not rows.

    This is what the explicit ``size`` entries buy, and the contrast is the argument for them: the
    same three fields with no declared size would inherit the 100-row assumption and price at
    roughly 3,000 — thirteen times the truth, for a query that reads a handful of buckets. The
    dashboard is Feature Area E's "multi-series analytics from a single query result", so it must
    be affordable in one document or the requirement is unmeetable.
    """
    assert validate(gql_schema, parse(DASHBOARD)) == []
    assert cost(gql_schema, DASHBOARD) == DASHBOARD_COST == 220


def test_an_aggregate_is_not_made_cheaper_by_a_limit(gql_schema: GraphQLSchema) -> None:
    """``filters.limit`` cannot lower an aggregate's price, because it cannot lower its work.

    The resolver ignores ``limit`` (an aggregate describes the whole matching set), so the cost
    model must ignore it too. If it did not, ``orderFunnel(filters: {limit: 1})`` would be priced at
    a fraction of a full-window ``COUNT(DISTINCT ...)`` that it is going to run in its entirety —
    a one-word discount on the most expensive read in the schema.
    """
    unbounded = cost(gql_schema, "{ orderFunnel { status ordersReached } }")
    bounded = cost(gql_schema, "{ orderFunnel(filters: {limit: 1}) { status ordersReached } }")

    assert unbounded == bounded == 40 + 2 * len(OrderStatus)


def test_the_by_id_entry_points_are_priced_like_query_log(gql_schema: GraphQLSchema) -> None:
    """A single batched primary-key read, and nothing multiplies under it.

    ``orderEvent(id:)`` is not a list, so its sub-selection is resolved once — 5 for the lookup plus
    one per selected leaf. The regression this would catch is the field being *treated* as a list by
    the walker, which would price a two-field selection at 205 instead of 7 and make every by-id
    hydration look like a table scan.
    """
    assert cost(gql_schema, '{ orderEvent(id: "1") { id status } }') == 5 + 2
    assert cost(gql_schema, '{ paymentEvent(id: "1") { id outcome } }') == 5 + 2
    assert cost(gql_schema, '{ userEvent(id: "1") { id activityType } }') == 5 + 2
