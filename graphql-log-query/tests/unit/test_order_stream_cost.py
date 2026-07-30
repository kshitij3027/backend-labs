"""What the cost gate charges for ``orderStatusStream`` — the file ``src/graphql/cost.py`` names.

The weight table's C12 entry carries four worked numbers in its comment. This is where they are
checked rather than trusted, on the same discipline as ``tests/unit/test_cost.py`` and
``tests/unit/test_ecommerce_cost.py``: **every expectation is an exact integer and the arithmetic
is written out**, because a range hides the regression the file exists to catch. A walker that
stopped multiplying nested lists would still return "a big number" for a big subscription, and
``> 25_000`` would stay green through it.

.. rubric:: Why a subscription needs pricing at all, and what the unit is

``orderStatusStream`` returns ``OrderEvent!`` — **one** event, not a list — so the root multiplier
stays 1 and what is priced is the work *one arriving transition* costs. The stream's length is
bounded elsewhere entirely (``SUBSCRIPTION_QUEUE_MAXSIZE`` and the drop policy in ``src.broker``).
What this gate bounds is what a client can attach to each event, for as long as the socket is open —
and that is a real exposure, because ``OrderEvent`` carries **three** traversals where ``LogEntry``
carries one, and a subscription pays for them again on every frame rather than once per request.

.. rubric:: Both sides of every boundary

A gate that rejects everything rejects deep queries too, so each claim below comes in a pair: a
document that must be **admitted** and one that must be **refused**, both priced against the
**shipped** ``MAX_QUERY_COMPLEXITY`` — read off ``Settings.model_fields``, never off a constructed
``Settings``, because the compose ``test`` service overrides that variable so the suite's own
oversized documents can run.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest
from graphql import GraphQLSchema, build_schema as build_graphql_schema, parse, validate

from src.config import Settings
from src.graphql.cost import DEFAULT_WEIGHTS, CostConfig, document_cost
from src.graphql.schema import schema as strawberry_schema

#: The shipped budget and list sizes as literals, so the arithmetic below reads as arithmetic
#: rather than as a function of the environment. Tied back to ``src/config.py`` by
#: :func:`test_the_shipped_budget_did_not_move`.
CONFIG = CostConfig(max_complexity=25_000, default_list_size=100, max_list_size=500)

SHIPPED_COMPLEXITY = 25_000

# =================================================================================================
# The four documents `src/graphql/cost.py` prices in its C12 comment, and their arithmetic.
#
# Root multiplier is 1 throughout: `orderStatusStream` yields ONE event per frame. The traversals
# under it are lists with no client-supplied bound, so each takes THE default assumption — 100 rows
# — exactly as `LogEntry.relatedLogs` does.
# =================================================================================================

#: 10 (the stream) + 2 scalar leaves x 1
SCALARS = "subscription { orderStatusStream { orderId status } }"
SCALARS_COST = 12

#: 10 + payments 10 x 1 + `id` 1 x 100
ONE_TRAVERSAL = "subscription { orderStatusStream { payments { id } } }"
ONE_TRAVERSAL_COST = 120

#: 10 + 3 x (10 + 100). The dossier shape, per delivered event.
THREE_TRAVERSALS = """
subscription {
  orderStatusStream {
    payments { id }
    userActivity { id }
    relatedLogs { id }
  }
}
"""
THREE_TRAVERSALS_COST = 340

#: Alternating list -> single -> list, reached through the subscription root instead of through
#: `orderEvents`. `PaymentEvent.order` is the cheapest field in the C11 block (weight 5, not a list)
#: and is therefore the one that could have made a cycle look affordable.
#:   10 (stream)
#: + 10      payments  at multiplier 1
#: + 500     order     at multiplier 100
#: + 1,000   payments  at multiplier 100
#: + 50,000  order     at multiplier 10,000
#: + 100,000 payments  at multiplier 10,000
#: + 1,000,000  id     at multiplier 1,000,000
DEEP_CYCLE = """
subscription {
  orderStatusStream {
    payments { order { payments { order { payments { id } } } } }
  }
}
"""
DEEP_CYCLE_COST = 1_151_520

#: What C13's order board actually opens: one filtered stream, scalar fields only. Named separately
#: from :data:`SCALARS` because "the dashboard is affordable" is a different claim from "the model
#: multiplies", and the second one moving must not be able to hide behind the first.
DASHBOARD_STREAM = """
subscription OrderBoard($orderId: String, $status: OrderStatus, $userId: String) {
  orderStatusStream(orderId: $orderId, status: $status, userId: $userId) {
    id
    timestamp
    orderId
    userId
    status
    service
    level
    traceId
  }
}
"""
#: 10 + 8 leaves x 1. Arguments cost nothing; only selections do.
DASHBOARD_STREAM_COST = 18


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


# --- The table is anchored to what is actually shipped -------------------------------------------


def test_the_shipped_budget_did_not_move() -> None:
    """C12 added a stream and priced it; it did not widen the gate to fit it.

    Read off ``Settings.model_fields`` rather than a constructed ``Settings``: ``_env_file=None``
    disables only the dotenv source, and the compose ``test`` service sets
    ``MAX_QUERY_COMPLEXITY=980000`` so this suite's own oversized documents can execute. A
    constructed ``Settings`` would therefore see 980,000 and this assertion — the one in the file
    that is about a decision rather than about arithmetic — would be the only one that could not
    see the shipped number.
    """
    declared = Settings.model_fields

    assert declared["max_query_complexity"].default == SHIPPED_COMPLEXITY == CONFIG.max_complexity
    assert declared["default_query_limit"].default == CONFIG.default_list_size
    assert declared["max_query_limit"].default == CONFIG.max_list_size


def test_the_stream_is_priced_like_the_log_stream_and_declares_no_page_size() -> None:
    """One delivery is one dequeue and one serialisation either way, so the weights match.

    The absent ``size`` is the other half: declaring one would price the field as a page and make
    every selection under it a hundred times more expensive than the single event it actually
    carries.
    """
    entry = DEFAULT_WEIGHTS["Subscription.orderStatusStream"]

    assert entry.weight == DEFAULT_WEIGHTS["Subscription.logStream"].weight == 10
    assert entry.size is None


# --- The four pinned numbers ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        pytest.param(SCALARS, SCALARS_COST, id="scalars-only"),
        pytest.param(ONE_TRAVERSAL, ONE_TRAVERSAL_COST, id="one-traversal"),
        pytest.param(THREE_TRAVERSALS, THREE_TRAVERSALS_COST, id="three-traversals"),
        pytest.param(DEEP_CYCLE, DEEP_CYCLE_COST, id="alternating-cycle"),
        pytest.param(DASHBOARD_STREAM, DASHBOARD_STREAM_COST, id="c13-order-board"),
    ],
)
def test_known_subscription_documents_score_known_costs(
    gql_schema: GraphQLSchema, document: str, expected: int
) -> None:
    """The exact integers from ``src/graphql/cost.py``'s C12 comment, checked rather than trusted."""
    assert cost(gql_schema, document) == expected


@pytest.mark.parametrize(
    "document",
    [
        pytest.param(SCALARS, id="scalars-only"),
        pytest.param(ONE_TRAVERSAL, id="one-traversal"),
        pytest.param(THREE_TRAVERSALS, id="three-traversals"),
        pytest.param(DASHBOARD_STREAM, id="c13-order-board"),
    ],
)
def test_the_useful_subscriptions_are_admitted(gql_schema: GraphQLSchema, document: str) -> None:
    """The half a "deep queries are rejected" test can silently stop proving.

    Every one of these is a subscription somebody would actually open — including the full dossier
    per delivered event, which is the most a client can reasonably want. A gate that refused them
    would be a wall.
    """
    assert cost(gql_schema, document) <= SHIPPED_COMPLEXITY


def test_the_alternating_cycle_is_refused(gql_schema: GraphQLSchema) -> None:
    """Forty-six times the budget, so no plausible re-tuning admits it.

    Deliberately far over rather than just over: this must stay a statement about the *model*
    multiplying, not a calibration that fails whenever a weight is legitimately adjusted.
    """
    assert cost(gql_schema, DEEP_CYCLE) > SHIPPED_COMPLEXITY
    assert cost(gql_schema, DEEP_CYCLE) > 45 * SHIPPED_COMPLEXITY


@pytest.mark.parametrize(
    "document", [SCALARS, ONE_TRAVERSAL, THREE_TRAVERSALS, DEEP_CYCLE, DASHBOARD_STREAM]
)
def test_every_priced_document_is_a_valid_subscription(
    gql_schema: GraphQLSchema, document: str
) -> None:
    """A typo would price at the default weight and look like a cheap query.

    The cost walker prices a field it cannot find in the schema at ``DEFAULT_COST`` rather than
    crashing — correct behaviour, and the reason a cost test that never validates its own documents
    can drift into pricing nothing at all.
    """
    assert validate(gql_schema, parse(document)) == []


# --- The properties behind the numbers -------------------------------------------------------------


def test_the_root_multiplier_is_one_because_the_field_is_not_a_list(
    gql_schema: GraphQLSchema,
) -> None:
    """The single most load-bearing fact about pricing this field.

    If ``orderStatusStream`` were ``[OrderEvent!]!`` the walker would multiply its sub-selection by
    the default page size, and ``{ payments { id } }`` would price at 10 + 100 x (10 + 100) =
    11,010 instead of 120 — within budget either way, but the *three*-traversal shape would jump
    from 340 to 33,010 and be refused. So the arity of this field is a cost decision as much as an
    API one, and this is the assertion that ties the two together.
    """
    subscription = gql_schema.subscription_type
    assert subscription is not None
    assert str(subscription.fields["orderStatusStream"].type) == "OrderEvent!"

    # 10 + 1 leaf x 1 — a single selection costs one unit, not one hundred.
    assert cost(gql_schema, "subscription { orderStatusStream { id } }") == 11


def test_nesting_multiplies_rather_than_adds(gql_schema: GraphQLSchema) -> None:
    """The gradient spec §3 Feature Area D asks for is already in the model.

    One traversal is 120 and two levels of it is five figures — the difference is a product, not a
    sum. An additive walker would put the second document at a few hundred and admit the cycle.
    """
    one = cost(gql_schema, ONE_TRAVERSAL)
    two = cost(gql_schema, "subscription { orderStatusStream { payments { order { id } } } }")

    # 10 + 10 (payments) + 5 x 100 (order) + 1 x 100 (id) = 620
    assert two == 620
    assert two > one * 5, "a second level cost a multiple, not an increment"


def test_the_three_traversals_cost_the_same_as_each_other(gql_schema: GraphQLSchema) -> None:
    """Each is one batched ``WHERE key IN (...)`` against an indexed column — identical work.

    Pricing identical work differently would put a number in the table that no measurement
    supports, and would let a client pick the cheap spelling of the same query.
    """
    costs = {
        traversal: cost(gql_schema, f"subscription {{ orderStatusStream {{ {traversal} {{ id }} }} }}")
        for traversal in ("payments", "userActivity", "relatedLogs")
    }

    assert set(costs.values()) == {ONE_TRAVERSAL_COST}


def test_the_stream_and_a_single_order_read_price_a_traversal_identically(
    gql_schema: GraphQLSchema,
) -> None:
    """One delivered event and one fetched order do the same work per traversal.

    ``orderEvent(id:)`` is the by-id read; both are a single ``OrderEvent``, so ``{ payments { id } }``
    under either must cost the same *above* the root weight. A discrepancy would mean a client could
    dodge the gate by asking for the same data through the other door.
    """
    streamed = cost(gql_schema, ONE_TRAVERSAL)
    fetched = cost(gql_schema, '{ orderEvent(id: "1") { payments { id } } }')

    stream_weight = DEFAULT_WEIGHTS["Subscription.orderStatusStream"].weight
    read_weight = DEFAULT_WEIGHTS["Query.orderEvent"].weight

    assert streamed - stream_weight == fetched - read_weight


def test_a_second_operation_in_the_document_is_priced_too(gql_schema: GraphQLSchema) -> None:
    """``document_cost`` reports the most expensive operation, so a cheap first one is no cover.

    ``graphql-transport-ws`` sends one operation per ``subscribe`` message, but the payload is a
    whole document and nothing stops a client putting a second, expensive definition in it.
    """
    document = """
    subscription Cheap { orderStatusStream { id } }
    subscription Expensive {
      orderStatusStream { payments { order { payments { order { payments { id } } } } } }
    }
    """

    assert cost(gql_schema, document) == DEEP_CYCLE_COST


def test_a_mutation_and_a_subscription_agree_on_what_an_order_event_costs(
    gql_schema: GraphQLSchema,
) -> None:
    """``createOrderEvent`` and ``orderStatusStream`` carry the same weight and the same arity.

    So the same selection under either prices the same, and "write it, then watch it" costs a
    client the same on both halves — which is what makes the budget explicable rather than a table
    of unrelated numbers.
    """
    written = cost(
        gql_schema,
        'mutation { createOrderEvent(orderData: {orderId: "o", userId: "u", status: CREATED}) '
        "{ payments { id } } }",
    )

    assert written == ONE_TRAVERSAL_COST
