"""The published shape of C12's two new fields — spec §3 Feature Area C.

Names and types, not behaviour. Behaviour is ``tests/integration/test_order_subscriptions.py``'s;
what is pinned here is the surface a client writes its document against, because a rename is the
one kind of regression that leaves every behavioural test green and breaks every caller.

.. rubric:: Two callers already depend on these exact spellings

* ``scripts/verify_e2e.py`` sends ``createOrderEvent(orderData: $orderData)`` and
  ``orderStatusStream(orderId: …, status: …, userId: …)`` as literal strings against a running
  container. A renamed argument fails ``make e2e`` with a GraphQL validation error rather than a
  useful message.
* C13's dashboard will do the same from Apollo.

The Python parameters are ``order_data``, ``order_id`` and ``user_id``; Strawberry's
``auto_camel_case`` is what publishes them as ``orderData``, ``orderId`` and ``userId``. That
transformation is a library setting, and this file is what notices if it is ever turned off.

.. rubric:: Read against the SDL the server renders, not against an introspection query

``schema.as_str()`` parsed by ``graphql-core`` — the same technique ``tests/unit/test_ecommerce_cost.py``
uses. The schema is async-only (its extension hooks are async generators), so ``execute_sync`` is
unavailable and an introspection query would need an event loop for what is fundamentally a string
comparison.
"""

from __future__ import annotations

from typing import Any

import pytest
from graphql import GraphQLSchema, build_schema as build_graphql_schema

from src.graphql.cost import DEFAULT_WEIGHTS
from src.graphql.enums import OrderStatus
from src.graphql.mutation import CREATE_ORDER_EVENT_DESCRIPTION
from src.graphql.schema import schema as strawberry_schema
from src.graphql.subscription import ORDER_STATUS_STREAM_DESCRIPTION


@pytest.fixture(scope="module")
def gql_schema() -> GraphQLSchema:
    """The published schema as a ``GraphQLSchema``, built from the SDL the server renders."""
    return build_graphql_schema(strawberry_schema.as_str())


def field_of(gql_schema: GraphQLSchema, type_name: str, field_name: str) -> Any:
    """One field of a root type, failing with the available names rather than a ``KeyError``."""
    root = gql_schema.type_map.get(type_name)
    assert root is not None, f"the schema publishes no type named {type_name!r}"
    fields = getattr(root, "fields", None)
    assert fields is not None, f"{type_name} is not a fielded type"
    assert field_name in fields, (
        f"{type_name} publishes no field {field_name!r}; it has {sorted(fields)}"
    )
    return fields[field_name]


def args_of(gql_schema: GraphQLSchema, type_name: str, field_name: str) -> dict[str, str]:
    """``{argument name: rendered SDL type}`` for one field."""
    field = field_of(gql_schema, type_name, field_name)
    return {name: str(arg.type) for name, arg in field.args.items()}


def input_fields_of(gql_schema: GraphQLSchema, type_name: str) -> dict[str, str]:
    """``{field name: rendered SDL type}`` for an input object type."""
    record = gql_schema.type_map.get(type_name)
    assert record is not None, f"the schema publishes no input type named {type_name!r}"
    fields = getattr(record, "fields", None)
    assert fields is not None, f"{type_name} is not a fielded type"
    return {name: str(field.type) for name, field in fields.items()}


# =================================================================================================
# Subscription.orderStatusStream
# =================================================================================================


def test_the_order_stream_is_published_on_the_subscription_root(gql_schema: GraphQLSchema) -> None:
    """Spec §3 Feature Area C's field exists, beside C6's rather than instead of it."""
    subscription = gql_schema.subscription_type

    assert subscription is not None, "the schema publishes no Subscription root"
    assert "orderStatusStream" in subscription.fields
    assert "logStream" in subscription.fields, "C12 added a stream; it did not replace one"


def test_the_order_stream_yields_one_event_rather_than_a_list(gql_schema: GraphQLSchema) -> None:
    """``OrderEvent!`` — one transition per frame, non-null.

    Non-null matters for the cost model as much as for the client: a list here would make the root
    multiplier the default page size instead of 1, and every selection under the subscription would
    be priced a hundred times over. See ``tests/unit/test_order_stream_cost.py``.
    """
    field = field_of(gql_schema, "Subscription", "orderStatusStream")

    assert str(field.type) == "OrderEvent!"


def test_the_order_stream_takes_exactly_the_three_documented_filters(
    gql_schema: GraphQLSchema,
) -> None:
    """"Filtering by order id, status, and/or user" — three arguments, and only three.

    All three optional, because "and/or" means none of them is a required scope: a board watching
    one order, an ops view watching every CANCELLED, and the unfiltered firehose C13 opens are all
    legitimate subscriptions.

    ``status`` is the ``OrderStatus`` enum rather than ``String``, and that is the whole reason a
    typo is a validation error naming the seven legal values instead of a subscription that opens,
    matches nothing forever, and is indistinguishable from a quiet server.
    """
    assert args_of(gql_schema, "Subscription", "orderStatusStream") == {
        "orderId": "String",
        "status": "OrderStatus",
        "userId": "String",
    }


def test_the_order_stream_publishes_no_service_or_level_filter(gql_schema: GraphQLSchema) -> None:
    """Deliberately absent: every order event has one service and a severity derived from its
    status, so both would be filters that match everything or nothing — a surface that looks like
    a choice and is not."""
    args = args_of(gql_schema, "Subscription", "orderStatusStream")

    assert "service" not in args
    assert "level" not in args


def test_the_log_stream_arguments_did_not_move(gql_schema: GraphQLSchema) -> None:
    """C6's surface, asserted here so "C12 added a stream" is checked rather than asserted."""
    field = field_of(gql_schema, "Subscription", "logStream")

    assert str(field.type) == "LogEntry!"
    assert args_of(gql_schema, "Subscription", "logStream") == {
        "service": "String",
        "level": "LogLevel",
    }


def test_the_order_stream_carries_its_documented_description(gql_schema: GraphQLSchema) -> None:
    """The description is the client-facing half of the server-side-filtering guarantee.

    Compared against the module constant rather than by substring, so this fails when the published
    text and the source of truth drift apart rather than when the prose is edited. Stripped only
    because the SDL renders a description as an indented block string and the round trip through
    ``build_schema`` is entitled to differ by surrounding whitespace.
    """
    field = field_of(gql_schema, "Subscription", "orderStatusStream")

    assert field.description is not None
    assert field.description.strip() == ORDER_STATUS_STREAM_DESCRIPTION.strip()
    assert "SERVER-SIDE" in field.description
    assert "SLOW_CONSUMER" in field.description


# =================================================================================================
# Mutation.createOrderEvent
# =================================================================================================


def test_the_mutation_is_published_beside_create_log(gql_schema: GraphQLSchema) -> None:
    """The event source Feature Area C needs: transitions have to be able to *occur*."""
    mutation = gql_schema.mutation_type

    assert mutation is not None
    assert "createOrderEvent" in mutation.fields
    assert "createLog" in mutation.fields


def test_the_mutation_takes_order_data_and_returns_the_created_event(
    gql_schema: GraphQLSchema,
) -> None:
    """``createOrderEvent(orderData: CreateOrderEventInput!): OrderEvent!``.

    The argument name is not cosmetic: ``scripts/verify_e2e.py`` sends ``orderData`` as a literal
    string. Renaming it to ``input`` (the Relay convention) or ``event`` would break the verifier
    while leaving every behavioural test green — which is exactly what happened to ``logData``'s
    shape test at C4 and why that one exists.

    Returning the created object rather than a status is what lets C13 render the transition
    immediately, and what makes an optimistic update reconcilable.
    """
    field = field_of(gql_schema, "Mutation", "createOrderEvent")

    assert str(field.type) == "OrderEvent!"
    assert args_of(gql_schema, "Mutation", "createOrderEvent") == {
        "orderData": "CreateOrderEventInput!"
    }


def test_the_mutation_carries_its_documented_description(gql_schema: GraphQLSchema) -> None:
    """It states the publish-after-commit ordering, which is the guarantee a subscriber relies on."""
    field = field_of(gql_schema, "Mutation", "createOrderEvent")

    assert field.description is not None
    assert field.description.strip() == CREATE_ORDER_EVENT_DESCRIPTION.strip()
    assert "committed FIRST" in field.description


def test_the_input_publishes_the_required_three_and_defaults_the_rest(
    gql_schema: GraphQLSchema,
) -> None:
    """Three required fields and five optional ones, and the asymmetry with ``createLog`` is the
    domain's.

    ``service`` and ``level`` are optional *here* and required on ``CreateLogInput``, because an
    order transition has one obvious emitter and a severity that is a property of its status, while
    a log line's source is genuinely unknown to this server. Making them non-null would turn a
    convenience into a constraint on every caller; dropping them entirely would refuse a partner
    feed with its own service name.
    """
    assert input_fields_of(gql_schema, "CreateOrderEventInput") == {
        "orderId": "String!",
        "userId": "String!",
        "status": "OrderStatus!",
        "service": "String",
        "level": "LogLevel",
        "timestamp": "DateTime",
        "metadata": "JSON",
        "traceId": "String",
    }


def test_the_status_enum_the_filter_and_the_input_share_is_fully_published(
    gql_schema: GraphQLSchema,
) -> None:
    """One vocabulary for the filter argument, the input field and the returned event.

    Pinned against the Python enum rather than a literal list, so a status added to
    ``src.graphql.enums.OrderStatus`` cannot be published on one of the three and not the others.
    """
    published = gql_schema.type_map.get("OrderStatus")

    assert published is not None
    assert set(published.values) == {member.name for member in OrderStatus}
    assert len(published.values) == 7


# =================================================================================================
# The cost gate has no new holes
#
# The existing tripwires walk `Query`'s list fields (`test_ecommerce_schema.py`) and the traversal
# fields on the three event types. NEITHER covers `Subscription` or `Mutation`, so C12's two new
# fields would have been priced at the default weight of 1 without anything failing. These two
# close that gap on the same principle.
# =================================================================================================


@pytest.mark.parametrize("root", ["Subscription", "Mutation"])
def test_every_field_on_a_write_or_stream_root_carries_an_explicit_cost_weight(
    gql_schema: GraphQLSchema, root: str
) -> None:
    """An unweighted root field is priced at 1 — a hole, not a neutral default.

    A subscription is priced **per delivered event**, so its weight is what bounds the work a
    client can attach to each frame for as long as the socket stays open; a mutation's is what
    prices an INSERT plus a fan-out. Either at weight 1 is indistinguishable from selecting a
    scalar. The check walks the **schema**, so a field added in C13 without a weight fails here
    rather than surviving until somebody prices a document by hand.
    """
    record = gql_schema.type_map.get(root)
    assert record is not None, f"the schema publishes no {root} root"

    names = list(record.fields)
    assert names, f"{root} publishes no fields, so this test proves nothing"

    unweighted = [
        name
        for name in names
        if f"{root}.{name}" not in DEFAULT_WEIGHTS and name not in DEFAULT_WEIGHTS
    ]

    assert unweighted == [], (
        f"these {root} fields have no src.graphql.cost.DEFAULT_WEIGHTS entry and are priced at the "
        f"default weight of 1: {unweighted}"
    )


def test_the_c12_fields_specifically_are_weighted(gql_schema: GraphQLSchema) -> None:
    """Named explicitly as well as covered by the walk, so a failure says which field.

    Both are priced at exactly what their C6/C4 counterparts cost, because they do exactly the same
    work: one dequeue and one serialisation for the stream, one INSERT and one fan-out for the
    write. Pricing them differently would be a claim about relative cost that nothing has measured.
    """
    assert DEFAULT_WEIGHTS["Subscription.orderStatusStream"].weight == (
        DEFAULT_WEIGHTS["Subscription.logStream"].weight
    )
    assert DEFAULT_WEIGHTS["Mutation.createOrderEvent"].weight == (
        DEFAULT_WEIGHTS["Mutation.createLog"].weight
    )
    assert DEFAULT_WEIGHTS["Subscription.orderStatusStream"].size is None, (
        "the stream yields one event per frame; a declared size would price it as a page"
    )


def test_the_order_event_traversals_are_reachable_through_the_subscription_root(
    gql_schema: GraphQLSchema,
) -> None:
    """The reason the weight above matters more here than it does for ``logStream``.

    ``OrderEvent`` carries three traversals where ``LogEntry`` carries one, and every one of them is
    selectable *inside a subscription* — so the cost of a delivered frame is a client's choice, not
    a constant. ``tests/unit/test_order_stream_cost.py`` prices the consequences.
    """
    order_event = gql_schema.type_map.get("OrderEvent")

    assert order_event is not None
    for traversal in ("payments", "userActivity", "relatedLogs"):
        assert traversal in order_event.fields
        assert f"OrderEvent.{traversal}" in DEFAULT_WEIGHTS
