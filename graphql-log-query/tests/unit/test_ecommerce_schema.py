"""The published shape of Feature Area A: the ``LogEvent`` interface and its four implementors.

Asserted through **introspection**, exactly as ``tests/unit/test_graphql_schema.py`` asserts the
core schema, and for the same reason: reaching into ``OrderEvent.__strawberry_definition__`` would
test the decorator's bookkeeping, while a schema is only wrong in ways that matter when the
*published* contract is wrong.

What is pinned here, and the regression each assertion would catch:

* **The interface exists and is an interface.** A ``@strawberry.type`` typo makes ``LogEvent`` an
  ordinary object that nothing implements; the SDL would still contain the name.
* **All four types implement it — ``LogEntry`` included.** An interface implemented only by the
  three e-commerce types is a *parallel hierarchy*: a second notion of "an event with a correlation
  id" beside the one the schema already had, and a client that cannot ask for a trace's log lines
  and its order events in one selection. This is the assertion most likely to be broken by a
  well-meant refactor.
* **The interface is queryable.** ``Query.correlatedEvents`` returns ``[LogEvent!]!``, which is
  what forces a client to write inline fragments and is therefore what makes the interface load
  bearing rather than decorative. The integration suite proves it returns a real ``__typename``
  mix; this proves the *type* is right.
* **The core contract survived.** ``LogEntry`` gained a base class, so the spec's §5 acceptance
  document is re-validated here against the live schema.
* **Every list field on the Query root carries a cost weight.** A list field with no entry in
  ``DEFAULT_WEIGHTS`` is priced at 1 — a hole in C8's gate that no cost test would notice, because
  cost tests price documents somebody wrote down.

No database, no lifespan, no HTTP: the schema is a pure function of the source.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any, Optional, TypeVar

import pytest
from graphql import build_schema as build_graphql_schema, parse, validate

from src.generators import (
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_OUTCOMES,
    USER_ACTIVITIES,
)
from src.graphql.cost import DEFAULT_WEIGHTS
from src.graphql.schema import schema

_T = TypeVar("_T")

#: The four types the interface must publish as its implementors.
IMPLEMENTORS = {"LogEntry", "OrderEvent", "PaymentEvent", "UserEvent"}

#: The spec's §5 acceptance command, verbatim. C10 changes ``LogEntry``'s base class, which is the
#: single most likely way this commit could break the core contract.
SPEC_ACCEPTANCE_DOCUMENT = "{ logs { id service level message } }"

SHAPE_QUERY = """
query Shape($name: String!) {
  __type(name: $name) {
    kind
    name
    description
    fields {
      name
      args { name type { ...Ref } }
      type { ...Ref }
    }
    inputFields { name type { ...Ref } }
    enumValues { name }
    interfaces { name }
    possibleTypes { name }
  }
}

fragment Ref on __Type {
  kind
  name
  ofType { kind name ofType { kind name ofType { kind name } } }
}
"""


def _run(coro: Awaitable[_T]) -> _T:
    """Run ``coro`` on a private event loop.

    ``execute_sync`` is unavailable on this schema — ``PerOperationResources``' hook is an async
    generator — and a private loop leaves the ambient policy pytest-asyncio manages untouched.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)  # type: ignore[arg-type]
    finally:
        loop.close()


def _render_type(ref: Optional[dict[str, Any]]) -> str:
    """Render an introspection type reference back into SDL notation (``[OrderEvent!]!``)."""
    if ref is None:
        return "<null>"
    kind = ref["kind"]
    if kind == "NON_NULL":
        return f"{_render_type(ref['ofType'])}!"
    if kind == "LIST":
        return f"[{_render_type(ref['ofType'])}]"
    return str(ref["name"])


def _introspect(name: str) -> dict[str, Any]:
    """The introspection record for one named type, failing loudly if it is absent."""
    result = _run(schema.execute(SHAPE_QUERY, variable_values={"name": name}))

    assert result.errors is None, f"introspecting {name} failed: {result.errors}"
    assert result.data is not None
    record = result.data["__type"]
    assert record is not None, f"the schema publishes no type named {name!r}"
    return record


def _field_types(name: str) -> dict[str, str]:
    """``{field name: rendered SDL type}`` for an object or interface type."""
    record = _introspect(name)
    assert record["fields"] is not None, f"{name} is a {record['kind']}, not a fielded type"
    return {field["name"]: _render_type(field["type"]) for field in record["fields"]}


def _input_field_types(name: str) -> dict[str, str]:
    """``{field name: rendered SDL type}`` for an input object type."""
    record = _introspect(name)
    assert record["inputFields"] is not None, f"{name} is a {record['kind']}, not an input type"
    return {field["name"]: _render_type(field["type"]) for field in record["inputFields"]}


def _enum_values(name: str) -> tuple[str, ...]:
    """The published members of an enum type, in declaration order."""
    record = _introspect(name)
    assert record["enumValues"] is not None, f"{name} is a {record['kind']}, not an enum"
    return tuple(value["name"] for value in record["enumValues"])


def _args(type_name: str, field_name: str) -> dict[str, str]:
    """``{argument name: rendered SDL type}`` for one field."""
    record = _introspect(type_name)
    matches = [field for field in record["fields"] if field["name"] == field_name]
    assert matches, f"{type_name} has no field {field_name!r}"
    return {arg["name"]: _render_type(arg["type"]) for arg in matches[0]["args"]}


# --- The interface --------------------------------------------------------------------------------


def test_log_event_is_published_as_an_interface() -> None:
    """``INTERFACE``, not ``OBJECT``.

    A ``@strawberry.type`` where ``@strawberry.interface`` was meant produces a schema that still
    contains a type called ``LogEvent`` — implemented by nothing, referenced by
    ``correlatedEvents``, and unable to hold anything at all. The SDL would look almost right.
    """
    record = _introspect("LogEvent")

    assert record["kind"] == "INTERFACE"


def test_log_event_carries_exactly_the_four_common_log_fields() -> None:
    """Spec §3 Feature Area A: "timestamp, service, level, trace/correlation id".

    Exactly four. Anything else on the interface would have to mean the same thing on a log line, an
    order transition, a payment outcome and a user action — and a client selecting it off the
    interface has no idea which it is reading. ``message`` (an order event has none), ``metadata``
    and ``id`` are all deliberately absent; see the block comment in ``src/graphql/types.py``.

    ``traceId`` is nullable and that is load-bearing rather than lax: ~40% of the log corpus carries
    no trace id, and C5's ``relatedLogs`` empty-list branch is defined over exactly those rows. A
    non-null interface field cannot be nullable on an implementor.
    """
    assert _field_types("LogEvent") == {
        "timestamp": "DateTime!",
        "service": "String!",
        "level": "LogLevel!",
        "traceId": "String",
    }


def test_the_interface_documents_itself_in_the_sdl() -> None:
    """A description reaches GraphiQL and the committed SDL; a docstring would not.

    Descriptions on this schema are opt-in (see the note in ``src/graphql/enums.py``), so an
    interface whose whole purpose is to tell a client "these four things are substitutable" has to
    say so explicitly or the SDL is silent about the one thing it exists for.
    """
    description = _introspect("LogEvent")["description"] or ""

    assert description
    assert "inline fragment" in description.lower()


def test_all_four_event_types_implement_the_interface() -> None:
    """``LogEntry`` included — the assertion this whole commit turns on.

    An interface implemented only by the three e-commerce types would be a parallel hierarchy: two
    unrelated notions of "an event with a correlation id" in one schema, and no way for a client to
    ask "everything correlated with this trace" and get its log lines back with its order events.
    """
    possible = {entry["name"] for entry in _introspect("LogEvent")["possibleTypes"]}

    assert possible == IMPLEMENTORS


@pytest.mark.parametrize("type_name", sorted(IMPLEMENTORS))
def test_each_implementor_declares_the_interface(type_name: str) -> None:
    """Asserted from the implementor's side too.

    ``possibleTypes`` and ``interfaces`` are two independent projections of the same fact in the
    introspection schema, and a type registered in one place but not the other is a schema that
    validates inconsistently depending on which direction a client reads it from.
    """
    interfaces = {entry["name"] for entry in _introspect(type_name)["interfaces"]}

    assert "LogEvent" in interfaces


@pytest.mark.parametrize("type_name", sorted(IMPLEMENTORS))
def test_each_implementor_republishes_the_interface_fields_with_the_same_types(
    type_name: str,
) -> None:
    """An implementor must carry all four fields at the interface's exact types.

    GraphQL enforces this at schema-build time, so a violation is a startup crash rather than a
    silent bug — which is precisely why it is worth asserting: this test is what says the crash
    would be *about* the interface rather than about something further downstream.
    """
    fields = _field_types(type_name)
    interface_fields = _field_types("LogEvent")

    for name, rendered in interface_fields.items():
        assert fields.get(name) == rendered, f"{type_name}.{name}"


# --- The three concrete types ---------------------------------------------------------------------


def test_order_event_publishes_its_status_and_both_identifiers() -> None:
    """"Order events carry status" (spec §3 Area A), plus the modeled order -> user edge.

    ``status`` is the ``OrderStatus`` **enum**, not a string, for the same reason ``level`` is an
    enum: ``status: "SHIPED"`` must be rejected during validation with a message naming the legal
    values rather than reaching a resolver and returning an empty list.
    """
    assert _field_types("OrderEvent") == {
        "timestamp": "DateTime!",
        "service": "String!",
        "level": "LogLevel!",
        "traceId": "String",
        "id": "ID!",
        "orderId": "String!",
        "userId": "String!",
        "status": "OrderStatus!",
        "metadata": "JSON",
    }


def test_payment_event_publishes_its_method_and_outcome() -> None:
    """"Payment events carry method and outcome" (spec §3 Area A), filed under an ``orderId``."""
    assert _field_types("PaymentEvent") == {
        "timestamp": "DateTime!",
        "service": "String!",
        "level": "LogLevel!",
        "traceId": "String",
        "id": "ID!",
        "orderId": "String!",
        "method": "PaymentMethod!",
        "outcome": "PaymentOutcome!",
        "metadata": "JSON",
    }


def test_user_event_publishes_its_activity_type() -> None:
    """"User events carry activity type" (spec §3 Area A), published camel-cased."""
    assert _field_types("UserEvent") == {
        "timestamp": "DateTime!",
        "service": "String!",
        "level": "LogLevel!",
        "traceId": "String",
        "id": "ID!",
        "userId": "String!",
        "activityType": "UserActivity!",
        "metadata": "JSON",
    }


@pytest.mark.parametrize("type_name", ["OrderEvent", "PaymentEvent", "UserEvent"])
def test_the_new_types_publish_no_list_field_yet(type_name: str) -> None:
    """C11 owns nested traversal, and a nested list without a cost weight is a gate hole.

    ``OrderEvent.payments`` and friends land in C11 **with** their DataLoaders and **with** rows in
    ``DEFAULT_WEIGHTS``. This test is the tripwire: adding one here without the other two makes the
    suite red rather than making the cost gate quietly cheaper.
    """
    lists = [name for name, rendered in _field_types(type_name).items() if "[" in rendered]

    assert lists == [], (
        f"{type_name} gained the list field(s) {lists}; add a src.graphql.cost.DEFAULT_WEIGHTS "
        "entry and a batching DataLoader before relaxing this test"
    )


# --- The enums ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("enum_name", "roster"),
    [
        ("OrderStatus", ORDER_STATUSES),
        ("PaymentMethod", PAYMENT_METHODS),
        ("PaymentOutcome", PAYMENT_OUTCOMES),
        ("UserActivity", USER_ACTIVITIES),
    ],
)
def test_each_enum_publishes_exactly_its_generator_roster(
    enum_name: str, roster: tuple[str, ...]
) -> None:
    """The published enum and the corpus vocabulary agree, in order.

    The enum is the half a client writes queries against; the roster is the half the seeded corpus,
    every oracle and C12's verifier are built from. A member here the corpus cannot produce is a
    filter that always returns nothing; a roster value missing here is a stored row that cannot be
    serialised back out at all.

    ``src.graphql.enums`` already fails at **import** on a mismatch. This asserts the same thing
    through introspection, so the guarantee holds against the *published* schema rather than only
    against the Python object — the two can differ (a camel-casing setting, a decorator that
    renamed a member).
    """
    assert _enum_values(enum_name) == roster


# --- The query surface ----------------------------------------------------------------------------


def test_the_three_event_list_fields_return_non_null_lists_of_non_null_events() -> None:
    """Flat lists, exactly like ``Query.logs`` — not connections and not nullable.

    "No matching events" is an empty list, never ``null``: a nullable list gives a client two
    spellings of one fact and forces it to handle both.
    """
    query_fields = _field_types("Query")

    assert query_fields["orderEvents"] == "[OrderEvent!]!"
    assert query_fields["userEvents"] == "[UserEvent!]!"
    assert query_fields["paymentEvents"] == "[PaymentEvent!]!"


def test_correlated_events_returns_the_interface_so_it_must_be_queried_with_fragments() -> None:
    """``[LogEvent!]!`` — the only field in the schema typed by the interface.

    That is what makes ``LogEvent`` observable: every other field returns a concrete type, so an
    interface nothing returned could never appear in a client's selection set and would be a
    documentation device with a syntax.
    """
    assert _field_types("Query")["correlatedEvents"] == "[LogEvent!]!"


def test_correlated_events_requires_a_trace_id_and_accepts_an_optional_limit() -> None:
    """``traceId`` is **non-null**, deliberately.

    An optional trace id would make the field mean "every event in the system" when omitted, which
    is what the three list fields are for and which no per-table cap makes cheap. Requiring it is
    also what lets the resolver validate one value instead of branching.
    """
    assert _args("Query", "correlatedEvents") == {"traceId": "String!", "limit": "Int"}


@pytest.mark.parametrize(
    ("field_name", "input_name"),
    [
        ("orderEvents", "OrderEventFilterInput"),
        ("userEvents", "UserEventFilterInput"),
        ("paymentEvents", "PaymentEventFilterInput"),
    ],
)
def test_each_event_list_field_takes_its_own_optional_filter_input(
    field_name: str, input_name: str
) -> None:
    """``filters`` is optional on every one, so "omitted filters are ignored" starts at the argument."""
    assert _args("Query", field_name) == {"filters": input_name}


# --- The filter inputs ----------------------------------------------------------------------------


def test_the_order_event_filter_publishes_every_dimension_c11_composes_over() -> None:
    """The shared five plus the three order dimensions, all camel-cased."""
    assert _input_field_types("OrderEventFilterInput") == {
        "service": "String",
        "level": "LogLevel",
        "startTime": "DateTime",
        "endTime": "DateTime",
        "traceId": "String",
        "orderId": "String",
        "userId": "String",
        "status": "OrderStatus",
        "searchText": "String",
        "limit": "Int",
    }


def test_the_payment_event_filter_publishes_method_and_outcome() -> None:
    """Both are enums, so an unknown value dies in validation rather than in a WHERE clause."""
    assert _input_field_types("PaymentEventFilterInput") == {
        "service": "String",
        "level": "LogLevel",
        "startTime": "DateTime",
        "endTime": "DateTime",
        "traceId": "String",
        "orderId": "String",
        "method": "PaymentMethod",
        "outcome": "PaymentOutcome",
        "searchText": "String",
        "limit": "Int",
    }


def test_the_user_event_filter_publishes_the_activity_type() -> None:
    """``activityType``, matching the field on ``UserEvent`` — the two must agree."""
    assert _input_field_types("UserEventFilterInput") == {
        "service": "String",
        "level": "LogLevel",
        "startTime": "DateTime",
        "endTime": "DateTime",
        "traceId": "String",
        "userId": "String",
        "activityType": "UserActivity",
        "searchText": "String",
        "limit": "Int",
    }


@pytest.mark.parametrize(
    "input_name",
    ["OrderEventFilterInput", "PaymentEventFilterInput", "UserEventFilterInput"],
)
def test_no_event_filter_field_is_required(input_name: str) -> None:
    """Every field optional, or "omitted filters are ignored" (spec §2 item 19) cannot hold."""
    required = [
        name for name, rendered in _input_field_types(input_name).items() if rendered.endswith("!")
    ]

    assert required == [], f"these filters are required and must not be: {required}"


# --- The core contract still holds ----------------------------------------------------------------


def test_the_spec_acceptance_document_still_validates_against_the_live_schema() -> None:
    """``{ logs { id service level message } }`` — unchanged by ``LogEntry`` gaining a base class.

    Inheriting from ``LogEvent`` reorders ``LogEntry``'s fields in the SDL (a dataclass puts base
    fields first). Field *order* is cosmetic — a GraphQL response is keyed by the client's selection
    set — but "cosmetic" is a claim worth checking rather than asserting, because the regression it
    would hide is the project's own acceptance criterion.
    """
    gql_schema = build_graphql_schema(schema.as_str())

    errors = validate(gql_schema, parse(SPEC_ACCEPTANCE_DOCUMENT))

    assert errors == [], f"the spec acceptance command no longer validates: {errors}"


def test_log_entry_keeps_all_eight_published_fields() -> None:
    """Nothing was lost or renamed when the four interface fields moved to the base class."""
    assert _field_types("LogEntry") == {
        "id": "ID!",
        "timestamp": "DateTime!",
        "service": "String!",
        "level": "LogLevel!",
        "message": "String!",
        "metadata": "JSON",
        "traceId": "String",
        "relatedLogs": "[LogEntry!]!",
    }


def test_an_interface_selection_with_inline_fragments_validates() -> None:
    """The document the integration suite executes is *valid* against the schema.

    Separated from the execution test on purpose: a validation failure and an empty result look
    identical from the client's side of an HTTP call, and this says which one a red integration
    test would be.
    """
    gql_schema = build_graphql_schema(schema.as_str())
    document = """
    {
      correlatedEvents(traceId: "abc") {
        __typename
        timestamp
        service
        level
        traceId
        ... on LogEntry { id message }
        ... on OrderEvent { id orderId userId status }
        ... on PaymentEvent { id orderId method outcome }
        ... on UserEvent { id userId activityType }
      }
    }
    """

    assert validate(gql_schema, parse(document)) == []


def test_selecting_a_concrete_field_off_the_interface_is_a_validation_error() -> None:
    """``correlatedEvents { orderId }`` must fail — that is what "interface" means.

    The negative half of the assertion above. Without it, a schema in which ``correlatedEvents``
    returned ``[OrderEvent!]!`` (or in which every field had been hoisted onto the interface) would
    pass every positive test in this module.
    """
    gql_schema = build_graphql_schema(schema.as_str())

    errors = validate(gql_schema, parse('{ correlatedEvents(traceId: "abc") { orderId } }'))

    assert errors, "orderId is not a field of LogEvent and must not validate against it"


# --- The cost gate has no new holes ----------------------------------------------------------------


def test_every_list_field_on_the_query_root_carries_an_explicit_cost_weight() -> None:
    """A list field with no weight is priced at 1, which is a hole rather than a neutral default.

    ``orderEvents(filters: {limit: 500})`` would otherwise score 10 for its 500 rows only because
    of what is selected *under* it, and the read itself — the expensive part — would be free. The
    check walks the **schema**, so a field added in C11 without a weight fails here rather than
    surviving until somebody prices a document by hand.
    """
    list_fields = [
        name for name, rendered in _field_types("Query").items() if rendered.startswith("[")
    ]

    assert list_fields, "the Query root must expose list fields or this test proves nothing"

    unweighted = [
        name
        for name in list_fields
        if f"Query.{name}" not in DEFAULT_WEIGHTS and name not in DEFAULT_WEIGHTS
    ]

    assert unweighted == [], (
        f"these Query list fields have no src.graphql.cost.DEFAULT_WEIGHTS entry and are priced "
        f"at the default weight of 1: {unweighted}"
    )


@pytest.mark.parametrize(
    "field_name", ["orderEvents", "userEvents", "paymentEvents", "correlatedEvents"]
)
def test_the_c10_entry_points_are_weighted_at_least_as_much_as_a_scalar_read(
    field_name: str,
) -> None:
    """Each new root field costs more than a plain field, and the four-table one costs most.

    Named explicitly as well as swept above, because the sweep would be satisfied by a weight of
    exactly 1 written down on purpose — which is the same hole with a comment on it.
    """
    weight = DEFAULT_WEIGHTS[f"Query.{field_name}"].weight

    assert weight > 1
    if field_name == "correlatedEvents":
        assert weight >= 4 * DEFAULT_WEIGHTS["Query.orderEvents"].weight, (
            "correlatedEvents issues four reads across four tables and must not be priced like one"
        )
