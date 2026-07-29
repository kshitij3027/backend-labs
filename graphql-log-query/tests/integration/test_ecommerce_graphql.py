"""The C10 GraphQL surface, executed against the real PostgreSQL store.

Two things are proved here that nothing else can prove:

1. **The three list fields return exactly the oracle-predicted set.** Same discipline as
   ``test_graphql_query.py``: the expected rows are computed in Python from the generated corpus and
   the response is asserted to equal them — same rows, same order, same values.

2. **The interface is queryable, not decorative.** ``correlatedEvents`` is selected with inline
   fragments per implementor and the returned ``__typename`` mix is asserted. A schema-shape test
   can say the interface exists and is implemented by four types; only an executed query can say
   that graphql-core resolves the concrete type of every object the resolver returns. That
   resolution is the part that silently breaks — a resolver returning ORM rows, or dicts, or a
   Strawberry type registered under a different name, produces "Abstract type LogEvent must
   resolve to an Object type at runtime" **at execution time only**.

The interface section builds to the whole point of Feature Area A: one query, four ``__typename``s,
one round trip, over data that would take four REST calls to assemble. It is proved twice, and both
are needed — once by *creating* a log line carrying an order's trace id (what a real service does
inside a traced request), and once against the **seeded** corpus, on a trace that
:func:`~src.generators.order_traces_with_logs` names in advance. The second is what says the
correlation is a property of the corpus rather than of the test that just wrote the row.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx
import pytest
from strawberry.types import ExecutionResult

from src.config import Settings
from src.db.session import Database
from src.generators import EventCorpus
from src.graphql.context import Context
from src.graphql.errors import ErrorCode
from src.graphql.schema import schema
from tests.integration.corpus import CorrelatedCorpus, matching_events

#: Above every stream in the fixed corpus, inside the suite's raised ``max_query_limit`` — so a
#: full-stream comparison is a set equality rather than a prefix check.
LIMIT_ALL = 5_000

ORDER_EVENTS_DOCUMENT = """
query OrderEvents($filters: OrderEventFilterInput) {
  orderEvents(filters: $filters) {
    id
    timestamp
    service
    level
    traceId
    orderId
    userId
    status
    metadata
  }
}
"""

PAYMENT_EVENTS_DOCUMENT = """
query PaymentEvents($filters: PaymentEventFilterInput) {
  paymentEvents(filters: $filters) {
    id
    timestamp
    service
    level
    traceId
    orderId
    method
    outcome
    metadata
  }
}
"""

USER_EVENTS_DOCUMENT = """
query UserEvents($filters: UserEventFilterInput) {
  userEvents(filters: $filters) {
    id
    timestamp
    service
    level
    traceId
    userId
    activityType
    metadata
  }
}
"""

#: The interface selection. Note the four common fields are selected **off the interface** — no
#: fragment names them — which is what proves they are really interface fields and not four
#: coincidentally identical fields on four types.
CORRELATED_DOCUMENT = """
query Correlated($traceId: String!, $limit: Int) {
  correlatedEvents(traceId: $traceId, limit: $limit) {
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

CREATE_LOG_DOCUMENT = """
mutation Create($logData: CreateLogInput!) {
  createLog(logData: $logData) { id traceId }
}
"""


async def _execute(context: Context, document: str, **variables: Any) -> ExecutionResult:
    """Run one operation against the real schema with the test's context."""
    return await schema.execute(document, variable_values=variables or None, context_value=context)


async def _data(context: Context, document: str, **variables: Any) -> dict[str, Any]:
    """Run an operation and return ``data``, asserting the response carried no errors."""
    result = await _execute(context, document, **variables)

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result.data


def _timestamps(rows: list[dict[str, Any]]) -> list[datetime]:
    """The ``timestamp`` of each returned row, parsed back from the ``DateTime`` scalar."""
    return [datetime.fromisoformat(row["timestamp"]) for row in rows]


def _fulfilled_trace(corpus: EventCorpus) -> str:
    """The trace id of an order that reached DELIVERED.

    Picked deliberately rather than taking the first order: the shortest lifecycle
    (``("CREATED",)``) produces a handful of events with almost no stream interleaving, so a test
    asserting that the four streams are *merged* rather than concatenated would be asserting
    against the one shape that cannot tell the difference.
    """
    delivered = {
        record.trace_id for record in corpus.orders if record.status == "DELIVERED"
    }
    assert delivered, "the corpus must contain a fulfilled order"
    # Sorted so the choice is stable across runs rather than dependent on set iteration order.
    return sorted(trace for trace in delivered if trace is not None)[0]


# --- The three list fields, graded against the oracle ----------------------------------------------


async def test_order_events_returns_the_whole_stream_newest_first(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Every generated order event, in the database's order, with every published field intact.

    Graded field by field rather than by count: a resolver that returned the right number of rows
    with ``userId`` always null would pass a length assertion and break the C11 traversal that
    depends on it.
    """
    data = await _data(gql_context, ORDER_EVENTS_DOCUMENT, filters={"limit": LIMIT_ALL})
    rows = data["orderEvents"]
    expected = list(reversed(seeded_events.orders))

    assert len(rows) == len(expected)
    assert _timestamps(rows) == [record.timestamp for record in expected]
    assert [row["status"] for row in rows] == [record.status for record in expected]
    assert [row["orderId"] for row in rows] == [record.order_id for record in expected]
    assert [row["userId"] for row in rows] == [record.user_id for record in expected]
    assert [row["traceId"] for row in rows] == [record.trace_id for record in expected]
    assert [row["level"] for row in rows] == [record.level for record in expected]
    assert [row["metadata"] for row in rows] == [record.metadata for record in expected]


async def test_payment_events_returns_the_whole_stream_newest_first(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Method and outcome are published as enum **names**, which equal their stored values."""
    data = await _data(gql_context, PAYMENT_EVENTS_DOCUMENT, filters={"limit": LIMIT_ALL})
    rows = data["paymentEvents"]
    expected = list(reversed(seeded_events.payments))

    assert len(rows) == len(expected)
    assert [row["method"] for row in rows] == [record.method for record in expected]
    assert [row["outcome"] for row in rows] == [record.outcome for record in expected]
    assert [row["orderId"] for row in rows] == [record.order_id for record in expected]


async def test_user_events_returns_the_whole_stream_newest_first(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """``activityType`` is camel-cased on the wire and carries the enum member name."""
    data = await _data(gql_context, USER_EVENTS_DOCUMENT, filters={"limit": LIMIT_ALL})
    rows = data["userEvents"]
    expected = list(reversed(seeded_events.user_activity))

    assert len(rows) == len(expected)
    assert [row["activityType"] for row in rows] == [
        record.activity_type for record in expected
    ]
    assert [row["userId"] for row in rows] == [record.user_id for record in expected]


async def test_omitted_filters_are_ignored_and_null_filters_mean_the_same_thing(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Three spellings of "no filter" produce the identical result.

    A GraphQL client building a filter object from a form sends ``{"status": null}`` for an empty
    dropdown rather than omitting the key, so the two cases must not diverge — and with ``= None``
    defaults they structurally cannot, because there is no third state.
    """
    omitted = (await _data(gql_context, ORDER_EVENTS_DOCUMENT))["orderEvents"]
    explicit_null = (await _data(gql_context, ORDER_EVENTS_DOCUMENT, filters=None))["orderEvents"]
    all_nulls = (
        await _data(
            gql_context,
            ORDER_EVENTS_DOCUMENT,
            filters={"service": None, "status": None, "userId": None, "traceId": None},
        )
    )["orderEvents"]

    assert omitted == explicit_null == all_nulls
    # And the default limit applied, rather than the whole stream coming back.
    assert len(omitted) == min(len(seeded_events.orders), gql_context.settings.default_query_limit)


@pytest.mark.parametrize("status", ["CREATED", "DELIVERED", "CANCELLED"])
async def test_the_status_filter_returns_exactly_the_oracle_predicted_set(
    status: str, seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Enum in, exactly the matching rows out."""
    expected = matching_events(seeded_events.orders, lambda r: r.status == status)
    assert expected

    data = await _data(
        gql_context, ORDER_EVENTS_DOCUMENT, filters={"status": status, "limit": LIMIT_ALL}
    )
    rows = data["orderEvents"]

    assert [row["status"] for row in rows] == [status] * len(expected)
    assert _timestamps(rows) == [record.timestamp for record in expected]


async def test_filters_compose_across_dimensions(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Two dimensions AND together — the flat precursor to C11's multi-dimensional composition."""
    outcome = "DECLINED"
    method = next(
        record.method for record in seeded_events.payments if record.outcome == outcome
    )
    expected = matching_events(
        seeded_events.payments,
        lambda r: r.outcome == outcome and r.method == method,
    )
    assert expected

    data = await _data(
        gql_context,
        PAYMENT_EVENTS_DOCUMENT,
        filters={"outcome": outcome, "method": method, "limit": LIMIT_ALL},
    )
    rows = data["paymentEvents"]

    assert _timestamps(rows) == [record.timestamp for record in expected]
    assert len(expected) < len(
        matching_events(seeded_events.payments, lambda r: r.outcome == outcome)
    ), "the second filter must actually narrow the result or this proves nothing"


async def test_an_unknown_enum_member_is_rejected_during_validation(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """``status: SHIPED`` never reaches a resolver.

    This is the entire argument for the enums: as a plain string it would compile, produce
    ``WHERE status = 'SHIPED'``, and return an empty list — a legitimate answer to a valid question,
    so the client could not tell a typo from a quiet period.
    """
    result = await _execute(
        gql_context,
        "query { orderEvents(filters: {status: SHIPED}) { id } }",
    )

    assert result.errors
    assert "SHIPED" in str(result.errors[0])


async def test_a_blank_trace_id_is_a_typed_validation_error(gql_context: Context) -> None:
    """A coded ``VALIDATION_ERROR`` envelope, never a stack trace and never a 500."""
    result = await _execute(gql_context, CORRELATED_DOCUMENT, traceId="   ")

    assert result.errors
    assert result.errors[0].extensions is not None
    assert result.errors[0].extensions.get("code") == ErrorCode.VALIDATION_ERROR.value


async def test_the_limit_is_clamped_on_every_new_path(
    seeded_events: EventCorpus, database: Database
) -> None:
    """Spec §2 item 22 on the three list fields, observed through a deliberately small ceiling."""
    capped = Context(
        # `default_query_limit` has to come down with the ceiling: a default above
        # `MAX_QUERY_LIMIT` is refused by `Settings._check_limit_ordering`, because it could never
        # take effect.
        settings=Settings(
            _env_file=None,
            max_query_limit=5,
            default_query_limit=5,
            seed_entries=0,
            seed_orders=0,
        ),
        session_factory=database.session_factory,
        db=database,
    )

    orders = (
        await _data(capped, ORDER_EVENTS_DOCUMENT, filters={"limit": LIMIT_ALL})
    )["orderEvents"]
    payments = (
        await _data(capped, PAYMENT_EVENTS_DOCUMENT, filters={"limit": LIMIT_ALL})
    )["paymentEvents"]
    users = (
        await _data(capped, USER_EVENTS_DOCUMENT, filters={"limit": LIMIT_ALL})
    )["userEvents"]

    assert len(orders) == len(payments) == len(users) == 5


# --- The interface is queryable --------------------------------------------------------------------


async def test_correlated_events_returns_a_real_typename_mix(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """One trace, three concrete types, selected through inline fragments.

    The corpus guarantees the mix: an order's status events, its payment events and the acting
    user's activity all carry the same trace id. A resolver that returned only order events would
    satisfy every "the field returns rows" assertion and fail this one.
    """
    trace_id = seeded_events.orders[0].trace_id
    assert trace_id is not None

    rows = (
        await _data(gql_context, CORRELATED_DOCUMENT, traceId=trace_id, limit=LIMIT_ALL)
    )["correlatedEvents"]

    assert {row["__typename"] for row in rows} == {"OrderEvent", "PaymentEvent", "UserEvent"}

    expected_total = (
        len(matching_events(seeded_events.orders, lambda r: r.trace_id == trace_id))
        + len(matching_events(seeded_events.payments, lambda r: r.trace_id == trace_id))
        + len(matching_events(seeded_events.user_activity, lambda r: r.trace_id == trace_id))
    )
    assert len(rows) == expected_total


async def test_the_interface_fields_are_populated_on_every_member(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """The four common fields resolve for every concrete type, selected off the interface itself.

    No fragment names ``timestamp``/``service``/``level``/``traceId`` in the document, so this only
    passes if they really are interface fields that graphql-core resolved against each object's
    concrete type.
    """
    trace_id = seeded_events.orders[0].trace_id

    rows = (
        await _data(gql_context, CORRELATED_DOCUMENT, traceId=trace_id, limit=LIMIT_ALL)
    )["correlatedEvents"]

    assert rows
    for row in rows:
        assert row["traceId"] == trace_id
        assert row["timestamp"]
        assert row["service"]
        assert row["level"]


async def test_correlated_events_are_returned_newest_first(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """The four streams are merged into one ordering rather than concatenated.

    Concatenating would produce a well-formed response with every expected row in it — and a
    timeline a client cannot render, because the order events would all precede the payments
    regardless of when they happened.

    Run against a **fulfilled** order, whose timeline alternates between the three streams at least
    five times (user trail, CREATED, checkout, authorize, PAID, capture, PACKED/SHIPPED/DELIVERED).
    The shortest lifecycle would not distinguish a merge from a concatenation.
    """
    trace_id = _fulfilled_trace(seeded_events)

    rows = (
        await _data(gql_context, CORRELATED_DOCUMENT, traceId=trace_id, limit=LIMIT_ALL)
    )["correlatedEvents"]
    timestamps = _timestamps(rows)

    assert timestamps == sorted(timestamps, reverse=True)
    # Merged, not grouped: the concrete types must interleave rather than come in blocks.
    typenames = [row["__typename"] for row in rows]
    assert len(set(typenames)) == 3
    changes = sum(1 for a, b in zip(typenames, typenames[1:]) if a != b)
    assert changes >= 5, f"the streams look concatenated rather than merged: {typenames}"


async def test_type_specific_fields_come_back_under_their_own_fragments(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """``status`` appears only on ``OrderEvent`` rows, ``outcome`` only on ``PaymentEvent`` rows.

    GraphQL omits a fragment's fields from objects the fragment does not apply to, so this is what
    says the four types are genuinely distinct rather than one type wearing four names.
    """
    trace_id = seeded_events.orders[0].trace_id

    rows = (
        await _data(gql_context, CORRELATED_DOCUMENT, traceId=trace_id, limit=LIMIT_ALL)
    )["correlatedEvents"]

    for row in rows:
        if row["__typename"] == "OrderEvent":
            assert "status" in row and "outcome" not in row
        elif row["__typename"] == "PaymentEvent":
            assert "outcome" in row and "status" not in row
        elif row["__typename"] == "UserEvent":
            assert "activityType" in row and "status" not in row


async def test_a_log_line_sharing_the_trace_joins_the_same_correlated_timeline(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """**All four** implementors in one result — the reason ``LogEntry`` implements the interface.

    A ``createLog`` carrying the order's trace id is exactly what a real service does when it emits
    a log line inside a traced request, and the payoff is this: one query returns the log line, the
    order transitions, the payment outcomes and the user's actions together. Without ``LogEntry``
    on the interface the log line would be unreachable from here and the correlation id would only
    correlate three quarters of the system.
    """
    trace_id = seeded_events.orders[0].trace_id

    created = await _data(
        gql_context,
        CREATE_LOG_DOCUMENT,
        logData={
            "service": "order-service",
            "level": "INFO",
            "message": "fulfilment webhook accepted",
            "traceId": trace_id,
        },
    )
    assert created["createLog"]["traceId"] == trace_id

    rows = (
        await _data(gql_context, CORRELATED_DOCUMENT, traceId=trace_id, limit=LIMIT_ALL)
    )["correlatedEvents"]

    assert {row["__typename"] for row in rows} == {
        "LogEntry",
        "OrderEvent",
        "PaymentEvent",
        "UserEvent",
    }
    log_rows = [row for row in rows if row["__typename"] == "LogEntry"]
    assert [row["message"] for row in log_rows] == ["fulfilment webhook accepted"]


async def test_a_seeded_order_trace_returns_all_four_typenames(
    seeded_correlated: CorrelatedCorpus, gql_context: Context
) -> None:
    """The whole point of Feature Area A, on a trace chosen **by construction**.

    The test above proves the four-type mix by *creating* a log line with the order's trace id. This
    one proves the seeded corpus ships that way — one query returning the order's transitions, its
    payments, the acting user's actions **and** the service log lines emitted under the same
    correlation id, which is the "would take 3+ REST calls" claim in spec §3 Feature Area B.

    ``shared_traces[0]`` is ``order_id_for(0)``'s trace, named by
    :func:`~src.generators.order_traces_with_logs` rather than found by scanning the corpus for a
    trace that happens to have log rows. That distinction is the reason this test exists: the
    correlation used to be an accident of both generators running on one seed, so a scan was the
    only way to find such a trace — and a scan finds nothing at all under a different
    ``RANDOM_SEED`` or ``SEED_ENTRIES``, which would leave this claim untested and the
    ``... on LogEntry`` fragment matching nothing, with everything still green.

    Graded against both oracles, so a resolver that dropped one table's rows fails on the count
    rather than passing on the typename set.
    """
    trace_id = seeded_correlated.shared_traces[0]

    rows = (
        await _data(gql_context, CORRELATED_DOCUMENT, traceId=trace_id, limit=LIMIT_ALL)
    )["correlatedEvents"]

    assert {row["__typename"] for row in rows} == {
        "LogEntry",
        "OrderEvent",
        "PaymentEvent",
        "UserEvent",
    }

    events = seeded_correlated.events
    expected_total = (
        len([r for r in seeded_correlated.logs if r.trace_id == trace_id])
        + len([r for r in events.orders if r.trace_id == trace_id])
        + len([r for r in events.payments if r.trace_id == trace_id])
        + len([r for r in events.user_activity if r.trace_id == trace_id])
    )
    assert len(rows) == expected_total

    # Merged into one timeline, not concatenated per table. The log rows' timestamps are drawn
    # independently of the order's cluster — correlation here is by trace id, not by time — so a
    # resolver that returned each table's rows in a block would fail this ordering.
    timestamps = _timestamps(rows)
    assert timestamps == sorted(timestamps, reverse=True)
    assert all(row["traceId"] == trace_id for row in rows)


async def test_a_log_only_trace_still_answers_with_log_entries_alone(
    seeded_correlated: CorrelatedCorpus, gql_context: Context
) -> None:
    """Not every log trace joins an order, and that population is load-bearing too.

    C5's ``relatedLogs`` needs traces that only log rows carry, so the correlation above is
    deliberately a *quarter* of the orders rather than all of them. Asserted against a store that
    holds both corpora — the same query on the same field, one trace over, returns a single
    ``__typename``.
    """
    trace_id = seeded_correlated.log_only_trace()

    rows = (
        await _data(gql_context, CORRELATED_DOCUMENT, traceId=trace_id, limit=LIMIT_ALL)
    )["correlatedEvents"]

    assert rows
    assert {row["__typename"] for row in rows} == {"LogEntry"}
    assert len(rows) == len(
        [record for record in seeded_correlated.logs if record.trace_id == trace_id]
    )


async def test_an_unknown_trace_returns_an_empty_list_rather_than_an_error(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """No correlated events is an ordinary answer, and the list is empty rather than null."""
    rows = (
        await _data(gql_context, CORRELATED_DOCUMENT, traceId="0" * 32, limit=LIMIT_ALL)
    )["correlatedEvents"]

    assert rows == []


# --- The core surface is unaffected ----------------------------------------------------------------


async def test_the_spec_acceptance_command_still_returns_log_entries(
    seeded: list[Any], gql_context: Context
) -> None:
    """``{ logs { id service level message } }`` executed, not merely validated.

    ``LogEntry`` gained a base class in C10. The schema-shape suite proves the document still
    *validates*; this proves it still *runs* against the real store and returns the rows it always
    did — which is the acceptance criterion the whole project is graded on.
    """
    data = await _data(gql_context, "{ logs { id service level message } }")

    rows = data["logs"]
    assert len(rows) == gql_context.settings.default_query_limit
    assert all(set(row) == {"id", "service", "level", "message"} for row in rows)


async def test_a_log_entry_can_be_selected_through_the_interface_by_a_client(
    seeded: list[Any], gql_context: Context
) -> None:
    """``... on LogEntry`` works on a trace that has only log entries — no event rows needed.

    The degenerate half of the mix test above: an interface implemented by four types must still
    behave correctly when only one of them has anything to say.
    """
    traced = next(record for record in seeded if record.trace_id is not None)

    rows = (
        await _data(
            gql_context, CORRELATED_DOCUMENT, traceId=traced.trace_id, limit=LIMIT_ALL
        )
    )["correlatedEvents"]

    assert rows
    assert {row["__typename"] for row in rows} == {"LogEntry"}
    assert all(row["traceId"] == traced.trace_id for row in rows)


# --- Over HTTP, through the assembled application --------------------------------------------------


async def test_the_new_fields_are_reachable_over_the_real_http_endpoint(
    http_client: httpx.AsyncClient,
) -> None:
    """The router mount, the ``context_getter`` dependency and the JSON envelope all work.

    ``schema.execute`` touches none of those, so a suite with only the tests above would stay green
    with the API unmounted. The seeded corpus here is whatever the lifespan wrote, so the assertion
    is on the *shape* of the envelope rather than on specific rows — the oracle comparisons belong
    to the deterministic fixtures above.
    """
    response = await http_client.post(
        "/graphql",
        json={
            "query": (
                "query { orderEvents(filters: {limit: 3}) { id orderId status } "
                "paymentEvents(filters: {limit: 3}) { id outcome } "
                "userEvents(filters: {limit: 3}) { id activityType } }"
            )
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body, body
    assert set(body["data"]) == {"orderEvents", "paymentEvents", "userEvents"}
    for key in body["data"]:
        assert isinstance(body["data"][key], list)


async def test_the_interface_query_works_over_http(
    http_client: httpx.AsyncClient,
) -> None:
    """An inline-fragment selection survives the HTTP transport and the error masking."""
    response = await http_client.post(
        "/graphql",
        json={
            "query": CORRELATED_DOCUMENT,
            "variables": {"traceId": "no-such-trace", "limit": 5},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert "errors" not in body, body
    assert body["data"]["correlatedEvents"] == []
