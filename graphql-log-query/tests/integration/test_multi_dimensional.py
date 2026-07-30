"""Spec §3 Feature Area B, executed against the real store.

Three requirements, one module:

1. **"A single GraphQL query returns data that would require 3 or more REST calls."** The flagship
   dossier below. The claim is made *checkable* rather than rhetorical — the REST calls it replaces
   are enumerated verbatim next to the document, and the test asserts the whole thing arrives in one
   operation and four SQL statements.
2. **"Nested resolution: an order query can traverse to its user and payment events in the same
   request."** Every nested list is graded against the **generator oracle** — the order's payments
   really are that order's, the user activity really is that user's, the correlated log lines really
   share that trace. Grading against the API's own other fields would be a tautology: "the payments
   all carry the parent's orderId" passes against a resolver that returns one arbitrary payment and
   drops the rest, and against one that returns the *same* group to every parent.
3. **"Filters compose across dimensions simultaneously."** Event type + status + time window + user
   attribute in one query, asserted to equal the oracle-predicted intersection **and** to be
   strictly narrower than each of its dimensions taken alone. The second half is what makes the
   first non-vacuous: a resolver that ignored two of the four filters would still return a set that
   "matches the filters it applied".
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

import httpx
import pytest
from strawberry.types import ExecutionResult

from src.db.models import OrderEventRecord
from src.db.session import Database
from src.generators import EventCorpus
from src.graphql.context import Context
from src.graphql.schema import schema
from tests.integration.corpus import CorrelatedCorpus, count_statements, matching, matching_events

# =================================================================================================
# THE FLAGSHIP — and the REST calls it collapses, written down so the claim can be checked
#
# Against a conventional REST API, assembling this screen takes FOUR requests, and three of them
# cannot even be ISSUED until the first has come back — the client does not know the userId or the
# traceId until it has read the order:
#
#   1. GET /orders/{orderId}/events            -> the order's status transitions
#   2. GET /orders/{orderId}/payments          -> its payment attempts        (needs #1's orderId)
#   3. GET /users/{userId}/activity            -> the buyer's activity trail  (needs #1's userId)
#   4. GET /traces/{traceId}/logs              -> the raw log output          (needs #1's traceId)
#
# That serialisation is the real cost: four round trips at three levels of dependency, each paying
# full latency, and the client assembling the join itself. The document below is ONE request, and
# the server resolves it in FOUR SQL statements no matter how many orders match — which is the
# `test_the_flagship_dossier_is_one_request_and_four_statements` assertion.
#
# It also over-fetches nothing, which the REST version cannot avoid: each of those endpoints returns
# whole resources, while this returns exactly the fields selected.
# =================================================================================================

FLAGSHIP = """
query OrderDossier($orderId: String!) {
  orderEvents(filters: {orderId: $orderId, limit: 10}) {
    id
    timestamp
    status
    orderId
    userId
    traceId
    payments { id timestamp orderId method outcome }
    userActivity { id timestamp userId activityType }
    relatedLogs { id timestamp traceId message }
  }
}
"""

#: The multi-dimensional filter, as a variable-driven document so the same text can be reused for
#: each single-dimension control below. Every dimension is optional, so a control simply omits the
#: ones it is not testing.
COMPOSED_DOCUMENT = """
query Composed($filters: OrderEventFilterInput!) {
  orderEvents(filters: $filters) {
    id
    timestamp
    orderId
    userId
    status
    level
  }
}
"""

#: Wide enough to hold any single stream of the fixed corpus, inside the suite's raised ceiling.
LIMIT_ALL = 5_000

#: The same "don't clip the result" ceiling, for documents that traverse TWO nested edges.
#:
#: It has to be its own number because the cost model multiplies rather than adds: a parent page
#: of P with two traversals under it prices at roughly ``P × 221``, so ``LIMIT_ALL`` (5,000) costs
#: 1,105,010 and is refused by the suite's own 980,000 ceiling — a test failure that reads like a
#: batching bug and is really an over-wide `limit`. Note the request is never clamped down to it
#: either: `tests/integration/conftest.py` raises MAX_QUERY_LIMIT for the oracle-scale documents,
#: so 5,000 is priced in full rather than as 500.
#:
#: 400 is chosen against the fixture corpus (120 orders → 348 order events), so it cannot clip any
#: filtered page, and prices at 88,410 — an order of magnitude inside the ceiling.
LIMIT_ALL_TWO_TRAVERSALS = 400


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


def _correlated_order(corpus: CorrelatedCorpus) -> OrderEventRecord:
    """An order whose trace **is declared** to carry log lines, so all four legs return data.

    Declared rather than discovered: :meth:`CorrelatedCorpus.shared_traces` reads
    :func:`~src.generators.order_traces_with_logs`, so the choice cannot silently land on an
    accidental collision, and a corpus that stopped correlating the two halves would fail here as a
    missing precondition rather than as a confusing empty list three assertions later.
    """
    traces = set(corpus.shared_traces)
    assert traces, "the corpus must declare order traces that also carry log lines"
    for record in corpus.events.orders:
        if record.trace_id in traces:
            return record
    raise AssertionError("no order event carries a declared shared trace")


# =================================================================================================
# Feature Area B — the 3-in-1 query
# =================================================================================================


async def test_the_flagship_dossier_returns_nested_data_that_matches_the_generator_oracle(
    seeded_correlated: CorrelatedCorpus, gql_context: Context
) -> None:
    """The order's payments, its buyer's activity and its trace's log lines — all three, all right.

    Every one of the three nested lists is compared against the corpus that was seeded, computed
    independently in Python. That is what distinguishes this from a test that would pass against a
    resolver handing **the same group to every parent**: the oracle knows which payments belong to
    *this* order, and it knows the buyer's activity spans their *other* orders too.
    """
    anchor = _correlated_order(seeded_correlated)
    corpus = seeded_correlated.events

    rows = (
        await _data(gql_context, FLAGSHIP, orderId=anchor.order_id)
    )["orderEvents"]

    expected_events = matching_events(corpus.orders, lambda r: r.order_id == anchor.order_id)
    assert rows, "the anchor order must have events"
    assert _timestamps(rows) == [record.timestamp for record in expected_events]

    expected_payments = matching_events(corpus.payments, lambda r: r.order_id == anchor.order_id)
    expected_activity = matching_events(
        corpus.user_activity, lambda r: r.user_id == anchor.user_id
    )
    expected_logs = matching(seeded_correlated.logs, lambda r: r.trace_id == anchor.trace_id)

    assert expected_payments, "the generator gives every order at least one payment event"
    assert expected_activity, "the generator gives every order's user an activity trail"
    assert expected_logs, (
        "the anchor trace is DECLARED to carry log lines; an empty oracle here means the corpus "
        "and the seeder have drifted apart, not that the resolver is wrong"
    )

    for row in rows:
        # Payments: exactly this order's stream, newest first. Both the instants and the outcomes,
        # because two payment events of one order can differ only in `outcome`.
        assert _timestamps(row["payments"]) == [r.timestamp for r in expected_payments]
        assert [p["outcome"] for p in row["payments"]] == [r.outcome for r in expected_payments]
        assert {p["orderId"] for p in row["payments"]} == {anchor.order_id}

        # User activity: everything this buyer did, across ALL their orders — which is why the
        # oracle filters the whole corpus by user_id rather than by this order's trace.
        assert _timestamps(row["userActivity"]) == [r.timestamp for r in expected_activity]
        assert {a["userId"] for a in row["userActivity"]} == {anchor.user_id}

        # Correlated log lines: the fourth leg, and the one that spans the two seeded corpora.
        assert _timestamps(row["relatedLogs"]) == [r.timestamp for r in expected_logs]
        assert [entry["message"] for entry in row["relatedLogs"]] == [
            record.message for record in expected_logs
        ]
        assert {entry["traceId"] for entry in row["relatedLogs"]} == {anchor.trace_id}


async def test_the_flagship_dossier_is_one_request_and_four_statements(
    seeded_correlated: CorrelatedCorpus, gql_context: Context, database: Database
) -> None:
    """The "3+ REST calls" claim, measured: one operation, four statements, four tables.

    The number four is the count of **tables**, not of parents and not of REST endpoints — which is
    the whole point. The REST assembly at the top of this module is four *round trips* at three
    levels of dependency; this is four *statements* inside one round trip, and it stays four however
    many order events the filter matches.
    """
    anchor = _correlated_order(seeded_correlated)

    with count_statements(database.engine) as counter:
        rows = (await _data(gql_context, FLAGSHIP, orderId=anchor.order_id))["orderEvents"]

    assert rows
    assert all(row["payments"] and row["userActivity"] and row["relatedLogs"] for row in rows), (
        "every leg of the dossier must have returned data, or the statement count below is "
        "measuring a query that did nothing"
    )

    assert counter.count("order_events") == 1, counter.report()
    assert counter.count("payment_events") == 1, counter.report()
    assert counter.count("user_events") == 1, counter.report()
    assert counter.count("log_entries") == 1, counter.report()
    assert len(counter) == 4, (
        f"the four-in-one dossier must cost exactly four statements:\n{counter.report()}"
    )


async def test_two_orders_in_one_page_get_their_own_payments_and_their_own_buyers(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """The alignment failure, observed through the published API rather than through a loader.

    A batched load function that returned the right number of groups in the wrong order gives every
    parent somebody else's rows — and the response still looks entirely plausible. This picks two
    orders placed by **different** users and asserts each row got its own, which is the assertion
    that fails when the alignment is wrong and passes for the right reason when it is not.
    """
    by_order: dict[str, OrderEventRecord] = {}
    for record in seeded_events.orders:
        by_order.setdefault(record.order_id, record)

    first, second = None, None
    for record in by_order.values():
        if first is None:
            first = record
        elif record.user_id != first.user_id:
            second = record
            break
    assert first is not None and second is not None, "the corpus must contain two distinct buyers"

    document = """
    query Pair($a: String!, $b: String!) {
      a: orderEvents(filters: {orderId: $a, limit: 5}) {
        orderId userId payments { orderId } userActivity { userId }
      }
      b: orderEvents(filters: {orderId: $b, limit: 5}) {
        orderId userId payments { orderId } userActivity { userId }
      }
    }
    """
    data = await _data(gql_context, document, a=first.order_id, b=second.order_id)

    for alias, expected in (("a", first), ("b", second)):
        rows = data[alias]
        assert rows, alias
        for row in rows:
            assert row["orderId"] == expected.order_id
            assert {payment["orderId"] for payment in row["payments"]} == {expected.order_id}
            assert {activity["userId"] for activity in row["userActivity"]} == {expected.user_id}


async def test_a_payment_traverses_back_up_to_its_order(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """``PaymentEvent.order`` is the newest transition of the order the payment names.

    Graded against the oracle's own idea of "newest": the last order event the generator emitted for
    that id. A resolver that returned the *oldest* — an ``ORDER BY timestamp`` with the direction
    reversed — would return a real, plausible, wrong row (always ``CREATED``), and a test that only
    checked ``order.orderId == payment.orderId`` would never notice.
    """
    payment = seeded_events.payments[0]
    newest = [r for r in seeded_events.orders if r.order_id == payment.order_id][-1]

    rows = (
        await _data(
            gql_context,
            """
            query One($orderId: String!) {
              paymentEvents(filters: {orderId: $orderId, limit: 10}) {
                orderId
                order { orderId status timestamp }
              }
            }
            """,
            orderId=payment.order_id,
        )
    )["paymentEvents"]

    assert rows
    for row in rows:
        assert row["order"]["orderId"] == payment.order_id
        assert row["order"]["status"] == newest.status
        assert datetime.fromisoformat(row["order"]["timestamp"]) == newest.timestamp


async def test_the_dossier_works_over_the_real_http_endpoint(
    seeded_correlated: CorrelatedCorpus, http_client: httpx.AsyncClient
) -> None:
    """One POST, one JSON body, the whole dossier — which is the client-visible claim.

    ``schema.execute`` proves the resolvers; only a real request proves the router mount, the
    ``context_getter`` dependency and the GraphQL envelope. The three nested lists are what a REST
    client would have paid three extra round trips for, and here they are keys in one response.
    """
    anchor = _correlated_order(seeded_correlated)

    response = await http_client.post(
        "/graphql", json={"query": FLAGSHIP, "variables": {"orderId": anchor.order_id}}
    )

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload, payload.get("errors")

    rows = payload["data"]["orderEvents"]
    assert rows
    for row in rows:
        assert row["payments"], "the payments leg came back empty over HTTP"
        assert row["userActivity"], "the user-activity leg came back empty over HTTP"
        assert row["relatedLogs"], "the correlated-logs leg came back empty over HTTP"


# =================================================================================================
# Feature Area B — filters composing across dimensions SIMULTANEOUSLY
# =================================================================================================


def _composition_probe(corpus: EventCorpus) -> tuple[str, str, datetime, datetime]:
    """Pick ``(status, user_id, start, end)`` that genuinely narrows on all three dimensions.

    Derived from the corpus rather than hardcoded, so the probe cannot become vacuous if the
    generator's mix shifts: the busiest ``(status, user)`` pair is chosen, and the window is then
    clipped to drop that pair's oldest and newest events — which guarantees the time bound removes
    something rather than merely being present in the request.
    """
    status = "PAID"
    with_status = [record for record in corpus.orders if record.status == status]
    assert with_status, "the corpus must contain PAID transitions"

    user_id = Counter(record.user_id for record in with_status).most_common(1)[0][0]
    candidates = [record for record in with_status if record.user_id == user_id]
    assert len(candidates) >= 4, (
        "the busiest (status, user) pair must have at least four events, or clipping the window "
        "at both ends would leave nothing to compare"
    )

    # `corpus.orders` is sorted oldest-first, so `candidates` is too.
    return status, user_id, candidates[1].timestamp, candidates[-2].timestamp


async def test_four_dimensions_compose_into_exactly_the_oracle_predicted_intersection(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Event type + status + time window + user attribute, in ONE query — spec §3 Feature Area B.

    The four dimensions the spec names, applied simultaneously: the *event type* is which stream is
    queried (``orderEvents``), then ``status``, then ``startTime``/``endTime``, then ``userId``.

    The expectation is the intersection computed in Python from the generated corpus, compared as an
    ordered list of instants. Asserting "every returned row has status PAID and userId X" would be a
    tautology — it passes against an implementation that returns one matching row and silently drops
    the other forty, which is exactly what a builder that ANDed its predicates wrongly would do.
    """
    status, user_id, start, end = _composition_probe(seeded_events)

    expected = matching_events(
        seeded_events.orders,
        lambda record: (
            record.status == status
            and record.user_id == user_id
            and start <= record.timestamp <= end
        ),
    )
    assert expected, "the probe must select a non-empty intersection"

    rows = (
        await _data(
            gql_context,
            COMPOSED_DOCUMENT,
            filters={
                "status": status,
                "userId": user_id,
                "startTime": start.isoformat(),
                "endTime": end.isoformat(),
                "limit": LIMIT_ALL,
            },
        )
    )["orderEvents"]

    assert _timestamps(rows) == [record.timestamp for record in expected]
    assert [row["orderId"] for row in rows] == [record.order_id for record in expected]
    assert {row["status"] for row in rows} == {status}
    assert {row["userId"] for row in rows} == {user_id}


@pytest.mark.parametrize("dropped", ["status", "userId", "window"])
async def test_the_composition_is_strictly_narrower_than_each_dimension_alone(
    dropped: str, seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Removing any one dimension returns **more** rows — so every one of them is really applied.

    This is the half that makes the equality test above non-vacuous. A resolver that quietly
    ignored ``userId`` would still return "rows that match the filters it applied", and the oracle
    comparison would catch it only because the oracle applies all four. Here the failure is direct:
    if a dimension is being ignored, dropping it changes nothing and the strict inequality fails.

    Note ``>`` and not ``>=``. A non-strict comparison would pass against a probe that happened to
    select the whole stream, which is precisely the vacuous case this exists to exclude.
    """
    status, user_id, start, end = _composition_probe(seeded_events)

    full = {
        "status": status,
        "userId": user_id,
        "startTime": start.isoformat(),
        "endTime": end.isoformat(),
        "limit": LIMIT_ALL,
    }
    relaxed = dict(full)
    if dropped == "window":
        relaxed.pop("startTime")
        relaxed.pop("endTime")
    else:
        relaxed.pop(dropped)

    composed = (await _data(gql_context, COMPOSED_DOCUMENT, filters=full))["orderEvents"]
    widened = (await _data(gql_context, COMPOSED_DOCUMENT, filters=relaxed))["orderEvents"]

    assert composed, "the composed filter must select something"
    assert len(widened) > len(composed), (
        f"dropping {dropped!r} returned {len(widened)} rows where the full composition returned "
        f"{len(composed)}; a dimension that changes nothing when removed is a dimension that was "
        "never applied"
    )
    # And the narrow result really is a subset of the wide one, rather than a different set of the
    # same size — which is what a predicate applied to the wrong column would produce.
    assert {row["id"] for row in composed} <= {row["id"] for row in widened}


async def test_composing_a_dimension_onto_a_nested_traversal_still_batches(
    seeded_events: EventCorpus, gql_context: Context, database: Database
) -> None:
    """A filtered parent page still costs one statement per table, not one per surviving parent.

    Worth asserting separately from the unfiltered case: a narrower ``WHERE`` returns fewer parents,
    so an N+1 implementation looks *faster* rather than broken, and a statement-count test written
    only against the wide page could stay green while the filtered path regressed.
    """
    status, user_id, start, end = _composition_probe(seeded_events)

    document = """
    query FilteredTraversal($filters: OrderEventFilterInput!) {
      orderEvents(filters: $filters) {
        id
        payments { id }
        userActivity { id }
      }
    }
    """
    with count_statements(database.engine) as counter:
        rows = (
            await _data(
                gql_context,
                document,
                filters={
                    "status": status,
                    "userId": user_id,
                    "startTime": start.isoformat(),
                    "endTime": end.isoformat(),
                    "limit": LIMIT_ALL_TWO_TRAVERSALS,
                },
            )
        )["orderEvents"]

    assert len(rows) >= 2, "the probe must leave at least two parents, or batching is unobservable"
    assert all(row["payments"] for row in rows)
    assert len(counter) == 3, (
        f"a filtered page traversing two edges must cost three statements:\n{counter.report()}"
    )
