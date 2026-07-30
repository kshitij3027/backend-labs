"""The N+1 proof extended to every entity — spec §3 Feature Area D, and §5's "DataLoader prevents
N+1 queries".

C5 proved it for one edge. This module proves it for the traversal spec §3 Feature Area B actually
asks for: **an order, its user's activity, its payments and its correlated log lines, in one
request.** Written the obvious way that is ``1 + 3N`` statements — a hundred orders is three hundred
and one round trips — and it returns byte-identical JSON either way, which is why every assertion
that matters here is a **count of the statements PostgreSQL was really sent**.

.. rubric:: The two rules the counting tests follow, both learned from tests that proved nothing

* **The expected count is exact, not "small".** ``<= 10`` passes against a loader that batches in
  pairs.
* **The count must not move when N doubles.** A fixed number is what "batched" means; a number that
  grows with N is an N+1 wearing a smaller constant. So the headline test measures at N=25 and N=50
  and asserts the two counts are **equal to each other**, not merely each below a ceiling.

.. rubric:: And the alignment half, which is the failure that does not raise

A load function returning the right *number* of results in the wrong *order* is accepted silently
and hands every parent somebody else's rows. The unit suite exercises that against the pure
functions with deliberately awkward batches; this module exercises it against what PostgreSQL
really returns, ordering and all — because only here can the rows be real.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from strawberry.types import ExecutionResult

from src.config import Settings
from src.db.session import Database
from src.generators import EventCorpus
from src.graphql.context import Context
from src.graphql.loaders import LoaderRegistry
from src.graphql.schema import schema
from tests.integration.corpus import CorrelatedCorpus, count_statements, matching_events

#: THE document. A page of order events, each traversing to its payments, its user's activity and
#: the log lines sharing its trace — one field selected under each, so the cost stays inside the
#: shipped budget at both page sizes and the statement count is the only thing under test.
#:
#: Priced (see tests/unit/test_ecommerce_cost.py for the model): 8,360 at limit 25 and 16,710 at
#: limit 50, both under MAX_QUERY_COMPLEXITY = 25,000. That is deliberate — a document the shipped
#: gate would refuse could not also be the document that demonstrates the feature.
TRAVERSAL_DOCUMENT = """
query Traversal($limit: Int!) {
  orderEvents(filters: {limit: $limit}) {
    id
    orderId
    userId
    traceId
    payments { id }
    userActivity { id }
    relatedLogs { id }
  }
}
"""

#: Every published field on both levels, for the tests that grade against the generator oracle. A
#: projection that omitted `timestamp` could not check the ordering, and one that omitted the
#: business key could not check that the rows belong to the parent that received them.
DOSSIER_DOCUMENT = """
query Dossier($filters: OrderEventFilterInput) {
  orderEvents(filters: $filters) {
    id
    timestamp
    orderId
    userId
    traceId
    payments { id timestamp orderId method outcome }
    userActivity { id timestamp userId activityType }
    relatedLogs { id timestamp traceId message }
  }
}
"""

BY_ID_DOCUMENT = """
query ByIds($a: ID!, $b: ID!, $missing: ID!) {
  a: orderEvent(id: $a) { id orderId }
  b: orderEvent(id: $b) { id orderId }
  missing: orderEvent(id: $missing) { id orderId }
}
"""

PAYMENT_TO_ORDER_DOCUMENT = """
query PaymentOrders($limit: Int!) {
  paymentEvents(filters: {limit: $limit}) {
    id
    orderId
    order { id orderId status timestamp }
  }
}
"""

#: The index each batched predicate depends on. Read out of `pg_indexes` rather than asserted in a
#: comment, because "there is an index for this" is exactly the claim that rots silently: dropping
#: one turns a batched lookup into one sequential scan per operation, which is still ONE statement
#: and therefore invisible to every count in this file.
REQUIRED_INDEXES: dict[str, tuple[str, ...]] = {
    "order_events": (
        "ix_order_events_order_ts",
        "ix_order_events_user_ts",
        "ix_order_events_trace_id",
    ),
    "payment_events": ("ix_payment_events_order_ts", "ix_payment_events_trace_id"),
    "user_events": ("ix_user_events_user_ts", "ix_user_events_trace_id"),
}


async def _execute(context: Context, document: str, **variables: Any) -> ExecutionResult:
    """Run one operation against the real schema, asserting it produced no errors."""
    result = await schema.execute(
        document, variable_values=variables or None, context_value=context
    )
    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result


# =================================================================================================
# THE HEADLINE: a fixed statement count, whatever N is
# =================================================================================================


@pytest.mark.parametrize("limit", [25, 50])
async def test_a_full_traversal_over_n_orders_costs_exactly_four_statements(
    limit: int,
    seeded_correlated: CorrelatedCorpus,
    gql_context: Context,
    database: Database,
) -> None:
    """One statement per **table**, not one per parent — spec §3 Feature Area D.

    Four: the order events themselves, then one batched ``WHERE order_id IN (...)`` for every
    payment, one ``WHERE user_id IN (...)`` for every activity, and one
    ``WHERE trace_id IN (...)`` for every correlated log line. The unbatched implementation of the
    same document is ``1 + 3N`` — 151 statements at N=50 — and produces identical JSON.

    Each count is asserted **per table** as well as in total, because a bare total of four could
    also be reached by a loader that batched two tables and forgot the third while another issued
    two statements. The per-table breakdown says which four.
    """
    with count_statements(database.engine) as counter:
        result = await _execute(gql_context, TRAVERSAL_DOCUMENT, limit=limit)

    rows = result.data["orderEvents"]
    assert len(rows) == limit

    # The generator guarantees both of these for every order it emits — every order has at least
    # one payment event, and the acting user always logs in, browses and adds to cart. So `all`
    # rather than `any`: a resolver returning [] for some parents would be a real failure here, not
    # a thin corpus.
    assert all(row["payments"] for row in rows), (
        "some orders came back with no payments; every generated order has at least one payment "
        "event, so this is the traversal failing rather than the corpus being sparse"
    )
    assert all(row["userActivity"] for row in rows), "some orders came back with no user activity"

    assert counter.count("order_events") == 1, (
        f"the parent read must be ONE statement:\n{counter.report()}"
    )
    assert counter.count("payment_events") == 1, (
        f"every order's payments must arrive in ONE batched read:\n{counter.report()}"
    )
    assert counter.count("user_events") == 1, (
        f"every order's user activity must arrive in ONE batched read:\n{counter.report()}"
    )
    assert counter.count("log_entries") == 1, (
        f"every order's correlated log lines must arrive in ONE batched read:\n{counter.report()}"
    )
    assert len(counter) == 4, (
        f"expected exactly four statements for a four-table traversal, got {len(counter)}:\n"
        f"{counter.report()}"
    )

    # The shape of each batch, not just its count: a loader that issued one statement per key and
    # happened to be called once would satisfy the counts above at N=1 and nothing here.
    assert counter.count("payment_events.order_id in") == 1, counter.report()
    assert counter.count("user_events.user_id in") == 1, counter.report()
    assert counter.count("log_entries.trace_id in") == 1, counter.report()


async def test_the_statement_count_does_not_move_when_the_order_count_doubles(
    seeded_correlated: CorrelatedCorpus, gql_context: Context, database: Database
) -> None:
    """The same measurement at N and 2N, compared **to each other**.

    The test above pins the number; this one pins the shape of the curve, which is the property the
    requirement is actually about. An implementation that issued one statement per five parents
    would satisfy "a small number" at N=25 and fail here — and so would a per-key loop that
    happened to sit under whatever ceiling a single-N test wrote down.
    """
    with count_statements(database.engine) as small:
        await _execute(gql_context, TRAVERSAL_DOCUMENT, limit=25)
    with count_statements(database.engine) as large:
        await _execute(gql_context, TRAVERSAL_DOCUMENT, limit=50)

    assert len(small) == len(large) == 4, (
        f"25 orders cost {len(small)} statements and 50 cost {len(large)}; batching means this "
        f"number does not depend on N.\n--- 25 ---\n{small.report()}\n--- 50 ---\n{large.report()}"
    )


async def test_a_payment_page_reaching_back_to_its_orders_costs_two_statements(
    seeded_events: EventCorpus, gql_context: Context, database: Database
) -> None:
    """``PaymentEvent.order`` is the single-valued edge, and it batches like the list ones.

    Worth its own test because it is the field a naive implementation is most likely to write as a
    per-parent ``SELECT ... ORDER BY timestamp DESC LIMIT 1`` — which is correct, cheap-looking, and
    one round trip per payment on the page. Here it is the head of a batched group read, so a page
    of payments costs one statement for the payments and one for every order behind them.
    """
    with count_statements(database.engine) as counter:
        result = await _execute(gql_context, PAYMENT_TO_ORDER_DOCUMENT, limit=40)

    rows = result.data["paymentEvents"]
    assert len(rows) == 40
    assert all(row["order"] is not None for row in rows), (
        "every generated payment belongs to an order that has events, so a null here is the "
        "traversal failing rather than the corpus"
    )
    assert all(row["order"]["orderId"] == row["orderId"] for row in rows), (
        "a payment was handed another order's newest event — the positional-alignment failure"
    )

    assert counter.count("payment_events") == 1, counter.report()
    assert counter.count("order_events") == 1, counter.report()
    assert len(counter) == 2, counter.report()


async def test_several_by_id_aliases_batch_into_one_statement(
    seeded_events: EventCorpus, gql_context: Context, database: Database
) -> None:
    """``{ a: orderEvent(id:…) b: orderEvent(id:…) … }`` is one ``WHERE id IN (…)``.

    The reason the by-id loaders exist at all: one lookup does not need batching, but a *document*
    is not one lookup. A client hydrating a list of ids writes exactly this, and every alias is a
    separate resolver call.

    The deliberate miss in the batch is doing two jobs: it proves a missing id is ``null`` with no
    ``errors`` entry, and it proves the alignment survives a batch that is shorter than its key
    list — which is the case that shifts every later answer by one when it is mishandled.
    """
    ids = [str(index) for index in (1, 2)]
    missing = str(10 ** 9)

    with count_statements(database.engine) as counter:
        result = await _execute(gql_context, BY_ID_DOCUMENT, a=ids[0], b=ids[1], missing=missing)

    data = result.data
    assert data["a"]["id"] == ids[0]
    assert data["b"]["id"] == ids[1]
    assert data["missing"] is None

    assert counter.count("order_events") == 1, (
        f"three aliases must collapse into one batched read:\n{counter.report()}"
    )


async def test_correlated_events_still_costs_four_statements_after_being_routed_through_loaders(
    seeded_correlated: CorrelatedCorpus, gql_context: Context, database: Database
) -> None:
    """C11 moved ``correlatedEvents`` onto the trace loaders; the statement count did not move.

    It was four flat SELECTs and it is four batched ones. What changed is that a document naming
    **several** traces under aliases now costs four in total rather than four per alias — which is
    what the second half of this test measures. Without the loaders the two counts below would be
    four and twelve.
    """
    traces = list(seeded_correlated.shared_traces[:3])
    assert len(traces) == 3, "the corpus must declare at least three order traces carrying logs"

    with count_statements(database.engine) as one_trace:
        await _execute(
            gql_context,
            "query One($t: String!) { correlatedEvents(traceId: $t) { __typename } }",
            t=traces[0],
        )

    with count_statements(database.engine) as three_traces:
        await _execute(
            gql_context,
            """
            query Three($a: String!, $b: String!, $c: String!) {
              a: correlatedEvents(traceId: $a) { __typename }
              b: correlatedEvents(traceId: $b) { __typename }
              c: correlatedEvents(traceId: $c) { __typename }
            }
            """,
            a=traces[0],
            b=traces[1],
            c=traces[2],
        )

    assert len(one_trace) == 4, one_trace.report()
    assert len(three_traces) == 4, (
        f"three correlated selections must still cost four statements, got {len(three_traces)}:\n"
        f"{three_traces.report()}"
    )


# =================================================================================================
# Positional alignment, against what PostgreSQL really returns
# =================================================================================================


@pytest.mark.parametrize(
    ("loader_name", "stream", "key_of"),
    [
        ("order_events_by_order_id", "orders", "order_id"),
        ("order_events_by_user_id", "orders", "user_id"),
        ("payment_events_by_order_id", "payments", "order_id"),
        ("user_events_by_user_id", "user_activity", "user_id"),
    ],
)
async def test_a_shuffled_batch_with_misses_and_duplicates_stays_aligned(
    loader_name: str,
    stream: str,
    key_of: str,
    seeded_events: EventCorpus,
    database: Database,
    session: AsyncSession,
    db_settings: Settings,
) -> None:
    """One batch, awkward keys, and every group answered at **its own** position.

    Driven through the registry rather than through a document, because a GraphQL operation cannot
    ask for a key that does not exist or ask for the same one twice in a controlled order — and
    those are precisely the cases where a load function that returns "the right number of results
    in the wrong order" hands every parent somebody else's rows.

    Every group is graded against the **generator oracle**, not against the API's own other fields:
    "the payments all have the parent's orderId" is a tautology that a resolver returning one
    arbitrary member of the group would satisfy.
    """
    records = getattr(seeded_events, stream)
    keys = sorted({getattr(record, key_of) for record in records})
    assert len(keys) >= 3, f"the corpus must contain at least three distinct {key_of} values"

    # Deliberately shuffled, with a miss in the middle and a duplicate that is NOT adjacent to its
    # first appearance: adjacency would let a buggy implementation that merged neighbours pass.
    batch = [keys[2], "no-such-key", keys[0], keys[2], keys[1]]
    registry = LoaderRegistry.from_session(session, db_settings, batch_window_ms=0)
    loader = getattr(registry, loader_name)

    with count_statements(database.engine) as counter:
        groups = await asyncio.gather(*(loader.load(key) for key in batch))

    assert len(counter) == 1, f"five loads must be one statement:\n{counter.report()}"

    for key, group in zip(batch, groups):
        expected = matching_events(records, lambda record: getattr(record, key_of) == key)
        assert [event.timestamp for event in group] == [
            record.timestamp for record in expected
        ], f"the group returned for {key!r} is not that key's"

    assert groups[1] == [], "a key with no rows gets an empty list at its own position"
    assert groups[0] == groups[3], "the repeated key is answered identically at both positions"
    assert groups[0] != groups[2], (
        "two different keys came back with the same rows, which is the alignment bug this test "
        "exists for"
    )


@pytest.mark.parametrize(
    ("loader_name", "stream"),
    [
        ("order_event_by_id", "orders"),
        ("payment_event_by_id", "payments"),
        ("user_event_by_id", "user_activity"),
    ],
)
async def test_a_by_id_batch_reports_misses_as_none_at_their_own_positions(
    loader_name: str,
    stream: str,
    seeded_events: EventCorpus,
    database: Database,
    session: AsyncSession,
    db_settings: Settings,
) -> None:
    """``None`` for an id with no row, the real value everywhere else, one statement for all of it.

    The ids are 1-based and contiguous because the fixture truncates with ``RESTART IDENTITY`` and
    the seeder inserts oldest first — which is also what makes the *value* checkable: id ``n`` is
    the ``n``-th record of that stream in the generated corpus, so the timestamp is an oracle
    rather than a round trip through the API.
    """
    records = getattr(seeded_events, stream)
    assert len(records) >= 3

    batch = [3, 10 ** 9, 1, 3, 2]
    registry = LoaderRegistry.from_session(session, db_settings, batch_window_ms=0)
    loader = getattr(registry, loader_name)

    with count_statements(database.engine) as counter:
        found = await asyncio.gather(*(loader.load(key) for key in batch))

    assert len(counter) == 1, f"five loads must be one statement:\n{counter.report()}"

    assert found[1] is None, "an id with no row is None at its own position"
    assert [event.id for event in (found[0], found[2], found[3], found[4])] == ["3", "1", "3", "2"]
    # Graded against the generator, not against another API call: record n-1 is the row with id n.
    assert found[2].timestamp == records[0].timestamp
    assert found[4].timestamp == records[1].timestamp
    assert found[0].timestamp == records[2].timestamp


async def test_an_empty_batch_costs_no_round_trip(
    seeded_events: EventCorpus, database: Database, session: AsyncSession, db_settings: Settings
) -> None:
    """``WHERE order_id IN ()`` is a statement whose answer is already known, so it is not issued.

    Load-bearing rather than tidy: this is what makes ``relatedLogs`` on a page of untraced events
    **free** rather than merely cheap, and it is asserted at the repository rather than through a
    document because a document cannot produce an empty batch on purpose.
    """
    registry = LoaderRegistry.from_session(session, db_settings, batch_window_ms=0)

    with count_statements(database.engine) as counter:
        assert await registry.load_payment_events_by_order_id([]) == []
        assert await registry.load_order_events_by_user_id([]) == []
        assert await registry.load_order_event_by_id([]) == []

    assert len(counter) == 0, f"an empty batch must not reach the database:\n{counter.report()}"


# =================================================================================================
# The index each batch depends on really exists
# =================================================================================================


@pytest.mark.parametrize(("table", "indexes"), sorted(REQUIRED_INDEXES.items()))
async def test_every_batched_predicate_is_served_by_a_declared_index(
    table: str, indexes: tuple[str, ...], database: Database, session: AsyncSession
) -> None:
    """Each ``IN (...)`` column leads an index — checked against ``pg_indexes``, not a comment.

    C11 added no index, which is a claim worth verifying rather than repeating: C10 declared each of
    these for the traversal it knew was coming, and if one were ever dropped the batched read would
    become a single sequential scan per operation — still ONE statement, and therefore invisible to
    every count in this file. This is the assertion that would notice.
    """
    rows = await session.execute(
        text("SELECT indexname FROM pg_indexes WHERE tablename = :table"), {"table": table}
    )
    present = {row.indexname for row in rows}

    missing = [name for name in indexes if name not in present]
    assert missing == [], (
        f"{table} is missing {missing}; every batched cross-entity read needs its key column to "
        f"lead an index or it is a sequential scan per operation. Present: {sorted(present)}"
    )
