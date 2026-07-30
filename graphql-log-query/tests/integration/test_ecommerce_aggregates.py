"""The three cached e-commerce aggregates — spec §3 Feature Area D.

*"Redis caching applied to aggregations, with an invalidation or TTL policy defined per
aggregation."* Four claims, and each one is asserted in a way that can fail:

1. **The numbers are right.** Graded against the deterministic corpus, computed independently in
   Python, and cross-checked against what PostgreSQL itself reports. "The buckets sum to something
   plausible" would pass against an aggregate that silently stopped at ``MAX_QUERY_LIMIT`` — which
   is the failure mode C4's section comment warns about and the reason the corpus here is larger
   than ``DEFAULT_QUERY_LIMIT``.
2. **They are computed in SQL.** Asserted as a statement count, because "computed in SQL" and
   "computed in Python from rows SQL returned" produce identical JSON. One statement per aggregate,
   whatever the corpus size.
3. **A hit costs zero SQL.** The same instrument C7 used, on the new keys.
4. **Each aggregate is stored under ITS OWN TTL.** Not asserted through the policy table alone —
   that would be reading the constant back — but through the ``SETEX`` the cache actually issues,
   captured by a recording client. A policy nothing consults is a comment.

.. rubric:: The distribution and the funnel are DIFFERENT numbers, and one test exists to prove it

They are both "orders by status" and they would look interchangeable in a code review. The
distribution counts each order **once**, at its newest status; the funnel counts every order at
**every** status it ever reached. On the seeded corpus CREATED is ~17 in one and 120 in the other,
so an implementation that answered both with the same statement fails loudly rather than plausibly.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import AsyncIterator
from typing import Any, Optional
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from strawberry.types import ExecutionResult

from src.cache import (
    KIND_LOG_STATS,
    KIND_ORDER_FUNNEL_AGG,
    KIND_ORDER_STATUS_AGG,
    KIND_PAYMENT_OUTCOME_AGG,
    TTL_POLICY,
    ResultCache,
    create_cache_redis_client,
)
from src.config import Settings
from src.db.session import Database
from src.generators import EventCorpus
from src.graphql.context import Context
from src.graphql.schema import schema
from tests.integration.corpus import count_statements

DISTRIBUTION_DOCUMENT = """
query Distribution($filters: OrderEventFilterInput) {
  orderStatusDistribution(filters: $filters) { status orders }
}
"""

FUNNEL_DOCUMENT = """
query Funnel($filters: OrderEventFilterInput) {
  orderFunnel(filters: $filters) { status ordersReached share }
}
"""

PAYMENTS_DOCUMENT = """
query Payments($filters: PaymentEventFilterInput) {
  paymentOutcomeBreakdown(filters: $filters) { method outcome events orders }
}
"""

#: What the C13 dashboard sends: three panels, one document, one round trip. Feature Area E's
#: "multi-series analytics from a single query result" is this string.
DASHBOARD_DOCUMENT = """
{
  orderStatusDistribution { status orders }
  orderFunnel { status ordersReached share }
  paymentOutcomeBreakdown { method outcome events orders }
}
"""


class RecordingRedis:
    """An in-memory stand-in that remembers the TTL every ``SETEX`` was issued with.

    The TTL policy is only real if it reaches Redis. Asserting ``cache.ttl_for(kind)`` alone would
    be reading the constant back out of the table it was written into — true, and satisfied by a
    :meth:`ResultCache.fetch` that ignored it and used a hardcoded 30. This captures the argument
    the cache actually passes, which is the number that governs the data.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    async def get(self, key: str) -> Optional[str]:
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        self.store[key] = value
        self.ttls[key] = ttl
        return True

    def ttl_of_kind(self, kind: str) -> int:
        """The TTL of the one key written under ``kind``, failing loudly if there is not exactly one."""
        matches = [ttl for key, ttl in self.ttls.items() if f":{kind}:" in key]
        assert len(matches) == 1, f"expected exactly one {kind} key, got {len(matches)}: {self.ttls}"
        return matches[0]


# =================================================================================================
# Fixtures
# =================================================================================================


@pytest.fixture()
def cache_settings(db_settings: Settings) -> Settings:
    """The suite's settings with the cache switched back on.

    ``docker-compose.yml`` pins ``CACHE_ENABLED=false`` for the whole ``test`` service, so every
    cache here is enabled explicitly *and* namespaced per test — the isolation an environment
    variable cannot express, and the reason one test cannot answer another's query with a plausible
    stale value while the tables are being truncated between them.
    """
    return db_settings.model_copy(update={"cache_enabled": True})


@pytest.fixture()
async def cache(cache_settings: Settings) -> AsyncIterator[ResultCache]:
    """A cache over the live Redis, under a namespace no other test can reach."""
    built = ResultCache(
        cache_settings,
        redis_client=create_cache_redis_client(cache_settings),
        namespace=f"test:{uuid4().hex}",
        owns_client=True,
    )
    assert built.enabled, (
        "the cache fixture could not build a Redis client from REDIS_URL="
        f"{cache_settings.redis_url!r}"
    )
    try:
        yield built
    finally:
        await built.aclose()


@pytest.fixture()
def cached_context(database: Database, cache_settings: Settings, cache: ResultCache) -> Context:
    """A GraphQL context wired to the test database **and** the test cache."""
    return Context(
        settings=cache_settings,
        session_factory=database.session_factory,
        db=database,
        cache=cache,
    )


async def _data(context: Context, document: str, **variables: Any) -> dict[str, Any]:
    """Run an operation and return ``data``, asserting the response carried no errors."""
    result: ExecutionResult = await schema.execute(
        document, variable_values=variables or None, context_value=context
    )
    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result.data


def _oracle_distribution(corpus: EventCorpus) -> dict[str, int]:
    """Each order's **newest** status, counted — computed from the generated corpus in Python.

    ``corpus.orders`` is sorted oldest-first with strictly increasing timestamps inside each order,
    so the last record seen for an ``order_id`` is that order's newest event. That is the same fact
    ``DISTINCT ON (order_id) ORDER BY timestamp DESC, id DESC`` exploits, arrived at independently.
    """
    newest: dict[str, str] = {}
    for record in corpus.orders:
        newest[record.order_id] = record.status
    return dict(Counter(newest.values()))


def _oracle_funnel(corpus: EventCorpus) -> dict[str, int]:
    """How many distinct orders ever reached each status."""
    reached: dict[str, set[str]] = {}
    for record in corpus.orders:
        reached.setdefault(record.status, set()).add(record.order_id)
    return {status: len(orders) for status, orders in reached.items()}


def _oracle_payment_cells(corpus: EventCorpus) -> dict[tuple[str, str], tuple[int, int]]:
    """``(method, outcome) -> (events, distinct orders)``."""
    cells: dict[tuple[str, str], list[Any]] = {}
    for record in corpus.payments:
        key = (record.method, record.outcome)
        entry = cells.setdefault(key, [0, set()])
        entry[0] += 1
        entry[1].add(record.order_id)
    return {key: (count, len(orders)) for key, (count, orders) in cells.items()}


# =================================================================================================
# The numbers
# =================================================================================================


async def test_the_status_distribution_matches_the_generator_oracle_and_the_database(
    seeded_events: EventCorpus, gql_context: Context, session: AsyncSession
) -> None:
    """Every bucket, graded twice: against the corpus in Python and against PostgreSQL directly.

    The Python oracle is the load-bearing one — it is an *independent* computation of the same
    answer, so it can fail for the right reason. The SQL probe is a second opinion on a different
    question ("does the table really hold this"), which is what would separate "the aggregate is
    wrong" from "the seeder wrote something else".
    """
    expected = _oracle_distribution(seeded_events)

    rows = (await _data(gql_context, DISTRIBUTION_DOCUMENT))["orderStatusDistribution"]
    actual = {row["status"]: row["orders"] for row in rows}

    assert actual == expected
    assert sum(actual.values()) == len({record.order_id for record in seeded_events.orders})

    stored = await session.execute(text("SELECT count(DISTINCT order_id) FROM order_events"))
    assert sum(actual.values()) == stored.scalar_one()


async def test_the_distribution_is_not_silently_capped_at_the_default_limit(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """The corpus holds more orders than ``DEFAULT_QUERY_LIMIT``, and the total says so.

    THE regression this whole "aggregates are computed in SQL" rule exists to prevent: an
    implementation that pulled rows and counted them in Python would go through ``clamp_limit`` and
    report a confident, plausible, capped total. It only changes an assertion at a corpus size above
    the limit, which is why ``EVENT_CORPUS_ORDERS`` is 120 and ``DEFAULT_QUERY_LIMIT`` is 100.
    """
    orders = len({record.order_id for record in seeded_events.orders})
    assert orders > gql_context.settings.default_query_limit, "the corpus is too small to prove this"

    rows = (await _data(gql_context, DISTRIBUTION_DOCUMENT))["orderStatusDistribution"]

    assert sum(row["orders"] for row in rows) == orders


async def test_the_funnel_matches_the_oracle_and_its_shares_are_relative_to_the_widest_stage(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Cumulative counts, in lifecycle order, with a conversion rate per stage."""
    expected = _oracle_funnel(seeded_events)

    rows = (await _data(gql_context, FUNNEL_DOCUMENT))["orderFunnel"]
    actual = {row["status"]: row["ordersReached"] for row in rows}

    assert actual == expected

    widest = max(expected.values())
    for row in rows:
        assert row["share"] == pytest.approx(round(row["ordersReached"] / widest, 4))
    assert max(row["share"] for row in rows) == 1.0, "the widest stage's share must be exactly 1"


async def test_the_funnel_and_the_distribution_are_genuinely_different_questions(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """Both are "orders by status" and they must not be the same number.

    Every generated order starts at CREATED, so the funnel counts all of them there; only the orders
    that never moved are *currently* CREATED. An implementation that answered both fields with one
    statement — the mistake a reviewer would be most likely to wave through, because the two look
    interchangeable — fails here by a factor of about seven.
    """
    distribution = (await _data(gql_context, DISTRIBUTION_DOCUMENT))["orderStatusDistribution"]
    funnel = (await _data(gql_context, FUNNEL_DOCUMENT))["orderFunnel"]

    current = {row["status"]: row["orders"] for row in distribution}
    reached = {row["status"]: row["ordersReached"] for row in funnel}

    assert reached["CREATED"] == len({record.order_id for record in seeded_events.orders})
    assert current["CREATED"] < reached["CREATED"], (
        "every order reached CREATED but only the ones that never moved are still there; equal "
        "numbers mean the two aggregates are running the same query"
    )
    # Cumulative dominates current everywhere, which is the invariant that relates the two.
    for status, count in current.items():
        assert count <= reached[status], status


async def test_the_payment_breakdown_matches_the_oracle_cell_by_cell(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """The (method x outcome) cross-tabulation, with both counts per cell.

    ``events`` and ``orders`` are asserted separately because their *difference* is the signal the
    field exists to publish — an implementation that returned ``count(*)`` for both would be right
    on this corpus for most cells and would silently hide retries on a real one.
    """
    expected = _oracle_payment_cells(seeded_events)

    rows = (await _data(gql_context, PAYMENTS_DOCUMENT))["paymentOutcomeBreakdown"]
    actual = {(row["method"], row["outcome"]): (row["events"], row["orders"]) for row in rows}

    assert actual == expected
    assert sum(events for events, _ in actual.values()) == len(seeded_events.payments)
    # Busiest cell first — what a stacked bar chart leads with, and a total order so the response is
    # diff-stable between two identical requests.
    assert [row["events"] for row in rows] == sorted(
        (row["events"] for row in rows), reverse=True
    )


async def test_each_aggregate_is_one_statement_and_no_rows_are_pulled(
    seeded_events: EventCorpus, gql_context: Context, database: Database
) -> None:
    """Computed **in** SQL, not from SQL — asserted as a count, because the JSON is identical.

    One statement per aggregate. An implementation that fetched the order stream and grouped it in
    Python would also be one statement, so the count alone is not the whole claim — but combined
    with the uncapped total above (which such an implementation could not produce) it is.

    The three-panel dashboard is measured as one operation on purpose: three root fields are three
    statements, not three round trips, which is the REST habit this project exists to argue against.
    """
    with count_statements(database.engine) as counter:
        await _data(gql_context, DISTRIBUTION_DOCUMENT)
    assert len(counter) == 1, counter.report()

    with count_statements(database.engine) as counter:
        await _data(gql_context, FUNNEL_DOCUMENT)
    assert len(counter) == 1, counter.report()

    with count_statements(database.engine) as counter:
        await _data(gql_context, PAYMENTS_DOCUMENT)
    assert len(counter) == 1, counter.report()

    with count_statements(database.engine) as counter:
        data = await _data(gql_context, DASHBOARD_DOCUMENT)
    assert len(counter) == 3, (
        f"the whole dashboard is three statements in ONE operation:\n{counter.report()}"
    )
    assert data["orderStatusDistribution"] and data["orderFunnel"]
    assert data["paymentOutcomeBreakdown"]


# =================================================================================================
# Filters compose on an aggregate exactly as they do on the rows beneath it
# =================================================================================================


async def test_an_aggregate_filter_narrows_the_same_way_the_row_query_does(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """A summary panel and the table under it describe **one** set.

    Both go through ``to_order_event_query`` and ``build_order_event_predicates``, so this is a test
    of that sharing rather than of a second predicate builder — and the point is that there is no
    second one. The oracle is recomputed under the same filter, so a resolver that dropped the
    filter on the aggregate path would fail here even though its unfiltered answer is correct.
    """
    user_id = Counter(record.user_id for record in seeded_events.orders).most_common(1)[0][0]

    expected = _oracle_funnel(
        EventCorpus(
            orders=[r for r in seeded_events.orders if r.user_id == user_id],
            payments=[],
            user_activity=[],
        )
    )

    rows = (
        await _data(gql_context, FUNNEL_DOCUMENT, filters={"userId": user_id})
    )["orderFunnel"]
    actual = {row["status"]: row["ordersReached"] for row in rows}

    assert actual == expected

    unfiltered = (await _data(gql_context, FUNNEL_DOCUMENT))["orderFunnel"]
    total_filtered = sum(actual.values())
    total_unfiltered = sum(row["ordersReached"] for row in unfiltered)
    assert 0 < total_filtered < total_unfiltered, (
        "filtering to one buyer must narrow the funnel, or the filter is not reaching the aggregate"
    )


async def test_a_window_that_matches_nothing_returns_an_empty_list_rather_than_an_error(
    seeded_events: EventCorpus, gql_context: Context
) -> None:
    """A quiet window is zeros, not a failure. A dashboard must render it, not crash on it."""
    rows = (
        await _data(
            gql_context,
            DISTRIBUTION_DOCUMENT,
            filters={"startTime": "2099-01-01T00:00:00+00:00"},
        )
    )["orderStatusDistribution"]

    assert rows == []


# =================================================================================================
# The cache, and the per-aggregation TTL policy
# =================================================================================================


@pytest.mark.parametrize(
    ("document", "field"),
    [
        (DISTRIBUTION_DOCUMENT, "orderStatusDistribution"),
        (FUNNEL_DOCUMENT, "orderFunnel"),
        (PAYMENTS_DOCUMENT, "paymentOutcomeBreakdown"),
    ],
)
async def test_a_second_identical_aggregate_query_issues_zero_sql_and_returns_the_same_numbers(
    document: str,
    field: str,
    seeded_events: EventCorpus,
    cached_context: Context,
    database: Database,
) -> None:
    """Spec §5's "caching measurably reduces database load", on the new aggregates.

    Zero, not "fewer": the response JSON is byte-identical on a hit and a miss, so a statement count
    is the only assertion that can fail for the right reason. The values are compared too, because
    a cache that returned the right *shape* and the wrong *numbers* would satisfy the count.
    """
    with count_statements(database.engine) as first:
        miss = (await _data(cached_context, document))[field]
    assert len(first) >= 1, "the first call must actually compute the aggregate"
    assert miss, "the aggregate must return buckets or the hit below proves nothing"

    with count_statements(database.engine) as second:
        hit = (await _data(cached_context, document))[field]

    assert len(second) == 0, (
        f"a cache hit must issue no SQL at all, got {len(second)}:\n{second.report()}"
    )
    assert hit == miss


async def test_the_policy_table_names_a_setting_for_every_cached_kind() -> None:
    """Every kind the cache can be asked for has a TTL of its own, and the settings really exist.

    ``ResultCache.ttl_for`` falls back to the short generic TTL for an unknown kind, which is the
    right *behaviour* and would quietly hide a missing policy row — so the table is checked against
    the kinds directly rather than through that fallback.
    """
    settings = Settings(_env_file=None)

    for kind in (
        KIND_LOG_STATS,
        KIND_ORDER_STATUS_AGG,
        KIND_ORDER_FUNNEL_AGG,
        KIND_PAYMENT_OUTCOME_AGG,
    ):
        assert kind in TTL_POLICY, f"{kind} has no TTL policy row"
        assert hasattr(settings, TTL_POLICY[kind]), TTL_POLICY[kind]


def test_the_three_ttls_are_ordered_the_way_the_policy_argues(cache_settings: Settings) -> None:
    """Volatile < additive < monotonic — the ordering IS the policy, so it is pinned.

    The distribution is redistributive (one event moves an order between buckets), the generic
    aggregates are additive (one write, +1 out of thousands), the funnel is monotonic (a stale read
    can only undercount). If those three numbers ever collapse to one value the policy has stopped
    being per-aggregation, and this is the assertion that says so.
    """
    assert cache_settings.order_status_agg_ttl_seconds < cache_settings.agg_cache_ttl_seconds
    assert cache_settings.agg_cache_ttl_seconds < cache_settings.funnel_agg_ttl_seconds


@pytest.mark.parametrize(
    ("document", "kind", "setting_name"),
    [
        (DISTRIBUTION_DOCUMENT, KIND_ORDER_STATUS_AGG, "order_status_agg_ttl_seconds"),
        (FUNNEL_DOCUMENT, KIND_ORDER_FUNNEL_AGG, "funnel_agg_ttl_seconds"),
        (PAYMENTS_DOCUMENT, KIND_PAYMENT_OUTCOME_AGG, "agg_cache_ttl_seconds"),
    ],
)
async def test_each_aggregate_is_stored_under_its_own_configured_ttl(
    document: str,
    kind: str,
    setting_name: str,
    seeded_events: EventCorpus,
    database: Database,
    cache_settings: Settings,
) -> None:
    """The TTL the cache actually issues its ``SETEX`` with — not the one the table claims.

    This is the assertion that makes "a TTL policy defined per aggregation" a property of the system
    rather than of a dict. A ``fetch`` that computed the key correctly and then stored under a
    hardcoded 30 would satisfy every other test in this module.

    Driven through a recording client rather than the live Redis because a real ``TTL`` round trip
    answers in whole seconds and starts counting immediately — readable, but a comparison against
    a 20-second policy that is already 19 by the time it is read is a flake waiting to happen.
    """
    recorder = RecordingRedis()
    cache = ResultCache(
        cache_settings,
        redis_client=recorder,
        namespace=f"test:{uuid4().hex}",
        owns_client=False,
    )
    context = Context(
        settings=cache_settings,
        session_factory=database.session_factory,
        db=database,
        cache=cache,
    )

    await _data(context, document)

    expected = getattr(cache_settings, setting_name)
    assert recorder.ttl_of_kind(kind) == expected == cache.ttl_for(kind)


async def test_the_two_order_aggregates_do_not_share_a_cache_entry(
    seeded_events: EventCorpus, database: Database, cache_settings: Settings
) -> None:
    """Identical filters, different questions — so the *kind* has to be inside the hash.

    ``orderStatusDistribution`` and ``orderFunnel`` take the same input type and are driven by
    byte-identical filter sets. If the key were derived from the payload alone the second would read
    the first's answer and return it: same shape, wrong numbers, no error. That is the exact
    collision :func:`src.cache.make_cache_key` hashes the kind for, and it is worth an assertion
    rather than a comment because it is silent.
    """
    recorder = RecordingRedis()
    cache = ResultCache(
        cache_settings,
        redis_client=recorder,
        namespace=f"test:{uuid4().hex}",
        owns_client=False,
    )
    context = Context(
        settings=cache_settings,
        session_factory=database.session_factory,
        db=database,
        cache=cache,
    )

    distribution = (await _data(context, DISTRIBUTION_DOCUMENT))["orderStatusDistribution"]
    funnel = (await _data(context, FUNNEL_DOCUMENT))["orderFunnel"]

    assert len(recorder.store) == 2, f"two questions must write two keys: {list(recorder.store)}"
    assert {row["status"]: row["orders"] for row in distribution} != {
        row["status"]: row["ordersReached"] for row in funnel
    }


@pytest.mark.parametrize(
    ("document", "field"),
    [
        (DISTRIBUTION_DOCUMENT, "orderStatusDistribution"),
        (FUNNEL_DOCUMENT, "orderFunnel"),
        (PAYMENTS_DOCUMENT, "paymentOutcomeBreakdown"),
    ],
)
async def test_a_disabled_cache_still_returns_the_right_numbers_every_time(
    document: str,
    field: str,
    seeded_events: EventCorpus,
    database: Database,
    db_settings: Settings,
    gql_context: Context,
) -> None:
    """``CACHE_ENABLED=false`` is a true bypass, and correctness does not depend on the cache.

    Two calls, both hitting the database, both returning the aggregate the cached path returns. The
    second half matters as much as the first: a resolver whose answer *changed* when caching was
    turned off would mean the cache was carrying something the computation does not, which is how a
    projection ends up applied on one path and skipped on the other.
    """
    disabled = ResultCache(
        db_settings.model_copy(update={"cache_enabled": False}),
        redis_client=RecordingRedis(),
        namespace=f"test:{uuid4().hex}",
        owns_client=False,
    )
    context = Context(
        settings=db_settings,
        session_factory=database.session_factory,
        db=database,
        cache=disabled,
    )
    assert not disabled.enabled

    with count_statements(database.engine) as counter:
        first = (await _data(context, document))[field]
        second = (await _data(context, document))[field]

    assert len(counter) == 2, (
        f"a disabled cache must recompute every time, got {len(counter)}:\n{counter.report()}"
    )
    assert first == second
    # And the uncached answer is the same one the ordinary context gives, so nothing about
    # correctness depends on the cache being there.
    assert first == (await _data(gql_context, document))[field]
