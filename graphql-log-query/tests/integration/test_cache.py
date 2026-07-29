"""The result cache against real PostgreSQL and real Redis — spec §2 items 30-31, §5 "caching
measurably reduces database load".

.. rubric:: The assertion that makes this module worth having is a STATEMENT COUNT

``{ logs { id service level message } }`` returns byte-identical JSON whether it cost one SQL
statement or none. So a test that graded the payload would stay green against a cache that was
never consulted, a cache that was consulted and ignored, and a cache that was deleted entirely —
which is to say it would not test the cache at all. Every headline test here therefore wraps the
second execution in :func:`~tests.integration.corpus.count_statements` — the same
``before_cursor_execute`` instrument C5 used to prove DataLoader batching — and asserts **zero**
statements against ``log_entries``, *and* that the data equals the first execution's. Either half
alone is passable by a broken implementation: zero statements with different data is a cache
serving somebody else's rows, and identical data with a statement is no cache.

.. rubric:: The near-miss key tests are the ones that matter

"Two different filter sets get different keys" is nearly free and proves nearly nothing. The
failures that actually happen are the pairs that differ in exactly one thing — only the limit, only
the search text, only one boundary of the window — because those are the fields a key builder
forgets, and the symptom is not an error but *one query's rows served as another query's answer*
for the length of the TTL. So each of those pairs is executed against the real database, through
the real resolver, and graded on the **rows**: a key test is satisfied by two hashes differing, and
only a row test can fail when they do not.

.. rubric:: Redis is real, except where determinism needs it not to be

Every test but two uses the live Redis the compose ``test`` service provides (logical DB 1), under
a namespace unique to the test — the corpus is deterministic, so a key leaking between two tests
would hold a *plausible* value and the resulting failure would be attributed to the wrong test.

The single-flight test uses an in-memory double instead, and deliberately: with a real client every
caller's ``GET`` is a network round trip, so "did the twentieth caller reach the in-flight map
before the leader's query finished" would become a race between Redis latency and PostgreSQL
latency. The double has no await point in ``get``, so no non-leader can park on anything except the
in-flight future — which means the event loop cannot go idle to complete the leader's database I/O
until every other caller has already coalesced. The interleaving becomes a property of the code
rather than of the machine the suite is running on. The database is still real, the resolvers are
still real, and the statements are still counted.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from strawberry.types import ExecutionResult

from src.cache import (
    DEFAULT_CACHE_NAMESPACE,
    ResultCache,
    cached_logs,
    create_cache_redis_client,
)
from src.config import Settings
from src.db.models import LogRecord
from src.db.repository import LogQuery
from src.db.session import Database
from src.graphql.context import Context
from src.graphql.enums import LogLevel
from src.graphql.schema import schema
from tests.integration.corpus import (
    ANCHOR,
    CORPUS_SIZE,
    count_statements,
    matching,
    newest_first,
)

#: The spec's §5 acceptance command, verbatim — the document the HTTP wiring test replays. Selecting
#: only four leaf fields keeps it valid against ``logs: [LogEntry!]!`` and keeps the assertion about
#: the transport rather than about the projection.
SPEC_ACCEPTANCE_DOCUMENT = "{ logs { id service level message } }"

#: Every published field on ``LogEntry``. A projection that omitted ``metadata`` or ``traceId``
#: could not tell a lossless round trip from a resolver that quietly dropped a field.
LOGS_DOCUMENT = """
query Logs($filters: LogFilterInput) {
  logs(filters: $filters) { id timestamp service level message metadata traceId }
}
"""

#: The narrowest possible selection, for the test that proves the cache is keyed on the FILTERS and
#: not on the selection set.
LOG_IDS_DOCUMENT = """
query LogIds($filters: LogFilterInput) {
  logs(filters: $filters) { id }
}
"""

RELATED_DOCUMENT = """
query Related($filters: LogFilterInput) {
  logs(filters: $filters) { id traceId relatedLogs { id } }
}
"""

STATS_DOCUMENT = """
query Stats($startTime: DateTime, $endTime: DateTime) {
  logStats(startTime: $startTime, endTime: $endTime) {
    totalLogs
    errorCount
    services
    serviceBreakdown { service count }
    levelBreakdown { level count }
    earliest
    latest
  }
}
"""

CREATE_DOCUMENT = """
mutation Create($data: CreateLogInput!) {
  createLog(logData: $data) { id service }
}
"""

#: A service from :data:`src.generators.SERVICES`, so a filtered query returns a real subset rather
#: than an empty list that every implementation agrees about.
CORPUS_SERVICE = "auth-service"

#: Two substrings that appear in different message templates and therefore select disjoint,
#: non-empty subsets of a 1200-row corpus: "timed out" is one of five ERROR templates (~10% of the
#: corpus split five ways) and "authenticated" is one of seven INFO templates (~62% split seven
#: ways). Both are asserted non-empty in the test that uses them, so a template edit fails loudly
#: here rather than quietly turning the test into `[] != []`.
SEARCH_TERM_A = "timed out"
SEARCH_TERM_B = "authenticated"

#: The service name the staleness test writes under. Deliberately **not** in the generated
#: vocabulary, so the "before" answer is an empty list and the "after" answer is exactly one row —
#: no arithmetic, and no dependence on which corpus rows happen to be newest.
STALENESS_SERVICE = "cache-staleness-svc"


# =================================================================================================
# Fixtures
# =================================================================================================


@pytest.fixture()
def cache_settings(db_settings: Settings) -> Settings:
    """The suite's settings with the cache turned back **on**.

    ``docker-compose.yml`` pins ``CACHE_ENABLED=false`` for the whole ``test`` service, and for a
    real reason: the suite truncates ``log_entries`` between tests while Redis is external to that,
    so a shared cache would let one test answer another's query with a plausible stale value. Every
    cache in this module is therefore switched on explicitly *and* namespaced per test — which is
    the isolation an environment variable cannot express.
    """
    return db_settings.model_copy(update={"cache_enabled": True})


@pytest.fixture()
async def cache(cache_settings: Settings) -> AsyncIterator[ResultCache]:
    """A cache over the live Redis, under a namespace no other test can reach.

    ``owns_client=True`` because this fixture built the client, so this fixture closes it — the
    same ownership rule :func:`src.main.lifespan` follows, exercised here rather than assumed.
    """
    built = ResultCache(
        cache_settings,
        redis_client=create_cache_redis_client(cache_settings),
        namespace=f"test:{uuid4().hex}",
        owns_client=True,
    )
    assert built.enabled, (
        "the cache fixture could not build a Redis client from REDIS_URL="
        f"{cache_settings.redis_url!r} — the compose `test` service points at redis:6379/1"
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


@pytest.fixture()
def uncached_context(database: Database, db_settings: Settings) -> Context:
    """A context with no cache at all — the C3/C4 arrangement, used here as the control.

    Several tests need to ask "what does the database actually hold right now" without consulting
    or disturbing the cache under test. Answering that through the cached context would be asking
    the thing under test to grade itself.
    """
    return Context(settings=db_settings, session_factory=database.session_factory, db=database)


# =================================================================================================
# Helpers
# =================================================================================================


async def execute(context: Context, document: str, **variables: Any) -> ExecutionResult:
    """Run one operation against the real schema, asserting it produced no errors."""
    result = await schema.execute(
        document, variable_values=variables or None, context_value=context
    )
    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result


async def fetch_logs(context: Context, **filters: Any) -> list[dict[str, Any]]:
    """``Query.logs`` with every field selected, as a list of plain dicts."""
    result = await execute(context, LOGS_DOCUMENT, filters=filters or None)
    return result.data["logs"]  # type: ignore[index]


def ids_of(rows: list[dict[str, Any]]) -> list[str]:
    return [row["id"] for row in rows]


async def _must_not_compute() -> list[Any]:
    """A ``compute`` that fails loudly, for the assertions that require the key to be populated."""
    raise AssertionError(
        "the cached value was expected to be present under this key, and it was not — which means "
        "the resolver and this test derived two different keys from the same filter set"
    )


class DictRedis:
    """An in-memory stand-in for Redis with **no await point in either method**.

    Used by the two tests whose assertions are about scheduling rather than about storage. See this
    module's docstring for why a real client would make those a race instead of a property.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.calls: list[str] = []

    async def get(self, key: str) -> Optional[str]:
        self.calls.append("get")
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        self.calls.append("setex")
        self.store[key] = value
        return True

    @property
    def setexes(self) -> int:
        return self.calls.count("setex")


# =================================================================================================
# Spec §2 item 31 — a hit reconstructs typed objects WITHOUT TOUCHING THE DATABASE
# =================================================================================================


async def test_a_second_identical_logs_query_issues_zero_sql_and_returns_the_same_rows(
    cached_context: Context, database: Database, seeded: list[LogRecord]
) -> None:
    """The headline claim, both halves: same rows, no statements.

    Graded against the generator oracle as well as against itself, so "the same rows" cannot be
    satisfied by two identical wrong answers.
    """
    expected = matching(seeded, lambda record: record.service == CORPUS_SERVICE)[:25]
    assert len(expected) == 25, "the corpus must contribute at least 25 rows for this service"

    with count_statements(database.engine) as first_pass:
        first = await fetch_logs(cached_context, service=CORPUS_SERVICE, limit=25)

    assert first_pass.count("log_entries") >= 1, (
        f"the first execution must actually query the database:\n{first_pass.report()}"
    )
    assert [row["message"] for row in first] == [record.message for record in expected]

    with count_statements(database.engine) as second_pass:
        second = await fetch_logs(cached_context, service=CORPUS_SERVICE, limit=25)

    assert second_pass.count("log_entries") == 0, (
        "a cache hit must issue NO statement against log_entries — spec §2 item 31. Recorded:\n"
        f"{second_pass.report()}"
    )
    assert second == first, "…and it must return exactly the same rows"

    assert cached_context.cache is not None
    stats = cached_context.cache.stats
    assert (stats.hits, stats.misses, stats.errors) == (1, 1, 0)


async def test_a_second_identical_stats_query_issues_zero_sql_and_returns_the_same_summary(
    cached_context: Context, database: Database, seeded: list[LogRecord]
) -> None:
    """The same proof for the aggregate — the more expensive of the two to recompute."""
    with count_statements(database.engine) as first_pass:
        first = await execute(cached_context, STATS_DOCUMENT)

    assert first_pass.count("log_entries") == 2, (
        "logStats is two statements: the scalar aggregate and the GROUP BY breakdown.\n"
        f"{first_pass.report()}"
    )
    assert first.data["logStats"]["totalLogs"] == CORPUS_SIZE  # type: ignore[index]

    with count_statements(database.engine) as second_pass:
        second = await execute(cached_context, STATS_DOCUMENT)

    assert second_pass.count("log_entries") == 0, (
        f"a cached aggregate must issue no statement. Recorded:\n{second_pass.report()}"
    )
    assert second.data == first.data


async def test_a_rehydrated_entry_is_fully_typed_not_a_bag_of_strings(
    cached_context: Context, uncached_context: Context, cache: ResultCache, seeded: list[LogRecord]
) -> None:
    """Every field of a cache-hit entry must be what a database-loaded one has. Field by field.

    The two that would otherwise pass a shallow check:

    * ``metadata`` — the corpus carries objects on 70% of rows and SQL ``NULL`` on the rest, so both
      branches appear in any window. They must stay distinguishable: a codec writing
      ``metadata or {}`` would turn every absent one into an empty object, a *different* answer that
      looks equally well-formed. C2 fixed a real bug about this distinction at the storage layer
      (``none_as_null``); undoing it here would put it back one layer up, where no SQL probe sees it.
    * ``timestamp`` — must come back timezone-aware and equal to the stored instant. A naive value
      compares unequal to every aware datetime in the system, and would still serialise into a
      perfectly plausible-looking response.

    The response comparison is made first, then the **objects the resolver returned** are inspected
    directly — because by the time a value reaches the JSON envelope, a ``LogLevel`` member and the
    string ``"ERROR"`` are indistinguishable.
    """
    from_database = await fetch_logs(uncached_context, limit=40)
    await fetch_logs(cached_context, limit=40)  # populate
    from_cache = await fetch_logs(cached_context, limit=40)

    assert from_cache == from_database, "a hit is indistinguishable from a read, field for field"

    # Reaches the cached value through the SAME helper the resolver uses, so a key derived
    # differently here would fail loudly rather than quietly re-querying.
    entries = await cached_logs(
        cache, LogQuery(limit=40), cached_context.settings, _must_not_compute
    )

    assert len(entries) == 40
    assert any(entry.metadata is not None for entry in entries), "the corpus carries both branches"
    assert any(entry.metadata is None for entry in entries)
    for entry in entries:
        assert isinstance(entry.id, str), "GraphQL ID is a string on both sides"
        assert entry.timestamp.tzinfo is not None, "a naive value is unequal to every aware one"
        assert entry.timestamp.utcoffset() == timedelta(0), "expressed in UTC"
        assert isinstance(entry.level, LogLevel), "the enum member, not its string value"
        assert entry.metadata is None or isinstance(entry.metadata, dict)

    # And the instants really are the stored ones, not merely aware.
    assert [entry.timestamp for entry in entries] == [
        record.timestamp for record in newest_first(seeded)[:40]
    ]


async def test_related_logs_resolves_identically_on_a_cache_hit_parent(
    cached_context: Context, database: Database, seeded: list[LogRecord]
) -> None:
    """The seam a naive dict-to-object rebuild breaks — spec §2 items 17 and 31 together.

    ``relatedLogs`` is a field resolver that reads ``self.trace_id`` and excludes ``self.id``. A
    rehydrated parent whose ``traceId`` came back ``None``, or whose ``id`` came back as an ``int``
    while the loader's are strings, would resolve the field to the *wrong set* — an empty one in the
    first case, one containing the parent itself in the second — and the response would be perfectly
    well-formed either way.

    The statement count on the second pass is the other half: **one**, not zero and not sixty. The
    parents come from the cache (zero) and the correlated batch still runs (one), which is exactly
    right — the cache stores the entries a query returned, never the fields a client may go on to
    select from them.
    """
    with count_statements(database.engine) as first_pass:
        first = await execute(cached_context, RELATED_DOCUMENT, filters={"limit": 60})

    assert first_pass.count("log_entries") == 2, (
        f"one query for the parents, one batch for the correlated entries:\n{first_pass.report()}"
    )

    with count_statements(database.engine) as second_pass:
        second = await execute(cached_context, RELATED_DOCUMENT, filters={"limit": 60})

    assert second_pass.count("log_entries") == 1, (
        "the parents must come from the cache and the relatedLogs batch must still run:\n"
        f"{second_pass.report()}"
    )
    assert second.data == first.data, "the correlated sets must be identical"

    # Graded against the corpus rather than against the first pass alone, so two identical wrong
    # answers cannot satisfy it.
    rows = second.data["logs"]  # type: ignore[index]
    traced = [row for row in rows if row["traceId"] is not None]
    untraced = [row for row in rows if row["traceId"] is None]
    assert traced, "the corpus must contribute traced entries to this window"
    assert untraced, "…and untraced ones, which must resolve to an empty list"

    for row in traced:
        group = matching(seeded, lambda record, t=row["traceId"]: record.trace_id == t)
        # The parent is excluded from its own related set, so the group is exactly one larger.
        assert len(row["relatedLogs"]) == len(group) - 1
    for row in untraced:
        assert row["relatedLogs"] == []


async def test_the_cache_is_keyed_on_the_filters_and_not_on_the_selection_set(
    cached_context: Context, database: Database, seeded: list[LogRecord]
) -> None:
    """A narrow selection populates the entry a wide one reads, because whole entries are cached.

    Safe precisely because ``LogEntry.from_orm`` always projects the whole row: the cached value is
    the complete entry and the selection set is applied by Strawberry afterwards, on the way out,
    exactly as it is on a miss. The failure this pins is the opposite arrangement — a cache that
    stored the *serialised response* would answer the second query with a list of bare ids.
    """
    await execute(cached_context, LOG_IDS_DOCUMENT, filters={"limit": 30})

    with count_statements(database.engine) as counter:
        wide = await fetch_logs(cached_context, limit=30)

    assert counter.count("log_entries") == 0, f"expected a hit:\n{counter.report()}"
    assert len(wide) == 30
    assert all(row["message"] for row in wide), "the full entry was cached, not the projection"
    assert any(row["metadata"] is not None for row in wide)


# =================================================================================================
# Key discrimination, graded on ROWS
# =================================================================================================


async def test_two_queries_that_differ_only_by_the_limit_do_not_share_an_answer(
    cached_context: Context, seeded: list[LogRecord]
) -> None:
    """The most obvious near miss, and the one a filters-only key falls straight into."""
    five = await fetch_logs(cached_context, service=CORPUS_SERVICE, limit=5)
    ten = await fetch_logs(cached_context, service=CORPUS_SERVICE, limit=10)

    assert len(five) == 5
    assert len(ten) == 10, "the second query must not have been served the first one's five rows"
    assert ids_of(ten)[:5] == ids_of(five), "the shorter answer is the prefix of the longer one"


async def test_two_queries_that_differ_only_by_the_search_text_do_not_share_an_answer(
    cached_context: Context, uncached_context: Context, seeded: list[LogRecord]
) -> None:
    """Same service, same window, same limit — one search term apart."""
    truth_a = await fetch_logs(uncached_context, searchText=SEARCH_TERM_A, limit=50)
    truth_b = await fetch_logs(uncached_context, searchText=SEARCH_TERM_B, limit=50)
    assert truth_a and truth_b, "both terms must select a non-empty subset of the corpus"
    assert ids_of(truth_a) != ids_of(truth_b), "…and the two subsets must genuinely differ"

    cached_a = await fetch_logs(cached_context, searchText=SEARCH_TERM_A, limit=50)
    cached_b = await fetch_logs(cached_context, searchText=SEARCH_TERM_B, limit=50)

    assert cached_a == truth_a
    assert cached_b == truth_b, (
        "two different searches were served one answer — searchText is missing from the key"
    )


async def test_two_queries_that_differ_only_by_one_boundary_do_not_share_an_answer(
    cached_context: Context, uncached_context: Context, seeded: list[LogRecord]
) -> None:
    """One microsecond of difference on the start bound, which moves exactly one row.

    The bound is placed **on an actual row's timestamp** and then nudged, because both bounds are
    inclusive (see ``build_predicates``): ``startTime == row.timestamp`` includes that row and one
    microsecond later excludes it. A boundary chosen at random would usually land between rows (the
    corpus is spread over 24 hours, so they are ~72 seconds apart), where a missing bound in the key
    is invisible.
    """
    boundary = newest_first(seeded)[10].timestamp

    inclusive = await fetch_logs(cached_context, startTime=boundary.isoformat(), limit=50)
    exclusive = await fetch_logs(
        cached_context, startTime=(boundary + timedelta(microseconds=1)).isoformat(), limit=50
    )

    assert len(inclusive) == 11, "the newest eleven rows are at or after that instant"
    assert len(exclusive) == 10, "…and nudging the bound past it drops exactly one"
    assert inclusive == await fetch_logs(
        uncached_context, startTime=boundary.isoformat(), limit=50
    )


async def test_the_same_instant_written_two_ways_shares_one_cached_answer(
    cached_context: Context, database: Database, seeded: list[LogRecord]
) -> None:
    """The positive half of key determinism: equal questions must not become two cache entries.

    A client in another timezone asking for the same moment is asking the same question, and the key
    has to agree — otherwise a dashboard rendering its bounds with a local offset never hits the
    cache at all, and the whole layer silently does nothing while every test above stays green.
    """
    india = timezone(timedelta(hours=5, minutes=30))
    boundary = newest_first(seeded)[10].timestamp

    utc_rows = await fetch_logs(cached_context, startTime=boundary.isoformat(), limit=50)

    with count_statements(database.engine) as counter:
        india_rows = await fetch_logs(
            cached_context, startTime=boundary.astimezone(india).isoformat(), limit=50
        )

    assert counter.count("log_entries") == 0, (
        f"the same instant in another offset must land on the same key:\n{counter.report()}"
    )
    assert india_rows == utc_rows


async def test_logs_and_log_stats_do_not_share_a_cached_answer(
    cached_context: Context, database: Database, seeded: list[LogRecord]
) -> None:
    """Both cached, both correct, neither serving the other's value.

    They are driven by the same time window and hand a kind-less key builder identical payloads. A
    collision would make the second call decode a list of entries with the aggregate codec — which
    the codec refuses, so the visible symptom would be a permanent 100% miss rate rather than wrong
    data. Both are worth failing on, so this asserts the hits AND the values.
    """
    start = newest_first(seeded)[100].timestamp.isoformat()

    rows = await fetch_logs(cached_context, startTime=start, limit=50)
    summary = await execute(cached_context, STATS_DOCUMENT, startTime=start)

    with count_statements(database.engine) as counter:
        rows_again = await fetch_logs(cached_context, startTime=start, limit=50)
        summary_again = await execute(cached_context, STATS_DOCUMENT, startTime=start)

    assert counter.count("log_entries") == 0, f"both should hit:\n{counter.report()}"
    assert rows_again == rows
    assert summary_again.data == summary.data
    assert summary.data["logStats"]["totalLogs"] == 101  # type: ignore[index]
    assert len(rows) == 50, "…and the list answer is still the list answer"


# =================================================================================================
# Degradation: the cache can never fail or falsify a request
# =================================================================================================


async def test_a_disabled_cache_hits_the_database_every_time(
    database: Database, db_settings: Settings, seeded: list[LogRecord]
) -> None:
    """``CACHE_ENABLED=false`` must be a bypass, not a no-op wrapper that still round-trips.

    The store double is handed in deliberately: "the cache is off" has to mean the store was never
    spoken to, and only a double can assert the absence of a command.
    """
    settings = db_settings.model_copy(update={"cache_enabled": False})
    store = DictRedis()
    disabled = ResultCache(
        settings,
        redis_client=store,  # type: ignore[arg-type]
        namespace=f"test:{uuid4().hex}",
    )
    context = Context(
        settings=settings, session_factory=database.session_factory, db=database, cache=disabled
    )

    with count_statements(database.engine) as counter:
        first = await fetch_logs(context, service=CORPUS_SERVICE, limit=20)
        second = await fetch_logs(context, service=CORPUS_SERVICE, limit=20)
        third = await fetch_logs(context, service=CORPUS_SERVICE, limit=20)

    assert counter.count("log_entries") == 3, (
        f"every call must reach the database when the cache is off:\n{counter.report()}"
    )
    assert first == second == third, "…and every one of them must still be correct"
    assert len(first) == 20
    assert store.calls == [], "a disabled cache issued a Redis command"
    assert disabled.enabled is False
    assert disabled.stats.bypassed == 3
    assert (disabled.stats.hits, disabled.stats.misses) == (0, 0)


async def test_an_unreachable_redis_still_answers_correctly_and_moves_the_error_counter(
    database: Database,
    cache_settings: Settings,
    uncached_context: Context,
    seeded: list[LogRecord],
) -> None:
    """The never-raises contract, against a real client pointed at a port nothing is listening on.

    Three things are asserted, and a test that checked only the first would pass against a cache
    that swallowed the failure and returned an empty list: the rows are **right** (graded against a
    cache-free context), the ``errors`` counter **moved** (so the failure was seen rather than
    skipped), and every call **reached the database** (so the answer came from the source rather
    than from a silently empty cache read).
    """
    settings = cache_settings.model_copy(update={"redis_url": "redis://127.0.0.1:6399/0"})
    broken = ResultCache(
        settings,
        redis_client=create_cache_redis_client(settings),
        namespace=f"test:{uuid4().hex}",
        owns_client=True,
    )
    context = Context(
        settings=settings, session_factory=database.session_factory, db=database, cache=broken
    )

    try:
        truth = await fetch_logs(uncached_context, service=CORPUS_SERVICE, limit=15)
        assert len(truth) == 15

        with count_statements(database.engine) as counter:
            first = await fetch_logs(context, service=CORPUS_SERVICE, limit=15)
            second = await fetch_logs(context, service=CORPUS_SERVICE, limit=15)

        assert first == truth, "an unreachable cache must not change the answer"
        assert second == truth, "…on the second call either"
        assert counter.count("log_entries") == 2, (
            f"both calls must have read through to the database:\n{counter.report()}"
        )
        assert broken.stats.errors >= 2, "the Redis failures were counted, not silently skipped"
        assert broken.stats.misses == 2
        assert broken.stats.hits == 0
    finally:
        await broken.aclose()


# =================================================================================================
# Single-flight — the stampede guard, counted in SQL statements rather than in counters
# =================================================================================================


async def test_twenty_concurrent_identical_queries_issue_one_query(
    database: Database, cache_settings: Settings, seeded: list[LogRecord]
) -> None:
    """Twenty operations, one key, **one** statement — and twenty correct answers.

    Without single-flight this is twenty identical scans issued in the same instant, each holding
    one of the ten connections in the pool. The counter is asserted too, but the statement count is
    the assertion that cannot be satisfied by a coalescing counter incremented in the wrong place.
    """
    store = DictRedis()
    cache = ResultCache(
        cache_settings,
        redis_client=store,  # type: ignore[arg-type]
        namespace=f"test:{uuid4().hex}",
    )
    context = Context(
        settings=cache_settings,
        session_factory=database.session_factory,
        db=database,
        cache=cache,
    )

    with count_statements(database.engine) as counter:
        results = await asyncio.gather(
            *(fetch_logs(context, service=CORPUS_SERVICE, limit=30) for _ in range(20))
        )

    assert counter.count("log_entries") == 1, (
        "twenty concurrent identical queries must produce ONE database round trip:\n"
        f"{counter.report()}"
    )
    assert store.setexes == 1, "only the leader writes the value back"
    assert all(result == results[0] for result in results), "and all twenty got the same answer"
    assert len(results[0]) == 30

    # The two assertions above are the proof; these are the consistency check. The exact SPLIT
    # between "coalesced" and "hit" is a scheduling detail — a caller that arrives after the leader
    # has already stored the value is a hit rather than a waiter, and both are correct — but the
    # SUM is not: each of the other nineteen callers must have been answered without computing.
    stats = cache.stats
    assert stats.coalesced + stats.hits == 19
    assert stats.coalesced >= 1, "nobody waited on the leader at all, so nothing was coalesced"


# =================================================================================================
# The lifespan wiring, over the real transport
#
# Everything above executes against `schema` with a hand-built Context, which never touches the
# mount, the `context_getter` dependency, or `app.state`. These two tests are what fail if the
# cache is built but never reaches a resolver.
# =================================================================================================


async def test_the_lifespan_builds_a_cache_and_closes_it_on_the_way_out(
    real_app: FastAPI,
) -> None:
    """``app.state.cache`` exists, is enabled, and is released at shutdown.

    The close is observable without touching a private attribute: :meth:`ResultCache.aclose` drops
    its client, and ``enabled`` is defined as "configured on **and** holding a client" — so a cache
    that reports ``False`` after shutdown is one that gave its connection pool back. A leaked pool
    is not a test failure anybody would notice otherwise; it is a container that takes a minute to
    stop and a Redis with a growing ``CLIENT LIST``.
    """
    real_app.state.settings = real_app.state.settings.model_copy(
        update={"cache_enabled": True}
    )

    async with real_app.router.lifespan_context(real_app):
        built = real_app.state.cache
        assert isinstance(built, ResultCache)
        assert built.enabled is True, "the lifespan must build a usable cache when it is enabled"
        assert built.namespace == DEFAULT_CACHE_NAMESPACE

    assert built.enabled is False, "the lifespan must release the cache's client on shutdown"


async def test_a_cache_hit_over_real_http_issues_zero_sql(real_app: FastAPI) -> None:
    """The whole path a client takes: POST -> router -> ``get_context`` -> ``app.state.cache``.

    The cache installed on ``app.state`` after startup is a namespaced one, for the reason
    :func:`cache_settings` gives — but it is installed exactly where the lifespan installs its own,
    so what this proves is that :func:`src.graphql.context.get_context` carries ``app.state.cache``
    onto the ``Context`` and that ``Query.logs`` reads through it. Without that wiring the cache
    would be built on every boot, hold a connection, and never be consulted by anything.
    """
    settings = real_app.state.settings.model_copy(update={"cache_enabled": True})
    real_app.state.settings = settings

    async with real_app.router.lifespan_context(real_app):
        database: Database = real_app.state.db
        async with database.engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE log_entries RESTART IDENTITY"))
        written = await database.seed_if_empty(120, 20260728, end_time=ANCHOR)
        assert written == 120

        installed = ResultCache(
            settings,
            redis_client=create_cache_redis_client(settings),
            namespace=f"test:{uuid4().hex}",
            owns_client=True,
        )
        real_app.state.cache = installed

        try:
            transport = httpx.ASGITransport(app=real_app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://graphql-log-query.test"
            ) as client:
                body = {"query": SPEC_ACCEPTANCE_DOCUMENT}

                first = await client.post("/graphql", json=body)
                assert first.status_code == 200
                assert "errors" not in first.json(), first.json().get("errors")

                with count_statements(database.engine) as counter:
                    second = await client.post("/graphql", json=body)

            assert second.status_code == 200
            assert second.json() == first.json()
            assert counter.count("log_entries") == 0, (
                "the mounted router must read through app.state.cache:\n" f"{counter.report()}"
            )
            assert installed.stats.hits == 1
        finally:
            await installed.aclose()


# =================================================================================================
# Staleness — the DOCUMENTED behaviour, asserted rather than wished away
# =================================================================================================


async def test_a_created_log_is_invisible_to_an_already_cached_query_for_the_ttl(
    cached_context: Context, uncached_context: Context, database: Database
) -> None:
    """**This is not write-through, and the test says so.**

    ``createLog`` does not invalidate anything, so a result already in the cache keeps answering
    without the new row until its TTL expires. That is the spec's own TTL-over-invalidation choice
    (item 30 asks for "a short TTL"; nothing anywhere asks for invalidation) and the argument is in
    :mod:`src.cache`.

    Asserting the documented behaviour rather than pretending consistency is the point. A test that
    expected the new row would be asserting a feature this system deliberately does not have, and
    would have to be "fixed" by building the dependency-tracking invalidator the design rejected.

    Note what is *also* asserted, because the staleness has to be bounded to be acceptable: the row
    really was written (a cache-free context sees it immediately) and a query under a key that was
    never populated sees it immediately too. Nothing went missing; one already-answered question
    keeps its answer.
    """
    before = await fetch_logs(cached_context, service=STALENESS_SERVICE, limit=10)
    assert before == [], "the staleness service starts empty, so the empty answer gets cached"

    created = await execute(
        cached_context,
        CREATE_DOCUMENT,
        data={
            "service": STALENESS_SERVICE,
            "level": "ERROR",
            "message": "written after the query was cached",
        },
    )
    new_id = created.data["createLog"]["id"]  # type: ignore[index]

    with count_statements(database.engine) as counter:
        after = await fetch_logs(cached_context, service=STALENESS_SERVICE, limit=10)

    assert counter.count("log_entries") == 0, f"still a cache hit:\n{counter.report()}"
    assert after == [], (
        "DOCUMENTED: a cached result is stale for up to CACHE_TTL_SECONDS after a write. If this "
        "assertion fails because the new row appeared, invalidation has been added — update "
        "src/cache.py's module docstring and the README, because the contract has changed."
    )

    uncached = await fetch_logs(uncached_context, service=STALENESS_SERVICE, limit=10)
    assert ids_of(uncached) == [new_id], "the row really is in the database"

    different_key = await fetch_logs(cached_context, service=STALENESS_SERVICE, limit=11)
    assert ids_of(different_key) == [new_id], "a key that was never populated sees it immediately"
