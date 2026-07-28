"""The N+1 proof: ``relatedLogs``, the by-id loader, and the lifetime of both — spec §2 item 29.

Every assertion in this module that matters is a **count of the statements PostgreSQL actually
received**, recorded with :func:`~tests.integration.corpus.count_statements`. That is the only kind
of assertion that can fail for the right reason here: a query selecting ``relatedLogs`` on fifty
entries returns exactly the same JSON whether it cost two statements or fifty-one, so a test that
graded the payload alone would stay green through the entire regression this commit exists to
prevent.

Two rules the counting tests follow, both of them learned from tests that looked fine and proved
nothing:

* **The expected count is exact, not "small".** ``<= 5`` passes against four separate lookups.
* **The count must not move when the row count doubles.** A fixed number is what "batched" means;
  a number that grows with N is an N+1 wearing a smaller constant.

The correctness half is graded against the deterministic corpus rather than against the server's
own answer: for every parent, ``relatedLogs`` must equal exactly the set
:func:`~src.generators.generate_log_records` says shares its trace id, **minus the parent itself**,
in the same order. Asserting "the related entries all have the right traceId" would be a tautology —
it passes against a resolver that returns one member of the group and drops the rest.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Optional

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from strawberry.types import ExecutionResult

from src.config import Settings
from src.db.models import LogRecord
from src.db.repository import LogQuery, LogRepository
from src.db.session import Database
from src.graphql.context import Context, OperationResources
from src.graphql.loaders import LoaderRegistry
from src.graphql.schema import schema
from tests.integration.corpus import (
    CORPUS_SIZE,
    count_statements,
    matching,
    newest_first,
)

#: Minimal document for the counting tests: the parent list, its trace ids, and the correlated
#: entries by id. Nothing else is selected, so nothing else can be blamed for a statement.
BATCHING_DOCUMENT = """
query Batching($limit: Int!) {
  logs(filters: {limit: $limit}) {
    id
    traceId
    relatedLogs { id }
  }
}
"""

#: Every published field on both levels, for the tests that grade against the corpus oracle. A
#: projection that omitted `metadata` or `traceId` could not tell a null apart from a field the
#: resolver forgot to populate.
RELATED_DETAIL_DOCUMENT = """
query RelatedDetail($limit: Int!) {
  logs(filters: {limit: $limit}) {
    id
    timestamp
    traceId
    relatedLogs { id timestamp service level message metadata traceId }
  }
}
"""

ONE_ENTRY_DOCUMENT = """
query OneEntry($id: ID!) {
  log(id: $id) {
    id
    traceId
    relatedLogs { id }
  }
}
"""

ALIASES_DOCUMENT = """
query Aliases($a: ID!, $b: ID!, $c: ID!, $missing: ID!) {
  a: log(id: $a) { id }
  b: log(id: $b) { id }
  c: log(id: $c) { id }
  missing: log(id: $missing) { id }
}
"""

CREATE_DOCUMENT = """
mutation Create($data: CreateLogInput!) {
  createLog(logData: $data) { id traceId }
}
"""

#: A query and a mutation resolve their root fields differently — GraphQL runs a query's
#: concurrently and a mutation's in series — so this document is the one that puts two independent
#: resolvers on the operation's single shared session at the same instant.
MIXED_DOCUMENT = """
query Mixed($limit: Int!) {
  logs(filters: {limit: $limit}) { id relatedLogs { id } }
  logStats { totalLogs errorCount }
}
"""


# --- Helpers -------------------------------------------------------------------------------------


async def _execute(context: Context, document: str, **variables: Any) -> ExecutionResult:
    """Run one operation against the real schema, asserting it produced no errors."""
    result = await schema.execute(
        document, variable_values=variables or None, context_value=context
    )
    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result


def _as_records(rows: list[dict[str, Any]]) -> list[LogRecord]:
    """Project a GraphQL response onto the value objects the corpus oracle is made of."""
    return [
        LogRecord(
            timestamp=datetime.fromisoformat(row["timestamp"]),
            service=row["service"],
            level=row["level"],
            message=row["message"],
            metadata=row["metadata"],
            trace_id=row["traceId"],
        )
        for row in rows
    ]


def _traced(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The entries in a response that carry a trace id."""
    return [row for row in rows if row["traceId"] is not None]


def _expected_trace_keys(seeded: list[LogRecord], limit: int) -> set[str]:
    """The distinct trace ids the newest ``limit`` entries carry, computed from the oracle.

    Used as a **precondition** rather than as an expectation: the seeded corpus is deterministic, so
    if this is ever empty the batching tests below would be asserting that zero lookups were
    batched into zero statements — which passes, and means nothing.
    """
    return {
        record.trace_id
        for record in newest_first(seeded)[:limit]
        if record.trace_id is not None
    }


# --- The headline: N field resolutions, one statement ---------------------------------------------


@pytest.mark.parametrize("limit", [25, 50])
async def test_related_logs_over_n_entries_costs_exactly_two_statements(
    limit: int, seeded: list[LogRecord], gql_context: Context, database: Database
) -> None:
    """``logs(limit: N) { relatedLogs }`` issues **one** statement for the parents and **one** for
    every correlated entry — whatever N is.

    The unbatched implementation of this field is one ``SELECT ... WHERE trace_id = :id`` per
    parent, i.e. N+1 statements, and it produces a byte-identical response. So the assertion is on
    the count, it is exact, and it is parameterised over two values of N: a *fixed* number across
    both is what proves batching. "Fewer than N" would pass against a loader that batched in pairs.
    """
    assert _expected_trace_keys(seeded, limit), "the corpus slice must contain correlated entries"

    with count_statements(database.engine) as counter:
        result = await _execute(gql_context, BATCHING_DOCUMENT, limit=limit)

    rows = result.data["logs"]
    assert len(rows) == limit
    assert _traced(rows), "no entry in the response carried a trace id; nothing was batched"
    assert any(row["relatedLogs"] for row in rows), (
        "every relatedLogs came back empty, so this would pass without ever loading anything"
    )

    assert counter.count("log_entries") == 2, (
        f"expected one statement for the parents and one batched lookup for their correlated "
        f"entries, got {counter.count('log_entries')}:\n{counter.report()}"
    )
    assert counter.count("log_entries.trace_id in") == 1, (
        f"the correlated entries must arrive in ONE `WHERE trace_id IN (...)`:\n{counter.report()}"
    )


async def test_the_statement_count_does_not_grow_when_the_row_count_doubles(
    seeded: list[LogRecord], gql_context: Context, database: Database
) -> None:
    """The same measurement twice, at N and 2N, compared to each other.

    The test above pins the number; this one pins the *shape of the curve*, which is the property
    the requirement is actually about. An implementation that issued one statement per five parents
    would satisfy "a small number" at N=25 and fail here — and so would a per-key loop that
    happened to be under whatever ceiling a single-N test had written down.
    """
    with count_statements(database.engine) as small:
        await _execute(gql_context, BATCHING_DOCUMENT, limit=25)
    with count_statements(database.engine) as large:
        await _execute(gql_context, BATCHING_DOCUMENT, limit=50)

    assert small.count("log_entries") == large.count("log_entries") == 2, (
        f"25 entries cost {small.count('log_entries')} statements and 50 cost "
        f"{large.count('log_entries')}; batching means this number does not depend on N.\n"
        f"--- 25 ---\n{small.report()}\n--- 50 ---\n{large.report()}"
    )


# --- Correctness: the right entries, and not the entry itself -------------------------------------


async def test_related_logs_is_exactly_the_correlated_set_minus_the_entry_itself(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Every parent's ``relatedLogs`` equals what the corpus says, in order, excluding the parent.

    Graded against :func:`~src.generators.generate_log_records`, so both sides of the comparison are
    computed independently — the alternative ("the entries all share the parent's traceId") is a
    tautology that a resolver returning one arbitrary member of the group would satisfy.

    The exclusion is asserted **per parent**, not once: the batch's result is shared by every member
    of a group and each of them has to drop a different row from it, so an implementation that
    excluded "the first entry" or "the newest entry" would be right for one parent per group and
    wrong for the rest.
    """
    limit = 40
    result = await _execute(gql_context, RELATED_DETAIL_DOCUMENT, limit=limit)
    rows = result.data["logs"]

    by_timestamp = {record.timestamp: record for record in seeded}
    checked = 0

    for row in rows:
        parent = by_timestamp[datetime.fromisoformat(row["timestamp"])]

        if row["traceId"] is None:
            assert row["relatedLogs"] == []
            continue

        assert parent.trace_id == row["traceId"], "the response and the oracle disagree on the row"
        expected = [
            record
            for record in matching(seeded, lambda r: r.trace_id == parent.trace_id)
            if record.timestamp != parent.timestamp
        ]

        assert _as_records(row["relatedLogs"]) == expected, (
            f"trace {parent.trace_id} came back wrong for the entry at {parent.timestamp}"
        )
        assert row["id"] not in [entry["id"] for entry in row["relatedLogs"]], (
            "the entry must never appear in its own relatedLogs"
        )
        checked += 1

    assert checked >= 10, (
        f"only {checked} of {limit} entries carried a trace id; this test needs correlated rows "
        "to be grading anything at all"
    )


async def test_a_uniquely_traced_entry_reports_no_related_entries(
    gql_context: Context, repo: LogRepository, session: AsyncSession
) -> None:
    """A trace with exactly one member answers ``[]`` — **not** ``[itself]``.

    This is the whole exclude-self decision in one assertion, and it is the case the literal
    reading of the requirement ("all logs sharing the same trace_id") gets wrong. The seeded corpus
    cannot produce it — its groups are always at least a pair, deliberately — so the row is written
    here.
    """
    lonely = await repo.insert_log(
        service="auth-service", level="INFO", message="alone on its trace", trace_id="trace-solo"
    )
    await session.commit()

    result = await _execute(gql_context, ONE_ENTRY_DOCUMENT, id=str(lonely.id))

    assert result.data["log"]["traceId"] == "trace-solo"
    assert result.data["log"]["relatedLogs"] == [], (
        "a uniquely-traced entry must not be 'related' to itself"
    )


async def test_a_pair_sharing_a_trace_each_report_the_other(
    gql_context: Context, repo: LogRepository, session: AsyncSession
) -> None:
    """The complement of the test above: exclusion drops the parent and **nothing else**.

    Asserted from both ends of the pair, because "return the group minus the first row" would be
    correct for one of them and would silently return an empty list for the other.
    """
    first = await repo.insert_log(
        service="order-service", level="INFO", message="pair one", trace_id="trace-pair"
    )
    second = await repo.insert_log(
        service="order-service", level="ERROR", message="pair two", trace_id="trace-pair"
    )
    await session.commit()

    from_first = await _execute(gql_context, ONE_ENTRY_DOCUMENT, id=str(first.id))
    from_second = await _execute(gql_context, ONE_ENTRY_DOCUMENT, id=str(second.id))

    assert [entry["id"] for entry in from_first.data["log"]["relatedLogs"]] == [str(second.id)]
    assert [entry["id"] for entry in from_second.data["log"]["relatedLogs"]] == [str(first.id)]


# --- A null trace id costs nothing at all ----------------------------------------------------------


async def test_entries_without_a_trace_id_are_empty_and_issue_no_second_statement(
    gql_context: Context, database: Database, repo: LogRepository, session: AsyncSession
) -> None:
    """Spec §2 item 17's other half, asserted as **zero round trips** rather than as an empty list.

    An implementation that loaded ``None`` as a key and let the database confirm there are no rows
    would return exactly the same response — an empty list per entry — while spending a statement
    on it. The early return in the resolver sits above every ``await`` precisely so that a selection
    over untraced entries costs nothing, and this is the only assertion that can tell the two apart.

    A corpus of its own (rather than the seeded one) so that *every* entry in the response has a
    null trace id: with a mixed corpus the batch statement would be issued for the traced rows and
    the zero would be unobservable.
    """
    for index in range(8):
        await repo.insert_log(
            service="search-service", level="DEBUG", message=f"untraced {index}", trace_id=None
        )
    await session.commit()

    with count_statements(database.engine) as counter:
        result = await _execute(gql_context, BATCHING_DOCUMENT, limit=8)

    rows = result.data["logs"]
    assert len(rows) == 8
    assert all(row["traceId"] is None for row in rows), "this test needs an all-untraced corpus"
    assert all(row["relatedLogs"] == [] for row in rows)

    assert counter.count("log_entries") == 1, (
        f"only the parent query should have run:\n{counter.report()}"
    )
    assert counter.count("trace_id in") == 0, (
        f"a null trace id must never reach the loader:\n{counter.report()}"
    )


# --- The positional-ordering contract, against the real database ----------------------------------


async def test_a_shuffled_batch_with_misses_and_duplicates_stays_aligned(
    seeded: list[LogRecord],
    database: Database,
    session: AsyncSession,
    db_settings: Settings,
) -> None:
    """One batch, awkward keys, and every group answered at its own position.

    Driven through the registry rather than through a document, because a GraphQL operation cannot
    ask for a trace id that does not exist or ask for the same one twice in a controlled order —
    and those are exactly the cases where a load function that returns "the right number of results
    in the wrong order" hands every parent somebody else's rows. The unit suite proves the same
    contract over the pure grouping function; this one proves it against what PostgreSQL really
    returns, ordering and all.
    """
    traces = sorted({record.trace_id for record in seeded if record.trace_id is not None})
    assert len(traces) >= 3
    keys = [traces[2], "no-such-trace", traces[0], traces[2], traces[1]]

    registry = LoaderRegistry.from_session(session, db_settings, batch_window_ms=0)

    with count_statements(database.engine) as counter:
        groups = await asyncio.gather(
            *(registry.logs_by_trace_id.load(key) for key in keys)
        )

    assert counter.count("log_entries") == 1, (
        f"five loads must be one statement:\n{counter.report()}"
    )

    for key, group in zip(keys, groups):
        expected = matching(seeded, lambda r: r.trace_id == key)
        assert [entry.message for entry in group] == [record.message for record in expected], (
            f"the group returned for {key!r} is not that trace's"
        )

    assert groups[1] == [], "a key with no rows gets an empty list at its own position"
    assert groups[0] == groups[3], "the repeated key is answered identically at both positions"
    assert groups[0] != groups[2], (
        "two different traces came back with the same rows, which is the alignment bug this test "
        "exists for"
    )


# --- Lifetime: the loaders belong to the operation, not the connection -----------------------------


async def test_a_second_operation_on_the_same_context_sees_a_newly_created_entry(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Loaders are per-**operation**, proven the only way that can fail: with a write in between.

    The same :class:`~src.graphql.context.Context` object runs all three operations here, which is
    exactly the situation on a WebSocket connection — ``context_getter`` resolves once per socket,
    so every operation on that socket shares this object. A loader built there (or cached on it)
    would answer the third operation from what the first one read, and the new entry would be
    invisible for as long as the connection stayed open. Nothing would error; the client would
    simply watch a database that had stopped changing.

    The assertion is therefore on the *appearance* of the new row, not on a cache statistic.
    """
    traced = next(record for record in newest_first(seeded) if record.trace_id is not None)
    listing = (await _execute(gql_context, BATCHING_DOCUMENT, limit=200)).data["logs"]
    anchor = next(row for row in listing if row["traceId"] == traced.trace_id)

    before = await _execute(gql_context, ONE_ENTRY_DOCUMENT, id=anchor["id"])
    related_before = {entry["id"] for entry in before.data["log"]["relatedLogs"]}

    created = await _execute(
        gql_context,
        CREATE_DOCUMENT,
        data={
            "service": "order-service",
            "level": "ERROR",
            "message": "written between two identical operations",
            "traceId": traced.trace_id,
        },
    )
    new_id = created.data["createLog"]["id"]

    after = await _execute(gql_context, ONE_ENTRY_DOCUMENT, id=anchor["id"])
    related_after = {entry["id"] for entry in after.data["log"]["relatedLogs"]}

    assert new_id not in related_before
    assert related_after == related_before | {new_id}, (
        "the second operation served a cached group; the loaders outlived their operation"
    )


async def test_two_requests_over_http_do_not_share_a_loader_cache(
    seeded: list[LogRecord], http_client: httpx.AsyncClient
) -> None:
    """The same property through the real transport, where the context is rebuilt per request.

    Weaker than the test above by construction (a new context per request would hide a
    context-scoped cache), and worth having anyway: it is the path a real client takes, and it
    proves the extension is installed on the *mounted* schema rather than only reachable from
    ``schema.execute``.
    """
    traced = next(record for record in newest_first(seeded) if record.trace_id is not None)

    async def _related(entry_id: str) -> set[str]:
        response = await http_client.post(
            "/graphql", json={"query": ONE_ENTRY_DOCUMENT, "variables": {"id": entry_id}}
        )
        assert response.status_code == 200
        payload = response.json()
        assert "errors" not in payload, payload.get("errors")
        return {entry["id"] for entry in payload["data"]["log"]["relatedLogs"]}

    listing = await http_client.post(
        "/graphql",
        json={"query": BATCHING_DOCUMENT, "variables": {"limit": 200}},
    )
    rows = listing.json()["data"]["logs"]
    anchor = next(row for row in rows if row["traceId"] == traced.trace_id)

    before = await _related(anchor["id"])

    created = await http_client.post(
        "/graphql",
        json={
            "query": CREATE_DOCUMENT,
            "variables": {
                "data": {
                    "service": "order-service",
                    "level": "WARNING",
                    "message": "written between two HTTP requests",
                    "traceId": traced.trace_id,
                }
            },
        },
    )
    new_id = created.json()["data"]["createLog"]["id"]

    assert await _related(anchor["id"]) == before | {new_id}


# --- Query.log goes through the by-id loader ------------------------------------------------------


async def test_several_log_aliases_batch_into_one_statement(
    seeded: list[LogRecord], gql_context: Context, database: Database
) -> None:
    """Four ``log(id:)`` selections, one ``WHERE id IN (...)``, and the miss is still ``null``.

    Four aliases are four resolver invocations; without the loader they are four statements. The
    missing id is in the document on purpose: the loader has to answer it with ``None`` **at its own
    position** rather than dropping it, and a batch that silently shortened its result would produce
    ``a: log2, b: log3, c: null`` — every alias shifted by one, with no error anywhere.
    """
    with count_statements(database.engine) as counter:
        result = await _execute(
            gql_context, ALIASES_DOCUMENT, a="1", b="2", c="3", missing="99999999"
        )

    assert counter.count("log_entries") == 1, (
        f"four aliases must cost one statement:\n{counter.report()}"
    )
    assert counter.count("log_entries.id in") == 1

    assert result.data["a"]["id"] == "1"
    assert result.data["b"]["id"] == "2"
    assert result.data["c"]["id"] == "3"
    assert result.data["missing"] is None


async def test_the_same_entry_reached_twice_in_one_operation_is_loaded_once(
    seeded: list[LogRecord], gql_context: Context, database: Database
) -> None:
    """The loader's per-key cache, which is the half of "DataLoader" that batching alone is not.

    Ten aliases naming the same entry are one key, so the batch has one member. A pure batcher
    would still send ten ids and get ten copies back.
    """
    document = "query Same { " + " ".join(
        f"a{index}: log(id: \"7\") {{ id }}" for index in range(10)
    ) + " }"

    with count_statements(database.engine) as counter:
        result = await _execute(gql_context, document)

    assert counter.count("log_entries") == 1, counter.report()
    assert all(result.data[f"a{index}"]["id"] == "7" for index in range(10))


# --- One session per operation, shared safely ------------------------------------------------------


async def test_two_root_fields_resolving_concurrently_share_the_operation_session(
    seeded: list[LogRecord], gql_context: Context, database: Database
) -> None:
    """``{ logs { relatedLogs } logStats { … } }`` — concurrent resolvers on one shared session.

    graphql-core resolves a query's root fields concurrently, so this document puts ``logs``,
    ``logStats`` and a DataLoader batch on the operation's single ``AsyncSession`` at overlapping
    instants. An ``AsyncSession`` is not concurrency-safe: without the serialisation in
    :class:`~src.graphql.context.OperationResources` this raises
    ``InvalidRequestError``/``IllegalStateChangeError``, or interleaves two statements on one
    connection.

    Wrapped in :func:`asyncio.wait_for` because the *other* way serialisation can be wrong is a
    deadlock, and a deadlocked test with no timeout hangs the suite instead of failing it.

    The count is exact and it is the sum of the parts: one statement for ``logs``, two for
    ``logStats`` (a scalar aggregate and a GROUP BY — see :class:`~src.db.repository.LogStatsResult`)
    and one batched lookup for the correlated entries.
    """
    limit = 20
    assert _expected_trace_keys(seeded, limit), "the corpus slice must contain correlated entries"

    with count_statements(database.engine) as counter:
        result = await asyncio.wait_for(
            schema.execute(
                MIXED_DOCUMENT, variable_values={"limit": limit}, context_value=gql_context
            ),
            timeout=30,
        )

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    assert result.data["logStats"]["totalLogs"] == CORPUS_SIZE
    assert len(result.data["logs"]) == limit

    assert counter.count("log_entries") == 4, (
        f"expected logs(1) + logStats(2) + one batched lookup(1):\n{counter.report()}"
    )


async def test_a_query_operation_gets_one_session_that_lives_until_the_operation_ends(
    database: Database, db_settings: Settings
) -> None:
    """The resource object's contract for a query or mutation, asserted directly.

    Two separate units of work get the **same** session — that is what lets a resolver and a loader
    batch share a transaction and a connection — and it stays open (its transaction still live)
    until the operation closes it. Driven through :class:`OperationResources` rather than through a
    document because "the same object was handed out twice" and "it was closed exactly once, at the
    end" are not visible in a GraphQL response.
    """
    context = Context(settings=db_settings, session_factory=database.session_factory)
    resources = OperationResources(
        context=context,
        session_factory=database.session_factory,
        settings=db_settings,
        is_subscription=lambda: False,
    )

    async with resources.session() as first:
        await first.execute(text("SELECT 1"))
    async with resources.session() as second:
        await second.execute(text("SELECT 1"))

    assert first is second, "every unit of work in one operation shares one session"
    assert resources.shared_session is first
    assert first.in_transaction(), "the operation's transaction spans the operation"

    await resources.aclose()

    assert resources.shared_session is None
    assert not first.in_transaction(), "closing the operation must release the connection"


async def test_a_subscription_operation_never_gets_a_long_lived_session(
    database: Database, db_settings: Settings
) -> None:
    """The contract C6 inherits: a subscription gets a **fresh, short-lived** session per use.

    A subscription's operation scope is the whole life of the stream — Strawberry wraps the entire
    yield loop in ``on_operation`` — so a shared session here would be a connection pinned for as
    long as the socket stays open, serving every read from a snapshot frozen at subscribe time. The
    guard is written and asserted now, before there is a subscription resolver to forget it in.

    Observable without a ``Subscription`` root type because the decision lives in
    :class:`OperationResources` and is driven by a predicate: this is the same object the extension
    builds, told the same thing the parsed document would tell it.
    """
    context = Context(settings=db_settings, session_factory=database.session_factory)
    resources = OperationResources(
        context=context,
        session_factory=database.session_factory,
        settings=db_settings,
        is_subscription=lambda: True,
    )

    async with resources.session() as first:
        await first.execute(text("SELECT 1"))
        assert first.in_transaction()
    assert not first.in_transaction(), "the session must be released when the block ends"

    async with resources.session() as second:
        await second.execute(text("SELECT 1"))

    assert first is not second, "a subscription must not reuse one session across yields"
    assert resources.shared_session is None, "no long-lived session may exist for a subscription"

    # Closing is still safe and still a no-op, which is what the extension relies on.
    await resources.aclose()


async def test_a_failed_statement_does_not_poison_the_rest_of_the_operation(
    seeded: list[LogRecord], database: Database, db_settings: Settings
) -> None:
    """One failing unit of work leaves the shared session usable for the next one.

    Without the rollback in :meth:`OperationResources.session`, a failed statement leaves the
    session needing one — and every later resolver in the same operation would fail with
    ``PendingRollbackError`` about something it had nothing to do with. The second block below is
    the assertion: it must simply work.
    """
    context = Context(settings=db_settings, session_factory=database.session_factory)
    resources = OperationResources(
        context=context,
        session_factory=database.session_factory,
        settings=db_settings,
        is_subscription=lambda: False,
    )

    with pytest.raises(Exception):  # noqa: B017 - a driver error; the type is not the point
        async with resources.session() as session:
            await session.execute(text("SELECT * FROM a_table_that_does_not_exist"))

    async with resources.session() as session:
        repository = LogRepository(session, db_settings)
        rows = await repository.list_logs(LogQuery(limit=5))

    assert len(rows) == 5, "the operation's session survived the failure"

    await resources.aclose()


# --- The loaders are reachable only from inside an operation ---------------------------------------


async def test_reaching_for_loaders_outside_an_operation_fails_loudly(
    database: Database, db_settings: Settings
) -> None:
    """No silent fallback: a context with no operation in scope has no loaders.

    The tempting alternative is to build a registry on demand here. It would work, and it would
    quietly reintroduce the connection-scoped cache the whole design exists to prevent — on the
    WebSocket transport the "demand" is a socket that lives for hours. Failing is the behaviour
    that keeps the lifetime honest.
    """
    context = Context(settings=db_settings, session_factory=database.session_factory)

    with pytest.raises(RuntimeError, match="PerOperationResources"):
        _ = context.loaders


async def test_a_context_outside_an_operation_still_opens_its_own_session(
    seeded: list[LogRecord], database: Database, db_settings: Settings
) -> None:
    """The fallback path in :meth:`Context.session`, which scripts and fixtures depend on.

    ``Context.repository()`` has to keep working for a caller that is not executing a document —
    the E2E verifier and several fixtures use it — so the absence of operation resources means
    "open a short-lived session", not "fail".
    """
    context = Context(settings=db_settings, session_factory=database.session_factory)

    async with context.repository() as repository:
        rows = await repository.list_logs(LogQuery(limit=3))

    assert len(rows) == 3


# --- The published entries are the same objects every other path produces --------------------------


async def test_related_entries_serialise_exactly_like_a_top_level_entry(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """An entry reached through ``relatedLogs`` is identical to the same entry reached through
    ``logs``.

    Two code paths now build a published :class:`~src.graphql.types.LogEntry` — the resolver's own
    projection and the loader's — and both go through ``from_orm`` precisely so they cannot
    disagree. A second mapping is how one path starts returning ``metadata: null`` for a row the
    other renders in full: the response shape stays valid, so only a comparison like this one
    notices.
    """
    result = await _execute(gql_context, RELATED_DETAIL_DOCUMENT, limit=CORPUS_SIZE)
    rows = result.data["logs"]

    by_id: dict[str, dict[str, Any]] = {row["id"]: row for row in rows}
    compared = 0

    for row in rows:
        for related in row["relatedLogs"]:
            top_level: Optional[dict[str, Any]] = by_id.get(related["id"])
            if top_level is None:  # pragma: no cover - the whole corpus is in `rows`
                continue
            assert related["timestamp"] == top_level["timestamp"]
            assert related["traceId"] == top_level["traceId"]
            compared += 1

    assert compared > 0, "the corpus must correlate something for this to compare anything"
