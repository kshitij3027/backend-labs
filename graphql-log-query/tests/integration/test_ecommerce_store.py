"""The C10 e-commerce tables, exercised against the real ``gqllogs_test`` PostgreSQL database.

Same strategy as ``test_db_store.py`` and for the same reason: every assertion computes its expected
answer **in Python**, by running the filter over the objects
:func:`~src.generators.generate_event_corpus` returned, and then asserts the database returned
exactly that set — same rows, same order, same values. Two independent computations compared.

The tautology this avoids is worth naming, because it is the obvious test to write: asserting that
``status="SHIPPED"`` returns rows whose status is SHIPPED passes against an implementation that
returns one matching row and silently drops the other forty, and against one that ignores the limit.

What is *only* checkable here, in SQL, and nowhere else:

* whether ``metadata`` is a SQL ``NULL`` or the JSONB scalar ``'null'`` — asyncpg deserialises both
  to the Python ``None``, so a Python-side assertion cannot fail;
* whether the declared indexes actually exist, and whether the composite ones carry the trailing
  ``id`` tiebreak;
* whether ``timestamp`` is ``timestamptz`` rather than ``timestamp``;
* whether the ``metadata_`` attribute really maps to a column named ``metadata``.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import Settings
from src.db.models import (
    OrderEventRecord,
    PaymentEventRecord,
    UserEventRecord,
)
from src.db.repository import (
    LogRepository,
    OrderEventQuery,
    PaymentEventQuery,
    UserEventQuery,
    build_order_event_predicates,
)
from src.db.session import Database
from src.generators import (
    ORDER_STATUS_LEVELS,
    ORDER_TRACE_LOG_RATIO,
    EventCorpus,
    order_id_for,
)
from tests.integration.corpus import (
    ANCHOR,
    EVENT_CORPUS_ORDERS,
    SEED,
    CorrelatedCorpus,
    event_metadata_counts,
    matching_events,
)

#: A limit comfortably above every stream in the fixed corpus but still inside
#: ``db_settings.max_query_limit``, so a full-stream comparison is a set equality rather than a
#: prefix check. The clamp itself is proved separately, by a test that chooses its own ceiling.
LIMIT_ALL = 5_000

#: The indexes each table must publish. Named rather than counted: a count would stay green while
#: the wrong index was created.
EXPECTED_INDEXES = {
    "order_events": {
        "ix_order_events_ts_id",
        "ix_order_events_order_ts",
        "ix_order_events_user_ts",
        "ix_order_events_status_ts",
        "ix_order_events_trace_id",
    },
    "payment_events": {
        "ix_payment_events_ts_id",
        "ix_payment_events_order_ts",
        "ix_payment_events_outcome_ts",
        "ix_payment_events_method_ts",
        "ix_payment_events_trace_id",
    },
    "user_events": {
        "ix_user_events_ts_id",
        "ix_user_events_user_ts",
        "ix_user_events_activity_ts",
        "ix_user_events_trace_id",
    },
}


def _orders(rows: list) -> list[OrderEventRecord]:
    """Project stored order rows onto the value objects the oracle is made of."""
    return [OrderEventRecord.from_orm_row(row) for row in rows]


def _payments(rows: list) -> list[PaymentEventRecord]:
    """Project stored payment rows onto the value objects the oracle is made of."""
    return [PaymentEventRecord.from_orm_row(row) for row in rows]


def _users(rows: list) -> list[UserEventRecord]:
    """Project stored user rows onto the value objects the oracle is made of."""
    return [UserEventRecord.from_orm_row(row) for row in rows]


# --- Seeding and round-tripping -------------------------------------------------------------------


async def test_the_whole_corpus_round_trips_through_all_three_tables(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Every generated record is stored and read back **byte-identically**, in reverse order.

    The strongest single assertion in this module: it grades every column of every row of all three
    streams at once, including the ``metadata`` JSONB and the tz-aware timestamps. Reversed rather
    than re-sorted because the generator emits oldest-first and seeding inserts in that order, so
    the database's ``ORDER BY timestamp DESC, id DESC`` is exactly the reverse — a relationship this
    test would notice breaking.
    """
    stored_orders = await repo.list_order_events(OrderEventQuery(limit=LIMIT_ALL))
    stored_payments = await repo.list_payment_events(PaymentEventQuery(limit=LIMIT_ALL))
    stored_users = await repo.list_user_events(UserEventQuery(limit=LIMIT_ALL))

    assert _orders(stored_orders) == list(reversed(seeded_events.orders))
    assert _payments(stored_payments) == list(reversed(seeded_events.payments))
    assert _users(stored_users) == list(reversed(seeded_events.user_activity))


async def test_ids_ascend_with_time_in_every_event_table(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Insert order is time order, so ``BIGSERIAL`` agrees with ``timestamp``.

    This is what makes the ``(timestamp, id)`` tiebreak total and what makes "reverse of generation
    order" a correct oracle even when two events share an instant.
    """
    for rows in (
        await repo.list_order_events(OrderEventQuery(limit=LIMIT_ALL)),
        await repo.list_payment_events(PaymentEventQuery(limit=LIMIT_ALL)),
        await repo.list_user_events(UserEventQuery(limit=LIMIT_ALL)),
    ):
        ids = [row.id for row in rows]
        timestamps = [row.timestamp for row in rows]
        assert ids == sorted(ids, reverse=True)
        assert timestamps == sorted(timestamps, reverse=True)


async def test_seeding_events_is_idempotent(
    seeded_events: EventCorpus, database: Database, repo: LogRepository
) -> None:
    """A second call writes nothing, so a restart against a persisted volume cannot double it.

    Asserted on the row counts rather than on the return value: ``seed_if_empty`` reports **log**
    rows written (deliberately unchanged by C10), so the only honest evidence that the event corpus
    was not written twice is the tables themselves.
    """
    before = len(await repo.list_order_events(OrderEventQuery(limit=LIMIT_ALL)))

    await database.seed_if_empty(0, SEED, orders=EVENT_CORPUS_ORDERS, end_time=ANCHOR)

    after = await repo.list_order_events(OrderEventQuery(limit=LIMIT_ALL))
    assert len(after) == before == len(seeded_events.orders)
    assert _orders(after) == list(reversed(seeded_events.orders))


async def test_a_second_database_over_the_same_store_also_declines_to_seed(
    seeded_events: EventCorpus, database: Database, db_settings: Settings
) -> None:
    """Idempotency holds across *processes*, not just across calls on one object.

    That is the restart case, and it is guarded by a row count plus the bootstrap advisory lock
    rather than by any in-memory flag — so a second :class:`Database` has to observe it too.
    """
    other = Database.create(db_settings)
    try:
        await other.seed_if_empty(0, SEED, orders=EVENT_CORPUS_ORDERS, end_time=ANCHOR)
    finally:
        await other.dispose()

    async with database.session_factory() as session:
        repository = LogRepository(session, db_settings)
        rows = await repository.list_order_events(OrderEventQuery(limit=LIMIT_ALL))

    assert len(rows) == len(seeded_events.orders)


async def test_seeding_zero_orders_writes_no_events(
    database: Database, repo: LogRepository
) -> None:
    """``SEED_ORDERS=0`` — the compose ``test`` service's configuration — is a no-op."""
    assert await database.seed_if_empty(0, SEED, orders=0, end_time=ANCHOR) == 0

    assert await repo.list_order_events(OrderEventQuery(limit=LIMIT_ALL)) == []
    assert await repo.list_payment_events(PaymentEventQuery(limit=LIMIT_ALL)) == []
    assert await repo.list_user_events(UserEventQuery(limit=LIMIT_ALL)) == []


async def test_seeding_logs_and_events_together_writes_both(
    database: Database, repo: LogRepository
) -> None:
    """One call, one advisory lock, one transaction — and the log return value is unaffected.

    The return value is what the lifespan logs and what several pre-C10 tests assert against
    ``SEED_ENTRIES``; folding an event count into it would silently break both.
    """
    written = await database.seed_if_empty(50, SEED, orders=10, end_time=ANCHOR)

    assert written == 50, "seed_if_empty must keep reporting LOG rows written"
    assert len(await repo.list_order_events(OrderEventQuery(limit=LIMIT_ALL))) > 0
    assert len(await repo.list_payment_events(PaymentEventQuery(limit=LIMIT_ALL))) > 0
    assert len(await repo.list_user_events(UserEventQuery(limit=LIMIT_ALL))) > 0


async def test_events_are_seeded_even_when_the_log_table_is_already_populated(
    seeded: list, database: Database, repo: LogRepository
) -> None:
    """The two emptiness checks are independent, which is what an operator raising SEED_ORDERS needs.

    ``log_entries`` already holds the fixed corpus (the ``seeded`` fixture), so a single shared
    "is the store empty" check would decline to write the event corpus and the operator would see
    an empty order board with no error anywhere.
    """
    await database.seed_if_empty(1000, SEED, orders=10, end_time=ANCHOR)

    assert len(await repo.list_order_events(OrderEventQuery(limit=LIMIT_ALL))) > 0


async def test_the_seeded_log_corpus_joins_exactly_the_declared_order_traces(
    seeded_correlated: CorrelatedCorpus, session: AsyncSession
) -> None:
    """A **non-zero, deterministic** number of order traces also carry log rows, asserted in SQL.

    This is the property ``correlatedEvents`` returning four ``__typename``s rests on, and it is
    pinned here rather than only against the generator because the wiring is where it would break:
    :meth:`~src.db.session.Database.seed_if_empty` has to generate the event corpus **first** and
    hand its trace ids to the log generator. Reorder those two calls, drop the argument, or skip
    regenerating the event corpus when its table is already populated, and the generator stays
    perfectly correct while the store loses the correlation entirely.

    Before this was declared, the overlap was an artefact of both generators running on one seed:
    fifteen of two hundred at the shipped defaults, eight at this fixture's size, and **zero** at
    4000 log rows. So an equality against a declared set is the only assertion that cannot pass by
    luck, and ``> 0`` would have passed for the accident too.

    The second half is the other side of the same requirement: correlating *everything* would
    satisfy the first half while leaving C5's ``relatedLogs`` and spec §2 item 17 (the null-trace
    empty list) with nothing to exercise.
    """
    declared = seeded_correlated.shared_traces
    assert len(declared) == int(EVENT_CORPUS_ORDERS * ORDER_TRACE_LOG_RATIO)
    assert len(declared) >= 10

    joined = (
        (
            await session.execute(
                text(
                    "SELECT DISTINCT o.trace_id FROM order_events o "
                    "JOIN log_entries l ON l.trace_id = o.trace_id"
                )
            )
        )
        .scalars()
        .all()
    )

    assert set(joined) == set(declared)

    counts = (
        await session.execute(
            text(
                "SELECT count(*) FILTER (WHERE trace_id IS NULL) AS untraced, "
                "count(DISTINCT trace_id) AS distinct_traces FROM log_entries"
            )
        )
    ).one()

    assert counts.untraced > 0, "no null trace id: relatedLogs' empty-list branch is unreachable"
    assert counts.distinct_traces - len(declared) > len(declared), (
        "log-only traces no longer outnumber the correlated ones, so the independent population "
        "the C5 tests depend on has been squeezed out"
    )


# --- Metadata storage: the question Python cannot answer -------------------------------------------


@pytest.mark.parametrize("table", ["order_events", "payment_events", "user_events"])
async def test_absent_metadata_is_a_sql_null_and_never_the_jsonb_scalar_null(
    table: str, seeded_events: EventCorpus, session: AsyncSession
) -> None:
    """``JSONB(none_as_null=True)`` survived onto every event table's seeding path.

    Without the flag a Python ``None`` is *serialised* into the JSON scalar ``null`` and stored as
    the JSONB value ``'null'`` — which deserialises straight back to ``None``, so every Python
    round-trip test above would still pass while ``metadata IS NULL`` matched no row,
    ``jsonb_typeof(metadata)`` returned the string ``'null'``, and C11's
    ``WHERE metadata IS NOT NULL`` aggregations silently counted every row.

    Both branches must be populated or the assertion is vacuous, which is what the corpus's
    per-order metadata roll guarantees and what the first assertion here checks.
    """
    sql_nulls, json_nulls, objects = await event_metadata_counts(session, table)

    assert sql_nulls > 0 and objects > 0, (
        f"{table} must exercise both metadata branches for this test to be able to fail"
    )
    assert json_nulls == 0, (
        f"{json_nulls} rows in {table} hold the JSONB scalar 'null' instead of SQL NULL — "
        "the seeding INSERT path lost none_as_null=True"
    )


async def test_metadata_is_stored_as_jsonb_not_text(
    seeded_events: EventCorpus, session: AsyncSession
) -> None:
    """The column really is JSONB, so PostgreSQL can index and query inside it.

    A ``Text`` column holding a JSON string would satisfy every round-trip assertion above and
    would still be the wrong type: C11's aggregations reach into this object from SQL.
    """
    row = (
        await session.execute(
            text(
                "SELECT count(*) AS n FROM order_events "
                "WHERE metadata ->> 'channel' IS NOT NULL"
            )
        )
    ).one()

    expected = sum(1 for record in seeded_events.orders if record.metadata is not None)
    assert int(row.n) == expected > 0


# --- Individual filters, each graded against the oracle -------------------------------------------


@pytest.mark.parametrize("status", ["CREATED", "SHIPPED", "CANCELLED", "REFUNDED"])
async def test_the_status_filter_selects_exactly_the_expected_order_events(
    status: str, seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Whole expected subset, nothing else, in the right order."""
    expected = matching_events(seeded_events.orders, lambda r: r.status == status)
    assert expected, "the corpus must contain this status or the test proves nothing"

    rows = await repo.list_order_events(OrderEventQuery(status=status, limit=LIMIT_ALL))

    assert _orders(rows) == expected


async def test_the_order_id_filter_returns_one_orders_whole_history(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """The order -> its own events read, which is what C11 traverses and C12 replays."""
    order_id = order_id_for(7)
    expected = matching_events(seeded_events.orders, lambda r: r.order_id == order_id)
    assert len(expected) >= 1

    rows = await repo.list_order_events(OrderEventQuery(order_id=order_id, limit=LIMIT_ALL))

    assert _orders(rows) == expected


async def test_the_user_id_filter_spans_several_orders(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """The order -> user edge has real fan-out, so a broken join cannot look correct.

    A corpus with one user per order would let a join that returned "the first order for this
    user" pass; asserting on a user with several is what makes it fail.
    """
    user_id = seeded_events.orders[0].user_id
    expected = matching_events(seeded_events.orders, lambda r: r.user_id == user_id)
    distinct_orders = {record.order_id for record in expected}
    assert len(distinct_orders) > 1, "pick a user with several orders or this proves nothing"

    rows = await repo.list_order_events(OrderEventQuery(user_id=user_id, limit=LIMIT_ALL))

    assert _orders(rows) == expected


@pytest.mark.parametrize("outcome", ["AUTHORIZED", "CAPTURED", "DECLINED", "REFUNDED"])
async def test_the_outcome_filter_selects_exactly_the_expected_payment_events(
    outcome: str, seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Every declared outcome occurs in the corpus and each selects its own subset."""
    expected = matching_events(seeded_events.payments, lambda r: r.outcome == outcome)
    assert expected

    rows = await repo.list_payment_events(PaymentEventQuery(outcome=outcome, limit=LIMIT_ALL))

    assert _payments(rows) == expected


async def test_the_method_filter_selects_exactly_the_expected_payment_events(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """The second payment dimension, filtered independently of the first."""
    method = seeded_events.payments[0].method
    expected = matching_events(seeded_events.payments, lambda r: r.method == method)
    assert expected

    rows = await repo.list_payment_events(PaymentEventQuery(method=method, limit=LIMIT_ALL))

    assert _payments(rows) == expected


@pytest.mark.parametrize("activity", ["LOGIN", "CHECKOUT", "LOGOUT"])
async def test_the_activity_filter_selects_exactly_the_expected_user_events(
    activity: str, seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Including LOGOUT, which only some sessions produce — a thin bucket that must still be exact."""
    expected = matching_events(seeded_events.user_activity, lambda r: r.activity_type == activity)
    assert expected

    rows = await repo.list_user_events(UserEventQuery(activity_type=activity, limit=LIMIT_ALL))

    assert _users(rows) == expected


async def test_the_level_filter_returns_only_the_declines(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Severity is mapped from the outcome, so the ERROR bucket is exactly the DECLINED events."""
    expected = matching_events(seeded_events.payments, lambda r: r.level == "ERROR")
    assert expected

    rows = await repo.list_payment_events(PaymentEventQuery(level="ERROR", limit=LIMIT_ALL))

    assert _payments(rows) == expected
    assert {row.outcome for row in rows} == {"DECLINED"}


async def test_the_service_filter_matches_the_declared_emitter(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Every order event comes from ``order-service``; nothing else does in this table."""
    rows = await repo.list_order_events(
        OrderEventQuery(service="order-service", limit=LIMIT_ALL)
    )
    empty = await repo.list_order_events(
        OrderEventQuery(service="payment-service", limit=LIMIT_ALL)
    )

    assert _orders(rows) == list(reversed(seeded_events.orders))
    assert empty == []


async def test_the_trace_filter_selects_one_orders_whole_correlated_set(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """The same trace id selects rows from all three tables — the correlation requirement in SQL."""
    trace_id = seeded_events.orders[0].trace_id
    assert trace_id is not None

    orders = await repo.list_order_events(OrderEventQuery(trace_id=trace_id, limit=LIMIT_ALL))
    payments = await repo.list_payment_events(
        PaymentEventQuery(trace_id=trace_id, limit=LIMIT_ALL)
    )
    users = await repo.list_user_events(UserEventQuery(trace_id=trace_id, limit=LIMIT_ALL))

    assert _orders(orders) == matching_events(
        seeded_events.orders, lambda r: r.trace_id == trace_id
    )
    assert _payments(payments) == matching_events(
        seeded_events.payments, lambda r: r.trace_id == trace_id
    )
    assert _users(users) == matching_events(
        seeded_events.user_activity, lambda r: r.trace_id == trace_id
    )
    assert orders and payments and users


async def test_the_time_range_filter_is_inclusive_at_both_ends(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Both bounds inclusive, exactly as they are for ``logs`` — one filter, one meaning.

    Asserted on the *boundary rows* specifically: a half-open interval would drop the row sitting
    exactly on ``end_time``, which is a single row and therefore the one an approximate assertion
    would never notice.
    """
    ordered = seeded_events.orders
    start = ordered[5].timestamp
    end = ordered[-5].timestamp
    expected = matching_events(ordered, lambda r: start <= r.timestamp <= end)

    rows = await repo.list_order_events(
        OrderEventQuery(start_time=start, end_time=end, limit=LIMIT_ALL)
    )

    assert _orders(rows) == expected
    assert ordered[5] in expected and ordered[-5] in expected


async def test_filters_compose_with_and_not_or(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """Two supplied filters narrow the result; they do not widen it.

    An ``OR`` composition would return a strict superset and would still contain every row an
    equality-only assertion looked for, so the check is against the *intersection* computed in
    Python.
    """
    status = "PAID"
    user_id = seeded_events.orders[0].user_id
    expected = matching_events(
        seeded_events.orders, lambda r: r.status == status and r.user_id == user_id
    )
    assert expected, "pick a combination that matches something"

    rows = await repo.list_order_events(
        OrderEventQuery(status=status, user_id=user_id, limit=LIMIT_ALL)
    )

    assert _orders(rows) == expected
    assert len(expected) < len(
        matching_events(seeded_events.orders, lambda r: r.status == status)
    )


async def test_an_omitted_filter_is_ignored_rather_than_matched_against_null(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """``OrderEventQuery()`` contributes no WHERE clause at all."""
    assert build_order_event_predicates(OrderEventQuery()) == []

    rows = await repo.list_order_events(OrderEventQuery(limit=LIMIT_ALL))
    assert len(rows) == len(seeded_events.orders)


async def test_search_text_escapes_like_metacharacters(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """A needle containing ``%`` matches ids CONTAINING a percent sign — i.e. none of them.

    The reason :func:`~src.db.repository.escape_like` is reused here rather than reimplemented: an
    unescaped ``%`` turns a substring search into a wildcard that matches every order in the table,
    which is a strictly larger and quietly wrong answer.
    """
    every = await repo.list_order_events(
        OrderEventQuery(search_text="ord-", limit=LIMIT_ALL)
    )
    wildcard = await repo.list_order_events(
        OrderEventQuery(search_text="ord%", limit=LIMIT_ALL)
    )

    assert len(every) == len(seeded_events.orders), "every generated order id starts with 'ord-'"
    assert wildcard == [], (
        "an unescaped '%' matched rows, so the LIKE metacharacter reached the pattern verbatim"
    )


async def test_search_text_finds_a_specific_order(
    seeded_events: EventCorpus, repo: LogRepository
) -> None:
    """The positive half: a real substring selects exactly the ids containing it."""
    needle = order_id_for(3)
    expected = matching_events(seeded_events.orders, lambda r: needle in r.order_id)
    assert expected

    rows = await repo.list_order_events(OrderEventQuery(search_text=needle, limit=LIMIT_ALL))

    assert _orders(rows) == expected


async def test_the_limit_is_clamped_on_the_event_paths_too(
    seeded_events: EventCorpus, session: AsyncSession
) -> None:
    """Spec §2 item 22 applies to every query path, including the three new ones.

    Driven through a repository built with a deliberately small ceiling, because the clamp is only
    observable when the test chooses the ceiling — the suite's own ``max_query_limit`` is raised
    above the corpus so full-corpus comparisons stay set equalities.
    """
    # `default_query_limit` comes down with the ceiling: `Settings._check_limit_ordering` refuses a
    # default above `MAX_QUERY_LIMIT`, because such a default could never take effect.
    capped = LogRepository(
        session, Settings(_env_file=None, max_query_limit=7, default_query_limit=7)
    )

    orders = await capped.list_order_events(OrderEventQuery(limit=LIMIT_ALL))
    payments = await capped.list_payment_events(PaymentEventQuery(limit=LIMIT_ALL))
    users = await capped.list_user_events(UserEventQuery(limit=LIMIT_ALL))

    assert len(orders) == len(payments) == len(users) == 7
    # And the clamped page is the NEWEST rows, not an arbitrary seven.
    assert _orders(orders) == list(reversed(seeded_events.orders))[:7]


async def test_a_zero_limit_still_returns_one_row(
    seeded_events: EventCorpus, session: AsyncSession
) -> None:
    """``LIMIT 0`` returns nothing, which is indistinguishable from "nothing matched".

    ``clamp_limit`` raises it to 1 for exactly that reason, and this asserts the event paths go
    through the same function rather than through a copy of it.
    """
    repository = LogRepository(session, Settings(_env_file=None))

    rows = await repository.list_order_events(OrderEventQuery(limit=0))

    assert len(rows) == 1


# --- Schema-level facts ----------------------------------------------------------------------------


@pytest.mark.parametrize("table", sorted(EXPECTED_INDEXES))
async def test_every_declared_index_exists_on_the_table(
    table: str, session: AsyncSession
) -> None:
    """All of them, after the real ``init_db`` — not a subset that happens to be enough today."""
    result = await session.execute(
        text("SELECT indexname FROM pg_indexes WHERE tablename = :table"), {"table": table}
    )
    present = set(result.scalars().all())

    missing = EXPECTED_INDEXES[table] - present
    assert not missing, f"missing indexes on {table}: {sorted(missing)}"


@pytest.mark.parametrize(
    ("table", "index_name", "expected_columns"),
    [
        ("order_events", "ix_order_events_ts_id", ["timestamp", "id"]),
        ("order_events", "ix_order_events_order_ts", ["order_id", "timestamp", "id"]),
        ("order_events", "ix_order_events_user_ts", ["user_id", "timestamp", "id"]),
        ("order_events", "ix_order_events_status_ts", ["status", "timestamp", "id"]),
        ("payment_events", "ix_payment_events_order_ts", ["order_id", "timestamp", "id"]),
        ("payment_events", "ix_payment_events_outcome_ts", ["outcome", "timestamp", "id"]),
        ("user_events", "ix_user_events_user_ts", ["user_id", "timestamp", "id"]),
        ("user_events", "ix_user_events_activity_ts", ["activity_type", "timestamp", "id"]),
    ],
)
async def test_the_ordering_indexes_end_with_the_id_tiebreak(
    table: str, index_name: str, expected_columns: list[str], session: AsyncSession
) -> None:
    """Every composite index carries ``id`` last, for the reason argued on ``log_entries``.

    Asserting the index *exists* is not enough: one missing its trailing ``id`` still serves the
    filter, and the planner simply adds a ``Sort`` node on top. Nothing fails; the query is just
    sorting rows it should have been reading in order.
    """
    result = await session.execute(
        text("SELECT indexdef FROM pg_indexes WHERE tablename = :table AND indexname = :name"),
        {"table": table, "name": index_name},
    )
    definition = result.scalar_one()

    # `indexdef` ends in the parenthesised column list; PostgreSQL quotes reserved words, so
    # `timestamp` comes back as `"timestamp"`.
    columns = [
        column.strip().strip('"')
        for column in definition.rpartition("(")[2].rstrip(")").split(",")
    ]

    assert columns == expected_columns, f"{index_name} is {columns}, expected {expected_columns}"


@pytest.mark.parametrize(
    ("table", "expected_columns"),
    [
        (
            "order_events",
            {
                "id",
                "timestamp",
                "service",
                "level",
                "trace_id",
                "order_id",
                "user_id",
                "status",
                "metadata",
            },
        ),
        (
            "payment_events",
            {
                "id",
                "timestamp",
                "service",
                "level",
                "trace_id",
                "order_id",
                "method",
                "outcome",
                "metadata",
            },
        ),
        (
            "user_events",
            {
                "id",
                "timestamp",
                "service",
                "level",
                "trace_id",
                "user_id",
                "activity_type",
                "metadata",
            },
        ),
    ],
)
async def test_the_columns_are_named_what_the_api_publishes(
    table: str, expected_columns: set[str], session: AsyncSession
) -> None:
    """Exact column set, and specifically ``metadata`` rather than ``metadata_``.

    The Python attribute has to carry the underscore (``metadata`` is reserved on a declarative
    class), and ``mapped_column``'s first positional argument renames the column back. If somebody
    "fixes" the underscore by dropping the rename, the GraphQL field keeps working and only a query
    written against the database by hand — or this test — notices.
    """
    result = await session.execute(
        text("SELECT column_name FROM information_schema.columns WHERE table_name = :table"),
        {"table": table},
    )
    columns = set(result.scalars().all())

    assert columns == expected_columns
    assert "metadata_" not in columns


@pytest.mark.parametrize("table", ["order_events", "payment_events", "user_events"])
async def test_the_timestamp_column_is_timezone_aware(
    table: str, session: AsyncSession
) -> None:
    """``timestamptz``, not ``timestamp``.

    Without the timezone PostgreSQL discards the offset on write and every comparison happens in
    whatever ``TimeZone`` the server is set to, so the same query returns different rows on a
    differently-configured database and no amount of normalisation in Python can fix it.
    """
    result = await session.execute(
        text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = :table AND column_name = 'timestamp'"
        ),
        {"table": table},
    )

    assert result.scalar_one() == "timestamp with time zone"


async def test_severity_is_stored_exactly_as_the_roster_mapping_says(
    seeded_events: EventCorpus, session: AsyncSession
) -> None:
    """The stored ``level`` per status is the mapping, checked against the database itself.

    Grades the *storage* rather than the generator, which the unit suite already covers — the two
    together are what rule out a seeding path that transformed the value on its way in.
    """
    rows = (
        await session.execute(
            text("SELECT DISTINCT status, level FROM order_events ORDER BY status")
        )
    ).all()

    assert {row.status: row.level for row in rows} == dict(ORDER_STATUS_LEVELS)
