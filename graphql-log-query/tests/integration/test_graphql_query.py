"""The GraphQL read surface, executed against the real ``gqllogs_test`` PostgreSQL database.

.. rubric:: What makes these tests worth having

Almost every assertion grades the API against the **deterministic corpus**: the expected result of
a filter is computed in Python from the objects :func:`~src.generators.generate_log_records`
returned, and the response is asserted to equal exactly that — same rows, same order, same field
values. That is a comparison between two independent computations. The tempting alternative,
asserting that a ``service`` filter returns entries whose ``service`` is the one requested, is a
tautology: it passes against a resolver that returns one matching row and drops the other forty,
and it passes against one that ignores the ``limit`` entirely.

The suite runs the same operations two ways, and both are necessary:

* ``schema.execute`` with an injected :class:`~src.graphql.context.Context` — fast, and the only
  way to run an operation under *deliberately different* settings (the ``MAX_QUERY_LIMIT`` clamp
  is only observable when the test chooses the ceiling).
* the assembled ASGI app over HTTP — which is what actually exercises the router mount, the
  ``context_getter`` FastAPI dependency and the GraphQL JSON envelope. None of those are touched by
  ``schema.execute``, so a suite with only the first would stay green with the API unmounted.

Fixtures (schema creation, ``TRUNCATE`` isolation, the seeded corpus, the context, the HTTP client)
live in ``conftest.py``; the corpus constants and oracle projections in ``corpus.py``.
"""

from __future__ import annotations

import base64
from datetime import datetime
from typing import Any, Optional

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from strawberry.types import ExecutionResult

from src.config import Settings
from src.db.models import LogRecord
from src.db.repository import LogQuery, LogRepository
from src.db.session import Database
from src.graphql.context import Context
from src.graphql.schema import schema
from tests.integration.corpus import CORPUS_SIZE, matching, newest_first

#: The spec's own §5 acceptance command, verbatim. Kept as a module constant so the two tests that
#: use it (through the schema and through HTTP) cannot drift into checking different documents.
SPEC_ACCEPTANCE_DOCUMENT = '{ logs { id service level message } }'

#: Every published field of every entry — used wherever the response is graded against the oracle,
#: because a projection that omitted `metadata` or `traceId` could not tell a null apart from a
#: field the resolver forgot.
LOGS_DOCUMENT = """
query Logs($filters: LogFilterInput) {
  logs(filters: $filters) {
    id
    timestamp
    service
    level
    message
    metadata
    traceId
  }
}
"""

LOG_BY_ID_DOCUMENT = """
query Log($id: ID!) {
  log(id: $id) { id timestamp service level message metadata traceId }
}
"""

CONNECTION_DOCUMENT = """
query Page($filters: LogFilterInput, $first: Int, $after: String) {
  logsConnection(filters: $filters, first: $first, after: $after) {
    totalCount
    pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
    edges { cursor node { id timestamp service level message } }
  }
}
"""


# --- Helpers -------------------------------------------------------------------------------------


async def _execute(
    context: Context, document: str, **variables: Any
) -> ExecutionResult:
    """Run one operation against the real schema with the test's context."""
    return await schema.execute(
        document,
        variable_values=variables or None,
        context_value=context,
    )


async def _logs(
    context: Context, filters: Optional[dict[str, Any]] = None
) -> list[dict[str, Any]]:
    """Run ``LOGS_DOCUMENT`` and return the entries, asserting the response carried no errors."""
    result = await _execute(context, LOGS_DOCUMENT, filters=filters)

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result.data["logs"]


def _records(rows: list[dict[str, Any]]) -> list[LogRecord]:
    """Project a GraphQL response onto the same value objects the oracle is made of.

    This is what makes an equality against the corpus possible at all. ``timestamp`` arrives as an
    ISO-8601 string (that is what the ``DateTime`` scalar serialises to) and is parsed back; the
    ``level`` enum arrives as its **name**, which by construction equals its value — see
    :func:`src.graphql.enums._assert_levels_match_the_corpus`.
    """
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


def _ids(rows: list[dict[str, Any]]) -> list[str]:
    """The ids of a response, in the order they were returned."""
    return [row["id"] for row in rows]


# --- The spec's own acceptance command ------------------------------------------------------------


async def test_the_spec_acceptance_query_returns_structured_log_data(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``{ logs { id service level message } }`` returns the newest ``DEFAULT_QUERY_LIMIT`` rows.

    Spec §5. The document is executed exactly as written — no variables, no ``filters`` argument —
    which is only valid because ``logs`` returns a bare list. Against a Relay connection this
    string would fail validation, which is why the pagination bonus lives on ``logsConnection``.

    The response is graded against the oracle rather than merely counted: the *right* hundred rows,
    in the right order.
    """
    result = await _execute(gql_context, SPEC_ACCEPTANCE_DOCUMENT)

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    rows = result.data["logs"]

    expected = newest_first(seeded)[: gql_context.settings.default_query_limit]
    assert len(rows) == len(expected)
    assert [(row["service"], row["level"], row["message"]) for row in rows] == [
        (record.service, record.level, record.message) for record in expected
    ]


async def test_only_the_requested_fields_come_back(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Spec §2 item 28: no over-fetching into the response.

    Asserted as an exact key set on every entry, not as "the four asked-for keys are present". A
    resolver that serialised the whole row would satisfy the weaker check and would ship
    ``timestamp``, ``metadata`` and ``traceId`` to a client that asked for none of them — which is
    the field-level cost GraphQL exists to avoid, and which grows with C5's ``related_logs``.
    """
    result = await _execute(gql_context, SPEC_ACCEPTANCE_DOCUMENT)

    assert result.data is not None
    rows = result.data["logs"]
    assert rows, "the corpus must be non-empty for this to prove anything"

    extra = {key for row in rows for key in row} - {"id", "service", "level", "message"}
    assert extra == set(), f"the response carried fields nobody asked for: {sorted(extra)}"
    assert all(set(row) == {"id", "service", "level", "message"} for row in rows)


# --- Each filter alone, graded against the oracle -------------------------------------------------


@pytest.mark.parametrize("service", ["auth-service", "payment-service", "analytics-service"])
async def test_the_service_filter_selects_exactly_the_expected_entries(
    service: str, seeded: list[LogRecord], gql_context: Context
) -> None:
    """The whole expected subset comes back, and nothing else."""
    expected = matching(seeded, lambda r: r.service == service)
    assert expected, "the corpus must contain rows for this service or the test proves nothing"

    rows = await _logs(gql_context, {"service": service, "limit": CORPUS_SIZE})

    assert _records(rows) == expected


@pytest.mark.parametrize("level", ["ERROR", "INFO", "CRITICAL"])
async def test_the_level_filter_selects_exactly_the_expected_entries(
    level: str, seeded: list[LogRecord], gql_context: Context
) -> None:
    """Same for the enum, including the thin 1% CRITICAL tail."""
    expected = matching(seeded, lambda r: r.level == level)
    assert expected

    rows = await _logs(gql_context, {"level": level, "limit": CORPUS_SIZE})

    assert _records(rows) == expected


async def test_the_time_range_filter_selects_exactly_the_expected_entries(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """A window in the middle of the corpus returns a meaningful *partial* set.

    Both bounds are inclusive (see :func:`src.db.repository.build_predicates`), so the two rows
    sitting exactly on ``startTime`` and ``endTime`` are part of the expected set — which is what
    makes this able to fail for a half-open interval rather than only for a broken filter.
    """
    start = seeded[300].timestamp
    end = seeded[500].timestamp
    expected = matching(seeded, lambda r: start <= r.timestamp <= end)

    rows = await _logs(
        gql_context,
        {"startTime": start.isoformat(), "endTime": end.isoformat(), "limit": CORPUS_SIZE},
    )

    assert 0 < len(expected) < CORPUS_SIZE, "the window must be a proper subset to prove anything"
    assert _records(rows) == expected
    assert len(expected) == 201, "inclusive on both ends: 300..500 is 201 rows, not 199 or 200"


async def test_a_start_bound_alone_leaves_the_other_end_unbounded(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """One bound supplied, the other omitted — the omitted one imposes nothing."""
    start = seeded[900].timestamp
    expected = matching(seeded, lambda r: r.timestamp >= start)

    rows = await _logs(gql_context, {"startTime": start.isoformat(), "limit": CORPUS_SIZE})

    assert _records(rows) == expected
    assert len(expected) == CORPUS_SIZE - 900


async def test_the_search_text_filter_selects_exactly_the_matching_messages(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Substring search over ``message``, graded against a Python ``in`` over the same corpus."""
    needle = "timed out"
    expected = matching(seeded, lambda r: needle in r.message.lower())
    assert expected

    rows = await _logs(gql_context, {"searchText": needle, "limit": CORPUS_SIZE})

    assert _records(rows) == expected
    assert len(expected) < CORPUS_SIZE, "a needle that matches everything proves nothing"


async def test_a_search_for_a_literal_percent_sign_is_not_a_wildcard(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``searchText: "%"`` finds messages *containing* a percent sign, not the entire corpus.

    ``%`` is the LIKE "any run of characters" metacharacter, so without
    :func:`src.db.repository.escape_like` the pattern becomes ``%%%`` — three wildcards — and this
    query returns everything while looking completely reasonable. The expected set is computed with
    a plain Python ``in``, so the two computations agree only if the escaping survived the whole
    way from the GraphQL variable to the bound parameter.

    Proved here as well as in the repository suite on purpose: this is the path a *client* takes,
    and an input-sanitising layer added at the GraphQL edge in some later commit could easily strip
    or double-escape the character without any repository test noticing.
    """
    expected = matching(seeded, lambda r: "%" in r.message)
    assert expected, "the corpus must contain messages with a literal '%'"
    assert len(expected) < CORPUS_SIZE, "and messages without one"

    rows = await _logs(gql_context, {"searchText": "%", "limit": CORPUS_SIZE})

    assert _records(rows) == expected


async def test_a_search_for_a_literal_underscore_is_not_a_wildcard(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``_`` is the single-character wildcard; unescaped, ``%_%`` matches every non-empty message."""
    expected = matching(seeded, lambda r: "_" in r.message)
    assert expected
    assert len(expected) < CORPUS_SIZE

    rows = await _logs(gql_context, {"searchText": "_", "limit": CORPUS_SIZE})

    assert _records(rows) == expected


# --- Composition and omission ---------------------------------------------------------------------


async def test_all_four_filters_compose_into_an_intersection(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Four dimensions at once narrow to the intersection, not the union.

    A resolver that ORed its filters, or that let a later one overwrite an earlier one, still
    returns "rows that look right" for every single-filter test above. Only the intersection
    catches it — and it is asserted to be a *proper*, non-empty subset of what each filter returns
    alone, so a resolver that silently dropped one of the four is caught too.

    The values are taken from one real record in the middle of the corpus rather than hardcoded,
    which guarantees the intersection is non-empty by construction: a four-way intersection of
    invented values could easily select nothing, and a test whose expected set is empty passes by
    asserting ``[] == []``.
    """
    pivot = seeded[CORPUS_SIZE // 2]
    needle = pivot.message.split()[0]
    start, end = seeded[0].timestamp, seeded[-1].timestamp

    expected = matching(
        seeded,
        lambda r: (
            r.service == pivot.service
            and r.level == pivot.level
            and start <= r.timestamp <= end
            and needle.lower() in r.message.lower()
        ),
    )
    assert pivot in expected, "the pivot must satisfy the filters built from it"

    rows = await _logs(
        gql_context,
        {
            "service": pivot.service,
            "level": pivot.level,
            "startTime": start.isoformat(),
            "endTime": end.isoformat(),
            "searchText": needle,
            "limit": CORPUS_SIZE,
        },
    )

    assert _records(rows) == expected

    service_only = await _logs(gql_context, {"service": pivot.service, "limit": CORPUS_SIZE})
    level_only = await _logs(gql_context, {"level": pivot.level, "limit": CORPUS_SIZE})
    assert len(rows) < len(service_only)
    assert len(rows) < len(level_only)


async def test_an_omitted_argument_an_explicit_null_and_all_null_fields_agree(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Spec §2 item 19, in the three spellings a real client actually produces.

    1. ``{ logs { … } }`` — no ``filters`` argument at all.
    2. ``logs(filters: $filters)`` with ``$filters = null`` — what ``json.dumps`` writes for a
       filter object the UI has not populated.
    3. ``logs(filters: {service: null, level: null, …})`` — what a form binding produces when every
       box is empty, and the case a truthiness check (``if filters.service:``) gets wrong in the
       *other* direction.

    All three must be the identical unfiltered result. This is the test that would fail if omitted
    and explicitly-null ever stopped meaning the same thing — which is exactly what introducing an
    ``UNSET`` sentinel here would risk.
    """
    omitted = await _execute(gql_context, "{ logs { id } }")
    explicit_null = await _execute(
        gql_context,
        "query Logs($filters: LogFilterInput) { logs(filters: $filters) { id } }",
        filters=None,
    )
    every_field_null = await _execute(
        gql_context,
        """
        {
          logs(filters: {
            service: null, level: null, startTime: null,
            endTime: null, searchText: null, limit: null
          }) { id }
        }
        """,
    )

    for label, result in (
        ("omitted", omitted),
        ("explicit null", explicit_null),
        ("every field null", every_field_null),
    ):
        assert result.errors is None, f"{label} filters must be accepted: {result.errors}"
        assert result.data is not None

    assert _ids(omitted.data["logs"]) == _ids(explicit_null.data["logs"])
    assert _ids(omitted.data["logs"]) == _ids(every_field_null.data["logs"])
    assert len(omitted.data["logs"]) == gql_context.settings.default_query_limit
    assert len(omitted.data["logs"]) < CORPUS_SIZE, (
        "the corpus must exceed DEFAULT_QUERY_LIMIT, or 'unfiltered' and 'capped' would be "
        "indistinguishable and this would pass against a resolver that returned everything"
    )


async def test_a_filter_matching_nothing_returns_an_empty_list_not_an_error(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """No rows is a legitimate answer to a well-formed question."""
    result = await _execute(gql_context, LOGS_DOCUMENT, filters={"service": "no-such-service"})

    assert result.errors is None
    assert result.data == {"logs": []}


# --- Limits ---------------------------------------------------------------------------------------


async def test_the_limit_caps_results_clamps_an_absurd_value_and_defaults_when_absent(
    seeded: list[LogRecord], database: Database
) -> None:
    """Spec §2 item 22, on the GraphQL path, with a ceiling small enough to observe.

    A context of its own, because the suite-wide ``db_settings`` deliberately raises
    ``MAX_QUERY_LIMIT`` above the corpus size so oracle comparisons can ask for everything. The
    numbers here (10 / 25) match nothing in the project's defaults, so a resolver that ignored the
    injected settings and read the global configuration would return 100 and fail.

    An over-large ``limit`` is **clamped, not rejected**: a client asking for a million rows gets
    ``MAX_QUERY_LIMIT`` of them and a usable answer, which is what a cap is for. Turning it into a
    validation error would make the cap a breaking change for every client that guessed high.
    """
    capped = Context(
        settings=Settings(_env_file=None, default_query_limit=10, max_query_limit=25),
        session_factory=database.session_factory,
    )

    absurd = await _execute(capped, LOGS_DOCUMENT, filters={"limit": 1_000_000})
    assert absurd.errors is None, f"an over-large limit must clamp, not error: {absurd.errors}"
    assert absurd.data is not None
    assert len(absurd.data["logs"]) == 25

    assert len(await _logs(capped, {"limit": 7})) == 7
    assert len(await _logs(capped, {})) == 10, "an omitted limit uses DEFAULT_QUERY_LIMIT"
    assert len(await _logs(capped, None)) == 10, "so does an omitted filter object"
    # 0 clamps UP to 1 rather than returning an empty list that reads as "nothing matched".
    assert len(await _logs(capped, {"limit": 0})) == 1


# --- Ordering -------------------------------------------------------------------------------------


async def test_results_are_newest_first_and_stable_across_identical_calls(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``(timestamp DESC, id DESC)``, and repeating the query returns the identical sequence.

    Stability is what a cursor is built on. An ordering that were merely "sorted by timestamp"
    could legally permute rows sharing an instant between two calls, and a client paging through
    the result would silently skip or repeat one.
    """
    first = await _logs(gql_context, {"limit": 200})
    second = await _logs(gql_context, {"limit": 200})

    assert _ids(first) == _ids(second)

    timestamps = [datetime.fromisoformat(row["timestamp"]) for row in first]
    assert timestamps == sorted(timestamps, reverse=True)
    assert [int(row["id"]) for row in first] == sorted(
        (int(row["id"]) for row in first), reverse=True
    )


async def test_id_breaks_ties_between_entries_sharing_a_timestamp(
    gql_context: Context, repo: LogRepository, session: AsyncSession
) -> None:
    """Three entries at one instant come back highest-id first, every time.

    The seeded corpus has no duplicate timestamps by construction, so the tiebreak would otherwise
    never be exercised through the API — but ``createLog`` under load produces them readily, and
    this is precisely the case where ``ORDER BY timestamp DESC`` alone is non-deterministic.
    """
    moment = datetime.fromisoformat("2026-07-25T12:00:00+00:00")
    for suffix in ("a", "b", "c"):
        await repo.insert_log(
            service="api-gateway", level="INFO", message=f"tie {suffix}", timestamp=moment
        )
    await session.commit()

    rows = await _logs(gql_context, {})

    assert [row["message"] for row in rows] == ["tie c", "tie b", "tie a"]


# --- The enum is a validation boundary -------------------------------------------------------------


async def test_an_invalid_level_is_a_graphql_validation_error_not_a_500(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """A level outside the enum is rejected **before execution**, in the errors envelope.

    This is the whole point of spec §2 item 14. With ``level`` typed as ``String`` the same request
    would reach the database, produce ``WHERE level = 'NOT_A_LEVEL'`` and return an empty list —
    and the client could not tell a typo from a quiet hour. With the enum, graphql-core rejects the
    document during validation, ``data`` is ``null`` because execution never started, and the
    message names the offending value.

    ``result.errors`` rather than an exception is the second half of the requirement (§2 item 24):
    no stack trace, no HTTP 500 — a normal GraphQL error response.
    """
    result = await _execute(gql_context, '{ logs(filters: {level: NOT_A_LEVEL}) { id } }')

    assert result.errors, "an unknown enum member must be rejected"
    assert result.data is None, "validation failed, so execution must not have started"
    assert "NOT_A_LEVEL" in result.errors[0].message


async def test_an_invalid_level_supplied_as_a_variable_is_also_rejected(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """The literal path and the variable path are separate code in graphql-core; both must reject.

    A schema can be safe against inline literals and unsafe against variables (they go through
    ``parse_value`` rather than ``parse_literal``), and a real client always uses variables.
    """
    result = await _execute(
        gql_context, LOGS_DOCUMENT, filters={"level": "NOT_A_LEVEL"}
    )

    assert result.errors
    assert result.data is None
    assert "NOT_A_LEVEL" in str(result.errors[0])


# --- Query.log -------------------------------------------------------------------------------------


async def test_log_by_id_returns_the_entry(
    seeded: list[LogRecord], gql_context: Context, repo: LogRepository
) -> None:
    """A single fetch returns the same row the repository holds, field for field."""
    row = (await repo.list_logs(LogQuery(limit=1)))[0]

    result = await _execute(gql_context, LOG_BY_ID_DOCUMENT, id=str(row.id))

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    entry = result.data["log"]
    assert entry is not None
    assert entry["id"] == str(row.id)
    assert entry["service"] == row.service
    assert entry["level"] == row.level
    assert entry["message"] == row.message
    assert entry["metadata"] == row.metadata_
    assert entry["traceId"] == row.trace_id
    assert datetime.fromisoformat(entry["timestamp"]) == row.timestamp


async def test_log_by_a_nonexistent_id_is_null_with_no_errors(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Absence is an ordinary answer, not a failure.

    The ``errors`` assertion is the load-bearing half: a resolver that raised ``NOT_FOUND`` for a
    miss would make "does this row exist?" un-askable without error handling, and would make a
    partially-populated dashboard look broken.
    """
    result = await _execute(gql_context, LOG_BY_ID_DOCUMENT, id="999999999")

    assert result.errors is None
    assert result.data == {"log": None}


async def test_a_malformed_id_is_a_clean_graphql_error(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``log(id: "not-a-number")`` is a bad request, not a miss and not a 500.

    Folding it into "not found" would tell a client with a broken id-building bug that the row
    simply does not exist, and it would go on believing that forever. What must not happen either
    is an unhandled ``ValueError`` from ``int()`` reaching the executor.
    """
    result = await _execute(gql_context, LOG_BY_ID_DOCUMENT, id="not-a-number")

    assert result.errors
    assert result.data == {"log": None}, "the field is nullable, so only it is nulled"
    assert "not-a-number" in result.errors[0].message


# --- logsConnection: the cursor-pagination bonus ---------------------------------------------------


async def _page(
    context: Context,
    *,
    first: Optional[int] = None,
    after: Optional[str] = None,
    filters: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Fetch one connection page, asserting the response carried no errors."""
    result = await schema.execute(
        CONNECTION_DOCUMENT,
        variable_values={"filters": filters, "first": first, "after": after},
        context_value=context,
    )

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result.data["logsConnection"]


async def test_walking_every_page_yields_each_entry_exactly_once(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """The property that makes pagination worth having: no duplicates, no skips, no early stop.

    37 is deliberately not a divisor of 1200, so the final page is short and the ``hasNextPage``
    logic is exercised at a ragged boundary rather than at a clean one. An OFFSET-based
    implementation would pass this against a static table and fail the moment a row was inserted
    mid-walk; a keyset cursor is stable by construction, which is why it is the one used.
    """
    page_size = 37
    seen: list[int] = []
    after: Optional[str] = None
    pages = 0

    while True:
        page = await _page(gql_context, first=page_size, after=after)
        pages += 1

        assert page["totalCount"] == CORPUS_SIZE, "totalCount must not change as we page"
        assert page["pageInfo"]["hasPreviousPage"] is (after is not None)
        assert len(page["edges"]) <= page_size

        seen.extend(int(edge["node"]["id"]) for edge in page["edges"])

        if not page["pageInfo"]["hasNextPage"]:
            break

        after = page["pageInfo"]["endCursor"]
        assert after is not None, "hasNextPage is true, so there must be a cursor to continue from"
        assert pages < 100, "the walk is not terminating; the cursor is probably not advancing"

    assert pages == 33, "1200 rows in pages of 37 is 32 full pages and a short one"
    assert len(seen) == CORPUS_SIZE, "every row was returned"
    assert len(set(seen)) == CORPUS_SIZE, "and none of them twice"
    assert seen == sorted(seen, reverse=True), "in the same total order the list field uses"


async def test_the_first_page_matches_the_head_of_the_list_field(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """The connection and ``logs`` are two windows onto one result, not two results.

    If they ever diverged, a dashboard that paginated would show a different dataset from one that
    did not — and both would look internally consistent.
    """
    page = await _page(gql_context, first=25)
    rows = await _logs(gql_context, {"limit": 25})

    assert [edge["node"]["id"] for edge in page["edges"]] == _ids(rows)


async def test_has_next_page_is_exact_at_the_boundary(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Asking for exactly the whole result says "no more"; one fewer says "more".

    This is the off-by-one the lookahead probe exists to get right, and it is the difference
    between a client that stops and a client that loops forever on an empty final page.
    """
    exact = await _page(gql_context, first=CORPUS_SIZE)
    one_short = await _page(gql_context, first=CORPUS_SIZE - 1)

    assert len(exact["edges"]) == CORPUS_SIZE
    assert exact["pageInfo"]["hasNextPage"] is False
    assert len(one_short["edges"]) == CORPUS_SIZE - 1
    assert one_short["pageInfo"]["hasNextPage"] is True


async def test_total_count_is_the_filtered_total_and_agrees_with_the_repository(
    seeded: list[LogRecord], gql_context: Context, repo: LogRepository
) -> None:
    """``totalCount`` counts what matches the filters — not the page, and not the remainder.

    Cross-checked against :meth:`~src.db.repository.LogRepository.count_logs` (a separate SQL
    statement) *and* against the Python oracle, so agreement between the two is not an accident of
    both reading the same wrong thing.
    """
    service = "order-service"
    expected = matching(seeded, lambda r: r.service == service)

    page = await _page(gql_context, first=5, filters={"service": service})

    assert len(page["edges"]) == 5, "the page is small..."
    assert page["totalCount"] == len(expected), "...but the count is of everything that matches"
    assert page["totalCount"] == await repo.count_logs(LogQuery(service=service))


async def test_an_empty_connection_reports_no_cursors(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """No rows means no ``startCursor``/``endCursor`` and no next page — not an error."""
    page = await _page(gql_context, first=10, filters={"service": "no-such-service"})

    assert page["edges"] == []
    assert page["totalCount"] == 0
    assert page["pageInfo"]["hasNextPage"] is False
    assert page["pageInfo"]["startCursor"] is None
    assert page["pageInfo"]["endCursor"] is None


async def test_a_cursor_carries_the_filters_it_was_issued_under(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Paging a filtered connection stays inside the filter.

    A keyset cursor names a position in an *ordering*, not in a result set, so it is the caller's
    job to re-supply the filters on the next page — and the resolver's job to apply both the filter
    predicates and the cursor predicate together. Dropping either one here is the classic
    keyset-pagination bug: page 2 of a filtered query quietly returns unfiltered rows.
    """
    service = "auth-service"
    expected = matching(seeded, lambda r: r.service == service)
    assert len(expected) > 20, "the corpus must hold enough rows for two pages"

    first_page = await _page(gql_context, first=10, filters={"service": service})
    second_page = await _page(
        gql_context,
        first=10,
        after=first_page["pageInfo"]["endCursor"],
        filters={"service": service},
    )

    combined = [edge["node"] for edge in first_page["edges"] + second_page["edges"]]

    assert len(combined) == 20
    assert all(node["service"] == service for node in combined)
    # Graded against the oracle, not merely checked for the right `service`: a resolver that
    # applied the filter but dropped the cursor would also return 20 auth-service rows — the
    # *first* 10 twice. Comparing against the newest 20 the corpus says exist catches that.
    assert [node["message"] for node in combined] == [
        record.message for record in expected[:20]
    ]
    assert len({node["id"] for node in combined}) == 20, "no row appears on both pages"


async def test_a_malformed_after_cursor_is_a_clean_graphql_error(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """A bad cursor is a readable error, never a traceback and never a 500.

    ``base64.b64decode`` raises ``binascii.Error`` and ``bytes.decode`` raises
    ``UnicodeDecodeError``; letting either escape the resolver would produce an internal error, and
    once C4 installs ``MaskErrors`` the client would be told "an unexpected error occurred" about a
    request it could have fixed itself.
    """
    result = await schema.execute(
        CONNECTION_DOCUMENT,
        variable_values={"filters": None, "first": 5, "after": "!!!not a cursor!!!"},
        context_value=gql_context,
    )

    assert result.errors
    assert "cursor" in result.errors[0].message.lower()
    # `logsConnection` is non-null, so nulling it propagates to the root — that is the GraphQL
    # spec's behaviour, and it is asserted here so a later nullability change is noticed.
    assert result.data is None


async def test_a_cursor_from_a_different_scheme_is_rejected_rather_than_misread(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """A well-formed base64 blob that is not one of ours does not silently become a position."""
    foreign = base64.urlsafe_b64encode(b"offset:250").decode("ascii").rstrip("=")

    result = await schema.execute(
        CONNECTION_DOCUMENT,
        variable_values={"filters": None, "first": 5, "after": foreign},
        context_value=gql_context,
    )

    assert result.errors
    assert result.data is None


# --- Over HTTP, through the assembled application --------------------------------------------------


async def test_the_mounted_router_answers_the_spec_curl_command(
    seeded: list[LogRecord], http_client: httpx.AsyncClient
) -> None:
    """The spec's §5 command, end to end: HTTP POST -> router -> context getter -> Postgres.

    Everything above executes against ``schema`` directly, which never touches the mount, the
    FastAPI dependency that builds the context, or the JSON envelope. This is the test that fails
    if ``/graphql`` is not registered, if ``get_context`` cannot reach ``app.state.db``, or if the
    router is shadowed by another route — none of which any schema-level test can see.
    """
    response = await http_client.post(
        "/graphql", json={"query": SPEC_ACCEPTANCE_DOCUMENT}
    )

    assert response.status_code == 200
    payload = response.json()

    assert "errors" not in payload, payload.get("errors")
    rows = payload["data"]["logs"]
    assert rows, "the seeded corpus must come back through the mounted router"
    assert all(set(row) == {"id", "service", "level", "message"} for row in rows)


async def test_a_variable_driven_filtered_query_returns_only_matching_rows_over_http(
    seeded: list[LogRecord], http_client: httpx.AsyncClient
) -> None:
    """The spec's second §5 verification command, over the real transport.

    Variables travel as JSON here, which is a different coercion path from an inline literal — and
    the one every real client uses.
    """
    service = "payment-service"
    expected = matching(seeded, lambda r: r.service == service and r.level == "ERROR")
    assert expected, "the corpus must contain payment-service errors"

    response = await http_client.post(
        "/graphql",
        json={
            "query": LOGS_DOCUMENT,
            "variables": {"filters": {"service": service, "level": "ERROR", "limit": CORPUS_SIZE}},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "errors" not in payload, payload.get("errors")

    assert _records(payload["data"]["logs"]) == expected


async def test_a_graphql_error_is_a_200_with_an_errors_envelope_not_a_500(
    seeded: list[LogRecord], http_client: httpx.AsyncClient
) -> None:
    """Spec §2 item 24, at the HTTP boundary.

    GraphQL reports failures inside the response body; a transport-level 500 would break every
    client that reads ``data``/``errors``, and would leak whatever the framework decided to print.
    Asserted over HTTP because the status code simply does not exist at the schema level.
    """
    response = await http_client.post(
        "/graphql", json={"query": '{ logs(filters: {level: NOT_A_LEVEL}) { id } }'}
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload.get("errors"), "the failure must be reported in the envelope"
    assert payload.get("data") is None
    assert "Traceback" not in response.text


async def test_the_playground_is_served_on_get(http_client: httpx.AsyncClient) -> None:
    """Spec §2 item 13: ``GET /graphql`` serves an interactive IDE while it is enabled.

    Gated on ``GRAPHQL_PLAYGROUND_ENABLED``, which defaults to true and is what the test container
    runs with. POST is unaffected by the setting either way, so turning the IDE off in a hostile
    environment does not turn the API off.
    """
    response = await http_client.get("/graphql")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "graphiql" in response.text.lower()


async def test_health_still_answers_after_the_graphql_router_is_mounted(
    http_client: httpx.AsyncClient,
) -> None:
    """Route order is a correctness property, so it gets an assertion rather than a comment.

    ``/health`` is registered before ``/graphql`` and stays dependency-free; the container
    HEALTHCHECK and compose's ``condition: service_healthy`` both target it, and C13 adds an SPA
    catch-all that must not shadow either route. This is the cheap regression guard for that
    ordering.
    """
    response = await http_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}
