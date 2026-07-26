"""``Query.logStats`` against the real ``gqllogs_test`` PostgreSQL database — spec §2 item 23.

.. rubric:: Every number is graded against the generator oracle, not against itself

The expected total, error count and service set are computed **in Python** from the objects
:func:`~src.generators.generate_log_records` returned, and the response is asserted to equal exactly
that. That is a comparison between two independent computations. The tempting alternative —
asserting ``totalLogs > 0``, or that ``errorCount <= totalLogs`` — passes against an aggregate that
counts the wrong rows, and passes just as happily against one that silently stops at
``MAX_QUERY_LIMIT``.

That last failure is the specific reason the corpus size matters here. ``CORPUS_SIZE`` is 1200 and
``DEFAULT_QUERY_LIMIT`` is 100: an implementation that fetched rows and counted them in Python would
report **100**, a plausible number, and only an oracle comparison catches it.

.. rubric:: And the two views are graded against each other

``logStats`` and ``logs`` are two answers to the same question over the same window, computed by
different SQL. A test that pins their *agreement* catches a whole class of bug neither can catch
alone: a predicate that means one thing in the aggregate and another in the list — an exclusive
bound, a missing UTC normalisation, a filter applied to one statement and not the other. Both would
look internally consistent and would disagree with each other, which is exactly what a dashboard
showing a summary above a table would surface to a user as "these numbers do not add up".
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Optional

import httpx
import pytest

from src.db.models import LogRecord
from src.db.repository import ERROR_LEVEL
from src.graphql.context import Context
from src.graphql.schema import schema
from tests.integration.corpus import CORPUS_SIZE

#: The spec's §5 acceptance command for stats, **verbatim**. Selecting ``services`` as a leaf is
#: the whole reason it is published as ``[String!]!`` — see the block comment above ``LogStats``.
SPEC_STATS_DOCUMENT = "{ logStats { totalLogs errorCount services } }"

#: Everything ``logStats`` publishes, for the tests that grade the full response.
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

WINDOW_LOGS_DOCUMENT = """
query WindowLogs($filters: LogFilterInput) {
  logs(filters: $filters) { id timestamp service level }
}
"""


async def _stats(
    context: Context,
    *,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> dict[str, Any]:
    """Run ``STATS_DOCUMENT`` over a window, asserting the response carried no errors."""
    result = await schema.execute(
        STATS_DOCUMENT,
        variable_values={
            "startTime": start_time.isoformat() if start_time else None,
            "endTime": end_time.isoformat() if end_time else None,
        },
        context_value=context,
    )

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result.data["logStats"]


# --- The spec's own acceptance command --------------------------------------------------------------


async def test_the_spec_acceptance_query_returns_a_statistical_summary(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``{ logStats { totalLogs errorCount services } }`` — spec §5, executed as written.

    Two things are under test and only one of them is the numbers. The other is that the document
    **parses at all**: ``services`` is selected as a leaf, so publishing it as a list of objects
    would make this a validation error rather than a wrong answer. That is why the richer
    per-service view lives on ``serviceBreakdown`` instead of replacing this field.
    """
    result = await schema.execute(SPEC_STATS_DOCUMENT, context_value=gql_context)

    assert result.errors is None, (
        f"the spec's acceptance command must validate and execute as written: {result.errors}"
    )
    assert result.data is not None
    stats = result.data["logStats"]

    assert set(stats) == {"totalLogs", "errorCount", "services"}, (
        "only the requested fields come back (spec §2 item 28)"
    )
    assert stats["totalLogs"] == CORPUS_SIZE
    assert stats["errorCount"] == sum(1 for record in seeded if record.level == ERROR_LEVEL)
    assert sorted(stats["services"]) == sorted({record.service for record in seeded})


# --- Graded against the oracle ----------------------------------------------------------------------


async def test_total_logs_is_the_whole_corpus_not_the_default_page(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """1200, not 100 — the assertion that fails against a count done in Python over fetched rows.

    ``DEFAULT_QUERY_LIMIT`` is 100 and ``CORPUS_SIZE`` is 1200, so the two are impossible to
    confuse. An aggregate built on ``list_logs`` would report the clamped page size and nothing
    about the response would look wrong.
    """
    stats = await _stats(gql_context)

    assert stats["totalLogs"] == CORPUS_SIZE
    assert stats["totalLogs"] > gql_context.settings.default_query_limit


async def test_error_count_is_the_oracles_error_count_and_is_error_only(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``errorCount`` counts ``ERROR`` and nothing else.

    The corpus carries ~10% ERROR and ~1% CRITICAL, so "ERROR only" and "ERROR plus CRITICAL" are
    genuinely different numbers here — the second assertion is what makes the first able to fail
    for the right reason. A client wanting "errors and worse" sums ``levelBreakdown``, which is
    why that field exists.
    """
    expected_errors = sum(1 for record in seeded if record.level == ERROR_LEVEL)
    expected_critical = sum(1 for record in seeded if record.level == "CRITICAL")

    stats = await _stats(gql_context)

    assert expected_errors > 0 and expected_critical > 0, "the corpus must contain both severities"
    assert stats["errorCount"] == expected_errors
    assert stats["errorCount"] != expected_errors + expected_critical


async def test_services_is_exactly_the_distinct_services_in_the_corpus(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Set equality, so both a missing service and an invented one fail."""
    expected = {record.service for record in seeded}

    stats = await _stats(gql_context)

    assert set(stats["services"]) == expected
    assert len(stats["services"]) == len(expected), "no duplicates: this is a GROUP BY, not a scan"


async def test_the_service_breakdown_carries_the_oracles_counts_busiest_first(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Per-service counts graded individually, plus the published ordering.

    The ordering matters beyond aesthetics: it is what ``services`` is derived from, so a broken
    sort would silently reorder the spec's field too.
    """
    expected = Counter(record.service for record in seeded)

    stats = await _stats(gql_context)
    breakdown = stats["serviceBreakdown"]

    assert {entry["service"]: entry["count"] for entry in breakdown} == dict(expected)

    counts = [entry["count"] for entry in breakdown]
    assert counts == sorted(counts, reverse=True), "busiest first"
    assert stats["services"] == [entry["service"] for entry in breakdown], (
        "`services` is derived from the breakdown, so the two can never disagree about which "
        "services exist or in what order"
    )


async def test_the_level_breakdown_carries_the_oracles_counts_in_severity_order(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Counts per severity, ordered the way the ``LogLevel`` enum is declared (ascending)."""
    expected = Counter(record.level for record in seeded)

    stats = await _stats(gql_context)
    breakdown = stats["levelBreakdown"]

    assert {entry["level"]: entry["count"] for entry in breakdown} == dict(expected)

    severity = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    returned = [entry["level"] for entry in breakdown]
    assert returned == [level for level in severity if level in returned]


async def test_both_breakdowns_sum_to_the_total_which_a_separate_statement_produced(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """The cross-check between the two SQL statements ``logStats`` issues.

    ``totalLogs``/``errorCount`` come from a scalar aggregate; the breakdowns come from a
    ``GROUP BY``. Nothing forces them to agree — a predicate applied to one and not the other, or a
    ``LIMIT`` sneaking onto the grouped statement, would leave both looking perfectly reasonable in
    isolation. Their sum is the assertion that notices.
    """
    stats = await _stats(gql_context)

    assert sum(entry["count"] for entry in stats["serviceBreakdown"]) == stats["totalLogs"]
    assert sum(entry["count"] for entry in stats["levelBreakdown"]) == stats["totalLogs"]

    errors_from_breakdown = next(
        entry["count"] for entry in stats["levelBreakdown"] if entry["level"] == ERROR_LEVEL
    )
    assert errors_from_breakdown == stats["errorCount"]


async def test_earliest_and_latest_report_the_span_the_data_actually_covers(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``min``/``max`` over the matching rows, not an echo of the requested window.

    The generator emits oldest-first with strictly increasing timestamps, so the oracle's first and
    last records are the exact instants PostgreSQL must report.
    """
    stats = await _stats(gql_context)

    assert datetime.fromisoformat(stats["earliest"]) == seeded[0].timestamp
    assert datetime.fromisoformat(stats["latest"]) == seeded[-1].timestamp


# --- Windows ------------------------------------------------------------------------------------------


async def test_a_time_window_narrows_every_number_consistently(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """A window in the middle of the corpus, graded against the oracle over the same window.

    Asserted to be a *proper* subset at both ends, so a resolver that ignored the bounds (returning
    the whole corpus) and one that dropped everything both fail.
    """
    start = seeded[300].timestamp
    end = seeded[800].timestamp
    inside = [record for record in seeded if start <= record.timestamp <= end]

    stats = await _stats(gql_context, start_time=start, end_time=end)

    assert 0 < len(inside) < CORPUS_SIZE
    assert stats["totalLogs"] == len(inside) == 501, "both bounds inclusive: 300..800 is 501 rows"
    assert stats["errorCount"] == sum(1 for r in inside if r.level == ERROR_LEVEL)
    assert set(stats["services"]) == {record.service for record in inside}
    assert datetime.fromisoformat(stats["earliest"]) == inside[0].timestamp
    assert datetime.fromisoformat(stats["latest"]) == inside[-1].timestamp


async def test_one_bound_alone_leaves_the_other_end_unbounded(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """Omitted means unbounded — the same rule ``LogFilterInput`` follows."""
    start = seeded[1000].timestamp

    from_start = await _stats(gql_context, start_time=start)
    until_start = await _stats(gql_context, end_time=start)

    assert from_start["totalLogs"] == CORPUS_SIZE - 1000
    assert until_start["totalLogs"] == 1001, "inclusive on both ends, so the pivot row is in both"
    assert from_start["totalLogs"] + until_start["totalLogs"] == CORPUS_SIZE + 1


async def test_stats_and_logs_agree_over_the_same_window(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """The consistency property, pinned. Worth more than either query graded alone.

    Both are asked for the identical window through the identical predicate builder, so every
    number the summary reports must be reproducible by counting the rows the list returns. A
    divergence here means a filter means two different things depending on which field a client
    asked for — and both answers would look right on their own.

    The list query asks for ``limit: CORPUS_SIZE`` deliberately: the suite raises
    ``MAX_QUERY_LIMIT`` above the corpus so this comparison sees the whole window rather than a
    clamped prefix, which would make the two disagree for an uninteresting reason.
    """
    start = seeded[150].timestamp
    end = seeded[950].timestamp

    stats = await _stats(gql_context, start_time=start, end_time=end)
    listed = await schema.execute(
        WINDOW_LOGS_DOCUMENT,
        variable_values={
            "filters": {
                "startTime": start.isoformat(),
                "endTime": end.isoformat(),
                "limit": CORPUS_SIZE,
            }
        },
        context_value=gql_context,
    )

    assert listed.errors is None, f"unexpected errors: {listed.errors}"
    assert listed.data is not None
    rows = listed.data["logs"]

    assert len(rows) < CORPUS_SIZE, "the window must be a proper subset or this proves nothing"
    assert stats["totalLogs"] == len(rows)
    assert stats["errorCount"] == sum(1 for row in rows if row["level"] == ERROR_LEVEL)
    assert set(stats["services"]) == {row["service"] for row in rows}
    assert {entry["service"]: entry["count"] for entry in stats["serviceBreakdown"]} == dict(
        Counter(row["service"] for row in rows)
    )
    assert {entry["level"]: entry["count"] for entry in stats["levelBreakdown"]} == dict(
        Counter(row["level"] for row in rows)
    )
    timestamps = [datetime.fromisoformat(row["timestamp"]) for row in rows]
    assert datetime.fromisoformat(stats["earliest"]) == min(timestamps)
    assert datetime.fromisoformat(stats["latest"]) == max(timestamps)


async def test_an_empty_window_returns_zeros_rather_than_an_error(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """"Nothing happened" is an answer a dashboard has to be able to render.

    The nullable ``earliest``/``latest`` exist for exactly this row: SQL ``min``/``max`` over no
    rows is ``NULL``, and reporting the requested window instead would invent a span the data does
    not have.
    """
    future = seeded[-1].timestamp + timedelta(days=365)

    stats = await _stats(gql_context, start_time=future)

    assert stats["totalLogs"] == 0
    assert stats["errorCount"] == 0
    assert stats["services"] == []
    assert stats["serviceBreakdown"] == []
    assert stats["levelBreakdown"] == []
    assert stats["earliest"] is None
    assert stats["latest"] is None


async def test_stats_over_an_empty_table_are_zeros_too(gql_context: Context) -> None:
    """No ``seeded`` fixture: the table is empty, which is a normal state, not an edge case.

    The compose ``test`` service runs with ``SEED_ENTRIES=0``, so this is literally how a fresh
    deployment answers its first stats request.
    """
    stats = await _stats(gql_context)

    assert stats["totalLogs"] == 0
    assert stats["services"] == []
    assert stats["earliest"] is None


# --- Validation ---------------------------------------------------------------------------------------


async def test_an_inverted_window_is_a_typed_validation_error_not_a_silent_zero(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``startTime > endTime`` can never match, so zeros would be indistinguishable from a quiet hour.

    The code is what a client branches on: ``VALIDATION_ERROR`` says "fix the request", and any
    other code (or none) would leave a dashboard unable to tell a user why its date pickers
    produced nothing.
    """
    start = seeded[-1].timestamp
    end = seeded[0].timestamp

    result = await schema.execute(
        STATS_DOCUMENT,
        variable_values={"startTime": start.isoformat(), "endTime": end.isoformat()},
        context_value=gql_context,
    )

    assert result.errors
    error = result.errors[0]
    assert error.extensions["code"] == "VALIDATION_ERROR"
    assert "startTime" in error.message and "endTime" in error.message
    assert "Traceback" not in error.message


# --- Over HTTP, through the assembled application --------------------------------------------------


async def test_the_spec_stats_command_answers_over_http(
    seeded: list[LogRecord], http_client: httpx.AsyncClient
) -> None:
    """Spec §5 end to end: HTTP POST -> router -> context getter -> Postgres -> JSON envelope.

    Everything above executes against ``schema`` directly, which never touches the mount, the
    FastAPI dependency that builds the context, or the JSON serialisation of the response. A
    ``DateTime`` that failed to serialise, or a field name the router mangled, is only visible here.
    """
    response = await http_client.post("/graphql", json={"query": SPEC_STATS_DOCUMENT})

    assert response.status_code == 200
    payload = response.json()

    assert "errors" not in payload, payload.get("errors")
    stats = payload["data"]["logStats"]
    assert stats["totalLogs"] == CORPUS_SIZE
    assert stats["errorCount"] == sum(1 for record in seeded if record.level == ERROR_LEVEL)
    assert sorted(stats["services"]) == sorted({record.service for record in seeded})


@pytest.mark.parametrize("field", ["earliest", "latest"])
async def test_the_timestamps_serialise_as_iso_strings_over_http(
    field: str, seeded: list[LogRecord], http_client: httpx.AsyncClient
) -> None:
    """The nullable ``DateTime`` fields survive JSON serialisation with their UTC offset intact.

    An offset-less timestamp on the wire is unusable: the client has to guess a zone, and every
    client guesses its own. Asserted through HTTP because the scalar's serialisation is exactly
    what ``schema.execute`` returns as a ``datetime`` object and never renders.
    """
    response = await http_client.post(
        "/graphql", json={"query": "{ logStats { earliest latest } }"}
    )

    assert response.status_code == 200
    value = response.json()["data"]["logStats"][field]
    assert isinstance(value, str), "the seeded corpus is non-empty, so neither bound is null"

    parsed = datetime.fromisoformat(value)
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timedelta(0), "stored and published in UTC"
