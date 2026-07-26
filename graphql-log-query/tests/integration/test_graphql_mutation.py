"""``Mutation.createLog`` against the real ``gqllogs_test`` PostgreSQL database — spec §2 item 24.

.. rubric:: The assertion that matters is not the mutation's own response

A ``createLog`` that returned a well-formed ``LogEntry`` and never committed would satisfy every
test that reads the mutation payload. So the tests below are built around **round trips**: the row
is fetched back by a separate query, through a separate session, and graded there. Reading the row
in SQL — not through the same ORM instance that was just built in Python — is what makes the write
path actually under test.

The same principle drives the ``metadata`` test, which asks PostgreSQL how the column is stored
rather than asking Python what it deserialises to. See
:func:`tests.integration.corpus.metadata_storage`: a JSONB scalar ``'null'`` and a SQL ``NULL`` are
both ``None`` in Python, so the obvious assertion cannot fail for the condition it documents.

.. rubric:: And the error tests assert on absence

"Errors are GraphQL-shaped, never a stack trace" (spec §2 item 35) is a claim about what is **not**
in a response, so the masking test checks the raw HTTP body for the exception's message, its type
name and any ``/app/...`` path. Asserting only that an error came back would pass against a
response carrying the entire traceback.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.models import LogRecord
from src.db.repository import LogQuery, LogRepository
from src.graphql.context import Context
from src.graphql.errors import MASKED_ERROR_MESSAGE
from src.graphql.schema import schema
from src.graphql.validation import MAX_SERVICE_LENGTH, MAX_TRACE_ID_LENGTH
from tests.integration.corpus import CORPUS_SIZE, metadata_storage

#: The spec's §5 acceptance command shape: the argument is ``logData`` and the selection is the
#: created object. Kept as a module constant so no test drifts into checking a different name.
SPEC_MUTATION_DOCUMENT = """
mutation Create($logData: CreateLogInput!) {
  createLog(logData: $logData) { id service }
}
"""

#: Every published field of the created entry, for the tests that grade the whole object.
CREATE_DOCUMENT = """
mutation Create($logData: CreateLogInput!) {
  createLog(logData: $logData) {
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

LOGS_DOCUMENT = """
query Logs($filters: LogFilterInput) {
  logs(filters: $filters) { id timestamp service level message metadata traceId }
}
"""

STATS_DOCUMENT = "{ logStats { totalLogs errorCount services } }"


def _payload(**overrides: Any) -> dict[str, Any]:
    """A minimal valid ``logData`` payload, with fields replaced or added by keyword."""
    payload: dict[str, Any] = {
        "service": "payment-service",
        "level": "ERROR",
        "message": "payment authorization declined for order ord-99999",
    }
    payload.update(overrides)
    return payload


async def _create(context: Context, document: str = CREATE_DOCUMENT, **fields: Any) -> dict[str, Any]:
    """Run ``createLog`` and return the created entry, asserting the response carried no errors."""
    result = await schema.execute(
        document,
        variable_values={"logData": _payload(**fields)},
        context_value=context,
    )

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result.data["createLog"]


async def _create_raw(context: Context, **fields: Any) -> Any:
    """Run ``createLog`` and return the raw ``ExecutionResult`` — for the failure cases."""
    return await schema.execute(
        CREATE_DOCUMENT,
        variable_values={"logData": _payload(**fields)},
        context_value=context,
    )


async def _logs(context: Context, filters: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
    """Run a filtered ``logs`` query and return the entries."""
    result = await schema.execute(
        LOGS_DOCUMENT, variable_values={"filters": filters}, context_value=context
    )

    assert result.errors is None, f"unexpected errors: {result.errors}"
    assert result.data is not None
    return result.data["logs"]


# --- The spec's own acceptance command --------------------------------------------------------------


async def test_the_spec_mutation_creates_a_record(gql_context: Context) -> None:
    """``mutation { createLog(logData: {…}) { id service } }`` — spec §5, argument name and all.

    The selection is the spec's two fields, so this also pins that only what was asked for comes
    back: a mutation that returned the whole row would ship ``metadata`` and ``traceId`` to a
    client that requested neither.
    """
    entry = await _create(gql_context, SPEC_MUTATION_DOCUMENT)

    assert set(entry) == {"id", "service"}
    assert entry["service"] == "payment-service"
    assert entry["id"].isdigit(), "the server assigns a BIGSERIAL id and publishes it as an ID"
    assert int(entry["id"]) > 0


# --- The real data-flow assertion: the row is visible to a follow-up query ---------------------------


async def test_a_created_entry_is_returned_by_a_following_logs_query(
    gql_context: Context,
) -> None:
    """The write actually committed, and the read path can see it.

    This is the assertion a mutation that built a fine-looking response and never committed would
    fail. The follow-up query runs in a **different session** — every resolver opens its own — so
    an uncommitted row would be invisible to it, exactly as it would be to another process.
    """
    created = await _create(
        gql_context,
        service="order-service",
        level="WARNING",
        message="queue ingest backlog grew to 4210 messages",
        metadata={"host": "node-9", "region": "us-east-1"},
        traceId="feedface00000001",
    )

    rows = await _logs(gql_context, {"service": "order-service"})

    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == created["id"]
    assert row["service"] == "order-service"
    assert row["level"] == "WARNING"
    assert row["message"] == "queue ingest backlog grew to 4210 messages"
    assert row["metadata"] == {"host": "node-9", "region": "us-east-1"}
    assert row["traceId"] == "feedface00000001"
    assert row["timestamp"] == created["timestamp"]


async def test_the_created_entry_is_readable_from_the_database_by_another_session(
    gql_context: Context, repo: LogRepository
) -> None:
    """The strongest form of the same claim: the row is in PostgreSQL, fetched by primary key.

    Through the test's own session — a different connection from the one the mutation committed on
    — so nothing here can be answered out of the mutation's identity map. This is what "committed"
    means operationally: another session can see it.
    """
    created = await _create(
        gql_context, service="search-service", level="INFO", message="index rebuild finished"
    )

    stored = await repo.get_by_id(int(created["id"]))

    assert stored is not None
    assert stored.service == "search-service"
    assert stored.level == "INFO"
    assert stored.message == "index rebuild finished"
    assert stored.timestamp.tzinfo is not None, "stored as timestamptz, read back aware"
    assert await repo.count_logs(LogQuery()) == 1, "exactly one row, so nothing was written twice"


async def test_creating_one_entry_increments_total_logs_by_exactly_one(
    seeded: list[LogRecord], gql_context: Context
) -> None:
    """``logStats`` and ``createLog`` are two halves of the same store, so the number moves by one.

    Against a seeded corpus rather than an empty table on purpose: 1200 -> 1201 is a fact about
    counting, while 0 -> 1 would also pass against an aggregate that returned "how many rows did
    this request write".
    """
    before = (await schema.execute(STATS_DOCUMENT, context_value=gql_context)).data
    assert before is not None
    assert before["logStats"]["totalLogs"] == CORPUS_SIZE

    await _create(gql_context, level="CRITICAL", message="primary replica lost, shutting down")

    after = (await schema.execute(STATS_DOCUMENT, context_value=gql_context)).data
    assert after is not None
    assert after["logStats"]["totalLogs"] == CORPUS_SIZE + 1
    assert after["logStats"]["errorCount"] == before["logStats"]["errorCount"], (
        "a CRITICAL entry must not move errorCount — the field counts ERROR only"
    )

    await _create(gql_context, level="ERROR")

    final = (await schema.execute(STATS_DOCUMENT, context_value=gql_context)).data
    assert final is not None
    assert final["logStats"]["totalLogs"] == CORPUS_SIZE + 2
    assert final["logStats"]["errorCount"] == before["logStats"]["errorCount"] + 1


# --- Defaults and storage -----------------------------------------------------------------------------


async def test_an_omitted_timestamp_gets_a_server_assigned_utc_instant(
    gql_context: Context,
) -> None:
    """Bracketed by two real clock reads, so "now" is asserted rather than assumed.

    The bracket is what makes this able to fail: an assertion that the timestamp is merely
    non-null passes against a hardcoded epoch, and one that it is "recent" passes against a value
    the client sent.
    """
    before = datetime.now(timezone.utc)
    entry = await _create(gql_context)
    after = datetime.now(timezone.utc)

    assigned = datetime.fromisoformat(entry["timestamp"])

    assert assigned.tzinfo is not None, "naive timestamps are compared in the server's zone"
    assert assigned.utcoffset() == timedelta(0)
    assert before <= assigned <= after


async def test_a_supplied_timestamp_is_stored_verbatim_and_a_naive_one_becomes_utc(
    gql_context: Context,
) -> None:
    """A client that knows when the event happened is believed; only its zone is normalised."""
    moment = datetime(2026, 7, 26, 9, 30, tzinfo=timezone.utc)

    aware = await _create(gql_context, timestamp=moment.isoformat(), message="aware")
    naive = await _create(gql_context, timestamp="2026-07-26T09:30:00", message="naive")

    assert datetime.fromisoformat(aware["timestamp"]) == moment
    assert datetime.fromisoformat(naive["timestamp"]) == moment, (
        "a naive value is interpreted as UTC, the same rule every other path applies"
    )


async def test_metadata_round_trips_as_a_real_jsonb_object(
    gql_context: Context, session: AsyncSession
) -> None:
    """Values keep their JSON types, and the column holds an ``object`` rather than a string.

    ``503`` coming back as an ``int`` is the part a naive "serialise the dict to text" storage
    would fail, and it is invisible in a response body — ``503`` and ``"503"`` differ by two
    characters in the raw JSON and by nothing at all in a casual assertion.
    """
    payload = {"host": "node-3", "region": "eu-west-1", "latency_ms": 42, "status_code": 503}

    entry = await _create(gql_context, metadata=payload)

    assert entry["metadata"] == payload
    assert isinstance(entry["metadata"]["status_code"], int)

    is_sql_null, json_type = await metadata_storage(session, int(entry["id"]))
    assert not is_sql_null
    assert json_type == "object", f"the column must hold a JSONB object, got {json_type!r}"


async def test_omitted_metadata_is_stored_as_sql_null_not_the_json_scalar_null(
    gql_context: Context, session: AsyncSession
) -> None:
    """Asked of PostgreSQL, because Python cannot tell the two apart.

    asyncpg deserialises both SQL ``NULL`` and the JSONB scalar ``'null'`` to ``None``, so
    ``entry["metadata"] is None`` is true in either world — it was true, for the whole of C2, while
    the column held the wrong thing. The difference only surfaces in SQL, and only bites later:
    ``WHERE metadata IS NULL`` matches nothing, and C11's aggregations over "rows that have
    metadata" quietly count the entire table.

    The Python-visible assertion stays, because it is still the contract a client sees. It is just
    no longer the only one.
    """
    entry = await _create(gql_context, message="health check passed")

    assert entry["metadata"] is None

    is_sql_null, json_type = await metadata_storage(session, int(entry["id"]))
    assert is_sql_null, (
        "omitted metadata must be SQL NULL; `metadata IS NULL` was false, so the column holds the "
        "JSONB scalar 'null' (SQLAlchemy's JSONB defaults to none_as_null=False)"
    )
    assert json_type is None, (
        f"jsonb_typeof(metadata) must be SQL NULL for an absent value, got {json_type!r}"
    )


async def test_an_omitted_trace_id_is_null_which_is_what_related_logs_needs(
    gql_context: Context,
) -> None:
    """Spec §2 item 17 depends on this: a NULL trace id yields an empty ``relatedLogs`` at C5."""
    entry = await _create(gql_context)

    assert entry["traceId"] is None


# --- Validation, through the real GraphQL path --------------------------------------------------------
#
# Each case is the same rule the unit suite proves in isolation, re-asserted through an actual
# execution — because a rule that is correct and never called protects nothing, and because the
# CODE is only observable once the error has been through graphql-core's wrapping.


@pytest.mark.parametrize(
    ("case", "fields"),
    [
        ("blank service", {"service": "   "}),
        ("over-long service", {"service": "s" * (MAX_SERVICE_LENGTH + 1)}),
        ("blank message", {"message": "\t\n "}),
        ("blank trace id", {"traceId": "  "}),
        ("over-long trace id", {"traceId": "t" * (MAX_TRACE_ID_LENGTH + 1)}),
        ("nul byte in service", {"service": "auth\x00service"}),
        ("nul byte in message", {"message": "boom\x00"}),
        ("metadata is a scalar", {"metadata": 5}),
        ("metadata is an array", {"metadata": [1, 2, 3]}),
        ("metadata too deep", {"metadata": {"a": {"b": {"c": {"d": {"e": {"f": 1}}}}}}}),
        ("metadata too large", {"metadata": {"blob": "x" * 9000}}),
    ],
)
async def test_every_validation_rule_is_rejected_with_the_validation_error_code(
    case: str, fields: dict[str, Any], gql_context: Context, repo: LogRepository
) -> None:
    """A typed error, a message that survived masking, and — crucially — no row written.

    The row count is the half that would otherwise be assumed: a mutation that validated *after*
    inserting would return exactly this error and leave the entry behind, and every assertion
    about the error would still pass.
    """
    result = await _create_raw(gql_context, **fields)

    assert result.errors, f"{case} must be rejected"
    error = result.errors[0]
    assert error.extensions["code"] == "VALIDATION_ERROR", (
        f"{case} produced {error.extensions!r}; a client cannot branch on an untyped error"
    )
    assert error.message != MASKED_ERROR_MESSAGE, f"{case} must keep its real message"
    assert "Traceback" not in error.message

    assert await repo.count_logs(LogQuery()) == 0, f"{case} must not have written a row"


async def test_an_invalid_level_is_a_graphql_validation_error_not_a_500(
    gql_context: Context,
) -> None:
    """The enum is a validation boundary on the write path too, and it fires before execution.

    ``data`` is ``null`` because execution never started — graphql-core rejected the document
    during validation, with a message naming the offending value. That is a strictly better
    outcome than a resolver-level check: no session is opened, and the client is told what the
    legal values are.
    """
    result = await schema.execute(
        CREATE_DOCUMENT,
        variable_values={"logData": _payload(level="NOT_A_LEVEL")},
        context_value=gql_context,
    )

    assert result.errors
    assert result.data is None, "validation failed, so execution must not have started"
    assert "NOT_A_LEVEL" in str(result.errors[0])


async def test_a_missing_required_field_is_rejected_before_a_resolver_runs(
    gql_context: Context,
) -> None:
    """``service``, ``level`` and ``message`` are non-null in the input, so the schema enforces them."""
    result = await schema.execute(
        CREATE_DOCUMENT,
        variable_values={"logData": {"service": "auth-service", "level": "INFO"}},
        context_value=gql_context,
    )

    assert result.errors
    assert result.data is None
    assert "message" in str(result.errors[0])


# --- Over HTTP: the envelope, the status code, and what must NOT be in the body ------------------------


async def test_create_log_answers_over_http_and_the_row_is_visible_afterwards(
    http_client: httpx.AsyncClient,
) -> None:
    """The whole flow through the mounted router: POST -> commit -> a second POST reads it back.

    ``schema.execute`` never touches the mount, the ``context_getter`` dependency or the JSON
    envelope, so this is the test that fails if the mutation is unreachable through the transport
    a client actually uses.
    """
    create = await http_client.post(
        "/graphql",
        json={
            "query": SPEC_MUTATION_DOCUMENT,
            "variables": {"logData": _payload(service="notification-service")},
        },
    )

    assert create.status_code == 200
    created = create.json()
    assert "errors" not in created, created.get("errors")
    entry = created["data"]["createLog"]
    assert entry["service"] == "notification-service"

    read = await http_client.post(
        "/graphql",
        json={
            "query": LOGS_DOCUMENT,
            "variables": {"filters": {"service": "notification-service"}},
        },
    )

    assert read.status_code == 200
    rows = read.json()["data"]["logs"]
    assert [row["id"] for row in rows] == [entry["id"]]


async def test_a_validation_failure_is_a_200_with_an_errors_envelope(
    http_client: httpx.AsyncClient,
) -> None:
    """Spec §2 item 35 at the HTTP boundary: the failure is in the body, not in the status code."""
    response = await http_client.post(
        "/graphql",
        json={
            "query": SPEC_MUTATION_DOCUMENT,
            "variables": {"logData": _payload(service="")},
        },
    )

    assert response.status_code == 200
    payload = response.json()

    assert payload.get("errors"), "the failure must be reported in the envelope"
    assert payload["errors"][0]["extensions"]["code"] == "VALIDATION_ERROR"
    assert "service" in payload["errors"][0]["message"]
    assert "Traceback" not in response.text


async def test_an_unexpected_internal_failure_is_masked_on_the_wire(
    http_client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Force a genuine fault and assert on **absence**: nothing about it reaches the client.

    The forced exception deliberately carries an internal path and a distinctive token, because
    those are precisely what leaks when masking is missing — and an assertion that something is
    gone is the only kind that can fail for the right reason here. Checking ``response.text``
    rather than the parsed message covers the whole body, including anywhere a framework might
    have attached a traceback.

    A ``VALIDATION_ERROR`` in the same shape would prove nothing, which is why this monkeypatches
    the repository rather than sending a bad payload: the error has to originate *below* the
    validation layer, in code that never expected to fail.
    """

    async def _explode(self: LogRepository, **kwargs: Any) -> None:
        raise RuntimeError("kaboom-sentinel while writing /app/src/db/repository.py row")

    monkeypatch.setattr(LogRepository, "insert_log", _explode)

    response = await http_client.post(
        "/graphql",
        json={"query": SPEC_MUTATION_DOCUMENT, "variables": {"logData": _payload()}},
    )

    assert response.status_code == 200, "an internal failure is still a GraphQL envelope, not a 500"
    payload = response.json()

    assert payload.get("errors")
    error = payload["errors"][0]
    assert error["extensions"]["code"] == "INTERNAL_ERROR"
    assert error["message"] == MASKED_ERROR_MESSAGE

    body = response.text
    assert "kaboom-sentinel" not in body, "the exception message leaked to the client"
    assert "RuntimeError" not in body, "the exception type leaked to the client"
    assert "/app/" not in body, "an internal file path leaked to the client"
    assert "Traceback" not in body
    assert "repository.py" not in body


async def test_the_masked_failure_wrote_nothing(
    http_client: httpx.AsyncClient, gql_context: Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed mutation leaves the store exactly as it was — the transaction never committed.

    Compared before and after rather than asserted empty, so the test says what it means ("this
    request changed nothing") regardless of what the table happened to hold when it started.
    """
    before = await _logs(gql_context, None)

    async def _explode(self: LogRepository, **kwargs: Any) -> None:
        raise RuntimeError("kaboom-sentinel")

    monkeypatch.setattr(LogRepository, "insert_log", _explode)
    response = await http_client.post(
        "/graphql",
        json={"query": SPEC_MUTATION_DOCUMENT, "variables": {"logData": _payload()}},
    )
    monkeypatch.undo()

    assert response.json().get("errors"), "the mutation must have failed for this to prove anything"
    after = await _logs(gql_context, None)
    assert [row["id"] for row in after] == [row["id"] for row in before]
