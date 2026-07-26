"""Input validation and sanitisation rules — spec §2 item 34.

Pure: no database, no schema execution, no HTTP. Every rule is exercised against the functions in
:mod:`src.graphql.validation` directly, plus the two funnels that must call them
(:meth:`~src.graphql.inputs.LogFilterInput.to_log_query` and
:func:`~src.graphql.validation.validate_create_log`).

.. rubric:: Every rule is tested at its boundary, in both directions

A length cap tested only with an obviously-huge value passes against an off-by-one, against a cap
read from the wrong constant, and against a cap of 1. So each length rule is asserted twice: the
longest **accepted** value and the shortest **rejected** one, one character apart. The same
principle applies to the blank checks (``" "`` rejected, ``" x "`` accepted) and to the time range
(``start == end`` accepted, one microsecond later rejected).

.. rubric:: And every rejection is checked for a message that helps

``pytest.raises(ValidationError)`` alone would pass against ``raise ValidationError("")``. The
assertions below require the message to name the field and, where there is one, the limit — because
the entire point of validating at this layer rather than letting PostgreSQL raise is that the
client is told what to change.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.config import Settings
from src.graphql.enums import LogLevel
from src.graphql.errors import ErrorCode, ValidationError
from src.graphql.inputs import CreateLogInput, LogFilterInput
from src.graphql.validation import (
    MAX_METADATA_BYTES,
    MAX_METADATA_DEPTH,
    MAX_METADATA_NODES,
    MAX_MESSAGE_LENGTH,
    MAX_SEARCH_TEXT_LENGTH,
    MAX_SERVICE_LENGTH,
    MAX_TRACE_ID_LENGTH,
    validate_create_log,
    validate_log_filter,
    validate_metadata,
    validate_time_range,
)

UTC = timezone.utc
MOMENT = datetime(2026, 7, 26, 12, 0, 0, tzinfo=UTC)


def _create_input(**overrides: object) -> CreateLogInput:
    """A minimal valid ``createLog`` payload, with fields replaced by keyword."""
    payload: dict[str, object] = {
        "service": "auth-service",
        "level": LogLevel.INFO,
        "message": "user u-1001 authenticated successfully",
    }
    payload.update(overrides)
    return CreateLogInput(**payload)  # type: ignore[arg-type]


def _assert_validation_error(excinfo: pytest.ExceptionInfo[ValidationError], *fragments: str) -> None:
    """Assert the raised error carries the taxonomy code and a message naming ``fragments``."""
    error = excinfo.value

    assert error.extensions["code"] == ErrorCode.VALIDATION_ERROR.value
    for fragment in fragments:
        assert fragment in error.message, (
            f"the message must mention {fragment!r} for a client to know what to fix; got "
            f"{error.message!r}"
        )


# --- The caps are the column widths, not numbers invented here -------------------------------------


def test_the_length_caps_mirror_the_columns_they_protect() -> None:
    """``service`` and ``traceId`` caps are imported from the model, not restated.

    If these ever diverged the validator would be worse than useless: a cap **below** the column
    rejects values PostgreSQL would happily store, and a cap **above** it lets through exactly the
    values this layer exists to catch early — which then surface as a masked internal error.
    """
    from src.db.models import SERVICE_MAX_LENGTH, TRACE_ID_MAX_LENGTH

    assert MAX_SERVICE_LENGTH == SERVICE_MAX_LENGTH
    assert MAX_TRACE_ID_LENGTH == TRACE_ID_MAX_LENGTH


# --- Filters ---------------------------------------------------------------------------------------


def test_a_service_filter_at_the_cap_is_accepted_and_one_over_is_rejected() -> None:
    """The boundary, both sides. One character apart, so an off-by-one fails here."""
    validate_log_filter(LogFilterInput(service="s" * MAX_SERVICE_LENGTH))

    with pytest.raises(ValidationError) as excinfo:
        validate_log_filter(LogFilterInput(service="s" * (MAX_SERVICE_LENGTH + 1)))

    _assert_validation_error(excinfo, "service", str(MAX_SERVICE_LENGTH))


@pytest.mark.parametrize("blank", ["", " ", "\t", "\n  "])
def test_a_blank_service_filter_is_rejected_rather_than_matching_nothing(blank: str) -> None:
    """An empty service name can match no row, so returning ``[]`` would be a lie by omission.

    This is the shape a UI produces when an "All services" option is bound to an empty string
    instead of being omitted: the request succeeds, the table is empty, and nothing anywhere says
    the filter was meaningless. The error names the fix.
    """
    with pytest.raises(ValidationError) as excinfo:
        validate_log_filter(LogFilterInput(service=blank))

    _assert_validation_error(excinfo, "service", "omit")


def test_a_search_text_at_the_cap_is_accepted_and_one_over_is_rejected() -> None:
    validate_log_filter(LogFilterInput(search_text="x" * MAX_SEARCH_TEXT_LENGTH))

    with pytest.raises(ValidationError) as excinfo:
        validate_log_filter(LogFilterInput(search_text="x" * (MAX_SEARCH_TEXT_LENGTH + 1)))

    _assert_validation_error(excinfo, "searchText", str(MAX_SEARCH_TEXT_LENGTH))


def test_an_empty_search_text_is_accepted_because_it_means_no_constraint() -> None:
    """The one deliberate asymmetry with ``service``, and it is documented in two places.

    C2 defines ``search_text=""`` as matching every message — which is what an empty search box
    should do. Rejecting it here would break that, and would make the C13 filter bar have to
    special-case its own empty state.
    """
    validate_log_filter(LogFilterInput(search_text=""))
    validate_log_filter(LogFilterInput(search_text="   "))


@pytest.mark.parametrize(
    ("field", "filters"),
    [
        ("service", LogFilterInput(service="auth\x00service")),
        ("searchText", LogFilterInput(search_text="timed\x00out")),
    ],
)
def test_a_nul_byte_is_rejected_before_it_reaches_the_driver(
    field: str, filters: LogFilterInput
) -> None:
    """PostgreSQL ``text`` cannot hold ``U+0000``; asyncpg raises on encode.

    Without this the client gets a masked internal error for a single stray byte it could have
    stripped. Caught here, it gets the field name and the reason.
    """
    with pytest.raises(ValidationError) as excinfo:
        validate_log_filter(filters)

    _assert_validation_error(excinfo, field, "NUL")


def test_the_filter_funnel_actually_calls_the_validator() -> None:
    """The rules are only worth having if the read path cannot skip them.

    ``to_log_query`` is the single conversion ``Query.logs`` and ``Query.logsConnection`` both
    perform, so validating inside it is what makes "filters were checked" structural. This is the
    test that fails if somebody moves the call out of the conversion and into one resolver.
    """
    settings = Settings(_env_file=None)

    with pytest.raises(ValidationError):
        LogFilterInput(service="s" * (MAX_SERVICE_LENGTH + 1)).to_log_query(settings)

    # ...and a valid filter still converts, so the guard is not simply refusing everything.
    query = LogFilterInput(service="auth-service", level=LogLevel.ERROR).to_log_query(settings)
    assert query.service == "auth-service"
    assert query.level == "ERROR"


# --- Time ranges -----------------------------------------------------------------------------------


def test_a_single_instant_window_is_legal_but_an_inverted_one_is_not() -> None:
    """Both bounds are inclusive, so ``start == end`` selects the rows at that instant.

    One microsecond the other way is the rejection, which is what makes this a boundary test
    rather than a demonstration.
    """
    validate_time_range(MOMENT, MOMENT)

    with pytest.raises(ValidationError) as excinfo:
        validate_time_range(MOMENT + timedelta(microseconds=1), MOMENT)

    _assert_validation_error(excinfo, "startTime", "endTime")


def test_one_bound_alone_is_always_legal() -> None:
    """An open-ended range is the normal case ("everything since 9am"), not an omission to catch."""
    assert validate_time_range(MOMENT, None) == (MOMENT, None)
    assert validate_time_range(None, MOMENT) == (None, MOMENT)
    assert validate_time_range(None, None) == (None, None)


def test_the_comparison_happens_after_utc_normalisation_not_before() -> None:
    """A mixed-offset pair is ordered by instant, which is the only ordering that means anything.

    ``13:00+02:00`` is 11:00 UTC and therefore **earlier** than ``12:00Z``. Comparing the raw
    values would call this pair inverted and reject a perfectly good window — and would accept the
    genuinely inverted one below.
    """
    eastern = datetime(2026, 7, 26, 13, 0, tzinfo=timezone(timedelta(hours=2)))
    utc_noon = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)

    start, end = validate_time_range(eastern, utc_noon)

    assert start == datetime(2026, 7, 26, 11, 0, tzinfo=UTC)
    assert end == utc_noon
    assert start.tzinfo is not None and end.tzinfo is not None

    with pytest.raises(ValidationError):
        validate_time_range(utc_noon, eastern)


def test_a_naive_bound_is_treated_as_utc() -> None:
    """The same rule C2 applies everywhere, reused rather than reimplemented."""
    start, end = validate_time_range(datetime(2026, 7, 26, 11, 0), datetime(2026, 7, 26, 12, 0))

    assert start == datetime(2026, 7, 26, 11, 0, tzinfo=UTC)
    assert end == datetime(2026, 7, 26, 12, 0, tzinfo=UTC)


# --- metadata --------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", [5, "text", 1.5, True, ["a", "b"], []])
def test_metadata_must_be_a_json_object(value: object) -> None:
    """``JSON`` is untyped on the wire, so a scalar or an array satisfies the schema and must not.

    Every one of these stores cleanly into ``JSONB``. What breaks is the consumers — an
    aggregation keyed on ``metadata->>'host'``, the dashboard's key/value table — and it breaks
    silently, per-row, long after the write.
    """
    with pytest.raises(ValidationError) as excinfo:
        validate_metadata(value)

    _assert_validation_error(excinfo, "metadata", "object")


def test_metadata_none_passes_through_as_none() -> None:
    """Omitted metadata is a normal state (30% of the seeded corpus), not a violation."""
    assert validate_metadata(None) is None


def test_metadata_at_the_depth_limit_is_accepted_and_one_deeper_is_not() -> None:
    """Boundary on the nesting cap, built programmatically so it tracks the constant."""
    at_limit: dict[str, object] = {"leaf": 1}
    for _ in range(MAX_METADATA_DEPTH - 1):
        at_limit = {"nested": at_limit}
    validate_metadata(at_limit)

    too_deep: dict[str, object] = {"nested": at_limit}
    with pytest.raises(ValidationError) as excinfo:
        validate_metadata(too_deep)

    _assert_validation_error(excinfo, "metadata", str(MAX_METADATA_DEPTH))


def test_metadata_with_too_many_members_is_rejected() -> None:
    """A payload that is shallow but enormous — the case the depth cap alone does not bound."""
    at_limit = {f"k{index}": index for index in range(MAX_METADATA_NODES)}
    validate_metadata(at_limit)

    with pytest.raises(ValidationError) as excinfo:
        validate_metadata({f"k{index}": index for index in range(MAX_METADATA_NODES + 1)})

    _assert_validation_error(excinfo, "metadata", str(MAX_METADATA_NODES))


def test_metadata_over_the_byte_ceiling_is_rejected() -> None:
    """Few keys, huge values: neither the depth nor the member cap catches this one."""
    with pytest.raises(ValidationError) as excinfo:
        validate_metadata({"blob": "x" * (MAX_METADATA_BYTES + 1)})

    _assert_validation_error(excinfo, "metadata", str(MAX_METADATA_BYTES))


def test_metadata_containing_nan_is_rejected_with_a_message_that_names_it() -> None:
    """``NaN`` round-trips through Python's json module and is rejected by ``JSONB``.

    So it passes every Python-side sanity check and fails at the server, as a masked internal
    error, for a value that is trivially describable.
    """
    with pytest.raises(ValidationError) as excinfo:
        validate_metadata({"ratio": float("nan")})

    _assert_validation_error(excinfo, "metadata", "NaN")


def test_metadata_of_an_unserialisable_type_is_rejected() -> None:
    """Reachable from a Python caller (a test, C12's verifier) even if not from the wire."""
    with pytest.raises(ValidationError) as excinfo:
        validate_metadata({"when": datetime(2026, 7, 26, tzinfo=UTC)})

    _assert_validation_error(excinfo, "metadata", "datetime")


def test_a_realistic_metadata_object_is_accepted_unchanged() -> None:
    """The rules must not reject the shape the seeded corpus itself produces."""
    payload = {"host": "node-3", "region": "eu-west-1", "latency_ms": 42, "status_code": 503}

    assert validate_metadata(payload) is payload


# --- createLog -------------------------------------------------------------------------------------


def test_a_valid_payload_normalises_into_repository_arguments() -> None:
    """The happy path, asserted field by field — including the enum -> column-string reduction."""
    params = validate_create_log(
        _create_input(
            level=LogLevel.ERROR,
            timestamp=MOMENT,
            metadata={"host": "node-1"},
            trace_id="c0ffee0000000001",
        )
    )

    assert params.service == "auth-service"
    assert params.level == "ERROR", "the column holds the member's value, not the member"
    assert params.message == "user u-1001 authenticated successfully"
    assert params.timestamp == MOMENT
    assert params.metadata == {"host": "node-1"}
    assert params.trace_id == "c0ffee0000000001"


def test_an_omitted_timestamp_stays_none_for_the_repository_to_default() -> None:
    """Resolved in one place only — see :class:`CreateLogParams`. Two "now"s would be two answers."""
    assert validate_create_log(_create_input()).timestamp is None


def test_a_naive_timestamp_is_normalised_to_utc() -> None:
    """A naive value bound against ``timestamptz`` is read in the *server's* zone, not the client's."""
    params = validate_create_log(_create_input(timestamp=datetime(2026, 7, 26, 12, 0)))

    assert params.timestamp == MOMENT
    assert params.timestamp.tzinfo is not None


@pytest.mark.parametrize("blank", ["", "   ", "\n\t"])
def test_a_blank_service_or_message_is_rejected(blank: str) -> None:
    """Neither is a log line anybody meant to write."""
    with pytest.raises(ValidationError) as excinfo:
        validate_create_log(_create_input(service=blank))
    _assert_validation_error(excinfo, "service")

    with pytest.raises(ValidationError) as excinfo:
        validate_create_log(_create_input(message=blank))
    _assert_validation_error(excinfo, "message")


def test_service_is_stripped_but_message_is_not() -> None:
    """The asymmetry is deliberate: one is a grouping key, the other is content.

    ``"auth-service "`` untrimmed becomes a second service in ``logStats``, sitting next to the
    real one in every chart and impossible to spot. A message's leading whitespace is part of the
    text — a wrapped stack trace is indented on purpose, and nothing ever groups by it.
    """
    params = validate_create_log(
        _create_input(service="  auth-service  ", message="  indented line  ", trace_id="  abc  ")
    )

    assert params.service == "auth-service"
    assert params.trace_id == "abc"
    assert params.message == "  indented line  "


def test_the_service_cap_is_applied_after_stripping() -> None:
    """Trailing spaces must not push an otherwise-legal name over the column width."""
    padded = "  " + "s" * MAX_SERVICE_LENGTH + "  "

    params = validate_create_log(_create_input(service=padded))

    assert len(params.service) == MAX_SERVICE_LENGTH


def test_an_over_long_service_message_or_trace_id_is_rejected_at_the_boundary() -> None:
    """Three caps, each asserted one character either side of its limit."""
    validate_create_log(_create_input(service="s" * MAX_SERVICE_LENGTH))
    with pytest.raises(ValidationError) as excinfo:
        validate_create_log(_create_input(service="s" * (MAX_SERVICE_LENGTH + 1)))
    _assert_validation_error(excinfo, "service", str(MAX_SERVICE_LENGTH))

    validate_create_log(_create_input(message="m" * MAX_MESSAGE_LENGTH))
    with pytest.raises(ValidationError) as excinfo:
        validate_create_log(_create_input(message="m" * (MAX_MESSAGE_LENGTH + 1)))
    _assert_validation_error(excinfo, "message", str(MAX_MESSAGE_LENGTH))

    validate_create_log(_create_input(trace_id="t" * MAX_TRACE_ID_LENGTH))
    with pytest.raises(ValidationError) as excinfo:
        validate_create_log(_create_input(trace_id="t" * (MAX_TRACE_ID_LENGTH + 1)))
    _assert_validation_error(excinfo, "traceId", str(MAX_TRACE_ID_LENGTH))


def test_a_blank_trace_id_is_rejected_rather_than_folded_into_null() -> None:
    """``NULL`` and ``""`` mean opposite things to C5's ``related_logs``.

    ``NULL`` means "not correlated" and yields an empty list. An empty-string trace id is a trace
    id, shared by every other row that sent one — so ``relatedLogs`` would correlate unrelated
    requests into one enormous group, and the bigger the corpus the worse it gets.
    """
    with pytest.raises(ValidationError) as excinfo:
        validate_create_log(_create_input(trace_id="   "))

    _assert_validation_error(excinfo, "traceId")


def test_an_omitted_trace_id_stays_none() -> None:
    assert validate_create_log(_create_input()).trace_id is None


@pytest.mark.parametrize(
    ("field", "overrides"),
    [
        ("service", {"service": "auth\x00service"}),
        ("message", {"message": "boom\x00"}),
        ("traceId", {"trace_id": "abc\x00"}),
    ],
)
def test_a_nul_byte_anywhere_in_the_payload_is_rejected(
    field: str, overrides: dict[str, object]
) -> None:
    with pytest.raises(ValidationError) as excinfo:
        validate_create_log(_create_input(**overrides))

    _assert_validation_error(excinfo, field, "NUL")


def test_mutation_metadata_goes_through_the_same_rules_as_everything_else() -> None:
    """The mutation does not get its own, laxer copy of the metadata rules."""
    with pytest.raises(ValidationError) as excinfo:
        validate_create_log(_create_input(metadata=[1, 2, 3]))

    _assert_validation_error(excinfo, "metadata", "object")
