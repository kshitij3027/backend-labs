"""Unit tests for src.models — the wire vocabulary and the clamping rule.

These tests are contract tests, not implementation tests. Nearly every assertion here is
traceable to a specific sentence or a literal example in the README, because the README *is*
this project's specification:

* the ``2026-07-27T10:31:04.512Z`` timestamp and the ``{items, page{limit, returned,
  next_cursor, has_more, total}}`` envelope both appear verbatim in its *Pagination* section;
* "a client asking for 10,000 rows gets the ceiling and a header saying so — not a ``422``" is
  what the :func:`~src.models.clamp_limit` tests pin.

Settings are constructed directly (never via environment mutation), matching
``tests/conftest.py``: kwargs outrank both the environment and ``.env`` in pydantic-settings'
source order, so nothing ambient can shift a threshold under a test. Where an exact ceiling
matters the test builds its own Settings; where it does not, it reuses the shared ``settings``
fixture and asserts against the *attribute* rather than a hard-coded number, so the compose
``test`` service's env overrides can never make it pass or fail for the wrong reason.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from src.config import Settings
from src.models import (
    CLAMPED_HEADER,
    CURSOR_TRUNCATED_HEADER,
    ERROR_LEVELS,
    LEVEL_ORDER,
    MAX_ATTR_KEY_LEN,
    MAX_ATTR_VALUE_LEN,
    MAX_ATTRS_KEYS,
    ErrorBody,
    LogCreate,
    LogEntry,
    LogLevel,
    LogPage,
    LogQuery,
    PageInfo,
    SortOrder,
    clamp_limit,
)

#: The README's pagination example, to the microsecond. Every serialisation test grades against
#: this exact instant so a regression shows up as a literal string diff.
README_TS = datetime(2026, 7, 27, 10, 31, 4, 512_000, tzinfo=UTC)
README_TS_WIRE = "2026-07-27T10:31:04.512Z"

#: RFC-3339, UTC, exactly three fractional digits, 'Z' suffix. Nothing else is acceptable.
RFC3339_MS_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")

VALID_SECRET = "9f2c1a7b4e6d8f0a3c5e7b9d1f3a5c7e9b1d3f5a7c9e1b3d5f7a9c1e3b5d7f9a"


def make_settings(**overrides: Any) -> Settings:
    """Hermetic Settings: explicit kwargs, no .env file, no ambient environment."""
    overrides.setdefault("jwt_secret", VALID_SECRET)
    return Settings(_env_file=None, **overrides)


def make_entry(**overrides: Any) -> LogEntry:
    """A valid LogEntry matching the README's example row, with per-test overrides."""
    fields: dict[str, Any] = {
        "id": "3f6c1c1e0b7a4a19a4b0b0f0d2b6d1a7",
        "ts": README_TS,
        "level": LogLevel.ERROR,
        "service": "auth-svc",
        "host": "node-3",
        "message": "invalid token",
    }
    fields.update(overrides)
    return LogEntry(**fields)


# ---------------------------------------------------------------------------------------------
# LogEntry — timestamp contract
# ---------------------------------------------------------------------------------------------


def test_log_entry_serialises_ts_as_rfc3339_z():
    """The wire form is the README's, byte for byte: 'Z' suffix, millisecond precision.

    Python's default ``isoformat()`` would emit ``+00:00`` and six fractional digits, which is
    legal RFC-3339 but is not what the README documents and not what a JS ``new Date(...)``
    or a log pipeline expects.
    """
    entry = make_entry()

    dumped = json.loads(entry.model_dump_json())

    assert dumped["ts"] == README_TS_WIRE
    assert dumped["ts"].endswith("Z")
    assert "+00:00" not in dumped["ts"]
    assert RFC3339_MS_Z.match(dumped["ts"]), dumped["ts"]
    # mode="json" dumps agree with model_dump_json — FastAPI serialises responses in json mode.
    assert entry.model_dump(mode="json")["ts"] == README_TS_WIRE
    # And the string parses back into the exact same instant.
    assert LogEntry(**{**dumped}) == entry


def test_log_entry_ts_truncates_to_milliseconds_never_rounds_up():
    """Microsecond input is truncated to 3 digits; a timestamp must never be reported late."""
    entry = make_entry(ts=datetime(2026, 7, 27, 10, 31, 4, 512_987, tzinfo=UTC))

    assert entry.model_dump(mode="json")["ts"] == "2026-07-27T10:31:04.512Z"


def test_log_entry_coerces_naive_ts_to_utc():
    """A naive datetime means UTC, not 'whatever TZ the container happens to have'."""
    entry = make_entry(ts=datetime(2026, 7, 27, 10, 31, 4, 512_000))

    assert entry.ts.tzinfo is not None
    assert entry.ts.utcoffset() == timedelta(0)
    assert entry.model_dump(mode="json")["ts"] == README_TS_WIRE


def test_log_entry_converts_aware_non_utc_ts_to_utc():
    """An aware datetime in another zone is converted, so the corpus is never mixed-tz."""
    ist = timezone(timedelta(hours=5, minutes=30))
    entry = make_entry(ts=datetime(2026, 7, 27, 16, 1, 4, 512_000, tzinfo=ist))

    assert entry.ts == README_TS
    assert entry.ts.utcoffset() == timedelta(0)
    assert entry.model_dump(mode="json")["ts"] == README_TS_WIRE


def test_log_entry_accepts_rfc3339_string_ts():
    """The wire form round-trips: what we emit is what we accept."""
    entry = make_entry(ts=README_TS_WIRE)

    assert entry.ts == README_TS


# ---------------------------------------------------------------------------------------------
# LogEntry — model configuration
# ---------------------------------------------------------------------------------------------


def test_log_entry_is_frozen():
    """An appended entry is shared by scans, SSE subscribers and the stats pass at once.

    If any of them could mutate it, a concurrent scan could observe a half-updated entry —
    freezing removes the possibility rather than relying on discipline.
    """
    entry = make_entry()

    with pytest.raises(ValidationError):
        entry.message = "mutated"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        entry.level = LogLevel.DEBUG  # type: ignore[misc]

    assert entry.message == "invalid token"


@pytest.mark.parametrize("bad", ["TRACE", "warn", "error", "CRITICAL", "", "WARNING"])
def test_log_entry_rejects_unknown_level(bad):
    """Levels are a closed set, and the match is case-sensitive on the wire spelling."""
    with pytest.raises(ValidationError):
        make_entry(level=bad)


def test_log_entry_rejects_extra_field():
    """extra='forbid'. Notably, `seq` is NOT a field of the wire model.

    C4's store keeps ``seq`` on its own internal record; hoisting it into ``LogEntry`` would
    publish a storage detail in every response body and in the OpenAPI document, where it
    could never be withdrawn.
    """
    with pytest.raises(ValidationError):
        make_entry(seq=41)
    with pytest.raises(ValidationError):
        make_entry(severity="ERROR")

    assert "seq" not in LogEntry.model_fields


def test_log_entry_attrs_default_to_empty_and_are_per_instance():
    """The mutable default must not be shared between instances."""
    first, second = make_entry(), make_entry()

    assert first.attrs == {} and second.attrs == {}
    first.attrs["k"] = "v"  # the dict itself is not frozen, only rebinding the field is
    assert second.attrs == {}


def test_log_entry_field_order_matches_readme_example():
    """Declaration order is wire order; the README prints id, ts, level, service, host, message."""
    assert list(LogEntry.model_fields) == [
        "id",
        "ts",
        "level",
        "service",
        "host",
        "message",
        "attrs",
    ]


# ---------------------------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------------------------


def test_page_info_shape_matches_readme():
    """The envelope is the README's example, key for key and order for order."""
    page = LogPage(
        items=[make_entry()],
        page=PageInfo(
            limit=50, returned=50, next_cursor="b64:abc", has_more=True, total=12840
        ),
    )

    dumped = json.loads(page.model_dump_json())

    assert list(dumped) == ["items", "page"]
    assert list(dumped["page"]) == [
        "limit",
        "returned",
        "next_cursor",
        "has_more",
        "total",
    ]
    assert dumped["page"] == {
        "limit": 50,
        "returned": 50,
        "next_cursor": "b64:abc",
        "has_more": True,
        "total": 12840,
    }
    assert list(dumped["items"][0]) == [
        "id",
        "ts",
        "level",
        "service",
        "host",
        "message",
        "attrs",
    ]
    assert dumped["items"][0]["ts"] == README_TS_WIRE
    assert dumped["items"][0]["level"] == "ERROR"


def test_page_is_an_envelope_not_a_bare_array():
    """A top-level array can never grow a sibling field without breaking every client."""
    dumped = json.loads(
        LogPage(
            items=[], page=PageInfo(limit=50, returned=0, has_more=False, total=0)
        ).model_dump_json()
    )

    assert isinstance(dumped, dict)
    assert dumped["items"] == []
    assert dumped["page"]["next_cursor"] is None  # explicit null, not an omitted key


def test_error_body_keeps_detail_first_and_required():
    """A superset of FastAPI's default {'detail': ...} shape, so existing clients keep working."""
    assert list(ErrorBody.model_fields) == ["detail", "code", "request_id"]

    body = json.loads(ErrorBody(detail="not found").model_dump_json())
    assert list(body) == ["detail", "code", "request_id"]
    assert body == {"detail": "not found", "code": None, "request_id": None}

    with pytest.raises(ValidationError):
        ErrorBody(code="x")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------------------------
# clamp_limit — the README's "clamped, never rejected" rule
# ---------------------------------------------------------------------------------------------


def test_limit_clamps_to_max_page_size(settings: Settings):
    """10,000 rows gets the ceiling and a flag — never a 422."""
    effective, clamped = clamp_limit(10_000, settings)

    assert effective == settings.max_page_size
    assert clamped is True


def test_limit_defaults_when_omitted(settings: Settings):
    """No preference expressed means DEFAULT_PAGE_SIZE, and nothing of the client's was clamped."""
    effective, clamped = clamp_limit(None, settings)

    assert effective == settings.default_page_size
    assert clamped is False


@pytest.mark.parametrize("requested", [0, -1, -10_000])
def test_limit_clamps_floor(requested):
    """'Give me zero rows' has no useful reading; the floor is clamped like the ceiling."""
    effective, clamped = clamp_limit(requested, make_settings())

    assert effective == 1
    assert clamped is True


@pytest.mark.parametrize("requested", [1, 7, 50, 499, 500])
def test_limit_within_range_is_unchanged(requested):
    """Anything inside [1, MAX_PAGE_SIZE] passes through untouched and unflagged."""
    settings = make_settings(default_page_size=50, max_page_size=500)

    assert clamp_limit(requested, settings) == (requested, False)


def test_limit_reads_the_configured_ceiling_not_a_constant():
    """The ceiling comes from Settings, so an operator lowering MAX_PAGE_SIZE actually binds."""
    settings = make_settings(default_page_size=5, max_page_size=10)

    assert clamp_limit(11, settings) == (10, True)
    assert clamp_limit(10, settings) == (10, False)
    assert clamp_limit(None, settings) == (5, False)


def test_limit_never_raises_on_absurd_input():
    """Whatever a client sends, this function returns a usable page size."""
    settings = make_settings()

    for requested in (None, 0, -1, 1, 10**9):
        effective, _ = clamp_limit(requested, settings)
        assert 1 <= effective <= settings.max_page_size


def test_misconfigured_default_above_ceiling_is_still_capped():
    """A DEFAULT_PAGE_SIZE > MAX_PAGE_SIZE typo must not serve a page above the documented max."""
    settings = make_settings(default_page_size=5_000, max_page_size=500)

    assert clamp_limit(None, settings) == (500, False)


# ---------------------------------------------------------------------------------------------
# LogCreate — the write body
# ---------------------------------------------------------------------------------------------


def test_log_create_defaults_ts_and_id():
    """The server fills the gaps, and honours whatever the client did supply."""
    body = LogCreate(level=LogLevel.INFO, service="auth-svc", host="node-3", message="hi")
    before = datetime.now(UTC)

    minted = body.to_entry()

    assert minted.id and len(minted.id) == 32  # uuid4 hex
    assert minted.ts >= before - timedelta(seconds=5)
    assert minted.ts.utcoffset() == timedelta(0)
    assert (minted.level, minted.service, minted.host, minted.message) == (
        LogLevel.INFO,
        "auth-svc",
        "node-3",
        "hi",
    )

    # Injected server values are used when the body omits them...
    injected = body.to_entry(now=README_TS, new_id="server-minted")
    assert injected.ts == README_TS
    assert injected.id == "server-minted"

    # ...and the client's own values win when it supplied them (a shipper replaying its buffer
    # must be able to preserve the original event time and its own idempotency key).
    explicit = LogCreate(
        level=LogLevel.FATAL,
        service="auth-svc",
        host="node-3",
        message="hi",
        ts=README_TS,
        id="client-supplied",
    )
    entry = explicit.to_entry(now=datetime.now(UTC), new_id="ignored")
    assert entry.ts == README_TS
    assert entry.id == "client-supplied"

    assert isinstance(entry, LogEntry)
    assert json.loads(entry.model_dump_json())["ts"] == README_TS_WIRE


def test_log_create_normalises_supplied_naive_ts_through_to_entry():
    """The UTC invariant holds no matter which side supplied the timestamp."""
    body = LogCreate(
        level=LogLevel.WARN,
        service="s",
        host="h",
        message="m",
        ts=datetime(2026, 7, 27, 10, 31, 4, 512_000),
    )

    assert body.to_entry().model_dump(mode="json")["ts"] == README_TS_WIRE


def test_log_create_entry_does_not_alias_the_bodys_attrs():
    """The frozen entry must not share a mutable dict with the (unfrozen) request body."""
    body = LogCreate(
        level=LogLevel.INFO, service="s", host="h", message="m", attrs={"a": "1"}
    )
    entry = body.to_entry()

    body.attrs["b"] = "2"

    assert entry.attrs == {"a": "1"}


def test_log_create_rejects_empty_message():
    """An empty log line carries no information and must not enter the ring."""
    with pytest.raises(ValidationError):
        LogCreate(level=LogLevel.INFO, service="s", host="h", message="")

    # ...and an absent one is a different error, but still an error.
    with pytest.raises(ValidationError):
        LogCreate(level=LogLevel.INFO, service="s", host="h")  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "field,value",
    [
        ("service", ""),
        ("host", ""),
        ("service", "s" * 129),
        ("host", "h" * 129),
        ("message", "m" * 8193),
    ],
)
def test_log_create_rejects_out_of_bounds_string_fields(field, value):
    fields = {"level": LogLevel.INFO, "service": "s", "host": "h", "message": "m"}
    fields[field] = value

    with pytest.raises(ValidationError):
        LogCreate(**fields)


@pytest.mark.parametrize(
    "attrs",
    [
        {f"k{i}": "v" for i in range(MAX_ATTRS_KEYS + 1)},
        {"k" * (MAX_ATTR_KEY_LEN + 1): "v"},
        {"k": "v" * (MAX_ATTR_VALUE_LEN + 1)},
    ],
    ids=["too-many-keys", "key-too-long", "value-too-long"],
)
def test_log_create_rejects_oversized_attrs(attrs):
    """The ring bounds the entry COUNT; only these caps bound the per-entry size."""
    with pytest.raises(ValidationError):
        LogCreate(level=LogLevel.INFO, service="s", host="h", message="m", attrs=attrs)


def test_log_create_accepts_attrs_at_the_cap():
    """The caps are inclusive — exactly MAX_ATTRS_KEYS keys is fine."""
    attrs = {f"k{i}": "v" for i in range(MAX_ATTRS_KEYS)}

    body = LogCreate(
        level=LogLevel.INFO, service="s", host="h", message="m", attrs=attrs
    )

    assert len(body.to_entry().attrs) == MAX_ATTRS_KEYS


def test_log_create_rejects_extra_field():
    """A misspelled field is a 422 the caller can act on, not a silently defaulted entry."""
    with pytest.raises(ValidationError):
        LogCreate(
            level=LogLevel.INFO,
            service="s",
            host="h",
            message="m",
            severity="ERROR",  # type: ignore[call-arg]
        )


# ---------------------------------------------------------------------------------------------
# LogQuery — the shared filter bundle
# ---------------------------------------------------------------------------------------------


def test_log_query_rejects_cursor_and_offset_together():
    """No meaningful interpretation exists, so guessing would silently return the wrong page."""
    with pytest.raises(ValidationError) as excinfo:
        LogQuery(cursor="b64:abc", offset=100)

    assert "mutually exclusive" in str(excinfo.value)

    # Either one alone is fine.
    assert LogQuery(cursor="b64:abc").offset is None
    assert LogQuery(offset=100).cursor is None


def test_log_query_rejects_since_after_until():
    """An empty range looks identical to 'no matching logs' — the worst thing to report."""
    with pytest.raises(ValidationError) as excinfo:
        LogQuery(since=README_TS, until=README_TS - timedelta(seconds=1))

    assert "since must not be after until" in str(excinfo.value)

    # Equal bounds are a legitimate point query, not an error.
    assert LogQuery(since=README_TS, until=README_TS).since == README_TS


def test_log_query_normalises_bounds_to_utc():
    """Both bounds and every stored ts must live in one timezone or comparison raises."""
    ist = timezone(timedelta(hours=5, minutes=30))
    query = LogQuery(
        since=datetime(2026, 7, 27, 16, 1, 4, 512_000, tzinfo=ist),
        until=datetime(2026, 7, 27, 11, 0, 0),
    )

    assert query.since == README_TS
    assert query.since.utcoffset() == timedelta(0)
    assert query.until.utcoffset() == timedelta(0)


def test_log_query_defaults_are_newest_first_and_unfiltered():
    query = LogQuery()

    assert query.order is SortOrder.DESC
    assert (query.level, query.service, query.host) == (None, None, None)
    assert (query.since, query.until, query.q) == (None, None, None)
    assert (query.limit, query.cursor, query.offset) == (None, None, None)


def test_log_query_limit_is_unconstrained_so_clamping_owns_it():
    """A `le=` on this field would turn an over-large page into the 422 the README forbids."""
    settings = make_settings()
    query = LogQuery(limit=10_000)  # accepted, not rejected

    assert clamp_limit(query.limit, settings) == (settings.max_page_size, True)


def test_log_query_rejects_negative_offset_and_overlong_q():
    with pytest.raises(ValidationError):
        LogQuery(offset=-1)
    with pytest.raises(ValidationError):
        LogQuery(q="x" * 257)


def test_log_query_rejects_unknown_level_in_list():
    with pytest.raises(ValidationError):
        LogQuery(level=["ERROR", "TRACE"])

    assert LogQuery(level=["ERROR", "FATAL"]).level == [LogLevel.ERROR, LogLevel.FATAL]


# ---------------------------------------------------------------------------------------------
# Level vocabulary
# ---------------------------------------------------------------------------------------------


def test_level_order_is_strictly_increasing():
    """Enum members compare by identity, so this map is what makes gte/lte on `level` possible."""
    ordinals = [LEVEL_ORDER[level] for level in LogLevel]

    assert set(LEVEL_ORDER) == set(LogLevel)  # every level has an ordinal
    assert ordinals == [0, 1, 2, 3, 4]
    assert all(later > earlier for earlier, later in zip(ordinals, ordinals[1:]))
    assert LEVEL_ORDER[LogLevel.FATAL] > LEVEL_ORDER[LogLevel.ERROR] > LEVEL_ORDER[LogLevel.INFO]
    # StrEnum members hash equal to their wire strings, so a raw query-param value indexes it.
    assert LEVEL_ORDER["WARN"] == LEVEL_ORDER[LogLevel.WARN]


def test_error_levels_are_error_and_fatal():
    """One definition of 'an error', shared by C11's top-errors panel and everything else."""
    assert ERROR_LEVELS == frozenset({LogLevel.ERROR, LogLevel.FATAL})
    assert LogLevel.WARN not in ERROR_LEVELS
    assert all(LEVEL_ORDER[level] >= LEVEL_ORDER[LogLevel.ERROR] for level in ERROR_LEVELS)


def test_log_level_members_are_their_wire_strings():
    assert LogLevel.ERROR == "ERROR"
    assert json.loads(make_entry().model_dump_json())["level"] == "ERROR"
    assert [level.value for level in LogLevel] == [
        "DEBUG",
        "INFO",
        "WARN",
        "ERROR",
        "FATAL",
    ]


def test_sort_order_values_are_lowercase():
    assert SortOrder.ASC == "asc"
    assert SortOrder.DESC == "desc"


# ---------------------------------------------------------------------------------------------
# Header-name constants
# ---------------------------------------------------------------------------------------------


def test_clamped_header_constants_match_main_expose_headers():
    """A header not in CORS `expose_headers` is unreadable by browser JS, whatever we send.

    ``src.models`` owns the two names and ``src.main`` owns the allowlist; nothing in the code
    connects them, so a rename in one place would silently stop the dashboard from being able
    to see the header. This test is that connection.
    """
    from src.main import EXPOSE_HEADERS

    assert CLAMPED_HEADER == "X-Page-Limit-Clamped"
    assert CURSOR_TRUNCATED_HEADER == "X-Cursor-Truncated"
    assert CLAMPED_HEADER in EXPOSE_HEADERS
    assert CURSOR_TRUNCATED_HEADER in EXPOSE_HEADERS
