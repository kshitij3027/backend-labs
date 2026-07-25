"""Unit tests for :mod:`src.db.repository` — the parts that are wrong before SQL is involved.

Everything here runs without a database, because everything here is about the *statement* rather
than about the rows: clamping, LIKE escaping, timezone normalisation, and which predicates a given
:class:`~src.db.repository.LogQuery` does and does not produce. Those are the pieces that are
cheap to get wrong and expensive to notice — an unescaped ``%`` returns *more* rows, which looks
like a working search until someone types a percent sign.

The matching integration suite (``tests/integration/test_db_store.py``) then proves the same
statements select the rows the deterministic corpus says they should. Neither suite is sufficient
alone: this one can pass against a statement that is well-formed and semantically wrong, and that
one cannot tell you *why* a query returned the wrong set.

Statements are compiled against the real PostgreSQL dialect rather than the default one, because
``ILIKE``, the ``ESCAPE`` clause and ``JSONB`` are all dialect-specific and a generic compile
would prove nothing about what the server actually receives.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.dialects import postgresql

from src.config import Settings
from src.db.repository import (
    LIKE_ESCAPE_CHARACTER,
    LogQuery,
    as_utc,
    build_count_select,
    build_log_select,
    build_predicates,
    clamp_limit,
    escape_like,
)

#: Deliberately NOT the production 100/500. If the builder ignored the injected configuration and
#: read the global defaults instead, assertions written against 100/500 would still pass.
LIMITS = {"default_query_limit": 10, "max_query_limit": 25}

MOMENT = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def limits() -> Settings:
    """Settings whose limit knobs are distinguishable from every default in the project."""
    return Settings(_env_file=None, log_level="WARNING", **LIMITS)


def _sql(statement) -> str:  # noqa: ANN001 - any SQLAlchemy Executable
    """Compile to a PostgreSQL SQL string (bind parameters left as placeholders)."""
    return str(statement.compile(dialect=postgresql.dialect()))


def _params(statement) -> dict[str, object]:  # noqa: ANN001 - any SQLAlchemy Executable
    """The bound parameter values the server would receive."""
    return dict(statement.compile(dialect=postgresql.dialect()).params)


# --- clamp_limit --------------------------------------------------------------------------------


def test_omitted_limit_falls_back_to_the_configured_default(limits: Settings) -> None:
    """``None`` means "the client did not ask", which is ``DEFAULT_QUERY_LIMIT`` — not a literal."""
    assert clamp_limit(None, limits) == LIMITS["default_query_limit"]


@pytest.mark.parametrize("requested", [0, -1, -10_000])
def test_limits_below_one_clamp_up_to_one(requested: int, limits: Settings) -> None:
    """``LIMIT 0`` returns nothing at all and a negative limit raises — neither is ever intended.

    ``0`` is the dangerous one: PostgreSQL accepts it and returns an empty result set, which is
    indistinguishable from "no rows matched your filters". Clamping to 1 turns a silent wrong
    answer into a small right one.
    """
    assert clamp_limit(requested, limits) == 1


@pytest.mark.parametrize("requested", [1, 10, 24, 25])
def test_limits_inside_the_range_pass_through(requested: int, limits: Settings) -> None:
    """Including both endpoints — the ceiling itself is a legal request, not an over-request."""
    assert clamp_limit(requested, limits) == requested


@pytest.mark.parametrize("requested", [26, 500, 1_000_000])
def test_limits_above_the_ceiling_clamp_down(requested: int, limits: Settings) -> None:
    """Over-asking is capped rather than rejected: the client gets a usable answer, bounded."""
    assert clamp_limit(requested, limits) == LIMITS["max_query_limit"]


def test_the_builder_applies_the_clamp_not_just_the_helper(limits: Settings) -> None:
    """The clamp is inside the statement builder, so no caller can route around it.

    Spec §2 item 22 requires the cap "on every query path". Proving ``clamp_limit`` works says
    nothing about whether the SELECT uses it — this asserts on the value actually bound into the
    statement, which is the thing the database sees.
    """
    assert LIMITS["max_query_limit"] in _params(build_log_select(LogQuery(limit=10_000), limits)).values()
    assert 1 in _params(build_log_select(LogQuery(limit=0), limits)).values()
    assert LIMITS["default_query_limit"] in _params(build_log_select(LogQuery(), limits)).values()


def test_every_select_carries_a_limit(limits: Settings) -> None:
    """Even a completely unfiltered query is bounded — there is no unlimited path."""
    assert "LIMIT" in _sql(build_log_select(LogQuery(), limits))


# --- escape_like --------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("plain text", "plain text"),
        ("100%", "100\\%"),
        ("user_id", "user\\_id"),
        ("%_%", "\\%\\_\\%"),
        ("a\\b", "a\\\\b"),
        # The escape character arriving already doubled must not be collapsed: the client typed
        # two backslashes and means two backslashes.
        ("a\\\\b", "a\\\\\\\\b"),
        ("", ""),
    ],
)
def test_escape_like_neutralises_every_metacharacter(raw: str, expected: str) -> None:
    """``%``, ``_`` and the escape character itself are all escaped, escape character first."""
    assert escape_like(raw) == expected


def test_escape_character_is_escaped_before_the_wildcards() -> None:
    """Order matters, and this is the case that proves it.

    If ``%`` were escaped before ``\\``, the backslash inserted by the first pass would be escaped
    again by the second, yielding ``\\\\%`` — an escaped backslash followed by a *live* wildcard.
    The pattern would compile, run, and match everything.
    """
    assert escape_like("%") == LIKE_ESCAPE_CHARACTER + "%"
    assert escape_like(LIKE_ESCAPE_CHARACTER) == LIKE_ESCAPE_CHARACTER * 2


def test_a_literal_percent_search_binds_an_escaped_pattern(limits: Settings) -> None:
    """Searching for ``100%`` binds ``%100\\%%`` with an explicit ``ESCAPE`` clause.

    This is the unit half of the literal-``%`` guarantee: the *shape* of what reaches the server.
    ``tests/integration/test_db_store.py`` proves the other half — that this pattern matches only
    the messages containing a percent sign, and not the whole table.

    Note the trailing ``%`` of the bound value is the search wildcard the repository adds, while
    the ``\\%`` in the middle is the client's literal character. Getting those two confused is the
    entire bug being guarded against.
    """
    statement = build_log_select(LogQuery(search_text="100%"), limits)

    assert "%100\\%%" in _params(statement).values()
    assert "ESCAPE" in _sql(statement), (
        "the pattern is escaped but the statement does not declare an escape character, so the "
        "server would read the backslashes as literal text and the wildcard would stay live"
    )


def test_search_uses_case_insensitive_matching(limits: Settings) -> None:
    """Substring search over human-written log text ignores case (spec §2 item 20)."""
    sql = _sql(build_log_select(LogQuery(search_text="Timeout"), limits)).upper()

    assert "ILIKE" in sql or "LOWER(" in sql


def test_search_text_is_wrapped_in_wildcards_on_both_sides(limits: Settings) -> None:
    """Substring, not prefix: ``search_text`` matches anywhere inside the message."""
    bound = [v for v in _params(build_log_select(LogQuery(search_text="refused"), limits)).values()
             if isinstance(v, str)]

    assert bound == ["%refused%"]


# --- as_utc -------------------------------------------------------------------------------------


def test_naive_datetimes_are_read_as_utc() -> None:
    """A naive value keeps its wall time and gains UTC — it is not shifted."""
    naive = datetime(2026, 7, 25, 12, 0, 0)

    assert as_utc(naive) == MOMENT
    assert as_utc(naive).tzinfo is not None  # type: ignore[union-attr]


def test_aware_datetimes_are_converted_to_utc() -> None:
    """Another zone names the same instant, and is normalised to the one the column stores."""
    ist = timezone(timedelta(hours=5, minutes=30))

    converted = as_utc(MOMENT.astimezone(ist))

    assert converted == MOMENT
    assert converted.utcoffset() == timedelta(0)  # type: ignore[union-attr]


def test_none_passes_through() -> None:
    """An omitted bound stays omitted rather than becoming "the epoch"."""
    assert as_utc(None) is None


def test_naive_bounds_reach_the_statement_as_aware_utc(limits: Settings) -> None:
    """Normalisation happens inside the builder, so no caller can bind a naive value.

    asyncpg will happily send a naive datetime to a ``timestamptz`` column, and PostgreSQL then
    interprets it in the *server's* ``TimeZone``. The query succeeds and quietly selects a
    different set of rows depending on how the database container is configured — which is a bug
    that reproduces only on someone else's machine.
    """
    statement = build_log_select(
        LogQuery(start_time=datetime(2026, 7, 25, 12, 0, 0)), limits
    )

    bound = [value for value in _params(statement).values() if isinstance(value, datetime)]
    assert bound == [MOMENT]
    assert all(value.tzinfo is not None for value in bound)


# --- build_predicates ---------------------------------------------------------------------------


def test_an_empty_query_produces_no_predicates() -> None:
    """Omitted filters are ignored (spec §2 item 19) — asserted as absence, not as SQL text."""
    assert build_predicates(LogQuery()) == []


def test_an_unfiltered_select_has_no_where_clause(limits: Settings) -> None:
    """The absence carries all the way into the statement, not just into the predicate list."""
    assert "WHERE" not in _sql(build_log_select(LogQuery(), limits))


@pytest.mark.parametrize(
    "query",
    [
        LogQuery(service="auth-service"),
        LogQuery(level="ERROR"),
        LogQuery(start_time=MOMENT),
        LogQuery(end_time=MOMENT),
        LogQuery(search_text="timeout"),
    ],
    ids=["service", "level", "start_time", "end_time", "search_text"],
)
def test_each_supplied_filter_contributes_exactly_one_predicate(query: LogQuery) -> None:
    """One filter in, one condition out — no filter is silently dropped or doubled."""
    assert len(build_predicates(query)) == 1


def test_all_supplied_filters_are_anded_together(limits: Settings) -> None:
    """Five filters compose into five conditions joined by AND, never OR."""
    query = LogQuery(
        service="auth-service",
        level="ERROR",
        start_time=MOMENT - timedelta(hours=1),
        end_time=MOMENT,
        search_text="timeout",
    )

    assert len(build_predicates(query)) == 5

    sql = _sql(build_log_select(query, limits)).upper()
    assert " OR " not in sql, "filters must narrow the result set, never widen it"
    assert sql.count(" AND ") == 4


def test_an_empty_string_is_a_supplied_filter_not_an_omitted_one() -> None:
    """``""`` is distinguishable from ``None``, and the distinction is deliberate.

    A truthiness check would collapse them and make ``service: ""`` silently mean "any service".
    The chosen semantics: ``None`` is omitted; ``""`` is a real (and empty) value, so it matches
    nothing for ``service`` and everything for ``search_text`` — which is what an empty search box
    should do.
    """
    assert len(build_predicates(LogQuery(service=""))) == 1
    assert len(build_predicates(LogQuery(search_text=""))) == 1


# --- ordering and counting ----------------------------------------------------------------------


def test_results_are_ordered_newest_first_with_an_id_tiebreak(limits: Settings) -> None:
    """``ORDER BY timestamp DESC, id DESC`` — the second column makes the order *total*.

    Two rows sharing a timestamp have no defined relative order under ``timestamp DESC`` alone, so
    the same query can return them in either order on repeated calls, and C3's keyset cursor built
    on that order can skip or repeat a row at a page boundary.
    """
    sql = _sql(build_log_select(LogQuery(), limits))

    assert "ORDER BY log_entries.timestamp DESC, log_entries.id DESC" in sql


def test_the_count_statement_aggregates_in_sql(limits: Settings) -> None:
    """The count is computed by the database, never by pulling rows and calling ``len``."""
    sql = _sql(build_count_select(LogQuery())).lower()

    assert "count(" in sql
    assert "log_entries" in sql


def test_the_count_ignores_the_limit_and_the_ordering() -> None:
    """Counting reports how many rows *match*, not how many a limited query would return.

    ``LogConnection.totalCount`` and C4's ``logStats.totalLogs`` both need the true total: a count
    that respected the limit could never exceed it, so it could never tell a client there is more
    data — the only thing either field is for.
    """
    sql = _sql(build_count_select(LogQuery(limit=5))).upper()

    assert "LIMIT" not in sql
    assert "ORDER BY" not in sql


def test_the_count_applies_the_same_predicates_as_the_select(limits: Settings) -> None:
    """One predicate builder feeds both, so a filter can never apply to rows but not to the total."""
    query = LogQuery(service="auth-service", level="ERROR", search_text="timeout")

    count_params = {v for v in _params(build_count_select(query)).values() if isinstance(v, str)}
    select_params = {
        v for v in _params(build_log_select(query, limits)).values() if isinstance(v, str)
    }

    assert count_params == select_params == {"auth-service", "ERROR", "%timeout%"}


# --- the query object itself ----------------------------------------------------------------------


def test_log_query_is_immutable() -> None:
    """A query object is passed to a cache-key function in C7; a mutable one would poison the key."""
    query = LogQuery(service="auth-service")

    with pytest.raises((AttributeError, TypeError)):
        query.service = "other"  # type: ignore[misc]


def test_log_query_defaults_to_everything_omitted() -> None:
    """The zero-argument query is "no filters", which is what ``Query.logs`` with no input means."""
    query = LogQuery()

    assert (
        query.service,
        query.level,
        query.start_time,
        query.end_time,
        query.search_text,
        query.limit,
    ) == (None, None, None, None, None, None)
