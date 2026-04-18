"""Unit tests for the faceted-search core (C3).

Covers the pure SQL builder in ``src.search.query_builder`` and the
end-to-end behaviour of ``src.search.facet_counter.search`` /
``facets_only`` against a real ``aiosqlite`` connection seeded with a
hand-crafted, deterministic dataset so every assertion on counts can
be exact.

The single most important assertion family in this file is the
**excluded-self** check: when the user filters by ``service=payments``
the ``service`` facet in the response MUST still show non-zero
counts for *other* services (``auth``, ``api-gateway``), otherwise
the UI's multi-select UX silently dies. That is the entire reason
for the UNION-ALL-with-skip pattern in ``build_facet_sql``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pytest
import pytest_asyncio

from src.models import LogEntry
from src.search import facet_counter, query_builder
from src.search.query_builder import (
    FACET_DIMS,
    build_facet_sql,
    build_results_sql,
    build_where,
    escape_like,
)
from src.storage import sqlite_store


# ---------------------------------------------------------------------------
# Deterministic seeded dataset.
#
# Total = 32 rows, grouped so every faceted assertion can be computed
# by hand. The ts values are clustered within a known 60-second window
# (BASE_TS..BASE_TS+31) so ts_start/ts_end tests have real semantics.
# ---------------------------------------------------------------------------

BASE_TS = 1_700_000_000  # 2023-11-14 22:13:20 UTC — all rows in hour 22.


def _entry(
    idx: int,
    service: str,
    level: str,
    region: str,
    latency_ms: float,
    message: str = "ok",
) -> LogEntry:
    """Build a LogEntry with a deterministic, unique id + ts offset."""
    return LogEntry(
        id=f"seed-{idx:03d}",
        ts=BASE_TS + idx,
        service=service,
        level=level,  # type: ignore[arg-type]
        region=region,
        response_time_ms=latency_ms,
        source_ip="10.0.0.1",
        request_id=f"req-{idx:04d}",
        message=message,
        metadata={"i": idx},
    )


def _seed_entries() -> List[LogEntry]:
    """Hand-crafted 32-row dataset documented in the module docstring.

    Row layout by bucket (latency bucket derived from latency_ms):
      *  0-100ms : payments/INFO/us-east-1 x10     (50ms)
      * 100-500ms: payments/ERROR/us-east-1 x5     (300ms)
      *  500ms-2s: payments/ERROR/us-west-2 x3     (1200ms)
      *  0-100ms : auth/INFO/us-east-1 x8          (70ms)
      * 100-500ms: auth/WARN/eu-west-1 x4          (250ms)
      *  2s+     : api-gateway/ERROR/ap-south-1 x2 (2500ms)
    """
    entries: List[LogEntry] = []
    idx = 0

    def _push(n: int, svc: str, lvl: str, region: str, lat: float) -> None:
        nonlocal idx
        for _ in range(n):
            entries.append(_entry(idx, svc, lvl, region, lat))
            idx += 1

    _push(10, "payments", "INFO", "us-east-1", 50)
    _push(5, "payments", "ERROR", "us-east-1", 300)
    _push(3, "payments", "ERROR", "us-west-2", 1200)
    _push(8, "auth", "INFO", "us-east-1", 70)
    _push(4, "auth", "WARN", "eu-west-1", 250)
    _push(2, "api-gateway", "ERROR", "ap-south-1", 2500)
    return entries


@pytest_asyncio.fixture
async def seeded_conn(tmp_path: Path):
    """Yield an aiosqlite connection pre-populated with the 32-row set."""
    db_file = str(tmp_path / "seed.db")
    conn = await sqlite_store.connect(db_file)
    try:
        await sqlite_store.migrate(conn)
        await sqlite_store.insert_logs(conn, _seed_entries())
        yield conn
    finally:
        await sqlite_store.close(conn)


@pytest_asyncio.fixture
async def empty_conn(tmp_path: Path):
    """Yield an aiosqlite connection with a migrated but empty ``logs`` table."""
    db_file = str(tmp_path / "empty.db")
    conn = await sqlite_store.connect(db_file)
    try:
        await sqlite_store.migrate(conn)
        yield conn
    finally:
        await sqlite_store.close(conn)


# ---------------------------------------------------------------------------
# escape_like
# ---------------------------------------------------------------------------

def test_escape_like():
    """Verify the order of replacements: backslash, then %, then _."""
    # Single %: becomes \%
    assert escape_like("foo%bar") == "foo\\%bar"
    # Single _: becomes \_
    assert escape_like("a_b") == "a\\_b"
    # Leading backslash: becomes \\
    assert escape_like("\\path") == "\\\\path"
    # Mixed: backslash first so subsequent replacements don't double-escape.
    # Input source "50%\\x_y" -> 7-char string "50%\x_y"
    # Expected: "50\\%\\\\x\\_y" -> 10-char string "50\%\\x\_y"
    assert escape_like("50%\\x_y") == "50\\%\\\\x\\_y"


# ---------------------------------------------------------------------------
# build_where
# ---------------------------------------------------------------------------

def test_build_where_empty():
    """No filters / query / ts bounds -> sentinel '1=1' and empty params."""
    sql, params = build_where({}, None, None, None)
    assert sql == "1=1"
    assert params == []


def test_build_where_single_facet():
    """One facet dim produces a single IN (?) clause."""
    sql, params = build_where({"service": ["payments"]}, None, None, None)
    assert "service IN (?)" in sql
    assert params == ["payments"]


def test_build_where_skip_dim():
    """``skip=service`` must omit the service predicate but keep others."""
    sql, params = build_where(
        {"service": ["payments"], "level": ["ERROR"]},
        None,
        None,
        None,
        skip="service",
    )
    assert "level IN (?)" in sql
    assert "service IN" not in sql
    assert params == ["ERROR"]


def test_build_where_unknown_dim_ignored():
    """Keys that aren't real facet dims should be silently dropped."""
    sql, params = build_where({"nonsense": ["x"]}, None, None, None)
    assert sql == "1=1"
    assert params == []


def test_build_where_hour_bucket_int_coercion():
    """hour_bucket values arriving as strings must bind as ints."""
    sql, params = build_where({"hour_bucket": ["3", "4"]}, None, None, None)
    assert "hour_bucket IN (?,?)" in sql
    # Critical: must be ints, not the original strings.
    assert params == [3, 4]
    assert all(isinstance(p, int) for p in params)


def test_build_where_ts_bounds_and_query():
    """ts_start/ts_end produce range clauses and query produces LIKE ESCAPE."""
    sql, params = build_where({}, "timeout", 100, 200)
    assert "ts >= ?" in sql
    assert "ts <= ?" in sql
    assert "message LIKE ? ESCAPE" in sql
    assert params == [100, 200, "%timeout%"]


# ---------------------------------------------------------------------------
# Helpers for facet-response assertions.
# ---------------------------------------------------------------------------

def _facet_by_name(response_facets, name: str):
    """Return the FacetSummary with the given dim name, or raise."""
    for f in response_facets:
        if f.name == name:
            return f
    raise AssertionError(f"facet {name!r} missing from response")


def _value_counts(facet_summary) -> Dict[Any, int]:
    """Collapse a FacetSummary's values list into {value: count}."""
    return {fv.value: fv.count for fv in facet_summary.values}


# ---------------------------------------------------------------------------
# Excluded-self single filter -- the central correctness guarantee of C3.
# ---------------------------------------------------------------------------

async def test_excluded_self_single_filter(seeded_conn):
    """filter=service:payments -> service facet still shows auth, api-gateway>0.

    This is the "excluded-self" check: the sub-query for the service
    dimension is built with skip='service' so the filter doesn't
    wipe out the sibling counts the user needs to broaden their
    selection.
    """
    resp = await facet_counter.search(
        conn=seeded_conn,
        filters={"service": ["payments"]},
        query=None,
        ts_start=None,
        ts_end=None,
        cursor=None,
        limit=10,
    )

    # Stable facet ordering -- 5 dims, payload-level.
    assert [f.name for f in resp.facets] == list(FACET_DIMS)

    # --- service facet (skip=service -> reflects ALL services with rows)
    svc = _facet_by_name(resp.facets, "service")
    svc_counts = _value_counts(svc)
    assert svc_counts == {"payments": 18, "auth": 12, "api-gateway": 2}
    payments_val = next(v for v in svc.values if v.value == "payments")
    assert payments_val.selected is True
    # Crucial excluded-self assertion:
    assert svc_counts["auth"] > 0
    assert svc_counts["api-gateway"] > 0

    # --- level facet (skip=level, service=payments applied)
    lvl = _facet_by_name(resp.facets, "level")
    lvl_counts = _value_counts(lvl)
    assert lvl_counts == {"INFO": 10, "ERROR": 8}
    assert "WARN" not in lvl_counts
    assert "DEBUG" not in lvl_counts

    # --- region facet (skip=region, service=payments applied)
    reg = _facet_by_name(resp.facets, "region")
    reg_counts = _value_counts(reg)
    assert reg_counts == {"us-east-1": 15, "us-west-2": 3}
    assert "eu-west-1" not in reg_counts
    assert "ap-south-1" not in reg_counts

    # 18 rows match, limit=10 -> has_more=True, total_count=None.
    assert resp.has_more is True
    assert resp.total_count is None
    assert len(resp.logs) == 10
    assert resp.next_cursor is not None
    assert resp.applied_filters == {"service": ["payments"]}


# ---------------------------------------------------------------------------
# OR within a dim, AND across dims.
# ---------------------------------------------------------------------------

async def test_or_within_dim(seeded_conn):
    """filter=level:[ERROR,WARN] -> 14 rows (ERROR=10 + WARN=4)."""
    resp = await facet_counter.search(
        conn=seeded_conn,
        filters={"level": ["ERROR", "WARN"]},
        query=None,
        ts_start=None,
        ts_end=None,
        cursor=None,
        limit=200,
    )
    # 5 (payments/ERROR/us-east-1) + 3 (payments/ERROR/us-west-2)
    # + 4 (auth/WARN/eu-west-1) + 2 (api-gateway/ERROR/ap-south-1) = 14.
    assert len(resp.logs) == 14
    assert resp.has_more is False
    assert resp.total_count == 14

    # level facet (skip=level) -> still sees ALL levels across dataset.
    lvl = _facet_by_name(resp.facets, "level")
    lvl_counts = _value_counts(lvl)
    assert lvl_counts == {"INFO": 18, "ERROR": 10, "WARN": 4}

    # service facet (skip=service, apply level IN (ERROR,WARN)):
    #   payments: 5+3 = 8 (ERROR only), auth: 4 (WARN only), api-gateway: 2.
    svc = _facet_by_name(resp.facets, "service")
    svc_counts = _value_counts(svc)
    assert svc_counts == {"payments": 8, "auth": 4, "api-gateway": 2}


async def test_and_across_dims(seeded_conn):
    """filter=service:payments,level:ERROR -> strict intersection = 8 rows."""
    resp = await facet_counter.search(
        conn=seeded_conn,
        filters={"service": ["payments"], "level": ["ERROR"]},
        query=None,
        ts_start=None,
        ts_end=None,
        cursor=None,
        limit=50,
    )
    # payments/ERROR rows: 5 (us-east-1) + 3 (us-west-2) = 8
    assert len(resp.logs) == 8
    assert resp.has_more is False
    assert resp.total_count == 8

    # service facet (skip=service, apply level=ERROR):
    #   ERROR rows by service: payments=8, api-gateway=2, auth=0 (no auth ERROR).
    svc = _facet_by_name(resp.facets, "service")
    svc_counts = _value_counts(svc)
    assert svc_counts == {"payments": 8, "api-gateway": 2}
    assert "auth" not in svc_counts  # auth has 0 ERROR rows

    # level facet (skip=level, apply service=payments):
    #   payments rows by level: INFO=10, ERROR=8.
    lvl = _facet_by_name(resp.facets, "level")
    lvl_counts = _value_counts(lvl)
    assert lvl_counts == {"INFO": 10, "ERROR": 8}


# ---------------------------------------------------------------------------
# Free-text search behaviour.
# ---------------------------------------------------------------------------

async def test_free_text_search_matches_message(empty_conn):
    """query='timeout' should match one row with 'timeout' in message."""
    entries = [
        LogEntry(
            id="ft1",
            ts=BASE_TS,
            service="payments",
            level="ERROR",
            region="us-east-1",
            response_time_ms=200.0,
            source_ip="10.0.0.1",
            request_id="req-ft1",
            message="connection timeout on payments",
            metadata={},
        ),
        LogEntry(
            id="ft2",
            ts=BASE_TS + 1,
            service="payments",
            level="INFO",
            region="us-east-1",
            response_time_ms=50.0,
            source_ip="10.0.0.1",
            request_id="req-ft2",
            message="ok",
            metadata={},
        ),
    ]
    await sqlite_store.insert_logs(empty_conn, entries)

    resp = await facet_counter.search(
        conn=empty_conn,
        filters={},
        query="timeout",
        ts_start=None,
        ts_end=None,
        cursor=None,
        limit=10,
    )
    assert len(resp.logs) == 1
    assert "timeout" in resp.logs[0]["message"]

    # Facet counts reflect the ONE matching row:
    lvl = _facet_by_name(resp.facets, "level")
    lvl_counts = _value_counts(lvl)
    assert lvl_counts == {"ERROR": 1}
    svc = _facet_by_name(resp.facets, "service")
    assert _value_counts(svc) == {"payments": 1}


async def test_free_text_escapes_wildcards(empty_conn):
    """A literal '%' in the query must NOT act as a SQL wildcard."""
    entries = [
        LogEntry(
            id="lit1",
            ts=BASE_TS,
            service="api-gateway",
            level="WARN",
            region="us-east-1",
            response_time_ms=50.0,
            source_ip="10.0.0.1",
            request_id="req-lit1",
            message="rate limit 50% reached",
            metadata={},
        ),
        LogEntry(
            id="lit2",
            ts=BASE_TS + 1,
            service="api-gateway",
            level="INFO",
            region="us-east-1",
            response_time_ms=20.0,
            source_ip="10.0.0.1",
            request_id="req-lit2",
            message="ok 50ms",
            metadata={},
        ),
        LogEntry(
            id="lit3",
            ts=BASE_TS + 2,
            service="api-gateway",
            level="INFO",
            region="us-east-1",
            response_time_ms=25.0,
            source_ip="10.0.0.1",
            request_id="req-lit3",
            message="ok 50xx",
            metadata={},
        ),
    ]
    await sqlite_store.insert_logs(empty_conn, entries)

    # Exact literal '50%' must match only lit1, not lit2/lit3 which
    # would match if % were treated as a SQL wildcard.
    resp_pct = await facet_counter.search(
        conn=empty_conn,
        filters={},
        query="50%",
        ts_start=None,
        ts_end=None,
        cursor=None,
        limit=10,
    )
    assert len(resp_pct.logs) == 1
    assert resp_pct.logs[0]["id"] == "lit1"

    # Partial substring '50' without % is still just a substring -- all 3 rows.
    resp_plain = await facet_counter.search(
        conn=empty_conn,
        filters={},
        query="50",
        ts_start=None,
        ts_end=None,
        cursor=None,
        limit=10,
    )
    assert len(resp_plain.logs) == 3


# ---------------------------------------------------------------------------
# Keyset pagination.
# ---------------------------------------------------------------------------

async def test_keyset_pagination_disjoint_pages(empty_conn):
    """Three successive calls with cursor must walk the 15-row set disjointly.

    The fixture inserts 15 rows with monotonically increasing ts=1..15,
    then pages through with limit=5. Pages must not overlap, and the
    exhaustion call must report has_more=False, next_cursor=None.
    """
    entries: List[LogEntry] = []
    for i in range(1, 16):  # ts = 1..15
        entries.append(
            LogEntry(
                id=f"page-{i:02d}",
                ts=i,
                service="payments",
                level="INFO",
                region="us-east-1",
                response_time_ms=50.0,
                source_ip="10.0.0.1",
                request_id=f"req-{i}",
                message="ok",
                metadata={},
            )
        )
    await sqlite_store.insert_logs(empty_conn, entries)

    # --- page 1: ts DESC, top 5 rows -> ts 15..11, next_cursor=11
    r1 = await facet_counter.search(
        conn=empty_conn, filters={}, query=None, ts_start=None, ts_end=None,
        cursor=None, limit=5,
    )
    assert r1.has_more is True
    assert [row["ts"] for row in r1.logs] == [15, 14, 13, 12, 11]
    assert r1.next_cursor == 11
    assert r1.total_count is None  # has_more => total_count undefined

    # --- page 2: cursor=11 -> ts < 11 => rows 10..6, next_cursor=6
    r2 = await facet_counter.search(
        conn=empty_conn, filters={}, query=None, ts_start=None, ts_end=None,
        cursor=r1.next_cursor, limit=5,
    )
    assert r2.has_more is True
    assert [row["ts"] for row in r2.logs] == [10, 9, 8, 7, 6]
    assert r2.next_cursor == 6

    # Disjoint: page1 and page2 ts sets must not intersect.
    p1_ts = {row["ts"] for row in r1.logs}
    p2_ts = {row["ts"] for row in r2.logs}
    assert p1_ts.isdisjoint(p2_ts)

    # --- page 3: cursor=6 -> ts < 6 => only 5 rows (ts=5..1), exhausts.
    r3 = await facet_counter.search(
        conn=empty_conn, filters={}, query=None, ts_start=None, ts_end=None,
        cursor=r2.next_cursor, limit=5,
    )
    assert r3.has_more is False
    assert r3.next_cursor is None
    assert [row["ts"] for row in r3.logs] == [5, 4, 3, 2, 1]
    # When has_more is False, total_count == len(returned rows).
    assert r3.total_count == 5


# ---------------------------------------------------------------------------
# Selected-but-zero re-injection.
# ---------------------------------------------------------------------------

async def test_selected_but_zero_kept_in_facet(seeded_conn):
    """A selected value with 0 rows must still appear (count=0, selected=True).

    With the seeded dataset, ap-south-1 has only ERROR rows. Asking for
    level=INFO AND region=ap-south-1 yields no rows at all. The level
    facet's INFO entry still has to render so the user can uncheck it.
    """
    resp = await facet_counter.search(
        conn=seeded_conn,
        filters={"region": ["ap-south-1"], "level": ["INFO"]},
        query=None,
        ts_start=None,
        ts_end=None,
        cursor=None,
        limit=10,
    )

    # Zero actual result rows.
    assert resp.logs == []

    # Level facet: skip=level, region=ap-south-1 applied.
    # ap-south-1 has 2 ERROR rows -> ERROR=2 is the only "real" count.
    # INFO is selected-but-zero -> re-injected at the end.
    lvl = _facet_by_name(resp.facets, "level")
    lvl_values = {fv.value: (fv.count, fv.selected) for fv in lvl.values}
    assert "INFO" in lvl_values
    assert lvl_values["INFO"] == (0, True)
    # ERROR should appear (non-zero from apply region=ap-south-1).
    assert lvl_values.get("ERROR") == (2, False)

    # Region facet: skip=region, level=INFO applied.
    # INFO rows by region: us-east-1 = 10 (payments) + 8 (auth) = 18.
    reg = _facet_by_name(resp.facets, "region")
    reg_values = {fv.value: (fv.count, fv.selected) for fv in reg.values}
    assert reg_values.get("us-east-1") == (18, False)
    # ap-south-1 is selected but has no INFO rows -> re-injected with count=0.
    assert "ap-south-1" in reg_values
    assert reg_values["ap-south-1"] == (0, True)


# ---------------------------------------------------------------------------
# Empty-DB response shape.
# ---------------------------------------------------------------------------

async def test_empty_db_response_shape(empty_conn):
    """No rows at all -> response shape intact, counts empty, timing present."""
    resp = await facet_counter.search(
        conn=empty_conn,
        filters={},
        query=None,
        ts_start=None,
        ts_end=None,
        cursor=None,
        limit=10,
    )
    assert resp.logs == []
    assert resp.total_count == 0
    assert resp.has_more is False
    assert resp.next_cursor is None
    # Every facet dim should still be present (even with empty values list).
    assert [f.name for f in resp.facets] == list(FACET_DIMS)
    for f in resp.facets:
        assert f.values == []
    assert resp.query_time_ms >= 0
