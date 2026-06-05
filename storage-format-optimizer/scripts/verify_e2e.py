"""Cross-container end-to-end verifier for the storage-format optimizer.

Run by the compose ``e2e`` profile service (``Dockerfile.test``), this script
talks to the live ``app`` container over HTTP + WebSocket — reaching it by
**service name** (``http://app:8000`` via ``APP_URL``), never ``localhost`` —
and asserts the end-to-end behaviour an isolated unit test cannot: that a real
ingest -> query -> stats -> migration flow works against a running server.

Every check is fully isolated by a unique **per-run nonce tenant**
(``e2e_<nonce>``), so the verifier is order-independent and safe to run against a
server that already holds data from ``make up`` or a previous run.

Checks (each prints PASS/FAIL; any FAIL exits non-zero):

1. ``check_health``               — ``GET /health`` returns ``{"status":"healthy"}``.
2. ``ingest_recent``              — a small RECENT-ts batch lands (``ingested==N``,
   ``partitions_touched`` non-empty).
3. ``query_full_record``         — a no-columns query returns the rows and
   classifies as ``full_record``.
4. ``query_analytical_projection``— a single-column query returns only that key
   and classifies as ``analytical``.
5. ``stats_coherent``            — ``GET /api/stats`` has ``storage.total_bytes>0``,
   a ``formats.distribution``, and lists the nonce tenant.
6. ``ws_tick``                   — ``WS /ws`` delivers one ``type=="tick"`` message
   carrying ``stats`` / ``series`` / ``tiers``.
7. ``migration_observed``        — ingest >=300 rows with OLD ts (a COLD
   partition), run analytical projections cycling 5+ distinct single columns,
   then poll ``GET /api/stats`` until ``formats.distribution.columnar>=1`` AND
   ``migrations.completed>=1``; confirm via ``GET /api/stats/{tenant}`` that a
   partition is COLUMNAR with a non-empty reason, and re-query to prove the OLD
   rows survived the migration.

``time.time()`` is fine here — this runs as a normal OS process, not a frozen
workflow script.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time

import httpx
import websockets

# Talk to the app by service name inside the compose network.
APP_URL = os.environ.get("APP_URL", "http://app:8000")

# A fresh, unique tenant per process run isolates this verifier from any data the
# server already holds (e.g. a long-running ``make up`` or a prior e2e run), so
# every assertion below is order-independent.
_NONCE = f"{int(time.time())}_{os.getpid()}"
TENANT = f"e2e_{_NONCE}"

# Per-request HTTP timeout (queries fan across partitions + decode real files).
_HTTP_TIMEOUT = 20.0

# Distinct single columns cycled through analytical projections to drive a
# scan-heavy, narrow (low fraction-of-columns) access pattern -> COLUMNAR pick.
_PROJECTION_COLUMNS = ["c0", "c1", "c2", "c3", "c4", "c5"]

# A small epoch so the partition's time-bucket is far in the past -> COLD tier.
_OLD_TS = 100_000.0
# Recent rows for the basic flow (use the real wall clock so they land HOT).
_RECENT_BATCH = 5
# Rows pushed into the single OLD partition to clear select_min_rows (256).
_OLD_ROWS = 320


def _ok(message: str) -> None:
    """Print a per-check PASS line."""
    print(f"PASS: {message}")


def _fail(message: str) -> None:
    """Print a failure banner and exit non-zero."""
    print(f"FAIL: {message}")
    print("E2E: FAILED")
    sys.exit(1)


def _old_entries(n: int, *, ts: float = _OLD_TS) -> list[dict]:
    """Build ``n`` ingest entries carrying every projection column, at OLD ts.

    Each entry shares the same ``ts`` so all ``n`` rows land in ONE partition
    (the bucket derived from ``ts``), and every entry carries all of
    :data:`_PROJECTION_COLUMNS` so a single-column projection over any of them is
    valid (and therefore counts as a narrow analytical scan).
    """
    entries: list[dict] = []
    for i in range(n):
        fields = {col: f"{col}_v{i % 7}" for col in _PROJECTION_COLUMNS}
        fields["seq"] = i
        entries.append({"ts": ts, "fields": fields})
    return entries


async def check_health(client: httpx.AsyncClient) -> None:
    """Poll ``GET /health`` up to 30x1s until ``{"status":"healthy"}``."""
    url = f"{APP_URL}/health"
    print(f"E2E: polling {url} (max 30 attempts) ...")
    for attempt in range(1, 31):
        try:
            resp = await client.get(url, timeout=5.0)
            if resp.status_code == 200 and resp.json().get("status") == "healthy":
                _ok(f"health (attempt {attempt})")
                return
            print(f"  attempt {attempt}: HTTP {resp.status_code}")
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            print(f"  attempt {attempt}: {exc}")
        await asyncio.sleep(1)
    _fail("health check never returned healthy")


async def ingest_recent(client: httpx.AsyncClient) -> None:
    """Ingest a small RECENT-ts batch under the nonce tenant."""
    print("E2E: ingest recent batch ...")
    entries = [
        {"ts": time.time(), "fields": {"level": "INFO", "msg": f"hello-{i}", "seq": i}}
        for i in range(_RECENT_BATCH)
    ]
    resp = await client.post(
        f"{APP_URL}/api/ingest",
        json={"tenant": TENANT, "entries": entries},
        timeout=_HTTP_TIMEOUT,
    )
    if resp.status_code != 200:
        _fail(f"/api/ingest returned HTTP {resp.status_code}: {resp.text}")
    body = resp.json()
    if body.get("ingested") != _RECENT_BATCH:
        _fail(f"expected ingested=={_RECENT_BATCH}, got {body.get('ingested')!r}")
    if not body.get("partitions_touched"):
        _fail(f"expected non-empty partitions_touched, got {body.get('partitions_touched')!r}")
    if body.get("tenant") != TENANT:
        _fail(f"expected tenant=={TENANT!r}, got {body.get('tenant')!r}")
    _ok(f"ingest recent ({body['ingested']} rows -> {body['partitions_touched']})")


async def query_full_record(client: httpx.AsyncClient) -> None:
    """A no-columns query returns the ingested rows, classified full_record."""
    print("E2E: full-record query ...")
    resp = await client.post(
        f"{APP_URL}/api/query",
        json={"tenant": TENANT},
        timeout=_HTTP_TIMEOUT,
    )
    if resp.status_code != 200:
        _fail(f"/api/query returned HTTP {resp.status_code}: {resp.text}")
    body = resp.json()
    rows = body.get("rows")
    if not isinstance(rows, list) or len(rows) < _RECENT_BATCH:
        _fail(f"full-record query expected >= {_RECENT_BATCH} rows, got {rows!r}")
    qclass = body.get("meta", {}).get("query_class")
    if qclass != "full_record":
        _fail(f"expected meta.query_class=='full_record', got {qclass!r}")
    # The recent batch's fields should be present (full record, not a projection).
    if not any("msg" in r for r in rows):
        _fail("full-record rows missing the 'msg' field (not a full record)")
    _ok(f"full-record query ({len(rows)} rows, query_class={qclass})")


async def query_analytical_projection(client: httpx.AsyncClient) -> None:
    """A single-column query returns only that key, classified analytical."""
    print("E2E: analytical projection query ...")
    column = "msg"
    resp = await client.post(
        f"{APP_URL}/api/query",
        json={"tenant": TENANT, "columns": [column]},
        timeout=_HTTP_TIMEOUT,
    )
    if resp.status_code != 200:
        _fail(f"/api/query (projection) returned HTTP {resp.status_code}: {resp.text}")
    body = resp.json()
    rows = body.get("rows")
    if not isinstance(rows, list) or not rows:
        _fail(f"projection query expected rows, got {rows!r}")
    for r in rows:
        extra = set(r.keys()) - {column}
        if extra:
            _fail(f"projection row carried unexpected keys {sorted(extra)} (want only [{column!r}])")
    qclass = body.get("meta", {}).get("query_class")
    if qclass != "analytical":
        _fail(f"expected meta.query_class=='analytical', got {qclass!r}")
    _ok(f"analytical projection ({len(rows)} rows, only [{column!r}], query_class={qclass})")


async def stats_coherent(client: httpx.AsyncClient) -> None:
    """`GET /api/stats` is coherent and lists the nonce tenant."""
    print("E2E: /api/stats coherence ...")
    resp = await client.get(f"{APP_URL}/api/stats", timeout=_HTTP_TIMEOUT)
    if resp.status_code != 200:
        _fail(f"/api/stats returned HTTP {resp.status_code}: {resp.text}")
    data = resp.json()

    total_bytes = data.get("storage", {}).get("total_bytes")
    if not isinstance(total_bytes, int) or total_bytes <= 0:
        _fail(f"storage.total_bytes expected > 0 after ingest, got {total_bytes!r}")

    distribution = data.get("formats", {}).get("distribution")
    if not isinstance(distribution, dict):
        _fail(f"formats.distribution missing/not a dict: {distribution!r}")

    tenants = data.get("tenants")
    if not isinstance(tenants, list) or TENANT not in tenants:
        _fail(f"nonce tenant {TENANT!r} not in stats.tenants ({tenants!r})")

    _ok(
        f"/api/stats (total_bytes={total_bytes}, distribution={distribution}, "
        f"tenant listed)"
    )


async def ws_tick() -> None:
    """Connect to ``WS /ws`` and assert one ``tick`` with stats/series/tiers."""
    ws_url = APP_URL.replace("https://", "wss://").replace("http://", "ws://")
    ws_url = f"{ws_url}/ws"
    print(f"E2E: WebSocket {ws_url} ...")
    try:
        async with websockets.connect(ws_url, open_timeout=10) as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
    except Exception as exc:  # noqa: BLE001 — any failure here is a hard fail
        _fail(f"WebSocket connect/recv failed: {exc}")

    try:
        data = json.loads(raw)
    except (ValueError, TypeError) as exc:
        _fail(f"WebSocket payload was not JSON: {exc}")

    if data.get("type") != "tick":
        _fail(f"WebSocket message expected type='tick', got {data.get('type')!r}")
    for key in ("stats", "series", "tiers"):
        if key not in data:
            _fail(f"WebSocket tick missing '{key}': keys={sorted(data)}")
    _ok("WebSocket tick (carries stats/series/tiers)")


async def _ingest_old_partition(client: httpx.AsyncClient) -> str:
    """Ingest >=300 OLD-ts rows under the nonce tenant; return the partition id.

    All rows share one OLD ts so they collapse into a single COLD partition with
    enough rows (> select_min_rows) to be worth reformatting.
    """
    resp = await client.post(
        f"{APP_URL}/api/ingest",
        json={"tenant": TENANT, "entries": _old_entries(_OLD_ROWS)},
        timeout=_HTTP_TIMEOUT,
    )
    if resp.status_code != 200:
        _fail(f"OLD-ts /api/ingest returned HTTP {resp.status_code}: {resp.text}")
    body = resp.json()
    if body.get("ingested") != _OLD_ROWS:
        _fail(f"OLD-ts ingest expected {_OLD_ROWS} rows, got {body.get('ingested')!r}")
    touched = body.get("partitions_touched") or []
    if len(touched) != 1:
        _fail(f"OLD-ts batch should land in ONE partition, got {touched!r}")
    return touched[0]


async def _drive_analytical_scans(client: httpx.AsyncClient, *, rounds: int = 8) -> None:
    """Issue analytical projections cycling 5+ distinct single columns.

    Distinct projected columns across many queries make the access pattern
    scan-heavy with a LOW fraction-of-columns-touched-per-query (narrow), which is
    exactly what the selector reads as "recommend COLUMNAR".
    """
    for i in range(rounds):
        column = _PROJECTION_COLUMNS[i % len(_PROJECTION_COLUMNS)]
        resp = await client.post(
            f"{APP_URL}/api/query",
            json={"tenant": TENANT, "columns": [column]},
            timeout=_HTTP_TIMEOUT,
        )
        if resp.status_code != 200:
            _fail(f"scan query [{column}] returned HTTP {resp.status_code}: {resp.text}")


async def migration_observed(client: httpx.AsyncClient) -> None:
    """Force a COLD scan-heavy partition to migrate ROW->COLUMNAR and verify it.

    Recipe (mirrors the in-process e2e test):
      1. Ingest >=300 rows at an OLD ts -> one COLD partition above select_min_rows.
      2. Drive ~8 analytical projections cycling 5+ distinct single columns ->
         scan-heavy, narrow access pattern.
      3. Poll GET /api/stats (loop ticks ~every 5s) up to 30x1s until
         formats.distribution.columnar>=1 AND migrations.completed>=1.
      4. Confirm via GET /api/stats/{tenant} that a partition is COLUMNAR with a
         non-empty reason, and re-query to prove the OLD rows survived.
    """
    print("E2E: migration observed (COLD + scan-heavy -> COLUMNAR) ...")
    pid = await _ingest_old_partition(client)
    print(f"  ingested {_OLD_ROWS} OLD-ts rows into partition {pid}")
    await _drive_analytical_scans(client)
    print("  drove analytical projections over distinct single columns")

    print("  polling /api/stats for a columnar migration (max 30x1s) ...")
    migrated = False
    for attempt in range(1, 31):
        resp = await client.get(f"{APP_URL}/api/stats", timeout=_HTTP_TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            columnar = data.get("formats", {}).get("distribution", {}).get("columnar", 0)
            completed = data.get("migrations", {}).get("completed", 0)
            if columnar >= 1 and completed >= 1:
                migrated = True
                print(
                    f"  migration observed (attempt {attempt}: "
                    f"columnar={columnar}, completed={completed})"
                )
                break
        # Keep nudging the access pattern so the partition stays a clear pick.
        await _drive_analytical_scans(client, rounds=len(_PROJECTION_COLUMNS))
        await asyncio.sleep(1)
    if not migrated:
        _fail(
            "no columnar migration observed within 30s "
            "(expected formats.distribution.columnar>=1 and migrations.completed>=1)"
        )

    # Confirm the tenant's partition is COLUMNAR with an explained reason.
    resp = await client.get(f"{APP_URL}/api/stats/{TENANT}", timeout=_HTTP_TIMEOUT)
    if resp.status_code != 200:
        _fail(f"/api/stats/{TENANT} returned HTTP {resp.status_code}: {resp.text}")
    partitions = resp.json().get("partitions", [])
    columnar_parts = [p for p in partitions if p.get("format") == "columnar"]
    if not columnar_parts:
        _fail(f"no partition reports format=='columnar' for tenant {TENANT}")
    if not any((p.get("reason") or "").strip() for p in columnar_parts):
        _fail("columnar partition has an empty 'reason' (decision not explained)")
    _ok(
        f"tenant {TENANT} has {len(columnar_parts)} columnar partition(s) "
        f"with a reason"
    )

    # Data must survive the migration: re-query the OLD rows in full.
    resp = await client.post(
        f"{APP_URL}/api/query",
        json={"tenant": TENANT},
        timeout=_HTTP_TIMEOUT,
    )
    if resp.status_code != 200:
        _fail(f"post-migration /api/query returned HTTP {resp.status_code}: {resp.text}")
    rows = resp.json().get("rows") or []
    # Recent batch + the OLD partition's rows must all still be readable.
    if len(rows) < _OLD_ROWS + _RECENT_BATCH:
        _fail(
            f"post-migration query lost rows: got {len(rows)}, "
            f"expected >= {_OLD_ROWS + _RECENT_BATCH}"
        )
    _ok(f"data preserved across migration ({len(rows)} rows re-queried)")


async def main() -> None:
    """Run every check in order; exit 0 only if all pass."""
    print(f"E2E target: {APP_URL}  (tenant={TENANT})")
    async with httpx.AsyncClient() as client:
        await check_health(client)
        await ingest_recent(client)
        await query_full_record(client)
        await query_analytical_projection(client)
        await stats_coherent(client)
        await ws_tick()
        await migration_observed(client)
    print("E2E: all checks passed")
    sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
