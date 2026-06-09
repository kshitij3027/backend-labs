"""Cross-container end-to-end verifier for the bloom-filter membership stack.

Run by the compose ``e2e`` profile service (``Dockerfile.test``), this script
drives the LIVE ``app`` and ``dashboard`` containers over HTTP + WebSocket —
reaching them by **service name** (``http://app:8001`` / ``http://dashboard:8002``
via ``APP_URL`` / ``DASHBOARD_URL``), never ``localhost`` — and asserts the
end-to-end behaviour the in-process tests cannot: that real containers, real
uvicorn processes, the compose network, and the dashboard's HTTP-only view of
the API all hold together.

Every key this verifier touches is prefixed with a per-run **nonce** (8 hex
chars of a uuid4), so checks are order-independent and safe against a server
that already holds data from ``make up``, the seed script, or a previous run
(the ``./data`` bind mount persists filters and sqlite rows across runs).

Unlike a fail-fast script, every check RUNS regardless of earlier failures:
each prints ``PASS: <name> — <detail>`` or ``FAIL: <name> — <detail>``, the
failures are collected, and the process exits 1 if any check failed. A failed
early check can cascade (e.g. the proxy check re-queries a key the roundtrip
check added) — each cascade still shows up as its own labelled FAIL.

Checks, in order:

 1.  health-api          — GET /health answers {"status": "healthy"}.
 2.  health-dashboard    — dashboard GET /health answers healthy.
 3.  add-query-roundtrip — add + query a nonce key per log type → probably_exists.
 4.  absent-negative     — a never-added nonce key → definitely_not_exist.
 5.  invalid-type-422    — an unknown log_type is rejected with HTTP 422.
 6.  populate-and-stats  — /demo/populate?count=3000 moves /stats adds_total by
                           exactly 3000 (1000 per log-type filter, 0 sessions).
 7.  perf-demo-gates     — /demo/performance-test: speedup_vs_linear > 10 and
                           memory_ratio < 0.05 (the two spec criteria).
 8.  pipeline-two-tier   — ingest → lookup hit via storage; absent lookup skips
                           storage (storage_checked false); skip pct > 0.
 9.  pipeline-fp-detection — a bloom-only key (no sqlite row) is disproved by
                           storage: false_positive true, found false.
 10. sessions-flow       — ingest → query found; absent session short-circuits
                           storage; /sessions/stats says memory_under_2mb.
 11. sessions-perf-gates — /sessions/performance-test: 100% of non-existent
                           sessions identified, >=40% storage calls avoided,
                           both paths < 1 ms avg (speedup reported, NOT gated —
                           against warm in-process sqlite the honest win is
                           storage-call avoidance, see the C11 handler).
 12. stats-coherence     — uptime >= 0, all four filters report memory_bytes>0,
                           totals grew coherently post-populate. (Restart
                           persistence cannot be exercised from inside the
                           network — that lives in the integration tests and
                           the final main-thread `docker compose restart` check.)
 13. dashboard-page      — GET / serves the page with the chart canvas, the
                           sessions card, and the vendored Chart.js reference.
 14. dashboard-static    — the vendored chart.umd.min.js serves and is >100 KB.
 15. dashboard-ws-tick   — WS /ws delivers a first tick (within 12s) of type
                           "tick" with error null and all four filter names.
 16. dashboard-proxy     — POST /proxy/query relays to the API and finds a key
                           this run added.

Final line: ``E2E: <n_pass>/<n_total> checks passed``; exit 0 only on 16/16.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections.abc import Callable
from uuid import uuid4

import httpx
import websockets

# Service-name URLs inside the compose network (env-overridable for local runs).
APP_URL = os.environ.get("APP_URL", "http://app:8001").rstrip("/")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://dashboard:8002").rstrip("/")

#: Per-run isolation nonce — prefixes/joins every key this verifier creates,
#: so reruns against a warm, bind-mount-persisted server stay independent.
NONCE = uuid4().hex[:8]

LOG_TYPES: tuple[str, ...] = ("error_logs", "access_logs", "security_logs")
ALL_FILTERS: tuple[str, ...] = LOG_TYPES + ("sessions",)

#: Default per-request timeout. Hot endpoints answer in microseconds; 10s only
#: ever matters when something is genuinely wrong.
_TIMEOUT = 10.0

#: Timeout for the two benchmark endpoints: the demo perf test runs a
#: deliberately slow linear scan (~40M string compares at defaults) and the
#: sessions perf test seeds 1500 sqlite rows — both legitimately take seconds.
_SLOW_TIMEOUT = 120.0

#: /demo/populate count — divisible by 3, so the round-robin lands exactly
#: count/3 in each of the three log-type filters.
POPULATE_COUNT = 3_000

#: Keys this run creates (all nonce-derived; later checks reuse earlier ones).
ROUNDTRIP_KEYS = {lt: f"{NONCE}-rt-{lt}" for lt in LOG_TYPES}
ABSENT_KEY = f"absent-{NONCE}"
PIPE_KEY = f"{NONCE}-pipe-ingested"
PIPE_ABSENT_KEY = f"pipe-absent-{NONCE}"
FP_ONLY_KEY = f"{NONCE}-bloom-only-no-row"
SESSION_ID = f"sess-{NONCE}"
SESSION_ABSENT_ID = f"sess-absent-{NONCE}"

_2MB = 2 * 1024 * 1024


class Fail(AssertionError):
    """One check's failure, carrying the detail for its FAIL line."""


def _require(condition: bool, detail: str) -> None:
    """Raise :class:`Fail` with ``detail`` unless ``condition`` holds."""
    if not condition:
        raise Fail(detail)


def _wait_healthy(client: httpx.Client, base_url: str) -> int:
    """Poll ``GET {base_url}/health`` up to 30x1s; return the winning attempt.

    compose's ``depends_on: condition: service_healthy`` means attempt 1
    should win — the retry budget covers running this script by hand against
    a stack that is still starting.
    """
    last_error = "no attempt made"
    for attempt in range(1, 31):
        try:
            resp = client.get(f"{base_url}/health", timeout=5.0)
            if resp.status_code == 200 and resp.json().get("status") == "healthy":
                return attempt
            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except httpx.HTTPError as exc:
            last_error = repr(exc)
        time.sleep(1)
    raise Fail(f"{base_url}/health never healthy in 30 attempts (last: {last_error})")


# --------------------------------------------------------------------- #
# checks (each returns the PASS-line detail or raises Fail)             #
# --------------------------------------------------------------------- #


def check_health_api(client: httpx.Client) -> str:
    attempt = _wait_healthy(client, APP_URL)
    return f"{APP_URL}/health healthy (attempt {attempt})"


def check_health_dashboard(client: httpx.Client) -> str:
    attempt = _wait_healthy(client, DASHBOARD_URL)
    return f"{DASHBOARD_URL}/health healthy (attempt {attempt})"


def check_add_query_roundtrip(client: httpx.Client) -> str:
    """Per log type: add a nonce key, then query it back as probably_exists."""
    times = []
    for log_type, key in ROUNDTRIP_KEYS.items():
        body = {"log_type": log_type, "log_key": key}
        add = client.post(f"{APP_URL}/logs/add", json=body)
        _require(add.status_code == 200, f"{log_type} add HTTP {add.status_code}")
        added = add.json()
        _require(added.get("status") == "added", f"{log_type} add body {added}")

        query = client.post(f"{APP_URL}/logs/query", json=body)
        _require(query.status_code == 200, f"{log_type} query HTTP {query.status_code}")
        answer = query.json()
        _require(answer.get("might_exist") is True, f"{log_type} might_exist {answer}")
        _require(
            answer.get("confidence") == "probably_exists",
            f"{log_type} confidence {answer.get('confidence')!r}",
        )
        ms = answer.get("processing_time_ms")
        _require(
            isinstance(ms, (int, float)) and ms < 50,
            f"{log_type} processing_time_ms {ms!r} not < 50",
        )
        times.append(f"{log_type}={ms}ms")
    return f"3/3 log types roundtripped ({', '.join(times)})"


def check_absent_negative(client: httpx.Client) -> str:
    """A never-added key is a definite no — the zero-false-negative claim."""
    answer = client.post(
        f"{APP_URL}/logs/query",
        json={"log_type": "error_logs", "log_key": ABSENT_KEY},
    ).json()
    _require(answer.get("might_exist") is False, f"might_exist {answer}")
    _require(
        answer.get("confidence") == "definitely_not_exist",
        f"confidence {answer.get('confidence')!r}",
    )
    return f"{ABSENT_KEY!r} → definitely_not_exist"


def check_invalid_type_422(client: httpx.Client) -> str:
    resp = client.post(
        f"{APP_URL}/logs/add",
        json={"log_type": "bogus_logs", "log_key": f"{NONCE}-bogus"},
    )
    _require(resp.status_code == 422, f"expected 422, got HTTP {resp.status_code}")
    return "log_type 'bogus_logs' rejected with 422"


def check_populate_and_stats(client: httpx.Client) -> str:
    """/demo/populate moves the metered add counters by exactly its count."""

    def adds_snapshot() -> tuple[int, dict[str, int]]:
        stats = client.get(f"{APP_URL}/stats").json()
        per_filter = {
            name: stats["filters"][name]["adds_total"] for name in ALL_FILTERS
        }
        return stats["totals"]["adds_total"], per_filter

    before_total, before = adds_snapshot()

    resp = client.post(
        f"{APP_URL}/demo/populate",
        params={"count": POPULATE_COUNT},
        timeout=60.0,
    )
    _require(resp.status_code == 200, f"populate HTTP {resp.status_code}")
    body = resp.json()
    _require(
        body == {"status": "completed", "records_added": POPULATE_COUNT},
        f"populate body {body} (exact spec shape expected)",
    )

    after_total, after = adds_snapshot()
    _require(
        after_total - before_total == POPULATE_COUNT,
        f"totals.adds_total grew {after_total - before_total}, want {POPULATE_COUNT}",
    )
    per_type = POPULATE_COUNT // len(LOG_TYPES)
    for log_type in LOG_TYPES:
        grew = after[log_type] - before[log_type]
        _require(
            grew == per_type,
            f"{log_type}.adds_total grew {grew}, want {per_type}",
        )
    sessions_grew = after["sessions"] - before["sessions"]
    _require(sessions_grew == 0, f"sessions.adds_total grew {sessions_grew}, want 0")
    return (
        f"populate {POPULATE_COUNT} → totals +{POPULATE_COUNT}, "
        f"+{per_type}/log-type filter, sessions untouched"
    )


def check_perf_demo_gates(client: httpx.Client) -> str:
    """Bloom-vs-linear benchmark meets both spec gates at the defaults."""
    resp = client.post(f"{APP_URL}/demo/performance-test", timeout=_SLOW_TIMEOUT)
    _require(resp.status_code == 200, f"HTTP {resp.status_code}: {resp.text[:200]}")
    body = resp.json()
    speedup = body.get("speedup_vs_linear")
    ratio = body.get("memory_ratio")
    _require(
        isinstance(speedup, (int, float)) and speedup > 10,
        f"speedup_vs_linear {speedup!r} not > 10",
    )
    _require(
        isinstance(ratio, (int, float)) and ratio < 0.05,
        f"memory_ratio {ratio!r} not < 0.05",
    )
    return (
        f"speedup_vs_linear={speedup}x (gate >10), memory_ratio={ratio} "
        f"(gate <0.05), fp_observed={body.get('false_positives_observed')}"
    )


def check_pipeline_two_tier(client: httpx.Client) -> str:
    """Ingest lands in both tiers; a bloom negative skips storage entirely."""
    ingest_body = {"log_type": "error_logs", "log_key": PIPE_KEY}
    ingest = client.post(f"{APP_URL}/pipeline/ingest", json=ingest_body)
    _require(ingest.status_code == 200, f"ingest HTTP {ingest.status_code}")
    _require(
        ingest.json().get("status") == "stored", f"ingest body {ingest.json()}"
    )

    hit = client.post(f"{APP_URL}/pipeline/lookup", json=ingest_body).json()
    _require(hit.get("found") is True, f"hit lookup {hit}")
    _require(hit.get("source") == "storage", f"hit source {hit.get('source')!r}")

    miss = client.post(
        f"{APP_URL}/pipeline/lookup",
        json={"log_type": "error_logs", "log_key": PIPE_ABSENT_KEY},
    ).json()
    _require(miss.get("found") is False, f"miss lookup {miss}")
    _require(
        miss.get("storage_checked") is False,
        f"miss storage_checked {miss.get('storage_checked')!r} — the skip proof",
    )
    _require(
        miss.get("source") == "bloom_negative",
        f"miss source {miss.get('source')!r}",
    )

    totals = client.get(f"{APP_URL}/pipeline/stats").json()["_totals"]
    skipped_pct = totals.get("storage_skipped_pct")
    _require(
        isinstance(skipped_pct, (int, float)) and skipped_pct > 0,
        f"_totals.storage_skipped_pct {skipped_pct!r} not > 0",
    )
    return (
        f"hit via storage, miss skipped storage (bloom_negative), "
        f"storage_skipped_pct={skipped_pct}%"
    )


def check_pipeline_fp_detection(client: httpx.Client) -> str:
    """A key in the filter but NOT in sqlite is a storage-disproved positive."""
    add = client.post(
        f"{APP_URL}/logs/add",
        json={"log_type": "error_logs", "log_key": FP_ONLY_KEY},
    )
    _require(add.status_code == 200, f"bloom-only add HTTP {add.status_code}")

    lookup = client.post(
        f"{APP_URL}/pipeline/lookup",
        json={"log_type": "error_logs", "log_key": FP_ONLY_KEY},
    ).json()
    _require(lookup.get("found") is False, f"found {lookup}")
    _require(
        lookup.get("false_positive") is True,
        f"false_positive {lookup.get('false_positive')!r}",
    )
    _require(
        lookup.get("storage_checked") is True,
        f"storage_checked {lookup.get('storage_checked')!r}",
    )
    return "bloom positive disproved by storage → false_positive=true, found=false"


def check_sessions_flow(client: httpx.Client) -> str:
    """Ingest → query found; absent session never touches storage; <2MB live."""
    ingest = client.post(
        f"{APP_URL}/sessions/ingest", json={"session_id": SESSION_ID}
    )
    _require(ingest.status_code == 200, f"ingest HTTP {ingest.status_code}")

    found = client.post(
        f"{APP_URL}/sessions/query", json={"session_id": SESSION_ID}
    ).json()
    _require(found.get("found") is True, f"ingested session not found: {found}")

    absent = client.post(
        f"{APP_URL}/sessions/query", json={"session_id": SESSION_ABSENT_ID}
    ).json()
    _require(absent.get("found") is False, f"absent session found: {absent}")
    _require(
        absent.get("storage_checked") is False,
        f"absent storage_checked {absent.get('storage_checked')!r}",
    )

    stats = client.get(f"{APP_URL}/sessions/stats").json()
    memory_bytes = stats.get("filter", {}).get("memory_bytes")
    _require(
        stats.get("memory_under_2mb") is True,
        f"memory_under_2mb {stats.get('memory_under_2mb')!r}",
    )
    _require(
        isinstance(memory_bytes, int) and memory_bytes < _2MB,
        f"filter.memory_bytes {memory_bytes!r} not < {_2MB}",
    )
    return (
        f"ingest/query/absent-skip OK, filter at {memory_bytes} bytes "
        f"(< 2 MiB) for 1M capacity"
    )


def check_sessions_perf_gates(client: httpx.Client) -> str:
    """With/without-bloom benchmark meets the Extended-A gates.

    Gated: 100% of non-existent sessions identified, >=40% storage calls
    avoided, both paths' averages < 1 ms. ``speedup`` is reported only —
    against a warm in-process sqlite (~µs point-SELECTs) the filter's honest,
    transferable win is the avoided storage calls, not per-call latency.
    """
    resp = client.post(
        f"{APP_URL}/sessions/performance-test",
        params={"sessions": 1_500, "lookups": 800},
        timeout=_SLOW_TIMEOUT,
    )
    _require(resp.status_code == 200, f"HTTP {resp.status_code}: {resp.text[:200]}")
    body = resp.json()

    pct_correct = body.get("non_existent_correctly_identified_pct")
    avoided = body.get("storage_calls_avoided_pct")
    with_ms = body.get("with_bloom_avg_ms")
    without_ms = body.get("without_bloom_avg_ms")
    _require(
        pct_correct == 100.0,
        f"non_existent_correctly_identified_pct {pct_correct!r} != 100.0",
    )
    _require(
        isinstance(avoided, (int, float)) and avoided >= 40.0,
        f"storage_calls_avoided_pct {avoided!r} not >= 40.0",
    )
    _require(
        isinstance(with_ms, (int, float)) and with_ms < 1.0,
        f"with_bloom_avg_ms {with_ms!r} not < 1.0",
    )
    _require(
        isinstance(without_ms, (int, float)) and without_ms < 1.0,
        f"without_bloom_avg_ms {without_ms!r} not < 1.0",
    )
    return (
        f"100% non-existent identified, {avoided}% storage calls avoided, "
        f"avg {with_ms}ms with / {without_ms}ms without bloom "
        f"(speedup={body.get('speedup')}x, reported only)"
    )


def check_stats_coherence(client: httpx.Client) -> str:
    """Post-populate /stats stays coherent across all four filters.

    Restart persistence is deliberately NOT exercised here — a container
    cannot restart its sibling from inside the compose network. That path is
    covered by tests/integration/test_persistence_reload.py and the final
    main-thread `docker compose restart app` verification.
    """
    stats = client.get(f"{APP_URL}/stats").json()
    uptime = stats.get("uptime_seconds")
    _require(
        isinstance(uptime, (int, float)) and uptime >= 0,
        f"uptime_seconds {uptime!r} not >= 0",
    )
    filters = stats.get("filters", {})
    _require(
        set(filters) == set(ALL_FILTERS),
        f"filters {sorted(filters)} != {sorted(ALL_FILTERS)}",
    )
    for name, f in filters.items():
        _require(
            isinstance(f.get("memory_bytes"), int) and f["memory_bytes"] > 0,
            f"{name}.memory_bytes {f.get('memory_bytes')!r} not > 0",
        )
    totals = stats["totals"]
    # This run alone pushed >= POPULATE_COUNT unique keys through manager.add,
    # so elements_added must show at least that (snapshots can only add more).
    _require(
        totals.get("elements_added", 0) >= POPULATE_COUNT,
        f"totals.elements_added {totals.get('elements_added')!r} < {POPULATE_COUNT}",
    )
    _require(
        totals.get("memory_bytes", 0)
        == sum(f["memory_bytes"] for f in filters.values()),
        "totals.memory_bytes is not the sum of the per-filter figures",
    )
    return (
        f"uptime={uptime}s, 4 filters all report memory_bytes>0, "
        f"totals.elements_added={totals['elements_added']} (>= {POPULATE_COUNT})"
    )


def check_dashboard_page(client: httpx.Client) -> str:
    resp = client.get(f"{DASHBOARD_URL}/")
    _require(resp.status_code == 200, f"GET / HTTP {resp.status_code}")
    html = resp.text
    for marker in ("fp-chart", "card-sessions", "chart.umd.min.js"):
        _require(marker in html, f"page missing {marker!r}")
    return "page serves with fp-chart canvas, sessions card, vendored Chart.js"


def check_dashboard_static(client: httpx.Client) -> str:
    resp = client.get(f"{DASHBOARD_URL}/static/chart.umd.min.js")
    _require(resp.status_code == 200, f"HTTP {resp.status_code}")
    size = len(resp.content)
    _require(size > 100_000, f"chart.umd.min.js only {size} bytes (>100KB expected)")
    return f"chart.umd.min.js serves ({size} bytes)"


async def _read_first_tick() -> dict:
    """Connect to the dashboard WS and return its first (immediate) tick."""
    ws_url = DASHBOARD_URL.replace("https://", "wss://").replace("http://", "ws://")
    async with websockets.connect(f"{ws_url}/ws", open_timeout=10) as ws:
        raw = await asyncio.wait_for(ws.recv(), timeout=12)
    return json.loads(raw)


def check_dashboard_ws_tick(client: httpx.Client) -> str:
    """The /ws contract: an immediate, error-free tick naming all 4 filters."""
    tick = asyncio.run(_read_first_tick())
    _require(tick.get("type") == "tick", f"type {tick.get('type')!r}")
    _require(tick.get("error") is None, f"tick.error {tick.get('error')!r}")
    api = tick.get("api")
    _require(isinstance(api, dict), f"tick.api is {type(api).__name__}, want dict")
    filter_names = set(api.get("filters", {}))
    _require(
        filter_names == set(ALL_FILTERS),
        f"tick.api.filters {sorted(filter_names)} != {sorted(ALL_FILTERS)}",
    )
    return f"first tick within 12s, error=null, filters={sorted(filter_names)}"


def check_dashboard_proxy(client: httpx.Client) -> str:
    """The browser path: dashboard /proxy/query relays to the API's /logs/query."""
    key = ROUNDTRIP_KEYS["error_logs"]  # added by check 3
    resp = client.post(
        f"{DASHBOARD_URL}/proxy/query",
        json={"log_type": "error_logs", "log_key": key},
    )
    _require(resp.status_code == 200, f"HTTP {resp.status_code}: {resp.text[:200]}")
    body = resp.json()
    _require(body.get("might_exist") is True, f"proxied query body {body}")
    return f"proxy roundtrip found {key!r} (confidence={body.get('confidence')!r})"


# --------------------------------------------------------------------- #
# runner                                                                #
# --------------------------------------------------------------------- #

CHECKS: tuple[tuple[str, Callable[[httpx.Client], str]], ...] = (
    ("health-api", check_health_api),
    ("health-dashboard", check_health_dashboard),
    ("add-query-roundtrip", check_add_query_roundtrip),
    ("absent-negative", check_absent_negative),
    ("invalid-type-422", check_invalid_type_422),
    ("populate-and-stats", check_populate_and_stats),
    ("perf-demo-gates", check_perf_demo_gates),
    ("pipeline-two-tier", check_pipeline_two_tier),
    ("pipeline-fp-detection", check_pipeline_fp_detection),
    ("sessions-flow", check_sessions_flow),
    ("sessions-perf-gates", check_sessions_perf_gates),
    ("stats-coherence", check_stats_coherence),
    ("dashboard-page", check_dashboard_page),
    ("dashboard-static", check_dashboard_static),
    ("dashboard-ws-tick", check_dashboard_ws_tick),
    ("dashboard-proxy", check_dashboard_proxy),
)


def main() -> int:
    """Run every check in order, report PASS/FAIL per check, summarize."""
    print(f"E2E target: app={APP_URL} dashboard={DASHBOARD_URL} nonce={NONCE}")
    failures: list[str] = []
    with httpx.Client(timeout=_TIMEOUT) as client:
        for name, fn in CHECKS:
            try:
                detail = fn(client)
                print(f"PASS: {name} — {detail}", flush=True)
            except Fail as exc:
                failures.append(name)
                print(f"FAIL: {name} — {exc}", flush=True)
            except Exception as exc:  # noqa: BLE001 — a crash is a labelled FAIL
                failures.append(name)
                print(
                    f"FAIL: {name} — unexpected {type(exc).__name__}: {exc}",
                    flush=True,
                )
    n_total = len(CHECKS)
    n_pass = n_total - len(failures)
    print(f"E2E: {n_pass}/{n_total} checks passed")
    if failures:
        print(f"E2E: FAILED — {', '.join(failures)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
