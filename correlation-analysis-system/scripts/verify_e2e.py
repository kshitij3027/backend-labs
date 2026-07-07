"""Black-box end-to-end verifier for the Correlation Analysis System (C8).

Runs **inside Docker** (the profile-gated ``e2e`` compose service) against the
LIVE backend over HTTP only — no ``src`` imports, no Redis access — and walks
14 ordered checks through the whole loop: generation -> parsing -> collection
-> aggregation -> all five correlation detectors -> pattern learning -> alerts
-> the full v1 API surface. The first failing check prints a loud ``FAIL`` line
and exits non-zero immediately, so ``make e2e`` propagates it.

The 14 checks, in order:

 1. /health contract (spec-verbatim ``status``/``service``)
 2. logs flowing: exact count semantics, all 5 sources, fresh timestamps
 3. ingest throughput: events_processed delta >= MIN_EVENTS_PER_SEC
 4. correlations appear within 30 s, every row structurally valid
 5. all 5 correlation types within E2E_TYPES_TIMEOUT
 6. stats contract: exactly {total, types, avg_strength, recent_count} + growth
 7. type-filter purity + unknown type -> HTTP 422
 8. detection latency: p95 over >= 20 event-pair rows <= E2E_MAX_P95_LATENCY
 9. session accuracy vs /api/v1/debug/ground-truth: recall >=
    E2E_MIN_SESSION_RECALL over >= E2E_MIN_GT_JOURNEYS journeys, 100% id-match
    precision
10. the 3 scenario target metric pairs at p_adj < E2E_MAX_P_ADJ plus a
    database -> web/api_service error cascade, within E2E_TARGETS_TIMEOUT
11. alerts fire: valid severities, at least one critical, within
    E2E_ALERTS_TIMEOUT
12. dashboard payload shape: exact 9 keys, canonical symmetric 5x5 matrix,
    ascending timeline, capped scatter
13. pattern learning: some correlation carries details.pattern_count >= 2
14. backend memory_mb < MAX_BACKEND_MEM_MB

The 1000-row /api/v1/correlations window rotates quickly (~70% of it is
session_based), so every fetch is folded into one cumulative
:class:`Sightings` accumulator — types, session correlation ids, target metric
pairs, cascade directions, pattern counts. Checks 5, 9, 10, 11 and 13 assert
against the accumulated evidence, never against a single snapshot.

Environment knobs (all optional):

* ``E2E_BASE_URL``           backend base URL (default ``http://backend:8000``)
* ``E2E_READY_TIMEOUT``      seconds to wait for /health (default 90)
* ``MIN_EVENTS_PER_SEC``     ingest throughput gate (default 100)
* ``E2E_MAX_P95_LATENCY``    detection-latency p95 gate, seconds (default 5.0)
* ``E2E_MIN_SESSION_RECALL`` session recall gate (default 0.95)
* ``E2E_MAX_P_ADJ``          BH-adjusted p ceiling for target pairs (default 0.05)
* ``MAX_BACKEND_MEM_MB``     backend RSS gate, MB (default 200)
* ``E2E_MIN_GT_JOURNEYS``    ground-truth sample floor for check 9 (default 20)
* ``E2E_MIN_LATENCY_ROWS``   event-pair sample floor for check 8 (default 20)
* ``E2E_TYPES_TIMEOUT`` / ``E2E_SESSION_TIMEOUT`` / ``E2E_TARGETS_TIMEOUT`` /
  ``E2E_ALERTS_TIMEOUT``     polling budgets, seconds (120/120/180/180)

Exit code: 0 with ``E2E PASSED (14/14)`` only when every check holds.
"""

from __future__ import annotations

import math
import os
import sys
import time
from collections.abc import Callable
from typing import Any

import httpx

# --------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------- #
BASE_URL = os.environ.get("E2E_BASE_URL", "http://backend:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("E2E_READY_TIMEOUT", "90"))
MIN_EVENTS_PER_SEC = float(os.environ.get("MIN_EVENTS_PER_SEC", "100"))
MAX_P95_LATENCY = float(os.environ.get("E2E_MAX_P95_LATENCY", "5.0"))
MIN_SESSION_RECALL = float(os.environ.get("E2E_MIN_SESSION_RECALL", "0.95"))
MAX_P_ADJ = float(os.environ.get("E2E_MAX_P_ADJ", "0.05"))
MAX_BACKEND_MEM_MB = float(os.environ.get("MAX_BACKEND_MEM_MB", "200"))
MIN_GT_JOURNEYS = int(os.environ.get("E2E_MIN_GT_JOURNEYS", "20"))
MIN_LATENCY_ROWS = int(os.environ.get("E2E_MIN_LATENCY_ROWS", "20"))
TYPES_TIMEOUT = float(os.environ.get("E2E_TYPES_TIMEOUT", "120"))
SESSION_TIMEOUT = float(os.environ.get("E2E_SESSION_TIMEOUT", "120"))
TARGETS_TIMEOUT = float(os.environ.get("E2E_TARGETS_TIMEOUT", "180"))
ALERTS_TIMEOUT = float(os.environ.get("E2E_ALERTS_TIMEOUT", "180"))

TOTAL_CHECKS = 14

#: Spec-verbatim vocabulary the live payloads must match exactly.
SOURCES = ("web", "database", "api_service", "payment", "inventory")
CORRELATION_TYPES = frozenset(
    {"temporal", "session_based", "user_based", "error_cascade", "metric_based"}
)
REQUIRED_EVENT_KEYS = frozenset({"id", "timestamp", "source", "service", "level", "message"})
STATS_KEYS = frozenset({"total", "types", "avg_strength", "recent_count"})
DASHBOARD_KEYS = frozenset(
    {
        "generated_at",
        "status",
        "stats",
        "timeline",
        "scatter",
        "matrix",
        "recent_correlations",
        "recent_logs",
        "alerts",
    }
)
SCATTER_FIELDS = frozenset({"strength", "confidence", "type", "detected_at"})
ALERT_SEVERITIES = frozenset({"info", "warning", "critical"})

#: The 3 relationships the generator's incident scenarios manufacture, as
#: sorted (metric_a, metric_b) keys (mirrors src.engine.metric.TARGET_PAIRS).
TARGET_METRIC_PAIRS: tuple[tuple[str, str], ...] = tuple(
    tuple(sorted(pair))  # type: ignore[misc]
    for pair in (
        ("web.error_rate", "db.pool_utilization"),
        ("payment.latency_ms_avg", "user.abandonment_count"),
        ("inventory.timeout_count", "checkout.failure_count"),
    )
)

#: Check 9 only trusts ground-truth journeys completed at least this many
#: seconds ago — younger ones may legitimately still be inside the detection
#: pipeline, so counting them would measure latency, not accuracy.
GT_MIN_COMPLETED_AGE = 10.0

#: Cursory-poll cadences (seconds) for the polling checks.
_POLL_FAST = 5.0
_POLL_SLOW = 10.0


class CheckFailure(AssertionError):
    """Raised inside a check to fail it with a single clear detail line."""


# --------------------------------------------------------------------------- #
# HTTP plumbing
# --------------------------------------------------------------------------- #
CLIENT = httpx.Client(base_url=BASE_URL, timeout=15.0)


def get_json(path: str, params: dict[str, Any] | None = None) -> tuple[int, Any]:
    """GET ``path`` and return (status_code, parsed JSON body or None)."""
    try:
        resp = CLIENT.get(path, params=params)
    except Exception as exc:  # noqa: BLE001 — network failure = check failure
        raise CheckFailure(f"GET {path} raised {type(exc).__name__}: {exc}") from exc
    try:
        body = resp.json()
    except ValueError:
        body = None
    return resp.status_code, body


def api(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET ``path`` expecting HTTP 200 JSON; anything else fails the check."""
    status, body = get_json(path, params)
    if status != 200 or body is None:
        raise CheckFailure(f"GET {path} -> HTTP {status} (expected 200 with a JSON body)")
    return body


def wait_ready(timeout: float = READY_TIMEOUT) -> None:
    """Poll GET /health until it answers 200, or exit 1 at the timeout."""
    print(f"[e2e] waiting for {BASE_URL}/health (up to {timeout:.0f}s)...", flush=True)
    deadline = time.time() + timeout
    last = "no response"
    while time.time() < deadline:
        try:
            resp = CLIENT.get("/health", timeout=5.0)
            if resp.status_code == 200:
                print("[e2e] backend is ready", flush=True)
                return
            last = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001 — the service may still be starting
            last = type(exc).__name__
        time.sleep(2.0)
    print(f"FAIL bootstrap: /health not ready after {timeout:.0f}s (last: {last})", flush=True)
    sys.exit(1)


# --------------------------------------------------------------------------- #
# Cumulative evidence across every /api/v1/correlations fetch
# --------------------------------------------------------------------------- #
class Sightings:
    """Everything observed so far across every correlations poll.

    The live 1000-row window holds only the newest ~1-2 minutes of findings
    (and ~70% of it is session_based), so rare rows — a specific target metric
    pair, a cascade with the right direction, an early journey's session row —
    rotate out between polls. Folding EVERY fetched batch in here lets the
    polling checks assert against all evidence ever seen, not one snapshot.
    """

    def __init__(self) -> None:
        self.types: set[str] = set()
        #: correlation_ids of every session_based row's linked journey.
        self.session_ids: set[str] = set()
        #: sorted (metric_a, metric_b) -> best (lowest) p_adj observed.
        self.metric_pair_best: dict[tuple[str, str], float] = {}
        #: sorted metric pairs already seen with p_adj < MAX_P_ADJ.
        self.metric_pairs_ok: set[tuple[str, str]] = set()
        #: an error_cascade chained database into web or api_service.
        self.cascade_db_to_frontline = False
        self.max_pattern_count = 0

    def ingest(self, rows: list[dict[str, Any]]) -> None:
        """Fold one fetched batch of correlation rows into the accumulators."""
        for row in rows:
            rtype = row.get("correlation_type")
            if isinstance(rtype, str):
                self.types.add(rtype)
            details = row.get("details") or {}
            count = details.get("pattern_count")
            if isinstance(count, int) and count > self.max_pattern_count:
                self.max_pattern_count = count
            if rtype == "session_based":
                cid = (row.get("event_a") or {}).get("correlation_id")
                if cid:
                    self.session_ids.add(cid)
            elif rtype == "metric_based":
                self._ingest_metric(details)
            elif rtype == "error_cascade":
                chain = details.get("chain") or []
                chain_sources = {
                    hop.get("source") for hop in chain if isinstance(hop, dict)
                }
                if "database" in chain_sources and chain_sources & {"web", "api_service"}:
                    self.cascade_db_to_frontline = True

    def _ingest_metric(self, details: dict[str, Any]) -> None:
        metric_a, metric_b = details.get("metric_a"), details.get("metric_b")
        p_adj = details.get("p_adj")  # absent on jaccard/MI rows — skip those
        if not metric_a or not metric_b or not isinstance(p_adj, (int, float)):
            return
        key: tuple[str, str] = tuple(sorted((metric_a, metric_b)))  # type: ignore[assignment]
        best = self.metric_pair_best.get(key)
        if best is None or p_adj < best:
            self.metric_pair_best[key] = float(p_adj)
        if p_adj < MAX_P_ADJ:
            self.metric_pairs_ok.add(key)


SIGHT = Sightings()


def fetch_correlations(limit: int = 1000) -> list[dict[str, Any]]:
    """GET /api/v1/correlations?limit=N, fold it into SIGHT, return the rows."""
    rows = api("/api/v1/correlations", params={"limit": limit}).get("correlations") or []
    SIGHT.ingest(rows)
    return rows


# --------------------------------------------------------------------------- #
# Check runner
# --------------------------------------------------------------------------- #
_counter = 0


def check(name: str, fn: Callable[[], str]) -> None:
    """Run one check; print PASS with evidence, or FAIL + exit 1 immediately."""
    global _counter
    _counter += 1
    prefix = f"[{_counter:2d}/{TOTAL_CHECKS}]"
    try:
        evidence = fn()
    except CheckFailure as exc:
        print(f"{prefix} FAIL {name}: {exc}", flush=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 — an unexpected error is still a failure
        print(f"{prefix} FAIL {name}: unexpected {type(exc).__name__}: {exc}", flush=True)
        sys.exit(1)
    print(f"{prefix} PASS {name} ({evidence})", flush=True)


# --------------------------------------------------------------------------- #
# The 14 checks, in run order
# --------------------------------------------------------------------------- #
def check_health() -> str:
    """1. /health answers 200 with the spec-verbatim status/service values."""
    body = api("/health")
    if body.get("status") != "healthy":
        raise CheckFailure(f'status is {body.get("status")!r} (want "healthy")')
    if body.get("service") != "correlation-analysis":
        raise CheckFailure(f'service is {body.get("service")!r} (want "correlation-analysis")')
    return f'status/service verbatim; uptime {body.get("uptime_seconds", 0):.0f}s'


def check_logs_flowing() -> str:
    """2. Recent-logs semantics: exact count, full event shape, 5 live sources."""
    events = api("/api/v1/logs/recent", params={"count": 5}).get("events")
    if not isinstance(events, list) or len(events) != 5:
        got = len(events) if isinstance(events, list) else repr(events)
        raise CheckFailure(f"count=5 returned {got} events (want exactly 5)")
    for event in events:
        missing = REQUIRED_EVENT_KEYS - set(event)
        if missing:
            raise CheckFailure(f"event {event.get('id')!r} missing keys {sorted(missing)}")

    seen_sources: set[str] = set()
    newest_age = math.inf
    for attempt in range(3):
        batch = api("/api/v1/logs/recent", params={"count": 200}).get("events") or []
        seen_sources |= {event.get("source") for event in batch}
        if batch:
            newest_age = time.time() - max(event["timestamp"] for event in batch)
        if set(SOURCES) <= seen_sources:
            break
        if attempt < 2:
            time.sleep(_POLL_FAST)
    missing_sources = set(SOURCES) - seen_sources
    if missing_sources:
        raise CheckFailure(
            f"sources never seen across 3 polls of count=200: {sorted(missing_sources)}"
        )
    if newest_age > 30.0:
        raise CheckFailure(f"newest event is {newest_age:.1f}s old (> 30s: pipeline stalled?)")
    return f"exact count + full shape; all 5 sources; newest event {newest_age:.1f}s old"


def check_throughput() -> str:
    """3. events_processed delta over 15 s sustains the ingest gate."""
    first = api("/api/v1/dashboard")["stats"].get("events_processed", 0)
    t0 = time.perf_counter()
    time.sleep(15.0)
    second = api("/api/v1/dashboard")["stats"].get("events_processed", 0)
    elapsed = time.perf_counter() - t0
    rate = (second - first) / elapsed
    if rate < MIN_EVENTS_PER_SEC:
        raise CheckFailure(
            f"{rate:.1f} events/s < gate {MIN_EVENTS_PER_SEC:.0f} "
            f"(events_processed {first} -> {second} over {elapsed:.1f}s)"
        )
    return (
        f"{rate:.1f} events/s >= {MIN_EVENTS_PER_SEC:.0f} "
        f"({first} -> {second} in {elapsed:.1f}s)"
    )


def _validate_row(row: dict[str, Any]) -> None:
    """Structural validity of one correlation row (types, ranges, event refs)."""
    row_id = row.get("id", "<no id>")
    if row.get("correlation_type") not in CORRELATION_TYPES:
        raise CheckFailure(f"row {row_id}: bad correlation_type {row.get('correlation_type')!r}")
    for field in ("strength", "confidence"):
        value = row.get(field)
        if not isinstance(value, (int, float)) or not 0.0 <= float(value) <= 1.0:
            raise CheckFailure(f"row {row_id}: {field}={value!r} outside [0, 1]")
    for side in ("event_a", "event_b"):
        ref = row.get(side)
        if not isinstance(ref, dict) or not ref:
            raise CheckFailure(f"row {row_id}: {side} is {ref!r} (want a non-null event ref)")
        if not ref.get("source") or not isinstance(ref.get("timestamp"), (int, float)):
            raise CheckFailure(f"row {row_id}: {side} lacks source/timestamp: {ref!r}")


def check_correlations_appear() -> str:
    """4. Correlations exist within 30 s and every row is structurally valid."""
    deadline = time.time() + 30.0
    while True:
        rows = api("/api/v1/correlations", params={"limit": 100}).get("correlations") or []
        if rows:
            break
        if time.time() >= deadline:
            raise CheckFailure("no correlations within 30s of the check starting")
        time.sleep(_POLL_FAST)
    SIGHT.ingest(rows)
    for row in rows:
        _validate_row(row)
    return f"{len(rows)} rows, all structurally valid"


def check_all_types() -> str:
    """5. All 5 correlation types accumulate within the budget."""
    start = time.time()
    while True:
        fetch_correlations()
        if CORRELATION_TYPES <= SIGHT.types:
            return f"all 5 types after {time.time() - start:.0f}s"
        if time.time() - start >= TYPES_TIMEOUT:
            missing = sorted(CORRELATION_TYPES - SIGHT.types)
            raise CheckFailure(f"types never seen within {TYPES_TIMEOUT:.0f}s: {missing}")
        time.sleep(_POLL_SLOW)


def check_stats_contract() -> str:
    """6. Stats payload is exactly the 4 spec keys and total strictly grows."""
    first = api("/api/v1/correlations/stats")
    if set(first) != STATS_KEYS:
        raise CheckFailure(f"keys {sorted(first)} != {sorted(STATS_KEYS)}")
    avg = first.get("avg_strength")
    if not isinstance(avg, (int, float)) or not 0.0 < float(avg) <= 1.0:
        raise CheckFailure(f"avg_strength {avg!r} outside (0, 1]")
    time.sleep(10.0)
    second = api("/api/v1/correlations/stats")
    if set(second) != STATS_KEYS:
        raise CheckFailure(f"keys drifted between polls: {sorted(second)}")
    if second["total"] <= first["total"]:
        raise CheckFailure(f"total did not grow over 10s ({first['total']} -> {second['total']})")
    return f"exact 4 keys; total {first['total']} -> {second['total']}; avg_strength {avg:.3f}"


def check_type_filter() -> str:
    """7. Type filter returns only that type; a bogus type is HTTP 422."""
    body = api("/api/v1/correlations/types/session_based", params={"limit": 50})
    rows = body.get("correlations") or []
    if not rows:
        raise CheckFailure("no session_based rows returned (they were seen in check 5)")
    impure = [row for row in rows if row.get("correlation_type") != "session_based"]
    if impure:
        raise CheckFailure(
            f"{len(impure)} rows of other types leaked in "
            f"(first: {impure[0].get('correlation_type')!r})"
        )
    SIGHT.ingest(rows)
    status, _ = get_json("/api/v1/correlations/types/bogus")
    if status != 422:
        raise CheckFailure(f"/types/bogus -> HTTP {status} (want 422)")
    return f"{len(rows)} session_based rows pure; bogus type -> 422"


def check_detection_latency() -> str:
    """8. p95 of detected_at - newest event timestamp over event-pair rows."""
    deadline = time.time() + 60.0
    while True:
        rows = [
            row
            for row in fetch_correlations()
            # metric_based rows are series-level (refs are representative, not
            # the causal events), so they are excluded from the latency gate.
            if row.get("correlation_type") != "metric_based"
        ]
        if len(rows) >= MIN_LATENCY_ROWS:
            break
        if time.time() >= deadline:
            raise CheckFailure(
                f"only {len(rows)} event-pair rows within 60s (need >= {MIN_LATENCY_ROWS})"
            )
        time.sleep(_POLL_FAST)

    latencies: list[float] = []
    for row in rows:
        newest_event = max(row["event_a"]["timestamp"], row["event_b"]["timestamp"])
        latency = row["detected_at"] - newest_event
        if latency < -0.001:
            raise CheckFailure(
                f"row {row.get('id')} detected {-latency:.3f}s BEFORE its newest event"
            )
        latencies.append(latency)
    latencies.sort()
    p95 = latencies[math.ceil(0.95 * len(latencies)) - 1]
    worst = latencies[-1]
    if p95 > MAX_P95_LATENCY:
        raise CheckFailure(
            f"p95 {p95:.2f}s > gate {MAX_P95_LATENCY}s over n={len(latencies)} "
            f"(max {worst:.2f}s)"
        )
    return f"p95 {p95:.2f}s, max {worst:.2f}s over n={len(latencies)} event-pair rows"


def check_session_accuracy() -> str:
    """9. Recall vs generator ground truth + 100% id-match precision."""
    deadline = time.time() + SESSION_TIMEOUT
    last_recall: float | None = None
    last_n = 0
    while True:
        gt_journeys = api("/api/v1/debug/ground-truth", params={"max_age": 120}).get(
            "journeys"
        ) or []
        now = time.time()
        gt_ids = {
            journey["correlation_id"]
            for journey in gt_journeys
            if len(set(journey["sources"])) >= 2
            and now - journey["completed_at"] >= GT_MIN_COMPLETED_AGE
        }

        rows = api(
            "/api/v1/correlations/types/session_based", params={"limit": 1000}
        ).get("correlations") or []
        for row in rows:  # precision: every session row links one single journey
            a_cid = (row.get("event_a") or {}).get("correlation_id")
            b_cid = (row.get("event_b") or {}).get("correlation_id")
            if a_cid is None or a_cid != b_cid:
                raise CheckFailure(
                    f"precision violation: session row {row.get('id')} links "
                    f"{a_cid!r} with {b_cid!r}"
                )
        SIGHT.ingest(rows)

        if len(gt_ids) >= MIN_GT_JOURNEYS:
            recall = len(gt_ids & SIGHT.session_ids) / len(gt_ids)
            last_recall, last_n = recall, len(gt_ids)
            if recall >= MIN_SESSION_RECALL:
                return (
                    f"recall {recall:.3f} over n={len(gt_ids)} ground-truth journeys "
                    f"(gate {MIN_SESSION_RECALL}); precision 1.000 over {len(rows)} rows"
                )
        if time.time() >= deadline:
            if last_recall is None:
                raise CheckFailure(
                    f"never saw >= {MIN_GT_JOURNEYS} eligible ground-truth journeys "
                    f"within {SESSION_TIMEOUT:.0f}s (last poll had {len(gt_ids)})"
                )
            raise CheckFailure(
                f"recall {last_recall:.3f} < gate {MIN_SESSION_RECALL} over "
                f"n={last_n} after {SESSION_TIMEOUT:.0f}s"
            )
        time.sleep(_POLL_FAST)


def check_target_correlations() -> str:
    """10. The 3 scenario metric pairs (p_adj significant) + db cascade direction."""
    start = time.time()
    while True:
        fetch_correlations()
        missing_pairs = [
            pair for pair in TARGET_METRIC_PAIRS if pair not in SIGHT.metric_pairs_ok
        ]
        if not missing_pairs and SIGHT.cascade_db_to_frontline:
            return (
                f"3/3 target metric pairs at p_adj < {MAX_P_ADJ} and a "
                f"database -> web/api_service cascade after {time.time() - start:.0f}s"
            )
        if time.time() - start >= TARGETS_TIMEOUT:
            problems = []
            for pair in missing_pairs:
                best = SIGHT.metric_pair_best.get(pair)
                seen = f"best p_adj {best:.4f}" if best is not None else "never seen"
                problems.append(f"metric pair {pair[0]} ~ {pair[1]} ({seen})")
            if not SIGHT.cascade_db_to_frontline:
                problems.append("no error_cascade chaining database into web/api_service")
            raise CheckFailure(
                f"after {TARGETS_TIMEOUT:.0f}s still missing: " + "; ".join(problems)
            )
        time.sleep(_POLL_SLOW)


def check_alerts() -> str:
    """11. Alerts exist with valid severities and at least one critical fires."""
    start = time.time()
    seen: dict[str, str] = {}  # alert id -> severity (accumulated across polls)
    while True:
        alerts = api("/api/v1/dashboard").get("alerts") or []
        for alert in alerts:
            severity = alert.get("severity")
            if severity not in ALERT_SEVERITIES:
                raise CheckFailure(
                    f"alert {alert.get('id')} has invalid severity {severity!r}"
                )
            seen[alert.get("id", repr(alert))] = severity
        if seen and "critical" in seen.values():
            counts = {
                level: sum(1 for value in seen.values() if value == level)
                for level in sorted({*seen.values()})
            }
            return (
                f"{len(seen)} distinct alerts {counts} incl. critical, "
                f"after {time.time() - start:.0f}s"
            )
        if time.time() - start >= ALERTS_TIMEOUT:
            if not seen:
                raise CheckFailure(f"no alerts at all within {ALERTS_TIMEOUT:.0f}s")
            raise CheckFailure(
                f"no critical alert within {ALERTS_TIMEOUT:.0f}s "
                f"({len(seen)} alerts seen, severities {sorted(set(seen.values()))})"
            )
        time.sleep(_POLL_SLOW)


def check_dashboard_shape() -> str:
    """12. Dashboard payload: exact keys, canonical symmetric matrix, sane feeds."""
    body = api("/api/v1/dashboard")
    if set(body) != DASHBOARD_KEYS:
        extra = sorted(set(body) - DASHBOARD_KEYS)
        missing = sorted(DASHBOARD_KEYS - set(body))
        raise CheckFailure(f"top-level keys drifted (extra={extra}, missing={missing})")

    matrix = body["matrix"]
    if matrix.get("sources") != list(SOURCES):
        raise CheckFailure(f"matrix.sources {matrix.get('sources')!r} != {list(SOURCES)}")
    cells = matrix.get("cells")
    if not isinstance(cells, list) or len(cells) != 5 or any(len(row) != 5 for row in cells):
        raise CheckFailure("matrix.cells is not 5x5")
    for i in range(5):
        for j in range(5):
            if abs(cells[i][j] - cells[j][i]) >= 1e-9:
                raise CheckFailure(
                    f"matrix asymmetric at [{i}][{j}]: {cells[i][j]} vs {cells[j][i]}"
                )

    timeline = body["timeline"]
    if len(timeline) > 60:
        raise CheckFailure(f"timeline has {len(timeline)} buckets (> 60)")
    stamps = [bucket["t"] for bucket in timeline]
    if any(b <= a for a, b in zip(stamps, stamps[1:])):
        raise CheckFailure("timeline bucket timestamps are not strictly ascending")

    scatter = body["scatter"]
    if len(scatter) > 200:
        raise CheckFailure(f"scatter has {len(scatter)} points (> 200)")
    for point in scatter:
        if set(point) != SCATTER_FIELDS:
            raise CheckFailure(f"scatter point fields {sorted(point)} != {sorted(SCATTER_FIELDS)}")
    return (
        f"exact 9 keys; 5x5 symmetric matrix; timeline {len(timeline)} buckets "
        f"ascending; scatter {len(scatter)} points"
    )


def check_pattern_learning() -> str:
    """13. Some correlation was re-detected (details.pattern_count >= 2)."""
    deadline = time.time() + 60.0
    while True:
        fetch_correlations()
        if SIGHT.max_pattern_count >= 2:
            return (
                f"max details.pattern_count {SIGHT.max_pattern_count} "
                f"(>= 2: patterns re-detected and learned)"
            )
        if time.time() >= deadline:
            raise CheckFailure(
                f"no row reached pattern_count >= 2 within 60s "
                f"(max seen {SIGHT.max_pattern_count})"
            )
        time.sleep(_POLL_FAST)


def check_memory() -> str:
    """14. Backend-reported RSS stays under the ceiling."""
    memory_mb = api("/health").get("memory_mb")
    if not isinstance(memory_mb, (int, float)) or isinstance(memory_mb, bool):
        raise CheckFailure(f"memory_mb is {memory_mb!r} (want a number)")
    if memory_mb >= MAX_BACKEND_MEM_MB:
        raise CheckFailure(f"memory_mb {memory_mb:.1f} >= gate {MAX_BACKEND_MEM_MB:.0f} MB")
    return f"{memory_mb:.1f} MB < {MAX_BACKEND_MEM_MB:.0f} MB"


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #
def main() -> None:
    print(f"[e2e] == Correlation Analysis System black-box verifier vs {BASE_URL} ==", flush=True)
    print(
        f"[e2e] gates: ingest >= {MIN_EVENTS_PER_SEC:.0f} events/s; detection p95 <= "
        f"{MAX_P95_LATENCY}s; session recall >= {MIN_SESSION_RECALL}; target p_adj < "
        f"{MAX_P_ADJ}; memory < {MAX_BACKEND_MEM_MB:.0f} MB",
        flush=True,
    )
    wait_ready()

    check("health contract", check_health)
    check("logs flowing from all 5 sources", check_logs_flowing)
    check("ingest throughput gate", check_throughput)
    check("correlations appear and validate", check_correlations_appear)
    check("all 5 correlation types detected", check_all_types)
    check("stats contract and growth", check_stats_contract)
    check("type filter purity + bogus 422", check_type_filter)
    check("detection latency p95 gate", check_detection_latency)
    check("session accuracy vs ground truth", check_session_accuracy)
    check("target correlations and cascade direction", check_target_correlations)
    check("alerts fire including critical", check_alerts)
    check("dashboard payload shape", check_dashboard_shape)
    check("pattern learning re-detection", check_pattern_learning)
    check("memory ceiling", check_memory)

    print(f"E2E PASSED ({TOTAL_CHECKS}/{TOTAL_CHECKS})", flush=True)


if __name__ == "__main__":
    main()
