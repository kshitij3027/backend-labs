#!/usr/bin/env python3
"""Black-box full-flow end-to-end verifier (Commit 13).

The single, authoritative *"does the whole user/data flow actually work?"* check for
the clustering engine. It runs **inside Docker** (the ``e2e`` compose profile) against
the live ``app`` over HTTP + WebSocket and **never imports the app's API layer** — it
only speaks the public REST/WS surface, exactly as a real client (or the React
dashboard) would. It *does* import :func:`src.log_generator.generate_logs` to fabricate
deterministic request bodies (``PYTHONPATH=/app`` in the tester image), which is pure
data generation and touches none of the running service.

It walks the complete flow end to end, printing ``PASS`` for each step and exiting
non-zero on the first failure so a broken flow fails ``make e2e`` loudly:

1. wait for ``GET /health`` to report ``status == "ok"`` with 3 algorithms;
2. ``POST /cluster`` of a hand-built security log (failed logins from a bad IP) returns
   3 per-algorithm results (kmeans/dbscan/hdbscan), masks the IP in ``masked_message``,
   and carries a ``pattern_type``;
3. ``POST /cluster/batch`` of ``generate_logs(50, seed=5)`` returns 50 assignments;
4. ``GET /stats`` reports ``total_processed >= 51`` and carries the throughput /
   total_clusters / silhouette keys;
5. ``GET /clusters`` has all 3 algorithm keys; ``GET /clusters/kmeans`` is a list;
   ``GET /clusters/kmeans/0`` is a dict; ``GET /clusters/bogus`` is a 404;
6. ``GET /patterns`` is non-empty; ``GET /anomalies?limit=10`` is a list;
   ``GET /scatter/kmeans?limit=50`` is a list of ``{x, y, cluster_id}``;
   ``GET /config`` carries kmeans/dbscan/hdbscan;
7. the ``WS /ws/stream`` socket pushes a ``{"type": "snapshot", "stats": {...}}`` frame;
8. print ``E2E PASS`` and exit 0; any failed assertion prints ``FAIL: ...`` and exits 1.

Configuration (environment)
---------------------------
* ``APP_URL`` — base URL of the live app (default ``http://app:8000``). The WebSocket
  URL is derived from it (``http``/``https`` -> ``ws``/``wss``, path ``/ws/stream``).
* ``HEALTH_TIMEOUT_SEC`` — bounded wait for the app to become ready (default 90s).
* ``WS_RECV_TIMEOUT_SEC`` — bound on the WebSocket first-frame wait (default 15s).
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Optional

import requests
from websocket import create_connection  # from the ``websocket-client`` package

from src.log_generator import generate_logs

# --- configuration (all overridable via env, all bounded) --------------------

APP_URL: str = os.environ.get("APP_URL", "http://app:8000").rstrip("/")
#: How long to wait for the app to report ready before giving up.
HEALTH_TIMEOUT_SEC: float = float(os.environ.get("HEALTH_TIMEOUT_SEC", "90"))
#: How long to block on the WebSocket waiting for the first snapshot frame.
WS_RECV_TIMEOUT_SEC: float = float(os.environ.get("WS_RECV_TIMEOUT_SEC", "15"))

#: The three algorithms the engine must always expose.
ALGORITHMS: tuple[str, ...] = ("kmeans", "dbscan", "hdbscan")

#: A hand-built security log: repeated failed logins from a single bad IP. The engine
#: must mask the embedded IP in ``masked_message`` and assign a ``pattern_type``.
SECURITY_IP: str = "203.0.113.77"
SECURITY_LOG: dict[str, Any] = {
    "timestamp": "2026-06-23T02:14:05",
    "service": "auth",
    "level": "ERROR",
    "message": (
        f"Multiple failed login attempts detected from {SECURITY_IP} (37 attempts)"
    ),
    "source_ip": SECURITY_IP,
    "endpoint": "/api/v1/login",
    "response_time_ms": 42.0,
    "status_code": 429,
}


class StepError(Exception):
    """Raised by a check helper to signal a failed E2E step (caught in ``main``)."""


def _ws_url() -> str:
    """Derive the ``ws(s)://.../ws/stream`` URL from :data:`APP_URL`."""
    if APP_URL.startswith("https://"):
        return "wss://" + APP_URL[len("https://"):] + "/ws/stream"
    if APP_URL.startswith("http://"):
        return "ws://" + APP_URL[len("http://"):] + "/ws/stream"
    # No scheme — assume plain ws over the given host:port.
    return "ws://" + APP_URL + "/ws/stream"


def _passed(step: str, detail: str = "") -> None:
    """Print a uniform ``PASS`` line for ``step`` (with optional detail)."""
    suffix = f" — {detail}" if detail else ""
    print(f"[e2e] PASS: {step}{suffix}")


def _require(condition: bool, step: str, detail: str) -> None:
    """Raise :class:`StepError` (failing the step) unless ``condition`` holds."""
    if not condition:
        raise StepError(f"{step}: {detail}")


def _get(session: requests.Session, path: str, **kw: Any) -> requests.Response:
    """``GET {APP_URL}{path}`` with a default timeout."""
    kw.setdefault("timeout", 30)
    return session.get(f"{APP_URL}{path}", **kw)


def _post(session: requests.Session, path: str, **kw: Any) -> requests.Response:
    """``POST {APP_URL}{path}`` with a default timeout."""
    kw.setdefault("timeout", 60)
    return session.post(f"{APP_URL}{path}", **kw)


# --- individual steps --------------------------------------------------------


def step_wait_health(session: requests.Session) -> None:
    """Block until ``GET /health`` reports ``status == "ok"`` with 3 algorithms."""
    deadline = time.time() + HEALTH_TIMEOUT_SEC
    last_err: Optional[str] = None
    while time.time() < deadline:
        try:
            resp = _get(session, "/health", timeout=5)
            if resp.status_code == 200:
                body = resp.json()
                if body.get("status") == "ok" and len(body.get("algorithms", [])) == 3:
                    _passed("GET /health is ok", str(body))
                    return
                last_err = f"status={body.get('status')}, algos={body.get('algorithms')}"
            else:
                last_err = f"HTTP {resp.status_code}"
        except requests.RequestException as exc:
            last_err = repr(exc)
        time.sleep(2)
    raise StepError(f"GET /health never reported ready ({last_err})")


def step_cluster_security(session: requests.Session) -> None:
    """``POST /cluster`` a security log -> 3 results, IP masked, pattern_type set."""
    resp = _post(session, "/cluster", json=SECURITY_LOG)
    _require(resp.status_code == 200, "POST /cluster", f"HTTP {resp.status_code}: {resp.text[:200]}")
    body = resp.json()

    results = body.get("results") or []
    _require(
        len(results) == 3,
        "POST /cluster",
        f"got {len(results)} results, expected 3",
    )
    algos = sorted(r.get("algorithm") for r in results)
    _require(
        algos == sorted(ALGORITHMS),
        "POST /cluster",
        f"result algorithms {algos}, expected {sorted(ALGORITHMS)}",
    )

    masked = body.get("masked_message")
    _require(
        isinstance(masked, str) and masked != "",
        "POST /cluster",
        f"masked_message missing/empty: {masked!r}",
    )
    _require(
        SECURITY_IP not in masked,
        "POST /cluster",
        f"masked_message still contains the raw IP {SECURITY_IP!r}: {masked!r}",
    )

    _require(
        body.get("pattern_type") is not None,
        "POST /cluster",
        f"pattern_type is None (body keys: {sorted(body)})",
    )
    _passed(
        "POST /cluster security log -> 3 results, IP masked",
        f"pattern_type={body.get('pattern_type')!r}, masked={masked!r}",
    )


def step_cluster_batch(session: requests.Session) -> None:
    """``POST /cluster/batch`` of 50 generated logs -> 50 assignments."""
    logs = [log.model_dump(mode="json") for log in generate_logs(50, seed=5)]
    _require(len(logs) == 50, "POST /cluster/batch", f"generator produced {len(logs)} logs, expected 50")
    resp = _post(session, "/cluster/batch", json={"logs": logs})
    _require(
        resp.status_code == 200,
        "POST /cluster/batch",
        f"HTTP {resp.status_code}: {resp.text[:200]}",
    )
    body = resp.json()
    _require(
        isinstance(body, list) and len(body) == 50,
        "POST /cluster/batch",
        f"expected a list of 50 assignments, got {type(body).__name__} of len "
        f"{len(body) if isinstance(body, list) else 'n/a'}",
    )
    # Sanity: each assignment should itself carry 3 per-algorithm results.
    first = body[0]
    _require(
        len(first.get("results", [])) == 3,
        "POST /cluster/batch",
        f"first assignment has {len(first.get('results', []))} results, expected 3",
    )
    _passed("POST /cluster/batch (50 logs) -> 50 assignments")


def step_stats(session: requests.Session) -> None:
    """``GET /stats`` -> ``total_processed >= 51`` with the expected keys."""
    resp = _get(session, "/stats")
    _require(resp.status_code == 200, "GET /stats", f"HTTP {resp.status_code}")
    body = resp.json()
    for key in ("throughput_per_sec", "total_clusters", "silhouette"):
        _require(key in body, "GET /stats", f"missing key {key!r} (got {sorted(body)})")
    total = int(body.get("total_processed", 0))
    _require(
        total >= 51,
        "GET /stats",
        f"total_processed is {total}, expected >= 51 (1 + 50 just processed)",
    )
    _passed(
        "GET /stats total_processed >= 51",
        f"total_processed={total}, throughput_per_sec={body.get('throughput_per_sec')}, "
        f"total_clusters={body.get('total_clusters')}",
    )


def step_clusters(session: requests.Session) -> None:
    """Cluster views: all-algos dict, per-algo list, drill-down dict, 404 on bogus."""
    # All three algorithms keyed.
    resp = _get(session, "/clusters")
    _require(resp.status_code == 200, "GET /clusters", f"HTTP {resp.status_code}")
    body = resp.json()
    _require(isinstance(body, dict), "GET /clusters", f"expected a dict, got {type(body).__name__}")
    missing = [a for a in ALGORITHMS if a not in body]
    _require(not missing, "GET /clusters", f"missing algorithm keys {missing}")

    # One algorithm's summaries -> a list.
    resp = _get(session, "/clusters/kmeans")
    _require(resp.status_code == 200, "GET /clusters/kmeans", f"HTTP {resp.status_code}")
    _require(
        isinstance(resp.json(), list),
        "GET /clusters/kmeans",
        f"expected a list, got {type(resp.json()).__name__}",
    )

    # Drill-down for one cluster -> a dict.
    resp = _get(session, "/clusters/kmeans/0")
    _require(resp.status_code == 200, "GET /clusters/kmeans/0", f"HTTP {resp.status_code}")
    _require(
        isinstance(resp.json(), dict),
        "GET /clusters/kmeans/0",
        f"expected a dict, got {type(resp.json()).__name__}",
    )

    # Unknown algorithm -> 404.
    resp = _get(session, "/clusters/bogus")
    _require(
        resp.status_code == 404,
        "GET /clusters/bogus",
        f"expected HTTP 404 for an unknown algorithm, got {resp.status_code}",
    )
    _passed("GET /clusters (all/one/drill-down) + 404 on unknown algorithm")


def step_patterns_anomalies_scatter_config(session: requests.Session) -> None:
    """``/patterns`` non-empty, ``/anomalies`` list, ``/scatter`` points, ``/config`` keys."""
    # Patterns: non-empty list.
    resp = _get(session, "/patterns")
    _require(resp.status_code == 200, "GET /patterns", f"HTTP {resp.status_code}")
    patterns = resp.json()
    _require(
        isinstance(patterns, list) and len(patterns) > 0,
        "GET /patterns",
        f"expected a non-empty list, got {type(patterns).__name__} of len "
        f"{len(patterns) if isinstance(patterns, list) else 'n/a'}",
    )

    # Anomalies: a list (may be empty depending on the data).
    resp = _get(session, "/anomalies", params={"limit": 10})
    _require(resp.status_code == 200, "GET /anomalies", f"HTTP {resp.status_code}")
    anomalies = resp.json()
    _require(
        isinstance(anomalies, list),
        "GET /anomalies?limit=10",
        f"expected a list, got {type(anomalies).__name__}",
    )

    # Scatter: a list of {x, y, cluster_id}.
    resp = _get(session, "/scatter/kmeans", params={"limit": 50})
    _require(resp.status_code == 200, "GET /scatter/kmeans", f"HTTP {resp.status_code}")
    points = resp.json()
    _require(
        isinstance(points, list) and len(points) > 0,
        "GET /scatter/kmeans?limit=50",
        f"expected a non-empty list of points, got {type(points).__name__} of len "
        f"{len(points) if isinstance(points, list) else 'n/a'}",
    )
    p0 = points[0]
    for key in ("x", "y", "cluster_id"):
        _require(
            key in p0,
            "GET /scatter/kmeans?limit=50",
            f"scatter point missing key {key!r}: {p0}",
        )

    # Config: carries the three algorithm sub-configs.
    resp = _get(session, "/config")
    _require(resp.status_code == 200, "GET /config", f"HTTP {resp.status_code}")
    cfg = resp.json()
    missing = [a for a in ALGORITHMS if a not in cfg]
    _require(not missing, "GET /config", f"missing algorithm config keys {missing}")
    _passed(
        "GET /patterns + /anomalies + /scatter + /config",
        f"{len(patterns)} patterns, {len(anomalies)} anomalies, {len(points)} scatter points",
    )


def step_ws_stream() -> None:
    """Connect ``WS /ws/stream``, receive a snapshot frame, assert type + stats dict."""
    url = _ws_url()
    try:
        ws = create_connection(url, timeout=WS_RECV_TIMEOUT_SEC)
    except Exception as exc:  # noqa: BLE001 - any connect failure fails the step
        raise StepError(f"WS connect to {url} failed: {exc!r}") from exc
    try:
        ws.settimeout(WS_RECV_TIMEOUT_SEC)
        raw = ws.recv()
    except Exception as exc:  # noqa: BLE001
        raise StepError(f"WS recv from {url} failed: {exc!r}") from exc
    finally:
        try:
            ws.close()
        except Exception:  # noqa: BLE001 - best-effort close
            pass

    snapshot = json.loads(raw)
    _require(
        snapshot.get("type") == "snapshot",
        "WS /ws/stream",
        f"frame type is {snapshot.get('type')!r}, expected 'snapshot'",
    )
    stats = snapshot.get("stats")
    _require(
        isinstance(stats, dict) and len(stats) > 0,
        "WS /ws/stream",
        f"frame 'stats' is not a non-empty dict: {stats!r}",
    )
    _passed(
        "WS /ws/stream snapshot frame",
        f"type=snapshot, stats.total_processed={stats.get('total_processed')}",
    )


# --- driver ------------------------------------------------------------------


def main() -> int:
    """Run the full E2E flow and return a process exit code (0 = all passed)."""
    print(f"[e2e] target app: {APP_URL}")
    print(f"[e2e] websocket : {_ws_url()}")
    print(f"[e2e] started   : {datetime.now().isoformat(timespec='seconds')}")
    session = requests.Session()

    try:
        step_wait_health(session)
        step_cluster_security(session)
        step_cluster_batch(session)
        step_stats(session)
        step_clusters(session)
        step_patterns_anomalies_scatter_config(session)
        step_ws_stream()
    except StepError as exc:
        print(f"\n[e2e] FAIL: {exc}")
        return 1
    except requests.RequestException as exc:
        print(f"\n[e2e] FAIL: HTTP error during E2E: {exc!r}")
        return 1

    print("\n========================================")
    print("[e2e] E2E PASS")
    print("  flow: health(ok) -> cluster(security, IP masked) -> cluster/batch(50) ->")
    print("        stats(>=51) -> clusters(all/one/drill-down/404) ->")
    print("        patterns/anomalies/scatter/config -> ws/stream(snapshot)")
    print("========================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
