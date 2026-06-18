#!/usr/bin/env python3
"""Black-box end-to-end verifier for the Adaptive Resource Allocation System.

Runs INSIDE Docker (the ``e2e`` compose service) against the live ``app`` service
over HTTP + WebSocket. It is a **pure external black box**: it never imports
``src.*`` and knows the system only through its documented contract:

    GET  /health                         -> {"status": "healthy", ...}
    GET  /api/status                     -> full orchestrator snapshot
    GET  /api/metrics                    -> {current_metrics, series, ...}
    POST /api/scaling {direction|target} -> scaling decision (reason == "manual")
    POST /api/load    {arrival_rate,...} -> {"status": "ramping", ...}
    SocketIO: emits status_update + metrics_update (both on connect)

The single most important check is :func:`check_load_drives_scaling` — the causal
chain. It injects demand far beyond capacity (60000 msgs/s against a ~800 msgs/s
base capacity ⇒ effective_utilization ≈ 7500%, an order of magnitude past the 75%
scale-up threshold) and then waits for the AUTOSCALER (not a manual action) to add
workers, proving monitor → forecast → decide → scale works end-to-end on real data.

Every check runs regardless of whether earlier checks failed; each prints
``PASS: <name> — <detail>`` or ``FAIL: <name> — <reason>`` and the process exits 1
if any check failed (so the Makefile ``e2e`` target propagates the failure).
"""

from __future__ import annotations

import os
import sys
import time
import uuid

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# The live app, by compose service name. Trailing slash stripped so we can build
# URLs with a leading "/" without producing a double slash.
APP_URL = os.environ.get("APP_URL", "http://app:8080").rstrip("/")

# A per-run nonce, printed in the header purely for traceability across logs. It is
# deliberately NOT used in any assertion — every check is deterministic.
NONCE = uuid.uuid4().hex[:12]

# Short HTTP timeout for individual requests; the slow waits are handled by polling.
HTTP_TIMEOUT = 5

# The reactive/predictive reasons that prove the AUTOSCALER acted (as opposed to a
# manual operator scale, whose reason is "manual"). Matches src/scaler.py.
AUTOSCALE_REASONS = {"reactive_util", "predictive", "reactive_cpu", "reactive_mem"}

# Documented top-level keys of each payload (subset we assert on).
STATUS_KEYS = (
    "current_metrics",
    "forecast",
    "workers",
    "last_decision",
    "scaling_history",
    "anomaly",
    "cost",
)
METRICS_KEYS = ("current_metrics", "series")


class Fail(Exception):
    """Raised by a check to signal a clean, reportable failure (not a crash)."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_json(path: str, timeout: int = HTTP_TIMEOUT) -> tuple[int, dict]:
    """GET ``{APP_URL}{path}`` and return ``(status_code, json_or_{})``.

    Any transport error or non-JSON body is surfaced as a :class:`Fail` so the
    calling check reports it cleanly rather than raising an opaque exception.
    """
    url = f"{APP_URL}{path}"
    try:
        r = requests.get(url, timeout=timeout)
    except requests.RequestException as exc:
        raise Fail(f"GET {path} errored: {exc}") from exc
    try:
        body = r.json()
    except ValueError as exc:
        raise Fail(f"GET {path} returned non-JSON (status={r.status_code})") from exc
    if not isinstance(body, dict):
        raise Fail(f"GET {path} JSON was {type(body).__name__}, expected object")
    return r.status_code, body


def _post_json(path: str, payload: dict, timeout: int = HTTP_TIMEOUT) -> tuple[int, dict]:
    """POST ``payload`` as JSON to ``{APP_URL}{path}``; return ``(status, json_or_{})``."""
    url = f"{APP_URL}{path}"
    try:
        r = requests.post(url, json=payload, timeout=timeout)
    except requests.RequestException as exc:
        raise Fail(f"POST {path} errored: {exc}") from exc
    try:
        body = r.json()
    except ValueError as exc:
        raise Fail(f"POST {path} returned non-JSON (status={r.status_code})") from exc
    if not isinstance(body, dict):
        raise Fail(f"POST {path} JSON was {type(body).__name__}, expected object")
    return r.status_code, body


def _workers_current(status: dict) -> int:
    """Extract ``workers.current`` from a /api/status snapshot defensively."""
    workers = status.get("workers") or {}
    try:
        return int(workers.get("current"))
    except (TypeError, ValueError) as exc:
        raise Fail(f"status.workers.current missing/invalid: {workers!r}") from exc


def _wait_healthy(timeout: int = 40) -> dict:
    """Poll ``GET /health`` up to ~``timeout``×1s until ``status == "healthy"``.

    Returns the health body on success; raises :class:`Fail` if the app never
    reports healthy within the window. (Compose already gates the e2e service on
    ``service_healthy``, but we re-check so a direct/local run is robust too.)
    """
    deadline = time.time() + timeout
    last_err = "no response"
    while time.time() < deadline:
        try:
            r = requests.get(f"{APP_URL}/health", timeout=HTTP_TIMEOUT)
            if r.status_code == 200 and r.json().get("status") == "healthy":
                return r.json()
            last_err = f"status={r.status_code} body={r.text[:120]!r}"
        except requests.RequestException as exc:
            last_err = str(exc)
        except ValueError as exc:  # non-JSON health body
            last_err = f"non-JSON health body: {exc}"
        time.sleep(1)
    raise Fail(f"app never became healthy within {timeout}s (last: {last_err})")


# ---------------------------------------------------------------------------
# Checks — each returns a detail string on success or raises Fail
# ---------------------------------------------------------------------------

def check_health() -> str:
    """1) /health eventually reports ``status == "healthy"``."""
    body = _wait_healthy(timeout=40)
    return f"service={body.get('service')} status={body.get('status')}"


def check_endpoints_valid() -> str:
    """2) /api/status and /api/metrics return 200 with the documented top-level keys."""
    s_code, status = _get_json("/api/status")
    if s_code != 200:
        raise Fail(f"/api/status returned {s_code}")
    missing_status = [k for k in STATUS_KEYS if k not in status]
    if missing_status:
        raise Fail(f"/api/status missing keys: {missing_status}")

    m_code, metrics = _get_json("/api/metrics")
    if m_code != 200:
        raise Fail(f"/api/metrics returned {m_code}")
    missing_metrics = [k for k in METRICS_KEYS if k not in metrics]
    if missing_metrics:
        raise Fail(f"/api/metrics missing keys: {missing_metrics}")
    if not isinstance(metrics.get("series"), dict):
        raise Fail("/api/metrics 'series' is not an object")

    return (
        f"/api/status has {len(STATUS_KEYS)} documented keys; "
        f"/api/metrics has current_metrics+series"
    )


def check_manual_scale() -> str:
    """3) A manual scale-up adds a worker and is reflected in /api/status.

    Reads the current worker count, POSTs ``{"direction": "up"}``, asserts the
    decision is a manual scale that raises the target, then confirms the live
    snapshot's ``workers.current`` increased. (Baseline equilibrium sits at a
    handful of workers, far below ``max_workers``, so ``+1`` always strictly
    increases — this is not racing the ceiling.)
    """
    _, status_before = _get_json("/api/status")
    before = _workers_current(status_before)

    code, decision = _post_json("/api/scaling", {"direction": "up"})
    if code != 200:
        raise Fail(f"/api/scaling returned {code}: {decision}")
    if decision.get("reason") != "manual":
        raise Fail(f"expected reason='manual', got {decision.get('reason')!r}")

    to_workers = int(decision.get("to_workers", -1))
    from_workers = int(decision.get("from_workers", -1))
    if to_workers <= from_workers:
        raise Fail(
            f"manual up did not raise target: from={from_workers} to={to_workers}"
        )

    # Confirm the move is observable in the live snapshot.
    _, status_after = _get_json("/api/status")
    after = _workers_current(status_after)
    if after <= before:
        raise Fail(f"workers.current did not increase: before={before} after={after}")

    return f"manual scale-up workers {before}→{after} (decision {from_workers}→{to_workers})"


def check_websocket_status_update() -> str:
    """4) A SocketIO client receives a status_update OR metrics_update on connect.

    The server's connect handler emits both events to the new socket immediately,
    so the very first ``receive`` should yield one of them with a dict payload. Any
    WebSocket hiccup is converted into a clean :class:`Fail` rather than a crash.
    """
    try:
        import socketio  # python-socketio, transport via websocket-client
    except ImportError as exc:  # pragma: no cover - dependency is pinned
        raise Fail(f"python-socketio not importable: {exc}") from exc

    client = socketio.SimpleClient()
    try:
        client.connect(APP_URL, wait_timeout=10)
        event = client.receive(timeout=10)
    except Exception as exc:  # noqa: BLE001 - any WS error is a clean check failure
        raise Fail(f"WebSocket connect/receive failed: {exc}") from exc
    finally:
        try:
            client.disconnect()
        except Exception:  # noqa: BLE001 - disconnect best-effort
            pass

    if not event or not isinstance(event, (list, tuple)):
        raise Fail(f"unexpected receive() shape: {event!r}")
    name = event[0]
    payload = event[1] if len(event) > 1 else None
    if name not in ("status_update", "metrics_update"):
        raise Fail(f"unexpected first event '{name}' (want status/metrics_update)")
    if not isinstance(payload, dict):
        raise Fail(f"event '{name}' payload was {type(payload).__name__}, expected object")

    return f"received '{name}' with dict payload ({len(payload)} keys)"


def check_load_drives_scaling() -> str:
    """5) THE CAUSAL CHAIN — injected load makes the AUTOSCALER add workers.

    1. Record ``workers_before`` from /api/status.
    2. POST a load ramp of 60000 msgs/s over 3s. Against the ~800 msgs/s base
       capacity that drives effective_utilization to ~7500% — an order of
       magnitude past the 75% scale-up threshold — so a reactive scale-up is
       deterministic, not probabilistic.
    3. Poll /api/status every 2s for up to ~60s until BOTH hold:
         * ``workers.current > workers_before`` (the pool actually grew), AND
         * a scale-up decision with an AUTOSCALER reason
           (reactive_util / predictive / reactive_cpu / reactive_mem) appears in
           ``last_decision`` or anywhere in ``scaling_history`` — i.e. the engine
           reacted on its own, not via a manual action.

    Raises :class:`Fail` if 60s elapse without the autoscaler reacting.
    """
    _, status_before = _get_json("/api/status")
    workers_before = _workers_current(status_before)

    code, body = _post_json(
        "/api/load", {"arrival_rate": 60000, "ramp_seconds": 3}
    )
    if code != 200:
        raise Fail(f"/api/load returned {code}: {body}")
    if body.get("status") != "ramping":
        raise Fail(f"/api/load did not confirm ramp: {body}")

    deadline = time.time() + 60
    started = time.time()
    last_seen = (workers_before, None)
    while time.time() < deadline:
        time.sleep(2)
        try:
            _, status = _get_json("/api/status")
        except Fail:
            # A transient read error mid-run must not abort the whole chain; retry.
            continue

        workers_now = _workers_current(status)
        reason = _autoscale_reason(status)
        last_seen = (workers_now, reason)

        if workers_now > workers_before and reason is not None:
            elapsed = round(time.time() - started, 1)
            return (
                f"workers {workers_before}→{workers_now} via reason={reason} "
                f"in {elapsed}s"
            )

    workers_now, reason = last_seen
    raise Fail(
        "autoscaler did not react within 60s "
        f"(workers {workers_before}→{workers_now}, last autoscale reason={reason})"
    )


def _autoscale_reason(status: dict) -> str | None:
    """Return an AUTOSCALER scale-up reason from a snapshot, or ``None``.

    Inspects ``last_decision`` and every entry of ``scaling_history`` for a
    ``scale_up`` action whose ``reason`` is one of :data:`AUTOSCALE_REASONS`
    (deliberately excluding ``"manual"`` so an operator action can never satisfy
    the causal-chain check).
    """
    def _is_autoscale_up(decision: object) -> bool:
        if not isinstance(decision, dict):
            return False
        return (
            decision.get("action") == "scale_up"
            and decision.get("reason") in AUTOSCALE_REASONS
        )

    last = status.get("last_decision")
    if _is_autoscale_up(last):
        return last["reason"]

    history = status.get("scaling_history") or []
    if isinstance(history, list):
        # Newest last — scan from the end for the most recent autoscale scale-up.
        for decision in reversed(history):
            if _is_autoscale_up(decision):
                return decision["reason"]
    return None


def check_effective_utilization_rose() -> str:
    """6) (optional) After the load injection, effective_utilization is elevated.

    Polls /api/metrics a few times and asserts ``current_metrics.effective_utilization``
    exceeds the 75% scale-up threshold at some sample — corroborating that the load
    ramp from check 5 genuinely raised demand-over-capacity (not just the worker count).
    """
    threshold = 75.0
    best = 0.0
    for _ in range(8):
        try:
            _, metrics = _get_json("/api/metrics")
        except Fail:
            time.sleep(1)
            continue
        cm = metrics.get("current_metrics") or {}
        try:
            util = float(cm.get("effective_utilization", 0.0) or 0.0)
        except (TypeError, ValueError):
            util = 0.0
        best = max(best, util)
        if util > threshold:
            return f"effective_utilization peaked at {util:.0f}% (> {threshold:.0f}%)"
        time.sleep(1)
    raise Fail(
        f"effective_utilization stayed <= {threshold:.0f}% (peak observed {best:.0f}%)"
    )


# Ordered like the sibling project's CHECKS tuple: (name, callable).
CHECKS = (
    ("health", check_health),
    ("endpoints_valid", check_endpoints_valid),
    ("manual_scale", check_manual_scale),
    ("websocket_status_update", check_websocket_status_update),
    ("load_drives_scaling", check_load_drives_scaling),
    ("effective_utilization_rose", check_effective_utilization_rose),
)


def main() -> int:
    """Run every check, print PASS/FAIL per check, return 0 if all pass else 1."""
    print("=" * 70)
    print("Adaptive Resource Allocation — E2E verification (black box)")
    print(f"  APP_URL : {APP_URL}")
    print(f"  run id  : {NONCE}")
    print("=" * 70)

    failures = 0
    for name, fn in CHECKS:
        try:
            detail = fn()
            print(f"PASS: {name} — {detail}")
        except Fail as exc:
            failures += 1
            print(f"FAIL: {name} — {exc}")
        except Exception as exc:  # noqa: BLE001 - unexpected error is still a failure
            failures += 1
            print(f"FAIL: {name} — unexpected error: {exc}")

    total = len(CHECKS)
    passed = total - failures
    print("=" * 70)
    print(f"E2E: {passed}/{total} checks passed")
    print("=" * 70)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
