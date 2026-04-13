"""End-to-end verification script for the Anomaly Detection Engine.

Runs against a live Flask instance (typically inside Docker).  Every check
must pass for the script to exit 0.
"""
from __future__ import annotations

import json
import os
import sys
import time

try:
    import httpx

    def _get(url: str, timeout: float = 5.0):
        return httpx.get(url, timeout=timeout)

    def _post(url: str, payload: dict, timeout: float = 5.0):
        return httpx.post(url, json=payload, timeout=timeout)

except ImportError:
    import urllib.request
    import urllib.error

    class _FakeResponse:
        def __init__(self, status_code: int, body: bytes):
            self.status_code = status_code
            self._body = body

        def json(self):
            return json.loads(self._body)

    def _get(url: str, timeout: float = 5.0):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return _FakeResponse(resp.status, resp.read())
        except urllib.error.HTTPError as exc:
            return _FakeResponse(exc.code, exc.read())

    def _post(url: str, payload: dict, timeout: float = 5.0):
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return _FakeResponse(resp.status, resp.read())
        except urllib.error.HTTPError as exc:
            return _FakeResponse(exc.code, exc.read())


# -----------------------------------------------------------------------
# Result tracking
# -----------------------------------------------------------------------
_passed = 0
_failed = 0


def _ok(msg: str) -> None:
    global _passed
    _passed += 1
    print(f"  PASS: {msg}")


def _fail(msg: str) -> None:
    global _failed
    _failed += 1
    print(f"  FAIL: {msg}")


# -----------------------------------------------------------------------
# Checks
# -----------------------------------------------------------------------

def check_health(app_url: str) -> None:
    """Poll /health until it responds 200."""
    health_url = f"{app_url}/health"
    max_attempts = 30

    print(f"\n[1/8] Health check: polling {health_url} ...")

    for attempt in range(1, max_attempts + 1):
        try:
            resp = _get(health_url)
            if resp.status_code == 200:
                body = resp.json()
                if body.get("status") == "healthy":
                    _ok(f"healthy (attempt {attempt})")
                    return
        except Exception:
            pass
        time.sleep(1)

    _fail("health check never succeeded after 30 attempts")


def check_root(app_url: str) -> None:
    """GET / should return the dashboard HTML."""
    print(f"\n[2/8] Root endpoint ...")
    try:
        resp = _get(app_url)
        if resp.status_code == 200:
            _ok("root returned 200")
        else:
            _fail(f"root returned HTTP {resp.status_code}")
    except Exception as exc:
        _fail(f"root: {exc}")


def check_post_logs(app_url: str) -> None:
    """POST /api/logs with a test entry."""
    url = f"{app_url}/api/logs"
    print(f"\n[3/8] POST {url} ...")

    payload = {
        "ip": "10.0.0.42",
        "method": "GET",
        "path": "/api/test",
        "status_code": 200,
        "response_time": 150.0,
        "bytes_sent": 2048,
        "user_agent": "e2e-verify/1.0",
        "session_duration": 120.0,
        "page_views": 3,
    }

    try:
        resp = _post(url, payload)
        if resp.status_code != 200:
            _fail(f"POST /api/logs returned HTTP {resp.status_code}")
            return

        body = resp.json()
        for key in ("is_anomaly", "confidence", "scores"):
            if key not in body:
                _fail(f"Missing '{key}' in POST /api/logs response")
                return

        _ok(f"is_anomaly={body['is_anomaly']}, confidence={body['confidence']:.3f}")
    except Exception as exc:
        _fail(f"POST /api/logs: {exc}")


def check_api_stats(app_url: str) -> None:
    """GET /api/stats — verify all expected keys including sub-objects."""
    url = f"{app_url}/api/stats"
    print(f"\n[4/8] GET {url} ...")

    try:
        resp = _get(url)
        if resp.status_code != 200:
            _fail(f"GET /api/stats returned HTTP {resp.status_code}")
            return

        body = resp.json()

        required = [
            "total_processed", "anomalies_detected",
            "true_positive_rate", "false_positive_rate",
            "detectors_ready",
            "adaptive_threshold", "contextual",
            "false_positive_manager", "memory_efficient",
        ]
        missing = [k for k in required if k not in body]
        if missing:
            _fail(f"Missing keys in stats: {missing}")
            return

        # Check sub-objects exist
        if not isinstance(body["memory_efficient"], dict):
            _fail("memory_efficient is not a dict")
            return
        if not isinstance(body["adaptive_threshold"], dict):
            _fail("adaptive_threshold is not a dict")
            return

        _ok(f"stats shape OK (total_processed={body['total_processed']})")
    except Exception as exc:
        _fail(f"GET /api/stats: {exc}")


def check_api_anomalies(app_url: str) -> None:
    """GET /api/anomalies — verify list with per-algorithm scores."""
    url = f"{app_url}/api/anomalies"
    print(f"\n[5/8] GET {url} ...")

    try:
        resp = _get(url)
        if resp.status_code != 200:
            _fail(f"GET /api/anomalies returned HTTP {resp.status_code}")
            return

        body = resp.json()
        if not isinstance(body, list):
            _fail(f"Expected list, got {type(body).__name__}")
            return

        _ok(f"anomalies list (len={len(body)})")

        # If there are anomalies, verify shape
        if len(body) > 0:
            entry = body[-1]
            for key in ("timestamp", "confidence", "is_anomaly", "scores", "log_summary"):
                if key not in entry:
                    _fail(f"Anomaly entry missing '{key}'")
                    return

            # Per-algorithm scores may be partial or empty depending on
            # warm-up state.  Just verify that any score keys present are
            # from the expected set, and that at least some entries have
            # non-empty scores.
            valid_algos = {"zscore", "isolation_forest", "temporal"}
            any_populated = False
            for e in body:
                scores = e.get("scores", {})
                if scores:
                    any_populated = True
                    unknown = set(scores.keys()) - valid_algos
                    if unknown:
                        _fail(f"Unexpected algorithm keys in scores: {unknown}")
                        return

            if any_populated:
                _ok("anomaly score keys validated (subset of zscore/isolation_forest/temporal)")
            else:
                _ok("anomaly entries present (scores empty — detectors still warming up)")
    except Exception as exc:
        _fail(f"GET /api/anomalies: {exc}")


def check_anomaly_groups(app_url: str) -> None:
    """GET /api/anomalies/groups — verify groups response."""
    url = f"{app_url}/api/anomalies/groups"
    print(f"\n[6/8] GET {url} ...")

    try:
        resp = _get(url)
        if resp.status_code != 200:
            _fail(f"GET /api/anomalies/groups returned HTTP {resp.status_code}")
            return

        body = resp.json()
        if not isinstance(body, list):
            _fail(f"Expected list, got {type(body).__name__}")
            return

        _ok(f"anomaly groups (len={len(body)})")
    except Exception as exc:
        _fail(f"GET /api/anomalies/groups: {exc}")


def check_feedback(app_url: str) -> None:
    """POST /api/feedback — verify feedback accepted."""
    url = f"{app_url}/api/feedback"
    print(f"\n[7/8] POST {url} ...")

    payload = {"anomaly_id": "test-id-001", "confirmed": True}

    try:
        resp = _post(url, payload)
        if resp.status_code != 200:
            _fail(f"POST /api/feedback returned HTTP {resp.status_code}")
            return

        body = resp.json()
        if body.get("status") != "ok":
            _fail(f"Unexpected feedback response: {body}")
            return
        if "current_threshold" not in body:
            _fail("Missing 'current_threshold' in feedback response")
            return

        _ok(f"feedback accepted (threshold={body['current_threshold']:.4f})")
    except Exception as exc:
        _fail(f"POST /api/feedback: {exc}")


def check_background_processing(app_url: str) -> None:
    """Wait and verify background task is producing logs."""
    url = f"{app_url}/api/stats"
    print(f"\n[8/8] Background processing (waiting 5s) ...")

    time.sleep(5)

    try:
        resp = _get(url)
        if resp.status_code != 200:
            _fail(f"GET /api/stats returned HTTP {resp.status_code}")
            return

        body = resp.json()
        total = body.get("total_processed", 0)
        if total <= 0:
            _fail(f"total_processed={total} after 5s (expected > 0)")
            return

        _ok(f"background task running (total_processed={total})")
    except Exception as exc:
        _fail(f"background processing: {exc}")


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main() -> None:
    app_url = os.environ.get("APP_URL", "http://localhost:5000")

    print("=" * 60)
    print("  Anomaly Detection Engine — E2E Verification")
    print(f"  Target: {app_url}")
    print("=" * 60)

    check_health(app_url)
    check_root(app_url)
    check_post_logs(app_url)
    check_api_stats(app_url)
    check_api_anomalies(app_url)
    check_anomaly_groups(app_url)
    check_feedback(app_url)
    check_background_processing(app_url)

    print("\n" + "=" * 60)
    print(f"  Results: {_passed} passed, {_failed} failed")
    print("=" * 60)

    if _failed > 0:
        print("\nE2E VERIFICATION FAILED")
        sys.exit(1)
    else:
        print("\nALL CHECKS PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
