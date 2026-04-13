"""End-to-end verification script for the Anomaly Detection Engine."""
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
# Checks
# -----------------------------------------------------------------------

def check_health(app_url: str) -> None:
    """Poll the /health endpoint until it responds successfully."""
    health_url = f"{app_url}/health"
    max_attempts = 30
    sleep_seconds = 1

    print(f"E2E: polling {health_url} (max {max_attempts} attempts) ...")

    for attempt in range(1, max_attempts + 1):
        try:
            resp = _get(health_url)
            if resp.status_code == 200:
                body = resp.json()
                assert body.get("status") == "healthy", f"Unexpected body: {body}"
                print(f"  PASSED (attempt {attempt}): {body}")
                return
            else:
                print(f"  attempt {attempt}: HTTP {resp.status_code}")
        except Exception as exc:
            print(f"  attempt {attempt}: {exc}")

        time.sleep(sleep_seconds)

    print("E2E FAILED: health check never succeeded")
    sys.exit(1)


def check_root(app_url: str) -> None:
    """Verify the root endpoint returns the dashboard page."""
    print(f"E2E: GET {app_url}/ ...")
    try:
        resp = _get(app_url)
        assert resp.status_code == 200, f"Root returned HTTP {resp.status_code}"
        print("  PASSED: Root endpoint returned 200")
    except Exception as exc:
        print(f"E2E FAILED: root check failed: {exc}")
        sys.exit(1)


def check_post_logs(app_url: str) -> None:
    """POST a test log entry to /api/logs and verify the response."""
    url = f"{app_url}/api/logs"
    print(f"E2E: POST {url} ...")

    payload = {
        "ip": "10.0.0.42",
        "method": "GET",
        "path": "/api/test",
        "status_code": 200,
        "response_time": 150.0,
        "bytes_sent": 2048,
        "user_agent": "e2e-test/1.0",
        "session_duration": 120.0,
        "page_views": 3,
    }

    try:
        resp = _post(url, payload)
        assert resp.status_code == 200, f"POST /api/logs returned HTTP {resp.status_code}"
        body = resp.json()
        assert "is_anomaly" in body, f"Missing 'is_anomaly' in response: {body}"
        assert "confidence" in body, f"Missing 'confidence' in response: {body}"
        assert "scores" in body, f"Missing 'scores' in response: {body}"
        print(f"  PASSED: POST /api/logs -> is_anomaly={body['is_anomaly']}, confidence={body['confidence']:.3f}")
    except Exception as exc:
        print(f"E2E FAILED: POST /api/logs: {exc}")
        sys.exit(1)


def check_api_stats(app_url: str) -> None:
    """GET /api/stats and verify the JSON shape."""
    url = f"{app_url}/api/stats"
    print(f"E2E: GET {url} ...")

    try:
        resp = _get(url)
        assert resp.status_code == 200, f"GET /api/stats returned HTTP {resp.status_code}"
        body = resp.json()

        required_keys = [
            "total_processed",
            "anomalies_detected",
            "true_positive_rate",
            "false_positive_rate",
            "detectors_ready",
        ]
        for key in required_keys:
            assert key in body, f"Missing key '{key}' in stats: {body}"

        print(f"  PASSED: stats shape OK (total_processed={body['total_processed']})")
    except Exception as exc:
        print(f"E2E FAILED: GET /api/stats: {exc}")
        sys.exit(1)


def check_api_anomalies(app_url: str) -> None:
    """GET /api/anomalies and verify the response is a JSON list."""
    url = f"{app_url}/api/anomalies"
    print(f"E2E: GET {url} ...")

    try:
        resp = _get(url)
        assert resp.status_code == 200, f"GET /api/anomalies returned HTTP {resp.status_code}"
        body = resp.json()
        assert isinstance(body, list), f"Expected list, got {type(body).__name__}"
        print(f"  PASSED: anomalies endpoint returned list (len={len(body)})")
    except Exception as exc:
        print(f"E2E FAILED: GET /api/anomalies: {exc}")
        sys.exit(1)


def check_background_processing(app_url: str) -> None:
    """Wait a few seconds and verify that the background task is processing logs."""
    url = f"{app_url}/api/stats"
    print("E2E: checking background processing (waiting 5s) ...")

    time.sleep(5)

    try:
        resp = _get(url)
        assert resp.status_code == 200
        body = resp.json()
        total = body.get("total_processed", 0)
        assert total > 0, f"Expected total_processed > 0 after 5s, got {total}"
        print(f"  PASSED: background task running (total_processed={total})")
    except Exception as exc:
        print(f"E2E FAILED: background processing check: {exc}")
        sys.exit(1)


def check_socketio(app_url: str) -> None:
    """Optionally test SocketIO connectivity (non-fatal if library missing)."""
    try:
        import socketio as sio_client
    except ImportError:
        print("E2E: SKIPPED SocketIO check (python-socketio not installed)")
        return

    print("E2E: testing SocketIO connection ...")
    client = sio_client.SimpleClient()

    try:
        client.connect(app_url, transports=["websocket"])
        event = client.receive(timeout=5)
        assert event is not None, "No event received within 5s"
        print(f"  PASSED: received SocketIO event: {event[0]}")
    except Exception as exc:
        print(f"  WARNING: SocketIO test failed (non-fatal): {exc}")
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main() -> None:
    app_url = os.environ.get("APP_URL", "http://localhost:5000")

    check_health(app_url)
    check_root(app_url)
    check_post_logs(app_url)
    check_api_stats(app_url)
    check_api_anomalies(app_url)
    check_background_processing(app_url)
    check_socketio(app_url)

    print("\n" + "=" * 40)
    print("E2E: ALL CHECKS PASSED")
    print("=" * 40)


if __name__ == "__main__":
    main()
