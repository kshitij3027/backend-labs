"""End-to-end verification script for the Anomaly Detection Engine."""
from __future__ import annotations

import os
import sys
import time

try:
    import httpx

    def _get(url: str, timeout: float = 5.0):
        return httpx.get(url, timeout=timeout)

except ImportError:
    import json
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
                print(f"E2E PASSED (attempt {attempt}): {body}")
                return
            else:
                print(f"  attempt {attempt}: HTTP {resp.status_code}")
        except Exception as exc:
            print(f"  attempt {attempt}: {exc}")

        time.sleep(sleep_seconds)

    print("E2E FAILED: health check never succeeded")
    sys.exit(1)


def check_root(app_url: str) -> None:
    """Verify the root endpoint returns the placeholder page."""
    print(f"E2E: GET {app_url}/ ...")
    try:
        resp = _get(app_url)
        assert resp.status_code == 200, f"Root returned HTTP {resp.status_code}"
        print("  Root endpoint: OK")
    except Exception as exc:
        print(f"E2E FAILED: root check failed: {exc}")
        sys.exit(1)


def main() -> None:
    app_url = os.environ.get("APP_URL", "http://localhost:5000")

    check_health(app_url)
    check_root(app_url)

    print("\nE2E: all checks passed")


if __name__ == "__main__":
    main()
