"""End-to-end verification script for the Distributed User Sessionization Engine."""
from __future__ import annotations

import os
import sys
import time

import httpx


def main() -> None:
    app_url = os.environ.get("APP_URL", "http://engine:8000")
    health_url = f"{app_url}/health"
    max_attempts = 30
    sleep_seconds = 1

    print(f"E2E: polling {health_url} (max {max_attempts} attempts) ...")

    for attempt in range(1, max_attempts + 1):
        try:
            resp = httpx.get(health_url, timeout=5.0)
            if resp.status_code == 200:
                body = resp.json()
                assert body.get("status") == "healthy", f"Unexpected body: {body}"
                print(f"E2E PASSED (attempt {attempt}): {body}")
                sys.exit(0)
            else:
                print(f"  attempt {attempt}: HTTP {resp.status_code}")
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            print(f"  attempt {attempt}: {exc}")

        time.sleep(sleep_seconds)

    print("E2E FAILED: health check never succeeded")
    sys.exit(1)


if __name__ == "__main__":
    main()
