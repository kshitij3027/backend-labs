"""End-to-end verification script for the Distributed User Sessionization Engine."""
from __future__ import annotations

import os
import sys
import time

import httpx
import redis


def check_health(app_url: str) -> None:
    """Poll the /health endpoint until it responds successfully."""
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
                return
            else:
                print(f"  attempt {attempt}: HTTP {resp.status_code}")
        except (httpx.RequestError, httpx.TimeoutException) as exc:
            print(f"  attempt {attempt}: {exc}")

        time.sleep(sleep_seconds)

    print("E2E FAILED: health check never succeeded")
    sys.exit(1)


def check_redis(redis_url: str) -> None:
    """Connect to Redis and verify connectivity with PING."""
    print(f"E2E: checking Redis connectivity ({redis_url}) ...")
    try:
        r = redis.from_url(redis_url, decode_responses=True)
        result = r.ping()
        assert result is True, f"Redis PING returned {result}"
        print("Redis connectivity: OK")
        r.close()
    except Exception as exc:
        print(f"E2E FAILED: Redis connectivity check failed: {exc}")
        sys.exit(1)


def main() -> None:
    app_url = os.environ.get("APP_URL", "http://engine:8000")
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")

    check_health(app_url)
    check_redis(redis_url)

    print("E2E: all checks passed")


if __name__ == "__main__":
    main()
