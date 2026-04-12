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


def check_api_endpoints(app_url: str) -> None:
    """Verify the API endpoints work end-to-end."""

    # --- Single event ingestion ---
    print("E2E: POST /api/events (single event) ...")
    single_event = {
        "user_id": "e2e_user_1",
        "event_type": "page_view",
        "page_url": "/home",
        "device_type": "desktop",
    }
    resp = httpx.post(f"{app_url}/api/events", json=single_event, timeout=10.0)
    assert resp.status_code == 200, f"Single event POST failed: {resp.status_code}"
    body = resp.json()
    assert body.get("success") is True, f"Expected success=True, got {body}"
    assert "session_id" in body, f"Missing session_id in response: {body}"
    print(f"  Single event: OK (session_id={body['session_id']})")

    # --- Batch event ingestion ---
    print("E2E: POST /api/events (batch of 3) ...")
    batch_events = [
        {"user_id": "e2e_user_2", "event_type": "page_view", "page_url": "/a"},
        {"user_id": "e2e_user_3", "event_type": "click", "page_url": "/b"},
        {"user_id": "e2e_user_4", "event_type": "search", "page_url": "/c"},
    ]
    resp = httpx.post(f"{app_url}/api/events", json=batch_events, timeout=10.0)
    assert resp.status_code == 200, f"Batch event POST failed: {resp.status_code}"
    body = resp.json()
    assert body.get("success") is True, f"Expected success=True, got {body}"
    assert body.get("processed") == 3, f"Expected processed=3, got {body}"
    print("  Batch events: OK (processed=3)")

    # --- Sessions query ---
    print("E2E: GET /api/sessions/e2e_user_1 ...")
    resp = httpx.get(f"{app_url}/api/sessions/e2e_user_1", timeout=10.0)
    assert resp.status_code == 200, f"Sessions GET failed: {resp.status_code}"
    sessions = resp.json()
    assert isinstance(sessions, list), f"Expected list, got {type(sessions)}"
    assert len(sessions) > 0, "Expected non-empty session list for e2e_user_1"
    print(f"  Sessions for e2e_user_1: OK ({len(sessions)} session(s))")

    # --- Analytics ---
    print("E2E: GET /api/analytics ...")
    resp = httpx.get(f"{app_url}/api/analytics", timeout=10.0)
    assert resp.status_code == 200, f"Analytics GET failed: {resp.status_code}"
    analytics = resp.json()
    assert analytics.get("active_sessions", 0) > 0, f"Expected active_sessions > 0: {analytics}"
    assert analytics.get("total_events", 0) > 0, f"Expected total_events > 0: {analytics}"
    print(f"  Analytics: OK (active={analytics['active_sessions']}, events={analytics['total_events']})")

    print("API endpoints: OK")


def main() -> None:
    app_url = os.environ.get("APP_URL", "http://engine:8000")
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")

    check_health(app_url)
    check_redis(redis_url)
    check_api_endpoints(app_url)

    print("E2E: all checks passed")


if __name__ == "__main__":
    main()
