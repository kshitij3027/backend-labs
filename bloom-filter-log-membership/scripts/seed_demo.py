"""Seed a running membership API with predictable demo data.

Meant to run INSIDE the compose network (the app service is reached by its
service name), e.g. via the test-runner image:

    docker compose run --rm --no-deps test python scripts/seed_demo.py

Environment:
    APP_URL     base URL of the API service        (default http://app:8001)
    SEED_COUNT  count passed to /demo/populate     (default 5000)

What it does, in order:

1. waits for ``GET /health`` to answer healthy (~30s retry budget — covers
   a freshly started container still loading snapshots),
2. ``POST /demo/populate?count=$SEED_COUNT`` — bulk random-nonce demo keys
   round-robined across the three log-type filters,
3. adds a handful of WELL-KNOWN named keys via ``/logs/add`` so dashboard /
   UI testing has predictable names that are guaranteed to answer
   ``probably_exists`` (the bulk populate keys are nonce-randomized and
   unguessable on purpose),
4. prints the resulting ``/stats`` totals as a seeding receipt.

Exit code 0 on success, 1 on ANY failure — usable as a gating step in E2E
flows. Output is flushed per line so container logs stream live.
"""
from __future__ import annotations

import os
import sys
import time

import httpx

#: (log_type, log_key) pairs every seeded environment can rely on. The
#: dashboard / Chrome UI test queries these exact names and expects a
#: "probably_exists" answer; keep them stable.
WELL_KNOWN_KEYS: tuple[tuple[str, str], ...] = (
    ("error_logs", "demo-error-disk-full"),
    ("error_logs", "demo-error-oom-killer"),
    ("access_logs", "demo-access-GET-/index"),
    ("access_logs", "demo-access-POST-/api/login"),
    ("security_logs", "demo-security-failed-login"),
    ("security_logs", "demo-security-port-scan"),
)

#: How long to keep retrying /health before giving up.
HEALTH_BUDGET_SECONDS = 30.0

#: Pause between /health attempts.
HEALTH_RETRY_DELAY_SECONDS = 1.0

#: Per-request timeout. Generous: a SEED_COUNT at the 1M API cap keeps the
#: populate handler busy for several seconds inside the container.
REQUEST_TIMEOUT_SECONDS = 60.0


def _say(message: str) -> None:
    """Print one progress line, flushed so `docker logs` streams it live."""
    print(f"seed_demo: {message}", flush=True)


def wait_for_health(client: httpx.Client, app_url: str) -> None:
    """Poll ``GET /health`` until healthy or the retry budget is exhausted."""
    deadline = time.monotonic() + HEALTH_BUDGET_SECONDS
    last_error = "no attempt made"
    while time.monotonic() < deadline:
        try:
            response = client.get(f"{app_url}/health")
            if (
                response.status_code == 200
                and response.json().get("status") == "healthy"
            ):
                return
            last_error = f"HTTP {response.status_code}: {response.text[:200]}"
        except httpx.HTTPError as exc:
            last_error = repr(exc)
        time.sleep(HEALTH_RETRY_DELAY_SECONDS)
    raise RuntimeError(
        f"{app_url}/health never became healthy within "
        f"{HEALTH_BUDGET_SECONDS:.0f}s (last error: {last_error})"
    )


def seed(client: httpx.Client, app_url: str, seed_count: int) -> None:
    """Run the populate + well-known-keys + stats-receipt sequence."""
    response = client.post(
        f"{app_url}/demo/populate", params={"count": seed_count}
    )
    response.raise_for_status()
    body = response.json()
    if body.get("status") != "completed" or body.get("records_added") != seed_count:
        raise RuntimeError(f"unexpected populate response: {body}")
    _say(f"populated {body['records_added']} bulk demo records")

    for log_type, log_key in WELL_KNOWN_KEYS:
        response = client.post(
            f"{app_url}/logs/add",
            json={"log_type": log_type, "log_key": log_key},
        )
        response.raise_for_status()
        _say(f"added well-known key {log_type}/{log_key}")

    response = client.get(f"{app_url}/stats")
    response.raise_for_status()
    totals = response.json()["totals"]
    _say(
        "done — /stats totals: "
        f"elements_added={totals['elements_added']} "
        f"adds_total={totals['adds_total']} "
        f"queries_total={totals['queries_total']} "
        f"memory_mb={totals['memory_mb']}"
    )


def main() -> int:
    """Seed the API named by ``APP_URL``; return a process exit code."""
    app_url = os.environ.get("APP_URL", "http://app:8001").rstrip("/")
    try:
        seed_count = int(os.environ.get("SEED_COUNT", "5000"))
        _say(f"target={app_url} populate count={seed_count}")
        with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            wait_for_health(client, app_url)
            _say("API is healthy")
            seed(client, app_url, seed_count)
        return 0
    except Exception as exc:  # any failure at all means the seed failed
        print(f"seed_demo: FAILED — {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
