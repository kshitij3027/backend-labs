"""End-to-end verification harness for the sliding-window analytics engine.

Runs inside the ``e2e`` docker-compose profile and exercises the full
data path:

1. Poll ``/api/health`` until the app reports ``healthy`` (up to 30s).
2. Assert the canonical 7-window layout is active.
3. POST a custom ``response_time`` metric event.
4. Sleep briefly to let the generator and the custom metric land in
   the 1-minute window.
5. GET ``/api/stats`` and verify the expected metrics + resolutions
   are present with non-zero counts.
6. Connect to the WebSocket ``/ws`` endpoint and verify that at least
   three consecutive ``metrics_update`` payloads are broadcast, each
   containing a non-empty ``metrics`` dict.

Prints ``E2E PASSED`` on success and exits 0; prints
``E2E FAILED: <reason>`` and exits 1 on any failure. The script uses
``httpx`` (already declared in ``requirements.txt``) plus the
``websockets`` package for the WebSocket phase.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from urllib.parse import urlparse

import httpx
import websockets

APP_URL = os.environ.get("APP_URL", "http://app:8000")
HEALTH_TIMEOUT_SECONDS = 30.0
POLL_INTERVAL_SECONDS = 1.0
EXPECTED_METRICS = ("response_time", "throughput", "error_rate")
WS_MESSAGES_REQUIRED = 3
WS_MESSAGE_TIMEOUT_SECONDS = 10.0


def _fail(reason: str) -> None:
    print(f"E2E FAILED: {reason}", flush=True)
    sys.exit(1)


def _ws_url_from_app_url(app_url: str) -> str:
    """Derive the ``ws://host:port/ws`` URL from the HTTP ``APP_URL``."""
    parsed = urlparse(app_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    netloc = parsed.netloc or parsed.path  # fall back for scheme-less inputs
    return f"{scheme}://{netloc}/ws"


async def _verify_ws(ws_url: str) -> None:
    """Open a WebSocket and assert ``WS_MESSAGES_REQUIRED`` metric updates arrive."""
    async with websockets.connect(ws_url, open_timeout=10.0) as ws:
        received = 0
        while received < WS_MESSAGES_REQUIRED:
            try:
                raw = await asyncio.wait_for(
                    ws.recv(), timeout=WS_MESSAGE_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                _fail(
                    f"WS timeout after {received}/{WS_MESSAGES_REQUIRED} messages "
                    f"(limit {WS_MESSAGE_TIMEOUT_SECONDS}s)"
                )
                return
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError as exc:
                _fail(f"WS message {received + 1} was not valid JSON: {exc}")
                return
            if not isinstance(msg, dict):
                _fail(f"WS message {received + 1} was not a dict: {msg!r}")
                return
            if msg.get("type") != "metrics_update":
                # Skip any stray non-update frames; they don't count.
                continue
            metrics = msg.get("metrics")
            if not isinstance(metrics, dict) or not metrics:
                _fail(
                    f"WS message {received + 1} had empty or missing 'metrics': {msg!r}"
                )
                return
            received += 1
            print(
                f"E2E WS: received metrics_update {received}/{WS_MESSAGES_REQUIRED} "
                f"(metrics={list(metrics.keys())})",
                flush=True,
            )


def _wait_for_health(client: httpx.Client) -> dict:
    """Poll the health endpoint until the app is ready or timeout expires."""
    deadline = time.time() + HEALTH_TIMEOUT_SECONDS
    last_error: str | None = None
    while time.time() < deadline:
        try:
            resp = client.get("/api/health", timeout=5.0)
            if resp.status_code == 200:
                body = resp.json()
                if body.get("status") == "healthy":
                    return body
                last_error = f"status was {body.get('status')!r}"
            else:
                last_error = f"HTTP {resp.status_code}"
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(POLL_INTERVAL_SECONDS)
    _fail(f"health endpoint never became healthy (last error: {last_error})")
    raise RuntimeError("unreachable")  # for type checkers


def main() -> None:
    print(f"E2E: targeting {APP_URL}", flush=True)
    with httpx.Client(base_url=APP_URL) as client:
        # 1/2. Health + active window count.
        health_body = _wait_for_health(client)
        active = int(health_body.get("active_windows", 0))
        if active < 7:
            _fail(f"expected active_windows >= 7, got {active}")
        print(f"E2E: health OK, active_windows={active}", flush=True)

        # 3. POST a custom metric.
        try:
            post_resp = client.post(
                "/api/metric",
                json={
                    "metric": "response_time",
                    "value": 123.45,
                    "metadata": {"source": "e2e"},
                },
                timeout=5.0,
            )
        except Exception as exc:
            _fail(f"POST /api/metric raised {type(exc).__name__}: {exc}")
            return
        if post_resp.status_code != 200:
            _fail(f"POST /api/metric returned HTTP {post_resp.status_code}")
        post_body = post_resp.json()
        if not post_body.get("accepted"):
            _fail(f"POST /api/metric returned accepted={post_body.get('accepted')!r}")
        print(f"E2E: POST /api/metric accepted id={post_body.get('event_id')}", flush=True)

        # 4. Give the generator + ingest path a moment to update windows.
        time.sleep(1.0)

        # 5. GET /api/stats and inspect the shape + counts.
        try:
            stats_resp = client.get("/api/stats", timeout=5.0)
        except Exception as exc:
            _fail(f"GET /api/stats raised {type(exc).__name__}: {exc}")
            return
        if stats_resp.status_code != 200:
            _fail(f"GET /api/stats returned HTTP {stats_resp.status_code}")
        stats_body = stats_resp.json()

        metrics = stats_body.get("metrics")
        if not isinstance(metrics, dict):
            _fail(f"stats body missing 'metrics' dict: {stats_body!r}")
        for name in EXPECTED_METRICS:
            if name not in metrics:
                _fail(f"metric {name!r} missing from stats response")

        rt = metrics["response_time"]
        if "1m" not in rt:
            _fail("response_time window missing '1m' resolution")
        one_min = rt["1m"]
        count = int(one_min.get("count", 0))
        if count < 1:
            _fail(f"expected response_time.1m.count >= 1, got {count}")
        print(
            f"E2E: stats OK, response_time.1m.count={count} "
            f"active_windows={stats_body.get('active_windows')}",
            flush=True,
        )

        # Commit 6: verify the backpressure-aware ingest counters.
        ingest = stats_body.get("ingest")
        if not isinstance(ingest, dict):
            _fail(f"stats body missing 'ingest' dict: {stats_body!r}")
        queue_maxsize = int(ingest.get("queue_maxsize", 0))
        processed = int(ingest.get("processed", 0))
        if queue_maxsize < 1:
            _fail(f"expected ingest.queue_maxsize >= 1, got {queue_maxsize}")
        if processed < 1:
            _fail(f"expected ingest.processed >= 1, got {processed}")
        print(
            f"E2E INGEST PASSED (queue_maxsize={queue_maxsize} "
            f"processed={processed} "
            f"dropped={ingest.get('dropped')} "
            f"sampled={ingest.get('sampled')})",
            flush=True,
        )

    # 6. WebSocket phase — connect and verify live broadcasts.
    ws_url = _ws_url_from_app_url(APP_URL)
    print(f"E2E WS: connecting to {ws_url}", flush=True)
    try:
        asyncio.run(_verify_ws(ws_url))
    except SystemExit:
        raise
    except Exception as exc:
        _fail(f"WS phase raised {type(exc).__name__}: {exc}")
        return
    print("E2E WS PASSED", flush=True)

    print("E2E PASSED", flush=True)
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - safety net
        _fail(f"unexpected exception: {type(exc).__name__}: {exc}")
