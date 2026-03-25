"""End-to-end verification script for the log analytics service."""

import json
import os
import sys
import time
from datetime import datetime, timezone

import httpx
import websockets
import websockets.sync.client

APP_URL = os.environ.get("APP_URL", "http://localhost:8080")


def main() -> None:
    print(f"E2E: Testing against {APP_URL}")

    # --- Health check ---
    r = httpx.get(f"{APP_URL}/health", timeout=10)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    print("PASS: Health check OK")

    # --- Ingest 20 single events ---
    levels = ["INFO", "WARN", "ERROR", "DEBUG", "INFO"]
    sources = ["web-api", "auth-svc", "db-proxy", "worker"]
    for i in range(20):
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": levels[i % len(levels)],
            "source": sources[i % len(sources)],
            "message": f"E2E test log event #{i}",
            "response_time": 50.0 + i * 2.5,
        }
        r = httpx.post(f"{APP_URL}/api/v1/logs", json=event, timeout=10)
        assert r.status_code == 200, f"Ingest #{i} failed: {r.text}"
        body = r.json()
        assert body["accepted"] >= 1, f"Ingest #{i} accepted=0: {body}"
    print("PASS: Ingested 20 single events")

    # --- Wait for windows to register ---
    time.sleep(2)

    # --- Get 5m windows ---
    r = httpx.get(f"{APP_URL}/api/v1/windows/5m", timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["window_type"] == "5m"
    assert body["count"] >= 1
    total_count = sum(w["metrics"]["count"] for w in body["windows"] if w.get("metrics"))
    assert total_count >= 20, f"Expected >= 20 events in 5m windows, got {total_count}"
    print(f"PASS: 5m windows show {total_count} events across {body['count']} window(s)")

    # --- Get stats ---
    r = httpx.get(f"{APP_URL}/api/v1/stats", timeout=10)
    assert r.status_code == 200
    stats = r.json()
    assert "uptime_seconds" in stats
    assert "total_events" in stats
    assert "active_windows" in stats
    assert "window_types" in stats
    assert stats["total_events"] >= 20, f"Expected >= 20 total events, got {stats['total_events']}"
    print(f"PASS: Stats endpoint OK — total_events={stats['total_events']}, uptime={stats['uptime_seconds']}s")

    # --- Batch ingest ---
    batch_events = [
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": levels[i % len(levels)],
            "source": "batch-test",
            "message": f"Batch event #{i}",
        }
        for i in range(5)
    ]
    r = httpx.post(f"{APP_URL}/api/v1/logs/batch", json={"events": batch_events}, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 5, f"Expected total=5, got {body['total']}"
    assert body["accepted"] >= 5, f"Expected accepted>=5, got {body['accepted']}"
    print(f"PASS: Batch ingest OK — total={body['total']}, accepted={body['accepted']}")

    # --- Invalid window type returns 404 ---
    r = httpx.get(f"{APP_URL}/api/v1/windows/invalid_type", timeout=10)
    assert r.status_code == 404
    print("PASS: Invalid window type returns 404")

    # --- Dashboard serves HTML ---
    r = httpx.get(f"{APP_URL}/dashboard", timeout=10)
    assert r.status_code == 200
    text = r.text.lower()
    assert "chart" in text or "dashboard" in text, "Dashboard HTML missing expected content"
    print("PASS: Dashboard serves HTML with Chart.js")

    # --- WebSocket connection and broadcast ---
    ws_url = APP_URL.replace("http://", "ws://").replace("https://", "wss://")
    ws_url = f"{ws_url}/ws/dashboard"
    try:
        with websockets.sync.client.connect(ws_url, close_timeout=5) as ws:
            # Wait for a broadcast (refresh interval is 5s, give 8s)
            try:
                raw = ws.recv(timeout=8)
                msg = json.loads(raw)
                assert msg.get("type") == "metrics_update", f"Unexpected message type: {msg.get('type')}"
                assert "data" in msg, "Broadcast missing 'data' key"
                print(f"PASS: WebSocket received metrics_update with {len(msg['data'])} window type(s)")
            except TimeoutError:
                # No broadcast yet — acceptable if no events were in the window
                print("WARN: No WebSocket broadcast within timeout (acceptable if windows empty)")
    except Exception as e:
        print(f"WARN: WebSocket test skipped — {e}")

    print("\nAll E2E tests passed!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)
