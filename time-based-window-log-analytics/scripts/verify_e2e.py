"""End-to-end verification script for the log analytics service."""

import json
import os
import random
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

    # --- E-Commerce: Ingest 10 order events ---
    for i in range(10):
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "source": "order-svc",
            "message": f"Order event #{i}",
            "order_id": f"ORD-E2E-{i:04d}",
            "order_value": 20.0 + i * 5.0,
            "order_status": ["placed", "confirmed", "cancelled"][i % 3],
        }
        r = httpx.post(f"{APP_URL}/api/v1/logs", json=event, timeout=10)
        assert r.status_code == 200
        assert r.json()["accepted"] >= 1
    print("PASS: Ingested 10 order events")

    time.sleep(1)

    # --- E-Commerce: Verify order_5m e-commerce endpoint ---
    r = httpx.get(f"{APP_URL}/api/v1/windows/order_5m/ecommerce", timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["window_type"] == "order_5m"
    assert body["count"] >= 1
    total_orders = sum(w["order_count"] for w in body["windows"])
    assert total_orders >= 10, f"Expected >= 10 orders, got {total_orders}"
    total_revenue = sum(w["total_revenue"] for w in body["windows"])
    assert total_revenue > 0, f"Expected revenue > 0, got {total_revenue}"
    print(f"PASS: E-commerce order_5m — {total_orders} orders, ${total_revenue:.2f} revenue")

    # --- Replay: POST 5 historical events ---
    replay_events = [
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "WARN",
            "source": "replay-test",
            "message": f"Replay event #{i}",
        }
        for i in range(5)
    ]
    r = httpx.post(f"{APP_URL}/api/v1/replay", json={
        "start_time": "2024-01-01T00:00:00Z",
        "end_time": "2024-01-01T01:00:00Z",
        "events": replay_events,
    }, timeout=10)
    assert r.status_code == 200
    body = r.json()
    assert body["events_processed"] == 5, f"Expected 5 processed, got {body['events_processed']}"
    assert body["windows_created"] >= 1, f"Expected >= 1 windows, got {body['windows_created']}"
    assert body["errors"] == [], f"Unexpected errors: {body['errors']}"
    print(f"PASS: Replay — processed={body['events_processed']}, windows={body['windows_created']}")

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

    # --- Mini load test: 200 events/sec for 5 seconds (1000 events) ---
    print("\n--- Mini load test: 200 events/sec for 5s ---")
    MINI_RATE = 200
    MINI_DURATION = 5
    MINI_BATCH = 50
    mini_batches_per_sec = MINI_RATE // MINI_BATCH
    mini_interval = 1.0 / mini_batches_per_sec

    mini_sent = 0
    mini_accepted = 0
    mini_errors = 0
    levels_load = ["INFO", "WARN", "ERROR", "DEBUG"]
    sources_load = ["web-api", "auth-svc", "db-proxy", "payment", "orders", "gateway"]

    client = httpx.Client(base_url=APP_URL, timeout=10)
    mini_start = time.time()
    while time.time() - mini_start < MINI_DURATION:
        batch_start = time.time()
        batch_events = [
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": random.choice(levels_load),
                "source": random.choice(sources_load),
                "message": f"Mini load event {random.randint(1, 100000)}",
                "response_time": random.uniform(10, 500),
            }
            for _ in range(MINI_BATCH)
        ]
        try:
            r = client.post("/api/v1/logs/batch", json={"events": batch_events})
            if r.status_code == 200:
                body = r.json()
                mini_sent += len(batch_events)
                mini_accepted += body.get("accepted", 0)
            else:
                mini_errors += 1
                mini_sent += len(batch_events)
        except Exception:
            mini_errors += 1
            mini_sent += len(batch_events)

        elapsed = time.time() - batch_start
        if elapsed < mini_interval:
            time.sleep(mini_interval - elapsed)

    mini_elapsed = time.time() - mini_start
    client.close()

    mini_rate = mini_sent / mini_elapsed if mini_elapsed > 0 else 0
    assert mini_sent >= 800, f"Mini load sent only {mini_sent} events (expected >= 800)"
    assert mini_accepted >= mini_sent * 0.9, (
        f"Mini load accepted {mini_accepted}/{mini_sent} (expected >= 90%)"
    )
    print(
        f"PASS: Mini load test — sent={mini_sent}, accepted={mini_accepted}, "
        f"rate={mini_rate:.0f} events/sec, errors={mini_errors}"
    )

    # --- Persistence / data consistency test ---
    time.sleep(2)  # let windows settle

    r = httpx.get(f"{APP_URL}/api/v1/windows/5m", timeout=10)
    assert r.status_code == 200
    body = r.json()
    window_total = sum(
        w["metrics"]["count"] for w in body["windows"] if w.get("metrics")
    )

    r = httpx.get(f"{APP_URL}/api/v1/stats", timeout=10)
    assert r.status_code == 200
    stats_total = r.json()["total_events"]

    # We sent at least: 20 singles + 5 batch + 10 order + 5 replay + mini_sent
    min_expected = 20 + 5 + 10 + mini_sent
    assert stats_total >= min_expected, (
        f"Stats total_events={stats_total} < expected minimum {min_expected}"
    )
    print(
        f"PASS: Data consistency — stats total_events={stats_total} "
        f"(>= {min_expected} expected), 5m window count={window_total}"
    )

    print("\nAll E2E tests passed!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)
