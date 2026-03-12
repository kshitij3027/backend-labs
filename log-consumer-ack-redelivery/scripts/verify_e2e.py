#!/usr/bin/env python3
"""End-to-end verification script for log-consumer-ack-redelivery.

Runs inside Docker. Expects:
- RabbitMQ at rabbitmq:5672
- App at app:8000
"""

import json
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx
import pika

APP_URL = "http://app:8000"
RABBITMQ_HOST = "rabbitmq"
MAIN_QUEUE = "logs.incoming"

results = []


def log_result(name: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    results.append((name, passed))
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))


def wait_for_health(max_wait: int = 60) -> bool:
    """Poll app:8000/health until it responds 200."""
    print("\n=== Waiting for app health ===")
    start = time.time()
    while time.time() - start < max_wait:
        try:
            r = httpx.get(f"{APP_URL}/health", timeout=5)
            if r.status_code == 200:
                log_result("Health endpoint responds", True, f"{r.json()}")
                return True
        except Exception:
            pass
        time.sleep(2)
    log_result("Health endpoint responds", False, f"Timed out after {max_wait}s")
    return False


def send_test_messages(count: int = 50) -> list[str]:
    """Publish test messages directly to RabbitMQ."""
    print(f"\n=== Sending {count} test messages ===")
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST, port=5672, credentials=credentials
        )
    )
    channel = connection.channel()

    msg_ids = []
    for i in range(count):
        msg_id = str(uuid.uuid4())
        msg = {
            "id": msg_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO",
            "service": "e2e-test",
            "message": f"E2E test message {i}",
            "metadata": {"test": True, "index": i},
        }
        channel.basic_publish(
            exchange="",
            routing_key=MAIN_QUEUE,
            body=json.dumps(msg),
            properties=pika.BasicProperties(
                delivery_mode=2, content_type="application/json"
            ),
        )
        msg_ids.append(msg_id)

    connection.close()
    log_result(f"Sent {count} messages", True)
    return msg_ids


def wait_for_processing(expected_min: int, max_wait: int = 120) -> dict:
    """Poll /api/stats until total_received >= expected_min or timeout."""
    print(f"\n=== Waiting for processing (min {expected_min} messages) ===")
    start = time.time()
    stats = {}
    while time.time() - start < max_wait:
        try:
            r = httpx.get(f"{APP_URL}/api/stats", timeout=5)
            if r.status_code == 200:
                stats = r.json()
                received = stats.get("total_received", 0)
                acked = stats.get("total_acked", 0)
                processing = stats.get("processing_count", 0)
                pending = stats.get("pending_count", 0)

                # Done when we've received enough AND nothing is in-flight
                if received >= expected_min and processing == 0 and pending == 0:
                    elapsed = time.time() - start
                    log_result(
                        "Processing complete",
                        True,
                        f"{received} received, {acked} acked in {elapsed:.1f}s",
                    )
                    return stats
        except Exception:
            pass
        time.sleep(3)

    log_result("Processing complete", False, f"Timed out. Last stats: {stats}")
    return stats


def validate_stats(stats: dict) -> None:
    """Validate the final stats meet expectations."""
    print("\n=== Validating stats ===")

    total_received = stats.get("total_received", 0)
    total_acked = stats.get("total_acked", 0)
    success_rate = stats.get("success_rate", 0)
    is_connected = stats.get("is_connected", False)

    log_result("total_received > 0", total_received > 0, f"got {total_received}")
    log_result("total_acked > 0", total_acked > 0, f"got {total_acked}")
    log_result(
        "success_rate >= 80%", success_rate >= 80.0, f"got {success_rate}%"
    )
    log_result("consumer is connected", is_connected, f"got {is_connected}")


def validate_dashboard() -> None:
    """Fetch / and check HTML content."""
    print("\n=== Validating dashboard ===")
    try:
        r = httpx.get(f"{APP_URL}/", timeout=10)
        html = r.text
        checks = [
            ("Title present", "Log Consumer Dashboard" in html),
            (
                "Stats grid present",
                "total-received" in html or "Total Received" in html,
            ),
            ("Success rate present", "Success Rate" in html),
            (
                "Auto-refresh present",
                "setInterval" in html or "fetchStats" in html,
            ),
        ]
        for name, passed in checks:
            log_result(name, passed)
    except Exception as e:
        log_result("Dashboard accessible", False, str(e))


def main():
    print("=" * 60)
    print("  Log Consumer Ack Redelivery — E2E Verification")
    print("=" * 60)

    if not wait_for_health():
        print("\nFATAL: App not healthy. Aborting.")
        sys.exit(1)

    send_test_messages(count=50)
    stats = wait_for_processing(expected_min=50, max_wait=120)
    validate_stats(stats)
    validate_dashboard()

    # Summary
    print("\n" + "=" * 60)
    total = len(results)
    passed = sum(1 for _, p in results if p)
    failed = total - passed
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    print("=" * 60)

    if failed > 0:
        print("\nFAILED checks:")
        for name, p in results:
            if not p:
                print(f"  - {name}")
        sys.exit(1)
    else:
        print("\nAll checks PASSED!")
        sys.exit(0)


if __name__ == "__main__":
    main()
