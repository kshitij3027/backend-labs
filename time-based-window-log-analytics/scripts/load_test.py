"""Load test: verify the system can handle 500+ events/sec sustained."""
import os, sys, time, random, statistics
from datetime import datetime, timezone
import httpx

APP_URL = os.environ.get("APP_URL", "http://localhost:8080")
TARGET_RATE = 600  # events per second
DURATION = 20  # seconds
BATCH_SIZE = 50

def generate_batch(n):
    levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    sources = ["web-api", "auth-svc", "db-proxy", "payment", "orders", "gateway"]
    events = []
    for _ in range(n):
        events.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": random.choice(levels),
            "source": random.choice(sources),
            "message": f"Load test event {random.randint(1, 100000)}",
            "response_time": random.uniform(10, 500),
        })
    return events

def main():
    print(f"Load test: targeting {TARGET_RATE} events/sec for {DURATION}s against {APP_URL}")

    # Wait for healthy
    for _ in range(10):
        try:
            r = httpx.get(f"{APP_URL}/health", timeout=5)
            if r.status_code == 200:
                break
        except:
            time.sleep(1)

    batches_per_sec = TARGET_RATE // BATCH_SIZE
    interval = 1.0 / batches_per_sec

    total_sent = 0
    total_accepted = 0
    latencies = []
    errors = 0

    client = httpx.Client(base_url=APP_URL, timeout=10)

    start = time.time()
    while time.time() - start < DURATION:
        batch_start = time.time()
        batch = generate_batch(BATCH_SIZE)

        try:
            t0 = time.time()
            r = client.post("/api/v1/logs/batch", json={"events": batch})
            latency = (time.time() - t0) * 1000  # ms
            latencies.append(latency)

            if r.status_code == 200:
                body = r.json()
                total_sent += len(batch)
                total_accepted += body.get("accepted", 0)
            else:
                errors += 1
                total_sent += len(batch)
        except Exception as e:
            errors += 1
            total_sent += len(batch)

        # Rate limiting
        elapsed = time.time() - batch_start
        if elapsed < interval:
            time.sleep(interval - elapsed)

    elapsed_total = time.time() - start
    client.close()

    # Report
    actual_rate = total_sent / elapsed_total
    p50 = statistics.median(latencies) if latencies else 0
    p95 = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0
    p99 = sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0

    print(f"\n=== Load Test Results ===")
    print(f"Duration: {elapsed_total:.1f}s")
    print(f"Events sent: {total_sent}")
    print(f"Events accepted: {total_accepted}")
    print(f"Errors: {errors}")
    print(f"Actual rate: {actual_rate:.0f} events/sec")
    print(f"Latency p50: {p50:.0f}ms")
    print(f"Latency p95: {p95:.0f}ms")
    print(f"Latency p99: {p99:.0f}ms")

    # Assertions
    assert actual_rate >= 500, f"Rate {actual_rate:.0f} < 500 events/sec"
    assert p99 < 2000, f"p99 latency {p99:.0f}ms >= 2000ms"
    print(f"\nPASS: {actual_rate:.0f} events/sec sustained, p99={p99:.0f}ms")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)
