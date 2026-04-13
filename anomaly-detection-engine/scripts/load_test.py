"""HTTP load test for the Anomaly Detection Engine /api/logs endpoint."""
from __future__ import annotations

import os
import statistics
import sys
import time

import httpx

# Add the project root so we can import the generator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.generator.log_generator import LogGenerator  # noqa: E402


def build_payload(gen: LogGenerator) -> dict:
    """Generate a single log entry and convert to a JSON-serialisable dict."""
    entry = gen.generate()
    return {
        "ip": entry.ip,
        "method": entry.method,
        "path": entry.path,
        "status_code": entry.status_code,
        "response_time": entry.response_time,
        "bytes_sent": entry.bytes_sent,
        "user_agent": entry.user_agent,
        "session_duration": entry.session_duration,
        "page_views": entry.page_views,
        "_is_anomaly": entry._is_anomaly,
        "_anomaly_type": entry._anomaly_type,
    }


def run_load_test(
    app_url: str,
    duration_secs: float = 10.0,
    batch_size: int = 10,
) -> None:
    """Send log entries to /api/logs as fast as possible and measure throughput."""
    endpoint = f"{app_url}/api/logs"
    gen = LogGenerator(anomaly_rate=0.05, seed=99)

    latencies: list[float] = []
    errors = 0
    total_sent = 0

    print(f"Load test: target={endpoint}  duration={duration_secs}s  batch_size={batch_size}")
    print("-" * 60)

    start = time.monotonic()

    with httpx.Client(timeout=10.0) as client:
        while True:
            elapsed = time.monotonic() - start
            if elapsed >= duration_secs:
                break

            for _ in range(batch_size):
                payload = build_payload(gen)
                req_start = time.monotonic()
                try:
                    resp = client.post(endpoint, json=payload)
                    req_end = time.monotonic()
                    latency_ms = (req_end - req_start) * 1000
                    latencies.append(latency_ms)
                    total_sent += 1

                    if resp.status_code != 200:
                        errors += 1
                except Exception:
                    errors += 1
                    total_sent += 1

    wall_time = time.monotonic() - start

    # ------------------------------------------------------------------
    # Results
    # ------------------------------------------------------------------
    throughput = total_sent / wall_time if wall_time > 0 else 0

    if latencies:
        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        avg = statistics.mean(latencies)
    else:
        p50 = p95 = p99 = avg = 0.0

    error_rate = (errors / total_sent * 100) if total_sent > 0 else 0.0

    print(f"\nLoad Test Results")
    print(f"{'=' * 40}")
    print(f"  Duration:         {wall_time:.1f}s")
    print(f"  Total requests:   {total_sent}")
    print(f"  Throughput:       {throughput:.1f} req/s")
    print(f"  Errors:           {errors} ({error_rate:.1f}%)")
    print(f"  Latency avg:      {avg:.1f}ms")
    print(f"  Latency p50:      {p50:.1f}ms")
    print(f"  Latency p95:      {p95:.1f}ms")
    print(f"  Latency p99:      {p99:.1f}ms")
    print(f"{'=' * 40}")

    # Exit non-zero if throughput target not met
    target_throughput = 100  # conservative for single-threaded sync client
    if throughput < target_throughput:
        print(f"\nWARNING: throughput {throughput:.0f} req/s is below target {target_throughput} req/s")
    else:
        print(f"\nPASSED: throughput {throughput:.0f} req/s meets target {target_throughput} req/s")


def main() -> None:
    app_url = os.environ.get("APP_URL", "http://localhost:5000")
    duration = float(os.environ.get("LOAD_DURATION", "10"))
    run_load_test(app_url, duration_secs=duration)


if __name__ == "__main__":
    main()
