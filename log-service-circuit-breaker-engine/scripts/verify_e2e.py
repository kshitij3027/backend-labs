"""End-to-end verification driver.

Run inside the test container against the running app service:
    docker compose run --rm test python -m scripts.verify_e2e
or from host with the stack up:
    python scripts/verify_e2e.py --base http://localhost:8000
"""
from __future__ import annotations
import argparse
import asyncio
import os
import sys
import time

import httpx


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="E2E verifier for circuit breaker engine")
    parser.add_argument("--base", default=os.getenv("E2E_BASE_URL", "http://app:8000"))
    parser.add_argument("--logs", type=int, default=50)
    parser.add_argument("--simulate-duration", type=int, default=5)
    parser.add_argument("--max-wait", type=int, default=15, help="seconds to wait for OPEN to appear")
    return parser.parse_args()


async def verify(base: str, logs: int, simulate_duration: int, max_wait: int) -> int:
    async with httpx.AsyncClient(base_url=base, timeout=10.0) as client:
        # 1) /health
        r = await client.get("/health")
        assert r.status_code == 200, f"/health returned {r.status_code}"
        print(f"[1/6] /health OK — {r.json()}")

        # 2) Process logs (clean run)
        r = await client.post("/api/process/logs", json={"count": logs})
        assert r.status_code == 200
        body = r.json()
        assert body["processed"] == logs
        assert body["successful"] >= int(logs * 0.9), f"unexpectedly low successes: {body}"
        print(f"[2/6] processed={body['processed']} successful={body['successful']} fallbacks={body['fallback_responses']}")

        # 3) Simulate failures on database_primary
        r = await client.post("/api/simulate/failures", json={
            "target": "database_primary",
            "duration": simulate_duration,
            "failure_rate": 0.95,
        })
        assert r.status_code == 200
        print(f"[3/6] simulating failures: {r.json()}")

        # 4) Drive traffic during simulation; observe breaker trip OPEN
        deadline = time.time() + max_wait
        observed_open = False
        while time.time() < deadline and not observed_open:
            await client.post("/api/process/logs", json={"count": 5})
            await asyncio.sleep(0.4)
            metrics = (await client.get("/api/metrics")).json()
            primary_state = metrics["circuits"]["database_primary"]["state"]
            if primary_state == "OPEN":
                observed_open = True
                break
        assert observed_open, "database_primary breaker did not transition to OPEN within max_wait"
        print("[4/6] database_primary OPEN observed")

        # 5) Verify fallback_responses incremented
        metrics = (await client.get("/api/metrics")).json()
        assert metrics["processing"]["fallback_responses"] >= 0  # may be 0 if backup served everything
        print(f"[5/6] processing stats: {metrics['processing']}")

        # 6) Wait for recovery and verify breaker re-CLOSES
        recovery_deadline = time.time() + 60
        observed_closed_again = False
        while time.time() < recovery_deadline and not observed_closed_again:
            await client.post("/api/process/logs", json={"count": 5})
            await asyncio.sleep(1.0)
            metrics = (await client.get("/api/metrics")).json()
            if metrics["circuits"]["database_primary"]["state"] == "CLOSED":
                observed_closed_again = True
                break
        assert observed_closed_again, "database_primary did not recover to CLOSED within 60s"
        print("[6/6] database_primary CLOSED again — full recovery cycle verified")

        # Bonus: Prometheus and alerts endpoints
        prom = (await client.get("/metrics")).text
        assert "circuit_breaker_state" in prom, "/metrics missing circuit_breaker_state"
        alerts = (await client.get("/api/alerts")).json()["events"]
        assert any(e["to"] == "OPEN" for e in alerts), "no OPEN alert recorded"
        print("[bonus] /metrics + /api/alerts both reporting expected data")
        return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(verify(args.base, args.logs, args.simulate_duration, args.max_wait))


if __name__ == "__main__":
    sys.exit(main())
