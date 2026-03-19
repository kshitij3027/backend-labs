#!/usr/bin/env python3
"""End-to-end verification script for the Kafka partitioning consumer group system."""
import json
import os
import sys
import time
import asyncio

import httpx


APP_URL = os.environ.get("APP_URL", "http://localhost:8080")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


class E2EVerifier:
    """Runs end-to-end checks against the running system."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.results = []

    def check(self, name: str, condition: bool, detail: str = "") -> bool:
        status = "PASS" if condition else "FAIL"
        self.results.append({"name": name, "status": status, "detail": detail})
        if condition:
            self.passed += 1
            print(f"  [PASS] {name}")
        else:
            self.failed += 1
            print(f"  [FAIL] {name}: {detail}")
        return condition

    def run(self) -> bool:
        print(f"\n{'='*60}")
        print("E2E Verification Suite")
        print(f"Target: {APP_URL}")
        print(f"{'='*60}\n")

        # 1. Health check
        print("[1/8] Health Check")
        try:
            resp = httpx.get(f"{APP_URL}/health", timeout=10)
            self.check("Health endpoint returns 200", resp.status_code == 200)
            data = resp.json()
            self.check("Status is ok", data.get("status") == "ok")
            self.check("Consumers running", data.get("consumers", 0) > 0,
                       f"consumers={data.get('consumers')}")
        except Exception as e:
            self.check("Health endpoint reachable", False, str(e))

        # 2. Dashboard page
        print("\n[2/8] Dashboard Page")
        try:
            resp = httpx.get(f"{APP_URL}/", timeout=10)
            self.check("Dashboard returns 200", resp.status_code == 200)
            self.check("Dashboard has title", "Kafka Partitioning" in resp.text)
            self.check("Dashboard has Chart.js", "chart.js" in resp.text.lower() or "Chart" in resp.text)
        except Exception as e:
            self.check("Dashboard reachable", False, str(e))

        # 3. Stats API
        print("\n[3/8] Stats API")
        try:
            resp = httpx.get(f"{APP_URL}/api/stats", timeout=10)
            self.check("Stats returns 200", resp.status_code == 200)
            data = resp.json()
            self.check("Has total_consumed", "total_consumed" in data)
            self.check("Has per_partition", "per_partition" in data)
            self.check("Has per_consumer", "per_consumer" in data)
            self.check("Has producer stats", "producer" in data)
        except Exception as e:
            self.check("Stats API reachable", False, str(e))

        # 4. Partitions API
        print("\n[4/8] Partitions API")
        try:
            resp = httpx.get(f"{APP_URL}/api/partitions", timeout=10)
            self.check("Partitions returns 200", resp.status_code == 200)
            data = resp.json()
            self.check("Has num_partitions", data.get("num_partitions") == 6,
                       f"num_partitions={data.get('num_partitions')}")
        except Exception as e:
            self.check("Partitions API reachable", False, str(e))

        # 5. WebSocket
        print("\n[5/8] WebSocket")
        try:
            ws_result = asyncio.run(self._test_websocket())
            self.check("WebSocket connects and receives data", ws_result)
        except Exception as e:
            self.check("WebSocket test", False, str(e))

        # 6. Message flow (wait and check stats increase)
        print("\n[6/8] Message Flow")
        try:
            resp1 = httpx.get(f"{APP_URL}/api/stats", timeout=10)
            count1 = resp1.json().get("total_consumed", 0)
            time.sleep(5)
            resp2 = httpx.get(f"{APP_URL}/api/stats", timeout=10)
            count2 = resp2.json().get("total_consumed", 0)
            self.check("Messages being consumed", count2 > count1,
                       f"before={count1}, after={count2}")
            self.check("Messages > 0", count2 > 0, f"count={count2}")
        except Exception as e:
            self.check("Message flow check", False, str(e))

        # 7. Partition distribution
        print("\n[7/8] Partition Distribution")
        try:
            resp = httpx.get(f"{APP_URL}/api/stats", timeout=10)
            data = resp.json()
            pp = data.get("per_partition", {})
            non_zero = sum(1 for v in pp.values() if int(v) > 0)
            self.check("Multiple partitions active", non_zero >= 3,
                       f"active_partitions={non_zero}")
            pc = data.get("per_consumer", {})
            self.check("Multiple consumers active", len(pc) >= 2,
                       f"active_consumers={len(pc)}")
        except Exception as e:
            self.check("Partition distribution check", False, str(e))

        # 8. Consumer group
        print("\n[8/8] Consumer Group")
        try:
            resp = httpx.get(f"{APP_URL}/api/stats", timeout=10)
            data = resp.json()
            rebal = data.get("rebalance_events", [])
            self.check("Rebalance events recorded", len(rebal) > 0,
                       f"events={len(rebal)}")
            # Check each consumer has partitions assigned
            for cid, info in data.get("per_consumer", {}).items():
                self.check(f"{cid} has partitions", len(info.get("partitions", [])) > 0)
        except Exception as e:
            self.check("Consumer group check", False, str(e))

        # Summary
        print(f"\n{'='*60}")
        print(f"Results: {self.passed} passed, {self.failed} failed")
        print(f"{'='*60}\n")

        return self.failed == 0

    async def _test_websocket(self) -> bool:
        import websockets
        ws_url = APP_URL.replace("http://", "ws://").replace("https://", "wss://") + "/ws"
        async with websockets.connect(ws_url) as ws:
            data = await asyncio.wait_for(ws.recv(), timeout=5.0)
            parsed = json.loads(data)
            return "total_consumed" in parsed


if __name__ == "__main__":
    # Wait for app to be ready
    print("Waiting for application to be ready...")
    for i in range(30):
        try:
            resp = httpx.get(f"{APP_URL}/health", timeout=5)
            if resp.status_code == 200:
                print(f"Application ready after {i+1}s")
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        print("Application not ready after 30s, proceeding anyway...")

    # Wait for some data to accumulate
    print("Waiting 15s for data accumulation...")
    time.sleep(15)

    verifier = E2EVerifier()
    success = verifier.run()
    sys.exit(0 if success else 1)
