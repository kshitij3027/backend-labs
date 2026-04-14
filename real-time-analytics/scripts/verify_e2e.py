#!/usr/bin/env python3
"""End-to-end verification for the Real-Time Analytics Dashboard."""

import os
import sys
import json
import time
import asyncio

import httpx

APP_URL = os.getenv("APP_URL", "http://localhost:8000")


class E2EVerifier:
    def __init__(self):
        self.client = httpx.Client(base_url=APP_URL, timeout=30)
        self.passed = 0
        self.failed = 0

    def check(self, name, condition, detail=""):
        if condition:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failed += 1
            print(f"  FAIL  {name} -- {detail}")

    def run(self):
        print(f"\n{'='*60}")
        print(f"E2E Verification: {APP_URL}")
        print(f"{'='*60}\n")

        # 1. Health check (with retry)
        print("[1/7] Health Check")
        healthy = False
        for attempt in range(30):
            try:
                resp = self.client.get("/health")
                data = resp.json()
                if data.get("status") == "healthy" and data.get("redis_connected"):
                    healthy = True
                    break
            except Exception:
                pass
            time.sleep(1)
        self.check("health endpoint returns healthy", healthy)
        self.check("redis connected", healthy)

        # 2. Generate sample data
        print("\n[2/7] Data Generation")
        resp = self.client.post(
            "/api/generate-sample-data",
            params={"service": "web-api", "count": 50},
        )
        data = resp.json()
        self.check("generate-sample-data returns 200", resp.status_code == 200)
        self.check(
            "metrics stored > 0",
            data.get("metrics_stored", 0) > 0,
            f"got {data}",
        )

        # Generate for second service
        resp2 = self.client.post(
            "/api/generate-sample-data",
            params={"service": "auth-service", "count": 30},
        )
        data2 = resp2.json()
        self.check(
            "second service data generated",
            data2.get("metrics_stored", 0) > 0,
        )

        # 3. Query metrics
        print("\n[3/7] Metrics Query")
        resp = self.client.get(
            "/api/metrics/web-api/response_time",
            params={"minutes": 10},
        )
        data = resp.json()
        self.check("metrics endpoint returns 200", resp.status_code == 200)
        self.check(
            "data points returned",
            data.get("count", 0) > 0,
            f"count={data.get('count')}",
        )
        self.check("trend present", data.get("trend") is not None)
        self.check(
            "trend has direction",
            data.get("trend", {}).get("direction")
            in ("increasing", "decreasing", "stable", "insufficient_data"),
        )

        # 4. Anomalies
        print("\n[4/7] Anomaly Detection")
        resp = self.client.get("/api/anomalies", params={"hours": 1})
        data = resp.json()
        self.check("anomalies endpoint returns 200", resp.status_code == 200)
        self.check("anomalies response has count", "count" in data)
        self.check("anomalies response has list", "anomalies" in data)

        # 5. Services listing
        print("\n[5/7] Service Listing")
        resp = self.client.get("/api/services")
        data = resp.json()
        self.check("services endpoint returns 200", resp.status_code == 200)
        self.check(
            "multiple services listed",
            len(data.get("services", [])) >= 2,
            f"got {data}",
        )

        # 6. Export
        print("\n[6/7] Export")
        resp_csv = self.client.get(
            "/api/export",
            params={
                "service": "web-api",
                "metric_name": "response_time",
                "format": "csv",
            },
        )
        self.check("CSV export returns 200", resp_csv.status_code == 200)
        self.check(
            "CSV has content-type",
            "text/csv" in resp_csv.headers.get("content-type", ""),
        )
        lines = resp_csv.text.strip().split("\n")
        self.check(
            "CSV has header + data rows",
            len(lines) > 1,
            f"got {len(lines)} lines",
        )

        resp_json = self.client.get(
            "/api/export",
            params={
                "service": "web-api",
                "metric_name": "response_time",
                "format": "json",
            },
        )
        self.check("JSON export returns 200", resp_json.status_code == 200)
        try:
            json.loads(resp_json.text)
            self.check("JSON export is valid JSON", True)
        except json.JSONDecodeError as e:
            self.check("JSON export is valid JSON", False, str(e))

        # 7. WebSocket
        print("\n[7/7] WebSocket")
        ws_ok = False
        try:
            import websockets

            async def test_ws():
                uri = APP_URL.replace("http", "ws") + "/ws"
                async with websockets.connect(uri) as ws:
                    msg = json.loads(
                        await asyncio.wait_for(ws.recv(), timeout=10)
                    )
                    if msg.get("type") == "connected":
                        await ws.send(
                            json.dumps(
                                {"type": "subscribe", "streams": ["metrics"]}
                            )
                        )
                        sub_msg = json.loads(
                            await asyncio.wait_for(ws.recv(), timeout=10)
                        )
                        return sub_msg.get("type") == "subscribed"
                return False

            ws_ok = asyncio.run(test_ws())
        except Exception as e:
            print(f"    WS error: {e}")
        self.check("WebSocket connect and subscribe", ws_ok)

        # Summary
        total = self.passed + self.failed
        print(f"\n{'='*60}")
        print(f"Results: {self.passed}/{total} passed, {self.failed} failed")
        print(f"{'='*60}")

        if self.failed > 0:
            print("\nE2E FAILED")
            return 1
        print("\nE2E PASSED")
        return 0


if __name__ == "__main__":
    verifier = E2EVerifier()
    sys.exit(verifier.run())
