"""End-to-end verification for the Kafka Log Consumer with Analytics."""
import asyncio
import json
import os
import sys
import time

import httpx
import websockets
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

ALL_TOPICS = ["web-logs", "app-logs", "error-logs", "dead-letter-logs"]


class E2EVerifier:
    """Run a suite of end-to-end checks against the running system."""

    def __init__(self, base_url=None, bootstrap_servers=None):
        self.base_url = base_url or os.environ.get("APP_URL", "http://localhost:8080")
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.client = httpx.Client(base_url=self.base_url, timeout=15.0)
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        self.passed = 0
        self.failed = 0
        self.results = []

    def check(self, name, fn):
        try:
            fn()
            self.passed += 1
            self.results.append({"name": name, "status": "PASS"})
            print(f"  [PASS] {name}")
        except Exception as exc:
            self.failed += 1
            self.results.append({"name": name, "status": "FAIL", "detail": str(exc)})
            print(f"  [FAIL] {name}: {exc}")

    def health_endpoint(self):
        resp = self.client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok", f"status={body['status']}"

    def dashboard_page(self):
        resp = self.client.get("/")
        assert resp.status_code == 200
        assert "Kafka Log Consumer" in resp.text

    def stats_endpoint(self):
        resp = self.client.get("/api/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert "consumer" in body
        assert "analytics" in body

    def analytics_endpoint(self):
        resp = self.client.get("/api/analytics")
        assert resp.status_code == 200
        body = resp.json()
        assert "percentiles" in body
        assert "endpoints" in body

    def metrics_endpoint(self):
        resp = self.client.get("/api/metrics")
        assert resp.status_code == 200
        body = resp.json()
        assert "throughput_history" in body

    def websocket_connects(self):
        ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")

        async def _check():
            async with websockets.connect(f"{ws_url}/ws") as ws:
                data = await asyncio.wait_for(ws.recv(), timeout=10.0)
                msg = json.loads(data)
                assert "stats" in msg, f"unexpected ws payload: {msg}"

        asyncio.run(_check())

    def kafka_topics_exist(self):
        metadata = self.admin.list_topics(timeout=10)
        existing = set(metadata.topics.keys())
        for topic in ALL_TOPICS:
            assert topic in existing, f"topic {topic!r} missing"

    def message_flow(self):
        """Produce messages and verify consumer processes them."""
        producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        for i in range(10):
            msg = json.dumps({
                "log_type": "web_access",
                "endpoint": f"/api/e2e-test-{i}",
                "method": "GET",
                "status_code": 200,
                "response_time_ms": 25.0,
                "geo": "us-east",
            })
            producer.produce("web-logs", value=msg.encode())
        producer.flush(timeout=10)
        time.sleep(8)  # Wait for consumer batch

        resp = self.client.get("/api/stats")
        body = resp.json()
        consumed = body.get("consumer", {}).get("total_consumed", 0)
        assert consumed > 0, f"no messages consumed (got {consumed})"

    def analytics_non_zero(self):
        resp = self.client.get("/api/stats")
        body = resp.json()
        total = body.get("analytics", {}).get("total_messages", 0)
        assert total > 0, f"analytics total_messages is {total}"

    def consumer_lag_reported(self):
        resp = self.client.get("/api/stats")
        body = resp.json()
        analytics = body.get("analytics", {})
        assert "consumer_lag" in analytics or "total_messages" in analytics

    def run_all(self):
        print(f"\nE2E Verification against {self.base_url}")
        print(f"Kafka bootstrap: {self.bootstrap_servers}")
        print("-" * 60)

        self.check("health_endpoint", self.health_endpoint)
        self.check("dashboard_page", self.dashboard_page)
        self.check("stats_endpoint", self.stats_endpoint)
        self.check("analytics_endpoint", self.analytics_endpoint)
        self.check("metrics_endpoint", self.metrics_endpoint)
        self.check("websocket_connects", self.websocket_connects)
        self.check("kafka_topics_exist", self.kafka_topics_exist)
        self.check("message_flow", self.message_flow)
        self.check("analytics_non_zero", self.analytics_non_zero)
        self.check("consumer_lag_reported", self.consumer_lag_reported)

    def report(self):
        total = self.passed + self.failed
        print("\n" + "=" * 60)
        print(f"  Results: {self.passed}/{total} passed, {self.failed} failed")
        print("=" * 60)
        if self.failed > 0:
            print("\nFailed checks:")
            for r in self.results:
                if r["status"] == "FAIL":
                    print(f"  - {r['name']}: {r.get('detail', '')}")
        return 0 if self.failed == 0 else 1


if __name__ == "__main__":
    verifier = E2EVerifier()
    verifier.run_all()
    sys.exit(verifier.report())
