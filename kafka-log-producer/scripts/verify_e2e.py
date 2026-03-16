"""End-to-end verification for the Kafka Log Producer."""

import asyncio
import json
import os
import sys
import time

import httpx
import websockets
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

ALL_TOPICS = [
    "logs-application",
    "logs-database",
    "logs-errors",
    "logs-security",
]


class E2EVerifier:
    """Run a suite of end-to-end checks against the running system."""

    def __init__(
        self,
        base_url: str | None = None,
        bootstrap_servers: str | None = None,
    ) -> None:
        self.base_url = base_url or os.environ.get("APP_URL", "http://localhost:8080")
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.client = httpx.Client(base_url=self.base_url, timeout=15.0)
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        self.passed = 0
        self.failed = 0
        self.results: list[dict] = []

    # ------------------------------------------------------------------
    # Runner
    # ------------------------------------------------------------------

    def check(self, name: str, fn: callable) -> None:
        """Execute a single check, recording PASS or FAIL."""
        try:
            fn()
            self.passed += 1
            self.results.append({"name": name, "status": "PASS", "detail": ""})
            print(f"  [PASS] {name}")
        except Exception as exc:
            self.failed += 1
            detail = str(exc)
            self.results.append({"name": name, "status": "FAIL", "detail": detail})
            print(f"  [FAIL] {name}: {detail}")

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def health_endpoint(self) -> None:
        resp = self.client.get("/health")
        assert resp.status_code == 200, f"status={resp.status_code}"
        body = resp.json()
        assert body.get("status") == "ok", f"body={body}"

    def dashboard_page(self) -> None:
        resp = self.client.get("/")
        assert resp.status_code == 200, f"status={resp.status_code}"
        assert "Kafka Log Producer" in resp.text, "dashboard title not found"

    def send_sample(self) -> None:
        resp = self.client.post("/api/send-sample")
        assert resp.status_code == 200, f"status={resp.status_code}"
        body = resp.json()
        assert body.get("logs_sent") == 10, f"body={body}"

    def stats_endpoint(self) -> None:
        resp = self.client.get("/api/stats")
        assert resp.status_code == 200, f"status={resp.status_code}"
        body = resp.json()
        assert "total_sent" in body, f"missing total_sent: {body}"
        assert "metrics" in body, f"missing metrics: {body}"

    def websocket_connects(self) -> None:
        ws_url = self.base_url.replace("http://", "ws://").replace(
            "https://", "wss://"
        )

        async def _ws_check():
            async with websockets.connect(f"{ws_url}/ws") as ws:
                data = await asyncio.wait_for(ws.recv(), timeout=10.0)
                msg = json.loads(data)
                assert "total_sent" in msg, f"unexpected ws payload: {msg}"

        asyncio.run(_ws_check())

    def kafka_topics_exist(self) -> None:
        metadata = self.admin.list_topics(timeout=10)
        existing = set(metadata.topics.keys())
        for topic in ALL_TOPICS:
            assert topic in existing, f"topic {topic!r} missing (have: {existing})"

    def messages_in_kafka(self) -> None:
        """After send-sample, verify messages are consumable from logs-application."""
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": "e2e-verify-app",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe(["logs-application"])
        try:
            found = False
            deadline = time.time() + 15
            while time.time() < deadline:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                data = json.loads(msg.value().decode())
                assert "level" in data, f"missing 'level' in {data}"
                assert "message" in data, f"missing 'message' in {data}"
                found = True
                break
            assert found, "no messages consumed from logs-application within timeout"
        finally:
            consumer.close()

    def topic_routing(self) -> None:
        """Send an error burst and verify messages land in logs-errors."""
        self.client.post("/api/send-error-burst")
        time.sleep(2)  # allow delivery

        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": "e2e-verify-errors",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe(["logs-errors"])
        try:
            found = False
            deadline = time.time() + 15
            while time.time() < deadline:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                data = json.loads(msg.value().decode())
                assert data.get("level") in (
                    "ERROR",
                    "CRITICAL",
                ), f"unexpected level: {data.get('level')}"
                found = True
                break
            assert found, "no ERROR/CRITICAL messages found in logs-errors"
        finally:
            consumer.close()

    def prometheus_metrics(self) -> None:
        metrics_url = os.environ.get("METRICS_URL", "http://localhost:8000")
        # Use a separate client for the metrics port
        resp = httpx.get(f"{metrics_url}/metrics", timeout=10.0)
        assert resp.status_code == 200, f"status={resp.status_code}"
        assert "messages_sent_total" in resp.text, "messages_sent_total not found"

    def send_error_burst(self) -> None:
        resp = self.client.post("/api/send-error-burst")
        assert resp.status_code == 200, f"status={resp.status_code}"
        body = resp.json()
        assert body.get("logs_sent") == 5, f"body={body}"

    def partition_key_present(self) -> None:
        """Verify that produced messages have a non-null partition key."""
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": "e2e-verify-key",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe(["logs-application"])
        try:
            deadline = time.time() + 15
            while time.time() < deadline:
                msg = consumer.poll(timeout=2.0)
                if msg is None or msg.error():
                    continue
                assert msg.key() is not None, "message key is None"
                return
            raise AssertionError("no messages consumed to check partition key")
        finally:
            consumer.close()

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run_all(self) -> None:
        """Run every check in sequence."""
        print(f"\nE2E Verification against {self.base_url}")
        print(f"Kafka bootstrap: {self.bootstrap_servers}")
        print("-" * 60)

        self.check("health_endpoint", self.health_endpoint)
        self.check("dashboard_page", self.dashboard_page)
        self.check("send_sample", self.send_sample)
        self.check("stats_endpoint", self.stats_endpoint)
        self.check("websocket_connects", self.websocket_connects)
        self.check("kafka_topics_exist", self.kafka_topics_exist)
        self.check("messages_in_kafka", self.messages_in_kafka)
        self.check("topic_routing", self.topic_routing)
        self.check("prometheus_metrics", self.prometheus_metrics)
        self.check("send_error_burst", self.send_error_burst)
        self.check("partition_key_present", self.partition_key_present)

    def report(self) -> int:
        """Print a summary and return the exit code."""
        total = self.passed + self.failed
        print("\n" + "=" * 60)
        print(f"  Results: {self.passed}/{total} passed, {self.failed} failed")
        print("=" * 60)

        if self.failed > 0:
            print("\nFailed checks:")
            for r in self.results:
                if r["status"] == "FAIL":
                    print(f"  - {r['name']}: {r['detail']}")
            print()

        return 0 if self.failed == 0 else 1


if __name__ == "__main__":
    verifier = E2EVerifier()
    verifier.run_all()
    sys.exit(verifier.report())
