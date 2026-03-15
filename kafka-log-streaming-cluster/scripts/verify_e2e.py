#!/usr/bin/env python3
"""End-to-end verification for Kafka Log Streaming Cluster.

Runs INSIDE Docker (via the e2e service). Connects to other services
using their Docker service names.
"""

import json
import os
import sys
import time

import httpx
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, RESOURCE_TOPIC

DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "dashboard")
DASHBOARD_PORT = os.environ.get("DASHBOARD_PORT", "8000")
KAFKA_UI_HOST = os.environ.get("KAFKA_UI_HOST", "kafka-ui")
KAFKA_UI_PORT = os.environ.get("KAFKA_UI_PORT", "8080")
BOOTSTRAP_SERVERS = os.environ.get(
    "BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29092,kafka-3:29092"
)

BASE_URL = f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}"
KAFKA_UI_URL = f"http://{KAFKA_UI_HOST}:{KAFKA_UI_PORT}"


class E2EVerifier:
    """Run all end-to-end checks and report results."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.results: list[dict] = []

    def check(self, name: str, condition: bool, detail: str = "") -> None:
        status = "PASS" if condition else "FAIL"
        self.results.append({"name": name, "status": status, "detail": detail})
        if condition:
            self.passed += 1
            suffix = f" -- {detail}" if detail else ""
            print(f"  [PASS] {name}{suffix}")
        else:
            self.failed += 1
            suffix = f" -- {detail}" if detail else ""
            print(f"  [FAIL] {name}{suffix}")

    # ------------------------------------------------------------------
    # Runner
    # ------------------------------------------------------------------

    def run_all(self) -> bool:
        print("=" * 60)
        print("Kafka Log Streaming Cluster -- E2E Verification")
        print(f"Dashboard: {BASE_URL}")
        print(f"Kafka:     {BOOTSTRAP_SERVERS}")
        print("=" * 60)

        self.test_health_endpoint()
        self.test_dashboard_page()
        self.test_logs_api()
        self.test_stats_api()
        self.test_errors_api()
        self.test_metrics_api()
        self.test_sse_stream()
        self.test_ordering_api()
        self.test_kafka_ui()
        self.test_message_flow()
        self.test_error_aggregation()
        self.test_consumer_groups()
        self.test_multi_broker()
        self.test_topic_configuration()
        self.test_retention_config()

        print()
        print("=" * 60)
        print("RESULTS SUMMARY")
        print("=" * 60)
        for r in self.results:
            tag = r["status"]
            suffix = f" -- {r['detail']}" if r["detail"] else ""
            print(f"  [{tag}] {r['name']}{suffix}")
        print()
        print(f"  {self.passed} passed, {self.failed} failed")
        print("=" * 60)

        if self.failed:
            print("\nE2E verification FAILED.\n")
        else:
            print("\nE2E verification PASSED.\n")

        return self.failed == 0

    # ------------------------------------------------------------------
    # Dashboard API checks
    # ------------------------------------------------------------------

    def test_health_endpoint(self):
        """Check 1: Dashboard health endpoint."""
        print("\n[Health Endpoint]")
        try:
            r = httpx.get(f"{BASE_URL}/health", timeout=5)
            self.check("Health endpoint returns 200", r.status_code == 200)
            data = r.json()
            self.check("Health shows OK status", data.get("status") == "ok")
            consumers = data.get("consumers", {})
            self.check(
                "Dashboard consumer running",
                consumers.get("dashboard") is True,
            )
            self.check(
                "Error aggregator running",
                consumers.get("error_aggregator") is True,
            )
        except Exception as e:
            self.check("Health endpoint reachable", False, str(e))

    def test_dashboard_page(self):
        """Check 2: Dashboard serves HTML."""
        print("\n[Dashboard Page]")
        try:
            r = httpx.get(f"{BASE_URL}/", timeout=5)
            self.check("Dashboard returns 200", r.status_code == 200)
            self.check(
                "Dashboard contains title",
                "Kafka Log Streaming" in r.text or "kafka" in r.text.lower(),
            )
        except Exception as e:
            self.check("Dashboard reachable", False, str(e))

    def test_logs_api(self):
        """Check 3: Logs API returns data."""
        print("\n[Logs API]")
        try:
            r = httpx.get(f"{BASE_URL}/api/logs", timeout=5)
            self.check("Logs API returns 200", r.status_code == 200)
            data = r.json()
            self.check("Logs API returns list", isinstance(data, list))
            self.check(
                "Logs API has messages",
                len(data) > 0,
                f"{len(data)} messages",
            )
            if data:
                msg = data[0]
                self.check("Log has data field", "data" in msg)
                self.check("Log has topic field", "topic" in msg)
        except Exception as e:
            self.check("Logs API reachable", False, str(e))

    def test_stats_api(self):
        """Check 4: Stats API returns expected structure."""
        print("\n[Stats API]")
        try:
            r = httpx.get(f"{BASE_URL}/api/stats", timeout=5)
            self.check("Stats API returns 200", r.status_code == 200)
            data = r.json()
            self.check(
                "Stats has total",
                "total" in data and data["total"] > 0,
                f"total={data.get('total')}",
            )
            self.check(
                "Stats has by_service",
                "by_service" in data and len(data.get("by_service", {})) > 0,
            )
            self.check(
                "Stats has by_level",
                "by_level" in data and len(data.get("by_level", {})) > 0,
            )
        except Exception as e:
            self.check("Stats API reachable", False, str(e))

    def test_errors_api(self):
        """Check 5: Errors API returns error data."""
        print("\n[Errors API]")
        try:
            r = httpx.get(f"{BASE_URL}/api/errors", timeout=5)
            self.check("Errors API returns 200", r.status_code == 200)
            data = r.json()
            self.check("Errors has recent_errors", "recent_errors" in data)
            self.check("Errors has error_counts", "error_counts" in data)
            self.check("Errors has error_rate", "error_rate" in data)
        except Exception as e:
            self.check("Errors API reachable", False, str(e))

    def test_metrics_api(self):
        """Check 6: Metrics API returns metrics."""
        print("\n[Metrics API]")
        try:
            r = httpx.get(f"{BASE_URL}/api/metrics", timeout=5)
            self.check("Metrics API returns 200", r.status_code == 200)
            data = r.json()
            self.check("Metrics has throughput", "throughput" in data)
            self.check("Metrics has consumer_lag", "consumer_lag" in data)
            self.check("Metrics has latency", "latency" in data)
        except Exception as e:
            self.check("Metrics API reachable", False, str(e))

    def test_sse_stream(self):
        """Check 7: SSE stream is accessible."""
        print("\n[SSE Stream]")
        try:
            with httpx.stream("GET", f"{BASE_URL}/api/stream", timeout=5) as r:
                self.check("SSE stream returns 200", r.status_code == 200)
                content_type = r.headers.get("content-type", "")
                self.check(
                    "SSE has event-stream type",
                    "text/event-stream" in content_type,
                )
        except Exception as e:
            self.check("SSE stream reachable", False, str(e))

    def test_ordering_api(self):
        """Check 8: Ordering verification."""
        print("\n[Ordering API]")
        try:
            r = httpx.get(f"{BASE_URL}/api/ordering", timeout=10)
            self.check("Ordering API returns 200", r.status_code == 200)
            data = r.json()
            self.check("Ordering has ordered field", "ordered" in data)
            violations = data.get("violations", [])
            self.check(
                "Messages are ordered",
                data.get("ordered") is True,
                f"groups={data.get('total_groups')}, violations={len(violations)}",
            )
        except Exception as e:
            self.check("Ordering API reachable", False, str(e))

    def test_kafka_ui(self):
        """Check 9: Kafka UI accessible."""
        print("\n[Kafka UI]")
        try:
            r = httpx.get(KAFKA_UI_URL, timeout=5)
            self.check("Kafka UI returns 200", r.status_code == 200)
        except Exception as e:
            self.check("Kafka UI reachable", False, str(e))

    # ------------------------------------------------------------------
    # End-to-end message flow checks
    # ------------------------------------------------------------------

    def test_message_flow(self):
        """Check 10: Produce a message and verify it appears in the API."""
        print("\n[Message Flow]")
        try:
            test_id = f"e2e-test-{int(time.time())}"
            producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
            test_msg = json.dumps(
                {
                    "timestamp": "2026-03-15T12:00:00+00:00",
                    "service": "web-api",
                    "level": "INFO",
                    "endpoint": "/e2e-test",
                    "status_code": 200,
                    "user_id": test_id,
                    "message": "E2E test message",
                    "sequence_number": 999999,
                }
            )
            producer.produce(
                "web-api-logs", test_msg.encode(), test_id.encode()
            )
            producer.flush(timeout=10)

            # Wait for the dashboard consumer to pick it up
            time.sleep(5)
            r = httpx.get(f"{BASE_URL}/api/logs", timeout=5)
            logs = r.json()
            found = any(
                msg.get("data", {}).get("user_id") == test_id for msg in logs
            )
            self.check(
                "Produced message appears in API",
                found,
                f"test_id={test_id}",
            )
        except Exception as e:
            self.check("Message flow test", False, str(e))

    def test_error_aggregation(self):
        """Check 11: Produce an ERROR and verify it in the errors API."""
        print("\n[Error Aggregation]")
        try:
            test_id = f"e2e-error-{int(time.time())}"
            producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
            error_msg = json.dumps(
                {
                    "timestamp": "2026-03-15T12:00:00+00:00",
                    "service": "payment-service",
                    "level": "ERROR",
                    "endpoint": "/e2e-error-test",
                    "status_code": 500,
                    "user_id": test_id,
                    "message": "E2E error test",
                    "sequence_number": 999998,
                }
            )
            # Publish to both the service topic and the critical-logs topic
            # (mimics what the producer does for ERROR messages)
            producer.produce(
                "payment-service-logs", error_msg.encode(), test_id.encode()
            )
            producer.produce(
                "critical-logs", error_msg.encode(), test_id.encode()
            )
            producer.flush(timeout=10)

            time.sleep(5)
            r = httpx.get(f"{BASE_URL}/api/errors", timeout=5)
            data = r.json()
            errors = data.get("recent_errors", [])
            found = any(
                e.get("data", {}).get("user_id") == test_id for e in errors
            )
            self.check(
                "ERROR message appears in errors API",
                found,
                f"test_id={test_id}",
            )
        except Exception as e:
            self.check("Error aggregation test", False, str(e))

    # ------------------------------------------------------------------
    # Kafka cluster checks
    # ------------------------------------------------------------------

    def test_consumer_groups(self):
        """Check 12: Verify consumer groups exist."""
        print("\n[Consumer Groups]")
        try:
            admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
            future = admin.list_consumer_groups(request_timeout=10)
            result = future.result()
            group_ids = [g.group_id for g in result.valid]
            has_dashboard = any("dashboard" in g for g in group_ids)
            has_error = any("error" in g for g in group_ids)
            self.check(
                "Dashboard consumer group exists",
                has_dashboard,
                f"groups={group_ids}",
            )
            self.check(
                "Error aggregator group exists",
                has_error,
                f"groups={group_ids}",
            )
        except Exception as e:
            self.check("Consumer groups check", False, str(e))

    def test_multi_broker(self):
        """Check 13: Verify 3 brokers in cluster."""
        print("\n[Multi-Broker]")
        try:
            admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
            metadata = admin.list_topics(timeout=10)
            broker_count = len(metadata.brokers)
            self.check(
                "3 brokers in cluster",
                broker_count == 3,
                f"brokers={broker_count}",
            )
        except Exception as e:
            self.check("Multi-broker check", False, str(e))

    def test_topic_configuration(self):
        """Check 14: Verify topics exist with correct partition counts."""
        print("\n[Topic Configuration]")
        try:
            admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
            metadata = admin.list_topics(timeout=10)
            topics = metadata.topics

            expected = {
                "web-api-logs": 3,
                "user-service-logs": 3,
                "payment-service-logs": 3,
                "critical-logs": 1,
            }

            for topic_name, expected_partitions in expected.items():
                exists = topic_name in topics
                self.check(f"Topic '{topic_name}' exists", exists)
                if exists:
                    actual_parts = len(topics[topic_name].partitions)
                    self.check(
                        f"  {topic_name} has {expected_partitions} partition(s)",
                        actual_parts == expected_partitions,
                        f"actual={actual_parts}",
                    )
        except Exception as e:
            self.check("Topic configuration check", False, str(e))

    def test_retention_config(self):
        """Check 15: Verify 7-day retention is configured."""
        print("\n[Retention Config]")
        try:
            admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
            resource = ConfigResource(RESOURCE_TOPIC, "web-api-logs")
            futures = admin.describe_configs([resource])
            for _res, future in futures.items():
                config = future.result()
                retention = config.get("retention.ms")
                if retention:
                    retention_val = int(retention.value)
                    expected = 604800000  # 7 days in ms
                    self.check(
                        "7-day retention configured",
                        retention_val == expected,
                        f"retention.ms={retention_val}",
                    )
                else:
                    self.check(
                        "7-day retention configured",
                        False,
                        "retention.ms not found in config",
                    )
        except Exception as e:
            self.check("Retention config check", False, str(e))


if __name__ == "__main__":
    verifier = E2EVerifier()
    success = verifier.run_all()
    sys.exit(0 if success else 1)
