#!/usr/bin/env python3
"""End-to-end verification for Kafka Streams Monitoring Dashboard.

Runs INSIDE Docker (via the e2e service). Connects to the app service
over HTTP and to the Kafka broker directly.
"""

import json
import logging
import os
import sys
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s -- %(message)s",
)
logger = logging.getLogger(__name__)

APP_URL = os.environ.get("APP_URL", "http://app:5000")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


class E2EVerifier:
    """Run all end-to-end checks and report results."""

    def __init__(self, app_url: str = APP_URL, kafka_bootstrap: str = KAFKA_BOOTSTRAP):
        self.app_url = app_url.rstrip("/")
        self.kafka_bootstrap = kafka_bootstrap
        self.passed = 0
        self.failed = 0
        self.results: list[dict] = []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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

    def wait_for_app(self, timeout: int = 120) -> bool:
        """Poll /health until the app is ready or timeout is reached."""
        logger.info("Waiting for app to be healthy at %s/health ...", self.app_url)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = requests.get(f"{self.app_url}/health", timeout=5)
                if r.status_code == 200:
                    logger.info("App is healthy.")
                    return True
            except Exception:
                pass
            time.sleep(2)
        logger.error("App did not become healthy within %ds.", timeout)
        return False

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_health_endpoint(self) -> None:
        """GET /health -- expect 200 with status 'healthy'."""
        print("\n[Health Endpoint]")
        try:
            r = requests.get(f"{self.app_url}/health", timeout=5)
            self.check("Health endpoint returns 200", r.status_code == 200)
            data = r.json()
            self.check(
                "Health status is 'healthy'",
                data.get("status") == "healthy",
                f"status={data.get('status')}",
            )
        except Exception as e:
            self.check("Health endpoint reachable", False, str(e))

    def test_dashboard_html(self) -> None:
        """GET / -- expect 200 with dashboard title and key JS libraries."""
        print("\n[Dashboard HTML]")
        try:
            r = requests.get(f"{self.app_url}/", timeout=5)
            self.check("Dashboard returns 200", r.status_code == 200)
            self.check(
                "Dashboard contains title",
                "Kafka Streams Monitoring Dashboard" in r.text,
            )
            self.check(
                "Dashboard contains Chart.js",
                "chart.js" in r.text.lower() or "Chart" in r.text,
            )
            self.check(
                "Dashboard contains Socket.IO",
                "socket.io" in r.text.lower(),
            )
        except Exception as e:
            self.check("Dashboard reachable", False, str(e))

    def test_metrics_api(self) -> None:
        """GET /api/metrics -- expect 200 with required keys."""
        print("\n[Metrics API]")
        try:
            r = requests.get(f"{self.app_url}/api/metrics", timeout=5)
            self.check("Metrics API returns 200", r.status_code == 200)
            data = r.json()
            expected_keys = [
                "total_events",
                "per_topic_counts",
                "error_rate",
                "avg_response_time",
                "p95_response_time",
                "events_per_second",
            ]
            for key in expected_keys:
                self.check(
                    f"Metrics has '{key}'",
                    key in data,
                    f"value={data.get(key)}" if key in data else "missing",
                )
        except Exception as e:
            self.check("Metrics API reachable", False, str(e))

    def test_historical_api(self) -> None:
        """GET /api/historical -- expect 200 with time-series arrays."""
        print("\n[Historical API]")
        try:
            r = requests.get(f"{self.app_url}/api/historical", timeout=5)
            self.check("Historical API returns 200", r.status_code == 200)
            data = r.json()
            for key in ["labels", "events", "error_rate", "response_times"]:
                self.check(
                    f"Historical has '{key}' array",
                    key in data and isinstance(data[key], list),
                )
        except Exception as e:
            self.check("Historical API reachable", False, str(e))

    def test_alerts_api(self) -> None:
        """GET /api/alerts -- expect 200 with active and history keys."""
        print("\n[Alerts API]")
        try:
            r = requests.get(f"{self.app_url}/api/alerts", timeout=5)
            self.check("Alerts API returns 200", r.status_code == 200)
            data = r.json()
            self.check("Alerts has 'active' key", "active" in data)
            self.check("Alerts has 'history' key", "history" in data)
        except Exception as e:
            self.check("Alerts API reachable", False, str(e))

    def test_business_metrics_api(self) -> None:
        """GET /api/business-metrics -- expect 200 with required keys."""
        print("\n[Business Metrics API]")
        try:
            r = requests.get(f"{self.app_url}/api/business-metrics", timeout=5)
            self.check("Business metrics API returns 200", r.status_code == 200)
            data = r.json()
            self.check("Has api_versions", "api_versions" in data)
            self.check("Has funnel", "funnel" in data)
            self.check("Has auth", "auth" in data)
        except Exception as e:
            self.check("Business metrics API reachable", False, str(e))

    def test_geo_api(self) -> None:
        """GET /api/geo -- expect 200 with geographic data."""
        print("\n[Geo API]")
        try:
            r = requests.get(f"{self.app_url}/api/geo", timeout=5)
            self.check("Geo API returns 200", r.status_code == 200)
            data = r.json()
            self.check("Has traffic_by_region", "traffic_by_region" in data)
            self.check("Has latency_by_region", "latency_by_region" in data)
        except Exception as e:
            self.check("Geo API reachable", False, str(e))

    def test_data_flowing(self) -> None:
        """GET /api/metrics twice with a gap -- verify data is flowing."""
        print("\n[Data Flowing]")
        try:
            r1 = requests.get(f"{self.app_url}/api/metrics", timeout=5)
            m1 = r1.json()
            count1 = m1.get("total_events", 0)

            time.sleep(5)

            r2 = requests.get(f"{self.app_url}/api/metrics", timeout=5)
            m2 = r2.json()
            count2 = m2.get("total_events", 0)
            eps = m2.get("events_per_second", 0)

            # total_events is windowed (bounded by deque size), so it may
            # plateau once the window is full.  Accept either an increase
            # OR a high sustained count with positive events_per_second.
            flowing = count2 > count1 or (count2 >= 500 and eps > 0)
            self.check(
                "Data is flowing (events increasing or window full)",
                flowing,
                f"before={count1}, after={count2}, eps={eps}",
            )
            self.check(
                "Events per second > 0",
                eps > 0,
            )
        except Exception as e:
            self.check("Data flowing check", False, str(e))

    def test_websocket_connection(self) -> None:
        """Test WebSocket connectivity and data reception."""
        print("\n[WebSocket]")
        try:
            import socketio

            sio = socketio.SimpleClient()
            sio.connect(self.app_url)
            self.check("WebSocket connected", True)

            event = sio.receive(timeout=10)
            self.check("Received metrics_update event", event[0] == "metrics_update")

            if len(event) > 1 and isinstance(event[1], dict):
                self.check("WebSocket payload has metrics", "metrics" in event[1])
                self.check("WebSocket payload has historical", "historical" in event[1])

            sio.disconnect()
        except Exception as e:
            self.check("WebSocket test", False, str(e))

    def test_derived_metrics_topic(self) -> None:
        """Verify derived-metrics topic has data."""
        print("\n[Derived Metrics Topic]")
        try:
            from confluent_kafka import Consumer

            c = Consumer(
                {
                    "bootstrap.servers": self.kafka_bootstrap,
                    "group.id": "e2e-verifier",
                    "auto.offset.reset": "earliest",
                }
            )
            c.subscribe(["derived-metrics"])

            msg = c.poll(timeout=15.0)
            if msg and not msg.error():
                data = json.loads(msg.value().decode("utf-8"))
                self.check("Derived metrics topic has data", True)
                self.check(
                    "Derived metrics has total_events",
                    "total_events" in data,
                )
            else:
                self.check("Derived metrics topic has data", False, "No messages received")

            c.close()
        except Exception as e:
            self.check("Derived metrics topic test", False, str(e))

    # ------------------------------------------------------------------
    # Runner
    # ------------------------------------------------------------------

    def run_all(self) -> bool:
        print("=" * 60)
        print("Kafka Streams Monitoring Dashboard -- E2E Verification")
        print(f"App URL: {self.app_url}")
        print(f"Kafka:   {self.kafka_bootstrap}")
        print("=" * 60)

        tests = [
            ("Health Endpoint", self.test_health_endpoint),
            ("Dashboard HTML", self.test_dashboard_html),
            ("Metrics API", self.test_metrics_api),
            ("Historical API", self.test_historical_api),
            ("Alerts API", self.test_alerts_api),
            ("Business Metrics API", self.test_business_metrics_api),
            ("Geo API", self.test_geo_api),
            ("Data Flow", self.test_data_flowing),
            ("WebSocket", self.test_websocket_connection),
            ("Derived Metrics Topic", self.test_derived_metrics_topic),
        ]

        for name, test_fn in tests:
            try:
                test_fn()
            except Exception as e:
                self.check(name, False, f"Exception: {e}")

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


if __name__ == "__main__":
    verifier = E2EVerifier()

    if not verifier.wait_for_app(timeout=120):
        print("FATAL: App never became healthy.")
        sys.exit(1)

    # Wait for the data generator to produce data and the consumer to process it
    logger.info("Waiting 15s for data to accumulate ...")
    time.sleep(15)

    success = verifier.run_all()
    sys.exit(0 if success else 1)
