#!/usr/bin/env python3
"""End-to-end verification for Kafka Log Compaction State Manager.

Runs INSIDE Docker (via the e2e service). Connects to the app service
over HTTP and to the Kafka broker directly.
"""

import json
import logging
import os
import sys
import time

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, ConfigResource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s -- %(message)s",
)
logger = logging.getLogger(__name__)

APP_URL = os.environ.get("APP_URL", "http://app:5555")
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka:29092")


class E2EVerifier:
    """Run all end-to-end checks and report results."""

    def __init__(self, app_url: str = APP_URL, kafka_bootstrap: str = BOOTSTRAP_SERVERS):
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

    def wait_for_app(self, timeout: int = 60) -> bool:
        """Poll /health until the app is ready or timeout is reached."""
        logger.info("Waiting for app to be healthy at %s/health ...", self.app_url)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = requests.get(f"{self.app_url}/health", timeout=3)
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
        """GET / -- expect 200 with 'Kafka Log Compaction' in body."""
        print("\n[Dashboard HTML]")
        try:
            r = requests.get(f"{self.app_url}/", timeout=5)
            self.check("Dashboard returns 200", r.status_code == 200)
            self.check(
                "Dashboard contains expected title",
                "Kafka Log Compaction" in r.text or "kafka" in r.text.lower(),
            )
        except Exception as e:
            self.check("Dashboard reachable", False, str(e))

    def test_stats_api(self) -> None:
        """GET /api/stats -- expect 200 with required keys."""
        print("\n[Stats API]")
        try:
            r = requests.get(f"{self.app_url}/api/stats", timeout=5)
            self.check("Stats API returns 200", r.status_code == 200)
            data = r.json()
            required_keys = [
                "active_profiles",
                "total_consumed",
                "compaction_metrics",
                "tombstones_processed",
                "updates_by_type",
            ]
            for key in required_keys:
                self.check(
                    f"Stats has '{key}'",
                    key in data,
                    f"value={data.get(key)}" if key in data else "missing",
                )
        except Exception as e:
            self.check("Stats API reachable", False, str(e))

    def test_profiles_api(self) -> None:
        """GET /api/profiles -- expect 200 with a list of profiles."""
        print("\n[Profiles API]")
        try:
            r = requests.get(f"{self.app_url}/api/profiles", timeout=5)
            self.check("Profiles API returns 200", r.status_code == 200)
            data = r.json()
            self.check("Profiles API returns a list", isinstance(data, list))
            self.check(
                "Profiles list is non-empty",
                len(data) > 0,
                f"count={len(data)}",
            )
            if data:
                profile = data[0]
                for field in ("user_id", "email", "first_name", "last_name"):
                    self.check(
                        f"Profile has '{field}'",
                        field in profile,
                    )
        except Exception as e:
            self.check("Profiles API reachable", False, str(e))

    def test_topic_config(self) -> None:
        """Verify the topic has cleanup.policy=compact via AdminClient."""
        print("\n[Topic Config]")
        try:
            admin = AdminClient({"bootstrap.servers": self.kafka_bootstrap})
            resource = ConfigResource("TOPIC", "user-profiles")
            futures = admin.describe_configs([resource])
            for _res, future in futures.items():
                configs = future.result()
                policy = configs.get("cleanup.policy")
                if policy:
                    self.check(
                        "cleanup.policy is 'compact'",
                        policy.value == "compact",
                        f"cleanup.policy={policy.value}",
                    )
                else:
                    self.check("cleanup.policy found", False, "key missing from config")
        except Exception as e:
            self.check("Topic config check", False, str(e))

    def test_data_flowing(self) -> None:
        """GET /api/stats twice with a gap -- verify total_consumed increases."""
        print("\n[Data Flowing]")
        try:
            r1 = requests.get(f"{self.app_url}/api/stats", timeout=5)
            count1 = r1.json().get("total_consumed", 0)

            time.sleep(5)

            r2 = requests.get(f"{self.app_url}/api/stats", timeout=5)
            count2 = r2.json().get("total_consumed", 0)

            self.check(
                "total_consumed increased",
                count2 > count1,
                f"before={count1}, after={count2}",
            )
        except Exception as e:
            self.check("Data flowing check", False, str(e))

    def test_tombstone_handling(self) -> None:
        """Verify tombstones are being processed (producer does ~10% deletes).

        First checks if any tombstones were already processed. If not,
        produces a tombstone directly and waits for it to be consumed.
        """
        print("\n[Tombstone Handling]")
        try:
            r = requests.get(f"{self.app_url}/api/stats", timeout=5)
            tombstones = r.json().get("tombstones_processed", 0)

            if tombstones > 0:
                self.check(
                    "Tombstones already processed",
                    True,
                    f"tombstones_processed={tombstones}",
                )
                return

            # Produce a tombstone directly
            logger.info("No tombstones yet -- producing one directly.")
            producer = Producer({"bootstrap.servers": self.kafka_bootstrap})
            producer.produce(
                topic="user-profiles",
                key=b"profile:test_tombstone_user",
                value=None,
            )
            producer.flush(timeout=10)

            # Wait for consumer to pick it up
            time.sleep(5)

            r2 = requests.get(f"{self.app_url}/api/stats", timeout=5)
            tombstones2 = r2.json().get("tombstones_processed", 0)
            self.check(
                "Tombstone processed after direct produce",
                tombstones2 > tombstones,
                f"before={tombstones}, after={tombstones2}",
            )
        except Exception as e:
            self.check("Tombstone handling check", False, str(e))

    def test_compaction_metrics(self) -> None:
        """Verify compaction_metrics has reasonable values."""
        print("\n[Compaction Metrics]")
        try:
            r = requests.get(f"{self.app_url}/api/stats", timeout=5)
            data = r.json()
            cm = data.get("compaction_metrics", {})

            total = cm.get("total_messages", 0)
            unique = cm.get("unique_keys", 0)
            ratio = cm.get("compaction_ratio", 0.0)

            self.check(
                "total_messages > 0",
                total > 0,
                f"total_messages={total}",
            )
            self.check(
                "unique_keys > 0",
                unique > 0,
                f"unique_keys={unique}",
            )
            self.check(
                "compaction_ratio > 0 and < 1",
                0 < ratio < 1,
                f"compaction_ratio={ratio}",
            )
        except Exception as e:
            self.check("Compaction metrics check", False, str(e))

    # ------------------------------------------------------------------
    # Runner
    # ------------------------------------------------------------------

    def run_all(self) -> bool:
        print("=" * 60)
        print("Kafka Log Compaction State Manager -- E2E Verification")
        print(f"App URL: {self.app_url}")
        print(f"Kafka:   {self.kafka_bootstrap}")
        print("=" * 60)

        self.test_health_endpoint()
        self.test_dashboard_html()
        self.test_stats_api()
        self.test_profiles_api()
        self.test_topic_config()
        self.test_data_flowing()
        self.test_tombstone_handling()
        self.test_compaction_metrics()

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

    if not verifier.wait_for_app(timeout=60):
        print("FATAL: App never became healthy.")
        sys.exit(1)

    # Wait for the producer to generate data and the consumer to process it
    logger.info("Waiting 30s for data to accumulate ...")
    time.sleep(30)

    success = verifier.run_all()
    sys.exit(0 if success else 1)
