"""Comprehensive end-to-end verification for the RabbitMQ log message queue."""

import json
import os
import sys
import time

import requests

# Add project root to path so we can import src modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import Config
from src.health_checker import HealthChecker
from src.publisher import LogPublisher
from src.queue_manager import QueueManager
from src.setup import RabbitMQSetup


class E2EVerifier:
    """Runs a full suite of end-to-end checks against a live RabbitMQ instance."""

    def __init__(self):
        self.config = Config()
        self.passed = 0
        self.failed = 0
        self.host = os.environ.get("RABBITMQ_HOST", "localhost")
        self.mgmt_url = f"http://{self.host}:15672/api"
        self.auth = ("guest", "guest")

    def check(self, name, condition, detail=""):
        if condition:
            print(f"  PASS: {name}")
            self.passed += 1
        else:
            print(f"  FAIL: {name}: {detail}")
            self.failed += 1

    def run(self):
        print("\n--- RabbitMQ E2E Verification ---\n")

        # 1. Setup topology
        print("Step 1: Setup topology")
        setup = RabbitMQSetup(self.config)
        setup.setup_all()
        time.sleep(1)

        # 2. Verify exchange exists
        print("\nStep 2: Verify exchanges")
        resp = requests.get(
            f"{self.mgmt_url}/exchanges/%2f/logs", auth=self.auth, timeout=5
        )
        self.check("Exchange 'logs' exists", resp.status_code == 200)
        if resp.status_code == 200:
            data = resp.json()
            self.check("Exchange type is 'topic'", data.get("type") == "topic")
            self.check("Exchange is durable", data.get("durable") is True)

        # Check DLX
        resp = requests.get(
            f"{self.mgmt_url}/exchanges/%2f/logs_dlx", auth=self.auth, timeout=5
        )
        self.check("DLX 'logs_dlx' exists", resp.status_code == 200)

        # 3. Verify queues exist
        print("\nStep 3: Verify queues")
        for qname in [
            "log_messages",
            "error_messages",
            "debug_messages",
            "dead_letter_queue",
        ]:
            resp = requests.get(
                f"{self.mgmt_url}/queues/%2f/{qname}", auth=self.auth, timeout=5
            )
            self.check(f"Queue '{qname}' exists", resp.status_code == 200)

        # 4. Purge queues before publish test
        for qname in ["log_messages", "error_messages", "debug_messages"]:
            requests.delete(
                f"{self.mgmt_url}/queues/%2f/{qname}/contents",
                auth=self.auth,
                timeout=5,
            )
        time.sleep(0.5)

        # 5. Publish and verify routing
        print("\nStep 4: Publish and verify routing")
        publisher = LogPublisher(self.config)
        publisher.publish("info", "web", "Test info message")
        publisher.publish("error", "db", "Test error message")
        publisher.publish("debug", "auth", "Test debug message")

        # Poll the management API until counts update (stats are async)
        expected = {"log_messages": 1, "error_messages": 1, "debug_messages": 1}
        for _ in range(10):
            time.sleep(1)
            all_ready = True
            for qname, count in expected.items():
                resp = requests.get(
                    f"{self.mgmt_url}/queues/%2f/{qname}",
                    auth=self.auth,
                    timeout=5,
                )
                if resp.status_code != 200 or resp.json().get("messages", 0) != count:
                    all_ready = False
                    break
            if all_ready:
                break

        for qname, count in expected.items():
            resp = requests.get(
                f"{self.mgmt_url}/queues/%2f/{qname}", auth=self.auth, timeout=5
            )
            if resp.status_code == 200:
                msgs = resp.json().get("messages", 0)
                self.check(
                    f"Queue '{qname}' has {count} message(s)",
                    msgs == count,
                    f"got {msgs}",
                )
            else:
                self.check(
                    f"Queue '{qname}' accessible",
                    False,
                    f"HTTP {resp.status_code}",
                )

        # 6. Consume and verify message content
        print("\nStep 5: Consume and verify content")
        for qname in ["log_messages", "error_messages", "debug_messages"]:
            resp = requests.post(
                f"{self.mgmt_url}/queues/%2f/{qname}/get",
                auth=self.auth,
                json={
                    "count": 1,
                    "ackmode": "ack_requeue_false",
                    "encoding": "auto",
                },
                timeout=5,
            )
            if resp.status_code == 200 and resp.json():
                msg = json.loads(resp.json()[0]["payload"])
                self.check(
                    f"Message from '{qname}' has valid structure",
                    all(
                        k in msg
                        for k in ["timestamp", "level", "source", "message"]
                    ),
                )
            else:
                self.check(f"Consumed from '{qname}'", False, "No messages")

        # 7. Health check
        print("\nStep 6: Health check")
        checker = HealthChecker(self.config)
        report = checker.run_health_check()
        self.check("Overall health: HEALTHY", report["overall"] == "healthy")
        self.check(
            "Connection check passed",
            report["connection"]["status"] == "healthy",
        )
        self.check(
            "Management API check passed",
            report["management_api"]["status"] == "healthy",
        )
        self.check(
            "Queues check passed",
            report["queues"]["status"] == "healthy",
        )

        # 8. Queue stats
        print("\nStep 7: Queue stats")
        manager = QueueManager(self.config)
        stats = manager.get_queue_stats()
        self.check(
            "Queue stats returned",
            len(stats) > 0,
            f"got {len(stats)} stats",
        )

        # Summary
        total = self.passed + self.failed
        print(f"\n{'=' * 50}")
        print(f"Results: {self.passed}/{total} checks passed")
        if self.failed == 0:
            print("All E2E checks passed!")
        else:
            print(f"{self.failed} check(s) failed")
        print(f"{'=' * 50}\n")

        return self.failed == 0


if __name__ == "__main__":
    from scripts.wait_for_rabbitmq import wait_for_rabbitmq

    if not wait_for_rabbitmq():
        sys.exit(1)

    verifier = E2EVerifier()
    success = verifier.run()
    sys.exit(0 if success else 1)
