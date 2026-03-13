"""End-to-end verification for RabbitMQ exchange and queue topology."""

import os
import sys
import time

import requests
from colorama import Fore, Style, init

from scripts.wait_for_rabbitmq import wait_for_rabbitmq
from src.config import Config
from src.models.log_message import LogMessage
from src.producer import LogProducer
from src.setup import RabbitMQSetup

init(autoreset=True)


class E2EVerifier:
    """Verifies that exchanges, queues, and bindings are correctly set up in RabbitMQ."""

    def __init__(self):
        self._config = Config()
        host = os.environ.get("RABBITMQ_HOST", "localhost")
        port = self._config.management_port
        self._base_url = f"http://{host}:{port}/api"
        self._auth = (self._config.username, self._config.password)
        self._passed = 0
        self._failed = 0

    def check(self, name, condition, detail=""):
        """Record and print a PASS/FAIL check result.

        Args:
            name: Short description of the check.
            condition: Boolean indicating pass/fail.
            detail: Optional extra detail to display on failure.
        """
        if condition:
            self._passed += 1
            print(f"  {Fore.GREEN}PASS{Style.RESET_ALL} {name}")
        else:
            self._failed += 1
            msg = f"  {Fore.RED}FAIL{Style.RESET_ALL} {name}"
            if detail:
                msg += f" — {detail}"
            print(msg)

    def run(self):
        """Execute full E2E verification.

        1. Run topology setup.
        2. Verify exchanges exist with correct types.
        3. Verify queues exist.
        4. Verify queue-to-exchange bindings.

        Returns:
            True if all checks passed, False otherwise.
        """
        # Step 1: Run setup
        print(f"\n{Fore.CYAN}=== Running topology setup ==={Style.RESET_ALL}")
        setup = RabbitMQSetup(config=self._config)
        setup.setup_all()

        # Step 2: Verify exchanges
        print(f"\n{Fore.CYAN}=== Verifying exchanges ==={Style.RESET_ALL}")
        expected_exchanges = {
            ex["name"]: ex["type"] for ex in self._config.get_exchange_configs()
        }
        for name, expected_type in expected_exchanges.items():
            url = f"{self._base_url}/exchanges/%2f/{name}"
            try:
                resp = requests.get(url, auth=self._auth, timeout=10)
                self.check(
                    f"Exchange '{name}' exists",
                    resp.status_code == 200,
                    detail=f"status={resp.status_code}",
                )
                if resp.status_code == 200:
                    data = resp.json()
                    self.check(
                        f"Exchange '{name}' type is '{expected_type}'",
                        data.get("type") == expected_type,
                        detail=f"got type='{data.get('type')}'",
                    )
            except requests.exceptions.RequestException as e:
                self.check(f"Exchange '{name}' exists", False, detail=str(e))

        # Step 3: Verify queues
        print(f"\n{Fore.CYAN}=== Verifying queues ==={Style.RESET_ALL}")
        queue_configs = self._config.get_queue_configs()
        for queue in queue_configs:
            queue_name = queue["name"]
            url = f"{self._base_url}/queues/%2f/{queue_name}"
            try:
                resp = requests.get(url, auth=self._auth, timeout=10)
                self.check(
                    f"Queue '{queue_name}' exists",
                    resp.status_code == 200,
                    detail=f"status={resp.status_code}",
                )
            except requests.exceptions.RequestException as e:
                self.check(f"Queue '{queue_name}' exists", False, detail=str(e))

        # Step 4: Verify bindings
        print(f"\n{Fore.CYAN}=== Verifying bindings ==={Style.RESET_ALL}")
        for queue in queue_configs:
            queue_name = queue["name"]
            expected_exchange = queue["exchange"]
            url = f"{self._base_url}/queues/%2f/{queue_name}/bindings"
            try:
                resp = requests.get(url, auth=self._auth, timeout=10)
                if resp.status_code == 200:
                    bindings = resp.json()
                    bound_exchanges = [
                        b["source"] for b in bindings if b["source"] != ""
                    ]
                    self.check(
                        f"Queue '{queue_name}' bound to '{expected_exchange}'",
                        expected_exchange in bound_exchanges,
                        detail=f"bound to: {bound_exchanges}",
                    )
                else:
                    self.check(
                        f"Queue '{queue_name}' bindings accessible",
                        False,
                        detail=f"status={resp.status_code}",
                    )
            except requests.exceptions.RequestException as e:
                self.check(
                    f"Queue '{queue_name}' bound to '{expected_exchange}'",
                    False,
                    detail=str(e),
                )

        # Step 5: Verify message routing
        print(f"\n{Fore.CYAN}=== Verifying message routing ==={Style.RESET_ALL}")

        # Purge all queues first
        all_queue_names = [q["name"] for q in queue_configs]
        for queue_name in all_queue_names:
            url = f"{self._base_url}/queues/%2f/{queue_name}/contents"
            try:
                requests.delete(url, auth=self._auth, timeout=10)
            except requests.exceptions.RequestException:
                pass  # Queue may be empty already

        # Create a test message and publish to all exchanges
        test_message = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="database",
            component="postgres",
            level="error",
            message="Test connection refused to backend service",
            metadata={"source_ip": "192.168.1.100", "request_id": "e2e-test-001"},
        )

        producer = LogProducer(config=self._config)
        try:
            producer.connect()
            producer.publish_to_all(test_message)
        except Exception as e:
            self.check("Producer publish_to_all", False, detail=str(e))
        finally:
            pass  # Keep producer open for now

        # Wait for messages to be routed and stats to update (poll with retries)
        expected_counts = {
            "error_logs": 1,      # direct routing by level "error"
            "database_logs": 1,   # topic routing "database.#"
            "audit_logs": 1,      # fanout
            "all_logs": 1,        # fanout
        }

        # Poll until management API stats reflect the published messages
        for attempt in range(15):
            time.sleep(1)
            all_ready = True
            for queue_name, expected_count in expected_counts.items():
                url = f"{self._base_url}/queues/%2f/{queue_name}"
                try:
                    resp = requests.get(url, auth=self._auth, timeout=10)
                    if resp.status_code == 200:
                        if resp.json().get("messages", 0) < expected_count:
                            all_ready = False
                            break
                except requests.exceptions.RequestException:
                    all_ready = False
                    break
            if all_ready:
                break

        # Verify message counts in expected queues
        for queue_name, expected_count in expected_counts.items():
            url = f"{self._base_url}/queues/%2f/{queue_name}"
            try:
                resp = requests.get(url, auth=self._auth, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    actual_count = data.get("messages", 0)
                    self.check(
                        f"Queue '{queue_name}' has {expected_count} message(s)",
                        actual_count >= expected_count,
                        detail=f"expected={expected_count}, got={actual_count}",
                    )
                else:
                    self.check(
                        f"Queue '{queue_name}' accessible",
                        False,
                        detail=f"status={resp.status_code}",
                    )
            except requests.exceptions.RequestException as e:
                self.check(
                    f"Queue '{queue_name}' has {expected_count} message(s)",
                    False,
                    detail=str(e),
                )

        # Verify message structure by getting a message from error_logs
        print(f"\n{Fore.CYAN}=== Verifying message structure ==={Style.RESET_ALL}")
        get_url = f"{self._base_url}/queues/%2f/error_logs/get"
        get_body = {"count": 1, "ackmode": "ack_requeue_true", "encoding": "auto"}
        try:
            resp = requests.post(
                get_url, json=get_body, auth=self._auth, timeout=10
            )
            if resp.status_code == 200:
                messages = resp.json()
                self.check(
                    "GET message from error_logs returned data",
                    len(messages) > 0,
                )
                if messages:
                    import json
                    payload = json.loads(messages[0].get("payload", "{}"))
                    required_fields = [
                        "timestamp", "service", "component",
                        "level", "routing_key", "metadata",
                    ]
                    for field_name in required_fields:
                        self.check(
                            f"Message has field '{field_name}'",
                            field_name in payload,
                            detail=f"payload keys: {list(payload.keys())}",
                        )
            else:
                self.check(
                    "GET message from error_logs",
                    False,
                    detail=f"status={resp.status_code}",
                )
        except requests.exceptions.RequestException as e:
            self.check(
                "GET message from error_logs",
                False,
                detail=str(e),
            )

        # Close producer connection
        producer.close()

        # Summary
        total = self._passed + self._failed
        print(f"\n{Fore.CYAN}=== Summary ==={Style.RESET_ALL}")
        print(f"  Passed: {self._passed}/{total}")
        if self._failed:
            print(f"  {Fore.RED}Failed: {self._failed}/{total}{Style.RESET_ALL}")
        else:
            print(f"  {Fore.GREEN}All checks passed!{Style.RESET_ALL}")

        return self._failed == 0


if __name__ == "__main__":
    if not wait_for_rabbitmq():
        print("RabbitMQ is not available. Aborting E2E verification.")
        sys.exit(1)

    verifier = E2EVerifier()
    success = verifier.run()
    sys.exit(0 if success else 1)
