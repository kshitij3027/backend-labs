"""Comprehensive end-to-end verification for the smart log routing pipeline."""

import json
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
    """Runs 15+ checks to verify topology, routing, dashboard, and message flow."""

    def __init__(self):
        self._config = Config()
        host = os.environ.get("RABBITMQ_HOST", "localhost")
        port = self._config.management_port
        self._base_url = f"http://{host}:{port}/api"
        self._auth = (self._config.username, self._config.password)
        self._passed = 0
        self._failed = 0
        self._check_num = 0

    def check(self, name, condition, detail=""):
        """Record and print a numbered PASS/FAIL check result.

        Args:
            name: Short description of the check.
            condition: Boolean indicating pass/fail.
            detail: Optional extra detail to display on failure.
        """
        self._check_num += 1
        if condition:
            self._passed += 1
            print(
                f"  {Fore.GREEN}[{self._check_num:>2}] PASS{Style.RESET_ALL} {name}"
            )
        else:
            self._failed += 1
            msg = (
                f"  {Fore.RED}[{self._check_num:>2}] FAIL{Style.RESET_ALL} {name}"
            )
            if detail:
                msg += f" -- {detail}"
            print(msg)

    def _purge_all_queues(self):
        """Delete all messages from every configured queue."""
        for queue in self._config.get_queue_configs():
            url = f"{self._base_url}/queues/%2f/{queue['name']}/contents"
            try:
                requests.delete(url, auth=self._auth, timeout=10)
            except requests.exceptions.RequestException:
                pass

    def _get_queue_message_count(self, queue_name):
        """Return the number of messages in a queue, or -1 on error."""
        url = f"{self._base_url}/queues/%2f/{queue_name}"
        try:
            resp = requests.get(url, auth=self._auth, timeout=10)
            if resp.status_code == 200:
                return resp.json().get("messages", 0)
        except requests.exceptions.RequestException:
            pass
        return -1

    def _wait_for_messages(self, expected_counts, timeout=15):
        """Poll until all queues have at least the expected message count."""
        for _ in range(timeout):
            time.sleep(1)
            all_ready = True
            for queue_name, expected in expected_counts.items():
                actual = self._get_queue_message_count(queue_name)
                if actual < expected:
                    all_ready = False
                    break
            if all_ready:
                return True
        return False

    def run(self):
        """Execute all E2E verification checks.

        Returns:
            True if all checks passed, False otherwise.
        """
        # ── Phase 1: Topology Setup ─────────────────────────────────────
        print(f"\n{Fore.CYAN}=== Phase 1: Topology Setup ==={Style.RESET_ALL}")
        setup = RabbitMQSetup(config=self._config)
        setup.setup_all()

        # ── Phase 2: Exchange Verification ──────────────────────────────
        print(f"\n{Fore.CYAN}=== Phase 2: Exchange Verification ==={Style.RESET_ALL}")
        expected_exchanges = {
            ex["name"]: ex["type"] for ex in self._config.get_exchange_configs()
        }
        # Check 1: All 3 exchanges exist
        all_exchanges_exist = True
        for name, expected_type in expected_exchanges.items():
            url = f"{self._base_url}/exchanges/%2f/{name}"
            try:
                resp = requests.get(url, auth=self._auth, timeout=10)
                exists = resp.status_code == 200
                if not exists:
                    all_exchanges_exist = False
            except requests.exceptions.RequestException:
                all_exchanges_exist = False

        self.check(
            f"All {len(expected_exchanges)} exchanges exist",
            all_exchanges_exist,
        )

        # Check 2: Exchange types are correct
        types_correct = True
        for name, expected_type in expected_exchanges.items():
            url = f"{self._base_url}/exchanges/%2f/{name}"
            try:
                resp = requests.get(url, auth=self._auth, timeout=10)
                if resp.status_code == 200:
                    actual_type = resp.json().get("type", "")
                    if actual_type != expected_type:
                        types_correct = False
                else:
                    types_correct = False
            except requests.exceptions.RequestException:
                types_correct = False

        self.check(
            "Exchange types correct (direct, topic, fanout)",
            types_correct,
        )

        # ── Phase 3: Queue Verification ─────────────────────────────────
        print(f"\n{Fore.CYAN}=== Phase 3: Queue Verification ==={Style.RESET_ALL}")
        queue_configs = self._config.get_queue_configs()

        # Check 3: All 8 queues exist
        all_queues_exist = True
        for queue in queue_configs:
            url = f"{self._base_url}/queues/%2f/{queue['name']}"
            try:
                resp = requests.get(url, auth=self._auth, timeout=10)
                if resp.status_code != 200:
                    all_queues_exist = False
            except requests.exceptions.RequestException:
                all_queues_exist = False

        self.check(
            f"All {len(queue_configs)} queues exist",
            all_queues_exist,
        )

        # Check 4: Queue bindings are correct
        bindings_correct = True
        binding_details = []
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
                    if expected_exchange not in bound_exchanges:
                        bindings_correct = False
                        binding_details.append(
                            f"{queue_name} not bound to {expected_exchange}"
                        )
                else:
                    bindings_correct = False
            except requests.exceptions.RequestException:
                bindings_correct = False

        self.check(
            "All queue bindings correct",
            bindings_correct,
            detail="; ".join(binding_details) if binding_details else "",
        )

        # ── Phase 4: Direct Exchange Routing ────────────────────────────
        print(
            f"\n{Fore.CYAN}=== Phase 4: Direct Exchange Routing ==={Style.RESET_ALL}"
        )
        self._purge_all_queues()

        # Create producer
        producer = LogProducer(config=self._config)
        producer.connect()

        # Check 5: Publish error message to direct exchange -> error_logs queue
        error_msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="database",
            component="postgres",
            level="error",
            message="Test error for direct routing",
            metadata={"request_id": "e2e-direct-error"},
        )
        producer.publish_to_direct(error_msg)

        # Check 6: Publish warning message to direct exchange -> warning_logs queue
        warning_msg = LogMessage(
            timestamp="2026-03-12T10:00:01+00:00",
            service="api",
            component="gateway",
            level="warning",
            message="Test warning for direct routing",
            metadata={"request_id": "e2e-direct-warning"},
        )
        producer.publish_to_direct(warning_msg)

        # Check 7: Publish critical message to direct exchange -> critical_logs queue
        critical_msg = LogMessage(
            timestamp="2026-03-12T10:00:02+00:00",
            service="security",
            component="firewall",
            level="critical",
            message="Test critical for direct routing",
            metadata={"request_id": "e2e-direct-critical"},
        )
        producer.publish_to_direct(critical_msg)

        self._wait_for_messages(
            {"error_logs": 1, "warning_logs": 1, "critical_logs": 1}
        )

        self.check(
            "Direct: error message routed to error_logs",
            self._get_queue_message_count("error_logs") >= 1,
            detail=f"count={self._get_queue_message_count('error_logs')}",
        )

        self.check(
            "Direct: warning message routed to warning_logs",
            self._get_queue_message_count("warning_logs") >= 1,
            detail=f"count={self._get_queue_message_count('warning_logs')}",
        )

        self.check(
            "Direct: critical message routed to critical_logs",
            self._get_queue_message_count("critical_logs") >= 1,
            detail=f"count={self._get_queue_message_count('critical_logs')}",
        )

        # ── Phase 5: Topic Exchange Routing ─────────────────────────────
        print(
            f"\n{Fore.CYAN}=== Phase 5: Topic Exchange Routing ==={Style.RESET_ALL}"
        )
        self._purge_all_queues()

        # Check 8: database.*.* messages route to database_logs (via database.#)
        db_msg = LogMessage(
            timestamp="2026-03-12T10:01:00+00:00",
            service="database",
            component="redis",
            level="info",
            message="Test topic routing for database",
            metadata={"request_id": "e2e-topic-db"},
        )
        producer.publish_to_topic(db_msg)

        # Check 9: security.*.* messages route to security_logs (via security.#)
        sec_msg = LogMessage(
            timestamp="2026-03-12T10:01:01+00:00",
            service="security",
            component="ids",
            level="warning",
            message="Test topic routing for security",
            metadata={"request_id": "e2e-topic-sec"},
        )
        producer.publish_to_topic(sec_msg)

        self._wait_for_messages({"database_logs": 1, "security_logs": 1})

        self.check(
            "Topic: database message routed to database_logs",
            self._get_queue_message_count("database_logs") >= 1,
            detail=f"count={self._get_queue_message_count('database_logs')}",
        )

        self.check(
            "Topic: security message routed to security_logs",
            self._get_queue_message_count("security_logs") >= 1,
            detail=f"count={self._get_queue_message_count('security_logs')}",
        )

        # ── Phase 6: Fanout Exchange Routing ────────────────────────────
        print(
            f"\n{Fore.CYAN}=== Phase 6: Fanout Exchange Routing ==={Style.RESET_ALL}"
        )
        self._purge_all_queues()

        # Check 10: Fanout messages go to both audit_logs and all_logs
        fanout_msg = LogMessage(
            timestamp="2026-03-12T10:02:00+00:00",
            service="payment",
            component="processor",
            level="info",
            message="Test fanout routing",
            metadata={"request_id": "e2e-fanout"},
        )
        producer.publish_to_fanout(fanout_msg)

        self._wait_for_messages({"audit_logs": 1, "all_logs": 1})

        self.check(
            "Fanout: message delivered to audit_logs",
            self._get_queue_message_count("audit_logs") >= 1,
            detail=f"count={self._get_queue_message_count('audit_logs')}",
        )

        self.check(
            "Fanout: message delivered to all_logs",
            self._get_queue_message_count("all_logs") >= 1,
            detail=f"count={self._get_queue_message_count('all_logs')}",
        )

        # ── Phase 7: Message Structure Verification ─────────────────────
        print(
            f"\n{Fore.CYAN}=== Phase 7: Message Structure ==={Style.RESET_ALL}"
        )
        self._purge_all_queues()

        struct_msg = LogMessage(
            timestamp="2026-03-12T10:03:00+00:00",
            service="database",
            component="postgres",
            level="error",
            message="Structure verification test",
            metadata={"request_id": "e2e-struct", "source_ip": "10.0.0.1"},
        )
        producer.publish_to_direct(struct_msg)

        self._wait_for_messages({"error_logs": 1})

        # Check 12: Message has all required fields
        get_url = f"{self._base_url}/queues/%2f/error_logs/get"
        get_body = {"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"}
        required_fields = [
            "timestamp", "service", "component", "level", "message",
            "routing_key", "metadata",
        ]
        try:
            resp = requests.post(
                get_url, json=get_body, auth=self._auth, timeout=10
            )
            if resp.status_code == 200:
                messages = resp.json()
                has_message = len(messages) > 0
                if has_message:
                    payload = json.loads(messages[0].get("payload", "{}"))
                    all_fields = all(f in payload for f in required_fields)
                    self.check(
                        f"Message contains all {len(required_fields)} required fields",
                        all_fields,
                        detail=f"missing: {[f for f in required_fields if f not in payload]}",
                    )
                else:
                    self.check(
                        "Message retrieved from error_logs for structure check",
                        False,
                        detail="no messages returned",
                    )
            else:
                self.check(
                    "Message retrieved from error_logs",
                    False,
                    detail=f"status={resp.status_code}",
                )
        except requests.exceptions.RequestException as e:
            self.check("Message retrieval for structure check", False, detail=str(e))

        # Close producer
        producer.close()

        # ── Phase 8: Dashboard Endpoint Verification ────────────────────
        print(
            f"\n{Fore.CYAN}=== Phase 8: Dashboard Endpoints ==={Style.RESET_ALL}"
        )

        # Use Flask test client to verify endpoints without starting the server
        from src.dashboard.app import app

        with app.test_client() as client:
            # Check 13: Dashboard health endpoint
            resp = client.get("/health")
            self.check(
                "Dashboard /health endpoint returns 200",
                resp.status_code == 200,
                detail=f"status={resp.status_code}",
            )

            # Check 14: Dashboard stats endpoint
            resp = client.get("/api/stats")
            self.check(
                "Dashboard /api/stats endpoint returns 200",
                resp.status_code == 200,
                detail=f"status={resp.status_code}",
            )

            # Check 15: Dashboard stats returns valid JSON list
            if resp.status_code == 200:
                data = resp.get_json()
                self.check(
                    "Dashboard /api/stats returns a JSON list",
                    isinstance(data, list),
                    detail=f"got type={type(data).__name__}",
                )
            else:
                self.check(
                    "Dashboard /api/stats returns a JSON list",
                    False,
                    detail="could not parse response",
                )

        # ── Phase 9: Multi-message Routing Flow ─────────────────────────
        print(
            f"\n{Fore.CYAN}=== Phase 9: Multi-message Routing Flow ==={Style.RESET_ALL}"
        )
        self._purge_all_queues()

        producer2 = LogProducer(config=self._config)
        producer2.connect()

        # Publish messages of different levels to all exchanges
        test_levels = ["error", "warning", "info", "critical"]
        for level in test_levels:
            msg = LogMessage(
                timestamp="2026-03-12T10:04:00+00:00",
                service="database",
                component="postgres",
                level=level,
                message=f"Multi-flow test {level}",
                metadata={"request_id": f"e2e-multi-{level}"},
            )
            producer2.publish_to_all(msg)

        # Wait for messages to settle
        self._wait_for_messages(
            {"audit_logs": 4, "all_logs": 4, "error_logs": 1, "database_logs": 4}
        )

        # Check 16: After publishing 4 messages to all exchanges, fanout queues
        # should each get 4 messages (one per publish_to_fanout call)
        audit_count = self._get_queue_message_count("audit_logs")
        all_count = self._get_queue_message_count("all_logs")
        self.check(
            "Multi-flow: fanout queues received all 4 messages",
            audit_count >= 4 and all_count >= 4,
            detail=f"audit_logs={audit_count}, all_logs={all_count}",
        )

        # Check 17: Direct exchange routed error/warning/critical correctly
        error_count = self._get_queue_message_count("error_logs")
        warning_count = self._get_queue_message_count("warning_logs")
        critical_count = self._get_queue_message_count("critical_logs")
        self.check(
            "Multi-flow: direct exchange routed error/warning/critical",
            error_count >= 1 and warning_count >= 1 and critical_count >= 1,
            detail=f"error={error_count}, warning={warning_count}, critical={critical_count}",
        )

        # Check 18: Topic exchange routed database.* messages
        db_count = self._get_queue_message_count("database_logs")
        self.check(
            "Multi-flow: topic exchange routed database messages",
            db_count >= 4,
            detail=f"database_logs={db_count} (expected >= 4)",
        )

        producer2.close()

        # ── Summary ─────────────────────────────────────────────────────
        total = self._passed + self._failed
        print(f"\n{Fore.CYAN}{'=' * 50}")
        print(f"  E2E Verification Summary")
        print(f"{'=' * 50}{Style.RESET_ALL}")
        print(f"  Checks run: {total}")
        print(f"  Passed:     {Fore.GREEN}{self._passed}{Style.RESET_ALL}")
        if self._failed:
            print(f"  Failed:     {Fore.RED}{self._failed}{Style.RESET_ALL}")
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
