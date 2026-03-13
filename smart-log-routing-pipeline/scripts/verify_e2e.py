"""End-to-end verification for RabbitMQ exchange and queue topology."""

import os
import sys

import requests
from colorama import Fore, Style, init

from scripts.wait_for_rabbitmq import wait_for_rabbitmq
from src.config import Config
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
