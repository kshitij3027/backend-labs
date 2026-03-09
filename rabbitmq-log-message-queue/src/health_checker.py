"""Health checker for RabbitMQ connectivity, management API, and queue status."""

import click
import requests
from rich.console import Console

from src.config import Config
from src.connection import RabbitMQConnection

console = Console()


class HealthChecker:
    """Runs health checks against RabbitMQ connection, management API, and queues."""

    def __init__(self, config=None):
        self._config = config or Config()

    def check_connection(self):
        """Check that a pika connection to RabbitMQ can be established.

        Returns:
            dict with 'status' ('healthy'/'unhealthy') and 'message'.
        """
        conn = RabbitMQConnection(self._config)
        try:
            conn.connect()
            return {"status": "healthy", "message": "Connection successful"}
        except Exception as exc:
            return {"status": "unhealthy", "message": f"Connection failed: {exc}"}
        finally:
            conn.close()

    def check_management_api(self):
        """Check that the RabbitMQ Management API is reachable.

        Returns:
            dict with 'status' ('healthy'/'unhealthy') and 'message'.
        """
        host = self._config.host
        mgmt_port = self._config.management_port
        username = self._config.username
        password = self._config.password

        url = f"http://{host}:{mgmt_port}/api/overview"
        try:
            resp = requests.get(url, auth=(username, password), timeout=5)
            resp.raise_for_status()
            data = resp.json()
            version = data.get("rabbitmq_version", "unknown")
            return {
                "status": "healthy",
                "message": f"Running (Version: {version})",
            }
        except Exception as exc:
            return {
                "status": "unhealthy",
                "message": f"Management API unreachable: {exc}",
            }

    def check_queues(self):
        """Check that every configured queue exists and is operational.

        Returns:
            dict with 'status' ('healthy'/'unhealthy') and 'message'.
        """
        host = self._config.host
        mgmt_port = self._config.management_port
        username = self._config.username
        password = self._config.password
        queues = self._config.get_queue_configs()

        missing = []
        for q in queues:
            url = f"http://{host}:{mgmt_port}/api/queues/%2f/{q['name']}"
            try:
                resp = requests.get(url, auth=(username, password), timeout=5)
                if resp.status_code != 200:
                    missing.append(q["name"])
            except requests.RequestException:
                missing.append(q["name"])

        if missing:
            return {
                "status": "unhealthy",
                "message": f"Missing queues: {', '.join(missing)}",
            }
        return {"status": "healthy", "message": "All queues operational"}

    def run_health_check(self):
        """Run all health checks and compute an overall status.

        Returns:
            dict with 'overall', 'connection', 'management_api', and 'queues' keys.
        """
        connection = self.check_connection()
        management_api = self.check_management_api()
        queues = self.check_queues()

        all_healthy = all(
            c["status"] == "healthy"
            for c in [connection, management_api, queues]
        )

        return {
            "overall": "healthy" if all_healthy else "unhealthy",
            "connection": connection,
            "management_api": management_api,
            "queues": queues,
        }

    def display_report(self, report):
        """Print a formatted health report to the console."""
        console.print()
        console.print("[bold]\U0001f3e5 RabbitMQ Health Report[/bold]")

        overall = report["overall"]
        if overall == "healthy":
            console.print(f"Overall Status: [green]\u2705 HEALTHY[/green]")
        else:
            console.print(f"Overall Status: [red]\u274c UNHEALTHY[/red]")

        for key in ("connection", "management_api", "queues"):
            check = report[key]
            label = key.replace("_", " ").title()
            if check["status"] == "healthy":
                console.print(f"{label}: [green]\u2705 {check['message']}[/green]")
            else:
                console.print(f"{label}: [red]\u274c {check['message']}[/red]")

        console.print()


@click.command()
def main():
    """Run a health check against RabbitMQ."""
    checker = HealthChecker()
    report = checker.run_health_check()
    checker.display_report(report)


if __name__ == "__main__":
    main()
