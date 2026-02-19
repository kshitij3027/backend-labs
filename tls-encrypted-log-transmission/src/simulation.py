"""Simulation runners for sending sample log entries."""

from src.client import TLSLogClient
from src.models import create_log_entry


def run_simulation(client: TLSLogClient):
    """Send 5 sample log entries of mixed severity."""
    logs = [
        create_log_entry("INFO", "Application started successfully"),
        create_log_entry("INFO", "Connected to database on port 5432"),
        create_log_entry("WARNING", "High memory usage detected: 85%"),
        create_log_entry("ERROR", "Failed to process request: timeout after 30s"),
        create_log_entry("INFO", "Scheduled backup completed"),
    ]

    transmitted = 0
    for i, entry in enumerate(logs, 1):
        client.send_log(entry)
        transmitted += 1
        print(f"[CLIENT] Sent log {i}/{len(logs)}: [{entry['level']}] {entry['message']}")

    print(f"\n[CLIENT] Simulation complete: {transmitted}/{len(logs)} transmitted")
    return transmitted
