"""Simulation runners for sending sample log entries."""

import random

from src.client import TLSLogClient
from src.models import create_log_entry
from src.anonymizer import create_anonymized_healthcare_entry


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


def run_healthcare_simulation(client: TLSLogClient):
    """Send 50 anonymized healthcare log entries."""
    departments = ["ER", "ICU", "Cardiology", "Radiology", "Oncology"]
    event_types = ["admission", "discharge", "lab_result", "medication", "vitals"]
    messages = [
        "Patient admitted for observation",
        "Lab results received — CBC normal",
        "Medication administered: Amoxicillin 500mg",
        "Vitals recorded: BP 120/80, HR 72",
        "Patient discharged with follow-up in 7 days",
        "X-ray completed — no abnormalities",
        "Blood glucose level: 95 mg/dL",
        "ECG performed — normal sinus rhythm",
        "Patient transferred to ICU",
        "Prescription renewed: Metformin 1000mg",
    ]

    total = 50
    transmitted = 0

    for i in range(1, total + 1):
        patient_id = f"P{random.randint(10000, 99999)}"
        entry = create_anonymized_healthcare_entry(
            patient_id=patient_id,
            event_type=random.choice(event_types),
            department=random.choice(departments),
            message=random.choice(messages),
        )
        client.send_log(entry)
        transmitted += 1

        if i % 10 == 0 or i == total:
            print(f"[CLIENT] Healthcare: {i}/{total} entries sent")

    print(f"\n[CLIENT] Healthcare simulation complete: {transmitted}/{total} transmitted")
    return transmitted
