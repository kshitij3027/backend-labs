"""Synthetic compliance log event generator (Faker-based).

This module produces realistic-looking ``LogEvent`` rows for SOX,
HIPAA, PCI-DSS, and GDPR scenarios so the reporting pipeline has
something interesting to aggregate without having to bolt onto a real
log source. Every output dict matches the column names of
:class:`src.persistence.models.LogEvent` exactly, so the repository's
bulk-insert path can construct rows via ``LogEvent(**event)``.

Determinism matters here for two reasons:

  1. Unit tests assert identical output for identical seeds (so we
     can compare against fixtures without flakiness).
  2. Demo seeding should be reproducible — re-running the seeder
     should yield the same population so screenshots and walkthroughs
     stay stable.

Both ``Faker.seed`` and ``random.seed`` are pinned per call. The
default window is the last 30 days ending at ``datetime.now(UTC)`` and
all timestamps are tz-aware UTC, matching the project's "every
datetime is aware UTC, always" rule.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Any

from faker import Faker

from ..logging_config import get_logger

logger = get_logger("logs.seeder")


# --- Per-framework event_type allowlists ---
# Each framework's reports only really care about a small set of event
# types. The seeder draws from these so generated rows make sense when
# the aggregator filters them downstream.
FRAMEWORK_EVENT_TYPES: dict[str, list[str]] = {
    "SOX": [
        "admin_login",
        "financial_transaction",
        "system_config_change",
        "approval_workflow",
        "sod_violation",
    ],
    "HIPAA": [
        "phi_access",
        "auth_failure",
        "phi_modification",
        "breach_event",
        "user_audit",
    ],
    "PCI_DSS": [
        "cardholder_data_access",
        "payment_processing",
        "key_rotation",
        "failed_auth_pci",
        "pci_config_change",
    ],
    "GDPR": [
        "personal_data_processing",
        "consent_record",
        "dsr_request",
        "breach_notification",
        "cross_border_transfer",
    ],
}

# --- Per-framework "module" identifiers used in the synthetic resource URI ---
# Mirrors how real compliance log streams identify the upstream service
# that emitted the event (e.g. ``/billing/<uuid>``).
FRAMEWORK_MODULES: dict[str, list[str]] = {
    "SOX": ["finance", "ledger", "billing", "approvals", "treasury"],
    "HIPAA": ["ehr", "patients", "labs", "imaging", "pharmacy"],
    "PCI_DSS": ["payments", "card-vault", "checkout", "tokenizer", "settlement"],
    "GDPR": ["consent", "subjects", "marketing", "support", "exports"],
}

# --- Verbs used in the ``action`` column per event_type ---
# A small, deterministic vocabulary keeps the generated data legible
# while still varying enough to look real.
EVENT_ACTIONS: dict[str, list[str]] = {
    # SOX
    "admin_login": ["login", "logout", "session_extend"],
    "financial_transaction": ["debit", "credit", "post", "reverse"],
    "system_config_change": ["update", "create", "delete"],
    "approval_workflow": ["approve", "reject", "request"],
    "sod_violation": ["override", "self_approve"],
    # HIPAA
    "phi_access": ["read", "view", "export"],
    "auth_failure": ["denied", "lockout", "rejected"],
    "phi_modification": ["update", "delete", "create"],
    "breach_event": ["detected", "reported", "contained"],
    "user_audit": ["snapshot", "review", "flagged"],
    # PCI-DSS
    "cardholder_data_access": ["read", "decrypt", "tokenize"],
    "payment_processing": ["authorize", "capture", "refund", "void"],
    "key_rotation": ["rotate", "retire", "activate"],
    "failed_auth_pci": ["denied", "blocked", "rate_limited"],
    "pci_config_change": ["update", "patch", "rollback"],
    # GDPR
    "personal_data_processing": ["process", "store", "transmit"],
    "consent_record": ["granted", "withdrawn", "renewed"],
    "dsr_request": ["access", "erasure", "portability", "rectification"],
    "breach_notification": ["draft", "submit", "acknowledge"],
    "cross_border_transfer": ["transfer", "review", "approve"],
}

# --- Outcome distribution ---
# Weighted to look like a realistic ops dataset: mostly successes with
# a non-trivial failure / denial tail to keep findings rules interesting.
OUTCOMES: list[str] = ["success", "failure", "denied"]
OUTCOME_WEIGHTS: list[int] = [85, 10, 5]

# --- Sensitivity distribution ---
SENSITIVITIES: list[str] = ["public", "internal", "confidential", "restricted"]
SENSITIVITY_WEIGHTS: list[int] = [20, 40, 30, 10]


def _build_payload(
    fake: Faker,
    rng: random.Random,
    event_type: str,
) -> dict[str, Any]:
    """Build a framework-relevant payload dict for a single event.

    The payload columns are intentionally small but carry the keys that
    a downstream finding rule might want to inspect. They're not
    schema-locked because ``LogEvent.payload`` is a JSON column.
    """
    if event_type == "financial_transaction":
        return {
            "amount_usd": round(rng.uniform(10.0, 250_000.0), 2),
            "currency": "USD",
            "account_id": fake.bothify(text="ACCT-#######"),
        }
    if event_type == "phi_access":
        return {
            "patient_id_hash": fake.sha256()[:32],
            "record_type": rng.choice(["lab_result", "imaging", "prescription", "encounter_note"]),
        }
    if event_type == "phi_modification":
        return {
            "patient_id_hash": fake.sha256()[:32],
            "field": rng.choice(["diagnosis", "medication", "address", "allergy"]),
        }
    if event_type == "cardholder_data_access":
        return {
            "card_token": fake.bothify(text="tok_##########"),
            "merchant_id": fake.bothify(text="MID-######"),
        }
    if event_type == "payment_processing":
        return {
            "amount_usd": round(rng.uniform(1.0, 5_000.0), 2),
            "currency": "USD",
            "merchant_id": fake.bothify(text="MID-######"),
        }
    if event_type == "key_rotation":
        return {
            "key_id": fake.bothify(text="kms-key-########"),
            "key_age_days": rng.randint(1, 365),
        }
    if event_type == "personal_data_processing":
        return {
            "subject_email_hash": fake.sha256()[:32],
            "purpose": rng.choice(["marketing", "analytics", "billing", "support"]),
            "legal_basis": rng.choice(["consent", "contract", "legitimate_interest"]),
        }
    if event_type == "consent_record":
        return {
            "subject_email_hash": fake.sha256()[:32],
            "scope": rng.choice(["all", "marketing", "analytics"]),
        }
    if event_type == "dsr_request":
        return {
            "request_id": fake.uuid4(),
            "kind": rng.choice(["access", "erasure", "portability"]),
        }
    if event_type == "cross_border_transfer":
        return {
            "source_region": rng.choice(["EU", "UK"]),
            "dest_region": rng.choice(["US", "IN", "BR"]),
            "subject_count": rng.randint(1, 5_000),
        }
    if event_type == "breach_event":
        return {
            "severity": rng.choice(["low", "medium", "high", "critical"]),
            "records_affected": rng.randint(1, 5_000),
        }
    if event_type == "breach_notification":
        return {
            "regulator": rng.choice(["ICO", "CNIL", "DPA-IE"]),
            "hours_to_disclosure": rng.randint(1, 96),
        }
    if event_type == "approval_workflow":
        return {
            "workflow_id": fake.uuid4(),
            "approver": fake.email(),
        }
    if event_type == "sod_violation":
        return {
            "violation_type": rng.choice(["self_approval", "cross_role_access"]),
            "control_id": fake.bothify(text="CTRL-####"),
        }
    if event_type == "system_config_change":
        return {
            "component": rng.choice(["auth", "billing", "logging", "rbac"]),
            "change_ref": fake.bothify(text="CHG-######"),
        }
    if event_type == "pci_config_change":
        return {
            "component": rng.choice(["card-vault", "tokenizer", "checkout"]),
            "change_ref": fake.bothify(text="CHG-######"),
        }
    if event_type == "user_audit":
        return {
            "snapshot_id": fake.uuid4(),
            "user_count": rng.randint(10, 500),
        }
    if event_type == "auth_failure" or event_type == "failed_auth_pci":
        return {
            "attempt_count": rng.randint(1, 10),
            "source_ip": fake.ipv4(),
        }
    if event_type == "admin_login":
        return {
            "source_ip": fake.ipv4(),
            "user_agent": fake.user_agent(),
        }
    # Sensible fallback so the column never stores an empty dict.
    return {"detail": fake.sentence(nb_words=4)}


def generate_synthetic_logs(
    count: int,
    *,
    frameworks: list[str],
    seed: int = 42,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> list[dict[str, Any]]:
    """Generate ``count`` synthetic ``LogEvent``-shaped dicts.

    Args:
        count: Number of events to generate.
        frameworks: Eligible framework codes to tag events against.
            Each event picks 1-2 of these. Unknown codes are skipped.
        seed: Seed for both ``Faker`` and ``random`` for reproducibility.
        period_start: Start of the window (tz-aware UTC). Defaults to
            30 days before ``period_end``.
        period_end: End of the window (tz-aware UTC). Defaults to
            ``datetime.now(UTC)``.

    Returns:
        A list of dicts whose keys match ``LogEvent`` columns:
        ``timestamp, framework_tags, event_type, actor, resource,
        action, outcome, sensitivity, payload``.
    """
    if count < 0:
        raise ValueError("count must be non-negative")

    # Drop unknown framework codes early so each event always has at
    # least one usable allowlist to draw an event_type from.
    eligible = [f for f in frameworks if f in FRAMEWORK_EVENT_TYPES]
    if not eligible:
        raise ValueError(
            f"no supported frameworks in {frameworks}; "
            f"expected any of {list(FRAMEWORK_EVENT_TYPES)}"
        )

    if period_end is None:
        period_end = datetime.now(timezone.utc)
    if period_start is None:
        period_start = period_end - timedelta(days=30)
    if period_start.tzinfo is None:
        period_start = period_start.replace(tzinfo=timezone.utc)
    if period_end.tzinfo is None:
        period_end = period_end.replace(tzinfo=timezone.utc)
    if period_start >= period_end:
        raise ValueError("period_start must be < period_end")

    # Pin both RNGs so output is reproducible across runs.
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)

    window_seconds = (period_end - period_start).total_seconds()

    events: list[dict[str, Any]] = []
    for _ in range(count):
        # Pick 1-2 framework tags from the eligible set.
        tag_count = rng.randint(1, min(2, len(eligible)))
        tags = rng.sample(eligible, tag_count)

        # Event type must belong to one of the tagged frameworks'
        # allowlists so the aggregator can classify it.
        chosen_framework = rng.choice(tags)
        event_type = rng.choice(FRAMEWORK_EVENT_TYPES[chosen_framework])

        action = rng.choice(EVENT_ACTIONS.get(event_type, ["execute"]))
        module = rng.choice(FRAMEWORK_MODULES[chosen_framework])
        resource = f"/{module}/{fake.uuid4()}"

        # Uniform distribution across the window keeps the timestamp
        # histogram flat enough for window-filter tests to be stable.
        offset = rng.uniform(0.0, window_seconds)
        timestamp = period_start + timedelta(seconds=offset)

        outcome = rng.choices(OUTCOMES, weights=OUTCOME_WEIGHTS, k=1)[0]
        sensitivity = rng.choices(
            SENSITIVITIES, weights=SENSITIVITY_WEIGHTS, k=1
        )[0]
        payload = _build_payload(fake, rng, event_type)

        events.append(
            {
                "timestamp": timestamp,
                "framework_tags": tags,
                "event_type": event_type,
                "actor": fake.email(),
                "resource": resource,
                "action": action,
                "outcome": outcome,
                "sensitivity": sensitivity,
                "payload": payload,
            }
        )

    logger.info(
        "synthetic_logs_generated",
        count=len(events),
        frameworks=eligible,
        period_start=period_start.isoformat(),
        period_end=period_end.isoformat(),
        seed=seed,
    )
    return events
