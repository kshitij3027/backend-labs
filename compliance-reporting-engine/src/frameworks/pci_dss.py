"""PCI-DSS (Payment Card Industry Data Security Standard) framework rules.

PCI-DSS evidence for the reporting engine breaks into five categories:

  * ``cardholder_access``    — reads / decrypts / tokenizations of card data
  * ``payment_processing``   — authorize / capture / refund / void events
  * ``key_rotation``         — KMS key rotate / retire / activate events
  * ``failed_auth``          — failed auth attempts against cardholder systems
  * ``config_changes``       — changes to the card-vault / tokenizer / checkout

The ``findings`` rule emits three kinds of human-readable strings:

  1. A gauge of how many **unique actors** touched cardholder data in the
     window. Even one is worth knowing about, but the gauge is only emitted
     when at least one such event exists — empty windows shouldn't add
     noise. Auditors care about who touched card data, not just how often.
  2. If failed auth attempts targeting cardholder data exceed 10, surface
     the count. The threshold is intentionally conservative — PCI-DSS
     control 8.1.6 requires lockout after 6 attempts, so >10 in a
     reporting window suggests either a brute-force attempt or a broken
     control.
  3. If the most recent ``key_rotation`` is older than 90 days (or there
     are none at all), surface a "rotation overdue" finding. PCI-DSS
     requirement 3.6.4 mandates cryptographic key rotation at least
     annually, but industry best practice (and the project's defaults)
     uses 90 days. The comparison is against an optional ``period_end``
     so reports remain deterministic when re-run for the same window.

The signature of ``findings`` widens the base contract with an optional
``period_end`` kwarg. This is LSP-compatible (the subclass accepts a
superset of inputs) and lets the aggregator pin "now" for reproducible
reports — useful for tests and for regenerating a historical period.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from . import register_framework
from .base import FrameworkRules

if TYPE_CHECKING:
    from src.persistence.models import LogEvent


# Industry-standard rotation window for cardholder-data encryption keys.
# PCI-DSS 3.6.4 only mandates "at least annually" but 90 days is the
# operational baseline most card processors apply.
_KEY_ROTATION_MAX_AGE = timedelta(days=90)

# Threshold above which failed cardholder-auth attempts become a finding.
_FAILED_AUTH_THRESHOLD = 10


@register_framework("PCI_DSS")
class PCIDSSRules(FrameworkRules):
    """Concrete ``FrameworkRules`` for PCI-DSS.

    Categories track the five buckets a PCI-DSS QSA expects to see in
    evidence: cardholder-data access, payment processing, key rotation,
    failed auth attempts against cardholder systems, and configuration
    changes.
    """

    name = "PCI_DSS"

    categories = [
        "cardholder_access",
        "payment_processing",
        "key_rotation",
        "failed_auth",
        "config_changes",
    ]

    event_type_to_category = {
        "cardholder_data_access": "cardholder_access",
        "payment_processing": "payment_processing",
        "key_rotation": "key_rotation",
        "failed_auth_pci": "failed_auth",
        "pci_config_change": "config_changes",
    }

    @classmethod
    def findings(
        cls,
        events: list["LogEvent"],
        *,
        period_end: datetime | None = None,
    ) -> list[str]:
        """Emit PCI-DSS-specific human-readable findings.

        Args:
            events: The classified ``LogEvent`` rows for the period.
            period_end: Optional anchor for the "now" used by the
                key-rotation freshness check. Defaults to
                ``datetime.now(UTC)`` when ``None``, but tests and
                deterministic regenerations should pass a fixed value.

        Three rules:
          * any ``cardholder_data_access`` events -> emit a unique-actor
            gauge; skipped when there are zero such events (empty
            windows shouldn't add noise).
          * ``failed_auth_pci`` count > 10 -> surface the count.
          * latest ``key_rotation`` older than 90 days (or none at all) ->
            surface a "rotation overdue" finding.
        """
        results: list[str] = []

        # --- Rule 1: unique-actor gauge for cardholder data access. ---
        cardholder_access_actors = {
            event.actor
            for event in events
            if event.event_type == "cardholder_data_access"
        }
        if cardholder_access_actors:
            n_unique_actors = len(cardholder_access_actors)
            results.append(
                f"Cardholder data accessed by {n_unique_actors} unique actors"
            )

        # --- Rule 2: failed cardholder-auth volume threshold. ---
        failed_auth_count = sum(
            1 for event in events if event.event_type == "failed_auth_pci"
        )
        if failed_auth_count > _FAILED_AUTH_THRESHOLD:
            results.append(
                f"{failed_auth_count} failed auth attempts targeting cardholder data"
            )

        # --- Rule 3: key rotation freshness. ---
        # Pin "now" to ``period_end`` when provided so deterministic
        # regenerations of the same report yield identical findings.
        anchor = period_end if period_end is not None else datetime.now(timezone.utc)
        # A small defensive tz-fill so callers can pass naive UTC and
        # still get a sensible comparison (our LogEvent timestamps are
        # always tz-aware UTC, but this avoids a TypeError on edge cases).
        if anchor.tzinfo is None:
            anchor = anchor.replace(tzinfo=timezone.utc)

        key_rotation_timestamps = [
            event.timestamp
            for event in events
            if event.event_type == "key_rotation"
        ]
        if not key_rotation_timestamps:
            # No key_rotation events at all -> by definition overdue. The
            # finding fires even for empty windows; auditors would rather
            # see the gap than have it silently swallowed.
            results.append(
                "Key rotation overdue — last rotation > 90 days ago"
            )
        else:
            latest_rotation = max(key_rotation_timestamps)
            # Match anchor's tz-awareness to the event timestamp's. The
            # LogEvent column is tz-aware UTC; this guard exists only so
            # naive timestamps from unit-test factories don't blow up.
            if latest_rotation.tzinfo is None:
                latest_rotation = latest_rotation.replace(tzinfo=timezone.utc)
            if anchor - latest_rotation > _KEY_ROTATION_MAX_AGE:
                results.append(
                    "Key rotation overdue — last rotation > 90 days ago"
                )

        return results
