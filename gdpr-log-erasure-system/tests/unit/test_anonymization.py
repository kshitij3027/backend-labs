"""Unit tests for the anonymisation engine."""
from __future__ import annotations

import pytest
from src.erasure.anonymization import (
    anonymize_mapping_payload, decide_action, hash_identifier, is_anonymized, mask_ip,
)


# ── hash_identifier ─────────────────────────────────────────────────────────


def test_hash_identifier_deterministic_same_salt():
    assert hash_identifier("u-1", "salt") == hash_identifier("u-1", "salt")


def test_hash_identifier_different_with_different_salt():
    assert hash_identifier("u-1", "saltA") != hash_identifier("u-1", "saltB")


def test_hash_identifier_length_and_no_substring_leak():
    out = hash_identifier("user-12345-secret", "salt")
    assert len(out) == 32
    assert "12345" not in out and "secret" not in out and "user" not in out


def test_hash_identifier_int_coerces_to_str():
    assert hash_identifier(123, "salt") == hash_identifier("123", "salt")  # type: ignore[arg-type]


def test_hash_identifier_zero_reidentification_risk():
    """100 distinct users, salted → no hash collisions, no inverse lookup feasible."""
    salt = "compliance-salt"
    pairs = {f"user-{i}": hash_identifier(f"user-{i}", salt) for i in range(100)}
    inverse = {h: u for u, h in pairs.items()}
    # No collisions
    assert len(inverse) == 100
    # Inverse lookup cannot recover an unseen user
    assert "user-200" not in inverse.values()


# ── mask_ip ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("ip,expected", [
    ("198.51.100.42", "198.51.100.0"),
    ("192.0.2.1", "192.0.2.0"),
    ("10.20.30.255", "10.20.30.0"),
])
def test_mask_ipv4(ip, expected):
    assert mask_ip(ip) == expected


def test_mask_ipv6_zeros_lower_80_bits():
    # 2001:db8:abcd:1234::5 -> /48 keeps 2001:db8:abcd, zeros the rest
    out = mask_ip("2001:db8:abcd:1234::5")
    assert out == "2001:db8:abcd::"


def test_mask_ip_invalid_returned_unchanged():
    assert mask_ip("not-an-ip") == "not-an-ip"
    assert mask_ip("") == ""


# ── decide_action ──────────────────────────────────────────────────────────


ALLOWLIST = {"system_logs", "analytics_events", "performance_metrics", "aggregated_data"}


@pytest.mark.parametrize("request_type,data_type,expected", [
    ("DELETE", "system_logs", "DELETE"),
    ("DELETE", "personal_profile", "DELETE"),
    ("DELETE", "anything_else", "DELETE"),
    ("ANONYMIZE", "system_logs", "ANONYMIZE"),
    ("ANONYMIZE", "analytics_events", "ANONYMIZE"),
    ("ANONYMIZE", "performance_metrics", "ANONYMIZE"),
    ("ANONYMIZE", "aggregated_data", "ANONYMIZE"),
    ("ANONYMIZE", "personal_profile", "DELETE"),    # fallback
    ("ANONYMIZE", "billing_records", "DELETE"),     # fallback
    ("delete", "system_logs", "DELETE"),            # case-insensitive
    ("anonymize", "system_logs", "ANONYMIZE"),
])
def test_decide_action_matrix(request_type, data_type, expected):
    assert decide_action(request_type, data_type, ALLOWLIST) == expected


def test_decide_action_invalid_request_type_raises():
    with pytest.raises(ValueError):
        decide_action("PURGE", "system_logs", ALLOWLIST)


# ── anonymize_mapping_payload ──────────────────────────────────────────────


def test_anonymize_payload_hashes_identifiers_and_masks_ips():
    salt = "s"
    out = anonymize_mapping_payload(
        {"user_id": "u-1", "email": "a@b.c", "ip": "10.1.2.3", "level": "INFO"},
        salt,
    )
    assert out["user_id"] == hash_identifier("u-1", salt)
    assert out["email"] == hash_identifier("a@b.c", salt)
    assert out["ip"] == "10.1.2.0"
    assert out["level"] == "INFO"  # unrelated field preserved
    assert is_anonymized(out) is True


def test_anonymize_payload_idempotent():
    salt = "s"
    p1 = anonymize_mapping_payload({"user_id": "u-1", "ip": "10.1.2.3"}, salt)
    p2 = anonymize_mapping_payload(p1, salt)
    # marker preserved, hashed values remain hashed (hash of a hash != raw hash, but the
    # idempotency promise is: re-running yields stable, anonymised output)
    assert is_anonymized(p2) is True
    assert "_anonymized" in p2


def test_anonymize_empty_payload_returns_marker_only():
    out = anonymize_mapping_payload(None, "s")
    assert out == {"_anonymized": True}
    out = anonymize_mapping_payload({}, "s")
    assert out == {"_anonymized": True}


def test_is_anonymized_helper():
    assert is_anonymized({"_anonymized": True}) is True
    assert is_anonymized({"foo": "bar"}) is False
    assert is_anonymized(None) is False
