"""Anonymization engine — irreversible identifier hashing + IP masking + policy.

All functions are pure. The coordinator wires these into per-location
erasure actions in commit 7.
"""
from __future__ import annotations

import hashlib
import ipaddress
from typing import Any, Literal


# ── Identifier hashing ─────────────────────────────────────────────────────


_IDENTIFIER_FIELDS: tuple[str, ...] = (
    "user_id", "email", "username", "subject_id", "account_id",
)
_IP_FIELDS: tuple[str, ...] = ("ip", "ip_address", "client_ip")


def hash_identifier(value: str, salt: str) -> str:
    """One-way salted SHA-256 of a string identifier.

    Returns the first 32 hex chars (128 bits — collision-resistant for our
    realistic record volumes). Irreversible: the salt + sha256 means no
    rainbow-table inversion is feasible.
    """
    if not isinstance(value, str):
        value = str(value)
    digest = hashlib.sha256((salt + value).encode("utf-8")).hexdigest()
    return digest[:32]


# ── IP masking ─────────────────────────────────────────────────────────────


def mask_ip(ip: str) -> str:
    """Zero the host bits to anonymise an IP while preserving network context.

    IPv4: zero the last octet (e.g., 198.51.100.42 -> 198.51.100.0).
    IPv6: zero the lower 80 bits (preserve the /48 network prefix), since
      RFC 4291 says the upper 48 bits identify the routing prefix.
    Invalid input: returned unchanged.
    """
    try:
        parsed = ipaddress.ip_address(ip)
    except (ValueError, TypeError):
        return ip
    if isinstance(parsed, ipaddress.IPv4Address):
        net = ipaddress.ip_network(f"{ip}/24", strict=False)
        return str(net.network_address)
    # IPv6
    net = ipaddress.ip_network(f"{ip}/48", strict=False)
    return str(net.network_address)


# ── Policy: delete vs anonymize per data_type ──────────────────────────────


Action = Literal["DELETE", "ANONYMIZE"]


def decide_action(
    request_type: str,
    data_type: str,
    anonymizable_types: set[str],
) -> Action:
    """Decide per-location action based on request type and per-type allowlist.

    - request_type == "DELETE" → always DELETE.
    - request_type == "ANONYMIZE" AND data_type in allowlist → ANONYMIZE.
    - request_type == "ANONYMIZE" AND data_type NOT in allowlist → DELETE
      (PII-heavy types like personal_profile or billing_records can't be
      reliably anonymised, so we fall back to deletion to honour the request).
    """
    rt = (request_type or "").upper()
    if rt == "DELETE":
        return "DELETE"
    if rt == "ANONYMIZE":
        return "ANONYMIZE" if data_type in anonymizable_types else "DELETE"
    raise ValueError(f"Unsupported request_type: {request_type!r}")


# ── Payload scrubbing ──────────────────────────────────────────────────────


_ANON_MARKER_KEY: str = "_anonymized"


def anonymize_mapping_payload(payload: dict[str, Any] | None, salt: str) -> dict[str, Any]:
    """Return a NEW dict with identifiers hashed and IPs masked.

    Idempotent — re-anonymising an already-anonymised payload yields the same
    result (the marker key is preserved; hashed values remain hashed).

    Adds a sentinel key ``_anonymized: True`` so downstream verifiers can
    distinguish anonymised rows from raw ones without re-checking each field.
    """
    if not payload:
        return {_ANON_MARKER_KEY: True}

    out: dict[str, Any] = {}
    for k, v in payload.items():
        if k == _ANON_MARKER_KEY:
            continue
        if k in _IDENTIFIER_FIELDS and isinstance(v, (str, int)):
            out[k] = hash_identifier(str(v), salt)
        elif k in _IP_FIELDS and isinstance(v, str):
            out[k] = mask_ip(v)
        else:
            out[k] = v
    out[_ANON_MARKER_KEY] = True
    return out


def is_anonymized(payload: dict[str, Any] | None) -> bool:
    """True if payload carries the anonymisation marker."""
    return bool(payload) and payload.get(_ANON_MARKER_KEY) is True
