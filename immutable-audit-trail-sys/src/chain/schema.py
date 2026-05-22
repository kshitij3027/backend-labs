"""Pydantic models for sealed audit records and digest helpers.

Two models:
- ``AuditRecordPayload`` is the pre-seal shape: every field that feeds
  the hash, in the canonical key order. The chain's tamper-evidence comes
  from this exact shape — change a field name or remove one and you
  invalidate every existing record's self_hash.
- ``AuditRecord`` is the full sealed shape: payload + self_hash + signature.

``compute_self_hash`` hashes the payload through ``sha256_hex`` (which
does sorted-key, separator-free JSON canonicalisation). ``args_digest``
and ``result_digest`` are best-effort digests of Python values — non-JSON
types fall back to ``repr`` rather than crashing, and very-large lists
collapse to a row-count summary so we don't pay O(n) for an audit op.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.crypto.hasher import sha256_hex


# Threshold above which sequence types collapse to a count summary in
# result_digest. 1000 elements is a soft cap chosen to stay well under
# 10ms of canonicalisation work on typical hardware.
_RESULT_DIGEST_LIST_THRESHOLD = 1000


class AuditRecordPayload(BaseModel):
    """The hashable portion of a record — every field that feeds self_hash."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    seq: int = Field(ge=0)
    timestamp_utc: str
    actor: str
    action: str
    resource: str
    success: bool
    error_message: str | None = None
    processing_ms: float | None = None
    args_digest: str
    result_digest: str
    prev_hash: str = Field(min_length=64, max_length=64)


class AuditRecord(AuditRecordPayload):
    """Sealed record — payload + cryptographic seals.

    Inheriting from ``AuditRecordPayload`` means the field order and types
    are guaranteed identical; canonical_dict() round-trips through both
    safely.
    """

    self_hash: str = Field(min_length=64, max_length=64)
    signature: str  # base64


def canonical_dict(record: AuditRecordPayload) -> dict[str, Any]:
    """Return the payload as an ordinary dict, ready for hashing.

    Crucially: drops ``self_hash`` and ``signature`` if present (so this
    helper is safe to call on a fully sealed ``AuditRecord`` too — used
    by the verifier when re-deriving self_hash for an existing row).
    """
    data = record.model_dump()
    data.pop("self_hash", None)
    data.pop("signature", None)
    return data


def compute_self_hash(record: AuditRecordPayload) -> str:
    """SHA-256 of the canonical bytes of the record's payload fields."""
    return sha256_hex(canonical_dict(record))


def _canonicalise_value(value: Any) -> Any:
    """Recursively coerce a value into something json.dumps can serialise.

    Falls back to repr() for arbitrary objects so that a misconfigured
    decorator never blocks the wrapped function with a JSON error. The
    cost is that two different objects with the same repr collide; an
    auditor can still see the call happened, just not distinguish them.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(k): _canonicalise_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        seq = list(value)
        if len(seq) > _RESULT_DIGEST_LIST_THRESHOLD:
            return {"__type__": "sequence", "__count__": len(seq)}
        return [_canonicalise_value(v) for v in seq]
    return repr(value)


def args_digest(args: tuple, kwargs: dict[str, Any]) -> str:
    """SHA-256 of the canonicalised positional + keyword args.

    The digest is over a dict ``{"args": [...], "kwargs": {...}}`` so we
    can tell apart e.g. ``f(1, 2)`` vs ``f(a=1, b=2)`` even if the
    underlying scalar values match.
    """
    payload = {
        "args": _canonicalise_value(list(args)),
        "kwargs": _canonicalise_value(kwargs),
    }
    return sha256_hex(payload)


def result_digest(result: Any) -> str:
    """SHA-256 of the canonicalised return value (or row-count summary)."""
    return sha256_hex({"result": _canonicalise_value(result)})


__all__ = [
    "AuditRecord",
    "AuditRecordPayload",
    "args_digest",
    "canonical_dict",
    "compute_self_hash",
    "result_digest",
]
