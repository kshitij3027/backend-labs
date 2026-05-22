"""Deterministic SHA-256 hashing over canonical-bytes payloads.

The canonical-bytes representation guarantees byte-identical output regardless
of dict insertion order, which is the foundation of the tamper-evidence
property of the audit chain: two parties hashing the same logical payload
will always produce the same digest.
"""

import hashlib
import json
from collections.abc import Mapping
from typing import Any

GENESIS_PREV_HASH: str = "0" * 64


def canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    """Serialize ``payload`` to a deterministic UTF-8 byte sequence.

    Keys are sorted, separators are tight (no whitespace), and non-ASCII
    characters are preserved verbatim. The resulting bytes are stable across
    Python versions and dict insertion orders, so SHA-256 over them yields a
    reproducible digest.
    """
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def sha256_hex(payload: Mapping[str, Any]) -> str:
    """Return the hex-encoded SHA-256 digest of ``canonical_bytes(payload)``."""
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()
