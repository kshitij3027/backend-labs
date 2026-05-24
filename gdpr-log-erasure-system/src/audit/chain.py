"""Hash-chain primitives (placeholder - full append logic lands in commit 5).

Commit 2 only needs the deterministic hashing math + chain constants so
:func:`src.persistence.db.init_db` can seed the genesis row. The richer
``append_audit_entry`` flow (lock the chain, fetch the previous entry's
hash, atomically insert the new row) arrives in commit 5 alongside the
verifier.

Chain invariants the hashing math has to honour:

* ``GENESIS_SEQUENCE`` is ``0`` and ``GENESIS_PREV_HASH`` is the 64-char
  all-zero string. Every subsequent entry's ``prev_hash`` is the previous
  row's ``entry_hash``.
* The hash input is a ``":"`` joined canonical string in a fixed field
  order. Keep the order stable across commits; the verifier will replay
  it byte-for-byte.
* ``payload_json`` is canonicalised by the caller (typically via
  ``json.dumps(..., sort_keys=True)``) before it reaches
  :func:`compute_entry_hash`. Doing the canonicalisation outside this
  function keeps the hashing math pure.
"""
from __future__ import annotations

import hashlib


GENESIS_SEQUENCE: int = 0
GENESIS_PREV_HASH: str = "0" * 64


def compute_entry_hash(
    prev_hash: str,
    sequence: int,
    event_type: str,
    payload_json_str: str,
    created_at_iso: str,
) -> str:
    """SHA-256 of a canonical ``":"`` joined string of the entry's fields.

    Field order is locked: ``prev_hash:sequence:event_type:payload_json_str:created_at_iso``.
    Changing the order would silently invalidate every existing chain, so
    don't reorder without a migration story.
    """
    payload = ":".join(
        [prev_hash, str(sequence), event_type, payload_json_str, created_at_iso]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
