"""SHA-256 hash-chain audit appender for the automated-log-retention service.

The chain is a single-writer append-only log persisted as the
``audit_entries`` table (see :mod:`src.persistence.models`). Each entry
carries:

  * ``seq`` — strictly monotonic non-autoincrement integer; ``seq=0`` is
    the genesis row inserted at ``init_db`` time.
  * ``prev_hash`` — the previous entry's ``entry_hash`` (or
    ``GENESIS_PREV_HASH`` = 64 zeros for ``seq=0``).
  * ``entry_hash`` — SHA-256 over the canonicalised dict of all the
    above fields plus ``ts_utc``, ``actor``, ``action``, ``resource``,
    and the parsed ``metadata`` dict.

Tamper-evidence: any single-field mutation (or any seq insertion that
breaks the prev-hash chain) is detected at the next
:meth:`ChainVerifier.verify_full` pass — see :mod:`src.audit.verifier`.

**No signatures.** This project uses a SHA-256 chain only — there is no
Ed25519 layer like in ``immutable-audit-trail-sys``. The chain is
sufficient for the use case (a SOX/PCI-style audit trail proving
retention actions happened in the recorded order); add signing if a
non-repudiation requirement appears later.

**Concurrency.** Two coroutines calling :meth:`AuditAppender.append`
concurrently must produce two strictly-monotonic seq values. We rely on
two layers:

  * An intra-process :class:`asyncio.Lock` held by the appender instance.
    Serialises the "read MAX(seq) -> compute hash -> INSERT" window so
    two coroutines on the same Python interpreter cannot interleave
    that read-modify-write and end up trying to insert the same seq.
  * SQLite's WAL-mode single-writer semantics. Each commit goes through
    the WAL writer; the ``audit_entries.seq`` PK uniqueness constraint
    is the backstop — even without the asyncio.Lock, two committed rows
    with the same seq would fail at the DB layer.

We can't use ``BEGIN IMMEDIATE`` here because SQLAlchemy's
``AsyncSession`` opens an implicit transaction at first use, so an
explicit ``BEGIN`` raises "cannot start a transaction within a
transaction". The asyncio.Lock + WAL combo is the right shape for our
single-worker uvicorn deployment (per the plan). A future multi-worker
deployment would need a retry loop around the UNIQUE-constraint
failure path — not relevant today.

**Canonicalisation.** The bytes fed to SHA-256 are
``json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")``.
Two callers passing ``{"a": 1, "b": 2}`` and ``{"b": 2, "a": 1}`` will
produce the same hash — dict iteration order is irrelevant. The same
canonicalisation runs in the verifier when re-deriving hashes, which is
what makes the round-trip safe across Python versions / interpreters.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.persistence.models import AuditEntry

log = logging.getLogger(__name__)


# Genesis prev_hash: 64 hex zeros. Mirrors the reference project's
# convention and lets the verifier anchor seq=0 without a magic constant
# repeated at every call site.
GENESIS_PREV_HASH: str = "0" * 64


def _canonical_bytes(
    seq: int,
    ts_utc: datetime,
    actor: str,
    action: str,
    resource: str,
    metadata: dict,
    prev_hash: str,
) -> bytes:
    """Build the canonical byte stream fed to SHA-256.

    The payload is a plain dict with stable key order via
    ``sort_keys=True`` and no whitespace via the tight ``separators``
    tuple. ``ts_utc`` is rendered with ``isoformat()`` so two equivalent
    datetimes (with and without microseconds) hash to different bytes
    if and only if their string forms differ — that is exactly the
    auditor's contract ("we hashed what we stored, byte-for-byte").

    The dict is recreated every call (no caching). At a few hundred
    entries per second the json.dumps overhead is negligible.
    """
    payload = {
        "seq": seq,
        "ts_utc": ts_utc.isoformat(),
        "actor": actor,
        "action": action,
        "resource": resource,
        "metadata": metadata,
        "prev_hash": prev_hash,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _compute_hash(
    seq: int,
    ts_utc: datetime,
    actor: str,
    action: str,
    resource: str,
    metadata: dict,
    prev_hash: str,
) -> str:
    """Return the hex SHA-256 of the canonical bytes."""
    return hashlib.sha256(
        _canonical_bytes(seq, ts_utc, actor, action, resource, metadata, prev_hash)
    ).hexdigest()


async def ensure_genesis(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Insert the genesis row (``seq=0``) if the chain is empty.

    Idempotent: safe to call on every boot. Looks up ``MAX(seq)``; if a
    row already exists the function returns without writing anything.
    Otherwise it writes a single seq=0 row with:

      * ``actor='system'``
      * ``action='genesis'``
      * ``resource='audit_chain'``
      * ``metadata_json='{}'``
      * ``prev_hash=GENESIS_PREV_HASH`` (64 hex zeros)
      * ``entry_hash`` derived via :func:`_compute_hash` so the row
        verifies under the same code path as every subsequent append.

    Called from :func:`src.persistence.db.init_db` so a fresh DB is
    immediately ready for :class:`AuditAppender.append` calls.
    """
    async with session_factory() as session:
        existing = await session.execute(select(func.max(AuditEntry.seq)))
        max_seq = existing.scalar_one_or_none()
        if max_seq is not None:
            return

        ts = datetime.utcnow()
        metadata: dict = {}
        entry_hash = _compute_hash(
            seq=0,
            ts_utc=ts,
            actor="system",
            action="genesis",
            resource="audit_chain",
            metadata=metadata,
            prev_hash=GENESIS_PREV_HASH,
        )
        row = AuditEntry(
            seq=0,
            ts_utc=ts,
            actor="system",
            action="genesis",
            resource="audit_chain",
            metadata_json=json.dumps(metadata, sort_keys=True),
            prev_hash=GENESIS_PREV_HASH,
            entry_hash=entry_hash,
        )
        session.add(row)
        await session.commit()
        log.info("ensure_genesis: inserted seq=0 genesis row")


class AuditAppender:
    """Append-only writer for the SHA-256 audit chain.

    One instance per process: holds the async session factory + a
    per-instance :class:`asyncio.Lock` and exposes :meth:`append`.
    Concurrent callers from the same Python interpreter serialize at
    the lock, so two coroutines never end up reading the same
    ``MAX(seq)`` and trying to insert duplicate ids.

    The appender does NOT bootstrap the genesis row itself — that is
    :func:`ensure_genesis`'s job, called from ``init_db``. If
    :meth:`append` finds an empty chain it raises ``RuntimeError``
    rather than silently writing seq=0; a missing genesis is a
    deployment bug worth surfacing loudly.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory
        # Serialize concurrent appends from the same process so the
        # "read MAX(seq) -> compute hash -> INSERT" window can't
        # interleave. SQLite's WAL mode handles cross-process
        # serialization at the DB layer; this lock handles intra-process
        # coroutine races (which is the only concurrency that matters
        # in our single-worker uvicorn deployment).
        self._lock = asyncio.Lock()

    async def append(
        self,
        *,
        actor: str,
        action: str,
        resource: str,
        metadata: dict | None = None,
        ts_utc: datetime | None = None,
    ) -> AuditEntry:
        """Append one entry; return the freshly-sealed ORM instance.

        Write path (executed under the per-appender ``asyncio.Lock``):

          1. ``SELECT MAX(seq)`` — gives the current head. Empty chain
             raises ``RuntimeError`` (genesis must exist).
          2. ``SELECT entry_hash WHERE seq = max_seq`` — gives prev_hash.
          3. Compute ``entry_hash`` over the new entry's canonical bytes.
          4. ``INSERT`` the new row.
          5. ``COMMIT`` — releases the SQLAlchemy implicit transaction.

        The returned object has ``seq`` and ``entry_hash`` populated,
        which lets callers correlate the audit row back to the action
        they just performed (handy for tests + downstream tracing).
        """
        metadata = metadata if metadata is not None else {}
        ts = ts_utc if ts_utc is not None else datetime.utcnow()

        # The lock guards the read-modify-write of MAX(seq). SQLAlchemy's
        # AsyncSession already manages its own transaction (an explicit
        # ``BEGIN IMMEDIATE`` would raise "cannot start a transaction
        # within a transaction"), so we rely on the asyncio.Lock for
        # intra-process serialization and on the PK uniqueness +
        # WAL-mode single-writer for the engine-level backstop.
        async with self._lock:
            async with self._sf() as session:
                max_seq_row = await session.execute(
                    select(func.max(AuditEntry.seq))
                )
                max_seq = max_seq_row.scalar_one_or_none()
                if max_seq is None:
                    # ensure_genesis must have run during init_db. We
                    # refuse to bootstrap from inside append() because
                    # the genesis row's actor/action/resource semantics
                    # are a deployment decision (system / genesis /
                    # audit_chain) — not the caller's.
                    raise RuntimeError(
                        "audit chain is empty; ensure_genesis() must run before append()"
                    )

                prev_row = await session.execute(
                    select(AuditEntry.entry_hash).where(AuditEntry.seq == max_seq)
                )
                prev_hash = prev_row.scalar_one()

                new_seq = max_seq + 1
                entry_hash = _compute_hash(
                    seq=new_seq,
                    ts_utc=ts,
                    actor=actor,
                    action=action,
                    resource=resource,
                    metadata=metadata,
                    prev_hash=prev_hash,
                )

                row = AuditEntry(
                    seq=new_seq,
                    ts_utc=ts,
                    actor=actor,
                    action=action,
                    resource=resource,
                    metadata_json=json.dumps(metadata, sort_keys=True),
                    prev_hash=prev_hash,
                    entry_hash=entry_hash,
                )
                session.add(row)
                await session.commit()
                await session.refresh(row)
                return row


__all__ = [
    "GENESIS_PREV_HASH",
    "AuditAppender",
    "ensure_genesis",
]
