"""ChainVerifier — replays the SHA-256 audit chain and reports breaks.

The verifier walks every ``audit_entries`` row ordered by ``seq`` and
re-derives the ``entry_hash`` from the stored fields. Three classes of
break are surfaced:

  * ``"genesis_mismatch"`` — the seq=0 row's ``prev_hash`` is not
    :data:`GENESIS_PREV_HASH` (64 hex zeros). Genesis was tampered with.
  * ``"prev_hash_mismatch"`` — a row's ``prev_hash`` doesn't equal the
    previous row's ``entry_hash`` (or there's a seq gap between them).
  * ``"hash_mismatch"`` — the recomputed ``entry_hash`` doesn't match
    the stored value: a payload field (actor, action, metadata, etc.)
    was mutated after the row was sealed.

The verifier returns a :class:`VerifyResult` describing the first break
(or success). Subsequent breaks are not enumerated — the canonical
answer for an auditor is "where did the chain first deviate"; once that
seq is patched (or quarantined) the verifier can be re-run for the next
break.

Pure-read code path — no writes, no signatures, no external state.
Cheap to call ad-hoc from a dashboard route or as a nightly cron tick.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.audit.chain import GENESIS_PREV_HASH, _compute_hash
from src.persistence.models import AuditEntry

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class VerifyResult:
    """Structured outcome of a :meth:`ChainVerifier.verify_full` pass.

    Fields:
      * ``ok`` — True iff every row's hash + chain linkage validated.
      * ``head_seq`` — seq of the last verified row, or ``None`` if the
        chain is empty (a brand-new DB with no genesis).
      * ``first_break_seq`` — seq at which the first break occurred, or
        ``None`` if ``ok`` is True. NOTE: this is the seq that failed,
        not the last good seq before it.
      * ``first_break_reason`` — one of ``"genesis_mismatch"``,
        ``"prev_hash_mismatch"``, ``"hash_mismatch"`` — populated iff
        ``ok`` is False.
    """

    ok: bool
    head_seq: int | None
    first_break_seq: int | None = None
    first_break_reason: str | None = None


class ChainVerifier:
    """Streams audit rows in seq order and re-derives each ``entry_hash``."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    async def verify_full(self) -> VerifyResult:
        """Verify the full chain from seq=0 to head.

        Returns immediately with ``ok=True, head_seq=None`` on an empty
        chain — nothing to verify, nothing to report.

        Walk algorithm:

          1. Fetch all rows ordered by ``seq ASC``.
          2. For the first row (seq=0): assert ``prev_hash ==
             GENESIS_PREV_HASH``; if not, break with
             ``"genesis_mismatch"``.
          3. For each subsequent row: assert ``row.prev_hash ==
             prev_row.entry_hash``; if not, break with
             ``"prev_hash_mismatch"``. (This also catches seq gaps
             implicitly — a missing intermediate would leave the chain
             dangling.)
          4. For every row: recompute ``entry_hash`` from the stored
             fields via :func:`_compute_hash`; if it doesn't match the
             stored value, break with ``"hash_mismatch"``.
          5. Track ``last_verified_seq`` so a successful walk returns
             the head seq.

        The first failing row causes an immediate return — we don't
        enumerate every break in the chain on one pass. An auditor who
        wants the full damage report patches the first break, re-runs,
        and repeats.
        """
        async with self._sf() as session:
            result = await session.execute(
                select(AuditEntry).order_by(AuditEntry.seq.asc())
            )
            rows = list(result.scalars().all())

        if not rows:
            # Empty chain — vacuously ok. The dashboard will render
            # "no audit entries yet" rather than "chain broken".
            return VerifyResult(ok=True, head_seq=None)

        prev_hash: str | None = None
        last_seq: int | None = None

        for row in rows:
            # 1. Chain linkage check.
            if prev_hash is None:
                # First row in the walk — must be seq=0 with the
                # genesis sentinel. (We accept rows in seq order; a
                # missing seq=0 would still surface as a
                # genesis_mismatch on whichever row is first.)
                if row.prev_hash != GENESIS_PREV_HASH:
                    return VerifyResult(
                        ok=False,
                        head_seq=last_seq,
                        first_break_seq=row.seq,
                        first_break_reason="genesis_mismatch",
                    )
            else:
                if row.prev_hash != prev_hash:
                    return VerifyResult(
                        ok=False,
                        head_seq=last_seq,
                        first_break_seq=row.seq,
                        first_break_reason="prev_hash_mismatch",
                    )

            # 2. Self-hash check: re-derive from the stored payload and
            #    compare to the stored ``entry_hash``. Any mutation to
            #    actor / action / resource / metadata / ts / prev_hash
            #    shifts the recomputed value and trips this check.
            try:
                metadata = json.loads(row.metadata_json)
            except json.JSONDecodeError:
                # Corrupt metadata is itself a tamper signal — surface
                # it as hash_mismatch (the auditor can drill in via the
                # raw row dump to see the malformed JSON).
                return VerifyResult(
                    ok=False,
                    head_seq=last_seq,
                    first_break_seq=row.seq,
                    first_break_reason="hash_mismatch",
                )

            recomputed = _compute_hash(
                seq=row.seq,
                ts_utc=row.ts_utc,
                actor=row.actor,
                action=row.action,
                resource=row.resource,
                metadata=metadata,
                prev_hash=row.prev_hash,
            )
            if recomputed != row.entry_hash:
                return VerifyResult(
                    ok=False,
                    head_seq=last_seq,
                    first_break_seq=row.seq,
                    first_break_reason="hash_mismatch",
                )

            # Advance the walk.
            prev_hash = row.entry_hash
            last_seq = row.seq

        # Full walk succeeded.
        return VerifyResult(ok=True, head_seq=last_seq)


__all__ = ["ChainVerifier", "VerifyResult"]
