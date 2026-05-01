"""Periodic state-snapshot persistence to Redis.

The :class:`StatePersister` captures the *counters* of a primary node's
:class:`~src.log_processor.LogProcessor` to Redis on a fixed cadence so
that, when a standby is promoted, the new primary picks up where the old
primary left off — its ``log_count`` and ``last_log_id`` continue from
the snapshot rather than restarting at zero.

What is and is NOT persisted
----------------------------
We deliberately do **not** ship the actual log entries through Redis.
This is a learning lab; replicating the full log payload is a separate
problem (see Kafka / Raft / cross-DC replication). Instead the snapshot
preserves the *counters* — ``log_count``, ``last_log_id``, and the
allocator's ``_next_id`` — so:

* The /metrics endpoint reports a continuous count across promotions.
* New ingest calls keep allocating monotonically-increasing ids that
  never collide with what the old primary already accepted.
* Idempotent retries of pre-failover ids are deduplicated even though
  their original payload is gone.

Failure semantics
-----------------
The snapshot interval (``STATE_SYNC_INTERVAL``, default 5s) bounds how
much un-snapshotted progress can be lost on an uncoordinated primary
kill. A clean ``release_lock`` path (manual failover / SIGTERM) does not
make the snapshot any fresher — it's purely time-driven. The README
documents this as "≤5 seconds of writes can be lost on uncoordinated
primary kill".

Lifecycle
---------
* ``start()``  — spawn the periodic snapshot task. Idempotent.
* ``stop()``   — cancel + await the task. Idempotent.
* ``snapshot_now()`` — single tick; only writes if state == PRIMARY.
* ``load_into(lp)`` — read the snapshot and seed ``lp``'s counters.
  Called on every ``* -> PRIMARY`` transition by the node orchestrator.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

from src.log_processor import LogProcessor
from src.models import NodeState, StateSnapshot, from_json, to_json
from src.redis_client import RedisClient

logger = logging.getLogger(__name__)


# Upper bound on ``last_log_id`` for which we'll backfill the
# ``_seen_ids`` set on load. Anything larger and we skip the dedup
# backfill (with a warning) — at that point the dedup cost outweighs
# the post-failover idempotency benefit. The new primary will still
# allocate new ids cleanly because ``_next_id`` is always seeded.
_SEEN_IDS_BACKFILL_LIMIT = 1_000_000


# Type alias mirroring how ``HeartbeatEmitter`` wires its state provider.
StateProvider = Callable[[], NodeState]


class StatePersister:
    """Periodic snapshot writer + on-promotion loader.

    The persister is paired with exactly one :class:`LogProcessor` and one
    :class:`RedisClient`. The state provider is a callable (typically
    ``lambda: state_machine.state``) so the persister observes the *live*
    state without holding any cross-component reference.
    """

    def __init__(
        self,
        redis_client: RedisClient,
        log_processor: LogProcessor,
        state_provider: StateProvider,
        sync_interval: float = 5.0,
        schema_version: int = 1,
    ) -> None:
        self.redis: RedisClient = redis_client
        self.log_processor: LogProcessor = log_processor
        self._state_provider: StateProvider = state_provider
        self.sync_interval: float = sync_interval
        self.schema_version: int = schema_version

        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event: asyncio.Event = asyncio.Event()

        # Counters — plain attributes so the /metrics endpoint can read
        # them directly (see http_server._COUNTER_DEFS).
        self.snapshots_written_total: int = 0
        self.snapshots_loaded_total: int = 0

    # --- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Spawn the periodic snapshot task. Idempotent.

        Calling ``start()`` while the task is already running is a
        no-op rather than spinning a second task.
        """
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="state-persister")

    async def stop(self) -> None:
        """Cancel the periodic task and await its exit. Idempotent."""
        self._stop_event.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("state-persister task raised on stop")

    # --- single-shot operations -----------------------------------------

    async def snapshot_now(self) -> bool:
        """Write a single snapshot regardless of schedule.

        Returns ``True`` if a snapshot was written, ``False`` if the
        node isn't currently PRIMARY (in which case we deliberately
        don't overwrite the snapshot a real primary may have just
        committed).
        """
        if self._state_provider() is not NodeState.PRIMARY:
            return False

        now = time.time()
        snap = StateSnapshot(
            version=self.schema_version,
            log_count=self.log_processor.log_count,
            last_log_id=self.log_processor.last_log_id,
            watermark=now,
            taken_at=now,
        )
        payload = to_json(snap)
        await self.redis.put_snapshot(payload)
        self.snapshots_written_total += 1
        logger.debug(
            "state snapshot written (last_log_id=%d, log_count=%d)",
            snap.last_log_id,
            snap.log_count,
        )
        return True

    async def load_into(self, log_processor: LogProcessor) -> bool:
        """Read the latest snapshot and seed ``log_processor`` counters.

        We only restore the *counters* (``_next_id`` and ``_seen_ids``),
        not the actual log entries — see the module docstring for why.

        Behaviour:

        * No snapshot in Redis → return ``False``.
        * ``snap.version != self.schema_version`` → log a warning and
          return ``False`` (refuse to load an incompatible snapshot).
        * Otherwise seed ``_next_id = max(current, snap.last_log_id + 1)``
          and (if ``snap.last_log_id`` is within
          :data:`_SEEN_IDS_BACKFILL_LIMIT`) backfill ``_seen_ids``
          with every id in ``[1, snap.last_log_id]`` so future
          idempotent retries dedup against pre-failover ids.

        Returns ``True`` if a snapshot was loaded, ``False`` otherwise.
        """
        raw = await self.redis.get_snapshot()
        if raw is None:
            return False

        try:
            snap = from_json(StateSnapshot, raw)
        except Exception:
            logger.exception("failed to parse state snapshot; refusing to load")
            return False

        if snap.version != self.schema_version:
            logger.warning(
                "snapshot schema version mismatch: got %d, expected %d — refusing to load",
                snap.version,
                self.schema_version,
            )
            return False

        # Seed the next-id allocator.
        candidate_next = snap.last_log_id + 1
        # Direct attribute reach: LogProcessor is co-owned by the node
        # and the persister is one of its trusted internals.
        if candidate_next > log_processor._next_id:
            log_processor._next_id = candidate_next

        # Backfill the dedup set, but only if it's a sane size.
        if snap.last_log_id > _SEEN_IDS_BACKFILL_LIMIT:
            logger.warning(
                "snapshot last_log_id=%d exceeds backfill limit %d; "
                "skipping _seen_ids backfill (allocation continuity preserved)",
                snap.last_log_id,
                _SEEN_IDS_BACKFILL_LIMIT,
            )
        elif snap.last_log_id > 0:
            log_processor._seen_ids.update(range(1, snap.last_log_id + 1))

        self.snapshots_loaded_total += 1
        logger.info(
            "state snapshot loaded (last_log_id=%d, log_count=%d, version=%d)",
            snap.last_log_id,
            snap.log_count,
            snap.version,
        )
        return True

    # --- background loop ------------------------------------------------

    async def _run(self) -> None:
        """Tick every ``sync_interval`` seconds; snapshot iff PRIMARY.

        On non-PRIMARY ticks we simply wait for the next interval
        (mirroring the heartbeat-emitter idle pattern). The loop
        survives a STANDBY → PRIMARY → STANDBY cycle without restart.
        """
        while not self._stop_event.is_set():
            try:
                await self.snapshot_now()
            except Exception:
                logger.exception("state-persister tick raised")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.sync_interval
                )
            except asyncio.TimeoutError:
                pass
