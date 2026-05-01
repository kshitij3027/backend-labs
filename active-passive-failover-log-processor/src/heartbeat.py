"""Heartbeat emit (primary side) and monitor (standby side).

* :class:`HeartbeatEmitter` runs on the PRIMARY. Every ``interval`` seconds
  it writes a fresh ``heartbeat:primary`` payload AND renews the leader
  lock. Renewal failure fires ``on_lock_lost`` so the caller can demote.
  The emitter does NOT exit when the node is no longer PRIMARY — it idles
  and re-checks each tick, so a STANDBY → PRIMARY flip seamlessly resumes
  emission without a restart.

* :class:`HeartbeatMonitor` runs on STANDBY (and INACTIVE) nodes. It polls
  ``redis_client.heartbeat_ttl()`` every ``poll_interval`` seconds. The
  monitor tracks the wall-clock time the heartbeat key was last seen
  alive (TTL > 0). If that timestamp is older than ``failure_timeout``,
  ``on_primary_failed`` fires exactly once per failure window. Bootstrap
  case: if the monitor never sees a live heartbeat but has been alive
  for at least ``failure_timeout`` since start, that also counts as a
  failure (the primary may have died before we came up).

Both classes use a stop ``asyncio.Event`` and ``asyncio.wait_for`` so
:py:meth:`stop` returns promptly even when the loop is sleeping.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Awaitable, Callable, Optional

from src.models import HeartbeatMessage, NodeState, to_json
from src.redis_client import RedisClient

logger = logging.getLogger(__name__)


StateProvider = Callable[[], NodeState]
MetricsProvider = Callable[[], dict[str, float]]
LockLostCallback = Callable[[], Awaitable[None]]
PrimaryFailedCallback = Callable[[], Awaitable[None]]


class HeartbeatEmitter:
    """Run on the PRIMARY. Every ``interval`` seconds: write heartbeat key
    AND renew the leader lock. On renewal failure, fire ``on_lock_lost``
    once and break the loop (caller demotes itself).
    """

    def __init__(
        self,
        redis_client: RedisClient,
        state_provider: StateProvider,
        metrics_provider: MetricsProvider,
        node_id: str,
        interval: float = 2.0,
        lock_ttl: int = 6,
        on_lock_lost: Optional[LockLostCallback] = None,
    ) -> None:
        self.redis: RedisClient = redis_client
        self._state_provider: StateProvider = state_provider
        self._metrics_provider: MetricsProvider = metrics_provider
        self.node_id: str = node_id
        self.interval: float = interval
        self.lock_ttl: int = lock_ttl
        self._on_lock_lost: Optional[LockLostCallback] = on_lock_lost

        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event: asyncio.Event = asyncio.Event()

        # Counters (plain attributes — the metrics handler in commit 4a
        # reads these directly; no locking needed because we only mutate
        # them from inside the single emit task).
        self.heartbeats_emitted_total: int = 0
        self.lock_renewal_failures_total: int = 0

    async def start(self) -> None:
        """Spawn the background emit task. Returns immediately."""
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="heartbeat-emitter")

    async def stop(self) -> None:
        """Signal the loop to exit and await its cancellation."""
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
                logger.exception("heartbeat emitter task raised on stop")

    async def emit_once(self) -> Optional[bool]:
        """Single tick. Returns:
            * ``None`` when the node isn't PRIMARY (no I/O performed),
            * ``True`` when the heartbeat was written and the lock renewed,
            * ``False`` when the lock renewal failed.

        Public so tests can drive a single tick without spinning up the loop.
        """
        if self._state_provider() is not NodeState.PRIMARY:
            return None

        msg = HeartbeatMessage(
            node_id=self.node_id,
            timestamp=time.time(),
            state=NodeState.PRIMARY,
            role="primary",
            metrics=self._metrics_provider(),
        )
        await self.redis.write_heartbeat(to_json(msg), ttl=self.lock_ttl)
        self.heartbeats_emitted_total += 1

        renewed = await self.redis.renew_lock(self.lock_ttl)
        if not renewed:
            self.lock_renewal_failures_total += 1
            logger.warning(
                "lock renewal failed for node=%s — firing on_lock_lost",
                self.node_id,
            )
            if self._on_lock_lost is not None:
                try:
                    await self._on_lock_lost()
                except Exception:
                    logger.exception("on_lock_lost callback raised")
        return renewed

    async def _run(self) -> None:
        """Main emit loop. Continues running across PRIMARY/non-PRIMARY flips.

        Exits when:

        * ``stop()`` is called, or
        * ``on_lock_lost`` is set AND the lock renewal returned False (the
          caller has now demoted us; running another tick would just
          stomp on someone else's heartbeat).

        Without an ``on_lock_lost`` callback, renewal failures are logged
        but the loop keeps running so the caller can observe the counter
        and decide what to do.
        """
        while not self._stop_event.is_set():
            try:
                result = await self.emit_once()
            except Exception:
                logger.exception("heartbeat emit tick raised")
                result = None  # treat as a non-event; keep looping

            # If we lost the lock AND a demotion callback is wired up,
            # exit the loop — the caller has flipped us out of PRIMARY.
            if result is False and self._on_lock_lost is not None:
                break

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.interval
                )
            except asyncio.TimeoutError:
                # normal: tick interval elapsed, keep looping
                pass


class HeartbeatMonitor:
    """Run on STANDBY (and INACTIVE) nodes.

    Polls ``redis_client.heartbeat_ttl()``. If the heartbeat key has been
    missing for ``failure_timeout`` consecutive seconds (measured by the
    monotonic wall clock), fires ``on_primary_failed`` exactly once until
    a fresh heartbeat reappears. The clock used is ``time.monotonic`` so
    NTP jumps don't trip the failure path.

    Bootstrap nuance: if the monitor never sees a live heartbeat (i.e.
    the primary was already dead at startup), but has been alive for
    ``failure_timeout`` since ``start()``, that also counts as a
    detected failure. After firing, ``_failure_fired`` stays set until a
    fresh heartbeat is observed.
    """

    def __init__(
        self,
        redis_client: RedisClient,
        state_provider: StateProvider,
        node_id: str,
        poll_interval: float = 1.0,
        failure_timeout: float = 6.0,
        on_primary_failed: Optional[PrimaryFailedCallback] = None,
    ) -> None:
        self.redis: RedisClient = redis_client
        self._state_provider: StateProvider = state_provider
        self.node_id: str = node_id
        self.poll_interval: float = poll_interval
        self.failure_timeout: float = failure_timeout
        self._on_primary_failed: Optional[PrimaryFailedCallback] = on_primary_failed

        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event: asyncio.Event = asyncio.Event()

        # Time bookkeeping (monotonic clock).
        self._last_seen_at: Optional[float] = None
        self._started_at: float = time.monotonic()
        self._failure_fired: bool = False

        # Counter — exposed for the /metrics endpoint in commit 4a.
        self.primary_failures_detected_total: int = 0

    async def start(self) -> None:
        """Spawn the background poll task and reset the freshness clock."""
        self._stop_event.clear()
        self.reset()
        self._task = asyncio.create_task(self._run(), name="heartbeat-monitor")

    def reset(self) -> None:
        """Reset the freshness clock and failure flag.

        Called by ``start()`` and also by the FailoverNode whenever the
        state machine transitions back into STANDBY from PRIMARY/ELECTION.
        Without this, a stale ``_failure_fired=True`` from an earlier
        failure window would block re-detection if the node was briefly
        promoted and then demoted again — the monitor would silently
        ignore the next missing-heartbeat condition until a fresh
        heartbeat reappeared (which it might never, if the new primary
        also dies).
        """
        self._last_seen_at = None
        self._started_at = time.monotonic()
        self._failure_fired = False

    async def stop(self) -> None:
        """Signal the loop to exit and await its cancellation."""
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
                logger.exception("heartbeat monitor task raised on stop")

    async def check_once(self) -> Optional[float]:
        """Single poll.

        Returns:

        * ``None``        — monitoring was skipped (state isn't STANDBY/INACTIVE).
        * ``0.0``         — heartbeat is currently alive (TTL > 0).
        * a positive float — diagnostic "time since the last live read" or
          (during the bootstrap grace window) "time since this monitor
          started". Tests use only ``None`` vs non-``None``; the precise
          value is informational.
        * ``math.inf``    — never seen a heartbeat AND grace window elapsed.

        May fire ``on_primary_failed`` as a side effect once per failure
        window. Public for tests so they can drive ticks without spinning
        up the loop.
        """
        # Only STANDBY and INACTIVE nodes monitor the primary heartbeat.
        # PRIMARY emits, ELECTION is mid-promotion, FAILED is terminal —
        # none of those should trip the failure callback.
        state = self._state_provider()
        if state not in (NodeState.STANDBY, NodeState.INACTIVE):
            return None

        ttl = await self.redis.heartbeat_ttl()
        now = time.monotonic()

        if ttl > 0:
            # Heartbeat key is alive — record the sighting and clear the
            # firing flag so the next failure window can fire fresh.
            self._last_seen_at = now
            if self._failure_fired:
                logger.info(
                    "node %s saw fresh heartbeat after a failure window",
                    self.node_id,
                )
            self._failure_fired = False
            return 0.0

        # ttl == -2 (missing) or ttl == -1 (no expiry; shouldn't happen
        # but treat as no liveness signal). Don't update _last_seen_at.
        if self._last_seen_at is not None:
            time_since_last = now - self._last_seen_at
        elif now - self._started_at >= self.failure_timeout:
            # Bootstrap case: never seen a heartbeat AND we've been alive
            # long enough to assume the primary was dead at startup.
            time_since_last = math.inf
        else:
            # Still in the bootstrap grace window — give the primary
            # a chance to come up before declaring failure.
            return now - self._started_at

        if (
            time_since_last >= self.failure_timeout
            and not self._failure_fired
        ):
            self._failure_fired = True
            self.primary_failures_detected_total += 1
            logger.warning(
                "node %s detected primary failure (time_since_last=%.2fs >= %.2fs)",
                self.node_id,
                time_since_last,
                self.failure_timeout,
            )
            if self._on_primary_failed is not None:
                try:
                    await self._on_primary_failed()
                except Exception:
                    logger.exception("on_primary_failed callback raised")
        return time_since_last

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.check_once()
            except Exception:
                logger.exception("heartbeat monitor tick raised")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.poll_interval
                )
            except asyncio.TimeoutError:
                pass
