"""Redis-backed checkpointing for the sliding window analytics engine.

Every ``interval_seconds`` (default 10s) the current
:class:`WindowManager` state is serialised to JSON and written to a
single Redis key. On startup the key is read, validated (via a
freshness timestamp), and replayed into the :class:`WindowManager` so
window statistics survive ``app`` container restarts without losing
trend continuity.

The checkpoint key is intentionally a single JSON blob rather than
individual hash fields: windows are small (bounded by ``max_size``
events each), so (de)serialising the whole thing is simpler than
piecewise updates and avoids partial-restore races on crash.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import redis.asyncio as aioredis

from src.window_manager import WindowManager

logger = logging.getLogger(__name__)

CHECKPOINT_KEY = "sliding_window:checkpoint:v1"


class CheckpointStore:
    """Async Redis wrapper for saving/loading :class:`WindowManager` snapshots.

    Wraps a single ``redis.asyncio.Redis`` client. All I/O is
    best-effort — if Redis is unreachable we log and continue; the
    in-memory window state is still the authoritative source.
    """

    def __init__(
        self,
        host: str,
        port: int,
        key: str = CHECKPOINT_KEY,
        max_age_seconds: float = 3600.0,
    ) -> None:
        self._host = host
        self._port = port
        self._key = key
        self._max_age = max_age_seconds
        self._client: aioredis.Redis | None = None

    @property
    def client(self) -> aioredis.Redis | None:
        """Expose the underlying client for tests (and for direct injection)."""
        return self._client

    @client.setter
    def client(self, value: aioredis.Redis | None) -> None:
        self._client = value

    async def connect(self) -> None:
        """Create the Redis client and send an initial ping.

        A failed ping is logged but not raised — the checkpoint loop
        will keep trying on each interval, and the app continues to
        serve traffic regardless of Redis availability.
        """
        self._client = aioredis.Redis(
            host=self._host,
            port=self._port,
            decode_responses=True,
            socket_connect_timeout=2.0,
            socket_timeout=2.0,
        )
        try:
            await self._client.ping()
        except Exception as exc:
            logger.warning("redis ping failed on connect: %s", exc)

    async def close(self) -> None:
        """Close the Redis client, swallowing any errors during teardown."""
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:  # pragma: no cover - defensive
                pass
            self._client = None

    async def save(self, window_manager: WindowManager) -> bool:
        """Serialize and write the window manager state.

        Returns ``True`` on a successful write, ``False`` on any
        error (no client, redis unreachable, serialisation failure).
        """
        if self._client is None:
            return False
        payload = {
            "saved_at": time.time(),
            "state": window_manager.state_dict(),
        }
        try:
            await self._client.set(self._key, json.dumps(payload))
            return True
        except Exception as exc:
            logger.warning("checkpoint save failed: %s", exc)
            return False

    async def load(self, window_manager: WindowManager) -> int:
        """Read the most recent checkpoint and apply it.

        Returns the number of windows restored (0 if no checkpoint,
        stale checkpoint, malformed payload, or an error during load).
        """
        if self._client is None:
            return 0
        try:
            raw = await self._client.get(self._key)
        except Exception as exc:
            logger.warning("checkpoint load failed: %s", exc)
            return 0
        if raw is None:
            return 0
        try:
            payload = json.loads(raw)
        except Exception:
            logger.warning("checkpoint payload was not valid JSON; ignoring")
            return 0
        if not isinstance(payload, dict):
            return 0
        saved_at = float(payload.get("saved_at", 0.0))
        age = time.time() - saved_at
        if age > self._max_age:
            logger.info(
                "checkpoint is stale (age=%.1fs); discarding", age
            )
            return 0
        state = payload.get("state", {})
        if not isinstance(state, dict):
            return 0
        return window_manager.load_state(state)


async def checkpoint_loop(
    store: CheckpointStore,
    window_manager: WindowManager,
    interval_seconds: float,
    stop_event: asyncio.Event,
) -> None:
    """Periodically save the window manager state until ``stop_event`` is set.

    Uses :func:`asyncio.wait_for` on the stop event with a timeout
    equal to ``interval_seconds``, so the loop reacts promptly to
    shutdown without needing an explicit ``asyncio.sleep`` cancel path.
    """
    while not stop_event.is_set():
        try:
            await store.save(window_manager)
        except Exception:
            logger.exception("checkpoint_loop save failed")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            # Expected path — interval elapsed without shutdown, loop again.
            pass
