"""Top-level orchestrator: wires every component into one node.

:py:class:`FailoverNode` is the only place that knows about *all* the
moving parts. Each subsystem (state machine, heartbeat emitter, monitor,
election coordinator, log processor, HTTP app) is constructed with
narrow, dependency-injected interfaces, then linked together here.

Lifecycle
---------
* :py:meth:`start` — connect to Redis, decide initial state (PRIMARY if
  ``IS_PRIMARY=true`` *and* the lock is free, STANDBY otherwise), kick
  off the heartbeat emitter and monitor as background tasks. Note that
  the FastAPI server itself is NOT started here — that's the job of
  :py:mod:`src.__main__`, which wraps a uvicorn ``Server`` around
  :py:attr:`app`.
* :py:meth:`stop` — stop the loops in reverse order, release the lock
  if we hold it, close the peer/redis transports.

Callbacks
---------
The node injects three callbacks that close over its own state:

* ``on_lock_lost`` (PRIMARY → STANDBY when ``HeartbeatEmitter`` notices
  a renewal failure)
* ``on_primary_failed`` (STANDBY → ELECTION → PRIMARY/STANDBY when the
  monitor declares the heartbeat dead)
* ``on_manual_failover`` (PRIMARY releases the lock + demotes when the
  ``/admin/trigger-failover`` endpoint is hit)

Test injection
--------------
The constructor accepts pre-built ``redis_client`` and ``peer_client``
kwargs so tests can pass a fake-redis-backed client and a stub peer
client without monkeypatching. Production callers (``src.__main__``) leave
both as ``None`` and let the defaults run.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import FastAPI

from src.config import NodeConfig
from src.election import ElectionCoordinator
from src.heartbeat import HeartbeatEmitter, HeartbeatMonitor
from src.http_server import create_app
from src.log_processor import LogProcessor
from src.models import NodeState
from src.peer_client import HttpxPeerClient, PeerClient
from src.redis_client import RedisClient
from src.state_machine import NodeStateMachine

logger = logging.getLogger(__name__)


class FailoverNode:
    """Single-node orchestrator.

    Holds references to every subsystem and exposes the FastAPI app that
    uvicorn will serve. Tests can build a node with injected fakes and
    drive its callbacks directly without spinning up uvicorn or a real
    Redis.
    """

    def __init__(
        self,
        config: NodeConfig,
        *,
        redis_client: Optional[RedisClient] = None,
        peer_client: Optional[PeerClient] = None,
    ) -> None:
        self.config: NodeConfig = config
        self.redis_client: RedisClient = redis_client or RedisClient(
            host=config.redis_host,
            port=config.redis_port,
            node_id=config.node_id,
        )
        self.peer_client: PeerClient = peer_client or HttpxPeerClient()

        # State machine starts INACTIVE; start() decides the next move.
        self.state_machine: NodeStateMachine = NodeStateMachine(
            initial_state=NodeState.INACTIVE,
            node_id=config.node_id,
        )
        self.log_processor: LogProcessor = LogProcessor()

        self.election_coordinator: ElectionCoordinator = ElectionCoordinator(
            node_id=config.node_id,
            priority=config.priority(),
            peers=config.peer_list(),
            redis_client=self.redis_client,
            peer_client=self.peer_client,
            lock_ttl=config.lock_ttl,
            election_timeout=config.election_timeout,
        )

        self.heartbeat_emitter: HeartbeatEmitter = HeartbeatEmitter(
            redis_client=self.redis_client,
            state_provider=lambda: self.state_machine.state,
            metrics_provider=self._metrics_for_heartbeat,
            node_id=config.node_id,
            interval=config.heartbeat_interval,
            lock_ttl=config.lock_ttl,
            on_lock_lost=self._on_lock_lost,
        )

        self.heartbeat_monitor: HeartbeatMonitor = HeartbeatMonitor(
            redis_client=self.redis_client,
            state_provider=lambda: self.state_machine.state,
            node_id=config.node_id,
            poll_interval=1.0,
            failure_timeout=config.heartbeat_timeout,
            on_primary_failed=self._on_primary_failed,
        )

        self.app: FastAPI = create_app(
            config=config,
            state_machine=self.state_machine,
            log_processor=self.log_processor,
            election_coordinator=self.election_coordinator,
            heartbeat_emitter=self.heartbeat_emitter,
            heartbeat_monitor=self.heartbeat_monitor,
            redis_client=self.redis_client,
            on_manual_failover=self._on_manual_failover,
        )

        # Concurrency guard: we don't want two parallel elections from
        # this node when the monitor fires twice in quick succession.
        self._election_in_progress: asyncio.Lock = asyncio.Lock()
        self._stopping: bool = False

    # --- helper accessors ----------------------------------------------

    def _metrics_for_heartbeat(self) -> dict[str, float]:
        """Snapshot the metrics the primary publishes inside its heartbeat.

        ``logs_per_sec`` is held at 0.0 for now — commit 4b layers a
        rolling-window throughput meter on top.
        """
        return {
            "logs_per_sec": 0.0,
            "log_count": float(self.log_processor.log_count),
            "last_log_id": float(self.log_processor.last_log_id),
        }

    # --- callbacks ------------------------------------------------------

    async def _on_lock_lost(self) -> None:
        """Self-demote when the heartbeat emitter loses the leader lock."""
        if self.state_machine.state is NodeState.PRIMARY:
            await self.state_machine.transition_to(
                NodeState.STANDBY, reason="lock_renewal_failed"
            )

    async def _on_primary_failed(self) -> None:
        """Run an election when the monitor declares the primary dead.

        The election lock guarantees we don't fire two elections in
        parallel from the same node — a duplicate ``on_primary_failed``
        invocation is silently dropped while one is already running.

        After the election, transition to whichever state the coordinator
        decided (PRIMARY on win, STANDBY on loss/timeout). If state has
        already drifted (e.g. someone else demoted us) we skip the run
        entirely.
        """
        if self._election_in_progress.locked():
            logger.info(
                "skipping primary-failure election: another election already in progress"
            )
            return

        async with self._election_in_progress:
            # Re-check state under the lock — it may have flipped while
            # we were waiting (e.g. a parallel `_on_lock_lost`).
            if self.state_machine.state is not NodeState.STANDBY:
                logger.info(
                    "skipping primary-failure election: state is %s, not STANDBY",
                    self.state_machine.state.value,
                )
                return

            await self.state_machine.transition_to(
                NodeState.ELECTION, reason="primary_failure_detected"
            )
            new_state = await self.election_coordinator.run_election()
            await self.state_machine.transition_to(
                new_state, reason="election_completed"
            )

    async def _on_manual_failover(self) -> None:
        """Release the lock and self-demote on /admin/trigger-failover.

        Standbys will detect the missing heartbeat within ~6s and
        promote one of themselves via the normal election flow. We
        deliberately do NOT wait for that to complete here — the HTTP
        endpoint returns 202 immediately so the operator gets prompt
        feedback.
        """
        if self.state_machine.state is NodeState.PRIMARY:
            await self.redis_client.release_lock()
            await self.state_machine.transition_to(
                NodeState.STANDBY, reason="manual_failover"
            )

    # --- lifecycle ------------------------------------------------------

    async def start(self) -> None:
        """Bring the node online and start the background loops.

        Decision tree:

        * ``IS_PRIMARY=true`` and lock is free → take it, transition to PRIMARY.
        * ``IS_PRIMARY=true`` but lock already held → start as STANDBY.
        * ``IS_PRIMARY=false`` → start as STANDBY unconditionally.

        After the initial state is set, the heartbeat emitter and
        monitor both start. They no-op when the current state doesn't
        warrant their work (emitter idles unless PRIMARY; monitor idles
        when PRIMARY/ELECTION/FAILED), so it's safe to leave both
        running for the entire node lifetime.
        """
        await self.redis_client.connect()

        if self.config.is_primary:
            won = await self.redis_client.acquire_lock(ttl=self.config.lock_ttl)
            if won:
                await self.state_machine.transition_to(
                    NodeState.PRIMARY, reason="initial_bootstrap_primary"
                )
            else:
                await self.state_machine.transition_to(
                    NodeState.STANDBY, reason="initial_lock_held_by_other"
                )
        else:
            await self.state_machine.transition_to(
                NodeState.STANDBY, reason="initial_bootstrap_standby"
            )

        await self.heartbeat_emitter.start()
        await self.heartbeat_monitor.start()

    async def stop(self) -> None:
        """Tear the node down cleanly.

        Stop background loops first so they don't observe a closed Redis
        client mid-tick; release the lock if we still hold it; close
        transports last. ``stop()`` is safe to call multiple times.
        """
        self._stopping = True
        await self.heartbeat_emitter.stop()
        await self.heartbeat_monitor.stop()
        if self.state_machine.state is NodeState.PRIMARY:
            try:
                await self.redis_client.release_lock()
            except Exception:
                logger.exception("release_lock raised inside stop()")
        try:
            await self.peer_client.close()
        except Exception:
            logger.exception("peer_client.close raised inside stop()")
        try:
            await self.redis_client.close()
        except Exception:
            logger.exception("redis_client.close raised inside stop()")
