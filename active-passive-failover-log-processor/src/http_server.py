"""FastAPI application factory for a single failover node.

This module is *only* an app factory — it never instantiates the
underlying components. Construction happens in :py:class:`src.node.FailoverNode`,
which wires every concrete implementation and passes them in through
:py:func:`create_app`. The factory pattern keeps the HTTP surface
trivially testable: every route reaches its dependency through
``request.app.state``, so unit tests just construct fakes and pass them
to ``create_app``.

Wire format
-----------
Routes that consume ``HeartbeatMessage`` / ``ElectionMessage`` /
``ElectionResult`` JSON bodies do so via :func:`src.models.from_json`,
because that's the canonical wire format used by inter-node traffic.
We deliberately do NOT use FastAPI's pydantic body inference for
those routes — the rest of the codebase already speaks orjson + dataclasses
and we don't want a second representation drifting from the first.

Server-generated bodies (``/health``, ``/role``, ``/logs`` GET, etc.)
go through ``JSONResponse`` for convenience; there's no roundtrip
dataclass requirement on responses.

Metrics format
--------------
``GET /metrics`` returns Prometheus exposition text — one ``# HELP``
line, one ``# TYPE`` line, then the ``name{node_id="..."} value`` pair
per counter. ``node_state`` is a gauge with one line per possible state,
where the matching state's value is ``1`` and every other line is ``0``.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse

from src.config import NodeConfig
from src.election import ElectionCoordinator
from src.heartbeat import HeartbeatEmitter, HeartbeatMonitor
from src.log_processor import LogProcessor
from src.models import (
    ElectionMessage,
    ElectionResult,
    HeartbeatMessage,
    NodeState,
    from_json,
)
from src.redis_client import RedisClient
from src.state_machine import NodeStateMachine

logger = logging.getLogger(__name__)


# Counter (name, help) pairs for /metrics. Order is preserved so the
# exposition output is stable for diff-friendly snapshots.
_COUNTER_DEFS: list[tuple[str, str]] = [
    ("heartbeats_emitted_total", "Number of heartbeats emitted by this node."),
    ("lock_renewal_failures_total", "Number of lock renewal failures observed by this node."),
    ("primary_failures_detected_total", "Number of times this node detected a primary failure."),
    ("elections_run_total", "Number of elections this node has initiated."),
    ("elections_won_total", "Number of elections this node has won."),
    ("elections_lost_total", "Number of elections this node has lost."),
    ("elections_timed_out_total", "Number of elections that hit the election timeout."),
    ("candidacies_received_total", "Number of candidacy messages received from peers."),
    ("results_received_total", "Number of election result messages received from peers."),
    ("logs_ingested_total", "Number of log entries accepted and stored on this node."),
    ("logs_rejected_total", "Number of log ingest attempts rejected (not primary)."),
]


def create_app(
    config: NodeConfig,
    state_machine: NodeStateMachine,
    log_processor: LogProcessor,
    election_coordinator: ElectionCoordinator,
    heartbeat_emitter: HeartbeatEmitter,
    heartbeat_monitor: HeartbeatMonitor,
    redis_client: RedisClient,
    on_manual_failover: Callable[[], Awaitable[None]],
) -> FastAPI:
    """Build the FastAPI app for one node.

    Every dependency is injected and stashed on ``app.state`` so route
    handlers can pull them out via ``request.app.state``. This keeps
    each route function trivial and the factory itself a pure wiring
    layer.
    """
    app = FastAPI(title=f"failover-node-{config.node_id}")

    # Stash dependencies on app.state — accessed by every route.
    app.state.config = config
    app.state.state_machine = state_machine
    app.state.log_processor = log_processor
    app.state.election_coordinator = election_coordinator
    app.state.heartbeat_emitter = heartbeat_emitter
    app.state.heartbeat_monitor = heartbeat_monitor
    app.state.redis_client = redis_client
    app.state.on_manual_failover = on_manual_failover

    # =====================================================================
    # GET /health — primary-only liveness probe.
    # =====================================================================
    @app.get("/health")
    async def health(request: Request) -> JSONResponse:
        sm: NodeStateMachine = request.app.state.state_machine
        cfg: NodeConfig = request.app.state.config
        body = {
            "status": "healthy" if sm.state is NodeState.PRIMARY else "unhealthy",
            "state": sm.state.value,
            "node_id": cfg.node_id,
        }
        code = (
            status.HTTP_200_OK
            if sm.state is NodeState.PRIMARY
            else status.HTTP_503_SERVICE_UNAVAILABLE
        )
        return JSONResponse(body, status_code=code)

    # =====================================================================
    # GET /role — always-200 introspection. Used by the dashboard and tests.
    # =====================================================================
    @app.get("/role")
    async def role(request: Request) -> JSONResponse:
        sm: NodeStateMachine = request.app.state.state_machine
        cfg: NodeConfig = request.app.state.config
        coord: ElectionCoordinator = request.app.state.election_coordinator
        rc: RedisClient = request.app.state.redis_client

        # read_lock_holder is best-effort; if Redis is flaky we still want
        # /role to respond rather than blow up.
        try:
            lock_holder = await rc.read_lock_holder()
        except Exception:
            logger.exception("read_lock_holder raised inside /role")
            lock_holder = None

        body = {
            "node_id": cfg.node_id,
            "state": sm.state.value,
            "role": sm.role,
            "lock_holder": lock_holder,
            "known_winner": coord.known_winner,
            "term": coord.current_term,
        }
        return JSONResponse(body, status_code=status.HTTP_200_OK)

    # =====================================================================
    # GET /metrics — Prometheus exposition.
    # =====================================================================
    @app.get("/metrics")
    async def metrics(request: Request) -> PlainTextResponse:
        cfg: NodeConfig = request.app.state.config
        emitter: HeartbeatEmitter = request.app.state.heartbeat_emitter
        monitor: HeartbeatMonitor = request.app.state.heartbeat_monitor
        coord: ElectionCoordinator = request.app.state.election_coordinator
        lp: LogProcessor = request.app.state.log_processor
        sm: NodeStateMachine = request.app.state.state_machine

        # Pull every counter in one shot so tests don't see partial state
        # from a concurrent mutation between reads.
        values: dict[str, int] = {
            "heartbeats_emitted_total": emitter.heartbeats_emitted_total,
            "lock_renewal_failures_total": emitter.lock_renewal_failures_total,
            "primary_failures_detected_total": monitor.primary_failures_detected_total,
            "elections_run_total": coord.elections_run_total,
            "elections_won_total": coord.elections_won_total,
            "elections_lost_total": coord.elections_lost_total,
            "elections_timed_out_total": coord.elections_timed_out_total,
            "candidacies_received_total": coord.candidacies_received_total,
            "results_received_total": coord.results_received_total,
            "logs_ingested_total": lp.logs_ingested_total,
            "logs_rejected_total": lp.logs_rejected_total,
        }

        lines: list[str] = []
        for name, help_text in _COUNTER_DEFS:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} counter")
            lines.append(f'{name}{{node_id="{cfg.node_id}"}} {values[name]}')

        # node_state gauge: one line per possible state, value = 1 iff
        # current state matches. This is the standard "info gauge" pattern
        # used by node_exporter and friends.
        lines.append("# HELP node_state Current node lifecycle state (1 = active).")
        lines.append("# TYPE node_state gauge")
        for state in NodeState:
            value = 1 if sm.state is state else 0
            lines.append(
                f'node_state{{node_id="{cfg.node_id}",state="{state.value}"}} {value}'
            )

        body = "\n".join(lines) + "\n"
        return PlainTextResponse(body, status_code=status.HTTP_200_OK)

    # =====================================================================
    # POST /logs — primary-only ingest.
    # =====================================================================
    @app.post("/logs")
    async def post_logs(request: Request) -> JSONResponse:
        sm: NodeStateMachine = request.app.state.state_machine
        lp: LogProcessor = request.app.state.log_processor

        if sm.state is not NodeState.PRIMARY:
            lp.reject()
            return JSONResponse(
                {"status": "rejected", "reason": "not_primary"},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        # Body parsing — accept either a structured JSON body or fail
        # cleanly. We don't bind to a pydantic model because the schema
        # is small and we want consistent 400 handling on parse errors.
        try:
            payload = await request.json()
        except Exception:
            return JSONResponse(
                {"status": "error", "reason": "invalid_json"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        if not isinstance(payload, dict):
            return JSONResponse(
                {"status": "error", "reason": "invalid_body"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        message = payload.get("message")
        if not isinstance(message, str):
            return JSONResponse(
                {"status": "error", "reason": "missing_message"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        level_value = payload.get("level", "INFO")
        level = level_value if isinstance(level_value, str) else "INFO"
        log_id_value = payload.get("log_id")
        log_id: int | None
        if log_id_value is None:
            log_id = None
        elif isinstance(log_id_value, int) and not isinstance(log_id_value, bool):
            log_id = log_id_value
        else:
            return JSONResponse(
                {"status": "error", "reason": "invalid_log_id"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        entry = lp.ingest(message=message, level=level, log_id=log_id)
        return JSONResponse(
            {"status": "accepted", "log_id": entry.log_id},
            status_code=status.HTTP_201_CREATED,
        )

    # =====================================================================
    # GET /logs — primary-only paged read.
    # =====================================================================
    @app.get("/logs")
    async def get_logs(request: Request) -> JSONResponse:
        sm: NodeStateMachine = request.app.state.state_machine
        lp: LogProcessor = request.app.state.log_processor

        if sm.state is not NodeState.PRIMARY:
            return JSONResponse(
                {"status": "rejected", "reason": "not_primary"},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        # Parse limit ourselves rather than via Query() so a bogus value
        # produces our standard 400 shape rather than FastAPI's default.
        raw_limit = request.query_params.get("limit", "100")
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            return JSONResponse(
                {"status": "error", "reason": "invalid_limit"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        entries = lp.get_recent(limit=limit)
        body = {
            "logs": [
                {
                    "log_id": e.log_id,
                    "message": e.message,
                    "level": e.level,
                    "timestamp": e.timestamp,
                }
                for e in entries
            ],
            "count": lp.log_count,
            "last_log_id": lp.last_log_id,
        }
        return JSONResponse(body, status_code=status.HTTP_200_OK)

    # =====================================================================
    # POST /admin/trigger-failover — primary-only, async-launched callback.
    # =====================================================================
    @app.post("/admin/trigger-failover")
    async def trigger_failover(request: Request) -> JSONResponse:
        sm: NodeStateMachine = request.app.state.state_machine
        if sm.state is not NodeState.PRIMARY:
            return JSONResponse(
                {"status": "rejected", "reason": "not_primary"},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        cb = request.app.state.on_manual_failover
        await cb()
        return JSONResponse(
            {"status": "failover_triggered"},
            status_code=status.HTTP_202_ACCEPTED,
        )

    # =====================================================================
    # POST /heartbeat — debug ping. Real heartbeat goes via Redis.
    # =====================================================================
    @app.post("/heartbeat")
    async def receive_heartbeat(request: Request) -> JSONResponse:
        body = await request.body()
        try:
            from_json(HeartbeatMessage, body)
        except Exception:
            return JSONResponse(
                {"status": "error", "reason": "invalid_heartbeat"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        return JSONResponse({"status": "ok"}, status_code=status.HTTP_200_OK)

    # =====================================================================
    # POST /election/candidacy — receive-side handler for ElectionMessage.
    # =====================================================================
    @app.post("/election/candidacy")
    async def receive_candidacy(request: Request) -> JSONResponse:
        coord: ElectionCoordinator = request.app.state.election_coordinator
        body = await request.body()
        try:
            msg = from_json(ElectionMessage, body)
        except Exception:
            return JSONResponse(
                {"status": "error", "reason": "invalid_candidacy"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        await coord.handle_candidacy(msg)
        return JSONResponse({"status": "ok"}, status_code=status.HTTP_200_OK)

    # =====================================================================
    # POST /election/result — receive-side handler for ElectionResult.
    # =====================================================================
    @app.post("/election/result")
    async def receive_election_result(request: Request) -> JSONResponse:
        coord: ElectionCoordinator = request.app.state.election_coordinator
        body = await request.body()
        try:
            result = from_json(ElectionResult, body)
        except Exception:
            return JSONResponse(
                {"status": "error", "reason": "invalid_result"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        await coord.handle_election_result(result)
        return JSONResponse({"status": "ok"}, status_code=status.HTTP_200_OK)

    return app


__all__ = ["create_app"]
