"""FastAPI application + REST surface for the Real-Time Log Pattern Clustering engine.

Commit 11 turns the C1 foundation into the real HTTP API the dashboard drives. On
startup (the FastAPI **lifespan**) the app:

1. Loads the resolved :class:`~src.config.AppConfig` into ``app.state.config``.
2. Constructs the :class:`~src.engine.ClusteringEngine` and **warms it up synchronously**
   on a historical batch (loaded via :func:`src.demo.load_corpus`, which falls back to
   generated logs when the committed corpus is absent — e.g. in the app image). The app
   only reports ready once warm-up completes; this fits comfortably inside the Docker
   healthcheck's ``start_period``. Tests shrink / bypass this via ``create_app``'s
   ``warmup_logs`` / ``warmup_n`` parameters.
3. Creates a :class:`~src.state.StateStore` (Redis when reachable, else in-memory — it
   never raises) on ``app.state.state_store`` and best-effort persists the initial stats
   snapshot.

The clustering handlers are **synchronous** ``def`` functions so Starlette runs them in
its threadpool: the engine is internally thread-safe (one ``RLock``) and the sklearn work
is CPU-bound, so keeping it off the event loop preserves concurrency. Endpoints guard with
``503`` until the engine is warmed.

Routes:

* ``GET  /health``                          — liveness + readiness (warming/ok).
* ``POST /cluster``                          — cluster one log.
* ``POST /cluster/batch``                    — cluster a batch of logs.
* ``GET  /stats``                            — aggregate engine statistics.
* ``GET  /clusters``                         — per-algorithm cluster summaries (all algos).
* ``GET  /clusters/{algorithm}``             — one algorithm's cluster summaries.
* ``GET  /clusters/{algorithm}/{cluster_id}``— drill-down detail for one cluster.
* ``GET  /patterns``                         — discovered patterns.
* ``GET  /patterns/temporal``                — batch-mined recurring temporal patterns (C18).
* ``GET  /patterns/performance``             — batch-mined latency bands + bottlenecks (C18).
* ``GET  /anomalies``                        — recent anomaly alerts.
* ``GET  /scatter/{algorithm}``              — 2-D scatter points for the dashboard.
* ``GET  /config``                           — the resolved configuration.
* ``WS   /ws/stream``                        — live snapshot stream for the dashboard (C12).
* ``GET  /``                                 — service banner.

Commit 12 adds the live layer on top of the C11 REST surface: a :class:`ConnectionManager`,
the ``/ws/stream`` WebSocket, and **one** background task launched in the lifespan that every
``broadcast_interval`` seconds (a) fans a stats/quality/patterns/anomalies snapshot out to all
connected dashboards and best-effort persists it, and (b) triggers the engine's periodic
sliding-window refit when due — running that CPU-bound, synchronous refit in a threadpool
executor so the event loop is never blocked. The broadcaster survives transient errors so the
live feed keeps running.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.config import AppConfig, load_config
from src.demo import load_corpus
from src.engine import ClusteringEngine
from src.metrics import ConnectionManager, build_snapshot_payload
from src.patterns.performance import mine_performance_patterns
from src.patterns.temporal import mine_temporal_patterns
from src.schemas import (
    AnomalyAlert,
    ClusterAssignment,
    HealthResponse,
    LogEntry,
    PatternRecord,
    StatsSnapshot,
)
from src.state import create_state_store

logger = logging.getLogger(__name__)

#: Service version reported by ``/health``. Bump as the engine evolves.
APP_VERSION = "0.1.0"


# --------------------------------------------------------------------------- #
# Request bodies (defined here so schemas.py stays the pure data contract)
# --------------------------------------------------------------------------- #


class ClusterBatchRequest(BaseModel):
    """Body for ``POST /cluster/batch``: a list of logs to cluster in one call."""

    logs: list[LogEntry]


# --------------------------------------------------------------------------- #
# Lifespan: load config, warm the engine, wire the state store
# --------------------------------------------------------------------------- #


def _build_lifespan(
    config: AppConfig | None,
    warmup_logs: "list | None",
    warmup_n: int,
    broadcast_interval: float,
):
    """Build the lifespan context manager, capturing the warm-up / broadcast parameters.

    Keeping the parameters in a closure (rather than reading globals) lets ``create_app``
    spin up independent apps — with different warm-up batches and broadcast cadences — in the
    same process, which is exactly what the test suite needs.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 1) Resolve config (honours CONFIG_PATH / env overrides) unless one was injected.
        cfg = config if config is not None else load_config()
        app.state.config = cfg

        # 2) Build + warm the engine SYNCHRONOUSLY so the app only serves once ready.
        engine = ClusteringEngine(cfg)
        if warmup_logs is not None:
            corpus = list(warmup_logs)
            batch = corpus
        else:
            # load_corpus falls back to generated logs when the corpus file is absent
            # (e.g. the minimal app image), so warm-up always has data.
            corpus = load_corpus(fallback_n=warmup_n)
            batch = corpus[:warmup_n]
        engine.warm_up(batch)
        app.state.engine = engine
        # Keep the warm-up corpus around so the batch pattern-mining endpoints
        # (/patterns/temporal, /patterns/performance) can mine the same logs the engine warmed
        # on. (When an explicit warmup batch was injected, that *is* the corpus.)
        app.state.corpus = corpus

        # 3) State store (never raises; degrades to in-memory). Persist an initial
        #    snapshot best-effort so a dashboard reading Redis directly has something.
        store = create_state_store(cfg)
        app.state.state_store = store
        try:
            store.save_stats(engine.stats_snapshot().model_dump(mode="json"))
        except Exception:  # noqa: BLE001 - persistence is best-effort, never fatal
            pass

        # 4) Live layer: one WebSocket registry + one background broadcaster task that
        #    fans snapshots out, persists them, and triggers periodic refits when due.
        app.state.manager = ConnectionManager()
        app.state._bcast_task = asyncio.create_task(
            _broadcast_loop(app, broadcast_interval)
        )

        yield

        # Shutdown: stop the broadcaster (await it, swallowing the cancellation) before
        # releasing the Redis client so no in-flight persist/refit outlives the app.
        task: asyncio.Task = app.state._bcast_task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        app.state.state_store.close()

    return lifespan


async def _broadcast_loop(app: FastAPI, interval: float) -> None:
    """Periodically broadcast a live snapshot and trigger refits — the single live task.

    Runs for the app's lifetime (cancelled on shutdown). Every ``interval`` seconds it:

    1. Builds the live payload via :func:`~src.metrics.build_snapshot_payload` and broadcasts
       it (as one JSON string) to every connected dashboard.
    2. Best-effort persists the stats + patterns to the state store (so a dashboard reading
       Redis directly stays fresh).
    3. If :meth:`~src.engine.ClusteringEngine.should_refit` is due, runs the **synchronous**,
       CPU-bound :meth:`~src.engine.ClusteringEngine.refit` in the default threadpool executor
       via ``loop.run_in_executor`` — so the event loop (and other connections) are never
       blocked by the sklearn re-fit.

    The whole body is wrapped so any transient error is logged and the loop continues: the
    live feed must never die because of one bad cycle. Only :class:`asyncio.CancelledError`
    (raised on shutdown) breaks out.

    Args:
        app: The FastAPI app whose ``state`` holds the engine / manager / state store.
        interval: Seconds to sleep between broadcasts (tests shrink this for speed).
    """
    loop = asyncio.get_running_loop()
    manager: ConnectionManager = app.state.manager
    engine: ClusteringEngine = app.state.engine
    state_store = app.state.state_store

    while True:
        await asyncio.sleep(interval)
        try:
            payload = build_snapshot_payload(engine)
            await manager.broadcast(json.dumps(payload, default=str))

            # Persist best-effort (the store never raises, but guard anyway).
            try:
                state_store.save_stats(payload["stats"])
                state_store.save_patterns(payload["patterns"])
            except Exception:  # noqa: BLE001 - persistence is best-effort, never fatal
                logger.debug("broadcast loop: state persist failed", exc_info=True)

            # Run the CPU-bound, synchronous refit off the event loop when due.
            if engine.should_refit():
                await loop.run_in_executor(None, engine.refit)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - one bad cycle must not kill the live feed
            logger.warning("broadcast loop cycle failed; continuing", exc_info=True)


# --------------------------------------------------------------------------- #
# Dependencies
# --------------------------------------------------------------------------- #


def _get_engine(request: Request) -> ClusteringEngine:
    """Return the warmed engine, or raise ``503`` while it is still warming up.

    Used by every data endpoint (not ``/health`` or ``/``) so a request that arrives before
    warm-up finishes gets a clean ``503`` instead of an ``AttributeError``.
    """
    engine: ClusteringEngine | None = getattr(request.app.state, "engine", None)
    if engine is None or not engine.is_warmed:
        raise HTTPException(status_code=503, detail="engine warming up")
    return engine


def _require_algorithm(algorithm: str) -> str:
    """Validate ``algorithm`` against the engine's set, raising ``404`` if unknown."""
    if algorithm not in ClusteringEngine.ALGORITHMS:
        raise HTTPException(
            status_code=404,
            detail=(
                f"unknown algorithm {algorithm!r}; "
                f"expected one of {list(ClusteringEngine.ALGORITHMS)}"
            ),
        )
    return algorithm


# --------------------------------------------------------------------------- #
# App factory
# --------------------------------------------------------------------------- #


def create_app(
    config: AppConfig | None = None,
    warmup_logs: "list | None" = None,
    warmup_n: int = 600,
    broadcast_interval: float = 1.5,
) -> FastAPI:
    """Build and return the FastAPI application.

    Args:
        config: Optional pre-loaded :class:`~src.config.AppConfig`. When ``None`` the
            config is resolved from defaults/YAML/env in the lifespan.
        warmup_logs: Optional explicit warm-up batch (a list of
            :class:`~src.schemas.LogEntry` or dicts). When provided the engine warms up on
            exactly these logs and the corpus is **not** loaded — tests pass a small batch
            here to keep startup fast. When ``None`` the corpus (or its generated fallback)
            is loaded and the first ``warmup_n`` logs are used.
        warmup_n: Number of corpus logs to warm up on when ``warmup_logs`` is ``None``.
        broadcast_interval: Seconds between live-snapshot broadcasts on ``/ws/stream`` (also
            the periodic-refit poll cadence). Defaults to ``1.5``; tests shrink it for speed.

    Returns:
        A configured :class:`fastapi.FastAPI` instance. The engine, config, state store,
        WebSocket :class:`~src.metrics.ConnectionManager` and the background broadcaster task
        are attached to ``app.state`` during the lifespan (so they spin up when the app is
        entered as a context / served by uvicorn or wrapped in ``TestClient(app)``).
    """
    app = FastAPI(
        title="Real-Time Log Pattern Clustering",
        version=APP_VERSION,
        lifespan=_build_lifespan(config, warmup_logs, warmup_n, broadcast_interval),
    )

    # Permissive CORS so the dashboard (different origin) can call the API directly.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ----------------------------------------------------------------- health/root

    @app.get("/health", response_model=HealthResponse)
    def health(request: Request) -> HealthResponse:
        """Liveness + readiness probe. ``ok`` once warmed, ``warming`` until then."""
        engine: ClusteringEngine | None = getattr(request.app.state, "engine", None)
        warmed = engine is not None and engine.is_warmed
        return HealthResponse(
            status="ok" if warmed else "warming",
            version=APP_VERSION,
            algorithms=list(ClusteringEngine.ALGORITHMS),
        )

    @app.get("/")
    def root() -> dict:
        """Tiny service banner pointing at the interactive docs."""
        return {"service": "real-time-log-pattern-clustering", "docs": "/docs"}

    # --------------------------------------------------------------- clustering

    @app.post("/cluster", response_model=ClusterAssignment)
    def cluster(log: LogEntry, engine: ClusteringEngine = Depends(_get_engine)) -> ClusterAssignment:
        """Cluster a single log across all algorithms and return the combined verdict."""
        return engine.process(log)

    @app.post("/cluster/batch", response_model=list[ClusterAssignment])
    def cluster_batch(
        req: ClusterBatchRequest, engine: ClusteringEngine = Depends(_get_engine)
    ) -> list[ClusterAssignment]:
        """Cluster a batch of logs (throughput path). An empty list returns ``[]``."""
        if not req.logs:
            return []
        return engine.process_batch(req.logs)

    # -------------------------------------------------------------------- stats

    @app.get("/stats", response_model=StatsSnapshot)
    def stats(engine: ClusteringEngine = Depends(_get_engine)) -> StatsSnapshot:
        """Return the current aggregate engine statistics for the dashboard stat cards."""
        return engine.stats_snapshot()

    # ----------------------------------------------------------------- clusters

    @app.get("/clusters")
    def clusters(engine: ClusteringEngine = Depends(_get_engine)) -> dict:
        """Return per-cluster summaries keyed by algorithm (all three algorithms)."""
        return {
            algorithm: engine.get_clusters(algorithm)
            for algorithm in ClusteringEngine.ALGORITHMS
        }

    @app.get("/clusters/{algorithm}")
    def clusters_for_algorithm(
        algorithm: str, engine: ClusteringEngine = Depends(_get_engine)
    ) -> list:
        """Return one algorithm's cluster summaries (``404`` if the algorithm is unknown)."""
        _require_algorithm(algorithm)
        return engine.get_clusters(algorithm)

    @app.get("/clusters/{algorithm}/{cluster_id}")
    def cluster_detail(
        algorithm: str,
        cluster_id: int,
        engine: ClusteringEngine = Depends(_get_engine),
    ) -> dict:
        """Drill-down detail for one cluster (``cluster_id`` may be ``-1`` for noise)."""
        _require_algorithm(algorithm)
        return engine.get_cluster_detail(algorithm, cluster_id)

    # ----------------------------------------------------------------- patterns

    @app.get("/patterns", response_model=list[PatternRecord])
    def patterns(engine: ClusteringEngine = Depends(_get_engine)) -> list[PatternRecord]:
        """Return every discovered pattern (count descending)."""
        return engine.get_patterns()

    # --------------------------------------------------- batch pattern mining (C18)

    @app.get("/patterns/temporal")
    def patterns_temporal(
        request: Request, engine: ClusteringEngine = Depends(_get_engine)
    ) -> list:
        """Mine the warm-up corpus for recurring temporal patterns.

        Groups the loaded corpus by hour-of-day / weekday and surfaces patterns such as the
        nightly 02:00 error spike, weekday service bursts, business-hours performance
        degradation, and hourly volume peaks. Returns ``503`` while the engine is warming (via
        the dependency) or if the corpus is unavailable.
        """
        corpus = getattr(request.app.state, "corpus", None)
        if not corpus:
            raise HTTPException(status_code=503, detail="corpus not loaded")
        return mine_temporal_patterns(corpus)

    @app.get("/patterns/performance")
    def patterns_performance(
        request: Request, engine: ClusteringEngine = Depends(_get_engine)
    ) -> dict:
        """Mine the warm-up corpus for performance patterns: latency bands + bottlenecks.

        Clusters response times into fast/normal/slow/critical bands and flags the slowest
        services/endpoints as bottleneck signatures. Returns ``503`` while the engine is warming
        (via the dependency) or if the corpus is unavailable.
        """
        corpus = getattr(request.app.state, "corpus", None)
        if not corpus:
            raise HTTPException(status_code=503, detail="corpus not loaded")
        return mine_performance_patterns(corpus)

    # ---------------------------------------------------------------- anomalies

    @app.get("/anomalies", response_model=list[AnomalyAlert])
    def anomalies(
        limit: int = Query(default=50, ge=0),
        engine: ClusteringEngine = Depends(_get_engine),
    ) -> list[AnomalyAlert]:
        """Return the most recent anomaly alerts (newest first), capped at ``limit``."""
        return engine.get_anomalies(limit)

    # ------------------------------------------------------------------ scatter

    @app.get("/scatter/{algorithm}")
    def scatter(
        algorithm: str,
        limit: int = Query(default=500, ge=0),
        engine: ClusteringEngine = Depends(_get_engine),
    ) -> list:
        """Return recent buffered points projected to 2-D, coloured by ``algorithm``."""
        _require_algorithm(algorithm)
        return engine.scatter_points(algorithm, limit)

    # --------------------------------------------------------------- websocket

    @app.websocket("/ws/stream")
    async def ws_stream(websocket: WebSocket) -> None:
        """Live snapshot stream for the dashboard.

        On connect the client is registered and immediately sent the current snapshot (so the
        UI paints without waiting a full broadcast interval); thereafter the single background
        broadcaster pushes a fresh snapshot every ``broadcast_interval`` seconds. Inbound
        frames are read and ignored (a keepalive channel) until the client disconnects, at
        which point the socket is deregistered.
        """
        manager: ConnectionManager = websocket.app.state.manager
        await manager.connect(websocket)
        try:
            # Immediate first paint — don't make the dashboard wait for the next tick.
            initial = build_snapshot_payload(websocket.app.state.engine)
            await websocket.send_text(json.dumps(initial, default=str))
            while True:
                # Keepalive: ignore whatever the client sends; we only push.
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            manager.disconnect(websocket)

    # ------------------------------------------------------------------- config

    @app.get("/config")
    def get_config(request: Request) -> dict:
        """Return the resolved application configuration as JSON."""
        cfg: AppConfig = request.app.state.config
        return cfg.model_dump(mode="json")

    return app
