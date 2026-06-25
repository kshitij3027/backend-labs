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

Routes (this commit):

* ``GET  /health``                          — liveness + readiness (warming/ok).
* ``POST /cluster``                          — cluster one log.
* ``POST /cluster/batch``                    — cluster a batch of logs.
* ``GET  /stats``                            — aggregate engine statistics.
* ``GET  /clusters``                         — per-algorithm cluster summaries (all algos).
* ``GET  /clusters/{algorithm}``             — one algorithm's cluster summaries.
* ``GET  /clusters/{algorithm}/{cluster_id}``— drill-down detail for one cluster.
* ``GET  /patterns``                         — discovered patterns.
* ``GET  /anomalies``                        — recent anomaly alerts.
* ``GET  /scatter/{algorithm}``              — 2-D scatter points for the dashboard.
* ``GET  /config``                           — the resolved configuration.
* ``GET  /``                                 — service banner.

The metrics WebSocket and the background broadcaster / periodic refit loop arrive in C12.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.config import AppConfig, load_config
from src.demo import load_corpus
from src.engine import ClusteringEngine
from src.schemas import (
    AnomalyAlert,
    ClusterAssignment,
    HealthResponse,
    LogEntry,
    PatternRecord,
    StatsSnapshot,
)
from src.state import create_state_store

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
):
    """Build the lifespan context manager, capturing the warm-up parameters.

    Keeping the parameters in a closure (rather than reading globals) lets ``create_app``
    spin up independent apps — with different warm-up batches — in the same process, which
    is exactly what the test suite needs.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 1) Resolve config (honours CONFIG_PATH / env overrides) unless one was injected.
        cfg = config if config is not None else load_config()
        app.state.config = cfg

        # 2) Build + warm the engine SYNCHRONOUSLY so the app only serves once ready.
        engine = ClusteringEngine(cfg)
        if warmup_logs is not None:
            batch = list(warmup_logs)
        else:
            # load_corpus falls back to generated logs when the corpus file is absent
            # (e.g. the minimal app image), so warm-up always has data.
            corpus = load_corpus(fallback_n=warmup_n)
            batch = corpus[:warmup_n]
        engine.warm_up(batch)
        app.state.engine = engine

        # 3) State store (never raises; degrades to in-memory). Persist an initial
        #    snapshot best-effort so a dashboard reading Redis directly has something.
        store = create_state_store(cfg)
        app.state.state_store = store
        try:
            store.save_stats(engine.stats_snapshot().model_dump(mode="json"))
        except Exception:  # noqa: BLE001 - persistence is best-effort, never fatal
            pass

        yield

        # Shutdown: release the Redis client if one was opened.
        app.state.state_store.close()

    return lifespan


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

    Returns:
        A configured :class:`fastapi.FastAPI` instance. The engine, config and state store
        are attached to ``app.state`` during the lifespan (so warm-up runs when the app is
        entered as a context / served by uvicorn or wrapped in ``TestClient(app)``).
    """
    app = FastAPI(
        title="Real-Time Log Pattern Clustering",
        version=APP_VERSION,
        lifespan=_build_lifespan(config, warmup_logs, warmup_n),
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

    # ------------------------------------------------------------------- config

    @app.get("/config")
    def get_config(request: Request) -> dict:
        """Return the resolved application configuration as JSON."""
        cfg: AppConfig = request.app.state.config
        return cfg.model_dump(mode="json")

    return app
