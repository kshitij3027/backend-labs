"""FastAPI application factory: ``/health``, ``/stats``, ``POST /classify`` (Commit 8).

This is the **base HTTP surface** of the service. It wires the trained model
(:class:`src.ensemble.LogClassifier`), the versioned registry
(:class:`src.model_store.ModelRegistry`) and the trainer
(:func:`src.trainer.train`) into a small FastAPI app that is *immediately usable*
the moment it finishes starting:

* On startup (the FastAPI **lifespan**) the app opens a registry rooted at
  ``cfg.model_dir``. If a persisted model exists it is loaded; otherwise (when
  ``auto_train`` is enabled) a model is trained on the generated corpus, persisted
  as ``v1``, and used. Either way ``app.state.classifier`` holds a ready model and
  ``app.state.model_status`` is ``"ready"`` before the server accepts a single
  request. (If ``auto_train`` is off and nothing is persisted, the app still
  starts but reports ``"untrained"`` and ``POST /classify`` returns ``503``.)
* Three routes are exposed:
  - ``GET /health`` — a liveness probe (used by the Docker healthcheck).
  - ``GET /stats`` — total classified count + model status.
  - ``POST /classify`` — a **synchronous** ``def`` handler so FastAPI runs the
    blocking sklearn inference in its worker threadpool rather than on the event
    loop.
* Permissive CORS is enabled so the browser dashboard (served from a different
  origin) can call the API directly.

Out of scope for this commit (later commits): streaming/batch inference, the
``POST /train`` endpoint, the live-metrics WebSocket, the metrics aggregator,
multi-service hierarchical classification, the adaptive retraining loop and A/B
serving. This module deliberately only does *load-or-train at startup* + the
three base routes.
"""

from __future__ import annotations

import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src import trainer
from src.config import Settings, get_config
from src.ensemble import LogClassifier
from src.model_store import ModelRegistry
from src.schemas import (
    ClassifyRequest,
    ClassifyResponse,
    HealthResponse,
    StatsResponse,
)


class Counter:
    """A tiny thread-safe monotonically increasing counter.

    ``POST /classify`` runs as a synchronous handler in FastAPI's threadpool, so
    several requests can call :meth:`increment` concurrently. A
    :class:`threading.Lock` makes the increment atomic; reads of :attr:`value`
    are likewise guarded so a reader never observes a torn update.
    """

    def __init__(self, start: int = 0) -> None:
        """Create a counter starting at ``start`` (default 0)."""
        self._value = int(start)
        self._lock = threading.Lock()

    def increment(self, amount: int = 1) -> int:
        """Atomically add ``amount`` (default 1) and return the new value."""
        with self._lock:
            self._value += int(amount)
            return self._value

    @property
    def value(self) -> int:
        """The current count (read under the lock)."""
        with self._lock:
            return self._value


def _startup_load_or_train(app: FastAPI, cfg: Settings, auto_train: bool) -> None:
    """Populate ``app.state`` with a ready (or explicitly untrained) classifier.

    Resolution order:

    1. Open a :class:`ModelRegistry` at ``cfg.model_dir``.
    2. If the registry :meth:`~ModelRegistry.has_models`, load the current
       version (:meth:`~ModelRegistry.get_current`) and mark the model
       ``"ready"``.
    3. Else if ``auto_train``: call :func:`src.trainer.train` (generates the
       corpus, fits, and persists ``v1`` into the same registry), keep the
       returned classifier, and mark it ``"ready"``.
    4. Else: leave ``app.state.classifier = None`` and mark ``"untrained"``.

    All of the registry, counter and config are also stashed on ``app.state`` so
    the route handlers stay thin.
    """
    registry = ModelRegistry(cfg.model_dir)

    classifier: Optional[LogClassifier]
    if registry.has_models():
        current = registry.get_current()
        if current is not None:
            version_id, classifier = current
            app.state.model_status = "ready"
            print(f"[api] loaded persisted model version '{version_id}' from {cfg.model_dir}")
        else:
            # has_models() was True but nothing is marked current — treat as empty.
            classifier = None
            app.state.model_status = "untrained"
    elif auto_train:
        print(f"[api] no persisted model found; training a fresh model into {cfg.model_dir} ...")
        result = trainer.train(cfg=cfg, registry=registry, persist=True)
        classifier = result["classifier"]
        app.state.model_status = "ready"
        print(f"[api] trained and persisted model version '{result['version']}'")
    else:
        classifier = None
        app.state.model_status = "untrained"
        print("[api] no persisted model and auto_train disabled; starting untrained")

    app.state.classifier = classifier
    app.state.registry = registry
    app.state.cfg = cfg
    app.state.counter = Counter()


def create_app(cfg: Optional[Settings] = None, auto_train: bool = True) -> FastAPI:
    """Build and return the FastAPI application.

    The app uses the **lifespan** pattern: the load-or-train step runs once on
    startup (before the server accepts requests) and its products live on
    ``app.state``. Because startup is fully synchronous-complete before serving,
    a successful ``GET /health`` implies the model is loaded.

    Args:
        cfg: Optional :class:`src.config.Settings`. Defaults to
            :func:`src.config.get_config`. Pass a small config
            (e.g. ``Settings(rf_n_estimators=5, gb_n_estimators=5,
            model_dir=<tmp>)``) to make first-boot training fast in tests.
        auto_train: When ``True`` (default) and no model is persisted, train one
            at startup so the service is immediately usable. When ``False`` and
            nothing is persisted, the app starts ``"untrained"`` and
            ``POST /classify`` returns ``503`` until a model exists.

    Returns:
        A configured :class:`fastapi.FastAPI` instance.
    """
    settings = cfg if cfg is not None else get_config()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # --- startup: load an existing model or train a fresh one. ---
        _startup_load_or_train(app, settings, auto_train)
        yield
        # --- shutdown: nothing to tear down (no background tasks in C8). ---

    app = FastAPI(
        title="ML Log Classifier",
        version="0.8.0",
        summary="Ensemble log classifier — severity + category + confidence.",
        lifespan=lifespan,
    )

    # Permissive CORS so the dashboard (a different origin) can call the API.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health", response_model=HealthResponse, tags=["ops"])
    def health() -> HealthResponse:
        """Liveness probe.

        Returns ``{"status": "healthy"}`` whenever the process is up. Since the
        lifespan startup (model load/train) completes before the server serves
        requests, ``healthy`` implies the model is ready. The current model
        status is included for convenience.
        """
        return HealthResponse(
            status="healthy",
            model_status=getattr(app.state, "model_status", None),
        )

    @app.get("/stats", response_model=StatsResponse, tags=["ops"])
    def stats() -> StatsResponse:
        """Aggregate service stats.

        Returns the number of logs classified since process start and the model
        lifecycle status (``"ready"`` / ``"untrained"``), matching the spec's
        ``{"total_classified": 0, "model_status": "ready"}`` shape.
        """
        return StatsResponse(
            total_classified=app.state.counter.value,
            model_status=app.state.model_status,
        )

    # NOTE: intentionally a *synchronous* ``def`` (not ``async def``). FastAPI
    # runs sync handlers in its worker threadpool, so the blocking sklearn
    # inference does not stall the event loop.
    @app.post("/classify", response_model=ClassifyResponse, tags=["inference"])
    def classify(req: ClassifyRequest) -> ClassifyResponse:
        """Classify a single raw log line into severity + category + confidence.

        Runs the loaded :class:`LogClassifier` on ``req.raw_log`` (and the optional
        ``req.timestamp``), increments the classified-count, and returns the
        structured result.

        Raises:
            HTTPException: ``503`` if no model is loaded (e.g. started with
                ``auto_train=False`` and nothing persisted yet).
        """
        classifier: Optional[LogClassifier] = app.state.classifier
        if classifier is None:
            raise HTTPException(status_code=503, detail="model not ready")

        result = classifier.classify(req.raw_log, req.timestamp)
        app.state.counter.increment()
        return result

    return app
