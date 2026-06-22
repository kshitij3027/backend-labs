"""FastAPI application factory: base routes + on-demand training & streaming (Commits 8–9).

This is the **HTTP surface** of the service. It wires the trained model
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
* Base routes (Commit 8):
  - ``GET /health`` — a liveness probe (used by the Docker healthcheck).
  - ``GET /stats`` — total classified count + model status.
  - ``POST /classify`` — a **synchronous** ``def`` handler so FastAPI runs the
    blocking sklearn inference in its worker threadpool rather than on the event
    loop.
* Training & bulk/streaming inference (Commit 9):
  - ``POST /train`` — kick off an **on-demand background retrain** in a daemon
    thread and return ``202`` immediately. The current model keeps serving
    throughout; the freshly trained classifier is swapped in with a single atomic
    reference assignment only on success (**graceful hot-swap** — no downtime, and
    a partially-trained model never replaces a good one). A second concurrent
    submit gets ``409``.
  - ``GET /train/status`` — poll the training lifecycle (model status, current
    version, ``is_training``, last metrics).
  - ``POST /classify/batch`` — classify a list of logs in one vectorized call.
  - ``POST /classify/stream`` — the **async** streaming inference endpoint: an
    ``application/x-ndjson`` response that yields one JSON result line per input
    log, each classified via the event loop's executor so the blocking sklearn
    call never stalls the loop.
* Permissive CORS is enabled so the browser dashboard (served from a different
  origin) can call the API directly.

Out of scope for this module (later commits): the live-metrics WebSocket, the
metrics aggregator, multi-service hierarchical classification, the adaptive
retraining loop and A/B serving.
"""

from __future__ import annotations

import asyncio
import json
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from src import trainer
from src.config import Settings, get_config
from src.ensemble import LogClassifier
from src.log_generator import generate_logs
from src.model_store import ModelRegistry
from src.schemas import (
    BatchClassifyRequest,
    BatchClassifyResponse,
    ClassifyRequest,
    ClassifyResponse,
    HealthResponse,
    StatsResponse,
    TrainRequest,
    TrainStatusResponse,
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

    # --- Commit 9: on-demand background training state. ---
    # ``is_training`` / ``model_status`` transitions are guarded by ``train_lock``
    # so a double-submit cannot both observe ``is_training == False`` and start two
    # concurrent retrains. ``last_train_metrics`` holds the metrics dict from the
    # most recent successful in-process training (None until one completes).
    app.state.is_training = False
    app.state.train_lock = threading.Lock()
    app.state.last_train_metrics = None


def _run_training(app: FastAPI, count: Optional[int], cv: Optional[int]) -> None:
    """Background worker that retrains the model and hot-swaps it on success.

    Runs in a daemon thread launched by ``POST /train``; never on the event loop.
    The contract here is **graceful, zero-downtime retraining**:

    * The currently-served ``app.state.classifier`` is left **untouched** for the
      entire duration of training, so ``/classify`` (and the batch/stream routes)
      keep answering with the old model.
    * Only after :func:`src.trainer.train` returns successfully is the new model
      installed, via a single atomic reference assignment
      (``app.state.classifier = result["classifier"]``). A half-built or failed
      model therefore can never replace a good one.
    * On any exception the old model is kept (no downtime); ``model_status`` is
      restored to ``"ready"`` if a model is loaded, else ``"untrained"``.
    * ``is_training`` is always cleared in ``finally`` so a failed run never wedges
      the service into a permanent "training" state and a later ``/train`` works.

    Args:
        app: The FastAPI app whose ``state`` carries ``cfg`` / ``registry`` /
            ``classifier`` / ``model_status`` / ``last_train_metrics`` /
            ``is_training`` / ``train_lock``.
        count: Corpus size to generate; falls back to ``cfg.sample_size``.
        cv: Cross-validation fold count; falls back to 5.
    """
    cfg: Settings = app.state.cfg
    try:
        n = count or cfg.sample_size
        folds = cv or 5
        print(f"[api] /train: generating {n} logs (seed={cfg.random_seed}) and retraining ...")
        records = generate_logs(n, cfg.random_seed)
        result = trainer.train(
            records=records,
            cfg=cfg,
            cv=folds,
            persist=True,
            registry=app.state.registry,
        )

        # --- graceful hot-swap: single atomic ref assignment on SUCCESS only. ---
        app.state.classifier = result["classifier"]
        app.state.last_train_metrics = result["metrics"]
        app.state.model_status = "ready"
        print(
            f"[api] /train: hot-swapped to new model version '{result['version']}' "
            f"(severity_test_acc={result['metrics'].get('severity_test_accuracy')})"
        )
    except Exception as exc:  # noqa: BLE001 - background thread must not crash silently
        # Keep the OLD classifier (no downtime); just report and restore status.
        print(f"[api] /train: training FAILED, keeping previous model: {exc!r}")
        app.state.model_status = "ready" if app.state.classifier is not None else "untrained"
    finally:
        # Always release the training flag so the service is not wedged.
        with app.state.train_lock:
            app.state.is_training = False


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

    # -- Commit 9: on-demand background training ---------------------------

    def _status_snapshot() -> TrainStatusResponse:
        """Build a :class:`TrainStatusResponse` from the current ``app.state``."""
        return TrainStatusResponse(
            model_status=app.state.model_status,
            current_version=app.state.registry.current_version,
            is_training=app.state.is_training,
            last_metrics=app.state.last_train_metrics,
        )

    # NOTE: a plain sync ``def`` — it only flips a flag and launches a daemon
    # thread, returning immediately; the actual training runs off the event loop
    # in :func:`_run_training`.
    @app.post(
        "/train",
        response_model=TrainStatusResponse,
        status_code=202,
        tags=["training"],
    )
    def train_endpoint(req: TrainRequest = TrainRequest()) -> TrainStatusResponse:
        """Kick off an on-demand background retrain and return immediately (202).

        Generates a fresh corpus (``req.count`` or ``cfg.sample_size`` logs),
        retrains the dual-target ensemble with cross-validation, persists a new
        version into the registry, and — only on success — **atomically hot-swaps**
        the live model. The current model keeps serving for the whole duration, so
        ``POST /classify`` never goes down during a retrain.

        The ``is_training`` transition is guarded by ``train_lock`` so two requests
        racing in cannot both start a retrain: the first wins, the second gets a
        ``409``.

        Args:
            req: Optional :class:`TrainRequest` (``count`` / ``cv``); an empty body
                trains on the configured defaults.

        Returns:
            A ``202`` :class:`TrainStatusResponse` snapshot taken right after the
            background thread is launched (``model_status == "training"``,
            ``is_training == True``).

        Raises:
            HTTPException: ``409`` if a retrain is already in progress.
        """
        # Guard the check-and-set so a double-submit can't start two retrains.
        with app.state.train_lock:
            if app.state.is_training:
                raise HTTPException(
                    status_code=409, detail="training already in progress"
                )
            app.state.is_training = True
            app.state.model_status = "training"

        thread = threading.Thread(
            target=_run_training,
            args=(app, req.count, req.cv),
            daemon=True,
        )
        thread.start()
        return _status_snapshot()

    @app.get("/train/status", response_model=TrainStatusResponse, tags=["training"])
    def train_status() -> TrainStatusResponse:
        """Report the current training lifecycle for polling.

        Returns the model status (``"ready"`` / ``"training"`` / ``"untrained"``),
        the registry's current version id, whether a retrain is running, and the
        metrics from the last successful in-process training (if any). Clients poll
        this after ``POST /train`` until ``is_training`` is ``False`` and
        ``model_status`` is ``"ready"`` (with ``current_version`` advanced).
        """
        return _status_snapshot()

    # -- Commit 9: bulk + streaming inference ------------------------------

    # NOTE: sync ``def`` (like ``/classify``) — one vectorized batch call runs in
    # FastAPI's threadpool, off the event loop.
    @app.post(
        "/classify/batch", response_model=BatchClassifyResponse, tags=["inference"]
    )
    def classify_batch(req: BatchClassifyRequest) -> BatchClassifyResponse:
        """Classify a list of logs in a single vectorized pass.

        Delegates to :meth:`LogClassifier.classify_batch` (one feature transform +
        one predict per axis for the whole batch), increments the classified-count
        by the number of results, and returns the results plus their count.

        Args:
            req: A :class:`BatchClassifyRequest` carrying a non-empty ``logs`` list.

        Returns:
            A :class:`BatchClassifyResponse` with one result per input log.

        Raises:
            HTTPException: ``503`` if no model is loaded.
        """
        classifier: Optional[LogClassifier] = app.state.classifier
        if classifier is None:
            raise HTTPException(status_code=503, detail="model not ready")

        results = classifier.classify_batch(
            [{"raw_log": r.raw_log, "timestamp": r.timestamp} for r in req.logs]
        )
        app.state.counter.increment(len(results))
        return BatchClassifyResponse(results=results, count=len(results))

    # NOTE: the ONLY ``async def`` route. It must not block the event loop, so each
    # blocking sklearn ``classify`` is offloaded to the default thread executor via
    # ``loop.run_in_executor`` and the results are streamed as NDJSON.
    @app.post("/classify/stream", tags=["inference"])
    async def classify_stream(req: BatchClassifyRequest) -> StreamingResponse:
        """Stream per-log classification results as newline-delimited JSON.

        Returns an ``application/x-ndjson`` :class:`~fastapi.responses.StreamingResponse`
        that yields one compact JSON object per input log (same five keys as
        :class:`ClassifyResponse`) in input order. Each log is classified on the
        event loop's default executor (``run_in_executor``) so the blocking sklearn
        inference never stalls the loop while the response streams. The
        classified-count is incremented once per emitted line.

        The ``503`` check is performed **before** returning the streaming response
        so a missing model surfaces as a normal error status rather than a broken
        stream.

        Args:
            req: A :class:`BatchClassifyRequest` carrying a non-empty ``logs`` list.

        Returns:
            A streaming NDJSON response, one classification result per line.

        Raises:
            HTTPException: ``503`` if no model is loaded.
        """
        classifier: Optional[LogClassifier] = app.state.classifier
        if classifier is None:
            raise HTTPException(status_code=503, detail="model not ready")

        async def _gen():
            loop = asyncio.get_running_loop()
            for item in req.logs:
                result = await loop.run_in_executor(
                    None, classifier.classify, item.raw_log, item.timestamp
                )
                app.state.counter.increment()
                yield json.dumps(result) + "\n"

        return StreamingResponse(_gen(), media_type="application/x-ndjson")

    return app
