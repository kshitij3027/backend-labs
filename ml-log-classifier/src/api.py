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

* Live metrics + dashboard streaming (Commit 10):
  - ``app.state.metrics`` (:class:`src.metrics.MetricsAggregator`) is now the
    **single source of truth** for ``total_classified``: every classify path
    (``/classify``, ``/classify/batch``, ``/classify/stream``) reports each
    classified log via ``metrics.record(result, raw_log)`` instead of bumping the
    old counter, and ``GET /stats`` reads the total/status from it.
  - ``GET /metrics`` — a plain REST mirror returning the latest aggregator
    :meth:`~src.metrics.MetricsAggregator.snapshot` (easy to poll/test without a
    socket).
  - ``WS /ws/metrics`` — the dashboard's live feed. A single background
    broadcaster task (:func:`_broadcast_loop`, started in the lifespan) is the
    **only** periodic sender; each connection gets one immediate snapshot on
    connect, then merely ``receive``-s to detect disconnect.

Out of scope for this module (later commits): multi-service hierarchical
classification, the adaptive retraining loop and A/B serving.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from src import trainer
from src.config import Settings, get_config
from src.ensemble import LogClassifier
from src.log_generator import generate_logs
from src.metrics import ConnectionManager, MetricsAggregator
from src.model_store import ModelRegistry
from src.multiservice import MultiServiceClassifier
from src.schemas import (
    BatchClassifyRequest,
    BatchClassifyResponse,
    ClassifyRequest,
    ClassifyResponse,
    HealthResponse,
    MultiServiceResponse,
    StatsResponse,
    TrainRequest,
    TrainStatusResponse,
)


#: Subdirectory of ``cfg.model_dir`` holding the multi-service classifier's
#: persisted artifacts (kept separate from the base model's registry so neither
#: clobbers the other).
MULTISERVICE_SUBDIR = "multiservice"


#: How often (seconds) the background broadcaster pushes a metrics snapshot to
#: every connected ``/ws/metrics`` client.
BROADCAST_INTERVAL_SEC = 1.0


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


def _multiservice_dir(cfg: Settings) -> str:
    """Return the directory holding the multi-service classifier artifacts."""
    return os.path.join(cfg.model_dir, MULTISERVICE_SUBDIR)


def _startup_load_or_multiservice(app: FastAPI, cfg: Settings, auto_train: bool) -> None:
    """Populate ``app.state`` with a ready (or untrained) multi-service classifier.

    Mirrors :func:`_startup_load_or_train` but for the **hierarchical** Feature-Area-A
    model, persisted under a *separate* path (``<cfg.model_dir>/multiservice``) so it
    never collides with the base model's versioned registry:

    1. If that directory exists, load it via
       :meth:`MultiServiceClassifier.load` and mark ``"ready"``.
    2. Else if ``auto_train``, fit a fresh :class:`MultiServiceClassifier` on the
       generated corpus, :meth:`~MultiServiceClassifier.save` it to that path, and
       mark ``"ready"``. (This adds ~one model's training to the FIRST boot only;
       it is cached on disk afterwards. Tiny estimators keep tests fast.)
    3. Else leave ``app.state.multiservice = None`` and mark ``"untrained"``.

    On any load/train failure the service still starts: the multi-service model is
    left ``None``/``"untrained"`` (the base ``/classify`` path is unaffected).
    """
    ms_dir = _multiservice_dir(cfg)
    model: Optional[MultiServiceClassifier] = None
    status = "untrained"
    try:
        if os.path.isdir(ms_dir):
            model = MultiServiceClassifier.load(ms_dir, cfg=cfg)
            status = "ready"
            print(f"[api] loaded persisted multi-service model from {ms_dir}")
        elif auto_train:
            print(
                f"[api] no persisted multi-service model; training a fresh one "
                f"into {ms_dir} ..."
            )
            records = generate_logs(cfg.sample_size, cfg.random_seed)
            model = MultiServiceClassifier(cfg).fit(records)
            model.save(ms_dir)
            status = "ready"
            print("[api] trained and persisted multi-service model")
        else:
            print("[api] no persisted multi-service model and auto_train disabled")
    except Exception as exc:  # noqa: BLE001 - never block startup on the multi-svc model
        print(f"[api] multi-service model unavailable ({exc!r}); continuing untrained")
        model = None
        status = "untrained"

    app.state.multiservice = model
    app.state.multiservice_status = status


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

    # --- Commit 10: live-metrics aggregator + WebSocket fan-out. ---
    # The aggregator is the single source of truth for ``total_classified`` and the
    # snapshot the dashboard renders; seed its status/version from the model we just
    # loaded or trained so ``/metrics`` agrees with ``/stats`` from the first request.
    metrics = MetricsAggregator()
    metrics.set_status(
        model_status=app.state.model_status,
        current_version=registry.current_version,
    )
    app.state.metrics = metrics
    app.state.ws_manager = ConnectionManager()
    # The broadcaster task itself is created in the lifespan (it needs a running
    # event loop); record a placeholder so shutdown can reference it unconditionally.
    app.state.broadcaster_task = None

    # --- Commit 9: on-demand background training state. ---
    # ``is_training`` / ``model_status`` transitions are guarded by ``train_lock``
    # so a double-submit cannot both observe ``is_training == False`` and start two
    # concurrent retrains. ``last_train_metrics`` holds the metrics dict from the
    # most recent successful in-process training (None until one completes).
    app.state.is_training = False
    app.state.train_lock = threading.Lock()
    app.state.last_train_metrics = None

    # --- Commit 11: hierarchical multi-service classifier (Feature Area A). ---
    # Loaded/trained against a SEPARATE persistence path so it never collides with
    # the base model's registry. Adds ~one model's training to the first boot only.
    _startup_load_or_multiservice(app, cfg, auto_train)


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
        # Keep the metrics aggregator's status/version in lockstep with app.state so
        # ``/metrics``, ``/stats`` and ``/train/status`` all agree post-swap.
        app.state.metrics.set_status(
            model_status="ready", current_version=app.state.registry.current_version
        )
        print(
            f"[api] /train: hot-swapped to new model version '{result['version']}' "
            f"(severity_test_acc={result['metrics'].get('severity_test_accuracy')})"
        )

        # --- Commit 11: also retrain + hot-swap the multi-service model. ---
        # Built from the SAME freshly-generated corpus, then atomically swapped and
        # re-saved to the multi-service path. Graceful: a failure here keeps the
        # previous multi-service model (and never affects the base swap above).
        try:
            ms_model = MultiServiceClassifier(cfg).fit(records)
            ms_model.save(_multiservice_dir(cfg))
            app.state.multiservice = ms_model  # atomic ref swap on success only
            app.state.multiservice_status = "ready"
            print("[api] /train: hot-swapped multi-service model")
        except Exception as ms_exc:  # noqa: BLE001 - keep the old multi-svc model
            print(
                f"[api] /train: multi-service retrain FAILED, keeping previous: "
                f"{ms_exc!r}"
            )
    except Exception as exc:  # noqa: BLE001 - background thread must not crash silently
        # Keep the OLD classifier (no downtime); just report and restore status.
        print(f"[api] /train: training FAILED, keeping previous model: {exc!r}")
        restored = "ready" if app.state.classifier is not None else "untrained"
        app.state.model_status = restored
        app.state.metrics.set_status(model_status=restored)
    finally:
        # Always release the training flag so the service is not wedged.
        with app.state.train_lock:
            app.state.is_training = False


async def _broadcast_loop(app: FastAPI) -> None:
    """Periodically push the live metrics snapshot to every WebSocket client.

    This is the **single** periodic sender for ``/ws/metrics`` (the connection
    endpoint only sends one immediate snapshot and otherwise just reads), so no two
    coroutines ever contend to send on the same socket. Started as an asyncio task
    in the lifespan and cancelled on shutdown.

    Every :data:`BROADCAST_INTERVAL_SEC` it reads
    :meth:`MetricsAggregator.snapshot` (which is taken under the aggregator's lock,
    safe to call from the event-loop thread while classify threads mutate it) and
    fans it out via :meth:`ConnectionManager.broadcast`. Per-iteration exceptions
    are swallowed (logged) so a transient failure never kills the loop and silently
    stops all dashboard updates; a :class:`asyncio.CancelledError` is allowed to
    propagate so shutdown can stop the task cleanly.

    Args:
        app: The FastAPI app whose ``state`` carries ``metrics`` and ``ws_manager``.
    """
    while True:
        try:
            snapshot = app.state.metrics.snapshot()
            await app.state.ws_manager.broadcast(snapshot)
        except asyncio.CancelledError:
            # Shutdown requested — stop the loop.
            raise
        except Exception as exc:  # noqa: BLE001 - never let the loop die silently
            print(f"[api] /ws/metrics broadcaster iteration failed: {exc!r}")
        await asyncio.sleep(BROADCAST_INTERVAL_SEC)


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
        # Start the periodic metrics broadcaster (needs the running event loop).
        app.state.broadcaster_task = asyncio.create_task(_broadcast_loop(app))
        yield
        # --- shutdown: stop the broadcaster task cleanly. ---
        task = getattr(app.state, "broadcaster_task", None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

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
        ``{"total_classified": 0, "model_status": "ready"}`` shape. As of Commit 10
        the total comes from the metrics aggregator (the single source of truth);
        ``model_status`` is read from ``app.state`` which is kept in lockstep with
        the aggregator on every training transition.
        """
        return StatsResponse(
            total_classified=app.state.metrics.total_classified,
            model_status=app.state.model_status,
        )

    # NOTE: intentionally a *synchronous* ``def`` (not ``async def``). FastAPI
    # runs sync handlers in its worker threadpool, so the blocking sklearn
    # inference does not stall the event loop.
    @app.post("/classify", response_model=ClassifyResponse, tags=["inference"])
    def classify(req: ClassifyRequest) -> ClassifyResponse:
        """Classify a single raw log line into severity + category + confidence.

        Runs the loaded :class:`LogClassifier` on ``req.raw_log`` (and the optional
        ``req.timestamp``), records the classification in the metrics aggregator
        (which owns the authoritative classified-count), and returns the structured
        result.

        Raises:
            HTTPException: ``503`` if no model is loaded (e.g. started with
                ``auto_train=False`` and nothing persisted yet).
        """
        classifier: Optional[LogClassifier] = app.state.classifier
        if classifier is None:
            raise HTTPException(status_code=503, detail="model not ready")

        result = classifier.classify(req.raw_log, req.timestamp)
        app.state.metrics.record(result, req.raw_log)
        return result

    # -- Commit 11: hierarchical multi-service classification --------------

    # NOTE: a *synchronous* ``def`` (like ``/classify``) so FastAPI runs the
    # blocking sklearn inference in its worker threadpool, not on the event loop.
    @app.post(
        "/classify/service",
        response_model=MultiServiceResponse,
        tags=["inference"],
    )
    def classify_service(req: ClassifyRequest) -> MultiServiceResponse:
        """Classify a log HIERARCHICALLY: service → its severity model + anomaly.

        Runs the loaded :class:`MultiServiceClassifier` on ``req.raw_log`` (and the
        optional ``req.timestamp``): it predicts the **service**, applies that
        service's own severity model, predicts the global category, and computes a
        cross-service ``anomaly_score`` from ensemble voting. The result is recorded
        in the metrics aggregator — and because it carries a ``service`` key, this is
        what populates the ``service_distribution`` in ``GET /metrics``.

        This is additive: the base ``POST /classify`` (and its 5-key response) is
        unchanged.

        Raises:
            HTTPException: ``503`` if the multi-service model is not ready (e.g.
                started with ``auto_train=False`` and nothing persisted yet).
        """
        model: Optional[MultiServiceClassifier] = app.state.multiservice
        if model is None:
            raise HTTPException(status_code=503, detail="multi-service model not ready")

        result = model.classify(req.raw_log, req.timestamp)
        # Populates service_distribution (result carries a ``service`` key).
        app.state.metrics.record(result, req.raw_log)
        return result

    @app.get("/services", tags=["inference"])
    def services() -> dict:
        """List the services the multi-service classifier knows + its readiness.

        Safe to call before the multi-service model is trained: returns an empty
        ``services`` list and ``status == "untrained"`` in that case. When ready it
        reports the service labels and, per service, the severity classes that
        service's model can emit.

        Returns:
            ``{"services": [...], "status": <str>,
               "per_service_severity_classes": {service: [severity, ...]}}``.
        """
        model: Optional[MultiServiceClassifier] = app.state.multiservice
        status = getattr(app.state, "multiservice_status", "untrained")
        if model is None:
            return {
                "services": [],
                "status": status,
                "per_service_severity_classes": {},
            }
        return {
            "services": list(model.services_ or []),
            "status": status,
            "per_service_severity_classes": {
                k: list(v)
                for k, v in (model.severity_classes_by_service_ or {}).items()
            },
        }

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
        one predict per axis for the whole batch), records each result in the
        metrics aggregator (one ``record`` call per classified log, paired with its
        originating ``raw_log``), and returns the results plus their count.

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
        # One record per classified log, paired with its source raw_log (results and
        # req.logs are in the same order, same length).
        for log, result in zip(req.logs, results):
            app.state.metrics.record(result, log.raw_log)
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
        inference never stalls the loop while the response streams. Each emitted line
        is recorded once in the metrics aggregator (the authoritative count).

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
                app.state.metrics.record(result, item.raw_log)
                yield json.dumps(result) + "\n"

        return StreamingResponse(_gen(), media_type="application/x-ndjson")

    # -- Commit 10: live metrics (REST mirror + WebSocket feed) ------------

    @app.get("/metrics", tags=["metrics"])
    def metrics() -> dict:
        """Return the latest live-metrics snapshot as plain JSON (REST mirror).

        Identical payload to what the ``/ws/metrics`` WebSocket streams, but pull-
        based — handy for one-off polling, smoke tests and any client that does not
        speak WebSocket. See :meth:`src.metrics.MetricsAggregator.snapshot` for the
        full shape (total, severity/category/service distributions, average
        confidence, throughput, recent predictions, model status/version, uptime).
        """
        return app.state.metrics.snapshot()

    # NOTE: the only WebSocket route. It does NOT send periodically itself — the
    # background :func:`_broadcast_loop` is the sole periodic sender, so there is no
    # concurrent send on the same socket. This handler only sends ONE immediate
    # snapshot (so the dashboard paints instantly) and then blocks on
    # ``receive_text`` purely to detect client disconnect.
    @app.websocket("/ws/metrics")
    async def metrics_ws(websocket: WebSocket) -> None:
        """Stream live metrics snapshots to a dashboard client.

        On connect the socket is accepted, registered with the
        :class:`~src.metrics.ConnectionManager`, and immediately sent one current
        snapshot so the UI renders without waiting for the next broadcast tick.
        Thereafter the periodic broadcaster pushes updates every
        :data:`BROADCAST_INTERVAL_SEC`; this coroutine just awaits
        ``receive_text()`` to keep the connection open and notice when the client
        goes away, at which point it deregisters the socket.
        """
        manager: ConnectionManager = app.state.ws_manager
        await manager.connect(websocket)
        try:
            # Paint-on-connect: one immediate snapshot (sent before the broadcaster
            # could race on this freshly-registered socket).
            await websocket.send_json(app.state.metrics.snapshot())
            # Block until the client disconnects; the broadcaster does the sending.
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(websocket)

    return app
