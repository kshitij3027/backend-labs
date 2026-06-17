"""Flask + Flask-SocketIO dashboard for the Adaptive Resource Allocation System.

This module is the *presentation/control plane*. It owns no system state of its
own — every route and every SocketIO event is a thin reader (or command) over a
single :class:`src.orchestrator.Orchestrator`, which is the source of truth for
metrics, forecasts, the worker pool and scaling decisions.

The design mirrors the sibling ``kafka-streams-monitoring-dashboard`` project:

* an **application factory** (:func:`create_app`) that builds the Flask app and the
  ``SocketIO`` server and stows collaborators on ``app.config`` so handlers can
  reach them without globals;
* a **503-when-missing idiom** — every ``/api/*`` route first resolves the
  orchestrator and returns ``503`` JSON if it is absent, so the app can boot and
  answer ``/health`` even before the control plane is wired;
* an **emit-on-connect** handler that pushes a full snapshot to a newly connected
  client so the page paints immediately rather than waiting for the next tick.

**Async model.** The production server runs under *eventlet*; the background loops
use :meth:`SocketIO.sleep` so they cooperate with the eventlet hub instead of
blocking it. ``async_mode`` is a parameter (default ``"eventlet"``) purely so the
test-suite can pass ``"threading"`` and avoid standing up the eventlet server.

The background emitter loops are **not** started inside :func:`create_app`; they
are launched explicitly via :func:`start_background_tasks`, which ``main.py`` calls
*after* the eventlet hub is up. Keeping them out of the factory means importing or
unit-testing ``create_app`` never spawns green threads.
"""

import logging

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit
from jinja2 import TemplateNotFound

from src.config import Settings

logger = logging.getLogger(__name__)

# The six scalar time-series the dashboard plots, plus the worker count (charted
# separately because it is stepwise rather than continuous). Declared once so the
# /api/metrics route and the metrics background loop emit an identical block.
_SERIES_FIELDS = (
    "cpu_percent",
    "memory_percent",
    "effective_utilization",
    "queue_depth",
    "latency_ms",
    "arrival_rate",
)

# How many trailing points of each series to ship to the client per update.
_SERIES_POINTS = 60

# Minimal HTML returned by ``/`` when the real template is absent (it lands in a
# later commit). Lets this commit's ``/`` route answer 200 instead of 500.
_INDEX_STUB = (
    "<!doctype html><html><head><title>Adaptive Resource Allocation</title></head>"
    "<body><h1>Adaptive Resource Allocation</h1>"
    "<p>Dashboard UI not yet built. API is live at <code>/api/status</code>.</p>"
    "</body></html>"
)


def _build_series_block(orchestrator) -> dict:
    """Assemble the canonical time-series payload from the orchestrator's history.

    Returns the exact structure emitted by both the ``/api/metrics`` route and the
    ``metrics_update`` SocketIO event: ``current_metrics`` plus a ``series`` map of
    field -> list and the worker count series broken out separately. Every history
    read is wrapped defensively so an empty or partially-populated history yields
    empty lists rather than raising.

    Args:
        orchestrator: The wired :class:`~src.orchestrator.Orchestrator`.

    Returns:
        ``{"current_metrics": {...}, "series": {field: [...]}, "workers_series": [...]}``
    """

    def _series(field: str) -> list:
        try:
            return orchestrator.history.series(field, _SERIES_POINTS)
        except Exception:  # pragma: no cover - history is defensive, guard anyway
            return []

    return {
        "current_metrics": orchestrator.current_metrics or {},
        "series": {field: _series(field) for field in _SERIES_FIELDS},
        "workers_series": _series("workers"),
    }


def create_app(
    config: Settings,
    orchestrator,
    async_mode: str = "eventlet",
) -> tuple[Flask, SocketIO]:
    """Build and configure the Flask app and its SocketIO server.

    The orchestrator is stored on ``app.config["ORCHESTRATOR"]`` and every API route
    resolves it through :func:`_require_orchestrator`, returning ``503`` if it is
    missing — so the app boots and serves ``/health`` even with no control plane.

    Args:
        config: The application :class:`~src.config.Settings`.
        orchestrator: The :class:`~src.orchestrator.Orchestrator` to read/command.
            May be ``None`` (then every ``/api/*`` route returns ``503``).
        async_mode: SocketIO async backend. Defaults to ``"eventlet"`` for
            production; tests pass ``"threading"`` to skip the eventlet server.

    Returns:
        A ``(app, socketio)`` tuple. Background loops are **not** started here — call
        :func:`start_background_tasks` once the eventlet hub is running.
    """
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.config["SECRET_KEY"] = "adaptive-resource-allocation"

    # Stow collaborators so handlers reach them without module-level globals.
    app.config["APP_CONFIG"] = config
    app.config["ORCHESTRATOR"] = orchestrator

    socketio = SocketIO(app, async_mode=async_mode, cors_allowed_origins="*")

    def _require_orchestrator():
        """Return ``(orchestrator, None)`` or ``(None, 503_response)`` if absent.

        The 503-when-missing idiom: API handlers call this first and short-circuit
        on the error tuple, so a half-wired app degrades to a clean 503 instead of
        an ``AttributeError`` on ``None``.
        """
        orch = app.config.get("ORCHESTRATOR")
        if orch is None:
            return None, (jsonify({"error": "orchestrator unavailable"}), 503)
        return orch, None

    # ── HTTP routes ──────────────────────────────────────────────────────

    @app.route("/")
    def index():
        """Serve the dashboard page, falling back to a stub if the template is absent.

        The real ``templates/index.html`` lands in a later commit; until then a
        :class:`jinja2.TemplateNotFound` is caught and a minimal HTML stub is
        returned with status 200 so the route is always healthy.
        """
        try:
            return render_template("index.html")
        except TemplateNotFound:
            return _INDEX_STUB, 200

    @app.route("/health")
    def health():
        """Liveness probe — always 200, independent of the orchestrator."""
        return jsonify({
            "status": "healthy",
            "service": "adaptive-resource-allocation",
        })

    @app.route("/api/status")
    def api_status():
        """Return the orchestrator's full status snapshot (the canonical payload)."""
        orch, error = _require_orchestrator()
        if error:
            return error
        return jsonify(orch.snapshot())

    @app.route("/api/metrics")
    def api_metrics():
        """Return current metrics plus the plotted time-series block."""
        orch, error = _require_orchestrator()
        if error:
            return error
        return jsonify(_build_series_block(orch))

    @app.route("/api/scaling", methods=["POST"])
    def api_scaling():
        """Apply a manual scale: body is ``{"direction": "up"|"down"}`` or ``{"target": N}``.

        Validates that exactly one of ``direction``/``target`` is usable and that
        ``direction`` (when given) is one of ``up``/``down``; otherwise returns 400.
        On success returns the orchestrator's decision dict (``reason == "manual"``).
        """
        orch, error = _require_orchestrator()
        if error:
            return error

        body = request.get_json(silent=True) or {}
        direction = body.get("direction")
        target = body.get("target")

        if direction is None and target is None:
            return jsonify({"error": "provide 'direction' or 'target'"}), 400

        if direction is not None and direction not in ("up", "down"):
            return jsonify({"error": "direction must be 'up' or 'down'"}), 400

        if target is not None:
            try:
                target = int(target)
            except (TypeError, ValueError):
                return jsonify({"error": "target must be an integer"}), 400

        decision = orch.request_manual_scale(direction=direction, target=target)
        return jsonify(decision)

    @app.route("/api/load", methods=["POST"])
    def api_load():
        """Inject a load ramp: body ``{"arrival_rate": N, "ramp_seconds": S}``.

        ``ramp_seconds`` is optional (default 10). ``arrival_rate`` must be a number
        ``>= 0`` else 400. On success the load model begins ramping toward the target
        and a confirmation payload is returned.
        """
        orch, error = _require_orchestrator()
        if error:
            return error

        body = request.get_json(silent=True) or {}
        arrival_rate = body.get("arrival_rate")
        ramp_seconds = body.get("ramp_seconds", 10)

        try:
            arrival_rate = float(arrival_rate)
        except (TypeError, ValueError):
            return jsonify({"error": "arrival_rate must be a number >= 0"}), 400
        if arrival_rate < 0:
            return jsonify({"error": "arrival_rate must be a number >= 0"}), 400

        try:
            ramp_seconds = float(ramp_seconds)
        except (TypeError, ValueError):
            return jsonify({"error": "ramp_seconds must be a number"}), 400

        orch.load_model.ramp(target_rate=arrival_rate, seconds=ramp_seconds)
        return jsonify({
            "status": "ramping",
            "target_arrival_rate": arrival_rate,
            "ramp_seconds": ramp_seconds,
        })

    # ── SocketIO events ──────────────────────────────────────────────────

    @socketio.on("connect")
    def handle_connect():
        """Paint the new client immediately with a full status + metrics snapshot.

        Emitting both events to *just this socket* (the default scope inside a
        connect handler) means the page renders the current state on load instead of
        waiting up to one emit interval for the next broadcast.
        """
        logger.info("Client connected")
        orch = app.config.get("ORCHESTRATOR")
        if orch is None:
            return
        emit("status_update", orch.snapshot())
        emit("metrics_update", _build_series_block(orch))

    @socketio.on("disconnect")
    def handle_disconnect():
        """Log the disconnect; no state to tear down (handlers are stateless)."""
        logger.info("Client disconnected")

    return app, socketio


def start_background_tasks(socketio: SocketIO, app: Flask, orchestrator, config: Settings) -> None:
    """Start the two server-side control loops as SocketIO background tasks.

    Two independent loops keep the collector and orchestration cadences separate
    (they may be configured differently) and each **broadcasts** to *all* connected
    clients via :meth:`SocketIO.emit`:

    * ``_metrics_loop`` — every ``config.monitoring_interval_seconds`` it advances
      the simulation (:meth:`Orchestrator.collector_tick`) and broadcasts a
      ``metrics_update`` (the same series block the ``/api/metrics`` route returns).
    * ``_orchestration_loop`` — every ``config.orchestration_interval_seconds`` it
      runs a scaling decision (:meth:`Orchestrator.orchestration_tick`) and
      broadcasts a ``status_update`` (the full :meth:`Orchestrator.snapshot`).

    Each loop body is wrapped in ``try/except`` so a transient error (e.g. a psutil
    hiccup) is logged and the loop continues rather than silently dying. ``sleep`` is
    :meth:`SocketIO.sleep`, which yields to the eventlet hub cooperatively.

    This function must be invoked **after** the eventlet hub is running (``main.py``
    does this); it is deliberately *not* called from :func:`create_app` so importing
    the factory never spawns threads.

    Args:
        socketio: The SocketIO server returned by :func:`create_app`.
        app: The Flask app (used for an app context inside each loop).
        orchestrator: The :class:`~src.orchestrator.Orchestrator` to drive.
        config: The :class:`~src.config.Settings` supplying the loop intervals.
    """

    def _metrics_loop():
        """Collect metrics on the monitoring cadence and broadcast them forever."""
        with app.app_context():
            while True:
                socketio.sleep(config.monitoring_interval_seconds)
                try:
                    orchestrator.collector_tick()
                    socketio.emit("metrics_update", _build_series_block(orchestrator))
                except Exception:  # noqa: BLE001 - a transient error must not kill the loop
                    logger.exception("metrics loop iteration failed; continuing")

    def _orchestration_loop():
        """Run scaling decisions on the orchestration cadence and broadcast status."""
        with app.app_context():
            while True:
                socketio.sleep(config.orchestration_interval_seconds)
                try:
                    orchestrator.orchestration_tick()
                    socketio.emit("status_update", orchestrator.snapshot())
                except Exception:  # noqa: BLE001 - a transient error must not kill the loop
                    logger.exception("orchestration loop iteration failed; continuing")

    socketio.start_background_task(_metrics_loop)
    socketio.start_background_task(_orchestration_loop)
