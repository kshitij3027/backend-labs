"""Flask + SocketIO dashboard application factory."""

import logging

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit

from src.config import Settings

logger = logging.getLogger(__name__)


def create_app(config: Settings, **components):
    """Create and configure the Flask + SocketIO dashboard application."""
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.config["SECRET_KEY"] = "kafka-streams-dashboard"

    socketio = SocketIO(app, async_mode="gevent", cors_allowed_origins="*")

    # Store config and components on the app for later use
    app.config["APP_CONFIG"] = config
    for key, value in components.items():
        app.config[key.upper()] = value

    # ── HTTP routes ──────────────────────────────────────────────────

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/health")
    def health():
        return jsonify({
            "status": "healthy",
            "service": "kafka-streams-monitoring-dashboard",
        })

    @app.route("/api/metrics")
    def api_metrics():
        metrics_store = app.config.get("METRICS_STORE")
        if metrics_store is None:
            return jsonify({"error": "metrics store not initialized"}), 503
        return jsonify(metrics_store.get_windowed_metrics(config.window_seconds))

    @app.route("/api/historical")
    def api_historical():
        metrics_store = app.config.get("METRICS_STORE")
        if metrics_store is None:
            return jsonify({"error": "metrics store not initialized"}), 503
        return jsonify(metrics_store.get_historical())

    # ── SocketIO events ──────────────────────────────────────────────

    @socketio.on("connect")
    def handle_connect():
        logger.info("Client connected")
        metrics_store = app.config.get("METRICS_STORE")
        if metrics_store:
            emit("metrics_update", {
                "metrics": metrics_store.get_windowed_metrics(config.window_seconds),
                "historical": metrics_store.get_historical(),
            })

    @socketio.on("disconnect")
    def handle_disconnect():
        logger.info("Client disconnected")

    return app, socketio


def start_background_tasks(socketio, app, producer=None):
    """Start the background emitter that pushes metrics to all clients."""

    def _metrics_emitter():
        with app.app_context():
            while True:
                socketio.sleep(app.config["APP_CONFIG"].ws_emit_interval)
                metrics_store = app.config.get("METRICS_STORE")
                if metrics_store:
                    config = app.config["APP_CONFIG"]
                    metrics = metrics_store.get_windowed_metrics(config.window_seconds)
                    historical = metrics_store.get_historical()

                    # Push to WebSocket clients
                    socketio.emit("metrics_update", {
                        "metrics": metrics,
                        "historical": historical,
                    })

                    # Produce derived metrics to Kafka
                    if producer:
                        producer.produce_derived_metrics(metrics)

    socketio.start_background_task(_metrics_emitter)
