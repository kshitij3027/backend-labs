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

    @app.route("/api/business-metrics")
    def api_business_metrics():
        bm = app.config.get("BUSINESS_METRICS")
        if bm is None:
            return jsonify({"api_versions": {}, "funnel": {}, "auth": {}})
        return jsonify(bm.get_business_metrics())

    @app.route("/api/geo")
    def api_geo():
        geo = app.config.get("GEO_ANALYZER")
        if geo is None:
            return jsonify({"traffic_by_region": {}, "latency_by_region": {}})
        return jsonify(geo.get_geo_metrics())

    @app.route("/api/alerts")
    def api_alerts():
        alert_manager = app.config.get("ALERT_MANAGER")
        if alert_manager is None:
            return jsonify({"active": [], "history": []})
        return jsonify({
            "active": alert_manager.get_active_alerts(),
            "history": alert_manager.get_alert_history(),
        })

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

                    # Gather business metrics and geo data
                    bm = app.config.get("BUSINESS_METRICS")
                    geo = app.config.get("GEO_ANALYZER")

                    # Push to WebSocket clients
                    socketio.emit("metrics_update", {
                        "metrics": metrics,
                        "historical": historical,
                        "business_metrics": bm.get_business_metrics() if bm else None,
                        "geo": geo.get_geo_metrics() if geo else None,
                    })

                    # Evaluate alerts and push to clients
                    alert_manager = app.config.get("ALERT_MANAGER")
                    if alert_manager:
                        new_alerts = alert_manager.evaluate(metrics)
                        if new_alerts:
                            socketio.emit("alert_update", {"alerts": new_alerts})

                    # Produce derived metrics to Kafka
                    if producer:
                        producer.produce_derived_metrics(metrics)

    socketio.start_background_task(_metrics_emitter)
