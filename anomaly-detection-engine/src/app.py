"""Flask-SocketIO application for the anomaly detection engine."""
from __future__ import annotations

import eventlet
eventlet.monkey_patch()

import logging
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit

from src.config import Config
from src.generator.log_generator import LogGenerator
from src.models import LogEntry
from src.pipeline.engine import DetectionEngine

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

config = Config.from_env()

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="../static",
)

socketio = SocketIO(app, async_mode="eventlet", cors_allowed_origins="*")

engine = DetectionEngine(config)
generator = LogGenerator(anomaly_rate=config.anomaly_rate, seed=config.random_seed)

_bg_started = False

# ---------------------------------------------------------------------------
# Background detection loop
# ---------------------------------------------------------------------------


def background_detection_loop() -> None:
    """Continuously generate logs, run them through the engine, and broadcast."""
    logger.info("Background detection loop started (rate=%d logs/sec)", config.log_rate)

    interval = 1.0 / max(config.log_rate, 1)
    broadcast_interval = 0.5
    last_broadcast = 0.0

    while True:
        # Generate and process one log entry
        log_entry = generator.generate()
        engine.process_log(log_entry)

        # Broadcast to connected clients at ~2 Hz
        now = eventlet.hubs.get_hub().clock()
        if now - last_broadcast >= broadcast_interval:
            last_broadcast = now
            stats = engine.get_stats()
            recent = engine.get_recent_anomalies(limit=20)
            payload = {
                "stats": stats,
                "recent_anomalies": recent,
                "is_warm": engine.is_warm(),
            }
            socketio.emit("anomaly_update", payload)

        eventlet.sleep(interval)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    """Serve the real-time monitoring dashboard."""
    return render_template("dashboard.html")


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


@app.route("/api/stats")
def api_stats():
    """Return current detection statistics."""
    return jsonify(engine.get_stats())


@app.route("/api/anomalies")
def api_anomalies():
    """Return recent anomalies."""
    limit = request.args.get("limit", 50, type=int)
    return jsonify(engine.get_recent_anomalies(limit=limit))


@app.route("/api/feedback", methods=["POST"])
def api_feedback():
    """Accept operator feedback on a flagged anomaly.

    Expects JSON: {"anomaly_id": "...", "confirmed": true/false}
    """
    data = request.get_json(force=True)
    anomaly_id = data.get("anomaly_id", "")
    confirmed = bool(data.get("confirmed", False))

    engine.feedback(anomaly_id, confirmed)

    return jsonify({
        "status": "ok",
        "current_threshold": engine._adaptive_threshold.get_threshold(),
    })


@app.route("/api/logs", methods=["POST"])
def api_logs():
    """Accept an external log entry, process it, and return the result."""
    data = request.get_json(force=True)

    log_entry = LogEntry(
        timestamp=datetime.now(timezone.utc),
        ip=data.get("ip", "0.0.0.0"),
        method=data.get("method", "GET"),
        path=data.get("path", "/"),
        status_code=data.get("status_code", 200),
        response_time=float(data.get("response_time", 100.0)),
        bytes_sent=int(data.get("bytes_sent", 1024)),
        user_agent=data.get("user_agent", "unknown"),
        session_duration=float(data.get("session_duration", 60.0)),
        page_views=int(data.get("page_views", 1)),
        _is_anomaly=data.get("_is_anomaly", False),
        _anomaly_type=data.get("_anomaly_type", ""),
    )

    result = engine.process_log(log_entry)

    return jsonify({
        "is_anomaly": result.is_anomaly,
        "confidence": result.confidence,
        "scores": result.scores,
    })


# ---------------------------------------------------------------------------
# SocketIO events
# ---------------------------------------------------------------------------


@socketio.on("connect")
def on_connect():
    """Handle new WebSocket connection."""
    global _bg_started
    logger.info("Client connected")

    # Send initial stats immediately
    emit("anomaly_update", {
        "stats": engine.get_stats(),
        "recent_anomalies": engine.get_recent_anomalies(limit=20),
        "is_warm": engine.is_warm(),
    })

    # Start the background loop on first connection (also started on app init below)
    if not _bg_started:
        _bg_started = True
        socketio.start_background_task(background_detection_loop)
        logger.info("Background task launched on first client connect")


@socketio.on("disconnect")
def on_disconnect():
    """Handle WebSocket disconnection."""
    logger.info("Client disconnected")


# ---------------------------------------------------------------------------
# Start background task eagerly so logs are processed even without SocketIO
# clients (e.g., during E2E / load tests that only use HTTP).
# ---------------------------------------------------------------------------

def _start_bg_on_ready() -> None:
    """Spawn the detection loop once the eventlet hub is running."""
    global _bg_started
    if not _bg_started:
        _bg_started = True
        socketio.start_background_task(background_detection_loop)
        logger.info("Background task launched on app startup")


# Use eventlet.spawn_after so the loop starts once the event loop is running.
_startup_timer = eventlet.spawn_after(1, _start_bg_on_ready)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=config.flask_port,
        debug=config.debug,
        use_reloader=False,
    )
