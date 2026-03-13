"""Flask + SocketIO real-time dashboard for the smart log routing pipeline."""

import os
import time

from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO

from src.dashboard.stats_collector import StatsCollector

# Resolve template directory relative to this file
_web_templates = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "web", "templates",
)

app = Flask(__name__, template_folder=os.path.normpath(_web_templates))
socketio = SocketIO(app, async_mode="eventlet", cors_allowed_origins="*")

stats_collector = StatsCollector()

# ── HTTP routes ──────────────────────────────────────────────────────

@app.route("/")
def index():
    """Serve the dashboard HTML page."""
    return render_template("index.html")


@app.route("/api/stats")
def api_stats():
    """Return queue stats as JSON."""
    return jsonify(stats_collector.get_queue_stats())


@app.route("/health")
def health():
    """Simple health-check endpoint."""
    return jsonify({"status": "ok"})


# ── SocketIO events ─────────────────────────────────────────────────

@socketio.on("connect")
def handle_connect():
    """Send initial stats payload as soon as a client connects."""
    stats = stats_collector.get_queue_stats()
    socketio.emit("stats_update", {"queues": stats, "timestamp": time.time()})


# ── Background emitter ──────────────────────────────────────────────

def _background_stats_emitter():
    """Periodically emit stats_update events to all connected clients."""
    while True:
        socketio.sleep(2)
        stats = stats_collector.get_queue_stats()
        socketio.emit("stats_update", {"queues": stats, "timestamp": time.time()})


def start_dashboard(host="0.0.0.0", port=5555):
    """Launch the SocketIO-powered dashboard server."""
    socketio.start_background_task(_background_stats_emitter)
    socketio.run(app, host=host, port=port)


if __name__ == "__main__":
    dashboard_port = int(os.environ.get("DASHBOARD_PORT", "5555"))
    start_dashboard(host="0.0.0.0", port=dashboard_port)
