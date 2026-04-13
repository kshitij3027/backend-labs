"""Minimal Flask application for the anomaly detection engine."""
from __future__ import annotations

import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify
from flask_socketio import SocketIO

from src.config import Config

config = Config.from_env()

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="../static",
)

socketio = SocketIO(app, async_mode="eventlet", cors_allowed_origins="*")


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


@app.route("/")
def index():
    """Dashboard placeholder."""
    return (
        "<h1>Anomaly Detection Engine</h1>"
        "<p>Dashboard coming soon...</p>"
    )


if __name__ == "__main__":
    socketio.run(
        app,
        host="0.0.0.0",
        port=config.flask_port,
        debug=config.debug,
    )
