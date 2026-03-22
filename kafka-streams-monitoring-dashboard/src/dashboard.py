"""Flask + SocketIO dashboard application factory."""

import logging

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO

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

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/health")
    def health():
        return jsonify({
            "status": "healthy",
            "service": "kafka-streams-monitoring-dashboard",
        })

    return app, socketio
