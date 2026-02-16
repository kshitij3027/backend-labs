"""Flask monitoring dashboard for the UDP log server."""

import os

from flask import Flask, jsonify, render_template

from src.error_tracker import ErrorTracker
from src.metrics import Metrics

_TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")


def create_dashboard_app(metrics: Metrics, error_tracker: ErrorTracker) -> Flask:
    app = Flask(__name__, template_folder=_TEMPLATE_DIR)

    @app.route("/")
    def index():
        return render_template("dashboard.html")

    @app.route("/stats")
    def stats():
        snap = metrics.snapshot()
        snap["recent_errors"] = error_tracker.get_recent(10)
        return jsonify(snap)

    @app.route("/health")
    def health():
        return jsonify(status="ok")

    return app


def run_dashboard(app: Flask, port: int):
    """Run the Flask app (intended for use in a daemon thread)."""
    app.run(host="0.0.0.0", port=port, use_reloader=False)
