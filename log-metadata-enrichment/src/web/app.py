"""Flask application factory for the log metadata enrichment web interface."""

from __future__ import annotations

from flask import Flask, jsonify, request, render_template

from src.config import AppConfig, load_config
from src.enricher import LogEnricher
from src.formatter import format_enriched_log_dict
from src.models import EnrichmentRequest


SAMPLE_LOGS = [
    "INFO: Application started successfully on port 8080",
    "ERROR: Failed to connect to database - connection timeout after 30s",
    "WARNING: Memory usage exceeds 80% threshold",
    "DEBUG: Processing request #12345 from 192.168.1.1",
    "CRITICAL: Unhandled exception in worker thread - NullPointerError",
]


def create_app(config: AppConfig = None) -> Flask:
    """Create and configure the Flask application.

    Args:
        config: Application configuration. If None, loads from environment.

    Returns:
        Configured Flask application instance.
    """
    if config is None:
        config = load_config()

    app = Flask(__name__, template_folder="templates")
    enricher = LogEnricher(config)

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/health")
    def health():
        return jsonify({
            "status": "healthy",
            "service": config.service_name,
            "version": config.version,
        })

    @app.route("/api/enrich", methods=["POST"])
    def enrich():
        data = request.get_json(silent=True)
        if not data or "log_message" not in data:
            return jsonify({"error": "log_message is required"}), 400

        req = EnrichmentRequest(
            log_message=data["log_message"],
            source=data.get("source", "unknown"),
        )
        enriched = enricher.enrich(req)
        return jsonify(format_enriched_log_dict(enriched))

    @app.route("/api/stats")
    def stats():
        return jsonify(enricher.get_stats().model_dump())

    @app.route("/api/sample-logs")
    def sample_logs():
        return jsonify(SAMPLE_LOGS)

    return app
