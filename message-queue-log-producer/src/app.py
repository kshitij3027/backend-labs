"""Flask HTTP API for accepting log entries."""

from flask import Flask, request, jsonify
from src.config import Config


REQUIRED_FIELDS = {"level", "message", "source"}


def create_app(config=None):
    """Flask application factory."""
    if config is None:
        config = Config()

    app = Flask(__name__)
    app.config["APP_CONFIG"] = config

    @app.route("/logs", methods=["POST"])
    def ingest_logs():
        """Accept one or more log entries via JSON body."""
        data = request.get_json(silent=True)
        if data is None:
            return jsonify({"error": "Invalid JSON"}), 400

        # Normalise to a list
        if isinstance(data, dict):
            entries = [data]
        elif isinstance(data, list):
            entries = data
        else:
            return jsonify({"error": "Expected JSON object or array"}), 400

        # Validate required fields
        for entry in entries:
            if not isinstance(entry, dict):
                return jsonify({"error": "Each entry must be a JSON object"}), 400
            missing = REQUIRED_FIELDS - entry.keys()
            if missing:
                return (
                    jsonify({"error": f"Missing fields: {sorted(missing)}"}),
                    400,
                )

        return jsonify({"accepted": len(entries)}), 202

    @app.route("/health", methods=["GET"])
    def health():
        """Health check endpoint (stub)."""
        return jsonify({"healthy": True, "status": "ok"}), 200

    @app.route("/metrics", methods=["GET"])
    def metrics():
        """Metrics endpoint (stub)."""
        return jsonify({"status": "stub"}), 200

    return app


def run_app(config=None):
    """Create and run the Flask app."""
    if config is None:
        config = Config()
    app = create_app(config)
    app.run(host="0.0.0.0", port=config.http_port)


if __name__ == "__main__":
    run_app()
