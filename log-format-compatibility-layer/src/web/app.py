"""Flask web application for the log format compatibility layer."""
import os
import json
import tempfile
from flask import Flask, request, jsonify, render_template
from src.detection import FormatDetectionEngine
from src.pipeline import process_file
from src.config import DEFAULT_LOG_DIR


def create_app():
    """Flask application factory."""
    app = Flask(__name__, template_folder="templates")
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max upload

    engine = FormatDetectionEngine()

    @app.route("/")
    def index():
        """Serve the main web UI."""
        return render_template("index.html")

    @app.route("/health")
    def health():
        """Health check endpoint."""
        return jsonify({
            "status": "healthy",
            "service": "log-format-compatibility-layer",
            "version": "1.0.0",
        })

    @app.route("/api/upload", methods=["POST"])
    def upload():
        """Process uploaded log data.

        Accepts either:
        - File upload via multipart form (field name: 'file')
        - Raw text via form field 'text'
        """
        output_format = request.form.get("format", "json")

        # Get log content
        content = None
        if "file" in request.files:
            file = request.files["file"]
            if file.filename:
                content = file.read().decode("utf-8", errors="replace")

        if content is None:
            content = request.form.get("text", "")

        if not content.strip():
            return jsonify({"error": "No log data provided"}), 400

        # Write to temp file for pipeline processing
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        try:
            results = []
            metrics = None

            for formatted, data in process_file(tmp_path, output_format=output_format):
                if formatted == "__metrics__":
                    metrics = data
                    continue
                if output_format == "json":
                    results.append(json.loads(formatted))
                else:
                    results.append(formatted)

            return jsonify({
                "results": results,
                "metrics": metrics,
                "count": len(results),
            })
        finally:
            os.unlink(tmp_path)

    @app.route("/api/sample")
    def api_sample():
        """Return sample log data for demonstration."""
        sample_type = request.args.get("type", "mixed")
        file_map = {
            "syslog": "syslog_sample.txt",
            "journald": "journald_sample.txt",
            "json": "json_sample.txt",
            "mixed": "mixed_sample.txt",
        }

        filename = file_map.get(sample_type, "mixed_sample.txt")
        filepath = os.path.join(DEFAULT_LOG_DIR, filename)

        if not os.path.exists(filepath):
            return jsonify({"error": f"Sample file not found: {filename}"}), 404

        # Read and process the sample
        results = []
        metrics = None

        for formatted, data in process_file(filepath, output_format="json"):
            if formatted == "__metrics__":
                metrics = data
                continue
            results.append(json.loads(formatted))

        # Read raw content too
        with open(filepath, "r") as f:
            raw_content = f.read()

        return jsonify({
            "raw": raw_content,
            "results": results,
            "metrics": metrics,
            "count": len(results),
            "sample_type": sample_type,
        })

    @app.route("/api/config")
    def api_config():
        """Return current configuration."""
        from src.config import (
            FACILITY_MAP, SEVERITY_MAP,
            CONFIDENCE_THRESHOLD, HIGH_CONFIDENCE_THRESHOLD,
        )

        adapter_info = []
        for adapter in engine.registry.adapters:
            adapter_info.append({
                "name": adapter.format_name,
                "class": adapter.__class__.__name__,
            })

        return jsonify({
            "adapters": adapter_info,
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "high_confidence_threshold": HIGH_CONFIDENCE_THRESHOLD,
            "supported_output_formats": ["json", "structured", "plain"],
            "facility_map": FACILITY_MAP,
            "severity_map": SEVERITY_MAP,
        })

    return app
