"""Flask application factory for Schema Registry Service."""
import os
from flask import Flask, jsonify, request, render_template

from src.storage import FileStorage
from src.registry import SchemaRegistry
from src.validators import ValidatorManager
from src.compatibility import CompatibilityChecker
from src.metrics import MetricsTracker


def create_app(storage_path=None):
    """Create and configure the Flask application.

    Args:
        storage_path: Optional path for the storage file. Defaults to data/registry.json.
    """
    template_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"
    )
    app = Flask(__name__, template_folder=template_dir)

    path = storage_path or os.environ.get("STORAGE_PATH", "data/registry.json")
    storage = FileStorage(path)
    registry = SchemaRegistry(storage)
    validators = ValidatorManager()
    compat_checker = CompatibilityChecker()
    metrics = MetricsTracker()

    # Recompile validators for existing schemas on startup
    state = storage.get_state()
    for sid, record in state.get("schemas", {}).items():
        try:
            validators.compile(record["id"], record["schema"], record["schema_type"])
        except Exception:
            pass  # Skip invalid schemas on startup

    @app.route("/health")
    def health():
        state = storage.get_state()
        schema_count = len(state.get("schemas", {}))
        subject_count = len(state.get("subjects", {}))
        return jsonify({
            "status": "healthy",
            "schema_count": schema_count,
            "subject_count": subject_count,
        })

    @app.route("/schemas", methods=["POST"])
    def register_schema():
        body = request.get_json(silent=True) or {}
        subject = body.get("subject")
        schema = body.get("schema")
        schema_type = body.get("schema_type", "json")

        if not subject or schema is None:
            return jsonify({"status": "error", "message": "Missing required fields: subject, schema"}), 400

        if schema_type not in ("json", "avro"):
            return jsonify({"status": "error", "message": f"Invalid schema_type: {schema_type}. Must be 'json' or 'avro'"}), 400

        record, created = registry.register(subject, schema, schema_type)

        if created:
            try:
                validators.compile(record["id"], record["schema"], record["schema_type"])
            except Exception as e:
                return jsonify({"status": "error", "message": f"Invalid schema: {str(e)}"}), 400

        status_code = 201 if created else 200
        if created:
            metrics.record_registration()
        return jsonify({"status": "success", "data": record}), status_code

    @app.route("/schemas/subjects")
    def list_subjects():
        subjects = registry.list_subjects()
        return jsonify({"status": "success", "data": subjects})

    @app.route("/schemas/subjects/<subject>")
    def get_latest_schema(subject):
        try:
            record = registry.get_latest(subject)
        except KeyError as e:
            return jsonify({"status": "error", "message": str(e)}), 404
        return jsonify({"status": "success", "data": record})

    @app.route("/schemas/subjects/<subject>/versions")
    def list_versions(subject):
        try:
            versions = registry.list_versions(subject)
        except KeyError as e:
            return jsonify({"status": "error", "message": str(e)}), 404
        return jsonify({"status": "success", "data": versions})

    @app.route("/schemas/subjects/<subject>/versions/<int:version>")
    def get_version(subject, version):
        try:
            record = registry.get_version(subject, version)
        except KeyError as e:
            return jsonify({"status": "error", "message": str(e)}), 404
        return jsonify({"status": "success", "data": record})

    @app.route("/validate", methods=["POST"])
    def validate_data():
        body = request.get_json(silent=True) or {}
        subject = body.get("subject")
        data = body.get("data")
        version = body.get("version")

        if not subject or data is None:
            return jsonify({"status": "error", "message": "Missing required fields: subject, data"}), 400

        try:
            if version:
                record = registry.get_version(subject, version)
            else:
                record = registry.get_latest(subject)
        except KeyError as e:
            return jsonify({"status": "error", "message": str(e)}), 404

        valid, errors = validators.validate(record["id"], data, record["schema_type"])
        metrics.record_validation(subject, valid)
        return jsonify({
            "status": "success",
            "data": {
                "valid": valid,
                "schema_id": record["id"],
                "schema_version": record["version"],
                "errors": errors,
            }
        })

    @app.route("/compatibility/subjects/<subject>", methods=["POST"])
    def check_compatibility(subject):
        body = request.get_json(silent=True) or {}
        schema = body.get("schema")
        schema_type = body.get("schema_type", "json")

        if schema is None:
            return jsonify({"status": "error", "message": "Missing required field: schema"}), 400

        try:
            latest = registry.get_latest(subject)
        except KeyError as e:
            return jsonify({"status": "error", "message": str(e)}), 404

        compatible, issues = compat_checker.check_backward(schema, latest["schema"], schema_type)
        return jsonify({
            "status": "success",
            "data": {
                "compatible": compatible,
                "issues": issues,
                "checked_against": {
                    "subject": subject,
                    "version": latest["version"],
                    "schema_id": latest["id"],
                },
            }
        })

    @app.route("/metrics")
    def get_metrics():
        return jsonify({"status": "success", "data": metrics.get_metrics()})

    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    # Store references on app for use by other modules/phases
    app.storage = storage
    app.registry = registry
    app.validators = validators
    app.compat_checker = compat_checker
    app.metrics = metrics

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=8080, debug=False)
