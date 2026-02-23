"""Flask application for the Avro Schema Evolution Dashboard."""

import base64
import os

from flask import Flask, jsonify, request, render_template

from src.schema_registry import SchemaRegistry
from src.serializer import AvroSerializer
from src.deserializer import AvroDeserializer
from src.compatibility import CompatibilityChecker
from src.log_event import LogEvent

# Module-level singletons
registry = SchemaRegistry()
serializer = AvroSerializer(registry)
deserializer = AvroDeserializer(registry)
checker = CompatibilityChecker(registry, serializer, deserializer)


def create_app():
    """Application factory for the Flask app."""
    template_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates"
    )
    app = Flask(__name__, template_folder=template_dir)

    # ------------------------------------------------------------------
    # 1. Dashboard
    # ------------------------------------------------------------------
    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    # ------------------------------------------------------------------
    # 2. Health check
    # ------------------------------------------------------------------
    @app.route("/health")
    def health():
        return jsonify({"status": "healthy"})

    # ------------------------------------------------------------------
    # 3. List all schemas
    # ------------------------------------------------------------------
    @app.route("/api/schemas")
    def list_schemas():
        versions = registry.list_versions()
        schemas = []
        for v in versions:
            raw = registry.get_schema(v)
            schemas.append(
                {
                    "version": v,
                    "name": raw.get("name", "LogEvent"),
                    "fields": registry.get_field_names(v),
                }
            )
        return jsonify({"status": "success", "data": {"schemas": schemas}})

    # ------------------------------------------------------------------
    # 4. Get schema detail
    # ------------------------------------------------------------------
    @app.route("/api/schemas/<version>")
    def get_schema(version):
        try:
            raw = registry.get_schema(version)
        except KeyError:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Schema version '{version}' not found",
                    }
                ),
                404,
            )
        return jsonify({"status": "success", "data": {"version": version, "schema": raw}})

    # ------------------------------------------------------------------
    # 5. Schema info + compatibility matrix
    # ------------------------------------------------------------------
    @app.route("/api/schema-info")
    def schema_info():
        versions = registry.list_versions()
        schema_details = []
        for v in versions:
            fields = registry.get_field_names(v)
            schema_details.append(
                {"version": v, "field_count": len(fields), "fields": fields}
            )
        matrix = checker.build_compatibility_matrix()
        return jsonify(
            {
                "status": "success",
                "data": {
                    "available_schemas": versions,
                    "schema_details": schema_details,
                    "compatibility_matrix": matrix,
                },
            }
        )

    # ------------------------------------------------------------------
    # 6. Check compatibility between two versions
    # ------------------------------------------------------------------
    @app.route("/api/compatibility/check", methods=["POST"])
    def compatibility_check():
        body = request.get_json(silent=True) or {}
        writer = body.get("writer_schema")
        reader = body.get("reader_schema")
        mode = body.get("mode", "backward")

        if not writer or not reader:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Missing required fields: writer_schema, reader_schema",
                    }
                ),
                400,
            )

        versions = registry.list_versions()
        if writer not in versions or reader not in versions:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Invalid schema version. Available: {versions}",
                    }
                ),
                400,
            )

        if mode == "backward":
            compatible = checker.check_compatibility(writer, reader)
            details = (
                f"Reader ({reader}) can read data written by writer ({writer})"
                if compatible
                else f"Reader ({reader}) CANNOT read data written by writer ({writer})"
            )
        elif mode == "forward":
            compatible = checker.check_compatibility(reader, writer)
            details = (
                f"Writer ({writer}) can read data written by reader ({reader})"
                if compatible
                else f"Writer ({writer}) CANNOT read data written by reader ({reader})"
            )
        elif mode == "full":
            backward_ok = checker.check_compatibility(writer, reader)
            forward_ok = checker.check_compatibility(reader, writer)
            compatible = backward_ok and forward_ok
            details = (
                "Full compatibility: both directions work"
                if compatible
                else f"Backward={'OK' if backward_ok else 'FAIL'}, Forward={'OK' if forward_ok else 'FAIL'}"
            )
        else:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Invalid mode '{mode}'. Use: backward, forward, full",
                    }
                ),
                400,
            )

        return jsonify(
            {
                "status": "success",
                "data": {
                    "compatible": compatible,
                    "mode": mode,
                    "writer_schema": writer,
                    "reader_schema": reader,
                    "details": details,
                },
            }
        )

    # ------------------------------------------------------------------
    # 7. Test compatibility (serialize a sample event)
    # ------------------------------------------------------------------
    @app.route("/api/test-compatibility", methods=["POST"])
    def test_compatibility():
        body = request.get_json(silent=True) or {}
        version = body.get("schema_version", "v1")

        versions = registry.list_versions()
        if version not in versions:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Invalid schema version '{version}'. Available: {versions}",
                    }
                ),
                400,
            )

        sample = LogEvent.generate_sample(version)
        event_dict = sample.to_dict(version)
        data = serializer.serialize(event_dict, version)

        return jsonify(
            {
                "status": "success",
                "message": f"Event processed with schema {version}. Size: {len(data)} bytes",
                "sample_data": event_dict,
            }
        )

    # ------------------------------------------------------------------
    # 8. Generate multiple events
    # ------------------------------------------------------------------
    @app.route("/api/generate", methods=["POST"])
    def generate_events():
        body = request.get_json(silent=True) or {}
        version = body.get("schema_version", "v1")
        count = body.get("count", 1)

        versions = registry.list_versions()
        if version not in versions:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Invalid schema version '{version}'. Available: {versions}",
                    }
                ),
                400,
            )

        count = min(max(int(count), 1), 100)
        events = []
        for _ in range(count):
            sample = LogEvent.generate_sample(version)
            event_dict = sample.to_dict(version)
            container_bytes = serializer.serialize_to_container([event_dict], version)
            events.append(
                {
                    "raw": event_dict,
                    "avro_binary_base64": base64.b64encode(container_bytes).decode(),
                    "size_bytes": len(container_bytes),
                }
            )

        return jsonify(
            {
                "status": "success",
                "data": {
                    "schema_version": version,
                    "count": count,
                    "events": events,
                },
            }
        )

    # ------------------------------------------------------------------
    # 9. Generate a single sample event
    # ------------------------------------------------------------------
    @app.route("/api/generate-sample/<version>")
    def generate_sample(version):
        versions = registry.list_versions()
        if version not in versions:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Schema version '{version}' not found",
                    }
                ),
                404,
            )

        sample = LogEvent.generate_sample(version)
        event_dict = sample.to_dict(version)
        return jsonify(
            {"status": "success", "data": {"version": version, "sample": event_dict}}
        )

    # ------------------------------------------------------------------
    # 10. Deserialize Avro data
    # ------------------------------------------------------------------
    @app.route("/api/deserialize", methods=["POST"])
    def deserialize_data():
        schema_version = None
        raw_data = None

        if request.content_type and "multipart/form-data" in request.content_type:
            schema_version = request.form.get("schema_version")
            uploaded = request.files.get("file")
            if uploaded:
                raw_data = uploaded.read()
        else:
            body = request.get_json(silent=True) or {}
            schema_version = body.get("schema_version")
            b64_data = body.get("data")
            if b64_data:
                raw_data = base64.b64decode(b64_data)

        if not schema_version or raw_data is None:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Missing schema_version or data",
                    }
                ),
                400,
            )

        versions = registry.list_versions()
        if schema_version not in versions:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Invalid schema version '{schema_version}'. Available: {versions}",
                    }
                ),
                400,
            )

        try:
            records = deserializer.deserialize_container(raw_data, reader_version=schema_version)
        except Exception as exc:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": f"Deserialization failed: {str(exc)}",
                    }
                ),
                400,
            )

        return jsonify(
            {
                "status": "success",
                "data": {
                    "schema_version": schema_version,
                    "records": records,
                    "record_count": len(records),
                },
            }
        )

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=False)
