"""Flask application factory for a single storage node.

Exposes REST endpoints for health checks, reading/writing log data,
listing stored files, receiving replicas, and viewing node statistics.
"""

from flask import Flask, jsonify, request

from src.config import ClusterConfig
from src.file_store import FileStore


def create_app(config: ClusterConfig) -> Flask:
    """Create and configure the Flask application for a storage node.

    Args:
        config: The cluster configuration for this node.

    Returns:
        A fully configured Flask app instance.
    """
    app = Flask(__name__)
    app.config["CLUSTER_CONFIG"] = config

    file_store = FileStore(config.storage_dir, config.node_id)
    app.config["FILE_STORE"] = file_store

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({
            "status": "healthy",
            "node_id": config.node_id,
            "port": config.port,
        })

    @app.route("/write", methods=["POST"])
    def write():
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        result = file_store.write(data)
        return jsonify(result), 201

    @app.route("/read/<path:file_path>", methods=["GET"])
    def read(file_path):
        record = file_store.read(file_path)
        if record is None:
            return jsonify({"error": "File not found"}), 404
        return jsonify(record)

    @app.route("/files", methods=["GET"])
    def list_files():
        files = file_store.list_files()
        return jsonify({"files": files, "count": len(files)})

    @app.route("/replicate", methods=["POST"])
    def replicate():
        payload = request.get_json()
        if not payload or "file_path" not in payload:
            return jsonify({"error": "Missing file_path"}), 400
        result = file_store.write_replica(
            payload["file_path"],
            payload.get("data", {}),
            payload.get("metadata", {}),
        )
        return jsonify(result), 201

    @app.route("/stats", methods=["GET"])
    def stats():
        return jsonify({
            "node_id": config.node_id,
            "stats": file_store.get_stats(),
            "files_count": len(file_store.list_files()),
        })

    return app
