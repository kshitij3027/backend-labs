"""Flask application factory for a single storage node.

Exposes REST endpoints for health checks, reading/writing log data,
listing stored files, receiving replicas, and viewing node statistics.
"""

import time

from flask import Flask, jsonify, request

from src.config import ClusterConfig
from src.file_store import FileStore


def create_app(config: ClusterConfig, replication_manager=None) -> Flask:
    """Create and configure the Flask application for a storage node.

    Args:
        config: The cluster configuration for this node.
        replication_manager: Optional ReplicationManager for async
            replication to peer nodes.

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

        # Trigger async replication to peer nodes
        if replication_manager:
            metadata = {
                "version": result["version"],
                "checksum": result["checksum"],
                "node_id": config.node_id,
                "created_at": time.time(),
            }
            replication_manager.replicate(result["file_path"], data, metadata)

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

    @app.route("/replication/status", methods=["GET"])
    def replication_status():
        if replication_manager:
            return jsonify(replication_manager.get_stats())
        return jsonify({"status": "replication not configured"})

    return app
