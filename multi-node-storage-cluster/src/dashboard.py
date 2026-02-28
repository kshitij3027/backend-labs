"""Monitoring dashboard for the multi-node storage cluster."""

import json
import os

import requests
from flask import Flask, jsonify, render_template


def create_dashboard_app(cluster_nodes=None) -> Flask:
    """Create the monitoring dashboard Flask app.

    Args:
        cluster_nodes: List of {"id", "host", "port"} dicts.
            If None, reads from CLUSTER_NODES env var.
    """
    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
    )

    if cluster_nodes is None:
        raw = os.environ.get("CLUSTER_NODES", "[]")
        try:
            cluster_nodes = json.loads(raw)
        except json.JSONDecodeError:
            cluster_nodes = []

    app.config["CLUSTER_NODES"] = cluster_nodes

    def _poll_node(node):
        """Poll a single node for its health, stats, files, replication, and cluster info."""
        base = f"http://{node['host']}:{node['port']}"
        result = {"id": node["id"], "host": node["host"], "port": node["port"]}

        # Health
        try:
            resp = requests.get(f"{base}/health", timeout=2)
            if resp.status_code == 200:
                result["health"] = resp.json()
                result["status"] = "healthy"
            else:
                result["status"] = "unhealthy"
                result["health"] = {"error": f"HTTP {resp.status_code}"}
        except requests.RequestException:
            result["status"] = "unhealthy"
            result["health"] = {"error": "unreachable"}

        # Stats
        try:
            resp = requests.get(f"{base}/stats", timeout=2)
            result["stats"] = resp.json() if resp.status_code == 200 else {}
        except requests.RequestException:
            result["stats"] = {}

        # Files
        try:
            resp = requests.get(f"{base}/files", timeout=2)
            result["files"] = (
                resp.json()
                if resp.status_code == 200
                else {"files": [], "count": 0}
            )
        except requests.RequestException:
            result["files"] = {"files": [], "count": 0}

        # Replication status
        try:
            resp = requests.get(f"{base}/replication/status", timeout=2)
            result["replication"] = resp.json() if resp.status_code == 200 else {}
        except requests.RequestException:
            result["replication"] = {}

        return result

    @app.route("/")
    def index():
        return render_template("dashboard.html")

    @app.route("/api/cluster")
    def api_cluster():
        """Poll all nodes and return cluster-wide status."""
        nodes = []
        for node in cluster_nodes:
            nodes.append(_poll_node(node))

        healthy_count = sum(1 for n in nodes if n["status"] == "healthy")
        total_count = len(nodes)
        quorum = healthy_count >= 2  # majority

        return jsonify(
            {
                "nodes": nodes,
                "healthy_count": healthy_count,
                "total_count": total_count,
                "quorum": quorum,
            }
        )

    @app.route("/api/files")
    def api_files():
        """Aggregate file list from all nodes."""
        all_files = {}
        for node in cluster_nodes:
            base = f"http://{node['host']}:{node['port']}"
            try:
                resp = requests.get(f"{base}/files", timeout=2)
                if resp.status_code == 200:
                    data = resp.json()
                    for f in data.get("files", []):
                        if f not in all_files:
                            all_files[f] = []
                        all_files[f].append(node["id"])
            except requests.RequestException:
                pass

        return jsonify(
            {
                "files": [
                    {"path": f, "replicas": nodes}
                    for f, nodes in sorted(all_files.items())
                ],
                "total": len(all_files),
            }
        )

    @app.route("/api/health")
    def api_health():
        return jsonify({"status": "dashboard running"})

    return app
