"""Flask application factory for the consistent hashing dashboard."""

import random
import string
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request

from src.config import ClusterConfig, load_config, load_config_from_env
from src.cluster_coordinator import ClusterCoordinator


def create_app(config: ClusterConfig | None = None) -> Flask:
    """Create and configure the Flask application."""
    if config is None:
        try:
            config = load_config()
        except FileNotFoundError:
            config = load_config_from_env()

    app = Flask(__name__)
    coordinator = ClusterCoordinator(config)

    app.config["coordinator"] = coordinator
    app.config["cluster_config"] = config

    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    @app.route("/health")
    def health():
        metrics = coordinator.get_cluster_metrics()
        return jsonify({
            "status": "healthy",
            "cluster_name": config.name,
            "node_count": metrics["node_count"],
            "total_logs": metrics["total_logs"],
        })

    @app.route("/api/logs", methods=["POST"])
    def store_logs():
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"error": "No JSON body provided"}), 400

        entries = data if isinstance(data, list) else [data]

        for entry in entries:
            if "source" not in entry:
                return jsonify({"error": "Each entry must have a 'source' field"}), 400
            if "timestamp" not in entry:
                entry["timestamp"] = datetime.now(timezone.utc).isoformat()
            if "level" not in entry:
                entry["level"] = "info"

        results = coordinator.store_logs(entries)
        return jsonify({
            "stored": len(results),
            "details": [
                {"node_id": r["node_id"], "log_key": r["log_key"]}
                for r in results
            ],
        }), 201

    @app.route("/api/stats")
    def stats():
        return jsonify(coordinator.get_cluster_metrics())

    @app.route("/api/nodes", methods=["POST"])
    def add_node():
        data = request.get_json()
        if not data or "node_id" not in data:
            return jsonify({"error": "Must provide node_id"}), 400

        node_id = data["node_id"]
        if node_id in coordinator.get_node_ids():
            return jsonify({"error": f"Node {node_id} already exists"}), 409

        result = coordinator.add_node(node_id)
        return jsonify(result), 201

    @app.route("/api/nodes/<node_id>", methods=["DELETE"])
    def remove_node(node_id):
        if node_id not in coordinator.get_node_ids():
            return jsonify({"error": f"Node {node_id} not found"}), 404

        result = coordinator.remove_node(node_id)
        return jsonify(result)

    @app.route("/api/ring")
    def ring_info():
        """Ring visualization data: vnode positions as percentage of ring."""
        metrics = coordinator.get_cluster_metrics()
        ring_metrics = metrics["ring_metrics"]

        # Get the hash ring for position data
        ring = coordinator._ring

        # Assign colors to nodes
        node_colors = {}
        colors = [
            "#e94560", "#0f3460", "#2ecc71", "#f1c40f",
            "#9b59b6", "#e67e22", "#1abc9c", "#3498db",
        ]
        for i, node_id in enumerate(sorted(ring.nodes)):
            node_colors[node_id] = colors[i % len(colors)]

        # Get vnode positions as percentages of ring
        vnodes = []
        with ring._lock:
            max_hash = 2 ** 160
            for pos in ring.sorted_keys:
                node_id = ring.ring[pos]
                pct = (pos / max_hash) * 100
                vnodes.append({
                    "position_pct": round(pct, 4),
                    "node_id": node_id,
                    "color": node_colors.get(node_id, "#888"),
                })

        return jsonify({
            "vnodes": vnodes,
            "node_colors": node_colors,
            "ring_metrics": ring_metrics,
        })

    @app.route("/api/simulate", methods=["POST"])
    def simulate():
        """Generate N random logs."""
        data = request.get_json() or {}
        count = data.get("count", 100)
        count = min(count, 100000)  # cap at 100K

        sources = [
            "web-server", "api-gateway", "auth-service",
            "database", "cache", "worker",
        ]
        levels = ["debug", "info", "warning", "error", "critical"]

        entries = []
        for _ in range(count):
            entries.append({
                "source": random.choice(sources),
                "level": random.choice(levels),
                "message": "".join(
                    random.choices(
                        string.ascii_lowercase + " ",
                        k=random.randint(20, 80),
                    )
                ),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        results = coordinator.store_logs(entries)
        metrics = coordinator.get_cluster_metrics()

        return jsonify({
            "generated": len(results),
            "total_logs": metrics["total_logs"],
            "distribution": {
                node_id: node_data["log_count"]
                for node_id, node_data in metrics["nodes"].items()
            },
        }), 201

    return app
