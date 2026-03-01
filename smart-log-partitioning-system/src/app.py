"""Flask application factory for the Smart Log Partitioning System."""

from datetime import datetime

from flask import Flask, jsonify, render_template, request

from src.config import PartitionConfig, load_config
from src.manager import PartitionManager
from src.optimizer import QueryOptimizer
from src.router import PartitionRouter


def create_app(config: PartitionConfig | None = None) -> Flask:
    """Create and configure the Flask application."""
    if config is None:
        config = load_config()

    app = Flask(__name__)

    # Initialize components
    router = PartitionRouter(config)
    manager = PartitionManager(config)
    optimizer = QueryOptimizer(router, manager)

    # Load existing data from disk
    manager.load_from_disk()

    # Store references for access in routes
    app.config["partition_config"] = config
    app.config["router"] = router
    app.config["manager"] = manager
    app.config["optimizer"] = optimizer

    @app.route("/health")
    def health():
        return jsonify(
            {"status": "healthy", "strategy": config.strategy, "num_nodes": config.num_nodes}
        )

    @app.route("/api/ingest", methods=["POST"])
    def ingest():
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON body provided"}), 400

        # Support both single entry and batch
        entries = data if isinstance(data, list) else [data]
        results = []

        for entry in entries:
            if "source" not in entry:
                return jsonify({"error": "Each entry must have a 'source' field"}), 400

            # Add timestamp if not present
            if "timestamp" not in entry:
                entry["timestamp"] = datetime.utcnow().isoformat()

            # Add level default
            if "level" not in entry:
                entry["level"] = "info"

            partition_id = router.route(entry)
            manager.store(partition_id, entry)
            results.append({"partition_id": partition_id, "source": entry["source"]})

        return jsonify(
            {
                "ingested": len(results),
                "details": results,
            }
        ), 201

    @app.route("/api/query")
    def query():
        # Build query from query params
        q = {}
        if request.args.get("source"):
            q["source"] = request.args["source"]
        if request.args.get("level"):
            q["level"] = request.args["level"]
        if request.args.get("start") and request.args.get("end"):
            q["time_range"] = {"start": request.args["start"], "end": request.args["end"]}

        # Optimize query
        optimization = optimizer.optimize(q)

        # Build filters for manager.query
        filters = {}
        if "source" in q:
            filters["source"] = q["source"]
        if "level" in q:
            filters["level"] = q["level"]
        if "time_range" in q:
            filters["time_range"] = q["time_range"]

        # Execute query
        results = manager.query(optimization["partition_ids"], filters)

        return jsonify(
            {
                "results": results,
                "count": len(results),
                "optimization": optimization,
            }
        )

    @app.route("/api/stats")
    def stats():
        partition_stats = manager.get_stats()
        efficiency = optimizer.get_efficiency_metrics()
        return jsonify(
            {
                "strategy": config.strategy,
                "num_nodes": config.num_nodes,
                "partitions": partition_stats,
                "query_efficiency": efficiency,
            }
        )

    @app.route("/")
    def dashboard():
        return render_template("dashboard.html")

    return app
