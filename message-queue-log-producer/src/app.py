"""Flask HTTP API for accepting log entries with full pipeline wiring."""

import atexit
import time
import queue
import logging

from flask import Flask, request, jsonify
from src.config import Config
from src.metrics import MetricsCollector
from src.circuit_breaker import CircuitBreaker
from src.fallback_storage import FallbackStorage
from src.batch_manager import BatchManager
from src.publisher import PublisherThread
from src.connection import RabbitMQConnection
from src.setup import setup_topology

logger = logging.getLogger(__name__)
REQUIRED_FIELDS = {"level", "message", "source"}


def create_app(config=None, publisher=None, metrics=None, circuit_breaker=None,
               batch_manager=None, fallback=None):
    """Flask application factory.

    If components are not provided, they are created from config.
    Passing components explicitly allows testing with mocks.
    """
    if config is None:
        config = Config()

    app = Flask(__name__)
    app.config["APP_CONFIG"] = config

    # Create or use provided components
    if metrics is None:
        metrics = MetricsCollector()
    app.config["METRICS"] = metrics

    if circuit_breaker is None:
        cb_cfg = config.circuit_breaker
        circuit_breaker = CircuitBreaker(
            failure_threshold=cb_cfg["failure_threshold"],
            recovery_timeout=cb_cfg["recovery_timeout"],
        )
    app.config["CIRCUIT_BREAKER"] = circuit_breaker

    if fallback is None:
        fallback = FallbackStorage()
    app.config["FALLBACK"] = fallback

    # Internal queue for batch -> publisher communication
    internal_queue = queue.Queue(maxsize=config.queue_maxsize)
    app.config["INTERNAL_QUEUE"] = internal_queue

    if publisher is None:
        publisher = PublisherThread(
            config=config,
            internal_queue=internal_queue,
            circuit_breaker=circuit_breaker,
            fallback=fallback,
            metrics=metrics,
        )
    app.config["PUBLISHER"] = publisher

    def on_flush(batch):
        """Callback from BatchManager — put batch on internal queue."""
        try:
            internal_queue.put(batch, timeout=5)
        except queue.Full:
            logger.error("Internal queue full, writing to fallback")
            fallback.write(batch)
            metrics.record_fallback_write(len(batch))

    if batch_manager is None:
        batch_cfg = config.batch
        batch_manager = BatchManager(
            max_size=batch_cfg["max_size"],
            flush_interval_s=batch_cfg["flush_interval"],
            on_flush=on_flush,
        )
    app.config["BATCH_MANAGER"] = batch_manager

    # Try to set up topology on startup (non-fatal if RabbitMQ is down)
    try:
        conn = RabbitMQConnection(config)
        conn.connect()
        setup_topology(conn.get_channel(), config)
        conn.close()
        logger.info("Topology setup complete")
    except Exception:
        logger.warning("Could not setup topology on startup (RabbitMQ may be down)")

    @app.route("/logs", methods=["POST"])
    def ingest_logs():
        """Accept one or more log entries via JSON body."""
        data = request.get_json(silent=True)
        if data is None:
            return jsonify({"error": "Invalid JSON"}), 400

        if isinstance(data, dict):
            entries = [data]
        elif isinstance(data, list):
            entries = data
        else:
            return jsonify({"error": "Expected JSON object or array"}), 400

        for entry in entries:
            if not isinstance(entry, dict):
                return jsonify({"error": "Each entry must be a JSON object"}), 400
            missing = REQUIRED_FIELDS - entry.keys()
            if missing:
                return jsonify({"error": f"Missing fields: {sorted(missing)}"}), 400

        # Enrich and add to batch manager
        for entry in entries:
            entry["timestamp"] = time.time()
            entry["_received_at"] = time.monotonic()
            batch_manager.add(entry)

        metrics.record_received(len(entries))
        return jsonify({"accepted": len(entries)}), 202

    @app.route("/health", methods=["GET"])
    def health():
        """Health check endpoint with real status."""
        cb_state = circuit_breaker.state.value
        healthy = cb_state != "open"
        return jsonify({
            "healthy": healthy,
            "status": "ok" if healthy else "degraded",
            "throughput": metrics.get_throughput(),
            "latency_p95": metrics.get_latency_p95(),
            "circuit_breaker": cb_state,
        }), 200

    @app.route("/metrics", methods=["GET"])
    def metrics_endpoint():
        """Full metrics snapshot."""
        snapshot = metrics.snapshot()
        snapshot["circuit_breaker_state"] = circuit_breaker.state.value
        snapshot["buffer_size"] = batch_manager.buffer_size
        snapshot["queue_depth"] = internal_queue.qsize()
        return jsonify(snapshot), 200

    def shutdown():
        """Graceful shutdown."""
        logger.info("Shutting down...")
        batch_manager.stop()
        publisher.stop()

    atexit.register(shutdown)

    return app


def run_app(config=None):
    """Create and run the Flask app."""
    logging.basicConfig(level=logging.INFO)
    if config is None:
        config = Config()
    app = create_app(config)
    app.run(host="0.0.0.0", port=config.http_port)


if __name__ == "__main__":
    run_app()
