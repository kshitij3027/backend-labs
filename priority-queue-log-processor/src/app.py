"""Flask application factory with API endpoints and dashboard."""

import atexit
import os
import threading
import time

from flask import Flask, Response, jsonify, request

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.aging import PriorityAgingMonitor
from src.classifier import MessageClassifier
from src.config import Settings, load_config
from src.generator import SyntheticLogGenerator
from src.metrics import MetricsTracker
from src.models import LogMessage, Priority
from src.priority_queue import ThreadSafePriorityQueue
from src.worker_pool import WorkerPool

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")


def create_app(
    settings=None,
    queue=None,
    metrics=None,
    classifier=None,
    worker_pool=None,
    aging_monitor=None,
    generator=None,
    start_background=True,
):
    """Create and configure the Flask application.

    When *start_background* is True the worker pool, aging monitor, and log
    generator are started in daemon threads.  Pass False for unit testing.
    """
    if settings is None:
        settings = load_config()

    if queue is None:
        queue = ThreadSafePriorityQueue(
            max_size=settings.max_queue_size, settings=settings
        )
    if metrics is None:
        metrics = MetricsTracker()
    if classifier is None:
        classifier = MessageClassifier()
    if worker_pool is None:
        worker_pool = WorkerPool(queue, metrics, settings)
    if aging_monitor is None:
        aging_monitor = PriorityAgingMonitor(queue, settings)
    if generator is None:
        generator = SyntheticLogGenerator()

    app = Flask(__name__)

    # Store components for access in route handlers
    app.config["settings"] = settings
    app.config["queue"] = queue
    app.config["metrics"] = metrics
    app.config["classifier"] = classifier
    app.config["worker_pool"] = worker_pool
    app.config["aging_monitor"] = aging_monitor
    app.config["generator"] = generator

    stop_event = threading.Event()
    app.config["stop_event"] = stop_event

    # ------------------------------------------------------------------
    # Background services
    # ------------------------------------------------------------------
    if start_background:
        worker_pool.start()
        aging_monitor.start()
        generator.start(queue, classifier, metrics, settings, stop_event)

        def _shutdown():
            stop_event.set()
            worker_pool.stop()
            aging_monitor.stop()

        atexit.register(_shutdown)

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.route("/")
    def index():
        index_path = os.path.join(TEMPLATE_DIR, "index.html")
        with open(index_path, "r") as f:
            content = f.read()
        return Response(content, content_type="text/html")

    @app.route("/health")
    def health():
        return jsonify(
            {
                "status": "ok",
                "timestamp": time.time(),
                "queue_size": queue.size,
                "workers": worker_pool.worker_count,
            }
        )

    @app.route("/api/status")
    def status():
        return jsonify(
            {
                "queue": queue.get_stats(),
                "metrics": metrics.get_stats(),
                "workers": {
                    "count": worker_pool.worker_count,
                    "is_running": worker_pool.is_running,
                },
                "recent_messages": metrics.get_recent_messages(),
                "aging": {"is_running": aging_monitor.is_running},
            }
        )

    @app.route("/api/inject/<priority_name>", methods=["POST"])
    def inject(priority_name):
        name_upper = priority_name.upper()
        try:
            priority = Priority[name_upper]
        except KeyError:
            return (
                jsonify({"error": f"Invalid priority: {priority_name}"}),
                400,
            )

        msg = LogMessage(
            source="inject",
            message=f"Injected {priority_name} test message",
            priority=priority,
        )

        pushed = queue.push(msg)
        if not pushed:
            return (
                jsonify({"error": "Queue full", "queue_size": queue.size}),
                503,
            )

        metrics.record_enqueued(priority)
        return jsonify(
            {
                "injected": True,
                "priority": name_upper,
                "queue_size": queue.size,
            }
        )

    @app.route("/metrics")
    def prometheus_metrics():
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    return app
