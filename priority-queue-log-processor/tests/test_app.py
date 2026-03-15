"""Tests for Flask API endpoints."""

import pytest

from src.aging import PriorityAgingMonitor
from src.app import create_app
from src.classifier import MessageClassifier
from src.config import Settings
from src.generator import SyntheticLogGenerator
from src.metrics import MetricsTracker
from src.priority_queue import ThreadSafePriorityQueue
from src.worker_pool import WorkerPool


@pytest.fixture
def app_client():
    """Create a Flask test client with background services disabled."""
    settings = Settings(max_queue_size=100, num_workers=1, generator_rate=0)
    queue = ThreadSafePriorityQueue(max_size=settings.max_queue_size, settings=settings)
    metrics = MetricsTracker()
    classifier = MessageClassifier()
    worker_pool = WorkerPool(queue, metrics, settings)
    aging_monitor = PriorityAgingMonitor(queue, settings)
    generator = SyntheticLogGenerator()

    app = create_app(
        settings=settings,
        queue=queue,
        metrics=metrics,
        classifier=classifier,
        worker_pool=worker_pool,
        aging_monitor=aging_monitor,
        generator=generator,
        start_background=False,
    )
    app.config["TESTING"] = True
    return app.test_client()


class TestHealthEndpoint:
    def test_health_endpoint(self, app_client):
        resp = app_client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "timestamp" in data
        assert "queue_size" in data
        assert "workers" in data


class TestStatusEndpoint:
    def test_status_endpoint(self, app_client):
        resp = app_client.get("/api/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "queue" in data
        assert "metrics" in data
        assert "workers" in data
        assert "recent_messages" in data
        assert "aging" in data


class TestInjectEndpoint:
    def test_inject_critical(self, app_client):
        resp = app_client.post("/api/inject/critical")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["injected"] is True
        assert data["priority"] == "CRITICAL"

    def test_inject_high(self, app_client):
        resp = app_client.post("/api/inject/high")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["injected"] is True
        assert data["priority"] == "HIGH"

    def test_inject_medium(self, app_client):
        resp = app_client.post("/api/inject/medium")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["injected"] is True
        assert data["priority"] == "MEDIUM"

    def test_inject_low(self, app_client):
        resp = app_client.post("/api/inject/low")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["injected"] is True
        assert data["priority"] == "LOW"

    def test_inject_invalid_priority(self, app_client):
        resp = app_client.post("/api/inject/invalid")
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data

    def test_inject_increases_queue(self, app_client):
        app_client.post("/api/inject/low")
        resp = app_client.get("/api/status")
        data = resp.get_json()
        assert data["queue"]["size"] > 0


class TestDashboard:
    def test_dashboard_loads(self, app_client):
        resp = app_client.get("/")
        assert resp.status_code == 200
        assert b"Priority Queue" in resp.data

    def test_dashboard_has_chart_canvas(self, app_client):
        resp = app_client.get("/")
        assert resp.status_code == 200
        assert b'id="processing-chart"' in resp.data or b"<canvas" in resp.data

    def test_dashboard_has_injection_buttons(self, app_client):
        resp = app_client.get("/")
        assert resp.status_code == 200
        assert b"Inject CRITICAL" in resp.data
        assert b"Inject LOW" in resp.data

    def test_status_has_recent_messages_key(self, app_client):
        app_client.post("/api/inject/critical")
        resp = app_client.get("/api/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "recent_messages" in data


class TestAlerts:
    def test_alerts_in_status(self):
        """Verify alerts key appears in /api/status metrics response."""
        settings = Settings(
            max_queue_size=10,
            num_workers=1,
            generator_rate=0,
            alert_queue_depth_threshold=5,
        )
        queue = ThreadSafePriorityQueue(max_size=settings.max_queue_size, settings=settings)
        metrics = MetricsTracker()
        classifier = MessageClassifier()
        worker_pool = WorkerPool(queue, metrics, settings)
        aging_monitor = PriorityAgingMonitor(queue, settings)
        generator = SyntheticLogGenerator()

        app = create_app(
            settings=settings,
            queue=queue,
            metrics=metrics,
            classifier=classifier,
            worker_pool=worker_pool,
            aging_monitor=aging_monitor,
            generator=generator,
            start_background=False,
        )
        app.config["TESTING"] = True
        client = app.test_client()

        # Fill queue past alert threshold
        from src.models import LogMessage, Priority

        for _ in range(7):
            msg = LogMessage(source="test", message="test alert", priority=Priority.LOW)
            queue.push(msg)

        # Trigger alert check
        metrics.check_alert(queue.size, settings.alert_queue_depth_threshold)

        resp = client.get("/api/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "alerts" in data["metrics"]
        assert "firing" in data["metrics"]["alerts"]
        assert "recent" in data["metrics"]["alerts"]
        assert data["metrics"]["alerts"]["firing"] is True
        assert len(data["metrics"]["alerts"]["recent"]) > 0


class TestMetricsEndpoint:
    def test_metrics_endpoint(self, app_client):
        resp = app_client.get("/metrics")
        assert resp.status_code == 200
        assert b"logs_processed_total" in resp.data
