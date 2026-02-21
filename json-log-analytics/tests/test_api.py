import pytest
import json


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "healthy"
        assert "total_logs" in data
        assert "current_stored" in data


class TestLogIngestion:
    def test_valid_log_accepted(self, client, sample_valid_log):
        resp = client.post("/api/logs", json=sample_valid_log)
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["status"] == "accepted"

    def test_invalid_log_rejected(self, client, sample_invalid_log):
        resp = client.post("/api/logs", json=sample_invalid_log)
        assert resp.status_code == 400
        data = resp.get_json()
        assert data["status"] == "invalid"
        assert len(data["errors"]) > 0

    def test_log_with_metadata_accepted(self, client, sample_log_with_metadata):
        resp = client.post("/api/logs", json=sample_log_with_metadata)
        assert resp.status_code == 201

    def test_log_with_extra_fields_rejected(self, client, sample_valid_log):
        sample_valid_log["extra_field"] = "not allowed"
        resp = client.post("/api/logs", json=sample_valid_log)
        assert resp.status_code == 400

    def test_multiple_logs_increment_count(self, client, sample_valid_log):
        for _ in range(5):
            client.post("/api/logs", json=sample_valid_log)
        resp = client.get("/health")
        data = resp.get_json()
        assert data["total_logs"] >= 5


class TestSimulation:
    def test_simulate_logs(self, client):
        resp = client.post("/api/simulate-logs", json={"count": 20})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "simulated"
        assert data["requested"] == 20
        assert data["accepted"] > 0

    def test_simulate_logs_capped_at_1000(self, client):
        resp = client.post("/api/simulate-logs", json={"count": 2000})
        data = resp.get_json()
        assert data["requested"] == 1000

    def test_simulate_errors(self, client):
        resp = client.post("/api/simulate-errors", json={"count": 30, "error_rate": 0.9, "service": "test-svc"})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["accepted"] > 0


class TestDashboardData:
    def test_dashboard_data_structure(self, client):
        # Ingest some data first
        client.post("/api/simulate-logs", json={"count": 50})
        resp = client.get("/api/advanced-dashboard-data")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "summary" in data
        assert "time_series" in data
        assert "error_trends" in data
        assert "service_health" in data
        assert "most_active_services" in data
        assert "user_activity" in data
        assert "recent_logs" in data
        assert "active_alerts" in data
        assert "validation_stats" in data

    def test_summary_fields(self, client):
        client.post("/api/simulate-logs", json={"count": 10})
        resp = client.get("/api/advanced-dashboard-data")
        summary = resp.get_json()["summary"]
        assert "total_logs" in summary
        assert "total_errors" in summary
        assert "error_rate" in summary
        assert "active_services" in summary

    def test_time_series_endpoint(self, client):
        client.post("/api/simulate-logs", json={"count": 10})
        resp = client.get("/api/time-series?minutes=5")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)

    def test_validation_stats_endpoint(self, client):
        resp = client.get("/api/validation-stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "total" in data
        assert "valid" in data
        assert "invalid" in data


class TestAlertTriggering:
    def test_error_rate_triggers_alert(self, client):
        # Inject high error rate
        client.post("/api/simulate-errors", json={"count": 50, "error_rate": 0.9})
        resp = client.get("/api/advanced-dashboard-data")
        data = resp.get_json()
        # Should have at least one alert
        alerts = data["active_alerts"]
        assert len(alerts) >= 1
        assert any(a["rule"] == "error_rate" for a in alerts)

    def test_alert_has_required_fields(self, client):
        client.post("/api/simulate-errors", json={"count": 50, "error_rate": 0.9})
        resp = client.get("/api/advanced-dashboard-data")
        alerts = resp.get_json()["active_alerts"]
        if alerts:
            alert = alerts[0]
            assert "rule" in alert
            assert "level" in alert
            assert "message" in alert
            assert "timestamp" in alert


class TestDashboardPage:
    def test_dashboard_renders(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert b"JSON Log Analytics" in resp.data


class TestFullFlow:
    def test_ingest_to_dashboard_flow(self, client):
        """Test the full flow: ingest logs -> check analytics -> verify dashboard data."""
        # 1. Health check
        resp = client.get("/health")
        assert resp.get_json()["status"] == "healthy"

        # 2. Ingest valid logs
        valid_log = {
            "timestamp": "2024-01-15T10:30:00Z",
            "level": "INFO",
            "service": "api-gateway",
            "message": "Request processed",
        }
        for _ in range(10):
            resp = client.post("/api/logs", json=valid_log)
            assert resp.status_code == 201

        # 3. Ingest error logs
        error_log = {
            "timestamp": "2024-01-15T10:30:00Z",
            "level": "ERROR",
            "service": "payment-service",
            "message": "Payment failed",
        }
        for _ in range(5):
            resp = client.post("/api/logs", json=error_log)
            assert resp.status_code == 201

        # 4. Check dashboard data reflects all logs
        resp = client.get("/api/advanced-dashboard-data")
        data = resp.get_json()
        assert data["summary"]["total_logs"] >= 15
        assert data["summary"]["total_errors"] >= 5
        assert data["summary"]["active_services"] >= 2

        # 5. Verify recent logs are returned
        assert len(data["recent_logs"]) > 0

        # 6. Verify validation stats
        assert data["validation_stats"]["total"] >= 15
        assert data["validation_stats"]["valid"] >= 15
