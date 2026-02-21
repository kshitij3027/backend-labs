import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock
from analytics import AnalyticsEngine
from alerting import AlertManager, ConsoleAlertHandler, AlertHandler


def make_log(service="auth-service", level="INFO", minutes_ago=0):
    ts = datetime.now(timezone.utc)
    if minutes_ago:
        ts = ts - timedelta(minutes=minutes_ago)
    return {
        "timestamp": ts.isoformat(),
        "level": level,
        "service": service,
        "message": "test",
    }


class TestAlertManager:
    def test_error_rate_alert(self):
        engine = AnalyticsEngine()
        manager = AlertManager(engine, config={"alerting": {"error_rate_threshold": 0.1, "cooldown_seconds": 0}})
        # 5 errors, 5 info = 50% error rate
        for _ in range(5):
            engine.record(make_log(level="ERROR"))
        for _ in range(5):
            engine.record(make_log(level="INFO"))
        manager.check_error_rate()
        alerts = manager.get_active_alerts()
        assert len(alerts) == 1
        assert alerts[0]["rule"] == "error_rate"
        assert alerts[0]["level"] == "CRITICAL"

    def test_high_volume_alert(self):
        engine = AnalyticsEngine()
        manager = AlertManager(engine, config={"alerting": {"high_volume_threshold": 10, "cooldown_seconds": 0}})
        for _ in range(15):
            engine.record(make_log(service="busy-svc"))
        manager.check_high_volume()
        alerts = manager.get_active_alerts()
        assert any(a["rule"] == "high_volume" for a in alerts)

    def test_service_down_alert(self):
        engine = AnalyticsEngine()
        manager = AlertManager(engine, config={"alerting": {"service_down_minutes": 2, "cooldown_seconds": 0}})
        # Record a log 5 minutes ago
        engine.record(make_log(service="dead-svc", minutes_ago=5))
        manager.check_service_down()
        alerts = manager.get_active_alerts()
        assert any(a["rule"] == "service_down" for a in alerts)

    def test_cooldown_prevents_duplicate_alerts(self):
        engine = AnalyticsEngine()
        manager = AlertManager(engine, config={"alerting": {"error_rate_threshold": 0.1, "cooldown_seconds": 300}})
        for _ in range(10):
            engine.record(make_log(level="ERROR"))
        manager.check_error_rate()
        initial_count = len(manager.get_active_alerts())
        manager.check_error_rate()  # Should be cooled down
        assert len(manager.get_active_alerts()) == initial_count

    def test_handler_dispatch(self):
        engine = AnalyticsEngine()
        mock_handler = MagicMock()
        manager = AlertManager(engine, config={"alerting": {"error_rate_threshold": 0.1, "cooldown_seconds": 0}})
        manager.add_handler(mock_handler)
        for _ in range(10):
            engine.record(make_log(level="ERROR"))
        manager.check_error_rate()
        mock_handler.handle.assert_called_once()

    def test_check_all(self):
        engine = AnalyticsEngine()
        manager = AlertManager(engine, config={"alerting": {"error_rate_threshold": 0.1, "cooldown_seconds": 0}})
        for _ in range(10):
            engine.record(make_log(level="ERROR"))
        new_alerts = manager.check_all()
        assert len(new_alerts) >= 1

    def test_clear_alerts(self):
        engine = AnalyticsEngine()
        manager = AlertManager(engine, config={"alerting": {"error_rate_threshold": 0.1, "cooldown_seconds": 0}})
        for _ in range(10):
            engine.record(make_log(level="ERROR"))
        manager.check_error_rate()
        assert len(manager.get_active_alerts()) > 0
        manager.clear_alerts()
        assert len(manager.get_active_alerts()) == 0

    def test_console_handler(self, capsys):
        handler = ConsoleAlertHandler()
        handler.handle({"level": "CRITICAL", "rule": "error_rate", "message": "High error rate"})
        captured = capsys.readouterr()
        assert "ALERT:CRITICAL" in captured.out
        assert "error_rate" in captured.out

    def test_alert_handler_protocol(self):
        assert isinstance(ConsoleAlertHandler(), AlertHandler)
