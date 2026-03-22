"""Tests for src.alerts module."""

import pytest

from src.alerts import AlertManager
from src.config import Settings


@pytest.fixture
def alert_config():
    return Settings(
        alert_error_rate_warning=3.0,
        alert_error_rate_critical=5.0,
        alert_response_time_warning=1000.0,
        alert_response_time_critical=2000.0,
        alert_cooldown_seconds=60.0,
    )


@pytest.fixture
def alert_manager(alert_config):
    return AlertManager(alert_config)


def test_no_alerts_when_healthy(alert_manager):
    metrics = {"error_rate": 1.0, "p95_response_time": 200}
    alerts = alert_manager.evaluate(metrics)
    assert alerts == []
    assert alert_manager.get_active_alerts() == []


def test_warning_alert_on_error_rate(alert_manager):
    metrics = {"error_rate": 4.0, "p95_response_time": 200}
    alerts = alert_manager.evaluate(metrics)
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "warning"
    assert alerts[0]["type"] == "error_rate"


def test_critical_alert_on_error_rate(alert_manager):
    metrics = {"error_rate": 6.0, "p95_response_time": 200}
    alerts = alert_manager.evaluate(metrics)
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "critical"


def test_warning_alert_on_response_time(alert_manager):
    metrics = {"error_rate": 1.0, "p95_response_time": 1500}
    alerts = alert_manager.evaluate(metrics)
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "warning"
    assert alerts[0]["type"] == "response_time"


def test_critical_alert_on_response_time(alert_manager):
    metrics = {"error_rate": 1.0, "p95_response_time": 2500}
    alerts = alert_manager.evaluate(metrics)
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "critical"


def test_multiple_alerts(alert_manager):
    metrics = {"error_rate": 6.0, "p95_response_time": 2500}
    alerts = alert_manager.evaluate(metrics)
    assert len(alerts) == 2


def test_cooldown_prevents_duplicate(alert_manager):
    metrics = {"error_rate": 6.0, "p95_response_time": 200}
    alerts1 = alert_manager.evaluate(metrics)
    assert len(alerts1) == 1
    alerts2 = alert_manager.evaluate(metrics)
    assert len(alerts2) == 0  # Cooldown active


def test_cooldown_expires(alert_manager):
    alert_manager._cooldown_seconds = 0  # Disable cooldown for test
    metrics = {"error_rate": 6.0, "p95_response_time": 200}
    alerts1 = alert_manager.evaluate(metrics)
    assert len(alerts1) == 1
    alerts2 = alert_manager.evaluate(metrics)
    assert len(alerts2) == 1  # Cooldown expired


def test_alert_clears_when_healthy(alert_manager):
    metrics_bad = {"error_rate": 6.0, "p95_response_time": 200}
    alert_manager.evaluate(metrics_bad)
    assert len(alert_manager.get_active_alerts()) == 1

    metrics_good = {"error_rate": 1.0, "p95_response_time": 200}
    alert_manager.evaluate(metrics_good)
    assert len(alert_manager.get_active_alerts()) == 0


def test_alert_history(alert_manager):
    alert_manager._cooldown_seconds = 0
    for i in range(5):
        alert_manager.evaluate({"error_rate": 6.0, "p95_response_time": 200})
    history = alert_manager.get_alert_history()
    assert len(history) == 5


def test_alert_history_bounded(alert_manager):
    alert_manager._cooldown_seconds = 0
    for i in range(60):
        alert_manager.evaluate({"error_rate": 6.0, "p95_response_time": 200})
    history = alert_manager.get_alert_history()
    assert len(history) == 50  # maxlen=50


def test_alert_structure(alert_manager):
    metrics = {"error_rate": 6.0, "p95_response_time": 200}
    alerts = alert_manager.evaluate(metrics)
    alert = alerts[0]
    assert "type" in alert
    assert "severity" in alert
    assert "message" in alert
    assert "action_required" in alert
    assert "timestamp" in alert
    assert "value" in alert
    assert "threshold" in alert
