import threading
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable


@runtime_checkable
class AlertHandler(Protocol):
    def handle(self, alert: dict) -> None: ...


class ConsoleAlertHandler:
    def handle(self, alert: dict) -> None:
        level = alert.get("level", "WARNING")
        rule = alert.get("rule", "unknown")
        message = alert.get("message", "")
        print(f"[ALERT:{level}] [{rule}] {message}")


class AlertManager:
    def __init__(self, analytics_engine, config=None):
        self._engine = analytics_engine
        alerting_config = (config or {}).get("alerting", {})
        self._error_rate_threshold = alerting_config.get("error_rate_threshold", 0.10)
        self._high_volume_threshold = alerting_config.get("high_volume_threshold", 100)
        self._service_down_minutes = alerting_config.get("service_down_minutes", 2)
        self._cooldown_seconds = alerting_config.get("cooldown_seconds", 300)
        self._handlers = []
        self._active_alerts = []
        self._cooldowns = {}
        self._lock = threading.Lock()

    def add_handler(self, handler):
        self._handlers.append(handler)

    def _is_cooled_down(self, rule, service=None):
        key = (rule, service)
        last_fired = self._cooldowns.get(key)
        if last_fired is None:
            return True
        elapsed = (datetime.now(timezone.utc) - last_fired).total_seconds()
        return elapsed >= self._cooldown_seconds

    def _fire_alert(self, rule, level, message, service=None, details=None):
        if not self._is_cooled_down(rule, service):
            return
        alert = {
            "rule": rule,
            "level": level,
            "message": message,
            "service": service,
            "details": details or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            self._active_alerts.append(alert)
            self._cooldowns[(rule, service)] = datetime.now(timezone.utc)
        for handler in self._handlers:
            handler.handle(alert)

    def check_error_rate(self):
        error_rate = self._engine.get_error_rate()
        if error_rate > self._error_rate_threshold:
            self._fire_alert(
                rule="error_rate",
                level="CRITICAL",
                message=f"Error rate {error_rate:.1%} exceeds threshold {self._error_rate_threshold:.1%}",
                details={"error_rate": error_rate, "threshold": self._error_rate_threshold},
            )

    def check_high_volume(self):
        active_services = self._engine.get_most_active_services()
        for entry in active_services:
            service = entry["service"]
            count = entry["count"]
            if count > self._high_volume_threshold:
                self._fire_alert(
                    rule="high_volume",
                    level="WARNING",
                    message=f"Service '{service}' has high volume: {count} logs/min",
                    service=service,
                    details={"service": service, "count": count},
                )

    def check_service_down(self):
        health = self._engine.get_service_health()
        for service, info in health.items():
            if info["status"] == "down":
                self._fire_alert(
                    rule="service_down",
                    level="CRITICAL",
                    message=f"Service '{service}' appears down (last seen: {info['last_seen']})",
                    service=service,
                    details={"service": service, "last_seen": info["last_seen"]},
                )

    def check_all(self):
        with self._lock:
            before_count = len(self._active_alerts)
        self.check_error_rate()
        self.check_high_volume()
        self.check_service_down()
        with self._lock:
            new_alerts = self._active_alerts[before_count:]
            return list(new_alerts)

    def get_active_alerts(self):
        with self._lock:
            return list(self._active_alerts)

    def clear_alerts(self):
        with self._lock:
            self._active_alerts.clear()
