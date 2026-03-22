"""Threshold-based alert system with severity levels and cooldowns."""

import logging
import time
from collections import deque

logger = logging.getLogger(__name__)


class AlertManager:
    def __init__(self, config):
        self._config = config
        self._thresholds = {
            "error_rate_warning": config.alert_error_rate_warning,
            "error_rate_critical": config.alert_error_rate_critical,
            "response_time_warning": config.alert_response_time_warning,
            "response_time_critical": config.alert_response_time_critical,
        }
        self._cooldowns = {}  # {alert_key: last_fired_timestamp}
        self._cooldown_seconds = config.alert_cooldown_seconds
        self._active_alerts = {}  # {alert_key: alert_dict}
        self._history = deque(maxlen=50)
        logger.info("AlertManager initialized with thresholds: %s", self._thresholds)

    def evaluate(self, metrics):
        """Evaluate metrics against thresholds. Returns list of NEW alerts fired."""
        now = time.time()
        new_alerts = []

        error_rate = metrics.get("error_rate", 0)
        p95_rt = metrics.get("p95_response_time", 0)

        # Error rate checks
        if error_rate >= self._thresholds["error_rate_critical"]:
            alert = self._maybe_fire(
                "error_rate_critical",
                "critical",
                f"Error rate {error_rate}% exceeds critical threshold "
                f"({self._thresholds['error_rate_critical']}%)",
                "Investigate error patterns immediately",
                error_rate,
                self._thresholds["error_rate_critical"],
                now,
            )
            if alert:
                new_alerts.append(alert)
        elif error_rate >= self._thresholds["error_rate_warning"]:
            alert = self._maybe_fire(
                "error_rate_warning",
                "warning",
                f"Error rate {error_rate}% exceeds warning threshold "
                f"({self._thresholds['error_rate_warning']}%)",
                "Monitor error patterns",
                error_rate,
                self._thresholds["error_rate_warning"],
                now,
            )
            if alert:
                new_alerts.append(alert)
        else:
            # Clear error rate alerts if below thresholds
            self._active_alerts.pop("error_rate_critical", None)
            self._active_alerts.pop("error_rate_warning", None)

        # Response time checks
        if p95_rt >= self._thresholds["response_time_critical"]:
            alert = self._maybe_fire(
                "response_time_critical",
                "critical",
                f"P95 response time {p95_rt}ms exceeds critical threshold "
                f"({self._thresholds['response_time_critical']}ms)",
                "Investigate slow endpoints immediately",
                p95_rt,
                self._thresholds["response_time_critical"],
                now,
            )
            if alert:
                new_alerts.append(alert)
        elif p95_rt >= self._thresholds["response_time_warning"]:
            alert = self._maybe_fire(
                "response_time_warning",
                "warning",
                f"P95 response time {p95_rt}ms exceeds warning threshold "
                f"({self._thresholds['response_time_warning']}ms)",
                "Monitor response time trends",
                p95_rt,
                self._thresholds["response_time_warning"],
                now,
            )
            if alert:
                new_alerts.append(alert)
        else:
            self._active_alerts.pop("response_time_critical", None)
            self._active_alerts.pop("response_time_warning", None)

        return new_alerts

    def _maybe_fire(self, alert_key, severity, message, action, value, threshold, now):
        """Fire alert if not in cooldown. Returns alert dict or None."""
        last_fired = self._cooldowns.get(alert_key, 0)
        if now - last_fired < self._cooldown_seconds:
            return None  # Still in cooldown

        alert = {
            "type": alert_key.rsplit("_", 1)[0],  # e.g., "error_rate"
            "severity": severity,
            "message": message,
            "action_required": action,
            "timestamp": now,
            "value": value,
            "threshold": threshold,
        }

        self._cooldowns[alert_key] = now
        self._active_alerts[alert_key] = alert
        self._history.appendleft(alert)
        logger.warning("ALERT [%s]: %s", severity.upper(), message)

        return alert

    def get_active_alerts(self):
        return list(self._active_alerts.values())

    def get_alert_history(self, limit=50):
        return list(self._history)[:limit]
