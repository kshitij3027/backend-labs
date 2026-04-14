"""Basic model and schema tests."""

from datetime import datetime

from src.models import Alert, AlertRule, AlertState, LogEntry


class TestAlertState:
    def test_enum_values(self):
        assert AlertState.NEW == "NEW"
        assert AlertState.ACKNOWLEDGED == "ACKNOWLEDGED"
        assert AlertState.ESCALATED == "ESCALATED"
        assert AlertState.RESOLVED == "RESOLVED"

    def test_enum_members(self):
        members = [e.value for e in AlertState]
        assert "NEW" in members
        assert "ACKNOWLEDGED" in members
        assert "ESCALATED" in members
        assert "RESOLVED" in members


class TestLogEntry:
    def test_instantiation(self):
        entry = LogEntry(
            level="ERROR",
            message="test error message",
            source="test-service",
        )
        assert entry.level == "ERROR"
        assert entry.message == "test error message"
        assert entry.source == "test-service"

    def test_defaults(self):
        entry = LogEntry(level="INFO", message="test")
        assert entry.processed is None or entry.processed is False
        assert entry.source is None
        assert entry.metadata_ is None


class TestAlert:
    def test_instantiation(self):
        now = datetime.utcnow()
        alert = Alert(
            pattern_name="auth_failure",
            severity="high",
            message="Authentication failures detected",
            first_occurrence=now,
            last_occurrence=now,
        )
        assert alert.pattern_name == "auth_failure"
        assert alert.severity == "high"

    def test_defaults(self):
        now = datetime.utcnow()
        alert = Alert(
            pattern_name="test",
            severity="low",
            message="test",
            first_occurrence=now,
            last_occurrence=now,
        )
        assert alert.state is None or alert.state == "NEW"
        assert alert.acknowledged_by is None
        assert alert.acknowledged_at is None
        assert alert.resolved_at is None


class TestAlertRule:
    def test_instantiation(self):
        rule = AlertRule(
            name="test_rule",
            pattern=r"error\s+occurred",
            threshold=5,
            window_seconds=60,
            severity="medium",
        )
        assert rule.name == "test_rule"
        assert rule.pattern == r"error\s+occurred"
        assert rule.threshold == 5
        assert rule.window_seconds == 60
        assert rule.severity == "medium"

    def test_defaults(self):
        rule = AlertRule(
            name="test",
            pattern="test",
            threshold=1,
            window_seconds=30,
            severity="low",
        )
        assert rule.enabled is None or rule.enabled is True
