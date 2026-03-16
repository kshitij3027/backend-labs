"""Tests for src.models — LogEntry creation, routing, serialization."""

from src.models import LogEntry, LogLevel


class TestLogEntryCreation:
    """Verify default field population on a fresh LogEntry."""

    def test_log_entry_creation(self) -> None:
        entry = LogEntry(message="hello world")
        assert entry.message == "hello world"
        assert entry.level == LogLevel.INFO
        assert entry.service == "unknown"
        assert entry.component == "main"
        assert entry.timestamp is not None
        assert len(entry.trace_id) == 16


class TestRouteTopic:
    """Topic routing must respect the documented priority order."""

    def test_route_topic_error(self) -> None:
        entry = LogEntry(message="boom", level=LogLevel.ERROR)
        assert entry.route_topic() == "logs-errors"

    def test_route_topic_critical(self) -> None:
        entry = LogEntry(message="fatal", level=LogLevel.CRITICAL)
        assert entry.route_topic() == "logs-errors"

    def test_route_topic_database(self) -> None:
        entry = LogEntry(message="query", service="database-proxy")
        assert entry.route_topic() == "logs-database"

    def test_route_topic_security(self) -> None:
        entry = LogEntry(message="login", service="auth-service")
        assert entry.route_topic() == "logs-security"

    def test_route_topic_application(self) -> None:
        entry = LogEntry(message="ok", service="user-service")
        assert entry.route_topic() == "logs-application"

    def test_route_topic_error_priority(self) -> None:
        """ERROR from a db service should still route to logs-errors."""
        entry = LogEntry(
            message="db crash",
            level=LogLevel.ERROR,
            service="database-proxy",
        )
        assert entry.route_topic() == "logs-errors"


class TestPartitionKey:
    """Partition key priority: user_id > session_id > service."""

    def test_partition_key_priority(self) -> None:
        # user_id present -> use user_id
        entry = LogEntry(
            message="m",
            user_id="u1",
            session_id="s1",
            service="svc",
        )
        assert entry.to_kafka_key() == "u1"

        # no user_id -> session_id
        entry2 = LogEntry(message="m", session_id="s1", service="svc")
        assert entry2.to_kafka_key() == "s1"

        # neither -> service
        entry3 = LogEntry(message="m", service="svc")
        assert entry3.to_kafka_key() == "svc"


class TestSerialization:
    """Round-trip through to_kafka_value / from_kafka_value."""

    def test_serialization_roundtrip(self, sample_log_entry: LogEntry) -> None:
        raw = sample_log_entry.to_kafka_value()
        restored = LogEntry.from_kafka_value(raw)

        assert restored.message == sample_log_entry.message
        assert restored.level == sample_log_entry.level
        assert restored.service == sample_log_entry.service
        assert restored.trace_id == sample_log_entry.trace_id
        assert restored.user_id == sample_log_entry.user_id
        assert restored.session_id == sample_log_entry.session_id
