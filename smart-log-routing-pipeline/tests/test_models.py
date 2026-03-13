"""Tests for LogMessage model."""

import json

from src.models.log_message import LEVELS, LogMessage, SERVICES


class TestLogMessageCreation:
    """Tests for LogMessage field assignment and basic properties."""

    def test_log_message_creation(self):
        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="database",
            component="postgres",
            level="error",
            message="Connection refused",
            metadata={"source_ip": "192.168.1.1", "request_id": "abc-123"},
        )
        assert msg.timestamp == "2026-03-12T10:00:00+00:00"
        assert msg.service == "database"
        assert msg.component == "postgres"
        assert msg.level == "error"
        assert msg.message == "Connection refused"
        assert msg.metadata == {"source_ip": "192.168.1.1", "request_id": "abc-123"}

    def test_routing_key_format(self):
        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="api",
            component="gateway",
            level="warning",
            message="High latency",
            metadata={},
        )
        assert msg.routing_key == "api.gateway.warning"

    def test_to_dict(self):
        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="security",
            component="firewall",
            level="critical",
            message="Breach detected",
            metadata={"source_ip": "10.0.0.1", "request_id": "xyz"},
        )
        d = msg.to_dict()
        expected_keys = {
            "timestamp", "service", "component", "level",
            "message", "metadata", "routing_key",
        }
        assert set(d.keys()) == expected_keys
        assert d["routing_key"] == "security.firewall.critical"
        assert d["service"] == "security"

    def test_to_json(self):
        msg = LogMessage(
            timestamp="2026-03-12T10:00:00+00:00",
            service="user",
            component="auth",
            level="info",
            message="Login successful",
            metadata={"source_ip": "192.168.1.5", "request_id": "def-456"},
        )
        json_str = msg.to_json()
        parsed = json.loads(json_str)
        assert parsed["service"] == "user"
        assert parsed["component"] == "auth"
        assert parsed["level"] == "info"
        assert parsed["routing_key"] == "user.auth.info"

    def test_generate_random(self):
        msg = LogMessage.generate_random()
        assert isinstance(msg, LogMessage)
        assert msg.service in SERVICES
        assert msg.level in LEVELS
        assert len(msg.timestamp) > 0
        assert len(msg.message) > 0

    def test_generate_random_has_metadata(self):
        msg = LogMessage.generate_random()
        assert "source_ip" in msg.metadata
        assert "request_id" in msg.metadata
        assert msg.metadata["source_ip"].startswith("192.168.")
        assert len(msg.metadata["request_id"]) > 0

    def test_routing_key_components(self):
        msg = LogMessage.generate_random()
        parts = msg.routing_key.split(".")
        assert len(parts) == 3, f"Expected 3 parts, got {len(parts)}: {msg.routing_key}"
        assert parts[0] == msg.service
        assert parts[1] == msg.component
        assert parts[2] == msg.level
