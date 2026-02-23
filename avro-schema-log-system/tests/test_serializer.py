"""Unit tests for AvroSerializer."""

from src.log_event import LogEvent


class TestAvroSerializer:
    """Tests for serialize() and serialize_to_container()."""

    def test_serialize_v1(self, serializer):
        """Serialize a v1 sample and verify output is non-empty bytes."""
        sample = LogEvent.generate_sample("v1")
        data = serializer.serialize(sample.to_dict("v1"), "v1")
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_serialize_v2(self, serializer):
        """Serialize a v2 sample and verify output is non-empty bytes."""
        sample = LogEvent.generate_sample("v2")
        data = serializer.serialize(sample.to_dict("v2"), "v2")
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_serialize_v3(self, serializer):
        """Serialize a v3 sample and verify output is non-empty bytes."""
        sample = LogEvent.generate_sample("v3")
        data = serializer.serialize(sample.to_dict("v3"), "v3")
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_serialize_size_ordering(self, serializer):
        """v1 bytes should be smaller than v2 bytes, which should be smaller than v3 bytes.

        Uses fixed data to eliminate randomness in field values.
        """
        base_event = LogEvent(
            timestamp="2026-01-01T00:00:00+00:00",
            level="INFO",
            message="Test message for size comparison",
            source="test-service",
            trace_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            span_id="1234567890abcdef",
            tags={"env": "production", "region": "us-east-1"},
            hostname="web-01.us-east-1",
        )

        v1_bytes = serializer.serialize(base_event.to_dict("v1"), "v1")
        v2_bytes = serializer.serialize(base_event.to_dict("v2"), "v2")
        v3_bytes = serializer.serialize(base_event.to_dict("v3"), "v3")

        assert len(v1_bytes) < len(v2_bytes), (
            f"v1 ({len(v1_bytes)}B) should be smaller than v2 ({len(v2_bytes)}B)"
        )
        assert len(v2_bytes) < len(v3_bytes), (
            f"v2 ({len(v2_bytes)}B) should be smaller than v3 ({len(v3_bytes)}B)"
        )

    def test_serialize_to_container(self, serializer):
        """Serialize multiple events to container format and verify output is bytes."""
        events = [
            LogEvent.generate_sample("v2").to_dict("v2") for _ in range(3)
        ]
        container_bytes = serializer.serialize_to_container(events, "v2")
        assert isinstance(container_bytes, bytes)
        assert len(container_bytes) > 0
