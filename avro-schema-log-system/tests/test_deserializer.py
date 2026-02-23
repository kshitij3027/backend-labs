"""Unit tests for AvroDeserializer."""

from src.log_event import LogEvent, VERSION_FIELDS


class TestAvroDeserializer:
    """Tests for deserialize() and deserialize_container()."""

    def test_deserialize_round_trip_v1(self, serializer, deserializer):
        """Serialize v1, deserialize v1, verify all fields match."""
        sample = LogEvent.generate_sample("v1")
        original = sample.to_dict("v1")
        data = serializer.serialize(original, "v1")
        result = deserializer.deserialize(data, "v1")

        for field in VERSION_FIELDS["v1"]:
            assert result[field] == original[field], f"Field '{field}' mismatch"

    def test_deserialize_round_trip_v2(self, serializer, deserializer):
        """Serialize v2, deserialize v2, verify all fields match."""
        sample = LogEvent.generate_sample("v2")
        original = sample.to_dict("v2")
        data = serializer.serialize(original, "v2")
        result = deserializer.deserialize(data, "v2")

        for field in VERSION_FIELDS["v2"]:
            assert result[field] == original[field], f"Field '{field}' mismatch"

    def test_deserialize_round_trip_v3(self, serializer, deserializer):
        """Serialize v3, deserialize v3, verify all fields match."""
        sample = LogEvent.generate_sample("v3")
        original = sample.to_dict("v3")
        data = serializer.serialize(original, "v3")
        result = deserializer.deserialize(data, "v3")

        for field in VERSION_FIELDS["v3"]:
            assert result[field] == original[field], f"Field '{field}' mismatch"

    def test_cross_version_v1_read_as_v2(self, serializer, deserializer):
        """Serialize v1, deserialize with reader=v2; new fields should be None (defaults)."""
        sample = LogEvent.generate_sample("v1")
        original = sample.to_dict("v1")
        data = serializer.serialize(original, "v1")
        result = deserializer.deserialize(data, writer_version="v1", reader_version="v2")

        # Original v1 fields should be intact
        for field in VERSION_FIELDS["v1"]:
            assert result[field] == original[field], f"Field '{field}' mismatch"

        # Fields added in v2 should be None (default)
        assert result["trace_id"] is None
        assert result["span_id"] is None

    def test_cross_version_v2_read_as_v1(self, serializer, deserializer):
        """Serialize v2, deserialize with reader=v1; only v1 fields should be returned."""
        sample = LogEvent.generate_sample("v2")
        original = sample.to_dict("v2")
        data = serializer.serialize(original, "v2")
        result = deserializer.deserialize(data, writer_version="v2", reader_version="v1")

        # Only v1 fields should be present
        assert set(result.keys()) == set(VERSION_FIELDS["v1"])
        for field in VERSION_FIELDS["v1"]:
            assert result[field] == original[field], f"Field '{field}' mismatch"

    def test_container_round_trip(self, serializer, deserializer):
        """Serialize to container, deserialize container, verify record count."""
        events = [
            LogEvent.generate_sample("v3").to_dict("v3") for _ in range(5)
        ]
        container_bytes = serializer.serialize_to_container(events, "v3")
        records = deserializer.deserialize_container(container_bytes)
        assert len(records) == 5

    def test_container_round_trip_with_reader_version(self, serializer, deserializer):
        """Serialize v3 container, deserialize with reader=v1, verify field projection."""
        events = [
            LogEvent.generate_sample("v3").to_dict("v3") for _ in range(3)
        ]
        container_bytes = serializer.serialize_to_container(events, "v3")
        records = deserializer.deserialize_container(container_bytes, reader_version="v1")
        assert len(records) == 3
        for record in records:
            assert set(record.keys()) == set(VERSION_FIELDS["v1"])
