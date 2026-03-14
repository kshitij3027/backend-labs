"""Tests for data models: LogMessage and FailedMessage serialization."""

import json

import pytest

from src.models import FailedMessage, FailureType, LogLevel, LogMessage


# ---------------------------------------------------------------------------
# LogLevel enum
# ---------------------------------------------------------------------------

class TestLogLevel:
    def test_all_values_exist(self):
        assert LogLevel.DEBUG.value == "DEBUG"
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING"
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.CRITICAL.value == "CRITICAL"

    def test_member_count(self):
        assert len(LogLevel) == 5

    def test_lookup_by_value(self):
        assert LogLevel("ERROR") is LogLevel.ERROR


# ---------------------------------------------------------------------------
# FailureType enum
# ---------------------------------------------------------------------------

class TestFailureType:
    def test_all_values_exist(self):
        assert FailureType.PARSING.value == "PARSING"
        assert FailureType.NETWORK.value == "NETWORK"
        assert FailureType.RESOURCE.value == "RESOURCE"
        assert FailureType.UNKNOWN.value == "UNKNOWN"

    def test_member_count(self):
        assert len(FailureType) == 4

    def test_lookup_by_value(self):
        assert FailureType("NETWORK") is FailureType.NETWORK


# ---------------------------------------------------------------------------
# LogMessage
# ---------------------------------------------------------------------------

class TestLogMessage:
    def test_defaults(self):
        msg = LogMessage()
        assert msg.level == LogLevel.INFO
        assert msg.source == ""
        assert msg.message == ""
        assert msg.metadata == {}
        assert msg.id  # non-empty uuid string
        assert msg.timestamp  # non-empty ISO string

    def test_serialization_round_trip(self):
        msg = LogMessage(
            id="abc-123",
            timestamp="2025-01-15T10:30:00+00:00",
            level=LogLevel.ERROR,
            source="auth-service",
            message="Login failed",
            metadata={"user_id": 42, "ip": "10.0.0.1"},
        )
        json_str = msg.to_json()
        restored = LogMessage.from_json(json_str)

        assert restored.id == msg.id
        assert restored.timestamp == msg.timestamp
        assert restored.level == msg.level
        assert restored.source == msg.source
        assert restored.message == msg.message
        assert restored.metadata == msg.metadata

    def test_to_json_stores_level_as_value_string(self):
        msg = LogMessage(level=LogLevel.CRITICAL)
        raw = json.loads(msg.to_json())
        assert raw["level"] == "CRITICAL"

    @pytest.mark.parametrize("level", list(LogLevel))
    def test_round_trip_all_levels(self, level):
        msg = LogMessage(level=level)
        restored = LogMessage.from_json(msg.to_json())
        assert restored.level == level

    def test_round_trip_preserves_empty_metadata(self):
        msg = LogMessage(metadata={})
        restored = LogMessage.from_json(msg.to_json())
        assert restored.metadata == {}

    def test_round_trip_preserves_nested_metadata(self):
        nested = {"tags": ["web", "prod"], "counts": {"ok": 1, "err": 2}}
        msg = LogMessage(metadata=nested)
        restored = LogMessage.from_json(msg.to_json())
        assert restored.metadata == nested

    def test_from_json_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            LogMessage.from_json("not valid json")

    def test_from_json_missing_field_raises(self):
        incomplete = json.dumps({"id": "x", "timestamp": "t"})
        with pytest.raises(KeyError):
            LogMessage.from_json(incomplete)

    def test_from_json_invalid_level_raises(self):
        bad = json.dumps({
            "id": "x",
            "timestamp": "t",
            "level": "NONEXISTENT",
            "source": "s",
            "message": "m",
            "metadata": {},
        })
        with pytest.raises(ValueError):
            LogMessage.from_json(bad)

    def test_unique_ids_per_instance(self):
        a = LogMessage()
        b = LogMessage()
        assert a.id != b.id


# ---------------------------------------------------------------------------
# FailedMessage
# ---------------------------------------------------------------------------

class TestFailedMessage:
    def _make_failed(self, **overrides):
        defaults = dict(
            original_message=LogMessage(
                id="msg-001",
                timestamp="2025-06-01T12:00:00+00:00",
                level=LogLevel.WARNING,
                source="payment-service",
                message="Timeout connecting to gateway",
                metadata={"txn": "T100"},
            ),
            failure_type=FailureType.NETWORK,
            error_details="Connection refused on port 443",
            retry_count=2,
            max_retries=3,
            first_failure="2025-06-01T12:00:01+00:00",
            last_failure="2025-06-01T12:00:05+00:00",
        )
        defaults.update(overrides)
        return FailedMessage(**defaults)

    def test_serialization_round_trip(self):
        fm = self._make_failed()
        json_str = fm.to_json()
        restored = FailedMessage.from_json(json_str)

        assert restored.failure_type == fm.failure_type
        assert restored.error_details == fm.error_details
        assert restored.retry_count == fm.retry_count
        assert restored.max_retries == fm.max_retries
        assert restored.first_failure == fm.first_failure
        assert restored.last_failure == fm.last_failure

        # Inner LogMessage must also survive
        orig = fm.original_message
        rest_orig = restored.original_message
        assert rest_orig.id == orig.id
        assert rest_orig.timestamp == orig.timestamp
        assert rest_orig.level == orig.level
        assert rest_orig.source == orig.source
        assert rest_orig.message == orig.message
        assert rest_orig.metadata == orig.metadata

    @pytest.mark.parametrize("ftype", list(FailureType))
    def test_round_trip_all_failure_types(self, ftype):
        fm = self._make_failed(failure_type=ftype)
        restored = FailedMessage.from_json(fm.to_json())
        assert restored.failure_type == ftype

    def test_to_json_embeds_original_as_dict(self):
        fm = self._make_failed()
        raw = json.loads(fm.to_json())
        assert isinstance(raw["original_message"], dict)
        assert raw["original_message"]["id"] == "msg-001"
        assert raw["failure_type"] == "NETWORK"

    def test_preserves_retry_count_zero(self):
        fm = self._make_failed(retry_count=0)
        restored = FailedMessage.from_json(fm.to_json())
        assert restored.retry_count == 0

    def test_preserves_high_retry_count(self):
        fm = self._make_failed(retry_count=99, max_retries=100)
        restored = FailedMessage.from_json(fm.to_json())
        assert restored.retry_count == 99
        assert restored.max_retries == 100

    def test_preserves_original_metadata_through_nesting(self):
        meta = {"env": "staging", "region": "us-east-1", "tags": [1, 2, 3]}
        msg = LogMessage(metadata=meta)
        fm = self._make_failed(original_message=msg)
        restored = FailedMessage.from_json(fm.to_json())
        assert restored.original_message.metadata == meta

    def test_from_json_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            FailedMessage.from_json("{bad json")

    def test_from_json_missing_field_raises(self):
        incomplete = json.dumps({"original_message": {}, "failure_type": "UNKNOWN"})
        with pytest.raises(KeyError):
            FailedMessage.from_json(incomplete)

    def test_from_json_invalid_failure_type_raises(self):
        fm = self._make_failed()
        raw = json.loads(fm.to_json())
        raw["failure_type"] = "DOES_NOT_EXIST"
        with pytest.raises(ValueError):
            FailedMessage.from_json(json.dumps(raw))

    def test_defaults(self):
        fm = FailedMessage()
        assert fm.failure_type == FailureType.UNKNOWN
        assert fm.error_details == ""
        assert fm.retry_count == 0
        assert fm.max_retries == 3
        assert fm.first_failure  # non-empty
        assert fm.last_failure  # non-empty
        assert isinstance(fm.original_message, LogMessage)
