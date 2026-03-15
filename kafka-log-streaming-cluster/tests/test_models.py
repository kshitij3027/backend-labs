"""Tests for src.models — LogMessage, LogLevel, ServiceName, and TOPIC_MAP."""

import json

import pytest
from pydantic import ValidationError

from src.models import TOPIC_MAP, LogLevel, LogMessage, ServiceName


class TestLogLevelEnum:
    """Test LogLevel enum values."""

    def test_info_value(self) -> None:
        assert LogLevel.INFO == "INFO"
        assert LogLevel.INFO.value == "INFO"

    def test_warn_value(self) -> None:
        assert LogLevel.WARN == "WARN"
        assert LogLevel.WARN.value == "WARN"

    def test_error_value(self) -> None:
        assert LogLevel.ERROR == "ERROR"
        assert LogLevel.ERROR.value == "ERROR"

    def test_has_exactly_three_members(self) -> None:
        assert len(LogLevel) == 3

    def test_invalid_level_raises(self) -> None:
        with pytest.raises(ValueError):
            LogLevel("DEBUG")


class TestServiceNameEnum:
    """Test ServiceName enum values."""

    def test_web_api_value(self) -> None:
        assert ServiceName.WEB_API == "web-api"
        assert ServiceName.WEB_API.value == "web-api"

    def test_user_service_value(self) -> None:
        assert ServiceName.USER_SERVICE == "user-service"
        assert ServiceName.USER_SERVICE.value == "user-service"

    def test_payment_service_value(self) -> None:
        assert ServiceName.PAYMENT_SERVICE == "payment-service"
        assert ServiceName.PAYMENT_SERVICE.value == "payment-service"

    def test_has_exactly_three_members(self) -> None:
        assert len(ServiceName) == 3

    def test_invalid_service_raises(self) -> None:
        with pytest.raises(ValueError):
            ServiceName("unknown-service")


class TestTopicMap:
    """Test the TOPIC_MAP mapping."""

    def test_has_all_three_services(self) -> None:
        assert len(TOPIC_MAP) == 3
        assert ServiceName.WEB_API in TOPIC_MAP
        assert ServiceName.USER_SERVICE in TOPIC_MAP
        assert ServiceName.PAYMENT_SERVICE in TOPIC_MAP

    def test_web_api_topic(self) -> None:
        assert TOPIC_MAP[ServiceName.WEB_API] == "web-api-logs"

    def test_user_service_topic(self) -> None:
        assert TOPIC_MAP[ServiceName.USER_SERVICE] == "user-service-logs"

    def test_payment_service_topic(self) -> None:
        assert TOPIC_MAP[ServiceName.PAYMENT_SERVICE] == "payment-service-logs"


class TestLogMessageCreation:
    """Test LogMessage instantiation with valid data."""

    def test_creation_with_all_fields(self, sample_log_message: LogMessage) -> None:
        assert sample_log_message.timestamp == "2026-03-15T10:30:00+00:00"
        assert sample_log_message.service == ServiceName.WEB_API
        assert sample_log_message.level == LogLevel.INFO
        assert sample_log_message.endpoint == "/api/users"
        assert sample_log_message.status_code == 200
        assert sample_log_message.user_id == "test-user-001"
        assert sample_log_message.message == "Request processed successfully"
        assert sample_log_message.sequence_number == 1

    def test_default_message_is_empty_string(self) -> None:
        msg = LogMessage(
            timestamp="2026-03-15T10:00:00+00:00",
            service=ServiceName.WEB_API,
            level=LogLevel.INFO,
            endpoint="/health",
            status_code=200,
            user_id="u-1",
        )
        assert msg.message == ""

    def test_default_sequence_number_is_zero(self) -> None:
        msg = LogMessage(
            timestamp="2026-03-15T10:00:00+00:00",
            service=ServiceName.WEB_API,
            level=LogLevel.INFO,
            endpoint="/health",
            status_code=200,
            user_id="u-1",
        )
        assert msg.sequence_number == 0


class TestLogMessageSerialization:
    """Test to_kafka_value / from_kafka_value round-trip."""

    def test_to_kafka_value_returns_bytes(self, sample_log_message: LogMessage) -> None:
        value = sample_log_message.to_kafka_value()
        assert isinstance(value, bytes)

    def test_to_kafka_value_is_valid_json(self, sample_log_message: LogMessage) -> None:
        value = sample_log_message.to_kafka_value()
        parsed = json.loads(value)
        assert parsed["service"] == "web-api"
        assert parsed["level"] == "INFO"
        assert parsed["user_id"] == "test-user-001"

    def test_round_trip_serialization(self, sample_log_message: LogMessage) -> None:
        serialized = sample_log_message.to_kafka_value()
        deserialized = LogMessage.from_kafka_value(serialized)
        assert deserialized == sample_log_message

    def test_round_trip_preserves_all_fields(self, sample_error_message: LogMessage) -> None:
        serialized = sample_error_message.to_kafka_value()
        deserialized = LogMessage.from_kafka_value(serialized)
        assert deserialized.timestamp == sample_error_message.timestamp
        assert deserialized.service == sample_error_message.service
        assert deserialized.level == sample_error_message.level
        assert deserialized.endpoint == sample_error_message.endpoint
        assert deserialized.status_code == sample_error_message.status_code
        assert deserialized.user_id == sample_error_message.user_id
        assert deserialized.message == sample_error_message.message
        assert deserialized.sequence_number == sample_error_message.sequence_number

    def test_batch_round_trip(self, sample_messages_batch: list[LogMessage]) -> None:
        for msg in sample_messages_batch:
            assert LogMessage.from_kafka_value(msg.to_kafka_value()) == msg


class TestLogMessageProperties:
    """Test the topic and partition_key properties."""

    def test_topic_for_web_api(self) -> None:
        msg = LogMessage(
            timestamp="2026-03-15T10:00:00+00:00",
            service=ServiceName.WEB_API,
            level=LogLevel.INFO,
            endpoint="/api/test",
            status_code=200,
            user_id="u-1",
        )
        assert msg.topic == "web-api-logs"

    def test_topic_for_user_service(self) -> None:
        msg = LogMessage(
            timestamp="2026-03-15T10:00:00+00:00",
            service=ServiceName.USER_SERVICE,
            level=LogLevel.WARN,
            endpoint="/users/test",
            status_code=404,
            user_id="u-2",
        )
        assert msg.topic == "user-service-logs"

    def test_topic_for_payment_service(self, sample_error_message: LogMessage) -> None:
        assert sample_error_message.topic == "payment-service-logs"

    def test_partition_key_returns_bytes(self, sample_log_message: LogMessage) -> None:
        key = sample_log_message.partition_key
        assert isinstance(key, bytes)

    def test_partition_key_is_user_id_encoded(self, sample_log_message: LogMessage) -> None:
        key = sample_log_message.partition_key
        assert key == b"test-user-001"

    def test_partition_key_different_users(self) -> None:
        msg1 = LogMessage(
            timestamp="2026-03-15T10:00:00+00:00",
            service=ServiceName.WEB_API,
            level=LogLevel.INFO,
            endpoint="/test",
            status_code=200,
            user_id="alice",
        )
        msg2 = LogMessage(
            timestamp="2026-03-15T10:00:00+00:00",
            service=ServiceName.WEB_API,
            level=LogLevel.INFO,
            endpoint="/test",
            status_code=200,
            user_id="bob",
        )
        assert msg1.partition_key != msg2.partition_key


class TestLogMessageValidation:
    """Test Pydantic validation rejects invalid data."""

    def test_invalid_log_level_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            LogMessage(
                timestamp="2026-03-15T10:00:00+00:00",
                service=ServiceName.WEB_API,
                level="DEBUG",  # type: ignore[arg-type]
                endpoint="/test",
                status_code=200,
                user_id="u-1",
            )

    def test_invalid_service_name_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            LogMessage(
                timestamp="2026-03-15T10:00:00+00:00",
                service="unknown-service",  # type: ignore[arg-type]
                level=LogLevel.INFO,
                endpoint="/test",
                status_code=200,
                user_id="u-1",
            )

    def test_missing_required_field_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError):
            LogMessage(
                timestamp="2026-03-15T10:00:00+00:00",
                service=ServiceName.WEB_API,
                level=LogLevel.INFO,
                # endpoint missing
                status_code=200,
                user_id="u-1",
            )  # type: ignore[call-arg]

    def test_invalid_json_from_kafka_raises(self) -> None:
        with pytest.raises(ValidationError):
            LogMessage.from_kafka_value(b"not valid json")
