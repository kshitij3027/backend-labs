"""Tests for Pydantic models: EnrichmentRequest, EnrichedLog, EnrichmentStats."""

import pytest
from pydantic import ValidationError

from src.models import EnrichedLog, EnrichmentRequest, EnrichmentStats


class TestEnrichmentRequest:
    """Tests for the EnrichmentRequest model."""

    def test_default_source_is_unknown(self):
        req = EnrichmentRequest(log_message="some log")
        assert req.source == "unknown"

    def test_custom_source(self):
        req = EnrichmentRequest(log_message="some log", source="my-service")
        assert req.source == "my-service"

    def test_log_message_is_required(self):
        with pytest.raises(ValidationError):
            EnrichmentRequest()


class TestEnrichedLog:
    """Tests for the EnrichedLog model."""

    def test_has_timestamp_by_default(self):
        log = EnrichedLog(message="hello", source="test")
        assert log.timestamp != ""
        assert "T" in log.timestamp  # ISO-8601 format contains 'T'

    def test_message_is_required(self):
        with pytest.raises(ValidationError):
            EnrichedLog(source="test")

    def test_optional_fields_default_to_none(self):
        log = EnrichedLog(message="hello", source="test")
        assert log.hostname is None
        assert log.os_info is None
        assert log.python_version is None
        assert log.service_name is None
        assert log.environment is None
        assert log.version is None
        assert log.region is None
        assert log.cpu_percent is None
        assert log.memory_percent is None
        assert log.disk_percent is None
        assert log.env_context is None

    def test_collectors_applied_defaults_to_empty_list(self):
        log = EnrichedLog(message="hello", source="test")
        assert log.collectors_applied == []
        assert log.enrichment_errors == []

    def test_can_set_all_fields(self):
        log = EnrichedLog(
            message="test message",
            source="my-service",
            timestamp="2024-01-01T00:00:00+00:00",
            hostname="server-1",
            os_info="Linux 6.1",
            python_version="3.12.0",
            service_name="enrichment",
            environment="production",
            version="2.0.0",
            region="us-east-1",
            cpu_percent=45.2,
            memory_percent=67.8,
            disk_percent=30.1,
            env_context={"key": "value"},
            enrichment_duration_ms=12.5,
            collectors_applied=["system_info", "environment"],
            enrichment_errors=["timeout on perf collector"],
        )
        assert log.message == "test message"
        assert log.hostname == "server-1"
        assert log.cpu_percent == 45.2
        assert log.env_context == {"key": "value"}
        assert log.enrichment_duration_ms == 12.5
        assert len(log.collectors_applied) == 2
        assert len(log.enrichment_errors) == 1


class TestEnrichmentStats:
    """Tests for the EnrichmentStats model."""

    def test_all_defaults_are_zero(self):
        stats = EnrichmentStats()
        assert stats.processed_count == 0
        assert stats.error_count == 0
        assert stats.success_rate == 0.0
        assert stats.average_throughput == 0.0
        assert stats.runtime_seconds == 0.0

    def test_can_set_values(self):
        stats = EnrichmentStats(
            processed_count=100,
            error_count=5,
            success_rate=95.0,
            average_throughput=50.5,
            runtime_seconds=120.0,
        )
        assert stats.processed_count == 100
        assert stats.error_count == 5
        assert stats.success_rate == 95.0
        assert stats.average_throughput == 50.5
        assert stats.runtime_seconds == 120.0
