"""Tests for the LogEnricher class."""

from src.enricher import LogEnricher
from src.models import EnrichedLog, EnrichmentRequest


class TestLogEnricher:
    """Verify end-to-end enrichment behaviour."""

    def test_info_message_has_system_and_env_fields(self, sample_config):
        enricher = LogEnricher(config=sample_config)
        result = enricher.enrich(EnrichmentRequest(log_message="INFO: service started", source="test"))

        # Default rule matches all messages with system_info + environment collectors.
        # That gives us: hostname, os_info, python_version, service_name, environment,
        # version, region -- plus message, source, timestamp.
        non_none = {k: v for k, v in result.model_dump().items() if v is not None}
        assert len(non_none) >= 8

    def test_error_message_has_performance_fields(self, sample_config):
        enricher = LogEnricher(config=sample_config)
        result = enricher.enrich(EnrichmentRequest(log_message="ERROR: disk full", source="test"))

        assert result.cpu_percent is not None
        assert result.memory_percent is not None
        assert result.disk_percent is not None

    def test_empty_message_produces_valid_enriched_log(self, sample_config):
        enricher = LogEnricher(config=sample_config)
        result = enricher.enrich(EnrichmentRequest(log_message="", source="test"))

        assert isinstance(result, EnrichedLog)
        assert result.message == ""
        assert result.source == "test"
        assert result.timestamp != ""

    def test_stats_increment_after_enriching(self, sample_config):
        enricher = LogEnricher(config=sample_config)
        enricher.enrich(EnrichmentRequest(log_message="some log", source="test"))
        stats = enricher.get_stats()
        assert stats.processed_count > 0

    def test_enriched_log_always_has_critical_fields(self, sample_config):
        enricher = LogEnricher(config=sample_config)
        result = enricher.enrich(EnrichmentRequest(log_message="anything", source="myapp"))

        assert result.message == "anything"
        assert result.source == "myapp"
        assert result.timestamp != ""

    def test_collectors_applied_not_empty(self, sample_config):
        enricher = LogEnricher(config=sample_config)
        result = enricher.enrich(EnrichmentRequest(log_message="some log message", source="test"))
        assert len(result.collectors_applied) > 0
