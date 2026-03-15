"""Tests for src.config — Settings dataclass and load_config()."""

from src.config import Settings, load_config


class TestSettingsDefaults:
    """Verify all default values on the Settings dataclass."""

    def test_bootstrap_servers_default(self, settings: Settings) -> None:
        assert settings.bootstrap_servers == "localhost:9092,localhost:9093,localhost:9094"

    def test_internal_bootstrap_servers_default(self, settings: Settings) -> None:
        assert settings.bootstrap_servers_internal == "kafka-1:29092,kafka-2:29092,kafka-3:29092"

    def test_use_internal_listeners_default(self, settings: Settings) -> None:
        assert settings.use_internal_listeners is False

    def test_topic_defaults(self, settings: Settings) -> None:
        assert settings.web_api_topic == "web-api-logs"
        assert settings.user_service_topic == "user-service-logs"
        assert settings.payment_service_topic == "payment-service-logs"
        assert settings.critical_topic == "critical-logs"

    def test_producer_defaults(self, settings: Settings) -> None:
        assert settings.producer_batch_size == 200000
        assert settings.producer_linger_ms == 100
        assert settings.producer_compression == "lz4"
        assert settings.producer_duration_seconds == 60
        assert settings.producer_rate_per_second == 100.0

    def test_consumer_defaults(self, settings: Settings) -> None:
        assert settings.dashboard_group_id == "dashboard-consumer"
        assert settings.error_aggregator_group_id == "error-aggregator-consumer"
        assert settings.consumer_auto_offset_reset == "earliest"

    def test_dashboard_defaults(self, settings: Settings) -> None:
        assert settings.dashboard_host == "0.0.0.0"
        assert settings.dashboard_port == 8000
        assert settings.sse_max_buffer == 1000


class TestActiveBootstrapServers:
    """Test the active_bootstrap_servers property."""

    def test_returns_external_by_default(self) -> None:
        s = Settings()
        assert s.active_bootstrap_servers == s.bootstrap_servers

    def test_returns_internal_when_enabled(self) -> None:
        s = Settings(use_internal_listeners=True)
        assert s.active_bootstrap_servers == s.bootstrap_servers_internal

    def test_returns_custom_internal(self) -> None:
        s = Settings(
            use_internal_listeners=True,
            bootstrap_servers_internal="custom:29092",
        )
        assert s.active_bootstrap_servers == "custom:29092"


class TestAllServiceTopics:
    """Test the all_service_topics property."""

    def test_returns_three_topics(self) -> None:
        s = Settings()
        topics = s.all_service_topics
        assert len(topics) == 3

    def test_contains_all_service_topics(self) -> None:
        s = Settings()
        topics = s.all_service_topics
        assert "web-api-logs" in topics
        assert "user-service-logs" in topics
        assert "payment-service-logs" in topics

    def test_reflects_custom_topic_names(self) -> None:
        s = Settings(web_api_topic="custom-web-logs")
        topics = s.all_service_topics
        assert "custom-web-logs" in topics


class TestLoadConfig:
    """Test load_config() reading from environment variables."""

    def test_returns_defaults_with_no_env(self, monkeypatch) -> None:
        monkeypatch.delenv("BOOTSTRAP_SERVERS", raising=False)
        monkeypatch.delenv("USE_INTERNAL_LISTENERS", raising=False)
        cfg = load_config()
        assert cfg.bootstrap_servers == "localhost:9092,localhost:9093,localhost:9094"
        assert cfg.use_internal_listeners is False

    def test_reads_bootstrap_servers(self, monkeypatch) -> None:
        monkeypatch.setenv("BOOTSTRAP_SERVERS", "custom:9092")
        cfg = load_config()
        assert cfg.bootstrap_servers == "custom:9092"

    def test_reads_producer_duration(self, monkeypatch) -> None:
        monkeypatch.setenv("PRODUCER_DURATION_SECONDS", "120")
        cfg = load_config()
        assert cfg.producer_duration_seconds == 120

    def test_reads_producer_rate(self, monkeypatch) -> None:
        monkeypatch.setenv("PRODUCER_RATE_PER_SECOND", "500.5")
        cfg = load_config()
        assert cfg.producer_rate_per_second == 500.5

    def test_reads_dashboard_port(self, monkeypatch) -> None:
        monkeypatch.setenv("DASHBOARD_PORT", "9090")
        cfg = load_config()
        assert cfg.dashboard_port == 9090

    def test_reads_sse_max_buffer(self, monkeypatch) -> None:
        monkeypatch.setenv("SSE_MAX_BUFFER", "2000")
        cfg = load_config()
        assert cfg.sse_max_buffer == 2000

    def test_invalid_int_falls_back_to_default(self, monkeypatch) -> None:
        monkeypatch.setenv("DASHBOARD_PORT", "not_a_number")
        cfg = load_config()
        assert cfg.dashboard_port == 8000


class TestBooleanParsing:
    """Test boolean parsing for USE_INTERNAL_LISTENERS."""

    def test_true_lowercase(self, monkeypatch) -> None:
        monkeypatch.setenv("USE_INTERNAL_LISTENERS", "true")
        cfg = load_config()
        assert cfg.use_internal_listeners is True

    def test_true_uppercase(self, monkeypatch) -> None:
        monkeypatch.setenv("USE_INTERNAL_LISTENERS", "True")
        cfg = load_config()
        assert cfg.use_internal_listeners is True

    def test_true_one(self, monkeypatch) -> None:
        monkeypatch.setenv("USE_INTERNAL_LISTENERS", "1")
        cfg = load_config()
        assert cfg.use_internal_listeners is True

    def test_true_yes(self, monkeypatch) -> None:
        monkeypatch.setenv("USE_INTERNAL_LISTENERS", "yes")
        cfg = load_config()
        assert cfg.use_internal_listeners is True

    def test_false_zero(self, monkeypatch) -> None:
        monkeypatch.setenv("USE_INTERNAL_LISTENERS", "0")
        cfg = load_config()
        assert cfg.use_internal_listeners is False

    def test_false_random_string(self, monkeypatch) -> None:
        monkeypatch.setenv("USE_INTERNAL_LISTENERS", "nope")
        cfg = load_config()
        assert cfg.use_internal_listeners is False

    def test_false_empty(self, monkeypatch) -> None:
        monkeypatch.setenv("USE_INTERNAL_LISTENERS", "")
        cfg = load_config()
        assert cfg.use_internal_listeners is False
