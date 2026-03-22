"""Tests for src.config module."""

from src.config import Settings, load_config


class TestSettingsDefaults:
    """Verify default values on the Settings dataclass."""

    def test_default_bootstrap_servers(self):
        s = Settings()
        assert s.bootstrap_servers == "kafka:29092"

    def test_default_group_id(self):
        s = Settings()
        assert s.group_id == "dashboard-consumer"

    def test_default_topics(self):
        s = Settings()
        assert s.topics == ["log-events", "error-events", "user-events"]

    def test_default_auto_offset_reset(self):
        s = Settings()
        assert s.auto_offset_reset == "earliest"

    def test_default_window_seconds(self):
        s = Settings()
        assert s.window_seconds == 60

    def test_default_deque_max_length(self):
        s = Settings()
        assert s.deque_max_length == 1000

    def test_default_ws_emit_interval(self):
        s = Settings()
        assert s.ws_emit_interval == 2.0

    def test_default_poll_timeout(self):
        s = Settings()
        assert s.poll_timeout_s == 1.0

    def test_default_dashboard_host(self):
        s = Settings()
        assert s.dashboard_host == "0.0.0.0"

    def test_default_dashboard_port(self):
        s = Settings()
        assert s.dashboard_port == 5000

    def test_default_derived_metrics_topic(self):
        s = Settings()
        assert s.derived_metrics_topic == "derived-metrics"

    def test_default_alert_thresholds(self):
        s = Settings()
        assert s.alert_error_rate_warning == 3.0
        assert s.alert_error_rate_critical == 5.0
        assert s.alert_response_time_warning == 1000.0
        assert s.alert_response_time_critical == 2000.0
        assert s.alert_cooldown_seconds == 60.0


class TestLoadConfig:
    """Verify load_config picks up environment variables."""

    def test_load_config_defaults(self):
        config = load_config()
        assert config.bootstrap_servers == "kafka:29092"
        assert config.dashboard_port == 5000

    def test_load_config_overrides_string(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "custom-broker:9092")
        config = load_config()
        assert config.bootstrap_servers == "custom-broker:9092"

    def test_load_config_overrides_int(self, monkeypatch):
        monkeypatch.setenv("DASHBOARD_PORT", "8080")
        config = load_config()
        assert config.dashboard_port == 8080

    def test_load_config_overrides_float(self, monkeypatch):
        monkeypatch.setenv("WS_EMIT_INTERVAL", "5.5")
        config = load_config()
        assert config.ws_emit_interval == 5.5

    def test_load_config_overrides_multiple(self, monkeypatch):
        monkeypatch.setenv("KAFKA_GROUP_ID", "my-group")
        monkeypatch.setenv("WINDOW_SECONDS", "120")
        monkeypatch.setenv("ALERT_ERROR_RATE_WARNING", "10.0")
        config = load_config()
        assert config.group_id == "my-group"
        assert config.window_seconds == 120
        assert config.alert_error_rate_warning == 10.0

    def test_load_config_invalid_int_falls_back(self, monkeypatch):
        monkeypatch.setenv("DASHBOARD_PORT", "not-a-number")
        config = load_config()
        assert config.dashboard_port == 5000  # default
