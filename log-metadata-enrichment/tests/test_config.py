"""Tests for configuration loading: AppConfig, load_config, load_rules."""

import os

from src.config import AppConfig, load_config, load_rules


class TestAppConfig:
    """Tests for AppConfig dataclass defaults."""

    def test_default_values(self):
        config = AppConfig()
        assert config.service_name == "log-enrichment"
        assert config.environment == "development"
        assert config.version == "1.0.0"
        assert config.region == "local"
        assert config.host == "0.0.0.0"
        assert config.port == 8080
        assert config.debug is False
        assert config.rules_path == "config/enrichment_rules.yaml"


class TestLoadConfig:
    """Tests for load_config() function."""

    def test_returns_appconfig_with_defaults(self):
        config = load_config()
        assert isinstance(config, AppConfig)
        assert config.service_name == "log-enrichment"

    def test_reads_env_vars_when_set(self, monkeypatch, tmp_path):
        # Create a temporary .env file with overrides
        env_file = tmp_path / ".env"
        env_file.write_text(
            "SERVICE_NAME=custom-service\n"
            "ENVIRONMENT=production\n"
            "VERSION=2.0.0\n"
            "REGION=us-west-2\n"
            "HOST=127.0.0.1\n"
            "PORT=9090\n"
            "DEBUG=true\n"
            "RULES_PATH=custom/rules.yaml\n"
        )
        # Change to the tmp dir so dotenv_values picks up .env
        monkeypatch.chdir(tmp_path)

        config = load_config()
        assert config.service_name == "custom-service"
        assert config.environment == "production"
        assert config.version == "2.0.0"
        assert config.region == "us-west-2"
        assert config.host == "127.0.0.1"
        assert config.port == 9090
        assert config.debug is True
        assert config.rules_path == "custom/rules.yaml"


class TestLoadRules:
    """Tests for load_rules() function."""

    def test_valid_yaml_parses_correctly(self):
        rules = load_rules("config/enrichment_rules.yaml")
        assert "rules" in rules
        assert len(rules["rules"]) == 3
        assert rules["rules"][0]["name"] == "critical_errors"
        assert rules["rules"][1]["name"] == "warnings"
        assert rules["rules"][2]["name"] == "default"

    def test_missing_file_returns_empty_dict(self):
        rules = load_rules("nonexistent/path/rules.yaml")
        assert rules == {}
