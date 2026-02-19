"""Tests for config module."""

import os
import pytest
from src.config import ServerConfig, ClientConfig, load_server_config, load_client_config, _parse_bool


class TestParseHelper:
    def test_parse_bool_true_values(self):
        assert _parse_bool("true") is True
        assert _parse_bool("1") is True
        assert _parse_bool("yes") is True
        assert _parse_bool("TRUE") is True

    def test_parse_bool_false_values(self):
        assert _parse_bool("false") is False
        assert _parse_bool("0") is False
        assert _parse_bool("no") is False


class TestServerConfig:
    def test_defaults(self):
        config = ServerConfig()
        assert config.host == "0.0.0.0"
        assert config.port == 8443
        assert config.cert_file == "/app/certs/server.crt"
        assert config.key_file == "/app/certs/server.key"

    def test_frozen(self):
        config = ServerConfig()
        with pytest.raises(AttributeError):
            config.port = 9999

    def test_load_from_env(self, monkeypatch):
        monkeypatch.setenv("SERVER_HOST", "127.0.0.1")
        monkeypatch.setenv("SERVER_PORT", "9443")
        config = load_server_config()
        assert config.host == "127.0.0.1"
        assert config.port == 9443


class TestClientConfig:
    def test_defaults(self):
        config = ClientConfig()
        assert config.host == "tls-server"
        assert config.port == 8443
        assert config.verify_certs is False

    def test_load_from_env(self, monkeypatch):
        monkeypatch.setenv("SERVER_HOST", "localhost")
        monkeypatch.setenv("VERIFY_CERTS", "true")
        config = load_client_config()
        assert config.host == "localhost"
        assert config.verify_certs is True
