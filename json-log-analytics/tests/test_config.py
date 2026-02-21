import os
import tempfile

import yaml

from config import Config


class TestConfig:
    def test_default_config(self, config):
        """Verify defaults are loaded when no file is given."""
        assert config["server"]["host"] == "0.0.0.0"
        assert config["server"]["port"] == 5000
        assert config["server"]["debug"] is False
        assert config["storage"]["max_logs"] == 1000
        assert config["analytics"]["time_bucket_minutes"] == 1
        assert config["analytics"]["max_buckets"] == 60
        assert config["alerting"]["error_rate_threshold"] == 0.10
        assert config["alerting"]["high_volume_threshold"] == 100
        assert config["alerting"]["service_down_minutes"] == 2
        assert config["alerting"]["cooldown_seconds"] == 300
        assert config["schema"]["path"] == "schemas/log_schema.json"

    def test_load_from_yaml(self):
        """Write a temp YAML with overrides, verify merge."""
        override = {
            "server": {"port": 8080, "debug": True},
            "storage": {"max_logs": 5000},
        }
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump(override, f)
            temp_path = f.name

        try:
            cfg = Config(temp_path)
            assert cfg["server"]["port"] == 8080
            assert cfg["server"]["debug"] is True
            assert cfg["server"]["host"] == "0.0.0.0"  # default preserved
            assert cfg["storage"]["max_logs"] == 5000
            assert cfg["analytics"]["time_bucket_minutes"] == 1  # default preserved
        finally:
            os.unlink(temp_path)

    def test_missing_file_uses_defaults(self):
        """Pass nonexistent path, verify defaults are used."""
        cfg = Config("/nonexistent/path/config.yaml")
        assert cfg["server"]["port"] == 5000
        assert cfg["storage"]["max_logs"] == 1000

    def test_deep_merge(self):
        """Verify nested override works (e.g., override only server.port)."""
        base = {"server": {"host": "localhost", "port": 5000, "debug": False}}
        override = {"server": {"port": 9090}}
        result = Config._deep_merge(base, override)
        assert result["server"]["port"] == 9090
        assert result["server"]["host"] == "localhost"
        assert result["server"]["debug"] is False

    def test_get_method(self, config):
        """Test get() with existing and missing keys."""
        server = config.get("server")
        assert server is not None
        assert server["port"] == 5000

        missing = config.get("nonexistent")
        assert missing is None

        default_val = config.get("nonexistent", "fallback")
        assert default_val == "fallback"

    def test_contains(self, config):
        """Test the 'in' operator."""
        assert "server" in config
        assert "storage" in config
        assert "nonexistent" not in config
