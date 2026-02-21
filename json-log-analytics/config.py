import yaml
import copy


class Config:
    """Configuration manager that loads from YAML and merges with defaults."""

    DEFAULTS = {
        "server": {
            "host": "0.0.0.0",
            "port": 5000,
            "debug": False,
        },
        "storage": {
            "max_logs": 1000,
        },
        "analytics": {
            "time_bucket_minutes": 1,
            "max_buckets": 60,
        },
        "alerting": {
            "error_rate_threshold": 0.10,
            "high_volume_threshold": 100,
            "service_down_minutes": 2,
            "cooldown_seconds": 300,
        },
        "schema": {
            "path": "schemas/log_schema.json",
        },
    }

    def __init__(self, config_path=None):
        self._config = copy.deepcopy(self.DEFAULTS)

        if config_path is not None:
            try:
                with open(config_path, "r") as f:
                    user_config = yaml.safe_load(f)

                if user_config and isinstance(user_config, dict):
                    self._config = self._deep_merge(self._config, user_config)
            except FileNotFoundError:
                pass
            except yaml.YAMLError:
                print(f"Warning: Invalid YAML in {config_path}, using defaults")

    @staticmethod
    def _deep_merge(base, override):
        """Recursively merge override dict into base dict."""
        result = copy.deepcopy(base)
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = Config._deep_merge(result[key], value)
            else:
                result[key] = copy.deepcopy(value)
        return result

    def get(self, key, default=None):
        """Get a top-level config key."""
        return self._config.get(key, default)

    def __getitem__(self, key):
        return self._config[key]

    def __contains__(self, key):
        return key in self._config
