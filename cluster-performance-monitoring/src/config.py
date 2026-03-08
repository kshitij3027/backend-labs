"""Application configuration with environment variable and YAML file support."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from pathlib import Path

import yaml


@dataclass
class Config:
    """Cluster performance monitoring configuration.

    Values are loaded from environment variables first, then optionally
    overlaid from config/monitoring_config.yaml if the file exists.
    """

    host: str = "0.0.0.0"
    port: int = 8080
    num_nodes: int = 3
    collection_interval: float = 5.0
    retention_seconds: int = 86400
    aggregation_window: int = 300

    # Threshold defaults
    cpu_warning: float = 70
    cpu_critical: float = 90
    memory_warning: float = 80
    memory_critical: float = 95
    latency_warning: float = 100
    latency_critical: float = 500

    dashboard_refresh: int = 10

    # Mapping from environment variable names to field names
    _ENV_MAP: dict[str, str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        # _ENV_MAP is a class-level helper, not a real config field
        pass

    @classmethod
    def _env_map(cls) -> dict[str, str]:
        """Return mapping of ENV_VAR -> field_name."""
        return {
            "HOST": "host",
            "PORT": "port",
            "NUM_NODES": "num_nodes",
            "COLLECTION_INTERVAL": "collection_interval",
            "RETENTION_SECONDS": "retention_seconds",
            "AGGREGATION_WINDOW": "aggregation_window",
            "CPU_WARNING": "cpu_warning",
            "CPU_CRITICAL": "cpu_critical",
            "MEMORY_WARNING": "memory_warning",
            "MEMORY_CRITICAL": "memory_critical",
            "LATENCY_WARNING": "latency_warning",
            "LATENCY_CRITICAL": "latency_critical",
            "DASHBOARD_REFRESH": "dashboard_refresh",
        }

    @classmethod
    def _field_types(cls) -> dict[str, type]:
        """Return mapping of field_name -> type for casting."""
        return {f.name: f.type for f in fields(cls) if f.name != "_ENV_MAP"}

    @classmethod
    def _cast(cls, field_name: str, value: str) -> object:
        """Cast a string value to the correct type for the given field."""
        field_type_map: dict[str, type] = {
            "host": str,
            "port": int,
            "num_nodes": int,
            "collection_interval": float,
            "retention_seconds": int,
            "aggregation_window": int,
            "cpu_warning": float,
            "cpu_critical": float,
            "memory_warning": float,
            "memory_critical": float,
            "latency_warning": float,
            "latency_critical": float,
            "dashboard_refresh": int,
        }
        caster = field_type_map.get(field_name, str)
        return caster(value)

    @classmethod
    def load(cls) -> Config:
        """Load configuration from environment variables and optional YAML file.

        Environment variables take precedence over defaults.  If the file
        ``config/monitoring_config.yaml`` exists, its values are applied on
        top of the defaults but *under* any explicitly-set env vars.
        """
        # Start with defaults
        kwargs: dict[str, object] = {}

        # Read env vars
        env_map = cls._env_map()
        env_overrides: dict[str, object] = {}
        for env_key, field_name in env_map.items():
            env_val = os.environ.get(env_key)
            if env_val is not None:
                env_overrides[field_name] = cls._cast(field_name, env_val)

        # Optionally overlay from YAML
        yaml_overrides: dict[str, object] = {}
        yaml_path = Path("config/monitoring_config.yaml")
        if yaml_path.exists():
            yaml_overrides = cls._parse_yaml(yaml_path)

        # Merge: defaults < yaml < env
        kwargs.update(yaml_overrides)
        kwargs.update(env_overrides)

        return cls(**kwargs)  # type: ignore[arg-type]

    @classmethod
    def _parse_yaml(cls, path: Path) -> dict[str, object]:
        """Parse the YAML config file and return a flat dict of field values."""
        with open(path) as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            return {}

        result: dict[str, object] = {}

        # monitoring section
        monitoring = data.get("monitoring", {})
        if monitoring:
            if "collection_interval" in monitoring:
                result["collection_interval"] = float(monitoring["collection_interval"])
            if "retention_period" in monitoring:
                result["retention_seconds"] = int(monitoring["retention_period"])
            if "aggregation_window" in monitoring:
                result["aggregation_window"] = int(monitoring["aggregation_window"])

        # cluster section
        cluster = data.get("cluster", {})
        if cluster:
            nodes = cluster.get("nodes", [])
            if nodes:
                result["num_nodes"] = len(nodes)

        # metrics thresholds
        metrics = data.get("metrics", {})
        thresholds = metrics.get("thresholds", {})
        if thresholds:
            for key in (
                "cpu_warning",
                "cpu_critical",
                "memory_warning",
                "memory_critical",
                "latency_warning",
                "latency_critical",
            ):
                if key in thresholds:
                    result[key] = float(thresholds[key])

        # dashboard section
        dashboard = data.get("dashboard", {})
        if dashboard:
            if "host" in dashboard:
                result["host"] = str(dashboard["host"])
            if "port" in dashboard:
                result["port"] = int(dashboard["port"])
            if "refresh_interval" in dashboard:
                result["dashboard_refresh"] = int(dashboard["refresh_interval"])

        return result
