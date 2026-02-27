"""Tests for the metadata collector subsystem."""

import platform
import socket

import pytest

from src.collectors import CollectorRegistry, create_default_registry
from src.collectors.base import MetadataCollector
from src.collectors.environment import EnvironmentCollector
from src.collectors.performance import PerformanceCollector
from src.collectors.system_info import SystemInfoCollector
from src.config import AppConfig


# ---------------------------------------------------------------------------
# SystemInfoCollector
# ---------------------------------------------------------------------------


class TestSystemInfoCollector:
    """Tests for SystemInfoCollector."""

    def test_collect_returns_expected_keys(self):
        collector = SystemInfoCollector()
        result = collector.collect()

        assert "hostname" in result
        assert "os_info" in result
        assert "python_version" in result

    def test_collect_values_match_platform(self):
        collector = SystemInfoCollector()
        result = collector.collect()

        assert result["hostname"] == socket.gethostname()
        assert result["os_info"] == f"{platform.system()} {platform.release()}"
        assert result["python_version"] == platform.python_version()

    def test_collect_caches_result(self):
        collector = SystemInfoCollector()
        first = collector.collect()
        second = collector.collect()

        # Same object reference means caching is working.
        assert first is second

    def test_name_property(self):
        assert SystemInfoCollector().name == "system_info"


# ---------------------------------------------------------------------------
# EnvironmentCollector
# ---------------------------------------------------------------------------


class TestEnvironmentCollector:
    """Tests for EnvironmentCollector."""

    def test_collect_returns_config_values(self, sample_config):
        collector = EnvironmentCollector(sample_config)
        result = collector.collect()

        assert result["service_name"] == sample_config.service_name
        assert result["environment"] == sample_config.environment
        assert result["version"] == sample_config.version
        assert result["region"] == sample_config.region

    def test_collect_returns_expected_keys(self, sample_config):
        collector = EnvironmentCollector(sample_config)
        result = collector.collect()

        assert set(result.keys()) == {"service_name", "environment", "version", "region"}

    def test_collect_caches_result(self, sample_config):
        collector = EnvironmentCollector(sample_config)
        first = collector.collect()
        second = collector.collect()

        assert first is second

    def test_name_property(self, sample_config):
        assert EnvironmentCollector(sample_config).name == "environment"

    def test_custom_config_values(self):
        config = AppConfig(
            service_name="custom-svc",
            environment="production",
            version="2.0.0",
            region="us-east-1",
        )
        collector = EnvironmentCollector(config)
        result = collector.collect()

        assert result["service_name"] == "custom-svc"
        assert result["environment"] == "production"
        assert result["version"] == "2.0.0"
        assert result["region"] == "us-east-1"


# ---------------------------------------------------------------------------
# PerformanceCollector
# ---------------------------------------------------------------------------


class TestPerformanceCollector:
    """Tests for PerformanceCollector."""

    def test_collect_returns_expected_keys(self):
        collector = PerformanceCollector()
        result = collector.collect()

        assert "cpu_percent" in result
        assert "memory_percent" in result
        assert "disk_percent" in result

    def test_collect_values_are_floats(self):
        collector = PerformanceCollector()
        result = collector.collect()

        assert isinstance(result["cpu_percent"], float)
        assert isinstance(result["memory_percent"], float)
        assert isinstance(result["disk_percent"], float)

    def test_collect_caches_within_ttl(self):
        collector = PerformanceCollector(cache_ttl=60.0)
        first = collector.collect()
        second = collector.collect()

        # Within TTL the exact same dict object should be returned.
        assert first is second

    def test_name_property(self):
        assert PerformanceCollector().name == "performance"


# ---------------------------------------------------------------------------
# CollectorRegistry
# ---------------------------------------------------------------------------


class TestCollectorRegistry:
    """Tests for CollectorRegistry."""

    def test_register_and_get(self):
        registry = CollectorRegistry()
        collector = SystemInfoCollector()
        registry.register(collector)

        assert registry.get("system_info") is collector

    def test_get_unknown_returns_none(self):
        registry = CollectorRegistry()
        assert registry.get("nonexistent") is None

    def test_collect_from_valid_names(self, sample_config):
        registry = CollectorRegistry()
        registry.register(SystemInfoCollector())
        registry.register(EnvironmentCollector(sample_config))

        merged, errors = registry.collect_from(["system_info", "environment"])

        assert errors == []
        # System info keys
        assert "hostname" in merged
        assert "os_info" in merged
        assert "python_version" in merged
        # Environment keys
        assert "service_name" in merged
        assert "environment" in merged
        assert "version" in merged
        assert "region" in merged

    def test_collect_from_unknown_name_returns_error(self):
        registry = CollectorRegistry()
        merged, errors = registry.collect_from(["missing_collector"])

        assert len(errors) == 1
        assert "missing_collector" in errors[0]
        assert merged == {}

    def test_collect_from_handles_collector_exception(self):
        """A collector that raises should produce an error, not crash."""

        class BrokenCollector(MetadataCollector):
            @property
            def name(self) -> str:
                return "broken"

            def collect(self):
                raise RuntimeError("boom")

        registry = CollectorRegistry()
        registry.register(BrokenCollector())

        merged, errors = registry.collect_from(["broken"])

        assert len(errors) == 1
        assert "boom" in errors[0]
        assert merged == {}

    def test_list_collectors(self, sample_config):
        registry = CollectorRegistry()
        registry.register(SystemInfoCollector())
        registry.register(EnvironmentCollector(sample_config))

        names = registry.list_collectors()
        assert set(names) == {"system_info", "environment"}


# ---------------------------------------------------------------------------
# create_default_registry factory
# ---------------------------------------------------------------------------


class TestCreateDefaultRegistry:
    """Tests for the create_default_registry factory function."""

    def test_returns_registry_with_three_collectors(self, sample_config):
        registry = create_default_registry(sample_config)
        assert len(registry.list_collectors()) == 3
        assert set(registry.list_collectors()) == {
            "system_info",
            "environment",
            "performance",
        }

    def test_collect_all_returns_seven_plus_fields(self, sample_config):
        registry = create_default_registry(sample_config)
        names = registry.list_collectors()
        merged, errors = registry.collect_from(names)

        assert errors == []
        # At minimum 7 unique keys from system_info (3) + environment (4).
        expected_keys = {
            "hostname",
            "os_info",
            "python_version",
            "service_name",
            "environment",
            "version",
            "region",
        }
        assert expected_keys.issubset(set(merged.keys()))
        # Performance adds 3 more.
        assert "cpu_percent" in merged
        assert "memory_percent" in merged
        assert "disk_percent" in merged
