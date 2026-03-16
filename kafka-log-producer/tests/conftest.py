"""Shared fixtures for kafka-log-producer tests."""

import textwrap
from pathlib import Path

import pytest

from src.config import Config
from src.log_generator import LogGenerator
from src.models import LogEntry, LogLevel


@pytest.fixture
def sample_log_entry() -> LogEntry:
    """Return a deterministic LogEntry useful for assertion-heavy tests."""
    return LogEntry(
        level=LogLevel.INFO,
        message="Test log message",
        service="test-service",
        component="handler",
        trace_id="abc123def456",
        user_id="user-42",
        session_id="sess-999999",
    )


@pytest.fixture
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Config:
    """Create a Config backed by a temporary YAML file.

    Clears env vars that would override YAML defaults so assertions
    against known values work regardless of the Docker environment.
    """
    for env_var in (
        "BOOTSTRAP_SERVERS", "KAFKA_ACKS", "KAFKA_BATCH_SIZE",
        "KAFKA_LINGER_MS", "KAFKA_COMPRESSION", "PROMETHEUS_PORT",
        "DASHBOARD_PORT",
    ):
        monkeypatch.delenv(env_var, raising=False)

    yaml_content = textwrap.dedent("""\
        kafka:
          bootstrap_servers: "localhost:9092"
          acks: "all"
          retries: 2147483647
          batch_size: 16384
          linger_ms: 5
          compression_type: "gzip"
          enable_idempotence: true

        prometheus:
          port: 8000

        dashboard:
          port: 8080
          ws_interval: 2

        fallback:
          storage_path: "/tmp/kafka_fallback.jsonl"
    """)
    config_file = tmp_path / "producer_config.yaml"
    config_file.write_text(yaml_content)
    return Config(config_path=str(config_file))


@pytest.fixture
def log_generator() -> LogGenerator:
    """Return a fresh LogGenerator instance."""
    return LogGenerator()
