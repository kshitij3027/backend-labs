"""Tests for ``src.config.Settings``.

Covers: (1) that every default matches the env-var table in
``project_requirements.md`` section 7, (2) that env-var overrides
actually land on the loaded Settings, and (3) that env parsing is
case-insensitive so producers using the lowercase form still work.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clear_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear every Settings-managed env var so defaults are observable.

    pydantic-settings reads the process environment at Settings()
    construction time, so a stray export in the dev shell (or CI job)
    would otherwise mask the built-in default. Scrub them here.
    """
    for name in (
        "BATCH_TIMEOUT_MS",
        "SEGMENT_MAX_DOCS",
        "SEGMENT_MAX_MEMORY_MB",
        "MAX_MEMORY_SEGMENTS",
        "REDIS_URL",
        "REDIS_STREAM_NAME",
        "REDIS_CONSUMER_GROUP",
        "HTTP_PORT",
        "DISK_SEGMENT_DIR",
        "BACKGROUND_MERGE_INTERVAL_S",
        "REDIS_RECONNECT_BACKOFF_BASE_S",
        "REDIS_RECONNECT_BACKOFF_MAX_S",
        "SAMPLE_GENERATION_BATCH_SIZE",
        "LOG_LEVEL",
        "WS_HEARTBEAT_INTERVAL_S",
    ):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.lower(), raising=False)


def test_defaults_match_requirements() -> None:
    """Every default must match the env-var table in the requirements.

    If this breaks it means the spec moved or config.py drifted —
    both worth a PR review, not a silent pass.
    """
    from src.config import Settings

    s = Settings()

    assert s.batch_timeout_ms == 100
    assert s.segment_max_docs == 10_000
    assert s.segment_max_memory_mb == 50
    assert s.max_memory_segments == 5
    assert s.redis_url == "redis://localhost:6379"
    assert s.redis_stream_name == "logs"
    assert s.redis_consumer_group == "indexer"
    assert s.http_port == 8080
    assert s.disk_segment_dir == "./data/segments"
    assert s.background_merge_interval_s == 30
    assert s.redis_reconnect_backoff_base_s == pytest.approx(0.5)
    assert s.redis_reconnect_backoff_max_s == pytest.approx(30.0)
    assert s.sample_generation_batch_size == 500
    assert s.log_level == "INFO"
    assert s.ws_heartbeat_interval_s == 20


def test_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Setting ``BATCH_TIMEOUT_MS`` via env changes the loaded value."""
    from src.config import Settings

    monkeypatch.setenv("BATCH_TIMEOUT_MS", "250")

    s = Settings()

    assert s.batch_timeout_ms == 250


def test_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Lowercase env names still reach their target field.

    ``case_sensitive=False`` in the Settings config should make the
    two spellings equivalent. Producers upstream sometimes write
    ``batch_timeout_ms=150`` by mistake; that should still work.
    """
    from src.config import Settings

    monkeypatch.setenv("batch_timeout_ms", "150")

    s = Settings()

    assert s.batch_timeout_ms == 150
