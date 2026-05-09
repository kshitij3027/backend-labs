from src.config import Settings


def test_defaults_match_spec(monkeypatch):
    for env_var in (
        "EWMA_ALPHA",
        "AIMD_BETA",
        "MIN_DWELL_SECONDS",
        "UP_NORMAL_TO_PRESSURE",
        "DOWN_OVERLOAD_TO_PRESSURE",
        "CRITICAL_QUEUE_MAX",
        "WORKER_COUNT",
    ):
        monkeypatch.delenv(env_var, raising=False)

    settings = Settings()

    assert settings.ewma_alpha == 0.3
    assert settings.aimd_beta == 0.7
    assert settings.min_dwell_seconds == 3.0
    assert settings.up_normal_to_pressure == 0.7
    assert settings.down_overload_to_pressure == 0.75
    assert settings.critical_queue_max == 1000
    assert settings.worker_count == 4


def test_env_override(monkeypatch):
    monkeypatch.setenv("EWMA_ALPHA", "0.5")
    monkeypatch.setenv("WORKER_COUNT", "8")

    settings = Settings()

    assert settings.ewma_alpha == 0.5
    assert settings.worker_count == 8


def test_log_level_override(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    settings = Settings()

    assert settings.log_level == "DEBUG"
