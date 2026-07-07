"""Application configuration for the Correlation Analysis System.

Configuration precedence (lowest to highest):

    field defaults  ->  .env file (optional)  ->  environment variables

Defaults live on the :class:`Settings` model (pydantic-settings v2 ``BaseSettings``).
This is the standard pydantic-settings source order (environment beats dotenv beats
defaults), so no source customization is needed. Environment variable names are the
upper-cased field names (pydantic-settings default), e.g. ``window_seconds`` <-
``WINDOW_SECONDS``.

Use :func:`get_settings` (LRU-cached) at call sites so the config is parsed once per
process; tests that monkeypatch the environment clear the cache via
``get_settings.cache_clear()``.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Flat application settings sourced from defaults, optional .env, then environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- Infrastructure ---
    redis_url: str = "redis://localhost:6379/0"
    log_level: str = "INFO"

    # --- Pipeline cadence / buffering ---
    #: Sliding correlation window: detectors only consider events/samples from the
    #: last N seconds.
    window_seconds: int = 30
    #: Pipeline tick — generate/parse/buffer/aggregate every N seconds.
    generation_interval_seconds: float = 1.0
    #: Detection tick — run the correlation detectors every N seconds.
    detection_interval_seconds: float = 2.0
    #: Target synthetic log volume produced by the generator. 135 rather than a
    #: round 120: realized ingest measures ~0.9x this target (journeys emit ~6
    #: lines in practice against the /7 spawn sizing), so 135 keeps the
    #: sustained measured rate (~119 eps) comfortably above the E2E/load
    #: throughput gate of 100 events/sec.
    events_per_second: int = 135
    #: Max parsed events held in the in-memory buffer (collections.deque maxlen).
    event_buffer_size: int = 5000
    #: Master switch for the background pipeline task (the compose `test` service
    #: sets PIPELINE_ENABLED=false so nothing generates in the background).
    pipeline_enabled: bool = True

    # --- Statistical detection ---
    #: Minimum paired samples before a metric correlation is even attempted.
    min_samples: int = 10
    #: Benjamini-Hochberg false-discovery-rate level applied across each detection
    #: cycle's p-values.
    fdr_q: float = 0.05
    #: Max spacing between ordered ERROR events that still chains into one cascade.
    cascade_window_seconds: int = 10
    #: TTL of the emitted-correlation dedupe cache (suppresses re-emission of the
    #: same pair within the window).
    dedup_ttl_seconds: int = 30

    # --- Alerting ---
    #: Minimum correlation strength before the generic alert rule fires.
    alert_strength_threshold: float = 0.8
    #: Minimum correlation confidence before the generic alert rule fires.
    alert_confidence_threshold: float = 0.6
    #: Per-(rule, pair) cooldown so a persistent condition alerts once per minute.
    alert_cooldown_seconds: int = 60

    # --- Incident scenario injection (generator) ---
    #: A new incident scenario starts every N seconds (rotating through scenarios)...
    scenario_period_seconds: int = 45
    #: ...and each scenario stays active for N seconds.
    scenario_duration_seconds: int = 20


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance."""
    return Settings()
