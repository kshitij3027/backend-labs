"""Runtime configuration for the bloom-filter log membership service.

Every field is overridable via an environment variable of the same name
(UPPER_SNAKE, case-insensitive) or a project-root ``.env`` file. ``.env.example``
documents the full set with defaults.

The class is deliberately flat and easy to extend: later commits append the
two-tier pipeline thresholds (C10) and the ``sessions`` filter sizing (C11) —
all as plain defaulted fields here, so one ``get_settings()`` call stays the
single source of configuration truth.
"""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide service configuration, resolved once via :func:`get_settings`."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- API / server ---
    api_host: str = "0.0.0.0"
    """Interface uvicorn binds to. 0.0.0.0 so the port is reachable in Docker."""

    api_port: int = 8001
    """TCP port of the membership API (the dashboard process later takes 8002)."""

    # --- storage ---
    data_dir: str = "./data"
    """Directory holding persisted filter snapshots (``*.bloom``).

    Compose sets this to ``/app/data`` and bind-mounts ``./data`` over it so
    filters survive container restarts; tests point it at a tmp dir.
    """

    # --- per-log-type filter sizing (spec defaults) ---
    error_logs_capacity: int = 1_000_000
    """Expected distinct error-log keys — slice-0 capacity of that filter."""

    error_logs_fp_rate: float = 0.01
    """Target compound false-positive rate for the ``error_logs`` filter."""

    access_logs_capacity: int = 5_000_000
    """Expected distinct access-log keys — slice-0 capacity of that filter."""

    access_logs_fp_rate: float = 0.05
    """Target compound false-positive rate for the ``access_logs`` filter."""

    security_logs_capacity: int = 100_000
    """Expected distinct security-log keys — slice-0 capacity of that filter."""

    security_logs_fp_rate: float = 0.001
    """Target compound false-positive rate for the ``security_logs`` filter."""

    # --- scalable filter growth (Extended B: adaptive sizing) ---
    sbf_growth_factor: int = 2
    """Capacity multiplier between consecutive SBF slices (paper's s, >= 2)."""

    sbf_tightening_ratio: float = 0.85
    """FP-budget ratio between consecutive SBF slices (paper's r, in (0, 1))."""

    # --- snapshots ---
    snapshot_interval_seconds: float = 30.0
    """Cadence of the background ``save_all()`` snapshot task (C8)."""

    # --- rotation (Extended B: time-based rotation) ---
    rotation_max_age_seconds: float = 86_400.0
    """Rotate a filter generation once it is this old. ``0`` disables rotation."""

    rotation_check_interval_seconds: float = 60.0
    """How often the background rotation task (C8) calls ``rotate_if_due()``."""

    # --- logging ---
    log_level: str = "INFO"
    """Stdlib logging level name (DEBUG / INFO / WARNING / ERROR)."""

    def filter_configs(self) -> dict[str, tuple[int, float]]:
        """Return ``{filter_name: (capacity, target_fp_rate)}`` for every filter.

        The single place the :class:`~src.manager.FilterManager` reads its
        routing table from — filter names, sizing, and FP targets all come
        from here, so adding a filter (C11 adds ``sessions``) is one new
        entry plus its two fields, with no manager changes.
        """
        return {
            "error_logs": (self.error_logs_capacity, self.error_logs_fp_rate),
            "access_logs": (self.access_logs_capacity, self.access_logs_fp_rate),
            "security_logs": (
                self.security_logs_capacity,
                self.security_logs_fp_rate,
            ),
        }


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance.

    The LRU cache makes this a cheap singleton accessor for request handlers
    and background tasks. Tests override env vars and call
    ``get_settings.cache_clear()`` to force a rebuild.
    """
    return Settings()
