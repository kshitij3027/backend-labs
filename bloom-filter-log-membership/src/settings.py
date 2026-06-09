"""Runtime configuration for the bloom-filter log membership service.

Every field is overridable via an environment variable of the same name
(UPPER_SNAKE, case-insensitive) or a project-root ``.env`` file. ``.env.example``
documents the full set with defaults.

The class is deliberately flat and easy to extend: C10 appended the
two-tier pipeline thresholds and C11 the ``sessions`` filter sizing — all as
plain defaulted fields here, so one ``get_settings()`` call stays the single
source of configuration truth.
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

    # --- dashboard process (C12) ---
    dashboard_host: str = "0.0.0.0"
    """Interface the SEPARATE dashboard process (``src.dashboard``) binds to."""

    dashboard_port: int = 8002
    """TCP port of the dashboard web UI — a different process than the API,
    so dashboard page loads and WebSocket fan-out never compete with the hot
    ``/logs/add`` / ``/logs/query`` path for the API's event loop."""

    api_base_url: str = "http://localhost:8001"
    """Where the dashboard process reaches the membership API over HTTP.

    The dashboard NEVER imports the manager or filters — process separation
    is the point — so this URL is its only window into the service. Compose
    sets it to ``http://app:8001`` (service-name DNS); the localhost default
    suits running both processes by hand on one machine.
    """

    dashboard_refresh_ms: int = 5000
    """Cadence (milliseconds) of the dashboard's poll-and-broadcast tick.

    Every interval the dashboard fetches ``/stats`` + ``/pipeline/stats`` +
    ``/sessions/stats`` from the API and pushes one tick to every connected
    WebSocket client. Tests park it at 100 ms to observe periodic ticks fast.
    """

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

    # --- session tracking filter (Extended A, C11) ---
    sessions_capacity: int = 1_000_000
    """Expected distinct session IDs per generation — ``sessions`` slice-0 capacity.

    Sized for Extended A's headline workload: 1M session-ID inserts *per
    day*. "Daily" is carried by rotation, not by this number alone — the
    default ``rotation_max_age_seconds`` (86 400 s) rotates every filter,
    sessions included, once a day, so one generation only ever absorbs ~one
    day's worth of inserts and this capacity is the honest per-generation
    bound. The <2MB success criterion is judged at exactly this sizing: at
    ``sessions_fp_rate`` 0.01 the SBF grants slice 0 the tightened budget
    ``0.01 × (1 − 0.85) = 0.0015``, giving ``m ≈ 13.53 Mbit ≈ 1.69 MB`` —
    comfortably under the 2 MB line (``/sessions/stats`` reports the live
    ``memory_under_2mb`` verdict).
    """

    sessions_fp_rate: float = 0.01
    """Target compound false-positive rate for the ``sessions`` filter.

    0.01 keeps the 1M-capacity slice 0 at ~1.69 MB (see
    ``sessions_capacity``); tightening it to 0.001 would push slice 0 to
    ~2.5 MB and bust the Extended-A <2MB memory criterion.
    """

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

    # --- two-tier pipeline (Extended C, C10) ---
    sqlite_path: str = "./data/logs.db"
    """Path of the sqlite database playing the "expensive storage" tier.

    Ground truth for the ``/pipeline`` endpoints: every ingested key lands
    here as a row, and bloom positives are verified against it. Defaults
    under ``data_dir`` so compose's ``./data`` bind mount persists it next to
    the filter snapshots; tests point it at a tmp file.
    """

    fp_fallback_threshold: float = 0.05
    """Live-FP-estimate breach point for the pipeline's bloom bypass.

    When a filter's CURRENT generation reports a ``compound_estimated_fp``
    above this value, ``/pipeline/lookup`` stops consulting the bloom filter
    and goes straight to storage (a saturated filter rarely answers "no", so
    its storage savings have evaporated — see :mod:`src.pipeline`).
    Meaningful values are strictly between 0 and 1: at or above 1.0 the
    estimate can never breach (fallback effectively disabled), while 0.0
    forces permanent fallback after the very first insert.
    """

    fp_rotate_on_breach: bool = True
    """Rotate a filter once per FP-threshold breach episode (C10).

    On the first fallback lookup of a breach, the pipeline calls
    ``FilterManager.rotate`` to install a fresh current generation and
    restore filter health. The trigger re-arms only after the estimate drops
    back under ``fp_fallback_threshold``, so a sustained breach can never
    cause a rotation storm.
    """

    # --- logging ---
    log_level: str = "INFO"
    """Stdlib logging level name (DEBUG / INFO / WARNING / ERROR)."""

    def filter_configs(self) -> dict[str, tuple[int, float]]:
        """Return ``{filter_name: (capacity, target_fp_rate)}`` for every filter.

        The single place the :class:`~src.manager.FilterManager` reads its
        routing table from — filter names, sizing, and FP targets all come
        from here, so adding a filter is one new entry plus its two fields,
        with no manager changes. Exactly how C11 added ``sessions``: the
        manager, pipeline counters, snapshots, and rotation all picked the
        fourth filter up from this dict automatically. The ``/logs/*`` and
        ``/demo/*`` endpoints deliberately did NOT — their ``log_type``
        universe is the API-level ``LogType`` Literal, so sessions traffic
        only flows through the dedicated ``/sessions/*`` endpoints.
        """
        return {
            "error_logs": (self.error_logs_capacity, self.error_logs_fp_rate),
            "access_logs": (self.access_logs_capacity, self.access_logs_fp_rate),
            "security_logs": (
                self.security_logs_capacity,
                self.security_logs_fp_rate,
            ),
            "sessions": (self.sessions_capacity, self.sessions_fp_rate),
        }


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance.

    The LRU cache makes this a cheap singleton accessor for request handlers
    and background tasks. Tests override env vars and call
    ``get_settings.cache_clear()`` to force a rebuild.
    """
    return Settings()
