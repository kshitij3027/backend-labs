"""Pydantic-settings loader for the chaos testing framework.

Precedence (highest wins):
    1. ``__init__`` kwargs (explicit code overrides).
    2. Environment variables prefixed ``CHAOS_`` (e.g. ``CHAOS_MAX_CONCURRENT_SCENARIOS``)
       and values read from a ``.env`` file in the working directory.
    3. Values in the YAML config file (default: ``./config/safety_config.yaml``;
       overridable with ``CHAOS_CONFIG_PATH`` or :meth:`Settings.from_yaml`).
    4. Field defaults declared on :class:`Settings` below.

Every settable field is exposed as an env var of the form ``CHAOS_<UPPER_CASE_FIELD>``:

    - ``CHAOS_MAX_CONCURRENT_SCENARIOS``           -> ``max_concurrent_scenarios``
    - ``CHAOS_CPU_EMERGENCY_THRESHOLD_PCT``        -> ``cpu_emergency_threshold_pct``
    - ``CHAOS_MEM_EMERGENCY_THRESHOLD_PCT``        -> ``mem_emergency_threshold_pct``
    - ``CHAOS_TARGET_ALLOWLIST``                   -> ``target_allowlist`` (JSON array)
    - ``CHAOS_METRICS_COLLECTION_INTERVAL_SECONDS``-> ``metrics_collection_interval_seconds``
    - ``CHAOS_METRICS_HISTORY_SIZE``               -> ``metrics_history_size``
    - ``CHAOS_RECOVERY_TEST_TIMEOUT_SECONDS``      -> ``recovery_test_timeout_seconds``
    - ``CHAOS_RECOVERY_GRACE_PERIOD_SECONDS``      -> ``recovery_grace_period_seconds``
    - ``CHAOS_DEFAULT_EXPERIMENT_DURATION``        -> ``default_experiment_duration``
    - ``CHAOS_DEFAULT_SEVERITY``                   -> ``default_severity``
    - ``CHAOS_DOCKER_SOCKET_PATH``                 -> ``docker_socket_path``
    - ``CHAOS_CHAOS_NETWORK_NAME``                 -> ``chaos_network_name``
    - ``CHAOS_DATABASE_URL``                       -> ``database_url``
    - ``CHAOS_API_HOST``                           -> ``api_host``
    - ``CHAOS_API_PORT``                           -> ``api_port``
    - ``CHAOS_LOG_LEVEL``                          -> ``log_level``

See ``config/safety_config.yaml`` for the canonical defaults used in production.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, field_validator
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import PydanticBaseSettingsSource


# Module-level override for the YAML path used by ``YamlConfigSettingsSource``.
# ``Settings.from_yaml(path)`` stashes the explicit path here so the custom
# source can pick it up when pydantic-settings constructs the instance.
# Reset back to ``None`` immediately after construction so state never leaks
# between calls.
_YAML_PATH_OVERRIDE: Path | None = None


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """Custom pydantic-settings source that loads values from a YAML file.

    Resolution order for the YAML path (first non-empty wins):
        1. ``_YAML_PATH_OVERRIDE`` module-level variable
           (set by :meth:`Settings.from_yaml` for explicit paths).
        2. ``CHAOS_CONFIG_PATH`` env var.
        3. ``./config/safety_config.yaml`` (relative to CWD).

    A missing file at the resolved path is treated as "no YAML available"
    here (empty dict). ``Settings.from_yaml`` performs its own existence
    check when an explicit path is provided so that the test contract
    (``FileNotFoundError`` for an explicitly bogus path) still holds.

    An empty YAML file is treated as an empty mapping; a non-mapping YAML
    root raises ``ValueError``.
    """

    def __init__(self, settings_cls: type[BaseSettings]) -> None:
        super().__init__(settings_cls)
        self._loaded: dict[str, Any] | None = None

    # -- Path resolution + cached load ------------------------------------ #

    def _resolve_path(self) -> Path:
        if _YAML_PATH_OVERRIDE is not None:
            return _YAML_PATH_OVERRIDE
        env_path = os.environ.get("CHAOS_CONFIG_PATH")
        if env_path:
            return Path(env_path)
        return Path("config") / "safety_config.yaml"

    def _load(self) -> dict[str, Any]:
        if self._loaded is not None:
            return self._loaded

        path = self._resolve_path()
        if not path.is_file():
            # No YAML file available -> source contributes nothing.
            # (``Settings.from_yaml`` separately enforces FileNotFoundError
            # for explicit-but-missing paths before construction.)
            self._loaded = {}
            return self._loaded

        with path.open("r", encoding="utf-8") as fh:
            loaded: Any = yaml.safe_load(fh)

        if loaded is None:
            loaded = {}
        if not isinstance(loaded, dict):
            raise ValueError(
                f"YAML root in {path!s} must be a mapping, got "
                f"{type(loaded).__name__}"
            )

        self._loaded = loaded
        return self._loaded

    # -- PydanticBaseSettingsSource interface ----------------------------- #

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> tuple[Any, str, bool]:
        """Return the value for a single field from the YAML file.

        ``(value, key, value_is_complex)`` per pydantic-settings v2.
        """
        data = self._load()
        if field_name in data:
            return data[field_name], field_name, False
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return all values this source contributes.

        We pass the entire YAML mapping through so unknown keys still trip
        ``extra='forbid'`` on the model (yielding ``ValidationError``).
        """
        return dict(self._load())


class Settings(BaseSettings):
    """Application settings backed by pydantic-settings v2.

    Precedence (highest wins): init kwargs > env (``CHAOS_*``) >
    YAML file (``YamlConfigSettingsSource``) > field defaults.
    """

    model_config = SettingsConfigDict(
        env_prefix="CHAOS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        case_sensitive=False,
    )

    # --- Concurrency + blast radius ---
    max_concurrent_scenarios: int = Field(default=3, ge=1, le=20)

    # --- Emergency thresholds ---
    cpu_emergency_threshold_pct: float = Field(default=90.0, gt=0.0, le=100.0)
    mem_emergency_threshold_pct: float = Field(default=90.0, gt=0.0, le=100.0)

    # --- Allowlist ---
    target_allowlist: list[str] = Field(
        default_factory=lambda: ["log-producer", "log-consumer", "redis"]
    )

    # --- Monitor cadence ---
    metrics_collection_interval_seconds: float = Field(default=5.0, gt=0.0, le=60.0)
    metrics_history_size: int = Field(default=1000, ge=1, le=100_000)

    # --- Recovery ---
    recovery_test_timeout_seconds: float = Field(default=30.0, gt=0.0)
    recovery_grace_period_seconds: float = Field(default=5.0, ge=0.0)

    # --- Defaults for experiments ---
    default_experiment_duration: int = Field(default=300, ge=1, le=3600)
    default_severity: int = Field(default=2, ge=1, le=5)

    # --- Infra ---
    docker_socket_path: str = Field(default="/var/run/docker.sock")
    chaos_network_name: str = Field(default="chaos-net")
    database_url: str = Field(default="sqlite+aiosqlite:////app/data/chaos.db")

    # --- API ---
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000, ge=1, le=65535)
    log_level: str = Field(default="INFO")

    # ------------------------------------------------------------------ #
    # Field validators
    # ------------------------------------------------------------------ #

    @field_validator("target_allowlist")
    @classmethod
    def _validate_target_allowlist(cls, value: list[str]) -> list[str]:
        """Reject empty lists, blank entries, and duplicates."""
        if not isinstance(value, list):
            raise ValueError("target_allowlist must be a list of strings")
        if not value:
            raise ValueError("target_allowlist must contain at least one entry")
        normalized: list[str] = []
        for entry in value:
            if not isinstance(entry, str):
                raise ValueError("target_allowlist entries must be strings")
            stripped = entry.strip()
            if not stripped:
                raise ValueError("target_allowlist entries must be non-empty strings")
            normalized.append(stripped)
        if len(set(normalized)) != len(normalized):
            raise ValueError("target_allowlist must not contain duplicate entries")
        return normalized

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, value: str) -> str:
        """Normalize to upper-case and reject anything outside the std set."""
        if not isinstance(value, str):
            raise ValueError("log_level must be a string")
        normalized = value.strip().upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if normalized not in allowed:
            raise ValueError(
                f"log_level must be one of {sorted(allowed)}, got {value!r}"
            )
        return normalized

    # ------------------------------------------------------------------ #
    # Source layering (pydantic-settings v2 idiomatic)
    # ------------------------------------------------------------------ #

    @classmethod
    def settings_customise_sources(  # noqa: PLR0913 -- signature mandated by base
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Layer sources so env (CHAOS_*) wins over YAML, and YAML over defaults.

        Order returned == priority (first = highest).
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )

    # ------------------------------------------------------------------ #
    # YAML loader
    # ------------------------------------------------------------------ #

    @classmethod
    def from_yaml(cls, path: str | Path | None = None) -> "Settings":
        """Construct a :class:`Settings` instance using ``path`` for the YAML layer.

        Behaviour:
            - If ``path`` is provided explicitly, the file must exist on disk
              (``FileNotFoundError`` otherwise). The explicit path is stashed
              in :data:`_YAML_PATH_OVERRIDE` so the custom source picks it up.
            - If ``path`` is ``None``, the source falls back to
              ``CHAOS_CONFIG_PATH`` and then ``./config/safety_config.yaml``.

        The module-level override is always reset to ``None`` after
        construction so subsequent calls (including from
        :func:`get_settings`) don't see leaked state.

        Raises:
            FileNotFoundError: ``path`` was provided explicitly but does not
                resolve to an existing file.
            pydantic.ValidationError: YAML contained an unknown key
                (``extra='forbid'``) or a value that fails field validation.
            ValueError: YAML root is not a mapping.
            yaml.YAMLError: File is not valid YAML.
        """
        global _YAML_PATH_OVERRIDE

        if path is not None:
            resolved = Path(path)
            if not resolved.is_file():
                raise FileNotFoundError(
                    f"Safety config YAML not found at {resolved!s} "
                    f"(set CHAOS_CONFIG_PATH or pass an explicit path)"
                )
            _YAML_PATH_OVERRIDE = resolved
        # If path is None we leave _YAML_PATH_OVERRIDE alone; the source
        # falls back to CHAOS_CONFIG_PATH / project default.

        try:
            return cls()
        finally:
            # Reset so the override never leaks across calls. Failures during
            # construction (e.g. ValidationError) still cleanly unwind.
            _YAML_PATH_OVERRIDE = None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Module-level singleton accessor for :class:`Settings`.

    Cached with :func:`functools.lru_cache` so repeated calls are free.
    Tests that mutate the environment before reading settings should call
    ``get_settings.cache_clear()`` first to force a fresh load.
    """
    return Settings.from_yaml()
