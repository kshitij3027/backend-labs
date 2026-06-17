"""Application configuration for the Adaptive Resource Allocation System.

Configuration precedence (lowest to highest):

    defaults  ->  YAML file  ->  environment variables

Defaults live on the :class:`Settings` dataclass. A YAML file (``config/config.yaml``
by default, overridable via the ``config_path`` argument or the ``CONFIG_PATH``
environment variable) may override any field. Finally, environment variables win
over everything else.

The loader is intentionally defensive: a missing or malformed YAML file must not
crash the application — it simply falls back to defaults plus any env overrides.
"""

import os
from dataclasses import dataclass, fields
from typing import Any, Callable

try:
    import yaml
except ImportError:  # pragma: no cover - yaml is a hard dependency, guard anyway
    yaml = None


# Default location for the YAML config, relative to the process working directory.
DEFAULT_CONFIG_PATH = "config/config.yaml"


@dataclass
class Settings:
    """Central, flat configuration for the autoscaler and its dashboard."""

    # Server / dashboard
    host: str = "0.0.0.0"
    port: int = 8080
    log_level: str = "INFO"

    # Scaling thresholds (percent)
    cpu_threshold_scale_up: float = 75.0
    cpu_threshold_scale_down: float = 40.0
    memory_threshold_scale_up: float = 80.0
    memory_threshold_scale_down: float = 50.0
    util_threshold_scale_up: float = 75.0      # effective_utilization %
    util_threshold_scale_down: float = 40.0

    # Worker bounds
    min_workers: int = 2
    max_workers: int = 20

    # Cooldowns (seconds) — scale-down is held longer than scale-up to damp flapping
    cooldown_period_seconds: float = 60.0
    scale_down_cooldown_seconds: float = 120.0

    # Loops (seconds)
    monitoring_interval_seconds: float = 5.0
    orchestration_interval_seconds: float = 5.0

    # History
    history_window_minutes: int = 15
    metrics_retention_hours: int = 24

    # Forecast (double exponential smoothing / Holt's method)
    forecast_alpha: float = 0.25
    forecast_beta: float = 0.10
    horizon_minutes: int = 10
    confidence_threshold: float = 0.70

    # Workload model + worker pool
    base_arrival_rate: float = 500.0           # msgs/sec baseline demand
    capacity_per_worker: float = 400.0         # msgs/sec each worker handles

    # Worker backend
    worker_backend: str = "simulated"          # "simulated" | "docker"
    worker_image: str = "adaptive-worker:latest"

    # Dashboard emit cadence (seconds)
    ws_emit_interval: float = 2.0


def _field_types() -> dict[str, Callable[[Any], Any]]:
    """Build a ``field_name -> converter`` map from the dataclass annotations.

    Keeping this derived from the dataclass (rather than hand-maintained) means
    env/YAML coercion stays in sync automatically as fields are added. ``bool`` is
    deliberately not used by any field; ints and floats get the obvious casters and
    everything else is treated as a string.
    """
    converters: dict[str, Callable[[Any], Any]] = {}
    for f in fields(Settings):
        if f.type in ("int", int):
            converters[f.name] = int
        elif f.type in ("float", float):
            converters[f.name] = float
        else:
            converters[f.name] = str
    return converters


# Computed once at import time; the field set never changes at runtime.
_CONVERTERS = _field_types()
_VALID_FIELDS = set(_CONVERTERS)


def _coerce(field_name: str, value: Any) -> Any:
    """Coerce ``value`` to the declared type of ``field_name``.

    Returns the coerced value, or ``None`` if the field is unknown or the value
    cannot be converted (callers treat ``None`` as "skip this override").
    """
    converter = _CONVERTERS.get(field_name)
    if converter is None:
        return None
    try:
        return converter(value)
    except (ValueError, TypeError):
        return None


# Mapping of nested YAML sections onto flat dataclass field names. Each entry maps a
# section name to a dict of ``yaml_key -> dataclass_field``. Keys not listed here are
# also accepted when they happen to match a dataclass field name directly (see
# ``_apply_section``), so the YAML can mix documented aliases and flat names freely.
_NESTED_ALIASES: dict[str, dict[str, str]] = {
    "monitoring": {
        "interval_seconds": "monitoring_interval_seconds",
        "history_window_minutes": "history_window_minutes",
        "metrics_retention_hours": "metrics_retention_hours",
    },
    "scaling": {
        "cpu_threshold_scale_up": "cpu_threshold_scale_up",
        "cpu_threshold_scale_down": "cpu_threshold_scale_down",
        "memory_threshold_scale_up": "memory_threshold_scale_up",
        "memory_threshold_scale_down": "memory_threshold_scale_down",
        "util_threshold_scale_up": "util_threshold_scale_up",
        "util_threshold_scale_down": "util_threshold_scale_down",
        "min_workers": "min_workers",
        "max_workers": "max_workers",
        "cooldown_period_seconds": "cooldown_period_seconds",
        "scale_down_cooldown_seconds": "scale_down_cooldown_seconds",
    },
    "forecast": {
        "alpha": "forecast_alpha",
        "beta": "forecast_beta",
        "horizon_minutes": "horizon_minutes",
        "confidence_threshold": "confidence_threshold",
    },
    "dashboard": {
        "host": "host",
        "port": "port",
        "log_level": "log_level",
        "ws_emit_interval": "ws_emit_interval",
    },
    "workload": {
        "base_arrival_rate": "base_arrival_rate",
        "capacity_per_worker": "capacity_per_worker",
    },
}


# Environment-variable names map to dataclass fields by UPPERCASING the field name.
# A couple of fields are intentionally not env-exposed (e.g. worker_image); everything
# the spec calls for is covered because field names uppercase to the documented keys.
def _env_key(field_name: str) -> str:
    return field_name.upper()


def _apply_section(section: dict[str, Any], alias_map: dict[str, str], out: dict[str, Any]) -> None:
    """Apply one nested YAML section to ``out`` using ``alias_map``.

    Both aliased keys (e.g. ``alpha`` -> ``forecast_alpha``) and direct field-name
    keys are honored; anything else is ignored gracefully.
    """
    if not isinstance(section, dict):
        return
    for key, raw in section.items():
        field_name = alias_map.get(key)
        if field_name is None and key in _VALID_FIELDS:
            field_name = key  # accept a flat field name nested under the section
        if field_name is None:
            continue  # unknown key — ignore
        coerced = _coerce(field_name, raw)
        if coerced is not None:
            out[field_name] = coerced


def _load_yaml(config_path: str) -> dict[str, Any]:
    """Parse the YAML file at ``config_path`` into ``{field_name: value}`` overrides.

    Returns an empty dict if the file is absent, unreadable, malformed, or yaml is
    unavailable. Supports both flat top-level keys and the documented nested sections
    (``monitoring``, ``scaling``, ``forecast``, ``dashboard``, ``workload``).
    """
    if not config_path or yaml is None or not os.path.isfile(config_path):
        return {}

    try:
        with open(config_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except (OSError, yaml.YAMLError):
        # Unreadable file or invalid YAML — fall back to defaults+env.
        return {}

    if not isinstance(data, dict):
        # Empty file (None) or a non-mapping document — nothing to apply.
        return {}

    overrides: dict[str, Any] = {}

    # 1) Flat top-level keys that match dataclass fields directly.
    for key, raw in data.items():
        if key in _VALID_FIELDS:
            coerced = _coerce(key, raw)
            if coerced is not None:
                overrides[key] = coerced

    # 2) Known nested sections mapped onto flat fields.
    for section_name, alias_map in _NESTED_ALIASES.items():
        section = data.get(section_name)
        if section is not None:
            _apply_section(section, alias_map, overrides)

    return overrides


def _load_env() -> dict[str, Any]:
    """Read environment variables into ``{field_name: value}`` overrides.

    Each field is sourced from its UPPERCASE name. ``USE_DOCKER=1`` is honored as a
    convenience alias that forces ``worker_backend="docker"``.
    """
    overrides: dict[str, Any] = {}

    for field_name in _VALID_FIELDS:
        raw = os.environ.get(_env_key(field_name))
        if raw is not None:
            coerced = _coerce(field_name, raw)
            if coerced is not None:
                overrides[field_name] = coerced

    # Convenience toggle: USE_DOCKER=1 selects the docker worker backend.
    use_docker = os.environ.get("USE_DOCKER")
    if use_docker is not None and use_docker.strip() not in ("", "0", "false", "False"):
        overrides["worker_backend"] = "docker"

    return overrides


def load_config(config_path: str | None = None) -> Settings:
    """Load :class:`Settings` applying defaults -> YAML -> environment precedence.

    Args:
        config_path: Optional path to a YAML config file. If ``None``, the
            ``CONFIG_PATH`` environment variable is consulted, falling back to
            ``config/config.yaml``.

    Returns:
        A fully-populated :class:`Settings`. Missing/malformed YAML never raises;
        it simply yields defaults overlaid with any environment overrides.
    """
    resolved_path = config_path or os.environ.get("CONFIG_PATH") or DEFAULT_CONFIG_PATH

    overrides: dict[str, Any] = {}
    overrides.update(_load_yaml(resolved_path))  # YAML over defaults
    overrides.update(_load_env())                # env over YAML

    return Settings(**overrides)


def get_config(config_path: str | None = None) -> Settings:
    """Alias for :func:`load_config` for call sites that read more naturally."""
    return load_config(config_path)
