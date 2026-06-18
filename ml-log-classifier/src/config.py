"""Application configuration for the ML Log Classifier.

Configuration precedence (lowest to highest):

    dataclass defaults  ->  YAML file  ->  environment variables

Defaults live on the :class:`Settings` dataclass. A YAML file (``/app/config/config.yaml``
by default, overridable via the ``config_path`` argument or the ``CONFIG_PATH``
environment variable) may override any field. Finally, environment variables —
sourced from each field's UPPERCASE name — win over everything else.

Values from YAML and the environment are coerced to each field's declared type
(``int`` / ``float`` / ``bool`` / ``list`` / ``str``). The loader is intentionally
defensive: a missing or malformed YAML file must not crash the application — it
simply falls back to defaults plus any environment overrides.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from typing import Any, Callable

try:
    import yaml
except ImportError:  # pragma: no cover - yaml is a hard dependency, guard anyway
    yaml = None


# Default location for the YAML config inside the container image.
DEFAULT_CONFIG_PATH = "/app/config/config.yaml"

# Strings that count as "false" when coercing a value to bool. Anything else
# non-empty is treated as true.
_FALSEY = {"", "0", "false", "no", "off", "none"}


@dataclass
class Settings:
    """Central, flat configuration for the classifier service and its dashboard.

    Field names map directly to the spec's Configurable Parameters (project
    requirements §7). Each field is overridable via YAML (lowercase key) or an
    environment variable (UPPERCASE field name).
    """

    # Server / dashboard
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

    # Filesystem locations (mounted volumes in Docker)
    model_dir: str = "/app/models"
    data_dir: str = "/app/data"

    # Sample-data generation / reproducibility
    sample_size: int = 1000
    random_seed: int = 42

    # TF-IDF feature extraction
    tfidf_max_features: int = 5000
    tfidf_ngram_max: int = 2

    # Ensemble member hyperparameters
    rf_n_estimators: int = 100
    gb_n_estimators: int = 100
    ensemble_weights: list = field(default_factory=lambda: [1, 2, 3])

    # Adaptive learning loop
    accuracy_retrain_threshold: float = 0.90
    drift_window: int = 100

    # Serving performance
    target_latency_ms: int = 100
    cache_size: int = 1024


def _bool(value: Any) -> bool:
    """Coerce ``value`` to a bool, honoring common falsey string spellings."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() not in _FALSEY


def _list(value: Any) -> list:
    """Coerce ``value`` to a list.

    Already-a-list values pass through. A scalar from YAML is wrapped. A string
    (typically from an env var) is split on commas, with each element coerced to
    an ``int`` or ``float`` when it looks numeric so that e.g. ``"1,2,3"`` becomes
    ``[1, 2, 3]`` rather than ``["1", "2", "3"]``.
    """
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",") if p.strip() != ""]
        return [_maybe_number(p) for p in parts]
    return [value]


def _maybe_number(token: str) -> Any:
    """Return ``token`` as an int, then float, else the original string."""
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        return token


def _field_types() -> dict[str, Callable[[Any], Any]]:
    """Build a ``field_name -> converter`` map from the dataclass annotations.

    Deriving this from the dataclass (rather than hand-maintaining it) keeps
    env/YAML coercion in sync automatically as fields are added.
    """
    converters: dict[str, Callable[[Any], Any]] = {}
    for f in fields(Settings):
        if f.type in ("bool", bool):
            converters[f.name] = _bool
        elif f.type in ("int", int):
            converters[f.name] = int
        elif f.type in ("float", float):
            converters[f.name] = float
        elif f.type in ("list", list, "list[int]", "list[str]"):
            converters[f.name] = _list
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


def _load_yaml(config_path: str) -> dict[str, Any]:
    """Parse the YAML file at ``config_path`` into ``{field_name: value}`` overrides.

    Returns an empty dict if the file is absent, unreadable, malformed, or yaml is
    unavailable. Only flat top-level keys matching dataclass field names are read.
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
    for key, raw in data.items():
        if key in _VALID_FIELDS:
            coerced = _coerce(key, raw)
            if coerced is not None:
                overrides[key] = coerced
    return overrides


def _load_env() -> dict[str, Any]:
    """Read environment variables into ``{field_name: value}`` overrides.

    Each field is sourced from its UPPERCASE name (e.g. ``ACCURACY_RETRAIN_THRESHOLD``).
    """
    overrides: dict[str, Any] = {}
    for field_name in _VALID_FIELDS:
        raw = os.environ.get(field_name.upper())
        if raw is not None:
            coerced = _coerce(field_name, raw)
            if coerced is not None:
                overrides[field_name] = coerced
    return overrides


def load_config(config_path: str | None = None) -> Settings:
    """Load :class:`Settings` applying defaults -> YAML -> environment precedence.

    Args:
        config_path: Optional path to a YAML config file. If ``None``, the
            ``CONFIG_PATH`` environment variable is consulted, falling back to
            :data:`DEFAULT_CONFIG_PATH`.

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
    """Alias for :func:`load_config` for call sites that read more naturally.

    Resolves the same way as :func:`load_config`: an explicit ``config_path``, then
    the ``CONFIG_PATH`` environment variable, then :data:`DEFAULT_CONFIG_PATH`.
    """
    return load_config(config_path)
