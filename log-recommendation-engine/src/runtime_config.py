"""Live, shared, runtime-tunable configuration (C12).

A subset of the ranking knobs can be retuned **at runtime** — without a restart and
consistently across every replica — via ``GET | PUT /config``. This module is the
single place that reconciles the two layers of configuration:

    static defaults (:func:`src.config.get_settings`)  ->  runtime overrides (Redis)

The static settings (defaults + YAML + env, parsed once per process) provide the
baseline; a small hash in Redis (``runtime_config``) holds any values an operator has
pushed with ``PUT /config``. :func:`get_effective_config` overlays the second on the
first, so a replica that never saw the PUT still picks up the change on its next read.

Why Redis (not a process global)
--------------------------------
The service scales horizontally. An in-process override would only retune the one
replica that served the PUT, so identical requests would get different rankings
depending on which pod answered. Storing the overrides — and a global **version
counter** — in Redis makes the retune fleet-wide and lets the recommendation cache key
embed the version so a change invalidates cached results everywhere at once.

Fault tolerance
---------------
Every Redis touch degrades gracefully (see :mod:`src.clients.redis`): if Redis is down,
:func:`get_effective_config` simply returns the static defaults, and a failed override
write is surfaced to the caller only insofar as the change won't take effect — it never
raises. Validation, by contrast, is strict and *local*: an unknown key or an
out-of-range value raises :class:`ValueError` **before** any Redis write, which the
router maps to HTTP 422.

Tunable surface
---------------
Only the keys in :data:`TUNABLE_KEYS` may be overridden. Everything else (database URL,
embedding model, ports, …) is deployment-level config that must not change under a live
request and is therefore *not* exposed here.
"""

from __future__ import annotations

from typing import Any, Callable

from src import observability
from src.clients import redis as redis_client
from src.config import get_settings

logger = observability.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Tunable surface + per-key validation
# --------------------------------------------------------------------------- #
#: The runtime-tunable keys. Each maps to a field of the same name on
#: :class:`src.config.Settings` (the static default) and may be overridden in Redis.
TUNABLE_KEYS: tuple[str, ...] = (
    "weight_semantic",
    "weight_contextual",
    "weight_feedback",
    "epsilon_explore",
    "diversity_threshold",
    "recency_half_life_days",
    "top_k",
    "high_confidence_threshold",
    "medium_confidence_threshold",
)


def _coerce_and_validate(key: str, value: Any) -> float | int:
    """Coerce ``value`` to the key's type and check it is in range, else ``ValueError``.

    Applied both when reading overrides back from Redis (values arrive as strings) and
    when validating a ``PUT /config`` body (values arrive as JSON numbers). Ranges:

    * ``weight_*`` — any float ≥ 0 (weights are non-negative; they need not sum to 1).
    * ``epsilon_explore`` / ``diversity_threshold`` /
      ``high_confidence_threshold`` / ``medium_confidence_threshold`` — a float in
      the closed unit interval ``[0, 1]``.
    * ``recency_half_life_days`` — a float strictly > 0 (a half-life of 0 is undefined).
    * ``top_k`` — an int ≥ 1.

    Raises :class:`ValueError` (message names the key) for an unknown key, a
    non-numeric value, or an out-of-range value.
    """
    if key not in TUNABLE_KEYS:
        raise ValueError(f"unknown config key: {key!r}")

    if key == "top_k":
        try:
            # Reject floats like 3.5 that don't represent a whole number, but accept
            # an integral float such as 3.0 (JSON has no separate int type).
            coerced = int(value)
            if float(value) != coerced:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError(f"{key} must be an integer") from None
        if coerced < 1:
            raise ValueError(f"{key} must be >= 1")
        return coerced

    try:
        fval = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a number") from None

    if key.startswith("weight_"):
        if fval < 0.0:
            raise ValueError(f"{key} must be >= 0")
        return fval

    if key == "recency_half_life_days":
        if fval <= 0.0:
            raise ValueError(f"{key} must be > 0")
        return fval

    # epsilon_explore, diversity_threshold, high/medium_confidence_threshold.
    if not (0.0 <= fval <= 1.0):
        raise ValueError(f"{key} must be within [0, 1]")
    return fval


# --------------------------------------------------------------------------- #
# Effective config = static defaults overlaid with Redis overrides
# --------------------------------------------------------------------------- #
def _static_defaults() -> dict[str, float | int]:
    """Return the static default for every tunable key from :func:`get_settings`."""
    settings = get_settings()
    return {key: getattr(settings, key) for key in TUNABLE_KEYS}


def get_effective_config() -> dict[str, float | int]:
    """Return the effective tunable config: static defaults overlaid with Redis overrides.

    Starts from :func:`src.config.get_settings` for every :data:`TUNABLE_KEYS` field,
    then overlays any override stored in the shared Redis ``runtime_config`` hash. A
    malformed / out-of-range stored value is ignored (logged) so one bad entry can
    never poison the whole config — the static default stands in for it.

    Fault tolerant: if Redis is unavailable :func:`src.clients.redis.get_runtime_config`
    returns ``{}`` and this is exactly the static-defaults dict, so ranking still works
    with the baked-in configuration. Callers (the recommendation service) read the
    weights / epsilon / diversity / top_k / half-life straight off the returned dict.
    """
    config = _static_defaults()
    overrides = redis_client.get_runtime_config()
    for key, raw in overrides.items():
        if key not in TUNABLE_KEYS:
            # Ignore stray keys that predate a tunable-set change; never surface them.
            continue
        try:
            config[key] = _coerce_and_validate(key, raw)
        except ValueError as exc:  # noqa: PERF203 - one bad entry must not break config
            logger.warning("ignoring invalid stored override %s=%r: %s", key, raw, exc)
    return config


def set_overrides(updates: dict[str, Any]) -> dict[str, float | int]:
    """Validate ``updates``, persist them to Redis, bump the version, return the new config.

    Steps, in order:

    1. **Validate everything first** — reject an empty body, an unknown key, or an
       out-of-range value with :class:`ValueError` (the router maps it to 422). Because
       validation runs to completion before any write, a partially-bad body writes
       *nothing* (all-or-nothing).
    2. **Write** the coerced overrides into the shared ``runtime_config`` hash (a merge:
       fields not in ``updates`` keep their prior override).
    3. **Bump** the global config version so every replica's next ``/recommend`` builds
       a fresh cache key and recomputes under the new values (no restart needed).
    4. Return the recomputed :func:`get_effective_config`.

    Raises :class:`ValueError` on any validation failure. Redis being down does not
    raise here — the write/bump degrade to no-ops — but then the change simply won't
    take effect; the returned dict still reflects what *would* apply.
    """
    if not isinstance(updates, dict) or not updates:
        raise ValueError("no config overrides supplied")

    # Validate + coerce the whole batch up front (all-or-nothing).
    coerced: dict[str, float | int] = {}
    for key, value in updates.items():
        coerced[key] = _coerce_and_validate(key, value)

    # Persist the merge, then bump the version so caches invalidate fleet-wide.
    redis_client.set_runtime_config(coerced)
    redis_client.bump_config_version()

    return get_effective_config()


def get_config_version() -> int:
    """Return the current global runtime-config version (passthrough to Redis).

    Used by :mod:`src.recommendation_service` to fold the version into the cache key so
    a ``PUT /config`` invalidates cached recommendations. Fault tolerant: ``0`` when
    Redis is unavailable or the key is unset (see
    :func:`src.clients.redis.get_config_version`).
    """
    return redis_client.get_config_version()


__all__ = [
    "TUNABLE_KEYS",
    "get_effective_config",
    "set_overrides",
    "get_config_version",
]
