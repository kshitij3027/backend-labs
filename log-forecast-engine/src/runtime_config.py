"""Process-local, hot-reloadable runtime configuration (Feature Area B, C11).

``project_requirements.md`` Feature Area B asks for **adjustable model weights,
confidence thresholds, and alert settings WITHOUT restart**. The static
:class:`src.config.Settings` (env / YAML) is parsed once per process and cached,
so it cannot satisfy that on its own. This module layers a small, mutable
*override store* on top of it:

* It is seeded from :func:`src.config.get_settings` defaults on first access.
* The API (``GET``/``PUT /config``) reads and updates it.
* The forecast routes consult it and pass the effective ``weights`` /
  ``high_threshold`` / ``medium_threshold`` into the prediction + alert path.

Scope / caveats
---------------
* **Per-process.** The store lives in module-level state guarded by a
  :class:`threading.Lock`. Multiple API replicas (or the Celery workers) each
  hold their *own* copy and do **not** share updates — there is no distributed
  config bus in this build. For a single-replica dev/demo deployment (the target
  here) this is sufficient and keeps the design simple. The scheduled tasks keep
  using the static settings; only the on-demand API forecast path honours the
  overrides. This trade-off is documented for the dashboard's benefit.
* **Best-effort & validated.** Updates are validated (non-negative weights;
  thresholds in ``[0, 1]`` with ``high > medium``) before being applied; an
  invalid update raises :class:`ValueError` (the API maps it to 422) and leaves
  the store untouched.
"""

from __future__ import annotations

import threading
from typing import Any

from src.config import get_settings

# Module-level mutable store + a lock guarding all reads/writes. ``None`` until
# first access, then a dict seeded from settings (see :func:`_ensure`).
_lock = threading.Lock()
_state: dict[str, Any] | None = None


# --------------------------------------------------------------------------- #
# Seeding / access
# --------------------------------------------------------------------------- #
def _seed_from_settings() -> dict[str, Any]:
    """Build the initial override dict from the static settings defaults."""
    settings = get_settings()
    return {
        "model_weights": dict(settings.model_weights),
        "high_confidence_threshold": float(settings.high_confidence_threshold),
        "medium_confidence_threshold": float(settings.medium_confidence_threshold),
        # Free-form alert settings dashboard can toggle (kept permissive on purpose
        # — only weights/thresholds feed the math; these are surfaced as-is).
        "alert_settings": {
            "enabled": True,
            # Tiers that should actually raise a notification (low never alerts).
            "notify_levels": ["high", "medium"],
        },
    }


def _ensure() -> dict[str, Any]:
    """Return the live store, seeding it from settings on first use.

    Caller must hold ``_lock`` (every public function below acquires it).
    """
    global _state
    if _state is None:
        _state = _seed_from_settings()
    return _state


def get_runtime_config() -> dict[str, Any]:
    """Return a deep-ish copy of the current runtime overrides.

    The returned dict is safe for the caller to mutate; the internal store is not
    affected. Nested ``model_weights`` / ``alert_settings`` are copied too.
    """
    with _lock:
        state = _ensure()
        return {
            "model_weights": dict(state["model_weights"]),
            "high_confidence_threshold": float(state["high_confidence_threshold"]),
            "medium_confidence_threshold": float(state["medium_confidence_threshold"]),
            "alert_settings": dict(state["alert_settings"]),
        }


def get_weights() -> dict[str, float]:
    """Convenience: current effective model weights (a fresh copy)."""
    with _lock:
        return {k: float(v) for k, v in _ensure()["model_weights"].items()}


def get_thresholds() -> tuple[float, float]:
    """Convenience: ``(high_confidence_threshold, medium_confidence_threshold)``."""
    with _lock:
        state = _ensure()
        return (
            float(state["high_confidence_threshold"]),
            float(state["medium_confidence_threshold"]),
        )


# --------------------------------------------------------------------------- #
# Update (validated, partial)
# --------------------------------------------------------------------------- #
def _validate_weights(weights: Any) -> dict[str, float]:
    """Validate + coerce a ``model_weights`` override.

    Requires a non-empty mapping of name -> non-negative finite number with at
    least one positive weight (an all-zero set would make the ensemble degenerate).
    """
    if not isinstance(weights, dict) or not weights:
        raise ValueError("model_weights must be a non-empty object")
    out: dict[str, float] = {}
    for name, raw in weights.items():
        try:
            val = float(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"weight for {name!r} must be a number") from exc
        if val != val or val in (float("inf"), float("-inf")):  # NaN / inf
            raise ValueError(f"weight for {name!r} must be finite")
        if val < 0.0:
            raise ValueError(f"weight for {name!r} must be non-negative")
        out[str(name)] = val
    if sum(out.values()) <= 0.0:
        raise ValueError("model_weights must contain at least one positive weight")
    return out


def _validate_threshold(value: Any, label: str) -> float:
    """Validate a single confidence threshold is a finite number in ``[0, 1]``."""
    try:
        v = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a number") from exc
    if v != v or v in (float("inf"), float("-inf")):
        raise ValueError(f"{label} must be finite")
    if not (0.0 <= v <= 1.0):
        raise ValueError(f"{label} must be in [0, 1]")
    return v


def update_runtime_config(
    *,
    model_weights: dict[str, float] | None = None,
    high_confidence_threshold: float | None = None,
    medium_confidence_threshold: float | None = None,
    alert_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply a **partial** update to the runtime config and return the new state.

    Only the supplied fields are changed. Validation runs against the *resulting*
    state so the ``high > medium`` invariant is checked even when only one
    threshold is provided. On any validation failure a :class:`ValueError` is
    raised and the store is left unchanged (the API maps this to HTTP 422).
    """
    with _lock:
        state = _ensure()
        # Work on a copy so a mid-update failure cannot partially mutate the store.
        new = {
            "model_weights": dict(state["model_weights"]),
            "high_confidence_threshold": float(state["high_confidence_threshold"]),
            "medium_confidence_threshold": float(state["medium_confidence_threshold"]),
            "alert_settings": dict(state["alert_settings"]),
        }

        if model_weights is not None:
            new["model_weights"] = _validate_weights(model_weights)
        if high_confidence_threshold is not None:
            new["high_confidence_threshold"] = _validate_threshold(
                high_confidence_threshold, "high_confidence_threshold"
            )
        if medium_confidence_threshold is not None:
            new["medium_confidence_threshold"] = _validate_threshold(
                medium_confidence_threshold, "medium_confidence_threshold"
            )
        if alert_settings is not None:
            if not isinstance(alert_settings, dict):
                raise ValueError("alert_settings must be an object")
            merged = dict(new["alert_settings"])
            merged.update(alert_settings)
            new["alert_settings"] = merged

        if new["high_confidence_threshold"] <= new["medium_confidence_threshold"]:
            raise ValueError(
                "high_confidence_threshold must be greater than "
                "medium_confidence_threshold"
            )

        # Commit the validated copy.
        state.clear()
        state.update(new)
        return get_runtime_config_locked(state)


def get_runtime_config_locked(state: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``state`` (caller already holds the lock)."""
    return {
        "model_weights": dict(state["model_weights"]),
        "high_confidence_threshold": float(state["high_confidence_threshold"]),
        "medium_confidence_threshold": float(state["medium_confidence_threshold"]),
        "alert_settings": dict(state["alert_settings"]),
    }


def reset() -> None:
    """Drop all overrides (re-seed from settings on next access). For tests."""
    global _state
    with _lock:
        _state = None


__all__ = [
    "get_runtime_config",
    "get_weights",
    "get_thresholds",
    "update_runtime_config",
    "reset",
]
