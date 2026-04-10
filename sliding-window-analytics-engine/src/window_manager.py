"""Multi-resolution window registry and dispatcher.

The :class:`WindowManager` owns a collection of :class:`SlidingWindow`
instances keyed by ``(metric, resolution)``. Incoming events are
dispatched to every window whose metric matches, regardless of
resolution — so a single ``response_time`` event simultaneously updates
the 1-minute, 15-minute and 4-hour windows.

A module-level :func:`build_default_manager` factory assembles the
canonical 7-window layout used by the rest of the service (see
``plan.md``).
"""

from __future__ import annotations

from typing import Any

from src.config import Config
from src.models import Event, WindowConfig, WindowResult
from src.sliding_window import SlidingWindow


class WindowManager:
    """Registry of :class:`SlidingWindow` instances.

    Windows are keyed by ``(metric, resolution)``. An auxiliary
    ``_by_metric`` index provides O(1) fan-out during :meth:`dispatch`.
    Neither ``add_window`` nor ``dispatch`` is thread-safe; external
    synchronisation is the caller's responsibility.
    """

    def __init__(self) -> None:
        self._windows: dict[tuple[str, str], SlidingWindow] = {}
        # For fast metric -> windows lookup during dispatch.
        self._by_metric: dict[str, list[SlidingWindow]] = {}

    def add_window(self, config: WindowConfig) -> None:
        """Register a new :class:`SlidingWindow` from a :class:`WindowConfig`.

        The window name is derived as ``"{metric}_{resolution}"`` so it
        is unique across the registry and self-describing in snapshots.
        """
        key = (config.metric, config.resolution)
        name = f"{config.metric}_{config.resolution}"
        window = SlidingWindow(
            name=name,
            resolution=config.resolution,
            window_size=config.window_size,
            slide_interval=config.slide_interval,
            max_size=config.max_size,
        )
        self._windows[key] = window
        self._by_metric.setdefault(config.metric, []).append(window)

    def dispatch(self, event: Event) -> None:
        """Dispatch ``event`` to every window matching ``event.metric``.

        Unknown metrics are silently ignored — the manager is a passive
        router, not a validator. Validation belongs at the ingest layer.
        """
        for window in self._by_metric.get(event.metric, ()):
            window.add(event)

    def snapshot_all(self, now: float) -> dict[str, dict[str, WindowResult]]:
        """Return a nested ``{metric: {resolution: WindowResult}}`` snapshot.

        Each inner :class:`WindowResult` is computed as of ``now``. The
        shape is chosen to be trivially serialisable for the HTTP and
        WebSocket layers.
        """
        out: dict[str, dict[str, WindowResult]] = {}
        for (metric, resolution), window in self._windows.items():
            out.setdefault(metric, {})[resolution] = window.snapshot(now)
        return out

    @property
    def active_count(self) -> int:
        """Number of registered windows."""
        return len(self._windows)

    def get_window(self, metric: str, resolution: str) -> SlidingWindow | None:
        """Look up a single window by ``(metric, resolution)``."""
        return self._windows.get((metric, resolution))

    def state_dict(self) -> dict[str, Any]:
        """Return ``{window_key: window_state_dict}`` for every window.

        ``window_key`` is ``f"{metric}__{resolution}"`` — a flat string
        so the whole thing serialises cleanly as JSON. Consumers should
        treat the keys as opaque and call :meth:`load_state` to restore.
        """
        out: dict[str, Any] = {}
        for (metric, resolution), window in self._windows.items():
            key = f"{metric}__{resolution}"
            out[key] = window.state_dict()
        return out

    def load_state(self, state: dict[str, Any]) -> int:
        """Restore all windows from a state dict produced by :meth:`state_dict`.

        Returns the number of windows successfully restored. Any keys
        that don't match a currently-registered window are silently
        skipped — this way a schema or layout change (e.g. adding a new
        resolution) simply drops the stale keys on the floor rather
        than blowing up startup.
        """
        if not isinstance(state, dict):
            return 0

        restored = 0
        for key, window_state in state.items():
            if not isinstance(key, str) or "__" not in key:
                continue
            metric, _, resolution = key.partition("__")
            window = self._windows.get((metric, resolution))
            if window is None:
                continue
            if not isinstance(window_state, dict):
                continue
            try:
                window.load_state(window_state)
            except Exception:  # pragma: no cover - defensive
                # A corrupt sub-state shouldn't take down the whole
                # restore; just skip it and keep going.
                continue
            restored += 1
        return restored


# Canonical resolution labels -> window size (seconds).
_DEFAULT_RESOLUTIONS: dict[str, float] = {
    "1m": 60.0,
    "15m": 900.0,
    "4h": 14400.0,
}

# Metric -> list of resolution labels to activate for that metric.
_DEFAULT_LAYOUT: dict[str, tuple[str, ...]] = {
    "response_time": ("1m", "15m", "4h"),
    "throughput": ("1m", "15m"),
    "error_rate": ("1m", "15m"),
}


def build_default_manager(config: Config) -> WindowManager:
    """Create the canonical 7-window layout.

    * ``response_time``: 1m, 15m, 4h
    * ``throughput``:    1m, 15m
    * ``error_rate``:    1m, 15m

    All windows share ``slide_interval=5.0`` seconds and use
    ``config.max_event_buffer_size`` as their hard size cap.
    """
    manager = WindowManager()
    for metric, resolutions in _DEFAULT_LAYOUT.items():
        for resolution in resolutions:
            manager.add_window(
                WindowConfig(
                    metric=metric,
                    resolution=resolution,
                    window_size=_DEFAULT_RESOLUTIONS[resolution],
                    slide_interval=5.0,
                    max_size=config.max_event_buffer_size,
                )
            )
    return manager
