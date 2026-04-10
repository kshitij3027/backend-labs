"""Unit tests for :class:`src.window_manager.WindowManager`."""

from __future__ import annotations

from src.config import Config
from src.models import WindowConfig, WindowResult
from src.window_manager import WindowManager, build_default_manager


def _rt_config(resolution: str, window_size: float) -> WindowConfig:
    return WindowConfig(
        metric="response_time",
        resolution=resolution,
        window_size=window_size,
        slide_interval=5.0,
        max_size=1000,
    )


def test_empty_manager(make_event):
    manager = WindowManager()
    assert manager.active_count == 0
    # dispatch on an empty manager should be a silent no-op.
    manager.dispatch(make_event(timestamp=100.0, value=42.0))
    assert manager.snapshot_all(100.0) == {}


def test_add_window_and_get():
    manager = WindowManager()
    manager.add_window(_rt_config("1m", 60.0))
    assert manager.active_count == 1
    window = manager.get_window("response_time", "1m")
    assert window is not None
    assert window.resolution == "1m"
    assert window.window_size == 60.0
    # A non-existent lookup returns None.
    assert manager.get_window("response_time", "4h") is None


def test_dispatch_routes_to_multiple_resolutions(make_event):
    manager = WindowManager()
    manager.add_window(_rt_config("1m", 60.0))
    manager.add_window(_rt_config("15m", 900.0))
    manager.add_window(_rt_config("4h", 14400.0))

    event = make_event(timestamp=1000.0, value=123.0)
    manager.dispatch(event)

    for resolution in ("1m", "15m", "4h"):
        window = manager.get_window("response_time", resolution)
        assert window is not None
        assert window.size() == 1
        snap = window.snapshot(1000.0)
        assert snap.count == 1
        assert snap.sum == 123.0


def test_dispatch_only_matches_metric(make_event):
    manager = WindowManager()
    manager.add_window(_rt_config("1m", 60.0))
    manager.add_window(
        WindowConfig(
            metric="throughput",
            resolution="1m",
            window_size=60.0,
            slide_interval=5.0,
            max_size=1000,
        )
    )

    manager.dispatch(make_event(timestamp=500.0, value=42.0, metric="response_time"))

    rt = manager.get_window("response_time", "1m")
    tp = manager.get_window("throughput", "1m")
    assert rt is not None and tp is not None
    assert rt.snapshot(500.0).count == 1
    assert tp.snapshot(500.0).count == 0


def test_snapshot_all_shape(make_event):
    manager = WindowManager()
    manager.add_window(_rt_config("1m", 60.0))
    manager.add_window(_rt_config("15m", 900.0))
    manager.add_window(
        WindowConfig(
            metric="error_rate",
            resolution="1m",
            window_size=60.0,
            slide_interval=5.0,
            max_size=1000,
        )
    )

    manager.dispatch(make_event(timestamp=1000.0, value=10.0, metric="response_time"))
    manager.dispatch(make_event(timestamp=1000.0, value=0.02, metric="error_rate"))

    snapshot = manager.snapshot_all(1000.0)
    assert set(snapshot.keys()) == {"response_time", "error_rate"}
    assert set(snapshot["response_time"].keys()) == {"1m", "15m"}
    assert set(snapshot["error_rate"].keys()) == {"1m"}

    rt_1m = snapshot["response_time"]["1m"]
    assert isinstance(rt_1m, WindowResult)
    assert rt_1m.count == 1
    assert rt_1m.sum == 10.0
    assert snapshot["error_rate"]["1m"].count == 1


def test_build_default_manager():
    manager = build_default_manager(Config())
    assert manager.active_count == 7

    # response_time has 1m/15m/4h
    for resolution in ("1m", "15m", "4h"):
        assert manager.get_window("response_time", resolution) is not None

    # throughput and error_rate have 1m/15m only
    for metric in ("throughput", "error_rate"):
        assert manager.get_window(metric, "1m") is not None
        assert manager.get_window(metric, "15m") is not None
        assert manager.get_window(metric, "4h") is None

    # Spot-check sizes on a sample window.
    rt_1m = manager.get_window("response_time", "1m")
    assert rt_1m is not None
    assert rt_1m.window_size == 60.0
    assert rt_1m.slide_interval == 5.0
    rt_4h = manager.get_window("response_time", "4h")
    assert rt_4h is not None
    assert rt_4h.window_size == 14400.0
