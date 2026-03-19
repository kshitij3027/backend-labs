"""Tests for auto-scaler."""
import time
import pytest
from unittest.mock import MagicMock
from src.config import Settings
from src.monitoring.metrics import MetricsCollector
from src.consumer.auto_scaler import AutoScaler


class TestAutoScaler:
    def _make_scaler(self, lag_threshold=100, max_consumers=6, cooldown=0):
        settings = Settings(
            bootstrap_servers="localhost:9092",
            lag_threshold=lag_threshold,
            max_consumers=max_consumers,
            scale_cooldown_s=cooldown,
        )
        metrics = MetricsCollector()
        add_fn = MagicMock()
        remove_fn = MagicMock()
        count_fn = MagicMock(return_value=3)
        scaler = AutoScaler(settings, metrics, add_fn, remove_fn, count_fn)
        return scaler, metrics, add_fn, remove_fn, count_fn

    def test_scale_up_on_high_lag(self):
        scaler, metrics, add_fn, remove_fn, count_fn = self._make_scaler(lag_threshold=100)
        # Set high lag
        for i in range(6):
            metrics.update_lag(i, 50)  # total = 300 > 100
        scaler._check_and_scale()
        add_fn.assert_called_once()

    def test_no_scale_when_lag_below_threshold(self):
        scaler, metrics, add_fn, remove_fn, count_fn = self._make_scaler(lag_threshold=1000)
        metrics.update_lag(0, 10)  # total = 10 < 1000
        scaler._check_and_scale()
        add_fn.assert_not_called()

    def test_no_scale_at_max_consumers(self):
        scaler, metrics, add_fn, remove_fn, count_fn = self._make_scaler(
            lag_threshold=100, max_consumers=3
        )
        count_fn.return_value = 3  # already at max
        for i in range(6):
            metrics.update_lag(i, 50)
        scaler._check_and_scale()
        add_fn.assert_not_called()

    def test_cooldown_respected(self):
        scaler, metrics, add_fn, remove_fn, count_fn = self._make_scaler(
            lag_threshold=100, cooldown=60
        )
        for i in range(6):
            metrics.update_lag(i, 50)
        # First scale should work
        scaler._check_and_scale()
        add_fn.assert_called_once()
        # Second should be blocked by cooldown
        add_fn.reset_mock()
        scaler._check_and_scale()
        add_fn.assert_not_called()

    def test_scaling_history_recorded(self):
        scaler, metrics, add_fn, remove_fn, count_fn = self._make_scaler(lag_threshold=100)
        for i in range(6):
            metrics.update_lag(i, 50)
        scaler._check_and_scale()
        history = scaler.scaling_history
        assert len(history) == 1
        assert history[0]["action"] == "scale_up"
        assert history[0]["from_count"] == 3
        assert history[0]["to_count"] == 4

    def test_scale_down_when_idle(self):
        scaler, metrics, add_fn, remove_fn, count_fn = self._make_scaler(lag_threshold=100)
        # Set zero lag but some consumed messages (system has been running)
        metrics.record_consumed("c-0", 0, 100)
        # No throughput recorded, so current_throughput == 0
        # All lag is zero by default
        scaler._check_and_scale()
        remove_fn.assert_called_once()

    def test_no_scale_down_below_one(self):
        scaler, metrics, add_fn, remove_fn, count_fn = self._make_scaler(lag_threshold=100)
        count_fn.return_value = 1  # only 1 consumer
        metrics.record_consumed("c-0", 0, 100)
        scaler._check_and_scale()
        remove_fn.assert_not_called()

    def test_start_stop(self):
        scaler, metrics, add_fn, remove_fn, count_fn = self._make_scaler()
        scaler.start()
        assert scaler._thread is not None
        assert scaler._thread.is_alive()
        scaler.stop()
        assert not scaler._thread.is_alive()
