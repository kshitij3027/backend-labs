"""Tests for the SlidingWindow class."""
from __future__ import annotations

import threading

import numpy as np
import pytest

from src.pipeline.sliding_window import SlidingWindow


@pytest.fixture
def window():
    return SlidingWindow(maxlen=50)


class TestSlidingWindow:
    """Unit tests for the thread-safe SlidingWindow."""

    def test_add_increases_length(self, window, make_feature_vector):
        assert len(window) == 0
        window.add(make_feature_vector())
        assert len(window) == 1

    def test_maxlen_enforced(self, make_feature_vector):
        window = SlidingWindow(maxlen=5)
        for _ in range(10):
            window.add(make_feature_vector())
        assert len(window) == 5

    def test_get_all_returns_2d_array(self, window, make_feature_vector):
        n = 7
        for _ in range(n):
            window.add(make_feature_vector())
        result = window.get_all()
        assert result.ndim == 2
        assert result.shape == (n, 9)

    def test_get_all_empty_returns_empty_array(self, window):
        result = window.get_all()
        assert result.size == 0

    def test_get_stats_correct_values(self):
        window = SlidingWindow(maxlen=100)
        v1 = np.array([2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0])
        v2 = np.array([4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0])
        window.add(v1)
        window.add(v2)

        mean, std = window.get_stats()

        expected_mean = (v1 + v2) / 2.0
        expected_std = np.std(np.array([v1, v2]), axis=0, ddof=0) + 1e-10

        np.testing.assert_allclose(mean, expected_mean)
        np.testing.assert_allclose(std, expected_std)

    def test_is_ready_before_and_after(self, make_feature_vector):
        window = SlidingWindow(maxlen=100)
        assert window.is_ready(min_size=5) is False
        for _ in range(4):
            window.add(make_feature_vector())
        assert window.is_ready(min_size=5) is False
        window.add(make_feature_vector())
        assert window.is_ready(min_size=5) is True

    def test_clear_empties_buffer(self, window, make_feature_vector):
        for _ in range(10):
            window.add(make_feature_vector())
        assert len(window) == 10
        window.clear()
        assert len(window) == 0

    def test_thread_safety(self, make_feature_vector):
        window = SlidingWindow(maxlen=1000)
        errors: list[Exception] = []

        def worker() -> None:
            try:
                for _ in range(100):
                    window.add(make_feature_vector())
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(window) == 1000  # 10 threads * 100 items, maxlen=1000
