"""Tests for adaptive compression level adjustment."""

import threading
import time
from unittest.mock import patch, MagicMock

import pytest

from src.adaptive import AdaptiveCompression
from src.compression import CompressionHandler


@pytest.fixture
def handler():
    """Create a CompressionHandler with default settings."""
    return CompressionHandler(algorithm="gzip", level=6)


@pytest.fixture
def adaptive(handler):
    """Create an AdaptiveCompression instance with a short check interval."""
    return AdaptiveCompression(
        compression_handler=handler,
        min_level=1,
        max_level=9,
        check_interval=0.1,
    )


class TestCalculateLevel:
    """Direct tests for the _calculate_level formula."""

    def test_high_cpu_gives_low_level(self, adaptive):
        """90% CPU should yield a level at or near min_level."""
        level = adaptive._calculate_level(90.0)
        assert level <= 2  # near min_level=1

    def test_low_cpu_gives_high_level(self, adaptive):
        """10% CPU should yield a level at or near max_level."""
        level = adaptive._calculate_level(10.0)
        assert level >= 8  # near max_level=9

    def test_mid_cpu_gives_mid_level(self, adaptive):
        """50% CPU should yield a level around the middle of the range."""
        level = adaptive._calculate_level(50.0)
        assert 4 <= level <= 6

    def test_zero_cpu_gives_max_level(self, adaptive):
        """0% CPU should give exactly max_level."""
        level = adaptive._calculate_level(0.0)
        assert level == 9

    def test_full_cpu_gives_min_level(self, adaptive):
        """100% CPU should give exactly min_level."""
        level = adaptive._calculate_level(100.0)
        assert level == 1

    def test_bounds_clamped_above_100(self, adaptive):
        """CPU above 100% (edge case) should still clamp to min_level."""
        level = adaptive._calculate_level(120.0)
        assert level == 1

    def test_bounds_clamped_below_0(self, adaptive):
        """CPU below 0% (edge case) should still clamp to max_level."""
        level = adaptive._calculate_level(-10.0)
        assert level == 9

    def test_exact_values_across_range(self, adaptive):
        """Verify the linear formula for specific CPU values."""
        # level = 9 - (cpu / 100) * 8
        # cpu=0   → 9.0 → 9
        # cpu=25  → 7.0 → 7
        # cpu=50  → 5.0 → 5
        # cpu=75  → 3.0 → 3
        # cpu=100 → 1.0 → 1
        assert adaptive._calculate_level(0.0) == 9
        assert adaptive._calculate_level(25.0) == 7
        assert adaptive._calculate_level(50.0) == 5
        assert adaptive._calculate_level(75.0) == 3
        assert adaptive._calculate_level(100.0) == 1


class TestAdaptiveThread:
    """Tests for the monitoring thread lifecycle."""

    @patch("src.adaptive.psutil")
    def test_thread_starts_and_stops_cleanly(self, mock_psutil, handler):
        """Thread should start, run, and stop without errors."""
        mock_psutil.cpu_percent.return_value = 50.0
        adaptive = AdaptiveCompression(
            compression_handler=handler, check_interval=0.05,
        )
        adaptive.start()
        assert adaptive._thread is not None
        assert adaptive._thread.is_alive()

        time.sleep(0.15)
        adaptive.stop()
        assert not adaptive._thread.is_alive()

    @patch("src.adaptive.psutil")
    def test_handler_level_updated_by_monitor(self, mock_psutil, handler):
        """Monitor should update the handler's compression level."""
        mock_psutil.cpu_percent.return_value = 90.0
        adaptive = AdaptiveCompression(
            compression_handler=handler, check_interval=0.05,
        )
        adaptive.start()
        time.sleep(0.2)
        adaptive.stop()

        # 90% CPU → level should be near min (1 or 2)
        assert handler.level <= 2

    @patch("src.adaptive.psutil")
    def test_current_cpu_property_updates(self, mock_psutil, handler):
        """current_cpu property should reflect the most recently read CPU value."""
        mock_psutil.cpu_percent.return_value = 42.5
        adaptive = AdaptiveCompression(
            compression_handler=handler, check_interval=0.05,
        )
        assert adaptive.current_cpu == 0.0  # initial value

        adaptive.start()
        time.sleep(0.2)
        adaptive.stop()

        assert adaptive.current_cpu == 42.5

    @patch("src.adaptive.psutil")
    def test_low_cpu_sets_high_compression(self, mock_psutil, handler):
        """Low CPU usage should result in high compression level."""
        mock_psutil.cpu_percent.return_value = 10.0
        adaptive = AdaptiveCompression(
            compression_handler=handler, check_interval=0.05,
        )
        adaptive.start()
        time.sleep(0.2)
        adaptive.stop()

        assert handler.level >= 8

    @patch("src.adaptive.psutil")
    def test_stop_without_start_is_safe(self, mock_psutil, handler):
        """Calling stop() without start() should not raise."""
        adaptive = AdaptiveCompression(
            compression_handler=handler, check_interval=0.05,
        )
        adaptive.stop()  # should not raise

    @patch("src.adaptive.psutil")
    def test_daemon_thread(self, mock_psutil, handler):
        """Monitor thread should be a daemon thread."""
        mock_psutil.cpu_percent.return_value = 50.0
        adaptive = AdaptiveCompression(
            compression_handler=handler, check_interval=0.05,
        )
        adaptive.start()
        assert adaptive._thread.daemon is True
        adaptive.stop()


class TestCustomRange:
    """Tests for non-default min/max level ranges."""

    def test_custom_min_max(self, handler):
        """Custom min_level and max_level should be respected."""
        adaptive = AdaptiveCompression(
            compression_handler=handler,
            min_level=3,
            max_level=7,
            check_interval=0.1,
        )
        # 0% CPU → max_level=7
        assert adaptive._calculate_level(0.0) == 7
        # 100% CPU → min_level=3
        assert adaptive._calculate_level(100.0) == 3
        # 50% CPU → 5
        assert adaptive._calculate_level(50.0) == 5
