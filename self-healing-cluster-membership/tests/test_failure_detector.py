"""Tests for the phi accrual failure detector."""

import time

import pytest

from src.config import ClusterConfig
from src.failure_detector import PhiAccrualFailureDetector


@pytest.fixture
def detector_config() -> ClusterConfig:
    """Return a config with a small heartbeat window for testing."""
    return ClusterConfig(
        node_id="test-node-1",
        port=5001,
        heartbeat_window_size=5,
    )


@pytest.fixture
def detector(detector_config: ClusterConfig) -> PhiAccrualFailureDetector:
    """Return a fresh PhiAccrualFailureDetector."""
    return PhiAccrualFailureDetector(detector_config)


class TestComputePhi:
    """Tests for phi computation."""

    async def test_no_heartbeat_returns_zero(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """compute_phi for an unknown node returns 0.0."""
        assert detector.compute_phi("unknown-node") == 0.0

    async def test_single_heartbeat_returns_zero(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """Only one heartbeat recorded, no interval yet, phi should be 0.0."""
        detector.record_heartbeat("node-a")
        assert detector.compute_phi("node-a") == 0.0

    async def test_regular_heartbeats_keep_phi_low(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """Regular heartbeats at consistent intervals keep phi low."""
        for _ in range(5):
            detector.record_heartbeat("node-a")
            time.sleep(0.05)

        phi = detector.compute_phi("node-a")
        assert phi < 2.0, f"Expected phi < 2.0 after regular heartbeats, got {phi}"

    async def test_missed_heartbeat_increases_phi(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """Missing heartbeats cause phi to increase significantly."""
        # Record regular heartbeats at ~0.05s intervals
        for _ in range(5):
            detector.record_heartbeat("node-a")
            time.sleep(0.05)

        # Simulate a gap of ~5x the normal interval
        time.sleep(0.25)
        phi = detector.compute_phi("node-a")
        assert phi > 3.0, f"Expected phi > 3.0 after missed heartbeat, got {phi}"

    async def test_phi_exceeds_threshold_after_long_gap(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """After a long gap (8x normal interval), phi should reach the threshold."""
        # Record regular heartbeats at ~0.1s intervals
        for _ in range(5):
            detector.record_heartbeat("node-a")
            time.sleep(0.1)

        # Simulate a gap of ~8x the normal interval
        time.sleep(0.8)
        phi = detector.compute_phi("node-a")
        assert phi >= 8.0, f"Expected phi >= 8.0 after long gap, got {phi}"


class TestInterpretPhi:
    """Tests for phi interpretation."""

    async def test_interpret_phi_normal(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """phi=0.5 should be interpreted as 'normal'."""
        assert detector.interpret_phi(0.5) == "normal"

    async def test_interpret_phi_minor_delay(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """phi=2.0 should be interpreted as 'minor_delay'."""
        assert detector.interpret_phi(2.0) == "minor_delay"

    async def test_interpret_phi_significant_delay(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """phi=5.0 should be interpreted as 'significant_delay'."""
        assert detector.interpret_phi(5.0) == "significant_delay"

    async def test_interpret_phi_probable_failure(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """phi=10.0 should be interpreted as 'probable_failure'."""
        assert detector.interpret_phi(10.0) == "probable_failure"


class TestWindowManagement:
    """Tests for sliding window and node lifecycle."""

    async def test_sliding_window_capped(
        self, detector: PhiAccrualFailureDetector, detector_config: ClusterConfig
    ) -> None:
        """The internal window should not exceed heartbeat_window_size."""
        # Record more heartbeats than the window size
        for _ in range(detector_config.heartbeat_window_size + 10):
            detector.record_heartbeat("node-a")
            time.sleep(0.01)

        window = detector._windows.get("node-a")
        assert window is not None
        assert len(window) <= detector_config.heartbeat_window_size

    async def test_remove_node_clears_data(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """After removing a node, phi should return 0.0."""
        detector.record_heartbeat("node-a")
        time.sleep(0.05)
        detector.record_heartbeat("node-a")

        # Verify data exists
        assert "node-a" in detector._last_heartbeat

        detector.remove_node("node-a")
        assert detector.compute_phi("node-a") == 0.0
        assert "node-a" not in detector._last_heartbeat
        assert "node-a" not in detector._windows

    async def test_reset_node_clears_data(
        self, detector: PhiAccrualFailureDetector
    ) -> None:
        """After resetting a node, phi should return 0.0."""
        detector.record_heartbeat("node-a")
        time.sleep(0.05)
        detector.record_heartbeat("node-a")

        detector.reset_node("node-a")
        assert detector.compute_phi("node-a") == 0.0
        assert "node-a" not in detector._last_heartbeat
        assert "node-a" not in detector._windows
