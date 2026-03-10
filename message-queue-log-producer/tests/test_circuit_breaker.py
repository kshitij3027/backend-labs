"""Tests for the CircuitBreaker class."""

from unittest.mock import patch

from src.circuit_breaker import CircuitBreaker, CircuitState


class TestCircuitBreaker:
    """Unit tests for CircuitBreaker."""

    def test_initial_state_closed(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED

    def test_allow_request_when_closed(self):
        cb = CircuitBreaker()
        assert cb.allow_request() is True

    def test_failures_below_threshold_stays_closed(self):
        cb = CircuitBreaker(failure_threshold=5)
        for _ in range(4):
            cb.record_failure()
        assert cb.state == CircuitState.CLOSED

    def test_failures_at_threshold_opens(self):
        cb = CircuitBreaker(failure_threshold=5)
        for _ in range(5):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_allow_request_when_open(self):
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.allow_request() is False

    @patch("src.circuit_breaker.time")
    def test_recovery_timeout_transitions_to_half_open(self, mock_time):
        mock_time.monotonic.return_value = 100.0
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

        # Advance time past recovery_timeout
        mock_time.monotonic.return_value = 131.0
        assert cb.state == CircuitState.HALF_OPEN

    @patch("src.circuit_breaker.time")
    def test_half_open_allows_one_request(self, mock_time):
        mock_time.monotonic.return_value = 100.0
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

        for _ in range(3):
            cb.record_failure()

        # Transition to HALF_OPEN
        mock_time.monotonic.return_value = 131.0
        assert cb.allow_request() is True
        assert cb.allow_request() is False

    @patch("src.circuit_breaker.time")
    def test_half_open_success_closes(self, mock_time):
        mock_time.monotonic.return_value = 100.0
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

        for _ in range(3):
            cb.record_failure()

        # Transition to HALF_OPEN
        mock_time.monotonic.return_value = 131.0
        cb.allow_request()  # consume the one allowed request
        cb.record_success()
        assert cb.state == CircuitState.CLOSED

    @patch("src.circuit_breaker.time")
    def test_half_open_failure_reopens(self, mock_time):
        mock_time.monotonic.return_value = 100.0
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

        for _ in range(3):
            cb.record_failure()

        # Transition to HALF_OPEN
        mock_time.monotonic.return_value = 131.0
        cb.allow_request()  # consume the one allowed request
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_record_success_resets_failure_count(self):
        cb = CircuitBreaker(failure_threshold=5)
        for _ in range(3):
            cb.record_failure()
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        # After reset, need full threshold again to open
        for _ in range(4):
            cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
