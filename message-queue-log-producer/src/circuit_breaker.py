"""Circuit breaker pattern for RabbitMQ publish protection."""

import enum
import threading
import time


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Thread-safe circuit breaker with lazy OPEN -> HALF_OPEN transition."""

    def __init__(self, failure_threshold=5, recovery_timeout=30):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._half_open_pending = False

    @property
    def state(self):
        """Current state with lazy OPEN -> HALF_OPEN transition."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self._recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_pending = False
            return self._state

    def allow_request(self):
        """Check if a request should be allowed through."""
        with self._lock:
            # Lazy transition
            if self._state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self._recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_pending = False

            if self._state == CircuitState.CLOSED:
                return True
            elif self._state == CircuitState.HALF_OPEN:
                if not self._half_open_pending:
                    self._half_open_pending = True
                    return True
                return False
            else:  # OPEN
                return False

    def record_success(self):
        """Record a successful request."""
        with self._lock:
            self._failure_count = 0
            self._state = CircuitState.CLOSED
            self._half_open_pending = False

    def record_failure(self):
        """Record a failed request."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._failure_count >= self._failure_threshold:
                self._state = CircuitState.OPEN
                self._half_open_pending = False
