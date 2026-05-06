"""Registry of named circuit breakers."""
from __future__ import annotations
import threading
from typing import Callable, Optional, Awaitable, Union

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.state import CircuitState

# Sync or async listener callable: (name, from_state, to_state, reason) -> None | Awaitable[None]
ListenerCallable = Callable[[str, CircuitState, CircuitState, str], Union[None, Awaitable[None]]]


class CircuitBreakerRegistry:
    """Manages a collection of named CircuitBreakers and their listeners."""

    def __init__(self) -> None:
        self._breakers: dict[str, CircuitBreaker] = {}
        self._global_listeners: list[ListenerCallable] = []
        self._lock = threading.Lock()

    def register(self, config: CircuitBreakerConfig) -> CircuitBreaker:
        """Create and register a new breaker. Returns the existing one if name already registered."""
        with self._lock:
            if config.name in self._breakers:
                return self._breakers[config.name]
            breaker = CircuitBreaker(config)
            for listener in self._global_listeners:
                breaker.add_listener(listener)
            self._breakers[config.name] = breaker
            return breaker

    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Look up a breaker by name; returns None if unknown."""
        return self._breakers.get(name)

    def all(self) -> dict[str, CircuitBreaker]:
        """Return a shallow copy of {name: breaker}."""
        return dict(self._breakers)

    def names(self) -> list[str]:
        """Return list of registered breaker names (sorted)."""
        return sorted(self._breakers.keys())

    def metrics_snapshot(self) -> dict:
        """Snapshot of all breakers' metrics for the API/dashboard."""
        return {name: br.to_dict() for name, br in self._breakers.items()}

    def add_global_listener(self, listener: ListenerCallable) -> None:
        """Attach a listener to all currently-registered AND future-registered breakers."""
        with self._lock:
            self._global_listeners.append(listener)
            for breaker in self._breakers.values():
                breaker.add_listener(listener)

    async def reset_all(self) -> None:
        """Reset every registered breaker to CLOSED + zeroed counters."""
        for breaker in list(self._breakers.values()):
            await breaker.reset()

    def clear(self) -> None:
        """Remove all registered breakers (for tests)."""
        with self._lock:
            self._breakers.clear()
            self._global_listeners.clear()


_singleton: Optional[CircuitBreakerRegistry] = None
_singleton_lock = threading.Lock()


def get_registry() -> CircuitBreakerRegistry:
    """Module-level singleton accessor."""
    global _singleton
    with _singleton_lock:
        if _singleton is None:
            _singleton = CircuitBreakerRegistry()
        return _singleton


def reset_registry_for_tests() -> None:
    """Replace the singleton (used only by tests to isolate state)."""
    global _singleton
    with _singleton_lock:
        _singleton = CircuitBreakerRegistry()
