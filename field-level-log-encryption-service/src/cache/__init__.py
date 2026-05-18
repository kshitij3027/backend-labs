"""Cache subsystem — pluggable key/value store behind a common interface.

The cache exists primarily to support **Feature Area D** of the project
requirements: "Track key usage frequency for security analysis". The
:class:`~src.processor.log_processor.LogProcessor` increments a per-key
counter on every encrypt and decrypt, and ``GET /v1/keys`` reads the
counters back so operators can see which DEKs are getting hammered.

Two backends ship in C9:

* :class:`InMemoryCache` — pure-Python, dict-backed, lock-guarded.
  Always available; used for unit tests and as the runtime fallback
  when Redis is unreachable.
* :class:`RedisCache` — networked, persists across restarts, shareable
  across multiple app processes (future-proofing for horizontal scale).

The factory :func:`build_cache` tries Redis first and falls back to
in-memory automatically, so the rest of the app never has to care which
backend is live.

Public surface (re-exported here so callers ``from src.cache import …``):

* :class:`CacheProvider`   — the ABC every backend implements.
* :class:`CacheUnavailable` — raised when a networked backend can't be
  reached (Redis connection refused / timeout).
* :class:`InMemoryCache`   — the pure-Python backend.
* :class:`RedisCache`      — the Redis backend.
* :func:`build_cache`      — factory with graceful fallback.
"""
from __future__ import annotations

from .factory import build_cache
from .in_memory import InMemoryCache
from .provider import CacheProvider, CacheUnavailable
from .redis_cache import RedisCache

__all__ = [
    "CacheProvider",
    "CacheUnavailable",
    "InMemoryCache",
    "RedisCache",
    "build_cache",
]
