"""In-process dictionary-backed :class:`Backend` implementation.

Used in two contexts:

1. **Unit tests** — the unit suite runs without a live Redis, so any
   collaborator that takes a ``Backend`` gets an :class:`InMemoryBackend`
   in tests. Stays dependency-free.
2. **Runtime fallback** — the C10 lifespan tries to construct a
   :class:`~src.cache.redis_backend.RedisBackend` first; if Redis is
   unreachable it falls back to :class:`InMemoryBackend` so the service
   continues serving traffic. The only behavioural change is that state
   is lost on restart and not shared across processes.

Implementation notes
--------------------
The single ``_data`` dict holds plain string values. TTL metadata lives
in a parallel ``_expiries`` dict mapping ``key -> absolute_epoch_seconds``.
Keys without a TTL do not appear in ``_expiries`` at all — checking
``key in _expiries`` is the cheapest TTL-presence test.

Why epoch seconds rather than ``datetime``?
* ``time.time()`` is faster than ``datetime.now(...)`` (no tzinfo
  wrapping) and we compare on every ``get``.
* Counters are not the bottleneck either way, but micro-cost matters
  on the token store hot path which mirrors every tokenize call here.

Expiry semantics
----------------
We evict lazily on access (``get`` discards expired entries; ``keys``
sweeps before returning). No background reaper thread — keeps the
class self-contained and avoids the lifecycle headache of a daemon
thread that has to be joined at shutdown.

Thread safety
-------------
A single :class:`threading.RLock` guards both dicts. ``incr`` calls
``get`` internally and we use an RLock so the recursive acquire from
the same thread is free. Contention is not a concern: the lock is held
for nanoseconds across a dict access pair.
"""
from __future__ import annotations

import threading
import time


class InMemoryBackend:
    """Dict-backed, lock-guarded :class:`Backend` implementation."""

    # Class-level so callers can introspect ``InMemoryBackend.name``
    # without instantiating. Mirrored as an instance attribute for the
    # ``Backend`` Protocol's `name: str` declaration.
    name = "in_memory"

    def __init__(self) -> None:
        # str → str value store. Counters live here too as their stringified
        # form ("1", "2", ...) — uniform value type keeps the get/set
        # contract simple and avoids a bytes-vs-int asymmetry.
        self._data: dict[str, str] = {}
        # Sparse TTL map: only keys with an expiry appear. The value is
        # ``time.time() + ttl_sec`` captured at set time. ``None`` slot
        # values are never stored — we omit the key instead.
        self._expiries: dict[str, float] = {}
        # RLock because ``incr`` re-enters via ``get``. A plain Lock would
        # deadlock; the RLock's cost over a Lock is one extra branch.
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Backend implementation
    # ------------------------------------------------------------------

    def get(self, key: str) -> str | None:
        """Return the value stored at ``key`` or ``None``.

        TTL check is lazy: if the key is in ``_expiries`` and the
        absolute expiry instant is in the past, we evict the key from
        both dicts and return ``None``. This is the only place expired
        keys are reaped (besides :meth:`keys`); a key that nobody
        accesses sits in the dicts until the process dies, which is
        fine because the memory cost is small.
        """
        with self._lock:
            # TTL check first — an expired key is logically absent.
            expiry = self._expiries.get(key)
            if expiry is not None and time.time() > expiry:
                # Evict eagerly so subsequent gets are O(1).
                self._data.pop(key, None)
                self._expiries.pop(key, None)
                return None
            return self._data.get(key)

    def set(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        """Store ``value`` at ``key`` with optional TTL in seconds.

        ``ttl_sec=None`` means no expiry — we make sure to also remove
        any prior expiry entry so re-setting a previously-TTL'd key
        without a TTL clears the expiry rather than silently inheriting
        the old one. (Mirrors Redis's behaviour where ``SET key value``
        without ``EX`` clears any existing TTL.)
        """
        with self._lock:
            self._data[key] = value
            if ttl_sec is None:
                # Setting without a TTL clears any prior expiry — match
                # the Redis SET-clears-TTL contract.
                self._expiries.pop(key, None)
            else:
                # Absolute epoch instant; computed once at set time so
                # subsequent gets don't re-derive it.
                self._expiries[key] = time.time() + ttl_sec

    def incr(self, key: str) -> int:
        """Atomically increment the counter at ``key``.

        Counter values live in the same ``_data`` dict as the bytes
        namespace, stored as their stringified integer form. We read
        the current value via :meth:`get` (which also handles TTL
        expiry of the counter — though counters in this app never have
        a TTL), parse as int, increment, and write back. The RLock
        makes the read-modify-write atomic relative to other threads.
        """
        with self._lock:
            current_str = self.get(key)
            current = int(current_str) if current_str is not None else 0
            new_value = current + 1
            # Counters are unlimited-lifetime — no ttl_sec here. ``set``
            # also clears any prior TTL on this key which is exactly
            # what we want.
            self.set(key, str(new_value))
            return new_value

    def keys(self, prefix: str = "") -> list[str]:
        """Return every key whose name starts with ``prefix``.

        Sweeps expired entries before returning so callers never see a
        key that ``get`` would refuse to return. The sweep is O(n_expiry)
        — cheap for our scale where ``_expiries`` is usually empty (the
        C10 mirror writes have no TTL).
        """
        with self._lock:
            # Sweep expired keys so the result is internally consistent
            # with what ``get`` would return for the same keys.
            now = time.time()
            expired = [k for k, exp in self._expiries.items() if now > exp]
            for k in expired:
                self._data.pop(k, None)
                self._expiries.pop(k, None)
            # Materialise the filtered list inside the lock so a
            # concurrent ``set`` can't mutate the dict mid-iteration.
            return [k for k in self._data.keys() if k.startswith(prefix)]

    def close(self) -> None:
        """No-op for the in-memory backend.

        Defined explicitly (rather than letting AttributeError leak)
        so callers can use the same shutdown path for either backend.
        """
        return None
