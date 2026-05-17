"""Thread-safe in-memory counters.

A single :class:`StatsCounters` instance is constructed at FastAPI
startup (C7) and shared across the request handlers, the C5 processor,
and the rotation background task. Every counter is a plain integer
guarded by one shared lock — there is no per-counter lock, because the
operations are nanosecond-scale and the lock is never held while waiting
on I/O.

Well-known counter names
------------------------
The following names are pre-populated to ``0`` at construction so the
dashboard / ``/api/stats`` response never has to special-case a missing
key on a fresh process:

* ``logs_processed``  — one increment per successful ``encrypt(log)``.
* ``fields_detected`` — total leaves the Detector flagged across all
  encrypt calls.
* ``fields_encrypted`` — total leaves successfully encrypted.
* ``fields_decrypted`` — total leaves successfully decrypted.
* ``errors``          — any exception during encrypt or decrypt.
* ``keys_rotated``    — DEK rotations (C7 will increment).

Arbitrary names also work — :meth:`incr` will create the key on first
use. This is intentional so future commits can add counters without
touching this file. :meth:`reset` will however leave unknown names in
place at zero rather than removing them, so a dashboard that already
fetched the snapshot's key set sees a stable shape.
"""
from __future__ import annotations

import threading


# The set of names we eagerly pre-populate. Listed as a module-level
# tuple so tests can reference it explicitly and future additions are
# one-line changes.
_WELL_KNOWN_COUNTERS: tuple[str, ...] = (
    "logs_processed",
    "fields_detected",
    "fields_encrypted",
    "fields_decrypted",
    "errors",
    "keys_rotated",
)


class StatsCounters:
    """A dict of integer counters with atomic increment.

    Notes
    -----
    All public methods acquire ``self._lock``. The lock is held only
    for the dict mutation or the snapshot copy — no I/O or expensive
    serialization runs under it — so contention is negligible even
    under heavy parallel writes from the C5 encrypt pool.

    The class does NOT use ``collections.Counter`` because the latter
    is not thread-safe and silently allows non-int values; we want
    explicit integer semantics enforced at the boundary.
    """

    def __init__(self) -> None:
        # One coarse lock is sufficient: every operation touches a single
        # dict entry, and the cost of acquiring an uncontended lock is
        # dwarfed by the cost of a single AES-GCM encrypt.
        self._lock = threading.Lock()
        # Pre-populate the well-known names at zero so the dashboard
        # always sees a stable key set. Unknown names are created
        # lazily on first incr() — the API is intentionally permissive
        # so future commits can add new counters without touching this
        # file.
        self._counters: dict[str, int] = {
            name: 0 for name in _WELL_KNOWN_COUNTERS
        }

    # -- public ----------------------------------------------------------

    def incr(self, name: str, n: int = 1) -> int:
        """Atomically increment ``name`` by ``n`` and return the new value.

        Creates the counter at zero if it doesn't exist yet — so this
        method is the canonical "new counter on first touch" path.

        Parameters
        ----------
        name : str
            Counter name. By convention snake_case (matches the
            well-known names).
        n : int, default 1
            Increment amount. Negative values are accepted (the lock
            still makes the operation atomic), but the well-known
            counters should monotonically increase in practice.

        Returns
        -------
        int
            The counter value AFTER the increment. Returning the new
            value lets callers do a one-shot "increment and threshold
            check" without a second read.
        """
        with self._lock:
            # dict.get(name, 0) handles the "not yet seen" case. We
            # then re-write under the same lock acquisition so the
            # read-modify-write is atomic.
            new_value = self._counters.get(name, 0) + n
            self._counters[name] = new_value
            return new_value

    def get(self, name: str) -> int:
        """Return the current value of ``name``, or ``0`` if missing.

        Lock-guarded so we never observe a half-mutated dict (it
        wouldn't happen in CPython today but the lock makes the
        contract explicit and portable across implementations).
        """
        with self._lock:
            return self._counters.get(name, 0)

    def snapshot(self) -> dict[str, int]:
        """Return a dict copy of every counter.

        The returned dict is a COPY — callers can mutate it without
        affecting the live state. The copy is materialized under the
        lock so concurrent writers can't shift the contents mid-snapshot.

        Used by ``/api/stats`` and the C8 dashboard's HTMX poll. Both
        consumers iterate the result outside the critical section, so
        the lock is held only for the brief ``dict(...)`` allocation.
        """
        with self._lock:
            return dict(self._counters)

    def reset(self) -> None:
        """Zero every counter that currently exists (including custom names).

        Used by tests; in production we never reset — counters are
        monotonic per process lifetime, and a process restart is the
        natural "reset" mechanism.

        We do NOT remove keys from the dict — only zero them — so a
        downstream consumer that has cached the key set sees a stable
        shape after reset.
        """
        with self._lock:
            # iterate over a snapshot of the keys so we can write back
            # safely without mutating during iteration. (Not strictly
            # needed in CPython since dict iteration tolerates value
            # updates, but the explicit list keeps the intent clear.)
            for key in list(self._counters.keys()):
                self._counters[key] = 0
