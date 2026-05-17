"""Threshold-gated parallel encryptor.

A :class:`ParallelEncryptor` owns exactly one
:class:`concurrent.futures.ThreadPoolExecutor` for the lifetime of the
service. Each :meth:`encrypt_many` call inspects the batch and picks one
of two execution paths:

* **Serial (default)** — if the batch has fewer than ``threshold_fields``
  items OR the total plaintext byte-count is below ``threshold_bytes``,
  we run ``encrypt_fn`` synchronously on the caller's thread. This avoids
  the ``submit``/``Future`` overhead which, for a typical ~5-30 µs AES-GCM
  encrypt, dwarfs the parallel speed-up.
* **Parallel** — only when **both** thresholds are exceeded do we hand
  the items to the thread pool via :meth:`ThreadPoolExecutor.map`. ``map``
  preserves input order in its result iterator, which is exactly what we
  need — the caller will later splice each ciphertext back at the
  matching ``field_path``.

Why ``ThreadPoolExecutor`` and not ``asyncio``?
-----------------------------------------------
The underlying ``cryptography`` ``AESGCM.encrypt``/``decrypt`` calls
release the GIL during the OpenSSL invocation, so threads can actually
run in parallel for the CPU-bound part. ``asyncio`` would not help here
because there is no async I/O to overlap.

Counters
--------
``_serial_calls`` and ``_parallel_calls`` increment once per
:meth:`encrypt_many` invocation (not once per item) so tests can verify
which path was taken. They are exposed via :attr:`is_parallel_pool_active`
(historical name; the helper accessors below are clearer to read in tests).
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable

from src.crypto.schema import EncryptedField


@dataclass
class _EncItem:
    """One unit of work for the parallel encryptor.

    Carries the minimum the encrypt function needs to splice the result
    back into the log later: the dotted path identifies the leaf, the
    bytes are the already-serialized plaintext (we serialize via
    ``str(value).encode("utf-8")`` in the caller — see
    :class:`src.processor.log_processor.LogProcessor`), and the
    ``field_type`` is carried through into the resulting
    :class:`EncryptedField` for downstream tooling.
    """

    field_path: str
    plaintext: bytes
    field_type: str


class ParallelEncryptor:
    """Threshold-gated dispatcher around a :class:`ThreadPoolExecutor`.

    Parameters
    ----------
    thread_pool_size : int
        Maximum worker threads the pool may spawn. Set this from
        :attr:`src.settings.Settings.thread_pool_size` in production
        wiring (default 4) — that's the right magnitude for a typical
        log-encryption workload where each item is microseconds.
    threshold_fields : int
        Minimum batch length to consider the parallel path. Below this
        we always run serially.
    threshold_bytes : int
        Minimum total plaintext byte count to consider the parallel
        path. Below this we always run serially.

    Notes
    -----
    BOTH thresholds must be exceeded for the parallel branch to fire.
    A 50-field batch of 10-byte plaintexts (small total) stays serial;
    a 2-field batch of 100KB plaintexts (few fields) also stays serial.
    The dispatch cost only pays off when there is enough total work
    AND enough work units to spread.
    """

    def __init__(
        self,
        *,
        thread_pool_size: int,
        threshold_fields: int,
        threshold_bytes: int,
    ) -> None:
        # One pool per ParallelEncryptor — never one-per-call. The pool
        # is cheap when idle and the OS thread lifecycle dwarfs the
        # encrypt time, so reuse is mandatory.
        self._pool = ThreadPoolExecutor(
            max_workers=thread_pool_size, thread_name_prefix="enc-"
        )
        self._threshold_fields = threshold_fields
        self._threshold_bytes = threshold_bytes
        # Counters expose which branch fired. Tests assert against them
        # to confirm the threshold logic without timing-dependent flakes.
        self._serial_calls: int = 0
        self._parallel_calls: int = 0

    # -- public ----------------------------------------------------------

    def encrypt_many(
        self,
        items: list[_EncItem],
        encrypt_fn: Callable[[_EncItem], EncryptedField],
    ) -> list[EncryptedField]:
        """Encrypt every item, returning results in input order.

        The dispatch path is decided per-call based on the current
        thresholds. ``encrypt_fn`` must be safe to call from a worker
        thread — :class:`src.crypto.aesgcm.AESGCMEncryptor` is
        thread-safe (the underlying ``AESGCM`` releases the GIL), so
        the default wiring meets that contract.

        Parameters
        ----------
        items : list[_EncItem]
            Work units. Empty list returns an empty list (still counted
            as a serial call so tests can detect zero-work invocations).
        encrypt_fn : Callable[[_EncItem], EncryptedField]
            The per-item encrypt closure. The :class:`LogProcessor`
            builds this with a captured ``active.encryptor`` so all
            items in this batch share the same DEK.

        Returns
        -------
        list[EncryptedField]
            Same length and order as ``items``. ``ThreadPoolExecutor.map``
            preserves input order, so the parallel branch's contract
            matches the serial branch exactly.
        """
        n = len(items)
        total_bytes = sum(len(item.plaintext) for item in items)

        if (
            n < self._threshold_fields
            or total_bytes < self._threshold_bytes
        ):
            # Serial path: no submit overhead, runs on the caller thread.
            self._serial_calls += 1
            return [encrypt_fn(item) for item in items]

        # Parallel path: submit to the pool and gather in order.
        self._parallel_calls += 1
        return list(self._pool.map(encrypt_fn, items))

    def close(self) -> None:
        """Shut down the underlying pool.

        ``wait=False`` so a slow shutdown doesn't block API response
        cycles; in-flight tasks finish on their own thread. Callers
        that want a clean drain should ``close()`` and then ``join`` /
        wait externally.
        """
        self._pool.shutdown(wait=False)

    # -- introspection (used by tests) ----------------------------------

    @property
    def serial_calls(self) -> int:
        """Total :meth:`encrypt_many` invocations that took the serial path."""
        return self._serial_calls

    @property
    def parallel_calls(self) -> int:
        """Total :meth:`encrypt_many` invocations that took the parallel path."""
        return self._parallel_calls

    @property
    def is_parallel_pool_active(self) -> bool:
        """``True`` iff the parallel branch has fired at least once.

        Historical accessor kept for symmetry with the spec; the
        granular counters above are the recommended way to assert.
        """
        return self._parallel_calls > 0
