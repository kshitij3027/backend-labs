"""Per-log-type filter manager — routing, rotation generations, persistence.

This module is where the project's three independent membership questions
become three independent filters, and where the system's entire concurrency
story is decided.

The routing map ("log types" hierarchy)
---------------------------------------
:meth:`Settings.filter_configs` declares the spec's per-type filters —
``error_logs`` (1M @ p=0.01), ``access_logs`` (5M @ p=0.05),
``security_logs`` (100K @ p=0.001) — and :class:`FilterManager` builds one
:class:`~src.scalable.ScalableBloomFilter` per entry (Extended B adaptive
sizing: each starts at its configured slice-0 capacity and grows
geometrically if the estimate was low). Every ``add``/``query`` is routed by
filter name, so a key seen in ``error_logs`` is *not* claimed by
``access_logs``: cross-type queries answer ``definitely_not_exist``. This
routing map is the "log types" level of Extended B's filter hierarchy; the
generations below are its "time periods" level.

Rotation generations ("time periods" hierarchy)
-----------------------------------------------
Each managed filter holds up to two generations:

* ``current`` — the generation receiving all new adds, and
* ``previous`` — the generation that was current before the last rotation.

:meth:`FilterManager.rotate` demotes ``current`` to ``previous`` and installs
a fresh, empty filter as ``current``; :meth:`FilterManager.rotate_if_due`
does so automatically once a generation is older than
``rotation_max_age_seconds`` (0 disables rotation). Queries check ``current``
first and fall back to ``previous``, so keys added before a rotation stay
answerable — **zero false negatives hold across exactly one rotation
boundary**. The caveat: after TWO rotations the oldest generation is
discarded and its keys read ``definitely_not_exist`` again. That is the
deliberate trade for bounded memory and a self-resetting false-positive
rate, and it is exactly right for "have we seen this log entry recently?"
dedup; the authoritative full history lives in the sqlite tier (C10), never
in the filters.

Lock discipline (the concurrency keystone)
------------------------------------------
Every managed filter owns one ``threading.Lock`` and *all* access to its
generations goes through it. The rules, relied on by C8's handlers:

* A real ``threading.Lock`` (not ``asyncio.Lock``): callers are both the
  asyncio event-loop thread (hot ``async def`` add/query handlers — these
  call the manager's sync methods inline, no await, so the lock is **never
  held across an await**) and AnyIO threadpool workers (sync bulk handlers).
* Critical sections are µs-scale: one hash-and-probe, or one in-memory
  serialize. Slow work — fresh-filter allocation in :meth:`rotate`, file
  fsync in :meth:`save_all` — happens *outside* the lock.
* At most one lock is held at any instant: filter locks never nest with
  each other or with the metrics locks (metrics are recorded after the
  filter lock is released), so there is no lock-ordering story to get wrong.
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from src.metrics import MetricsRegistry
from src.persistence import dumps_scalable, load_scalable, write_atomic
from src.scalable import ScalableBloomFilter
from src.settings import Settings

logger = logging.getLogger(__name__)

#: Exact spec wording for a positive membership answer ("yes" is only
#: probably right — bounded false positives).
CONFIDENCE_POSITIVE = "probably_exists"

#: Exact spec wording for a negative membership answer ("no" is always
#: right — zero false negatives within the two live generations).
CONFIDENCE_NEGATIVE = "definitely_not_exist"


@dataclass
class ManagedFilter:
    """One named filter: its two generations plus the lock guarding them.

    ``created_at`` is the epoch (per the manager's injected clock) at which
    the *current* generation was born — construction or the latest rotation —
    and is what :meth:`FilterManager.rotate_if_due` ages against.
    ``rotations`` counts rotations performed in this process's lifetime.
    """

    name: str
    current: ScalableBloomFilter
    previous: ScalableBloomFilter | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)
    created_at: float = field(default_factory=time.time)
    rotations: int = 0


class FilterManager:
    """Routes adds/queries to per-log-type filters and owns their lifecycle.

    Built from :meth:`Settings.filter_configs` — one
    :class:`~src.scalable.ScalableBloomFilter` per named filter, each with
    its own ``threading.Lock`` (module docstring). The ``clock`` parameter
    exists so rotation aging is testable with a fake clock; operation
    *durations* always come from ``time.perf_counter`` regardless.
    """

    def __init__(
        self,
        settings: Settings,
        metrics: MetricsRegistry | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._clock = clock
        #: Per-filter operation metrics; shared with the API layer (C8),
        #: which reads the same registry the manager records into.
        self.metrics = metrics if metrics is not None else MetricsRegistry()
        self._filters: dict[str, ManagedFilter] = {
            name: ManagedFilter(
                name=name,
                current=self._new_filter(name),
                created_at=clock(),
            )
            for name in settings.filter_configs()
        }

    # ------------------------------------------------------------------ #
    # construction helpers                                               #
    # ------------------------------------------------------------------ #

    def _new_filter(self, name: str) -> ScalableBloomFilter:
        """Build a fresh generation for ``name`` from the settings config.

        Used both at construction and on every rotation, so a rotated-in
        generation always carries the *currently configured* sizing — the
        one place filter parameters are turned into filters.
        """
        capacity, fp_rate = self._settings.filter_configs()[name]
        return ScalableBloomFilter(
            initial_capacity=capacity,
            target_fp_rate=fp_rate,
            growth=self._settings.sbf_growth_factor,
            tightening=self._settings.sbf_tightening_ratio,
        )

    # ------------------------------------------------------------------ #
    # lookup                                                             #
    # ------------------------------------------------------------------ #

    @property
    def names(self) -> tuple[str, ...]:
        """Registered filter names, in ``filter_configs()`` declaration order."""
        return tuple(self._filters)

    def get(self, name: str) -> ManagedFilter:
        """Return the managed filter called ``name``; KeyError if unknown.

        The API layer (C8) constrains ``log_type`` with a ``Literal`` and
        422s unknown types before they ever reach here, so a KeyError out of
        this method means an internal caller bug — let it surface loudly.
        """
        try:
            return self._filters[name]
        except KeyError:
            raise KeyError(
                f"unknown filter {name!r}; known filters: {sorted(self._filters)}"
            ) from None

    # ------------------------------------------------------------------ #
    # hot path: add / query                                              #
    # ------------------------------------------------------------------ #

    def add(self, name: str, key: str) -> tuple[bool, float]:
        """Admit ``key`` into ``name``'s current generation.

        Returns ``(added, duration_ms)`` where ``added`` is the underlying
        SBF dedup answer (False when the key already reads as present in the
        current generation). The duration covers lock wait plus the filter
        operation — the latency a caller actually experienced — and is
        recorded into this filter's metrics after the lock is released.

        Note adds deliberately do not consult ``previous``: re-adding a key
        that only survives in the old generation lands it in ``current``,
        which is exactly what refreshes its lifetime across rotations.
        """
        mf = self.get(name)
        start = time.perf_counter()
        with mf.lock:
            added = mf.current.add(key)
        duration_ms = (time.perf_counter() - start) * 1000.0
        self.metrics.get(name).record_add(duration_ms)
        return added, duration_ms

    def query(self, name: str, key: str) -> tuple[bool, str, float]:
        """Answer "have we seen ``key`` in ``name``?" across both generations.

        Checks ``current`` first and only probes ``previous`` on a negative
        (a positive anywhere is final), so keys added before the last
        rotation remain answerable — see the module docstring for the
        generation semantics. Returns ``(might_exist, confidence,
        duration_ms)`` with ``confidence`` being exactly
        ``"probably_exists"`` or ``"definitely_not_exist"`` per the spec.
        """
        mf = self.get(name)
        start = time.perf_counter()
        with mf.lock:
            might_exist = mf.current.might_contain(key)
            if not might_exist and mf.previous is not None:
                might_exist = mf.previous.might_contain(key)
        duration_ms = (time.perf_counter() - start) * 1000.0
        confidence = CONFIDENCE_POSITIVE if might_exist else CONFIDENCE_NEGATIVE
        self.metrics.get(name).record_query(duration_ms, positive=might_exist)
        return might_exist, confidence, duration_ms

    # ------------------------------------------------------------------ #
    # rotation (Extended B: time periods)                                #
    # ------------------------------------------------------------------ #

    def _swap_generation(self, mf: ManagedFilter, fresh: ScalableBloomFilter) -> None:
        """Install ``fresh`` as the current generation. Caller holds ``mf.lock``.

        The old current becomes ``previous`` (still queryable — zero false
        negatives across this one boundary); whatever was ``previous``
        before is dropped and its keys are forgotten (module docstring
        caveat). ``created_at`` restarts the generation age clock.
        """
        mf.previous = mf.current
        mf.current = fresh
        mf.created_at = self._clock()
        mf.rotations += 1

    def rotate(self, name: str) -> None:
        """Start a new generation for ``name``: previous ← current ← fresh.

        The two-generation hierarchy is Extended B's "time periods" level
        (the routing map being its "log types" level): the demoted
        generation stays queryable until the *next* rotation, so rotation
        never instantly forgets — it ages keys out one full period later.
        The fresh filter is allocated *before* taking the lock (slice-0 for
        a 5M-key filter is a multi-MB zeroed bitarray); the lock guards only
        the pointer swap.
        """
        mf = self.get(name)
        fresh = self._new_filter(name)
        with mf.lock:
            self._swap_generation(mf, fresh)
        logger.info(
            "rotated filter %r: generation %d started, previous generation "
            "stays queryable until the next rotation",
            name,
            mf.rotations,
        )

    def rotate_if_due(self) -> list[str]:
        """Rotate every filter whose current generation has reached max age.

        Returns the names rotated (empty when ``rotation_max_age_seconds``
        is 0 — rotation disabled — or nothing is old enough). Rotation
        refreshes ``created_at``, so a just-rotated filter is never due
        again on the immediately following check. The age test is performed
        again under the lock after the fresh filter is built, so a
        concurrent manual ``rotate`` (e.g. C10's FP-breach trigger) cannot
        cause a double rotation that would silently skip a generation.
        """
        max_age = self._settings.rotation_max_age_seconds
        if max_age <= 0:
            return []
        rotated: list[str] = []
        for name, mf in self._filters.items():
            # Unlocked pre-check: skip the multi-MB fresh-filter allocation
            # for filters that are clearly not due (the common case).
            if self._clock() - mf.created_at < max_age:
                continue
            fresh = self._new_filter(name)
            with mf.lock:
                if self._clock() - mf.created_at < max_age:
                    continue  # lost a race with a concurrent rotation
                self._swap_generation(mf, fresh)
            rotated.append(name)
            logger.info(
                "rotated filter %r (age limit %.0fs reached): generation %d started",
                name,
                max_age,
                mf.rotations,
            )
        return rotated

    # ------------------------------------------------------------------ #
    # persistence                                                        #
    # ------------------------------------------------------------------ #

    def save_all(self, data_dir: str | Path) -> None:
        """Snapshot every filter to ``<data_dir>/<name>.bloom`` (+ ``.prev``).

        Per filter: both generations are serialized to bytes *under the
        lock* in one acquisition (``dumps_scalable`` is a µs–ms in-memory
        copy, and grabbing both at once keeps the current/previous pair
        mutually consistent), then written via ``write_atomic`` — tmp +
        fsync + rename — *outside* the lock. Lock hold time stays tiny; the
        slow disk fsync happens lock-free, so the hot add/query path is
        never blocked behind I/O. ``previous`` is saved to
        ``<name>.bloom.prev`` only when a previous generation exists.
        """
        data_dir = Path(data_dir)
        for name, mf in self._filters.items():
            with mf.lock:
                current_blob = dumps_scalable(mf.current)
                previous_blob = (
                    dumps_scalable(mf.previous) if mf.previous is not None else None
                )
            write_atomic(current_blob, data_dir / f"{name}.bloom")
            if previous_blob is not None:
                write_atomic(previous_blob, data_dir / f"{name}.bloom.prev")
            logger.debug("saved filter %r snapshot(s) to %s", name, data_dir)

    def load_all(self, data_dir: str | Path) -> dict[str, bool]:
        """Adopt on-disk snapshots at startup; report ``{name: loaded?}``.

        For each filter, ``<name>.bloom`` is loaded and adopted as the
        current generation only if it parses (CRC etc. — corrupt or missing
        files yield ``None`` from :func:`load_scalable`, never an exception)
        AND its ``(initial_capacity, target_fp_rate)`` match the live
        settings — a snapshot taken under a different sizing config is
        rejected with a warning and the fresh empty filter stays, because
        adopting it would silently pin the old sizing forever.
        ``<name>.bloom.prev`` is restored into ``previous`` under the same
        validation. The returned bool per name reflects the *current*
        generation only. Never raises on bad files: snapshots are a
        warm-start optimization, not the system of record.
        """
        data_dir = Path(data_dir)
        results: dict[str, bool] = {}
        for name, mf in self._filters.items():
            current = self._load_validated(name, data_dir / f"{name}.bloom")
            previous = self._load_validated(name, data_dir / f"{name}.bloom.prev")
            with mf.lock:
                if current is not None:
                    mf.current = current
                if previous is not None:
                    mf.previous = previous
            results[name] = current is not None
            if current is not None:
                logger.info(
                    "restored filter %r from %s (count=%d, slices=%d%s)",
                    name,
                    data_dir / f"{name}.bloom",
                    current.count,
                    current.slice_count,
                    "; previous generation restored" if previous is not None else "",
                )
        return results

    def _load_validated(
        self, name: str, path: Path
    ) -> ScalableBloomFilter | None:
        """Load one snapshot and gate it against the live settings config.

        ``None`` means "keep the fresh filter": missing/corrupt files are
        already warned about by :func:`load_scalable`; a config mismatch is
        warned about here (the file is valid, just built for a different
        sizing — likely an operator changed capacity/FP env vars).
        """
        loaded = load_scalable(path)
        if loaded is None:
            return None
        capacity, fp_rate = self._settings.filter_configs()[name]
        if loaded.initial_capacity != capacity or loaded.target_fp_rate != fp_rate:
            logger.warning(
                "snapshot %s rejected: built for capacity=%d fp_rate=%r but "
                "settings now say capacity=%d fp_rate=%r; starting %r fresh",
                path,
                loaded.initial_capacity,
                loaded.target_fp_rate,
                capacity,
                fp_rate,
                name,
            )
            return None
        return loaded

    # ------------------------------------------------------------------ #
    # introspection                                                      #
    # ------------------------------------------------------------------ #

    def stats(self) -> dict[str, dict]:
        """Return per-filter stats: SBF gauges + generation info + op metrics.

        Each entry merges the current generation's
        :meth:`~src.scalable.ScalableBloomFilter.stats` with the manager's
        generation bookkeeping (``previous_count``, ``rotations``,
        ``created_at``, ``generation_age_seconds``, ``memory_bytes_total``
        covering both generations) and the filter's operation metrics under
        ``"ops"``. Filter state is read under the filter lock; the metrics
        snapshot is taken after it is released (one lock at a time). The
        ``/stats`` endpoint (C8) shapes this further.
        """
        now = self._clock()
        out: dict[str, dict] = {}
        for name, mf in self._filters.items():
            with mf.lock:
                merged = mf.current.stats()
                previous_count = mf.previous.count if mf.previous is not None else 0
                previous_memory = (
                    mf.previous.memory_bytes if mf.previous is not None else 0
                )
                rotations = mf.rotations
                created_at = mf.created_at
            merged["name"] = name
            merged["previous_count"] = previous_count
            merged["rotations"] = rotations
            merged["created_at"] = created_at
            merged["generation_age_seconds"] = max(0.0, now - created_at)
            merged["memory_bytes_total"] = merged["memory_bytes"] + previous_memory
            merged["ops"] = self.metrics.get(name).snapshot()
            out[name] = merged
        return out

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"FilterManager(filters={list(self._filters)})"
