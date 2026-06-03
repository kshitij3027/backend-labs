"""In-memory metrics aggregator for the adaptive storage-format optimizer.

Collects everything the dashboard and ``GET /api/stats`` need: per-format query
latency / throughput, rolling ingest rate, the latest storage + compression
snapshot, migration activity, and capped time-series for the live charts. It
holds **no** product state — the manifest is the source of truth for partition
layout — only derived, observational counters.

Design notes:

* **Everything is bounded.** All sample buffers and the time-series are
  ``collections.deque`` with a ``maxlen``, so memory is O(1) regardless of how
  long the process runs. This is one of the levers that keeps the service under
  the 512 MB Compose limit (see ``plan.md`` §"Risks", memory bullet).
* **The clock is injected.** Every time-dependent path (rolling ingest rate,
  default event timestamps) reads from ``self._clock``; the module never calls
  :func:`time.time` directly except as the *default* clock factory. This keeps
  the aggregator deterministically testable.
* **Format-agnostic inputs.** Recording methods accept either a :class:`Format`
  enum or its bare ``.value`` string and normalise to the string key, so callers
  don't have to import the enum.

The shape returned by :meth:`Metrics.snapshot` is the ``StatsResponse`` body
described in ``plan.md`` §"API + WebSocket"; the ``formats.distribution`` field
is deliberately left empty here and filled by the stats route from the manifest.
"""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque

__all__ = ["Metrics", "StorageSnapshot"]

# --- buffer caps (bounded growth) ---
# Recent raw latency samples retained per format for percentile math. 1000 is
# plenty for stable p50/p90 while staying tiny in memory.
_LATENCY_CAP = 1000
# Ingest events retained for the rolling rate window. Generous so a high ingest
# rate within the window is not truncated, but still bounded.
_INGEST_CAP = 10_000
# Most-recent migration records surfaced on the dashboard.
_MIGRATION_RECENT_CAP = 20

# Canonical format keys, in dashboard display order. Mirrors ``Format`` values.
_FORMATS: tuple[str, ...] = ("row", "columnar", "hybrid")


def _percentile(samples: list[float], q: float) -> float:
    """Return the ``q`` quantile of ``samples`` using the nearest-rank method.

    ``q`` is a fraction in ``[0, 1]`` (e.g. ``0.9`` for p90). The rank is
    ``ceil(q * N)`` clamped to ``[1, N]``; the value at that 1-based rank of the
    sorted samples is returned. An empty input yields ``0.0``.

    Nearest-rank is chosen over interpolation for simplicity and because it
    needs no special-casing of tiny samples.
    """
    n = len(samples)
    if n == 0:
        return 0.0
    rank = math.ceil(q * n)
    # Clamp into the valid 1-based range, then index 0-based.
    rank = max(1, min(rank, n))
    ordered = sorted(samples)
    return float(ordered[rank - 1])


def _fmt_key(fmt: Any) -> str:
    """Normalise a format argument to its canonical string key.

    Accepts a :class:`Format` enum (whose ``.value`` is the key), a bare string,
    or anything stringifiable. Enums are unwrapped via their ``value`` attribute;
    everything else is coerced with :func:`str`.
    """
    value = getattr(fmt, "value", fmt)
    return str(value)


@dataclass
class StorageSnapshot:
    """Latest observed on-disk storage totals.

    ``by_format`` maps each canonical format key to its byte total; the two
    aggregate fields hold the compressed total and the estimated uncompressed
    size used to derive the compression ratio.
    """

    by_format: dict[str, int] = field(default_factory=dict)
    total_bytes: int = 0
    uncompressed_estimate_bytes: int = 0


class Metrics:
    """Bounded, in-memory aggregator of runtime metrics.

    All public recording methods are cheap and side-effect free beyond updating
    bounded counters/deques, so they are safe to call on the hot ingest/query
    path. Read methods (:meth:`ingest_eps`, :meth:`compression_ratio`,
    :meth:`analytical_speedup_vs_row`, :meth:`snapshot`) derive their values
    lazily from the current state.
    """

    def __init__(
        self,
        *,
        history_points: int = 60,
        clock: Callable[[], float] = time.time,
        window_seconds: float = 60.0,
    ) -> None:
        """Initialise an empty aggregator.

        :param history_points: cap on each time-series deque (number of retained
            points); mirrors ``settings.metrics_history_points``.
        :param clock: zero-arg callable returning seconds (injected for tests).
        :param window_seconds: width of the rolling ingest-rate window.
        """
        self._clock = clock
        self._window_seconds = float(window_seconds)
        self._history_points = int(history_points)

        # Per-format raw latency samples (all query classes).
        self._latency: dict[str, Deque[float]] = {
            f: deque(maxlen=_LATENCY_CAP) for f in _FORMATS
        }
        # Per-format ANALYTICAL-only latency samples — drives the speedup metric.
        self._analytical_latency: dict[str, Deque[float]] = {
            f: deque(maxlen=_LATENCY_CAP) for f in _FORMATS
        }
        # Per-format counters.
        self._query_count: dict[str, int] = {f: 0 for f in _FORMATS}
        self._rows_scanned: dict[str, int] = {f: 0 for f in _FORMATS}
        self._rowgroups_skipped: dict[str, int] = {f: 0 for f in _FORMATS}

        # Rolling ingest: (timestamp, n) events + lifetime total.
        self._ingest_events: Deque[tuple[float, int]] = deque(maxlen=_INGEST_CAP)
        self._total_entries: int = 0

        # Latest storage snapshot.
        self._storage = StorageSnapshot()

        # Migration counters + most-recent records.
        self._migrations_completed: int = 0
        self._migrations_failed: int = 0
        self._migrations_in_flight: int = 0
        self._migrations_recent: Deque[dict[str, Any]] = deque(
            maxlen=_MIGRATION_RECENT_CAP
        )

        # Time-series (capped at history_points) for the live charts.
        self._series_ingest_eps: Deque[float] = deque(maxlen=self._history_points)
        self._series_row_p90: Deque[float] = deque(maxlen=self._history_points)
        self._series_columnar_p90: Deque[float] = deque(maxlen=self._history_points)
        self._series_hybrid_p90: Deque[float] = deque(maxlen=self._history_points)
        self._series_compression_ratio: Deque[float] = deque(
            maxlen=self._history_points
        )

    # ------------------------------------------------------------------ #
    # Recording (hot path)
    # ------------------------------------------------------------------ #
    def record_query(
        self,
        fmt: str,
        elapsed_ms: float,
        *,
        query_class: str | None = None,
        rows_scanned: int = 0,
        rowgroups_skipped: int = 0,
    ) -> None:
        """Record a single completed query against a format.

        Appends ``elapsed_ms`` to the format's latency buffer (and to the
        analytical-only buffer when ``query_class == "analytical"``), and bumps
        the per-format query / rows-scanned / rowgroups-skipped counters.

        ``fmt`` may be a :class:`Format` enum or its ``.value``; unknown keys are
        recorded under their own bucket so nothing is silently dropped.
        """
        key = _fmt_key(fmt)
        ms = float(elapsed_ms)
        self._latency.setdefault(key, deque(maxlen=_LATENCY_CAP)).append(ms)
        if query_class == "analytical":
            self._analytical_latency.setdefault(
                key, deque(maxlen=_LATENCY_CAP)
            ).append(ms)
        self._query_count[key] = self._query_count.get(key, 0) + 1
        self._rows_scanned[key] = self._rows_scanned.get(key, 0) + int(rows_scanned)
        self._rowgroups_skipped[key] = (
            self._rowgroups_skipped.get(key, 0) + int(rowgroups_skipped)
        )

    def record_ingest(self, n: int, *, ts: float | None = None) -> None:
        """Record an ingest of ``n`` entries at time ``ts`` (default: now).

        The event is appended to the rolling window and added to the lifetime
        total. ``ts`` falls back to ``self._clock()`` when not supplied.
        """
        when = self._clock() if ts is None else float(ts)
        count = int(n)
        self._ingest_events.append((when, count))
        self._total_entries += count

    def set_storage(
        self,
        by_format: dict,
        total_bytes: int,
        uncompressed_estimate_bytes: int,
    ) -> None:
        """Replace the latest storage snapshot.

        ``by_format`` keys are coerced to strings (so :class:`Format` enums are
        accepted) and their byte values to ``int``.
        """
        self._storage = StorageSnapshot(
            by_format={_fmt_key(k): int(v) for k, v in by_format.items()},
            total_bytes=int(total_bytes),
            uncompressed_estimate_bytes=int(uncompressed_estimate_bytes),
        )

    def record_migration(
        self,
        *,
        tenant: str,
        partition: str,
        from_fmt: str,
        to_fmt: str,
        ok: bool,
        reason: str,
        at: float | None = None,
    ) -> None:
        """Record the outcome of a completed migration attempt.

        Increments the completed or failed counter based on ``ok`` and appends a
        compact record to the bounded ``recent`` buffer. ``at`` defaults to the
        injected clock. Format arguments are normalised to their string keys.
        """
        when = self._clock() if at is None else float(at)
        if ok:
            self._migrations_completed += 1
        else:
            self._migrations_failed += 1
        self._migrations_recent.append(
            {
                "tenant": tenant,
                "partition": partition,
                "from": _fmt_key(from_fmt),
                "to": _fmt_key(to_fmt),
                "reason": reason,
                "at": when,
            }
        )

    def migration_started(self) -> None:
        """Mark a migration as started (increments the in-flight gauge)."""
        self._migrations_in_flight += 1

    def migration_finished(self) -> None:
        """Mark a migration as finished (decrements in-flight, floored at 0)."""
        self._migrations_in_flight = max(0, self._migrations_in_flight - 1)

    # ------------------------------------------------------------------ #
    # Derived reads
    # ------------------------------------------------------------------ #
    def ingest_eps(self) -> float:
        """Return the rolling ingest rate in entries per second.

        Sums ``n`` over all events whose timestamp is within the last
        ``window_seconds`` (relative to the current clock) and divides by the
        fixed window width. This is a simple *windowed* rate: it answers "how
        many entries arrived in the last N seconds, per second", and naturally
        decays to 0 once ingest stops and the window empties.
        """
        if self._window_seconds <= 0:
            return 0.0
        cutoff = self._clock() - self._window_seconds
        recent_total = sum(n for ts, n in self._ingest_events if ts >= cutoff)
        return recent_total / self._window_seconds

    def compression_ratio(self) -> float:
        """Return the overall compression ratio (uncompressed / compressed).

        A ratio ``> 1`` means data is smaller on disk than its estimated
        uncompressed size. Returns ``1.0`` when nothing is stored yet
        (``total_bytes == 0``), avoiding division by zero.
        """
        total = self._storage.total_bytes
        if total <= 0:
            return 1.0
        return self._storage.uncompressed_estimate_bytes / total

    def analytical_speedup_vs_row(self) -> float:
        """Return how much faster columnar serves analytical queries than row.

        Computed as ``row_analytical_p50 / columnar_analytical_p50`` over the
        analytical-only latency buffers. A value ``>= 1`` means columnar is
        faster (lower latency) for analytical access. Returns ``1.0`` when either
        side lacks samples, and guards against a zero or empty columnar p50.
        """
        row_samples = list(self._analytical_latency.get("row", ()))
        col_samples = list(self._analytical_latency.get("columnar", ()))
        if not row_samples or not col_samples:
            return 1.0
        row_p50 = _percentile(row_samples, 0.5)
        col_p50 = _percentile(col_samples, 0.5)
        if col_p50 <= 0:
            return 1.0
        return row_p50 / col_p50

    def _throughput(self, key: str) -> float:
        """Return rows scanned per second of total query time for ``key``.

        Aggregate, lifetime throughput: total rows scanned divided by the summed
        query latency (converted from ms to s). Returns ``0.0`` when no time has
        been spent (no samples), avoiding division by zero.
        """
        total_ms = sum(self._latency.get(key, ()))
        if total_ms <= 0:
            return 0.0
        return self._rows_scanned.get(key, 0) / (total_ms / 1000.0)

    # ------------------------------------------------------------------ #
    # Time-series
    # ------------------------------------------------------------------ #
    def append_series_point(self) -> None:
        """Append one point to every time-series deque.

        Captures the current rolling ingest rate, each format's p90 latency, and
        the current compression ratio. Intended to be called periodically by the
        WebSocket broadcast loop (wired in a later commit). Each deque is capped
        at ``history_points``, so old points fall off the left automatically.
        """
        self._series_ingest_eps.append(self.ingest_eps())
        self._series_row_p90.append(_percentile(list(self._latency["row"]), 0.9))
        self._series_columnar_p90.append(
            _percentile(list(self._latency["columnar"]), 0.9)
        )
        self._series_hybrid_p90.append(
            _percentile(list(self._latency["hybrid"]), 0.9)
        )
        self._series_compression_ratio.append(self.compression_ratio())

    def series(self) -> dict[str, list[float]]:
        """Return the current time-series as plain lists (for the WS payload)."""
        return {
            "ingest_eps": list(self._series_ingest_eps),
            "row_p90": list(self._series_row_p90),
            "columnar_p90": list(self._series_columnar_p90),
            "hybrid_p90": list(self._series_hybrid_p90),
            "compression_ratio": list(self._series_compression_ratio),
        }

    # ------------------------------------------------------------------ #
    # Snapshot
    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        """Return the ``StatsResponse``-shaped metrics document.

        Top-level keys are exactly ``storage``, ``formats``, ``performance``,
        ``migrations``, and ``ingest`` (see ``plan.md`` §"API + WebSocket").
        ``formats.distribution`` is intentionally empty here — the stats route
        fills it from the manifest. ``storage.by_format`` always exposes all
        three canonical format keys (defaulting to ``0``).
        """
        by_format_storage = {
            f: int(self._storage.by_format.get(f, 0)) for f in _FORMATS
        }

        performance_by_format: dict[str, dict[str, float]] = {}
        for f in _FORMATS:
            samples = list(self._latency.get(f, ()))
            performance_by_format[f] = {
                "p50": _percentile(samples, 0.5),
                "p90": _percentile(samples, 0.9),
                "throughput": self._throughput(f),
                "count": self._query_count.get(f, 0),
            }

        return {
            "storage": {
                "total_bytes": int(self._storage.total_bytes),
                "uncompressed_estimate_bytes": int(
                    self._storage.uncompressed_estimate_bytes
                ),
                "compression_ratio": self.compression_ratio(),
                "by_format": by_format_storage,
            },
            "formats": {
                # Filled by the stats route from the manifest; empty here.
                "distribution": {},
            },
            "performance": {
                "by_format": performance_by_format,
                "analytical_speedup_vs_row": self.analytical_speedup_vs_row(),
            },
            "migrations": {
                "completed": self._migrations_completed,
                "failed": self._migrations_failed,
                "in_flight": self._migrations_in_flight,
                "recent": list(self._migrations_recent),
            },
            "ingest": {
                "entries_per_sec": self.ingest_eps(),
                "total_entries": self._total_entries,
            },
        }
