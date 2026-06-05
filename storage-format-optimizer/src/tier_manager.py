"""Recency- and frequency-aware storage tiering (Feature D).

:class:`TierManager` maps a partition's access statistics onto a storage
:class:`~src.models.Tier` (HOT / WARM / COLD). The defining idea — and the
reason this is a *manager* rather than a one-line age check — is that tiering is
driven by **recency *and* frequency, not age alone**:

* a freshly written partition that is also being read stays **HOT**;
* an old partition that nobody has touched in a day drops to **COLD**;
* everything in between is **WARM**.

Design notes:
    * **Pure & deterministic.** All time comparisons go through an injected
      ``clock`` (or an explicit ``now`` argument). The only place
      :func:`time.time` is referenced is the default value of ``clock``; the
      decision logic never reads the wall clock implicitly, so tests are fully
      reproducible.
    * **Explainable.** :meth:`TierManager.tier_for_with_reason` returns the
      chosen tier alongside a short human-readable string, so the dashboard and
      logs can show *why* a partition landed where it did.
    * **Stdlib only** (``time``, ``typing``) — import-light and trivially
      testable.

The three input signals are:
    ``age_seconds``
        How long ago the partition's time-bucket began (supplied by the caller,
        which owns the bucket→wall-clock mapping). Drives the "young vs. old"
        axis.
    ``reads_per_min``
        A cheap *lifetime-average* read-rate proxy, ``reads / minutes_alive``.
        It deliberately trades precision for simplicity: it is monotone in total
        reads and needs no rolling window, which keeps the manager stateless.
    ``idle``
        Seconds since the partition was last touched (read *or* write), i.e.
        ``now - last_access``. Drives the "active vs. dormant" axis.
"""
from __future__ import annotations

import time
from typing import Callable

from src.models import Tier
from src.pattern_tracker import PartitionAccessStats

__all__ = ["TierManager"]


class TierManager:
    """Decide a partition's :class:`~src.models.Tier` from its access stats.

    The manager holds only configuration (the three thresholds and a clock); it
    keeps no per-partition state, so a single instance can classify every
    partition for every tenant. Each call is a pure function of the supplied
    ``stats``/``age_seconds`` and the current time.
    """

    def __init__(
        self,
        *,
        hot_max_age_seconds: float,
        cold_min_age_seconds: float,
        hot_min_reads_per_min: float,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Create a tier manager.

        Args:
            hot_max_age_seconds: Upper bound on ``age_seconds`` for a partition
                to still qualify as HOT, and the idle ceiling below which a
                young partition counts as "recently active". Mirrors
                ``settings.tier_hot_max_age_seconds`` (default ``3600``).
            cold_min_age_seconds: Lower bound on ``age_seconds`` (and on idle)
                before a partition may be demoted to COLD. Mirrors
                ``settings.tier_cold_min_age_seconds`` (default ``86400``).
            hot_min_reads_per_min: Lifetime-average read rate at or above which
                a partition is considered actively read. Mirrors
                ``settings.tier_hot_min_reads_per_min`` (default ``1.0``).
            clock: Zero-argument callable returning the current time as a float.
                Defaults to :func:`time.time`; tests inject a controllable
                clock. Used only when :meth:`tier_for` is called without an
                explicit ``now``.
        """
        self._hot_max_age_seconds = hot_max_age_seconds
        self._cold_min_age_seconds = cold_min_age_seconds
        self._hot_min_reads_per_min = hot_min_reads_per_min
        self._clock = clock

    @staticmethod
    def _reads_per_min(stats: PartitionAccessStats, age_seconds: float) -> float:
        """Lifetime-average read rate, ``reads / minutes_alive``.

        The denominator is floored at one minute so a brand-new partition that
        has already taken a few reads is not credited with an absurdly high
        rate. This is intentionally a coarse frequency proxy — it needs no
        rolling state and is monotone in total reads.
        """
        minutes_alive = max(age_seconds / 60.0, 1.0)
        return stats.reads / minutes_alive

    def _idle_seconds(self, stats: PartitionAccessStats, now: float) -> float:
        """Seconds since the partition was last touched (read or write).

        A partition that has never been accessed (``last_access == 0``) is
        treated as maximally idle (``now``), so it is never mistaken for active.
        The result is clamped at ``0.0`` to guard against a clock that has been
        moved backwards relative to a recorded ``last_access``.
        """
        if stats.last_access <= 0.0:
            return now
        return max(0.0, now - stats.last_access)

    def tier_for(
        self,
        stats: PartitionAccessStats,
        *,
        age_seconds: float,
        now: float | None = None,
    ) -> Tier:
        """Return the :class:`~src.models.Tier` for one partition.

        See :meth:`tier_for_with_reason` for the full decision logic; this is a
        thin wrapper that discards the explanation.

        Args:
            stats: The partition's current access statistics.
            age_seconds: Seconds since the partition's time-bucket began.
            now: Current time; falls back to the injected clock when ``None``.
        """
        tier, _reason = self.tier_for_with_reason(
            stats, age_seconds=age_seconds, now=now
        )
        return tier

    def tier_for_with_reason(
        self,
        stats: PartitionAccessStats,
        *,
        age_seconds: float,
        now: float | None = None,
    ) -> tuple[Tier, str]:
        """Return ``(tier, reason)`` for one partition.

        Decision logic (recency **and** frequency, evaluated HOT → COLD → WARM):

        * **HOT** — the partition is *young* and *active*::

              age_seconds <= hot_max_age_seconds
              AND (reads_per_min >= hot_min_reads_per_min OR idle <= hot_max_age_seconds)

          i.e. recent enough to live in the bucket window, and either read
          frequently over its life or touched within that same window.
        * **COLD** — the partition is *old* and *dormant*::

              age_seconds >= cold_min_age_seconds
              AND reads_per_min < hot_min_reads_per_min
              AND idle >= hot_max_age_seconds

          Old, not read at any meaningful rate, and not touched for at least the
          HOT window. (The idle floor is relaxed to ``hot_max_age_seconds``
          rather than ``cold_min_age_seconds`` so that an old-but-quiet
          partition can settle to COLD without needing a *full* cold-age span of
          silence.)
        * **WARM** — everything else: middling age, or old-but-recently-touched,
          or young-but-quiet.

        Args:
            stats: The partition's current access statistics.
            age_seconds: Seconds since the partition's time-bucket began.
            now: Current time; falls back to the injected clock when ``None``.

        Returns:
            A ``(Tier, reason)`` tuple where ``reason`` is a short, explainable
            description of which rule fired.
        """
        now = now if now is not None else self._clock()
        reads_per_min = self._reads_per_min(stats, age_seconds)
        idle = self._idle_seconds(stats, now)

        young = age_seconds <= self._hot_max_age_seconds
        old = age_seconds >= self._cold_min_age_seconds
        frequently_read = reads_per_min >= self._hot_min_reads_per_min
        recently_touched = idle <= self._hot_max_age_seconds

        if young and (frequently_read or recently_touched):
            reason = (
                f"HOT: age {age_seconds:.0f}s <= {self._hot_max_age_seconds:.0f}s and "
                + (
                    f"reads {reads_per_min:.2f}/min >= "
                    f"{self._hot_min_reads_per_min:.2f}/min"
                    if frequently_read
                    else f"idle {idle:.0f}s <= {self._hot_max_age_seconds:.0f}s"
                )
            )
            return Tier.HOT, reason

        if old and not frequently_read and idle >= self._hot_max_age_seconds:
            reason = (
                f"COLD: age {age_seconds:.0f}s >= {self._cold_min_age_seconds:.0f}s, "
                f"reads {reads_per_min:.2f}/min < {self._hot_min_reads_per_min:.2f}/min, "
                f"idle {idle:.0f}s >= {self._hot_max_age_seconds:.0f}s"
            )
            return Tier.COLD, reason

        reason = (
            f"WARM: age {age_seconds:.0f}s, reads {reads_per_min:.2f}/min, "
            f"idle {idle:.0f}s (neither HOT nor COLD)"
        )
        return Tier.WARM, reason
