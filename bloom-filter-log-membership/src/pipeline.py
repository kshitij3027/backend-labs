"""Two-tier membership pipeline: bloom filters in front of sqlite (Extended C).

The pattern
-----------
:meth:`TwoTierPipeline.ingest` writes BOTH tiers in one call — a sqlite row
(:class:`~src.storage.LogStore`, ground truth) plus the matching per-type
bloom filter (so the filter auto-updates as new logs arrive, Extended C #1).
:meth:`TwoTierPipeline.lookup` then asks the cheap tier first:

* The filter answers "definitely not" → storage is never touched. That is
  the entire speed win (Extended C #2): a µs bit-probe absorbs the lookup
  that would otherwise cost a ms-scale SQL round trip, and for log-dedup
  workloads — where most lookups are misses — that is *most* lookups.
* The filter answers "probably" → storage verifies. A miss there is an
  **observed false positive**: the bounded error a bloom filter trades for
  its memory win, counted both in the pipeline tallies and in the filter's
  own metrics ledger (it surfaces as ``observed_false_positives`` in
  ``/stats``).

What a bloom negative actually proves
-------------------------------------
:meth:`FilterManager.query` ORs the filter's CURRENT and PREVIOUS
generations, so a bloom negative means precisely "this key was not admitted
within the last two generations" — not "never ingested in all of history".
Within that window the short-circuit is safe because a bloom filter never
forgets what it was shown (zero false negatives). Across it there is a
rotation caveat: after TWO rotations the oldest generation's keys are gone
from the bloom tier, so a lookup for such a key could short-circuit as
``source: "bloom_negative"`` even though its row still sits in sqlite. Read
the field accordingly: ``"bloom_negative"`` asserts "definitely not ingested
within the filter's two-generation retention window"; the oracle for
older-than-retention questions is a direct storage query — sqlite remains
ground truth for all time. (The short-circuit also assumes the filter saw
every ingest: :meth:`ingest` writes storage first and the filter second, so
the only divergence window is a crash exactly between those two writes — an
accepted demo-tier caveat, the same gap class every cache-aside system has,
healed for a given key by re-ingesting it.)

The FP-threshold fallback (Extended C #3)
-----------------------------------------
A bloom filter only saves work when it answers "no". As a filter saturates,
its false-positive rate climbs, "no" becomes rare, and nearly every lookup
pays the bit-probe *and* the storage verification — the cheap tier decays
into pure overhead that answers nothing. So every lookup first reads the
CURRENT generation's live fill-based estimate
(:attr:`~src.scalable.ScalableBloomFilter.compound_estimated_fp`); above
``fp_fallback_threshold`` the pipeline skips the filter entirely and serves
ground truth straight from storage. If ``fp_rotate_on_breach`` is set, the
first fallback lookup of a breach episode additionally triggers exactly one
:meth:`FilterManager.rotate` — installing a fresh, empty current generation
whose estimate is ≈0, which restores two-tier service on the very next
lookup. A per-filter ``breach_handled`` flag stays set while the estimate
remains above the threshold and re-arms only once a lookup finds it back
under, so a sustained breach (rotation disabled, or re-saturation outpacing
rotation) can never cause a rotation storm.

Concurrency: counter bumps go through one pipeline-wide ``threading.Lock``
(ns-scale critical sections, threadpool callers). The pipeline lock is never
held while calling into the manager, the metrics, or the store — at most one
lock at a time, the same discipline the manager documents.
"""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass

from src.manager import FilterManager
from src.settings import Settings
from src.storage import LogStore

logger = logging.getLogger(__name__)


def _pct(part: int, whole: int) -> float:
    """Percentage rounded to 2 dp; 0.0 when the denominator is empty."""
    return round(100.0 * part / whole, 2) if whole else 0.0


@dataclass
class PipelineCounters:
    """Per-filter pipeline tallies (guarded by the pipeline's single lock).

    Every lookup lands in exactly one of three buckets: ``fallback_lookups``
    (estimate breached, bloom skipped), ``bloom_negatives`` (two-tier,
    storage skipped), or the remainder — two-tier bloom positives verified
    against storage, which is therefore derivable as ``lookups -
    bloom_negatives - fallback_lookups`` and serves as the observed-FP-rate
    denominator in :meth:`TwoTierPipeline.stats`.
    """

    lookups: int = 0
    bloom_negatives: int = 0
    storage_hits: int = 0
    false_positives: int = 0
    fallback_lookups: int = 0
    rotations_triggered: int = 0
    #: True while the current breach episode has already spent its one
    #: rotation; re-armed (set back to False) by the first lookup that finds
    #: the estimate back under the threshold.
    breach_handled: bool = False


class TwoTierPipeline:
    """Bloom-fronted membership over a :class:`~src.storage.LogStore`.

    Stateless beyond its counters: filters belong to the
    :class:`~src.manager.FilterManager`, rows to the store, thresholds to
    :class:`~src.settings.Settings`. The API layer (C10) exposes
    :meth:`ingest`, :meth:`lookup`, and :meth:`stats` as the ``/pipeline/*``
    endpoints; C11's session tracking reuses the same machinery. Unknown
    filter names raise ``KeyError`` out of the manager — the API's
    ``Literal`` validation makes that unreachable from HTTP, same stance as
    the manager itself.
    """

    def __init__(
        self, manager: FilterManager, store: LogStore, settings: Settings
    ) -> None:
        self._manager = manager
        self._store = store
        self._settings = settings
        self._lock = threading.Lock()
        self._counters: dict[str, PipelineCounters] = {}

    # ------------------------------------------------------------------ #
    # internals                                                          #
    # ------------------------------------------------------------------ #

    def _counters_for(self, log_type: str) -> PipelineCounters:
        """Get-or-create the tallies for ``log_type`` under the pipeline lock."""
        with self._lock:
            counters = self._counters.get(log_type)
            if counters is None:
                counters = PipelineCounters()
                self._counters[log_type] = counters
            return counters

    def _current_estimate(self, log_type: str) -> float:
        """Live compound FP estimate of the CURRENT generation (read under its lock).

        Current generation only, deliberately: the fallback question is "is
        the filter that admits new keys still healthy?" — the demoted
        previous generation takes no writes and rotation cannot cure it
        further, so it gets no vote. This is also what makes
        breach-then-rotate self-healing: the fresh current reads ≈0 and
        two-tier service resumes on the next lookup.
        """
        mf = self._manager.get(log_type)
        with mf.lock:
            return mf.current.compound_estimated_fp

    # ------------------------------------------------------------------ #
    # ingest                                                             #
    # ------------------------------------------------------------------ #

    def ingest(self, log_type: str, log_key: str) -> dict:
        """Write-through admit: storage row first, then the matching filter.

        Extended C #1 — the bloom filter auto-updates alongside every
        storage write, so it can answer for the key from this moment on.
        Storage goes first: if the sqlite insert raises, nothing claims the
        key anywhere, whereas the reverse order could leave the filter
        claiming a key storage never received. ``manager.add`` runs even
        for duplicate rows — it dedups internally, and a key whose filter
        bits were aged out by two rotations gets refreshed back into the
        current generation this way.

        Returns ``{"stored": <new row?>, "bloom_updated": True,
        "duplicate": <row already existed?>}``.
        """
        stored = self._store.insert(log_type, log_key)
        self._manager.add(log_type, log_key)
        return {"stored": stored, "bloom_updated": True, "duplicate": not stored}

    # ------------------------------------------------------------------ #
    # lookup                                                             #
    # ------------------------------------------------------------------ #

    def lookup(self, log_type: str, log_key: str) -> dict:
        """Two-tier membership answer for one key.

        Decision tree:

        1. Read the current generation's live FP estimate. Above
           ``fp_fallback_threshold`` → **fallback**: skip bloom entirely,
           answer from storage, and (``fp_rotate_on_breach`` permitting)
           fire the one rotation this breach episode gets.
        2. Otherwise **two-tier**: ask the filter. A negative is final and
           free — storage untouched. A positive is verified against
           storage; a miss there is an observed false positive.

        Returns the response dict the API serializes: ``found`` /
        ``might_exist`` / ``source`` (``"bloom_negative"`` | ``"storage"``)
        / ``storage_checked`` / ``false_positive`` / ``fallback_active``.
        """
        counters = self._counters_for(log_type)
        estimate = self._current_estimate(log_type)
        if estimate > self._settings.fp_fallback_threshold:
            return self._fallback_lookup(log_type, log_key, counters, estimate)
        return self._two_tier_lookup(log_type, log_key, counters)

    def _fallback_lookup(
        self,
        log_type: str,
        log_key: str,
        counters: PipelineCounters,
        estimate: float,
    ) -> dict:
        """Breached filter: serve ground truth directly, maybe rotate once.

        The rotation decision (check-and-set on ``breach_handled``) happens
        under the pipeline lock so concurrent breached lookups elect exactly
        one trigger; the rotation itself runs after the lock is released
        (one lock at a time — ``rotate`` allocates a fresh filter and takes
        the filter lock).
        """
        found = self._store.exists(log_type, log_key)
        rotate = False
        with self._lock:
            counters.lookups += 1
            counters.fallback_lookups += 1
            if found:
                counters.storage_hits += 1
            if self._settings.fp_rotate_on_breach and not counters.breach_handled:
                counters.breach_handled = True
                counters.rotations_triggered += 1
                rotate = True
        if rotate:
            logger.warning(
                "filter %r live FP estimate %.4f breached threshold %.4f: "
                "bypassing bloom and rotating in a fresh generation",
                log_type,
                estimate,
                self._settings.fp_fallback_threshold,
            )
            self._manager.rotate(log_type)
        return {
            "found": found,
            # Bloom was never consulted; ground truth is the only claim left.
            "might_exist": found,
            "source": "storage",
            "storage_checked": True,
            "false_positive": False,
            "fallback_active": True,
        }

    def _two_tier_lookup(
        self, log_type: str, log_key: str, counters: PipelineCounters
    ) -> dict:
        """Healthy filter: bloom first, storage only on a bloom positive.

        Both branches re-arm ``breach_handled`` — reaching here at all means
        the estimate is back at or under the threshold, so any prior breach
        episode is over and the next one earns a fresh rotation.
        """
        might_exist, _confidence, _duration_ms = self._manager.query(
            log_type, log_key
        )
        if not might_exist:
            with self._lock:
                counters.lookups += 1
                counters.bloom_negatives += 1
                counters.breach_handled = False
            return {
                "found": False,
                "might_exist": False,
                "source": "bloom_negative",
                "storage_checked": False,
                "false_positive": False,
                "fallback_active": False,
            }
        found = self._store.exists(log_type, log_key)
        with self._lock:
            counters.lookups += 1
            if found:
                counters.storage_hits += 1
            else:
                counters.false_positives += 1
            counters.breach_handled = False
        if not found:
            # The filter claimed a key ground truth disowns. Feed the
            # filter's own ledger too, so /stats observed_false_positives
            # tracks what the pipeline actually disproved.
            self._manager.metrics.get(log_type).record_false_positive()
        return {
            "found": found,
            "might_exist": True,
            "source": "storage",
            "storage_checked": True,
            "false_positive": not found,
            "fallback_active": False,
        }

    # ------------------------------------------------------------------ #
    # introspection                                                      #
    # ------------------------------------------------------------------ #

    def stats(self) -> dict[str, dict]:
        """Per-filter pipeline tallies plus a ``"_totals"`` rollup.

        Per filter: storage truth (``storage_rows``), the lookup traffic
        split, ``storage_skipped_pct`` — THE Extended-C effectiveness
        number: the share of lookups the bloom tier fully absorbed —
        ``observed_fp_rate`` (false positives over storage-verified bloom
        positives; fallback lookups never consult bloom, so they are
        excluded from the denominator), the live ``fallback_active`` flag,
        and the fallback/rotation counters. Counters are copied under the
        pipeline lock; sqlite counts and filter estimates are read outside
        it (one lock at a time).
        """
        threshold = self._settings.fp_fallback_threshold
        out: dict[str, dict] = {}
        totals = {
            "lookups": 0,
            "bloom_negatives": 0,
            "storage_hits": 0,
            "false_positives": 0,
            "fallback_lookups": 0,
            "rotations_triggered": 0,
        }
        total_verified = 0
        for name in self._manager.names:
            counters = self._counters_for(name)
            with self._lock:
                snap = {
                    "lookups": counters.lookups,
                    "bloom_negatives": counters.bloom_negatives,
                    "storage_hits": counters.storage_hits,
                    "false_positives": counters.false_positives,
                    "fallback_lookups": counters.fallback_lookups,
                    "rotations_triggered": counters.rotations_triggered,
                }
            # Two-tier bloom positives that storage verified (class docstring
            # of PipelineCounters explains the three-way partition).
            verified = (
                snap["lookups"] - snap["bloom_negatives"] - snap["fallback_lookups"]
            )
            total_verified += verified
            for key in totals:
                totals[key] += snap[key]
            out[name] = {
                "storage_rows": self._store.count(name),
                "lookups": snap["lookups"],
                "bloom_negatives": snap["bloom_negatives"],
                "storage_skipped_pct": _pct(
                    snap["bloom_negatives"], snap["lookups"]
                ),
                "storage_hits": snap["storage_hits"],
                "false_positives": snap["false_positives"],
                "observed_fp_rate": round(
                    snap["false_positives"] / max(1, verified), 6
                ),
                "fallback_active": self._current_estimate(name) > threshold,
                "fallback_lookups": snap["fallback_lookups"],
                "rotations_triggered": snap["rotations_triggered"],
            }
        out["_totals"] = {
            "storage_rows": self._store.count(),
            "lookups": totals["lookups"],
            "bloom_negatives": totals["bloom_negatives"],
            "storage_skipped_pct": _pct(
                totals["bloom_negatives"], totals["lookups"]
            ),
            "storage_hits": totals["storage_hits"],
            "false_positives": totals["false_positives"],
            "observed_fp_rate": round(
                totals["false_positives"] / max(1, total_verified), 6
            ),
            "fallback_lookups": totals["fallback_lookups"],
            "rotations_triggered": totals["rotations_triggered"],
        }
        return out

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"TwoTierPipeline(filters={list(self._manager.names)}, "
            f"store={self._store!r})"
        )
