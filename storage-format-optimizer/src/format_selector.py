"""Rule-based, explainable storage-format selection.

:class:`FormatSelector` maps a partition's real-time access statistics (from
:class:`~src.pattern_tracker.PartitionAccessStats`) plus its age, row count, and
:class:`~src.models.Tier` onto a recommended on-disk :class:`~src.models.Format`
(ROW / COLUMNAR / HYBRID). The recommendation carries a human-readable
``reason`` and a ``confidence`` so the migration engine can decide whether a
change is worth acting on and the dashboard can explain *why* each partition is
laid out the way it is.

Design notes:
    * **No ML — pure rules.** The decision is a short, ordered cascade of
      thresholded comparisons. It is fully deterministic: the same inputs always
      yield the same :class:`Recommendation`, with no hidden state, clock reads,
      or I/O.
    * **First match wins.** Rules are evaluated top-to-bottom; the first whose
      guard is satisfied returns immediately. The ordering is deliberate (see
      :meth:`FormatSelector.recommend`) so that the strongest, most specific
      signals (too-small, hot+write-heavy, point-lookup) are considered before
      the broader scan/mixed signals.
    * **Tier-aware.** ``tier`` folds into the rules so a HOT write-heavy
      partition stays ROW while a COLD scan-heavy partition tips to COLUMNAR,
      matching the recency-vs-analytics trade-off in ``plan.md``.
    * **Stdlib + project enums only.** Import-light and trivially testable; the
      thresholds are injected (sourced from :class:`~src.settings.Settings`)
      rather than hard-coded, so behaviour tracks configuration.
"""
from __future__ import annotations

from dataclasses import dataclass

from src.models import Format, Tier
from src.pattern_tracker import PartitionAccessStats

__all__ = ["FormatSelector", "Recommendation"]


@dataclass
class Recommendation:
    """A single format recommendation for a partition.

    Attributes:
        format: The recommended on-disk :class:`~src.models.Format`.
        score: A scalar in ``[0.0, 1.0]`` ranking the recommendation; here it
            mirrors :attr:`confidence` (a stronger signal scores higher). Useful
            for ordering candidate migrations when capacity is limited.
        reason: A short, human-readable explanation of which rule fired and the
            signals behind it. Surfaced verbatim on the dashboard, so it must
            stay readable.
        confidence: How strongly the rule believes in this format, in
            ``[0.0, 1.0]``. The migration engine compares this against
            ``min_confidence`` (via :meth:`FormatSelector.should_migrate`)
            before acting, which suppresses low-conviction flapping.
    """

    format: Format
    score: float
    reason: str
    confidence: float


class FormatSelector:
    """Rule-based selector mapping access patterns to a storage format.

    The selector is stateless aside from its injected thresholds. Callers (the
    migration engine, and the stats endpoint computing selection optimality)
    construct it once from :class:`~src.settings.Settings` and call
    :meth:`recommend` per partition.
    """

    def __init__(
        self,
        *,
        write_ratio_row: float,
        point_lookup_row: float,
        scan_ratio_columnar: float,
        few_columns_fraction: float,
        min_confidence: float,
        min_rows: int,
    ) -> None:
        """Create a selector from explicit decision thresholds.

        Args:
            write_ratio_row: Write fraction at or above which a HOT partition is
                recommended ROW (``select_write_ratio_row``, default ``0.3``).
            point_lookup_row: Point-lookup fraction at or above which any
                partition is recommended ROW (``select_point_lookup_row``,
                default ``0.5``).
            scan_ratio_columnar: Scan fraction at or above which a non-hot,
                narrow-projection partition is recommended COLUMNAR
                (``select_scan_ratio_columnar``, default ``0.6``).
            few_columns_fraction: Maximum mean fraction of columns touched per
                read for a partition to qualify as "narrow projection" for the
                COLUMNAR rule (``select_few_columns_fraction``, default ``0.4``).
            min_confidence: Confidence floor below which a recommendation is not
                worth acting on (``select_min_confidence``, default ``0.6``).
                Used by :meth:`should_migrate`.
            min_rows: Row count below which the partition is too small to be
                worth reformatting (``select_min_rows``, default ``256``).
        """
        self._write_ratio_row = write_ratio_row
        self._point_lookup_row = point_lookup_row
        self._scan_ratio_columnar = scan_ratio_columnar
        self._few_columns_fraction = few_columns_fraction
        self._min_confidence = min_confidence
        self._min_rows = min_rows

    def recommend(
        self,
        stats: PartitionAccessStats,
        *,
        age_seconds: float,
        row_count: int,
        tier: Tier,
        current_format: Format,
    ) -> Recommendation:
        """Recommend a storage format for one partition.

        Rules are applied in strict order; the **first match wins** and returns
        immediately. Each rule produces a distinct ``reason`` and a
        characteristic ``confidence``:

        1. **Too small to bother.** ``row_count < min_rows`` -> keep
           ``current_format`` with ``confidence=0.0``. Tiny partitions are not
           worth the rewrite cost regardless of their access shape.
        2. **Recent & write-heavy -> ROW.** ``tier == HOT`` and
           ``write_ratio >= write_ratio_row`` -> ROW (``confidence ~0.9``).
           Append-friendly row storage wins for hot, write-dominated data.
        3. **Point-lookup-dominant -> ROW.** ``point_lookup_ratio >=
           point_lookup_row`` -> ROW (``confidence ~0.75``). Selective key
           lookups read whole records, so row layout avoids columnar overhead.
        4. **Old & scan-heavy & narrow -> COLUMNAR.** not HOT (i.e. COLD/WARM)
           and ``scan_ratio >= scan_ratio_columnar`` and
           ``fraction_columns_touched <= few_columns_fraction`` -> COLUMNAR
           (``confidence ~0.85``, nudged up for COLD). Wide scans over a few
           columns are the textbook columnar workload.
        5. **Mixed -> HYBRID.** ``scan_ratio > 0.2`` and (``write_ratio > 0.1``
           or ``point_lookup_ratio > 0.2``) -> HYBRID (``confidence ~0.7``).
           A blend of scans with ongoing writes/lookups is best served by a
           recent-rows + sealed-columnar hybrid.
        6. **Default.** No strong signal -> keep ``current_format`` with a
           sub-``min_confidence`` confidence (``0.4``), so nothing migrates.

        Because earlier rules win, a HOT write-heavy partition is recommended
        ROW (rule 2) even if it also looks mixed (rule 5) — the freshness signal
        dominates by design.

        Args:
            stats: The partition's accumulated access statistics.
            age_seconds: Seconds since the partition was created. Accepted for
                interface completeness and future age-sensitive tuning; the
                current rules drive recency through ``tier`` rather than raw
                age.
            row_count: Number of rows currently stored in the partition.
            tier: The partition's hot/warm/cold tier.
            current_format: The partition's present on-disk format, returned
                unchanged when no rule recommends a switch.

        Returns:
            A :class:`Recommendation` whose ``score`` equals its ``confidence``.
        """
        # Rule 1: too small to be worth a rewrite — keep whatever we have.
        if row_count < self._min_rows:
            reason = (
                f"row_count {row_count} < min_rows {self._min_rows} "
                "— keep current"
            )
            return Recommendation(
                format=current_format,
                score=0.0,
                reason=reason,
                confidence=0.0,
            )

        write_ratio = stats.write_ratio
        point_lookup_ratio = stats.point_lookup_ratio
        scan_ratio = stats.scan_ratio
        frac_cols = stats.fraction_columns_touched
        is_hot = tier == Tier.HOT
        is_cold = tier == Tier.COLD

        # Rule 2: recent (hot) and write-heavy -> ROW.
        if is_hot and write_ratio >= self._write_ratio_row:
            reason = (
                f"hot tier with write_ratio {write_ratio:.2f} "
                f">= {self._write_ratio_row:.2f} — favour ROW for fast appends"
            )
            return Recommendation(
                format=Format.ROW,
                score=0.9,
                reason=reason,
                confidence=0.9,
            )

        # Rule 3: point-lookup-dominant -> ROW (full-record fetches by key).
        if point_lookup_ratio >= self._point_lookup_row:
            reason = (
                f"point_lookup_ratio {point_lookup_ratio:.2f} "
                f">= {self._point_lookup_row:.2f} "
                "— favour ROW for whole-record lookups"
            )
            return Recommendation(
                format=Format.ROW,
                score=0.75,
                reason=reason,
                confidence=0.75,
            )

        # Rule 4: not hot, scan-heavy, narrow projection -> COLUMNAR.
        # A COLD partition is an even stronger columnar candidate than a WARM
        # one, so nudge the confidence up when it is cold.
        if (
            not is_hot
            and scan_ratio >= self._scan_ratio_columnar
            and frac_cols <= self._few_columns_fraction
        ):
            confidence = 0.9 if is_cold else 0.85
            tier_word = "cold" if is_cold else "non-hot"
            reason = (
                f"{tier_word} tier with scan_ratio {scan_ratio:.2f} "
                f">= {self._scan_ratio_columnar:.2f} touching only "
                f"{frac_cols:.2f} <= {self._few_columns_fraction:.2f} of columns "
                "— favour COLUMNAR for wide scans over few columns"
            )
            return Recommendation(
                format=Format.COLUMNAR,
                score=confidence,
                reason=reason,
                confidence=confidence,
            )

        # Rule 5: mixed — scans plus ongoing writes or lookups -> HYBRID.
        if scan_ratio > 0.2 and (write_ratio > 0.1 or point_lookup_ratio > 0.2):
            reason = (
                f"mixed access (scan_ratio {scan_ratio:.2f}, "
                f"write_ratio {write_ratio:.2f}, "
                f"point_lookup_ratio {point_lookup_ratio:.2f}) "
                "— favour HYBRID (recent rows + sealed columnar)"
            )
            return Recommendation(
                format=Format.HYBRID,
                score=0.7,
                reason=reason,
                confidence=0.7,
            )

        # Rule 6: nothing fired strongly — keep current at low confidence so the
        # migration engine leaves the partition alone.
        return Recommendation(
            format=current_format,
            score=0.4,
            reason="no strong signal — keep current",
            confidence=0.4,
        )

    def should_migrate(self, rec: Recommendation, current_format: Format) -> bool:
        """Decide whether a recommendation is worth a migration.

        A migration is warranted only when the recommendation names a *different*
        format **and** its confidence clears the configured floor. The
        confidence gate (together with the per-partition cooldown enforced by the
        migration engine) suppresses low-conviction flapping between formats.

        Args:
            rec: The recommendation produced by :meth:`recommend`.
            current_format: The partition's present on-disk format.

        Returns:
            ``True`` if ``rec.format != current_format`` and
            ``rec.confidence >= min_confidence``; ``False`` otherwise.
        """
        return rec.format != current_format and rec.confidence >= self._min_confidence
