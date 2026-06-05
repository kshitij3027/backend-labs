"""Unit tests for :class:`~src.format_selector.FormatSelector` (C14).

The selector is a pure, ordered rule cascade (first match wins) mapping a
partition's :class:`~src.pattern_tracker.PartitionAccessStats` plus its tier and
row count onto a recommended :class:`~src.models.Format`. These tests build
:class:`PartitionAccessStats` directly via :func:`make_stats`, driving the
*public ratio properties* (``write_ratio``, ``point_lookup_ratio``,
``scan_ratio``, ``fraction_columns_touched``) to known values, then assert the
recommended format / confidence for each canonical workload.

The final test (§"selection-optimality suite") encodes the §5 ">=90% optimal
selection" proof: a table of >=20 unambiguously-labelled workloads is fed
through :meth:`FormatSelector.recommend` and the fraction matching their optimal
format is asserted to be at least 0.90.

Thresholds used throughout are the documented :class:`~src.settings.Settings`
defaults:
    * ``write_ratio_row        == 0.30``
    * ``point_lookup_row       == 0.50``
    * ``scan_ratio_columnar    == 0.60``
    * ``few_columns_fraction   == 0.40``
    * ``min_confidence         == 0.60``
    * ``min_rows               == 256``
"""
from __future__ import annotations

import collections

import pytest

from src.format_selector import FormatSelector, Recommendation
from src.models import Format, Tier
from src.pattern_tracker import PartitionAccessStats


def make_stats(
    *,
    writes: int = 0,
    reads: int = 0,
    point_lookups: int = 0,
    scans: int = 0,
    distinct_cols: int = 10,
    avg_cols: float = 2.0,
    last_access: float = 0.0,
) -> PartitionAccessStats:
    """Build a :class:`PartitionAccessStats` whose ratio properties hit targets.

    Sets the raw counters the selector reads so the derived *properties* land on
    known values:

    * ``write_ratio            == writes / (reads + writes)``
    * ``point_lookup_ratio     == point_lookups / (point_lookups + scans)``
    * ``scan_ratio             == 1 - point_lookup_ratio``
    * ``avg_columns_touched    == avg_cols`` (via ``columns_per_read_total``)
    * ``fraction_columns_touched == avg_cols / distinct_cols`` (clamped to
      ``[0, 1]`` by the property)

    ``column_counter`` is populated with ``distinct_cols`` distinct keys so
    :attr:`PartitionAccessStats.distinct_columns` equals ``distinct_cols``.
    """
    s = PartitionAccessStats()
    s.writes = writes
    s.reads = reads
    s.point_lookups = point_lookups
    s.scans = scans
    s.column_counter = collections.Counter({f"c{i}": 1 for i in range(distinct_cols)})
    # columns_per_read_total / reads == avg_cols  ->  avg_columns_touched == avg_cols
    s.columns_per_read_total = int(round(avg_cols * reads))
    s.last_access = last_access
    return s


def make_selector() -> FormatSelector:
    """Return a selector built from the documented Settings defaults."""
    return FormatSelector(
        write_ratio_row=0.3,
        point_lookup_row=0.5,
        scan_ratio_columnar=0.6,
        few_columns_fraction=0.4,
        min_confidence=0.6,
        min_rows=256,
    )


# --------------------------------------------------------------------------- #
# Sanity: make_stats drives the public ratio PROPERTIES to the right values.
# --------------------------------------------------------------------------- #


def test_make_stats_drives_properties() -> None:
    """Guard the helper itself: properties must equal the intended targets."""
    s = make_stats(
        writes=20, reads=80, point_lookups=20, scans=30, distinct_cols=10, avg_cols=8.0
    )
    assert s.write_ratio == pytest.approx(0.2)  # 20 / 100
    assert s.point_lookup_ratio == pytest.approx(0.4)  # 20 / 50
    assert s.scan_ratio == pytest.approx(0.6)  # 1 - 0.4
    assert s.avg_columns_touched == pytest.approx(8.0)
    assert s.fraction_columns_touched == pytest.approx(0.8)  # 8 / 10


# --------------------------------------------------------------------------- #
# 1-6: one focused test per canonical rule.
# --------------------------------------------------------------------------- #


def test_write_heavy_recent_recommends_row() -> None:
    """HOT + write_ratio ~0.5 -> ROW with confidence >= 0.6 (rule 2)."""
    sel = make_selector()
    stats = make_stats(writes=50, reads=50)  # write_ratio == 0.5
    rec = sel.recommend(
        stats,
        age_seconds=10.0,
        row_count=1000,
        tier=Tier.HOT,
        current_format=Format.COLUMNAR,
    )
    assert rec.format == Format.ROW
    assert rec.confidence >= 0.6


def test_cold_scan_few_columns_recommends_columnar() -> None:
    """COLD + scan_ratio 0.9 + narrow projection -> COLUMNAR (rule 4)."""
    sel = make_selector()
    # scan_ratio == 0.9, fraction_columns_touched == 0.2 (avg 2 / 10 distinct)
    stats = make_stats(
        reads=100, writes=0, point_lookups=10, scans=90, distinct_cols=10, avg_cols=2.0
    )
    rec = sel.recommend(
        stats,
        age_seconds=200000.0,
        row_count=1000,
        tier=Tier.COLD,
        current_format=Format.ROW,
    )
    assert rec.format == Format.COLUMNAR
    assert rec.confidence >= 0.6


def test_point_lookup_dominant_recommends_row() -> None:
    """point_lookup_ratio 0.8 -> ROW even on WARM tier (rule 3)."""
    sel = make_selector()
    stats = make_stats(reads=100, writes=0, point_lookups=80, scans=20)  # plr == 0.8
    rec = sel.recommend(
        stats,
        age_seconds=5000.0,
        row_count=1000,
        tier=Tier.WARM,
        current_format=Format.COLUMNAR,
    )
    assert rec.format == Format.ROW
    assert rec.confidence >= 0.6


def test_mixed_recommends_hybrid() -> None:
    """WARM, scans + ongoing writes, wide projection -> HYBRID (rule 5).

    Construction (so earlier rules deliberately do NOT fire):
        * not HOT                          -> rule 2 skipped
        * point_lookup_ratio 0.4 (< 0.5)   -> rule 3 skipped
        * fraction_columns_touched 0.8     -> rule 4's narrow-projection guard
          fails even though scan_ratio 0.6 >= 0.6
        * scan_ratio 0.6 > 0.2 AND write_ratio 0.2 > 0.1 -> rule 5 fires
    """
    sel = make_selector()
    stats = make_stats(
        writes=20,
        reads=80,
        point_lookups=20,
        scans=30,
        distinct_cols=10,
        avg_cols=8.0,  # fraction_columns_touched == 0.8 -> defeats COLUMNAR rule
    )
    rec = sel.recommend(
        stats,
        age_seconds=5000.0,
        row_count=1000,
        tier=Tier.WARM,
        current_format=Format.ROW,
    )
    assert rec.format == Format.HYBRID
    assert rec.confidence >= 0.6


def test_too_small_keeps_current_with_zero_confidence() -> None:
    """row_count < min_rows -> keep current_format, confidence == 0.0 (rule 1)."""
    sel = make_selector()
    # Stats that would otherwise scream COLUMNAR; row_count gate must win first.
    stats = make_stats(
        reads=100, writes=0, point_lookups=5, scans=95, distinct_cols=10, avg_cols=1.0
    )
    rec = sel.recommend(
        stats,
        age_seconds=200000.0,
        row_count=10,  # < 256
        tier=Tier.COLD,
        current_format=Format.COLUMNAR,
    )
    assert rec.format == Format.COLUMNAR
    assert rec.confidence == 0.0


def test_no_strong_signal_keeps_current_below_min_confidence() -> None:
    """All ratios 0 (no ops) -> keep current at sub-min_confidence (rule 6)."""
    sel = make_selector()
    stats = make_stats(reads=0, writes=0, point_lookups=0, scans=0)  # all ratios 0.0
    rec = sel.recommend(
        stats,
        age_seconds=5000.0,
        row_count=1000,
        tier=Tier.WARM,
        current_format=Format.ROW,
    )
    assert rec.format == Format.ROW
    assert rec.confidence < 0.6


# --------------------------------------------------------------------------- #
# 7: should_migrate gate (different-format AND confidence-floor).
# --------------------------------------------------------------------------- #


def test_should_migrate_gate() -> None:
    """Migrate only when the format differs AND confidence clears the floor."""
    sel = make_selector()

    high_conf_row = Recommendation(
        format=Format.ROW, score=0.9, reason="x", confidence=0.9
    )
    # Different format + high confidence -> migrate.
    assert sel.should_migrate(high_conf_row, Format.COLUMNAR) is True
    # Same format (even at high confidence) -> never migrate.
    assert sel.should_migrate(high_conf_row, Format.ROW) is False

    low_conf_row = Recommendation(
        format=Format.ROW, score=0.4, reason="x", confidence=0.4
    )
    # Different format but confidence below floor -> do not migrate.
    assert sel.should_migrate(low_conf_row, Format.COLUMNAR) is False


# --------------------------------------------------------------------------- #
# 8: every recommendation carries a non-empty, human-readable reason.
# --------------------------------------------------------------------------- #


def test_every_recommendation_has_nonempty_reason() -> None:
    """Each rule path must produce a non-empty string reason for the dashboard."""
    sel = make_selector()
    cases = [
        # (stats, tier, row_count, current_format)
        (make_stats(writes=50, reads=50), Tier.HOT, 1000, Format.COLUMNAR),  # rule 2
        (
            make_stats(reads=100, point_lookups=80, scans=20),
            Tier.WARM,
            1000,
            Format.COLUMNAR,
        ),  # rule 3
        (
            make_stats(reads=100, point_lookups=10, scans=90, avg_cols=2.0),
            Tier.COLD,
            1000,
            Format.ROW,
        ),  # rule 4
        (
            make_stats(
                writes=20, reads=80, point_lookups=20, scans=30, avg_cols=8.0
            ),
            Tier.WARM,
            1000,
            Format.ROW,
        ),  # rule 5
        (make_stats(reads=10, point_lookups=1, scans=9), Tier.COLD, 10, Format.ROW),  # rule 1
        (make_stats(), Tier.WARM, 1000, Format.ROW),  # rule 6
    ]
    for stats, tier, row_count, current in cases:
        rec = sel.recommend(
            stats,
            age_seconds=1000.0,
            row_count=row_count,
            tier=tier,
            current_format=current,
        )
        assert isinstance(rec.reason, str)
        assert rec.reason.strip(), f"empty reason for tier={tier} fmt={current}"
        # score mirrors confidence by contract.
        assert rec.score == pytest.approx(rec.confidence)


# --------------------------------------------------------------------------- #
# 9: SELECTION-OPTIMALITY SUITE — the §5 ">=90% optimal" proof.
# --------------------------------------------------------------------------- #

# >=20 unambiguously-labelled workloads spanning the 4 canonical patterns and
# hot/warm/cold variants. Each row's `expected` is the clearly-optimal format
# given the documented rules; deliberately ambiguous mixes are excluded.
OPTIMALITY_SCENARIOS = [
    # --- write-heavy & hot -> ROW (append-friendly) ------------------------- #
    {
        "name": "hot_write_heavy_basic",
        "tier": Tier.HOT, "writes": 50, "reads": 50,
        "point_lookups": 25, "scans": 25, "distinct_cols": 10, "avg_cols": 5.0,
        "row_count": 1000, "expected": Format.ROW,
    },
    {
        "name": "hot_write_dominant",
        "tier": Tier.HOT, "writes": 90, "reads": 10,
        "point_lookups": 5, "scans": 5, "distinct_cols": 10, "avg_cols": 5.0,
        "row_count": 5000, "expected": Format.ROW,
    },
    {
        "name": "hot_write_at_threshold",
        "tier": Tier.HOT, "writes": 40, "reads": 60,
        "point_lookups": 30, "scans": 30, "distinct_cols": 10, "avg_cols": 4.0,
        "row_count": 2000, "expected": Format.ROW,
    },
    {
        "name": "hot_pure_ingest",
        "tier": Tier.HOT, "writes": 200, "reads": 0,
        "point_lookups": 0, "scans": 0, "distinct_cols": 10, "avg_cols": 0.0,
        "row_count": 10000, "expected": Format.ROW,
    },
    # --- point-lookup dominant -> ROW (whole-record fetches) ---------------- #
    {
        "name": "warm_point_lookup_dominant",
        "tier": Tier.WARM, "writes": 0, "reads": 100,
        "point_lookups": 80, "scans": 20, "distinct_cols": 10, "avg_cols": 9.0,
        "row_count": 3000, "expected": Format.ROW,
    },
    {
        "name": "cold_point_lookup_heavy",
        "tier": Tier.COLD, "writes": 0, "reads": 100,
        "point_lookups": 95, "scans": 5, "distinct_cols": 10, "avg_cols": 8.0,
        "row_count": 4000, "expected": Format.ROW,
    },
    {
        "name": "warm_point_lookup_at_threshold",
        "tier": Tier.WARM, "writes": 0, "reads": 100,
        "point_lookups": 50, "scans": 50, "distinct_cols": 10, "avg_cols": 9.0,
        "row_count": 2000, "expected": Format.ROW,
    },
    {
        "name": "warm_key_lookups_wide_records",
        "tier": Tier.WARM, "writes": 10, "reads": 90,
        "point_lookups": 70, "scans": 20, "distinct_cols": 8, "avg_cols": 7.0,
        "row_count": 1500, "expected": Format.ROW,
    },
    # --- cold/warm scan + narrow projection -> COLUMNAR --------------------- #
    {
        "name": "cold_scan_narrow",
        "tier": Tier.COLD, "writes": 0, "reads": 100,
        "point_lookups": 10, "scans": 90, "distinct_cols": 10, "avg_cols": 2.0,
        "row_count": 5000, "expected": Format.COLUMNAR,
    },
    {
        "name": "cold_full_scan_one_column",
        "tier": Tier.COLD, "writes": 0, "reads": 100,
        "point_lookups": 0, "scans": 100, "distinct_cols": 20, "avg_cols": 1.0,
        "row_count": 8000, "expected": Format.COLUMNAR,
    },
    {
        "name": "warm_scan_narrow",
        "tier": Tier.WARM, "writes": 0, "reads": 100,
        "point_lookups": 20, "scans": 80, "distinct_cols": 10, "avg_cols": 3.0,
        "row_count": 3000, "expected": Format.COLUMNAR,
    },
    {
        "name": "cold_analytical_aggregation",
        "tier": Tier.COLD, "writes": 0, "reads": 200,
        "point_lookups": 20, "scans": 180, "distinct_cols": 12, "avg_cols": 2.0,
        "row_count": 6000, "expected": Format.COLUMNAR,
    },
    {
        "name": "cold_scan_at_threshold_narrow",
        "tier": Tier.COLD, "writes": 0, "reads": 100,
        "point_lookups": 40, "scans": 60, "distinct_cols": 10, "avg_cols": 2.0,
        "row_count": 2500, "expected": Format.COLUMNAR,
    },
    {
        "name": "warm_wide_scan_few_cols",
        "tier": Tier.WARM, "writes": 0, "reads": 150,
        "point_lookups": 15, "scans": 135, "distinct_cols": 25, "avg_cols": 4.0,
        "row_count": 7000, "expected": Format.COLUMNAR,
    },
    # --- mixed: scans + ongoing writes/lookups, wide projection -> HYBRID --- #
    {
        "name": "warm_mixed_write_and_scan_wide",
        "tier": Tier.WARM, "writes": 20, "reads": 80,
        "point_lookups": 20, "scans": 30, "distinct_cols": 10, "avg_cols": 8.0,
        "row_count": 3000, "expected": Format.HYBRID,
    },
    {
        "name": "warm_mixed_lookups_and_scan_wide",
        "tier": Tier.WARM, "writes": 5, "reads": 95,
        "point_lookups": 40, "scans": 60, "distinct_cols": 10, "avg_cols": 9.0,
        "row_count": 4000, "expected": Format.HYBRID,
    },
    {
        "name": "cold_mixed_write_scan_wide",
        "tier": Tier.COLD, "writes": 15, "reads": 85,
        "point_lookups": 30, "scans": 40, "distinct_cols": 10, "avg_cols": 9.0,
        "row_count": 5000, "expected": Format.HYBRID,
    },
    {
        "name": "warm_balanced_scan_writes_wide",
        "tier": Tier.WARM, "writes": 25, "reads": 75,
        "point_lookups": 25, "scans": 35, "distinct_cols": 12, "avg_cols": 10.0,
        "row_count": 3500, "expected": Format.HYBRID,
    },
    {
        "name": "cold_scan_writes_full_record",
        "tier": Tier.COLD, "writes": 20, "reads": 80,
        "point_lookups": 10, "scans": 50, "distinct_cols": 15, "avg_cols": 14.0,
        "row_count": 4500, "expected": Format.HYBRID,
    },
    # --- more hot/warm write-heavy variants -> ROW -------------------------- #
    {
        "name": "hot_write_heavy_with_scans",
        "tier": Tier.HOT, "writes": 60, "reads": 40,
        "point_lookups": 10, "scans": 30, "distinct_cols": 10, "avg_cols": 3.0,
        "row_count": 2000, "expected": Format.ROW,
    },
    {
        "name": "hot_write_heavy_narrow_reads",
        "tier": Tier.HOT, "writes": 70, "reads": 30,
        "point_lookups": 5, "scans": 25, "distinct_cols": 10, "avg_cols": 2.0,
        "row_count": 9000, "expected": Format.ROW,
    },
    {
        "name": "cold_deep_scan_two_cols",
        "tier": Tier.COLD, "writes": 0, "reads": 300,
        "point_lookups": 30, "scans": 270, "distinct_cols": 30, "avg_cols": 3.0,
        "row_count": 12000, "expected": Format.COLUMNAR,
    },
]


def test_optimality_suite_meets_ninety_percent() -> None:
    """At least 90% of labelled workloads get their optimal format (§5 proof)."""
    sel = make_selector()
    assert len(OPTIMALITY_SCENARIOS) >= 20, "need >=20 labelled scenarios for the proof"

    matches = 0
    mismatches: list[str] = []
    for sc in OPTIMALITY_SCENARIOS:
        stats = make_stats(
            writes=sc["writes"],
            reads=sc["reads"],
            point_lookups=sc["point_lookups"],
            scans=sc["scans"],
            distinct_cols=sc["distinct_cols"],
            avg_cols=sc["avg_cols"],
        )
        rec = sel.recommend(
            stats,
            age_seconds=1000.0,
            row_count=sc["row_count"],
            tier=sc["tier"],
            current_format=Format.ROW,
        )
        if rec.format == sc["expected"]:
            matches += 1
        else:
            mismatches.append(
                f"{sc['name']} (tier={sc['tier'].value}): "
                f"expected {sc['expected'].value}, got {rec.format.value} "
                f"[write_ratio={stats.write_ratio:.2f}, "
                f"plr={stats.point_lookup_ratio:.2f}, "
                f"scan_ratio={stats.scan_ratio:.2f}, "
                f"frac_cols={stats.fraction_columns_touched:.2f}]"
            )

    fraction = matches / len(OPTIMALITY_SCENARIOS)
    # Printed so a CI log shows the achieved optimality fraction even on success.
    print(
        f"\n[optimality] {matches}/{len(OPTIMALITY_SCENARIOS)} optimal "
        f"= {fraction:.2%}"
    )
    assert fraction >= 0.9, (
        f"selection optimality {fraction:.2%} < 90% "
        f"({matches}/{len(OPTIMALITY_SCENARIOS)}); mismatches:\n"
        + "\n".join(mismatches)
    )
