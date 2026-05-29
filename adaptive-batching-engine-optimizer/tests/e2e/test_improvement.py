"""In-process e2e check of the headline 30%+ throughput improvement.

Reuses :func:`scripts.improvement.measure_improvement` so the test and the
``make improvement`` demo assert on *exactly* the same deterministic experiment
(noise off, seeded RNG, fixed :class:`FakeMonitor`). No network, no sleeps.
"""

from __future__ import annotations

from src.settings import get_settings
from scripts.improvement import TARGET_IMPROVEMENT, measure_improvement


def test_adaptive_beats_static_by_at_least_30_percent() -> None:
    """The adaptive loop delivers >= 30% more throughput than the static baseline.

    This is spec Feature Area A / Success Criteria: a measurable 30%+ throughput
    improvement over static batching, demonstrated deterministically.
    """
    result = measure_improvement()

    print(
        f"\n[improvement] converged_batch={result['converged_batch']} "
        f"adaptive={result['adaptive_throughput']:.0f} "
        f"static@{result['static_batch']}={result['static_throughput']:.0f} "
        f"improvement={result['improvement'] * 100:.1f}% "
        f"optimal={result['optimal_batch']:.0f}"
    )

    assert result["improvement"] >= TARGET_IMPROVEMENT, (
        f"only {result['improvement'] * 100:.1f}% improvement; "
        f"need >= {TARGET_IMPROVEMENT * 100:.0f}%"
    )


def test_converged_batch_is_interior() -> None:
    """The discovered batch sits strictly inside the configured [min, max] bounds.

    A converged batch pinned at the floor or ceiling would mean the optimizer
    never found the interior optimum; the improvement would then be an artefact
    of the bound rather than genuine adaptation.
    """
    settings = get_settings()
    result = measure_improvement()

    assert settings.min_batch_size < result["converged_batch"] < settings.max_batch_size, (
        f"converged batch {result['converged_batch']} not interior to "
        f"({settings.min_batch_size}, {settings.max_batch_size})"
    )
    # The static baseline is the naive default seed (100), distinct from where
    # the adaptive loop actually settles.
    assert result["static_batch"] == settings.initial_batch_size
