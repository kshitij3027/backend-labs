"""Unit tests for ``src.vector_clock``.

We exercise every branch of compare (``-1`` / ``0`` / ``1`` / ``None``) and
verify ``merge`` and ``increment`` are pure and behave per the spec
(§2 of project_requirements.md).
"""

from __future__ import annotations

from src.vector_clock import increment, merge, vector_clock_compare


# ----------------------------- compare -----------------------------------


def test_compare_empty_clocks_equal():
    assert vector_clock_compare({}, {}) == 0


def test_compare_identical_clocks():
    a = {"us-east": 3, "europe": 1, "asia": 2}
    b = {"us-east": 3, "europe": 1, "asia": 2}
    assert vector_clock_compare(a, b) == 0


def test_compare_a_strictly_before_b():
    a = {"us-east": 1}
    b = {"us-east": 2}
    assert vector_clock_compare(a, b) == -1


def test_compare_b_strictly_before_a():
    a = {"us-east": 2}
    b = {"us-east": 1}
    assert vector_clock_compare(a, b) == 1


def test_compare_concurrent_clocks():
    """Each clock is ahead in exactly one region — incomparable."""
    a = {"us-east": 1, "europe": 0}
    b = {"us-east": 0, "europe": 1}
    assert vector_clock_compare(a, b) is None


def test_compare_with_missing_keys():
    """Missing keys count as 0 — a is missing europe so a <= b on every key."""
    a = {"us-east": 1}
    b = {"us-east": 1, "europe": 1}
    assert vector_clock_compare(a, b) == -1


def test_compare_concurrent_via_missing_keys():
    """One clock has only us-east advanced, the other has only europe — concurrent."""
    a = {"us-east": 1}
    b = {"europe": 1}
    assert vector_clock_compare(a, b) is None


# ------------------------------ merge ------------------------------------


def test_merge_takes_per_key_max():
    local = {"us-east": 3, "europe": 0}
    incoming = {"us-east": 1, "europe": 5, "asia": 2}
    out = merge(local, incoming)
    assert out == {"us-east": 3, "europe": 5, "asia": 2}


def test_merge_does_not_mutate_inputs():
    local = {"us-east": 1}
    incoming = {"europe": 1}
    _ = merge(local, incoming)
    assert local == {"us-east": 1}
    assert incoming == {"europe": 1}


def test_merge_then_increment_advances_local_region():
    """Replication semantics from §2: secondary first merges per-key max,
    then increments its own region's counter by 1."""
    local = {"us-east": 0, "europe": 2}
    incoming = {"us-east": 5, "europe": 1}

    merged = merge(local, incoming)
    advanced = increment(merged, "europe")  # secondary is "europe"

    # Per-key max from the merge.
    assert merged == {"us-east": 5, "europe": 2}
    # Then secondary advances its own slot by 1.
    assert advanced == {"us-east": 5, "europe": 3}


# ---------------------------- increment ----------------------------------


def test_increment_initializes_missing_region():
    assert increment({}, "us-east") == {"us-east": 1}


def test_increment_advances_existing_region_by_one():
    assert increment({"us-east": 4, "europe": 2}, "us-east") == {
        "us-east": 5,
        "europe": 2,
    }


def test_increment_does_not_mutate_input():
    vc = {"us-east": 1}
    _ = increment(vc, "us-east")
    assert vc == {"us-east": 1}
