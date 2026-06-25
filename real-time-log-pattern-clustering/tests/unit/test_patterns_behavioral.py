"""Unit tests for behavioral clustering (C19, Feature Area D).

These exercise :func:`src.patterns.behavioral.mine_behavioral_patterns` against the seeded
synthetic corpus. The generator embeds a small pool of brute-force "bad IPs" that repeat
across many security logs, so they form a distinct high-error / high-security cohort that the
behavioral clusterer should surface as ``error-heavy`` or ``security-suspect`` — that is the
substantive claim the headline test grounds in the data (it checks an actual bad IP lands in
such a group).
"""

from __future__ import annotations

from src.log_generator import _BAD_IPS, generate_logs
from src.patterns.behavioral import mine_behavioral_patterns

# Every group must carry these documented keys.
_REQUIRED_KEYS = {
    "group",
    "label",
    "count",
    "mean_requests",
    "mean_error_rate",
    "mean_response_ms",
    "example_entities",
}

# Labels that indicate a "bad actor" cohort (the brute-force IPs should land in one of these).
_SUSPECT_LABELS = {"error-heavy", "security-suspect"}


def test_returns_well_shaped_groups_summing_to_entities() -> None:
    """>= 2 groups, each fully shaped, and the group counts sum to the entity total."""
    logs = generate_logs(2000, seed=4)
    result = mine_behavioral_patterns(logs)

    assert isinstance(result, dict)
    assert {"groups", "entities"} <= result.keys()
    assert result["entities"] > 0

    groups = result["groups"]
    assert len(groups) >= 2

    for g in groups:
        assert _REQUIRED_KEYS <= g.keys()
        assert isinstance(g["example_entities"], list)
        assert g["count"] >= 1

    # The partition is complete: every entity belongs to exactly one group.
    assert sum(g["count"] for g in groups) == result["entities"]

    # Groups are sorted by count descending.
    counts = [g["count"] for g in groups]
    assert counts == sorted(counts, reverse=True)


def test_flags_a_suspect_cohort_containing_a_bad_ip() -> None:
    """At least one group is error-heavy/security-suspect and contains a brute-force bad IP."""
    logs = generate_logs(2000, seed=4)
    result = mine_behavioral_patterns(logs)

    suspect_groups = [g for g in result["groups"] if g["label"] in _SUSPECT_LABELS]
    assert suspect_groups, "expected at least one error-heavy/security-suspect cohort"

    # Ground the claim in the data: a known brute-force IP must surface in a suspect cohort's
    # example entities (the bad-IP pool repeats, so it clusters distinctly).
    suspect_examples = {e for g in suspect_groups for e in g["example_entities"]}
    assert suspect_examples & set(_BAD_IPS), (
        "no brute-force bad IP found among suspect cohorts: "
        f"{sorted(suspect_examples)[:8]}"
    )


def test_respects_n_groups_cap() -> None:
    """The number of groups never exceeds the requested n_groups (nor the entity count)."""
    logs = generate_logs(2000, seed=4)
    result = mine_behavioral_patterns(logs, n_groups=3)
    assert len(result["groups"]) <= 3


def test_single_entity_input_is_robust() -> None:
    """A 1-entity input yields a single well-formed group without raising."""
    # One log -> one entity (its source_ip).
    one = generate_logs(2000, seed=4)[:1]
    result = mine_behavioral_patterns(one)
    assert result["entities"] == 1
    assert len(result["groups"]) == 1
    assert _REQUIRED_KEYS <= result["groups"][0].keys()


def test_empty_input_returns_no_groups() -> None:
    """Empty input returns zero groups / zero entities (never raises)."""
    result = mine_behavioral_patterns([])
    assert result == {"groups": [], "entities": 0}
