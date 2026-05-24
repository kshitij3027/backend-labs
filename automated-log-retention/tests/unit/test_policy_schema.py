"""Unit tests for the retention policy Pydantic models.

Pure-dataclass tests — no I/O, no DB, no filesystem. Each test exercises
one observable behavior (selector matching, ordering rules, frozen-ness,
extra-field rejection) so failures point at a single contract.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from src.policy.schema import Phase, Policy, PolicySet, Selector


# ---------------------------------------------------------------------------
# Selector.matches
# ---------------------------------------------------------------------------


class TestSelectorMatches:
    """Behavioral matrix for ``Selector.matches``."""

    def test_exact_source_matches_when_equal(self) -> None:
        selector = Selector(source="app")
        file = SimpleNamespace(source="app", level="INFO", category="auth")
        assert selector.matches(file) is True

    def test_exact_source_rejects_mismatch(self) -> None:
        selector = Selector(source="app")
        file = SimpleNamespace(source="db", level="INFO", category="auth")
        assert selector.matches(file) is False

    def test_list_level_matches_member(self) -> None:
        selector = Selector(level=["INFO", "WARN"])
        file = SimpleNamespace(source="app", level="INFO", category="auth")
        assert selector.matches(file) is True

    def test_list_level_matches_other_member(self) -> None:
        selector = Selector(level=["INFO", "WARN"])
        file = SimpleNamespace(source="app", level="WARN", category="auth")
        assert selector.matches(file) is True

    def test_list_level_rejects_non_member(self) -> None:
        selector = Selector(level=["INFO", "WARN"])
        file = SimpleNamespace(source="app", level="ERROR", category="auth")
        assert selector.matches(file) is False

    def test_wildcard_selector_matches_everything(self) -> None:
        wildcard = Selector()
        file = SimpleNamespace(source="anything", level="any", category="any")
        assert wildcard.matches(file) is True

    def test_wildcard_matches_file_missing_attrs(self) -> None:
        # An all-None selector imposes no constraints, so a bare object
        # with no relevant attributes should still match.
        wildcard = Selector()
        assert wildcard.matches(SimpleNamespace()) is True

    def test_selector_rejects_file_lacking_required_attr(self) -> None:
        selector = Selector(source="app")
        file_without_source = SimpleNamespace(level="INFO", category="auth")
        assert selector.matches(file_without_source) is False

    def test_multi_field_selector_requires_all_match(self) -> None:
        selector = Selector(source="app", level="INFO")
        good_file = SimpleNamespace(source="app", level="INFO", category="auth")
        wrong_level = SimpleNamespace(source="app", level="ERROR", category="auth")
        wrong_source = SimpleNamespace(source="db", level="INFO", category="auth")
        assert selector.matches(good_file) is True
        assert selector.matches(wrong_level) is False
        assert selector.matches(wrong_source) is False


# ---------------------------------------------------------------------------
# Selector.specificity
# ---------------------------------------------------------------------------


class TestSelectorSpecificity:
    """Tiebreaker count for ``matcher.pick_policy``."""

    def test_wildcard_specificity_is_zero(self) -> None:
        assert Selector().specificity() == 0

    def test_one_field_specificity_is_one(self) -> None:
        assert Selector(source="app").specificity() == 1

    def test_two_fields_specificity_is_two(self) -> None:
        assert Selector(source="app", level="INFO").specificity() == 2

    def test_three_fields_specificity_is_three(self) -> None:
        assert (
            Selector(source="app", level="INFO", category="auth").specificity() == 3
        )

    def test_list_fields_count_as_one_each(self) -> None:
        # A list-typed selector field is still one constraint, not N.
        sel = Selector(source=["a", "b", "c"], level="INFO")
        assert sel.specificity() == 2


# ---------------------------------------------------------------------------
# Phase ordering on Policy
# ---------------------------------------------------------------------------


class TestPhaseOrdering:
    """``Policy._validate_phase_ordering`` contract."""

    def test_ascending_phases_construct(self) -> None:
        policy = Policy(
            name="example",
            selector=Selector(source="app"),
            phases=[
                Phase(after_days=30, action="promote", target_tier="warm"),
                Phase(after_days=60, action="archive", target_tier="cold"),
            ],
        )
        assert len(policy.phases) == 2
        assert policy.phases[0].after_days == 30
        assert policy.phases[1].after_days == 60

    def test_out_of_order_phases_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Policy(
                name="bad",
                selector=Selector(source="app"),
                phases=[
                    Phase(after_days=60, action="archive", target_tier="cold"),
                    Phase(after_days=30, action="promote", target_tier="warm"),
                ],
            )

    def test_duplicate_after_days_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Policy(
                name="dup",
                selector=Selector(source="app"),
                phases=[
                    Phase(after_days=30, action="promote", target_tier="warm"),
                    Phase(after_days=30, action="archive", target_tier="cold"),
                ],
            )

    def test_single_phase_policy_constructs(self) -> None:
        policy = Policy(
            name="single",
            selector=Selector(),
            phases=[Phase(after_days=7, action="delete")],
        )
        assert len(policy.phases) == 1

    def test_empty_phases_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Policy(name="empty", selector=Selector(), phases=[])


# ---------------------------------------------------------------------------
# Phase field constraints
# ---------------------------------------------------------------------------


class TestPhaseFields:
    """Field-level validation on ``Phase``."""

    def test_after_days_negative_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Phase(after_days=-1, action="delete")

    def test_after_days_zero_accepted(self) -> None:
        # ge=0 means zero is allowed (e.g., immediate-trigger phases).
        phase = Phase(after_days=0, action="delete")
        assert phase.after_days == 0

    def test_invalid_action_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Phase(after_days=30, action="bogus")  # type: ignore[arg-type]

    def test_invalid_target_tier_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Phase(after_days=30, action="promote", target_tier="frozen")  # type: ignore[arg-type]

    def test_target_tier_optional(self) -> None:
        phase = Phase(after_days=30, action="delete")
        assert phase.target_tier is None


# ---------------------------------------------------------------------------
# Policy field constraints
# ---------------------------------------------------------------------------


class TestPolicyFields:
    """Field-level validation on ``Policy``."""

    def test_empty_name_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Policy(
                name="",
                selector=Selector(),
                phases=[Phase(after_days=1, action="delete")],
            )

    def test_invalid_compliance_tag_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Policy(
                name="bad-tag",
                selector=Selector(),
                compliance_tag="iso27001",  # type: ignore[arg-type]
                phases=[Phase(after_days=1, action="delete")],
            )

    def test_valid_compliance_tags_all_accepted(self) -> None:
        for tag in ("gdpr", "sox", "hipaa", "pci_dss", "soc2"):
            policy = Policy(
                name=f"p-{tag}",
                selector=Selector(),
                compliance_tag=tag,  # type: ignore[arg-type]
                phases=[Phase(after_days=1, action="delete")],
            )
            assert policy.compliance_tag == tag

    def test_priority_defaults_to_zero(self) -> None:
        policy = Policy(
            name="p",
            selector=Selector(),
            phases=[Phase(after_days=1, action="delete")],
        )
        assert policy.priority == 0


# ---------------------------------------------------------------------------
# Frozen-ness
# ---------------------------------------------------------------------------


class TestFrozenModels:
    """All four models use ``frozen=True`` — mutation must raise."""

    def test_policy_mutation_raises(self) -> None:
        policy = Policy(
            name="p",
            selector=Selector(),
            phases=[Phase(after_days=1, action="delete")],
        )
        with pytest.raises(ValidationError):
            policy.name = "renamed"  # type: ignore[misc]

    def test_selector_mutation_raises(self) -> None:
        sel = Selector(source="app")
        with pytest.raises(ValidationError):
            sel.source = "db"  # type: ignore[misc]

    def test_phase_mutation_raises(self) -> None:
        phase = Phase(after_days=30, action="delete")
        with pytest.raises(ValidationError):
            phase.after_days = 60  # type: ignore[misc]

    def test_policyset_mutation_raises(self) -> None:
        ps = PolicySet(policies=[])
        with pytest.raises(ValidationError):
            ps.policies = []  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Extra-field rejection
# ---------------------------------------------------------------------------


class TestExtraFieldsForbidden:
    """``extra='forbid'`` — typos in YAML must fail loudly."""

    def test_selector_rejects_unknown_field(self) -> None:
        with pytest.raises(ValidationError):
            Selector(source="app", typo_field="x")  # type: ignore[call-arg]

    def test_phase_rejects_unknown_field(self) -> None:
        with pytest.raises(ValidationError):
            Phase(after_days=1, action="delete", typo_field="x")  # type: ignore[call-arg]

    def test_policy_rejects_unknown_field(self) -> None:
        with pytest.raises(ValidationError):
            Policy(
                name="p",
                selector=Selector(),
                phases=[Phase(after_days=1, action="delete")],
                typo_field="x",  # type: ignore[call-arg]
            )

    def test_policyset_rejects_unknown_field(self) -> None:
        with pytest.raises(ValidationError):
            PolicySet(policies=[], typo_field="x")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# PolicySet
# ---------------------------------------------------------------------------


class TestPolicySet:
    """Container model. Empty is legal; round-trips cleanly."""

    def test_empty_policy_set_constructs(self) -> None:
        ps = PolicySet(policies=[])
        assert ps.policies == []

    def test_default_factory_gives_empty_list(self) -> None:
        ps = PolicySet()
        assert ps.policies == []

    def test_policy_set_with_one_policy(self) -> None:
        ps = PolicySet(
            policies=[
                Policy(
                    name="p1",
                    selector=Selector(source="app"),
                    phases=[Phase(after_days=30, action="delete")],
                )
            ]
        )
        assert len(ps.policies) == 1
        assert ps.policies[0].name == "p1"

    def test_round_trip_via_model_dump(self) -> None:
        original = PolicySet(
            policies=[
                Policy(
                    name="payment_logs",
                    selector=Selector(source="payments", level=["INFO", "WARN"]),
                    priority=10,
                    compliance_tag="sox",
                    immutable=True,
                    phases=[
                        Phase(after_days=30, action="promote", target_tier="warm"),
                        Phase(
                            after_days=90,
                            action="archive",
                            target_tier="archive",
                            compression_level=19,
                        ),
                    ],
                )
            ]
        )
        dumped = original.model_dump()
        rebuilt = PolicySet(**dumped)
        assert rebuilt == original
        assert rebuilt.policies[0].name == "payment_logs"
        assert rebuilt.policies[0].selector.specificity() == 2
        assert len(rebuilt.policies[0].phases) == 2
