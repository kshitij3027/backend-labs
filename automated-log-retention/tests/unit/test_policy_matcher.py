"""Unit tests for ``src.policy.matcher`` (C05).

Pure-Python tests — no DB, no filesystem, no async. ``SimpleNamespace``
stands in for the ORM ``File`` model: the matcher is duck-typed on the
five attributes ``source``, ``level``, ``category``, ``oldest_record_ts``,
``tier``, so a cheap namespace fixture exercises every code path.

Three groups of tests:

  * ``TestPickPolicy`` — selector matching + the three-way tiebreaker
    (priority > specificity > name).
  * ``TestNextDuePhase`` — age threshold, already-applied-by-tier
    skipping for promote/compress/archive, and the special-case
    behavior of the ``delete`` phase (skipped only when tier is
    "pending").
  * ``TestFullFlowAgainstDemoYaml`` — one integration-ish test that
    loads ``config/retention_config.yaml`` and walks the SOX payment
    policy through its 30/90/365-day boundaries. Catches any off-by-one
    between the loader's YAML round-trip and the matcher's logic.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from src.policy.loader import load_policy_set
from src.policy.matcher import next_due_phase, pick_policy
from src.policy.schema import Phase, Policy, PolicySet, Selector

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_YAML = REPO_ROOT / "config" / "retention_config.yaml"


def _make_file(
    *,
    source: str = "app",
    level: str = "INFO",
    category: str = "auth",
    age_days: float = 0.0,
    tier: str = "hot",
    now: datetime | None = None,
) -> SimpleNamespace:
    """Build a duck-typed file-like for tests.

    ``oldest_record_ts`` is back-dated by ``age_days`` from ``now`` so
    the matcher's age computation falls out for free.
    """
    if now is None:
        now = datetime(2026, 5, 23, 0, 0, 0)
    return SimpleNamespace(
        source=source,
        level=level,
        category=category,
        oldest_record_ts=now - timedelta(days=age_days),
        tier=tier,
    )


# ---------------------------------------------------------------------------
# pick_policy
# ---------------------------------------------------------------------------


class TestPickPolicy:
    """Selector matching + the (priority, specificity, name) tiebreaker."""

    def test_pick_policy_returns_none_when_no_match(self) -> None:
        """No policy whose selector matches → None."""
        ps = PolicySet(
            policies=[
                Policy(
                    name="only",
                    selector=Selector(category="y"),
                    phases=[Phase(after_days=1, action="delete")],
                ),
            ]
        )
        file = _make_file(category="x")
        assert pick_policy(file, ps) is None

    def test_pick_policy_returns_only_matching_policy(self) -> None:
        """A single matching policy is returned unchanged."""
        winner = Policy(
            name="match",
            selector=Selector(category="x"),
            phases=[Phase(after_days=1, action="delete")],
        )
        ps = PolicySet(policies=[winner])
        file = _make_file(category="x")
        assert pick_policy(file, ps) is winner

    def test_pick_policy_higher_priority_wins(self) -> None:
        """Higher ``priority`` beats lower regardless of order."""
        low = Policy(
            name="low",
            selector=Selector(category="x"),
            priority=10,
            phases=[Phase(after_days=1, action="delete")],
        )
        high = Policy(
            name="high",
            selector=Selector(category="x"),
            priority=100,
            phases=[Phase(after_days=1, action="delete")],
        )
        ps = PolicySet(policies=[low, high])
        file = _make_file(category="x")
        assert pick_policy(file, ps) is high

    def test_pick_policy_tie_priority_higher_specificity_wins(self) -> None:
        """Same priority → more specific selector wins."""
        broad = Policy(
            name="broad",
            selector=Selector(category="x"),  # specificity 1
            priority=100,
            phases=[Phase(after_days=1, action="delete")],
        )
        narrow = Policy(
            name="narrow",
            selector=Selector(category="x", source="y"),  # specificity 2
            priority=100,
            phases=[Phase(after_days=1, action="delete")],
        )
        ps = PolicySet(policies=[broad, narrow])
        file = _make_file(category="x", source="y")
        assert pick_policy(file, ps) is narrow

    def test_pick_policy_tie_priority_and_specificity_name_wins(self) -> None:
        """Final tiebreaker: lexicographically smallest ``name``."""
        alpha = Policy(
            name="alpha",
            selector=Selector(category="x"),
            priority=100,
            phases=[Phase(after_days=1, action="delete")],
        )
        beta = Policy(
            name="beta",
            selector=Selector(category="x"),
            priority=100,
            phases=[Phase(after_days=1, action="delete")],
        )
        ps = PolicySet(policies=[beta, alpha])  # input order shouldn't matter
        file = _make_file(category="x")
        assert pick_policy(file, ps) is alpha


# ---------------------------------------------------------------------------
# next_due_phase
# ---------------------------------------------------------------------------


class TestNextDuePhase:
    """Age threshold + already-applied-by-tier skipping rules."""

    NOW = datetime(2026, 5, 23, 12, 0, 0)

    def _policy(self, phases: list[Phase]) -> Policy:
        return Policy(
            name="p",
            selector=Selector(),
            phases=phases,
        )

    def test_next_due_phase_returns_none_when_file_too_young(self) -> None:
        """Phase at after_days=30 + file 5 days old → None."""
        policy = self._policy(
            [Phase(after_days=30, action="promote", target_tier="warm")]
        )
        file = _make_file(age_days=5, tier="hot", now=self.NOW)
        assert next_due_phase(file, policy, self.NOW) is None

    def test_next_due_phase_returns_first_due_phase(self) -> None:
        """File 35 d, phases at 0/30/90 → the 30-day phase wins.

        The 0-day phase moves into ``hot`` (no-op for a hot file) and
        is therefore already-applied; the 30-day phase is the next due.
        """
        policy = self._policy(
            [
                Phase(after_days=0, action="promote", target_tier="hot"),
                Phase(after_days=30, action="promote", target_tier="warm"),
                Phase(after_days=90, action="archive", target_tier="archive"),
            ]
        )
        file = _make_file(age_days=35, tier="hot", now=self.NOW)
        result = next_due_phase(file, policy, self.NOW)
        assert result is not None
        assert result.after_days == 30
        assert result.target_tier == "warm"

    def test_next_due_phase_skips_already_applied_promote(self) -> None:
        """File already at ``warm``, age 35 → skip warm phase, return cold."""
        policy = self._policy(
            [
                Phase(after_days=0, action="promote", target_tier="warm"),
                Phase(after_days=30, action="promote", target_tier="cold"),
            ]
        )
        file = _make_file(age_days=35, tier="warm", now=self.NOW)
        result = next_due_phase(file, policy, self.NOW)
        assert result is not None
        assert result.target_tier == "cold"

    def test_next_due_phase_skips_compress_into_current_tier(self) -> None:
        """File at cold + compress→cold at 30d → already applied → None."""
        policy = self._policy(
            [
                Phase(
                    after_days=30,
                    action="compress",
                    target_tier="cold",
                    compression_level=3,
                ),
            ]
        )
        file = _make_file(age_days=60, tier="cold", now=self.NOW)
        assert next_due_phase(file, policy, self.NOW) is None

    def test_next_due_phase_archive_into_archive_treated_as_applied(self) -> None:
        """File at archive tier + archive→archive at 90d → already applied."""
        policy = self._policy(
            [
                Phase(
                    after_days=90,
                    action="archive",
                    target_tier="archive",
                    compression_level=19,
                ),
            ]
        )
        file = _make_file(age_days=120, tier="archive", now=self.NOW)
        assert next_due_phase(file, policy, self.NOW) is None

    def test_next_due_phase_delete_returned_when_file_not_pending(self) -> None:
        """File at archive past delete phase → delete is next."""
        policy = self._policy(
            [
                Phase(after_days=90, action="archive", target_tier="archive"),
                Phase(after_days=365, action="delete"),
            ]
        )
        file = _make_file(age_days=400, tier="archive", now=self.NOW)
        result = next_due_phase(file, policy, self.NOW)
        assert result is not None
        assert result.action == "delete"

    def test_next_due_phase_delete_skipped_when_file_pending(self) -> None:
        """File at pending + delete phase reached → None (mid-delete)."""
        policy = self._policy(
            [
                Phase(after_days=90, action="archive", target_tier="archive"),
                Phase(after_days=365, action="delete"),
            ]
        )
        file = _make_file(age_days=400, tier="pending", now=self.NOW)
        assert next_due_phase(file, policy, self.NOW) is None

    def test_next_due_phase_returns_none_when_all_phases_applied(self) -> None:
        """File at archive, every prior tier-moving phase done → None."""
        policy = self._policy(
            [
                Phase(after_days=0, action="promote", target_tier="hot"),
                Phase(after_days=30, action="promote", target_tier="warm"),
                Phase(after_days=90, action="archive", target_tier="archive"),
            ]
        )
        file = _make_file(age_days=1000, tier="archive", now=self.NOW)
        assert next_due_phase(file, policy, self.NOW) is None

    def test_age_computation_handles_microseconds(self) -> None:
        """File age 30.5 d, phase at 30 → phase returned (fractional days)."""
        policy = self._policy(
            [Phase(after_days=30, action="promote", target_tier="warm")]
        )
        file = _make_file(age_days=30.5, tier="hot", now=self.NOW)
        result = next_due_phase(file, policy, self.NOW)
        assert result is not None
        assert result.after_days == 30


# ---------------------------------------------------------------------------
# Integration: end-to-end against the demo YAML
# ---------------------------------------------------------------------------


class TestFullFlowAgainstDemoYaml:
    """Walk a SOX-tagged file through its lifecycle using the real config.

    Catches off-by-one bugs between the YAML loader's parse and the
    matcher's day-threshold logic. The SOX policy is a fat one (4
    phases at 0/30/90/365), so it exercises each branch.
    """

    NOW = datetime(2026, 5, 23, 12, 0, 0)

    def test_full_flow_against_demo_yaml(self) -> None:
        ps = load_policy_set(CONFIG_YAML)

        # A payment-category file: only the SOX payment policy should
        # match, since `payment_logs_sox` has the highest priority (1000)
        # and is the only policy with `category=payment`.
        file = _make_file(category="payment", age_days=0, tier="hot", now=self.NOW)
        policy = pick_policy(file, ps)
        assert policy is not None
        assert policy.name == "payment_logs_sox"
        assert policy.compliance_tag == "sox"

        # At age 0, tier=hot: promote→hot is already applied; next is
        # the 30-day promote→warm phase — but the file isn't 30 days old
        # yet, so the function returns None.
        assert next_due_phase(file, policy, self.NOW) is None

        # Age 30 d, still at hot → promote→warm is now due.
        file = _make_file(category="payment", age_days=30, tier="hot", now=self.NOW)
        phase = next_due_phase(file, policy, self.NOW)
        assert phase is not None
        assert phase.after_days == 30
        assert phase.target_tier == "warm"

        # Age 90 d, now at warm → compress→cold is due.
        file = _make_file(category="payment", age_days=90, tier="warm", now=self.NOW)
        phase = next_due_phase(file, policy, self.NOW)
        assert phase is not None
        assert phase.after_days == 90
        assert phase.target_tier == "cold"

        # Age 365 d, now at cold → archive→archive is due.
        file = _make_file(category="payment", age_days=365, tier="cold", now=self.NOW)
        phase = next_due_phase(file, policy, self.NOW)
        assert phase is not None
        assert phase.after_days == 365
        assert phase.target_tier == "archive"

        # Age 365 d, already at archive → lifecycle complete (no delete
        # phase on SOX — kept indefinitely).
        file = _make_file(category="payment", age_days=365, tier="archive", now=self.NOW)
        assert next_due_phase(file, policy, self.NOW) is None
