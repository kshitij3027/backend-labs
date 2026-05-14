"""End-to-end tests for RBACEngine.check(user, action). Locked matrix from the plan."""
import pytest

from src.auth.users import User
from src.rbac.engine import RBACEngine


def _user(role: str) -> User:
    """Build a minimal User for a single role. user_id/password_hash are irrelevant for RBAC tests."""
    return User(user_id=f"u-{role}", username=role, password_hash="x", roles=(role,))


@pytest.fixture(scope="module")
def engine() -> RBACEngine:
    return RBACEngine()


def test_known_roles(engine: RBACEngine) -> None:
    assert engine.known_roles() == ("administrator", "analyst", "developer", "support")


# Matrix: (role, requested, expected_allow, expected_reason_substring)
MATRIX = [
    # administrator — has read:* etc., but deny on export:business.financial
    ("administrator", "logs:read:application.auth", True, "allow match"),
    ("administrator", "logs:read:business.customer", True, "allow match"),
    ("administrator", "logs:export:business.financial", False, "explicit deny"),
    ("administrator", "logs:admin:rbac-policies", True, "allow match"),

    # developer — application.* OK; business.* denied; system.kernel read OK
    ("developer", "logs:read:application.auth", True, "allow match"),
    ("developer", "logs:read:application.worker", True, "allow match"),
    ("developer", "logs:read:business.metrics", False, "explicit deny"),
    ("developer", "logs:read:business.customer", False, "explicit deny"),
    ("developer", "logs:read:system.kernel", True, "allow match"),
    ("developer", "logs:export:system.kernel", False, "explicit deny"),
    ("developer", "logs:admin:rbac-policies", False, "no matching rule"),

    # analyst — business.* read OK except customer; export:business.metrics only
    ("analyst", "logs:read:business.metrics", True, "allow match"),
    ("analyst", "logs:read:business.customer", False, "explicit deny"),
    ("analyst", "logs:read:application.auth", False, "no matching rule"),
    ("analyst", "logs:export:business.metrics", True, "allow match"),
    ("analyst", "logs:export:business.financial", False, "explicit deny"),

    # support — limited reads only, no exports, no system
    ("support", "logs:read:application.auth", True, "allow match"),
    ("support", "logs:read:business.customer", True, "allow match"),
    ("support", "logs:read:system.kernel", False, "explicit deny"),
    ("support", "logs:export:application.auth", False, "explicit deny"),
]


@pytest.mark.parametrize("role,requested,expected_allow,expected_reason", MATRIX)
def test_rbac_matrix(
    engine: RBACEngine, role: str, requested: str, expected_allow: bool, expected_reason: str
) -> None:
    user = _user(role)
    decision = engine.check(user, requested)
    assert decision.allow is expected_allow, (
        f"{role} requesting {requested!r}: expected allow={expected_allow}, "
        f"got allow={decision.allow} (reason={decision.reason!r}, rule={decision.rule!r})"
    )
    assert decision.reason == expected_reason


def test_analyst_aggregated_only_tag_propagates(engine: RBACEngine) -> None:
    user = _user("analyst")
    d = engine.check(user, "logs:read:business.metrics")
    assert d.allow is True
    assert "aggregated_only" in d.tags


def test_support_mask_pii_tag_propagates(engine: RBACEngine) -> None:
    user = _user("support")
    d = engine.check(user, "logs:read:business.customer")
    assert d.allow is True
    assert "mask_pii" in d.tags


def test_unknown_role_returns_deny(engine: RBACEngine) -> None:
    """A user with a role we don't recognize should be denied everything."""
    user = User(user_id="u", username="x", password_hash="y", roles=("nobody",))
    d = engine.check(user, "logs:read:application.auth")
    assert d.allow is False
    assert d.reason == "no matching rule"


def test_multi_role_user_unions_permissions(engine: RBACEngine) -> None:
    """A user with both developer + analyst roles can read both application and business."""
    user = User(
        user_id="u", username="multi", password_hash="x",
        roles=("developer", "analyst"),
    )
    # developer allows application.auth
    assert engine.check(user, "logs:read:application.auth").allow is True
    # analyst allows business.metrics (but developer denies business.*; deny wins)
    d = engine.check(user, "logs:read:business.metrics")
    assert d.allow is False  # developer's `!logs:read:business.*` deny still applies
    assert d.reason == "explicit deny"


def test_engine_resolve_returns_union_of_role_permissions(engine: RBACEngine) -> None:
    user = User(user_id="u", username="multi", password_hash="x", roles=("administrator", "support"))
    perms = engine.resolve(user)
    # administrator has 5 + support has 5 = 10
    assert len(perms) == 10
