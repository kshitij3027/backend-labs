"""Tests that the locked role policies match the plan.md table exactly."""
import pytest

from src.rbac.roles import DEFAULT_SCOPES, ROLE_POLICIES


def test_four_roles_defined() -> None:
    assert set(ROLE_POLICIES.keys()) == {"administrator", "developer", "analyst", "support"}


@pytest.mark.parametrize("role,expected_count", [
    ("administrator", 5),  # 4 allows + 1 deny
    ("developer", 5),      # 3 allows + 2 denies
    ("analyst", 4),        # 2 allows + 2 denies
    ("support", 5),        # 3 allows + 2 denies
])
def test_role_has_expected_permission_count(role: str, expected_count: int) -> None:
    assert len(ROLE_POLICIES[role]) == expected_count


def test_administrator_has_admin_wildcard() -> None:
    raws = [p.raw for p in ROLE_POLICIES["administrator"]]
    assert "logs:admin:*" in raws
    assert "!logs:export:business.financial" in raws


def test_developer_blocks_business_logs() -> None:
    raws = [p.raw for p in ROLE_POLICIES["developer"]]
    assert "!logs:read:business.*" in raws


def test_analyst_business_read_tagged_aggregated_only() -> None:
    perms = ROLE_POLICIES["analyst"]
    aggregated = [p for p in perms if "aggregated_only" in p.tags]
    assert len(aggregated) == 1
    assert aggregated[0].raw == "logs:read:business.*"


def test_support_every_allow_tagged_mask_pii() -> None:
    perms = ROLE_POLICIES["support"]
    allows = [p for p in perms if not p.is_deny]
    assert len(allows) == 3
    for p in allows:
        assert "mask_pii" in p.tags


def test_default_scopes() -> None:
    assert DEFAULT_SCOPES == {
        "administrator": "*",
        "developer": "application",
        "analyst": "business",
        "support": "application.auth",
    }
