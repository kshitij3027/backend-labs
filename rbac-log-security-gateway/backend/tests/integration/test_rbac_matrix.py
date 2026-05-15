"""Locked RBAC matrix: 4 users × 8 resources × 2 actions. Source of truth for the role table."""
import pytest
from httpx import AsyncClient


# (action, resource): {role -> expected_status}
ROLE_EXPECTATIONS: dict[tuple[str, str], dict[str, int]] = {
    # --- READ ---
    ("read", "application.auth"):   {"admin": 200, "dev": 200, "analyst": 403, "support": 200},
    ("read", "application.api"):    {"admin": 200, "dev": 200, "analyst": 403, "support": 200},
    ("read", "application.worker"): {"admin": 200, "dev": 200, "analyst": 403, "support": 403},
    ("read", "business.metrics"):   {"admin": 200, "dev": 403, "analyst": 200, "support": 403},
    ("read", "business.financial"): {"admin": 200, "dev": 403, "analyst": 200, "support": 403},
    ("read", "business.customer"):  {"admin": 200, "dev": 403, "analyst": 403, "support": 200},
    ("read", "system.kernel"):      {"admin": 200, "dev": 200, "analyst": 403, "support": 403},
    ("read", "system.audit"):       {"admin": 200, "dev": 403, "analyst": 403, "support": 403},

    # --- EXPORT ---
    ("export", "application.auth"):   {"admin": 200, "dev": 200, "analyst": 403, "support": 403},
    ("export", "application.api"):    {"admin": 200, "dev": 200, "analyst": 403, "support": 403},
    ("export", "application.worker"): {"admin": 200, "dev": 200, "analyst": 403, "support": 403},
    ("export", "business.metrics"):   {"admin": 200, "dev": 403, "analyst": 200, "support": 403},
    ("export", "business.financial"): {"admin": 403, "dev": 403, "analyst": 403, "support": 403},  # the !logs:export:business.financial admin deny
    ("export", "business.customer"):  {"admin": 200, "dev": 403, "analyst": 403, "support": 403},
    ("export", "system.kernel"):      {"admin": 200, "dev": 403, "analyst": 403, "support": 403},
    ("export", "system.audit"):       {"admin": 200, "dev": 403, "analyst": 403, "support": 403},
}


TOKEN_FIXTURE_BY_ROLE = {
    "admin": "admin_token",
    "dev": "dev_token",
    "analyst": "analyst_token",
    "support": "support_token",
}


# Build the flat parametrize list: 64 cells.
_MATRIX_CELLS = [
    (action, resource, role, expected)
    for (action, resource), per_role in ROLE_EXPECTATIONS.items()
    for role, expected in per_role.items()
]


@pytest.mark.asyncio
@pytest.mark.parametrize("action,resource,role,expected_status", _MATRIX_CELLS)
async def test_rbac_matrix(
    async_client: AsyncClient,
    request: pytest.FixtureRequest,
    action: str,
    resource: str,
    role: str,
    expected_status: int,
) -> None:
    token = request.getfixturevalue(TOKEN_FIXTURE_BY_ROLE[role])
    headers = {"Authorization": f"Bearer {token}"}
    path = f"/api/logs/{'search' if action == 'read' else 'export'}?resource={resource}"
    r = await async_client.get(path, headers=headers)
    assert r.status_code == expected_status, (
        f"role={role} action={action} resource={resource}: "
        f"expected {expected_status}, got {r.status_code} (body={r.text[:200]})"
    )
