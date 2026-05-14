"""Role → permission policies. Locked strings from plan.md (do not reorder).

Tag application:
- analyst's `logs:read:business.*` carries the `aggregated_only` tag.
- ALL of support's allows carry the `mask_pii` tag.
- administrator and developer carry no tags.
"""
from __future__ import annotations

from typing import Dict, List

from src.rbac.permissions import Permission, parse


def _role_administrator() -> List[Permission]:
    return [
        parse("logs:read:*"),
        parse("logs:export:*"),
        parse("logs:audit:*"),
        parse("logs:admin:*"),
        parse("!logs:export:business.financial"),
    ]


def _role_developer() -> List[Permission]:
    return [
        parse("logs:read:application.*"),
        parse("logs:export:application.*"),
        parse("logs:read:system.kernel"),
        parse("!logs:read:business.*"),
        parse("!logs:export:system.*"),
    ]


def _role_analyst() -> List[Permission]:
    return [
        parse("logs:read:business.*", tags=["aggregated_only"]),
        parse("logs:export:business.metrics"),
        parse("!logs:read:business.customer"),
        parse("!logs:export:business.financial"),
    ]


def _role_support() -> List[Permission]:
    return [
        parse("logs:read:application.auth", tags=["mask_pii"]),
        parse("logs:read:application.api", tags=["mask_pii"]),
        parse("logs:read:business.customer", tags=["mask_pii"]),
        parse("!logs:export:*"),
        parse("!logs:read:system.*"),
    ]


# The locked role table. Keys are case-sensitive role names; values are ordered
# permission lists (order matters for deny short-circuit + first-allow-wins).
ROLE_POLICIES: Dict[str, List[Permission]] = {
    "administrator": _role_administrator(),
    "developer": _role_developer(),
    "analyst": _role_analyst(),
    "support": _role_support(),
}


# Default resource scope per role (used by frontend prompts / API filters,
# not enforced separately — the rules above are the source of truth).
DEFAULT_SCOPES: Dict[str, str] = {
    "administrator": "*",
    "developer": "application",
    "analyst": "business",
    "support": "application.auth",
}
