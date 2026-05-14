"""RBACEngine: resolves a user's permissions and answers check() queries."""
from __future__ import annotations

from typing import Dict, List

from src.auth.users import User
from src.rbac.permissions import Decision, Permission, match
from src.rbac.roles import ROLE_POLICIES


class RBACEngine:
    """Stateless wrapper over the role-policy table. Safe to share."""

    def __init__(self, role_policies: Dict[str, List[Permission]] | None = None) -> None:
        # Default to the module-level locked table.
        self._role_policies = role_policies if role_policies is not None else ROLE_POLICIES

    def resolve(self, user: User) -> List[Permission]:
        """Concatenate (deny + allow rules in declared order) across all of the user's roles.

        Per the matcher contract, deny rules are evaluated globally first. Concatenation
        preserves the relative order within each role and across the union.
        """
        merged: List[Permission] = []
        for role in user.roles:
            merged.extend(self._role_policies.get(role, []))
        return merged

    def check(self, user: User, requested: str) -> Decision:
        return match(self.resolve(user), requested)

    def known_roles(self) -> tuple[str, ...]:
        return tuple(sorted(self._role_policies.keys()))
