"""Module-level singletons shared across the application.

Per the requirements doc, AuthService / RBACEngine / AuditService MUST be
instantiated exactly once and shared. Do NOT construct them anywhere else.

`audit_service` is intentionally a stub until C8 (it has the right surface area
so callers can wire to it now). C8 swaps in the real AuditService.
"""
from __future__ import annotations

from typing import Any

from src.auth.service import AuthService
from src.rbac.engine import RBACEngine


class _AuditServiceStub:
    """Placeholder. Implements .append(entry) as a no-op so dependent code can wire to it.
    Replaced by the real AuditService in C8."""

    def append(self, entry: Any) -> None:  # noqa: D401
        return None

    def __repr__(self) -> str:
        return "<AuditServiceStub: replaced by real AuditService in C8>"


# The singletons. Import these — do NOT construct your own instance.
auth_service: AuthService = AuthService()
rbac_engine: RBACEngine = RBACEngine()
audit_service: Any = _AuditServiceStub()
