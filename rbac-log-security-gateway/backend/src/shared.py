"""Module-level singletons shared across the application.

Per the requirements doc, AuthService / RBACEngine / AuditService MUST be
instantiated exactly once and shared. Do NOT construct them anywhere else.
"""
from __future__ import annotations

from src.audit.service import AuditService
from src.auth.service import AuthService
from src.rbac.engine import RBACEngine


# The singletons. Import these — do NOT construct your own instance.
auth_service: AuthService = AuthService()
rbac_engine: RBACEngine = RBACEngine()
audit_service: AuditService = AuditService()
