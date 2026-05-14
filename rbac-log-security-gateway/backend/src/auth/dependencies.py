"""FastAPI dependencies for current-user resolution."""
from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer

from src.auth.jwt import InvalidTokenError, decode_token
from src.auth.service import AuthService
from src.auth.users import User, default_store
from src.rbac.engine import RBACEngine

# `auto_error=False` is critical — we want the route handler to decide 401 vs anonymous, and we
# want the audit middleware to be able to peek for an optional user without raising.
_oauth2 = OAuth2PasswordBearer(tokenUrl="/api/auth/login", auto_error=False)


def _user_from_token(token: str | None) -> User | None:
    if not token:
        return None
    try:
        payload = decode_token(token)
    except InvalidTokenError:
        return None
    username = payload.get("username")
    if not username:
        return None
    return default_store.get(username)


def get_optional_user(token: Annotated[str | None, Depends(_oauth2)]) -> User | None:
    """Return the User from the bearer token, or None if no/invalid token. Never raises."""
    return _user_from_token(token)


def get_current_user(
    request: Request,
    token: Annotated[str | None, Depends(_oauth2)],
) -> User:
    """Required-auth variant. Raises 401 if no/invalid token. Stores user on request.state for middleware."""
    user = _user_from_token(token)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    request.state.user = user
    return user


CurrentUser = Annotated[User, Depends(get_current_user)]
OptionalUser = Annotated[User | None, Depends(get_optional_user)]


# --- C7 additions: shared-singleton dependency providers --- #

from src.shared import auth_service as _shared_auth, audit_service as _shared_audit, rbac_engine as _shared_rbac


def get_auth() -> "AuthService":
    """Return the shared AuthService singleton."""
    return _shared_auth


def get_rbac() -> "RBACEngine":
    """Return the shared RBACEngine singleton."""
    return _shared_rbac


def get_audit():
    """Return the shared AuditService singleton (stub until C8)."""
    return _shared_audit
