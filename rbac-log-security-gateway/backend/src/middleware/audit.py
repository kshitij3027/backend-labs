"""HTTP middleware that records every request to the audit log."""
from __future__ import annotations

import time
from typing import Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from src.audit.models import AuditEntry, SecurityEvent, utc_now
from src.audit.service import AuditService
from src.auth.jwt import InvalidTokenError, decode_token
from src.auth.users import default_store


class AuditMiddleware(BaseHTTPMiddleware):
    """Writes an AuditEntry for every request. Emits a SecurityEvent for 401/403/auth fails."""

    def __init__(self, app, audit_service: AuditService) -> None:
        super().__init__(app)
        self._audit = audit_service

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start = time.perf_counter()
        # Best-effort user resolution from bearer token; never raises here.
        username, user_id = self._peek_user(request)
        response: Response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000.0

        # Determine decision tag from response state if available.
        decision = "n/a"
        rule = None
        reason = None
        if hasattr(request.state, "decision"):
            d = request.state.decision
            decision = "allow" if d.allow else "deny"
            rule = d.rule
            reason = d.reason

        entry = AuditEntry(
            timestamp=utc_now(),
            user_id=user_id,
            username=username,
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration_ms, 3),
            source_ip=self._client_ip(request),
            user_agent=request.headers.get("user-agent"),
            decision=decision,
            rule=rule,
            reason=reason,
        )
        self._audit.append(entry)

        # Security events on 401/403.
        if response.status_code in (401, 403):
            event_type = "auth_failure" if response.status_code == 401 else "authz_denied"
            self._audit.append_security_event(SecurityEvent(
                timestamp=utc_now(),
                event_type=event_type,
                username=username,
                path=request.url.path,
                status=response.status_code,
                source_ip=self._client_ip(request),
                reason=reason or f"http_{response.status_code}",
            ))

        return response

    # --- helpers ----------------------------------------------------------- #
    def _peek_user(self, request: Request) -> tuple[Optional[str], Optional[str]]:
        auth = request.headers.get("authorization") or ""
        if not auth.lower().startswith("bearer "):
            return (None, None)
        token = auth.split(" ", 1)[1].strip()
        try:
            payload = decode_token(token)
        except InvalidTokenError:
            return (None, None)
        username = payload.get("username")
        if not username:
            return (None, None)
        user = default_store.get(username)
        if user is None:
            return (username, None)
        return (user.username, user.user_id)

    @staticmethod
    def _client_ip(request: Request) -> Optional[str]:
        # Honor X-Forwarded-For if present (one hop only — demo grade).
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        if request.client:
            return request.client.host
        return None
