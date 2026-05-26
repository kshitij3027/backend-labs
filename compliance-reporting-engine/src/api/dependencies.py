"""Shared FastAPI dependencies for request handlers.

Two seams live here:

  * :func:`get_session` — yields a per-request ``AsyncSession`` bound
    to ``app.state.session_factory``. A ``rollback`` runs on any
    exception so a half-mutated transaction can't leak into the next
    request, then the ``async with`` block disposes the session
    cleanly. This matches the pattern used by the sibling
    ``gdpr-log-erasure-system`` project.
  * :func:`get_signing_key` — plain helper that pulls the primary
    HMAC signing key off ``app.state``. Routes that need to re-verify
    a stored signature use this instead of touching ``app.state``
    directly so they stay easy to unit-test (the test app fixture can
    inject any bytes here).

These helpers deliberately don't own auth — that's out of scope for
this project. A real deployment would layer Depends(...) over the top.
"""
from __future__ import annotations

from typing import AsyncIterator, Optional

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Yield a per-request ``AsyncSession`` bound to ``app.state.session_factory``.

    Wrapping the yield in a ``try / except Exception`` + ``rollback``
    keeps a partially-mutated transaction from leaking when the route
    raises mid-flight. The ``async with`` block on the factory handles
    the underlying ``aclose()`` so resources are released either way.
    """
    factory = request.app.state.session_factory
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


def get_signing_key(request: Request) -> bytes:
    """Return the primary HMAC signing key stashed on ``app.state``."""
    return request.app.state.signing_key


def get_secondary_signing_key(request: Request) -> Optional[bytes]:
    """Return the (optional) secondary HMAC signing key, or ``None``."""
    return getattr(request.app.state, "secondary_signing_key", None)
