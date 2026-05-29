"""FastAPI dependency providers backed by ``app.state``.

The :class:`~src.batcher.AdaptiveBatcher` and :class:`~src.settings.Settings`
instances are created once during the lifespan startup and stashed on
``app.state``. These thin providers hand them to route handlers via
``Annotated[..., Depends(...)]`` so the routes never reach into ``app.state``
directly.
"""

from __future__ import annotations

from fastapi import Request

from src.batcher import AdaptiveBatcher
from src.settings import Settings


def get_batcher(request: Request) -> AdaptiveBatcher:
    """Return the process-wide :class:`AdaptiveBatcher` from ``app.state``."""
    return request.app.state.batcher


def get_settings_dep(request: Request) -> Settings:
    """Return the cached :class:`Settings` instance from ``app.state``."""
    return request.app.state.settings
