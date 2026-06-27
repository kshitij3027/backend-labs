"""ASGI / uvicorn entry point for the Predictive Log Analytics Engine.

The production container launches the app via the module-level instance::

    uvicorn src.api:app --host 0.0.0.0 --port 8000

Running this module directly (``python -m src.main``) starts a uvicorn server on the
configured ``api_host`` / ``api_port`` (from :func:`src.config.get_settings`) for
local development. The same module-level ``app`` is re-exported here for convenience.
"""

from __future__ import annotations

from src.api import app  # re-exported so `uvicorn src.main:app` also works


def main() -> None:
    """Run a uvicorn server using host/port from settings."""
    import uvicorn

    from src.config import get_settings

    settings = get_settings()
    uvicorn.run(
        "src.api:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
