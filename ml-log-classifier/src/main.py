"""ASGI entry point for the ML Log Classifier service (Commit 8).

Exposes a module-level ``app`` so the production container can launch it with::

    uvicorn src.main:app --host 0.0.0.0 --port 8000

(which is exactly the Dockerfile ``CMD``). The app is built by
:func:`src.api.create_app` with the process configuration and ``auto_train=True``,
so on first boot — when no model has been persisted yet — it trains and persists a
model during startup and is ready to classify as soon as it accepts connections.

Running this module directly (``python -m src.main``) starts a uvicorn server on
the configured ``host``/``port`` for local development.
"""

from __future__ import annotations

from src.api import create_app

#: The ASGI application served by uvicorn (``uvicorn src.main:app``). Built once
#: at import with the process config and auto-train enabled.
app = create_app()


if __name__ == "__main__":
    import uvicorn

    from src.config import get_config

    cfg = get_config()
    uvicorn.run("src.main:app", host=cfg.host, port=cfg.port)
