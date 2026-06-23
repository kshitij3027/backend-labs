"""ASGI entry point for the Real-Time Log Pattern Clustering engine.

Exposes a module-level ``app`` so the production container can launch it with::

    uvicorn src.main:app --host 0.0.0.0 --port 8000

(which is exactly the Dockerfile ``CMD``). Running this module directly
(``python -m src.main``) starts a uvicorn server on the configured ``api.host`` /
``api.port`` for local development.
"""

from __future__ import annotations

from src.api import create_app

#: The ASGI application served by uvicorn (``uvicorn src.main:app``).
app = create_app()


if __name__ == "__main__":
    import uvicorn

    from src.config import get_config

    cfg = get_config()
    uvicorn.run("src.main:app", host=cfg.api.host, port=cfg.api.port)
