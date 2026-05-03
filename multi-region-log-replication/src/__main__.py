"""Entry point for ``python -m src``.

Loads ``AppConfig`` from environment, builds the real FastAPI app via
:func:`http_server.create_app`, and runs it under uvicorn. The bare
placeholder used during commit 1 was retired in commit 4 — this
module now wires the full HTTP + WS surface into a runnable server.
"""

from __future__ import annotations

import logging

import uvicorn

from src.config import AppConfig
from src.http_server import create_app


def main() -> None:
    """Boot the multi-region log replication service."""
    config = AppConfig.from_env()
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("multi-region-log-replication")
    log.info(
        "starting app host=%s port=%s regions=%s primary_pref=%s",
        config.host,
        config.port,
        config.regions,
        config.primary_preference,
    )
    app = create_app(config)
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower(),
    )


if __name__ == "__main__":
    main()
