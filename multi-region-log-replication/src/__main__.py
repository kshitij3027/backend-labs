"""Entry point for ``python -m src``.

Loads ``AppConfig`` from environment, constructs a FastAPI app, and runs it
under uvicorn. The full ``http_server.create_app`` factory lands in commit 4
— for now we run a bare ``FastAPI()`` instance with no routes so commit 1 can
verify the container boots and listens on the configured port.
"""

from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI

from src.config import AppConfig


def _build_placeholder_app(config: AppConfig) -> FastAPI:
    """Bare FastAPI app used until commit 4 wires in real routes.

    Intentionally has no routes — a ``GET /`` request should 404. Commit 1's
    smoke test only verifies the container boots and the port is reachable.
    """
    app = FastAPI(
        title="multi-region-log-replication",
        description=(
            "Scaffold app — routes land in commit 4 via http_server.create_app()."
        ),
    )
    app.state.config = config
    return app


def main() -> None:
    config = AppConfig.from_env()
    logging.basicConfig(
        level=config.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("multi-region-log-replication")
    log.info(
        "starting placeholder app host=%s port=%s regions=%s primary_pref=%s",
        config.host,
        config.port,
        config.regions,
        config.primary_preference,
    )
    # TODO(commit 4): replace with `app = http_server.create_app(config, controller, monitor)`
    app = _build_placeholder_app(config)
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower(),
    )


if __name__ == "__main__":
    main()
