"""Process entry point for the dashboard container.

Mirrors the structure of :mod:`src.__main__` but builds the
:func:`src.dashboard.create_app` factory instead of a FailoverNode.

The dashboard never holds the leader lock and never participates in the
election; it's purely an observer + UI host. Listening for SIGINT /
SIGTERM is still useful so ``docker compose stop`` cleans up the WS
connections gracefully.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import uvicorn

from src.dashboard import create_app

logger = logging.getLogger(__name__)


async def _run() -> None:
    """Build the dashboard app and serve it under uvicorn."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    app = create_app()
    config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="info")
    server = uvicorn.Server(config)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _graceful_shutdown(signame: str) -> None:
        logger.info("dashboard received %s — shutting down", signame)
        server.should_exit = True
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _graceful_shutdown, sig.name)
        except NotImplementedError:  # pragma: no cover — Windows
            pass

    server_task = asyncio.create_task(server.serve(), name="dashboard-uvicorn")
    try:
        await stop_event.wait()
    finally:
        await server_task


if __name__ == "__main__":
    asyncio.run(_run())
