"""Process entry point: ``python -m src``.

Loads :class:`NodeConfig` from the environment, builds a
:class:`FailoverNode`, and serves its FastAPI app under uvicorn. Listens
for ``SIGINT`` / ``SIGTERM`` and shuts the node down cleanly.

Why a custom event loop wrapper instead of ``uvicorn.run(...)``:

* uvicorn's :py:meth:`Server.serve` is async-friendly and lets us
  ``asyncio.gather`` it alongside the node's own startup logic.
* We need to call ``await node.start()`` BEFORE uvicorn binds the
  socket so the node's state is already PRIMARY/STANDBY by the time
  the first ``GET /health`` arrives.
* On SIGTERM we set ``server.should_exit`` (uvicorn's idiomatic
  graceful-shutdown flag) and await both the server task and
  ``node.stop()``.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import uvicorn

from src.config import NodeConfig
from src.node import FailoverNode

logger = logging.getLogger(__name__)


async def _run() -> None:
    """Build, start, serve, and tear down a FailoverNode."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    config = NodeConfig()  # type: ignore[call-arg]  # pydantic env-loaded
    node = FailoverNode(config)
    await node.start()

    uvicorn_config = uvicorn.Config(
        node.app,
        host="0.0.0.0",
        port=config.port,
        log_level="info",
    )
    server = uvicorn.Server(uvicorn_config)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _graceful_shutdown(signame: str) -> None:
        logger.info("received %s — shutting down", signame)
        server.should_exit = True
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _graceful_shutdown, sig.name)
        except NotImplementedError:  # pragma: no cover — Windows
            # Windows event loops don't support add_signal_handler;
            # uvicorn handles signals itself via its install_signal_handlers
            # path on those platforms.
            pass

    server_task = asyncio.create_task(server.serve(), name="uvicorn-serve")
    try:
        await stop_event.wait()
    finally:
        # Wait for uvicorn to drain in-flight requests, then stop the node.
        await server_task
        await node.stop()


if __name__ == "__main__":
    asyncio.run(_run())
