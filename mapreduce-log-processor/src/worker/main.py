import asyncio
import platform
import signal
import uuid

import httpx
import structlog

from src.config import settings
from src.worker.heartbeat import heartbeat_loop

logger = structlog.get_logger()


def generate_worker_id() -> str:
    """Generate a worker ID from hostname + short UUID."""
    hostname = platform.node()
    short_id = uuid.uuid4().hex[:8]
    return f"{hostname}-{short_id}"


async def task_poll_loop(worker_id: str) -> None:
    """Stub: poll coordinator for tasks. Will be implemented later."""
    while True:
        logger.info("polling_for_tasks", worker_id=worker_id)
        await asyncio.sleep(5)


async def register_worker(worker_id: str, coordinator_url: str) -> None:
    """Register this worker with the coordinator."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{coordinator_url}/workers/register",
            json={"worker_id": worker_id},
        )
        resp.raise_for_status()
    logger.info("worker_registered", worker_id=worker_id)


async def main() -> None:
    worker_id = settings.WORKER_ID or generate_worker_id()
    coordinator_url = f"http://{settings.COORDINATOR_HOST}:{settings.COORDINATOR_PORT}"

    logger.info(
        "worker_starting",
        worker_id=worker_id,
        coordinator_url=coordinator_url,
    )

    # Register with coordinator
    await register_worker(worker_id, coordinator_url)

    # Set up graceful shutdown
    shutdown_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("shutdown_signal_received", worker_id=worker_id)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    # Start background tasks
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(worker_id, coordinator_url, settings.HEARTBEAT_INTERVAL)
    )
    poll_task = asyncio.create_task(task_poll_loop(worker_id))

    logger.info("worker_ready", worker_id=worker_id)

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Cancel background tasks
    logger.info("worker_shutting_down", worker_id=worker_id)
    heartbeat_task.cancel()
    poll_task.cancel()
    try:
        await asyncio.gather(heartbeat_task, poll_task, return_exceptions=True)
    except asyncio.CancelledError:
        pass

    logger.info("worker_stopped", worker_id=worker_id)


if __name__ == "__main__":
    asyncio.run(main())
