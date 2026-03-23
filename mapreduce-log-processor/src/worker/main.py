import asyncio
import platform
import signal
import uuid

import httpx
import structlog

from src.config import settings
from src.redis_client import close_redis, init_redis
from src.worker.heartbeat import heartbeat_loop
from src.worker.mapper import close_binary_redis

logger = structlog.get_logger()


def generate_worker_id() -> str:
    """Generate a worker ID from hostname + short UUID."""
    hostname = platform.node()
    short_id = uuid.uuid4().hex[:8]
    return f"{hostname}-{short_id}"


async def task_poll_loop(worker_id: str, coordinator_url: str) -> None:
    """Poll coordinator for tasks and execute them."""
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            try:
                # Poll for a task
                resp = await client.get(
                    f"{coordinator_url}/tasks/next",
                    params={"worker_id": worker_id},
                )

                if resp.status_code == 204:
                    # No task available
                    await asyncio.sleep(2)
                    continue

                if resp.status_code != 200:
                    logger.warning(
                        "task_poll_unexpected_status",
                        worker_id=worker_id,
                        status_code=resp.status_code,
                    )
                    await asyncio.sleep(2)
                    continue

                task = resp.json()
                task_id = task["id"]
                task_type = task["type"]

                logger.info("task_received", worker_id=worker_id, task_id=task_id, type=task_type)

                try:
                    if task_type == "MAP":
                        from src.worker.mapper import execute_map_task

                        await execute_map_task(task)

                    # Report completion
                    await client.post(f"{coordinator_url}/tasks/{task_id}/complete")
                    logger.info("task_completed", worker_id=worker_id, task_id=task_id)

                except Exception as e:
                    logger.error("task_execution_error", worker_id=worker_id, task_id=task_id, error=str(e))
                    # Report failure
                    try:
                        await client.post(f"{coordinator_url}/tasks/{task_id}/failed")
                    except Exception:
                        pass

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("task_poll_error", worker_id=worker_id, error=str(e))
                await asyncio.sleep(2)


async def register_worker(worker_id: str, coordinator_url: str) -> None:
    """Register this worker with the coordinator."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{coordinator_url}/workers/register",
            json={"worker_id": worker_id},
        )
        resp.raise_for_status()
    logger.info("worker_registered", worker_id=worker_id)


def _register_map_functions() -> None:
    """Import all map function modules to trigger registration."""
    import src.mapfunctions.error_code  # noqa: F401
    import src.mapfunctions.url_path  # noqa: F401
    import src.mapfunctions.word_count  # noqa: F401


async def main() -> None:
    worker_id = settings.WORKER_ID or generate_worker_id()
    coordinator_url = f"http://{settings.COORDINATOR_HOST}:{settings.COORDINATOR_PORT}"

    logger.info(
        "worker_starting",
        worker_id=worker_id,
        coordinator_url=coordinator_url,
    )

    # Register map/reduce functions
    _register_map_functions()

    # Initialize Redis for mapper intermediate data
    await init_redis()

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
    poll_task = asyncio.create_task(task_poll_loop(worker_id, coordinator_url))

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

    # Close Redis connections
    await close_binary_redis()
    await close_redis()

    logger.info("worker_stopped", worker_id=worker_id)


if __name__ == "__main__":
    asyncio.run(main())
