import asyncio
from datetime import datetime, timezone

import structlog

from src.db import get_alive_workers, mark_worker_dead

logger = structlog.get_logger()


async def heartbeat_checker(interval: int, timeout: int) -> None:
    """Check worker heartbeats. Mark workers as DEAD if they miss heartbeats."""
    while True:
        await asyncio.sleep(interval)
        try:
            workers = await get_alive_workers()
            now = datetime.now(timezone.utc)
            for w in workers:
                last_hb = w["last_heartbeat"]
                # Ensure timezone-aware comparison
                if last_hb.tzinfo is None:
                    last_hb = last_hb.replace(tzinfo=timezone.utc)
                elapsed = (now - last_hb).total_seconds()
                if elapsed > timeout:
                    logger.warning(
                        "worker_marked_dead",
                        worker_id=w["id"],
                        elapsed_seconds=elapsed,
                        timeout=timeout,
                    )
                    await mark_worker_dead(w["id"])
        except Exception as e:
            logger.error("heartbeat_checker_error", error=str(e))
