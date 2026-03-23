import asyncio

import httpx
import structlog

logger = structlog.get_logger()


async def heartbeat_loop(worker_id: str, coordinator_url: str, interval: int) -> None:
    """Send heartbeat to coordinator every `interval` seconds."""
    async with httpx.AsyncClient() as client:
        while True:
            try:
                resp = await client.post(
                    f"{coordinator_url}/workers/{worker_id}/heartbeat"
                )
                logger.debug(
                    "heartbeat_sent",
                    worker_id=worker_id,
                    status_code=resp.status_code,
                )
            except Exception as e:
                logger.warning("heartbeat_failed", worker_id=worker_id, error=str(e))
            await asyncio.sleep(interval)
