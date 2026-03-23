import asyncio
from datetime import datetime, timezone

import structlog

from src.config import settings
from src.db import get_alive_workers, mark_worker_dead, reassign_tasks_for_worker, fail_task_and_check_job

logger = structlog.get_logger()


async def heartbeat_checker(interval: int, timeout: int) -> None:
    """Check worker heartbeats. Mark workers as DEAD if they miss heartbeats.
    When a worker is marked DEAD, reassign its running tasks."""
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

                    # Reassign tasks that were running on this dead worker
                    affected = await reassign_tasks_for_worker(
                        w["id"], settings.MAX_RETRIES
                    )
                    for task in affected:
                        if task["new_status"] == "PENDING":
                            logger.info(
                                "task_reassigned",
                                task_id=task["id"],
                                worker_id=w["id"],
                                retry_count=task["retry_count"],
                            )
                        elif task["new_status"] == "FAILED":
                            logger.warning(
                                "task_exceeded_retries",
                                task_id=task["id"],
                                worker_id=w["id"],
                                retry_count=task["retry_count"],
                            )
                            # Check if job should be failed
                            await fail_task_and_check_job(
                                task["id"], settings.MAX_RETRIES
                            )
            # After checking heartbeats, also check for stragglers
            try:
                from src.coordinator.straggler import detect_stragglers
                await detect_stragglers()
            except Exception as straggler_err:
                logger.error("straggler_detection_error", error=str(straggler_err))

        except Exception as e:
            logger.error("heartbeat_checker_error", error=str(e))
