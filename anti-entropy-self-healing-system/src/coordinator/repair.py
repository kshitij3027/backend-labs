import time
import structlog
from src.coordinator.scanner import RepairTask

logger = structlog.get_logger()


class RepairWorker:
    def __init__(self):
        self._queue: list[RepairTask] = []

    def add_tasks(self, tasks: list[RepairTask]):
        self._queue.extend(tasks)
        # Sort by priority (higher priority first)
        self._queue.sort(key=lambda t: t.priority, reverse=True)

    def execute_repairs(self) -> tuple[int, int]:
        """Execute all queued repairs. Returns (completed, failed)."""
        completed = 0
        failed = 0
        while self._queue:
            task = self._queue.pop(0)
            start = time.time()
            success = task.target_node.put_data(
                key=task.key,
                value=task.value,
                version=task.version,
                timestamp=task.timestamp,
            )
            duration = time.time() - start
            if success:
                completed += 1
                logger.info("repair.completed", key=task.key, target=task.target_node.node_id, duration_ms=round(duration * 1000))
            else:
                failed += 1
                logger.warning("repair.failed", key=task.key, target=task.target_node.node_id)
        return completed, failed

    @property
    def queue_size(self) -> int:
        return len(self._queue)
