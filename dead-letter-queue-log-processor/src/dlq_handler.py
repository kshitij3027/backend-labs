"""DLQ handler for the Dead Letter Queue Log Processor."""

import logging
from collections import Counter

from src.config import Settings
from src.models import FailedMessage, FailureType
from src.redis_client import RedisClient

logger = logging.getLogger(__name__)


class DLQHandler:
    """Manages dead letter queue operations: read, analyze, reprocess, purge."""

    def __init__(self, redis_client: RedisClient, settings: Settings) -> None:
        self.redis = redis_client
        self.settings = settings

    async def get_dlq_messages(
        self, start: int = 0, end: int = -1
    ) -> list[FailedMessage]:
        """Get messages from the DLQ. Returns parsed FailedMessage objects.

        Any message that fails to parse is skipped (logged but not raised).
        """
        raw_messages = await self.redis.get_dlq_messages(start, end)
        result: list[FailedMessage] = []
        for raw in raw_messages:
            try:
                result.append(FailedMessage.from_json(raw))
            except Exception:
                logger.warning("Skipping unparseable DLQ message: %s", raw[:120])
        return result

    async def get_dlq_count(self) -> int:
        """Return the current DLQ size."""
        return await self.redis.get_queue_length(self.settings.dlq_queue)

    async def analyze_dlq(self) -> dict:
        """Analyze DLQ contents and return breakdown.

        Returns a dict with total count, failure-type breakdown, source
        breakdown, average retry count, and oldest/newest failure timestamps.
        """
        messages = await self.get_dlq_messages()

        if not messages:
            return {
                "total": 0,
                "by_failure_type": {},
                "by_source": {},
                "avg_retry_count": 0.0,
                "oldest_failure": None,
                "newest_failure": None,
            }

        failure_counter: Counter[str] = Counter()
        source_counter: Counter[str] = Counter()
        total_retries = 0
        timestamps: list[str] = []

        for msg in messages:
            failure_counter[msg.failure_type.value] += 1
            source_counter[msg.original_message.source] += 1
            total_retries += msg.retry_count
            timestamps.append(msg.first_failure)

        timestamps.sort()

        return {
            "total": len(messages),
            "by_failure_type": dict(failure_counter),
            "by_source": dict(source_counter),
            "avg_retry_count": total_retries / len(messages),
            "oldest_failure": timestamps[0],
            "newest_failure": timestamps[-1],
        }

    async def reprocess_all(self) -> int:
        """Move ALL DLQ messages back to the main processing queue.

        Pops from DLQ one-by-one (RPOP), parses as FailedMessage, extracts
        original_message.to_json(), and enqueues to main_queue.
        Returns number of messages reprocessed.
        """
        count = 0
        while True:
            raw = await self.redis._redis.rpop(self.settings.dlq_queue)
            if raw is None:
                break
            try:
                failed = FailedMessage.from_json(raw)
                await self.redis.enqueue(
                    self.settings.main_queue,
                    failed.original_message.to_json(),
                )
                count += 1
            except Exception:
                logger.warning(
                    "Skipping unparseable DLQ message during reprocess: %s",
                    raw[:120],
                )
        return count

    async def reprocess_by_type(self, failure_type: FailureType) -> int:
        """Move only DLQ messages matching the given failure type back to main queue.

        Implementation:
        1. Get all DLQ messages (raw strings)
        2. Delete the DLQ key
        3. Re-push non-matching messages back to DLQ (preserving order)
        4. Enqueue matching originals to main queue

        Returns count of messages reprocessed.
        """
        raw_messages = await self.redis.get_dlq_messages()
        if not raw_messages:
            return 0

        matching: list[FailedMessage] = []
        non_matching_raw: list[str] = []

        for raw in raw_messages:
            try:
                msg = FailedMessage.from_json(raw)
                if msg.failure_type == failure_type:
                    matching.append(msg)
                else:
                    non_matching_raw.append(raw)
            except Exception:
                # Unparseable messages are kept in the DLQ
                non_matching_raw.append(raw)

        if not matching:
            return 0

        # Clear the DLQ
        await self.redis._redis.delete(self.settings.dlq_queue)

        # Re-push non-matching messages back to DLQ preserving original order.
        # get_dlq_messages returns LRANGE order (newest first since LPUSH adds
        # to the head). To restore the same order we push them back in reverse
        # so the first item in the list ends up at the head again.
        for raw in reversed(non_matching_raw):
            await self.redis.move_to_dlq(raw)

        # Enqueue matching originals to the main processing queue
        for msg in matching:
            await self.redis.enqueue(
                self.settings.main_queue,
                msg.original_message.to_json(),
            )

        return len(matching)

    async def purge(self) -> int:
        """Delete all messages from the DLQ. Returns count purged."""
        return await self.redis.clear_dlq()

    async def detect_poison_messages(
        self, threshold: int = 3
    ) -> list[FailedMessage]:
        """Find messages that have been retried more than `threshold` times.

        These are likely poison messages that will never succeed.
        """
        messages = await self.get_dlq_messages()
        return [m for m in messages if m.retry_count >= threshold]
