"""Core consumer loop — XREADGROUP, process, XACK with ConsumerManager orchestration."""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone

import structlog

from src.config import Config
from src.metrics import MetricsAggregator
from src.models import ConsumerStats
from src.processor import LogProcessor

logger = structlog.get_logger(__name__)


class LogConsumer:
    """Single consumer that reads from a Redis stream via XREADGROUP, processes
    messages through a LogProcessor, records metrics, and ACKs on success."""

    def __init__(
        self,
        consumer_id: str,
        redis,
        config: Config,
        processor: LogProcessor,
        metrics: MetricsAggregator,
        worker_index: int = 0,
    ) -> None:
        self.consumer_id = consumer_id
        self.redis = redis
        self.config = config
        self.processor = processor
        self.metrics = metrics
        self._worker_index = worker_index
        self._running = False
        self.processed_count = 0
        self.error_count = 0
        self.last_active: datetime | None = None
        self._stop_event = asyncio.Event()
        self._retry_counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Ensure group exists, recover pending, enter consume loop."""
        await self._ensure_group()
        await self._recover_pending()
        self._running = True
        await self._consume_loop()

    async def stop(self) -> None:
        """Signal stop and wait for drain."""
        self._stop_event.set()
        self._running = False

    def get_stats(self) -> ConsumerStats:
        """Return a snapshot of this consumer's statistics."""
        total = self.processed_count + self.error_count
        success_rate = self.processed_count / total if total > 0 else 1.0
        return ConsumerStats(
            consumer_id=self.consumer_id,
            processed_count=self.processed_count,
            error_count=self.error_count,
            success_rate=round(success_rate, 4),
            last_active=self.last_active,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _ensure_group(self) -> None:
        """XGROUP CREATE with MKSTREAM, catch BUSYGROUP."""
        try:
            await self.redis.xgroup_create(
                self.config.stream_key,
                self.config.consumer_group,
                id="0",
                mkstream=True,
            )
        except Exception as e:
            if "BUSYGROUP" in str(e):
                pass  # Group already exists
            else:
                raise

    async def _consume_loop(self) -> None:
        """Main loop: XREADGROUP, process, XACK."""
        while not self._stop_event.is_set():
            try:
                messages = await self.redis.xreadgroup(
                    self.config.consumer_group,
                    self.consumer_id,
                    {self.config.stream_key: ">"},
                    count=self.config.batch_size,
                    block=self.config.block_ms,
                )
                if messages:
                    for stream_name, stream_messages in messages:
                        for msg_id, msg_data in stream_messages:
                            await self._process_message(msg_id, msg_data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("consumer loop error", consumer_id=self.consumer_id, error=str(e))
                await asyncio.sleep(1)  # Back off on errors

    async def _process_message(self, msg_id: str, msg_data: dict) -> None:
        """Process a single message with idempotency, ordering, retry, and DLQ."""
        # --- Idempotency: skip if already processed ---
        is_new = await self.redis.set(
            f"processed:{msg_id}", "1", nx=True, ex=self.config.idempotency_ttl
        )
        if not is_new:
            await self.redis.xack(
                self.config.stream_key, self.config.consumer_group, msg_id
            )
            logger.info("Skipping duplicate", msg_id=msg_id, consumer_id=self.consumer_id)
            return

        # --- Ordering: only process if ordering_key hashes to this worker ---
        if self.config.enable_ordering and "ordering_key" in msg_data:
            target_worker = hash(msg_data["ordering_key"]) % self.config.num_workers
            if target_worker != self._worker_index:
                # Not our message for ordered processing — skip, don't ACK
                # Remove the idempotency key so the correct worker can process it
                await self.redis.delete(f"processed:{msg_id}")
                return

        max_retries = self.config.max_retries
        base_delay = self.config.retry_base_delay
        max_delay = self.config.retry_max_delay

        while True:
            try:
                entry = self.processor.process_message(msg_data)
                if entry:
                    await self.metrics.record(entry)
                self.processed_count += 1
                self.last_active = datetime.now(timezone.utc)
                await self.redis.xack(
                    self.config.stream_key,
                    self.config.consumer_group,
                    msg_id,
                )
                # Success — clean up any retry state
                self._retry_counts.pop(msg_id, None)
                return
            except Exception as e:
                attempt = self._retry_counts.get(msg_id, 0) + 1
                self._retry_counts[msg_id] = attempt

                if attempt < max_retries:
                    delay = min(max_delay, base_delay * (2 ** attempt) + random.uniform(0, 0.5))
                    logger.warning(
                        "retrying message",
                        consumer_id=self.consumer_id,
                        attempt=attempt,
                        max_retries=max_retries,
                        msg_id=msg_id,
                        delay=round(delay, 2),
                        error=str(e),
                    )
                    await asyncio.sleep(delay)
                    continue  # retry
                else:
                    # Exhausted retries — send to DLQ
                    self.error_count += 1
                    logger.error(
                        "max retries exceeded",
                        consumer_id=self.consumer_id,
                        max_retries=max_retries,
                        msg_id=msg_id,
                        error=str(e),
                    )
                    await self._send_to_dlq(msg_id, msg_data, e)
                    await self.redis.xack(
                        self.config.stream_key,
                        self.config.consumer_group,
                        msg_id,
                    )
                    return

    async def _send_to_dlq(self, msg_id: str, msg_data: dict, error: Exception) -> None:
        """Send a failed message to the dead letter queue stream."""
        attempt_count = self._retry_counts.pop(msg_id, 0)
        try:
            await self.redis.xadd(
                self.config.dlq_stream_key,
                {
                    "original_id": msg_id,
                    "original_data": str(msg_data),
                    "error": str(error),
                    "attempt_count": str(attempt_count),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            logger.info("sent to DLQ", consumer_id=self.consumer_id, msg_id=msg_id, attempt_count=attempt_count)
        except Exception as dlq_err:
            logger.error("failed to send to DLQ", consumer_id=self.consumer_id, msg_id=msg_id, error=str(dlq_err))

    async def _recover_pending(self) -> None:
        """Recover pending messages and claim abandoned ones."""
        try:
            # First, try to recover our own pending messages
            messages = await self.redis.xreadgroup(
                self.config.consumer_group,
                self.consumer_id,
                {self.config.stream_key: "0"},
                count=self.config.batch_size,
            )
            if messages:
                for stream_name, stream_messages in messages:
                    for msg_id, msg_data in stream_messages:
                        if msg_data:  # Skip empty pending entries
                            await self._process_message(msg_id, msg_data)

            # Then, claim messages that have been idle too long from other consumers
            await self._claim_abandoned()
        except Exception as e:
            logger.error("Pending recovery error", error=str(e), consumer_id=self.consumer_id)

    async def _claim_abandoned(self) -> None:
        """Use XPENDING + XCLAIM to take over messages idle longer than claim_idle_ms."""
        try:
            pending = await self.redis.xpending_range(
                self.config.stream_key,
                self.config.consumer_group,
                min="-",
                max="+",
                count=self.config.batch_size,
            )
            for entry in pending:
                idle_ms = entry.get("time_since_delivered", 0)
                if idle_ms > self.config.claim_idle_ms:
                    msg_id = entry["message_id"]
                    claimed = await self.redis.xclaim(
                        self.config.stream_key,
                        self.config.consumer_group,
                        self.consumer_id,
                        min_idle_time=self.config.claim_idle_ms,
                        message_ids=[msg_id],
                    )
                    for claimed_id, claimed_data in claimed:
                        if claimed_data:
                            logger.info(
                                "Claimed abandoned message",
                                msg_id=claimed_id,
                                consumer_id=self.consumer_id,
                            )
                            await self._process_message(claimed_id, claimed_data)
        except Exception as e:
            logger.warning("XCLAIM error (non-fatal)", error=str(e), consumer_id=self.consumer_id)


class ConsumerManager:
    """Orchestrates N LogConsumer instances against a shared Redis stream."""

    def __init__(
        self,
        config: Config,
        processor: LogProcessor,
        metrics: MetricsAggregator,
    ) -> None:
        self.config = config
        self.processor = processor
        self.metrics = metrics
        self.consumers: list[LogConsumer] = []
        self._tasks: list[asyncio.Task] = []

    async def start(self, redis) -> None:
        """Create N LogConsumers and start them concurrently."""
        for i in range(self.config.num_workers):
            consumer_id = f"{self.config.consumer_name}-worker-{i}"
            consumer = LogConsumer(
                consumer_id, redis, self.config, self.processor, self.metrics,
                worker_index=i,
            )
            self.consumers.append(consumer)
            task = asyncio.create_task(consumer.start())
            self._tasks.append(task)

    async def stop(self) -> None:
        """Stop all consumers."""
        for consumer in self.consumers:
            await consumer.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def get_consumer_stats(self) -> list[ConsumerStats]:
        """Return stats for every managed consumer."""
        return [c.get_stats() for c in self.consumers]
