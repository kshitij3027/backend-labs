"""Core consumer loop — XREADGROUP, process, XACK with ConsumerManager orchestration."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from src.config import Config
from src.metrics import MetricsAggregator
from src.models import ConsumerStats
from src.processor import LogProcessor

logger = logging.getLogger(__name__)


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
    ) -> None:
        self.consumer_id = consumer_id
        self.redis = redis
        self.config = config
        self.processor = processor
        self.metrics = metrics
        self._running = False
        self.processed_count = 0
        self.error_count = 0
        self.last_active: datetime | None = None
        self._stop_event = asyncio.Event()

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
                logger.error("Consumer %s error: %s", self.consumer_id, e)
                await asyncio.sleep(1)  # Back off on errors

    async def _process_message(self, msg_id: str, msg_data: dict) -> None:
        """Process a single message and ACK it."""
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
        except Exception as e:
            self.error_count += 1
            logger.error("Error processing %s: %s", msg_id, e)

    async def _recover_pending(self) -> None:
        """Read pending messages (ID '0') on startup."""
        try:
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
        except Exception as e:
            logger.error("Pending recovery error: %s", e)


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
                consumer_id, redis, self.config, self.processor, self.metrics
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
