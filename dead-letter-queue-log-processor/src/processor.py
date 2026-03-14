"""Message processor for the Dead Letter Queue Log Processor."""

import asyncio
import hashlib
import time
from datetime import datetime, timezone

from src.classifier import FailureClassifier
from src.config import Settings
from src.models import FailedMessage, FailureType, LogMessage
from src.redis_client import RedisClient


class MessageProcessor:
    """Consumes messages from the main queue, processes them, handles failures."""

    def __init__(
        self,
        redis_client: RedisClient,
        settings: Settings,
        classifier: FailureClassifier,
        stats_tracker=None,
    ) -> None:
        self.redis = redis_client
        self.settings = settings
        self.classifier = classifier
        self.stats = stats_tracker
        self._retry_counts: dict[str, int] = {}  # key -> retry_count
        self._failure_types: dict[str, FailureType] = {}  # key -> first failure type

    @staticmethod
    def _message_key(raw: str) -> str:
        """Derive a stable key for tracking retries.

        If the raw string is valid JSON with an 'id' field, use that id.
        Otherwise, use a SHA-256 hash of the raw content.
        """
        try:
            import json

            obj = json.loads(raw)
            if "id" in obj:
                return obj["id"]
        except Exception:
            pass
        return hashlib.sha256(raw.encode()).hexdigest()

    def _process_message(self, raw: str) -> LogMessage:
        """Attempt to parse and process a raw message string.

        Steps:
        1. Parse JSON via LogMessage.from_json(raw)
        2. Basic validation: source must not be empty, message must not be empty
        3. Simulate network errors: if 'timeout' in message.message.lower()
        4. Simulate resource errors: if len(raw) > 50000
        5. Return the parsed LogMessage on success
        """
        # Step 4 checked first so oversized messages fail before parsing
        if len(raw) > 50000:
            raise MemoryError(f"Message too large: {len(raw)} bytes")

        # Step 1: parse
        msg = LogMessage.from_json(raw)

        # Step 2: validate
        if not msg.source:
            raise ValueError("Empty source field")
        if not msg.message:
            raise ValueError("Empty message field")

        # Step 3: simulate network error
        if "timeout" in msg.message.lower():
            raise ConnectionError("Simulated network timeout")

        return msg

    def _compute_backoff(self, retry_count: int) -> float:
        """Compute backoff delay: base * 2^retry_count."""
        return self.settings.backoff_base * (2**retry_count)

    async def _handle_failure(self, raw: str, error: Exception) -> None:
        """Handle a processing failure.

        1. Classify the error
        2. Get max retries for that type
        3. Look up current retry count for this message
        4. If retry_count < max_retries: increment count, schedule retry with backoff
        5. Else: create FailedMessage, move to DLQ
        6. Update stats if stats_tracker is provided
        """
        failure_type = self.classifier.classify(error)
        max_retries = self.classifier.get_max_retries(failure_type)

        key = self._message_key(raw)

        # Track the first failure type seen for this message
        if key not in self._failure_types:
            self._failure_types[key] = failure_type

        current_retries = self._retry_counts.get(key, 0)

        if current_retries < max_retries:
            # Schedule a retry
            self._retry_counts[key] = current_retries + 1
            backoff = self._compute_backoff(current_retries)
            await self.redis.schedule_retry(raw, time.time() + backoff)
            if self.stats:
                await self.stats.increment("retries")
        else:
            # Exhausted retries — send to DLQ
            original = self._build_original_message(raw)
            now_iso = datetime.now(timezone.utc).isoformat()
            failed = FailedMessage(
                original_message=original,
                failure_type=self._failure_types.get(key, failure_type),
                error_details=str(error),
                retry_count=current_retries,
                max_retries=max_retries,
                first_failure=now_iso,
                last_failure=now_iso,
            )
            await self.redis.move_to_dlq(failed.to_json())
            # Clean up tracking dicts
            self._retry_counts.pop(key, None)
            self._failure_types.pop(key, None)
            if self.stats:
                await self.stats.increment("dead_lettered")

    @staticmethod
    def _build_original_message(raw: str) -> LogMessage:
        """Try to parse raw into a LogMessage; return a stub on failure."""
        try:
            return LogMessage.from_json(raw)
        except Exception:
            return LogMessage(
                source="unknown",
                message=raw[:500] if raw else "(empty)",
            )

    async def process_one(self, raw: str) -> None:
        """Process a single raw message. Called by the main loop."""
        try:
            self._process_message(raw)
            await self.redis.store_processed(raw)
            if self.stats:
                await self.stats.increment("processed")
        except Exception as e:
            await self._handle_failure(raw, e)

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main processing loop: BRPOP from main queue, process each message.

        Runs until stop_event is set.
        """
        while not stop_event.is_set():
            result = await self.redis.dequeue(
                self.settings.main_queue, timeout=1.0
            )
            if result:
                await self.process_one(result)
