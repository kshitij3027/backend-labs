"""Thread-safe acknowledgment tracker for message lifecycle management."""

import threading
from datetime import datetime, timezone
from typing import Optional

from src.logging_config import get_logger
from src.models import AckRecord, DashboardStats, MessageState

logger = get_logger(__name__)


class AckTracker:
    """Tracks acknowledgment state for all in-flight messages.

    All operations are protected by a threading lock to ensure
    safe concurrent access from multiple consumer threads.
    """

    def __init__(self) -> None:
        self._records: dict[str, AckRecord] = {}
        self._lock = threading.Lock()

    def start_tracking(self, msg_id: str, delivery_tag: int) -> AckRecord:
        """Create a new PENDING record for a message."""
        record = AckRecord(msg_id=msg_id, delivery_tag=delivery_tag)
        with self._lock:
            self._records[msg_id] = record
        logger.info(
            "tracking_started",
            msg_id=msg_id,
            delivery_tag=delivery_tag,
            state=record.state.value,
        )
        return record

    def _transition(
        self,
        msg_id: str,
        new_state: MessageState,
        error: Optional[str] = None,
        increment_retry: bool = False,
    ) -> Optional[AckRecord]:
        """Apply a state transition to an existing record.

        Returns the updated record, or None if msg_id is not found.
        """
        with self._lock:
            record = self._records.get(msg_id)
            if record is None:
                logger.warning("record_not_found", msg_id=msg_id)
                return None

            old_state = record.state
            record.state = new_state
            record.updated_at = datetime.now(timezone.utc)
            if error is not None:
                record.error = error
            if increment_retry:
                record.retry_count += 1

        logger.info(
            "state_transition",
            msg_id=msg_id,
            from_state=old_state.value,
            to_state=new_state.value,
            retry_count=record.retry_count,
        )
        return record

    def mark_processing(self, msg_id: str) -> Optional[AckRecord]:
        """Set a record's state to PROCESSING."""
        return self._transition(msg_id, MessageState.PROCESSING)

    def mark_acknowledged(self, msg_id: str) -> Optional[AckRecord]:
        """Set a record's state to ACKNOWLEDGED."""
        return self._transition(msg_id, MessageState.ACKNOWLEDGED)

    def mark_failed(self, msg_id: str, error: str) -> Optional[AckRecord]:
        """Set a record's state to FAILED."""
        return self._transition(msg_id, MessageState.FAILED, error=error)

    def mark_retrying(self, msg_id: str) -> Optional[AckRecord]:
        """Set a record's state to RETRYING and increment retry_count."""
        return self._transition(
            msg_id, MessageState.RETRYING, increment_retry=True
        )

    def mark_dead_lettered(self, msg_id: str) -> Optional[AckRecord]:
        """Set a record's state to DEAD_LETTERED."""
        return self._transition(msg_id, MessageState.DEAD_LETTERED)

    def get_record(self, msg_id: str) -> Optional[AckRecord]:
        """Return the record for a given msg_id, or None."""
        with self._lock:
            return self._records.get(msg_id)

    def get_timed_out(self, timeout_sec: float) -> list[AckRecord]:
        """Return records stuck in PROCESSING longer than timeout_sec."""
        now = datetime.now(timezone.utc)
        timed_out: list[AckRecord] = []
        with self._lock:
            for record in self._records.values():
                if record.state == MessageState.PROCESSING:
                    elapsed = (now - record.updated_at).total_seconds()
                    if elapsed > timeout_sec:
                        timed_out.append(record)
        return timed_out

    def get_stats(self) -> DashboardStats:
        """Aggregate counts across all tracked records."""
        with self._lock:
            records = list(self._records.values())

        total_received = len(records)
        total_acked = sum(
            1 for r in records if r.state == MessageState.ACKNOWLEDGED
        )
        total_failed = sum(
            1 for r in records if r.state == MessageState.FAILED
        )
        total_retried = sum(
            1 for r in records if r.state == MessageState.RETRYING
        )
        total_dead_lettered = sum(
            1 for r in records if r.state == MessageState.DEAD_LETTERED
        )
        pending_count = sum(
            1 for r in records if r.state == MessageState.PENDING
        )
        processing_count = sum(
            1 for r in records if r.state == MessageState.PROCESSING
        )

        success_rate = (total_acked / total_received * 100.0) if total_received > 0 else 0.0

        return DashboardStats(
            total_received=total_received,
            total_acked=total_acked,
            total_failed=total_failed,
            total_retried=total_retried,
            total_dead_lettered=total_dead_lettered,
            pending_count=pending_count,
            processing_count=processing_count,
            success_rate=success_rate,
        )
