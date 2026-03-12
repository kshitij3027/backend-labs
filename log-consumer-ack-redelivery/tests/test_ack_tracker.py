"""Tests for the AckTracker class."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from src.ack_tracker import AckTracker
from src.models import MessageState


def test_start_tracking_creates_pending_record(ack_tracker: AckTracker) -> None:
    """Verify that start_tracking creates a record with PENDING state."""
    record = ack_tracker.start_tracking("msg-001", delivery_tag=1)

    assert record.msg_id == "msg-001"
    assert record.delivery_tag == 1
    assert record.state == MessageState.PENDING
    assert record.retry_count == 0
    assert record.error is None


def test_full_lifecycle_success(ack_tracker: AckTracker) -> None:
    """Verify the happy path: PENDING -> PROCESSING -> ACKNOWLEDGED."""
    ack_tracker.start_tracking("msg-002", delivery_tag=2)

    record = ack_tracker.mark_processing("msg-002")
    assert record is not None
    assert record.state == MessageState.PROCESSING

    record = ack_tracker.mark_acknowledged("msg-002")
    assert record is not None
    assert record.state == MessageState.ACKNOWLEDGED


def test_get_timed_out_returns_stale_messages(ack_tracker: AckTracker) -> None:
    """Verify that get_timed_out finds records stuck in PROCESSING."""
    ack_tracker.start_tracking("msg-003", delivery_tag=3)
    ack_tracker.mark_processing("msg-003")

    # Manually backdate the updated_at to simulate a stale record
    record = ack_tracker.get_record("msg-003")
    assert record is not None
    record.updated_at = datetime.now(timezone.utc) - timedelta(seconds=60)

    timed_out = ack_tracker.get_timed_out(timeout_sec=30)
    assert len(timed_out) == 1
    assert timed_out[0].msg_id == "msg-003"

    # A record that is NOT stale should not appear
    ack_tracker.start_tracking("msg-004", delivery_tag=4)
    ack_tracker.mark_processing("msg-004")
    timed_out = ack_tracker.get_timed_out(timeout_sec=30)
    assert len(timed_out) == 1  # still only msg-003


def test_thread_safety() -> None:
    """Verify concurrent start_tracking calls from 50 threads produce 50 records."""
    tracker = AckTracker()

    def track(i: int) -> None:
        tracker.start_tracking(f"msg-{i:04d}", delivery_tag=i)

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(track, range(50)))

    stats = tracker.get_stats()
    assert stats.total_received == 50
    assert stats.pending_count == 50
