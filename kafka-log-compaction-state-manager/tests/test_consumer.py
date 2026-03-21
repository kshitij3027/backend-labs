"""Unit tests for StateConsumer."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.config import Settings
from src.models import StateUpdate, UpdateType, UserProfile


def _make_profile(user_id: str, version: int = 1, **overrides) -> UserProfile:
    """Helper to build a UserProfile with sensible defaults."""
    defaults = dict(
        user_id=user_id,
        email=f"{user_id}@test.com",
        first_name="Test",
        last_name="User",
        age=25,
        version=version,
        deleted=False,
        last_updated=datetime.now(timezone.utc).isoformat(),
    )
    defaults.update(overrides)
    return UserProfile(**defaults)


def _make_key(user_id: str) -> bytes:
    return f"profile:{user_id}".encode("utf-8")


def _make_state_update_bytes(user_id: str, update_type: UpdateType, profile=None) -> bytes:
    su = StateUpdate(user_id=user_id, update_type=update_type, profile=profile)
    return su.to_kafka_value()


@pytest.fixture
def consumer(config):
    """Return a StateConsumer with the confluent_kafka.Consumer mocked out."""
    with patch("src.consumer.Consumer") as MockConsumer:
        MockConsumer.return_value = MagicMock()
        from src.consumer import StateConsumer

        sc = StateConsumer(config)
        yield sc


class TestProcessMessageCreate:
    def test_profile_added_to_state(self, consumer):
        profile = _make_profile("user-001")
        value = _make_state_update_bytes("user-001", UpdateType.CREATE, profile)

        consumer._process_message(_make_key("user-001"), value)

        assert "user-001" in consumer.state
        assert consumer.state["user-001"].email == "user-001@test.com"
        assert consumer.total_messages_consumed == 1
        assert consumer.updates_by_type["CREATE"] == 1


class TestProcessMessageUpdate:
    def test_profile_updated_in_state(self, consumer):
        # First, create
        profile_v1 = _make_profile("user-002", version=1)
        consumer._process_message(
            _make_key("user-002"),
            _make_state_update_bytes("user-002", UpdateType.CREATE, profile_v1),
        )

        # Then, update with version 2
        profile_v2 = _make_profile("user-002", version=2, email="updated@test.com")
        consumer._process_message(
            _make_key("user-002"),
            _make_state_update_bytes("user-002", UpdateType.UPDATE, profile_v2),
        )

        assert consumer.state["user-002"].version == 2
        assert consumer.state["user-002"].email == "updated@test.com"
        assert consumer.total_messages_consumed == 2
        assert consumer.updates_by_type["UPDATE"] == 1


class TestProcessMessageTombstone:
    def test_profile_removed_on_tombstone(self, consumer):
        # Create first
        profile = _make_profile("user-003")
        consumer._process_message(
            _make_key("user-003"),
            _make_state_update_bytes("user-003", UpdateType.CREATE, profile),
        )
        assert "user-003" in consumer.state

        # Tombstone (value=None)
        consumer._process_message(_make_key("user-003"), None)

        assert "user-003" not in consumer.state
        assert consumer.tombstones_processed == 1
        assert consumer.updates_by_type["DELETE"] == 1


class TestProcessMessageDeleteEvent:
    def test_profile_removed_on_delete_event(self, consumer):
        # Create first
        profile = _make_profile("user-004")
        consumer._process_message(
            _make_key("user-004"),
            _make_state_update_bytes("user-004", UpdateType.CREATE, profile),
        )
        assert "user-004" in consumer.state

        # DELETE StateUpdate (not a tombstone — has a value)
        consumer._process_message(
            _make_key("user-004"),
            _make_state_update_bytes("user-004", UpdateType.DELETE, None),
        )

        assert "user-004" not in consumer.state
        assert consumer.tombstones_processed == 1
        assert consumer.updates_by_type["DELETE"] == 1


class TestGetActiveProfiles:
    def test_returns_only_non_deleted(self, consumer):
        # Create 3 profiles
        for i in range(1, 4):
            uid = f"user-{i:03d}"
            profile = _make_profile(uid)
            consumer._process_message(
                _make_key(uid),
                _make_state_update_bytes(uid, UpdateType.CREATE, profile),
            )

        # Delete one
        consumer._process_message(_make_key("user-001"), None)

        active = consumer.get_active_profiles()
        assert len(active) == 2
        assert "user-001" not in active
        assert "user-002" in active
        assert "user-003" in active


class TestGetStats:
    def test_stats_counts_are_correct(self, consumer):
        # Create 2, update 1, delete 1
        p1 = _make_profile("user-010")
        p2 = _make_profile("user-011")
        consumer._process_message(
            _make_key("user-010"),
            _make_state_update_bytes("user-010", UpdateType.CREATE, p1),
        )
        consumer._process_message(
            _make_key("user-011"),
            _make_state_update_bytes("user-011", UpdateType.CREATE, p2),
        )

        p1_v2 = _make_profile("user-010", version=2, email="new@test.com")
        consumer._process_message(
            _make_key("user-010"),
            _make_state_update_bytes("user-010", UpdateType.UPDATE, p1_v2),
        )

        consumer._process_message(_make_key("user-011"), None)

        stats = consumer.get_stats()
        assert stats["total_consumed"] == 4
        assert stats["active_profiles"] == 1
        assert stats["total_in_state"] == 1
        assert stats["tombstones_processed"] == 1
        assert stats["updates_by_type"]["CREATE"] == 2
        assert stats["updates_by_type"]["UPDATE"] == 1
        assert stats["updates_by_type"]["DELETE"] == 1
