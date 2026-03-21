"""Tests for Pydantic data models."""

from src.models import StateUpdate, UpdateType, UserProfile


def test_user_profile_serialization_roundtrip(sample_profile: UserProfile):
    """UserProfile should survive a serialize/deserialize roundtrip."""
    raw = sample_profile.to_kafka_value()
    restored = UserProfile.from_kafka_value(raw)
    assert restored == sample_profile


def test_state_update_serialization_roundtrip(sample_state_update: StateUpdate):
    """StateUpdate should survive a serialize/deserialize roundtrip."""
    raw = sample_state_update.to_kafka_value()
    restored = StateUpdate.from_kafka_value(raw)
    assert restored == sample_state_update


def test_tombstone_state_update():
    """A DELETE StateUpdate with profile=None should serialize correctly."""
    update = StateUpdate(
        user_id="user-999",
        update_type=UpdateType.DELETE,
        profile=None,
    )
    raw = update.to_kafka_value()
    restored = StateUpdate.from_kafka_value(raw)
    assert restored.update_type == UpdateType.DELETE
    assert restored.profile is None
    assert restored.user_id == "user-999"


def test_update_type_values():
    """UpdateType enum should contain CREATE, UPDATE, and DELETE."""
    assert UpdateType.CREATE == "CREATE"
    assert UpdateType.UPDATE == "UPDATE"
    assert UpdateType.DELETE == "DELETE"
    assert len(UpdateType) == 3
