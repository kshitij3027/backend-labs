"""Shared test fixtures for the kafka-log-compaction-state-manager tests."""

from datetime import datetime, timezone

import pytest

from src.config import Settings
from src.models import StateUpdate, UpdateType, UserProfile


@pytest.fixture
def config() -> Settings:
    """Return a Settings instance with all defaults."""
    return Settings()


@pytest.fixture
def sample_profile() -> UserProfile:
    """Return a sample UserProfile for testing."""
    return UserProfile(
        user_id="user-001",
        email="alice@example.com",
        first_name="Alice",
        last_name="Smith",
        age=30,
        version=1,
        deleted=False,
        last_updated=datetime.now(timezone.utc).isoformat(),
    )


@pytest.fixture
def sample_state_update(sample_profile: UserProfile) -> StateUpdate:
    """Return a sample StateUpdate with CREATE type and sample_profile."""
    return StateUpdate(
        user_id=sample_profile.user_id,
        update_type=UpdateType.CREATE,
        profile=sample_profile,
    )
