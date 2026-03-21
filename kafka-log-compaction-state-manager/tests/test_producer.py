"""Unit tests for ProfileProducer (no real Kafka broker required)."""

import unittest
from unittest.mock import MagicMock, patch

from src.config import Settings
from src.models import StateUpdate, UpdateType, UserProfile
from src.producer import ProfileProducer


class TestProfileProducerInit(unittest.TestCase):
    """Verify constructor wiring."""

    @patch("src.producer.Producer")
    def test_producer_init(self, mock_producer_cls):
        config = Settings()
        producer = ProfileProducer(config)

        mock_producer_cls.assert_called_once()
        call_kwargs = mock_producer_cls.call_args[0][0]
        self.assertEqual(call_kwargs["bootstrap.servers"], config.active_bootstrap_servers)
        self.assertTrue(call_kwargs["enable.idempotence"])
        self.assertEqual(call_kwargs["acks"], "all")

        self.assertIsInstance(producer.profiles, dict)
        self.assertEqual(len(producer.profiles), 0)
        self.assertIsInstance(producer.deleted_ids, set)
        self.assertEqual(producer.stats["total_produced"], 0)


class TestGenerateProfile(unittest.TestCase):
    """Verify profile generation fills all required fields."""

    @patch("src.producer.Producer")
    def test_generate_profile(self, mock_producer_cls):
        producer = ProfileProducer(Settings())
        profile = producer._generate_profile("user_abc123")

        self.assertEqual(profile.user_id, "user_abc123")
        self.assertIsInstance(profile.email, str)
        self.assertIn("@", profile.email)
        self.assertIsInstance(profile.first_name, str)
        self.assertIsInstance(profile.last_name, str)
        self.assertGreaterEqual(profile.age, 18)
        self.assertLessEqual(profile.age, 80)
        self.assertEqual(profile.version, 1)
        self.assertIsInstance(profile.last_updated, str)


class TestCreateUser(unittest.TestCase):
    """Verify _create_user produces a CREATE StateUpdate."""

    @patch("src.producer.Producer")
    def test_create_user(self, mock_producer_cls):
        producer = ProfileProducer(Settings())
        event = producer._create_user()

        self.assertEqual(event.update_type, UpdateType.CREATE)
        self.assertIsNotNone(event.profile)
        self.assertTrue(event.user_id.startswith("user_"))
        # Profile should be tracked internally
        self.assertIn(event.user_id, producer.profiles)


class TestUpdateUser(unittest.TestCase):
    """Verify _update_user increments version and returns UPDATE type."""

    @patch("src.producer.Producer")
    def test_update_user(self, mock_producer_cls):
        producer = ProfileProducer(Settings())

        # Seed one user first
        create_event = producer._create_user()
        original_version = create_event.profile.version

        update_event = producer._update_user()

        self.assertIsNotNone(update_event)
        self.assertEqual(update_event.update_type, UpdateType.UPDATE)
        self.assertEqual(update_event.profile.version, original_version + 1)

    @patch("src.producer.Producer")
    def test_update_user_no_profiles(self, mock_producer_cls):
        producer = ProfileProducer(Settings())
        self.assertIsNone(producer._update_user())


class TestDeleteUser(unittest.TestCase):
    """Verify _delete_user removes the profile and returns DELETE type."""

    @patch("src.producer.Producer")
    def test_delete_user(self, mock_producer_cls):
        producer = ProfileProducer(Settings())

        create_event = producer._create_user()
        user_id = create_event.user_id

        delete_event = producer._delete_user()

        self.assertIsNotNone(delete_event)
        self.assertEqual(delete_event.update_type, UpdateType.DELETE)
        self.assertIsNone(delete_event.profile)
        self.assertNotIn(user_id, producer.profiles)
        self.assertIn(user_id, producer.deleted_ids)

    @patch("src.producer.Producer")
    def test_delete_user_no_profiles(self, mock_producer_cls):
        producer = ProfileProducer(Settings())
        self.assertIsNone(producer._delete_user())


class TestProduceEvent(unittest.TestCase):
    """Verify produce_event sends correct key/value to Kafka."""

    @patch("src.producer.Producer")
    def test_produce_event_tombstone(self, mock_producer_cls):
        """DELETE events must produce value=None (tombstone)."""
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        producer = ProfileProducer(Settings())

        event = StateUpdate(
            user_id="user_dead",
            update_type=UpdateType.DELETE,
            profile=None,
        )
        producer.produce_event(event)

        mock_instance.produce.assert_called_once()
        call_kwargs = mock_instance.produce.call_args[1]
        self.assertEqual(call_kwargs["key"], b"profile:user_dead")
        self.assertIsNone(call_kwargs["value"])
        self.assertEqual(producer.stats["deleted"], 1)
        self.assertEqual(producer.stats["total_produced"], 1)

    @patch("src.producer.Producer")
    def test_produce_event_normal(self, mock_producer_cls):
        """CREATE/UPDATE events must produce non-None value bytes."""
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        producer = ProfileProducer(Settings())
        create_event = producer._create_user()
        producer.produce_event(create_event)

        mock_instance.produce.assert_called_once()
        call_kwargs = mock_instance.produce.call_args[1]
        self.assertIsNotNone(call_kwargs["value"])
        self.assertIsInstance(call_kwargs["value"], bytes)
        self.assertEqual(call_kwargs["key"], f"profile:{create_event.user_id}".encode("utf-8"))
        self.assertEqual(producer.stats["created"], 1)
        self.assertEqual(producer.stats["total_produced"], 1)


if __name__ == "__main__":
    unittest.main()
