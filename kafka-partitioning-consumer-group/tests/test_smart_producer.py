"""Tests for smart producer partitioning logic."""
import zlib
import pytest
from unittest.mock import MagicMock, patch
from src.config import Settings
from src.models import LogEntry, LogLevel
from src.producer.smart_producer import SmartProducer


class TestPartitionCalculation:
    """Test partition calculation without needing a real Kafka broker."""

    def test_key_based_deterministic(self):
        """Same user_id always maps to same partition."""
        settings = Settings(bootstrap_servers="localhost:9092")
        with patch("src.producer.smart_producer.Producer"):
            producer = SmartProducer(settings)

        entry = LogEntry(user_id="1234", level=LogLevel.INFO, service="test")
        expected = zlib.crc32("1234".encode()) % 6

        # Call multiple times — should be deterministic
        for _ in range(10):
            assert producer._calculate_partition(entry) == expected

    def test_different_users_can_map_differently(self):
        """Different user_ids should (likely) map to different partitions."""
        settings = Settings(bootstrap_servers="localhost:9092")
        with patch("src.producer.smart_producer.Producer"):
            producer = SmartProducer(settings)

        partitions = set()
        for uid in range(1000, 1100):
            entry = LogEntry(user_id=str(uid))
            partitions.add(producer._calculate_partition(entry))

        # With 100 different user_ids and 6 partitions, we should hit multiple
        assert len(partitions) > 1

    def test_round_robin_cycles(self):
        """Without a key, partitions should cycle through round-robin."""
        settings = Settings(bootstrap_servers="localhost:9092", partition_strategy="round-robin")
        with patch("src.producer.smart_producer.Producer"):
            producer = SmartProducer(settings)

        results = []
        for _ in range(12):
            entry = LogEntry(user_id="")  # No key
            results.append(producer._calculate_partition(entry))

        # Should cycle 0,1,2,3,4,5,0,1,2,3,4,5
        assert results == [0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5]

    def test_key_based_no_key_falls_to_round_robin(self):
        """Key-based strategy with empty user_id falls back to round-robin."""
        settings = Settings(bootstrap_servers="localhost:9092", partition_strategy="key-based")
        with patch("src.producer.smart_producer.Producer"):
            producer = SmartProducer(settings)

        results = []
        for _ in range(6):
            entry = LogEntry(user_id="")
            results.append(producer._calculate_partition(entry))

        assert results == [0, 1, 2, 3, 4, 5]

    def test_stats_initial_structure(self):
        """Stats should have the correct initial structure."""
        settings = Settings(bootstrap_servers="localhost:9092")
        with patch("src.producer.smart_producer.Producer"):
            producer = SmartProducer(settings)

        stats = producer.stats
        assert stats["produced"] == 0
        assert stats["errors"] == 0
        assert len(stats["per_partition"]) == 6
        assert all(v == 0 for v in stats["per_partition"].values())

    def test_partition_range(self):
        """All calculated partitions should be in valid range."""
        settings = Settings(bootstrap_servers="localhost:9092", num_partitions=6)
        with patch("src.producer.smart_producer.Producer"):
            producer = SmartProducer(settings)

        for uid in range(1000, 2000):
            entry = LogEntry(user_id=str(uid))
            p = producer._calculate_partition(entry)
            assert 0 <= p < 6, f"Partition {p} out of range for user_id {uid}"
