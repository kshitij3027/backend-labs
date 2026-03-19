"""Tests for log generator."""
import pytest
from collections import Counter
from src.models import LogLevel
from src.producer.log_generator import LogGenerator
from src.config import Settings


class TestLogGenerator:
    def test_generate_one_has_required_fields(self, log_generator):
        entry = log_generator.generate_one()
        assert entry.id
        assert entry.timestamp > 0
        assert entry.level in LogLevel
        assert entry.service
        assert entry.message
        assert entry.user_id

    def test_generate_one_service_from_list(self, settings, log_generator):
        for _ in range(50):
            entry = log_generator.generate_one()
            assert entry.service in settings.services

    def test_generate_one_user_id_in_range(self, settings, log_generator):
        for _ in range(50):
            entry = log_generator.generate_one()
            uid = int(entry.user_id)
            assert settings.user_id_min <= uid <= settings.user_id_max

    def test_severity_distribution(self, log_generator):
        """Check that severity weights are approximately correct over many samples."""
        counts = Counter()
        n = 5000
        for _ in range(n):
            entry = log_generator.generate_one()
            counts[entry.level] += 1

        info_pct = counts[LogLevel.INFO] / n
        warn_pct = counts[LogLevel.WARNING] / n
        error_pct = counts[LogLevel.ERROR] / n

        # Allow +-5% tolerance
        assert 0.60 < info_pct < 0.80, f"INFO: {info_pct:.2%}"
        assert 0.12 < warn_pct < 0.28, f"WARNING: {warn_pct:.2%}"
        assert 0.04 < error_pct < 0.18, f"ERROR: {error_pct:.2%}"

    def test_generate_batch(self, log_generator):
        batch = log_generator.generate_batch(10)
        assert len(batch) == 10
        for entry in batch:
            assert entry.id
            assert entry.service
