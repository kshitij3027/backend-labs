"""Tests for src.log_generator — generation, batching, error bursts."""

from src.log_generator import LogGenerator
from src.models import LogEntry, LogLevel


class TestGenerateOne:
    """Single entry generation."""

    def test_generate_one(self, log_generator: LogGenerator) -> None:
        entry = log_generator.generate_one()
        assert isinstance(entry, LogEntry)
        assert entry.message  # non-empty

    def test_generate_with_specific_level(self, log_generator: LogGenerator) -> None:
        entry = log_generator.generate_one(level=LogLevel.WARNING)
        assert entry.level == LogLevel.WARNING


class TestGenerateBatch:
    """Batch generation."""

    def test_generate_batch(self, log_generator: LogGenerator) -> None:
        batch = log_generator.generate_batch(count=20)
        assert len(batch) == 20
        assert all(isinstance(e, LogEntry) for e in batch)


class TestGenerateErrorBurst:
    """Error burst must produce only ERROR or CRITICAL entries."""

    def test_generate_error_burst(self, log_generator: LogGenerator) -> None:
        burst = log_generator.generate_error_burst(count=10)
        assert len(burst) == 10
        for entry in burst:
            assert entry.level in (LogLevel.ERROR, LogLevel.CRITICAL)
