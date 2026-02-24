"""Integration tests for LogNormalizer."""
import pytest
from src.normalizer import LogNormalizer
from src.models import LogEntry, LogLevel, UnsupportedFormatError
import src.handlers  # noqa: F401


class TestLogNormalizer:
    """Integration tests for the normalizer pipeline."""

    @pytest.fixture
    def normalizer(self):
        return LogNormalizer()

    # Auto-detection tests
    def test_auto_detect_json(self, normalizer, sample_json_bytes):
        entry = normalizer.normalize(sample_json_bytes)
        assert isinstance(entry, LogEntry)
        assert entry.source_format == "json"
        assert entry.message == "Application started successfully"

    def test_auto_detect_syslog_rfc5424(self, normalizer, sample_syslog_rfc5424_bytes):
        entry = normalizer.normalize(sample_syslog_rfc5424_bytes)
        assert isinstance(entry, LogEntry)
        assert entry.source_format == "text"

    def test_auto_detect_syslog_rfc3164(self, normalizer, sample_syslog_rfc3164_bytes):
        entry = normalizer.normalize(sample_syslog_rfc3164_bytes)
        assert isinstance(entry, LogEntry)
        assert entry.source_format == "text"

    def test_auto_detect_text(self, normalizer, sample_text_bytes):
        entry = normalizer.normalize(sample_text_bytes)
        assert isinstance(entry, LogEntry)
        assert entry.source_format == "text"

    def test_auto_detect_protobuf(self, normalizer, sample_protobuf_bytes):
        entry = normalizer.normalize(sample_protobuf_bytes)
        assert isinstance(entry, LogEntry)
        assert entry.source_format == "protobuf"

    def test_auto_detect_avro(self, normalizer, sample_avro_bytes):
        entry = normalizer.normalize(sample_avro_bytes)
        assert isinstance(entry, LogEntry)
        assert entry.source_format == "avro"

    # Explicit format hint tests
    def test_explicit_json(self, normalizer, sample_json_bytes):
        entry = normalizer.normalize(sample_json_bytes, source_format="json")
        assert entry.source_format == "json"

    def test_explicit_text(self, normalizer, sample_text_bytes):
        entry = normalizer.normalize(sample_text_bytes, source_format="text")
        assert entry.source_format == "text"

    def test_explicit_protobuf(self, normalizer, sample_protobuf_bytes):
        entry = normalizer.normalize(sample_protobuf_bytes, source_format="protobuf")
        assert entry.source_format == "protobuf"

    def test_explicit_avro(self, normalizer, sample_avro_bytes):
        entry = normalizer.normalize(sample_avro_bytes, source_format="avro")
        assert entry.source_format == "avro"

    # Error handling tests
    def test_unknown_format_raises(self, normalizer):
        with pytest.raises(UnsupportedFormatError):
            normalizer.normalize(b"\x00\x00\x00\x00")

    def test_unknown_explicit_format_raises(self, normalizer, sample_json_bytes):
        with pytest.raises(KeyError):
            normalizer.normalize(sample_json_bytes, source_format="xml")

    # Registry tests
    def test_registered_formats(self, normalizer):
        formats = normalizer.registered_formats
        assert "json" in formats
        assert "text" in formats
        assert "protobuf" in formats
        assert "avro" in formats

    # Cross-format normalization consistency
    def test_all_formats_produce_log_entry(self, normalizer, sample_json_bytes, sample_text_bytes, sample_protobuf_bytes, sample_avro_bytes):
        """All formats should produce valid LogEntry with required fields."""
        samples = [sample_json_bytes, sample_text_bytes, sample_protobuf_bytes, sample_avro_bytes]
        for sample in samples:
            entry = normalizer.normalize(sample)
            assert isinstance(entry, LogEntry)
            assert entry.timestamp is not None
            assert entry.level is not None
            assert isinstance(entry.level, LogLevel)
            assert entry.message  # non-empty
            assert entry.source_format  # non-empty

    def test_custom_handler_order(self):
        """Custom handler order should work."""
        normalizer = LogNormalizer(handler_order=["json", "text"])
        formats = normalizer.registered_formats
        # Should still have all registered formats
        assert "json" in formats
