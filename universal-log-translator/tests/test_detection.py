"""Tests for format detection with registered handlers."""
import json

import pytest

import src.handlers  # noqa: F401 - triggers handler registration
from src.detector import FormatDetector
from src.handlers.avro_handler import AvroHandler
from src.handlers.json_handler import JsonHandler
from src.handlers.protobuf_handler import ProtobufHandler
from src.handlers.text_handler import TextHandler
from src.models import UnsupportedFormatError


class TestFormatDetection:
    """Test format detection with the JSON and text handlers registered."""

    def test_detect_json(self):
        detector = FormatDetector()
        data = json.dumps({"message": "hello", "level": "INFO"}).encode("utf-8")
        handler = detector.detect(data)
        assert isinstance(handler, JsonHandler)
        assert handler.format_name == "json"

    def test_detect_text_syslog(self):
        detector = FormatDetector()
        data = b"<165>1 2024-01-15T10:30:00Z web-01 api-gateway 1234 - - Application started"
        handler = detector.detect(data)
        assert isinstance(handler, TextHandler)
        assert handler.format_name == "text"

    def test_detect_text_generic(self):
        detector = FormatDetector()
        data = b"2024-01-15 10:30:00 INFO Application started successfully"
        handler = detector.detect(data)
        assert isinstance(handler, TextHandler)
        assert handler.format_name == "text"

    def test_detect_json_over_text(self):
        """JSON-looking data should be detected as JSON, not text."""
        detector = FormatDetector()
        data = json.dumps({"message": "hello"}).encode("utf-8")
        handler = detector.detect(data)
        assert isinstance(handler, JsonHandler)
        assert handler.format_name == "json"

    def test_detect_protobuf(self, sample_protobuf_bytes):
        """Protobuf binary data should be detected as protobuf handler."""
        detector = FormatDetector()
        handler = detector.detect(sample_protobuf_bytes)
        assert isinstance(handler, ProtobufHandler)
        assert handler.format_name == "protobuf"

    def test_detect_avro(self, sample_avro_bytes):
        """Avro OCF bytes should be detected as avro handler."""
        detector = FormatDetector()
        handler = detector.detect(sample_avro_bytes)
        assert isinstance(handler, AvroHandler)
        assert handler.format_name == "avro"

    def test_cross_format_detection(
        self,
        sample_json_bytes,
        sample_syslog_rfc5424_bytes,
        sample_protobuf_bytes,
        sample_avro_bytes,
    ):
        """Each format should be detected by its correct handler."""
        detector = FormatDetector()

        json_handler = detector.detect(sample_json_bytes)
        assert isinstance(json_handler, JsonHandler)
        assert json_handler.format_name == "json"

        syslog_handler = detector.detect(sample_syslog_rfc5424_bytes)
        assert isinstance(syslog_handler, TextHandler)
        assert syslog_handler.format_name == "text"

        proto_handler = detector.detect(sample_protobuf_bytes)
        assert isinstance(proto_handler, ProtobufHandler)
        assert proto_handler.format_name == "protobuf"

        avro_handler = detector.detect(sample_avro_bytes)
        assert isinstance(avro_handler, AvroHandler)
        assert avro_handler.format_name == "avro"

    def test_detect_unknown_format(self):
        detector = FormatDetector()
        data = b"\x00\x01\x02\x03random binary garbage"
        with pytest.raises(UnsupportedFormatError):
            detector.detect(data)
