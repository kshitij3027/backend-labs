"""Tests for format detection with registered handlers."""
import json

import pytest

import src.handlers  # noqa: F401 - triggers handler registration
from src.detector import FormatDetector
from src.handlers.json_handler import JsonHandler
from src.models import UnsupportedFormatError


class TestFormatDetection:
    """Test format detection with the JSON handler registered."""

    def test_detect_json(self):
        detector = FormatDetector()
        data = json.dumps({"message": "hello", "level": "INFO"}).encode("utf-8")
        handler = detector.detect(data)
        assert isinstance(handler, JsonHandler)
        assert handler.format_name == "json"

    def test_detect_unknown_format(self):
        detector = FormatDetector()
        data = b"\x00\x01\x02\x03random binary garbage"
        with pytest.raises(UnsupportedFormatError):
            detector.detect(data)
