"""Tests for the FeatureExtractor class."""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pytest

from src.pipeline.feature_extractor import FeatureExtractor


@pytest.fixture
def extractor():
    return FeatureExtractor()


class TestFeatureExtractor:
    """Unit tests for FeatureExtractor.extract."""

    def test_extract_returns_correct_shape(self, extractor, make_log_entry):
        entry = make_log_entry()
        result = extractor.extract(entry)
        assert result.shape == (9,)
        assert result.dtype == np.float64

    def test_extract_values_are_numeric(self, extractor, make_log_entry):
        entry = make_log_entry()
        result = extractor.extract(entry)
        assert np.all(np.isfinite(result))

    def test_extract_hour_weekday_minute(self, extractor, make_log_entry):
        ts = datetime(2025, 6, 18, 14, 37, 0, tzinfo=timezone.utc)  # Wednesday
        entry = make_log_entry(timestamp=ts)
        result = extractor.extract(entry)

        assert result[0] == 14.0   # hour
        assert result[1] == 2.0    # weekday (Wednesday = 2)
        assert result[2] == 37.0   # minute

    def test_extract_performance_features(self, extractor, make_log_entry):
        entry = make_log_entry(
            response_time=250.5,
            status_code=404,
            bytes_sent=1234,
        )
        result = extractor.extract(entry)

        assert result[3] == 250.5   # response_time
        assert result[4] == 404.0   # status_code
        assert result[5] == 1234.0  # bytes_sent

    def test_extract_behavioral_features(self, extractor, make_log_entry):
        entry = make_log_entry(
            session_duration=600.0,
            page_views=12,
        )
        result = extractor.extract(entry)

        assert result[6] == 600.0  # session_duration
        assert result[7] == 12.0   # page_views

    def test_extract_user_agent_length(self, extractor, make_log_entry):
        ua = "TestAgent/1.0"
        entry = make_log_entry(user_agent=ua)
        result = extractor.extract(entry)

        assert result[8] == float(len(ua))

    def test_feature_names_constant(self, extractor):
        assert len(FeatureExtractor.FEATURE_NAMES) == 9
        assert FeatureExtractor.NUM_FEATURES == 9
        assert len(FeatureExtractor.FEATURE_NAMES) == FeatureExtractor.NUM_FEATURES
