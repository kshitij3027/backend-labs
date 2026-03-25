"""Tests for the multi-format timestamp parser."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.timestamp_parser import TimestampParser


class TestTimestampParser:
    """Verify all supported timestamp formats."""

    def test_iso8601_with_z(self, timestamp_parser: TimestampParser) -> None:
        dt = timestamp_parser.parse("2026-03-24T10:15:30Z")
        assert dt == datetime(2026, 3, 24, 10, 15, 30, tzinfo=timezone.utc)

    def test_iso8601_with_offset(self, timestamp_parser: TimestampParser) -> None:
        dt = timestamp_parser.parse("2026-03-24T15:45:30+05:30")
        assert dt.tzinfo is not None
        utc_dt = dt.astimezone(timezone.utc)
        assert utc_dt == datetime(2026, 3, 24, 10, 15, 30, tzinfo=timezone.utc)

    def test_iso8601_no_timezone(self, timestamp_parser: TimestampParser) -> None:
        dt = timestamp_parser.parse("2026-03-24T10:15:30")
        assert dt.tzinfo is not None
        assert dt == datetime(2026, 3, 24, 10, 15, 30, tzinfo=timezone.utc)

    def test_iso8601_with_milliseconds(self, timestamp_parser: TimestampParser) -> None:
        dt = timestamp_parser.parse("2026-03-24T10:15:30.123Z")
        assert dt.year == 2026
        assert dt.microsecond == 123000
        assert dt.tzinfo is not None

    def test_unix_epoch_seconds_int(self, timestamp_parser: TimestampParser) -> None:
        # 2026-03-24T10:15:30 UTC
        ts = 1774213530
        dt = timestamp_parser.parse(ts)
        assert dt.tzinfo is not None
        assert dt.year >= 2026

    def test_unix_epoch_seconds_float(self, timestamp_parser: TimestampParser) -> None:
        ts = 1774213530.123
        dt = timestamp_parser.parse(ts)
        assert dt.tzinfo is not None

    def test_unix_epoch_milliseconds(self, timestamp_parser: TimestampParser) -> None:
        ts_ms = 1774213530000
        dt = timestamp_parser.parse(ts_ms)
        assert dt.tzinfo is not None
        assert dt.year >= 2026

    def test_apache_format(self, timestamp_parser: TimestampParser) -> None:
        dt = timestamp_parser.parse("24/Mar/2026:10:15:30 +0000")
        assert dt == datetime(2026, 3, 24, 10, 15, 30, tzinfo=timezone.utc)

    def test_syslog_format(self, timestamp_parser: TimestampParser) -> None:
        dt = timestamp_parser.parse("Mar 24 10:15:30")
        assert dt.month == 3
        assert dt.day == 24
        assert dt.hour == 10
        assert dt.minute == 15
        assert dt.second == 30
        assert dt.tzinfo is not None

    def test_invalid_string_raises(self, timestamp_parser: TimestampParser) -> None:
        with pytest.raises(ValueError):
            timestamp_parser.parse("not-a-timestamp")

    def test_empty_string_raises(self, timestamp_parser: TimestampParser) -> None:
        with pytest.raises(ValueError):
            timestamp_parser.parse("")

    def test_all_results_utc_aware(self, timestamp_parser: TimestampParser) -> None:
        samples = [
            "2026-03-24T10:15:30Z",
            "2026-03-24T10:15:30+05:30",
            "2026-03-24T10:15:30",
            1774213530,
            1774213530000,
            "24/Mar/2026:10:15:30 +0000",
            "Mar 24 10:15:30",
        ]
        for sample in samples:
            dt = timestamp_parser.parse(sample)
            assert dt.tzinfo is not None, f"No tzinfo for {sample!r}"
