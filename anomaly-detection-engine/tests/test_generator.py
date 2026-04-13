"""Tests for the synthetic log generator."""
from __future__ import annotations

import pytest

from src.generator.log_generator import AnomalyType, LogGenerator
from src.models import LogEntry


class TestLogGenerator:
    """Test suite for LogGenerator."""

    def test_generate_returns_log_entry(self) -> None:
        """generate() returns a LogEntry instance with all fields populated."""
        gen = LogGenerator(seed=42)
        entry = gen.generate()

        assert isinstance(entry, LogEntry)
        assert entry.timestamp is not None
        assert isinstance(entry.ip, str) and len(entry.ip) > 0
        assert entry.method in ("GET", "POST", "PUT", "DELETE")
        assert isinstance(entry.path, str) and entry.path.startswith("/")
        assert 100 <= entry.status_code <= 599
        assert entry.response_time >= 0
        assert entry.bytes_sent >= 0
        assert isinstance(entry.user_agent, str)
        assert entry.session_duration > 0
        assert entry.page_views >= 1

    def test_generate_batch_correct_count(self) -> None:
        """generate_batch(50) returns exactly 50 entries."""
        gen = LogGenerator(seed=99)
        batch = gen.generate_batch(50)

        assert len(batch) == 50
        assert all(isinstance(e, LogEntry) for e in batch)

    def test_anomaly_rate_approximate(self) -> None:
        """With default 5% rate over 2000 entries, anomalies fall between 2% and 10%."""
        gen = LogGenerator(anomaly_rate=0.05, seed=123)
        entries = gen.generate_batch(2000)
        anomaly_count = sum(1 for e in entries if e._is_anomaly)
        rate = anomaly_count / len(entries)

        assert 0.02 <= rate <= 0.10, f"Anomaly rate {rate:.3f} outside [0.02, 0.10]"

    def test_all_anomaly_types_appear(self) -> None:
        """All 4 AnomalyType values appear in a large enough sample."""
        gen = LogGenerator(anomaly_rate=0.05, seed=77)
        entries = gen.generate_batch(2000)
        anomalous = [e for e in entries if e._is_anomaly]
        observed_types = {e._anomaly_type for e in anomalous}

        expected = {at.value for at in AnomalyType}
        assert expected == observed_types, (
            f"Missing anomaly types: {expected - observed_types}"
        )

    def test_slow_response_anomaly(self) -> None:
        """SLOW_RESPONSE anomalies have response_time > 2000."""
        gen = LogGenerator(anomaly_rate=0.5, seed=10)
        for entry in gen.generate_stream(500):
            if entry._is_anomaly and entry._anomaly_type == AnomalyType.SLOW_RESPONSE.value:
                assert entry.response_time > 2000
                return
        pytest.fail("No SLOW_RESPONSE anomaly found in 500 entries at 50% rate")

    def test_unusual_payload_anomaly(self) -> None:
        """UNUSUAL_PAYLOAD anomalies have bytes_sent > 40000."""
        gen = LogGenerator(anomaly_rate=0.5, seed=20)
        for entry in gen.generate_stream(500):
            if entry._is_anomaly and entry._anomaly_type == AnomalyType.UNUSUAL_PAYLOAD.value:
                assert entry.bytes_sent > 40000
                return
        pytest.fail("No UNUSUAL_PAYLOAD anomaly found in 500 entries at 50% rate")

    def test_suspicious_agent_anomaly(self) -> None:
        """SUSPICIOUS_AGENT anomalies have a user_agent not in normal agents."""
        gen = LogGenerator(anomaly_rate=0.5, seed=30)
        normal_agents = set(LogGenerator.NORMAL_USER_AGENTS)
        for entry in gen.generate_stream(500):
            if entry._is_anomaly and entry._anomaly_type == AnomalyType.SUSPICIOUS_AGENT.value:
                assert entry.user_agent not in normal_agents
                return
        pytest.fail("No SUSPICIOUS_AGENT anomaly found in 500 entries at 50% rate")

    def test_bad_status_anomaly(self) -> None:
        """BAD_STATUS anomalies have status_code >= 429."""
        gen = LogGenerator(anomaly_rate=0.5, seed=40)
        for entry in gen.generate_stream(500):
            if entry._is_anomaly and entry._anomaly_type == AnomalyType.BAD_STATUS.value:
                assert entry.status_code >= 429
                return
        pytest.fail("No BAD_STATUS anomaly found in 500 entries at 50% rate")

    def test_normal_entries_not_tagged(self) -> None:
        """Normal entries have _is_anomaly=False and empty _anomaly_type."""
        gen = LogGenerator(anomaly_rate=0.05, seed=55)
        entries = gen.generate_batch(200)
        normal = [e for e in entries if not e._is_anomaly]

        assert len(normal) > 0, "Expected at least one normal entry"
        for entry in normal:
            assert entry._is_anomaly is False
            assert entry._anomaly_type == ""

    def test_seed_reproducibility(self) -> None:
        """Two generators with the same seed produce identical output."""
        gen_a = LogGenerator(anomaly_rate=0.1, seed=42)
        gen_b = LogGenerator(anomaly_rate=0.1, seed=42)

        batch_a = gen_a.generate_batch(100)
        batch_b = gen_b.generate_batch(100)

        for a, b in zip(batch_a, batch_b):
            assert a.ip == b.ip
            assert a.method == b.method
            assert a.path == b.path
            assert a.status_code == b.status_code
            assert a.response_time == b.response_time
            assert a.bytes_sent == b.bytes_sent
            assert a.user_agent == b.user_agent
            assert a.session_duration == b.session_duration
            assert a.page_views == b.page_views
            assert a._is_anomaly == b._is_anomaly
            assert a._anomaly_type == b._anomaly_type

    def test_generate_stream_yields_correct_count(self) -> None:
        """generate_stream yields exactly the requested number of entries."""
        gen = LogGenerator(seed=60)
        entries = list(gen.generate_stream(75))

        assert len(entries) == 75
        assert all(isinstance(e, LogEntry) for e in entries)
