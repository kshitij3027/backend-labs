"""Tests for transmission metrics."""

from src.metrics import TransmissionMetrics


class TestTransmissionMetrics:
    def test_initial_snapshot(self):
        m = TransmissionMetrics()
        snap = m.snapshot()
        assert snap["logs_received"] == 0
        assert snap["bytes_compressed"] == 0
        assert snap["total_connections"] == 0

    def test_record_log(self):
        m = TransmissionMetrics()
        m.record_log(100, 500)
        m.record_log(200, 1000)
        snap = m.snapshot()
        assert snap["logs_received"] == 2
        assert snap["bytes_compressed"] == 300
        assert snap["bytes_decompressed"] == 1500
        assert snap["compression_ratio"] == 5.0

    def test_record_connection(self):
        m = TransmissionMetrics()
        m.record_connection()
        m.record_connection()
        snap = m.snapshot()
        assert snap["total_connections"] == 2
        assert snap["active_connections"] == 2

    def test_record_disconnection(self):
        m = TransmissionMetrics()
        m.record_connection()
        m.record_connection()
        m.record_disconnection()
        snap = m.snapshot()
        assert snap["total_connections"] == 2
        assert snap["active_connections"] == 1

    def test_elapsed_seconds(self):
        m = TransmissionMetrics()
        snap = m.snapshot()
        assert snap["elapsed_seconds"] >= 0
