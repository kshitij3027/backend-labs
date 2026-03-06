import pytest
from app.models import ConsistencyLevel, QuorumConfig, VectorClock, LogEntry
from app.metrics import QuorumMetrics


class TestConsistencyLevels:
    def test_consistency_levels(self):
        # STRONG: R=N, W=N
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.STRONG)
        assert config.read_quorum == 5
        assert config.write_quorum == 5

        # BALANCED: R=(N//2)+1, W=(N//2)+1
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        assert config.read_quorum == 3
        assert config.write_quorum == 3

        # EVENTUAL: R=1, W=1
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.EVENTUAL)
        assert config.read_quorum == 1
        assert config.write_quorum == 1

    def test_quorum_config_validation(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        assert config.validate_consistency() is True

        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.EVENTUAL)
        assert config.validate_consistency() is False

    def test_quorum_config_update_level(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        assert config.read_quorum == 3
        config.update_for_consistency_level(ConsistencyLevel.STRONG)
        assert config.read_quorum == 5
        assert config.write_quorum == 5
        config.update_for_consistency_level(ConsistencyLevel.EVENTUAL)
        assert config.read_quorum == 1
        assert config.write_quorum == 1

    def test_quorum_config_to_dict(self):
        config = QuorumConfig()
        d = config.to_dict()
        assert d["total_replicas"] == 5
        assert d["consistency_level"] == "balanced"


class TestVectorClock:
    def test_vector_clock_increment(self):
        vc = VectorClock()
        vc.increment("node-1")
        assert vc.clock["node-1"] == 1
        vc.increment("node-1")
        assert vc.clock["node-1"] == 2

    def test_vector_clock_update(self):
        vc1 = VectorClock()
        vc1.increment("node-1")
        vc1.increment("node-1")

        vc2 = VectorClock()
        vc2.increment("node-2")

        vc1.update(vc2)
        assert vc1.clock["node-1"] == 2
        assert vc1.clock["node-2"] == 1

    def test_vector_clock_compare_before(self):
        vc1 = VectorClock()
        vc1.increment("node-1")

        vc2 = VectorClock()
        vc2.increment("node-1")
        vc2.increment("node-1")

        assert vc1.compare(vc2) == "before"

    def test_vector_clock_compare_after(self):
        vc1 = VectorClock()
        vc1.increment("node-1")
        vc1.increment("node-1")

        vc2 = VectorClock()
        vc2.increment("node-1")

        assert vc1.compare(vc2) == "after"

    def test_vector_clock_compare_equal(self):
        vc1 = VectorClock()
        vc1.increment("node-1")

        vc2 = VectorClock()
        vc2.increment("node-1")

        assert vc1.compare(vc2) == "equal"

    def test_vector_clock_compare_concurrent(self):
        vc1 = VectorClock()
        vc1.increment("node-1")

        vc2 = VectorClock()
        vc2.increment("node-2")

        assert vc1.compare(vc2) == "concurrent"

    def test_vector_clock_serialization(self):
        vc = VectorClock()
        vc.increment("node-1")
        vc.increment("node-2")

        d = vc.to_dict()
        vc2 = VectorClock.from_dict(d)
        assert vc.clock == vc2.clock
        assert vc.compare(vc2) == "equal"

    def test_vector_clock_copy(self):
        vc = VectorClock()
        vc.increment("node-1")
        vc2 = vc.copy()
        vc.increment("node-1")
        assert vc2.clock["node-1"] == 1
        assert vc.clock["node-1"] == 2


class TestLogEntry:
    def test_log_entry_creation(self):
        entry = LogEntry(key="test", value="hello", node_id="node-1")
        assert entry.key == "test"
        assert entry.value == "hello"
        assert entry.node_id == "node-1"
        assert entry.timestamp > 0

    def test_log_entry_serialization(self):
        entry = LogEntry(key="test", value="hello", timestamp=1000.0, node_id="node-1")
        entry.vector_clock.increment("node-1")

        d = entry.to_dict()
        assert d["key"] == "test"
        assert d["value"] == "hello"
        assert d["timestamp"] == 1000.0
        assert d["vector_clock"] == {"node-1": 1}

        entry2 = LogEntry.from_dict(d)
        assert entry2.key == entry.key
        assert entry2.value == entry.value
        assert entry2.timestamp == entry.timestamp
        assert entry2.node_id == entry.node_id
        assert entry2.vector_clock.clock == entry.vector_clock.clock


class TestMetrics:
    def test_metrics_tracking(self):
        m = QuorumMetrics()
        m.record_read(True)
        m.record_read(False)
        m.record_write(True)
        m.record_write(False)
        m.record_violation()

        d = m.to_dict()
        assert d["total_reads"] == 2
        assert d["failed_reads"] == 1
        assert d["total_writes"] == 2
        assert d["failed_writes"] == 1
        assert d["consistency_violations"] == 1
