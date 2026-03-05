from src.partition.storage import LogStorage
from src.partition.data_generator import generate_sample_logs


class TestLogStorage:
    def test_load_entries(self):
        storage = LogStorage()
        entries = generate_sample_logs(100, 7, "test_partition")
        storage.load(entries)
        assert storage.count == 100

    def test_index_by_level(self):
        storage = LogStorage()
        entries = generate_sample_logs(1000, 7, "test_partition")
        storage.load(entries)
        errors = storage.get_by_index("level", "ERROR")
        assert len(errors) > 0
        assert all(e.level == "ERROR" for e in errors)

    def test_index_by_service(self):
        storage = LogStorage()
        entries = generate_sample_logs(1000, 7, "test_partition")
        storage.load(entries)
        auth = storage.get_by_index("service", "auth-service")
        assert len(auth) > 0
        assert all(e.service == "auth-service" for e in auth)

    def test_time_range(self):
        storage = LogStorage()
        entries = generate_sample_logs(100, 7, "test_partition")
        storage.load(entries)
        tr = storage.time_range
        assert tr is not None
        assert tr[0] <= tr[1]  # min <= max

    def test_empty_storage(self):
        storage = LogStorage()
        storage.load([])
        assert storage.count == 0
        assert storage.time_range is None

    def test_deterministic_generation(self):
        entries1 = generate_sample_logs(50, 7, "partition_1")
        entries2 = generate_sample_logs(50, 7, "partition_1")
        assert len(entries1) == len(entries2)
        for e1, e2 in zip(entries1, entries2):
            assert e1.level == e2.level
            assert e1.service == e2.service

    def test_different_partitions_different_data(self):
        entries1 = generate_sample_logs(100, 7, "partition_1")
        entries2 = generate_sample_logs(100, 7, "partition_2")
        # Data should differ between partitions
        messages1 = [e.message for e in entries1]
        messages2 = [e.message for e in entries2]
        assert messages1 != messages2
