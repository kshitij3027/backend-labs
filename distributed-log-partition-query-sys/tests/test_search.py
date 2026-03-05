from datetime import datetime, timedelta, timezone
from src.models import Query, QueryFilter, TimeRange
from src.partition.storage import LogStorage
from src.partition.search import LogSearchEngine
from src.partition.data_generator import generate_sample_logs


class TestLogSearchEngine:
    def setup_method(self):
        self.storage = LogStorage()
        self.entries = generate_sample_logs(500, 7, "test_partition")
        self.storage.load(self.entries)
        self.engine = LogSearchEngine()

    def test_no_filters(self):
        query = Query(limit=10)
        results = self.engine.search(self.storage, query)
        assert len(results) == 10

    def test_filter_by_level(self):
        query = Query(filters=[QueryFilter(field="level", operator="eq", value="ERROR")])
        results = self.engine.search(self.storage, query)
        assert all(r.level == "ERROR" for r in results)

    def test_filter_by_service(self):
        query = Query(filters=[QueryFilter(field="service", operator="eq", value="auth-service")])
        results = self.engine.search(self.storage, query)
        assert all(r.service == "auth-service" for r in results)

    def test_contains_filter(self):
        query = Query(filters=[QueryFilter(field="message", operator="contains", value="timeout")])
        results = self.engine.search(self.storage, query)
        assert all("timeout" in r.message.lower() for r in results)

    def test_time_range_filter(self):
        now = datetime.now(tz=timezone.utc)
        tr = TimeRange(start=now - timedelta(days=1), end=now)
        query = Query(time_range=tr)
        results = self.engine.search(self.storage, query)
        assert all(tr.start <= r.timestamp <= tr.end for r in results)

    def test_sort_ascending(self):
        query = Query(sort_order="asc", limit=20)
        results = self.engine.search(self.storage, query)
        for i in range(len(results) - 1):
            assert results[i].timestamp <= results[i + 1].timestamp

    def test_sort_descending(self):
        query = Query(sort_order="desc", limit=20)
        results = self.engine.search(self.storage, query)
        for i in range(len(results) - 1):
            assert results[i].timestamp >= results[i + 1].timestamp

    def test_combined_filters(self):
        query = Query(
            filters=[
                QueryFilter(field="level", operator="eq", value="ERROR"),
                QueryFilter(field="service", operator="eq", value="auth-service"),
            ],
            limit=50,
        )
        results = self.engine.search(self.storage, query)
        assert all(r.level == "ERROR" and r.service == "auth-service" for r in results)
