from datetime import datetime, timezone
from src.models import Query, LogEntry, QueryFilter, TimeRange, QueryResponse, PartitionInfo


class TestLogEntry:
    def test_create_log_entry(self):
        entry = LogEntry(timestamp=datetime.now(tz=timezone.utc), level="INFO", service="test", message="hello")
        assert entry.level == "INFO"

    def test_serialization(self):
        entry = LogEntry(timestamp=datetime.now(tz=timezone.utc), level="ERROR", service="auth", message="fail", partition_id="p1")
        data = entry.model_dump()
        assert data["level"] == "ERROR"
        assert data["partition_id"] == "p1"


class TestQuery:
    def test_defaults(self):
        q = Query()
        assert q.sort_field == "timestamp"
        assert q.sort_order == "desc"
        assert q.limit is None
        assert q.filters == []
        assert q.query_id  # auto-generated

    def test_with_filters(self):
        q = Query(filters=[QueryFilter(field="level", operator="eq", value="ERROR")])
        assert len(q.filters) == 1
        assert q.filters[0].value == "ERROR"

    def test_with_time_range(self):
        tr = TimeRange(start=datetime(2024, 1, 1, tzinfo=timezone.utc), end=datetime(2024, 1, 2, tzinfo=timezone.utc))
        q = Query(time_range=tr)
        assert q.time_range.start.year == 2024


class TestQueryResponse:
    def test_create_response(self):
        resp = QueryResponse(
            query_id="test-123",
            total_results=5,
            partitions_queried=2,
            partitions_successful=2,
            total_execution_time_ms=15.5,
            results=[],
        )
        assert resp.cached is False
        assert resp.total_results == 5


class TestPartitionInfo:
    def test_defaults(self):
        info = PartitionInfo(partition_id="p1", url="http://localhost:8081")
        assert info.healthy is True
        assert info.log_count == 0
