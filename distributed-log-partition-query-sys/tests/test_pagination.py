import pytest
from datetime import datetime, timedelta, timezone

from src.models import LogEntry, Query, PaginatedQueryResponse
from src.coordinator.merger import ResultMerger


def make_entries(count: int) -> list[LogEntry]:
    now = datetime.now(tz=timezone.utc)
    return [
        LogEntry(
            timestamp=now - timedelta(minutes=i),
            level="INFO",
            service="test",
            message=f"msg-{i}",
            partition_id="p1",
        )
        for i in range(count)
    ]


class TestPagination:
    def setup_method(self):
        self.merger = ResultMerger(max_merge_size=10000)

    def test_first_page(self):
        entries = make_entries(25)
        results, total = self.merger.merge_paginated(
            [entries], page=1, page_size=10, sort_order="desc"
        )
        assert len(results) == 10
        assert total == 25

    def test_second_page(self):
        entries = make_entries(25)
        results, total = self.merger.merge_paginated(
            [entries], page=2, page_size=10, sort_order="desc"
        )
        assert len(results) == 10
        assert total == 25

    def test_last_page_partial(self):
        entries = make_entries(25)
        results, total = self.merger.merge_paginated(
            [entries], page=3, page_size=10, sort_order="desc"
        )
        assert len(results) == 5
        assert total == 25

    def test_page_beyond_range(self):
        entries = make_entries(10)
        results, total = self.merger.merge_paginated(
            [entries], page=5, page_size=10, sort_order="desc"
        )
        assert len(results) == 0
        assert total == 10

    def test_pages_dont_overlap(self):
        entries = make_entries(20)
        page1, _ = self.merger.merge_paginated(
            [entries], page=1, page_size=10, sort_order="desc"
        )
        page2, _ = self.merger.merge_paginated(
            [entries], page=2, page_size=10, sort_order="desc"
        )
        page1_msgs = {e.message for e in page1}
        page2_msgs = {e.message for e in page2}
        assert page1_msgs.isdisjoint(page2_msgs)

    def test_paginated_response_model(self):
        resp = PaginatedQueryResponse(
            query_id="test",
            total_results=100,
            partitions_queried=2,
            partitions_successful=2,
            total_execution_time_ms=10.0,
            results=[],
            page=2,
            page_size=10,
            total_pages=10,
            has_next=True,
            has_previous=True,
        )
        assert resp.page == 2
        assert resp.has_next is True
        assert resp.has_previous is True
