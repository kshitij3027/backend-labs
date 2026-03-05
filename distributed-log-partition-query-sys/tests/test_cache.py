from src.coordinator.cache import QueryCache
from src.models import Query, QueryFilter, QueryResponse


def make_response(query_id: str = "test", total: int = 5) -> QueryResponse:
    return QueryResponse(
        query_id=query_id,
        total_results=total,
        partitions_queried=2,
        partitions_successful=2,
        total_execution_time_ms=10.0,
        results=[],
    )


class TestQueryCache:
    def test_put_and_get(self):
        cache = QueryCache(max_size=10)
        query = Query(limit=10)
        response = make_response()
        cache.put(query, response)
        cached = cache.get(query)
        assert cached is not None
        assert cached.total_results == 5

    def test_cache_miss(self):
        cache = QueryCache(max_size=10)
        query = Query(limit=10)
        assert cache.get(query) is None

    def test_different_queries_different_keys(self):
        cache = QueryCache(max_size=10)
        q1 = Query(limit=10)
        q2 = Query(limit=20)
        cache.put(q1, make_response("r1", 10))
        cache.put(q2, make_response("r2", 20))
        assert cache.get(q1).total_results == 10
        assert cache.get(q2).total_results == 20

    def test_same_query_different_id(self):
        """Same query params but different query_id should hit cache."""
        cache = QueryCache(max_size=10)
        q1 = Query(query_id="aaa", limit=10)
        q2 = Query(query_id="bbb", limit=10)
        cache.put(q1, make_response())
        assert cache.get(q2) is not None

    def test_lru_eviction(self):
        cache = QueryCache(max_size=2)
        q1 = Query(limit=1)
        q2 = Query(limit=2)
        q3 = Query(limit=3)
        cache.put(q1, make_response("r1", 1))
        cache.put(q2, make_response("r2", 2))
        cache.put(q3, make_response("r3", 3))
        # q1 should be evicted
        assert cache.get(q1) is None
        assert cache.get(q2) is not None
        assert cache.get(q3) is not None

    def test_stats_tracking(self):
        cache = QueryCache(max_size=10)
        query = Query(limit=10)
        cache.put(query, make_response())

        cache.get(query)  # hit
        cache.get(Query(limit=999))  # miss
        cache.get(query)  # hit

        stats = cache.stats
        assert stats["hits"] == 2
        assert stats["misses"] == 1
        assert stats["cache_size"] == 1
        assert stats["hit_rate"] > 0.6

    def test_clear(self):
        cache = QueryCache(max_size=10)
        cache.put(Query(limit=1), make_response())
        cache.clear()
        assert cache.stats["cache_size"] == 0
        assert cache.stats["hits"] == 0

    def test_filter_affects_key(self):
        cache = QueryCache(max_size=10)
        q1 = Query(
            filters=[QueryFilter(field="level", operator="eq", value="ERROR")]
        )
        q2 = Query(
            filters=[QueryFilter(field="level", operator="eq", value="INFO")]
        )
        cache.put(q1, make_response("r1", 10))
        assert cache.get(q2) is None
