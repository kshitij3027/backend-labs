import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from src.models import PartitionInfo, Query
from src.coordinator.scatter_gather import ScatterGather, ScatterResult


def make_partition(pid: str, port: int = 8081) -> PartitionInfo:
    return PartitionInfo(partition_id=pid, url=f"http://{pid}:{port}")


def make_mock_response(entries: list[dict], status_code: int = 200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = {
        "query_id": "test",
        "total_results": len(entries),
        "partitions_queried": 1,
        "partitions_successful": 1,
        "total_execution_time_ms": 5.0,
        "results": entries,
    }
    return mock


class TestScatterGather:
    @pytest.mark.asyncio
    async def test_both_succeed(self):
        now = datetime.now(tz=timezone.utc).isoformat()
        mock_entries = [
            {
                "timestamp": now,
                "level": "INFO",
                "service": "test",
                "message": "ok",
                "partition_id": "p1",
            }
        ]

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post = AsyncMock(return_value=make_mock_response(mock_entries))

        sg = ScatterGather(client=mock_client, timeout=5.0)
        partitions = [make_partition("p1"), make_partition("p2")]
        results = await sg.scatter(partitions, Query(limit=10))

        assert len(results) == 2
        assert all(r.success for r in results)
        assert all(len(r.entries) == 1 for r in results)

    @pytest.mark.asyncio
    async def test_one_timeout(self):
        now = datetime.now(tz=timezone.utc).isoformat()
        mock_entries = [
            {
                "timestamp": now,
                "level": "INFO",
                "service": "test",
                "message": "ok",
                "partition_id": "p1",
            }
        ]

        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return make_mock_response(mock_entries)
            raise httpx.TimeoutException("Timeout")

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post = mock_post

        sg = ScatterGather(client=mock_client, timeout=1.0)
        partitions = [make_partition("p1"), make_partition("p2")]
        results = await sg.scatter(partitions, Query(limit=10))

        assert len(results) == 2
        successes = [r for r in results if r.success]
        failures = [r for r in results if not r.success]
        assert len(successes) == 1
        assert len(failures) == 1
        assert "Timeout" in failures[0].error

    @pytest.mark.asyncio
    async def test_one_500_error(self):
        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                now = datetime.now(tz=timezone.utc).isoformat()
                return make_mock_response(
                    [
                        {
                            "timestamp": now,
                            "level": "INFO",
                            "service": "test",
                            "message": "ok",
                            "partition_id": "p1",
                        }
                    ]
                )
            return make_mock_response([], status_code=500)

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post = mock_post

        sg = ScatterGather(client=mock_client, timeout=5.0)
        partitions = [make_partition("p1"), make_partition("p2")]
        results = await sg.scatter(partitions, Query(limit=10))

        assert len(results) == 2
        failures = [r for r in results if not r.success]
        assert len(failures) == 1
        assert "500" in failures[0].error

    @pytest.mark.asyncio
    async def test_all_fail(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post = AsyncMock(
            side_effect=httpx.ConnectError("Connection refused")
        )

        sg = ScatterGather(client=mock_client, timeout=5.0)
        partitions = [make_partition("p1"), make_partition("p2")]
        results = await sg.scatter(partitions, Query(limit=10))

        assert len(results) == 2
        assert all(not r.success for r in results)

    @pytest.mark.asyncio
    async def test_empty_partitions(self):
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        sg = ScatterGather(client=mock_client, timeout=5.0)
        results = await sg.scatter([], Query(limit=10))
        assert results == []
