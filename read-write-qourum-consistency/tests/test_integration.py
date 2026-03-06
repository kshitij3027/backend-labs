import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock
import httpx

from app.coordinator import QuorumCoordinator, NodeConnection
from app.models import QuorumConfig, ConsistencyLevel, VectorClock
from app.metrics import QuorumMetrics
from app.main import app


def make_nodes(n=5):
    return [NodeConnection(node_id=f"node-{i+1}", base_url=f"http://node-{i+1}:8001") for i in range(n)]


def make_response(status_code=200, json_data=None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    return resp


class TestConcurrentOperations:
    """Test 50+ concurrent operations don't crash or corrupt."""

    async def test_50_concurrent_writes(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        mock_resp = make_response(200, {"success": True, "vector_clock": {}})
        coord.client = AsyncMock()
        coord.client.post = AsyncMock(return_value=mock_resp)

        # Fire 50 concurrent writes
        tasks = [coord.write(f"key-{i}", f"value-{i}") for i in range(50)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 50
        assert all(r["success"] for r in results)
        assert metrics.total_writes == 50
        assert metrics.failed_writes == 0

    async def test_50_concurrent_reads(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        entry = {"key": "k1", "value": "v1", "timestamp": 1000.0, "vector_clock": {"node-1": 1}, "node_id": "node-1"}
        mock_resp = make_response(200, entry)
        coord.client = AsyncMock()
        coord.client.get = AsyncMock(return_value=mock_resp)

        # Fire 50 concurrent reads
        tasks = [coord.read("k1") for _ in range(50)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 50
        assert all(r["success"] for r in results)
        assert metrics.total_reads == 50

    async def test_mixed_concurrent_operations(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        write_resp = make_response(200, {"success": True, "vector_clock": {}})
        read_entry = {"key": "k1", "value": "v1", "timestamp": 1000.0, "vector_clock": {"node-1": 1}, "node_id": "node-1"}
        read_resp = make_response(200, read_entry)

        coord.client = AsyncMock()
        coord.client.post = AsyncMock(return_value=write_resp)
        coord.client.get = AsyncMock(return_value=read_resp)

        # 25 writes + 25 reads concurrently
        write_tasks = [coord.write(f"key-{i}", f"val-{i}") for i in range(25)]
        read_tasks = [coord.read("k1") for _ in range(25)]
        results = await asyncio.gather(*(write_tasks + read_tasks))

        assert len(results) == 50
        assert metrics.total_writes == 25
        assert metrics.total_reads == 25


class TestResponseTime:
    """Verify operations complete quickly (mocked network, should be <100ms)."""

    async def test_write_response_time(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        mock_resp = make_response(200, {"success": True, "vector_clock": {}})
        coord.client = AsyncMock()
        coord.client.post = AsyncMock(return_value=mock_resp)

        start = time.monotonic()
        result = await coord.write("k1", "v1")
        elapsed_ms = (time.monotonic() - start) * 1000

        assert result["success"] is True
        assert elapsed_ms < 100, f"Write took {elapsed_ms:.1f}ms, expected <100ms"

    async def test_read_response_time(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        entry = {"key": "k1", "value": "v1", "timestamp": 1000.0, "vector_clock": {"node-1": 1}, "node_id": "node-1"}
        mock_resp = make_response(200, entry)
        coord.client = AsyncMock()
        coord.client.get = AsyncMock(return_value=mock_resp)

        start = time.monotonic()
        result = await coord.read("k1")
        elapsed_ms = (time.monotonic() - start) * 1000

        assert result["success"] is True
        assert elapsed_ms < 100, f"Read took {elapsed_ms:.1f}ms, expected <100ms"


class TestEdgeCases:
    """Edge cases and robustness tests."""

    async def test_write_with_all_nodes_down(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        coord.client = AsyncMock()
        coord.client.post = AsyncMock(side_effect=httpx.TimeoutException("timeout"))

        result = await coord.write("k1", "v1")
        assert result["success"] is False
        assert result["nodes_acked"] == 0
        assert metrics.failed_writes == 1

    async def test_read_with_all_nodes_down(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        coord.client = AsyncMock()
        coord.client.get = AsyncMock(side_effect=httpx.TimeoutException("timeout"))

        result = await coord.read("k1")
        assert result["success"] is False
        assert metrics.failed_reads == 1

    async def test_eventual_consistency_single_node_sufficient(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.EVENTUAL)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        call_count = 0
        async def mock_post(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return make_response(200, {"success": True, "vector_clock": {}})
            return make_response(503, {})

        coord.client = AsyncMock()
        coord.client.post = AsyncMock(side_effect=mock_post)

        result = await coord.write("k1", "v1")
        # EVENTUAL only needs W=1
        assert result["success"] is True
        assert result["nodes_acked"] == 1

    async def test_consistency_level_changes_affect_quorum(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        assert config.read_quorum == 3
        assert config.write_quorum == 3

        config.update_for_consistency_level(ConsistencyLevel.STRONG)
        assert config.read_quorum == 5
        assert config.write_quorum == 5

        config.update_for_consistency_level(ConsistencyLevel.EVENTUAL)
        assert config.read_quorum == 1
        assert config.write_quorum == 1

    async def test_write_empty_value(self):
        """Empty string value should still work."""
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        mock_resp = make_response(200, {"success": True, "vector_clock": {}})
        coord.client = AsyncMock()
        coord.client.post = AsyncMock(return_value=mock_resp)

        result = await coord.write("k1", "")
        assert result["success"] is True
        assert result["value"] == ""


class TestRequiredUnitTests:
    """Verify all 4 required unit tests are present and pass."""

    def test_consistency_levels_present(self):
        """Verify ConsistencyLevel enum has all required values."""
        assert ConsistencyLevel.STRONG.value == "strong"
        assert ConsistencyLevel.BALANCED.value == "balanced"
        assert ConsistencyLevel.EVENTUAL.value == "eventual"

    async def test_quorum_write_success_integration(self):
        """5-node cluster write with all nodes healthy."""
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        mock_resp = make_response(200, {"success": True, "vector_clock": {}})
        coord.client = AsyncMock()
        coord.client.post = AsyncMock(return_value=mock_resp)

        result = await coord.write("test-key", "test-value")
        assert result["success"] is True
        assert result["nodes_acked"] == 5
        assert result["key"] == "test-key"

    async def test_quorum_read_success_integration(self):
        """5-node cluster read with all nodes healthy."""
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        entry = {"key": "test-key", "value": "test-value", "timestamp": 1000.0, "vector_clock": {"node-1": 1}, "node_id": "node-1"}
        mock_resp = make_response(200, entry)
        coord.client = AsyncMock()
        coord.client.get = AsyncMock(return_value=mock_resp)

        result = await coord.read("test-key")
        assert result["success"] is True
        assert result["value"] == "test-value"
        assert result["nodes_responded"] == 5

    def test_conflict_resolution_deterministic(self):
        """Concurrent entries resolve deterministically."""
        config = QuorumConfig()
        metrics = QuorumMetrics()
        coord = QuorumCoordinator([], config, metrics)

        entry1 = {"key": "k1", "value": "v1", "timestamp": 1000.0, "vector_clock": {"node-1": 1}, "node_id": "node-1"}
        entry2 = {"key": "k1", "value": "v2", "timestamp": 2000.0, "vector_clock": {"node-2": 1}, "node_id": "node-2"}

        # Run multiple times to verify determinism
        for _ in range(10):
            winner = coord._resolve_conflicts([entry1, entry2])
            assert winner["value"] == "v2"
            winner2 = coord._resolve_conflicts([entry2, entry1])
            assert winner2["value"] == "v2"
