import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx

from app.coordinator import QuorumCoordinator, NodeConnection
from app.models import QuorumConfig, ConsistencyLevel, VectorClock
from app.metrics import QuorumMetrics


def make_nodes(n=5):
    return [NodeConnection(node_id=f"node-{i+1}", base_url=f"http://node-{i+1}:8001") for i in range(n)]


def make_entry_dict(key="k1", value="v1", node_id="node-1", vc=None, ts=1000.0):
    return {
        "key": key,
        "value": value,
        "timestamp": ts,
        "vector_clock": vc or {},
        "node_id": node_id,
    }


def make_response(status_code=200, json_data=None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    return resp


class TestQuorumWrite:
    @pytest.fixture
    def coordinator(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        return QuorumCoordinator(nodes, config, metrics)

    async def test_quorum_write_success(self, coordinator):
        """All 5 nodes respond 200 -> success with 5 acks"""
        mock_resp = make_response(200, {"success": True, "vector_clock": {}})
        coordinator.client = AsyncMock()
        coordinator.client.post = AsyncMock(return_value=mock_resp)

        result = await coordinator.write("k1", "v1")
        assert result["success"] is True
        assert result["nodes_acked"] == 5
        assert result["nodes_required"] == 3
        assert result["key"] == "k1"

    async def test_quorum_write_failure(self, coordinator):
        """3 nodes return 503, only 2 succeed -> failure"""
        call_count = 0

        async def mock_post(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return make_response(200, {"success": True, "vector_clock": {}})
            return make_response(503, {"detail": "unhealthy"})

        coordinator.client = AsyncMock()
        coordinator.client.post = AsyncMock(side_effect=mock_post)

        result = await coordinator.write("k1", "v1")
        assert result["success"] is False
        assert result["nodes_acked"] == 2

    async def test_node_failure_no_crash(self, coordinator):
        """Some nodes throw TimeoutException, coordinator doesn't crash"""
        call_count = 0

        async def mock_post(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                return make_response(200, {"success": True, "vector_clock": {}})
            raise httpx.TimeoutException("timeout")

        coordinator.client = AsyncMock()
        coordinator.client.post = AsyncMock(side_effect=mock_post)

        result = await coordinator.write("k1", "v1")
        assert result["success"] is True
        assert result["nodes_acked"] == 3


class TestQuorumRead:
    @pytest.fixture
    def coordinator(self):
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        return QuorumCoordinator(nodes, config, metrics)

    async def test_quorum_read_success(self, coordinator):
        """All nodes return same entry -> success"""
        entry = make_entry_dict("k1", "v1", "node-1", {"node-1": 1})
        mock_resp = make_response(200, entry)
        coordinator.client = AsyncMock()
        coordinator.client.get = AsyncMock(return_value=mock_resp)

        result = await coordinator.read("k1")
        assert result["success"] is True
        assert result["value"] == "v1"
        assert result["nodes_responded"] == 5

    async def test_quorum_read_failure(self, coordinator):
        """All nodes return 404 -> failure"""
        mock_resp = make_response(404, {"detail": "not found"})
        coordinator.client = AsyncMock()
        coordinator.client.get = AsyncMock(return_value=mock_resp)

        result = await coordinator.read("k1")
        assert result["success"] is False

    async def test_list_keys(self, coordinator):
        """Multiple nodes with different keys -> union"""
        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return make_response(200, {"keys": ["a", "b"]})
            elif call_count <= 4:
                return make_response(200, {"keys": ["b", "c"]})
            return make_response(200, {"keys": ["d"]})

        coordinator.client = AsyncMock()
        coordinator.client.get = AsyncMock(side_effect=mock_get)

        keys = await coordinator.list_keys()
        assert set(keys) == {"a", "b", "c", "d"}


class TestConflictResolution:
    def test_conflict_resolution(self):
        """Concurrent vector clocks -> higher timestamp wins"""
        config = QuorumConfig()
        metrics = QuorumMetrics()
        coord = QuorumCoordinator([], config, metrics)

        entry1 = make_entry_dict("k1", "v1", "node-1", {"node-1": 1}, ts=1000.0)
        entry2 = make_entry_dict("k1", "v2", "node-2", {"node-2": 1}, ts=2000.0)

        winner = coord._resolve_conflicts([entry1, entry2])
        assert winner["value"] == "v2"  # higher timestamp wins

    def test_conflict_resolution_causal(self):
        """One entry causally after another -> later wins"""
        config = QuorumConfig()
        metrics = QuorumMetrics()
        coord = QuorumCoordinator([], config, metrics)

        entry1 = make_entry_dict("k1", "v1", "node-1", {"node-1": 1}, ts=1000.0)
        entry2 = make_entry_dict("k1", "v2", "node-1", {"node-1": 2}, ts=2000.0)

        winner = coord._resolve_conflicts([entry1, entry2])
        assert winner["value"] == "v2"

    def test_conflict_resolution_same_timestamp(self):
        """Same timestamp, concurrent -> higher node_id wins"""
        config = QuorumConfig()
        metrics = QuorumMetrics()
        coord = QuorumCoordinator([], config, metrics)

        entry1 = make_entry_dict("k1", "v1", "node-1", {"node-1": 1}, ts=1000.0)
        entry2 = make_entry_dict("k1", "v2", "node-2", {"node-2": 1}, ts=1000.0)

        winner = coord._resolve_conflicts([entry1, entry2])
        assert winner["value"] == "v2"  # node-2 > node-1 lexicographically


class TestHintedHandoff:
    async def test_hinted_handoff(self):
        """Failed write stores hints, recover replays them"""
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.EVENTUAL)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        call_count = 0

        async def mock_post(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if "node-3" in url and "/store" in url and "/admin" not in url:
                return make_response(503, {"detail": "unhealthy"})
            return make_response(200, {"success": True, "vector_clock": {}, "node_id": "test", "is_healthy": True})

        coord.client = AsyncMock()
        coord.client.post = AsyncMock(side_effect=mock_post)

        await coord.write("k1", "v1")
        assert "node-3" in coord.hint_buffer
        assert len(coord.hint_buffer["node-3"]) == 1

        # Recover node-3 -> hints replayed
        await coord.recover_node("node-3")
        assert "node-3" not in coord.hint_buffer

    async def test_read_repair(self):
        """Stale node gets repaired after read"""
        config = QuorumConfig(total_replicas=5, consistency_level=ConsistencyLevel.BALANCED)
        metrics = QuorumMetrics()
        nodes = make_nodes(5)
        coord = QuorumCoordinator(nodes, config, metrics)

        # Create entries: node-1 has older version, others have newer
        stale_entry = make_entry_dict("k1", "old", "node-1", {"node-1": 1}, ts=1000.0)
        fresh_entry = make_entry_dict("k1", "new", "node-2", {"node-1": 1, "node-2": 1}, ts=2000.0)

        call_count = 0

        async def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return make_response(200, stale_entry)
            return make_response(200, fresh_entry)

        repair_calls = []

        async def mock_post(url, **kwargs):
            repair_calls.append(url)
            return make_response(200, {"success": True})

        coord.client = AsyncMock()
        coord.client.get = AsyncMock(side_effect=mock_get)
        coord.client.post = AsyncMock(side_effect=mock_post)

        result = await coord.read("k1")
        assert result["success"] is True
        assert result["value"] == "new"

        # Give async task a chance to run
        import asyncio
        await asyncio.sleep(0.1)
