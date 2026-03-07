import pytest
from unittest.mock import MagicMock
from src.coordinator.read_repair import ReadRepairHandler
from src.coordinator.client import NodeClient
from src.coordinator.strategies import Strategy


def make_mock_client(node_id, data=None):
    """Create a mock NodeClient. data is a dict of key -> entry_dict."""
    client = MagicMock(spec=NodeClient)
    client.node_id = node_id
    client.node_url = f"http://{node_id}:8001"
    client.get_data.side_effect = lambda k: (data or {}).get(k)
    client.put_data.return_value = True
    return client


class TestReadRepairHandler:
    def test_read_all_consistent(self):
        """All 3 nodes have the same data - returns entry, no repair."""
        entry = {"key": "k1", "value": "hello", "version": 1, "timestamp": 100.0}
        client_a = make_mock_client("node-a", data={"k1": entry.copy()})
        client_b = make_mock_client("node-b", data={"k1": entry.copy()})
        client_c = make_mock_client("node-c", data={"k1": entry.copy()})

        handler = ReadRepairHandler(clients=[client_a, client_b, client_c])
        result = handler.read_with_repair("k1")

        assert result is not None
        assert result["value"] == "hello"
        assert result["version"] == 1
        # No repair should have been triggered
        client_a.put_data.assert_not_called()
        client_b.put_data.assert_not_called()
        client_c.put_data.assert_not_called()

    def test_read_one_stale(self):
        """2 nodes have v2, 1 has v1 - returns v2, repairs the stale node."""
        entry_v1 = {"key": "k1", "value": "old", "version": 1, "timestamp": 100.0}
        entry_v2 = {"key": "k1", "value": "new", "version": 2, "timestamp": 200.0}

        client_a = make_mock_client("node-a", data={"k1": entry_v1.copy()})
        client_b = make_mock_client("node-b", data={"k1": entry_v2.copy()})
        client_c = make_mock_client("node-c", data={"k1": entry_v2.copy()})

        handler = ReadRepairHandler(clients=[client_a, client_b, client_c])
        result = handler.read_with_repair("k1")

        assert result is not None
        assert result["value"] == "new"
        assert result["version"] == 2
        # node-a should have been repaired
        client_a.put_data.assert_called_once_with(
            key="k1", value="new", version=2, timestamp=200.0
        )
        # node-b and node-c should NOT have been repaired
        client_b.put_data.assert_not_called()
        client_c.put_data.assert_not_called()

    def test_read_missing_on_one(self):
        """2 nodes have data, 1 returns None - repairs the missing node."""
        entry = {"key": "k1", "value": "present", "version": 1, "timestamp": 100.0}

        client_a = make_mock_client("node-a", data={"k1": entry.copy()})
        client_b = make_mock_client("node-b", data={"k1": entry.copy()})
        client_c = make_mock_client("node-c", data={})  # missing

        handler = ReadRepairHandler(clients=[client_a, client_b, client_c])
        result = handler.read_with_repair("k1")

        assert result is not None
        assert result["value"] == "present"
        # node-c should have been repaired (data was missing)
        client_c.put_data.assert_called_once_with(
            key="k1", value="present", version=1, timestamp=100.0
        )

    def test_read_not_found(self):
        """All nodes return None - returns None."""
        client_a = make_mock_client("node-a", data={})
        client_b = make_mock_client("node-b", data={})
        client_c = make_mock_client("node-c", data={})

        handler = ReadRepairHandler(clients=[client_a, client_b, client_c])
        result = handler.read_with_repair("nonexistent")

        assert result is None

    def test_read_repair_flag(self):
        """When repair happens, result has read_repaired=True."""
        entry_v1 = {"key": "k1", "value": "old", "version": 1, "timestamp": 100.0}
        entry_v2 = {"key": "k1", "value": "new", "version": 2, "timestamp": 200.0}

        client_a = make_mock_client("node-a", data={"k1": entry_v1.copy()})
        client_b = make_mock_client("node-b", data={"k1": entry_v2.copy()})

        handler = ReadRepairHandler(clients=[client_a, client_b])
        result = handler.read_with_repair("k1")

        assert result is not None
        assert result["read_repaired"] is True

    def test_read_no_repair_flag_when_consistent(self):
        """When all nodes are consistent, read_repaired should not be set (early return)."""
        entry = {"key": "k1", "value": "same", "version": 1, "timestamp": 100.0}

        client_a = make_mock_client("node-a", data={"k1": entry.copy()})
        client_b = make_mock_client("node-b", data={"k1": entry.copy()})

        handler = ReadRepairHandler(clients=[client_a, client_b])
        result = handler.read_with_repair("k1")

        assert result is not None
        assert result["value"] == "same"
        # Early return path - no read_repaired key since no conflict resolution was needed
        assert "read_repaired" not in result
