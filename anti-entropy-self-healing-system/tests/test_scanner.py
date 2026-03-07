import pytest
from unittest.mock import MagicMock
from src.coordinator.scanner import AntiEntropyScanner, RepairTask
from src.coordinator.client import NodeClient
from src.coordinator.strategies import Strategy


def make_mock_client(node_id, root_hash, leaves=None, data=None):
    client = MagicMock(spec=NodeClient)
    client.node_id = node_id
    client.get_merkle_root.return_value = root_hash
    client.get_merkle_leaves.return_value = leaves or {}
    # data is a dict of key -> entry_dict
    client.get_data.side_effect = lambda k: (data or {}).get(k)
    client.put_data.return_value = True
    return client


class TestAntiEntropyScanner:
    def test_scan_consistent_nodes(self):
        """All nodes have the same root hash - no repair tasks generated."""
        client_a = make_mock_client("node-a", root_hash="abc123")
        client_b = make_mock_client("node-b", root_hash="abc123")
        client_c = make_mock_client("node-c", root_hash="abc123")

        scanner = AntiEntropyScanner(clients=[client_a, client_b, client_c])
        tasks = scanner.run_scan()

        assert len(tasks) == 0

    def test_scan_detects_inconsistency(self):
        """Two nodes have different roots; scanner detects diff keys and generates repair tasks."""
        data_a = {
            "key-1": {"key": "key-1", "value": "hello", "version": 1, "timestamp": 100.0},
        }
        data_b = {
            "key-1": {"key": "key-1", "value": "world", "version": 2, "timestamp": 200.0},
        }

        client_a = make_mock_client(
            "node-a",
            root_hash="root-a",
            leaves={"key-1": "leaf-hash-a"},
            data=data_a,
        )
        client_b = make_mock_client(
            "node-b",
            root_hash="root-b",
            leaves={"key-1": "leaf-hash-b"},
            data=data_b,
        )

        scanner = AntiEntropyScanner(clients=[client_a, client_b])
        tasks = scanner.run_scan()

        # node-b has the latest write (timestamp 200), so node-a needs repair
        assert len(tasks) == 1
        assert tasks[0].key == "key-1"
        assert tasks[0].target_node == client_a
        assert tasks[0].value == "world"
        assert tasks[0].version == 2
        assert tasks[0].timestamp == 200.0

    def test_scan_node_unreachable(self):
        """One node returns None for root - scanner continues without crashing."""
        client_a = make_mock_client("node-a", root_hash="abc123")
        client_b = make_mock_client("node-b", root_hash=None)  # unreachable

        scanner = AntiEntropyScanner(clients=[client_a, client_b])
        tasks = scanner.run_scan()

        # Should not crash, no tasks generated since we can't compare
        assert len(tasks) == 0

    def test_scan_resolves_conflict_latest_write(self):
        """Two entries for the same key - latest timestamp wins."""
        data_a = {
            "key-x": {"key": "key-x", "value": "old-value", "version": 1, "timestamp": 100.0},
        }
        data_b = {
            "key-x": {"key": "key-x", "value": "new-value", "version": 1, "timestamp": 300.0},
        }

        client_a = make_mock_client(
            "node-a",
            root_hash="root-a",
            leaves={"key-x": "hash-a"},
            data=data_a,
        )
        client_b = make_mock_client(
            "node-b",
            root_hash="root-b",
            leaves={"key-x": "hash-b"},
            data=data_b,
        )

        scanner = AntiEntropyScanner(
            clients=[client_a, client_b],
            strategy=Strategy.LATEST_WRITE_WINS,
        )
        tasks = scanner.run_scan()

        assert len(tasks) == 1
        assert tasks[0].target_node == client_a
        assert tasks[0].value == "new-value"
        assert tasks[0].timestamp == 300.0

    def test_scan_resolves_conflict_highest_version(self):
        """Two entries for the same key - highest version wins."""
        data_a = {
            "key-y": {"key": "key-y", "value": "v3-value", "version": 3, "timestamp": 100.0},
        }
        data_b = {
            "key-y": {"key": "key-y", "value": "v1-value", "version": 1, "timestamp": 300.0},
        }

        client_a = make_mock_client(
            "node-a",
            root_hash="root-a",
            leaves={"key-y": "hash-a"},
            data=data_a,
        )
        client_b = make_mock_client(
            "node-b",
            root_hash="root-b",
            leaves={"key-y": "hash-b"},
            data=data_b,
        )

        scanner = AntiEntropyScanner(
            clients=[client_a, client_b],
            strategy=Strategy.HIGHEST_VERSION,
        )
        tasks = scanner.run_scan()

        # node-a has version 3, node-b has version 1 -> node-b needs repair
        assert len(tasks) == 1
        assert tasks[0].target_node == client_b
        assert tasks[0].value == "v3-value"
        assert tasks[0].version == 3

    def test_scan_missing_key_on_one_node(self):
        """A key exists on one node but not the other - repair task for the missing node."""
        data_a = {
            "key-z": {"key": "key-z", "value": "exists", "version": 1, "timestamp": 100.0},
        }

        client_a = make_mock_client(
            "node-a",
            root_hash="root-a",
            leaves={"key-z": "hash-a"},
            data=data_a,
        )
        client_b = make_mock_client(
            "node-b",
            root_hash="root-b",
            leaves={},  # no key-z
            data={},    # no key-z
        )

        scanner = AntiEntropyScanner(clients=[client_a, client_b])
        tasks = scanner.run_scan()

        assert len(tasks) == 1
        assert tasks[0].target_node == client_b
        assert tasks[0].key == "key-z"
        assert tasks[0].value == "exists"
