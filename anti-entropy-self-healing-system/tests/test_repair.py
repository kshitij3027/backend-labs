import pytest
from unittest.mock import MagicMock
from src.coordinator.repair import RepairWorker
from src.coordinator.scanner import RepairTask
from src.coordinator.client import NodeClient


def make_mock_client(node_id):
    client = MagicMock(spec=NodeClient)
    client.node_id = node_id
    client.put_data.return_value = True
    return client


def make_repair_task(key, node_id, priority=1, client=None):
    if client is None:
        client = make_mock_client(node_id)
    return RepairTask(
        key=key,
        target_node=client,
        value=f"value-for-{key}",
        version=1,
        timestamp=100.0,
        priority=priority,
    )


class TestRepairWorker:
    def test_execute_repairs_success(self):
        """All repair tasks complete successfully."""
        worker = RepairWorker()
        tasks = [
            make_repair_task("key-1", "node-a"),
            make_repair_task("key-2", "node-b"),
            make_repair_task("key-3", "node-c"),
        ]
        worker.add_tasks(tasks)

        completed, failed = worker.execute_repairs()

        assert completed == 3
        assert failed == 0
        assert worker.queue_size == 0

        # Verify put_data was called on each task's target node
        for task in tasks:
            task.target_node.put_data.assert_called_once_with(
                key=task.key,
                value=task.value,
                version=task.version,
                timestamp=task.timestamp,
            )

    def test_execute_repairs_failure(self):
        """Some repair tasks fail (put_data returns False)."""
        worker = RepairWorker()

        # Create tasks with a mix of success and failure
        client_ok = make_mock_client("node-a")
        client_ok.put_data.return_value = True

        client_fail = make_mock_client("node-b")
        client_fail.put_data.return_value = False

        tasks = [
            make_repair_task("key-1", "node-a", client=client_ok),
            make_repair_task("key-2", "node-b", client=client_fail),
            make_repair_task("key-3", "node-b", client=client_fail),
        ]
        worker.add_tasks(tasks)

        completed, failed = worker.execute_repairs()

        assert completed == 1
        assert failed == 2
        assert worker.queue_size == 0

    def test_queue_size(self):
        """Queue size reflects the number of pending tasks."""
        worker = RepairWorker()
        assert worker.queue_size == 0

        tasks = [
            make_repair_task("key-1", "node-a"),
            make_repair_task("key-2", "node-b"),
        ]
        worker.add_tasks(tasks)
        assert worker.queue_size == 2

        # Add more tasks
        worker.add_tasks([make_repair_task("key-3", "node-c")])
        assert worker.queue_size == 3

        # Execute and verify empty
        worker.execute_repairs()
        assert worker.queue_size == 0

    def test_priority_ordering(self):
        """Tasks are executed in priority order (highest first)."""
        worker = RepairWorker()

        # Track execution order
        execution_order = []

        def make_tracking_client(node_id, key):
            client = make_mock_client(node_id)
            def track_put(*args, **kwargs):
                execution_order.append(key)
                return True
            client.put_data.side_effect = track_put
            return client

        client_low = make_tracking_client("node-a", "low-priority")
        client_med = make_tracking_client("node-b", "med-priority")
        client_high = make_tracking_client("node-c", "high-priority")

        tasks = [
            make_repair_task("low-priority", "node-a", priority=1, client=client_low),
            make_repair_task("high-priority", "node-c", priority=10, client=client_high),
            make_repair_task("med-priority", "node-b", priority=5, client=client_med),
        ]
        worker.add_tasks(tasks)

        completed, failed = worker.execute_repairs()

        assert completed == 3
        assert failed == 0
        # Should execute in descending priority order
        assert execution_order == ["high-priority", "med-priority", "low-priority"]
