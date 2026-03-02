"""Cluster coordinator managing log distribution across storage nodes."""

import time
import threading
from src.hash_ring import HashRing
from src.storage_node import StorageNode
from src.config import ClusterConfig, NodeConfig


class ClusterCoordinator:
    """Manages the lifecycle of storage nodes and distributes logs via consistent hashing."""

    def __init__(self, config: ClusterConfig | None = None):
        """Initialize the coordinator.

        If config is provided, creates nodes from config.
        If no config, creates an empty coordinator (nodes can be added later).
        """
        self._lock = threading.RLock()

        virtual_nodes = config.virtual_nodes if config else 150
        self._ring = HashRing(virtual_nodes=virtual_nodes)
        self._storage_nodes: dict[str, StorageNode] = {}
        self._total_stored = 0

        if config and config.nodes:
            for node_config in config.nodes:
                self._add_node_internal(node_config.id)

    def _add_node_internal(self, node_id: str):
        """Add a node without rebalancing (for initial setup)."""
        self._storage_nodes[node_id] = StorageNode(node_id)
        self._ring.add_node(node_id)

    def _generate_log_key(self, log_entry: dict) -> str:
        """Generate a hash key from a log entry.

        Format: "{source}:{timestamp}:{hash(message)}"
        """
        source = log_entry.get("source", "unknown")
        timestamp = log_entry.get("timestamp", "")
        message = log_entry.get("message", "")
        msg_hash = hash(message)
        return f"{source}:{timestamp}:{msg_hash}"

    def store_log(self, log_entry: dict) -> dict:
        """Store a single log entry, routing to the appropriate node.

        Returns dict with: node_id, log_key, entry (the stored entry with metadata)
        """
        with self._lock:
            log_key = self._generate_log_key(log_entry)
            node_id = self._ring.get_node(log_key)

            if node_id is None or node_id not in self._storage_nodes:
                raise ValueError("No nodes available in the cluster")

            stored_entry = self._storage_nodes[node_id].store(log_entry)
            self._total_stored += 1

            return {
                "node_id": node_id,
                "log_key": log_key,
                "entry": stored_entry,
            }

    def store_logs(self, entries: list[dict]) -> list[dict]:
        """Store multiple log entries. Returns list of store results."""
        results = []
        for entry in entries:
            results.append(self.store_log(entry))
        return results

    def add_node(self, node_id: str) -> dict:
        """Add a new node to the cluster and rebalance.

        Rebalancing works by re-hashing all existing logs across all nodes
        and moving those that now map to the new node.

        Returns dict with: node_id, ring_update (from HashRing.add_node),
        logs_migrated, migration_time_ms, total_logs
        """
        with self._lock:
            start_time = time.time()

            # Create the storage node
            self._storage_nodes[node_id] = StorageNode(node_id)

            # Add to ring
            ring_update = self._ring.add_node(node_id)

            # Rebalance: check each log in every OTHER node to see if it
            # now belongs to the new node
            logs_migrated = 0
            for existing_node_id, storage_node in self._storage_nodes.items():
                if existing_node_id == node_id:
                    continue

                # Remove logs that should now go to a different node
                def should_move(log_entry, coordinator=self):
                    log_key = coordinator._generate_log_key(log_entry)
                    target = coordinator._ring.get_node(log_key)
                    return target != existing_node_id

                moved_logs = storage_node.remove_logs(should_move)

                # Route each moved log to its correct new node
                for log in moved_logs:
                    log_key = self._generate_log_key(log)
                    target_node_id = self._ring.get_node(log_key)
                    if target_node_id and target_node_id in self._storage_nodes:
                        # Remove old metadata before re-storing
                        clean_log = {k: v for k, v in log.items()
                                     if k not in ("stored_at", "node_id")}
                        self._storage_nodes[target_node_id].store(clean_log)
                        logs_migrated += 1

            elapsed_ms = (time.time() - start_time) * 1000

            return {
                "node_id": node_id,
                "ring_update": ring_update,
                "logs_migrated": logs_migrated,
                "migration_time_ms": round(elapsed_ms, 2),
                "total_logs": self._total_stored,
            }

    def remove_node(self, node_id: str) -> dict:
        """Remove a node from the cluster and redistribute its logs.

        Returns dict with: node_id, ring_update, logs_migrated, migration_time_ms
        """
        with self._lock:
            if node_id not in self._storage_nodes:
                raise ValueError(f"Node {node_id} not in cluster")

            start_time = time.time()

            # Get all logs from the node being removed
            removed_node = self._storage_nodes.pop(node_id)
            orphaned_logs = removed_node.get_logs()

            # Remove from ring
            ring_update = self._ring.remove_node(node_id)

            # Redistribute orphaned logs to remaining nodes
            logs_migrated = 0
            for log in orphaned_logs:
                log_key = self._generate_log_key(log)
                target_node_id = self._ring.get_node(log_key)
                if target_node_id and target_node_id in self._storage_nodes:
                    clean_log = {k: v for k, v in log.items()
                                 if k not in ("stored_at", "node_id")}
                    self._storage_nodes[target_node_id].store(clean_log)
                    logs_migrated += 1

            elapsed_ms = (time.time() - start_time) * 1000

            return {
                "node_id": node_id,
                "ring_update": ring_update,
                "logs_migrated": logs_migrated,
                "migration_time_ms": round(elapsed_ms, 2),
            }

    def get_cluster_metrics(self) -> dict:
        """Get comprehensive cluster metrics.

        Returns dict with: total_logs, node_count,
        nodes (per-node log count and percentage),
        ring_metrics (from HashRing), balance_variance
        """
        with self._lock:
            total_logs = sum(
                node.get_log_count() for node in self._storage_nodes.values()
            )

            node_stats = {}
            for node_id, node in self._storage_nodes.items():
                count = node.get_log_count()
                pct = (count / total_logs * 100) if total_logs > 0 else 0
                node_stats[node_id] = {
                    "log_count": count,
                    "log_percent": round(pct, 2),
                    **node.get_stats(),
                }

            ring_metrics = self._ring.get_ring_metrics()

            # Calculate balance variance (std dev of percentages)
            if node_stats:
                percentages = [s["log_percent"] for s in node_stats.values()]
                mean_pct = sum(percentages) / len(percentages)
                variance = sum((p - mean_pct) ** 2 for p in percentages) / len(percentages)
                std_dev = variance ** 0.5
            else:
                std_dev = 0.0

            return {
                "total_logs": total_logs,
                "node_count": len(self._storage_nodes),
                "nodes": node_stats,
                "ring_metrics": ring_metrics,
                "balance_variance": round(std_dev, 2),
            }

    def get_node_ids(self) -> list[str]:
        """Return list of active node IDs."""
        with self._lock:
            return list(self._storage_nodes.keys())
