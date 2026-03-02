"""Cluster coordinator managing log distribution across storage nodes."""

import copy
import time
import threading
from src.hash_ring import HashRing
from src.storage_node import StorageNode
from src.config import ClusterConfig, NodeConfig

# EMA smoothing factor for ingestion rate tracking
EMA_ALPHA = 0.3


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

        # EMA-based ingestion rate tracking
        self._ema_rate = 0.0
        self._last_store_time: float | None = None
        self._store_count_window = 0
        self._window_start: float | None = None

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

            # Update EMA-based ingestion rate
            now = time.time()
            if self._window_start is None:
                self._window_start = now
            self._store_count_window += 1
            elapsed = now - self._window_start
            if elapsed >= 1.0:
                current_rate = self._store_count_window / elapsed
                self._ema_rate = EMA_ALPHA * current_rate + (1 - EMA_ALPHA) * self._ema_rate
                self._store_count_window = 0
                self._window_start = now
            self._last_store_time = now

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

    def adjust_node_capacity(self, node_id: str, weight: float) -> dict:
        """Adjust a node's capacity by changing its vnode count proportionally.

        A weight of 1.0 means default vnode count, 2.0 means double, 0.5 means half.

        Args:
            node_id: The node to adjust.
            weight: Capacity weight multiplier (0.1 to 10.0).

        Returns:
            Dict with node_id, weight, new_vnode_count, ring_adjustment,
            logs_rebalanced, rebalance_time_ms.
        """
        with self._lock:
            if node_id not in self._storage_nodes:
                raise ValueError(f"Node {node_id} not in cluster")

            weight = max(0.1, min(10.0, weight))
            base_vnodes = self._ring.virtual_nodes
            new_count = max(1, int(base_vnodes * weight))

            start_time = time.time()

            ring_adjustment = self._ring.adjust_vnodes(node_id, new_count)

            # Rebalance all logs across all nodes
            logs_rebalanced = self._rebalance_all()

            elapsed_ms = (time.time() - start_time) * 1000

            return {
                "node_id": node_id,
                "weight": weight,
                "new_vnode_count": new_count,
                "ring_adjustment": ring_adjustment,
                "logs_rebalanced": logs_rebalanced,
                "rebalance_time_ms": round(elapsed_ms, 2),
            }

    def _rebalance_all(self) -> int:
        """Rebalance all logs across all nodes. Returns number of logs moved.

        Caller must hold self._lock.
        """
        # Collect all logs from all nodes
        all_logs = []
        for node_id, storage_node in self._storage_nodes.items():
            logs = storage_node.get_logs()
            # Clean metadata
            for log in logs:
                clean = {k: v for k, v in log.items() if k not in ("stored_at", "node_id")}
                all_logs.append((clean, node_id))  # (clean_log, original_node_id)

        # Clear all nodes
        for storage_node in self._storage_nodes.values():
            storage_node.remove_logs(lambda _: True)

        # Re-route all logs
        moved = 0
        for clean_log, original_node_id in all_logs:
            log_key = self._generate_log_key(clean_log)
            target_node_id = self._ring.get_node(log_key)
            if target_node_id and target_node_id in self._storage_nodes:
                self._storage_nodes[target_node_id].store(clean_log)
                if target_node_id != original_node_id:
                    moved += 1

        return moved

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

    @property
    def ingestion_rate(self) -> float:
        """Return the current EMA-smoothed ingestion rate (logs/sec)."""
        with self._lock:
            return self._ema_rate

    def detect_load_skew(self, threshold: float = 10.0) -> list[dict]:
        """Detect nodes with disproportionate load.

        If any node's log percentage deviates by more than `threshold`
        from the expected average, it is flagged.

        Args:
            threshold: Maximum allowed deviation in percentage points (default 10%).

        Returns:
            List of dicts with node_id, log_percent, expected_percent, deviation, status.
        """
        with self._lock:
            node_count = len(self._storage_nodes)
            if node_count == 0:
                return []

            total_logs = sum(
                node.get_log_count() for node in self._storage_nodes.values()
            )
            if total_logs == 0:
                return []

            expected_pct = 100.0 / node_count
            alerts = []

            for node_id, node in self._storage_nodes.items():
                count = node.get_log_count()
                pct = (count / total_logs) * 100.0
                deviation = pct - expected_pct

                if abs(deviation) > threshold:
                    status = "overloaded" if deviation > 0 else "underloaded"
                    alerts.append({
                        "node_id": node_id,
                        "log_percent": round(pct, 2),
                        "expected_percent": round(expected_pct, 2),
                        "deviation": round(deviation, 2),
                        "status": status,
                    })

            return alerts

    def predict_rebalance_impact(self, node_id_to_add: str) -> dict:
        """Predict the impact of adding a node without modifying the ring.

        Creates a temporary copy of the hash ring, adds the proposed node,
        and calculates how many existing log keys would move.

        Args:
            node_id_to_add: The node ID to simulate adding.

        Returns:
            Dict with predicted_logs_moved, predicted_movement_pct,
            current_total, predicted_per_node.
        """
        with self._lock:
            # Create a temporary ring copy
            temp_ring = HashRing(virtual_nodes=self._ring.virtual_nodes)
            for existing_node_id in self._storage_nodes:
                temp_ring.add_node(existing_node_id)
            temp_ring.add_node(node_id_to_add)

            # Count how many existing logs would change their target node
            logs_moved = 0
            current_total = 0
            predicted_counts: dict[str, int] = {nid: 0 for nid in self._storage_nodes}
            predicted_counts[node_id_to_add] = 0

            for node_id, storage_node in self._storage_nodes.items():
                for log in storage_node.get_logs():
                    current_total += 1
                    log_key = self._generate_log_key(log)
                    new_target = temp_ring.get_node(log_key)
                    if new_target != node_id:
                        logs_moved += 1
                    if new_target in predicted_counts:
                        predicted_counts[new_target] += 1

            movement_pct = (logs_moved / current_total * 100.0) if current_total > 0 else 0.0

            return {
                "predicted_logs_moved": logs_moved,
                "predicted_movement_pct": round(movement_pct, 2),
                "current_total": current_total,
                "predicted_per_node": predicted_counts,
            }

    def get_monitoring_data(self) -> dict:
        """Aggregate monitoring data for the cluster.

        Returns:
            Dict with ingestion_rate, alerts, node_count, total_logs,
            per_node_distribution, and ring_health.
        """
        with self._lock:
            total_logs = sum(
                node.get_log_count() for node in self._storage_nodes.values()
            )

            per_node_distribution = {}
            for node_id, node in self._storage_nodes.items():
                count = node.get_log_count()
                pct = (count / total_logs * 100.0) if total_logs > 0 else 0.0
                per_node_distribution[node_id] = {
                    "count": count,
                    "percent": round(pct, 2),
                }

            ring_metrics = self._ring.get_ring_metrics()

            # Calculate balance variance
            if per_node_distribution:
                percentages = [d["percent"] for d in per_node_distribution.values()]
                mean_pct = sum(percentages) / len(percentages)
                variance = sum((p - mean_pct) ** 2 for p in percentages) / len(percentages)
            else:
                variance = 0.0

            return {
                "ingestion_rate": self._ema_rate,
                "alerts": self.detect_load_skew(),
                "node_count": len(self._storage_nodes),
                "total_logs": total_logs,
                "per_node_distribution": per_node_distribution,
                "ring_health": {
                    "total_vnodes": ring_metrics.get("total_vnodes", 0),
                    "balance_variance": round(variance, 4),
                },
            }
