"""Cluster health monitoring and quorum enforcement.

Periodically polls each node's /health endpoint. After 3 consecutive
failures a node is marked unhealthy. Quorum requires at least
quorum_size nodes to be healthy before writes are accepted.
"""

import logging
import threading
import time

import requests

logger = logging.getLogger(__name__)


class ClusterManager:
    """Monitors cluster health and enforces quorum for writes.

    Periodically polls each node's /health endpoint. After 3 consecutive
    failures, a node is marked unhealthy. Quorum requires at least
    quorum_size nodes to be healthy.
    """

    def __init__(self, config):
        """
        Args:
            config: ClusterConfig for this node.
        """
        self.config = config
        self._lock = threading.Lock()
        self._running = True

        # Node registry: {node_id: {status, last_seen, consecutive_failures, host, port}}
        self._nodes = {}
        for node in config.cluster_nodes:
            self._nodes[node["id"]] = {
                "status": "healthy",  # assume healthy at start
                "last_seen": time.time(),
                "consecutive_failures": 0,
                "host": node["host"],
                "port": node["port"],
            }

        # Mark self as always healthy
        if config.node_id in self._nodes:
            self._nodes[config.node_id]["status"] = "healthy"
            self._nodes[config.node_id]["last_seen"] = time.time()

        # Start health check loop
        self._health_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True,
            name="health-checker",
        )
        self._health_thread.start()

    def _health_check_loop(self):
        """Periodically check health of all peer nodes."""
        while self._running:
            self._check_all_nodes()
            time.sleep(self.config.health_check_interval)

    def _check_all_nodes(self):
        """Poll /health on every node except self."""
        for node_id, info in list(self._nodes.items()):
            if node_id == self.config.node_id:
                # Self is always healthy
                with self._lock:
                    info["status"] = "healthy"
                    info["last_seen"] = time.time()
                    info["consecutive_failures"] = 0
                continue

            self._check_node(node_id, info)

    def _check_node(self, node_id: str, info: dict):
        """Check a single node's health."""
        url = f"http://{info['host']}:{info['port']}/health"
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                with self._lock:
                    info["status"] = "healthy"
                    info["last_seen"] = time.time()
                    info["consecutive_failures"] = 0
            else:
                self._record_failure(node_id, info)
        except requests.RequestException:
            self._record_failure(node_id, info)

    def _record_failure(self, node_id: str, info: dict):
        """Record a health check failure. 3 consecutive = unhealthy."""
        with self._lock:
            info["consecutive_failures"] += 1
            if info["consecutive_failures"] >= 3:
                if info["status"] != "unhealthy":
                    logger.warning(f"Node {node_id} marked unhealthy after 3 failures")
                info["status"] = "unhealthy"

    def has_quorum(self) -> bool:
        """Check if enough nodes are healthy for writes."""
        with self._lock:
            healthy_count = sum(
                1 for info in self._nodes.values()
                if info["status"] == "healthy"
            )
        return healthy_count >= self.config.quorum_size

    def get_healthy_nodes(self) -> list[str]:
        """Return list of healthy node IDs."""
        with self._lock:
            return [
                nid for nid, info in self._nodes.items()
                if info["status"] == "healthy"
            ]

    def get_cluster_status(self) -> dict:
        """Return full cluster status information."""
        with self._lock:
            nodes = {}
            for nid, info in self._nodes.items():
                nodes[nid] = {
                    "status": info["status"],
                    "last_seen": info["last_seen"],
                    "consecutive_failures": info["consecutive_failures"],
                    "host": info["host"],
                    "port": info["port"],
                }

        healthy_count = sum(1 for n in nodes.values() if n["status"] == "healthy")
        return {
            "quorum": healthy_count >= self.config.quorum_size,
            "healthy_nodes": healthy_count,
            "total_nodes": len(nodes),
            "nodes": nodes,
        }

    def shutdown(self):
        """Stop the health check thread."""
        self._running = False
