import json
import logging
import os
import random

import requests

logger = logging.getLogger(__name__)


class ClusterUnavailableError(Exception):
    """Raised when no healthy nodes are available."""
    pass


class ClusterClient:
    """Client for interacting with the storage cluster.

    Picks a healthy node for writes, retries on failure.
    Can be used from the dashboard or external applications.
    """

    def __init__(self, cluster_nodes=None):
        """
        Args:
            cluster_nodes: List of {"id", "host", "port"} dicts.
                If None, reads from CLUSTER_NODES env var.
        """
        if cluster_nodes is None:
            raw = os.environ.get("CLUSTER_NODES", "[]")
            try:
                cluster_nodes = json.loads(raw)
            except json.JSONDecodeError:
                cluster_nodes = []

        self.cluster_nodes = list(cluster_nodes)

    def _get_healthy_nodes(self) -> list[dict]:
        """Return list of nodes that respond to health checks."""
        healthy = []
        for node in self.cluster_nodes:
            try:
                url = f"http://{node['host']}:{node['port']}/health"
                resp = requests.get(url, timeout=2)
                if resp.status_code == 200:
                    healthy.append(node)
            except requests.RequestException:
                pass
        return healthy

    def write(self, log_data: dict) -> dict:
        """Write log data to the cluster.

        Picks a random healthy node and writes to it. Retries with
        another node on failure.

        Args:
            log_data: The log data to write.

        Returns:
            Dict with file_path, checksum, version, node_id.

        Raises:
            ClusterUnavailableError: If all nodes are down.
        """
        healthy = self._get_healthy_nodes()
        if not healthy:
            raise ClusterUnavailableError("No healthy nodes available")

        # Shuffle to distribute writes
        random.shuffle(healthy)

        last_error = None
        for node in healthy:
            try:
                url = f"http://{node['host']}:{node['port']}/write"
                resp = requests.post(url, json=log_data, timeout=5)
                if resp.status_code == 201:
                    result = resp.json()
                    result["node_id"] = node["id"]
                    return result
                elif resp.status_code == 503:
                    last_error = f"Node {node['id']}: no quorum"
                    continue
                else:
                    last_error = f"Node {node['id']}: HTTP {resp.status_code}"
                    continue
            except requests.RequestException as e:
                last_error = f"Node {node['id']}: {e}"
                continue

        raise ClusterUnavailableError(f"All write attempts failed. Last error: {last_error}")

    def read(self, file_path: str) -> dict:
        """Read a file from the cluster.

        Tries each node until one returns the file.

        Args:
            file_path: The file path to read.

        Returns:
            The file record dict.

        Raises:
            ClusterUnavailableError: If all nodes are down.
            FileNotFoundError: If no node has the file.
        """
        healthy = self._get_healthy_nodes()
        if not healthy:
            raise ClusterUnavailableError("No healthy nodes available")

        for node in healthy:
            try:
                url = f"http://{node['host']}:{node['port']}/read/{file_path}"
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 404:
                    continue
            except requests.RequestException:
                continue

        raise FileNotFoundError(f"File {file_path} not found on any node")

    def health(self) -> dict:
        """Get cluster health overview.

        Returns:
            Dict with healthy_nodes, total_nodes, quorum, and node details.
        """
        node_statuses = []
        for node in self.cluster_nodes:
            try:
                url = f"http://{node['host']}:{node['port']}/health"
                resp = requests.get(url, timeout=2)
                if resp.status_code == 200:
                    node_statuses.append({**node, "status": "healthy"})
                else:
                    node_statuses.append({**node, "status": "unhealthy"})
            except requests.RequestException:
                node_statuses.append({**node, "status": "unhealthy"})

        healthy_count = sum(1 for n in node_statuses if n["status"] == "healthy")
        return {
            "healthy_nodes": healthy_count,
            "total_nodes": len(node_statuses),
            "quorum": healthy_count >= 2,
            "nodes": node_statuses,
        }
