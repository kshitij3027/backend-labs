"""File versioning and read-repair for the storage cluster.

Tracks integer version counters per file and performs background
read-repair to synchronise stale or missing replicas across peer nodes.
"""

import logging
import threading

import requests

logger = logging.getLogger(__name__)


class VersionManager:
    """Tracks file versions and performs read-repair.

    Each file has an integer version counter. On read, compares the local
    version against peers and repairs stale replicas in the background.
    """

    def __init__(self, config):
        self.config = config
        self._versions = {}  # file_path -> version (int)
        self._lock = threading.Lock()

    def next_version(self, file_path: str) -> int:
        """Atomically increment and return the next version for a file."""
        with self._lock:
            current = self._versions.get(file_path, 0)
            new_version = current + 1
            self._versions[file_path] = new_version
            return new_version

    def get_version(self, file_path: str) -> int:
        """Get the current version of a file."""
        with self._lock:
            return self._versions.get(file_path, 0)

    def set_version(self, file_path: str, version: int):
        """Set the version of a file (used when receiving replicas).

        Only updates if *version* is greater than the currently stored
        value so that versions never go backwards.
        """
        with self._lock:
            current = self._versions.get(file_path, 0)
            if version > current:
                self._versions[file_path] = version

    def read_repair(self, file_path: str, local_record: dict, cluster_nodes: list, self_node_id: str):
        """Compare local version with peers and repair stale replicas.

        Runs in a background thread.  Checks each peer's version of
        the file.  If a peer has a newer version, logs it.  If a peer
        has a stale version or is missing the file, pushes our version.
        """
        thread = threading.Thread(
            target=self._do_read_repair,
            args=(file_path, local_record, cluster_nodes, self_node_id),
            daemon=True,
        )
        thread.start()

    def _do_read_repair(self, file_path: str, local_record: dict, cluster_nodes: list, self_node_id: str):
        """Actual read-repair logic."""
        local_version = local_record.get("metadata", {}).get("version", 0)

        for node in cluster_nodes:
            if node["id"] == self_node_id:
                continue

            try:
                url = f"http://{node['host']}:{node['port']}/read/{file_path}"
                resp = requests.get(url, timeout=3)

                if resp.status_code == 200:
                    peer_record = resp.json()
                    peer_version = peer_record.get("metadata", {}).get("version", 0)

                    if peer_version > local_version:
                        # Peer has newer version — log it (actual pull would need file_store ref)
                        logger.info(
                            f"Read-repair: peer {node['id']} has newer version "
                            f"({peer_version} > {local_version}) for {file_path}"
                        )
                    elif peer_version < local_version:
                        # Push our version to stale peer
                        self._push_to_peer(
                            node, file_path,
                            local_record.get("data", {}),
                            local_record.get("metadata", {}),
                        )
                elif resp.status_code == 404:
                    # Peer doesn't have the file — push it
                    self._push_to_peer(
                        node, file_path,
                        local_record.get("data", {}),
                        local_record.get("metadata", {}),
                    )
            except requests.RequestException as e:
                logger.debug(f"Read-repair check failed for {node['id']}: {e}")

    def _push_to_peer(self, node: dict, file_path: str, data: dict, metadata: dict):
        """Push a file to a peer node via /replicate."""
        url = f"http://{node['host']}:{node['port']}/replicate"
        payload = {"file_path": file_path, "data": data, "metadata": metadata}
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 201:
                logger.info(f"Read-repair: pushed {file_path} to {node['id']}")
            else:
                logger.warning(f"Read-repair push to {node['id']} returned {resp.status_code}")
        except requests.RequestException as e:
            logger.warning(f"Read-repair push to {node['id']} failed: {e}")
