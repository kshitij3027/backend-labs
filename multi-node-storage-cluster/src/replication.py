"""Asynchronous replication manager with hinted handoff.

After a local write succeeds, the ReplicationManager sends the file to
peer nodes determined by the consistent hash ring.  Failed replications
are queued for hinted handoff and retried periodically in a background
thread.
"""

import logging
import threading
import time
from collections import deque

import requests

from src.consistent_hash import HashRing

logger = logging.getLogger(__name__)


class ReplicationManager:
    """Manages async replication of files to peer nodes.

    After a local write, replicate() is called to send the file to
    peer nodes determined by the hash ring. Failed replications are
    queued for hinted handoff and retried periodically.
    """

    def __init__(self, config, hash_ring: HashRing):
        self.config = config
        self.hash_ring = hash_ring
        self._hint_queue = deque(maxlen=10000)
        self._lock = threading.Lock()
        self._stats = {
            "replications_sent": 0,
            "replications_failed": 0,
            "hints_queued": 0,
            "hints_replayed": 0,
        }
        self._running = True

        # Start hinted handoff replay thread
        self._replay_thread = threading.Thread(
            target=self._replay_hints_loop,
            daemon=True,
            name="hint-replay",
        )
        self._replay_thread.start()

    def replicate(self, file_path: str, data: dict, metadata: dict):
        """Replicate a file to peer nodes in a background thread.

        Uses the hash ring to determine which nodes should hold the file,
        excludes self, and sends to up to (replication_factor - 1) peers.
        """
        thread = threading.Thread(
            target=self._do_replicate,
            args=(file_path, data, metadata),
            daemon=True,
        )
        thread.start()

    def _do_replicate(self, file_path: str, data: dict, metadata: dict):
        """Actual replication logic — runs in background thread."""
        target_nodes = self.hash_ring.get_nodes(
            file_path, self.config.replication_factor
        )

        # Build node address map from config
        node_map = {n["id"]: n for n in self.config.cluster_nodes}

        for node_id in target_nodes:
            if node_id == self.config.node_id:
                continue  # skip self

            node_info = node_map.get(node_id)
            if not node_info:
                continue

            success = self._send_replica(node_info, file_path, data, metadata)
            if not success:
                self._enqueue_hint(node_info, file_path, data, metadata)

    def _send_replica(
        self, node_info: dict, file_path: str, data: dict, metadata: dict
    ) -> bool:
        """Send a single replica to a target node. Returns True on success."""
        url = f"http://{node_info['host']}:{node_info['port']}/replicate"
        payload = {
            "file_path": file_path,
            "data": data,
            "metadata": metadata,
        }
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 201:
                with self._lock:
                    self._stats["replications_sent"] += 1
                logger.info(f"Replicated {file_path} to {node_info['id']}")
                return True
            else:
                logger.warning(
                    f"Replication to {node_info['id']} returned {resp.status_code}"
                )
                with self._lock:
                    self._stats["replications_failed"] += 1
                return False
        except requests.RequestException as e:
            logger.warning(f"Replication to {node_info['id']} failed: {e}")
            with self._lock:
                self._stats["replications_failed"] += 1
            return False

    def _enqueue_hint(
        self, node_info: dict, file_path: str, data: dict, metadata: dict
    ):
        """Queue a failed replication for later retry."""
        hint = {
            "node_info": node_info,
            "file_path": file_path,
            "data": data,
            "metadata": metadata,
            "queued_at": time.time(),
        }
        with self._lock:
            self._hint_queue.append(hint)
            self._stats["hints_queued"] += 1
        logger.info(f"Queued hint for {node_info['id']}: {file_path}")

    def _replay_hints_loop(self):
        """Periodically retry failed replications from the hint queue."""
        while self._running:
            time.sleep(30)
            self._replay_hints()

    def _replay_hints(self):
        """Process all pending hints, re-queuing any that still fail."""
        with self._lock:
            pending = list(self._hint_queue)
            self._hint_queue.clear()

        for hint in pending:
            success = self._send_replica(
                hint["node_info"],
                hint["file_path"],
                hint["data"],
                hint["metadata"],
            )
            if success:
                with self._lock:
                    self._stats["hints_replayed"] += 1
            else:
                # Re-queue if still failing
                with self._lock:
                    self._hint_queue.append(hint)

    def get_stats(self) -> dict:
        """Return replication statistics."""
        with self._lock:
            stats = dict(self._stats)
            stats["hints_pending"] = len(self._hint_queue)
        return stats

    def shutdown(self):
        """Stop the replay thread."""
        self._running = False
