import time
import structlog
from src.merkle.tree import MerkleTree
from src.coordinator.client import NodeClient
from src.coordinator.strategies import resolve_conflict, Strategy

logger = structlog.get_logger()


class RepairTask:
    def __init__(self, key: str, target_node: NodeClient, value: str, version: int, timestamp: float, priority: int = 1):
        self.key = key
        self.target_node = target_node
        self.value = value
        self.version = version
        self.timestamp = timestamp
        self.priority = priority


class AntiEntropyScanner:
    def __init__(self, clients: list[NodeClient], strategy: Strategy = Strategy.LATEST_WRITE_WINS):
        self.clients = clients
        self.strategy = strategy

    def run_scan(self) -> list[RepairTask]:
        """Run pairwise comparison of all nodes. Returns list of RepairTasks."""
        repair_tasks = []
        all_diff_keys = set()

        # Step 1: Pairwise root hash comparison
        for i in range(len(self.clients)):
            for j in range(i + 1, len(self.clients)):
                client_a = self.clients[i]
                client_b = self.clients[j]

                root_a = client_a.get_merkle_root()
                root_b = client_b.get_merkle_root()

                if root_a is None or root_b is None:
                    logger.warning("scanner.node_unreachable", node_a=client_a.node_id, node_b=client_b.node_id)
                    continue

                if root_a == root_b:
                    logger.info("scanner.pair_consistent", node_a=client_a.node_id, node_b=client_b.node_id)
                    continue

                # Step 2: Leaf-level diff
                leaves_a = client_a.get_merkle_leaves()
                leaves_b = client_b.get_merkle_leaves()
                if leaves_a is None or leaves_b is None:
                    continue

                diff_keys = MerkleTree.diff_leaf_hashes(leaves_a, leaves_b)
                all_diff_keys.update(diff_keys)
                logger.info("scanner.inconsistency_detected", node_a=client_a.node_id, node_b=client_b.node_id, diff_keys=list(diff_keys))

        # Step 3: For each differing key, fetch from ALL nodes and resolve
        for key in all_diff_keys:
            entries = []
            node_entries = []  # (client, entry_dict_or_None)
            for client in self.clients:
                data = client.get_data(key)
                node_entries.append((client, data))
                if data is not None:
                    entries.append(data)

            if not entries:
                continue

            winner = resolve_conflict(entries, self.strategy)

            # Generate RepairTasks for nodes that don't have the winner
            for client, data in node_entries:
                needs_repair = False
                if data is None:
                    needs_repair = True
                elif data.get("value") != winner.get("value") or data.get("version") != winner.get("version"):
                    needs_repair = True
                if needs_repair:
                    repair_tasks.append(RepairTask(
                        key=key,
                        target_node=client,
                        value=winner["value"],
                        version=winner["version"],
                        timestamp=winner["timestamp"],
                    ))

        return repair_tasks
