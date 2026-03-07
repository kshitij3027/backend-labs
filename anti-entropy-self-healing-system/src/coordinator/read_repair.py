import structlog
from src.coordinator.client import NodeClient
from src.coordinator.strategies import resolve_conflict, Strategy

logger = structlog.get_logger()


class ReadRepairHandler:
    def __init__(self, clients: list[NodeClient], strategy: Strategy = Strategy.LATEST_WRITE_WINS):
        self.clients = clients
        self.strategy = strategy

    def read_with_repair(self, key: str) -> dict | None:
        """Read key from ALL nodes, compare, repair stale ones, return winner.
        Returns the winning entry dict or None if key not found on any node."""
        entries = []
        node_entries = []  # (client, entry_dict_or_None)

        for client in self.clients:
            data = client.get_data(key)
            node_entries.append((client, data))
            if data is not None:
                entries.append(data)

        if not entries:
            return None

        # If all entries identical, no repair needed
        values = set(e.get("value") for e in entries)
        if len(values) == 1 and len(entries) == len(self.clients):
            return entries[0]

        # Resolve conflict
        winner = resolve_conflict(entries, self.strategy)
        repaired = False

        # Repair stale nodes
        for client, data in node_entries:
            needs_repair = False
            if data is None:
                needs_repair = True
            elif data.get("value") != winner.get("value") or data.get("version") != winner.get("version"):
                needs_repair = True
            if needs_repair:
                success = client.put_data(
                    key=key,
                    value=winner["value"],
                    version=winner["version"],
                    timestamp=winner["timestamp"],
                )
                if success:
                    repaired = True
                    logger.info("read_repair.repaired", key=key, target=client.node_id)

        winner["read_repaired"] = repaired
        return winner
