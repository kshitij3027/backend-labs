from src.models import LogEntry


class LogStorage:
    def __init__(self):
        self.entries: list[LogEntry] = []
        self.indexes: dict[str, dict[str, list[int]]] = {}

    def load(self, entries: list[LogEntry]) -> None:
        """Load entries and build indexes."""
        self.entries = entries
        self.indexes = {}
        self._build_indexes()

    def _build_indexes(self) -> None:
        """Build indexes on level and service fields."""
        for field_name in ("level", "service"):
            self.indexes[field_name] = {}
            for i, entry in enumerate(self.entries):
                value = getattr(entry, field_name)
                if value not in self.indexes[field_name]:
                    self.indexes[field_name][value] = []
                self.indexes[field_name][value].append(i)

    def get_by_index(self, field: str, value: str) -> list[LogEntry]:
        """Get entries matching a field value using index."""
        if field in self.indexes and value in self.indexes[field]:
            return [self.entries[i] for i in self.indexes[field][value]]
        return []

    def get_all(self) -> list[LogEntry]:
        return list(self.entries)

    @property
    def count(self) -> int:
        return len(self.entries)

    @property
    def time_range(self):
        """Return (min_timestamp, max_timestamp) or None if empty."""
        if not self.entries:
            return None
        # entries are sorted desc, so last is oldest, first is newest
        return (self.entries[-1].timestamp, self.entries[0].timestamp)
