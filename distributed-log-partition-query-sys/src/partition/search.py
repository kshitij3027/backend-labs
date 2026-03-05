from datetime import datetime
from src.models import LogEntry, Query
from src.partition.storage import LogStorage


class LogSearchEngine:
    @staticmethod
    def search(storage: LogStorage, query: Query) -> list[LogEntry]:
        """Search storage applying filters, time range, sort, and limit."""
        # Start with indexed lookup if we have an eq filter on indexed field
        candidates = None
        remaining_filters = list(query.filters)

        for f in query.filters:
            if f.operator == "eq" and f.field in storage.indexes:
                indexed_results = storage.get_by_index(f.field, f.value)
                if candidates is None:
                    candidates = indexed_results
                else:
                    indexed_set = set(id(e) for e in indexed_results)
                    candidates = [e for e in candidates if id(e) in indexed_set]
                remaining_filters.remove(f)

        if candidates is None:
            candidates = storage.get_all()

        # Apply time range filter
        if query.time_range:
            start = query.time_range.start
            end = query.time_range.end
            candidates = [e for e in candidates if start <= e.timestamp <= end]

        # Apply remaining filters
        for f in remaining_filters:
            if f.operator == "eq":
                candidates = [e for e in candidates if getattr(e, f.field, None) == f.value]
            elif f.operator == "contains":
                candidates = [e for e in candidates if f.value.lower() in str(getattr(e, f.field, "")).lower()]

        # Sort
        reverse = query.sort_order == "desc"
        candidates.sort(key=lambda e: getattr(e, query.sort_field), reverse=reverse)

        # Limit
        if query.limit:
            candidates = candidates[:query.limit]

        return candidates
