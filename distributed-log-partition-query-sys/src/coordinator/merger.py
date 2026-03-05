import heapq
from functools import total_ordering

from src.models import LogEntry


@total_ordering
class _ComparableEntry:
    """Wrapper to make LogEntry comparable for heapq."""

    __slots__ = ("entry", "sort_key", "reverse", "tiebreaker")

    def __init__(
        self, entry: LogEntry, sort_field: str, reverse: bool, tiebreaker: int
    ):
        self.entry = entry
        self.sort_key = getattr(entry, sort_field)
        self.reverse = reverse
        self.tiebreaker = tiebreaker

    def __eq__(self, other):
        return self.sort_key == other.sort_key and self.tiebreaker == other.tiebreaker

    def __lt__(self, other):
        if self.reverse:
            # For desc: higher values should come first (be "less than" in heap)
            if self.sort_key != other.sort_key:
                return self.sort_key > other.sort_key
            return self.tiebreaker < other.tiebreaker
        else:
            if self.sort_key != other.sort_key:
                return self.sort_key < other.sort_key
            return self.tiebreaker < other.tiebreaker


class ResultMerger:
    def __init__(self, max_merge_size: int = 10000):
        self.max_merge_size = max_merge_size

    def merge(
        self,
        partition_results: list[list[LogEntry]],
        sort_field: str = "timestamp",
        sort_order: str = "desc",
        limit: int | None = None,
    ) -> list[LogEntry]:
        """Merge pre-sorted results from multiple partitions using heap-based merge.

        Each partition's results are assumed to be already sorted in the requested order.
        Uses heapq for O(n log k) merging where k = number of partitions.
        Implements early termination when limit is reached.
        """
        reverse = sort_order == "desc"

        # Build heap entries with tiebreakers for stability
        heap: list[_ComparableEntry] = []
        iter_map: dict[int, iter] = {}

        tiebreaker = 0
        for partition_idx, results in enumerate(partition_results):
            if not results:
                continue
            it = iter(results)
            first = next(it, None)
            if first is not None:
                heapq.heappush(
                    heap,
                    _ComparableEntry(first, sort_field, reverse, tiebreaker),
                )
                iter_map[tiebreaker] = it
                tiebreaker += 1

        merged: list[LogEntry] = []
        effective_limit = limit if limit else self.max_merge_size

        while heap and len(merged) < effective_limit:
            smallest = heapq.heappop(heap)
            merged.append(smallest.entry)

            # Get next from same partition's iterator
            it = iter_map.get(smallest.tiebreaker)
            if it is not None:
                next_entry = next(it, None)
                if next_entry is not None:
                    tiebreaker += 1
                    new_comparable = _ComparableEntry(
                        next_entry, sort_field, reverse, tiebreaker
                    )
                    # Keep same iterator mapping
                    iter_map[tiebreaker] = it
                    heapq.heappush(heap, new_comparable)

        return merged

    def merge_paginated(
        self,
        partition_results: list[list[LogEntry]],
        sort_field: str = "timestamp",
        sort_order: str = "desc",
        page: int = 1,
        page_size: int = 50,
    ) -> tuple[list[LogEntry], int]:
        """Merge results with pagination. Returns (page_results, total_merged_count).

        Merges all results first (up to max_merge_size), then slices for the requested page.
        """
        # Merge all (up to max_merge_size)
        all_merged = self.merge(
            partition_results,
            sort_field=sort_field,
            sort_order=sort_order,
            limit=None,  # merge all, let max_merge_size cap it
        )

        total = len(all_merged)
        start = (page - 1) * page_size
        end = start + page_size
        page_results = all_merged[start:end]

        return page_results, total
