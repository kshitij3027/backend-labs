import heapq
import asyncio
from typing import AsyncGenerator
from functools import total_ordering

from src.models import LogEntry


@total_ordering
class _StreamComparable:
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
            if self.sort_key != other.sort_key:
                return self.sort_key > other.sort_key
            return self.tiebreaker < other.tiebreaker
        else:
            if self.sort_key != other.sort_key:
                return self.sort_key < other.sort_key
            return self.tiebreaker < other.tiebreaker


class StreamingMerger:
    def __init__(self, max_merge_size: int = 10000):
        self.max_merge_size = max_merge_size

    async def merge_stream(
        self,
        partition_results: list[list[LogEntry]],
        sort_field: str = "timestamp",
        sort_order: str = "desc",
        limit: int | None = None,
    ) -> AsyncGenerator[LogEntry, None]:
        """Yield merged results one at a time as an async generator.

        Implements backpressure -- the consumer controls the pace by
        awaiting the next item.
        """
        reverse = sort_order == "desc"

        heap: list[_StreamComparable] = []
        iter_map: dict[int, iter] = {}
        tiebreaker = 0

        for results in partition_results:
            if not results:
                continue
            it = iter(results)
            first = next(it, None)
            if first is not None:
                heapq.heappush(
                    heap,
                    _StreamComparable(first, sort_field, reverse, tiebreaker),
                )
                iter_map[tiebreaker] = it
                tiebreaker += 1

        effective_limit = limit if limit else self.max_merge_size
        count = 0

        while heap and count < effective_limit:
            smallest = heapq.heappop(heap)
            yield smallest.entry
            count += 1

            it = iter_map.get(smallest.tiebreaker)
            if it is not None:
                next_entry = next(it, None)
                if next_entry is not None:
                    tiebreaker += 1
                    iter_map[tiebreaker] = it
                    heapq.heappush(
                        heap,
                        _StreamComparable(
                            next_entry, sort_field, reverse, tiebreaker
                        ),
                    )

            # Yield control for backpressure
            if count % 10 == 0:
                await asyncio.sleep(0)
