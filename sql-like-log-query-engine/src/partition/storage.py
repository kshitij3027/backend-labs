from __future__ import annotations

from typing import Iterable


# Fields for which we materialise equality-hash indexes. A field not in this
# set simply means "no index" — the executor falls back to a full scan.
_HASH_INDEXED_FIELDS: frozenset[str] = frozenset({"level", "service"})


class LogStorage:
    """In-memory log store with a pair of equality indexes + a sorted-ts index.

    The design keeps three complementary structures:

    1. ``_rows`` — the original record list, as handed in.
    2. ``_hash_indexes[field] = {value: {row_idx, ...}}`` — one inverted
       index per hash-indexed field (``level``, ``service``). Built lazily:
       the dict only has keys for fields actually requested via
       ``indexed_fields``.
    3. ``_ts_sorted = [(ts, row_idx), ...]`` sorted on ``ts`` for bisect-based
       range scans. Built only when ``timestamp`` is in ``indexed_fields``.

    This keeps the equality hot paths O(1) per value and the timestamp range
    scan O(log N + k), which is exactly what the local executor needs when
    the planner pushes a ``level = 'ERROR' AND timestamp BETWEEN ...`` filter
    down.
    """

    def __init__(self, records: list[dict], indexed_fields: list[str]) -> None:
        self._rows: list[dict] = records
        self._indexed_fields: list[str] = list(indexed_fields)

        self._hash_indexes: dict[str, dict[object, set[int]]] = {}
        for field in indexed_fields:
            if field in _HASH_INDEXED_FIELDS:
                idx: dict[object, set[int]] = {}
                for i, row in enumerate(records):
                    val = row.get(field)
                    idx.setdefault(val, set()).add(i)
                self._hash_indexes[field] = idx

        # Sorted-ts index for O(log N) range scans. Always built when the
        # caller lists ``timestamp`` as indexed, which matches the compose
        # file for every partition.
        self._ts_sorted: list[tuple[str, int]] = []
        self._has_ts_index: bool = "timestamp" in indexed_fields
        if self._has_ts_index:
            # Records are typically already sorted by generate_logs(), but we
            # sort again defensively — the index must be correct regardless.
            self._ts_sorted = sorted(
                ((row["timestamp"], i) for i, row in enumerate(records)),
                key=lambda pair: pair[0],
            )

    # --- accessors ------------------------------------------------------

    def rows(self) -> list[dict]:
        return self._rows

    @property
    def indexed_fields(self) -> list[str]:
        return list(self._indexed_fields)

    def has_hash_index(self, field: str) -> bool:
        return field in self._hash_indexes

    def has_ts_index(self) -> bool:
        return self._has_ts_index

    # --- lookups --------------------------------------------------------

    def filter_by_level(self, level: str) -> set[int]:
        """Return row indices whose ``level`` equals ``level``.

        Falls back to a linear scan if the index wasn't built.
        """

        idx = self._hash_indexes.get("level")
        if idx is not None:
            return set(idx.get(level, set()))
        return {i for i, row in enumerate(self._rows) if row.get("level") == level}

    def filter_by_service(self, service: str) -> set[int]:
        """Return row indices whose ``service`` equals ``service``."""

        idx = self._hash_indexes.get("service")
        if idx is not None:
            return set(idx.get(service, set()))
        return {
            i for i, row in enumerate(self._rows) if row.get("service") == service
        }

    def filter_by_timestamp_range(
        self, low: str | None, high: str | None
    ) -> set[int]:
        """Return indices whose ``timestamp`` falls in ``[low, high]`` (inclusive).

        ``None`` on either side means unbounded on that side. Falls back to a
        linear scan if the timestamp index wasn't built.
        """

        if self._has_ts_index:
            # Use explicit positional bisect so tuple-level tie-breaking on
            # row-index can't skew the boundary.
            lo_idx = self._bisect_left_ts(low) if low is not None else 0
            hi_idx = (
                self._bisect_right_ts(high)
                if high is not None
                else len(self._ts_sorted)
            )
            return {pair[1] for pair in self._ts_sorted[lo_idx:hi_idx]}

        # Fallback: linear scan.
        result: set[int] = set()
        for i, row in enumerate(self._rows):
            ts = row.get("timestamp")
            if ts is None:
                continue
            if low is not None and ts < low:
                continue
            if high is not None and ts > high:
                continue
            result.add(i)
        return result

    # --- internal bisect helpers ---------------------------------------

    def _bisect_left_ts(self, ts: str) -> int:
        """Return the insertion index for ``ts`` preserving ascending order
        on the ts field of ``_ts_sorted``. First index i where
        ``_ts_sorted[i][0] >= ts``.
        """

        lo, hi = 0, len(self._ts_sorted)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._ts_sorted[mid][0] < ts:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _bisect_right_ts(self, ts: str) -> int:
        """First index i where ``_ts_sorted[i][0] > ts``."""

        lo, hi = 0, len(self._ts_sorted)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._ts_sorted[mid][0] <= ts:
                lo = mid + 1
            else:
                hi = mid
        return lo

    # --- bulk ----------------------------------------------------------

    def rows_at(self, indices: Iterable[int]) -> list[dict]:
        """Materialise rows for a collection of indices (order preserved)."""

        return [self._rows[i] for i in indices]
