"""Ingest engine — flatten, partition, write, and account for incoming entries.

:class:`IngestEngine` is the single entry point for landing a batch of log
entries for one tenant. It owns the end-to-end ingest flow and nothing else
(format *selection* and *migration* live in later commits); on each call it:

1. **Normalises + flattens** every entry into the shared flat-record shape the
   storage backends expect — ``{"ts": <float>, **fields}``. An entry's timestamp
   defaults to a single ``now`` captured at the start of the call when absent,
   so a whole batch lands deterministically in one logical instant.
2. **Groups** the flat rows by partition id, derived from each row's ``ts`` via
   :func:`~src.paths.partition_id_for` and the configured bucket width. A batch
   spanning a bucket boundary is therefore split and written to each partition.
3. **Writes** each partition's rows through *its current* backend — the format
   recorded in the manifest (defaulting to ROW for a brand-new partition), so an
   ingest never overrides a partition a migration has already moved to
   COLUMNAR/HYBRID.
4. **Records** the write atomically into the manifest
   (:meth:`~src.manifest.ManifestStore.apply_ingest`), advances the in-memory
   access pattern (:meth:`~src.pattern_tracker.PatternTracker.record_write`),
   and once per batch bumps the ingest-rate metric
   (:meth:`~src.metrics.Metrics.record_ingest`).

Flattening collision rule
-------------------------
The flat row is built as ``{"ts": ts, **fields}``. Because the explicit ``ts``
key is written **first** and ``**fields`` is spread after it, a field literally
named ``"ts"`` would override the entry's resolved timestamp. To preserve the
partitioning timestamp as authoritative, any ``"ts"`` inside ``fields`` is
dropped before the spread, so the **explicit entry ``ts`` always wins**.

Uncompressed-size estimate
--------------------------
The manifest tracks an *approximate* uncompressed byte size per partition to
derive the dashboard's compression ratio. Computing a true uncompressed size
would mean re-serialising every stored row, so this engine uses a cheap proxy:
the JSON byte length of just *this* batch's rows, **accumulated** onto whatever
estimate the partition already had. It is intentionally an over/under
approximation (compact ``json.dumps`` with no schema overhead), not an exact
figure — good enough to trend the ratio without re-reading the partition.

Determinism
-----------
A single ``now`` is sampled from the injected ``clock`` at the very start of
:meth:`IngestEngine.ingest` and reused for: filling missing entry timestamps,
the manifest ``last_write``, the pattern-tracker timestamp, and the metrics
event. Tests inject a fixed clock to get fully reproducible behaviour.
"""

from __future__ import annotations

import json
from typing import Any, Callable

import time

from .manifest import ManifestStore
from .metrics import Metrics
from .models import Format
from .paths import partition_id_for, partition_paths
from .pattern_tracker import PatternTracker
from .settings import Settings
from .storage.base import StorageBackend

__all__ = ["IngestEngine"]


class IngestEngine:
    """Lands a batch of entries for one tenant: flatten → partition → write → record.

    Construct one per process, wired with the shared manifest store, the
    per-:class:`~src.models.Format` backend map, the access-pattern tracker, the
    metrics aggregator, and runtime settings. The engine is otherwise stateless
    between calls (all durable state lives in the manifest / on disk), so it is
    safe to reuse across requests.
    """

    def __init__(
        self,
        *,
        manifest: ManifestStore,
        backends: dict[Format, StorageBackend],
        pattern_tracker: PatternTracker,
        metrics: Metrics,
        settings: Settings,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Initialise the engine with its collaborators.

        Args:
            manifest: The per-tenant manifest store (source of truth for a
                partition's current format/paths and the target of the atomic
                ingest counter update).
            backends: Map from :class:`~src.models.Format` to the
                :class:`~src.storage.base.StorageBackend` that materialises it.
                Must contain at least :attr:`Format.ROW` (the default for new
                partitions) and every format a partition might currently hold.
            pattern_tracker: In-memory access-pattern tracker; each write is
                recorded against the touched partition.
            metrics: Bounded metrics aggregator; the batch's row count is recorded
                once for the rolling ingest rate.
            settings: Runtime settings — only ``data_dir`` and
                ``partition_bucket_seconds`` are read here.
            clock: Injectable time source (seconds). Sampled once per
                :meth:`ingest` call for determinism; defaults to
                :func:`time.time`.
        """
        self._manifest = manifest
        self._backends = backends
        self._pattern_tracker = pattern_tracker
        self._metrics = metrics
        self._settings = settings
        self._clock = clock

    @staticmethod
    def _normalise(entry: Any, now_ts: float) -> tuple[float, dict[str, Any]]:
        """Return ``(ts, fields)`` for one entry, filling ``ts`` from ``now_ts``.

        Accepts either an :class:`~src.models.IngestEntry` (or any object exposing
        ``.ts``/``.fields`` attributes) or a plain ``dict`` carrying ``"ts"`` /
        ``"fields"`` keys. A missing or ``None`` timestamp falls back to
        ``now_ts`` so the row partitions deterministically.

        Args:
            entry: The raw ingest item (pydantic model or dict).
            now_ts: The batch-wide timestamp used when the entry omits ``ts``.

        Returns:
            ``(ts, fields)`` where ``ts`` is a float and ``fields`` a dict (empty
            when absent).
        """
        if isinstance(entry, dict):
            ts = entry.get("ts")
            fields = entry.get("fields") or {}
        else:
            ts = getattr(entry, "ts", None)
            fields = getattr(entry, "fields", None) or {}
        if ts is None:
            ts = now_ts
        return float(ts), dict(fields)

    @staticmethod
    def _flatten(ts: float, fields: dict[str, Any]) -> dict[str, Any]:
        """Build the flat storage row ``{"ts": ts, **fields}``.

        The explicit ``ts`` is authoritative: any ``"ts"`` key inside ``fields``
        is discarded so it cannot shadow the partitioning timestamp (see the
        module docstring's collision rule).

        Args:
            ts: The resolved entry timestamp.
            fields: The entry's arbitrary fields.

        Returns:
            A flat record dict with ``ts`` first, then the (ts-stripped) fields.
        """
        if "ts" in fields:
            # Explicit entry ts wins over a field literally named "ts".
            fields = {k: v for k, v in fields.items() if k != "ts"}
        return {"ts": ts, **fields}

    async def ingest(self, tenant: str, entries: list) -> dict:
        """Land ``entries`` for ``tenant``: flatten, partition, write, and record.

        See the module docstring for the full flow and the collision /
        estimate / determinism rules. The whole batch shares one ``now_ts``
        sampled at entry.

        An **empty** ``entries`` list is a no-op: nothing is written, no metric
        is recorded, and ``{"ingested": 0, "partitions_touched": [], "tenant":
        tenant}`` is returned.

        Args:
            tenant: Tenant the batch belongs to.
            entries: Items to ingest — each an :class:`~src.models.IngestEntry`
                (or compatible object) or a plain dict with ``ts``/``fields``.

        Returns:
            A summary dict ``{"ingested": <int total rows>, "partitions_touched":
            <sorted list of partition ids>, "tenant": <tenant>}``.
        """
        now_ts = self._clock()

        if not entries:
            return {"ingested": 0, "partitions_touched": [], "tenant": tenant}

        bucket_seconds = self._settings.partition_bucket_seconds

        # Group flattened rows by their target partition id.
        rows_by_pid: dict[str, list[dict[str, Any]]] = {}
        for entry in entries:
            ts, fields = self._normalise(entry, now_ts)
            row = self._flatten(ts, fields)
            pid = partition_id_for(ts, bucket_seconds)
            rows_by_pid.setdefault(pid, []).append(row)

        total_rows = 0
        touched_pids: list[str] = []

        # Write each partition through its current backend, then record the write.
        for pid, rows in rows_by_pid.items():
            paths = partition_paths(self._settings.data_dir, tenant, pid)

            # Resolve the partition's current format (default ROW for new ones),
            # and read its prior uncompressed estimate so we can accumulate onto
            # it. Both come from the same sync manifest read.
            meta = self._manifest.get_partition(tenant, pid)
            fmt = meta.format if meta else Format.ROW
            prior_uncompressed = meta.uncompressed_estimate_bytes if meta else 0
            backend = self._backends[fmt]

            # Append this partition's rows via its backend.
            backend.write(rows, paths)

            # Cheap uncompressed-size proxy: JSON byte length of this batch's
            # rows, accumulated onto the partition's existing estimate.
            batch_json_bytes = sum(len(json.dumps(r)) for r in rows)
            new_uncompressed = prior_uncompressed + batch_json_bytes

            # Atomically fold the write into the manifest (row_count accumulates;
            # size/last_write/estimate are set). For a new partition this also
            # creates the record as ROW with the canonical row path.
            await self._manifest.apply_ingest(
                tenant,
                pid,
                row_count_delta=len(rows),
                size_bytes=backend.size_bytes(paths),
                last_write=now_ts,
                paths={"row": f"{pid}/row.jsonl.lz4"},
                create_format=Format.ROW,
                uncompressed_estimate_bytes=new_uncompressed,
            )

            # Advance the in-memory access pattern. Columns = union of all field
            # keys seen in this partition's rows, including "ts".
            columns: set[str] = set()
            for r in rows:
                columns.update(r.keys())
            self._pattern_tracker.record_write(
                tenant, pid, columns=columns, ts=now_ts
            )

            total_rows += len(rows)
            touched_pids.append(pid)

        # One ingest-rate sample for the whole batch.
        self._metrics.record_ingest(total_rows, ts=now_ts)

        return {
            "ingested": total_rows,
            "partitions_touched": sorted(touched_pids),
            "tenant": tenant,
        }
