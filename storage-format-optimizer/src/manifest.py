"""Per-tenant JSON manifest — the on-disk source of truth.

Each tenant owns one ``manifest.json`` describing every partition it holds:
the partition's current storage :class:`~src.models.Format`, its
:class:`~src.models.Tier`, the relative paths to its data files, row/byte
counters, the per-column codecs in use, any built index, the learned access
pattern, and a record of its last migration.

:class:`ManifestStore` is the only component that reads or writes these files.
It keeps an **in-memory cache** of every loaded :class:`TenantManifest` so the
hot read path (query routing) never hits the disk twice, and it persists
updates **atomically**:

    serialize whole manifest -> manifest.json.new -> flush + fsync ->
    os.replace(manifest.json.new, manifest.json)

``os.replace`` is atomic on a single filesystem, and the staging file lives in
the same directory as the live manifest (see :func:`src.paths.manifest_tmp_path`),
so a reader never observes a half-written file: it sees either the old manifest
or the new one, never a torn mix.

Concurrency model
-----------------
* **Sync readers** (:meth:`~ManifestStore.load`,
  :meth:`~ManifestStore.get_partition`, :meth:`~ManifestStore.list_partitions`,
  :meth:`~ManifestStore.all_tenants`) take no lock. They read the cached dict,
  transparently reloading from disk when the file's mtime has changed since the
  last load. The cache is only swapped *after* a successful disk write, so the
  object a reader holds is always internally consistent.
* **Async mutators** (:meth:`~ManifestStore.upsert_partition`,
  :meth:`~ManifestStore.swap_format`, :meth:`~ManifestStore.record_access`,
  :meth:`~ManifestStore.update_partition_stats`) each acquire that tenant's
  :class:`asyncio.Lock`, mutate the in-memory manifest, bump ``version``, stamp
  ``updated_at``, and then persist atomically. The per-tenant lock serializes
  concurrent mutators (e.g. an ingest counter bump racing a migration's format
  swap) so neither clobbers the other.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from src.models import Format, Tier
from src.paths import (
    MANIFEST_NAME,
    ensure_dir,
    manifest_path,
    manifest_tmp_path,
    tenant_dir,
)

__all__ = ["PartitionMeta", "TenantManifest", "ManifestStore"]


@dataclass
class PartitionMeta:
    """Manifest record for a single partition.

    Holds everything the engine needs to route a read or decide a migration
    without touching the data files: the active :class:`Format`/:class:`Tier`,
    the logical-name -> relative-path map, size/row counters, per-column codecs,
    any built index, the learned access pattern, and the last migration record.
    """

    partition_id: str
    format: Format = Format.ROW
    tier: Tier = Tier.HOT
    # Logical name -> path relative to the data dir, e.g. {"row": "p_5/row.jsonl.lz4"}.
    paths: dict[str, str] = field(default_factory=dict)
    row_count: int = 0
    size_bytes: int = 0
    uncompressed_estimate_bytes: int = 0
    # Column -> codec actually used on disk, e.g. {"status": "ZSTD"}.
    codecs: dict[str, str] = field(default_factory=dict)
    # Built index: {"columns": [...], "stats": {col: {"min": .., "max": ..}}}.
    index: dict[str, Any] = field(default_factory=dict)
    # Learned access pattern, the access{} shape from PatternTracker.to_dict().
    access: dict[str, Any] = field(default_factory=dict)
    # Record of the most recent format migration, or None if never migrated.
    last_migration: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dict (enums become their ``.value``)."""
        return {
            "partition_id": self.partition_id,
            "format": self.format.value,
            "tier": self.tier.value,
            "paths": dict(self.paths),
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "uncompressed_estimate_bytes": self.uncompressed_estimate_bytes,
            "codecs": dict(self.codecs),
            "index": dict(self.index),
            "access": dict(self.access),
            "last_migration": self.last_migration,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PartitionMeta":
        """Rebuild from a dict, coercing enum strings and tolerating missing keys.

        ``format``/``tier`` are coerced back from their string ``.value`` to the
        :class:`Format`/:class:`Tier` enums. Any absent key falls back to the
        dataclass default, so an older or partial manifest still loads cleanly.
        """
        return cls(
            partition_id=data["partition_id"],
            format=Format(data.get("format", Format.ROW.value)),
            tier=Tier(data.get("tier", Tier.HOT.value)),
            paths=dict(data.get("paths", {})),
            row_count=data.get("row_count", 0),
            size_bytes=data.get("size_bytes", 0),
            uncompressed_estimate_bytes=data.get("uncompressed_estimate_bytes", 0),
            codecs=dict(data.get("codecs", {})),
            index=dict(data.get("index", {})),
            access=dict(data.get("access", {})),
            last_migration=data.get("last_migration"),
        )


@dataclass
class TenantManifest:
    """The full manifest for one tenant: a monotonically-versioned partition map.

    ``version`` is bumped on every successful mutation and ``updated_at`` records
    the wall-clock time of that mutation, giving readers a cheap way to detect
    staleness. ``partitions`` is keyed by ``partition_id``.
    """

    tenant: str
    version: int = 0
    updated_at: float = 0.0
    partitions: dict[str, PartitionMeta] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dict (partitions keyed by id)."""
        return {
            "tenant": self.tenant,
            "version": self.version,
            "updated_at": self.updated_at,
            "partitions": {
                pid: meta.to_dict() for pid, meta in self.partitions.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TenantManifest":
        """Rebuild from a dict, tolerating missing keys via dataclass defaults."""
        partitions_raw = data.get("partitions", {})
        return cls(
            tenant=data["tenant"],
            version=data.get("version", 0),
            updated_at=data.get("updated_at", 0.0),
            partitions={
                pid: PartitionMeta.from_dict(meta)
                for pid, meta in partitions_raw.items()
            },
        )


class ManifestStore:
    """Cached, atomically-persisted store of per-tenant manifests.

    Construct one per process, pointed at the data directory. Sync readers serve
    from the in-memory cache (reloading on mtime change); async mutators take the
    per-tenant lock, edit in memory, then persist via ``os.replace``.
    """

    def __init__(
        self, data_dir: str | Path, *, clock: Callable[[], float] = time.time
    ) -> None:
        """Initialize the store.

        :param data_dir: Root directory holding ``<tenant>/manifest.json`` trees.
        :param clock: Injectable time source (seconds) used to stamp
            ``updated_at`` and migration timestamps. Defaults to :func:`time.time`;
            override it in tests for determinism.
        """
        self._data_dir = Path(data_dir)
        self._clock = clock
        # tenant -> cached manifest (swapped wholesale only after a good write).
        self._cache: dict[str, TenantManifest] = {}
        # tenant -> mtime of the manifest file the cache was last loaded from.
        self._mtime: dict[str, float] = {}
        # tenant -> lazily-created lock serializing that tenant's mutators.
        self._locks: dict[str, asyncio.Lock] = {}

    # ------------------------------------------------------------------ #
    # Sync readers (no lock — read the cache, reload on mtime change)     #
    # ------------------------------------------------------------------ #
    def load(self, tenant: str) -> TenantManifest:
        """Return ``tenant``'s manifest, reloading from disk if it changed.

        If a ``manifest.json`` exists and its mtime differs from what the cache
        was loaded at, it is re-parsed into the cache. If no file exists, an
        empty ``TenantManifest(tenant, version=0)`` is cached and returned.
        Never raises on a missing file.
        """
        path = manifest_path(self._data_dir, tenant)
        try:
            current_mtime = path.stat().st_mtime
        except FileNotFoundError:
            # No manifest on disk yet — serve (and remember) an empty one.
            if tenant not in self._cache:
                self._cache[tenant] = TenantManifest(tenant=tenant, version=0)
            return self._cache[tenant]

        cached = self._cache.get(tenant)
        if cached is not None and self._mtime.get(tenant) == current_mtime:
            return cached

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        manifest = TenantManifest.from_dict(data)
        self._cache[tenant] = manifest
        self._mtime[tenant] = current_mtime
        return manifest

    def get_partition(
        self, tenant: str, partition_id: str
    ) -> PartitionMeta | None:
        """Return one partition's metadata, or ``None`` if it doesn't exist."""
        return self.load(tenant).partitions.get(partition_id)

    def list_partitions(self, tenant: str) -> list[PartitionMeta]:
        """Return all of ``tenant``'s partitions as a list."""
        return list(self.load(tenant).partitions.values())

    def all_tenants(self) -> list[str]:
        """Return every tenant that has a ``manifest.json`` on disk.

        Scans the immediate subdirectories of the data dir. Tolerates a missing
        data dir (returns ``[]``).
        """
        try:
            entries = list(self._data_dir.iterdir())
        except FileNotFoundError:
            return []
        tenants: list[str] = []
        for entry in entries:
            if entry.is_dir() and (entry / MANIFEST_NAME).exists():
                tenants.append(entry.name)
        return tenants

    # ------------------------------------------------------------------ #
    # Async mutators (lock -> mutate in memory -> bump version -> persist)#
    # ------------------------------------------------------------------ #
    async def upsert_partition(self, tenant: str, meta: PartitionMeta) -> None:
        """Insert or replace a partition record, then persist atomically."""
        async with self._lock(tenant):
            manifest = self.load(tenant)
            manifest.partitions[meta.partition_id] = meta
            self._bump_and_persist(tenant, manifest)

    async def swap_format(
        self,
        tenant: str,
        partition_id: str,
        *,
        new_format: Format,
        new_paths: dict[str, str],
        new_codecs: dict[str, str],
        new_size: int,
        new_index: dict[str, Any] | None = None,
        uncompressed_estimate_bytes: int | None = None,
        reason: str | None = None,
        from_format: Format | None = None,
    ) -> None:
        """Flip a partition to a freshly-written format and record the migration.

        Replaces the partition's ``format``, ``paths``, ``codecs`` and
        ``size_bytes``, and (when provided) its ``index`` and
        ``uncompressed_estimate_bytes``. Stamps ``last_migration`` with the
        clock time, the prior format (or ``from_format`` if given), the new
        format, and the optional ``reason``.

        :raises KeyError: if ``partition_id`` is not present in the manifest.
        """
        async with self._lock(tenant):
            manifest = self.load(tenant)
            meta = manifest.partitions.get(partition_id)
            if meta is None:
                raise KeyError(
                    f"partition {partition_id!r} not found for tenant {tenant!r}"
                )
            old_format = meta.format
            meta.format = new_format
            meta.paths = dict(new_paths)
            meta.codecs = dict(new_codecs)
            meta.size_bytes = new_size
            if new_index is not None:
                meta.index = dict(new_index)
            if uncompressed_estimate_bytes is not None:
                meta.uncompressed_estimate_bytes = uncompressed_estimate_bytes
            meta.last_migration = {
                "at": self._clock(),
                "from": (from_format.value if from_format else old_format.value),
                "to": new_format.value,
                "reason": reason,
            }
            self._bump_and_persist(tenant, manifest)

    async def record_access(
        self, tenant: str, partition_id: str, access: dict[str, Any]
    ) -> None:
        """Replace a partition's learned ``access`` dict, then persist.

        Used sparingly by the migration evaluator (which snapshots the live
        :class:`~src.pattern_tracker.PatternTracker` state), **not** on the
        per-query hot path. No-op if the partition is absent.
        """
        async with self._lock(tenant):
            manifest = self.load(tenant)
            meta = manifest.partitions.get(partition_id)
            if meta is None:
                return
            meta.access = dict(access)
            self._bump_and_persist(tenant, manifest)

    async def update_partition_stats(
        self,
        tenant: str,
        partition_id: str,
        *,
        row_count: int | None = None,
        size_bytes: int | None = None,
        uncompressed_estimate_bytes: int | None = None,
        last_write: float | None = None,
    ) -> None:
        """Bump a partition's counters from ingest, then persist.

        Each argument is applied only when not ``None``. ``last_write`` is stored
        inside the ``access`` dict (created if needed) rather than as a top-level
        field. No-op if the partition is absent.
        """
        async with self._lock(tenant):
            manifest = self.load(tenant)
            meta = manifest.partitions.get(partition_id)
            if meta is None:
                return
            if row_count is not None:
                meta.row_count = row_count
            if size_bytes is not None:
                meta.size_bytes = size_bytes
            if uncompressed_estimate_bytes is not None:
                meta.uncompressed_estimate_bytes = uncompressed_estimate_bytes
            if last_write is not None:
                meta.access["last_write"] = last_write
            self._bump_and_persist(tenant, manifest)

    # ------------------------------------------------------------------ #
    # Internal helpers                                                    #
    # ------------------------------------------------------------------ #
    def _bump_and_persist(self, tenant: str, manifest: TenantManifest) -> None:
        """Bump version + ``updated_at`` and atomically write the manifest."""
        manifest.version += 1
        manifest.updated_at = self._clock()
        self._persist(tenant, manifest)

    def _persist(self, tenant: str, manifest: TenantManifest) -> None:
        """Atomically write ``manifest`` to disk and refresh the cache.

        Serializes the whole manifest to the ``.new`` staging file beside the
        live one, flushes + ``fsync``s it, then ``os.replace``s it over the live
        path (atomic on one filesystem). Finally swaps the cache entry and
        records the new file mtime so sync readers see the update without a
        reload.
        """
        ensure_dir(tenant_dir(self._data_dir, tenant))
        tmp = manifest_tmp_path(self._data_dir, tenant)
        live = manifest_path(self._data_dir, tenant)
        payload = json.dumps(manifest.to_dict(), indent=2)
        with tmp.open("w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, live)
        self._cache[tenant] = manifest
        try:
            self._mtime[tenant] = live.stat().st_mtime
        except FileNotFoundError:  # pragma: no cover - just-written file
            self._mtime.pop(tenant, None)

    def _lock(self, tenant: str) -> asyncio.Lock:
        """Return ``tenant``'s mutator lock, creating it on first use."""
        lock = self._locks.get(tenant)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[tenant] = lock
        return lock
