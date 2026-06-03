"""On-disk layout and partition-identity helpers.

This module is the single source of truth for *where* a tenant's manifest and
partition files live on disk, and *how* a log entry's timestamp maps to a
partition. It is intentionally side-effect free: importing it touches nothing,
and only :func:`ensure_dir` ever writes to the filesystem — and only when an
caller explicitly invokes it.

On-disk layout::

    <data_dir>/<tenant>/manifest.json              (+ manifest.json.new transient)
    <data_dir>/<tenant>/p_<bucket>/row.jsonl.lz4    (+ .new)
    <data_dir>/<tenant>/p_<bucket>/data.parquet     (+ .new)
    <data_dir>/<tenant>/p_<bucket>/recent.jsonl.lz4 (+ .new)   # HYBRID recent side

The ``.new`` suffix marks a transient *staging* file written during a
copy-on-write migration; once fully written and fsynced it is atomically
``os.replace``-d over the live file (see ``manifest.py`` / ``migration_engine.py``).

Tenant names are assumed to be simple, filesystem-safe strings; no sanitisation
is performed here.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "bucket_for",
    "partition_id_for",
    "tenant_dir",
    "manifest_path",
    "manifest_tmp_path",
    "partition_dir",
    "PartitionPaths",
    "partition_paths",
    "ensure_dir",
]

# --- file-name constants (kept here so backends import a single source) ---
MANIFEST_NAME = "manifest.json"
MANIFEST_TMP_NAME = "manifest.json.new"
ROW_NAME = "row.jsonl.lz4"
PARQUET_NAME = "data.parquet"
RECENT_NAME = "recent.jsonl.lz4"

# Suffix appended to produce a copy-on-write staging path.
STAGING_SUFFIX = ".new"


def bucket_for(ts: float, bucket_seconds: int) -> int:
    """Return the integer time-bucket index for ``ts``.

    Pure integer math: ``floor(ts / bucket_seconds)``. Deterministic — given the
    same arguments it always returns the same bucket.
    """
    return int(ts // bucket_seconds)


def partition_id_for(ts: float, bucket_seconds: int) -> str:
    """Return the partition id (``p_<bucket>``) that ``ts`` falls into."""
    return f"p_{bucket_for(ts, bucket_seconds)}"


def tenant_dir(data_dir: str | Path, tenant: str) -> Path:
    """Return the root directory holding ``tenant``'s manifest and partitions."""
    return Path(data_dir) / tenant


def manifest_path(data_dir: str | Path, tenant: str) -> Path:
    """Return the path to ``tenant``'s live ``manifest.json``."""
    return tenant_dir(data_dir, tenant) / MANIFEST_NAME


def manifest_tmp_path(data_dir: str | Path, tenant: str) -> Path:
    """Return the transient ``manifest.json.new`` staging path for ``tenant``.

    This lives beside the live manifest (same directory / filesystem) so an
    ``os.replace`` over :func:`manifest_path` is atomic.
    """
    return tenant_dir(data_dir, tenant) / MANIFEST_TMP_NAME


def partition_dir(data_dir: str | Path, tenant: str, partition_id: str) -> Path:
    """Return the directory holding a single partition's data files."""
    return tenant_dir(data_dir, tenant) / partition_id


@dataclass(frozen=True)
class PartitionPaths:
    """Resolved on-disk paths for one partition's data files.

    Holds the partition directory plus the three backend file paths (ROW,
    COLUMNAR/sealed Parquet, and the HYBRID recent side). The ``*_new``
    properties expose the matching copy-on-write staging paths.
    """

    tenant: str
    partition_id: str
    dir: Path
    row: Path
    parquet: Path
    recent: Path

    def staging(self, p: Path) -> Path:
        """Return the ``.new`` staging variant of ``p`` (suffix appended)."""
        return p.with_suffix(p.suffix + STAGING_SUFFIX)

    @property
    def row_new(self) -> Path:
        """Staging path for the ROW file."""
        return self.staging(self.row)

    @property
    def parquet_new(self) -> Path:
        """Staging path for the Parquet file."""
        return self.staging(self.parquet)

    @property
    def recent_new(self) -> Path:
        """Staging path for the HYBRID recent file."""
        return self.staging(self.recent)


def partition_paths(
    data_dir: str | Path, tenant: str, partition_id: str
) -> PartitionPaths:
    """Build a :class:`PartitionPaths` for ``(tenant, partition_id)``."""
    pdir = partition_dir(data_dir, tenant, partition_id)
    return PartitionPaths(
        tenant=tenant,
        partition_id=partition_id,
        dir=pdir,
        row=pdir / ROW_NAME,
        parquet=pdir / PARQUET_NAME,
        recent=pdir / RECENT_NAME,
    )


def ensure_dir(path: Path) -> Path:
    """Create ``path`` (and parents) if absent, then return it.

    The only function in this module that touches the filesystem, and only when
    explicitly called. Idempotent: ``exist_ok=True``.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path
