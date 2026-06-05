"""ROW storage backend — append-only length-prefixed LZ4 blocks of JSONL.

The ROW format is optimised for cheap, frequent appends and full-record reads
(the access pattern of recent/write-heavy partitions). Each :meth:`RowBackend.write`
call serialises its batch to JSONL, compresses it into a single self-describing
LZ4 frame (see :mod:`src.compression`), and appends one **length-prefixed block**
to the partition file:

    ┌───────────────┬───────────────────────────────┐
    │ uint32 (BE)   │  LZ4 frame  (frame_lz4(JSONL)) │
    │ block length  │  ... block length bytes ...    │
    └───────────────┴───────────────────────────────┘

Multiple ``write()`` calls therefore concatenate multiple independent blocks in
the same file. The length prefix lets :meth:`RowBackend.read` walk block-by-block
and decode every batch correctly, without a separate index or rewriting existing
data on append. The on-disk file is ``paths.row`` (``row.jsonl.lz4``).

Empty-batch policy
------------------
An empty ``entries`` list still writes a real (trivial) block: the JSONL payload
is the empty string, ``frame_lz4(b"")`` is a valid frame, and the block is framed
and appended like any other. This keeps the format uniform — :meth:`read` handles
such blocks transparently (they contribute zero rows) and :meth:`size_bytes`
honestly reflects the few bytes the block occupies. The alternative (skipping the
write) was rejected so that ``size_bytes`` is never silently out of step with the
number of ``write()`` calls made.

This backend is pure stdlib + the project's LZ4 framing helpers — no PyArrow.
"""

from __future__ import annotations

import json
import os
import struct
from typing import Any

from ..compression import frame_lz4, unframe_lz4
from ..models import Filter, Format
from ..paths import PartitionPaths, ensure_dir
from .base import (
    ReadResult,
    RewriteResult,
    StorageBackend,
    WriteResult,
    apply_filters,
    project_columns,
)

__all__ = ["RowBackend"]

# Block framing: a big-endian unsigned 32-bit length prefix precedes each LZ4
# frame. ``>I`` -> 4 bytes, network byte order.
_LEN_STRUCT = struct.Struct(">I")
_LEN_SIZE = _LEN_STRUCT.size


def _encode_block(entries: list[dict]) -> bytes:
    """Serialise ``entries`` to one length-prefixed LZ4 block.

    The payload is newline-delimited JSON (one compact object per entry) with a
    trailing newline; it is then LZ4-framed and prefixed with its big-endian
    uint32 byte length. An empty ``entries`` list yields a valid block wrapping
    an empty payload.

    Args:
        entries: Flat record dicts to encode.

    Returns:
        ``len-prefix || frame_lz4(jsonl)`` ready to append to the partition file.
    """
    if entries:
        payload = ("\n".join(json.dumps(e) for e in entries) + "\n").encode("utf-8")
    else:
        # Trivial block: no rows, empty payload (still a valid LZ4 frame).
        payload = b""
    block = frame_lz4(payload)
    return _LEN_STRUCT.pack(len(block)) + block


def _decode_into(fh, rows: list[dict]) -> None:
    """Decode every length-prefixed block from ``fh`` into ``rows``.

    Walks the file block-by-block: read the 4-byte length prefix (a short/empty
    read signals clean EOF and ends the loop), read exactly that many frame
    bytes, LZ4-decompress, then JSON-decode each non-empty line. Mutates ``rows``
    in place to avoid building and concatenating intermediate lists.

    Args:
        fh: A binary file handle positioned at a block boundary (start of file).
        rows: The list to append decoded records to.
    """
    while True:
        header = fh.read(_LEN_SIZE)
        if len(header) < _LEN_SIZE:
            # Clean EOF (or a truncated trailing header) — stop reading.
            break
        (block_len,) = _LEN_STRUCT.unpack(header)
        block = fh.read(block_len)
        if len(block) < block_len:
            # Truncated final block (e.g. interrupted append) — stop reading
            # rather than raising; already-decoded rows remain valid.
            break
        payload = unframe_lz4(block)
        text = payload.decode("utf-8")
        for line in text.split("\n"):
            if line:
                rows.append(json.loads(line))


class RowBackend(StorageBackend):
    """Append-only row store: length-prefixed LZ4 blocks of JSONL.

    See the module docstring for the on-disk block format. Writes append a single
    block; reads walk all blocks; filtering and projection run in-process via the
    shared helpers in :mod:`src.storage.base`.
    """

    format = Format.ROW

    def write(self, entries: list[dict], paths: PartitionPaths) -> WriteResult:
        """Append ``entries`` as one LZ4 block to ``paths.row``.

        Creates the partition directory if needed, then opens the row file in
        binary append mode so existing blocks are preserved.

        Args:
            entries: Flat record dicts to append.
            paths: Resolved partition paths.

        Returns:
            A :class:`WriteResult` with ``row_count_delta == len(entries)``, the
            post-write file size, and an empty ``codecs`` map (ROW uses a single
            whole-block frame, not per-column codecs).
        """
        ensure_dir(paths.dir)
        block = _encode_block(entries)
        with open(paths.row, "ab") as fh:
            fh.write(block)
        return WriteResult(
            row_count_delta=len(entries),
            size_bytes=self.size_bytes(paths),
            codecs={},
        )

    def read(
        self,
        paths: PartitionPaths,
        *,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        index: Any = None,
    ) -> ReadResult:
        """Read all rows, apply ``filters``, then project ``columns``.

        Args:
            paths: Resolved partition paths.
            columns: Top-level keys to project; ``None`` for full records.
            filters: Predicates to AND; ``None``/empty for no filtering.
            index: Unused by ROW (no min/max row-group index); accepted for ABC
                compatibility.

        Returns:
            A :class:`ReadResult`. ``rows_scanned`` counts every decoded row
            (pre-filter); ``rowgroups_skipped`` is always ``0`` for ROW. A
            missing partition file yields ``ReadResult([], 0)``.
        """
        if not paths.row.exists():
            return ReadResult([], 0)

        rows: list[dict] = []
        with open(paths.row, "rb") as fh:
            _decode_into(fh, rows)

        scanned = len(rows)
        rows = apply_filters(rows, filters)
        rows = project_columns(rows, columns)
        return ReadResult(rows, rows_scanned=scanned)

    def size_bytes(self, paths: PartitionPaths) -> int:
        """Return ``paths.row``'s size in bytes, or ``0`` if it does not exist."""
        try:
            return paths.row.stat().st_size
        except FileNotFoundError:
            return 0

    def rewrite_from(
        self,
        source: "StorageBackend",
        src_paths: PartitionPaths,
        dst_paths: PartitionPaths,
        *,
        codecs: dict[str, str] | None = None,
        index_cols: list[str] | None = None,
    ) -> RewriteResult:
        """Copy-on-write rewrite of ``source``'s data into ROW format.

        Reads *all* rows from ``source`` (full record, no filter) and writes them
        as a single fresh ROW block to the **staging** path ``dst_paths.row_new``
        (truncating/creating, never appending), then fsyncs that file so it is
        durable before the migration engine flips the manifest. The live file is
        left untouched.

        ``codecs`` and ``index_cols`` are accepted for ABC compatibility but
        ignored — ROW has no per-column codecs or row-group index.

        Args:
            source: Backend to migrate from.
            src_paths: Where the source data lives.
            dst_paths: Target partition paths (the ``.new`` staging side is used).
            codecs: Ignored (ROW has no per-column codecs).
            index_cols: Ignored (ROW has no index).

        Returns:
            A :class:`RewriteResult` whose ``staged`` holds the single
            ``(dst_paths.row_new, dst_paths.row)`` pair for the engine to
            ``os.replace``.
        """
        rows = source.read(src_paths, columns=None, filters=None).rows
        ensure_dir(dst_paths.dir)

        staging = dst_paths.row_new
        block = _encode_block(rows)
        # Truncate/create (not append): the staging file is a complete rewrite.
        with open(staging, "wb") as fh:
            fh.write(block)
            fh.flush()
            os.fsync(fh.fileno())

        return RewriteResult(
            row_count=len(rows),
            size_bytes=staging.stat().st_size,
            codecs={},
            staged=[(staging, dst_paths.row)],
        )
