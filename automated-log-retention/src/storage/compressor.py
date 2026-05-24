"""Streaming zstd compression helpers for the cold and archive tiers.

The retention applier (C10) moves segments from warm to cold and from
cold to archive. Both moves involve zstandard compression: cold uses a
fast preset (level 3) to keep apply latency low, while archive uses the
slowest preset (level 19) to maximise long-term storage savings on data
that will rarely be read again.

Both helpers stream through a 1 MiB chunk loop so a 100 MB segment never
materialises in memory in one piece. Decompression is symmetric and is
used only on rare read paths (debug, compliance export); the applier
itself never decompresses during the normal lifecycle.

The public surface is intentionally tiny:

    compress_file(src, dst, level) -> int   # bytes written to dst
    decompress_file(src, dst)      -> int   # bytes written to dst

plus the two default level constants. Everything else (atomic rename,
catalog update, audit entry) is the caller's responsibility — this
module owns nothing but the byte-for-byte transformation.
"""

from __future__ import annotations

from pathlib import Path

import zstandard as zstd

# Compression presets used by the lifecycle applier (C10).
#
# Level 3 is zstd's default; on text-heavy log data it produces 3-5x
# compression at a few hundred MB/s on a single core, which keeps the
# warm -> cold transition cheap enough to run inline on the apply
# scheduler tick.
#
# Level 19 is the slowest "normal" preset (level 22 is the ultra mode
# that needs a flag); it trades ~50x more CPU for an additional 10-25%
# size reduction on already-compressed text. That's the right trade for
# the archive tier, where compactness over years matters more than
# throughput on the one-time write.
DEFAULT_COLD_LEVEL: int = 3
DEFAULT_ARCHIVE_LEVEL: int = 19

# Reading and writing in 1 MiB chunks bounds peak memory regardless of
# segment size. The zstandard library will internally buffer a frame's
# worth of data; we don't need to match the frame size exactly.
_CHUNK_SIZE: int = 1 * 1024 * 1024  # 1 MiB


def compress_file(src: Path | str, dst: Path | str, level: int) -> int:
    """Stream-compress ``src`` to ``dst`` at the given zstd ``level``.

    Returns the number of bytes written to ``dst`` (i.e. the compressed
    size on disk), which the caller persists in the catalog so the
    dashboard can report bytes saved per tier.

    The function is plain blocking I/O — it is meant to be called from
    a thread (or from within the scheduler tick) where the cost is
    acceptable. There is no async variant because zstandard itself is
    a CPU-bound C extension; an async wrapper would yield no benefit.
    """
    src_path = Path(src)
    dst_path = Path(dst)

    compressor = zstd.ZstdCompressor(level=level)
    with src_path.open("rb") as src_fh, dst_path.open("wb") as dst_fh:
        # ``copy_stream`` reads from ``src_fh`` in ``read_size`` chunks,
        # feeds the compressor, and writes framed output to ``dst_fh``.
        # It handles flushing the trailing frame on close, so the file
        # is a valid zstd stream as soon as the ``with`` block exits.
        compressor.copy_stream(src_fh, dst_fh, read_size=_CHUNK_SIZE)

    # ``stat`` after the file handle has been closed gives us the true
    # on-disk size including any trailing zstd frame footer.
    return dst_path.stat().st_size


def decompress_file(src: Path | str, dst: Path | str) -> int:
    """Stream-decompress ``src`` (zstd) to ``dst`` and return bytes written.

    Mirror of :func:`compress_file`. The decompressor does not need a
    level argument — the frame header carries the parameters required
    to reconstruct the original bytes.
    """
    src_path = Path(src)
    dst_path = Path(dst)

    decompressor = zstd.ZstdDecompressor()
    with src_path.open("rb") as src_fh, dst_path.open("wb") as dst_fh:
        decompressor.copy_stream(src_fh, dst_fh, read_size=_CHUNK_SIZE)

    return dst_path.stat().st_size
