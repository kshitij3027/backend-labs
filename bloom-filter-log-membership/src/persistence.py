"""Crash-safe Bloom filter snapshots — the ``BLM1`` on-disk format.

This module turns a single :class:`~src.bloom.BloomFilter` into one
self-validating binary blob and back, plus the atomic file plumbing that gets
the blob on and off disk without ever exposing a half-written snapshot.

The ``BLM1`` format (version 1, all fields little-endian)
---------------------------------------------------------
::

    offset     size    field
    ------     ----    -----
    0          4       magic  b"BLM1"
    4          2       format version, u16  (currently 1)
    6          8       m  — bit-array size in bits, u64
    14         2       k  — hash probes per key, u16
    16         8       expected_items (n), u64
    24         8       fp_rate (p), IEEE-754 f64
    32         8       seed — murmur3 hash seed, u64
    40         8       count — distinct-ish inserts, u64
    48         m // 8  payload — raw bits via bitarray.tobytes()
    48 + m//8  4       CRC32 (zlib.crc32) over bytes [0, 48 + m//8), u32

``m`` is always a multiple of 8 (:func:`~src.bloom.optimal_m` rounds up), so
the payload is exactly ``m // 8`` bytes with zero trailing pad bits and the
``tobytes()`` / ``frombytes()`` roundtrip is bit-exact. The header carries
*everything* the filter was built from — restoring never re-runs the sizing
formulas, so a snapshot stays readable even if a future version tweaks the
sizing math (the stored m/k/seed are the geometry the bits were written with;
re-deriving them would silently shift every probe position).

Why a CRC32 trailer
-------------------
A snapshot that *exists* is not a snapshot that is *trustworthy*: a crash
mid-write, a torn sector, or plain bit rot leaves a file that opens fine and
parses into garbage bits — which a Bloom filter would happily serve as wrong
membership answers. The CRC32 over everything before the trailer turns any
such corruption into a detected, loggable rejection instead. ``zlib.crc32``
is stdlib, C-speed, and more than strong enough for accidental-corruption
detection (this is an integrity check, not an authenticity one).

Why tmp + fsync + rename
------------------------
:func:`write_atomic` writes to ``<path>.tmp`` in the *same directory*, flushes
and ``os.fsync``\\ s it (so the bytes are on disk, not just in the page
cache), then ``os.replace``\\ s it over the live path — an atomic rename on
POSIX. A crash at any instant therefore leaves either the old complete
snapshot or the new complete snapshot, never a mix and never a truncated
file at the live path. The CRC is the belt to this suspender: even if a
filesystem breaks the rename promise, the corruption is caught at load.

Why ``loads``/``load`` return ``None`` instead of raising
---------------------------------------------------------
Snapshots are a warm-start optimization, not the system of record. If the
service crash-looped on a bad snapshot it could never come back up without
human surgery; booting with a fresh, empty filter (and a loud warning in the
logs) keeps the service available and lets it re-learn membership. Callers
get a simple contract: ``None`` means "start fresh", anything else is a
fully validated filter.

Why ``dumps``/``write_atomic`` are separate primitives
------------------------------------------------------
The snapshot path in C7/C8 serializes **under the filter's lock** (a cheap,
µs-scale memory copy) and performs the slow file I/O **outside** it.
:func:`save` composes the two for single-owner convenience.

NOTE: C6 adds an ``SBF1`` container that wraps a sequence of these ``BLM1``
blobs plus scalable-filter parameters. Everything in this module is
deliberately single-filter-scoped until then.
"""
from __future__ import annotations

import logging
import os
import struct
import zlib
from pathlib import Path

from bitarray import bitarray

from src.bloom import BloomFilter

logger = logging.getLogger(__name__)

#: 4-byte format magic: "BLooM, format 1".
MAGIC = b"BLM1"

#: Current format version, stored as u16 right after the magic.
VERSION = 1

# Fixed little-endian struct codecs (no alignment padding with "<").
_VERSION_STRUCT = struct.Struct("<H")  # version: u16
_HEADER_STRUCT = struct.Struct("<QHQdQQ")  # m, k, expected_items, fp_rate, seed, count
_CRC_STRUCT = struct.Struct("<I")  # crc32: u32

#: Byte offset where the fixed header ends and the bit payload begins (48).
_PREFIX_LEN = len(MAGIC) + _VERSION_STRUCT.size + _HEADER_STRUCT.size

#: Smallest structurally possible blob: prefix + empty payload + CRC (52).
_MIN_LEN = _PREFIX_LEN + _CRC_STRUCT.size


# ---------------------------------------------------------------------- #
# bytes-level primitives                                                 #
# ---------------------------------------------------------------------- #


def dumps(bf: BloomFilter) -> bytes:
    """Serialize ``bf`` into one self-validating ``BLM1`` blob.

    Cheap enough to run under the filter's lock: one ``tobytes()`` copy of
    the bitset plus a C-speed CRC pass. Hand the result to
    :func:`write_atomic` *outside* the lock.
    """
    body = b"".join(
        (
            MAGIC,
            _VERSION_STRUCT.pack(VERSION),
            _HEADER_STRUCT.pack(
                bf.m,
                bf.k,
                bf.expected_items,
                bf.fp_rate,
                bf.seed,
                bf.count,
            ),
            bf.bits.tobytes(),
        )
    )
    return body + _CRC_STRUCT.pack(zlib.crc32(body))


def loads(data: bytes) -> BloomFilter | None:
    """Parse a ``BLM1`` blob back into a BloomFilter, or ``None`` if invalid.

    Validates, in cheap-to-expensive order: structural length, magic,
    version, CRC32 over everything before the trailer, and header
    self-consistency (``m`` a positive multiple of 8 whose ``m // 8``
    matches the payload length, sane ``k``/``n``/``p``). Any failure logs a
    warning and returns ``None`` — corrupt input never raises, so the
    service boots with a fresh filter instead of crash-looping (see module
    docstring).
    """
    if len(data) < _MIN_LEN:
        logger.warning(
            "snapshot rejected: %d bytes is below the %d-byte structural minimum",
            len(data),
            _MIN_LEN,
        )
        return None

    if data[: len(MAGIC)] != MAGIC:
        logger.warning(
            "snapshot rejected: bad magic %r (expected %r)",
            bytes(data[: len(MAGIC)]),
            MAGIC,
        )
        return None

    (version,) = _VERSION_STRUCT.unpack_from(data, len(MAGIC))
    if version != VERSION:
        logger.warning(
            "snapshot rejected: unsupported format version %d (this build reads %d)",
            version,
            VERSION,
        )
        return None

    (stored_crc,) = _CRC_STRUCT.unpack_from(data, len(data) - _CRC_STRUCT.size)
    computed_crc = zlib.crc32(data[: -_CRC_STRUCT.size])
    if stored_crc != computed_crc:
        logger.warning(
            "snapshot rejected: CRC mismatch (stored 0x%08x, computed 0x%08x) "
            "— torn write or corruption",
            stored_crc,
            computed_crc,
        )
        return None

    m, k, expected_items, fp_rate, seed, count = _HEADER_STRUCT.unpack_from(
        data, len(MAGIC) + _VERSION_STRUCT.size
    )
    payload = data[_PREFIX_LEN : len(data) - _CRC_STRUCT.size]

    if m <= 0 or m % 8 != 0:
        logger.warning(
            "snapshot rejected: m=%d is not a positive multiple of 8", m
        )
        return None
    if len(payload) != m // 8:
        logger.warning(
            "snapshot rejected: header says m=%d (%d payload bytes) "
            "but payload is %d bytes",
            m,
            m // 8,
            len(payload),
        )
        return None
    if k < 1 or expected_items < 1 or not 0.0 < fp_rate < 1.0:
        logger.warning(
            "snapshot rejected: implausible header (k=%d, expected_items=%d, "
            "fp_rate=%r)",
            k,
            expected_items,
            fp_rate,
        )
        return None

    # bloom.py builds its bitarray with the library default bit-endianness
    # ("big"); pin the same here so frombytes lays the bits out identically.
    bits = bitarray(endian="big")
    bits.frombytes(bytes(payload))
    # Defensive trim: frombytes yields whole bytes, and m is byte-aligned,
    # so this is a no-op today — it only matters if alignment ever changes.
    del bits[m:]

    return _restore_filter(
        m=m,
        k=k,
        expected_items=expected_items,
        fp_rate=fp_rate,
        seed=seed,
        count=count,
        bits=bits,
    )


def _restore_filter(
    *,
    m: int,
    k: int,
    expected_items: int,
    fp_rate: float,
    seed: int,
    count: int,
    bits: bitarray,
) -> BloomFilter:
    """Rebuild a BloomFilter from stored state, bypassing ``__init__``.

    ``BloomFilter.__init__`` *recomputes* m and k from the sizing formulas.
    A snapshot must instead restore the exact geometry its bits were written
    with — if a future release ever adjusted the formulas, re-deriving m/k
    would silently shift every probe position and scramble membership
    answers. ``object.__new__`` plus direct attribute assignment restores
    the saved values verbatim, and keeps ``src/bloom.py`` untouched by the
    persistence layer.
    """
    bf = object.__new__(BloomFilter)
    bf._expected_items = expected_items
    bf._fp_rate = fp_rate
    bf._seed = seed
    bf._m = m
    bf._k = k
    bf._bits = bits
    bf._count = count
    return bf


# ---------------------------------------------------------------------- #
# file-level operations                                                  #
# ---------------------------------------------------------------------- #


def write_atomic(data: bytes, path: str | Path) -> None:
    """Write ``data`` to ``path`` so a crash can never corrupt the live file.

    Writes to ``<path>.tmp`` in the same directory (rename is only atomic
    within one filesystem), flushes and ``os.fsync``\\ s so the bytes reach
    disk, then atomically ``os.replace``\\ s it over ``path``. Readers see
    either the old complete file or the new complete file — never a partial
    one. The parent directory is created if missing, the temp file is
    removed if anything fails, and the directory entry is fsynced
    best-effort afterwards so the rename itself survives a power cut.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        # Never leave a stray .tmp behind — and never let cleanup mask
        # the original error.
        try:
            tmp.unlink(missing_ok=True)
        except OSError:  # pragma: no cover - cleanup is best-effort
            pass
        raise
    _fsync_dir(path.parent)


def _fsync_dir(directory: Path) -> None:
    """Best-effort fsync of a directory so a completed rename is durable.

    POSIX only persists the renamed directory entry once the directory is
    synced; skipping this risks the *old* name reappearing after a power
    loss (still a complete old snapshot — correctness holds either way,
    which is why failure here is ignorable). Some platforms cannot open
    directories at all, hence best-effort.
    """
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:  # pragma: no cover - platform-dependent (e.g. Windows)
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover - not supported on every FS
        pass
    finally:
        os.close(fd)


def save(bf: BloomFilter, path: str | Path) -> None:
    """Serialize ``bf`` and atomically write it to ``path``.

    Convenience composition of :func:`dumps` + :func:`write_atomic` for
    single-owner callers (tests, shutdown hooks). The concurrent snapshot
    path (C7/C8) calls the two primitives itself: ``dumps`` under the
    filter's lock, ``write_atomic`` outside it.
    """
    write_atomic(dumps(bf), path)


def load(path: str | Path) -> BloomFilter | None:
    """Read and validate the snapshot at ``path``; ``None`` means start fresh.

    A missing file is the normal first-boot case (info log); an unreadable
    or invalid file is logged as a warning by :func:`loads`. Stray
    ``<path>.tmp`` files from a crash mid-write are ignored entirely — only
    the live path is ever read.
    """
    path = Path(path)
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        logger.info("snapshot %s not found; starting with a fresh filter", path)
        return None
    except OSError as exc:
        logger.warning(
            "snapshot %s unreadable (%s); starting with a fresh filter", path, exc
        )
        return None
    return loads(data)
