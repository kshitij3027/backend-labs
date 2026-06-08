"""Crash-safe Bloom filter snapshots — the ``BLM1`` and ``SBF1`` formats.

This module turns a single :class:`~src.bloom.BloomFilter` into one
self-validating binary blob and back (``BLM1``), wraps a whole
:class:`~src.scalable.ScalableBloomFilter` series the same way (``SBF1``),
and provides the atomic file plumbing that gets either on and off disk
without ever exposing a half-written snapshot.

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

The ``SBF1`` container format (version 1, all fields little-endian)
-------------------------------------------------------------------
A scalable filter (C6) is a small parameter block plus an ordered series of
plain Bloom filter slices, and its snapshot mirrors that shape exactly: an
``SBF1`` header followed by one complete, length-prefixed ``BLM1`` blob per
slice. ::

    offset     size    field
    ------     ----    -----
    0          4       magic  b"SBF1"
    4          2       container format version, u16  (currently 1)
    6          8       initial_capacity (n0), u64
    14         8       target_fp_rate, IEEE-754 f64
    22         2       growth factor (s), u16
    24         8       tightening ratio (r), IEEE-754 f64
    32         8       seed — base hash seed (slice i hashes with seed+i), u64
    40         2       slice_count, u16
    42         ...     slice_count × ( blob_length u32  +  one BLM1 blob )
    last 4     4       CRC32 (zlib.crc32) over everything before it, u32

Embedding whole ``BLM1`` blobs (inner CRCs included) instead of inventing a
second bitset encoding keeps exactly one parser and one validator per filter
shape — :func:`loads_scalable` walks the slice table and delegates every
entry to :func:`loads`. Restoring follows the same no-re-derivation rule as
``BLM1``: the stored parameters and slice list are taken verbatim via
``object.__new__`` (never re-run through the sizing/budget formulas), and
*any* invalid byte — container or inner slice — rejects the whole snapshot
with ``None`` plus a logged warning, never an exception.
"""
from __future__ import annotations

import logging
import os
import struct
import zlib
from pathlib import Path

from bitarray import bitarray

from src.bloom import BloomFilter
from src.scalable import ScalableBloomFilter

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


# ---------------------------------------------------------------------- #
# SBF1 — scalable-filter container (params header + one BLM1 per slice)  #
# ---------------------------------------------------------------------- #

#: 4-byte container magic: "Scalable Bloom Filter, format 1".
SCALABLE_MAGIC = b"SBF1"

#: Current SBF1 container version, stored as u16 right after the magic.
SCALABLE_VERSION = 1

#: Series parameters, little-endian: initial_capacity u64, target_fp_rate
#: f64, growth u16, tightening f64, seed u64, slice_count u16.
_SBF_PARAMS_STRUCT = struct.Struct("<QdHdQH")

#: Per-slice length prefix: size of the following BLM1 blob, u32.
_SBF_SLICE_LEN_STRUCT = struct.Struct("<I")

#: Byte offset where the params header ends and the slice table begins (42).
_SBF_PREFIX_LEN = (
    len(SCALABLE_MAGIC) + _VERSION_STRUCT.size + _SBF_PARAMS_STRUCT.size
)

#: Smallest structurally possible container: prefix + empty table + CRC (46).
_SBF_MIN_LEN = _SBF_PREFIX_LEN + _CRC_STRUCT.size


def dumps_scalable(sbf: ScalableBloomFilter) -> bytes:
    """Serialize ``sbf`` into one self-validating ``SBF1`` container.

    Each slice is embedded as a complete :func:`dumps` ``BLM1`` blob (inner
    CRC included) behind a u32 length prefix, so the container needs no
    second bitset encoding and the loader can delegate slice validation
    wholesale to :func:`loads`. Like :func:`dumps`, this is pure in-memory
    work — run it under the filter's lock (C7) and hand the bytes to
    :func:`write_atomic` outside it.
    """
    slices = sbf.slices
    parts = [
        SCALABLE_MAGIC,
        _VERSION_STRUCT.pack(SCALABLE_VERSION),
        _SBF_PARAMS_STRUCT.pack(
            sbf.initial_capacity,
            sbf.target_fp_rate,
            sbf.growth,
            sbf.tightening,
            sbf.seed,
            len(slices),
        ),
    ]
    for s in slices:
        blob = dumps(s)
        parts.append(_SBF_SLICE_LEN_STRUCT.pack(len(blob)))
        parts.append(blob)
    body = b"".join(parts)
    return body + _CRC_STRUCT.pack(zlib.crc32(body))


def loads_scalable(data: bytes) -> ScalableBloomFilter | None:
    """Parse an ``SBF1`` container, or return ``None`` if anything is invalid.

    Validates, in cheap-to-expensive order: structural length, magic,
    version, container CRC32, parameter plausibility, then the slice table
    (every length prefix in bounds, every inner blob a valid ``BLM1`` per
    :func:`loads`, no bytes left over). Any failure — including a single bad
    slice — logs a warning and rejects the whole snapshot with ``None``: a
    scalable filter with a missing slice would violate the zero-false-
    negative contract for every key that lived there, so partial restores
    are never attempted. The filter is then rebuilt verbatim (parameters and
    slices exactly as stored, no re-derivation) via ``object.__new__``.
    """
    if len(data) < _SBF_MIN_LEN:
        logger.warning(
            "SBF1 snapshot rejected: %d bytes is below the %d-byte "
            "structural minimum",
            len(data),
            _SBF_MIN_LEN,
        )
        return None

    if data[: len(SCALABLE_MAGIC)] != SCALABLE_MAGIC:
        logger.warning(
            "SBF1 snapshot rejected: bad magic %r (expected %r)",
            bytes(data[: len(SCALABLE_MAGIC)]),
            SCALABLE_MAGIC,
        )
        return None

    (version,) = _VERSION_STRUCT.unpack_from(data, len(SCALABLE_MAGIC))
    if version != SCALABLE_VERSION:
        logger.warning(
            "SBF1 snapshot rejected: unsupported container version %d "
            "(this build reads %d)",
            version,
            SCALABLE_VERSION,
        )
        return None

    (stored_crc,) = _CRC_STRUCT.unpack_from(data, len(data) - _CRC_STRUCT.size)
    computed_crc = zlib.crc32(data[: -_CRC_STRUCT.size])
    if stored_crc != computed_crc:
        logger.warning(
            "SBF1 snapshot rejected: CRC mismatch (stored 0x%08x, computed "
            "0x%08x) — torn write or corruption",
            stored_crc,
            computed_crc,
        )
        return None

    (
        initial_capacity,
        target_fp_rate,
        growth,
        tightening,
        seed,
        slice_count,
    ) = _SBF_PARAMS_STRUCT.unpack_from(data, len(SCALABLE_MAGIC) + _VERSION_STRUCT.size)

    # Same plausibility gates as the ScalableBloomFilter constructor (NaN in
    # either float field fails its comparison and is rejected here too).
    if (
        initial_capacity < 1
        or not 0.0 < target_fp_rate < 1.0
        or growth < 2
        or not 0.0 < tightening < 1.0
        or slice_count < 1
    ):
        logger.warning(
            "SBF1 snapshot rejected: implausible params (initial_capacity=%d, "
            "target_fp_rate=%r, growth=%d, tightening=%r, slice_count=%d)",
            initial_capacity,
            target_fp_rate,
            growth,
            tightening,
            slice_count,
        )
        return None

    # Walk the slice table: slice_count length-prefixed BLM1 blobs that must
    # consume exactly the bytes between the header and the CRC trailer.
    offset = _SBF_PREFIX_LEN
    end = len(data) - _CRC_STRUCT.size
    slices: list[BloomFilter] = []
    for index in range(slice_count):
        if offset + _SBF_SLICE_LEN_STRUCT.size > end:
            logger.warning(
                "SBF1 snapshot rejected: slice table truncated at slice %d "
                "of %d",
                index,
                slice_count,
            )
            return None
        (blob_len,) = _SBF_SLICE_LEN_STRUCT.unpack_from(data, offset)
        offset += _SBF_SLICE_LEN_STRUCT.size
        if offset + blob_len > end:
            logger.warning(
                "SBF1 snapshot rejected: slice %d of %d claims %d bytes but "
                "only %d remain",
                index,
                slice_count,
                blob_len,
                end - offset,
            )
            return None
        inner = loads(data[offset : offset + blob_len])
        if inner is None:
            # loads() already logged the specific reason.
            logger.warning(
                "SBF1 snapshot rejected: inner BLM1 blob for slice %d of %d "
                "failed validation",
                index,
                slice_count,
            )
            return None
        slices.append(inner)
        offset += blob_len

    if offset != end:
        logger.warning(
            "SBF1 snapshot rejected: %d trailing bytes after the last of %d "
            "slices",
            end - offset,
            slice_count,
        )
        return None

    return _restore_scalable(
        initial_capacity=initial_capacity,
        target_fp_rate=target_fp_rate,
        growth=growth,
        tightening=tightening,
        seed=seed,
        slices=slices,
    )


def _restore_scalable(
    *,
    initial_capacity: int,
    target_fp_rate: float,
    growth: int,
    tightening: float,
    seed: int,
    slices: list[BloomFilter],
) -> ScalableBloomFilter:
    """Rebuild a ScalableBloomFilter from stored state, bypassing ``__init__``.

    The constructor would eagerly create a fresh slice 0 from the sizing and
    budget formulas; a snapshot must instead restore the exact slices its
    keys were admitted into (same rationale as :func:`_restore_filter`). The
    stored parameters still matter — they drive every *future* slice the
    restored filter appends — so they are restored verbatim alongside the
    slice list.
    """
    sbf = object.__new__(ScalableBloomFilter)
    sbf._initial_capacity = initial_capacity
    sbf._target_fp_rate = target_fp_rate
    sbf._growth = growth
    sbf._tightening = tightening
    sbf._seed = seed
    sbf._slices = slices
    return sbf


def save_scalable(sbf: ScalableBloomFilter, path: str | Path) -> None:
    """Serialize ``sbf`` and atomically write it to ``path``.

    Convenience composition of :func:`dumps_scalable` + :func:`write_atomic`
    for single-owner callers, mirroring :func:`save`. The concurrent
    snapshot path (C7/C8) calls the primitives itself: serialize under the
    filter's lock, write outside it.
    """
    write_atomic(dumps_scalable(sbf), path)


def load_scalable(path: str | Path) -> ScalableBloomFilter | None:
    """Read and validate the ``SBF1`` container at ``path``.

    ``None`` means "start fresh", exactly like :func:`load`: a missing file
    is the normal first boot (info log); unreadable or invalid content is
    warned about by :func:`loads_scalable` and never raises.
    """
    path = Path(path)
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        logger.info(
            "SBF1 snapshot %s not found; starting with a fresh filter", path
        )
        return None
    except OSError as exc:
        logger.warning(
            "SBF1 snapshot %s unreadable (%s); starting with a fresh filter",
            path,
            exc,
        )
        return None
    return loads_scalable(data)
