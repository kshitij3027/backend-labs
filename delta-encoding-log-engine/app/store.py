"""In-memory segment store: the codec wrapped in honest byte accounting.

This is the stateful layer the API talks to. It holds exactly one batch at a
time — the current raw entries and their delta-encoded :class:`~app.codec.EncodedLog`
— and answers two kinds of question about it:

* **How much did delta encoding actually save?** :class:`CompressionStats` is the
  authoritative byte-accounting record (see *plan.md → "Byte-accounting
  methodology"*). It reports three independent reductions and never folds the
  general compressor into the headline:

  - ``delta_reduction`` — encoded (keyframe + delta envelopes) vs raw. **This is
    the ≥60% claim.**
  - ``gzip_raw_reduction`` — stdlib gzip of the *raw* entries vs raw. The honest
    general-compressor baseline the delta layer must beat.
  - ``delta_plus_gzip_reduction`` — gzip *on top of* the delta stream vs raw. The
    "stacks on top" bonus.

  Every byte count is measured on :func:`~app.codec.canonical_bytes` (compact,
  sorted-key UTF-8 — the same basis the codec uses for equality and the size
  guard), never ``sys.getsizeof``. gzip is always called with ``mtime=0`` so the
  counts are byte-for-byte reproducible across runs.

* **What was entry _i_ (or this page) originally?** Reconstruction delegates
  straight to the codec — :func:`~app.codec.decode` for whole-batch and
  :func:`~app.codec.reconstruct_index` for bounded random access — so the store
  never reimplements diff/apply and the codec's round-trip contract is preserved
  verbatim: ``reconstruct_all()`` after ``compress(entries)`` is element-wise
  canonically equal to ``entries``.

**Thread-safety.** The API drives writes (``compress``) and reads
(``reconstruct*`` / ``stats``) concurrently — heavy handlers run in Starlette's
threadpool. A single :class:`threading.Lock` guards all access to the mutable
trio (``_raw`` / ``_encoded`` / ``_stats``). There is exactly one logical writer,
so to keep the code simple the (modest) ``canonical_bytes``/gzip work is done
under the lock; reads take deep copies before returning so callers can mutate
freely without racing the store.

**Isolation by deep copy.** ``set_raw`` / ``compress`` deep-copy the caller's
list on the way in and ``get_raw`` deep-copies on the way out, so neither the
caller mutating their original list nor mutating a returned list can corrupt
stored state. (The codec already deep-copies keyframes and returns fresh decoded
entries, so reconstruction is safe without extra copying here.)
"""
from __future__ import annotations

import copy
import gzip
import threading
from dataclasses import dataclass

from app.codec import (
    EncodedLog,
    canonical_bytes,
    decode,
    encode,
    reconstruct_index,
)
from app.encoders import EncoderConfig
from app.models import LogEntry


def _gzip_len(data: bytes) -> int:
    """Length of ``data`` gzipped with a fixed mtime (reproducible byte count).

    ``mtime=0`` zeroes the timestamp field in the gzip header so the compressed
    output — and therefore this length — is identical run to run for identical
    input. Used for both the raw-entries baseline and the delta-stream bonus.
    """
    return len(gzip.compress(data, mtime=0))


@dataclass
class CompressionStats:
    """Byte-accounting result for one compressed batch (all sizes in bytes).

    Every size is computed on canonical UTF-8 bytes (:func:`~app.codec.canonical_bytes`),
    the same basis the codec uses for equality and its per-delta size guard, so the
    numbers here are directly comparable to what the codec stores. The three
    reductions are reported separately and never combined into a single headline
    (see the module docstring / *plan.md*). All gzip sizes use ``mtime=0``.
    """

    count: int
    """Number of original entries this accounting covers."""

    keyframe_count: int
    """Number of keyframes stored (one per segment)."""

    delta_count: int
    """Number of deltas stored (``count - keyframe_count`` for a full batch)."""

    raw_bytes: int
    """Σ ``len(canonical_bytes(entry))`` over the originals — the fair baseline."""

    encoded_bytes: int
    """Σ ``len(canonical_bytes(keyframe))`` + Σ ``len(canonical_bytes(delta))``.

    Delta envelope overhead is *included* (every ``"~"`` / ``"-"`` / ``"@"`` wrapper
    is paid for), and the deltas counted are the ones actually stored — i.e. the
    typed-encoded form when the encoder layer adopted it. This is the honest cost
    of the delta representation.
    """

    gzip_raw_bytes: int
    """``len(gzip.compress(b"".join(canonical_bytes(e) for e in entries), mtime=0))``.

    Stdlib gzip applied to the concatenated raw entries — the general-compressor
    baseline the delta layer is measured against.
    """

    delta_plus_gzip_bytes: int
    """``len(gzip.compress(canonical_bytes(encoded.to_dict()), mtime=0))``.

    Stdlib gzip applied on top of the serialized delta stream — the "delta then
    gzip stacks" bonus number.
    """

    delta_reduction: float
    """``round(100 * (raw - encoded) / raw, 2)`` — the ≥60% claim (0.0 if raw==0)."""

    gzip_raw_reduction: float
    """``round(100 * (raw - gzip_raw) / raw, 2)`` — gzip-of-raw baseline (0.0 if raw==0)."""

    delta_plus_gzip_reduction: float
    """``round(100 * (raw - delta_plus_gzip) / raw, 2)`` — bonus (0.0 if raw==0)."""

    compression_ratio: float
    """``round(encoded / raw, 4)`` — stored fraction of the original (0.0 if raw==0)."""

    def to_dict(self) -> dict:
        """Return all fields as a plain JSON-native dict (``json.dumps``-safe)."""
        return {
            "count": self.count,
            "keyframe_count": self.keyframe_count,
            "delta_count": self.delta_count,
            "raw_bytes": self.raw_bytes,
            "encoded_bytes": self.encoded_bytes,
            "gzip_raw_bytes": self.gzip_raw_bytes,
            "delta_plus_gzip_bytes": self.delta_plus_gzip_bytes,
            "delta_reduction": self.delta_reduction,
            "gzip_raw_reduction": self.gzip_raw_reduction,
            "delta_plus_gzip_reduction": self.delta_plus_gzip_reduction,
            "compression_ratio": self.compression_ratio,
        }


def _compute_stats(entries: list[LogEntry], encoded: EncodedLog) -> CompressionStats:
    """Build :class:`CompressionStats` from the originals and their encoding.

    Pure function (no store state, no locking) so the byte-accounting formulas
    live in one auditable place. Every formula here matches *plan.md →
    "Byte-accounting methodology"* exactly; the divide-by-zero guard collapses all
    reductions and the ratio to ``0.0`` for an empty batch.
    """
    count = len(entries)
    keyframe_count = len(encoded.segments)
    delta_count = sum(len(seg.deltas) for seg in encoded.segments)

    # raw = Σ canonical size of each original entry (compact JSON, fair baseline).
    raw_bytes = sum(len(canonical_bytes(e)) for e in entries)

    # encoded = Σ canonical size of each stored keyframe + each stored delta. The
    # deltas are the ones actually persisted (typed-encoded when adopted), so this
    # is the true on-the-wire cost of the delta representation.
    encoded_bytes = sum(
        len(canonical_bytes(seg.keyframe)) for seg in encoded.segments
    ) + sum(
        len(canonical_bytes(d)) for seg in encoded.segments for d in seg.deltas
    )

    # gzip of the concatenated raw entries — the general-compressor baseline.
    gzip_raw_bytes = _gzip_len(b"".join(canonical_bytes(e) for e in entries))

    # gzip on top of the serialized delta stream — the "stacks on top" bonus.
    delta_plus_gzip_bytes = _gzip_len(canonical_bytes(encoded.to_dict()))

    if raw_bytes == 0:
        delta_reduction = 0.0
        gzip_raw_reduction = 0.0
        delta_plus_gzip_reduction = 0.0
        compression_ratio = 0.0
    else:
        delta_reduction = round(100.0 * (raw_bytes - encoded_bytes) / raw_bytes, 2)
        gzip_raw_reduction = round(100.0 * (raw_bytes - gzip_raw_bytes) / raw_bytes, 2)
        delta_plus_gzip_reduction = round(
            100.0 * (raw_bytes - delta_plus_gzip_bytes) / raw_bytes, 2
        )
        compression_ratio = round(encoded_bytes / raw_bytes, 4)

    return CompressionStats(
        count=count,
        keyframe_count=keyframe_count,
        delta_count=delta_count,
        raw_bytes=raw_bytes,
        encoded_bytes=encoded_bytes,
        gzip_raw_bytes=gzip_raw_bytes,
        delta_plus_gzip_bytes=delta_plus_gzip_bytes,
        delta_reduction=delta_reduction,
        gzip_raw_reduction=gzip_raw_reduction,
        delta_plus_gzip_reduction=delta_plus_gzip_reduction,
        compression_ratio=compression_ratio,
    )


class SegmentStore:
    """Thread-safe in-memory holder for one raw batch + its delta encoding.

    Wraps :mod:`app.codec` with byte accounting and bounded reconstruction. The
    encode-time configuration (``keyframe_interval`` / ``baseline`` /
    ``encoder_config``) is fixed at construction and applied on every
    :meth:`compress`, so the stored :class:`~app.codec.EncodedLog` is always
    self-consistent with the store's reported config.
    """

    def __init__(
        self,
        *,
        keyframe_interval: int = 100,
        baseline: str = "previous",
        encoder_config: EncoderConfig | None = None,
        gzip_deltas: bool = False,
    ) -> None:
        """Configure the store. ``encoder_config`` defaults to ``EncoderConfig.all_on()``.

        Defaulting the encoder to *all on* means typed encoding (int-delta +
        string prefix/suffix) is active out of the box; the codec's per-delta size
        guard still guarantees it can never grow a delta. ``gzip_deltas`` is
        carried as configuration metadata (surfaced in :meth:`stats`); it does not
        change what is stored — both gzip baselines are always computed.
        """
        self._keyframe_interval = keyframe_interval
        self._baseline = baseline
        self._encoder_config = (
            encoder_config if encoder_config is not None else EncoderConfig.all_on()
        )
        self._gzip_deltas = gzip_deltas

        self._raw: list[LogEntry] = []
        self._encoded: EncodedLog | None = None
        self._stats: CompressionStats | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Raw batch management.
    # ------------------------------------------------------------------ #
    def set_raw(self, entries: list[LogEntry]) -> int:
        """Store a deep copy of ``entries`` as the pending raw batch (no compress).

        Returns the entry count. The deep copy means a later mutation of the
        caller's list — or of the entries within it — cannot reach into stored
        state. Does not touch the encoded/stats artifacts; call :meth:`compress`
        to (re)build those.
        """
        with self._lock:
            self._raw = copy.deepcopy(entries)
            return len(self._raw)

    def get_raw(self) -> list[LogEntry]:
        """Return a deep copy of the current raw batch (empty list if none)."""
        with self._lock:
            return copy.deepcopy(self._raw)

    def compress(
        self,
        entries: list[LogEntry] | None = None,
        *,
        keyframe_interval: int | None = None,
        baseline: str | None = None,
    ) -> CompressionStats:
        """Delta-encode a batch, store the result, and return its byte accounting.

        When ``entries`` is ``None`` the stored raw batch is used (raising
        ``ValueError`` if none has been set); otherwise a deep copy of ``entries``
        becomes the new raw batch first. The batch is encoded with the store's
        ``encoder_config`` and its configured ``keyframe_interval`` / ``baseline``;
        the resulting :class:`~app.codec.EncodedLog` and its :class:`CompressionStats`
        are saved and the stats returned.

        ``keyframe_interval`` / ``baseline`` are **per-call overrides**: when given
        (not ``None``) they are used for *this* encode only and do **not** change the
        store's configured defaults, so a subsequent ``compress`` with no overrides
        encodes with the original config again. When omitted, the store's configured
        values are used (the original behaviour), so existing ``compress(entries)`` /
        ``compress()`` calls are unaffected.
        """
        with self._lock:
            if entries is None:
                if not self._raw:
                    raise ValueError(
                        "no raw batch to compress: call set_raw() or pass entries"
                    )
                source = self._raw
            else:
                # Adopt a private deep copy as the new raw batch so neither side
                # can mutate the other's data after this call.
                source = copy.deepcopy(entries)
                self._raw = source

            # Resolve per-call overrides against the store's configured defaults. The
            # store's own _keyframe_interval / _baseline are never mutated here, so an
            # override is scoped strictly to this encode.
            kf = keyframe_interval if keyframe_interval is not None else self._keyframe_interval
            base = baseline if baseline is not None else self._baseline

            encoded = encode(
                source,
                keyframe_interval=kf,
                baseline=base,
                encoder_config=self._encoder_config,
            )
            stats = _compute_stats(source, encoded)

            self._encoded = encoded
            self._stats = stats
            return stats

    # ------------------------------------------------------------------ #
    # Reconstruction (delegates to the codec — no diff/apply logic here).
    # ------------------------------------------------------------------ #
    def reconstruct_all(self) -> list[LogEntry]:
        """Reconstruct every entry of the stored batch (empty list if none).

        Delegates to :func:`app.codec.decode`, so the result is element-wise
        canonically equal to the batch last passed to :meth:`compress`.
        """
        with self._lock:
            if self._encoded is None:
                return []
            return decode(self._encoded)

    def reconstruct_index(self, index: int) -> LogEntry:
        """Random-access reconstruct the single entry at global ``index``.

        Delegates to :func:`app.codec.reconstruct_index` (cost bounded by
        ``keyframe_interval``). Raises ``IndexError`` if ``index`` is out of range
        or nothing has been compressed yet.
        """
        with self._lock:
            if self._encoded is None:
                raise IndexError(
                    f"index {index} out of range: nothing compressed yet"
                )
            return reconstruct_index(self._encoded, index)

    def reconstruct_range(self, start: int, end: int) -> list[LogEntry]:
        """Reconstruct the half-open range ``[start, end)``, clamped to ``[0, count]``.

        Negative / oversized bounds are clamped rather than raising, and an empty
        or inverted range yields ``[]``. Decodes the batch once and slices, which
        is comfortably efficient at these batch sizes and reuses the codec wholesale.
        """
        with self._lock:
            if self._encoded is None:
                return []
            count = self._encoded.count
            lo = max(0, min(start, count))
            hi = max(0, min(end, count))
            if hi <= lo:
                return []
            return decode(self._encoded)[lo:hi]

    def page(self, offset: int, limit: int) -> list[LogEntry]:
        """Reconstruct ``limit`` entries starting at ``offset`` (a paging convenience).

        Thin wrapper over :meth:`reconstruct_range(offset, offset + limit)`; a
        non-positive ``limit`` yields ``[]`` (the range collapses).
        """
        return self.reconstruct_range(offset, offset + limit)

    def nearest_keyframe_index(self, index: int) -> int:
        """Global index of the keyframe at or before ``index``.

        Equals ``(index // keyframe_interval) * keyframe_interval`` — the start of
        the segment containing ``index``. Raises ``IndexError`` if ``index`` is out
        of range or nothing has been compressed yet.
        """
        with self._lock:
            if self._encoded is None:
                raise IndexError(
                    f"index {index} out of range: nothing compressed yet"
                )
            if index < 0 or index >= self._encoded.count:
                raise IndexError(
                    f"index {index} out of range for log of length "
                    f"{self._encoded.count}"
                )
            k = self._keyframe_interval
            return (index // k) * k

    # ------------------------------------------------------------------ #
    # Live counts (cheap properties read straight off the stored encoding).
    # ------------------------------------------------------------------ #
    @property
    def count(self) -> int:
        """Number of entries currently stored (0 if nothing compressed)."""
        with self._lock:
            return self._encoded.count if self._encoded is not None else 0

    @property
    def keyframe_count(self) -> int:
        """Number of keyframes stored (one per segment; 0 if nothing compressed)."""
        with self._lock:
            if self._encoded is None:
                return 0
            return len(self._encoded.segments)

    @property
    def delta_count(self) -> int:
        """Number of deltas stored across all segments (0 if nothing compressed)."""
        with self._lock:
            if self._encoded is None:
                return 0
            return sum(len(seg.deltas) for seg in self._encoded.segments)

    # ------------------------------------------------------------------ #
    # Aggregate stats view (for the API / dashboard).
    # ------------------------------------------------------------------ #
    def stats(self) -> dict:
        """Return the current byte accounting merged with live counts + config.

        The base is :meth:`CompressionStats.to_dict`; live ``count`` /
        ``keyframe_count`` / ``delta_count`` (read off the stored encoding) and the
        store's config (``keyframe_interval`` / ``baseline`` / ``gzip_deltas``) are
        merged on top. When nothing has been compressed yet, a fully-formed
        **zeroed** dict (same keys, ``count`` 0, all bytes/reductions 0,
        ``compression_ratio`` 0.0) is returned so callers never ``KeyError``.
        """
        with self._lock:
            if self._stats is not None and self._encoded is not None:
                base = self._stats.to_dict()
                base["count"] = self._encoded.count
                base["keyframe_count"] = len(self._encoded.segments)
                base["delta_count"] = sum(
                    len(seg.deltas) for seg in self._encoded.segments
                )
            else:
                # Well-formed zeroed shape — identical keys to a real stats dict so
                # the dashboard/API can read fields unconditionally.
                base = CompressionStats(
                    count=0,
                    keyframe_count=0,
                    delta_count=0,
                    raw_bytes=0,
                    encoded_bytes=0,
                    gzip_raw_bytes=0,
                    delta_plus_gzip_bytes=0,
                    delta_reduction=0.0,
                    gzip_raw_reduction=0.0,
                    delta_plus_gzip_reduction=0.0,
                    compression_ratio=0.0,
                ).to_dict()

            base["keyframe_interval"] = self._keyframe_interval
            base["baseline"] = self._baseline
            base["gzip_deltas"] = self._gzip_deltas
            return base

    def reset(self) -> None:
        """Clear the raw batch, the encoding, and the stats back to empty."""
        with self._lock:
            self._raw = []
            self._encoded = None
            self._stats = None
