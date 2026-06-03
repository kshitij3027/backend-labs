"""Adaptive compression: codec-by-dtype defaults + learned-best-codec (Feature B).

This module is the foundation of the optimizer's compression strategy. It has
two responsibilities:

1. **ROW framing** — :func:`frame_lz4` / :func:`unframe_lz4` wrap ``lz4.frame``
   so the ROW backend can store its append-only JSONL as a single LZ4 frame and
   read it back byte-for-byte.

2. **Columnar codec choice** — for the Parquet (COLUMNAR / HYBRID-sealed) side
   we pick a per-column codec. :func:`codec_for_dtype` gives a fast, explainable
   default keyed only on a logical dtype string, while
   :class:`CompressionChooser` can *learn* a better codec for a given column by
   trial-compressing a small sample and scoring each candidate on a weighted
   blend of compressed size and round-trip latency.

Design notes (see ``plan.md`` §"Config defaults" and the codec-by-dtype research
notes):

* The defaults are **speed-first** for numeric/temporal data (SNAPPY) and
  **ratio-first** for low-cardinality and long-text columns (GZIP / ZSTD). The
  learner only ever *narrows* this down — if it is disabled or every candidate
  errors, we transparently fall back to :func:`codec_for_dtype`.
* Learning is deliberately simple and bounded: a plain in-memory dict keyed by
  ``(key, dtype)``, samples capped at ``sample_rows`` rows, and a monotonic
  clock (:func:`time.perf_counter`) so timing is immune to wall-clock jumps.
* Every PyArrow call in the learner is wrapped so that a codec which is missing
  or unsupported on a particular platform is *skipped* rather than fatal.
"""
from __future__ import annotations

import io
import time
from typing import TYPE_CHECKING

import lz4.frame
import pyarrow
import pyarrow.parquet

if TYPE_CHECKING:  # pragma: no cover - import only for type checkers
    import pyarrow as _pa  # noqa: F401

__all__ = [
    "VALID_CODECS",
    "frame_lz4",
    "unframe_lz4",
    "codec_for_dtype",
    "CompressionChooser",
]


# Parquet compression codecs this project supports. Kept in sync with the
# candidates the learner trials and the codecs :func:`codec_for_dtype` returns.
# ``LZ4_RAW`` is the Parquet-native LZ4 codec (distinct from the frame format
# used by the ROW backend) and is offered for completeness / manual override.
VALID_CODECS: tuple[str, ...] = ("SNAPPY", "GZIP", "ZSTD", "LZ4_RAW")

# Safe fallback used everywhere a codec cannot be determined.
_DEFAULT_CODEC = "SNAPPY"


# ---------------------------------------------------------------------------
# ROW backend framing
# ---------------------------------------------------------------------------
def frame_lz4(data: bytes) -> bytes:
    """Compress ``data`` into a single self-describing LZ4 frame.

    Thin wrapper over :func:`lz4.frame.compress`. The result carries its own
    frame header, so :func:`unframe_lz4` needs no side-channel length/metadata.
    Round-trips losslessly: ``unframe_lz4(frame_lz4(b)) == b`` for any ``bytes``
    (including ``b""``).

    Args:
        data: Raw bytes to compress (e.g. UTF-8 encoded JSONL).

    Returns:
        The LZ4-framed, compressed bytes.
    """
    return lz4.frame.compress(data)


def unframe_lz4(data: bytes) -> bytes:
    """Decompress a single LZ4 frame produced by :func:`frame_lz4`.

    Thin wrapper over :func:`lz4.frame.decompress`.

    Args:
        data: LZ4-framed bytes as produced by :func:`frame_lz4`.

    Returns:
        The original, decompressed bytes.
    """
    return lz4.frame.decompress(data)


# ---------------------------------------------------------------------------
# Codec-by-dtype defaults
# ---------------------------------------------------------------------------
def codec_for_dtype(dtype: str | None) -> str:
    """Return a sensible default Parquet codec for a logical column ``dtype``.

    This is the rule-based, zero-I/O default used whenever no learned codec is
    available. The mapping reflects the codec-by-dtype research in ``plan.md``:

    * **Temporal** (``timestamp`` / ``datetime`` / ``date`` / ``time``) ->
      ``SNAPPY``. Timestamps compress well and benefit from fast scans; under
      the hood Parquet delta-encodes sorted integer columns, so a light, fast
      codec on top is the right trade-off.
    * **Low-cardinality text** (``text_low_cardinality`` / ``category`` /
      ``enum``) -> ``GZIP``. Parquet dictionary-encodes these to a handful of
      values; GZIP over that dictionary is extremely compact.
    * **High-cardinality / long text** (``text`` / ``message`` /
      ``string_high_cardinality``) -> ``ZSTD``. Best ratio on long, redundant
      text and remains tunable via compression level.
    * **Numeric** (``int`` / ``float`` / ``number`` / ``high_cardinality``) ->
      ``SNAPPY``. Speed-first; numeric columns rarely benefit enough from a
      heavier codec to justify the CPU cost on hot paths.
    * **Unknown / None** -> ``SNAPPY`` (the safe default).

    The lookup is case-insensitive and tolerant of surrounding whitespace.

    Args:
        dtype: A logical dtype string (not an Arrow type), or ``None``.

    Returns:
        One of :data:`VALID_CODECS` (a member of the supported set).
    """
    if not dtype:
        return _DEFAULT_CODEC

    key = dtype.strip().lower()

    # Temporal columns -> SNAPPY (fast scans; Parquet delta-encodes sorted ints).
    if key in {"timestamp", "datetime", "date", "time", "temporal"}:
        return "SNAPPY"

    # Low-cardinality text -> GZIP (dictionary + GZIP is very compact).
    if key in {"text_low_cardinality", "category", "categorical", "enum", "bool", "boolean"}:
        return "GZIP"

    # Long / high-cardinality text -> ZSTD (good ratio on redundant text).
    if key in {"text", "message", "string_high_cardinality", "string", "str"}:
        return "ZSTD"

    # Numeric / high-cardinality -> SNAPPY (speed-first).
    if key in {"int", "integer", "float", "double", "number", "numeric", "high_cardinality"}:
        return "SNAPPY"

    # Unknown -> safe default.
    return _DEFAULT_CODEC


# ---------------------------------------------------------------------------
# Learned-best-codec chooser
# ---------------------------------------------------------------------------
class CompressionChooser:
    """Choose a Parquet codec per column, optionally *learning* the best one.

    The chooser starts from the dtype defaults (:func:`codec_for_dtype`) and,
    when :meth:`learn` is called with a representative sample, trials each
    candidate codec and remembers the winner for that ``(key, dtype)`` pair.
    Scoring blends compressed size and round-trip latency::

        score = size_weight * size_bytes + latency_weight * latency_ms

    The lowest score wins. With the default weights (``size_weight=1.0``,
    ``latency_weight=0.2``) size dominates but a much slower codec is penalised,
    which keeps hot-path reads responsive.

    The learned map is a plain in-memory ``dict`` keyed by ``(key, dtype)``; it
    is intentionally simple and small (one entry per learned column).

    Attributes:
        enabled: When ``False``, :meth:`learn` is a no-op that returns the dtype
            default without storing anything.
        sample_rows: Maximum number of rows trial-compressed during learning.
        size_weight: Weight applied to compressed ``size_bytes`` in the score.
        latency_weight: Weight applied to round-trip ``latency_ms`` in the score.
        candidates: Ordered tuple of codecs to trial during learning.
    """

    def __init__(
        self,
        *,
        enabled: bool = True,
        sample_rows: int = 2000,
        size_weight: float = 1.0,
        latency_weight: float = 0.2,
        candidates: tuple[str, ...] = ("SNAPPY", "GZIP", "ZSTD"),
    ) -> None:
        """Initialise the chooser.

        Args:
            enabled: Master switch for learning (see :meth:`learn`).
            sample_rows: Cap on rows trial-compressed per :meth:`learn` call.
            size_weight: Weight on compressed size in the blended score.
            latency_weight: Weight on round-trip latency (ms) in the score.
            candidates: Codecs to trial. Any entry not in :data:`VALID_CODECS`
                is dropped; if that leaves nothing, the full :data:`VALID_CODECS`
                set (minus the frame-only LZ4) is used as a safe fallback.
        """
        self.enabled = enabled
        self.sample_rows = sample_rows
        self.size_weight = size_weight
        self.latency_weight = latency_weight

        # Keep only codecs we actually support; preserve caller order.
        valid = tuple(c for c in candidates if c in VALID_CODECS)
        self.candidates: tuple[str, ...] = valid or ("SNAPPY", "GZIP", "ZSTD")

        # Learned winners, keyed by (key, dtype). Bounded by the number of
        # distinct columns the engine asks us to learn.
        self._learned: dict[tuple[str, str], str] = {}

    def codec_for(self, name: str, dtype: str) -> str:
        """Return the codec to use for column ``name`` of logical ``dtype``.

        Prefers a previously learned codec for ``(name, dtype)``; otherwise
        falls back to the dtype default.

        Args:
            name: The grouping/column key the codec was (or will be) learned
                under. Treated as opaque.
            dtype: The column's logical dtype string.

        Returns:
            A codec from :data:`VALID_CODECS`.
        """
        learned = self.learned_codec(name, dtype)
        return learned if learned is not None else codec_for_dtype(dtype)

    def learned_codec(self, key: str, dtype: str) -> str | None:
        """Return the remembered winner for ``(key, dtype)`` or ``None``.

        Args:
            key: The opaque grouping key used at :meth:`learn` time.
            dtype: The column's logical dtype string.

        Returns:
            The learned codec, or ``None`` if nothing has been learned yet.
        """
        return self._learned.get((key, dtype))

    def learn(self, key: str, dtype: str, sample_table: "_pa.Table") -> str:
        """Trial each candidate codec on ``sample_table`` and remember the best.

        For each candidate the sample (truncated to :attr:`sample_rows` rows) is
        written to an in-memory Parquet buffer and read back; the compressed
        size and the encode+decode latency are measured and combined into a
        score (lower is better). The winner is stored under ``(key, dtype)`` and
        returned.

        Behaviour / safety:
            * If learning is disabled, returns :func:`codec_for_dtype` *without*
              storing anything.
            * Empty or row-less tables short-circuit to the dtype default
              (also without storing).
            * Each candidate is trialled in isolation; one that raises (e.g. a
              codec unavailable on the host) is skipped. If *every* candidate
              fails, the dtype default is returned and not stored.
            * Timing uses :func:`time.perf_counter` (monotonic), never
              wall-clock time.

        Args:
            key: Opaque grouping key (e.g. ``f"{tenant}:{dtype}"`` or a column
                name) under which the winner is remembered.
            dtype: The column's logical dtype string, used for the fallback and
                as part of the learned-map key.
            sample_table: A representative :class:`pyarrow.Table` to trial.

        Returns:
            The winning codec, or :func:`codec_for_dtype` on any miss.
        """
        default = codec_for_dtype(dtype)

        # Master switch: do no work and remember nothing.
        if not self.enabled:
            return default

        # Guard against empty / row-less / malformed tables.
        try:
            num_rows = sample_table.num_rows
        except AttributeError:
            return default
        if num_rows <= 0:
            return default

        # Bound the work: trial at most ``sample_rows`` rows.
        sample = sample_table
        if self.sample_rows > 0 and num_rows > self.sample_rows:
            sample = sample_table.slice(0, self.sample_rows)

        best_codec: str | None = None
        best_score = float("inf")

        for codec in self.candidates:
            measured = self._measure(sample, codec)
            if measured is None:
                # Codec errored on this platform / for this data — skip it.
                continue
            size_bytes, latency_ms = measured
            score = self.size_weight * size_bytes + self.latency_weight * latency_ms
            if score < best_score:
                best_score = score
                best_codec = codec

        # Every candidate failed -> fall back without remembering.
        if best_codec is None:
            return default

        self._learned[(key, dtype)] = best_codec
        return best_codec

    def _measure(self, table: "_pa.Table", codec: str) -> tuple[int, float] | None:
        """Compress+decompress ``table`` with ``codec``, returning (size, ms).

        Writes the table to an in-memory Parquet buffer and reads it straight
        back, timing the full round-trip with a monotonic clock. Any PyArrow
        error (e.g. an unsupported codec) is swallowed and reported as ``None``
        so the caller can skip this candidate.

        Args:
            table: The (already truncated) sample to trial.
            codec: A Parquet codec name to pass as ``compression=``.

        Returns:
            ``(size_bytes, latency_ms)`` on success, or ``None`` if the codec
            could not be applied.
        """
        try:
            start = time.perf_counter()

            # Encode to an in-memory Parquet buffer.
            buf = io.BytesIO()
            pyarrow.parquet.write_table(table, buf, compression=codec)
            payload = buf.getvalue()

            # Decode it straight back to exercise the read path too.
            pyarrow.parquet.read_table(io.BytesIO(payload))

            latency_ms = (time.perf_counter() - start) * 1000.0
            return len(payload), latency_ms
        except Exception:
            # Unsupported / unavailable codec on this platform — skip it.
            return None
