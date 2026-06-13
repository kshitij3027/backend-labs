"""Core delta codec: canonical serialization + adjacent-entry field diff/apply.

This module is the **fidelity surface** of the engine. A structured log stream is
extraordinarily repetitive — consecutive entries share almost every field and only
a couple of values move. Rather than re-store every field on every line, the engine
stores an occasional full **keyframe** (a complete entry) and, for each entry in
between, only the **field-level delta** from the previous entry. Reconstruction
replays the deltas forward from the nearest keyframe.

A :data:`~app.models.LogEntry` is a plain JSON-native ``dict[str, Any]`` (see
``app/models.py``). We deliberately do **not** wrap it in a fixed schema: the set of
keys varies from entry to entry (an ``error`` field that exists only on ERROR lines,
say), and that varying shape is exactly the structural redundancy delta encoding
exploits.

**Fidelity basis — canonical JSON.** Equality between entries is defined by their
*canonical* JSON encoding (sorted keys, compact separators, real UTF-8 — see
:func:`canonical_bytes`), **not** by any incidental whitespace or key ordering in the
caller's input. Sorting keys also canonicalizes nested objects recursively, so two
dicts that differ only in key order are equal here. The round-trip contract the rest
of the engine relies on is::

    entries_equal(apply_delta(prev, diff_entries(prev, cur)), cur)  # always True

**Delta wire shape (field level).** A delta is a dict with up to two keys:

* ``"~"`` → a dict of keys that were **added or changed**, mapping key → the *new*
  value taken from ``cur`` (the whole value, even for nested dicts/lists).
* ``"-"`` → a **sorted** list of keys that were **removed** (present in ``prev``,
  absent from ``cur``).

Either part is omitted entirely when empty; an unchanged pair yields the empty
delta ``{}``.

**Null vs missing — classify by key-set membership, never ``.get()``.** A key set to
JSON ``null`` is *present* with value ``None``; that is categorically different from a
key being absent. ``diff_entries({"a": 1}, {"a": None})`` is a *change* to null
(``{"~": {"a": None}}``), whereas ``diff_entries({"a": 1}, {})`` is a *removal*
(``{"-": ["a"]}``). Using ``dict.get`` would conflate these two cases, so membership
(``k in prev`` / ``k in cur``) is the only test used below.

**Nested objects are opaque whole values (no recursive diff in v1).** When a nested
dict or list changes, the entire new value is re-emitted under ``"~"``. Structural
(``!=``) comparison in Python already does the right thing — it compares nested
content regardless of key order — so a nested value that differs only in key order
compares equal and is correctly omitted.

The keyframe/segment layer (keyframe-every-N, ``reconstruct(index)`` replay, baseline
modes) is built on top of these primitives in a later commit; the function
names/signatures here are the stable foundation it depends on.
"""
from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any

from app.models import LogEntry

# Delta wire keys. Kept as module constants so the segment/keyframe layer (and tests)
# can reference the exact tokens instead of re-typing the literals.
CHANGED_KEY = "~"  # maps key -> new value for added/changed fields
REMOVED_KEY = "-"  # sorted list of keys removed from prev -> cur


# --------------------------------------------------------------------------- #
# Canonical serialization — the equality and byte-accounting basis.
# --------------------------------------------------------------------------- #
def canonical_json(obj: Any) -> str:
    """Deterministic JSON text: sorted keys, compact separators, real UTF-8.

    ``sort_keys=True`` makes the encoding independent of input key order (recursively,
    including nested objects); ``separators=(",", ":")`` strips all incidental
    whitespace; ``ensure_ascii=False`` keeps non-ASCII characters as real UTF-8 code
    points rather than ``\\uXXXX`` escapes, so byte accounting reflects the true
    payload. This is the single source of truth for entry equality and for the byte
    sizes the metrics layer reports.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_bytes(obj: Any) -> bytes:
    """UTF-8 encoding of :func:`canonical_json` — the canonical byte form of ``obj``."""
    return canonical_json(obj).encode("utf-8")


def entries_equal(a: Any, b: Any) -> bool:
    """True iff ``a`` and ``b`` have identical canonical bytes.

    This is the fidelity predicate the round-trip contract is stated in terms of: it
    ignores key order and incidental formatting and compares the canonical payloads.
    """
    return canonical_bytes(a) == canonical_bytes(b)


# --------------------------------------------------------------------------- #
# Adjacent-entry delta: diff (prev -> cur) and apply (prev + delta -> cur).
# --------------------------------------------------------------------------- #
def diff_entries(prev: LogEntry, cur: LogEntry) -> dict:
    """Compute the field-level delta transforming ``prev`` into ``cur``.

    Classification is strictly by **key-set membership** (never ``dict.get``), so a
    value set to ``None`` is treated as a present value, not as an absence:

    * key ``k`` → ``"~"`` (with value ``cur[k]``) iff ``k`` is new (``k not in prev``)
      or its value changed (``prev[k] != cur[k]``);
    * key ``k`` → ``"-"`` iff it was removed (``k in prev and k not in cur``);
    * keys present in both with equal values are omitted.

    Nested dict/list values are treated as **opaque whole values**: ``!=`` compares
    them structurally (so key-order-only nested differences are *not* a change), and a
    genuinely changed nested value is re-emitted whole under ``"~"``. ``"-"`` is stored
    as ``sorted(removed_keys)`` so the delta itself has a canonical, deterministic form.
    The empty delta ``{}`` is returned when nothing changed (in particular for
    ``diff_entries(x, x)``).
    """
    changed: dict[str, Any] = {}
    for k in cur:
        # Added (not in prev) OR changed (structurally unequal value). Equal values —
        # including two equal nulls or two nested structures equal up to key order —
        # fall through and are omitted.
        if k not in prev or prev[k] != cur[k]:
            changed[k] = cur[k]

    removed = [k for k in prev if k not in cur]

    delta: dict[str, Any] = {}
    if changed:
        delta[CHANGED_KEY] = changed
    if removed:
        # Sorted for canonical determinism (so canonical_bytes(delta) is stable
        # regardless of prev's iteration order).
        delta[REMOVED_KEY] = sorted(removed)
    return delta


def apply_delta(prev: LogEntry, delta: dict) -> LogEntry:
    """Reconstruct ``cur`` from ``prev`` and ``delta`` without mutating ``prev``.

    Starts from a shallow copy of ``prev`` (correct here because values are treated
    immutably and every ``"~"`` value came fresh from ``cur`` during diffing), removes
    each key listed in ``delta["-"]``, then applies the added/changed values from
    ``delta["~"]``. The two parts are disjoint by construction, so doing removals
    before additions is purely conventional. Inverse of :func:`diff_entries`:
    ``apply_delta(prev, diff_entries(prev, cur))`` is canonically equal to ``cur``.
    """
    result = dict(prev)  # shallow copy — never mutate the caller's prev
    for k in delta.get(REMOVED_KEY, []):
        result.pop(k, None)
    result.update(delta.get(CHANGED_KEY, {}))
    return result


# --------------------------------------------------------------------------- #
# Segment / keyframe layer — keyframe-every-N, whole-batch decode, and bounded
# random access. Built strictly on top of the adjacent-entry primitives above
# (``diff_entries`` / ``apply_delta`` / ``entries_equal``): this layer never
# reimplements field diffing, it only orchestrates *where* keyframes fall and
# *which* baseline each delta is computed against.
#
# **Segment geometry.** With keyframe interval ``K``, keyframes fall at global
# indices ``0, K, 2K, …``. Segment ``s`` covers the half-open range
# ``[s*K, (s+1)*K)``: the entry at ``s*K`` is the keyframe (stored full) and the
# remaining (up to ``K-1``) entries are stored as deltas. The final segment is
# short when ``count`` is not a multiple of ``K``; with ``K == 1`` every entry is
# its own keyframe and every ``deltas`` list is empty.
#
# **Baseline modes** (how each delta's *prev* operand is chosen):
#
# * ``"previous"`` — each delta diffs against the immediately preceding ORIGINAL
#   entry (the keyframe for the first delta in a segment, otherwise the prior
#   entry). Smallest diffs / best ratio; reconstructing offset ``o`` replays the
#   chain ``keyframe → delta[0] → … → delta[o-1]`` (up to ``K-1`` applies).
# * ``"keyframe"`` — each delta diffs against the segment keyframe. Slightly
#   larger diffs, but reconstruction of any entry is a single ``apply_delta`` hop
#   from the keyframe.
#
# Both modes are exact inverses of encoding: ``decode(encode(entries))`` and
# ``reconstruct_index(encode(entries), i)`` are canonically equal to the inputs.
# --------------------------------------------------------------------------- #
@dataclass
class Segment:
    """One keyframe plus the ordered deltas for the entries that follow it.

    ``start_index`` is the global index of the keyframe (always a multiple of the
    encode-time ``keyframe_interval``). ``keyframe`` is a full :data:`LogEntry`
    stored verbatim (a deep copy, so caller mutations of decoded entries cannot
    corrupt it). ``deltas`` holds one field-level delta per non-keyframe entry in
    the segment, in global order; the meaning of each delta's baseline (previous
    vs keyframe) is fixed by the owning :class:`EncodedLog`'s ``baseline``.
    """

    start_index: int
    keyframe: LogEntry
    deltas: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Serialize to a JSON-native dict (deep-copied, so the result is detached)."""
        return {
            "start_index": self.start_index,
            "keyframe": copy.deepcopy(self.keyframe),
            "deltas": copy.deepcopy(self.deltas),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Segment":
        """Inverse of :meth:`to_dict`; deep-copies so the Segment owns its data."""
        return cls(
            start_index=d["start_index"],
            keyframe=copy.deepcopy(d["keyframe"]),
            deltas=copy.deepcopy(d["deltas"]),
        )


@dataclass
class EncodedLog:
    """A full delta-encoded log: the segment list plus the metadata to decode it.

    ``count`` is the number of ORIGINAL entries (the decode contract:
    ``len(decode(self)) == self.count``). ``keyframe_interval`` (``K``) and
    ``baseline`` record exactly how the segments were produced, so decoding /
    random access need no external configuration — an :class:`EncodedLog` is a
    self-describing, serializable artifact the store and API can persist.
    """

    count: int
    keyframe_interval: int
    baseline: str
    segments: list[Segment] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Serialize to a JSON-native dict (``json.dumps``-safe)."""
        return {
            "count": self.count,
            "keyframe_interval": self.keyframe_interval,
            "baseline": self.baseline,
            "segments": [s.to_dict() for s in self.segments],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "EncodedLog":
        """Inverse of :meth:`to_dict`; round-trips to an equivalent EncodedLog."""
        return cls(
            count=d["count"],
            keyframe_interval=d["keyframe_interval"],
            baseline=d["baseline"],
            segments=[Segment.from_dict(s) for s in d["segments"]],
        )


def _validate_params(keyframe_interval: int, baseline: str) -> None:
    """Raise ``ValueError`` for an out-of-range interval or unknown baseline."""
    if keyframe_interval < 1:
        raise ValueError(f"keyframe_interval must be >= 1, got {keyframe_interval}")
    if baseline not in ("previous", "keyframe"):
        raise ValueError(
            f"baseline must be 'previous' or 'keyframe', got {baseline!r}"
        )


def encode(
    entries: list[LogEntry],
    *,
    keyframe_interval: int = 100,
    baseline: str = "previous",
) -> EncodedLog:
    """Delta-encode ``entries`` into keyframe + delta segments.

    A full keyframe is emitted every ``keyframe_interval`` (``K``) entries; the
    entries between keyframes are stored as field-level deltas whose baseline is
    chosen by ``baseline`` (see the module section above). The input ``entries``
    list is never mutated, and each stored keyframe is a deep copy of its source
    entry so later caller mutations of decoded entries cannot corrupt the encoded
    form. ``encode([], …)`` yields an empty :class:`EncodedLog` with no segments.

    Raises ``ValueError`` if ``keyframe_interval < 1`` or ``baseline`` is not one
    of ``"previous"`` / ``"keyframe"``.
    """
    _validate_params(keyframe_interval, baseline)

    count = len(entries)
    encoded = EncodedLog(
        count=count,
        keyframe_interval=keyframe_interval,
        baseline=baseline,
        segments=[],
    )
    if count == 0:
        return encoded

    k = keyframe_interval
    # Iterate segment by segment over the global index space. ``start`` is always a
    # multiple of K and marks the keyframe; the segment ends at min(start+K, count).
    for start in range(0, count, k):
        end = min(start + k, count)
        keyframe_entry = entries[start]
        deltas: list[dict] = []
        # Diff every following entry in this segment against its baseline. In
        # ``previous`` mode the baseline walks forward (keyframe → entry[start+1]
        # → …); in ``keyframe`` mode it is pinned to the segment keyframe.
        for i in range(start + 1, end):
            if baseline == "previous":
                deltas.append(diff_entries(entries[i - 1], entries[i]))
            else:  # baseline == "keyframe"
                deltas.append(diff_entries(keyframe_entry, entries[i]))
        encoded.segments.append(
            Segment(
                start_index=start,
                # Deep copy so a later mutation of a decoded entry (which shares no
                # structure with this) — and, defensively, any caller mutation of
                # the original — cannot reach back into the encoded keyframe.
                keyframe=copy.deepcopy(keyframe_entry),
                deltas=deltas,  # freshly built dicts from diff_entries — already safe
            )
        )
    return encoded


def decode(encoded: EncodedLog) -> list[LogEntry]:
    """Reconstruct every original entry, in order, into a fresh list.

    For each segment the keyframe is emitted first (as a copy), then each delta is
    replayed: in ``previous`` mode against a running ``current`` (the chain
    keyframe → delta[0] → …), in ``keyframe`` mode always against the segment
    keyframe (single hop). The returned list satisfies
    ``len(decode(enc)) == enc.count`` and is element-wise canonically equal to the
    entries originally passed to :func:`encode`.
    """
    baseline = encoded.baseline
    result: list[LogEntry] = []
    for seg in encoded.segments:
        # Emit the keyframe as a fresh copy so the returned list never aliases the
        # EncodedLog's stored keyframe (callers may mutate decoded entries freely).
        result.append(copy.deepcopy(seg.keyframe))
        if baseline == "previous":
            current = seg.keyframe
            for delta in seg.deltas:
                current = apply_delta(current, delta)  # builds a new dict each hop
                result.append(current)
        else:  # baseline == "keyframe"
            for delta in seg.deltas:
                result.append(apply_delta(seg.keyframe, delta))
    return result


def reconstruct_index(encoded: EncodedLog, index: int) -> LogEntry:
    """Random-access reconstruct the single entry at global ``index``.

    Locates segment ``s = index // K`` and offset ``o = index % K``. Offset 0 is
    the keyframe (returned as a copy). Otherwise, in ``previous`` mode the chain
    ``keyframe → deltas[0] → … → deltas[o-1]`` is replayed (at most ``K-1``
    applies); in ``keyframe`` mode the answer is a single
    ``apply_delta(keyframe, deltas[o-1])``. Either way the cost is bounded by
    ``keyframe_interval`` delta applications regardless of ``index`` — the whole
    reason keyframes exist. Raises ``IndexError`` if ``index`` is out of range
    (in particular for any index into an empty log).
    """
    if index < 0 or index >= encoded.count:
        raise IndexError(
            f"index {index} out of range for log of length {encoded.count}"
        )

    k = encoded.keyframe_interval
    seg = encoded.segments[index // k]
    offset = index % k

    if offset == 0:
        return copy.deepcopy(seg.keyframe)

    if encoded.baseline == "previous":
        # Replay the chain from the keyframe up to (and including) deltas[offset-1].
        current = seg.keyframe
        for j in range(offset):
            current = apply_delta(current, seg.deltas[j])
        return current

    # baseline == "keyframe": single hop from the keyframe.
    return apply_delta(seg.keyframe, seg.deltas[offset - 1])


def keyframe_indices(encoded: EncodedLog) -> list[int]:
    """Global indices of the keyframes actually present (one per segment).

    Equals ``[0, K, 2K, …]`` truncated to the number of segments. Handy for the
    store's nearest-keyframe lookup and for tests asserting segment geometry.
    """
    return [seg.start_index for seg in encoded.segments]
