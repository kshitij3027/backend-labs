"""Typed field encoders — an OPTIONAL, reversible layer on top of field diffs.

The base codec (:mod:`app.codec`) already captures the dominant structural
redundancy of a structured log stream by storing only the *changed* fields of
each entry (the ``"~"`` part of a delta) and the *removed* keys (the ``"-"``
part). That field-diff is the star: it clears the project's reduction target on
its own, with every typed encoder here switched OFF.

This module squeezes the remaining redundancy *inside* the values that did
change, for two value shapes that move predictably between adjacent log lines:

* **Numeric int-delta** — designated counter/timestamp fields (``ts``,
  ``bytes_sent``) advance by a small step each line. Storing ``new - base``
  instead of the full (often 13-digit) integer is shorter and trivially lossless
  for integers. We restrict this to a *named allow-list* of fields, never random
  numerics: a ``status`` flip 200→500 or a ``latency_ms`` of 1473 has no
  monotonic structure to exploit and would only add an envelope.
* **String common-prefix/suffix delta** — ids, endpoints and messages share long
  stems with the previous value (``/v1/login`` → ``/v1/logout``;
  ``"request started"`` → ``"request completed"``). We store the shared
  ``prefix_len`` and ``suffix_len`` plus the differing ``middle`` substring — the
  lightweight VCDIFF-style variant. Prefix/suffix are measured in **code points**
  (normal Python ``str`` slicing), never bytes, so multibyte text (``"café"``,
  ``"日本語"``, emoji) round-trips exactly.

**Integration contract — out-of-band ``"@"`` instructions.** A plain delta stores
changed values verbatim under ``"~"``. The encoder *moves* the eligible ones into
a new ``"@"`` part, replacing each verbatim value with a compact, self-describing,
reversible **instruction** (a JSON list whose first element tags its kind). The
``"-"`` (removed keys) part is carried through untouched. Reconstruction
(:func:`expand_delta`) reads ``"@"`` against the **base entry** the delta was
computed against — the previous original in ``previous`` mode, the segment
keyframe in ``keyframe`` mode — and rebuilds a *plain* delta, which the frozen
commit-4 :func:`~app.codec.apply_delta` then applies unchanged.

This keeps the commit-4 diff/apply primitives completely untouched: the encoder
transforms deltas *around* them, never inside them. The instructions are
self-describing, so :func:`expand_delta` needs no configuration to reverse them —
only the base entry. Crucially, :func:`expand_delta` is a transparent **no-op**
for any delta without a ``"@"`` part, so plain commit-4/commit-5 deltas pass
through byte-identically.

This module imports **only stdlib** (``json``/``dataclasses``/``typing``) and
deliberately does **not** import :mod:`app.codec`; the dependency is one-way
(codec imports encoders) to avoid a circular import. The size comparison that
guarantees typed encoding never *grows* a delta lives in the codec, not here:
:func:`compress_delta` always produces the typed candidate, and the codec adopts
it only when its canonical form is strictly smaller.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# A log entry is a plain JSON-native ``dict[str, Any]`` (see ``app/models.py``).
# Re-declared structurally here rather than imported so this module stays free of
# any ``app`` import beyond the type alias' shape (it must not pull in app.codec,
# and app.models drags in pydantic). This is purely a type alias.
LogEntry = dict[str, Any]

# Delta wire keys. ``"@"`` is the out-of-band instruction part introduced by this
# layer; ``"~"`` (changed values) and ``"-"`` (removed keys) mirror the codec's
# own constants. Kept local (no app.codec import) but intentionally identical.
CHANGED_KEY = "~"   # maps key -> new value for the still-verbatim changed fields
REMOVED_KEY = "-"   # sorted list of keys removed from base -> current (untouched)
ENCODED_KEY = "@"   # maps key -> a self-describing reversible instruction list

# Instruction tags (first element of each instruction list).
INT_DELTA_TAG = "i"  # ["i", new - base]                  -> base + delta
STR_DELTA_TAG = "s"  # ["s", prefix_len, suffix_len, mid]  -> base[:p] + mid + base[len-s:]


@dataclass(frozen=True)
class EncoderConfig:
    """Which typed encoders are active, and for which fields.

    Frozen so a single config can be shared freely without any risk of a caller
    mutating it mid-encode. The defaults are the *conservative* set: the whole
    layer is **off** unless explicitly enabled, and even when enabled the
    int-delta encoder only touches the named counter/timestamp fields.

    Attributes:
        enabled: Master switch. When ``False`` the layer is a complete no-op —
            :func:`compress_delta` returns its input delta unchanged, so the
            codec's output is byte-identical to the plain (pre-encoder) form.
        int_delta_fields: The *only* fields eligible for numeric int-delta. These
            are designated counters/timestamps with monotonic-ish structure, not
            arbitrary numerics — a ``status`` or ``latency_ms`` is never
            int-delta'd because it is not in this set.
        str_delta: When ``True``, changed string fields get common-prefix/suffix
            delta encoding (measured on code points). When ``False``, strings are
            left verbatim in ``"~"``.
    """

    enabled: bool = False
    int_delta_fields: frozenset[str] = field(
        default_factory=lambda: frozenset({"ts", "bytes_sent"})
    )
    str_delta: bool = True

    @classmethod
    def disabled(cls) -> "EncoderConfig":
        """The off switch: a config with ``enabled=False`` (defaults otherwise).

        With this config the encoder layer is a guaranteed no-op end to end, so
        ``encode(..., encoder_config=EncoderConfig.disabled())`` is byte-identical
        to ``encode(...)`` with no config at all.
        """
        return cls(enabled=False)

    @classmethod
    def all_on(cls) -> "EncoderConfig":
        """Everything on: ``enabled=True`` with the default field allow-list.

        Enables both int-delta (for the designated ``{"ts", "bytes_sent"}``
        fields) and string prefix/suffix delta. The codec's per-delta size guard
        still ensures no individual delta ever grows.
        """
        return cls(enabled=True)


def _common_prefix_len(a: str, b: str) -> int:
    """Length (in code points) of the longest common prefix of ``a`` and ``b``.

    Operates on normal Python ``str`` iteration, which is code-point based, so the
    boundary it reports is always a valid character boundary — never a split
    multibyte sequence. Bounded by ``min(len(a), len(b))``.
    """
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return i


def _common_suffix_len(a: str, b: str) -> int:
    """Length (in code points) of the longest common suffix of ``a`` and ``b``.

    Walks from the end using negative indexing on code points (again never a byte
    split). Bounded by ``min(len(a), len(b))``. Callers pass the *remainders after
    the common prefix*, which keeps prefix and suffix from overlapping.
    """
    n = min(len(a), len(b))
    i = 0
    while i < n and a[-1 - i] == b[-1 - i]:
        i += 1
    return i


def compress_delta(delta: dict, base: LogEntry, config: EncoderConfig) -> dict:
    """Produce a typed *candidate* delta, moving eligible values into ``"@"``.

    For each ``(key, new_value)`` in ``delta["~"]`` (the verbatim changed values),
    decide whether a typed, reversible instruction is applicable against ``base``
    (the entry this delta was computed against):

    * **int-delta** when ``key`` is in :attr:`EncoderConfig.int_delta_fields` and
      both ``new_value`` and ``base[key]`` are real ``int`` (a ``bool`` is
      *excluded* even though it is an ``int`` subclass — encoding ``True`` as a
      numeric delta would lose its type on reconstruction). Emits
      ``["i", new_value - base[key]]``.
    * **str-delta** (when :attr:`EncoderConfig.str_delta`) when both values are
      ``str`` and ``key`` is in ``base``. The common prefix length ``p`` is
      computed first; the common suffix length ``s`` is then computed on the
      *remainders after the prefix* (``base[key][p:]`` vs ``new_value[p:]``) so
      prefix and suffix never overlap (guaranteeing ``p + s <= min(len(base[key]),
      len(new_value))``). ``middle = new_value[p:len(new_value) - s]``. Emits
      ``["s", p, s, middle]``. Even a degenerate ``p == s == 0`` is a valid,
      reversible instruction; the codec's size guard discards it if it isn't
      actually smaller.
    * **otherwise** the value stays verbatim in ``"~"``.

    Returns a fresh delta dict with up to three parts — ``"~"`` (still-verbatim
    changed values), ``"@"`` (the instructions) and ``"-"`` (removed keys carried
    through unchanged) — each omitted when empty. When ``config.enabled`` is
    ``False`` the input ``delta`` is returned unchanged (identity), so the layer
    is a true no-op when off.

    This function does **not** size-compare against the plain delta; it always
    yields the typed candidate and lets the codec decide whether to adopt it
    (adopting only when strictly smaller). ``base`` and ``delta`` are never
    mutated.
    """
    if not config.enabled:
        return delta

    changed_in = delta.get(CHANGED_KEY, {})

    verbatim: dict[str, Any] = {}
    instructions: dict[str, list] = {}

    for key, new_value in changed_in.items():
        # int-delta: designated field, both sides real ints (bools excluded).
        if (
            key in config.int_delta_fields
            and isinstance(new_value, int)
            and not isinstance(new_value, bool)
            and key in base
            and isinstance(base[key], int)
            and not isinstance(base[key], bool)
        ):
            instructions[key] = [INT_DELTA_TAG, new_value - base[key]]
        # str-delta: both sides strings, key present in base.
        elif (
            config.str_delta
            and isinstance(new_value, str)
            and key in base
            and isinstance(base[key], str)
        ):
            base_str = base[key]
            p = _common_prefix_len(base_str, new_value)
            # Suffix on the REMAINDER after the prefix so the two never overlap.
            s = _common_suffix_len(base_str[p:], new_value[p:])
            middle = new_value[p:len(new_value) - s]
            instructions[key] = [STR_DELTA_TAG, p, s, middle]
        else:
            # Not eligible — leave it verbatim for the codec to store as-is.
            verbatim[key] = new_value

    out: dict[str, Any] = {}
    if verbatim:
        out[CHANGED_KEY] = verbatim
    if instructions:
        out[ENCODED_KEY] = instructions
    # Carry removed keys through verbatim (never touched by the typed layer).
    if REMOVED_KEY in delta:
        out[REMOVED_KEY] = delta[REMOVED_KEY]
    return out


def expand_delta(typed_delta: dict, base: LogEntry) -> dict:
    """Reverse :func:`compress_delta` back into a *plain* (commit-4) delta.

    Fast path: if there is no ``"@"`` part, ``typed_delta`` is already a plain
    delta (or was never typed) and is returned **unchanged** — this is what makes
    the wrapper a transparent no-op for every commit-4/commit-5 delta, preserving
    byte-identical behaviour when the encoder layer is off.

    Otherwise a plain delta is rebuilt against ``base`` (the entry the delta was
    computed against — the running reconstructed entry in ``previous`` mode, the
    keyframe in ``keyframe`` mode):

    * start from a copy of the verbatim ``"~"`` values;
    * for each ``["i", d]`` instruction, set ``base[key] + d``;
    * for each ``["s", p, s, middle]`` instruction, set
      ``base[key][:p] + middle + base[key][len-s:]`` (the trailing slice omitted
      when ``s == 0`` so ``base[key][len:]`` never produces ``""`` ambiguity).

    The returned dict has only ``"~"`` (changed values, omitted if empty) and
    ``"-"`` (removed keys, carried through if present) — **never** a ``"@"`` part,
    because it is fully expanded. The result is exactly the plain delta the codec
    would have stored without encoders, so feeding it to the frozen
    :func:`~app.codec.apply_delta` reconstructs the original entry. Neither
    ``typed_delta`` nor ``base`` is mutated.

    Raises:
        ValueError: if an instruction carries an unknown tag (corrupt / future
            format) — fail loudly rather than silently drop the field.
    """
    if ENCODED_KEY not in typed_delta:
        # No typed instructions: already a plain delta. Identity passthrough.
        return typed_delta

    # Rebuild the full set of changed values: verbatim ones plus expanded ones.
    changed: dict[str, Any] = dict(typed_delta.get(CHANGED_KEY, {}))

    for key, instr in typed_delta[ENCODED_KEY].items():
        tag = instr[0]
        if tag == INT_DELTA_TAG:
            # ["i", delta] -> base[key] + delta
            changed[key] = base[key] + instr[1]
        elif tag == STR_DELTA_TAG:
            # ["s", prefix_len, suffix_len, middle] -> stitched string.
            p, s, middle = instr[1], instr[2], instr[3]
            b = base[key]
            changed[key] = b[:p] + middle + (b[len(b) - s:] if s else "")
        else:
            raise ValueError(f"unknown encoder instruction tag: {tag!r}")

    plain: dict[str, Any] = {}
    if changed:
        plain[CHANGED_KEY] = changed
    if REMOVED_KEY in typed_delta:
        plain[REMOVED_KEY] = typed_delta[REMOVED_KEY]
    return plain
