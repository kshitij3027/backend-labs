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

import json
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
