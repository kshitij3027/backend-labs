"""Unit tests for :mod:`app.codec` — the fidelity surface of the engine.

This is the highest-risk module: every byte the encoder stores and every entry it
later reconstructs flows through ``diff_entries`` / ``apply_delta``, and equality
is defined by ``canonical_bytes``. The tests below pin down the *exact* contract,
not just "round-trips usually work":

* **Hard wire-shape invariants** — each asserted with exact ``==`` on the produced
  delta dict, covering the null-vs-missing distinction (classified by key-set
  membership, never ``.get()``), the empty delta, nested opacity (whole-value
  re-emit), key-order-only nested non-changes, and the *sorted* removed list.
* **Round-trip fidelity** — ``entries_equal(apply_delta(prev, diff_entries(prev,
  cur)), cur)`` over a large hand-crafted matrix of edit kinds, plus the literal
  Python-object reconstruction equalling ``cur``.
* **Generator-driven chains** — adjacent-pair *and* cumulative-replay round-trips
  over realistic churny streams from :func:`app.generator.generate_logs`, across
  several seeds and the full churn spectrum (0.0 → 1.0).
* **No mutation** — diffing and applying never mutate the caller's ``prev``/``cur``
  (verified against a pre-call ``deepcopy``).
* **Canonical serialization** — sorted keys, compact separators, key-order-blind
  equality, and full Unicode fidelity (real UTF-8 round-trip, ``ensure_ascii=False``,
  no ``\\uXXXX`` escapes).
"""
from __future__ import annotations

import copy
import json

import pytest

from app.codec import (
    canonical_bytes,
    canonical_json,
    diff_entries,
    entries_equal,
    apply_delta,
)
from app.generator import generate_logs

# Mirror the module's wire tokens locally so the assertions read against the
# documented constants rather than re-typed literals scattered through the file.
CHANGED = "~"
REMOVED = "-"


# --------------------------------------------------------------------------- #
# Hard invariant 1: null-vs-missing is classified by membership, never .get().
#   A key set to JSON null is *present* (a change-to-null), categorically unlike
#   an absent key (a removal). These four cases are the crux of the whole codec.
# --------------------------------------------------------------------------- #
def test_value_to_null_is_a_change_not_a_removal():
    """``{"a":1}`` -> ``{"a":None}`` is a change to null under ``"~"``."""
    assert diff_entries({"a": 1}, {"a": None}) == {"~": {"a": None}}


def test_key_dropped_is_a_removal():
    """``{"a":1}`` -> ``{}`` is a removal under ``"-"`` (not a null change)."""
    assert diff_entries({"a": 1}, {}) == {"-": ["a"]}


def test_added_null_value_is_a_change():
    """``{}`` -> ``{"a":None}`` adds a present null value under ``"~"``."""
    assert diff_entries({}, {"a": None}) == {"~": {"a": None}}


def test_two_equal_nulls_yield_empty_delta():
    """``{"a":None}`` -> ``{"a":None}`` is unchanged (empty delta)."""
    assert diff_entries({"a": None}, {"a": None}) == {}


# --------------------------------------------------------------------------- #
# Hard invariant 2: diff_entries(x, x) == {} for varied shapes.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "x",
    [
        {},
        {"a": 1},
        {"a": 1, "b": "two", "c": True},
        {"a": None},
        {"a": 1, "b": None, "c": "x"},
        {"m": {"x": 1, "y": 2}, "n": [1, 2, 3]},
        {"deep": {"l1": {"l2": {"l3": [1, {"k": None}]}}}},
        {"a": 1, "b": {"x": None, "y": [True, False]}, "c": None},
    ],
    ids=[
        "empty",
        "flat-single",
        "flat-mixed",
        "single-null",
        "flat-with-null",
        "nested-dict-and-list",
        "deeply-nested-with-null",
        "mixed-nested-and-null",
    ],
)
def test_diff_of_identical_is_empty(x):
    """Diffing any entry against itself yields the empty delta ``{}``."""
    assert diff_entries(x, x) == {}


# --------------------------------------------------------------------------- #
# Hard invariant 3: nested values are opaque whole values.
# --------------------------------------------------------------------------- #
def test_nested_change_reemits_whole_value():
    """A changed nested dict is re-emitted *whole* under ``"~"`` (no sub-diff)."""
    out = diff_entries({"m": {"x": 1, "y": 2}}, {"m": {"x": 1, "y": 3}})
    assert out == {"~": {"m": {"x": 1, "y": 3}}}


def test_nested_key_order_only_is_not_a_change():
    """A nested dict differing only in key order compares equal → empty delta."""
    out = diff_entries({"m": {"x": 1, "y": 2}}, {"m": {"y": 2, "x": 1}})
    assert out == {}


def test_nested_list_reorder_is_a_change():
    """Lists are order-significant: reordering elements is a genuine change."""
    out = diff_entries({"m": [1, 2, 3]}, {"m": [3, 2, 1]})
    assert out == {"~": {"m": [3, 2, 1]}}


# --------------------------------------------------------------------------- #
# Hard invariant 4: the removed list is sorted (canonical, deterministic).
# --------------------------------------------------------------------------- #
def test_removed_list_is_sorted():
    """Removed keys are emitted as ``sorted(...)`` regardless of input order."""
    assert diff_entries({"b": 1, "a": 1, "c": 1}, {}) == {"-": ["a", "b", "c"]}


def test_removed_list_is_sorted_with_scrambled_order():
    """A more scrambled removal set still comes out fully sorted."""
    prev = {"zebra": 1, "alpha": 1, "mike": 1, "bravo": 1, "delta": 1}
    assert diff_entries(prev, {}) == {
        "-": ["alpha", "bravo", "delta", "mike", "zebra"]
    }


def test_simultaneous_change_and_remove():
    """A delta can carry both ``"~"`` (add/change) and ``"-"`` (sorted removals)."""
    prev = {"a": 1, "b": 2, "c": 3, "d": 4}
    cur = {"a": 1, "b": 99, "e": 5}  # b changed, e added, c & d removed
    assert diff_entries(prev, cur) == {"~": {"b": 99, "e": 5}, "-": ["c", "d"]}


def test_changed_part_omitted_when_only_removals():
    """When nothing was added/changed, only ``"-"`` appears (no empty ``"~"``)."""
    out = diff_entries({"a": 1, "b": 2}, {"a": 1})
    assert out == {"-": ["b"]}
    assert CHANGED not in out


def test_removed_part_omitted_when_only_changes():
    """When nothing was removed, only ``"~"`` appears (no empty ``"-"``)."""
    out = diff_entries({"a": 1}, {"a": 2, "b": 3})
    assert out == {"~": {"a": 2, "b": 3}}
    assert REMOVED not in out


# --------------------------------------------------------------------------- #
# Round-trip fidelity over a hand-crafted matrix of edit kinds.
#   Each pair exercises a distinct add / remove / null-transition / nested /
#   reorder / key-scramble / empty-boundary case. For every pair we assert both
#   the canonical-equality contract AND literal Python-object reconstruction.
# --------------------------------------------------------------------------- #
ROUNDTRIP_PAIRS = [
    # (id, prev, cur)
    ("noop-flat", {"a": 1, "b": 2}, {"a": 1, "b": 2}),
    ("noop-empty", {}, {}),
    ("add-key", {"a": 1}, {"a": 1, "b": 2}),
    ("remove-key", {"a": 1, "b": 2}, {"a": 1}),
    ("change-value", {"a": 1}, {"a": 2}),
    ("value-to-null", {"a": 1}, {"a": None}),
    ("null-to-value", {"a": None}, {"a": 7}),
    ("null-to-null", {"a": None}, {"a": None}),
    ("add-null", {}, {"a": None}),
    ("remove-null", {"a": None}, {}),
    ("empty-to-nonempty", {}, {"a": 1, "b": "x", "c": None}),
    ("nonempty-to-empty", {"a": 1, "b": "x", "c": None}, {}),
    ("nested-dict-change", {"m": {"x": 1, "y": 2}}, {"m": {"x": 1, "y": 9}}),
    ("nested-dict-reorder", {"m": {"x": 1, "y": 2}}, {"m": {"y": 2, "x": 1}}),
    ("nested-list-change", {"m": [1, 2, 3]}, {"m": [1, 2, 4]}),
    ("nested-list-reorder", {"m": [1, 2, 3]}, {"m": [3, 2, 1]}),
    ("nested-add-remove", {"m": {"x": 1}}, {"m": {"y": 2}}),
    (
        "scramble-top-order",
        {"a": 1, "b": 2, "c": 3, "d": 4},
        {"d": 4, "c": 3, "b": 2, "a": 1},
    ),
    (
        "mixed-everything",
        {"a": 1, "b": 2, "c": 3, "keep": "same", "drop": True},
        {"b": 20, "c": 3, "d": 4, "keep": "same", "n": None},
    ),
    (
        "deep-nested-change",
        {"o": {"p": {"q": [1, {"r": 1}]}}, "z": 0},
        {"o": {"p": {"q": [1, {"r": 2}]}}, "z": 0},
    ),
    (
        "type-flip",
        {"a": 1, "b": "2", "c": "yes"},
        {"a": "1", "b": 2, "c": "no"},
    ),
]


@pytest.mark.parametrize(
    "prev,cur", [(p, c) for (_id, p, c) in ROUNDTRIP_PAIRS],
    ids=[_id for (_id, _p, _c) in ROUNDTRIP_PAIRS],
)
def test_roundtrip_canonical_and_object_equality(prev, cur):
    """``apply_delta(prev, diff_entries(prev, cur))`` reproduces ``cur`` exactly.

    Asserts both layers of the contract: canonical equality (``entries_equal``)
    *and* that the reconstructed object is literally ``==`` to ``cur`` as a
    Python object (so e.g. ``True`` vs ``1`` discrepancies surface).
    """
    delta = diff_entries(prev, cur)
    rebuilt = apply_delta(prev, delta)
    assert entries_equal(rebuilt, cur)
    assert rebuilt == cur


def test_bool_int_collision_is_a_known_diff_limitation():
    """``diff_entries`` uses Python ``!=``, under which ``True == 1`` — a no-op.

    This documents (rather than fights) a deliberate boundary of v1: change
    detection is structural (``prev[k] != cur[k]``), and Python treats ``True``
    and ``1`` (and ``False``/``0``) as equal, so a ``True`` → ``1`` "change"
    produces the *empty* delta and is not reconstructed. Canonical JSON *would*
    distinguish them (``true`` vs ``1``), so the two notions disagree only on
    this bool/int collision. The synthetic generator never emits a field that
    flips between a bool and the equal int, so real chains are unaffected; this
    test simply pins the observed behaviour so a future change is intentional.
    """
    # Structural diff sees no change (True == 1), hence an empty delta.
    assert diff_entries({"a": True}, {"a": 1}) == {}
    # Apply of the empty delta leaves prev as-is (still the boolean).
    rebuilt = apply_delta({"a": True}, diff_entries({"a": True}, {"a": 1}))
    assert rebuilt == {"a": True}
    # And canonical JSON *does* tell true from 1 (entries_equal would be False).
    assert canonical_json({"a": True}) == '{"a":true}'
    assert canonical_json({"a": 1}) == '{"a":1}'
    assert entries_equal({"a": True}, {"a": 1}) is False


# --------------------------------------------------------------------------- #
# Generator-driven chain round-trips: adjacent + cumulative replay.
# --------------------------------------------------------------------------- #
def _assert_chain_roundtrips(logs):
    """Adjacent-pair and cumulative-replay fidelity over a whole chain.

    *Adjacent*: for every i>0, applying ``diff_entries(logs[i-1], logs[i])`` to
    ``logs[i-1]`` recovers ``logs[i]``.

    *Cumulative*: starting from ``logs[0]`` and applying each successive delta in
    turn must recover every ``logs[i]`` exactly — this catches drift that an
    adjacent-only check (which feeds the *true* prev each step) would miss.
    """
    assert len(logs) >= 2, "chain must have at least two entries to be meaningful"

    # Pre-images for the no-mutation guard across the whole walk.
    pristine = copy.deepcopy(logs)

    deltas = []
    for i in range(1, len(logs)):
        delta = diff_entries(logs[i - 1], logs[i])
        deltas.append(delta)
        rebuilt = apply_delta(logs[i - 1], delta)
        assert entries_equal(rebuilt, logs[i]), (
            f"adjacent round-trip failed at index {i}: "
            f"delta={delta!r} prev={logs[i - 1]!r} cur={logs[i]!r} got={rebuilt!r}"
        )
        assert rebuilt == logs[i], f"adjacent object mismatch at index {i}"

    # Cumulative replay: walk forward from logs[0] using the recovered current
    # entry as the next prev, never peeking at logs[i] as the source.
    running = copy.deepcopy(logs[0])
    assert entries_equal(running, logs[0])
    for i, delta in enumerate(deltas, start=1):
        running = apply_delta(running, delta)
        assert entries_equal(running, logs[i]), (
            f"cumulative replay diverged at index {i}: "
            f"delta={delta!r} got={running!r} want={logs[i]!r}"
        )
        assert running == logs[i], f"cumulative object mismatch at index {i}"

    # Diffing must not have mutated any entry in the chain.
    assert logs == pristine, "diff_entries mutated an entry in the chain"


def test_generator_chain_roundtrip_reference_case():
    """The task's reference chain: 500 entries, seed 7, churn 0.3, width 10."""
    logs = generate_logs(500, seed=7, churn=0.3, schema_width=10)
    _assert_chain_roundtrips(logs)


@pytest.mark.parametrize("seed", [0, 1, 7, 42, 123, 999])
@pytest.mark.parametrize("churn", [0.0, 0.2, 0.5, 1.0])
def test_generator_chain_roundtrip_matrix(seed, churn):
    """Adjacent + cumulative fidelity across seeds × the full churn spectrum.

    churn=0.0 (only ``ts`` moves, plus error add/remove on ERROR baselines),
    0.2 / 0.5 (mixed), and 1.0 (every field eligible to change each step) all
    must reconstruct losslessly, both adjacently and via cumulative replay.
    """
    logs = generate_logs(300, seed=seed, churn=churn, schema_width=10)
    _assert_chain_roundtrips(logs)


def test_generator_chain_narrow_schema_roundtrips():
    """A ts-only schema (width 1) still round-trips (degenerate but valid)."""
    logs = generate_logs(200, seed=3, churn=1.0, schema_width=1)
    _assert_chain_roundtrips(logs)


def test_generator_chain_wide_high_churn_roundtrips():
    """Widest catalogue at full churn — maximal add/remove/change pressure."""
    logs = generate_logs(400, seed=5, churn=1.0, schema_width=12)
    _assert_chain_roundtrips(logs)


# --------------------------------------------------------------------------- #
# No-mutation guarantees (diff and apply both leave inputs untouched).
# --------------------------------------------------------------------------- #
def test_apply_delta_does_not_mutate_prev():
    """After ``apply_delta``, ``prev`` is byte-identical to its pre-call deepcopy."""
    prev = {"a": 1, "b": {"x": 1, "y": [1, 2]}, "c": None, "d": 4}
    snapshot = copy.deepcopy(prev)
    delta = {"~": {"a": 99, "e": 5}, "-": ["d"]}

    _ = apply_delta(prev, delta)

    assert prev == snapshot
    # Nested container identity preserved too (shallow copy must not deep-mutate).
    assert prev["b"] == snapshot["b"]


def test_apply_delta_result_is_independent_of_prev():
    """Mutating the returned dict must not retroactively change ``prev``."""
    prev = {"a": 1, "b": 2}
    snapshot = copy.deepcopy(prev)
    result = apply_delta(prev, {"~": {"c": 3}})

    result["a"] = 12345
    result["z"] = "added-after"

    assert prev == snapshot


def test_diff_entries_does_not_mutate_inputs():
    """``diff_entries`` leaves both ``prev`` and ``cur`` untouched."""
    prev = {"a": 1, "b": 2, "c": {"k": [1, 2]}, "drop": True}
    cur = {"a": 1, "b": 99, "d": 4, "c": {"k": [1, 2]}}
    prev_snap = copy.deepcopy(prev)
    cur_snap = copy.deepcopy(cur)

    _ = diff_entries(prev, cur)

    assert prev == prev_snap
    assert cur == cur_snap


# --------------------------------------------------------------------------- #
# apply_delta edge behaviours.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "x",
    [
        {},
        {"a": 1},
        {"a": 1, "b": None, "c": {"x": 1}},
        {"deep": {"l": [1, {"k": None}]}},
    ],
    ids=["empty", "flat", "with-null-and-nested", "deeply-nested"],
)
def test_empty_delta_apply_is_identity(x):
    """``apply_delta(x, {})`` reproduces ``x`` (canonically and as an object)."""
    out = apply_delta(x, {})
    assert entries_equal(out, x)
    assert out == x


def test_apply_remove_missing_key_is_noop():
    """Removing a key that isn't present is tolerated (``pop(k, None)``)."""
    out = apply_delta({"a": 1}, {"-": ["does-not-exist"]})
    assert out == {"a": 1}


def test_apply_removal_before_addition_disjoint():
    """Removals and additions are disjoint; both are honoured in one delta."""
    out = apply_delta({"a": 1, "b": 2}, {"~": {"c": 3}, "-": ["b"]})
    assert out == {"a": 1, "c": 3}


# --------------------------------------------------------------------------- #
# Canonical serialization: sorting, separators, key-order-blind equality.
# --------------------------------------------------------------------------- #
def test_canonical_json_sorts_keys_and_is_compact():
    """Keys are sorted and separators carry no whitespace."""
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_canonical_json_sorts_nested_keys_recursively():
    """Sorting is recursive: nested object keys are sorted too, no spaces."""
    obj = {"z": {"d": 1, "a": 2}, "a": [3, 2, 1]}
    assert canonical_json(obj) == '{"a":[3,2,1],"z":{"a":2,"d":1}}'


def test_canonical_json_has_no_whitespace_separators():
    """No ``", "`` or ``": "`` — compact separators throughout."""
    s = canonical_json({"a": 1, "b": {"c": 2, "d": 3}})
    assert ", " not in s
    assert ": " not in s


def test_canonical_bytes_is_utf8_of_canonical_json():
    """``canonical_bytes`` is exactly the UTF-8 encoding of ``canonical_json``."""
    obj = {"b": 1, "a": "x"}
    assert canonical_bytes(obj) == canonical_json(obj).encode("utf-8")


def test_entries_equal_ignores_key_order():
    """Key order is irrelevant to canonical equality."""
    assert entries_equal({"a": 1, "b": 2}, {"b": 2, "a": 1}) is True


def test_entries_equal_ignores_nested_key_order():
    """Nested key order is also irrelevant (recursive sort)."""
    assert entries_equal({"m": {"x": 1, "y": 2}}, {"m": {"y": 2, "x": 1}}) is True


def test_entries_equal_differs_on_value():
    """Differing values are not equal."""
    assert entries_equal({"a": 1, "b": 2}, {"a": 1, "b": 3}) is False


def test_entries_equal_distinguishes_null_from_missing():
    """A present null and an absent key are not canonically equal."""
    assert entries_equal({"a": None}, {}) is False


def test_entries_equal_distinguishes_bool_from_int():
    """``True`` and ``1`` render differently in canonical JSON (``true`` vs ``1``)."""
    assert entries_equal({"a": True}, {"a": 1}) is False


# --------------------------------------------------------------------------- #
# Unicode fidelity: non-ASCII survives diff/apply and canonical encoding.
# --------------------------------------------------------------------------- #
UNICODE_VALUES = ["café", "日本語", "Ωμέγα", "emoji-🚀-tail", "naïve résumé", "Привет"]


@pytest.mark.parametrize("text", UNICODE_VALUES)
def test_unicode_value_roundtrips_through_delta(text):
    """Non-ASCII string values survive a diff/apply round-trip intact."""
    prev = {"msg": "ascii-old", "k": 1}
    cur = {"msg": text, "k": 1}
    rebuilt = apply_delta(prev, diff_entries(prev, cur))
    assert rebuilt == cur
    assert rebuilt["msg"] == text


@pytest.mark.parametrize("text", UNICODE_VALUES)
def test_canonical_json_keeps_non_ascii_literal(text):
    """``ensure_ascii=False``: non-ASCII stays literal, never ``\\uXXXX``-escaped."""
    s = canonical_json({"msg": text})
    assert text in s  # the real characters are present verbatim
    assert "\\u" not in s  # no escape sequences anywhere


@pytest.mark.parametrize("text", UNICODE_VALUES)
def test_canonical_bytes_is_valid_utf8_roundtrip(text):
    """``canonical_bytes`` is valid UTF-8 and decodes back to the same JSON."""
    obj = {"msg": text}
    raw = canonical_bytes(obj)
    decoded = raw.decode("utf-8")  # raises on invalid UTF-8
    assert decoded == canonical_json(obj)
    assert json.loads(decoded) == obj


def test_unicode_key_roundtrips_and_sorts():
    """Non-ASCII *keys* round-trip and participate in sorted canonical output."""
    prev = {"a": 1}
    cur = {"a": 1, "café": "value", "日本語": "テスト"}
    rebuilt = apply_delta(prev, diff_entries(prev, cur))
    assert rebuilt == cur
    # Keys remain literal (not escaped) and the blob is valid UTF-8.
    s = canonical_json(rebuilt)
    assert "café" in s and "日本語" in s
    assert canonical_bytes(rebuilt).decode("utf-8") == s


def test_unicode_chain_roundtrip():
    """A small chain of Unicode-laden entries replays losslessly (adjacent+cumulative)."""
    logs = [
        {"ts": 1, "msg": "café", "user": "naïve"},
        {"ts": 2, "msg": "日本語", "user": "naïve", "note": "🚀"},
        {"ts": 3, "msg": "日本語", "user": "Ωμέγα"},  # note removed, user changed
        {"ts": 4, "msg": "Привет", "user": "Ωμέγα", "extra": None},
    ]
    _assert_chain_roundtrips(logs)
