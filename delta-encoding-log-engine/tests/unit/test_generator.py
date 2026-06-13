"""Unit tests for :mod:`app.generator`.

Exercises the deterministic synthetic-log generator's core contract:

* **Determinism & isolation** — a given ``(count, seed, churn, schema_width)``
  yields byte-identical output, a different seed diverges, and the process-wide
  :mod:`random` state is never disturbed (a *local* ``Random`` is used).
* **Timestamps** — every ``ts`` is a real ``int`` (never ``bool``) and strictly
  increases across the whole batch.
* **JSON-native values** — the batch serialises with :func:`json.dumps`, and no
  value anywhere is a ``float`` / ``NaN`` / ``Inf`` / ``None`` (``bool`` is fine).
* **Schema drift** — the optional ``error`` field is present exactly on ERROR
  lines and absent everywhere else (the codec's add/remove paths).
* **Churn extremes & schema width** — ``churn=0.0`` freezes everything but
  ``ts``; ``churn=1.0`` runs cleanly; ``schema_width`` controls base field count
  and clamps to the catalogue.
* **Count edges** — ``count<1`` → ``[]``; ``count==1`` → one baseline entry.
* **Settings wiring** — :func:`generate_from_settings` reflects configured churn
  / schema width.

All RNG-dependent invariants are asserted over enough entries (and, where the
property is probabilistic, across several seeds) that the check is robust rather
than relying on one lucky draw.
"""
from __future__ import annotations

import json
import math
import random

import pytest

from app.generator import generate_from_settings, generate_logs
from app.settings import get_settings

# The generator's field catalogue size (ts + 11 non-ts fields). Mirrors
# ``app.generator._CATALOGUE_SIZE`` without importing the private name so the
# clamp tests stay meaningful even if the catalogue is reordered.
CATALOGUE_SIZE = 12


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _canonical(batch):
    """Stable, order-independent JSON rendering for equality comparisons."""
    return json.dumps(batch, sort_keys=True)


def _iter_values(batch):
    """Yield every scalar value across every entry in the batch."""
    for entry in batch:
        yield from entry.values()


# --------------------------------------------------------------------------- #
# determinism & RNG isolation
# --------------------------------------------------------------------------- #
def test_same_seed_is_byte_identical():
    """Two calls with identical args produce equal output (canonical JSON)."""
    a = generate_logs(100, seed=42, churn=0.2, schema_width=8)
    b = generate_logs(100, seed=42, churn=0.2, schema_width=8)

    assert _canonical(a) == _canonical(b)


def test_different_seed_diverges():
    """A different seed yields different output for the same shape."""
    a = generate_logs(100, seed=42, churn=0.2, schema_width=8)
    b = generate_logs(100, seed=43, churn=0.2, schema_width=8)

    assert _canonical(a) != _canonical(b)


def test_does_not_pollute_global_rng():
    """Seeding the global RNG then generating must not consume global draws.

    If the generator used the module-level :mod:`random` functions instead of a
    local ``Random``, the post-call global sequence would shift. We capture the
    expected global sequence, reseed identically, run the generator in between,
    and assert the observed global sequence is unchanged.
    """
    random.seed(12345)
    expected = [random.random() for _ in range(5)]

    random.seed(12345)
    generate_logs(250, seed=999, churn=0.5, schema_width=10)
    observed = [random.random() for _ in range(5)]

    assert observed == expected


def test_no_seed_still_leaves_global_rng_untouched():
    """Even a seedless (non-deterministic) run must not touch global state."""
    random.seed(2024)
    expected = [random.random() for _ in range(5)]

    random.seed(2024)
    generate_logs(50)  # seed=None -> non-deterministic, but local RNG only
    observed = [random.random() for _ in range(5)]

    assert observed == expected


# --------------------------------------------------------------------------- #
# timestamps
# --------------------------------------------------------------------------- #
def test_timestamps_are_int_not_bool():
    """Every ``ts`` is a real int and never a bool (``True``/``False``)."""
    batch = generate_logs(200, seed=7, churn=0.3, schema_width=8)

    for entry in batch:
        ts = entry["ts"]
        assert isinstance(ts, int)
        assert not isinstance(ts, bool)


def test_timestamps_strictly_increasing():
    """``ts`` strictly increases across the whole batch (no plateaus)."""
    batch = generate_logs(500, seed=11, churn=0.4, schema_width=9)

    timestamps = [entry["ts"] for entry in batch]
    for earlier, later in zip(timestamps, timestamps[1:]):
        assert later > earlier


# --------------------------------------------------------------------------- #
# JSON-native / no floats / no None
# --------------------------------------------------------------------------- #
def test_batch_is_json_serialisable():
    """A large batch serialises cleanly via the stdlib JSON encoder."""
    batch = generate_logs(1000, seed=3, churn=0.5, schema_width=12)

    dumped = json.dumps(batch)  # raises TypeError on any non-JSON-native value
    assert dumped


def test_no_float_nan_inf_or_none_anywhere():
    """No value anywhere is a float / NaN / Inf / None; bools are allowed."""
    batch = generate_logs(1000, seed=3, churn=0.5, schema_width=12)

    for value in _iter_values(batch):
        assert value is not None
        # bool is a subclass of int and is explicitly permitted.
        assert not isinstance(value, float)
        # Defensive: NaN/Inf would also be floats, but guard the float path too.
        if isinstance(value, float):  # pragma: no cover - guarded above
            assert not math.isnan(value)
            assert not math.isinf(value)


def test_values_are_only_int_str_bool():
    """Positively assert the value type whitelist (int/str/bool)."""
    batch = generate_logs(300, seed=8, churn=0.6, schema_width=11)

    for value in _iter_values(batch):
        assert isinstance(value, (int, str, bool))  # int covers bool


# --------------------------------------------------------------------------- #
# error field add/remove (schema drift)
# --------------------------------------------------------------------------- #
def test_error_field_tracks_error_level_for_one_seed():
    """For a batch that contains both cases, ``error`` ⇔ ``level == 'ERROR'``."""
    # Wide schema so ``level`` is in the schema; large count so ERROR appears.
    batch = generate_logs(800, seed=5, churn=0.5, schema_width=8)

    saw_error_row = False
    saw_non_error_row = False
    for entry in batch:
        if entry.get("level") == "ERROR":
            saw_error_row = True
            assert "error" in entry
            assert isinstance(entry["error"], str)
        else:
            saw_non_error_row = True
            assert "error" not in entry

    # The chosen seed/size must actually exercise both branches.
    assert saw_error_row, "expected at least one ERROR row in this batch"
    assert saw_non_error_row, "expected at least one non-ERROR row in this batch"


def test_error_invariant_holds_across_many_seeds():
    """Across several seeds the invariant never breaks, and ERROR does occur.

    Robustness check independent of any single lucky seed: the present/absent
    invariant must hold on every entry of every batch, and collectively at least
    one ERROR row must show up so the add-path is genuinely covered.
    """
    saw_any_error = False
    for seed in range(20):
        batch = generate_logs(200, seed=seed, churn=0.5, schema_width=8)
        for entry in batch:
            if entry.get("level") == "ERROR":
                saw_any_error = True
                assert "error" in entry
            else:
                assert "error" not in entry

    assert saw_any_error, "no ERROR rows across 20 seeds — add-path untested"


def test_error_field_absent_when_level_not_in_schema():
    """With a schema too narrow to include ``level``, ``error`` never appears."""
    # schema_width=1 -> only ``ts``; schema_width=1 means no non-ts fields, so
    # ``level`` is absent and the error field must never be added.
    batch = generate_logs(300, seed=4, churn=1.0, schema_width=1)

    for entry in batch:
        assert "level" not in entry
        assert "error" not in entry
        assert set(entry.keys()) == {"ts"}


# --------------------------------------------------------------------------- #
# churn extremes
# --------------------------------------------------------------------------- #
def test_churn_zero_changes_only_timestamp():
    """``churn=0.0`` ⇒ between consecutive entries only ``ts`` differs.

    The optional ``error`` field is re-rolled on ERROR lines even at zero churn,
    so to isolate the "nothing but ts moves" property we use a seed whose
    baseline level is *not* ERROR; with zero churn the level never changes, so
    ``error`` is never introduced and every non-ts field stays frozen.
    """
    seed = _seed_with_non_error_baseline(churn=0.0, schema_width=8)
    batch = generate_logs(150, seed=seed, churn=0.0, schema_width=8)

    # Baseline must genuinely have non-ts fields to make this meaningful.
    assert len(batch[0]) > 1
    assert batch[0].get("level") != "ERROR"

    for prev, curr in zip(batch, batch[1:]):
        # ts strictly advances ...
        assert curr["ts"] > prev["ts"]
        # ... and every other key/value is identical to the previous entry.
        prev_wo_ts = {k: v for k, v in prev.items() if k != "ts"}
        curr_wo_ts = {k: v for k, v in curr.items() if k != "ts"}
        assert curr_wo_ts == prev_wo_ts


def test_churn_one_runs_and_changes_many_fields():
    """``churn=1.0`` runs without error and mutates many fields over the batch.

    At full churn every non-ts field is eligible to change each step, so across
    a long batch the great majority of fields should take more than one distinct
    value. We assert no exception, a serialisable result, and broad variation.
    """
    batch = generate_logs(400, seed=6, churn=1.0, schema_width=10)

    assert json.dumps(batch)  # runs cleanly / stays JSON-native

    # Collect the distinct values seen per non-ts field across the batch.
    non_ts_fields = (set().union(*[e.keys() for e in batch])) - {"ts", "error"}
    multi_valued = 0
    for field in non_ts_fields:
        seen = {e[field] for e in batch if field in e}
        if len(seen) > 1:
            multi_valued += 1

    # "many fields change": the clear majority of base fields vary.
    assert multi_valued >= max(1, (len(non_ts_fields) * 2) // 3)


# --------------------------------------------------------------------------- #
# schema width
# --------------------------------------------------------------------------- #
def test_schema_width_five_gives_five_base_fields():
    """``schema_width=5`` ⇒ exactly 5 base fields (ts + 4), plus maybe ``error``.

    ``ts`` is always present; the optional ``error`` is the only key allowed
    beyond the 5 base fields, and only on ERROR rows.
    """
    batch = generate_logs(300, seed=9, churn=0.5, schema_width=5)

    for entry in batch:
        assert "ts" in entry
        base_keys = set(entry.keys()) - {"error"}
        assert len(base_keys) == 5
        # Any extra key beyond the 5 base fields can only be ``error``.
        extra = set(entry.keys()) - base_keys
        assert extra <= {"error"}
        if "error" in entry:
            assert entry.get("level") == "ERROR"


def test_schema_width_clamps_above_catalogue():
    """A ``schema_width`` beyond the catalogue clamps to the catalogue size."""
    # Far above the catalogue; base width must clamp to CATALOGUE_SIZE.
    over = generate_logs(50, seed=2, churn=0.3, schema_width=999)
    at_max = generate_logs(50, seed=2, churn=0.3, schema_width=CATALOGUE_SIZE)

    for entry in over:
        base_keys = set(entry.keys()) - {"error"}
        assert len(base_keys) == CATALOGUE_SIZE

    # Clamping should make an oversized request identical to the max request.
    assert _canonical(over) == _canonical(at_max)


def test_schema_width_one_is_ts_only():
    """``schema_width=1`` ⇒ each entry is exactly ``{'ts': ...}``."""
    batch = generate_logs(20, seed=1, churn=0.5, schema_width=1)

    for entry in batch:
        assert set(entry.keys()) == {"ts"}


# --------------------------------------------------------------------------- #
# count edges
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("count", [0, -1, -100])
def test_count_below_one_returns_empty(count):
    """``count < 1`` returns an empty list (no entries)."""
    assert generate_logs(count, seed=1) == []


def test_count_one_returns_single_baseline():
    """``count == 1`` returns exactly one fully-populated baseline entry."""
    batch = generate_logs(1, seed=1, churn=0.5, schema_width=8)

    assert len(batch) == 1
    entry = batch[0]
    assert "ts" in entry
    # Baseline carries the full base schema (8 fields), plus maybe ``error``.
    base_keys = set(entry.keys()) - {"error"}
    assert len(base_keys) == 8


def test_count_matches_requested_length():
    """The returned list length equals the requested ``count`` (when >= 1)."""
    for count in (1, 2, 5, 37, 100):
        assert len(generate_logs(count, seed=1, schema_width=8)) == count


# --------------------------------------------------------------------------- #
# generate_from_settings
# --------------------------------------------------------------------------- #
def test_generate_from_settings_uses_configured_width(monkeypatch):
    """Configured ``GENERATOR_SCHEMA_WIDTH`` drives the produced base width."""
    monkeypatch.setenv("GENERATOR_SCHEMA_WIDTH", "5")
    monkeypatch.setenv("GENERATOR_FIELD_CHURN", "0.5")
    get_settings.cache_clear()
    try:
        batch = generate_from_settings(40, seed=21)
    finally:
        get_settings.cache_clear()

    assert len(batch) == 40
    for entry in batch:
        base_keys = set(entry.keys()) - {"error"}
        assert len(base_keys) == 5


def test_generate_from_settings_reflects_zero_churn(monkeypatch):
    """Configured ``GENERATOR_FIELD_CHURN=0`` freezes all non-ts fields.

    Drives the churn knob through settings (not a direct arg) and confirms the
    wrapper actually forwards it: with zero churn and a non-ERROR baseline, only
    ``ts`` moves between consecutive entries.
    """
    monkeypatch.setenv("GENERATOR_SCHEMA_WIDTH", "8")
    monkeypatch.setenv("GENERATOR_FIELD_CHURN", "0.0")
    get_settings.cache_clear()
    try:
        # Find a seed whose baseline is non-ERROR so ``error`` never appears.
        seed = _seed_with_non_error_baseline(churn=0.0, schema_width=8)
        batch = generate_from_settings(60, seed=seed)
    finally:
        get_settings.cache_clear()

    for prev, curr in zip(batch, batch[1:]):
        assert curr["ts"] > prev["ts"]
        prev_wo_ts = {k: v for k, v in prev.items() if k != "ts"}
        curr_wo_ts = {k: v for k, v in curr.items() if k != "ts"}
        assert curr_wo_ts == prev_wo_ts


def test_generate_from_settings_defaults_to_width_eight(monkeypatch):
    """With no overrides the wrapper uses the documented width-8 default."""
    for name in ("GENERATOR_SCHEMA_WIDTH", "GENERATOR_FIELD_CHURN"):
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    try:
        batch = generate_from_settings(30, seed=1)
    finally:
        get_settings.cache_clear()

    for entry in batch:
        base_keys = set(entry.keys()) - {"error"}
        assert len(base_keys) == 8


# --------------------------------------------------------------------------- #
# internal helper used by the zero-churn tests
# --------------------------------------------------------------------------- #
def _seed_with_non_error_baseline(*, churn: float, schema_width: int) -> int:
    """Return the first seed in [0, 200) whose entry-0 ``level`` is not ERROR.

    The zero-churn "only ts changes" property only holds cleanly when the
    baseline level is not ERROR (an ERROR baseline re-rolls its ``error`` code
    each step). Such seeds are overwhelmingly common (levels are INFO-heavy), so
    this scans a small range and is guaranteed to find one quickly.
    """
    for seed in range(200):
        first = generate_logs(1, seed=seed, churn=churn, schema_width=schema_width)[0]
        if first.get("level") != "ERROR":
            return seed
    raise AssertionError("no non-ERROR baseline seed found in [0, 200)")
