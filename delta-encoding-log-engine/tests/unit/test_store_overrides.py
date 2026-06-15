"""Unit tests for :meth:`app.store.SegmentStore.compress` per-call overrides.

Commit 10 gave ``compress`` keyword-only ``keyframe_interval`` / ``baseline``
overrides that are scoped to a *single* encode and must not mutate the store's
configured defaults. These tests pin three properties:

* **Backward-compat**: ``compress(entries)`` with no overrides produces the exact same
  byte accounting as before — verified against an independent :func:`app.codec.encode`
  with the store's *own* configured ``keyframe_interval`` / ``baseline`` / encoder.
* **Overrides take effect**: ``keyframe_interval=K`` yields ``keyframe_count ==
  ceil(n/K)`` and ``baseline="keyframe"`` still round-trips (reconstruct_all equals the
  originals).
* **Overrides are not sticky**: a subsequent ``compress(entries)`` with no override
  uses the original configured default again (geometry returns to the store's default).
"""
from __future__ import annotations

import gzip
import math

import pytest

from app.codec import canonical_bytes, encode, entries_equal
from app.encoders import EncoderConfig
from app.generator import generate_logs
from app.store import SegmentStore

_BASELINES = ["previous", "keyframe"]


def _lists_canon_equal(a: list, b: list) -> bool:
    """True iff two entry lists are element-wise canonically equal (same length)."""
    if len(a) != len(b):
        return False
    return all(entries_equal(x, y) for x, y in zip(a, b))


# --------------------------------------------------------------------------- #
# Backward-compat: no overrides == the store's configured defaults.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_no_override_matches_independent_encode_with_store_defaults(
    keyframe_interval, baseline
):
    """``compress(entries)`` (no overrides) == an independent encode with store config.

    Recomputes the byte fields from a fresh :func:`encode` using the store's *own*
    configured interval/baseline/encoder, so an override accidentally leaking into the
    default path would show up as a geometry / byte-count mismatch.
    """
    entries = generate_logs(300, seed=99, churn=0.2, schema_width=11)
    cfg = EncoderConfig.all_on()  # the store's default encoder config
    store = SegmentStore(
        keyframe_interval=keyframe_interval, baseline=baseline, encoder_config=cfg
    )
    stats = store.compress(entries)  # NO overrides — original behaviour

    enc = encode(
        entries,
        keyframe_interval=keyframe_interval,
        baseline=baseline,
        encoder_config=cfg,
    )
    exp_raw = sum(len(canonical_bytes(e)) for e in entries)
    exp_encoded = sum(len(canonical_bytes(s.keyframe)) for s in enc.segments) + sum(
        len(canonical_bytes(d)) for s in enc.segments for d in s.deltas
    )
    exp_gzip_raw = len(
        gzip.compress(b"".join(canonical_bytes(e) for e in entries), mtime=0)
    )

    assert stats.count == len(entries)
    assert stats.keyframe_count == len(enc.segments)
    assert stats.delta_count == sum(len(s.deltas) for s in enc.segments)
    assert stats.raw_bytes == exp_raw
    assert stats.encoded_bytes == exp_encoded
    assert stats.gzip_raw_bytes == exp_gzip_raw
    assert stats.compression_ratio == round(exp_encoded / exp_raw, 4)
    # And the configured geometry: ceil(n / K) keyframes.
    assert stats.keyframe_count == math.ceil(len(entries) / keyframe_interval)


# --------------------------------------------------------------------------- #
# Override: keyframe_interval changes the geometry for this encode only.
# --------------------------------------------------------------------------- #
def test_keyframe_interval_override_changes_keyframe_count():
    """``compress(entries, keyframe_interval=7)`` yields keyframe_count == ceil(n/7)."""
    n = 200
    entries = generate_logs(n, seed=5, churn=0.25, schema_width=10)
    # Store default interval is 100; the override should win for this call.
    store = SegmentStore(keyframe_interval=100, baseline="previous")

    stats = store.compress(entries, keyframe_interval=7)
    assert stats.keyframe_count == math.ceil(n / 7)
    assert stats.delta_count == n - stats.keyframe_count
    # Live counts reflect the just-stored (overridden) encoding too.
    assert store.keyframe_count == math.ceil(n / 7)


def test_keyframe_interval_override_still_round_trips():
    """An overridden interval still reconstructs the originals exactly.

    ``reconstruct_all`` / ``reconstruct_index`` read the geometry off the stored
    ``EncodedLog`` (which carries the overridden interval), so both honour the
    override. (``nearest_keyframe_index`` intentionally reports against the store's
    *configured* interval, not the per-call override, so it is not asserted here —
    the override contract only promises encode/decode fidelity.)
    """
    entries = generate_logs(150, seed=8, churn=0.3, schema_width=9)
    store = SegmentStore(keyframe_interval=100, baseline="previous")
    store.compress(entries, keyframe_interval=13)
    assert _lists_canon_equal(store.reconstruct_all(), entries)
    # Random access from the overridden geometry is also correct.
    for i in (0, 1, 13, 14, 99, 149):
        assert entries_equal(store.reconstruct_index(i), entries[i])


# --------------------------------------------------------------------------- #
# Override: baseline changes the diff mode for this encode only.
# --------------------------------------------------------------------------- #
def test_baseline_override_round_trips():
    """``compress(entries, baseline="keyframe")`` round-trips (store default is previous)."""
    entries = generate_logs(180, seed=21, churn=0.25, schema_width=10)
    store = SegmentStore(keyframe_interval=40, baseline="previous")
    store.compress(entries, baseline="keyframe")
    assert _lists_canon_equal(store.reconstruct_all(), entries)
    # And single-entry random access matches under the overridden baseline.
    for i in (0, 1, 39, 40, 41, 179):
        assert entries_equal(store.reconstruct_index(i), entries[i])


def test_both_overrides_together_round_trip():
    """Overriding interval AND baseline at once still round-trips and sets geometry."""
    n = 130
    entries = generate_logs(n, seed=33, churn=0.3, schema_width=9)
    store = SegmentStore(keyframe_interval=100, baseline="previous")
    stats = store.compress(entries, keyframe_interval=11, baseline="keyframe")
    assert stats.keyframe_count == math.ceil(n / 11)
    assert _lists_canon_equal(store.reconstruct_all(), entries)


# --------------------------------------------------------------------------- #
# Overrides are NOT sticky — the store's configured defaults are preserved.
# --------------------------------------------------------------------------- #
def test_override_does_not_mutate_store_default_interval():
    """After an overridden compress, a no-override compress uses the original default."""
    n = 200
    entries = generate_logs(n, seed=44, churn=0.2, schema_width=10)
    store = SegmentStore(keyframe_interval=100, baseline="previous")

    # Override once with a small interval.
    over = store.compress(entries, keyframe_interval=5)
    assert over.keyframe_count == math.ceil(n / 5)

    # Now compress again WITHOUT an override -> must fall back to default K=100.
    default = store.compress(entries)
    assert default.keyframe_count == math.ceil(n / 100), (
        "override leaked into the store's configured keyframe_interval"
    )
    # The stats() config view also still reports the original default.
    assert store.stats()["keyframe_interval"] == 100
    assert store.stats()["baseline"] == "previous"


def test_override_does_not_mutate_store_default_baseline():
    """A baseline override doesn't change the default used by a later plain compress."""
    entries = generate_logs(120, seed=51, churn=0.25, schema_width=10)
    store = SegmentStore(keyframe_interval=30, baseline="previous")

    # Encode once with the keyframe baseline override.
    store.compress(entries, baseline="keyframe")
    # The store still advertises its configured default baseline.
    assert store.stats()["baseline"] == "previous"

    # A subsequent plain compress must encode with baseline="previous". We can't read
    # the internal EncodedLog, but byte-for-byte it must equal an independent encode
    # using the *default* baseline (and differ, in general, from the keyframe one).
    cfg = EncoderConfig.all_on()
    plain = store.compress(entries)
    enc_prev = encode(
        entries, keyframe_interval=30, baseline="previous", encoder_config=cfg
    )
    exp_encoded_prev = sum(
        len(canonical_bytes(s.keyframe)) for s in enc_prev.segments
    ) + sum(len(canonical_bytes(d)) for s in enc_prev.segments for d in s.deltas)
    assert plain.encoded_bytes == exp_encoded_prev, (
        "post-override plain compress did not use the default 'previous' baseline"
    )


def test_explicit_none_overrides_are_equivalent_to_omitting_them():
    """Passing ``keyframe_interval=None, baseline=None`` == omitting them entirely."""
    entries = generate_logs(160, seed=63, churn=0.2, schema_width=10)
    store_a = SegmentStore(keyframe_interval=25, baseline="keyframe")
    store_b = SegmentStore(keyframe_interval=25, baseline="keyframe")

    a = store_a.compress(entries)
    b = store_b.compress(entries, keyframe_interval=None, baseline=None)

    assert a.to_dict() == b.to_dict()
