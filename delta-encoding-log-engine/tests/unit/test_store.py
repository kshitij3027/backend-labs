"""Unit tests for :mod:`app.store` — the stateful segment store + byte accounting.

The store wraps the codec (:mod:`app.codec`) with two responsibilities and these
tests pin both down hard:

* **Honest byte accounting** (:class:`~app.store.CompressionStats`). Rather than
  trust the store's own numbers, every byte count and reduction is *independently
  recomputed* here straight from the originals and from a fresh re-encoding of the
  same batch with the same config (the store does not expose its internal
  ``EncodedLog``, so we reproduce it via :func:`app.codec.encode` with matching
  ``keyframe_interval`` / ``baseline`` / ``encoder_config``). The formulas mirror
  *plan.md → "Byte-accounting methodology"* exactly:
    - ``raw_bytes`` = Σ ``len(canonical_bytes(entry))`` over originals;
    - ``encoded_bytes`` = Σ keyframe canon + Σ delta canon (envelope included);
    - ``gzip_raw_bytes`` = ``len(gzip.compress(b"".join(canon(e)), mtime=0))``;
    - ``delta_plus_gzip_bytes`` = ``len(gzip.compress(canon(encoded.to_dict()), mtime=0))``;
    - the three reductions = ``round(100*(raw-X)/raw, 2)``;
    - ``compression_ratio`` = ``round(encoded/raw, 4)``.
  Plus gzip determinism (mtime=0 reproducibility), the divide-by-zero guard for an
  empty batch, and the ≥60% ``delta_reduction`` target that validates the whole
  premise (the achieved numbers are printed under ``-s``).

* **Reconstruction fidelity + random access / paging.** Round-trip through the
  store across baselines and intervals; ``reconstruct_index`` / ``reconstruct_range``
  (half-open, clamped) / ``page`` / ``nearest_keyframe_index`` semantics and their
  error paths; live counts; deep-copy isolation in *both* directions; ``reset``;
  and a small concurrency smoke.
"""
from __future__ import annotations

import copy
import gzip
import math
import threading

import pytest

from app.codec import (
    canonical_bytes,
    encode,
    entries_equal,
)
from app.encoders import EncoderConfig
from app.generator import generate_logs
from app.store import CompressionStats, SegmentStore


# --------------------------------------------------------------------------- #
# Shared helpers.
# --------------------------------------------------------------------------- #
# Keyframe intervals spanning the qualitatively distinct regimes: 1 (every entry
# its own keyframe, all delta lists empty), 7 (several short+full segments), 100
# (the default). Baselines: both modes the codec supports.
_INTERVALS = [1, 7, 100]
_BASELINES = ["previous", "keyframe"]


def _lists_canon_equal(a: list, b: list) -> bool:
    """True iff two entry lists are element-wise canonically equal (same length)."""
    if len(a) != len(b):
        return False
    return all(entries_equal(x, y) for x, y in zip(a, b))


def _expected_byte_accounting(
    entries: list,
    *,
    keyframe_interval: int,
    baseline: str,
    encoder_config: EncoderConfig,
) -> dict:
    """Recompute every byte field independently from ``entries`` + a fresh encode.

    Re-encodes ``entries`` with the *same* config the store used (so the typed
    encoder adopts exactly the same per-delta candidates and the keyframe geometry
    matches), then applies the plan's formulas. This is deliberately a separate
    code path from ``app.store._compute_stats`` so a wrong formula in the store
    cannot hide behind a shared helper.
    """
    enc = encode(
        entries,
        keyframe_interval=keyframe_interval,
        baseline=baseline,
        encoder_config=encoder_config,
    )

    raw_bytes = sum(len(canonical_bytes(e)) for e in entries)

    keyframe_bytes = sum(len(canonical_bytes(seg.keyframe)) for seg in enc.segments)
    delta_bytes = sum(
        len(canonical_bytes(d)) for seg in enc.segments for d in seg.deltas
    )
    encoded_bytes = keyframe_bytes + delta_bytes

    gzip_raw_bytes = len(
        gzip.compress(b"".join(canonical_bytes(e) for e in entries), mtime=0)
    )
    delta_plus_gzip_bytes = len(gzip.compress(canonical_bytes(enc.to_dict()), mtime=0))

    keyframe_count = len(enc.segments)
    delta_count = sum(len(seg.deltas) for seg in enc.segments)

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

    return {
        "count": len(entries),
        "keyframe_count": keyframe_count,
        "delta_count": delta_count,
        "raw_bytes": raw_bytes,
        "encoded_bytes": encoded_bytes,
        "gzip_raw_bytes": gzip_raw_bytes,
        "delta_plus_gzip_bytes": delta_plus_gzip_bytes,
        "delta_reduction": delta_reduction,
        "gzip_raw_reduction": gzip_raw_reduction,
        "delta_plus_gzip_reduction": delta_plus_gzip_reduction,
        "compression_ratio": compression_ratio,
    }


# --------------------------------------------------------------------------- #
# Round-trip through the store.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
def test_reconstruct_all_round_trips_via_compress(keyframe_interval, baseline):
    """``compress(entries)`` then ``reconstruct_all()`` == ``entries`` element-wise."""
    entries = generate_logs(250, seed=7, churn=0.3, schema_width=9)
    store = SegmentStore(keyframe_interval=keyframe_interval, baseline=baseline)
    store.compress(entries)
    out = store.reconstruct_all()
    assert _lists_canon_equal(out, entries), (
        f"round-trip mismatch K={keyframe_interval} baseline={baseline}"
    )


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
def test_set_raw_then_compress_none_round_trips(keyframe_interval, baseline):
    """``set_raw(entries)`` then ``compress(entries=None)`` reconstructs ``entries``."""
    entries = generate_logs(180, seed=21, churn=0.25, schema_width=10)
    store = SegmentStore(keyframe_interval=keyframe_interval, baseline=baseline)
    n = store.set_raw(entries)
    assert n == len(entries)
    # Nothing compressed yet → empty reconstruction.
    assert store.reconstruct_all() == []
    stats = store.compress(entries=None)
    assert stats.count == len(entries)
    assert _lists_canon_equal(store.reconstruct_all(), entries)


def test_compress_none_with_no_batch_raises_value_error():
    """``compress(entries=None)`` on a fresh store raises ``ValueError``."""
    store = SegmentStore()
    with pytest.raises(ValueError):
        store.compress(entries=None)


def test_compress_none_after_reset_raises_value_error():
    """After ``reset()`` the pending raw batch is gone → ``compress(None)`` raises."""
    store = SegmentStore()
    store.set_raw(generate_logs(10, seed=1))
    store.reset()
    with pytest.raises(ValueError):
        store.compress(entries=None)


# --------------------------------------------------------------------------- #
# Byte accounting — independent recomputation.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_byte_accounting_matches_independent_recompute(keyframe_interval, baseline):
    """Every field of the returned stats equals an independent recomputation."""
    entries = generate_logs(300, seed=99, churn=0.2, schema_width=11)
    cfg = EncoderConfig.all_on()  # the store's default encoder config
    store = SegmentStore(
        keyframe_interval=keyframe_interval, baseline=baseline, encoder_config=cfg
    )
    stats = store.compress(entries)

    expected = _expected_byte_accounting(
        entries,
        keyframe_interval=keyframe_interval,
        baseline=baseline,
        encoder_config=cfg,
    )

    assert stats.count == expected["count"]
    assert stats.keyframe_count == expected["keyframe_count"]
    assert stats.delta_count == expected["delta_count"]
    assert stats.raw_bytes == expected["raw_bytes"]
    assert stats.encoded_bytes == expected["encoded_bytes"]
    assert stats.gzip_raw_bytes == expected["gzip_raw_bytes"]
    assert stats.delta_plus_gzip_bytes == expected["delta_plus_gzip_bytes"]
    assert stats.delta_reduction == expected["delta_reduction"]
    assert stats.gzip_raw_reduction == expected["gzip_raw_reduction"]
    assert stats.delta_plus_gzip_reduction == expected["delta_plus_gzip_reduction"]
    assert stats.compression_ratio == expected["compression_ratio"]


def test_raw_bytes_is_sum_of_canonical_entry_sizes():
    """``raw_bytes`` == Σ ``len(canonical_bytes(entry))`` — the fair baseline."""
    entries = generate_logs(120, seed=3, churn=0.4, schema_width=8)
    stats = SegmentStore(keyframe_interval=50).compress(entries)
    assert stats.raw_bytes == sum(len(canonical_bytes(e)) for e in entries)


def test_gzip_raw_bytes_matches_stdlib_gzip_of_concatenated_canon():
    """``gzip_raw_bytes`` == ``len(gzip.compress(b"".join(canon(e)), mtime=0))``."""
    entries = generate_logs(200, seed=44, churn=0.2, schema_width=10)
    stats = SegmentStore(keyframe_interval=100).compress(entries)
    expected = len(
        gzip.compress(b"".join(canonical_bytes(e) for e in entries), mtime=0)
    )
    assert stats.gzip_raw_bytes == expected


def test_reductions_and_ratio_derive_from_byte_fields():
    """The three reductions + ratio are exactly the rounded formulas on the bytes."""
    entries = generate_logs(260, seed=15, churn=0.2, schema_width=10)
    stats = SegmentStore(keyframe_interval=100, baseline="previous").compress(entries)

    raw = stats.raw_bytes
    assert raw > 0
    assert stats.delta_reduction == round(100.0 * (raw - stats.encoded_bytes) / raw, 2)
    assert stats.gzip_raw_reduction == round(
        100.0 * (raw - stats.gzip_raw_bytes) / raw, 2
    )
    assert stats.delta_plus_gzip_reduction == round(
        100.0 * (raw - stats.delta_plus_gzip_bytes) / raw, 2
    )
    assert stats.compression_ratio == round(stats.encoded_bytes / raw, 4)


def test_gzip_is_deterministic_across_repeated_compress():
    """mtime=0 reproducibility: identical entries ⇒ identical gzip byte counts."""
    entries = generate_logs(220, seed=8, churn=0.2, schema_width=10)
    store = SegmentStore(keyframe_interval=100, baseline="previous")
    s1 = store.compress(entries)
    s2 = store.compress(entries)
    assert s1.gzip_raw_bytes == s2.gzip_raw_bytes
    assert s1.delta_plus_gzip_bytes == s2.delta_plus_gzip_bytes
    # And a brand-new store on the same entries yields the same numbers too.
    s3 = SegmentStore(keyframe_interval=100, baseline="previous").compress(entries)
    assert s3.gzip_raw_bytes == s1.gzip_raw_bytes
    assert s3.delta_plus_gzip_bytes == s1.delta_plus_gzip_bytes


def test_empty_batch_divide_by_zero_guard():
    """``compress([])`` → raw 0, all reductions/ratio 0.0 (no exception)."""
    store = SegmentStore()
    stats = store.compress([])
    assert stats.count == 0
    assert stats.keyframe_count == 0
    assert stats.delta_count == 0
    assert stats.raw_bytes == 0
    assert stats.encoded_bytes == 0
    assert stats.delta_reduction == 0.0
    assert stats.gzip_raw_reduction == 0.0
    assert stats.delta_plus_gzip_reduction == 0.0
    assert stats.compression_ratio == 0.0
    assert store.reconstruct_all() == []


def test_fresh_store_stats_is_zeroed_and_well_formed():
    """``stats()`` on a fresh store: every key present, zeroed, no KeyError."""
    store = SegmentStore(keyframe_interval=100, baseline="previous")
    d = store.stats()
    expected_keys = {
        "count",
        "keyframe_count",
        "delta_count",
        "raw_bytes",
        "encoded_bytes",
        "gzip_raw_bytes",
        "delta_plus_gzip_bytes",
        "delta_reduction",
        "gzip_raw_reduction",
        "delta_plus_gzip_reduction",
        "compression_ratio",
        # config fields merged on top
        "keyframe_interval",
        "baseline",
        "gzip_deltas",
    }
    assert expected_keys.issubset(d.keys())
    assert d["count"] == 0
    assert d["keyframe_count"] == 0
    assert d["delta_count"] == 0
    assert d["raw_bytes"] == 0
    assert d["encoded_bytes"] == 0
    assert d["gzip_raw_bytes"] == 0
    assert d["delta_plus_gzip_bytes"] == 0
    assert d["delta_reduction"] == 0.0
    assert d["gzip_raw_reduction"] == 0.0
    assert d["delta_plus_gzip_reduction"] == 0.0
    assert d["compression_ratio"] == 0.0
    # Config is surfaced even on a fresh store.
    assert d["keyframe_interval"] == 100
    assert d["baseline"] == "previous"
    assert d["gzip_deltas"] is False


# --------------------------------------------------------------------------- #
# The ≥60% target — validates the whole premise (printed under -s).
# --------------------------------------------------------------------------- #
def test_delta_reduction_clears_60_percent_target():
    """1000-entry churn-0.2 batch: ``delta_reduction`` >= 60% (numbers printed)."""
    entries = generate_logs(1000, seed=11, churn=0.2, schema_width=10)
    stats = SegmentStore(keyframe_interval=100, baseline="previous").compress(entries)

    print(
        "\n[REDUCTION] 1000 entries, seed=11, churn=0.2, schema_width=10, "
        "K=100, baseline=previous:"
    )
    print(f"    delta_reduction            = {stats.delta_reduction:.2f}%")
    print(f"    gzip_raw_reduction         = {stats.gzip_raw_reduction:.2f}%")
    print(f"    delta_plus_gzip_reduction  = {stats.delta_plus_gzip_reduction:.2f}%")
    print(f"    compression_ratio          = {stats.compression_ratio}")
    print(
        f"    raw_bytes={stats.raw_bytes} encoded_bytes={stats.encoded_bytes} "
        f"gzip_raw_bytes={stats.gzip_raw_bytes} "
        f"delta_plus_gzip_bytes={stats.delta_plus_gzip_bytes}"
    )

    assert stats.delta_reduction >= 60.0, (
        f"delta_reduction {stats.delta_reduction} did not clear the 60% target"
    )


# --------------------------------------------------------------------------- #
# Random access / paging.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
def test_reconstruct_index_matches_entries(keyframe_interval, baseline):
    """``reconstruct_index(i)`` == ``entries[i]`` for sampled i incl. boundaries."""
    entries = generate_logs(213, seed=5, churn=0.3, schema_width=9)
    store = SegmentStore(keyframe_interval=keyframe_interval, baseline=baseline)
    store.compress(entries)
    n = len(entries)
    K = keyframe_interval
    # Sampled indices plus every segment boundary that exists in range.
    sample = {0, n - 1, n // 2, n // 3}
    for b in range(0, n, max(1, K)):
        sample.update({b, b - 1, b + 1})
    sample = {i for i in sample if 0 <= i < n}
    for i in sorted(sample):
        assert entries_equal(store.reconstruct_index(i), entries[i]), (
            f"index {i} mismatch K={K} baseline={baseline}"
        )


def test_reconstruct_index_out_of_range_raises():
    """Out-of-range / negative indices raise ``IndexError``."""
    entries = generate_logs(40, seed=6, churn=0.2, schema_width=8)
    store = SegmentStore(keyframe_interval=10)
    store.compress(entries)
    with pytest.raises(IndexError):
        store.reconstruct_index(40)
    with pytest.raises(IndexError):
        store.reconstruct_index(-1)
    with pytest.raises(IndexError):
        store.reconstruct_index(10_000)


def test_reconstruct_index_empty_store_raises():
    """``reconstruct_index`` before any compress raises ``IndexError``."""
    store = SegmentStore()
    with pytest.raises(IndexError):
        store.reconstruct_index(0)


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
def test_reconstruct_range_half_open_and_clamped(keyframe_interval, baseline):
    """``reconstruct_range`` is half-open ``[start,end)`` and clamped to ``[0,count]``."""
    entries = generate_logs(150, seed=12, churn=0.25, schema_width=10)
    store = SegmentStore(keyframe_interval=keyframe_interval, baseline=baseline)
    store.compress(entries)
    n = len(entries)

    # Interior half-open slice equals the original slice.
    assert _lists_canon_equal(store.reconstruct_range(10, 25), entries[10:25])
    assert _lists_canon_equal(store.reconstruct_range(0, n), entries[0:n])

    # Clamped: hugely over/under bounds return the whole list.
    assert _lists_canon_equal(store.reconstruct_range(-5, 10_000), entries)

    # Empty / inverted ranges yield [].
    assert store.reconstruct_range(5, 5) == []
    assert store.reconstruct_range(25, 10) == []
    assert store.reconstruct_range(n, n + 50) == []
    # Fully negative range clamps to empty.
    assert store.reconstruct_range(-50, -10) == []


def test_reconstruct_range_empty_store_returns_empty():
    """``reconstruct_range`` before any compress returns ``[]`` (no raise)."""
    store = SegmentStore()
    assert store.reconstruct_range(0, 10) == []


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
def test_page_equals_reconstruct_range(keyframe_interval, baseline):
    """``page(offset, limit)`` == ``reconstruct_range(offset, offset+limit)``."""
    entries = generate_logs(130, seed=33, churn=0.3, schema_width=9)
    store = SegmentStore(keyframe_interval=keyframe_interval, baseline=baseline)
    store.compress(entries)
    for offset, limit in [(0, 10), (20, 30), (100, 50), (125, 20), (0, 0), (10, 0)]:
        page = store.page(offset, limit)
        rng = store.reconstruct_range(offset, offset + limit)
        assert _lists_canon_equal(page, rng), f"page!=range for {offset},{limit}"
    # A non-positive limit collapses to empty.
    assert store.page(10, 0) == []
    assert store.page(10, -5) == []


@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
def test_nearest_keyframe_index(keyframe_interval):
    """``nearest_keyframe_index(i)`` == ``(i // K) * K`` for in-range i."""
    entries = generate_logs(205, seed=17, churn=0.2, schema_width=9)
    store = SegmentStore(keyframe_interval=keyframe_interval)
    store.compress(entries)
    n = len(entries)
    K = keyframe_interval
    for i in range(n):
        assert store.nearest_keyframe_index(i) == (i // K) * K


def test_nearest_keyframe_index_out_of_range_raises():
    """``nearest_keyframe_index`` out of range / empty-store raises ``IndexError``."""
    entries = generate_logs(50, seed=2, churn=0.2, schema_width=8)
    store = SegmentStore(keyframe_interval=10)
    store.compress(entries)
    with pytest.raises(IndexError):
        store.nearest_keyframe_index(50)
    with pytest.raises(IndexError):
        store.nearest_keyframe_index(-1)

    fresh = SegmentStore()
    with pytest.raises(IndexError):
        fresh.nearest_keyframe_index(0)


# --------------------------------------------------------------------------- #
# Live counts.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
def test_count_properties(keyframe_interval, baseline):
    """``count`` / ``keyframe_count`` / ``delta_count`` are correct + consistent."""
    n = 234
    entries = generate_logs(n, seed=19, churn=0.2, schema_width=10)
    store = SegmentStore(keyframe_interval=keyframe_interval, baseline=baseline)
    store.compress(entries)
    K = keyframe_interval

    assert store.count == n
    assert store.keyframe_count == math.ceil(n / K)
    assert store.delta_count == n - store.keyframe_count
    # And they agree with the stats() dict.
    d = store.stats()
    assert d["count"] == store.count
    assert d["keyframe_count"] == store.keyframe_count
    assert d["delta_count"] == store.delta_count


def test_count_properties_on_fresh_store_are_zero():
    """All count properties are 0 before anything is compressed."""
    store = SegmentStore()
    assert store.count == 0
    assert store.keyframe_count == 0
    assert store.delta_count == 0


# --------------------------------------------------------------------------- #
# Deep-copy isolation — both directions.
# --------------------------------------------------------------------------- #
def test_mutating_input_list_after_compress_does_not_corrupt_store():
    """Mutating the list (and its entries) passed to ``compress`` is isolated."""
    entries = generate_logs(60, seed=23, churn=0.2, schema_width=9)
    snapshot = copy.deepcopy(entries)
    store = SegmentStore(keyframe_interval=20)
    store.compress(entries)

    # Mutate the caller's list AND the dicts inside it.
    entries.append({"ts": 999})
    entries[0]["level"] = "CORRUPTED"
    entries[0]["injected"] = True
    del entries[1]

    # Store is unaffected: still reconstructs the original snapshot.
    assert _lists_canon_equal(store.reconstruct_all(), snapshot)
    assert store.count == len(snapshot)


def test_mutating_input_list_after_set_raw_does_not_corrupt_store():
    """Mutating the list passed to ``set_raw`` (pre-compress) is isolated."""
    entries = generate_logs(40, seed=24, churn=0.2, schema_width=8)
    snapshot = copy.deepcopy(entries)
    store = SegmentStore(keyframe_interval=10)
    store.set_raw(entries)

    entries[0]["level"] = "CORRUPTED"
    entries.append({"ts": 1})

    store.compress(entries=None)  # uses the stored (deep-copied) batch
    assert _lists_canon_equal(store.reconstruct_all(), snapshot)


def test_mutating_returned_lists_does_not_corrupt_store():
    """Mutating lists returned by ``get_raw`` / ``reconstruct_all`` is isolated."""
    entries = generate_logs(80, seed=25, churn=0.3, schema_width=10)
    snapshot = copy.deepcopy(entries)
    store = SegmentStore(keyframe_interval=25)
    store.compress(entries)

    # Mutate the deep copy returned by reconstruct_all.
    out1 = store.reconstruct_all()
    out1.clear()
    out1.append({"poisoned": True})

    # Mutate the deep copy returned by get_raw, incl. its entries.
    raw = store.get_raw()
    if raw:
        raw[0]["level"] = "CORRUPTED"
        raw[0]["extra"] = 1
    raw.append({"ts": -1})

    # Subsequent reads are unaffected.
    assert _lists_canon_equal(store.reconstruct_all(), snapshot)
    assert _lists_canon_equal(store.get_raw(), snapshot)
    # And stats() count is unchanged.
    assert store.stats()["count"] == len(snapshot)


# --------------------------------------------------------------------------- #
# reset.
# --------------------------------------------------------------------------- #
def test_reset_clears_everything():
    """After ``reset()``: count 0, ``reconstruct_all()`` empty, ``stats()`` zeroed."""
    entries = generate_logs(70, seed=26, churn=0.2, schema_width=9)
    store = SegmentStore(keyframe_interval=20)
    store.compress(entries)
    assert store.count == 70  # sanity: populated before reset

    store.reset()

    assert store.count == 0
    assert store.keyframe_count == 0
    assert store.delta_count == 0
    assert store.reconstruct_all() == []
    assert store.get_raw() == []
    d = store.stats()
    assert d["count"] == 0
    assert d["raw_bytes"] == 0
    assert d["encoded_bytes"] == 0
    assert d["delta_reduction"] == 0.0
    assert d["compression_ratio"] == 0.0


# --------------------------------------------------------------------------- #
# Concurrency smoke (small / fast).
# --------------------------------------------------------------------------- #
def test_concurrent_reads_during_compress_no_exception():
    """Threaded reads while a writer re-compresses: no exception; final state OK."""
    batch_a = generate_logs(200, seed=51, churn=0.2, schema_width=10)
    batch_b = generate_logs(200, seed=52, churn=0.3, schema_width=10)
    store = SegmentStore(keyframe_interval=50, baseline="previous")
    store.compress(batch_a)  # ensure there is always something to read

    errors: list[BaseException] = []
    stop = threading.Event()

    def reader():
        try:
            while not stop.is_set():
                store.reconstruct_all()
                store.stats()
                _ = store.count
        except BaseException as exc:  # noqa: BLE001 — capture anything for the assert
            errors.append(exc)

    def writer():
        try:
            # Alternate the two batches; finish on a known-final batch.
            for i in range(30):
                store.compress(batch_a if i % 2 == 0 else batch_b)
            store.compress(batch_a)  # deterministic final state
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    readers = [threading.Thread(target=reader) for _ in range(3)]
    w = threading.Thread(target=writer)
    for t in readers:
        t.start()
    w.start()
    w.join()
    stop.set()
    for t in readers:
        t.join()

    assert not errors, f"concurrency raised: {errors!r}"
    # Final compressed batch was batch_a; reconstruction must equal it.
    assert _lists_canon_equal(store.reconstruct_all(), batch_a)
