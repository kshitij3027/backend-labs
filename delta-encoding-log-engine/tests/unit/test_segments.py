"""Unit tests for the segment / keyframe layer of :mod:`app.codec`.

This layer sits on top of the adjacent-entry primitives
(``diff_entries`` / ``apply_delta`` / ``entries_equal``) tested in
``test_codec.py``. It owns *where keyframes fall* and *which baseline each delta
is computed against* — and its contract is purely about reconstruction fidelity
plus the serializable shape of an :class:`~app.codec.EncodedLog`. The tests below
pin that contract down hard:

* **Full round-trip** — ``decode(encode(entries, …))`` is element-wise
  canonically equal to ``entries`` for *every* index (not just the last), across
  keyframe intervals ``K ∈ {1, 7, 100, len+1}``, both baselines, churny
  generator chains (incl. churn 0.0 and 1.0), and hand-crafted lists.
* **Random access** — ``reconstruct_index(enc, i)`` equals ``entries[i]`` for
  every i, with the segment-boundary indices ``K-1, K, K+1, 0, len-1`` asserted
  explicitly.
* **Structure** — segment count ``== ceil(count / K)``, keyframe indices ``==
  range(0, count, K)``, ``K == 1`` ⇒ empty delta lists, keyframes stored full.
* **Serialization** — ``to_dict`` / ``from_dict`` and a full ``json`` round-trip
  preserve the decode.
* **No mutation / no corruption** — ``encode`` never touches ``entries``; mutating
  a decoded entry can't corrupt the encoded keyframe (a second ``decode`` is
  unaffected).
* **Edge / errors** — empty input, out-of-range / negative indices, bad interval,
  bad baseline.
* **Both-baseline equivalence** — ``previous`` and ``keyframe`` produce different
  encodings but identical reconstructions.
"""
from __future__ import annotations

import copy
import json
import math

import pytest

from app.codec import (
    EncodedLog,
    Segment,
    decode,
    encode,
    entries_equal,
    keyframe_indices,
    reconstruct_index,
)
from app.generator import generate_logs


# --------------------------------------------------------------------------- #
# Shared helpers and fixtures.
# --------------------------------------------------------------------------- #
# Keyframe intervals exercised across the round-trip / random-access matrix.
# 1 (every entry is its own keyframe), 7 (creates several short+full segments),
# 100 (default), and a "len+1"-style value (single all-keyframe segment) are the
# qualitatively distinct regimes. The literal large value stands in for the
# len+1 case for the fixed-length chains below; per-list len+1 is also tested.
_INTERVALS = [1, 7, 100]
_BASELINES = ["previous", "keyframe"]


def _assert_full_roundtrip(entries, *, keyframe_interval, baseline):
    """``decode(encode(...))`` is element-wise canonically equal to ``entries``.

    Asserts length first, then every index (not just the last) so any mid-chain
    drift surfaces with the offending index, K and baseline in the message.
    """
    enc = encode(entries, keyframe_interval=keyframe_interval, baseline=baseline)
    dec = decode(enc)
    assert len(dec) == len(entries), (
        f"length mismatch: K={keyframe_interval} baseline={baseline} "
        f"got {len(dec)} want {len(entries)}"
    )
    for i in range(len(entries)):
        assert entries_equal(dec[i], entries[i]), (
            f"round-trip diverged at index {i}: K={keyframe_interval} "
            f"baseline={baseline} got={dec[i]!r} want={entries[i]!r}"
        )
    return enc, dec


def _assert_random_access(entries, enc, *, keyframe_interval, baseline):
    """``reconstruct_index`` equals ``entries[i]`` for every i.

    Then re-asserts the segment-boundary indices ``0, K-1, K, K+1, len-1``
    explicitly (those in range), since boundaries are where off-by-one segment
    location or offset math would bite.
    """
    n = len(entries)
    for i in range(n):
        got = reconstruct_index(enc, i)
        assert entries_equal(got, entries[i]), (
            f"reconstruct_index diverged at index {i}: K={keyframe_interval} "
            f"baseline={baseline} got={got!r} want={entries[i]!r}"
        )

    # Explicit boundary checks (only those that fall inside the chain).
    k = keyframe_interval
    boundary = {0, k - 1, k, k + 1, n - 1}
    for i in sorted(b for b in boundary if 0 <= b < n):
        got = reconstruct_index(enc, i)
        assert entries_equal(got, entries[i]), (
            f"boundary reconstruct_index failed at index {i}: K={k} "
            f"baseline={baseline} got={got!r} want={entries[i]!r}"
        )


# A handful of small hand-crafted chains with deliberate add/remove/null/nested
# transitions, so the matrix isn't only generator-driven.
_HANDCRAFTED = {
    "single": [{"ts": 1, "level": "INFO"}],
    "pair-change": [{"ts": 1, "a": 1}, {"ts": 2, "a": 2}],
    "add-remove-null": [
        {"ts": 1, "level": "INFO", "service": "auth"},
        {"ts": 2, "level": "ERROR", "service": "auth", "error": "ETIMEDOUT"},
        {"ts": 3, "level": "INFO", "service": "auth"},  # error removed
        {"ts": 4, "level": "INFO", "service": "auth", "note": None},  # add null
        {"ts": 5, "level": "INFO", "service": "billing", "note": None},
    ],
    "nested-and-reorder": [
        {"ts": 1, "m": {"x": 1, "y": 2}, "tags": [1, 2, 3]},
        {"ts": 2, "m": {"y": 2, "x": 1}, "tags": [1, 2, 3]},  # key-order-only noop
        {"ts": 3, "m": {"x": 1, "y": 9}, "tags": [3, 2, 1]},  # real nested change
        {"ts": 4, "m": {"x": 1, "y": 9}, "tags": [3, 2, 1], "extra": "🚀"},
    ],
    "wide-eight": [{"ts": t, **{f"f{j}": t * 10 + j for j in range(8)}} for t in range(8)],
}


# --------------------------------------------------------------------------- #
# Full round-trip — generator-driven churny chains.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
def test_roundtrip_reference_churny_chain(keyframe_interval, baseline):
    """Reference churny chain N=250, churn=0.3, W=10 round-trips at every index."""
    entries = generate_logs(250, seed=7, churn=0.3, schema_width=10)
    enc, _dec = _assert_full_roundtrip(
        entries, keyframe_interval=keyframe_interval, baseline=baseline
    )
    _assert_random_access(
        entries, enc, keyframe_interval=keyframe_interval, baseline=baseline
    )


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
@pytest.mark.parametrize(
    "seed,churn",
    [(0, 0.0), (3, 0.2), (42, 0.5), (123, 1.0), (999, 0.0), (5, 1.0)],
)
def test_roundtrip_seed_churn_matrix(keyframe_interval, baseline, seed, churn):
    """Round-trip + random access across seeds × churn (incl. 0.0 and 1.0)."""
    entries = generate_logs(200, seed=seed, churn=churn, schema_width=10)
    enc, _dec = _assert_full_roundtrip(
        entries, keyframe_interval=keyframe_interval, baseline=baseline
    )
    _assert_random_access(
        entries, enc, keyframe_interval=keyframe_interval, baseline=baseline
    )


@pytest.mark.parametrize("baseline", _BASELINES)
def test_roundtrip_keyframe_interval_len_plus_one(baseline):
    """``K = len(entries) + 1`` ⇒ one segment, all-but-keyframe stored as deltas.

    A single segment that still must reconstruct every entry exactly — the
    "keyframe interval larger than the stream" regime called out in the spec.
    """
    entries = generate_logs(60, seed=11, churn=0.4, schema_width=9)
    k = len(entries) + 1
    enc, _dec = _assert_full_roundtrip(entries, keyframe_interval=k, baseline=baseline)
    # Exactly one segment spanning the whole stream.
    assert len(enc.segments) == 1
    assert enc.segments[0].start_index == 0
    assert len(enc.segments[0].deltas) == len(entries) - 1
    _assert_random_access(entries, enc, keyframe_interval=k, baseline=baseline)


# --------------------------------------------------------------------------- #
# Full round-trip — hand-crafted small chains.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
@pytest.mark.parametrize("name", sorted(_HANDCRAFTED))
def test_roundtrip_handcrafted(name, keyframe_interval, baseline):
    """Hand-crafted add/remove/null/nested chains round-trip at every index."""
    entries = _HANDCRAFTED[name]
    enc, _dec = _assert_full_roundtrip(
        entries, keyframe_interval=keyframe_interval, baseline=baseline
    )
    _assert_random_access(
        entries, enc, keyframe_interval=keyframe_interval, baseline=baseline
    )


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("name", sorted(_HANDCRAFTED))
def test_roundtrip_handcrafted_len_plus_one(name, baseline):
    """Hand-crafted chains with ``K == len+1`` (single segment) also round-trip."""
    entries = _HANDCRAFTED[name]
    k = len(entries) + 1
    _assert_full_roundtrip(entries, keyframe_interval=k, baseline=baseline)


# --------------------------------------------------------------------------- #
# Random access — explicit boundary indices on a clean K=7 chain.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
def test_random_access_explicit_boundaries(baseline):
    """Explicitly assert K-1, K, K+1, 0 and len-1 on a multi-segment K=7 chain."""
    entries = generate_logs(50, seed=21, churn=0.3, schema_width=10)
    k = 7
    enc = encode(entries, keyframe_interval=k, baseline=baseline)
    n = len(entries)

    # 0 and K are keyframes; K-1 is the last delta of segment 0; K+1 is the
    # first delta of segment 1; len-1 is the tail (a short final segment).
    for idx in (0, k - 1, k, k + 1, n - 1):
        got = reconstruct_index(enc, idx)
        assert entries_equal(got, entries[idx]), (
            f"boundary index {idx} mismatch (baseline={baseline}): "
            f"got={got!r} want={entries[idx]!r}"
        )


# --------------------------------------------------------------------------- #
# Structure: segment count, keyframe indices, K==1 degeneracy, full keyframes.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
@pytest.mark.parametrize("count", [1, 6, 7, 8, 13, 14, 50, 99, 100, 101, 200])
def test_segment_count_is_ceil(count, keyframe_interval, baseline):
    """Number of segments == ``ceil(count / K)``; keyframe indices == range(0,n,K)."""
    entries = generate_logs(count, seed=count + 1, churn=0.3, schema_width=10)
    enc = encode(entries, keyframe_interval=keyframe_interval, baseline=baseline)

    assert len(enc.segments) == math.ceil(count / keyframe_interval), (
        f"segment count for count={count} K={keyframe_interval}: "
        f"got {len(enc.segments)} want {math.ceil(count / keyframe_interval)}"
    )
    assert keyframe_indices(enc) == list(range(0, count, keyframe_interval))
    assert enc.count == count


@pytest.mark.parametrize("baseline", _BASELINES)
def test_keyframe_interval_one_has_empty_deltas(baseline):
    """``K == 1``: every segment's ``deltas == []`` and segment count == count."""
    entries = generate_logs(40, seed=4, churn=0.5, schema_width=10)
    enc = encode(entries, keyframe_interval=1, baseline=baseline)

    assert len(enc.segments) == len(entries)
    for s, seg in enumerate(enc.segments):
        assert seg.deltas == [], f"segment {s} expected empty deltas, got {seg.deltas!r}"
        assert seg.start_index == s
    assert keyframe_indices(enc) == list(range(len(entries)))


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_keyframes_stored_full(keyframe_interval, baseline):
    """Each segment's stored keyframe canonically equals ``entries[s*K]``."""
    entries = generate_logs(120, seed=8, churn=0.3, schema_width=10)
    enc = encode(entries, keyframe_interval=keyframe_interval, baseline=baseline)

    for s, seg in enumerate(enc.segments):
        expected_global = s * keyframe_interval
        assert seg.start_index == expected_global
        assert entries_equal(seg.keyframe, entries[expected_global]), (
            f"keyframe of segment {s} (global {expected_global}) not stored full: "
            f"got={seg.keyframe!r} want={entries[expected_global]!r}"
        )


@pytest.mark.parametrize("baseline", _BASELINES)
def test_deltas_count_per_segment(baseline):
    """Each segment holds exactly (segment_length - 1) deltas (full+short tail)."""
    entries = generate_logs(45, seed=2, churn=0.3, schema_width=10)
    k = 7
    enc = encode(entries, keyframe_interval=k, baseline=baseline)
    n = len(entries)
    for s, seg in enumerate(enc.segments):
        start = s * k
        seg_len = min(start + k, n) - start
        assert len(seg.deltas) == seg_len - 1, (
            f"segment {s}: expected {seg_len - 1} deltas, got {len(seg.deltas)}"
        )


def test_previous_and_keyframe_encodings_actually_differ():
    """The two baselines yield genuinely different stored deltas (sanity check).

    Not part of the spec's required asserts, but guards against a silent
    collapse where ``baseline`` is ignored: a churny chain with K>2 must produce
    at least one delta whose canonical form differs between the two modes.
    """
    entries = generate_logs(30, seed=13, churn=0.5, schema_width=10)
    k = 7
    enc_prev = encode(entries, keyframe_interval=k, baseline="previous")
    enc_kf = encode(entries, keyframe_interval=k, baseline="keyframe")

    prev_deltas = [d for seg in enc_prev.segments for d in seg.deltas]
    kf_deltas = [d for seg in enc_kf.segments for d in seg.deltas]
    assert prev_deltas != kf_deltas, (
        "previous and keyframe baselines produced identical deltas — "
        "baseline appears to be ignored"
    )


# --------------------------------------------------------------------------- #
# Serialization: to_dict / from_dict and full JSON round-trip.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_from_dict_to_dict_preserves_decode(keyframe_interval, baseline):
    """``EncodedLog.from_dict(enc.to_dict())`` decodes identically to the original."""
    entries = generate_logs(130, seed=17, churn=0.3, schema_width=10)
    enc = encode(entries, keyframe_interval=keyframe_interval, baseline=baseline)

    rebuilt = EncodedLog.from_dict(enc.to_dict())
    assert isinstance(rebuilt, EncodedLog)

    dec_original = decode(enc)
    dec_rebuilt = decode(rebuilt)
    assert len(dec_rebuilt) == len(dec_original)
    for i in range(len(dec_original)):
        assert entries_equal(dec_rebuilt[i], dec_original[i]), (
            f"to_dict/from_dict altered decode at index {i}"
        )


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_json_dumps_succeeds_and_full_json_roundtrip_equals_entries(
    keyframe_interval, baseline
):
    """``json.dumps(enc.to_dict())`` works; json→from_dict→decode equals entries."""
    entries = generate_logs(150, seed=19, churn=0.3, schema_width=10)
    enc = encode(entries, keyframe_interval=keyframe_interval, baseline=baseline)

    # json.dumps must not raise (the artifact is JSON-native).
    blob = json.dumps(enc.to_dict())
    assert isinstance(blob, str)

    # Round-trip through real JSON text, rebuild, and decode.
    revived = EncodedLog.from_dict(json.loads(blob))
    dec = decode(revived)
    assert len(dec) == len(entries)
    for i in range(len(entries)):
        assert entries_equal(dec[i], entries[i]), (
            f"json round-trip altered entry at index {i}: "
            f"K={keyframe_interval} baseline={baseline}"
        )


def test_to_dict_carries_metadata():
    """``to_dict`` round-trips the count / interval / baseline metadata verbatim."""
    entries = generate_logs(25, seed=23, churn=0.3, schema_width=8)
    enc = encode(entries, keyframe_interval=7, baseline="keyframe")
    d = enc.to_dict()
    assert d["count"] == 25
    assert d["keyframe_interval"] == 7
    assert d["baseline"] == "keyframe"
    assert len(d["segments"]) == math.ceil(25 / 7)


# --------------------------------------------------------------------------- #
# No-mutation / no-corruption.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_encode_does_not_mutate_entries(keyframe_interval, baseline):
    """``encode`` leaves the input list byte-identical to its pre-call deepcopy."""
    entries = generate_logs(120, seed=29, churn=0.4, schema_width=10)
    snapshot = copy.deepcopy(entries)

    _ = encode(entries, keyframe_interval=keyframe_interval, baseline=baseline)

    assert entries == snapshot, "encode mutated the input entries list"


@pytest.mark.parametrize("baseline", _BASELINES)
def test_decode_keyframe_is_deepcopied_no_corruption(baseline):
    """Mutating a decoded entry must not corrupt the encoded keyframe.

    Decode once, scribble on ``dec[0]`` (a keyframe — global index 0), then
    decode the *same* EncodedLog again: the second decode must still equal the
    originals, proving the stored keyframe was deep-copied and is unaliased.
    """
    entries = generate_logs(60, seed=31, churn=0.3, schema_width=10)
    enc = encode(entries, keyframe_interval=7, baseline=baseline)

    dec = decode(enc)
    # Corrupt the first decoded entry every way that could leak into shared state.
    dec[0]["__poison__"] = "corrupted"
    dec[0]["ts"] = -999999
    dec[0].clear()  # nuke it entirely

    dec2 = decode(enc)
    assert len(dec2) == len(entries)
    for i in range(len(entries)):
        assert entries_equal(dec2[i], entries[i]), (
            f"second decode corrupted at index {i} after mutating dec[0] "
            f"(baseline={baseline}) — keyframe was not deep-copied"
        )


def test_reconstruct_index_returns_independent_copy():
    """A reconstructed keyframe entry is unaliased: mutating it can't leak back."""
    entries = generate_logs(20, seed=37, churn=0.3, schema_width=10)
    enc = encode(entries, keyframe_interval=7, baseline="previous")

    got0 = reconstruct_index(enc, 0)  # offset-0 keyframe path
    got0["__poison__"] = "x"
    got0.clear()

    # The keyframe and a fresh reconstruct are untouched.
    again = reconstruct_index(enc, 0)
    assert entries_equal(again, entries[0])
    assert entries_equal(enc.segments[0].keyframe, entries[0])


# --------------------------------------------------------------------------- #
# Edge cases / error handling.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_encode_empty_entries(keyframe_interval, baseline):
    """``encode([], …)`` ⇒ count 0, no segments, ``decode`` ⇒ ``[]``."""
    enc = encode([], keyframe_interval=keyframe_interval, baseline=baseline)
    assert enc.count == 0
    assert enc.segments == []
    assert keyframe_indices(enc) == []
    assert decode(enc) == []


def test_reconstruct_index_on_empty_raises_index_error():
    """``reconstruct_index`` into an empty log raises ``IndexError`` for any index."""
    enc = encode([], keyframe_interval=7, baseline="previous")
    with pytest.raises(IndexError):
        reconstruct_index(enc, 0)
    with pytest.raises(IndexError):
        reconstruct_index(enc, 1)
    with pytest.raises(IndexError):
        reconstruct_index(enc, -1)


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_reconstruct_index_out_of_range_raises(keyframe_interval, baseline):
    """Negative and ``>= count`` indices both raise ``IndexError``."""
    entries = generate_logs(30, seed=41, churn=0.3, schema_width=10)
    enc = encode(entries, keyframe_interval=keyframe_interval, baseline=baseline)
    n = len(entries)

    with pytest.raises(IndexError):
        reconstruct_index(enc, -1)
    with pytest.raises(IndexError):
        reconstruct_index(enc, n)  # one past the end
    with pytest.raises(IndexError):
        reconstruct_index(enc, n + 100)


def test_encode_rejects_zero_interval():
    """``keyframe_interval == 0`` raises ``ValueError``."""
    entries = generate_logs(10, seed=1, churn=0.3, schema_width=8)
    with pytest.raises(ValueError):
        encode(entries, keyframe_interval=0)


@pytest.mark.parametrize("bad_k", [0, -1, -7])
def test_encode_rejects_non_positive_interval(bad_k):
    """Any interval ``< 1`` raises ``ValueError``."""
    entries = generate_logs(10, seed=1, churn=0.3, schema_width=8)
    with pytest.raises(ValueError):
        encode(entries, keyframe_interval=bad_k)


def test_encode_rejects_unknown_baseline():
    """An unrecognized baseline (e.g. ``"sideways"``) raises ``ValueError``."""
    entries = generate_logs(10, seed=1, churn=0.3, schema_width=8)
    with pytest.raises(ValueError):
        encode(entries, baseline="sideways")


def test_encode_validates_before_touching_empty_or_not():
    """Validation fires even on empty input (param checks precede the count==0 path)."""
    with pytest.raises(ValueError):
        encode([], keyframe_interval=0)
    with pytest.raises(ValueError):
        encode([], baseline="diagonal")


# --------------------------------------------------------------------------- #
# Both-baseline equivalence: different encodings, identical reconstructions.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("keyframe_interval", [1, 7, 100])
def test_both_baselines_reconstruct_identically(keyframe_interval):
    """``previous`` and ``keyframe`` both ``decode`` back to ``entries`` exactly."""
    entries = generate_logs(180, seed=43, churn=0.4, schema_width=10)

    dec_prev = decode(encode(entries, keyframe_interval=keyframe_interval, baseline="previous"))
    dec_kf = decode(encode(entries, keyframe_interval=keyframe_interval, baseline="keyframe"))

    assert len(dec_prev) == len(entries)
    assert len(dec_kf) == len(entries)
    for i in range(len(entries)):
        assert entries_equal(dec_prev[i], entries[i]), f"previous diverged at {i}"
        assert entries_equal(dec_kf[i], entries[i]), f"keyframe diverged at {i}"
        # And the two reconstructions agree with each other.
        assert entries_equal(dec_prev[i], dec_kf[i]), (
            f"previous vs keyframe reconstruction disagree at index {i}"
        )


def test_both_baselines_random_access_agree():
    """``reconstruct_index`` agrees across baselines for every index on a churny chain."""
    entries = generate_logs(80, seed=47, churn=0.5, schema_width=10)
    k = 7
    enc_prev = encode(entries, keyframe_interval=k, baseline="previous")
    enc_kf = encode(entries, keyframe_interval=k, baseline="keyframe")

    for i in range(len(entries)):
        a = reconstruct_index(enc_prev, i)
        b = reconstruct_index(enc_kf, i)
        assert entries_equal(a, entries[i]), f"previous random-access diverged at {i}"
        assert entries_equal(b, entries[i]), f"keyframe random-access diverged at {i}"
        assert entries_equal(a, b), f"baselines disagree at random-access index {i}"


# --------------------------------------------------------------------------- #
# Dataclass-level sanity: Segment / EncodedLog round-trip their own dicts.
# --------------------------------------------------------------------------- #
def test_segment_to_from_dict_roundtrip_and_detached():
    """``Segment.from_dict(seg.to_dict())`` is equal and detached (deep-copied)."""
    seg = Segment(
        start_index=14,
        keyframe={"ts": 1, "level": "ERROR", "error": "EPIPE"},
        deltas=[{"~": {"ts": 2}}, {"-": ["error"], "~": {"ts": 3, "level": "INFO"}}],
    )
    d = seg.to_dict()
    rebuilt = Segment.from_dict(d)

    assert rebuilt.start_index == 14
    assert entries_equal(rebuilt.keyframe, seg.keyframe)
    assert rebuilt.deltas == seg.deltas

    # Mutating the rebuilt copy must not reach back into the source dict or seg.
    rebuilt.keyframe["__poison__"] = 1
    rebuilt.deltas.append({"~": {"poison": True}})
    assert "__poison__" not in d["keyframe"]
    assert "__poison__" not in seg.keyframe
    assert len(seg.deltas) == 2
