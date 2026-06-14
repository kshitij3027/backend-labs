"""Unit tests for :mod:`app.encoders` — the OPTIONAL typed-encoder layer.

This layer wraps the frozen commit-4 field-diff primitives without touching them:
:func:`~app.encoders.compress_delta` moves *eligible* changed values out of a
plain delta's ``"~"`` part into a self-describing, reversible ``"@"`` instruction
part, and :func:`~app.encoders.expand_delta` rebuilds the plain delta against the
same baseline. The tests pin down the contract at two altitudes:

* **Encoder-level (unit):** int-delta and str-delta round-trips, field scoping
  (only the allow-list is int-delta'd, bools excluded), exact code-point fidelity
  for unicode prefix/suffix deltas, the ``expand_delta`` identity fast path, and
  the general ``expand(compress(d)) == d`` inverse over many generator pairs.
* **Codec integration (the load-bearing guarantees):** byte-identity when the
  layer is off (no ``encoder_config`` ≡ ``disabled()`` ≡ plain output, with NO
  ``"@"`` anywhere), full typed round-trip across baselines × K × seeds × churn,
  the per-delta "never larger" size guard (segment-by-segment vs the plain
  encoding), and proof the typed path is actually exercised (some ``ts`` deltas
  carry an ``"i"`` instruction, and the total encoded size never grows).
"""
from __future__ import annotations

import pytest

from app.encoders import EncoderConfig, compress_delta, expand_delta
from app.codec import (
    encode,
    decode,
    reconstruct_index,
    entries_equal,
    canonical_bytes,
    diff_entries,
)
from app.generator import generate_logs

# Mirror the encoder wire tokens locally so assertions read against the documented
# constants rather than re-typed literals scattered through the file.
CHANGED = "~"
REMOVED = "-"
ENCODED = "@"
INT_TAG = "i"
STR_TAG = "s"


# --------------------------------------------------------------------------- #
# Small helpers.
# --------------------------------------------------------------------------- #
def _delta_from_changed(changed: dict, removed: list | None = None) -> dict:
    """Build a plain (commit-4 shaped) delta dict from a ``~`` map (+ optional ``-``)."""
    d: dict = {}
    if changed:
        d[CHANGED] = dict(changed)
    if removed:
        d[REMOVED] = list(removed)
    return d


def _all_deltas(enc) -> list[dict]:
    """Flatten every stored delta across all segments, in global order."""
    return [d for seg in enc.segments for d in seg.deltas]


# =========================================================================== #
# ENCODER-LEVEL UNIT TESTS
# =========================================================================== #

# --------------------------------------------------------------------------- #
# int-delta round-trip: a designated field (``ts``) is moved to ``"@"`` as
# ["i", new-base]; expand recovers the original plain value. Negative / zero /
# large deltas all covered.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "base_ts,new_ts",
    [
        (1_700_000_000_000, 1_700_000_000_137),   # small positive
        (1_700_000_000_137, 1_700_000_000_000),   # negative delta
        (1_700_000_000_000, 1_700_000_000_000),   # zero delta (still a "change" entry)
        (5, 10_000_000_000_000),                   # very large positive delta
        (10_000_000_000_000, 5),                   # very large negative delta
        (0, 1),                                    # minimal
    ],
)
def test_int_delta_roundtrip_ts(base_ts, new_ts):
    """``ts`` change compresses to ``["i", new-base]`` and expands back exactly."""
    cfg = EncoderConfig.all_on()
    base = {"ts": base_ts, "level": "INFO"}
    plain = _delta_from_changed({"ts": new_ts})

    typed = compress_delta(plain, base, cfg)
    # ts moved into "@" as an int-delta instruction, not left verbatim in "~".
    assert ENCODED in typed
    assert typed[ENCODED]["ts"] == [INT_TAG, new_ts - base_ts]
    assert CHANGED not in typed or "ts" not in typed.get(CHANGED, {})

    # Expansion recovers the exact original plain delta / value.
    recovered = expand_delta(typed, base)
    assert recovered == plain
    assert recovered[CHANGED]["ts"] == new_ts


def test_int_delta_bytes_sent_is_also_designated():
    """``bytes_sent`` (the other allow-list field) is int-delta'd too."""
    cfg = EncoderConfig.all_on()
    base = {"ts": 1, "bytes_sent": 4096}
    plain = _delta_from_changed({"bytes_sent": 8192})

    typed = compress_delta(plain, base, cfg)
    assert typed[ENCODED]["bytes_sent"] == [INT_TAG, 8192 - 4096]
    assert expand_delta(typed, base) == plain


# --------------------------------------------------------------------------- #
# int-delta scoping: only allow-list fields are int-delta'd; bools excluded.
# --------------------------------------------------------------------------- #
def test_int_delta_does_not_touch_status():
    """``status`` is an int but NOT in ``int_delta_fields`` → stays verbatim in ``~``."""
    cfg = EncoderConfig.all_on()
    base = {"ts": 1, "status": 200}
    plain = _delta_from_changed({"status": 500})

    typed = compress_delta(plain, base, cfg)
    assert typed.get(CHANGED, {}).get("status") == 500
    assert "status" not in typed.get(ENCODED, {})
    assert expand_delta(typed, base) == plain


def test_int_delta_does_not_touch_latency_ms():
    """``latency_ms`` (arbitrary numeric, not designated) is never int-delta'd."""
    cfg = EncoderConfig.all_on()
    base = {"ts": 1, "latency_ms": 1473}
    plain = _delta_from_changed({"latency_ms": 12})

    typed = compress_delta(plain, base, cfg)
    assert typed.get(CHANGED, {}).get("latency_ms") == 12
    assert "latency_ms" not in typed.get(ENCODED, {})
    assert expand_delta(typed, base) == plain


def test_bool_in_designated_field_is_not_int_delta():
    """A ``bool`` value in a designated field is excluded (bool is an int subclass)."""
    cfg = EncoderConfig.all_on()
    # Put a bool where an int-delta field name lives on both sides.
    base = {"ts": True}
    plain = _delta_from_changed({"ts": False})

    typed = compress_delta(plain, base, cfg)
    # Bool must NOT be int-delta'd; left verbatim so its type survives.
    assert "ts" not in typed.get(ENCODED, {})
    assert typed.get(CHANGED, {}).get("ts") is False
    assert expand_delta(typed, base) == plain


def test_int_delta_skipped_when_base_value_is_bool():
    """Even if the new value is a real int, a bool BASE blocks int-delta (type safety)."""
    cfg = EncoderConfig.all_on()
    base = {"ts": True}                      # base is bool
    plain = _delta_from_changed({"ts": 5})   # new is int
    typed = compress_delta(plain, base, cfg)
    assert "ts" not in typed.get(ENCODED, {})
    assert typed.get(CHANGED, {}).get("ts") == 5
    assert expand_delta(typed, base) == plain


# --------------------------------------------------------------------------- #
# str-delta round-trip: shared prefix/suffix → ["s", p, s, middle].
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "base_str,new_str",
    [
        ("request started", "request completed"),   # shared prefix "request "
        ("/api/v1/users", "/api/v1/orders"),         # shared prefix + suffix "s"
        ("/v1/login", "/v1/logout"),                 # one extends the other-ish
        ("abcXYZdef", "abcQQQQdef"),                 # prefix "abc" + suffix "def"
        ("hello world", "hello world"),              # identical (degenerate)
        ("prefix", "prefixABC"),                     # base is a prefix of new
        ("prefixABC", "prefix"),                      # new is a prefix of base
        ("", "nonempty"),                            # empty base
        ("nonempty", ""),                            # empty new
        ("", ""),                                    # both empty
        ("totally", "different"),                    # no common affix
        ("abcabc", "abc"),                           # repeated-stem ambiguity guard
    ],
)
def test_str_delta_roundtrip(base_str, new_str):
    """Prefix/suffix string delta compresses and expands to the exact original."""
    cfg = EncoderConfig.all_on()
    base = {"msg": base_str, "ts": 1}
    plain = _delta_from_changed({"msg": new_str})

    typed = compress_delta(plain, base, cfg)
    assert "msg" in typed.get(ENCODED, {}), "string field should be str-delta'd"
    tag, p, s, middle = typed[ENCODED]["msg"]
    assert tag == STR_TAG
    # prefix/suffix must not overlap: p + s <= min(len(base), len(new)).
    assert p + s <= min(len(base_str), len(new_str))
    # Reconstruct and assert exact equality (the recovered plain delta == original).
    recovered = expand_delta(typed, base)
    assert recovered == plain
    assert recovered[CHANGED]["msg"] == new_str


@pytest.mark.parametrize(
    "base_str,new_str",
    [
        ("café", "cafés"),                  # accented, suffix grows by 1 codepoint
        ("cafés", "café"),                  # accented, shrink
        ("日本語ログ", "日本語データ"),        # CJK shared prefix "日本語"
        ("日本語", "日本語"),                 # identical CJK
        ("emoji-🚀-tail", "emoji-🎉-tail"),  # emoji middle swap, shared affixes
        ("🚀🚀🚀", "🚀🎉🚀"),                  # all-emoji middle swap
        ("naïve résumé", "naïve resume"),    # accents in the middle differ
        ("Ωμέγα", "Ωμικρόν"),                # Greek shared prefix "Ω"
        ("Привет", "Прощай"),                # Cyrillic shared prefix "Пр"
    ],
)
def test_str_delta_unicode_exact_codepoint_roundtrip(base_str, new_str):
    """Unicode strings round-trip at the exact code-point level (no byte split)."""
    cfg = EncoderConfig.all_on()
    base = {"msg": base_str, "ts": 1}
    plain = _delta_from_changed({"msg": new_str})

    typed = compress_delta(plain, base, cfg)
    recovered = expand_delta(typed, base)
    assert recovered == plain
    got = recovered[CHANGED]["msg"]
    assert got == new_str
    # Code-point identity: same length and same ordinals, character by character.
    assert len(got) == len(new_str)
    assert [ord(c) for c in got] == [ord(c) for c in new_str]


def test_str_delta_not_applied_when_key_absent_in_base():
    """A string field that is NEW (not in base) is left verbatim, not str-delta'd."""
    cfg = EncoderConfig.all_on()
    base = {"ts": 1}
    plain = _delta_from_changed({"msg": "brand new"})
    typed = compress_delta(plain, base, cfg)
    assert "msg" not in typed.get(ENCODED, {})
    assert typed.get(CHANGED, {}).get("msg") == "brand new"
    assert expand_delta(typed, base) == plain


def test_str_delta_disabled_leaves_strings_verbatim():
    """With ``str_delta=False`` strings stay verbatim even when ``enabled=True``."""
    cfg = EncoderConfig(enabled=True, str_delta=False)
    base = {"msg": "request started", "ts": 1}
    plain = _delta_from_changed({"msg": "request completed"})
    typed = compress_delta(plain, base, cfg)
    assert "msg" not in typed.get(ENCODED, {})
    assert typed.get(CHANGED, {}).get("msg") == "request completed"
    assert expand_delta(typed, base) == plain


def test_removed_keys_carried_through_compress_and_expand():
    """The ``"-"`` (removed keys) part is carried through untouched by both directions."""
    cfg = EncoderConfig.all_on()
    base = {"ts": 1, "error": "EPIPE", "msg": "request started"}
    plain = _delta_from_changed({"ts": 2, "msg": "request completed"}, removed=["error"])

    typed = compress_delta(plain, base, cfg)
    assert typed.get(REMOVED) == ["error"]
    recovered = expand_delta(typed, base)
    assert recovered == plain
    assert recovered.get(REMOVED) == ["error"]


# --------------------------------------------------------------------------- #
# expand_delta identity fast-path: no "@" key ⇒ returned unchanged.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "delta",
    [
        {},
        {"~": {"a": 1}},
        {"-": ["x"]},
        {"~": {"a": 1, "b": "two"}, "-": ["c", "d"]},
        {"~": {"nested": {"k": [1, 2, 3]}}},
    ],
    ids=["empty", "changed-only", "removed-only", "both", "nested-value"],
)
def test_expand_delta_identity_when_no_encoded_part(delta):
    """A delta without ``"@"`` is returned unchanged (identity passthrough)."""
    base = {"a": 0, "b": "x", "c": 1, "d": 2, "nested": {"k": [9]}}
    out = expand_delta(delta, base)
    assert out == delta
    # It is the very same object (no copy on the fast path).
    assert out is delta


def test_expand_delta_raises_on_unknown_instruction_tag():
    """An unknown instruction tag fails loudly rather than silently dropping a field."""
    base = {"x": 1}
    bad = {ENCODED: {"x": ["?", 99]}}
    with pytest.raises(ValueError):
        expand_delta(bad, base)


# --------------------------------------------------------------------------- #
# expand inverts compress over many (base, cur) pairs with cfg = all_on().
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("seed", [0, 1, 7, 42, 123, 999])
@pytest.mark.parametrize("churn", [0.0, 0.2, 0.5, 1.0])
def test_expand_inverts_compress_over_generator_pairs(seed, churn):
    """For adjacent generator pairs, ``expand(compress(diff, base)) == diff``.

    The encoder layer is a *reversible reshaping* of the plain delta: expanding
    the typed candidate against the same baseline must reproduce exactly what
    ``diff_entries(base, cur)`` produced — byte-for-byte the plain delta.
    """
    cfg = EncoderConfig.all_on()
    logs = generate_logs(120, seed=seed, churn=churn, schema_width=10)
    for i in range(1, len(logs)):
        base, cur = logs[i - 1], logs[i]
        plain = diff_entries(base, cur)
        typed = compress_delta(plain, base, cfg)
        recovered = expand_delta(typed, base)
        assert recovered == plain, (
            f"expand∘compress != plain at i={i} seed={seed} churn={churn}: "
            f"plain={plain!r} typed={typed!r} recovered={recovered!r}"
        )


def test_compress_disabled_is_identity():
    """With ``enabled=False`` (``disabled()``), compress returns its input unchanged."""
    cfg = EncoderConfig.disabled()
    base = {"ts": 1, "msg": "request started", "status": 200}
    plain = _delta_from_changed({"ts": 2, "msg": "request completed", "status": 500})
    out = compress_delta(plain, base, cfg)
    assert out is plain  # exact identity object on the off path
    assert ENCODED not in out


def test_compress_and_base_not_mutated():
    """Neither the input delta nor the base entry is mutated by compress/expand."""
    cfg = EncoderConfig.all_on()
    base = {"ts": 1, "msg": "request started"}
    plain = _delta_from_changed({"ts": 2, "msg": "request completed"})
    base_snap = dict(base)
    plain_snap = {k: (dict(v) if isinstance(v, dict) else v) for k, v in plain.items()}

    typed = compress_delta(plain, base, cfg)
    _ = expand_delta(typed, base)

    assert base == base_snap, "compress/expand mutated base"
    assert plain == plain_snap, "compress mutated the input delta"


# =========================================================================== #
# CODEC INTEGRATION TESTS — the load-bearing guarantees.
# =========================================================================== #

_BASELINES = ["previous", "keyframe"]
_INTERVALS = [1, 7, 100]


# --------------------------------------------------------------------------- #
# Backward-compat byte-identity: no config ≡ disabled() ≡ plain output, and NO
# "@" appears anywhere when encoders are off.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
@pytest.mark.parametrize("seed,churn", [(7, 0.3), (42, 0.0), (123, 1.0)])
def test_backward_compat_byte_identity_off_paths(
    keyframe_interval, baseline, seed, churn
):
    """``encode(...)`` (no cfg) == ``encoder_config=disabled()`` == plain — byte-identical.

    All three must produce the identical serialized artifact, and no stored delta
    may contain a ``"@"`` part when the layer is off.
    """
    entries = generate_logs(200, seed=seed, churn=churn, schema_width=10)

    enc_none = encode(entries, keyframe_interval=keyframe_interval, baseline=baseline)
    enc_disabled = encode(
        entries,
        keyframe_interval=keyframe_interval,
        baseline=baseline,
        encoder_config=EncoderConfig.disabled(),
    )

    # Byte-identical serialized artifacts.
    assert enc_none.to_dict() == enc_disabled.to_dict(), (
        "disabled() encoder drifted from the no-config (plain) encoding"
    )

    # No "@" anywhere in any delta on either off-path artifact.
    for label, enc in (("no-cfg", enc_none), ("disabled", enc_disabled)):
        for d in _all_deltas(enc):
            assert ENCODED not in d, (
                f"{label} encoding unexpectedly produced an '@' part: {d!r}"
            )


def test_disabled_config_object_equals_none_canonical_bytes():
    """Stronger: the canonical bytes of the two off-path artifacts are identical."""
    entries = generate_logs(150, seed=5, churn=0.5, schema_width=10)
    a = encode(entries, keyframe_interval=7, baseline="previous")
    b = encode(
        entries,
        keyframe_interval=7,
        baseline="previous",
        encoder_config=EncoderConfig.disabled(),
    )
    assert canonical_bytes(a.to_dict()) == canonical_bytes(b.to_dict())


# --------------------------------------------------------------------------- #
# Typed round-trip: decode(encode(..., all_on())) == entries; reconstruct_index
# matches every index. Across baselines × K × seeds × churn.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
@pytest.mark.parametrize(
    "seed,churn",
    [(0, 0.0), (3, 0.2), (42, 0.5), (123, 1.0), (999, 0.0), (5, 1.0)],
)
def test_typed_full_roundtrip_decode(keyframe_interval, baseline, seed, churn):
    """``decode(encode(..., all_on()))`` is element-wise canonically equal to entries."""
    cfg = EncoderConfig.all_on()
    entries = generate_logs(180, seed=seed, churn=churn, schema_width=10)
    enc = encode(
        entries,
        keyframe_interval=keyframe_interval,
        baseline=baseline,
        encoder_config=cfg,
    )
    dec = decode(enc)
    assert len(dec) == len(entries)
    for i in range(len(entries)):
        assert entries_equal(dec[i], entries[i]), (
            f"typed round-trip diverged at index {i}: K={keyframe_interval} "
            f"baseline={baseline} seed={seed} churn={churn} "
            f"got={dec[i]!r} want={entries[i]!r}"
        )


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
@pytest.mark.parametrize("seed,churn", [(7, 0.2), (42, 0.5), (5, 1.0)])
def test_typed_reconstruct_index_matches_every_index(
    keyframe_interval, baseline, seed, churn
):
    """``reconstruct_index`` matches every index under the typed encoding."""
    cfg = EncoderConfig.all_on()
    entries = generate_logs(120, seed=seed, churn=churn, schema_width=10)
    enc = encode(
        entries,
        keyframe_interval=keyframe_interval,
        baseline=baseline,
        encoder_config=cfg,
    )
    for i in range(len(entries)):
        got = reconstruct_index(enc, i)
        assert entries_equal(got, entries[i]), (
            f"typed reconstruct_index diverged at index {i}: K={keyframe_interval} "
            f"baseline={baseline} seed={seed} churn={churn} "
            f"got={got!r} want={entries[i]!r}"
        )


def test_typed_roundtrip_with_wide_schema_and_unicode_ish_fields():
    """Widest catalogue at full churn under the typed encoder still round-trips."""
    cfg = EncoderConfig.all_on()
    entries = generate_logs(300, seed=77, churn=1.0, schema_width=12)
    enc = encode(entries, keyframe_interval=50, baseline="previous", encoder_config=cfg)
    dec = decode(enc)
    for i in range(len(entries)):
        assert entries_equal(dec[i], entries[i]), f"diverged at {i}"


# --------------------------------------------------------------------------- #
# Never larger: every typed delta's canonical bytes <= the plain delta stored at
# the same position. Encode both ways and compare segment-by-segment.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
@pytest.mark.parametrize(
    "seed,churn",
    [(0, 0.0), (3, 0.2), (42, 0.5), (123, 1.0), (5, 1.0)],
)
def test_typed_delta_never_larger_than_plain(
    keyframe_interval, baseline, seed, churn
):
    """For every delta, ``len(canonical_bytes(typed)) <= len(canonical_bytes(plain))``.

    Encodes the same entries with the encoder OFF (plain) and ON (all_on()) and
    compares the stored deltas position-by-position. The codec's per-delta size
    guard must guarantee the typed delta is never larger than the plain one.
    """
    entries = generate_logs(200, seed=seed, churn=churn, schema_width=10)

    enc_plain = encode(
        entries, keyframe_interval=keyframe_interval, baseline=baseline
    )
    enc_typed = encode(
        entries,
        keyframe_interval=keyframe_interval,
        baseline=baseline,
        encoder_config=EncoderConfig.all_on(),
    )

    assert len(enc_plain.segments) == len(enc_typed.segments)
    for s, (sp, st) in enumerate(zip(enc_plain.segments, enc_typed.segments)):
        assert len(sp.deltas) == len(st.deltas), (
            f"segment {s} delta count drift: plain={len(sp.deltas)} typed={len(st.deltas)}"
        )
        for j, (pd, td) in enumerate(zip(sp.deltas, st.deltas)):
            lp = len(canonical_bytes(pd))
            lt = len(canonical_bytes(td))
            assert lt <= lp, (
                f"typed delta LARGER than plain at segment {s} delta {j}: "
                f"plain={lp}B {pd!r} vs typed={lt}B {td!r} "
                f"(K={keyframe_interval} baseline={baseline} seed={seed} churn={churn})"
            )


# --------------------------------------------------------------------------- #
# Actually exercised: realistic data DOES trip the typed path (an "i" instruction
# on ts), and the total encoded size never grows vs encoders-off.
# --------------------------------------------------------------------------- #
def test_typed_encoding_actually_kicks_in_on_ts():
    """With K large and small ts steps, some deltas carry an ``"i"`` instruction for ``ts``.

    K=100 keeps a whole segment diffing forward, churn=0.2 keeps ts steps small
    relative to the 13-digit absolute ts, so the int-delta is strictly shorter and
    the size guard adopts it. We require at least one such adopted instruction.
    """
    cfg = EncoderConfig.all_on()
    entries = generate_logs(300, seed=7, churn=0.2, schema_width=10)
    enc = encode(entries, keyframe_interval=100, baseline="previous", encoder_config=cfg)

    ts_int_deltas = 0
    for d in _all_deltas(enc):
        instr = d.get(ENCODED, {})
        if isinstance(instr.get("ts"), list) and instr["ts"][0] == INT_TAG:
            ts_int_deltas += 1
    assert ts_int_deltas > 0, (
        "expected at least one adopted int-delta '@' instruction for ts; "
        "the typed encoder never kicked in on realistic data"
    )


def test_typed_encoding_uses_str_delta_when_it_pays_off():
    """When strings share a LONG prefix/suffix, str-delta ``"s"`` instructions get adopted.

    The generator's vocabularies are deliberately short (``/v1/login`` etc.), so the
    4-element ``["s", p, s, mid]`` envelope usually costs more than the verbatim
    value and the codec's size guard correctly rejects it — that rejection is the
    guard working, not a bug. To prove the str-delta path *is* live, we feed strings
    with long shared affixes (where it genuinely wins) and require: (a) at least one
    adopted ``"s"`` instruction, (b) every such instruction's delta is strictly
    smaller than the plain delta (the reason it was adopted), and (c) it all still
    round-trips exactly.
    """
    cfg = EncoderConfig.all_on()
    # Long, stable prefix and suffix with only a short middle changing each step —
    # exactly the shape common-prefix/suffix delta is built to exploit.
    entries = []
    ts = 1_700_000_000_000
    head, tail = "req-" + ("A" * 40), ("Z" * 40) + "-end"
    for i in range(40):
        ts += 100
        entries.append(
            {
                "ts": ts,
                "trace_id": f"{head}-seg{i:03d}-{tail}",
                "msg": "the quick brown fox jumps over the lazy dog number " + str(i % 7),
            }
        )

    enc_typed = encode(
        entries, keyframe_interval=100, baseline="previous", encoder_config=cfg
    )
    enc_plain = encode(entries, keyframe_interval=100, baseline="previous")

    str_deltas = 0
    for st, sp in zip(enc_typed.segments, enc_plain.segments):
        for td, pd in zip(st.deltas, sp.deltas):
            has_str = any(
                isinstance(instr, list) and instr and instr[0] == STR_TAG
                for instr in td.get(ENCODED, {}).values()
            )
            if has_str:
                str_deltas += 1
                # It was adopted precisely because it is strictly smaller.
                assert len(canonical_bytes(td)) < len(canonical_bytes(pd)), (
                    f"adopted str-delta not strictly smaller: typed={td!r} plain={pd!r}"
                )

    assert str_deltas > 0, (
        "expected at least one adopted str-delta '@' instruction on long-affix data"
    )

    # And the whole thing round-trips exactly under the str-delta path.
    dec = decode(enc_typed)
    for i in range(len(entries)):
        assert entries_equal(dec[i], entries[i]), f"str-delta round-trip diverged at {i}"


@pytest.mark.parametrize("baseline", _BASELINES)
@pytest.mark.parametrize("keyframe_interval", _INTERVALS)
@pytest.mark.parametrize("seed,churn", [(7, 0.2), (42, 0.5), (5, 1.0), (123, 0.0)])
def test_total_encoded_size_never_grows_with_encoders_on(
    keyframe_interval, baseline, seed, churn
):
    """Total encoded byte size with ``all_on()`` is ``<=`` the size with encoders off.

    A non-strict improvement at minimum — the per-delta guard makes a regression
    impossible — measured on the canonical bytes of the full serialized artifact.
    """
    entries = generate_logs(250, seed=seed, churn=churn, schema_width=10)

    size_off = len(
        canonical_bytes(
            encode(
                entries, keyframe_interval=keyframe_interval, baseline=baseline
            ).to_dict()
        )
    )
    size_on = len(
        canonical_bytes(
            encode(
                entries,
                keyframe_interval=keyframe_interval,
                baseline=baseline,
                encoder_config=EncoderConfig.all_on(),
            ).to_dict()
        )
    )
    assert size_on <= size_off, (
        f"encoders-on grew the artifact: on={size_on}B off={size_off}B "
        f"(K={keyframe_interval} baseline={baseline} seed={seed} churn={churn})"
    )


def test_typed_encoding_strictly_smaller_in_favorable_regime():
    """In a ts-friendly regime the typed encoding is *strictly* smaller (real win).

    Not just non-regression: with a large K (long forward chains) and small ts
    steps the int-delta on ``ts`` shaves real bytes off many deltas, so the total
    must come out strictly below the encoders-off size.
    """
    entries = generate_logs(500, seed=7, churn=0.2, schema_width=10)
    size_off = len(canonical_bytes(encode(entries, keyframe_interval=100).to_dict()))
    size_on = len(
        canonical_bytes(
            encode(
                entries, keyframe_interval=100, encoder_config=EncoderConfig.all_on()
            ).to_dict()
        )
    )
    assert size_on < size_off, (
        f"expected a strict size win in the favorable regime: on={size_on}B off={size_off}B"
    )
