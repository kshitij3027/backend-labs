import pytest
from pydantic import ValidationError

from src.chain.schema import (
    AuditRecord,
    AuditRecordPayload,
    args_digest,
    canonical_dict,
    compute_self_hash,
    result_digest,
)


# --- Helpers ----------------------------------------------------------------

ZERO64 = "0" * 64
FAKE_HASH = "a" * 64
FAKE_SIG = "fakesignaturebase64=="


def _payload(**overrides):
    base = dict(
        seq=1,
        timestamp_utc="2026-05-22T10:00:00+00:00",
        actor="alice",
        action="read",
        resource="LOG_/var/log/app",
        success=True,
        error_message=None,
        processing_ms=12.5,
        args_digest=ZERO64,
        result_digest=ZERO64,
        prev_hash=ZERO64,
    )
    base.update(overrides)
    return AuditRecordPayload(**base)


# --- AuditRecordPayload basics ---------------------------------------------

def test_payload_constructs_with_all_fields():
    p = _payload()
    assert p.seq == 1
    assert p.actor == "alice"
    assert p.prev_hash == ZERO64


def test_payload_rejects_unknown_field():
    with pytest.raises(ValidationError):
        AuditRecordPayload(
            seq=1, timestamp_utc="t", actor="a", action="r", resource="x",
            success=True, args_digest=ZERO64, result_digest=ZERO64,
            prev_hash=ZERO64, unknown_field="boom",
        )


def test_payload_rejects_wrong_prev_hash_length():
    with pytest.raises(ValidationError):
        _payload(prev_hash="0" * 63)


def test_payload_is_frozen():
    p = _payload()
    with pytest.raises(ValidationError):
        p.actor = "mallory"  # type: ignore[misc]


def test_payload_seq_must_be_nonneg():
    with pytest.raises(ValidationError):
        _payload(seq=-1)


# --- AuditRecord (sealed) -------------------------------------------------

def test_audit_record_adds_self_hash_and_signature():
    record = AuditRecord(
        seq=1, timestamp_utc="t", actor="a", action="r", resource="x",
        success=True, args_digest=ZERO64, result_digest=ZERO64,
        prev_hash=ZERO64, self_hash=FAKE_HASH, signature=FAKE_SIG,
    )
    assert record.self_hash == FAKE_HASH
    assert record.signature == FAKE_SIG


def test_audit_record_rejects_wrong_self_hash_length():
    with pytest.raises(ValidationError):
        AuditRecord(
            seq=1, timestamp_utc="t", actor="a", action="r", resource="x",
            success=True, args_digest=ZERO64, result_digest=ZERO64,
            prev_hash=ZERO64, self_hash="a" * 63, signature=FAKE_SIG,
        )


# --- canonical_dict drops seal fields --------------------------------------

def test_canonical_dict_excludes_seal_fields():
    record = AuditRecord(
        seq=1, timestamp_utc="t", actor="a", action="r", resource="x",
        success=True, args_digest=ZERO64, result_digest=ZERO64,
        prev_hash=ZERO64, self_hash=FAKE_HASH, signature=FAKE_SIG,
    )
    d = canonical_dict(record)
    assert "self_hash" not in d
    assert "signature" not in d
    assert d["seq"] == 1


def test_canonical_dict_on_payload_returns_all_payload_fields():
    p = _payload()
    d = canonical_dict(p)
    assert set(d.keys()) == {
        "seq", "timestamp_utc", "actor", "action", "resource",
        "success", "error_message", "processing_ms",
        "args_digest", "result_digest", "prev_hash",
    }


# --- compute_self_hash determinism ----------------------------------------

def test_compute_self_hash_deterministic():
    h1 = compute_self_hash(_payload())
    h2 = compute_self_hash(_payload())
    assert h1 == h2
    assert len(h1) == 64


def test_compute_self_hash_changes_on_field_change():
    h1 = compute_self_hash(_payload(actor="alice"))
    h2 = compute_self_hash(_payload(actor="bob"))
    assert h1 != h2


def test_compute_self_hash_ignores_construction_order():
    """Constructing payload with kwargs in different orders shouldn't matter."""
    # Pydantic stores fields by declaration order, but we pass them in
    # arbitrary order here — outcome should be byte-identical.
    a = AuditRecordPayload(
        seq=1, prev_hash=ZERO64, actor="alice", action="r", resource="x",
        timestamp_utc="t", success=True, args_digest=ZERO64,
        result_digest=ZERO64,
    )
    b = AuditRecordPayload(
        prev_hash=ZERO64, seq=1, success=True, action="r",
        timestamp_utc="t", actor="alice", args_digest=ZERO64,
        result_digest=ZERO64, resource="x",
    )
    assert compute_self_hash(a) == compute_self_hash(b)


# --- args_digest -----------------------------------------------------------

def test_args_digest_deterministic():
    d1 = args_digest((1, 2, "x"), {"k": 1})
    d2 = args_digest((1, 2, "x"), {"k": 1})
    assert d1 == d2


def test_args_digest_differs_for_different_args():
    a = args_digest((1, 2), {})
    b = args_digest((1, 3), {})
    assert a != b


def test_args_digest_differs_args_vs_kwargs():
    """f(1, 2) vs f(a=1, b=2) must produce different digests."""
    a = args_digest((1, 2), {})
    b = args_digest((), {"a": 1, "b": 2})
    assert a != b


def test_args_digest_handles_non_jsonable():
    class Obj:
        def __repr__(self):
            return "Obj(stable)"

    d1 = args_digest((Obj(),), {})
    d2 = args_digest((Obj(),), {})
    assert d1 == d2  # repr-based fallback is deterministic if repr is


# --- result_digest --------------------------------------------------------

def test_result_digest_deterministic():
    assert result_digest({"a": 1, "b": 2}) == result_digest({"b": 2, "a": 1})


def test_result_digest_collapses_huge_lists():
    """A list of 10_000 entries must collapse to a count summary."""
    big = list(range(10_000))
    d = result_digest(big)
    # The summary form must be deterministic across calls with same size.
    assert d == result_digest(list(range(10_000)))
    # But two lists with same elements but different size must differ.
    assert d != result_digest(list(range(10_001)))


def test_result_digest_handles_none():
    # Sanity: must not raise.
    assert isinstance(result_digest(None), str)
    assert len(result_digest(None)) == 64


def test_result_digest_handles_nested():
    d = result_digest({"rows": [{"id": 1}, {"id": 2}]})
    assert len(d) == 64
