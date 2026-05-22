"""Unit tests for the GDPR renderer — exercised through a live SQLite DB."""
import base64
import os
from datetime import datetime, timezone

import pytest

from src.chain.appender import ChainAppender
from src.chain.verifier import ChainVerifier
from src.crypto.signer import Ed25519Signer, Ed25519Verifier
from src.persistence.db import init_db, make_engine, make_session_factory
from src.reports.gdpr import render_gdpr_report
from src.crypto.hasher import sha256_hex


@pytest.fixture
def signer():
    return Ed25519Signer(base64.b64encode(os.urandom(32)).decode())


@pytest.fixture
async def chain_with_records(tmp_path, signer):
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    engine = make_engine(url)
    await init_db(engine, signer, "test")
    factory = make_session_factory(engine)
    appender = ChainAppender(factory, signer)
    chain_verifier = ChainVerifier(factory, Ed25519Verifier(signer.public_key_b64()))
    # Seed: 3 read, 1 export, 1 redact across two actors.
    actions_resources = [
        ("read", "LOG_app"),
        ("read", "LOG_app"),
        ("read", "LOG_app"),
        ("export", "LOG_app"),
        ("redact", "LOG_pii"),
    ]
    for i, (act, res) in enumerate(actions_resources):
        await appender.append(
            actor="alice" if i % 2 == 0 else "bob",
            action=act,
            resource=res,
            success=True,
            args_digest="0" * 64,
            result_digest="0" * 64,
            processing_ms=1.0,
        )
    yield factory, chain_verifier, signer
    await engine.dispose()


@pytest.mark.asyncio
async def test_gdpr_report_includes_all_records_in_range(chain_with_records):
    factory, chain_verifier, signer = chain_with_records
    bundle = await render_gdpr_report(
        session_factory=factory,
        chain_verifier=chain_verifier,
        signer=signer,
        from_ts="2024-01-01T00:00:00+00:00",
        to_ts="2030-01-01T00:00:00+00:00",
    )
    assert bundle.framework == "gdpr"
    # 5 seeded records + 1 genesis (in 2026-01-01) all fall in the range.
    assert len(bundle.records) == 6
    # Annotations cover all bundle records.
    annotations = bundle.extras["annotations"]
    assert len(annotations) == 6
    # Each annotation must have lawful_basis + processing_purpose.
    for ann in annotations:
        assert "lawful_basis" in ann
        assert "processing_purpose" in ann
        assert "retention_period_days" in ann


@pytest.mark.asyncio
async def test_gdpr_report_actor_filter(chain_with_records):
    factory, chain_verifier, signer = chain_with_records
    bundle = await render_gdpr_report(
        session_factory=factory,
        chain_verifier=chain_verifier,
        signer=signer,
        from_ts="2024-01-01T00:00:00+00:00",
        to_ts="2030-01-01T00:00:00+00:00",
        actor="alice",
    )
    assert bundle.filters["actor"] == "alice"
    # Genesis row is actor="system" so it should be filtered out.
    actors = {r.actor for r in bundle.records}
    assert actors == {"alice"}


@pytest.mark.asyncio
async def test_gdpr_lawful_basis_mapping(chain_with_records):
    factory, chain_verifier, signer = chain_with_records
    bundle = await render_gdpr_report(
        session_factory=factory,
        chain_verifier=chain_verifier,
        signer=signer,
        from_ts="2024-01-01T00:00:00+00:00",
        to_ts="2030-01-01T00:00:00+00:00",
    )
    by_action = {ann["action"]: ann["lawful_basis"] for ann in bundle.extras["annotations"]}
    assert by_action["read"] == "legitimate_interests"
    assert by_action["export"] == "legal_obligation"
    assert by_action["redact"] == "legal_obligation"
    assert by_action["genesis"] == "system_bootstrap"


@pytest.mark.asyncio
async def test_gdpr_attestation_signature_verifies(chain_with_records):
    factory, chain_verifier, signer = chain_with_records
    bundle = await render_gdpr_report(
        session_factory=factory,
        chain_verifier=chain_verifier,
        signer=signer,
        from_ts="2024-01-01T00:00:00+00:00",
        to_ts="2030-01-01T00:00:00+00:00",
    )
    # Re-derive the signed payload and verify the signature.
    verifier = Ed25519Verifier(signer.public_key_b64())
    sig_payload = {
        "framework": bundle.framework,
        "generated_at": bundle.generated_at,
        "time_range": list(bundle.time_range),
        "filters": bundle.filters,
        "record_seqs": [r.seq for r in bundle.records],
        "record_self_hashes": [r.self_hash for r in bundle.records],
        "verify_ok": bundle.verify_result.ok,
        "verify_head_seq": bundle.verify_result.head_seq,
        "verify_first_break_seq": bundle.verify_result.first_break_seq,
        "extras": bundle.extras,
    }
    digest = sha256_hex(sig_payload)
    assert verifier.verify(bundle.attestation_signature, digest) is True
