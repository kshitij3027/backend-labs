"""Unit tests for the HIPAA renderer — filters to PHI-prefixed resources."""
import base64
import os

import pytest

from src.chain.appender import ChainAppender
from src.chain.verifier import ChainVerifier
from src.crypto.signer import Ed25519Signer, Ed25519Verifier
from src.persistence.db import init_db, make_engine, make_session_factory
from src.reports.hipaa import render_hipaa_report


@pytest.fixture
def signer():
    return Ed25519Signer(base64.b64encode(os.urandom(32)).decode())


@pytest.fixture
async def chain_with_phi_and_non_phi(tmp_path, signer):
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    engine = make_engine(url)
    await init_db(engine, signer, "test")
    factory = make_session_factory(engine)
    appender = ChainAppender(factory, signer)
    chain_verifier = ChainVerifier(factory, Ed25519Verifier(signer.public_key_b64()))
    # Mix: 2 PHI reads, 1 PHI export, 1 PHI failed read, 2 non-PHI reads.
    seed = [
        ("doc1", "read", "PATIENT_42", True, None),
        ("doc1", "export", "PATIENT_42", True, None),
        ("doc2", "read", "PATIENT_43", True, None),
        ("doc1", "read", "PATIENT_42", False, "denied"),
        ("doc1", "read", "LOG_app", True, None),
        ("doc1", "read", "LOG_app", True, None),
    ]
    for actor, act, res, success, err in seed:
        await appender.append(
            actor=actor, action=act, resource=res, success=success,
            args_digest="0" * 64, result_digest="0" * 64 if success else "",
            processing_ms=1.0, error_message=err,
        )
    yield factory, chain_verifier, signer
    await engine.dispose()


@pytest.mark.asyncio
async def test_hipaa_report_filters_to_phi_only(chain_with_phi_and_non_phi):
    factory, chain_verifier, signer = chain_with_phi_and_non_phi
    bundle = await render_hipaa_report(
        session_factory=factory,
        chain_verifier=chain_verifier,
        signer=signer,
        from_ts="2024-01-01T00:00:00+00:00",
        to_ts="2030-01-01T00:00:00+00:00",
    )
    # 4 PHI records (3 successful + 1 failure), 0 non-PHI.
    for rec in bundle.records:
        assert rec.resource.startswith("PATIENT_"), rec.resource
    assert len(bundle.records) == 4
    assert bundle.extras["phi_access_count"] == 4
    assert bundle.extras["suspicious_access_count"] == 1  # the failed read
    assert bundle.extras["regulation_reference"].startswith("HIPAA")


@pytest.mark.asyncio
async def test_hipaa_attestation_signature_verifies(chain_with_phi_and_non_phi):
    factory, chain_verifier, signer = chain_with_phi_and_non_phi
    from src.crypto.hasher import sha256_hex
    bundle = await render_hipaa_report(
        session_factory=factory,
        chain_verifier=chain_verifier,
        signer=signer,
        from_ts="2024-01-01T00:00:00+00:00",
        to_ts="2030-01-01T00:00:00+00:00",
    )
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
