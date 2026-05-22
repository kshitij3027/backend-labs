"""Unit tests for the PCI DSS 10.2 renderer."""
import base64
import os

import pytest

from src.chain.appender import ChainAppender
from src.chain.verifier import ChainVerifier
from src.crypto.hasher import sha256_hex
from src.crypto.signer import Ed25519Signer, Ed25519Verifier
from src.persistence.db import init_db, make_engine, make_session_factory
from src.reports.pci_dss import render_pci_dss_report


@pytest.fixture
def signer():
    return Ed25519Signer(base64.b64encode(os.urandom(32)).decode())


@pytest.fixture
async def chain(tmp_path, signer):
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    engine = make_engine(url)
    await init_db(engine, signer, "test")
    factory = make_session_factory(engine)
    appender = ChainAppender(factory, signer)
    chain_verifier = ChainVerifier(factory, Ed25519Verifier(signer.public_key_b64()))
    yield factory, appender, chain_verifier, signer
    await engine.dispose()


@pytest.mark.asyncio
async def test_pci_report_filters_to_cardholder_only(chain):
    factory, appender, chain_verifier, signer = chain
    # Seed: 2 CARDHOLDER_ events, 1 non-CARDHOLDER.
    await appender.append(actor="a", action="read", resource="CARDHOLDER_4111",
                          success=True, args_digest="0"*64, result_digest="0"*64, processing_ms=1.0)
    await appender.append(actor="b", action="read", resource="CARDHOLDER_4222",
                          success=False, error_message="x",
                          args_digest="0"*64, result_digest="", processing_ms=1.0)
    await appender.append(actor="a", action="read", resource="LOG_app",
                          success=True, args_digest="0"*64, result_digest="0"*64, processing_ms=1.0)
    bundle = await render_pci_dss_report(
        session_factory=factory,
        chain_verifier=chain_verifier,
        signer=signer,
        from_ts="2024-01-01T00:00:00+00:00",
        to_ts="2030-01-01T00:00:00+00:00",
    )
    for r in bundle.records:
        assert r.resource.startswith("CARDHOLDER_")
    assert len(bundle.records) == 2
    assert bundle.extras["chd_access_count"] == 2
    assert bundle.extras["failed_attempt_count"] == 1
    assert "PCI DSS" in bundle.extras["regulation_reference"]


@pytest.mark.asyncio
async def test_pci_attestation_verifies(chain):
    factory, appender, chain_verifier, signer = chain
    await appender.append(actor="a", action="read", resource="CARDHOLDER_x",
                          success=True, args_digest="0"*64, result_digest="0"*64, processing_ms=1.0)
    bundle = await render_pci_dss_report(
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
