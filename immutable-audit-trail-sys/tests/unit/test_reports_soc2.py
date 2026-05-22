"""Unit tests for the SOC 2 CC7.2 renderer."""
import base64
import os
from datetime import datetime, timezone

import pytest

from src.chain.appender import ChainAppender
from src.chain.verifier import ChainVerifier
from src.crypto.hasher import sha256_hex
from src.crypto.signer import Ed25519Signer, Ed25519Verifier
from src.persistence.db import init_db, make_engine, make_session_factory
from src.reports.soc2 import render_soc2_report, _is_off_hours


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


def test_off_hours_classifier():
    assert _is_off_hours("2026-05-22T23:30:00+00:00") is True   # 23:30
    assert _is_off_hours("2026-05-22T03:15:00+00:00") is True   # 03:15
    assert _is_off_hours("2026-05-22T14:00:00+00:00") is False  # 14:00
    assert _is_off_hours("malformed") is False                  # bad input -> False


@pytest.mark.asyncio
async def test_soc2_report_with_mixed_records(chain):
    factory, appender, chain_verifier, signer = chain
    # Seed: 2 normal-hours success, 1 off-hours success, 2 failures.
    await appender.append(
        actor="a", action="r", resource="X", success=True,
        args_digest="0"*64, result_digest="0"*64, processing_ms=1.0,
        timestamp_utc="2026-05-22T14:00:00+00:00",
    )
    await appender.append(
        actor="a", action="r", resource="X", success=True,
        args_digest="0"*64, result_digest="0"*64, processing_ms=1.0,
        timestamp_utc="2026-05-22T15:00:00+00:00",
    )
    await appender.append(
        actor="a", action="r", resource="X", success=True,
        args_digest="0"*64, result_digest="0"*64, processing_ms=1.0,
        timestamp_utc="2026-05-22T03:00:00+00:00",  # off-hours
    )
    await appender.append(
        actor="a", action="r", resource="X", success=False,
        error_message="denied",
        args_digest="0"*64, result_digest="", processing_ms=1.0,
        timestamp_utc="2026-05-22T16:00:00+00:00",
    )
    await appender.append(
        actor="a", action="r", resource="X", success=False,
        error_message="denied",
        args_digest="0"*64, result_digest="", processing_ms=1.0,
        timestamp_utc="2026-05-22T23:30:00+00:00",  # off-hours AND fail
    )
    bundle = await render_soc2_report(
        session_factory=factory,
        chain_verifier=chain_verifier,
        signer=signer,
        from_ts="2024-01-01T00:00:00+00:00",
        to_ts="2030-01-01T00:00:00+00:00",
    )
    assert bundle.framework == "soc2"
    assert bundle.extras["anomaly_indicators"]["failure_count"] == 2
    # 03:00 + 23:30 + genesis (00:00 on 2026-01-01) all fall in [22:00, 06:00)
    assert bundle.extras["anomaly_indicators"]["off_hours_count"] == 3
    assert bundle.extras["regulation_reference"].startswith("SOC 2")


@pytest.mark.asyncio
async def test_soc2_attestation_verifies(chain):
    factory, appender, chain_verifier, signer = chain
    await appender.append(
        actor="a", action="r", resource="X", success=True,
        args_digest="0"*64, result_digest="0"*64, processing_ms=1.0,
    )
    bundle = await render_soc2_report(
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
