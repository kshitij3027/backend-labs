"""Integration tests for the stats / multi-tenant surface (Commit 20).

Drives the three read-only endpoints added in C20 entirely over HTTP against the
shared ``client`` fixture (see ``tests/conftest.py``): an isolated per-test data
dir with the background migration loop pinned to an hour, so observed state is
deterministic and no migration fires underneath a test.

* ``GET /api/stats`` — the system-wide snapshot (storage / formats / performance
  / migrations / ingest / tenants / selection_optimality).
* ``GET /api/stats/{tenant}`` — one tenant's partitions, each as a
  ``PartitionDecision`` (current layout next to the selector's recommendation +
  reason + confidence) — this is Feature A's "explain every partition" view.
* ``GET /api/partitions?tenant=<t>`` — raw per-partition manifest records.

The forced-migration test reaches into ``client.app.state.migration_engine`` (the
live object graph published by the lifespan) and drives the async migration
primitive via ``asyncio.run(...)`` from a sync test, then asserts the change is
reflected in ``/api/stats`` *and* that the migrated partition's data is still
fully readable through ``/api/query`` (data preserved post-migration).
"""
from __future__ import annotations

import asyncio

from src.models import Format

# --------------------------------------------------------------------------- #
# Shared payloads / helpers.
# --------------------------------------------------------------------------- #
# partition_bucket_seconds default = 3600, so bucket = int(ts / 3600). Every ts
# in [3600, 3660) lands in the same bucket -> partition ``p_1`` (one partition).
_BUCKET_TS_BASE = 3600.0


def _acme_batch(n: int = 10) -> dict:
    """Build an ingest body of ``n`` rows for tenant 'acme' in one time bucket."""
    return {
        "tenant": "acme",
        "entries": [
            {
                "ts": _BUCKET_TS_BASE + i,  # all in bucket 1 -> partition p_1
                "fields": {"user": f"u{i}", "level": "INFO" if i % 2 else "ERROR"},
            }
            for i in range(n)
        ],
    }


def _ingest(client, body) -> dict:
    """POST a batch and return the parsed JSON, asserting a 200."""
    r = client.post("/api/ingest", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def _stats(client) -> dict:
    """GET the system-wide stats snapshot, asserting a 200."""
    r = client.get("/api/stats")
    assert r.status_code == 200, r.text
    return r.json()


def _tenant_stats(client, tenant: str) -> dict:
    """GET one tenant's stats, asserting a 200."""
    r = client.get(f"/api/stats/{tenant}")
    assert r.status_code == 200, r.text
    return r.json()


def _partitions(client, tenant: str) -> list[dict]:
    """GET the raw partition records for a tenant, asserting a 200."""
    r = client.get("/api/partitions", params={"tenant": tenant})
    assert r.status_code == 200, r.text
    return r.json()


# --------------------------------------------------------------------------- #
# 1. Empty system is safe — sane zeros, never a 404, never a raise.
# --------------------------------------------------------------------------- #
def test_stats_empty_system(client):
    """A fresh system reports zeros, no tenants, and optimality 1.0."""
    body = _stats(client)

    assert body["tenants"] == []
    assert body["selection_optimality"] == 1.0
    assert body["storage"]["total_bytes"] == 0

    # Distribution carries every canonical format key, all zero.
    dist = body["formats"]["distribution"]
    assert dist["row"] == 0
    assert dist["columnar"] == 0
    assert dist["hybrid"] == 0


def test_tenant_stats_unknown_tenant(client):
    """An unknown tenant yields a 200 with empty partitions and zeroed aggregates."""
    body = _tenant_stats(client, "ghost")

    assert body["tenant"] == "ghost"
    assert body["partitions"] == []
    assert body["format_distribution"] == {}
    assert body["tier_distribution"] == {}
    assert body["index_columns_total"] == 0
    assert body["storage_bytes"] == 0


def test_partitions_unknown_tenant(client):
    """Listing partitions for an unknown tenant returns an empty list (200)."""
    assert _partitions(client, "ghost") == []


# --------------------------------------------------------------------------- #
# 2. After ingest, stats reflect storage + distribution + tenants.
# --------------------------------------------------------------------------- #
def test_stats_reflect_ingest(client):
    """Ingesting a batch surfaces storage, a ROW partition, the tenant, and entries."""
    _ingest(client, _acme_batch(10))

    body = _stats(client)
    assert body["storage"]["total_bytes"] > 0
    # New partitions are written ROW-first.
    assert body["formats"]["distribution"]["row"] >= 1
    assert "acme" in body["tenants"]
    assert body["ingest"]["total_entries"] >= 10


def test_tenant_stats_explain_partitions(client):
    """Per-tenant stats list partitions, each with a recommendation + reason (Feature A)."""
    _ingest(client, _acme_batch(10))

    body = _tenant_stats(client, "acme")

    # At least one ROW partition rolled into the format distribution.
    assert body["format_distribution"].get("row", 0) >= 1
    assert body["storage_bytes"] > 0

    partitions = body["partitions"]
    assert partitions  # non-empty
    for part in partitions:
        # Every partition explains itself: a recommended format + a real reason.
        assert part["recommended_format"] in {"row", "columnar", "hybrid"}
        assert isinstance(part["reason"], str)
        assert part["reason"].strip()  # non-empty explanation
        assert 0.0 <= part["confidence"] <= 1.0


# --------------------------------------------------------------------------- #
# 3. A forced migration is reflected in stats; data survives the migration.
# --------------------------------------------------------------------------- #
def test_forced_migration_reflected_in_stats(client):
    """Forcing a partition to columnar shows in /api/stats and preserves its data."""
    _ingest(client, _acme_batch(10))

    # Grab the (single) partition id from the raw manifest view.
    parts = _partitions(client, "acme")
    assert parts, "expected at least one partition after ingest"
    pid = parts[0]["partition_id"]

    # Sanity: it starts as ROW, and a full-record query sees all 10 rows.
    before = _tenant_stats(client, "acme")
    before_by_pid = {p["partition_id"]: p for p in before["partitions"]}
    assert before_by_pid[pid]["format"] == "row"

    pre_query = client.post("/api/query", json={"tenant": "acme"})
    assert pre_query.status_code == 200, pre_query.text
    assert len(pre_query.json()["rows"]) == 10

    # Drive the async migration primitive directly off the live object graph.
    engine = client.app.state.migration_engine
    migrated = asyncio.run(engine.migrate_to("acme", pid, Format.COLUMNAR))
    assert migrated is True

    # System-wide stats now show a columnar partition and a completed migration.
    sys_stats = _stats(client)
    assert sys_stats["formats"]["distribution"]["columnar"] >= 1
    assert sys_stats["migrations"]["completed"] >= 1

    # Per-tenant view: that partition is now columnar, still explained.
    after = _tenant_stats(client, "acme")
    after_by_pid = {p["partition_id"]: p for p in after["partitions"]}
    assert pid in after_by_pid
    decision = after_by_pid[pid]
    assert decision["format"] == "columnar"
    assert decision["recommended_format"] in {"row", "columnar", "hybrid"}
    assert decision["reason"].strip()  # recommendation/reason still surfaced

    # Data preserved post-migration: the same 10 rows still come back via the API.
    post_query = client.post("/api/query", json={"tenant": "acme"})
    assert post_query.status_code == 200, post_query.text
    post_rows = post_query.json()["rows"]
    assert len(post_rows) == 10
    assert {r["user"] for r in post_rows} == {f"u{i}" for i in range(10)}


# --------------------------------------------------------------------------- #
# 4. selection_optimality is a fraction in [0.0, 1.0].
# --------------------------------------------------------------------------- #
def test_selection_optimality_is_fraction(client):
    """After ingest (no forced migration), optimality is a fraction in [0, 1]."""
    _ingest(client, _acme_batch(10))

    body = _stats(client)
    assert 0.0 <= body["selection_optimality"] <= 1.0
