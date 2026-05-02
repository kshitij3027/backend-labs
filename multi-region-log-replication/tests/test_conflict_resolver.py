"""Unit tests for ``src.conflict_resolver``.

We exercise every branch of :func:`resolve`:

* causally-before, causally-after, identical (the three deterministic
  vector-clock branches);
* concurrent + LWW tiebreaker on each of the four tuple positions
  (``logical_ts``, ``created_at``, ``region``, ``log_id``).

For tests that need *concurrent* vector clocks we use::

    vc_a = {"us-east": 1, "europe": 0}
    vc_b = {"us-east": 0, "europe": 1}

— each clock is ahead of the other in exactly one region, so
``vector_clock_compare`` returns ``None``.
"""

from __future__ import annotations

from src.conflict_resolver import resolve
from src.models import LogEntry


# Helper to build LogEntry quickly with explicit fields. Pydantic
# defaults still fire for the bits we don't set.
def _entry(
    *,
    log_id: str,
    region: str,
    vc: dict[str, int],
    logical_ts: int,
    created_at: float,
    data: dict | None = None,
) -> LogEntry:
    return LogEntry(
        log_id=log_id,
        data=data or {"message": "x"},
        region=region,
        created_at=created_at,
        vector_clock=vc,
        logical_ts=logical_ts,
    )


# ---------------------------------------------------------------------
# Deterministic VC-only branches
# ---------------------------------------------------------------------


def test_existing_happens_before_incoming_returns_incoming():
    """vc(existing) < vc(incoming) → keep incoming (it's causally newer)."""
    existing = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 1, "europe": 0},
        logical_ts=1,
        created_at=100.0,
    )
    incoming = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 2, "europe": 0},
        logical_ts=2,
        created_at=101.0,
    )

    assert resolve(existing, incoming) is incoming


def test_incoming_happens_before_existing_returns_existing():
    """vc(incoming) < vc(existing) → keep existing (local copy is newer)."""
    existing = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 5, "europe": 1},
        logical_ts=5,
        created_at=200.0,
    )
    incoming = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 3, "europe": 0},
        logical_ts=3,
        created_at=190.0,
    )

    assert resolve(existing, incoming) is existing


def test_identical_vcs_returns_incoming():
    """Identical clocks → idempotent re-application; we pick incoming."""
    existing = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 2, "europe": 1},
        logical_ts=2,
        created_at=300.0,
    )
    incoming = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 2, "europe": 1},
        logical_ts=2,
        created_at=300.0,
    )

    assert resolve(existing, incoming) is incoming


# ---------------------------------------------------------------------
# Concurrent → LWW tiebreaker on (logical_ts, created_at, region, log_id)
# ---------------------------------------------------------------------


def test_concurrent_lww_higher_logical_ts_wins():
    """Concurrent clocks; existing.logical_ts=2 > incoming.logical_ts=1 → existing wins."""
    existing = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 1, "europe": 0},
        logical_ts=2,
        created_at=100.0,
    )
    incoming = _entry(
        log_id="L1",
        region="europe",
        vc={"us-east": 0, "europe": 1},
        logical_ts=1,
        created_at=200.0,  # later created_at, but lower logical_ts → loses
    )

    assert resolve(existing, incoming) is existing


def test_concurrent_lww_tie_breaks_on_created_at():
    """Same logical_ts; later created_at wins."""
    existing = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 1, "europe": 0},
        logical_ts=1,
        created_at=100.0,
    )
    incoming = _entry(
        log_id="L1",
        region="europe",
        vc={"us-east": 0, "europe": 1},
        logical_ts=1,
        created_at=200.0,  # later
    )

    assert resolve(existing, incoming) is incoming


def test_concurrent_lww_tie_breaks_on_region():
    """Same logical_ts and created_at; lex-larger region wins (us-east > europe)."""
    existing = _entry(
        log_id="L1",
        region="europe",
        vc={"us-east": 0, "europe": 1},
        logical_ts=1,
        created_at=100.0,
    )
    incoming = _entry(
        log_id="L1",
        region="us-east",
        vc={"us-east": 1, "europe": 0},
        logical_ts=1,
        created_at=100.0,
    )

    # "us-east" > "europe" lexicographically → incoming wins.
    assert "us-east" > "europe"  # sanity-check the assumption itself
    assert resolve(existing, incoming) is incoming


def test_concurrent_lww_tie_breaks_on_log_id():
    """Same logical_ts, created_at, and region; lex-larger log_id wins."""
    existing = _entry(
        log_id="aaaa",
        region="us-east",
        vc={"us-east": 1, "europe": 0},
        logical_ts=1,
        created_at=100.0,
    )
    incoming = _entry(
        log_id="zzzz",
        region="us-east",
        vc={"us-east": 0, "europe": 1},
        logical_ts=1,
        created_at=100.0,
    )

    # NB: same log_id is the realistic case for a conflict resolver call,
    # but the resolver itself doesn't enforce that — and using distinct
    # log_ids is the only way to hit this branch in isolation. The
    # vector clocks are still concurrent, so we exercise the tiebreaker.
    assert resolve(existing, incoming) is incoming


def test_resolution_is_deterministic():
    """Calling the resolver twice with freshly-rebuilt inputs yields the same answer."""

    def build_pair() -> tuple[LogEntry, LogEntry]:
        e = _entry(
            log_id="L1",
            region="europe",
            vc={"us-east": 0, "europe": 1},
            logical_ts=1,
            created_at=100.0,
        )
        i = _entry(
            log_id="L1",
            region="us-east",
            vc={"us-east": 1, "europe": 0},
            logical_ts=1,
            created_at=100.0,
        )
        return e, i

    e1, i1 = build_pair()
    e2, i2 = build_pair()
    winner_a = resolve(e1, i1)
    winner_b = resolve(e2, i2)

    # Same fields → same winner. Compare by region (since the entry
    # objects are different instances after rebuild).
    assert winner_a.region == winner_b.region
    assert winner_a.logical_ts == winner_b.logical_ts
    assert winner_a.created_at == winner_b.created_at
    assert winner_a.log_id == winner_b.log_id
