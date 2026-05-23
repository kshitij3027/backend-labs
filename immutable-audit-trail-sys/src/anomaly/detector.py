"""AnomalyDetector — periodic scan + integrity-break hook.

Three pattern checks:
  1. Frequency spike: more than N reads from a single actor in the
     last WINDOW_SEC seconds (defaults: N=10, WINDOW_SEC=60).
  2. Off-hours access: events whose timestamp_utc hour is in
     [22:00, 06:00) UTC.
  3. Unknown actor: actor not in a known-set; only active if a known-set
     file is provided.

The detector runs as a background task started in lifespan; it scans
on a fixed interval (default 30s) and pushes findings into the global
AlertSink (deduplication left to the consumer for now — this is a
learning project, not prod alerting).
"""
from __future__ import annotations

import asyncio
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.anomaly.alerts import AlertSink, get_sink
from src.persistence.models import AuditRecord as AuditRecordORM

log = logging.getLogger(__name__)


_FREQ_SPIKE_THRESHOLD = 10
_FREQ_SPIKE_WINDOW_SEC = 60
_OFF_HOURS_START = 22  # 22:00 UTC
_OFF_HOURS_END = 6     # 06:00 UTC


def _is_off_hours_hour(iso_ts: str) -> bool:
    try:
        dt = datetime.fromisoformat(iso_ts)
    except ValueError:
        return False
    hour = dt.hour
    return hour >= _OFF_HOURS_START or hour < _OFF_HOURS_END


class AnomalyDetector:
    """Periodic scanner that pushes Alert objects into the AlertSink."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        sink: AlertSink,
        known_actors_path: Optional[Path] = None,
    ) -> None:
        self._sessions = session_factory
        self._sink = sink
        self._known_actors: set[str] = set()
        if known_actors_path is not None and known_actors_path.exists():
            self._known_actors = {
                line.strip() for line in known_actors_path.read_text().splitlines()
                if line.strip() and not line.startswith("#")
            }
        # Internal dedupe: track which (actor, off_hours_ts) we've already alerted
        # on so we don't re-fire the same alert each interval.
        self._seen_off_hours: set[tuple[str, str]] = set()
        self._seen_freq_actors: set[str] = set()

    async def evaluate(self) -> list:
        """Run all three checks once; return list of alerts emitted."""
        now = datetime.now(timezone.utc)
        window_start = (now - timedelta(seconds=_FREQ_SPIKE_WINDOW_SEC)).isoformat()
        emitted = []

        async with self._sessions() as session:
            # Pull recent activity (last window for spike check + small lookback
            # for off-hours and unknown-actor patterns).
            stmt = sa.select(AuditRecordORM).where(
                AuditRecordORM.timestamp_utc >= window_start
            )
            recent = (await session.execute(stmt)).scalars().all()

        # --- Frequency spike --------------------------------------------------
        per_actor = Counter(r.actor for r in recent)
        for actor, count in per_actor.items():
            if count > _FREQ_SPIKE_THRESHOLD and actor not in self._seen_freq_actors:
                emitted.append(self._sink.add(
                    type="frequency_spike",
                    severity="warning",
                    actor=actor,
                    message=f"{count} events from {actor} in last {_FREQ_SPIKE_WINDOW_SEC}s",
                ))
                self._seen_freq_actors.add(actor)

        # --- Off-hours --------------------------------------------------------
        for r in recent:
            key = (r.actor, r.timestamp_utc)
            if key in self._seen_off_hours:
                continue
            if _is_off_hours_hour(r.timestamp_utc):
                emitted.append(self._sink.add(
                    type="off_hours_access",
                    severity="info",
                    actor=r.actor,
                    resource=r.resource,
                    message=f"off-hours access at {r.timestamp_utc}",
                ))
                self._seen_off_hours.add(key)

        # --- Unknown actor (only if known set provided) ----------------------
        if self._known_actors:
            for r in recent:
                if r.actor == "system":
                    continue  # genesis row is intentionally "system"
                if r.actor not in self._known_actors:
                    emitted.append(self._sink.add(
                        type="unknown_actor",
                        severity="critical",
                        actor=r.actor,
                        resource=r.resource,
                        message=f"unknown actor {r.actor} accessed {r.resource}",
                    ))
        return emitted


async def run_detector_periodically(
    detector: AnomalyDetector,
    interval_sec: float = 30.0,
) -> None:
    """Background task: evaluate every interval_sec until cancelled."""
    try:
        while True:
            try:
                await detector.evaluate()
            except Exception as exc:  # noqa: BLE001
                log.warning("anomaly detector tick failed: %s", exc, exc_info=True)
            await asyncio.sleep(interval_sec)
    except asyncio.CancelledError:
        return


def emit_integrity_break_alert(*, first_break_seq: int, reason: str) -> None:
    """Synchronous hook for the verifier — fires a critical alert on tamper."""
    get_sink().add(
        type="integrity_break",
        severity="critical",
        message=f"chain integrity broken at seq={first_break_seq} ({reason})",
    )
