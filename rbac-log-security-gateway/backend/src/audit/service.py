"""AuditService: append-only in-memory store + simple queries.

Per the requirements doc: in-memory append-only (no eviction). All admin-endpoint
queries read this same instance via the shared singleton.
"""
from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Iterable, List, Optional

from src.audit.models import AuditEntry, SecurityEvent


class AuditService:
    def __init__(self, *, max_entries: int = 0) -> None:
        """`max_entries=0` means unbounded. Tests can pass a small bound."""
        self._entries: List[AuditEntry] = []
        self._security: List[SecurityEvent] = []
        self._lock = Lock()
        self._max_entries = max_entries

    # ----- writes ---------------------------------------------------------- #
    def append(self, entry: AuditEntry) -> None:
        with self._lock:
            self._entries.append(entry)
            if self._max_entries and len(self._entries) > self._max_entries:
                # Drop oldest. Only triggers in tests where max_entries is set.
                self._entries = self._entries[-self._max_entries :]

    def append_security_event(self, event: SecurityEvent) -> None:
        with self._lock:
            self._security.append(event)

    # ----- reads ---------------------------------------------------------- #
    def query(
        self,
        *,
        limit: int = 100,
        since: Optional[datetime] = None,
        username: Optional[str] = None,
    ) -> List[AuditEntry]:
        with self._lock:
            entries: Iterable[AuditEntry] = reversed(self._entries)
            if since is not None:
                entries = (e for e in entries if e.timestamp >= since)
            if username is not None:
                entries = (e for e in entries if e.username == username)
            return list(entries)[:limit]

    def security_events(self, *, limit: int = 100) -> List[SecurityEvent]:
        with self._lock:
            return list(reversed(self._security))[:limit]

    def summary(self) -> dict:
        with self._lock:
            total = len(self._entries)
            by_status: dict[int, int] = {}
            by_user: dict[str, int] = {}
            allows = 0
            denies = 0
            for e in self._entries:
                by_status[e.status] = by_status.get(e.status, 0) + 1
                if e.username:
                    by_user[e.username] = by_user.get(e.username, 0) + 1
                if e.decision == "allow":
                    allows += 1
                elif e.decision == "deny":
                    denies += 1
            return {
                "total_entries": total,
                "by_status": by_status,
                "by_user": by_user,
                "allow_decisions": allows,
                "deny_decisions": denies,
                "security_events": len(self._security),
            }

    def clear(self) -> None:
        """For tests only."""
        with self._lock:
            self._entries.clear()
            self._security.clear()

    def __len__(self) -> int:
        return len(self._entries)
