from enum import Enum
from typing import Optional

from src.aimd import AIMDLimiter
from src.config import Settings
from src.logging_setup import TAG_ADMIT, TAG_DROP, TAG_THROTTLE, get_logger
from src.state import PressureLevel, Priority
from src.upstream_breaker import UpstreamBreaker


class AdmissionVerdict(str, Enum):
    ACCEPT = "accept"
    THROTTLE_429 = "throttle_429"
    DROP_SILENT = "drop_silent"
    REJECT_503 = "reject_503"


class Admission:
    """Per-priority admission decisions with hysteresis-aware drop matrix."""

    def __init__(
        self,
        settings: Settings,
        aimd: AIMDLimiter,
        upstream_breaker: UpstreamBreaker,
    ) -> None:
        self._settings = settings
        self._aimd = aimd
        self._upstream = upstream_breaker
        self._log = get_logger("admission")
        self._counters = {
            "accepted": 0,
            "throttled": 0,
            "dropped": 0,
            "rejected": 0,
        }

    @property
    def counters(self) -> dict:
        return dict(self._counters)

    def decide(self, priority: Priority, level: PressureLevel) -> AdmissionVerdict:
        # Upstream breaker forces 503 on non-CRITICAL while tripped.
        if self._upstream.is_open and priority != Priority.CRITICAL:
            return self._record(AdmissionVerdict.REJECT_503, priority, level, reason="upstream_open")

        verdict = self._matrix(priority, level)
        return self._record(verdict, priority, level, reason="matrix")

    def _matrix(self, priority: Priority, level: PressureLevel) -> AdmissionVerdict:
        if level == PressureLevel.NORMAL:
            return self._maybe_aimd(priority, AdmissionVerdict.ACCEPT)

        if level == PressureLevel.PRESSURE:
            if priority == Priority.LOW:
                return AdmissionVerdict.DROP_SILENT
            if priority == Priority.NORMAL:
                return self._maybe_aimd(priority, AdmissionVerdict.ACCEPT, throttle_on_full=True)
            return AdmissionVerdict.ACCEPT  # HIGH, CRITICAL unconditionally accepted in PRESSURE

        if level == PressureLevel.OVERLOAD:
            if priority == Priority.CRITICAL:
                return AdmissionVerdict.ACCEPT
            if priority == Priority.HIGH:
                return AdmissionVerdict.REJECT_503
            return AdmissionVerdict.DROP_SILENT  # NORMAL, LOW

        # RECOVERY: same as PRESSURE except NORMAL/HIGH may throttle (429) under AIMD pressure.
        if priority == Priority.CRITICAL:
            return AdmissionVerdict.ACCEPT
        if priority == Priority.LOW:
            return AdmissionVerdict.DROP_SILENT
        return self._maybe_aimd(priority, AdmissionVerdict.ACCEPT, throttle_on_full=True)

    def _maybe_aimd(
        self,
        priority: Priority,
        base: AdmissionVerdict,
        throttle_on_full: bool = False,
    ) -> AdmissionVerdict:
        """Run an AIMD acquire; if AIMD is exhausted and throttle_on_full, return 429."""
        if priority == Priority.CRITICAL:
            return base
        if self._aimd.try_acquire():
            return base
        if throttle_on_full:
            return AdmissionVerdict.THROTTLE_429
        return base

    def _record(
        self,
        verdict: AdmissionVerdict,
        priority: Priority,
        level: PressureLevel,
        reason: str,
    ) -> AdmissionVerdict:
        if verdict == AdmissionVerdict.ACCEPT:
            self._counters["accepted"] += 1
            self._log.info(
                "admit",
                tag=TAG_ADMIT,
                priority=priority.value,
                level=level.value,
            )
        elif verdict == AdmissionVerdict.THROTTLE_429:
            self._counters["throttled"] += 1
            self._log.info(
                "throttle",
                tag=TAG_THROTTLE,
                priority=priority.value,
                level=level.value,
                reason=reason,
            )
        elif verdict == AdmissionVerdict.DROP_SILENT:
            self._counters["dropped"] += 1
            self._log.info(
                "drop",
                tag=TAG_DROP,
                priority=priority.value,
                level=level.value,
                reason=reason,
            )
        else:  # REJECT_503
            self._counters["rejected"] += 1
            self._log.info(
                "reject",
                tag=TAG_DROP,
                priority=priority.value,
                level=level.value,
                reason=reason,
            )
        return verdict
