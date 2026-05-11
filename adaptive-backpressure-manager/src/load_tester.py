import asyncio
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from src.admission import Admission, AdmissionVerdict
from src.config import Settings
from src.logging_setup import get_logger
from src.queues import PriorityQueues
from src.state import Priority
from src.state_machine import BackpressureManager


class LoadPhase(str, Enum):
    IDLE = "idle"
    SMOKE = "smoke"
    RAMP = "ramp"
    SPIKE = "spike"
    SOAK = "soak"
    RECOVERY = "recovery"


@dataclass
class LoadTesterStatus:
    state: str
    profile: str
    current_phase: str
    elapsed_s: float
    emitted: int
    accepted: int
    throttled: int
    dropped: int
    rejected: int


_PRIORITY_WEIGHTS = [
    (Priority.CRITICAL, 0.05),
    (Priority.HIGH, 0.15),
    (Priority.NORMAL, 0.60),
    (Priority.LOW, 0.20),
]


def _pick_priority(rng: random.Random) -> Priority:
    r = rng.random()
    cum = 0.0
    for p, w in _PRIORITY_WEIGHTS:
        cum += w
        if r < cum:
            return p
    return Priority.NORMAL


class InternalLoadTester:
    """In-process 5-phase load generator. Drives the admission pipeline directly."""

    def __init__(
        self,
        admission: Admission,
        queues: PriorityQueues,
        manager: BackpressureManager,
        settings: Settings,
        rng_seed: Optional[int] = None,
    ) -> None:
        self._admission = admission
        self._queues = queues
        self._manager = manager
        self._settings = settings
        self._task: Optional[asyncio.Task] = None
        self._rng = random.Random(rng_seed)
        self._log = get_logger("load_tester")
        self._reset_counters()
        self._current_phase: LoadPhase = LoadPhase.IDLE
        self._started_at: float = 0.0
        self._profile_name: str = "idle"
        self._stop = asyncio.Event()

    def _reset_counters(self) -> None:
        self._emitted = 0
        self._accepted = 0
        self._throttled = 0
        self._dropped = 0
        self._rejected = 0

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def status(self) -> LoadTesterStatus:
        elapsed = (time.monotonic() - self._started_at) if self.is_running else 0.0
        return LoadTesterStatus(
            state="running" if self.is_running else "idle",
            profile=self._profile_name if self.is_running else "idle",
            current_phase=self._current_phase.value,
            elapsed_s=elapsed,
            emitted=self._emitted,
            accepted=self._accepted,
            throttled=self._throttled,
            dropped=self._dropped,
            rejected=self._rejected,
        )

    async def start(
        self,
        profile: str = "full",
        rps: int = 200,
        duration_seconds: int = 60,
        spike_multiplier: float = 10.0,
    ) -> LoadTesterStatus:
        if self.is_running:
            await self.stop()
        self._reset_counters()
        self._profile_name = profile
        self._started_at = time.monotonic()
        self._stop.clear()
        self._task = asyncio.create_task(
            self._run(profile, rps, duration_seconds, spike_multiplier),
            name="load-tester",
        )
        return self.status()

    async def stop(self) -> LoadTesterStatus:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except asyncio.TimeoutError:
                self._task.cancel()
            except asyncio.CancelledError:
                pass
            self._task = None
        self._current_phase = LoadPhase.IDLE
        return self.status()

    async def _run(self, profile: str, baseline_rps: int, duration_seconds: int, spike_multiplier: float) -> None:
        try:
            schedule = self._build_schedule(profile, baseline_rps, duration_seconds, spike_multiplier)
            for phase, phase_duration, target_rps in schedule:
                if self._stop.is_set():
                    break
                self._current_phase = phase
                await self._drive_phase(phase, phase_duration, target_rps, baseline_rps, spike_multiplier)
        finally:
            self._current_phase = LoadPhase.IDLE

    def _build_schedule(self, profile: str, baseline_rps: int, duration_seconds: int, spike_multiplier: float):
        if profile == "full":
            return [
                (LoadPhase.SMOKE, 15, int(baseline_rps * 0.1)),
                (LoadPhase.RAMP, 15, baseline_rps),
                (LoadPhase.SPIKE, 10, int(baseline_rps * spike_multiplier)),
                (LoadPhase.SOAK, 15, baseline_rps),
                (LoadPhase.RECOVERY, 5, 0),
            ]
        if profile == "smoke":
            return [(LoadPhase.SMOKE, duration_seconds, max(1, int(baseline_rps * 0.1)))]
        if profile == "ramp":
            return [(LoadPhase.RAMP, duration_seconds, baseline_rps)]
        if profile == "spike":
            return [(LoadPhase.SPIKE, duration_seconds, int(baseline_rps * spike_multiplier))]
        if profile == "soak":
            return [(LoadPhase.SOAK, duration_seconds, baseline_rps)]
        if profile == "recovery":
            return [(LoadPhase.RECOVERY, duration_seconds, 0)]
        return [(LoadPhase.SOAK, duration_seconds, baseline_rps)]

    async def _drive_phase(
        self,
        phase: LoadPhase,
        phase_duration: int,
        target_rps: int,
        baseline_rps: int,
        spike_multiplier: float,
    ) -> None:
        if phase_duration <= 0:
            return
        phase_start = time.monotonic()
        phase_end = phase_start + phase_duration
        while not self._stop.is_set():
            now = time.monotonic()
            if now >= phase_end:
                break
            if phase == LoadPhase.RAMP:
                progress = (now - phase_start) / phase_duration
                rps_now = max(1, int(baseline_rps * (0.1 + 0.9 * progress)))
            else:
                rps_now = max(0, target_rps)
            if rps_now <= 0:
                await asyncio.sleep(min(0.1, phase_end - now))
                continue
            interval = 1.0 / rps_now
            self._emit_one()
            await asyncio.sleep(interval)

    def _emit_one(self) -> None:
        priority = _pick_priority(self._rng)
        verdict = self._admission.decide(priority, self._manager.level)
        self._emitted += 1
        if verdict == AdmissionVerdict.ACCEPT:
            try:
                self._queues.put_nowait(priority, "lt_msg", time.monotonic())
                self._accepted += 1
            except Exception:
                self._dropped += 1
        elif verdict == AdmissionVerdict.THROTTLE_429:
            self._throttled += 1
        elif verdict == AdmissionVerdict.DROP_SILENT:
            self._dropped += 1
        else:
            self._rejected += 1
