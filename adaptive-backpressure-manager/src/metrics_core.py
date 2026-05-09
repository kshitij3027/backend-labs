"""EWMA-smoothed pressure score fusion across queue, lag, CPU, and memory dimensions."""

from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Tuple


@dataclass
class PressureMetrics:
    queue_depth_ratio: float
    normalized_lag: float
    cpu: float
    mem: float
    ts: float


class PressureFuser:
    """
    Fuses per-dimension pressure into a single EWMA-smoothed score.
    raw = max(0.5*qdr + 0.3*lag + 0.2*max(cpu, mem),
              max(qdr, lag, cpu, mem))
    score = alpha*raw + (1 - alpha)*prev_score
    """

    def __init__(self, alpha: float = 0.3, history_size: int = 100) -> None:
        self._alpha = alpha
        self._prev: float = 0.0
        self._history: Deque[Tuple[float, float]] = deque(maxlen=history_size)

    @property
    def alpha(self) -> float:
        return self._alpha

    @alpha.setter
    def alpha(self, value: float) -> None:
        self._alpha = value

    @property
    def last_score(self) -> float:
        return self._prev

    def history(self) -> list[Tuple[float, float]]:
        return list(self._history)

    def fuse(self, m: PressureMetrics) -> float:
        weighted = 0.5 * m.queue_depth_ratio + 0.3 * m.normalized_lag + 0.2 * max(m.cpu, m.mem)
        peak = max(m.queue_depth_ratio, m.normalized_lag, m.cpu, m.mem)
        raw = max(weighted, peak)
        if raw < 0.0:
            raw = 0.0
        elif raw > 1.0:
            raw = 1.0
        score = self._alpha * raw + (1.0 - self._alpha) * self._prev
        self._prev = score
        self._history.append((m.ts, score))
        return score
