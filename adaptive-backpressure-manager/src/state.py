from dataclasses import dataclass
from enum import Enum


class PressureLevel(str, Enum):
    NORMAL = "normal"
    PRESSURE = "pressure"
    OVERLOAD = "overload"
    RECOVERY = "recovery"


class Priority(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


@dataclass
class PressureState:
    level: PressureLevel
    score: float
    entered_at: float
