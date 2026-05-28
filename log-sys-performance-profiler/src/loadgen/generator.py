from __future__ import annotations

import json
import random
import string
import time
from dataclasses import dataclass, field
from typing import Iterator


@dataclass(slots=True)
class WorkloadSpec:
    count: int = 1000
    level_distribution: dict[str, float] = field(
        default_factory=lambda: {"info": 0.7, "warn": 0.2, "error": 0.1}
    )
    message_length: int = 80
    extra_field_count: int = 4


class SyntheticLogGenerator:
    """Deterministic generator: same seed -> identical record sequence."""

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self._seed = seed

    @property
    def seed(self) -> int:
        return self._seed

    def _pick_level(self, dist: dict[str, float]) -> str:
        r = self._rng.random()
        cumulative = 0.0
        for level, prob in dist.items():
            cumulative += prob
            if r <= cumulative:
                return level
        return next(iter(dist))

    def _random_msg(self, length: int) -> str:
        return "".join(
            self._rng.choice(string.ascii_letters + string.digits + " ")
            for _ in range(length)
        )

    def generate(self, spec: WorkloadSpec) -> Iterator[dict]:
        for _ in range(spec.count):
            payload = {
                "ts": time.time(),
                "level": self._pick_level(spec.level_distribution),
                "msg": self._random_msg(spec.message_length),
            }
            for k in range(spec.extra_field_count):
                payload[f"field_{k}"] = self._rng.randint(0, 10_000)
            yield {"line": json.dumps(payload)}
