from __future__ import annotations

import asyncio
from typing import FrozenSet

from src.instrumentation.decorator import profile_stage
from src.instrumentation.pipeline import (
    LogPipeline,
    ParseStage,
    TransformStage,
    ValidateStage,
    WriteStage,
)
from src.optimizations.registry import register_optimization
from src.settings import Settings


class PrecompiledValidator:
    """Compiles the required-key set into a frozenset at init time so per-record
    validation is a single hash lookup."""

    __slots__ = ("_required",)

    def __init__(self, required: FrozenSet[str] = frozenset({"ts", "level", "msg"})) -> None:
        self._required = required

    def validate(self, record: dict) -> bool:
        return self._required.issubset(record.keys())


class PrecompiledValidateStage(ValidateStage):
    """Validate stage using a precompiled validator."""

    def __init__(self, inbound: asyncio.Queue, outbound: asyncio.Queue | None) -> None:
        super().__init__(inbound=inbound, outbound=outbound)
        self._validator = PrecompiledValidator()

    @profile_stage("validate")
    async def process(self, record: dict) -> dict | None:
        return record if self._validator.validate(record) else None


@register_optimization(
    "precompiled_validator",
    "Precompiled validator for the validate stage — single hash lookup per record",
)
def build_precompiled_validator_pipeline(settings: Settings) -> LogPipeline:
    qmax = settings.queue_maxsize
    q1: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q2: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q3: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q4: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    parse = ParseStage(inbound=q1, outbound=q2)
    validate = PrecompiledValidateStage(inbound=q2, outbound=q3)
    transform = TransformStage(inbound=q3, outbound=q4)
    write = WriteStage(inbound=q4)
    return LogPipeline([parse, validate, transform, write])
