from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from src.instrumentation.pipeline import LogPipeline
from src.settings import Settings

PipelineFactory = Callable[[Settings], LogPipeline]


@dataclass(slots=True)
class OptimizationMeta:
    name: str
    description: str
    factory: PipelineFactory


OPTIMIZATIONS: dict[str, OptimizationMeta] = {}


def register_optimization(name: str, description: str) -> Callable[[PipelineFactory], PipelineFactory]:
    def deco(factory: PipelineFactory) -> PipelineFactory:
        OPTIMIZATIONS[name] = OptimizationMeta(name=name, description=description, factory=factory)
        return factory
    return deco


def factory_for(name: str) -> PipelineFactory:
    if name not in OPTIMIZATIONS:
        raise KeyError(f"Unknown optimization: {name!r}")
    return OPTIMIZATIONS[name].factory


def list_optimizations() -> list[dict]:
    return [
        {"name": m.name, "description": m.description}
        for m in OPTIMIZATIONS.values()
    ]
