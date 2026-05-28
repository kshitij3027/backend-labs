from __future__ import annotations

from fastapi import Request

from src.benchmark.harness import BeforeAfterHarness
from src.loadgen.runner import LoadRunner
from src.metrics.collector import MetricsCollector
from src.metrics.ring_buffer import RingBuffer
from src.settings import Settings
from src.store.run_store import RunStore


def get_settings_dep(request: Request) -> Settings:
    return request.app.state.settings


def get_buffer(request: Request) -> RingBuffer:
    return request.app.state.buffer


def get_collector(request: Request) -> MetricsCollector:
    return request.app.state.collector


def get_runner(request: Request) -> LoadRunner:
    return request.app.state.runner


def get_store(request: Request) -> RunStore:
    return request.app.state.run_store


def get_harness(request: Request) -> BeforeAfterHarness:
    return request.app.state.harness
