from __future__ import annotations

import asyncio
import mmap
from pathlib import Path
from typing import Iterator

from src.instrumentation.pipeline import (
    LogPipeline,
    ParseStage,
    TransformStage,
    ValidateStage,
    WriteStage,
)
from src.optimizations.registry import register_optimization
from src.settings import Settings


class MmapLogReader:
    """mmap-backed line iterator for bulk-reading large log files without
    per-line read() syscalls.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    def __iter__(self) -> Iterator[dict]:
        with open(self._path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                while True:
                    line = mm.readline()
                    if not line:
                        return
                    yield {"line": line.decode("utf-8", errors="replace").rstrip("\n")}


@register_optimization(
    "mmap_reader",
    "Use mmap for the input source - avoid per-line read() syscalls on large files",
)
def build_mmap_reader_pipeline(settings: Settings) -> LogPipeline:
    """The mmap optimization operates at the load-runner level (input source);
    the pipeline shape is the default - MmapLogReader can be passed as the
    record iterable to pipeline.feed()."""
    qmax = settings.queue_maxsize
    q1: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q2: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q3: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q4: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    parse = ParseStage(inbound=q1, outbound=q2)
    validate = ValidateStage(inbound=q2, outbound=q3)
    transform = TransformStage(inbound=q3, outbound=q4)
    write = WriteStage(inbound=q4)
    return LogPipeline([parse, validate, transform, write])
