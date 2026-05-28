from __future__ import annotations

import asyncio
import json

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


class FSMParser:
    """Table-driven FSM tailored to the synthetic-log JSON shape:
    {"ts": <number>, "level": "<word>", "msg": "<string>", "field_N": <int>}.

    Falls back to json.loads() on anything unexpected so it never throws.
    """

    __slots__ = ()

    def parse(self, line: str) -> dict:
        # Fast path: assume well-formed top-level dict. Defer to json on first
        # surprise so we stay correct for off-shape lines.
        if not line or line[0] != "{":
            return json.loads(line)
        try:
            return self._parse_object(line, 0)[0]
        except (ValueError, IndexError):
            return json.loads(line)

    # --- helpers ---

    def _skip_ws(self, s: str, i: int) -> int:
        while i < len(s) and s[i] in " \t\n\r":
            i += 1
        return i

    def _parse_object(self, s: str, i: int) -> tuple[dict, int]:
        if s[i] != "{":
            raise ValueError("expected object")
        i += 1
        out: dict = {}
        i = self._skip_ws(s, i)
        if i < len(s) and s[i] == "}":
            return out, i + 1
        while True:
            i = self._skip_ws(s, i)
            key, i = self._parse_string(s, i)
            i = self._skip_ws(s, i)
            if s[i] != ":":
                raise ValueError("expected colon")
            i = self._skip_ws(s, i + 1)
            value, i = self._parse_value(s, i)
            out[key] = value
            i = self._skip_ws(s, i)
            if i < len(s) and s[i] == ",":
                i += 1
                continue
            if i < len(s) and s[i] == "}":
                return out, i + 1
            raise ValueError("expected , or }")

    def _parse_string(self, s: str, i: int) -> tuple[str, int]:
        if s[i] != '"':
            raise ValueError("expected string")
        i += 1
        start = i
        while i < len(s) and s[i] != '"':
            if s[i] == "\\":
                # Defer to json for escape sequences — uncommon in synthetic logs.
                raise ValueError("escape — defer")
            i += 1
        return s[start:i], i + 1

    def _parse_value(self, s: str, i: int) -> tuple[object, int]:
        c = s[i]
        if c == '"':
            return self._parse_string(s, i)
        if c == "{":
            return self._parse_object(s, i)
        if c == "-" or c.isdigit():
            return self._parse_number(s, i)
        if s.startswith("true", i):
            return True, i + 4
        if s.startswith("false", i):
            return False, i + 5
        if s.startswith("null", i):
            return None, i + 4
        raise ValueError(f"unexpected char {c!r}")

    def _parse_number(self, s: str, i: int) -> tuple[float | int, int]:
        start = i
        if s[i] == "-":
            i += 1
        while i < len(s) and (s[i].isdigit() or s[i] in ".eE+-"):
            i += 1
        text = s[start:i]
        if any(c in text for c in ".eE"):
            return float(text), i
        return int(text), i


class FSMParseStage(ParseStage):
    """Parse stage using FSMParser instead of json.loads."""

    def __init__(self, inbound: asyncio.Queue, outbound: asyncio.Queue | None) -> None:
        super().__init__(inbound=inbound, outbound=outbound)
        self._fsm = FSMParser()

    @profile_stage("parse")
    async def process(self, record: dict) -> dict | None:
        line = record.get("line", "")
        if not line:
            return None
        try:
            parsed = self._fsm.parse(line)
            if not isinstance(parsed, dict):
                return {"raw": line, "level": "info"}
            return parsed
        except (ValueError, TypeError):
            return {"raw": line, "level": "info"}


@register_optimization(
    "fsm_parser",
    "FSM-based parser for the parse stage — replaces json.loads on the hot path",
)
def build_fsm_parser_pipeline(settings: Settings) -> LogPipeline:
    qmax = settings.queue_maxsize
    q1: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q2: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q3: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    q4: asyncio.Queue = asyncio.Queue(maxsize=qmax)
    parse = FSMParseStage(inbound=q1, outbound=q2)
    validate = ValidateStage(inbound=q2, outbound=q3)
    transform = TransformStage(inbound=q3, outbound=q4)
    write = WriteStage(inbound=q4)
    return LogPipeline([parse, validate, transform, write])
