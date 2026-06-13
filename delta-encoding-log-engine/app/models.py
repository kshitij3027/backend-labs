"""API and core data models for the delta-encoding log engine.

A **log entry is a plain JSON-serializable ``dict[str, Any]``**, *not* a Pydantic
model. The codec diffs these dicts field-by-field, and the round-trip fidelity
contract is canonical JSON of the dicts — so every value must stay JSON-native
(``int`` / ``str`` / ``bool``; timestamps are integer epoch-ms). Wrapping entries
in a fixed Pydantic schema would fight the whole point of delta encoding, which is
that the schema *varies* (fields appear and disappear) from one entry to the next.

The Pydantic models below cover only the synthetic-generator API surface. The
compress / reconstruct / stats request and response models are introduced in a
later commit, alongside the encoder, and intentionally do not live here yet.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

# A single structured log record. Deliberately an open dict: the set of keys
# differs between entries (e.g. an ``error`` field that only exists on ERROR
# lines), which is exactly the structure delta encoding exploits.
LogEntry = dict[str, Any]


class GenerateRequest(BaseModel):
    """Parameters for ``POST /api/generate`` (synthetic structured logs).

    Each ``None`` knob means "fall back to the configured default" from
    :class:`app.settings.Settings`, rather than a literal absence of churn or
    width — the route layer resolves the defaults before generation.
    """

    count: int = Field(50, ge=1, le=100000)
    """How many entries to generate (bounded to keep payloads sane)."""

    seed: int | None = None
    """RNG seed for reproducible batches; ``None`` is a non-deterministic run."""

    churn: float | None = Field(None, ge=0.0, le=1.0)
    """Fraction of non-timestamp fields that change per entry; ``None`` → default."""

    schema_width: int | None = Field(None, ge=1, le=40)
    """Base field count per entry; ``None`` → default. Clamped to the catalogue."""


class GenerateResponse(BaseModel):
    """Result of ``POST /api/generate``: the generated entries and their count."""

    logs: list[LogEntry]
    """The generated structured log entries, in chronological order."""

    count: int
    """Number of entries returned (mirrors ``len(logs)`` for convenience)."""
