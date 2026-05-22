"""Pydantic request/response models for the HTTP API.

Kept separate from src/chain/schema.py so that the wire shape can drift
from the on-disk shape without forcing chain logic to recompute hashes.
For now they line up, but pinning them as separate types makes future
additions (e.g. a v2 endpoint with extra metadata) safe.
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class AppendRequest(BaseModel):
    """Body for POST /v1/audit/append.

    ``actor`` is optional — if absent and no X-User-ID header, the
    anonymous fallback kicks in. Digests are caller-supplied so external
    services can attest to args/result content without having to ship
    the originals.
    """
    model_config = ConfigDict(extra="forbid")

    actor: Optional[str] = None
    action: str = Field(min_length=1, max_length=64)
    resource: str = Field(min_length=1, max_length=512)
    success: bool = True
    args_digest: str = Field(min_length=64, max_length=64)
    result_digest: str = Field(min_length=0, max_length=64)
    processing_ms: Optional[float] = None
    error_message: Optional[str] = None


class RecordResponse(BaseModel):
    """Response shape for GET /v1/records and POST /v1/audit/append."""
    seq: int
    timestamp_utc: str
    actor: str
    action: str
    resource: str
    success: bool
    error_message: Optional[str] = None
    processing_ms: Optional[float] = None
    args_digest: str
    result_digest: str
    prev_hash: str
    self_hash: str
    signature: str


class RecordsList(BaseModel):
    """Wraps a page of records with the requested filters echoed back."""
    records: list[RecordResponse]
    count: int  # records returned (<= limit)
    limit: int
    offset: int
