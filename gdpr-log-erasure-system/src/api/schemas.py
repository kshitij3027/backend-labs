"""Pydantic request/response schemas for the API."""
from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


# ── Tracking ─────────────────────────────────────────────────────────────


class UserDataMappingCreate(BaseModel):
    user_id: str = Field(min_length=1, max_length=255)
    data_type: str = Field(min_length=1, max_length=100)
    storage_location: str = Field(min_length=1, max_length=255)
    data_path: Optional[str] = Field(default=None, max_length=1024)
    metadata: Optional[dict[str, Any]] = None


class UserDataMappingResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: int
    user_id: str
    data_type: str
    storage_location: str
    data_path: Optional[str] = None
    metadata: Optional[dict[str, Any]] = Field(
        default=None,
        validation_alias="metadata_json",
        serialization_alias="metadata",
    )
    created_at: dt.datetime


# ── Erasure requests ────────────────────────────────────────────────────────


class ErasureRequestTypeIn(str, Enum):
    DELETE = "DELETE"
    ANONYMIZE = "ANONYMIZE"


class ErasureRequestCreate(BaseModel):
    user_id: str = Field(min_length=1, max_length=255)
    request_type: ErasureRequestTypeIn


class AuditEntryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    sequence: int
    event_type: str
    payload: dict[str, Any] = Field(validation_alias="payload_json", serialization_alias="payload")
    prev_hash: str
    entry_hash: str
    created_at: dt.datetime


class ErasureRequestResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    request_type: str
    state: str
    error_message: Optional[str] = None
    created_at: dt.datetime
    started_at: Optional[dt.datetime] = None
    completed_at: Optional[dt.datetime] = None
    audit_entries: list[AuditEntryResponse] = Field(default_factory=list)
