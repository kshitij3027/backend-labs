"""Pydantic request/response schemas for the API."""
from __future__ import annotations

import datetime as dt
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
    model_config = ConfigDict(from_attributes=True)

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
