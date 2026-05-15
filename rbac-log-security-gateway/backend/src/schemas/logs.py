"""Pydantic schemas for /api/logs endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class LogRecordOut(BaseModel):
    id: str
    resource: str
    timestamp: datetime
    level: str
    message: str
    fields: Dict[str, str] = Field(default_factory=dict)


class LogSearchResponse(BaseModel):
    resource: str
    count: int
    records: Optional[List[LogRecordOut]] = None       # populated for unaggregated reads
    aggregated: Optional[Dict[str, Any]] = None        # populated when aggregated_only tag fired
    masked: bool = False                               # True when mask_pii tag fired
    rbac_rule: Optional[str] = None                    # the matched permission for transparency
