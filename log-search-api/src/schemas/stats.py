from datetime import datetime

from pydantic import BaseModel, ConfigDict


class CacheStats(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    hits: int
    misses: int
    errors: int
    hit_rate: float


class IndexStats(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    index: str
    doc_count: int
    size_in_bytes: int


class StatsResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    cache: CacheStats
    index: IndexStats
    timestamp: datetime
