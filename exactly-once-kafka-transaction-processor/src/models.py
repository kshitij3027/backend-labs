"""Pydantic models for Kafka transaction messages."""

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class TransactionType(str, Enum):
    """Types of financial transactions."""

    TRANSFER = "TRANSFER"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    PAYMENT = "PAYMENT"


class TransactionMessage(BaseModel):
    """A financial transaction message for Kafka transport."""

    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: TransactionType
    from_account: Optional[str] = None
    to_account: Optional[str] = None
    amount: Decimal
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    metadata: dict = Field(default_factory=dict)

    def to_kafka_value(self) -> bytes:
        """Serialize to JSON bytes for Kafka, handling Decimal and enum types."""
        data = self.model_dump()
        data["amount"] = str(data["amount"])
        data["type"] = data["type"].value if isinstance(data["type"], Enum) else data["type"]
        return json.dumps(data).encode("utf-8")

    @classmethod
    def from_kafka_value(cls, data: bytes) -> "TransactionMessage":
        """Deserialize from Kafka JSON bytes."""
        parsed = json.loads(data.decode("utf-8"))
        parsed["amount"] = Decimal(parsed["amount"])
        return cls(**parsed)
