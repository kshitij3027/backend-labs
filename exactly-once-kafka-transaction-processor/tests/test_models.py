"""Tests for transaction message models."""

import json
from decimal import Decimal

import pytest

from src.models import TransactionMessage, TransactionType


class TestTransactionType:
    """Tests for the TransactionType enum."""

    def test_all_enum_values_exist(self):
        assert TransactionType.TRANSFER.value == "TRANSFER"
        assert TransactionType.DEPOSIT.value == "DEPOSIT"
        assert TransactionType.WITHDRAWAL.value == "WITHDRAWAL"
        assert TransactionType.PAYMENT.value == "PAYMENT"

    def test_enum_has_exactly_four_members(self):
        assert len(TransactionType) == 4


class TestTransactionMessageSerialization:
    """Tests for serialization roundtrip."""

    def test_roundtrip_transfer(self):
        msg = TransactionMessage(
            transaction_id="abc-123",
            type=TransactionType.TRANSFER,
            from_account="ACC001",
            to_account="ACC002",
            amount=Decimal("1234.56"),
            timestamp="2026-01-01T00:00:00+00:00",
        )
        raw = msg.to_kafka_value()
        restored = TransactionMessage.from_kafka_value(raw)

        assert restored.transaction_id == msg.transaction_id
        assert restored.type == msg.type
        assert restored.from_account == msg.from_account
        assert restored.to_account == msg.to_account
        assert restored.amount == msg.amount
        assert restored.timestamp == msg.timestamp
        assert restored.metadata == {}

    def test_roundtrip_deposit(self):
        msg = TransactionMessage(
            transaction_id="dep-456",
            type=TransactionType.DEPOSIT,
            to_account="ACC003",
            amount=Decimal("5000.00"),
            timestamp="2026-02-15T12:30:00+00:00",
        )
        raw = msg.to_kafka_value()
        restored = TransactionMessage.from_kafka_value(raw)

        assert restored.type == TransactionType.DEPOSIT
        assert restored.from_account is None
        assert restored.to_account == "ACC003"
        assert restored.amount == Decimal("5000.00")

    def test_roundtrip_withdrawal(self):
        msg = TransactionMessage(
            transaction_id="wd-789",
            type=TransactionType.WITHDRAWAL,
            from_account="ACC005",
            amount=Decimal("200.75"),
            timestamp="2026-03-10T08:00:00+00:00",
        )
        raw = msg.to_kafka_value()
        restored = TransactionMessage.from_kafka_value(raw)

        assert restored.type == TransactionType.WITHDRAWAL
        assert restored.from_account == "ACC005"
        assert restored.to_account is None

    def test_roundtrip_with_metadata(self):
        msg = TransactionMessage(
            transaction_id="meta-001",
            type=TransactionType.TRANSFER,
            from_account="ACC001",
            to_account="ACC002",
            amount=Decimal("100.00"),
            timestamp="2026-01-01T00:00:00+00:00",
            metadata={"source": "api", "ip": "127.0.0.1"},
        )
        raw = msg.to_kafka_value()
        restored = TransactionMessage.from_kafka_value(raw)

        assert restored.metadata == {"source": "api", "ip": "127.0.0.1"}

    def test_decimal_precision_preserved(self):
        msg = TransactionMessage(
            transaction_id="prec-001",
            type=TransactionType.DEPOSIT,
            to_account="ACC001",
            amount=Decimal("9999.99"),
            timestamp="2026-01-01T00:00:00+00:00",
        )
        raw = msg.to_kafka_value()
        restored = TransactionMessage.from_kafka_value(raw)

        assert restored.amount == Decimal("9999.99")
        assert isinstance(restored.amount, Decimal)

    def test_decimal_small_value_preserved(self):
        msg = TransactionMessage(
            transaction_id="prec-002",
            type=TransactionType.WITHDRAWAL,
            from_account="ACC002",
            amount=Decimal("0.01"),
            timestamp="2026-01-01T00:00:00+00:00",
        )
        raw = msg.to_kafka_value()
        restored = TransactionMessage.from_kafka_value(raw)

        assert restored.amount == Decimal("0.01")


class TestTransactionMessageFromKafkaValue:
    """Tests for deserialization edge cases."""

    def test_valid_json(self):
        data = json.dumps(
            {
                "transaction_id": "test-id",
                "type": "TRANSFER",
                "from_account": "ACC001",
                "to_account": "ACC002",
                "amount": "500.00",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "metadata": {},
            }
        ).encode("utf-8")

        msg = TransactionMessage.from_kafka_value(data)
        assert msg.transaction_id == "test-id"
        assert msg.amount == Decimal("500.00")

    def test_invalid_json_raises(self):
        with pytest.raises(Exception):
            TransactionMessage.from_kafka_value(b"not-json")

    def test_missing_required_field_raises(self):
        data = json.dumps({"transaction_id": "x"}).encode("utf-8")
        with pytest.raises(Exception):
            TransactionMessage.from_kafka_value(data)

    def test_to_kafka_value_returns_bytes(self):
        msg = TransactionMessage(
            transaction_id="bytes-check",
            type=TransactionType.DEPOSIT,
            to_account="ACC001",
            amount=Decimal("100"),
            timestamp="2026-01-01T00:00:00+00:00",
        )
        result = msg.to_kafka_value()
        assert isinstance(result, bytes)

    def test_to_kafka_value_contains_enum_string(self):
        msg = TransactionMessage(
            transaction_id="enum-check",
            type=TransactionType.WITHDRAWAL,
            from_account="ACC003",
            amount=Decimal("50"),
            timestamp="2026-01-01T00:00:00+00:00",
        )
        raw = msg.to_kafka_value()
        parsed = json.loads(raw)
        assert parsed["type"] == "WITHDRAWAL"

    def test_to_kafka_value_contains_amount_as_string(self):
        msg = TransactionMessage(
            transaction_id="amt-check",
            type=TransactionType.DEPOSIT,
            to_account="ACC001",
            amount=Decimal("123.45"),
            timestamp="2026-01-01T00:00:00+00:00",
        )
        raw = msg.to_kafka_value()
        parsed = json.loads(raw)
        assert parsed["amount"] == "123.45"
        assert isinstance(parsed["amount"], str)
