"""Tests for the transactional Kafka producer."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.config import Settings
from src.models import TransactionMessage, TransactionType
from src.producer import ACCOUNT_POOL, TransactionalProducer


class TestProducerConfig:
    """Tests for producer configuration."""

    @patch("src.producer.Producer")
    def test_producer_uses_idempotence(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        config = Settings()
        TransactionalProducer(config)

        call_args = mock_producer_cls.call_args[0][0]
        assert call_args["enable.idempotence"] is True

    @patch("src.producer.Producer")
    def test_producer_uses_acks_all(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        config = Settings()
        TransactionalProducer(config)

        call_args = mock_producer_cls.call_args[0][0]
        assert call_args["acks"] == "all"

    @patch("src.producer.Producer")
    def test_producer_transactional_id(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        config = Settings()
        TransactionalProducer(config)

        call_args = mock_producer_cls.call_args[0][0]
        assert call_args["transactional.id"] == "txn-processor-producer"

    @patch("src.producer.Producer")
    def test_producer_max_in_flight(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        config = Settings()
        TransactionalProducer(config)

        call_args = mock_producer_cls.call_args[0][0]
        assert call_args["max.in.flight.requests.per.connection"] == 5

    @patch("src.producer.Producer")
    def test_producer_calls_init_transactions(self, mock_producer_cls):
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance
        config = Settings()
        TransactionalProducer(config)

        mock_instance.init_transactions.assert_called_once()


class TestGenerateTransaction:
    """Tests for random transaction generation."""

    @patch("src.producer.Producer")
    def test_returns_transaction_message(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        producer = TransactionalProducer(Settings())
        msg = producer._generate_transaction()

        assert isinstance(msg, TransactionMessage)
        assert msg.type in list(TransactionType)
        assert msg.amount > 0

    @patch("src.producer.Producer")
    def test_transfer_has_both_accounts(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        producer = TransactionalProducer(Settings())

        # Generate until we get a TRANSFER
        for _ in range(100):
            msg = producer._generate_transaction()
            if msg.type == TransactionType.TRANSFER:
                assert msg.from_account is not None
                assert msg.to_account is not None
                assert msg.from_account != msg.to_account
                assert msg.from_account in ACCOUNT_POOL
                assert msg.to_account in ACCOUNT_POOL
                assert Decimal("10") <= msg.amount <= Decimal("5000")
                return

        pytest.fail("No TRANSFER generated in 100 attempts")

    @patch("src.producer.Producer")
    def test_deposit_has_to_account_only(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        producer = TransactionalProducer(Settings())

        for _ in range(100):
            msg = producer._generate_transaction()
            if msg.type == TransactionType.DEPOSIT:
                assert msg.to_account is not None
                assert msg.from_account is None
                assert msg.to_account in ACCOUNT_POOL
                assert Decimal("100") <= msg.amount <= Decimal("10000")
                return

        pytest.fail("No DEPOSIT generated in 100 attempts")

    @patch("src.producer.Producer")
    def test_withdrawal_has_from_account_only(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        producer = TransactionalProducer(Settings())

        for _ in range(100):
            msg = producer._generate_transaction()
            if msg.type == TransactionType.WITHDRAWAL:
                assert msg.from_account is not None
                assert msg.to_account is None
                assert msg.from_account in ACCOUNT_POOL
                assert Decimal("10") <= msg.amount <= Decimal("2000")
                return

        pytest.fail("No WITHDRAWAL generated in 100 attempts")

    @patch("src.producer.Producer")
    def test_transaction_id_is_uuid(self, mock_producer_cls):
        import uuid

        mock_producer_cls.return_value = MagicMock()
        producer = TransactionalProducer(Settings())
        msg = producer._generate_transaction()

        # Should not raise
        uuid.UUID(msg.transaction_id)


class TestSendTransaction:
    """Tests for producing messages to Kafka."""

    @patch("src.producer.Producer")
    def test_send_calls_produce(self, mock_producer_cls):
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance
        config = Settings()
        producer = TransactionalProducer(config)

        msg = TransactionMessage(
            transaction_id="send-test",
            type=TransactionType.DEPOSIT,
            to_account="ACC001",
            amount=Decimal("500.00"),
            timestamp="2026-01-01T00:00:00+00:00",
        )
        producer.send_transaction(msg)

        mock_instance.produce.assert_called_once()
        call_kwargs = mock_instance.produce.call_args[1]
        assert call_kwargs["topic"] == config.topic_pending
        assert call_kwargs["key"] == b"send-test"
        assert isinstance(call_kwargs["value"], bytes)
        assert callable(call_kwargs["callback"])
